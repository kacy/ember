//! Cluster coordination layer for the ember server.
//!
//! Wraps the ember-cluster crate's types into a server-integrated
//! coordinator that handles gossip networking, cluster commands,
//! and slot ownership validation.
//!
//! Cluster configuration follows the Redis Cluster model. Each node owns
//! its slots at a config epoch and announces them over gossip; when two
//! nodes claim a slot, the higher epoch wins. Failover elections run over
//! gossip as well, and a promoted replica claims its slots at a new epoch.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use ember_cluster::{
    key_slot, ClusterNode, ClusterSecret, ClusterState, ConfigParseError, Election, GossipConfig,
    GossipEngine, GossipEvent, GossipMessage, MigrationManager, NodeId, NodeRole, SlotRange,
    SLOT_COUNT,
};
use ember_core::Engine;
use ember_protocol::Frame;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Where a command's keys are served, from [`ClusterCoordinator::check_slot_with_migration`].
#[derive(Debug, PartialEq)]
pub enum SlotCheck {
    /// Run the command here.
    Local,
    /// Reply with this error instead: MOVED, CLUSTERDOWN or TRYAGAIN.
    Reply(Frame),
    /// The slot is moving to `addr`. Serve the command here if its keys
    /// are still here, and send the client there with ASK if they are gone.
    AskIfMissing { slot: u16, addr: String },
}

mod commands;
mod failover;
mod gossip;
mod replication;

/// Snapshot of cluster health for the /health HTTP endpoint.
pub struct ClusterHealthSummary {
    pub state: String,
    pub known_nodes: usize,
    pub slots_assigned: usize,
}

/// Integration struct wrapping cluster crate types for the running server.
///
/// Thread-safe via interior mutability: `RwLock` for state (many readers,
/// rare writers) and `Mutex` for gossip (single writer during ticks).
pub struct ClusterCoordinator {
    state: RwLock<ClusterState>,
    gossip: Mutex<GossipEngine>,
    migration: Mutex<MigrationManager>,
    local_id: NodeId,
    gossip_port_offset: u16,
    /// local data-plane bind address (used to compute replication port)
    bind_addr: SocketAddr,
    /// bound UDP socket for gossip, set after spawn_gossip
    udp_socket: Mutex<Option<Arc<UdpSocket>>>,
    /// directory for nodes.conf persistence (None disables saving)
    data_dir: Option<PathBuf>,
    /// held for the whole of each nodes.conf save, so saves don't share
    /// the tmp file and the last one written holds the newest state
    save_lock: Mutex<()>,
    /// engine handle for replication; set once during startup
    engine: std::sync::OnceLock<Arc<Engine>>,
    /// temporarily pauses writes on this node during failover coordination.
    /// set by the primary when a replica requests failover; prevents new
    /// mutations from arriving after the replica has decided to promote.
    writes_paused: std::sync::atomic::AtomicBool,
    /// in-progress automatic failover election (we are the candidate).
    election: Mutex<Option<ElectionAttempt>>,
    /// the replica this node last voted for to replace each failed
    /// primary, and when. it votes for one replica per failed primary at a
    /// time, so two replicas of the same primary can't both win.
    failover_votes: Mutex<HashMap<NodeId, (NodeId, std::time::Instant)>>,
    /// optional shared secret for authenticating cluster transport messages.
    secret: Option<Arc<ClusterSecret>>,
    /// the task pulling data from our primary, while this node is a replica
    replication_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Tracks an in-progress automatic failover election that this node initiated.
struct ElectionAttempt {
    inner: Election,
    /// The primaries allowed to vote, fixed when the election starts. Votes
    /// from anyone else are ignored.
    voters: HashSet<NodeId>,
}

impl std::fmt::Debug for ClusterCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterCoordinator")
            .field("local_id", &self.local_id)
            .finish_non_exhaustive()
    }
}

impl ClusterCoordinator {
    /// Creates a new cluster coordinator.
    ///
    /// Returns the coordinator and a receiver for gossip events that
    /// should be consumed by a background task.
    ///
    /// # Errors
    ///
    /// Returns an error if `bind_addr.port() + gossip_config.gossip_port_offset` overflows u16.
    pub fn new(
        local_id: NodeId,
        bind_addr: SocketAddr,
        gossip_config: GossipConfig,
        bootstrap: bool,
        data_dir: Option<PathBuf>,
        secret: Option<Arc<ClusterSecret>>,
    ) -> Result<(Self, mpsc::Receiver<GossipEvent>), String> {
        let (event_tx, event_rx) = mpsc::channel(256);

        let port_offset = gossip_config.gossip_port_offset;
        let gossip_port = bind_addr.port().checked_add(port_offset).ok_or_else(|| {
            format!(
                "gossip port overflow: {} + {} exceeds u16 range",
                bind_addr.port(),
                port_offset
            )
        })?;
        let gossip_addr = SocketAddr::new(bind_addr.ip(), gossip_port);

        let mut gossip = GossipEngine::new(local_id, gossip_addr, gossip_config, event_tx);

        let state = if bootstrap {
            let mut node = ClusterNode::new_primary_with_offset(local_id, bind_addr, port_offset);
            node.set_myself();
            let cs = ClusterState::single_node(node);
            // Populate local_slots so Welcome replies correctly advertise all owned slots
            // instead of sending an empty list and triggering a stale SlotsChanged event.
            gossip.set_local_slots(cs.slot_map.slots_for_node(local_id), cs.local_epoch());
            cs
        } else {
            let mut cs = ClusterState::new(local_id);
            let mut node = ClusterNode::new_primary_with_offset(local_id, bind_addr, port_offset);
            node.set_myself();
            cs.add_node(node);
            cs
        };

        let coordinator = Self {
            state: RwLock::new(state),
            gossip: Mutex::new(gossip),
            migration: Mutex::new(MigrationManager::new()),
            local_id,
            gossip_port_offset: port_offset,
            bind_addr,
            udp_socket: Mutex::new(None),
            data_dir,
            engine: std::sync::OnceLock::new(),
            writes_paused: std::sync::atomic::AtomicBool::new(false),
            election: Mutex::new(None),
            failover_votes: Mutex::new(HashMap::new()),
            save_lock: Mutex::new(()),
            replication_task: Mutex::new(None),
            secret,
        };

        Ok((coordinator, event_rx))
    }

    /// Restores a cluster coordinator from a previously saved `nodes.conf`.
    ///
    /// The gossip engine is seeded with peer addresses from the loaded state
    /// so it can reconnect to existing cluster members.
    pub fn from_config(
        data: &str,
        bind_addr: SocketAddr,
        gossip_config: GossipConfig,
        data_dir: PathBuf,
        secret: Option<Arc<ClusterSecret>>,
    ) -> Result<(Self, mpsc::Receiver<GossipEvent>), ConfigParseError> {
        let (state, incarnation) = ClusterState::from_nodes_conf(data)?;
        let local_id = state.local_id;
        let port_offset = gossip_config.gossip_port_offset;

        let (event_tx, event_rx) = mpsc::channel(256);

        let gossip_port = bind_addr.port().checked_add(port_offset).ok_or_else(|| {
            ConfigParseError::InvalidAddress(format!(
                "gossip port overflow: {} + {} exceeds u16 range",
                bind_addr.port(),
                port_offset
            ))
        })?;
        let gossip_addr = SocketAddr::new(bind_addr.ip(), gossip_port);

        let mut gossip = GossipEngine::new(local_id, gossip_addr, gossip_config, event_tx);

        // restore incarnation so we don't regress
        gossip.set_incarnation(incarnation);

        // seed gossip with known peers so it can reconnect
        for node in state.nodes.values() {
            if node.id != local_id {
                gossip.add_seed(node.id, node.cluster_bus_addr);
            }
        }

        // set local slots and role in gossip engine
        let local_slots = state.slot_map.slots_for_node(local_id);
        gossip.set_local_slots(local_slots, state.local_epoch());
        gossip.set_local_role(state.nodes.get(&local_id).and_then(|n| n.replicates));

        let coordinator = Self {
            state: RwLock::new(state),
            gossip: Mutex::new(gossip),
            migration: Mutex::new(MigrationManager::new()),
            local_id,
            gossip_port_offset: port_offset,
            bind_addr,
            udp_socket: Mutex::new(None),
            data_dir: Some(data_dir),
            engine: std::sync::OnceLock::new(),
            writes_paused: std::sync::atomic::AtomicBool::new(false),
            election: Mutex::new(None),
            failover_votes: Mutex::new(HashMap::new()),
            save_lock: Mutex::new(()),
            replication_task: Mutex::new(None),
            secret,
        };

        Ok((coordinator, event_rx))
    }

    /// Returns a snapshot of cluster health for the /health HTTP endpoint.
    ///
    /// Acquires a read lock briefly to copy out the summary values.
    pub async fn health_summary(&self) -> ClusterHealthSummary {
        let state = self.state.read().await;
        let assigned = SLOT_COUNT as usize - state.slot_map.unassigned_count();
        ClusterHealthSummary {
            state: state.state.to_string(),
            known_nodes: state.nodes.len(),
            slots_assigned: assigned,
        }
    }

    // -- cluster command handlers --

    // -- slot migration (SETSLOT) commands --

    // -- slot ownership check --

    // -- replication --

    /// Returns `true` if this node is currently configured as a replica.
    pub async fn is_replica(&self) -> bool {
        let state = self.state.read().await;
        state
            .nodes
            .get(&self.local_id)
            .map(|n| n.role == NodeRole::Replica)
            .unwrap_or(false)
    }

    /// Returns `true` if writes are temporarily paused on this node.
    ///
    /// Set by the primary during failover coordination to prevent new mutations
    /// from arriving after the replica has committed to promoting.
    pub fn is_writes_paused(&self) -> bool {
        self.writes_paused
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Pauses write commands on this node.
    ///
    /// Called by the primary when a replica requests a coordinated failover,
    /// ensuring no new writes arrive after the replica decides to promote.
    #[allow(dead_code)]
    pub fn pause_writes(&self) {
        self.writes_paused
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Resumes write commands after a failover pause.
    #[allow(dead_code)]
    pub fn resume_writes(&self) {
        self.writes_paused
            .store(false, std::sync::atomic::Ordering::Release);
    }

    // -- automatic failover --

    /// Persists the current cluster state to `nodes.conf` in the data directory.
    ///
    /// Writes a tmp file and renames it over the old one, so a crash
    /// mid-write leaves the previous config. The file write and fsyncs run
    /// on a blocking thread. This is called after every topology mutation.
    pub async fn save_config(&self) {
        let Some(ref dir) = self.data_dir else {
            return;
        };
        // render the state while holding the lock, so a later save always
        // writes a state at least as new as an earlier one
        let _saving = self.save_lock.lock().await;

        let incarnation = {
            let gossip = self.gossip.lock().await;
            gossip.local_incarnation()
        };
        let content = {
            let state = self.state.read().await;
            state.to_nodes_conf(incarnation)
        };

        let dir = dir.clone();
        let written = tokio::task::spawn_blocking(move || {
            write_atomic(&dir, "nodes.conf", content.as_bytes())
        })
        .await
        .unwrap_or_else(|e| Err(std::io::Error::other(e)));
        if let Err(e) = written {
            error!("failed to save nodes.conf: {e}");
        }
    }

    // -- gossip networking --
}

/// Writes data to a temporary file and atomically renames it to the target path.
///
/// Ensures the file is fully flushed before rename so a crash mid-write
/// never leaves a partial `nodes.conf`.
fn write_atomic(dir: &std::path::Path, name: &str, data: &[u8]) -> std::io::Result<()> {
    use std::io::Write;

    let target = dir.join(name);
    let tmp = dir.join(format!("{name}.tmp"));
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(data)?;
    f.sync_all()?;
    std::fs::rename(&tmp, &target)?;
    // fsync the directory so the rename itself survives a crash
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests;
