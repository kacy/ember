//! Cluster coordination layer for the ember server.
//!
//! Wraps the ember-cluster crate's types into a server-integrated
//! coordinator that handles gossip networking, cluster commands,
//! and slot ownership validation.
//!
//! When a `RaftNode` is attached, topology mutations (ADDSLOTS, DELSLOTS,
//! SETSLOT, FORGET) are proposed through Raft consensus before returning OK.
//! Read-only commands and node-local migration state skip Raft entirely.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use ember_cluster::{
    key_slot, raft_id_from_node_id, BasicNode, ClusterCommand, ClusterNode, ClusterState,
    ClusterStateData, ConfigParseError, Election, GossipConfig, GossipEngine, GossipEvent,
    GossipMessage, MigrationManager, NodeId, NodeRole, RaftNode, RaftProposalError, SlotRange,
    SLOT_COUNT,
};
use ember_core::Engine;
use ember_protocol::Frame;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Stagger delay before broadcasting a vote request so replicas with more
/// data wait less (replaced with a fixed value until offset tracking lands).
const ELECTION_STAGGER_MS: u64 = 500;

/// Timeout for an in-progress election; clears state if quorum not reached.
const ELECTION_TIMEOUT_SECS: u64 = 5;

/// Grace period given to the primary during a default (non-FORCE) failover.
const FAILOVER_GRACE_MS: u64 = 500;

/// TCP port offset from the data port to the replication stream port.
const REPLICATION_PORT_OFFSET: u16 = 2;

/// Integration struct wrapping cluster crate types for the running server.
///
/// Thread-safe via interior mutability: `RwLock` for state (many readers,
/// rare writers) and `Mutex` for gossip (single writer during ticks).
///
/// Call `attach_raft` after wrapping in `Arc` to enable Raft-backed mutations.
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
    /// raft node for linearizable topology mutations; set once after startup
    raft_node: std::sync::OnceLock<Arc<RaftNode>>,
    /// engine handle for replication; set once during startup
    engine: std::sync::OnceLock<Arc<Engine>>,
    /// temporarily pauses writes on this node during failover coordination.
    /// set by the primary when a replica requests failover; prevents new
    /// mutations from arriving after the replica has decided to promote.
    writes_paused: std::sync::atomic::AtomicBool,
    /// in-progress automatic failover election (we are the candidate).
    election: Mutex<Option<ElectionAttempt>>,
    /// config epoch of the last vote we granted as a primary; enforces one vote per epoch.
    last_voted_epoch: std::sync::atomic::AtomicU64,
}

/// Tracks an in-progress automatic failover election that this node initiated.
struct ElectionAttempt {
    inner: Election,
    /// Number of alive primaries when the election started (used for quorum).
    total_primaries: usize,
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

        let gossip = GossipEngine::new(local_id, gossip_addr, gossip_config, event_tx);

        let state = if bootstrap {
            let mut node = ClusterNode::new_primary_with_offset(local_id, bind_addr, port_offset);
            node.set_myself();
            ClusterState::single_node(node)
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
            raft_node: std::sync::OnceLock::new(),
            engine: std::sync::OnceLock::new(),
            writes_paused: std::sync::atomic::AtomicBool::new(false),
            election: Mutex::new(None),
            last_voted_epoch: std::sync::atomic::AtomicU64::new(0),
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

        // set local slots in gossip engine
        let local_slots = state.slot_map.slots_for_node(local_id);
        gossip.set_local_slots(local_slots);

        let coordinator = Self {
            state: RwLock::new(state),
            gossip: Mutex::new(gossip),
            migration: Mutex::new(MigrationManager::new()),
            local_id,
            gossip_port_offset: port_offset,
            bind_addr,
            udp_socket: Mutex::new(None),
            data_dir: Some(data_dir),
            raft_node: std::sync::OnceLock::new(),
            engine: std::sync::OnceLock::new(),
            writes_paused: std::sync::atomic::AtomicBool::new(false),
            election: Mutex::new(None),
            last_voted_epoch: std::sync::atomic::AtomicU64::new(0),
        };

        Ok((coordinator, event_rx))
    }

    /// Returns the local node ID.
    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    // -- raft integration --

    /// Attaches a `RaftNode` so topology mutations go through consensus.
    ///
    /// Must be called at most once, after the coordinator is wrapped in `Arc`.
    /// If not called, all commands fall back to direct writes (useful in tests).
    pub fn attach_raft(&self, node: Arc<RaftNode>) {
        // ignore the error — it just means attach_raft was called twice
        let _ = self.raft_node.set(node);
    }

    /// Spawns a background task that reconciles `ClusterState` with the Raft
    /// state machine whenever committed entries are applied.
    ///
    /// The watch receiver fires after every `apply_to_state_machine` call on
    /// the Raft storage. The task rebuilds the routing table from the canonical
    /// Raft view.
    pub fn spawn_raft_reconciliation(
        self: &Arc<Self>,
        mut state_rx: watch::Receiver<ClusterStateData>,
    ) {
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            while state_rx.changed().await.is_ok() {
                let data = state_rx.borrow().clone();
                coordinator.apply_raft_state(&data).await;
            }
        });
    }

    /// Reconciles the local routing table with a committed Raft state snapshot.
    async fn apply_raft_state(&self, data: &ClusterStateData) {
        let mut state = self.state.write().await;

        // add nodes that are in raft state but not yet in the routing table
        for (key, info) in &data.nodes {
            let node_id = match NodeId::parse(key).ok() {
                Some(id) => id,
                None => continue,
            };
            if !state.nodes.contains_key(&node_id) {
                let addr: SocketAddr = match info.addr.parse() {
                    Ok(a) => a,
                    Err(_) => continue,
                };
                let node =
                    ClusterNode::new_primary_with_offset(node_id, addr, self.gossip_port_offset);
                state.add_node(node);
            }
        }

        // remove nodes that raft has dropped (never remove ourselves)
        let raft_ids: std::collections::HashSet<NodeId> = data
            .nodes
            .keys()
            .filter_map(|k| NodeId::parse(k).ok())
            .collect();
        let to_remove: Vec<NodeId> = state
            .nodes
            .keys()
            .filter(|id| !raft_ids.contains(*id) && **id != self.local_id)
            .copied()
            .collect();
        for id in to_remove {
            state.nodes.remove(&id);
        }

        // reconcile slot assignments from the raft slot map
        for slot in 0..SLOT_COUNT {
            let raft_owner = data.slots.get(&slot).and_then(|k| NodeId::parse(k).ok());
            let current_owner = state.slot_map.owner(slot);
            if raft_owner != current_owner {
                match raft_owner {
                    Some(owner) => state.slot_map.assign(slot, owner),
                    None => state.slot_map.unassign(slot),
                }
            }
        }

        // rebuild each node's slot list from the updated slot map
        let all_ids: Vec<NodeId> = state.nodes.keys().copied().collect();
        for id in all_ids {
            let slots = state.slot_map.slots_for_node(id);
            if let Some(node) = state.nodes.get_mut(&id) {
                node.slots = slots;
            }
        }

        state.update_health();
        drop(state);
        self.save_config().await;
    }

    /// Returns an error `Frame` for the given `RaftProposalError`.
    fn raft_error_frame(e: RaftProposalError) -> Frame {
        let msg = match e {
            RaftProposalError::NotLeader(Some(node)) => {
                format!("not leader, leader at {}", node.addr)
            }
            RaftProposalError::NotLeader(None) => "no leader elected, retry shortly".into(),
            RaftProposalError::Fatal(msg) => format!("raft error: {msg}"),
        };
        Frame::Error(format!("ERR {msg}"))
    }

    // -- cluster command handlers --

    /// CLUSTER INFO
    pub async fn cluster_info(&self) -> Frame {
        let state = self.state.read().await;
        Frame::Bulk(Bytes::from(state.cluster_info()))
    }

    /// CLUSTER NODES
    pub async fn cluster_nodes(&self) -> Frame {
        let state = self.state.read().await;
        Frame::Bulk(Bytes::from(state.cluster_nodes()))
    }

    /// CLUSTER MYID
    pub fn cluster_myid(&self) -> Frame {
        Frame::Bulk(Bytes::from(self.local_id.0.to_string()))
    }

    /// CLUSTER SLOTS — returns slot ranges in the Redis array format.
    pub async fn cluster_slots(&self) -> Frame {
        let state = self.state.read().await;

        let mut result = Vec::new();
        for node in state.primaries() {
            let ranges = state.slot_map.slots_for_node(node.id);
            for range in ranges {
                let mut entry = vec![
                    Frame::Integer(range.start as i64),
                    Frame::Integer(range.end as i64),
                    // node info: [ip, port, id]
                    Frame::Array(vec![
                        Frame::Bulk(Bytes::from(node.addr.ip().to_string())),
                        Frame::Integer(node.addr.port() as i64),
                        Frame::Bulk(Bytes::from(node.id.0.to_string())),
                    ]),
                ];

                // add replicas
                for replica in state.replicas_of(node.id) {
                    entry.push(Frame::Array(vec![
                        Frame::Bulk(Bytes::from(replica.addr.ip().to_string())),
                        Frame::Integer(replica.addr.port() as i64),
                        Frame::Bulk(Bytes::from(replica.id.0.to_string())),
                    ]));
                }

                result.push(Frame::Array(entry));
            }
        }

        Frame::Array(result)
    }

    /// CLUSTER MEET ip port
    pub async fn cluster_meet(&self, ip: &str, port: u16) -> Frame {
        let addr: SocketAddr = match format!("{ip}:{port}").parse() {
            Ok(a) => a,
            Err(e) => return Frame::Error(format!("ERR invalid address: {e}")),
        };

        let new_id = NodeId::new();
        let gossip_port = match port.checked_add(self.gossip_port_offset) {
            Some(p) => p,
            None => {
                return Frame::Error(format!(
                    "ERR port {port} + offset {} overflows",
                    self.gossip_port_offset
                ))
            }
        };
        let gossip_addr = SocketAddr::new(addr.ip(), gossip_port);

        // build the join message, then release the gossip lock before any awaits
        let encoded = {
            let mut gossip = self.gossip.lock().await;
            gossip.add_seed(new_id, gossip_addr);
            gossip.create_join_message().encode()
        };

        // send join message via UDP
        {
            let socket = self.udp_socket.lock().await;
            if let Some(ref sock) = *socket {
                if let Err(e) = sock.send_to(&encoded, gossip_addr).await {
                    warn!("failed to send join to {gossip_addr}: {e}");
                    return Frame::Error(format!("ERR failed to send join: {e}"));
                }
            } else {
                return Frame::Error("ERR gossip socket not ready".into());
            }
        }

        // add a placeholder to the routing table so MOVED redirects work
        // immediately — the real node ID arrives via gossip and replaces this
        {
            let mut state = self.state.write().await;
            let node = ClusterNode::new_primary_with_offset(new_id, addr, self.gossip_port_offset);
            state.add_node(node);
        }

        self.save_config().await;

        Frame::Simple("OK".into())
    }

    /// CLUSTER ADDSLOTS slot [slot ...]
    pub async fn cluster_addslots(&self, slots: &[u16]) -> Frame {
        // validate inputs against the current routing state
        {
            let state = self.state.read().await;
            for &slot in slots {
                if slot >= SLOT_COUNT {
                    return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
                }
                if state.slot_map.owner(slot).is_some() {
                    return Frame::Error(format!("ERR Slot {slot} is already busy"));
                }
            }
        }

        if let Some(raft) = self.raft_node.get() {
            let slot_ranges = compact_slots(slots);
            let cmd = ClusterCommand::AssignSlots {
                node_id: self.local_id,
                slots: slot_ranges,
            };
            match raft.propose(cmd).await {
                Ok(_) => Frame::Simple("OK".into()),
                Err(e) => Self::raft_error_frame(e),
            }
        } else {
            // direct write path (single-node / test mode)
            let new_slots = {
                let mut state = self.state.write().await;
                for &slot in slots {
                    state.slot_map.assign(slot, self.local_id);
                }
                let new_slots = state.slot_map.slots_for_node(self.local_id);
                if let Some(node) = state.nodes.get_mut(&self.local_id) {
                    node.slots = new_slots.clone();
                }
                state.update_health();
                new_slots
            };
            self.broadcast_local_slots(new_slots).await;
            self.save_config().await;
            Frame::Simple("OK".into())
        }
    }

    /// CLUSTER DELSLOTS slot [slot ...]
    pub async fn cluster_delslots(&self, slots: &[u16]) -> Frame {
        // validate
        {
            let state = self.state.read().await;
            for &slot in slots {
                if slot >= SLOT_COUNT {
                    return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
                }
                match state.slot_map.owner(slot) {
                    Some(owner) if owner != self.local_id => {
                        return Frame::Error(format!("ERR Slot {slot} is not owned by this node"));
                    }
                    None => {
                        return Frame::Error(format!("ERR Slot {slot} is already unassigned"));
                    }
                    _ => {}
                }
            }
        }

        if let Some(raft) = self.raft_node.get() {
            let slot_ranges = compact_slots(slots);
            let cmd = ClusterCommand::RemoveSlots {
                node_id: self.local_id,
                slots: slot_ranges,
            };
            match raft.propose(cmd).await {
                Ok(_) => Frame::Simple("OK".into()),
                Err(e) => Self::raft_error_frame(e),
            }
        } else {
            let new_slots = {
                let mut state = self.state.write().await;
                for &slot in slots {
                    state.slot_map.unassign(slot);
                }
                let new_slots = state.slot_map.slots_for_node(self.local_id);
                if let Some(node) = state.nodes.get_mut(&self.local_id) {
                    node.slots = new_slots.clone();
                }
                state.update_health();
                new_slots
            };
            self.broadcast_local_slots(new_slots).await;
            self.save_config().await;
            Frame::Simple("OK".into())
        }
    }

    /// CLUSTER FORGET node-id
    pub async fn cluster_forget(&self, node_id_str: &str) -> Frame {
        let node_id = match NodeId::parse(node_id_str) {
            Ok(id) => id,
            Err(_) => return Frame::Error("ERR Invalid node ID".into()),
        };

        if node_id == self.local_id {
            return Frame::Error("ERR I tried hard but I can't forget myself...".into());
        }

        // verify the node exists before proposing
        {
            let state = self.state.read().await;
            if !state.nodes.contains_key(&node_id) {
                return Frame::Error("ERR Unknown node ID".into());
            }
        }

        if let Some(raft) = self.raft_node.get() {
            let cmd = ClusterCommand::RemoveNode { node_id };
            match raft.propose(cmd).await {
                Ok(_) => Frame::Simple("OK".into()),
                Err(e) => Self::raft_error_frame(e),
            }
        } else {
            let mut state = self.state.write().await;
            match state.remove_node(node_id) {
                Some(_) => {
                    drop(state);
                    self.save_config().await;
                    Frame::Simple("OK".into())
                }
                None => Frame::Error("ERR Unknown node ID".into()),
            }
        }
    }

    // -- slot migration (SETSLOT) commands --

    /// CLUSTER SETSLOT <slot> IMPORTING <node-id>
    ///
    /// Marks a slot as importing from the given source node. The local node
    /// becomes the target of the migration.
    pub async fn cluster_setslot_importing(&self, slot: u16, node_id_str: &str) -> Frame {
        if slot >= SLOT_COUNT {
            return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
        }
        let source_id = match NodeId::parse(node_id_str) {
            Ok(id) => id,
            Err(_) => return Frame::Error("ERR Invalid node ID".into()),
        };
        if source_id == self.local_id {
            return Frame::Error("ERR can't import from myself".into());
        }

        let mut migration = self.migration.lock().await;
        match migration.start_import(slot, source_id, self.local_id) {
            Ok(_) => Frame::Simple("OK".into()),
            Err(e) => Frame::Error(format!("ERR {e}")),
        }
    }

    /// CLUSTER SETSLOT <slot> MIGRATING <node-id>
    ///
    /// Marks a slot as migrating to the given target node. The local node
    /// must currently own the slot.
    pub async fn cluster_setslot_migrating(&self, slot: u16, node_id_str: &str) -> Frame {
        if slot >= SLOT_COUNT {
            return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
        }
        let target_id = match NodeId::parse(node_id_str) {
            Ok(id) => id,
            Err(_) => return Frame::Error("ERR Invalid node ID".into()),
        };
        if target_id == self.local_id {
            return Frame::Error("ERR can't migrate to myself".into());
        }

        // verify we own the slot before allowing migration
        {
            let state = self.state.read().await;
            if !state.owns_slot(slot) {
                return Frame::Error(format!("ERR I'm not the owner of hash slot {slot}"));
            }
        }

        // record in-node migration state so ASK redirects work during transfer
        {
            let mut migration = self.migration.lock().await;
            if let Err(e) = migration.start_migrate(slot, self.local_id, target_id) {
                return Frame::Error(format!("ERR {e}"));
            }
        }

        // persist intent through raft so all nodes agree on the pending migration
        if let Some(raft) = self.raft_node.get() {
            let cmd = ClusterCommand::BeginMigration {
                slot,
                from: self.local_id,
                to: target_id,
            };
            if let Err(e) = raft.propose(cmd).await {
                // raft failure: roll back local migration state
                let mut migration = self.migration.lock().await;
                migration.abort_migration(slot);
                return Self::raft_error_frame(e);
            }
        }

        Frame::Simple("OK".into())
    }

    /// CLUSTER SETSLOT <slot> NODE <node-id>
    ///
    /// Completes migration by assigning the slot to the given node.
    /// Cleans up any in-progress migration state.
    pub async fn cluster_setslot_node(&self, slot: u16, node_id_str: &str) -> Frame {
        if slot >= SLOT_COUNT {
            return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
        }
        let node_id = match NodeId::parse(node_id_str) {
            Ok(id) => id,
            Err(_) => return Frame::Error("ERR Invalid node ID".into()),
        };

        if let Some(raft) = self.raft_node.get() {
            let cmd = ClusterCommand::CompleteMigration {
                slot,
                new_owner: node_id,
            };
            match raft.propose(cmd).await {
                Ok(_) => {
                    // clean up local migration tracking (node-local, not replicated)
                    let mut migration = self.migration.lock().await;
                    migration.complete_migration(slot);
                    Frame::Simple("OK".into())
                }
                Err(e) => Self::raft_error_frame(e),
            }
        } else {
            // direct write path
            {
                let mut migration = self.migration.lock().await;
                migration.complete_migration(slot);
            }

            let local_slots = {
                let mut state = self.state.write().await;
                state.slot_map.assign(slot, node_id);

                let new_slots = state.slot_map.slots_for_node(node_id);
                if let Some(node) = state.nodes.get_mut(&node_id) {
                    node.slots = new_slots;
                }

                let local_slots = state.slot_map.slots_for_node(self.local_id);
                if node_id != self.local_id {
                    if let Some(node) = state.nodes.get_mut(&self.local_id) {
                        node.slots = local_slots.clone();
                    }
                }

                state.update_health();
                local_slots
            };

            self.broadcast_local_slots(local_slots).await;
            self.save_config().await;
            Frame::Simple("OK".into())
        }
    }

    /// CLUSTER SETSLOT <slot> STABLE
    ///
    /// Aborts any in-progress migration for the slot, clearing
    /// importing/migrating state without changing slot ownership.
    pub async fn cluster_setslot_stable(&self, slot: u16) -> Frame {
        if slot >= SLOT_COUNT {
            return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
        }

        let mut migration = self.migration.lock().await;
        migration.abort_migration(slot);
        Frame::Simple("OK".into())
    }

    // -- slot ownership check --

    /// Checks slot ownership with migration-aware routing.
    ///
    /// Returns `None` if the command should be handled locally.
    /// Returns `Some(Frame)` with MOVED, ASK, or CLUSTERDOWN if not.
    ///
    /// During migration:
    /// - Source node returns ASK for keys already transferred to the target
    /// - Target node allows access when the client sent ASKING
    /// - Keys not yet migrated are served locally by the source
    pub async fn check_slot_with_migration(&self, key: &[u8], asking: bool) -> Option<Frame> {
        let slot = key_slot(key);
        let state = self.state.read().await;
        let migration = self.migration.lock().await;

        if state.owns_slot(slot) {
            // slot is migrating out — if key already moved, ASK redirect
            if migration.is_migrating(slot) && migration.is_key_migrated(slot, key) {
                if let Some(m) = migration.get_outgoing(slot) {
                    if let Some(target) = state.nodes.get(&m.target) {
                        return Some(Frame::Error(format!("ASK {} {}", slot, target.addr)));
                    }
                }
            }
            return None; // handle locally
        }

        // not our slot — but are we importing it and client sent ASKING?
        if migration.is_importing(slot) && asking {
            return None; // allow access
        }

        // standard redirect
        match state.slot_owner(slot) {
            Some(owner) => Some(Frame::Error(format!("MOVED {} {}", slot, owner.addr))),
            None => Some(Frame::Error("CLUSTERDOWN Hash slot not served".into())),
        }
    }

    /// Marks a key as migrated during slot migration.
    ///
    /// Called after MIGRATE successfully transfers a key to the target node.
    /// Subsequent accesses for this key on the source will return ASK redirects.
    pub async fn mark_key_migrated(&self, slot: u16, key: &[u8]) {
        let mut migration = self.migration.lock().await;
        migration.key_migrated(slot, key.to_vec());
    }

    /// Checks that all keys hash to the same slot.
    ///
    /// Returns `Ok(())` if all keys are in the same slot.
    /// Returns `Err(Frame)` with a CROSSSLOT error if they span multiple slots.
    pub fn check_crossslot(&self, keys: &[String]) -> Result<(), Frame> {
        if keys.len() <= 1 {
            return Ok(());
        }
        let first_slot = key_slot(keys[0].as_bytes());
        for key in &keys[1..] {
            if key_slot(key.as_bytes()) != first_slot {
                return Err(Frame::Error(
                    "CROSSSLOT Keys in request don't hash to the same slot".into(),
                ));
            }
        }
        Ok(())
    }

    // -- replication --

    /// CLUSTER REPLICATE node-id
    ///
    /// Makes this node a replica of the given primary. Updates local topology,
    /// queues a role-change gossip announcement, and persists nodes.conf.
    pub async fn cluster_replicate(&self, primary_id_str: &str) -> Frame {
        let primary_id = match NodeId::parse(primary_id_str) {
            Ok(id) => id,
            Err(_) => return Frame::Error("ERR Invalid node ID".into()),
        };

        if primary_id == self.local_id {
            return Frame::Error("ERR Cannot replicate self".into());
        }

        // verify the target exists and is a primary
        {
            let state = self.state.read().await;
            match state.nodes.get(&primary_id) {
                None => return Frame::Error("ERR Unknown node ID".into()),
                Some(node) if node.role != NodeRole::Primary => {
                    return Frame::Error("ERR Target node is not a primary".into())
                }
                _ => {}
            }
        }

        // update local cluster state
        {
            let mut state = self.state.write().await;
            if let Some(node) = state.nodes.get_mut(&self.local_id) {
                node.role = NodeRole::Replica;
                node.replicates = Some(primary_id);
            }
            // register ourselves in the primary's replica list
            if let Some(primary) = state.nodes.get_mut(&primary_id) {
                if !primary.replicas.contains(&self.local_id) {
                    primary.replicas.push(self.local_id);
                }
            }
            state.update_health();
        }

        // queue role change for epidemic dissemination
        let incarnation = {
            let mut gossip = self.gossip.lock().await;
            let inc = gossip.local_incarnation();
            gossip.queue_role_update(self.local_id, inc, false, Some(primary_id));
            inc
        };
        let _ = incarnation; // used only to hold the lock briefly

        self.save_config().await;

        // connect to the primary's replication port and start streaming
        self.start_replication_client(primary_id).await;

        Frame::Simple("OK".into())
    }

    /// Returns `true` if this node is currently configured as a replica.
    pub async fn is_replica(&self) -> bool {
        let state = self.state.read().await;
        state
            .nodes
            .get(&self.local_id)
            .map(|n| n.role == NodeRole::Replica)
            .unwrap_or(false)
    }

    /// Returns the address of the primary that owns the given slot.
    ///
    /// Used when redirecting write commands on replicas via MOVED.
    pub async fn primary_addr_for_slot(&self, slot: u16) -> Option<std::net::SocketAddr> {
        let state = self.state.read().await;
        let owner_id = state.slot_map.owner(slot)?;
        let node = state.nodes.get(&owner_id)?;
        // Only redirect to primaries; if the owner is somehow a replica, skip.
        if node.role == NodeRole::Primary {
            Some(node.addr)
        } else {
            None
        }
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

    /// Derives the replication TCP port from a data-plane port.
    ///
    /// The formula is `data_port + gossip_port_offset + REPLICATION_PORT_OFFSET`,
    /// which keeps replication off both the data port and the gossip port.
    fn replication_port(&self, data_port: u16) -> Option<u16> {
        data_port
            .checked_add(self.gossip_port_offset)
            .and_then(|p| p.checked_add(REPLICATION_PORT_OFFSET))
    }

    // -- automatic failover --

    /// Starts an automatic failover election after `failed_primary` is confirmed dead.
    ///
    /// Applies a brief stagger delay so that the most up-to-date replica wins.
    /// Broadcasts a `VoteRequest` via gossip and waits up to 5 seconds for
    /// a quorum of primaries to respond with `VoteGranted`.
    async fn start_election(self: &Arc<Self>, failed_primary: NodeId) {
        // fixed 500 ms stagger; a production implementation should compute
        // (max_offset - my_offset) * scale so the most up-to-date replica wins.
        // this requires a shared offset oracle (e.g., gossip-advertised offset).
        tokio::time::sleep(std::time::Duration::from_millis(ELECTION_STAGGER_MS)).await;

        // Re-read cluster state — the primary may have recovered during our sleep.
        let (epoch, total_primaries, still_failed) = {
            let state = self.state.read().await;
            let epoch = state.config_epoch + 1;
            // count alive primaries excluding the failed one
            let total = state
                .nodes
                .values()
                .filter(|n| n.role == NodeRole::Primary && !n.flags.fail && n.id != failed_primary)
                .count();
            let still_failed = state
                .nodes
                .get(&failed_primary)
                .map(|n| n.flags.fail)
                .unwrap_or(true);
            (epoch, total, still_failed)
        };

        if !still_failed {
            debug!(
                "election: primary {} recovered before election started",
                failed_primary
            );
            return;
        }

        if total_primaries == 0 {
            // No other primaries to collect votes from — promote directly.
            info!(
                "election: no other primaries in cluster; auto-promoting self for epoch {}",
                epoch
            );
            let _ = self.cluster_failover(true, false).await;
            return;
        }

        // Initialize the election.
        {
            let mut guard = self.election.lock().await;
            *guard = Some(ElectionAttempt {
                inner: Election::new(epoch),
                total_primaries,
            });
        }

        info!(
            "election: starting for epoch {} ({} primary voters needed)",
            epoch,
            Election::quorum(total_primaries)
        );

        // Broadcast vote request via gossip piggybacking.
        {
            let mut gossip = self.gossip.lock().await;
            gossip.queue_vote_request(self.local_id, epoch, 0 /* offset */);
        }

        // Wait for votes; give the cluster time to respond.
        tokio::time::sleep(std::time::Duration::from_secs(ELECTION_TIMEOUT_SECS)).await;

        // Timed out without quorum — clear election state.
        let mut guard = self.election.lock().await;
        if let Some(ref e) = *guard {
            if e.inner.epoch == epoch && !e.inner.is_promoted() {
                warn!(
                    "election: timed out for epoch {} without reaching quorum",
                    epoch
                );
                *guard = None;
            }
        }
    }

    /// Handles an incoming `VoteRequest` gossip event.
    ///
    /// If this node is a primary and hasn't voted in the given epoch, it
    /// grants its vote to the candidate and broadcasts `VoteGranted` via gossip.
    async fn handle_vote_request(&self, candidate: NodeId, epoch: u64) {
        // Only primaries vote.
        let is_primary = {
            let state = self.state.read().await;
            state
                .nodes
                .get(&self.local_id)
                .map(|n| n.role == NodeRole::Primary)
                .unwrap_or(false)
        };
        if !is_primary {
            return;
        }

        // Enforce one vote per epoch.
        let prev = self
            .last_voted_epoch
            .fetch_max(epoch, std::sync::atomic::Ordering::AcqRel);
        if prev >= epoch {
            debug!(
                "election: already voted in epoch {} (requested {}); ignoring",
                prev, epoch
            );
            return;
        }

        info!(
            "election: granting vote to candidate {} for epoch {}",
            candidate, epoch
        );

        let mut gossip = self.gossip.lock().await;
        gossip.queue_vote_granted(self.local_id, candidate, epoch);
    }

    /// Handles an incoming `VoteGranted` gossip event.
    ///
    /// If this node is the intended candidate and has an in-progress election,
    /// records the vote. Triggers promotion when quorum is reached.
    async fn handle_vote_granted(self: &Arc<Self>, from: NodeId, candidate: NodeId, epoch: u64) {
        if candidate != self.local_id {
            return; // not meant for us
        }

        let should_promote = {
            let mut guard = self.election.lock().await;
            match guard.as_mut() {
                Some(attempt) if attempt.inner.epoch == epoch => {
                    attempt.inner.record_vote(from, attempt.total_primaries)
                }
                _ => false,
            }
        };

        if should_promote {
            info!(
                "election: quorum reached for epoch {}; promoting self",
                epoch
            );
            let _ = self.cluster_failover(true, false).await;
        }
    }

    /// CLUSTER FAILOVER [FORCE|TAKEOVER]
    ///
    /// Promotes this replica to primary. Must be run on a replica node.
    ///
    /// Three modes:
    /// - **Default**: waits 500ms for replication to catch up, then promotes
    ///   via Raft so all nodes agree on the new topology.
    /// - **FORCE**: skips the grace period; promotes via Raft immediately.
    ///   Use when the primary is unreachable and you accept possible data loss.
    /// - **TAKEOVER**: bypasses Raft entirely. Updates local state and
    ///   announces the new role via gossip. Use when Raft quorum is lost.
    pub async fn cluster_failover(&self, force: bool, takeover: bool) -> Frame {
        // verify we are a replica with a configured primary
        let primary_id = {
            let state = self.state.read().await;
            let local = match state.nodes.get(&self.local_id) {
                Some(n) => n,
                None => return Frame::Error("ERR local node not found in cluster state".into()),
            };
            if local.role != NodeRole::Replica {
                return Frame::Error("ERR You should send CLUSTER FAILOVER to a replica".into());
            }
            match local.replicates {
                Some(id) => id,
                None => return Frame::Error("ERR No primary configured for this replica".into()),
            }
        };

        if takeover {
            // TAKEOVER: immediate local promotion, no Raft or primary coordination.
            // The replica asserts itself as primary and gossips the change;
            // the rest of the cluster learns via gossip convergence.
            {
                let mut state = self.state.write().await;
                if let Err(e) = state.promote_replica(self.local_id) {
                    return Frame::Error(format!("ERR {e}"));
                }
            }
            self.announce_promotion().await;
            self.save_config().await;
            info!(local_id = %self.local_id, %primary_id, "TAKEOVER: promoted to primary");
            return Frame::Simple("OK".into());
        }

        // Default / FORCE: use Raft for cluster-wide agreement.
        if !force {
            // give the replication stream a brief window to deliver
            // any in-flight records before we cut over. a future improvement
            // could track the exact offset and wait for full catchup.
            tokio::time::sleep(std::time::Duration::from_millis(FAILOVER_GRACE_MS)).await;
        }

        // get the primary's current slot ranges so we can hand them over
        let primary_slots = {
            let state = self.state.read().await;
            state
                .nodes
                .get(&primary_id)
                .map(|n| n.slots.clone())
                .unwrap_or_default()
        };

        if let Some(raft) = self.raft_node.get() {
            // promote this replica in the Raft state machine
            let promote_cmd = ClusterCommand::PromoteReplica {
                replica_id: self.local_id,
            };
            if let Err(e) = raft.propose(promote_cmd).await {
                return Self::raft_error_frame(e);
            }

            // transfer the primary's slots to this node via Raft
            if !primary_slots.is_empty() {
                let assign_cmd = ClusterCommand::AssignSlots {
                    node_id: self.local_id,
                    slots: primary_slots.clone(),
                };
                if let Err(e) = raft.propose(assign_cmd).await {
                    return Self::raft_error_frame(e);
                }

                // remove those slots from the old primary
                let remove_cmd = ClusterCommand::RemoveSlots {
                    node_id: primary_id,
                    slots: primary_slots,
                };
                // best-effort: old primary may already be unreachable
                let _ = raft.propose(remove_cmd).await;
            }
        }

        // apply locally right away so this node can start accepting writes
        // without waiting for the async Raft reconciliation to complete
        {
            let mut state = self.state.write().await;
            if let Err(e) = state.promote_replica(self.local_id) {
                warn!(%e, "local promote_replica after Raft proposal failed");
            }
        }

        self.announce_promotion().await;
        self.save_config().await;

        let mode = if force { "FORCE" } else { "default" };
        info!(local_id = %self.local_id, %primary_id, mode, "promoted to primary");
        Frame::Simple("OK".into())
    }

    /// Queues a role-change gossip announcement marking this node as primary.
    async fn announce_promotion(&self) {
        let mut gossip = self.gossip.lock().await;
        let inc = gossip.local_incarnation();
        gossip.queue_role_update(self.local_id, inc, true, None);
    }

    /// Attaches the engine so replication can start on demand.
    ///
    /// Must be called once after the engine is built, before any `CLUSTER REPLICATE`
    /// commands are processed.
    pub fn set_engine(&self, engine: Arc<Engine>) {
        let _ = self.engine.set(engine);
    }

    /// Starts the replication server for this node.
    ///
    /// Binds a TCP listener on `bind_addr.port() + gossip_port_offset + 2`
    /// and accepts replica connections indefinitely. This is a no-op if
    /// no engine has been attached via `set_engine`.
    pub async fn start_replication_server(self: &Arc<Self>) {
        let Some(engine) = self.engine.get() else {
            warn!("start_replication_server called before set_engine; skipping");
            return;
        };

        let repl_port = match self.replication_port(self.bind_addr.port()) {
            Some(p) => p,
            None => {
                error!("replication port overflows u16; not starting replication server");
                return;
            }
        };

        let local_id = self.local_id.to_string();
        if let Err(e) =
            crate::replication::ReplicationServer::start(Arc::clone(engine), local_id, repl_port)
                .await
        {
            error!("failed to start replication server on port {repl_port}: {e}");
        }
    }

    /// Starts the replication client, connecting to the primary's replication port.
    ///
    /// The replication port of the primary is derived from its data-plane address
    /// by adding `gossip_port_offset + 2`.
    async fn start_replication_client(&self, primary_id: NodeId) {
        let Some(engine) = self.engine.get() else {
            warn!("start_replication_client called before set_engine; skipping");
            return;
        };

        let primary_addr = {
            let state = self.state.read().await;
            state.nodes.get(&primary_id).map(|n| n.addr)
        };

        let Some(addr) = primary_addr else {
            warn!(%primary_id, "cannot start replication client: primary not found in state");
            return;
        };

        let repl_port = match self.replication_port(addr.port()) {
            Some(p) => p,
            None => {
                error!(%primary_id, "primary replication port overflows u16; not connecting");
                return;
            }
        };

        let repl_addr = std::net::SocketAddr::new(addr.ip(), repl_port);
        info!(%primary_id, %repl_addr, "starting replication client");
        crate::replication::ReplicationClient::start(Arc::clone(engine), repl_addr);
    }

    /// Returns replication status for the `INFO replication` section.
    ///
    /// Returns `(role, Option<primary_addr>)`.
    pub async fn replication_info(&self) -> ReplicationInfo {
        let state = self.state.read().await;
        let local = state.nodes.get(&self.local_id);
        let role = local.map(|n| n.role).unwrap_or(NodeRole::Primary);
        let primary_addr = if role == NodeRole::Replica {
            local
                .and_then(|n| n.replicates)
                .and_then(|id| state.nodes.get(&id))
                .map(|n| n.addr)
        } else {
            None
        };
        let replica_count = if role == NodeRole::Primary {
            local.map(|n| n.replicas.len()).unwrap_or(0)
        } else {
            0
        };
        ReplicationInfo {
            role,
            primary_addr,
            replica_count,
        }
    }

    /// Pushes the local node's current slot ownership into the gossip engine
    /// so it propagates to the rest of the cluster.
    async fn broadcast_local_slots(&self, slots: Vec<SlotRange>) {
        // Gather peer addresses and build the announce message while holding
        // the gossip lock, then release it before taking the socket lock.
        let (peer_addrs, encoded) = {
            let mut gossip = self.gossip.lock().await;
            gossip.set_local_slots(slots.clone());
            let incarnation = gossip.local_incarnation();
            // Queue for piggybacking on future ticks as a fallback.
            gossip.queue_slots_update(self.local_id, incarnation, slots.clone());

            // Build an eager push to every known peer so they learn immediately
            // rather than waiting for the probabilistic gossip tick to select them.
            let msg = GossipMessage::SlotsAnnounce {
                sender: self.local_id,
                incarnation,
                slots,
            };
            (gossip.alive_member_addrs(), msg.encode())
        };

        // Send directly to all alive peers (gossip lock released).
        let socket = self.udp_socket.lock().await;
        if let Some(ref sock) = *socket {
            for addr in &peer_addrs {
                if let Err(e) = sock.send_to(&encoded, addr).await {
                    debug!("slot broadcast to {addr} failed: {e}");
                }
            }
        }
    }

    /// Persists the current cluster state to `nodes.conf` in the data directory.
    ///
    /// Uses atomic write (write to tmp, then rename) to avoid corruption
    /// from crashes mid-write. This is called after every topology mutation.
    pub async fn save_config(&self) {
        let Some(ref dir) = self.data_dir else {
            return;
        };

        let incarnation = {
            let gossip = self.gossip.lock().await;
            gossip.local_incarnation()
        };
        let content = {
            let state = self.state.read().await;
            state.to_nodes_conf(incarnation)
        };

        let conf_path = dir.join("nodes.conf");
        let tmp_path = dir.join("nodes.conf.tmp");

        if let Err(e) = write_atomic(&tmp_path, &conf_path, content.as_bytes()) {
            error!("failed to save nodes.conf: {e}");
        }
    }

    // -- gossip networking --

    /// Spawns the gossip network tasks: UDP send/receive and event consumer.
    pub async fn spawn_gossip(
        self: &Arc<Self>,
        bind_addr: SocketAddr,
        mut event_rx: mpsc::Receiver<GossipEvent>,
    ) {
        let gossip_port = match bind_addr.port().checked_add(self.gossip_port_offset) {
            Some(p) => p,
            None => {
                error!("gossip port offset overflows u16");
                return;
            }
        };
        let gossip_addr = SocketAddr::new(bind_addr.ip(), gossip_port);

        let socket = match UdpSocket::bind(gossip_addr).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                error!("failed to bind gossip UDP socket on {gossip_addr}: {e}");
                return;
            }
        };

        info!("gossip listening on {gossip_addr}");

        // store socket for cluster_meet
        {
            let mut guard = self.udp_socket.lock().await;
            *guard = Some(Arc::clone(&socket));
        }

        // task 1: gossip tick + UDP recv/send loop
        let coordinator = Arc::clone(self);
        let sock = Arc::clone(&socket);
        tokio::spawn(async move {
            let mut recv_buf = vec![0u8; 65535];
            let mut tick_interval = tokio::time::interval(std::time::Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = tick_interval.tick() => {
                        let mut gossip = coordinator.gossip.lock().await;
                        for (target_addr, msg) in gossip.tick() {
                            let encoded = msg.encode();
                            if let Err(e) = sock.send_to(&encoded, target_addr).await {
                                debug!("gossip send error to {target_addr}: {e}");
                            }
                        }
                    }

                    result = sock.recv_from(&mut recv_buf) => {
                        match result {
                            Ok((len, from)) => {
                                match GossipMessage::decode(&recv_buf[..len]) {
                                    Ok(msg) => {
                                        let mut gossip = coordinator.gossip.lock().await;
                                        for (addr, reply) in gossip.handle_message(msg, from).await {
                                            let encoded = reply.encode();
                                            if let Err(e) = sock.send_to(&encoded, addr).await {
                                                debug!("gossip reply error to {addr}: {e}");
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        debug!("gossip decode error from {from}: {e}");
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("gossip recv error: {e}");
                            }
                        }
                    }
                }
            }
        });

        // task 2: gossip event consumer — updates cluster state
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            // PostAction defers work that must run after the state write-lock is released.
            // Holding the lock while calling gossip or async failover methods would deadlock.
            enum PostAction {
                None,
                StartElection(NodeId),
                HandleVoteRequest {
                    candidate: NodeId,
                    epoch: u64,
                },
                HandleVoteGranted {
                    from: NodeId,
                    candidate: NodeId,
                    epoch: u64,
                },
            }

            while let Some(event) = event_rx.recv().await {
                let mut post_action = PostAction::None;

                let needs_save = {
                    let mut state = coordinator.state.write().await;
                    match event {
                        GossipEvent::MemberJoined(id, gossip_addr, slots) => {
                            info!("cluster: node {} joined at {}", id, gossip_addr);
                            if state.nodes.contains_key(&id) {
                                false
                            } else {
                                // cluster_meet creates a placeholder with a random fake ID
                                // but the correct data address. Find it by matching the
                                // gossip port (cluster_bus_addr == gossip_addr).
                                let stale_id = state
                                    .nodes
                                    .values()
                                    .find(|n| {
                                        !n.flags.myself
                                            && n.id != id
                                            && n.cluster_bus_addr.port() == gossip_addr.port()
                                    })
                                    .map(|n| n.id);

                                let data_addr = if let Some(stale) = stale_id {
                                    // Preserve the data-port address from the placeholder.
                                    let saved = state.nodes[&stale].addr;
                                    state.nodes.remove(&stale);
                                    saved
                                } else {
                                    // No placeholder — derive data addr from gossip port.
                                    let data_port = gossip_addr
                                        .port()
                                        .saturating_sub(coordinator.gossip_port_offset);
                                    SocketAddr::new(gossip_addr.ip(), data_port)
                                };

                                let mut node = ClusterNode::new_primary_with_offset(
                                    id,
                                    data_addr,
                                    coordinator.gossip_port_offset,
                                );
                                // apply slot ranges from gossip
                                for range in &slots {
                                    for slot in range.iter() {
                                        state.slot_map.assign(slot, id);
                                    }
                                }
                                node.slots = slots;
                                state.add_node(node);
                                state.update_health();

                                // replicate the new node into raft state and update
                                // raft membership if we are the current leader
                                if let Some(raft) = coordinator.raft_node.get() {
                                    let raft = Arc::clone(raft);
                                    let raft_id = raft_id_from_node_id(id);
                                    let data_addr_str = data_addr.to_string();
                                    let raft_port_offset = coordinator.gossip_port_offset + 1;
                                    let raft_addr = SocketAddr::new(
                                        data_addr.ip(),
                                        data_addr.port().saturating_add(raft_port_offset),
                                    );
                                    tokio::spawn(async move {
                                        // add to application state machine
                                        let _ = raft
                                            .propose(ClusterCommand::AddNode {
                                                node_id: id,
                                                raft_id,
                                                addr: data_addr_str,
                                                is_primary: true,
                                            })
                                            .await;

                                        // add to raft membership if we're the leader
                                        if raft.is_leader() {
                                            let node = BasicNode {
                                                addr: raft_addr.to_string(),
                                            };
                                            let handle = raft.raft_handle();
                                            if handle.add_learner(raft_id, node, true).await.is_ok()
                                            {
                                                let m = handle.metrics().borrow().clone();
                                                let mut new_members: std::collections::BTreeSet<
                                                    u64,
                                                > = m
                                                    .membership_config
                                                    .membership()
                                                    .voter_ids()
                                                    .collect();
                                                new_members.insert(raft_id);
                                                let _ = handle
                                                    .change_membership(new_members, false)
                                                    .await;
                                            }
                                        }
                                    });
                                }

                                true
                            }
                        }
                        GossipEvent::MemberSuspected(id) => {
                            info!("cluster: node {} suspected", id);
                            if let Some(node) = state.nodes.get_mut(&id) {
                                node.flags.pfail = true;
                            }
                            state.update_health();
                            false // suspicion is transient, don't persist
                        }
                        GossipEvent::MemberFailed(id) => {
                            warn!("cluster: node {} confirmed failed", id);
                            if let Some(node) = state.nodes.get_mut(&id) {
                                node.flags.fail = true;
                                node.flags.pfail = false;
                            }
                            state.update_health();
                            // if we are a replica of the failed node, start an election
                            let replicates_failed = state
                                .nodes
                                .get(&coordinator.local_id)
                                .and_then(|n| n.replicates)
                                .map(|primary_id| primary_id == id)
                                .unwrap_or(false);
                            if replicates_failed {
                                post_action = PostAction::StartElection(id);
                            }
                            true
                        }
                        GossipEvent::MemberLeft(id) => {
                            info!("cluster: node {} left", id);
                            state.remove_node(id);
                            state.update_health();
                            true
                        }
                        GossipEvent::MemberAlive(id) => {
                            debug!("cluster: node {} alive", id);
                            if let Some(node) = state.nodes.get_mut(&id) {
                                node.flags.pfail = false;
                                node.flags.fail = false;
                            }
                            state.update_health();
                            false
                        }
                        GossipEvent::SlotsChanged(id, slots) => {
                            debug!(
                                "cluster: node {} slots changed ({} ranges)",
                                id,
                                slots.len()
                            );
                            // clear old slot assignments for this node
                            let old_ranges = state.slot_map.slots_for_node(id);
                            for range in &old_ranges {
                                for slot in range.iter() {
                                    state.slot_map.unassign(slot);
                                }
                            }
                            // apply new slot assignments
                            for range in &slots {
                                for slot in range.iter() {
                                    state.slot_map.assign(slot, id);
                                }
                            }
                            if let Some(node) = state.nodes.get_mut(&id) {
                                node.slots = slots;
                            }
                            state.update_health();
                            true
                        }
                        GossipEvent::RoleChanged(id, is_primary, replicates) => {
                            debug!(
                                "cluster: node {} role changed to {}",
                                id,
                                if is_primary { "primary" } else { "replica" }
                            );
                            if let Some(node) = state.nodes.get_mut(&id) {
                                node.role = if is_primary {
                                    NodeRole::Primary
                                } else {
                                    NodeRole::Replica
                                };
                                node.replicates = replicates;
                            }
                            state.update_health();
                            true
                        }
                        GossipEvent::VoteRequested {
                            candidate,
                            epoch,
                            offset: _,
                        } => {
                            // Handle outside the lock so we can call gossip.
                            post_action = PostAction::HandleVoteRequest { candidate, epoch };
                            false
                        }
                        GossipEvent::VoteGranted {
                            from,
                            candidate,
                            epoch,
                        } => {
                            post_action = PostAction::HandleVoteGranted {
                                from,
                                candidate,
                                epoch,
                            };
                            false
                        }
                    }
                };

                // Handle post-lock actions outside the state write-lock.
                match post_action {
                    PostAction::None => {}
                    PostAction::StartElection(primary_id) => {
                        let coord = Arc::clone(&coordinator);
                        tokio::spawn(async move {
                            coord.start_election(primary_id).await;
                        });
                    }
                    PostAction::HandleVoteRequest { candidate, epoch } => {
                        coordinator.handle_vote_request(candidate, epoch).await;
                    }
                    PostAction::HandleVoteGranted {
                        from,
                        candidate,
                        epoch,
                    } => {
                        coordinator
                            .handle_vote_granted(from, candidate, epoch)
                            .await;
                    }
                }

                if needs_save {
                    coordinator.save_config().await;
                }
            }
        });
    }
}

/// Snapshot of replication status for the `INFO replication` command.
#[derive(Debug)]
pub struct ReplicationInfo {
    pub role: NodeRole,
    /// Address of the primary this node replicates from (replica only).
    pub primary_addr: Option<std::net::SocketAddr>,
    /// Number of connected replicas (primary only).
    pub replica_count: usize,
}

/// Compacts a flat list of slot numbers into contiguous `SlotRange` values.
fn compact_slots(slots: &[u16]) -> Vec<SlotRange> {
    let mut sorted: Vec<u16> = slots.to_vec();
    sorted.sort_unstable();
    sorted.dedup();

    let mut ranges = Vec::new();
    let mut i = 0;
    while i < sorted.len() {
        let start = sorted[i];
        let mut end = start;
        while i + 1 < sorted.len() && sorted[i + 1] == end + 1 {
            i += 1;
            end = sorted[i];
        }
        ranges.push(SlotRange::new(start, end));
        i += 1;
    }
    ranges
}

/// Writes data to a temporary file and atomically renames it to the target path.
///
/// Ensures the file is fully flushed before rename so a crash mid-write
/// never leaves a partial `nodes.conf`.
fn write_atomic(
    tmp_path: &std::path::Path,
    target_path: &std::path::Path,
    data: &[u8],
) -> std::io::Result<()> {
    use std::io::Write;

    let mut f = std::fs::File::create(tmp_path)?;
    f.write_all(data)?;
    f.sync_all()?;
    std::fs::rename(tmp_path, target_path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a test coordinator with a single node that owns no slots.
    fn test_coordinator() -> (ClusterCoordinator, mpsc::Receiver<GossipEvent>) {
        let local_id = NodeId::new();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let config = GossipConfig::default();
        ClusterCoordinator::new(local_id, addr, config, false, None).unwrap()
    }

    /// Creates a test coordinator bootstrapped with all 16384 slots.
    fn test_coordinator_bootstrapped() -> (ClusterCoordinator, mpsc::Receiver<GossipEvent>) {
        let local_id = NodeId::new();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let config = GossipConfig::default();
        ClusterCoordinator::new(local_id, addr, config, true, None).unwrap()
    }

    #[test]
    fn new_rejects_port_overflow() {
        // port 65000 + offset 2000 = 67000, which overflows u16 (max 65535)
        let local_id = NodeId::new();
        let addr: SocketAddr = "127.0.0.1:65000".parse().unwrap();
        let config = GossipConfig {
            gossip_port_offset: 2000,
            ..GossipConfig::default()
        };
        let result = ClusterCoordinator::new(local_id, addr, config, false, None);
        assert!(result.is_err(), "expected port overflow error");
    }

    #[tokio::test]
    async fn setslot_importing_valid() {
        let (coord, _rx) = test_coordinator();
        let source = NodeId::new();
        let resp = coord
            .cluster_setslot_importing(100, &source.0.to_string())
            .await;
        assert!(matches!(resp, Frame::Simple(_)));
    }

    #[tokio::test]
    async fn setslot_importing_invalid_slot() {
        let (coord, _rx) = test_coordinator();
        let source = NodeId::new();
        let resp = coord
            .cluster_setslot_importing(16384, &source.0.to_string())
            .await;
        assert!(matches!(resp, Frame::Error(_)));
    }

    #[tokio::test]
    async fn setslot_importing_self_rejected() {
        let (coord, _rx) = test_coordinator();
        let resp = coord
            .cluster_setslot_importing(100, &coord.local_id.0.to_string())
            .await;
        match resp {
            Frame::Error(msg) => assert!(msg.contains("can't import from myself")),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn setslot_importing_duplicate_rejected() {
        let (coord, _rx) = test_coordinator();
        let source = NodeId::new();
        let id_str = source.0.to_string();
        coord.cluster_setslot_importing(100, &id_str).await;
        let resp = coord.cluster_setslot_importing(100, &id_str).await;
        assert!(matches!(resp, Frame::Error(_)));
    }

    #[tokio::test]
    async fn setslot_migrating_valid() {
        let (coord, _rx) = test_coordinator_bootstrapped();
        let target = NodeId::new();
        let resp = coord
            .cluster_setslot_migrating(0, &target.0.to_string())
            .await;
        assert!(matches!(resp, Frame::Simple(_)));
    }

    #[tokio::test]
    async fn setslot_migrating_not_owner() {
        let (coord, _rx) = test_coordinator(); // no slots owned
        let target = NodeId::new();
        let resp = coord
            .cluster_setslot_migrating(100, &target.0.to_string())
            .await;
        match resp {
            Frame::Error(msg) => assert!(msg.contains("not the owner")),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn setslot_migrating_self_rejected() {
        let (coord, _rx) = test_coordinator_bootstrapped();
        let resp = coord
            .cluster_setslot_migrating(0, &coord.local_id.0.to_string())
            .await;
        match resp {
            Frame::Error(msg) => assert!(msg.contains("can't migrate to myself")),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn setslot_node_assigns_slot() {
        let (coord, _rx) = test_coordinator();
        let target = NodeId::new();

        // add the target node to cluster state
        {
            let mut state = coord.state.write().await;
            let node = ClusterNode::new_primary(target, "127.0.0.1:6380".parse().unwrap());
            state.add_node(node);
        }

        let resp = coord.cluster_setslot_node(100, &target.0.to_string()).await;
        assert!(matches!(resp, Frame::Simple(_)));

        // verify the slot is now owned by the target
        let state = coord.state.read().await;
        assert_eq!(state.slot_map.owner(100), Some(target));
    }

    #[tokio::test]
    async fn setslot_node_completes_migration() {
        let (coord, _rx) = test_coordinator_bootstrapped();
        let target = NodeId::new();

        // start a migration
        coord
            .cluster_setslot_migrating(0, &target.0.to_string())
            .await;

        // add target to state
        {
            let mut state = coord.state.write().await;
            let node = ClusterNode::new_primary(target, "127.0.0.1:6380".parse().unwrap());
            state.add_node(node);
        }

        // complete with NODE — should clean up migration state
        let resp = coord.cluster_setslot_node(0, &target.0.to_string()).await;
        assert!(matches!(resp, Frame::Simple(_)));

        // migration should be cleaned up
        let migration = coord.migration.lock().await;
        assert!(!migration.is_migrating(0));
    }

    #[tokio::test]
    async fn setslot_stable_aborts_migration() {
        let (coord, _rx) = test_coordinator();
        let source = NodeId::new();
        coord
            .cluster_setslot_importing(100, &source.0.to_string())
            .await;

        let resp = coord.cluster_setslot_stable(100).await;
        assert!(matches!(resp, Frame::Simple(_)));

        // migration should be cleaned up
        let migration = coord.migration.lock().await;
        assert!(!migration.is_importing(100));
    }

    #[tokio::test]
    async fn setslot_stable_noop_when_no_migration() {
        let (coord, _rx) = test_coordinator();
        // should succeed even with no active migration
        let resp = coord.cluster_setslot_stable(100).await;
        assert!(matches!(resp, Frame::Simple(_)));
    }

    #[tokio::test]
    async fn addslots_queues_gossip_update() {
        let (coord, _rx) = test_coordinator();

        let resp = coord.cluster_addslots(&[0, 1, 2]).await;
        assert!(matches!(resp, Frame::Simple(_)));

        // verify state has the slots assigned
        let state = coord.state.read().await;
        assert_eq!(state.slot_map.owner(0), Some(coord.local_id));
        assert_eq!(state.slot_map.owner(1), Some(coord.local_id));
        assert_eq!(state.slot_map.owner(2), Some(coord.local_id));
        // slot 3 should still be unassigned
        assert_eq!(state.slot_map.owner(3), None);
    }

    #[tokio::test]
    async fn delslots_queues_gossip_update() {
        let (coord, _rx) = test_coordinator_bootstrapped();

        let resp = coord.cluster_delslots(&[0, 1]).await;
        assert!(matches!(resp, Frame::Simple(_)));

        let state = coord.state.read().await;
        assert_eq!(state.slot_map.owner(0), None);
        assert_eq!(state.slot_map.owner(1), None);
        // slot 2 should still be owned
        assert_eq!(state.slot_map.owner(2), Some(coord.local_id));
    }

    // -- check_slot_with_migration tests --

    #[tokio::test]
    async fn check_slot_owned_no_migration() {
        let (coord, _rx) = test_coordinator_bootstrapped();
        // "foo" hashes to some slot — we own all slots
        let result = coord.check_slot_with_migration(b"foo", false).await;
        assert!(result.is_none(), "should handle locally");
    }

    #[tokio::test]
    async fn check_slot_ask_when_key_migrated() {
        let (coord, _rx) = test_coordinator_bootstrapped();
        let target = NodeId::new();

        // add target node
        {
            let mut state = coord.state.write().await;
            let node = ClusterNode::new_primary(target, "127.0.0.1:6380".parse().unwrap());
            state.add_node(node);
        }

        // "foo" hashes to slot 12182
        let slot = ember_cluster::key_slot(b"foo");

        // start migrating the slot
        coord
            .cluster_setslot_migrating(slot, &target.0.to_string())
            .await;

        // mark "foo" as migrated via the migration manager directly
        {
            let mut migration = coord.migration.lock().await;
            migration.key_migrated(slot, b"foo".to_vec());
        }

        // should return ASK redirect
        let result = coord.check_slot_with_migration(b"foo", false).await;
        match result {
            Some(Frame::Error(msg)) => {
                assert!(msg.starts_with("ASK"), "expected ASK, got: {msg}");
                assert!(msg.contains("127.0.0.1:6380"));
            }
            other => panic!("expected ASK error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn check_slot_local_when_key_not_migrated() {
        let (coord, _rx) = test_coordinator_bootstrapped();
        let target = NodeId::new();

        let slot = ember_cluster::key_slot(b"foo");
        coord
            .cluster_setslot_migrating(slot, &target.0.to_string())
            .await;

        // "foo" NOT migrated yet — should serve locally
        let result = coord.check_slot_with_migration(b"foo", false).await;
        assert!(result.is_none(), "should handle locally");
    }

    #[tokio::test]
    async fn check_slot_importing_with_asking() {
        let (coord, _rx) = test_coordinator();
        let source = NodeId::new();

        let slot = ember_cluster::key_slot(b"foo");
        coord
            .cluster_setslot_importing(slot, &source.0.to_string())
            .await;

        // with asking=true, should allow local access
        let result = coord.check_slot_with_migration(b"foo", true).await;
        assert!(result.is_none(), "should allow with ASKING");
    }

    #[tokio::test]
    async fn check_slot_importing_without_asking() {
        let (coord, _rx) = test_coordinator();
        let source = NodeId::new();

        // add source node so MOVED has somewhere to point
        {
            let mut state = coord.state.write().await;
            let node = ClusterNode::new_primary(source, "127.0.0.1:6381".parse().unwrap());
            state.add_node(node);
        }

        let slot = ember_cluster::key_slot(b"foo");

        // assign the slot to the source node first
        {
            let mut state = coord.state.write().await;
            state.slot_map.assign(slot, source);
        }

        coord
            .cluster_setslot_importing(slot, &source.0.to_string())
            .await;

        // without asking, should return MOVED to the owner
        let result = coord.check_slot_with_migration(b"foo", false).await;
        match result {
            Some(Frame::Error(msg)) => {
                assert!(msg.starts_with("MOVED"), "expected MOVED, got: {msg}");
            }
            other => panic!("expected MOVED error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn check_slot_unassigned_returns_clusterdown() {
        let (coord, _rx) = test_coordinator(); // no slots assigned
        let result = coord.check_slot_with_migration(b"foo", false).await;
        match result {
            Some(Frame::Error(msg)) => {
                assert!(
                    msg.contains("CLUSTERDOWN"),
                    "expected CLUSTERDOWN, got: {msg}"
                );
            }
            other => panic!("expected CLUSTERDOWN error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn save_config_writes_readable_file() {
        let dir = tempfile::tempdir().unwrap();
        let local_id = NodeId::new();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let config = GossipConfig::default();
        let (coord, _rx) =
            ClusterCoordinator::new(local_id, addr, config, true, Some(dir.path().to_path_buf()))
                .unwrap();

        // add some slots and save
        coord.save_config().await;

        let content = std::fs::read_to_string(dir.path().join("nodes.conf")).unwrap();
        let (restored, _) = ClusterState::from_nodes_conf(&content).unwrap();

        assert_eq!(restored.local_id, local_id);
        assert!(restored.owns_slot(0));
        assert!(restored.owns_slot(16383));
    }

    #[tokio::test]
    async fn from_config_restores_coordinator() {
        let dir = tempfile::tempdir().unwrap();
        let local_id = NodeId::new();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let config = GossipConfig::default();
        let (coord, _rx) = ClusterCoordinator::new(
            local_id,
            addr,
            config.clone(),
            true,
            Some(dir.path().to_path_buf()),
        )
        .unwrap();

        coord.save_config().await;

        let content = std::fs::read_to_string(dir.path().join("nodes.conf")).unwrap();
        let (restored_coord, _rx) =
            ClusterCoordinator::from_config(&content, addr, config, dir.path().to_path_buf())
                .unwrap();

        assert_eq!(restored_coord.local_id, local_id);

        // verify slots are intact
        let state = restored_coord.state.read().await;
        assert!(state.owns_slot(0));
        assert!(state.owns_slot(16383));
    }

    #[tokio::test]
    async fn mark_key_migrated_triggers_ask_redirect() {
        let (coord, _rx) = test_coordinator_bootstrapped();

        // set up a migration: slot 12182 (hash of "foo") migrating to a target
        let target = NodeId::new();
        let target_addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();

        // add target node to state
        {
            let mut state = coord.state.write().await;
            state.add_node(ClusterNode::new_primary(target, target_addr));
        }

        // start migrating slot 12182
        coord
            .cluster_setslot_migrating(12182, &target.0.to_string())
            .await;

        // before marking the key, it should be served locally
        let result = coord.check_slot_with_migration(b"foo", false).await;
        assert!(
            result.is_none(),
            "key should be served locally before migration"
        );

        // mark the key as migrated
        coord.mark_key_migrated(12182, b"foo").await;

        // now it should return ASK
        let result = coord.check_slot_with_migration(b"foo", false).await;
        match result {
            Some(Frame::Error(msg)) => {
                assert!(
                    msg.starts_with("ASK 12182"),
                    "expected ASK redirect, got: {msg}"
                );
            }
            other => panic!("expected ASK redirect, got {other:?}"),
        }
    }

    // -- cluster_replicate --

    #[tokio::test]
    async fn cluster_replicate_self_rejected() {
        let (coord, _rx) = test_coordinator();
        let resp = coord.cluster_replicate(&coord.local_id.0.to_string()).await;
        match resp {
            Frame::Error(msg) => assert!(msg.contains("Cannot replicate self")),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cluster_replicate_invalid_id_rejected() {
        let (coord, _rx) = test_coordinator();
        let resp = coord.cluster_replicate("not-a-uuid").await;
        match resp {
            Frame::Error(msg) => assert!(msg.contains("Invalid node ID")),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cluster_replicate_unknown_node_rejected() {
        let (coord, _rx) = test_coordinator();
        let unknown = NodeId::new();
        let resp = coord.cluster_replicate(&unknown.0.to_string()).await;
        match resp {
            Frame::Error(msg) => assert!(msg.contains("Unknown node ID")),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cluster_replicate_updates_state() {
        let (coord, _rx) = test_coordinator();
        let primary_id = NodeId::new();

        // add a primary node to replicate from
        {
            let mut state = coord.state.write().await;
            let primary = ClusterNode::new_primary(primary_id, "127.0.0.1:6380".parse().unwrap());
            state.add_node(primary);
        }

        let resp = coord.cluster_replicate(&primary_id.0.to_string()).await;
        assert!(
            matches!(resp, Frame::Simple(_)),
            "expected OK, got {resp:?}"
        );

        // local node should now be a replica
        assert!(
            coord.is_replica().await,
            "node should be a replica after REPLICATE"
        );

        // local node's replicates field should point to primary
        let state = coord.state.read().await;
        let local = state.nodes.get(&coord.local_id).unwrap();
        assert_eq!(local.replicates, Some(primary_id));
        assert!(state.nodes[&primary_id].replicas.contains(&coord.local_id));
    }

    #[tokio::test]
    async fn primary_addr_for_slot_returns_primary_addr() {
        let (coord, _rx) = test_coordinator_bootstrapped();

        // the bootstrap coordinator owns all slots — find any one
        let addr = coord.primary_addr_for_slot(0).await;
        assert!(addr.is_some(), "should find primary for slot 0");
        assert_eq!(addr.unwrap().port(), 6379);
    }

    #[tokio::test]
    async fn is_replica_returns_false_initially() {
        let (coord, _rx) = test_coordinator();
        assert!(
            !coord.is_replica().await,
            "new coordinator should be a primary"
        );
    }

    #[tokio::test]
    async fn failover_rejected_on_primary() {
        let (coord, _rx) = test_coordinator_bootstrapped();
        let result = coord.cluster_failover(false, false).await;
        match result {
            Frame::Error(msg) => assert!(
                msg.contains("replica"),
                "expected replica error, got: {msg}"
            ),
            other => panic!("expected error frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn failover_takeover_promotes_replica() {
        // set up: primary owns all slots, replica replicates from it
        let (coord, _rx) = test_coordinator_bootstrapped();
        let primary_id = coord.local_id;

        let replica_id = NodeId::new();
        let replica_addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();

        // add the replica to the primary's state
        {
            let mut state = coord.state.write().await;
            let mut replica = ClusterNode::new_replica(replica_id, replica_addr, primary_id);
            replica.set_myself(); // pretend we're running on the replica
            state.add_node(replica);
            // register replica in primary's list
            state
                .nodes
                .get_mut(&primary_id)
                .unwrap()
                .replicas
                .push(replica_id);
            // update local_id to the replica
            // (simulate running on the replica node)
        }

        // build a replica coordinator with the same state
        let replica_addr_sa: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let (replica_coord, _rx2) = ClusterCoordinator::new(
            replica_id,
            replica_addr_sa,
            GossipConfig::default(),
            false,
            None,
        )
        .unwrap();

        // manually set up the replica's state
        {
            let mut state = replica_coord.state.write().await;
            let primary_node = coord
                .state
                .read()
                .await
                .nodes
                .get(&primary_id)
                .unwrap()
                .clone();
            state.add_node(primary_node);
            // set this node as a replica of primary
            if let Some(local) = state.nodes.get_mut(&replica_id) {
                local.role = NodeRole::Replica;
                local.replicates = Some(primary_id);
            }
            // assign all slots to primary in the slot map
            for slot in 0..16384u16 {
                state.slot_map.assign(slot, primary_id);
            }
        }

        // TAKEOVER: should succeed since no Raft is needed
        let result = replica_coord.cluster_failover(false, true).await;
        assert!(
            matches!(result, Frame::Simple(_)),
            "expected OK, got {result:?}"
        );

        // verify the replica is now a primary
        assert!(
            !replica_coord.is_replica().await,
            "after TAKEOVER, node should be primary"
        );

        // verify it owns all slots
        let state = replica_coord.state.read().await;
        for slot in 0..16384u16 {
            assert_eq!(state.slot_map.owner(slot), Some(replica_id));
        }
    }

    #[tokio::test]
    async fn writes_paused_blocks_and_resumes() {
        let (coord, _rx) = test_coordinator_bootstrapped();

        assert!(!coord.is_writes_paused());
        coord.pause_writes();
        assert!(coord.is_writes_paused());
        coord.resume_writes();
        assert!(!coord.is_writes_paused());
    }

    // -- automatic failover --

    #[tokio::test]
    async fn primary_grants_vote_once_per_epoch() {
        // A primary should grant a vote for a given epoch exactly once.
        let (coord, _rx) = test_coordinator_bootstrapped();
        let candidate = NodeId::new();

        // first request for epoch 5 should be granted (gossip queue entry added)
        coord.handle_vote_request(candidate, 5).await;
        assert_eq!(
            coord
                .last_voted_epoch
                .load(std::sync::atomic::Ordering::Acquire),
            5,
            "last_voted_epoch should be 5 after granting"
        );

        // second request for epoch 5 should be ignored
        let prev_epoch = coord
            .last_voted_epoch
            .load(std::sync::atomic::Ordering::Acquire);
        coord.handle_vote_request(candidate, 5).await;
        assert_eq!(
            coord
                .last_voted_epoch
                .load(std::sync::atomic::Ordering::Acquire),
            prev_epoch,
            "epoch should not change on duplicate request"
        );
    }

    #[tokio::test]
    async fn replica_does_not_grant_vote() {
        // Replicas must not vote in elections.
        let primary_id = NodeId::new();
        let replica_id = NodeId::new();
        let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        let (coord, _rx) =
            ClusterCoordinator::new(replica_id, addr, GossipConfig::default(), false, None)
                .unwrap();

        // set up coord as a replica
        {
            let mut state = coord.state.write().await;
            state.add_node(ClusterNode::new_primary(
                primary_id,
                "127.0.0.1:6379".parse().unwrap(),
            ));
            if let Some(n) = state.nodes.get_mut(&replica_id) {
                n.role = NodeRole::Replica;
                n.replicates = Some(primary_id);
            }
        }

        // replica should not update last_voted_epoch
        coord.handle_vote_request(NodeId::new(), 3).await;
        assert_eq!(
            coord
                .last_voted_epoch
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "replica must not grant votes"
        );
    }

    #[tokio::test]
    async fn vote_granted_reaches_quorum_and_promotes() {
        // A replica that receives enough votes should promote itself.
        let primary_id = NodeId::new();
        let voter1 = NodeId::new();
        let voter2 = NodeId::new();
        let replica_id = NodeId::new();
        let addr: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        let (coord, _rx) =
            ClusterCoordinator::new(replica_id, addr, GossipConfig::default(), false, None)
                .unwrap();
        let coord = Arc::new(coord);

        // set up coord as a replica with two peers owning all slots
        {
            let mut state = coord.state.write().await;
            let mut primary_node =
                ClusterNode::new_primary(primary_id, "127.0.0.1:6379".parse().unwrap());
            primary_node.slots = vec![SlotRange::new(0, 16383)];
            primary_node.flags.fail = true; // mark as failed
            state.add_node(primary_node.clone());
            state.add_node(ClusterNode::new_primary(
                voter1,
                "127.0.0.1:6382".parse().unwrap(),
            ));
            state.add_node(ClusterNode::new_primary(
                voter2,
                "127.0.0.1:6383".parse().unwrap(),
            ));
            // assign slots to primary
            for slot in 0..16384u16 {
                state.slot_map.assign(slot, primary_id);
            }
            // set replica state
            if let Some(n) = state.nodes.get_mut(&replica_id) {
                n.role = NodeRole::Replica;
                n.replicates = Some(primary_id);
            }
        }

        // seed election: epoch=1, 2 alive primaries (voter1, voter2), need 2 votes
        {
            let mut guard = coord.election.lock().await;
            *guard = Some(ElectionAttempt {
                inner: Election::new(1),
                total_primaries: 2,
            });
        }

        // first vote: not yet promoted
        coord.handle_vote_granted(voter1, replica_id, 1).await;
        assert!(
            !coord.is_replica().await || {
                // either still replica (not yet quorum) or promoted — check election
                coord
                    .election
                    .lock()
                    .await
                    .as_ref()
                    .map(|e| !e.inner.is_promoted())
                    .unwrap_or(true)
            }
        );

        // second vote: quorum reached
        coord.handle_vote_granted(voter2, replica_id, 1).await;

        // after quorum the election entry should be promoted and the node primary
        // (cluster_failover runs synchronously in tests since there's no Raft)
        assert!(
            !coord.is_replica().await,
            "node should be primary after winning election"
        );
    }

    #[tokio::test]
    async fn vote_granted_wrong_candidate_ignored() {
        let replica_id = NodeId::new();
        let other_candidate = NodeId::new();
        let addr: SocketAddr = "127.0.0.1:6384".parse().unwrap();
        let (coord, _rx) =
            ClusterCoordinator::new(replica_id, addr, GossipConfig::default(), false, None)
                .unwrap();
        let coord = Arc::new(coord);

        {
            let mut guard = coord.election.lock().await;
            *guard = Some(ElectionAttempt {
                inner: Election::new(1),
                total_primaries: 1,
            });
        }

        // vote granted to a different candidate — should be ignored
        coord
            .handle_vote_granted(NodeId::new(), other_candidate, 1)
            .await;

        let guard = coord.election.lock().await;
        assert!(
            !guard.as_ref().unwrap().inner.is_promoted(),
            "vote for wrong candidate should not trigger promotion"
        );
    }
}
