//! Raft consensus for cluster configuration.
//!
//! Uses openraft to achieve consensus on cluster topology changes.
//! Only configuration changes go through Raft - data operations use
//! primary-replica async replication for lower latency.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::net::SocketAddr;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::error::{
    ClientWriteError, InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory as RaftNetworkFactoryTrait};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{Adaptor, LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, OptionalSend, Raft, RaftStorage, RaftTypeConfig,
    ServerState, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, RwLock};
use tracing::{debug, warn};

use crate::raft_transport::{read_frame, write_frame, RaftRpc, RaftRpcResponse};
use crate::slots::SLOT_COUNT;
use crate::{NodeId, SlotRange};

/// Type configuration for openraft.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct TypeConfig;

impl RaftTypeConfig for TypeConfig {
    type D = ClusterCommand;
    type R = ClusterResponse;
    type Node = BasicNode;
    type NodeId = u64;
    type Entry = Entry<TypeConfig>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<TypeConfig>;
}

/// Commands that modify cluster configuration.
///
/// These are replicated through Raft to ensure all nodes agree
/// on the cluster topology.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClusterCommand {
    /// Add a new node to the cluster.
    AddNode {
        node_id: NodeId,
        raft_id: u64,
        addr: String,
        is_primary: bool,
    },
    /// Remove a node from the cluster.
    RemoveNode { node_id: NodeId },
    /// Assign slots to a node.
    AssignSlots {
        node_id: NodeId,
        slots: Vec<SlotRange>,
    },
    /// Remove specific slots from a node.
    RemoveSlots {
        node_id: NodeId,
        slots: Vec<SlotRange>,
    },
    /// Promote a replica to primary (during failover).
    PromoteReplica { replica_id: NodeId },
    /// Mark a slot as migrating.
    BeginMigration { slot: u16, from: NodeId, to: NodeId },
    /// Complete a slot migration.
    CompleteMigration { slot: u16, new_owner: NodeId },
}

/// Response from applying a cluster command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClusterResponse {
    Ok,
    Error(String),
}

/// State machine snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterSnapshot {
    pub last_applied: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, BasicNode>,
    /// Serialized cluster state.
    pub state_data: Vec<u8>,
}

/// Internal cluster state managed by the state machine.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterStateData {
    /// Node ID to raft ID mapping.
    pub nodes: BTreeMap<String, NodeInfo>,
    /// Slot assignments.
    pub slots: BTreeMap<u16, String>,
    /// Ongoing migrations.
    pub migrations: BTreeMap<u16, MigrationState>,
}

/// Information about a cluster node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub raft_id: u64,
    pub addr: String,
    pub is_primary: bool,
    pub slots: Vec<SlotRange>,
}

/// State of an ongoing slot migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationState {
    pub from: String,
    pub to: String,
}

/// Combined log and state machine storage for Raft.
#[derive(Debug)]
pub struct Storage {
    vote: RwLock<Option<Vote<u64>>>,
    log: RwLock<BTreeMap<u64, Entry<TypeConfig>>>,
    last_purged: RwLock<Option<LogId<u64>>>,
    last_applied: RwLock<Option<LogId<u64>>>,
    last_membership: RwLock<StoredMembership<u64, BasicNode>>,
    snapshot: RwLock<Option<StoredSnapshot>>,
    state: Arc<RwLock<ClusterStateData>>,
    /// Notifies watchers whenever `apply_to_state_machine` commits entries.
    state_tx: watch::Sender<ClusterStateData>,
}

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

impl Default for Storage {
    fn default() -> Self {
        // creates a disconnected watch channel (state changes are not observed externally)
        let (state_tx, _) = watch::channel(ClusterStateData::default());
        Self {
            vote: RwLock::new(None),
            log: RwLock::new(BTreeMap::new()),
            last_purged: RwLock::new(None),
            last_applied: RwLock::new(None),
            last_membership: RwLock::new(StoredMembership::default()),
            snapshot: RwLock::new(None),
            state: Arc::new(RwLock::new(ClusterStateData::default())),
            state_tx,
        }
    }
}

impl Storage {
    /// Creates a new storage instance and returns a receiver that fires
    /// whenever the Raft state machine commits entries.
    pub fn new() -> (Arc<Self>, watch::Receiver<ClusterStateData>) {
        let (state_tx, state_rx) = watch::channel(ClusterStateData::default());
        let storage = Arc::new(Self {
            vote: RwLock::new(None),
            log: RwLock::new(BTreeMap::new()),
            last_purged: RwLock::new(None),
            last_applied: RwLock::new(None),
            last_membership: RwLock::new(StoredMembership::default()),
            snapshot: RwLock::new(None),
            state: Arc::new(RwLock::new(ClusterStateData::default())),
            state_tx,
        });
        (storage, state_rx)
    }

    pub fn state(&self) -> Arc<RwLock<ClusterStateData>> {
        Arc::clone(&self.state)
    }

    fn apply_command(cmd: &ClusterCommand, state: &mut ClusterStateData) -> ClusterResponse {
        match cmd {
            ClusterCommand::AddNode {
                node_id,
                raft_id,
                addr,
                is_primary,
            } => {
                let key = node_id.as_key();
                state.nodes.insert(
                    key.clone(),
                    NodeInfo {
                        node_id: key,
                        raft_id: *raft_id,
                        addr: addr.clone(),
                        is_primary: *is_primary,
                        slots: Vec::new(),
                    },
                );
                ClusterResponse::Ok
            }

            ClusterCommand::RemoveNode { node_id } => {
                let key = node_id.as_key();
                state.nodes.remove(&key);
                state.slots.retain(|_, owner| owner != &key);
                ClusterResponse::Ok
            }

            ClusterCommand::AssignSlots { node_id, slots } => {
                // validate all slot ranges before applying
                for range in slots {
                    if range.start > range.end || range.end >= SLOT_COUNT {
                        return ClusterResponse::Error(format!(
                            "invalid slot range {}..={} (max {})",
                            range.start,
                            range.end,
                            SLOT_COUNT - 1
                        ));
                    }
                }
                let key = node_id.as_key();
                if let Some(node) = state.nodes.get_mut(&key) {
                    node.slots = slots.clone();
                    for slot_range in slots {
                        for slot in slot_range.start..=slot_range.end {
                            state.slots.insert(slot, key.clone());
                        }
                    }
                    ClusterResponse::Ok
                } else {
                    ClusterResponse::Error(format!("node {} not found", node_id))
                }
            }

            ClusterCommand::RemoveSlots { node_id, slots } => {
                for range in slots {
                    if range.start > range.end || range.end >= SLOT_COUNT {
                        return ClusterResponse::Error(format!(
                            "invalid slot range {}..={} (max {})",
                            range.start,
                            range.end,
                            SLOT_COUNT - 1
                        ));
                    }
                }
                let key = node_id.as_key();
                for slot_range in slots {
                    for slot in slot_range.start..=slot_range.end {
                        // only remove if this node is the current owner
                        if state.slots.get(&slot).map(|s| s.as_str()) == Some(key.as_str()) {
                            state.slots.remove(&slot);
                        }
                    }
                }
                // rebuild node's slot list from what remains; split the borrow
                let remaining = slots_for_node_in_state(state, &key);
                if let Some(node) = state.nodes.get_mut(&key) {
                    node.slots = remaining;
                }
                ClusterResponse::Ok
            }

            ClusterCommand::PromoteReplica { replica_id } => {
                let key = replica_id.as_key();
                if let Some(node) = state.nodes.get_mut(&key) {
                    node.is_primary = true;
                    ClusterResponse::Ok
                } else {
                    ClusterResponse::Error(format!("replica {} not found", replica_id))
                }
            }

            ClusterCommand::BeginMigration { slot, from, to } => {
                if *slot >= SLOT_COUNT {
                    return ClusterResponse::Error(format!(
                        "slot {slot} out of range (max {})",
                        SLOT_COUNT - 1
                    ));
                }
                state.migrations.insert(
                    *slot,
                    MigrationState {
                        from: from.as_key(),
                        to: to.as_key(),
                    },
                );
                ClusterResponse::Ok
            }

            ClusterCommand::CompleteMigration { slot, new_owner } => {
                if !state.migrations.contains_key(slot) {
                    return ClusterResponse::Error(format!(
                        "no migration in progress for slot {slot}"
                    ));
                }
                state.migrations.remove(slot);
                let key = new_owner.as_key();
                state.slots.insert(*slot, key);
                ClusterResponse::Ok
            }
        }
    }
}

/// Returns the slot ranges owned by `node_key` according to the slot map.
fn slots_for_node_in_state(state: &ClusterStateData, node_key: &str) -> Vec<SlotRange> {
    let mut slots: Vec<u16> = state
        .slots
        .iter()
        .filter(|(_, v)| v.as_str() == node_key)
        .map(|(k, _)| *k)
        .collect();
    slots.sort_unstable();

    // compress into contiguous ranges
    let mut ranges = Vec::new();
    let mut i = 0;
    while i < slots.len() {
        let start = slots[i];
        let mut end = start;
        while i + 1 < slots.len() && slots[i + 1] == end + 1 {
            i += 1;
            end = slots[i];
        }
        ranges.push(SlotRange::new(start, end));
        i += 1;
    }
    ranges
}

impl RaftLogReader<TypeConfig> for Arc<Storage> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let log = self.log.read().await;
        Ok(log.range(range).map(|(_, v)| v.clone()).collect())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<Storage> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let last_applied = *self.last_applied.read().await;
        let membership = self.last_membership.read().await.clone();
        let state = self.state.read().await;

        let state_data =
            serde_json::to_vec(&*state).map_err(|e| StorageIOError::write_snapshot(None, &e))?;

        let snapshot = ClusterSnapshot {
            last_applied,
            last_membership: membership.clone(),
            state_data,
        };

        let data =
            serde_json::to_vec(&snapshot).map_err(|e| StorageIOError::write_snapshot(None, &e))?;

        let snapshot_id = last_applied
            .map(|id| format!("{}-{}", id.leader_id, id.index))
            .unwrap_or_else(|| "0-0".to_string());

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id,
        };

        // Store the snapshot
        *self.snapshot.write().await = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<TypeConfig> for Arc<Storage> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let log = self.log.read().await;
        let last = log.iter().next_back().map(|(_, e)| e.log_id);
        let purged = *self.last_purged.read().await;

        Ok(LogState {
            last_purged_log_id: purged,
            last_log_id: last,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        *self.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(*self.vote.read().await)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Arc::clone(self)
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut log = self.log.write().await;
        let to_remove: Vec<_> = log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in to_remove {
            log.remove(&key);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut log = self.log.write().await;
        let to_remove: Vec<_> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in to_remove {
            log.remove(&key);
        }
        *self.last_purged.write().await = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let last_applied = *self.last_applied.read().await;
        let membership = self.last_membership.read().await.clone();
        Ok((last_applied, membership))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<ClusterResponse>, StorageError<u64>> {
        let mut results = Vec::new();
        let mut state = self.state.write().await;

        for entry in entries {
            *self.last_applied.write().await = Some(entry.log_id);

            match &entry.payload {
                EntryPayload::Blank => {
                    results.push(ClusterResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    let result = Storage::apply_command(cmd, &mut state);
                    results.push(result);
                }
                EntryPayload::Membership(m) => {
                    *self.last_membership.write().await =
                        StoredMembership::new(Some(entry.log_id), m.clone());
                    results.push(ClusterResponse::Ok);
                }
            }
        }

        // notify the reconciliation watcher after releasing the write lock
        let state_snapshot = state.clone();
        drop(state);
        let _ = self.state_tx.send_replace(state_snapshot);

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Arc::clone(self)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let snap: ClusterSnapshot = serde_json::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        *self.last_applied.write().await = snap.last_applied;
        *self.last_membership.write().await = snap.last_membership;

        let state_data: ClusterStateData = serde_json::from_slice(&snap.state_data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        *self.state.write().await = state_data.clone();

        *self.snapshot.write().await = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });

        // notify after snapshot install, same as after apply
        let _ = self.state_tx.send_replace(state_data);

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let snap = self.snapshot.read().await;
        Ok(snap.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

// -- network implementation --

/// Per-peer network handle. Opens a short-lived TCP connection per RPC call.
///
/// One connection per RPC is acceptable because Raft RPCs are infrequent:
/// one heartbeat per 500 ms per follower.
pub struct RaftNetworkClient {
    target_addr: SocketAddr,
}

impl RaftNetwork<TypeConfig> for RaftNetworkClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let resp = self.call(RaftRpc::AppendEntries(rpc)).await?;
        match resp {
            RaftRpcResponse::AppendEntries(r) => Ok(r),
            _ => Err(RPCError::Network(NetworkError::new(&io_error(
                "unexpected response variant",
            )))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let resp = self.call(RaftRpc::Vote(rpc)).await?;
        match resp {
            RaftRpcResponse::Vote(r) => Ok(r),
            _ => Err(RPCError::Network(NetworkError::new(&io_error(
                "unexpected response variant",
            )))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let resp = self
            .call_snapshot(RaftRpc::InstallSnapshot(rpc))
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        match resp {
            RaftRpcResponse::InstallSnapshot(r) => Ok(r),
            _ => Err(RPCError::Network(NetworkError::new(&io_error(
                "unexpected response variant",
            )))),
        }
    }
}

impl RaftNetworkClient {
    /// Sends a Raft RPC to the target and returns the response.
    ///
    /// Opens a TCP connection, sends one frame, reads one frame, and closes.
    async fn call(
        &self,
        rpc: RaftRpc,
    ) -> Result<RaftRpcResponse, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.send_rpc(rpc)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }

    /// Same as `call` but maps errors to the install-snapshot error type.
    async fn call_snapshot(&self, rpc: RaftRpc) -> std::io::Result<RaftRpcResponse> {
        self.send_rpc(rpc).await
    }

    async fn send_rpc(&self, rpc: RaftRpc) -> std::io::Result<RaftRpcResponse> {
        let mut stream = TcpStream::connect(self.target_addr).await?;
        write_frame(&mut stream, &rpc).await?;
        read_frame(&mut stream).await
    }
}

/// Factory that creates per-peer `RaftNetworkClient` instances.
///
/// The `node.addr` field in `BasicNode` must contain `"ip:raft_port"`.
pub struct RaftNetworkFactory;

impl RaftNetworkFactoryTrait<TypeConfig> for RaftNetworkFactory {
    type Network = RaftNetworkClient;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> RaftNetworkClient {
        let target_addr = node
            .addr
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap());
        RaftNetworkClient { target_addr }
    }
}

// -- TCP listener for inbound Raft RPCs --

/// Spawns a task that accepts incoming Raft RPC connections.
///
/// Reads one `RaftRpc` frame, dispatches to the local Raft instance,
/// writes one `RaftRpcResponse` frame, then closes the connection.
pub(crate) fn spawn_raft_listener(raft: Raft<TypeConfig>, bind_addr: SocketAddr) {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(bind_addr).await {
            Ok(l) => l,
            Err(e) => {
                warn!("raft listener failed to bind on {bind_addr}: {e}");
                return;
            }
        };

        tracing::info!("raft listener on {bind_addr}");

        loop {
            let (mut stream, peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    warn!("raft accept error: {e}");
                    continue;
                }
            };

            let raft = raft.clone();
            tokio::spawn(async move {
                let rpc: RaftRpc = match read_frame(&mut stream).await {
                    Ok(r) => r,
                    Err(e) => {
                        debug!("raft read error from {peer}: {e}");
                        return;
                    }
                };

                let response = match rpc {
                    RaftRpc::AppendEntries(req) => match raft.append_entries(req).await {
                        Ok(r) => RaftRpcResponse::AppendEntries(r),
                        Err(e) => {
                            debug!("append_entries error: {e}");
                            return;
                        }
                    },
                    RaftRpc::Vote(req) => match raft.vote(req).await {
                        Ok(r) => RaftRpcResponse::Vote(r),
                        Err(e) => {
                            debug!("vote error: {e}");
                            return;
                        }
                    },
                    RaftRpc::InstallSnapshot(req) => {
                        // convert chunked snapshot to full snapshot for install
                        let vote = req.vote;
                        let meta = req.meta.clone();
                        let data = req.data.clone();
                        let snapshot = Snapshot {
                            meta,
                            snapshot: Box::new(Cursor::new(data)),
                        };
                        match raft.install_full_snapshot(vote, snapshot).await {
                            Ok(r) => RaftRpcResponse::InstallSnapshot(InstallSnapshotResponse {
                                vote: r.vote,
                            }),
                            Err(e) => {
                                debug!("install_snapshot error: {e}");
                                return;
                            }
                        }
                    }
                };

                if let Err(e) = write_frame(&mut stream, &response).await {
                    debug!("raft write error to {peer}: {e}");
                }
            });
        }
    });
}

// -- RaftNode wrapper --

/// Error from proposing a command through Raft.
#[derive(Debug)]
pub enum RaftProposalError {
    /// This node is not the leader. The leader's address is provided when known.
    NotLeader(Option<BasicNode>),
    /// Fatal Raft error.
    Fatal(String),
}

impl std::fmt::Display for RaftProposalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RaftProposalError::NotLeader(Some(node)) => {
                write!(f, "not leader, leader at {}", node.addr)
            }
            RaftProposalError::NotLeader(None) => write!(f, "no leader elected"),
            RaftProposalError::Fatal(msg) => write!(f, "raft fatal: {msg}"),
        }
    }
}

/// High-level Raft node wrapper.
///
/// Owns the `Raft<TypeConfig>` instance and exposes the operations needed
/// by `ClusterCoordinator`: proposing mutations and checking leader status.
pub struct RaftNode {
    raft: Raft<TypeConfig>,
    local_raft_id: u64,
    local_raft_addr: SocketAddr,
}

impl RaftNode {
    /// Starts a Raft node bound to `raft_addr`.
    ///
    /// Creates the Raft consensus engine, wraps `storage` with the openraft
    /// adaptor, and spawns the TCP listener for inbound RPCs.
    pub async fn start(
        local_raft_id: u64,
        raft_addr: SocketAddr,
        storage: Arc<Storage>,
    ) -> Result<Self, openraft::error::Fatal<u64>> {
        let config = Arc::new(
            Config {
                cluster_name: "ember".to_string(),
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                ..Config::default()
            }
            .validate()
            .expect("raft config validation failed"),
        );

        let (log_store, state_machine) = Adaptor::new(Arc::clone(&storage));

        let raft = Raft::new(
            local_raft_id,
            config,
            RaftNetworkFactory,
            log_store,
            state_machine,
        )
        .await?;

        spawn_raft_listener(raft.clone(), raft_addr);

        Ok(Self {
            raft,
            local_raft_id,
            local_raft_addr: raft_addr,
        })
    }

    /// Initializes a single-node cluster.
    ///
    /// Must only be called once, on first boot, before any log entries exist.
    /// Subsequent boots should NOT call this — the existing log is sufficient.
    pub async fn bootstrap_single(&self) -> Result<(), String> {
        let mut members = BTreeMap::new();
        members.insert(
            self.local_raft_id,
            BasicNode {
                addr: self.local_raft_addr.to_string(),
            },
        );

        self.raft
            .initialize(members)
            .await
            .map_err(|e| e.to_string())
    }

    /// Proposes a cluster configuration change through Raft.
    ///
    /// Blocks until the entry is committed and applied to the state machine
    /// on a quorum of nodes. Returns `NotLeader` if this node is not the leader.
    pub async fn propose(&self, cmd: ClusterCommand) -> Result<ClusterResponse, RaftProposalError> {
        match self.raft.client_write(cmd).await {
            Ok(resp) => Ok(resp.data),
            Err(e) => match e {
                openraft::error::RaftError::APIError(ClientWriteError::ForwardToLeader(fwd)) => {
                    Err(RaftProposalError::NotLeader(fwd.leader_node))
                }
                other => Err(RaftProposalError::Fatal(other.to_string())),
            },
        }
    }

    /// Returns `true` if this node is currently the Raft leader.
    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().state == ServerState::Leader
    }

    /// Returns the current leader's BasicNode info, if known.
    pub fn current_leader_node(&self) -> Option<BasicNode> {
        let m = self.raft.metrics().borrow().clone();
        let leader_id = m.current_leader?;
        m.membership_config
            .membership()
            .get_node(&leader_id)
            .cloned()
    }

    /// Exposes the underlying `Raft` handle for membership management.
    pub fn raft_handle(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    pub fn local_raft_id(&self) -> u64 {
        self.local_raft_id
    }

    pub fn raft_addr(&self) -> SocketAddr {
        self.local_raft_addr
    }
}

/// Derives a stable `u64` raft ID from a `NodeId` UUID.
///
/// Uses the upper 64 bits of the UUID which are as random as the full value.
pub fn raft_id_from_node_id(node_id: NodeId) -> u64 {
    node_id.0.as_u64_pair().0
}

fn io_error(msg: &str) -> std::io::Error {
    std::io::Error::other(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;

    /// Helper to create a LogId for tests.
    fn log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, 0), index)
    }

    #[tokio::test]
    async fn storage_add_node() {
        let (storage, _rx) = Storage::new();
        let mut storage_clone = Arc::clone(&storage);

        let node_id = NodeId::new();
        let entry = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(ClusterCommand::AddNode {
                node_id,
                raft_id: 1,
                addr: "127.0.0.1:6379".to_string(),
                is_primary: true,
            }),
        };

        let results = storage_clone
            .apply_to_state_machine(&[entry])
            .await
            .unwrap();
        assert_eq!(results, vec![ClusterResponse::Ok]);

        let state_arc = storage.state();
        let state = state_arc.read().await;
        assert!(state.nodes.contains_key(&node_id.as_key()));
    }

    #[tokio::test]
    async fn storage_assign_slots() {
        let (storage, _rx) = Storage::new();
        let mut storage_clone = Arc::clone(&storage);

        let node_id = NodeId::new();

        // Add node first
        let add_entry = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(ClusterCommand::AddNode {
                node_id,
                raft_id: 1,
                addr: "127.0.0.1:6379".to_string(),
                is_primary: true,
            }),
        };
        storage_clone
            .apply_to_state_machine(&[add_entry])
            .await
            .unwrap();

        // Assign slots
        let assign_entry = Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(ClusterCommand::AssignSlots {
                node_id,
                slots: vec![SlotRange::new(0, 5460)],
            }),
        };
        let results = storage_clone
            .apply_to_state_machine(&[assign_entry])
            .await
            .unwrap();
        assert_eq!(results, vec![ClusterResponse::Ok]);

        let state_arc = storage.state();
        let state = state_arc.read().await;
        assert_eq!(state.slots.get(&0), Some(&node_id.as_key()));
        assert_eq!(state.slots.get(&5460), Some(&node_id.as_key()));
    }

    #[tokio::test]
    async fn storage_remove_slots() {
        let (storage, _rx) = Storage::new();
        let mut s = Arc::clone(&storage);
        let node_id = NodeId::new();

        let add = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(ClusterCommand::AddNode {
                node_id,
                raft_id: 1,
                addr: "127.0.0.1:6379".into(),
                is_primary: true,
            }),
        };
        s.apply_to_state_machine(&[add]).await.unwrap();

        let assign = Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(ClusterCommand::AssignSlots {
                node_id,
                slots: vec![SlotRange::new(0, 10)],
            }),
        };
        s.apply_to_state_machine(&[assign]).await.unwrap();

        let remove = Entry {
            log_id: log_id(1, 3),
            payload: EntryPayload::Normal(ClusterCommand::RemoveSlots {
                node_id,
                slots: vec![SlotRange::new(0, 5)],
            }),
        };
        let results = s.apply_to_state_machine(&[remove]).await.unwrap();
        assert_eq!(results, vec![ClusterResponse::Ok]);

        let state = storage.state();
        let state = state.read().await;
        assert!(!state.slots.contains_key(&0));
        assert!(state.slots.contains_key(&6));
    }

    #[tokio::test]
    async fn storage_migration() {
        let (storage, _rx) = Storage::new();
        let mut storage_clone = Arc::clone(&storage);

        let node1 = NodeId::new();
        let node2 = NodeId::new();

        // Add nodes
        let entries: Vec<Entry<TypeConfig>> = [node1, node2]
            .iter()
            .enumerate()
            .map(|(i, node_id)| Entry {
                log_id: log_id(1, i as u64 + 1),
                payload: EntryPayload::Normal(ClusterCommand::AddNode {
                    node_id: *node_id,
                    raft_id: i as u64 + 1,
                    addr: format!("127.0.0.1:{}", 6379 + i),
                    is_primary: true,
                }),
            })
            .collect();
        storage_clone
            .apply_to_state_machine(&entries)
            .await
            .unwrap();

        // Begin migration
        let begin_entry = Entry {
            log_id: log_id(1, 3),
            payload: EntryPayload::Normal(ClusterCommand::BeginMigration {
                slot: 100,
                from: node1,
                to: node2,
            }),
        };
        storage_clone
            .apply_to_state_machine(&[begin_entry])
            .await
            .unwrap();

        {
            let state_arc = storage.state();
            let state = state_arc.read().await;
            assert!(state.migrations.contains_key(&100));
        }

        // Complete migration
        let complete_entry = Entry {
            log_id: log_id(1, 4),
            payload: EntryPayload::Normal(ClusterCommand::CompleteMigration {
                slot: 100,
                new_owner: node2,
            }),
        };
        storage_clone
            .apply_to_state_machine(&[complete_entry])
            .await
            .unwrap();

        {
            let state_arc = storage.state();
            let state = state_arc.read().await;
            assert!(!state.migrations.contains_key(&100));
            assert_eq!(state.slots.get(&100), Some(&node2.0.to_string()));
        }
    }

    #[tokio::test]
    async fn assign_slots_rejects_invalid_range() {
        let (storage, _rx) = Storage::new();
        let mut s = Arc::clone(&storage);

        let node_id = NodeId::new();
        let add = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(ClusterCommand::AddNode {
                node_id,
                raft_id: 1,
                addr: "127.0.0.1:6379".into(),
                is_primary: true,
            }),
        };
        s.apply_to_state_machine(&[add]).await.unwrap();

        // craft a SlotRange with start > end (bypassing SlotRange::new)
        let bad_range = SlotRange {
            start: 100,
            end: 50,
        };
        let assign = Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(ClusterCommand::AssignSlots {
                node_id,
                slots: vec![bad_range],
            }),
        };
        let results = s.apply_to_state_machine(&[assign]).await.unwrap();
        assert!(
            matches!(&results[0], ClusterResponse::Error(msg) if msg.contains("invalid slot range"))
        );
    }

    #[tokio::test]
    async fn assign_slots_rejects_out_of_range() {
        let (storage, _rx) = Storage::new();
        let mut s = Arc::clone(&storage);

        let node_id = NodeId::new();
        let add = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(ClusterCommand::AddNode {
                node_id,
                raft_id: 1,
                addr: "127.0.0.1:6379".into(),
                is_primary: true,
            }),
        };
        s.apply_to_state_machine(&[add]).await.unwrap();

        // slot end >= SLOT_COUNT
        let bad_range = SlotRange {
            start: 0,
            end: 16384,
        };
        let assign = Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(ClusterCommand::AssignSlots {
                node_id,
                slots: vec![bad_range],
            }),
        };
        let results = s.apply_to_state_machine(&[assign]).await.unwrap();
        assert!(
            matches!(&results[0], ClusterResponse::Error(msg) if msg.contains("invalid slot range"))
        );
    }

    #[tokio::test]
    async fn complete_migration_without_begin_errors() {
        let (storage, _rx) = Storage::new();
        let mut s = Arc::clone(&storage);

        let node_id = NodeId::new();
        let complete = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(ClusterCommand::CompleteMigration {
                slot: 100,
                new_owner: node_id,
            }),
        };
        let results = s.apply_to_state_machine(&[complete]).await.unwrap();
        assert!(matches!(&results[0], ClusterResponse::Error(msg) if msg.contains("no migration")));
    }

    #[tokio::test]
    async fn begin_migration_rejects_invalid_slot() {
        let (storage, _rx) = Storage::new();
        let mut s = Arc::clone(&storage);

        let node1 = NodeId::new();
        let node2 = NodeId::new();
        let begin = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(ClusterCommand::BeginMigration {
                slot: 16384,
                from: node1,
                to: node2,
            }),
        };
        let results = s.apply_to_state_machine(&[begin]).await.unwrap();
        assert!(matches!(&results[0], ClusterResponse::Error(msg) if msg.contains("out of range")));
    }

    #[tokio::test]
    async fn storage_log_operations() {
        let (storage, _rx) = Storage::new();
        let mut storage_clone = Arc::clone(&storage);

        let entry = Entry::<TypeConfig> {
            log_id: log_id(1, 1),
            payload: EntryPayload::Blank,
        };

        storage_clone.append_to_log(vec![entry]).await.unwrap();

        let state = storage_clone.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(log_id(1, 1)));
    }

    #[tokio::test]
    async fn storage_vote() {
        let (storage, _rx) = Storage::new();
        let mut storage_clone = Arc::clone(&storage);

        let vote = Vote::new(1, 1);
        storage_clone.save_vote(&vote).await.unwrap();

        let read_vote = storage_clone.read_vote().await.unwrap();
        assert_eq!(read_vote, Some(vote));
    }

    #[tokio::test]
    async fn watch_channel_notified_on_apply() {
        let (storage, mut rx) = Storage::new();
        let mut s = Arc::clone(&storage);

        let node_id = NodeId::new();
        let entry = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(ClusterCommand::AddNode {
                node_id,
                raft_id: 1,
                addr: "127.0.0.1:6379".into(),
                is_primary: true,
            }),
        };

        // the watch channel starts with the initial state, so borrow it first to
        // mark it as seen, then apply — changed() should fire
        let _ = rx.borrow_and_update();

        s.apply_to_state_machine(&[entry]).await.unwrap();

        assert!(
            rx.changed().await.is_ok(),
            "watch channel should have fired"
        );
        let data = rx.borrow();
        assert!(data.nodes.contains_key(&node_id.as_key()));
    }
}
