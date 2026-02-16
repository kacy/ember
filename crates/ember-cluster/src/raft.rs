//! Raft consensus for cluster configuration.
//!
//! Uses openraft to achieve consensus on cluster topology changes.
//! Only configuration changes go through Raft - data operations use
//! primary-replica async replication for lower latency.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftStorage, RaftTypeConfig, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

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
}

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage {
    pub fn new() -> Self {
        Self {
            vote: RwLock::new(None),
            log: RwLock::new(BTreeMap::new()),
            last_purged: RwLock::new(None),
            last_applied: RwLock::new(None),
            last_membership: RwLock::new(StoredMembership::default()),
            snapshot: RwLock::new(None),
            state: Arc::new(RwLock::new(ClusterStateData::default())),
        }
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
                let key = node_id.0.to_string();
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
                let key = node_id.0.to_string();
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
                let key = node_id.0.to_string();
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

            ClusterCommand::PromoteReplica { replica_id } => {
                let key = replica_id.0.to_string();
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
                        from: from.0.to_string(),
                        to: to.0.to_string(),
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
                let key = new_owner.0.to_string();
                state.slots.insert(*slot, key);
                ClusterResponse::Ok
            }
        }
    }
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
        *self.state.write().await = state_data;

        *self.snapshot.write().await = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });

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
        let storage = Arc::new(Storage::new());
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
        assert!(state.nodes.contains_key(&node_id.0.to_string()));
    }

    #[tokio::test]
    async fn storage_assign_slots() {
        let storage = Arc::new(Storage::new());
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
        assert_eq!(state.slots.get(&0), Some(&node_id.0.to_string()));
        assert_eq!(state.slots.get(&5460), Some(&node_id.0.to_string()));
    }

    #[tokio::test]
    async fn storage_migration() {
        let storage = Arc::new(Storage::new());
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
        let storage = Arc::new(Storage::new());
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
        let storage = Arc::new(Storage::new());
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
        let storage = Arc::new(Storage::new());
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
        let storage = Arc::new(Storage::new());
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
        let storage = Arc::new(Storage::new());
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
        let storage = Arc::new(Storage::new());
        let mut storage_clone = Arc::clone(&storage);

        let vote = Vote::new(1, 1);
        storage_clone.save_vote(&vote).await.unwrap();

        let read_vote = storage_clone.read_vote().await.unwrap();
        assert_eq!(read_vote, Some(vote));
    }
}
