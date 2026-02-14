//! Live slot migration for cluster resharding.
//!
//! This module implements Redis-compatible slot migration that allows moving
//! slots between nodes without downtime. The protocol follows these steps:
//!
//! 1. Target marks slot as IMPORTING from source
//! 2. Source marks slot as MIGRATING to target
//! 3. Keys are streamed in batches from source to target
//! 4. During migration, reads go to source, writes get ASK redirect to target
//! 5. Final batch completes, ownership transfers via Raft
//!
//! # Example
//!
//! ```ignore
//! // On target node:
//! CLUSTER SETSLOT 100 IMPORTING <source-node-id>
//!
//! // On source node:
//! CLUSTER SETSLOT 100 MIGRATING <target-node-id>
//!
//! // Migrate keys (repeated for each key in slot):
//! MIGRATE <target-host> <target-port> <key> 0 5000
//!
//! // Finalize on both nodes:
//! CLUSTER SETSLOT 100 NODE <target-node-id>
//! ```

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// Unique identifier for a migration operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MigrationId(pub u64);

impl MigrationId {
    /// Generate a new migration ID from timestamp, slot, and random bits.
    ///
    /// The ID combines nanosecond timestamp with the slot number and 16 bits
    /// of randomness. The random component prevents collisions when the
    /// system clock is unavailable (`unwrap_or_default` returns zero) or
    /// when two migrations for the same slot start within the same nanosecond.
    pub fn new(slot: u16) -> Self {
        use rand::Rng;
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let noise: u16 = rand::rng().random();
        Self(ts ^ (slot as u64) ^ ((noise as u64) << 48))
    }
}

/// Current state of a slot migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationState {
    /// Target has marked slot as importing, waiting for source.
    Importing,
    /// Source has marked slot as migrating, ready to stream keys.
    Migrating,
    /// Keys are being transferred in batches.
    Streaming,
    /// Final batch being sent, brief pause on writes.
    Finalizing,
    /// Migration complete, ownership transferred.
    Complete,
}

impl std::fmt::Display for MigrationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Importing => write!(f, "importing"),
            Self::Migrating => write!(f, "migrating"),
            Self::Streaming => write!(f, "streaming"),
            Self::Finalizing => write!(f, "finalizing"),
            Self::Complete => write!(f, "complete"),
        }
    }
}

/// A single slot migration operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    /// Unique migration identifier.
    pub id: MigrationId,
    /// The slot being migrated.
    pub slot: u16,
    /// Node currently owning the slot.
    pub source: NodeId,
    /// Node receiving the slot.
    pub target: NodeId,
    /// Current migration state.
    pub state: MigrationState,
    /// When migration started.
    #[serde(skip)]
    pub started_at: Option<Instant>,
    /// Number of keys migrated so far.
    pub keys_migrated: u64,
    /// Total keys to migrate (if known).
    pub keys_total: Option<u64>,
}

impl Migration {
    /// Create a new migration in the importing state (target perspective).
    pub fn new_importing(slot: u16, source: NodeId, target: NodeId) -> Self {
        Self {
            id: MigrationId::new(slot),
            slot,
            source,
            target,
            state: MigrationState::Importing,
            started_at: Some(Instant::now()),
            keys_migrated: 0,
            keys_total: None,
        }
    }

    /// Create a new migration in the migrating state (source perspective).
    pub fn new_migrating(slot: u16, source: NodeId, target: NodeId) -> Self {
        Self {
            id: MigrationId::new(slot),
            slot,
            source,
            target,
            state: MigrationState::Migrating,
            started_at: Some(Instant::now()),
            keys_migrated: 0,
            keys_total: None,
        }
    }

    /// Check if this migration involves a specific node.
    pub fn involves(&self, node: &NodeId) -> bool {
        self.source == *node || self.target == *node
    }

    /// Get progress as a percentage (0-100).
    pub fn progress(&self) -> Option<u8> {
        self.keys_total.map(|total| {
            if total == 0 {
                100
            } else {
                (self.keys_migrated.saturating_mul(100) / total).min(100) as u8
            }
        })
    }

    /// Transition to streaming state.
    pub fn start_streaming(&mut self, total_keys: u64) {
        self.state = MigrationState::Streaming;
        self.keys_total = Some(total_keys);
    }

    /// Record migrated keys.
    pub fn record_migrated(&mut self, count: u64) {
        self.keys_migrated += count;
    }

    /// Transition to finalizing state.
    pub fn start_finalizing(&mut self) {
        self.state = MigrationState::Finalizing;
    }

    /// Mark migration as complete.
    pub fn complete(&mut self) {
        self.state = MigrationState::Complete;
    }
}

/// Tracks all active migrations for a node.
#[derive(Debug, Default)]
pub struct MigrationManager {
    /// Migrations where this node is the source (slot is migrating out).
    outgoing: HashMap<u16, Migration>,
    /// Migrations where this node is the target (slot is importing in).
    incoming: HashMap<u16, Migration>,
    /// Keys that have been migrated but not yet confirmed.
    pending_keys: HashMap<u16, HashSet<Vec<u8>>>,
}

impl MigrationManager {
    /// Create a new migration manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a slot is currently migrating out.
    pub fn is_migrating(&self, slot: u16) -> bool {
        self.outgoing.contains_key(&slot)
    }

    /// Check if a slot is currently being imported.
    pub fn is_importing(&self, slot: u16) -> bool {
        self.incoming.contains_key(&slot)
    }

    /// Get migration info for a slot that's migrating out.
    pub fn get_outgoing(&self, slot: u16) -> Option<&Migration> {
        self.outgoing.get(&slot)
    }

    /// Get migration info for a slot that's being imported.
    pub fn get_incoming(&self, slot: u16) -> Option<&Migration> {
        self.incoming.get(&slot)
    }

    /// Start importing a slot from another node.
    ///
    /// Returns error if slot is already involved in a migration.
    pub fn start_import(
        &mut self,
        slot: u16,
        source: NodeId,
        local_id: NodeId,
    ) -> Result<&Migration, MigrationError> {
        if self.outgoing.contains_key(&slot) {
            return Err(MigrationError::SlotAlreadyMigrating { slot });
        }
        if self.incoming.contains_key(&slot) {
            return Err(MigrationError::SlotAlreadyImporting { slot });
        }

        let migration = Migration::new_importing(slot, source, local_id);
        self.incoming.insert(slot, migration);
        self.pending_keys.insert(slot, HashSet::new());
        self.incoming
            .get(&slot)
            .ok_or(MigrationError::NoMigrationInProgress { slot })
    }

    /// Start migrating a slot to another node.
    ///
    /// Returns error if slot is already involved in a migration.
    pub fn start_migrate(
        &mut self,
        slot: u16,
        local_id: NodeId,
        target: NodeId,
    ) -> Result<&Migration, MigrationError> {
        if self.outgoing.contains_key(&slot) {
            return Err(MigrationError::SlotAlreadyMigrating { slot });
        }
        if self.incoming.contains_key(&slot) {
            return Err(MigrationError::SlotAlreadyImporting { slot });
        }

        let migration = Migration::new_migrating(slot, local_id, target);
        self.outgoing.insert(slot, migration);
        self.pending_keys.insert(slot, HashSet::new());
        self.outgoing
            .get(&slot)
            .ok_or(MigrationError::NoMigrationInProgress { slot })
    }

    /// Record that a key has been migrated.
    pub fn key_migrated(&mut self, slot: u16, key: Vec<u8>) {
        if let Some(keys) = self.pending_keys.get_mut(&slot) {
            keys.insert(key);
        }
        if let Some(migration) = self.outgoing.get_mut(&slot) {
            migration.record_migrated(1);
        }
    }

    /// Check if a specific key has been migrated.
    pub fn is_key_migrated(&self, slot: u16, key: &[u8]) -> bool {
        self.pending_keys
            .get(&slot)
            .is_some_and(|keys| keys.contains(key))
    }

    /// Complete a migration and clean up state.
    pub fn complete_migration(&mut self, slot: u16) -> Option<Migration> {
        self.pending_keys.remove(&slot);
        // Try outgoing first, then incoming
        self.outgoing
            .remove(&slot)
            .or_else(|| self.incoming.remove(&slot))
            .map(|mut m| {
                m.complete();
                m
            })
    }

    /// Abort a migration and clean up state.
    pub fn abort_migration(&mut self, slot: u16) -> Option<Migration> {
        self.pending_keys.remove(&slot);
        self.outgoing
            .remove(&slot)
            .or_else(|| self.incoming.remove(&slot))
    }

    /// Get all active outgoing migrations.
    pub fn outgoing_migrations(&self) -> impl Iterator<Item = &Migration> {
        self.outgoing.values()
    }

    /// Get all active incoming migrations.
    pub fn incoming_migrations(&self) -> impl Iterator<Item = &Migration> {
        self.incoming.values()
    }

    /// Get total number of active migrations.
    pub fn active_count(&self) -> usize {
        self.outgoing.len() + self.incoming.len()
    }
}

/// Represents a batch of keys to migrate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationBatch {
    /// The slot being migrated.
    pub slot: u16,
    /// Keys and their values in this batch.
    pub entries: Vec<MigrationEntry>,
    /// Whether this is the final batch.
    pub is_final: bool,
    /// Sequence number for ordering.
    pub sequence: u64,
}

/// A single key-value entry being migrated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationEntry {
    /// The key being migrated.
    pub key: Vec<u8>,
    /// Serialized value data.
    pub value: Vec<u8>,
    /// TTL remaining in milliseconds (0 = no expiry).
    pub ttl_ms: u64,
}

impl MigrationBatch {
    /// Create a new migration batch.
    pub fn new(slot: u16, sequence: u64) -> Self {
        Self {
            slot,
            entries: Vec::new(),
            is_final: false,
            sequence,
        }
    }

    /// Add an entry to the batch.
    pub fn add_entry(&mut self, key: Vec<u8>, value: Vec<u8>, ttl_ms: u64) {
        self.entries.push(MigrationEntry { key, value, ttl_ms });
    }

    /// Mark this as the final batch.
    pub fn mark_final(&mut self) {
        self.is_final = true;
    }

    /// Get the number of entries in this batch.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Estimate the size of this batch in bytes.
    pub fn size_bytes(&self) -> usize {
        self.entries
            .iter()
            .map(|e| e.key.len() + e.value.len() + 8)
            .sum()
    }
}

/// Errors that can occur during migration.
#[derive(Debug, Clone, thiserror::Error)]
pub enum MigrationError {
    /// Slot is already being migrated out.
    #[error("slot {slot} is already migrating")]
    SlotAlreadyMigrating { slot: u16 },

    /// Slot is already being imported.
    #[error("slot {slot} is already importing")]
    SlotAlreadyImporting { slot: u16 },

    /// No migration in progress for this slot.
    #[error("no migration in progress for slot {slot}")]
    NoMigrationInProgress { slot: u16 },

    /// Migration target is unreachable.
    #[error("cannot reach migration target {addr}: {reason}")]
    TargetUnreachable { addr: SocketAddr, reason: String },

    /// Migration was aborted.
    #[error("migration for slot {slot} was aborted")]
    Aborted { slot: u16 },

    /// Invalid migration state transition.
    #[error("invalid state transition from {from} to {to}")]
    InvalidStateTransition {
        from: MigrationState,
        to: MigrationState,
    },

    /// Timeout during migration.
    #[error("migration timeout after {elapsed:?}")]
    Timeout { elapsed: Duration },
}

/// Result of checking whether a command should be redirected during migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationRedirect {
    /// No redirect needed, handle locally.
    None,
    /// Send MOVED redirect (slot permanently moved).
    Moved { slot: u16, addr: SocketAddr },
    /// Send ASK redirect (slot temporarily at another node).
    Ask { slot: u16, addr: SocketAddr },
}

impl MigrationRedirect {
    /// Format as RESP error string.
    pub fn to_error_string(&self) -> Option<String> {
        match self {
            Self::None => None,
            Self::Moved { slot, addr } => Some(format!("MOVED {} {}", slot, addr)),
            Self::Ask { slot, addr } => Some(format!("ASK {} {}", slot, addr)),
        }
    }
}

/// Configuration for migration behavior.
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// Maximum keys per batch.
    pub batch_size: usize,
    /// Maximum batch size in bytes.
    pub batch_bytes: usize,
    /// Timeout for individual key migration.
    pub key_timeout: Duration,
    /// Timeout for entire migration.
    pub migration_timeout: Duration,
    /// Delay between batches to avoid overwhelming the network.
    pub batch_delay: Duration,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            batch_bytes: 1024 * 1024, // 1MB
            key_timeout: Duration::from_secs(5),
            migration_timeout: Duration::from_secs(3600), // 1 hour
            batch_delay: Duration::from_millis(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_id() -> NodeId {
        NodeId::new()
    }

    #[test]
    fn migration_new_importing() {
        let source = node_id();
        let target = node_id();
        let m = Migration::new_importing(100, source, target);

        assert_eq!(m.slot, 100);
        assert_eq!(m.source, source);
        assert_eq!(m.target, target);
        assert_eq!(m.state, MigrationState::Importing);
        assert_eq!(m.keys_migrated, 0);
    }

    #[test]
    fn migration_new_migrating() {
        let source = node_id();
        let target = node_id();
        let m = Migration::new_migrating(100, source, target);

        assert_eq!(m.state, MigrationState::Migrating);
    }

    #[test]
    fn migration_involves() {
        let source = node_id();
        let target = node_id();
        let other = node_id();
        let m = Migration::new_importing(100, source, target);

        assert!(m.involves(&source));
        assert!(m.involves(&target));
        assert!(!m.involves(&other));
    }

    #[test]
    fn migration_progress() {
        let mut m = Migration::new_migrating(100, node_id(), node_id());

        // No total set
        assert_eq!(m.progress(), None);

        // Set total and migrate some
        m.start_streaming(100);
        assert_eq!(m.progress(), Some(0));

        m.record_migrated(50);
        assert_eq!(m.progress(), Some(50));

        m.record_migrated(50);
        assert_eq!(m.progress(), Some(100));
    }

    #[test]
    fn migration_state_transitions() {
        let mut m = Migration::new_migrating(100, node_id(), node_id());

        assert_eq!(m.state, MigrationState::Migrating);

        m.start_streaming(50);
        assert_eq!(m.state, MigrationState::Streaming);

        m.start_finalizing();
        assert_eq!(m.state, MigrationState::Finalizing);

        m.complete();
        assert_eq!(m.state, MigrationState::Complete);
    }

    #[test]
    fn manager_start_import() {
        let mut manager = MigrationManager::new();
        let source = node_id();
        let local = node_id();

        let result = manager.start_import(100, source, local);
        assert!(result.is_ok());
        assert!(manager.is_importing(100));
        assert!(!manager.is_migrating(100));
    }

    #[test]
    fn manager_start_migrate() {
        let mut manager = MigrationManager::new();
        let local = node_id();
        let target = node_id();

        let result = manager.start_migrate(100, local, target);
        assert!(result.is_ok());
        assert!(manager.is_migrating(100));
        assert!(!manager.is_importing(100));
    }

    #[test]
    fn manager_double_migration_error() {
        let mut manager = MigrationManager::new();
        let local = node_id();
        let target = node_id();

        manager.start_migrate(100, local, target).unwrap();

        // Can't migrate same slot again
        let result = manager.start_migrate(100, local, node_id());
        assert!(matches!(
            result,
            Err(MigrationError::SlotAlreadyMigrating { slot: 100 })
        ));

        // Can't import a slot that's migrating
        let result = manager.start_import(100, node_id(), local);
        assert!(matches!(
            result,
            Err(MigrationError::SlotAlreadyMigrating { slot: 100 })
        ));
    }

    #[test]
    fn manager_key_tracking() {
        let mut manager = MigrationManager::new();
        let local = node_id();
        let target = node_id();

        manager.start_migrate(100, local, target).unwrap();

        assert!(!manager.is_key_migrated(100, b"key1"));

        manager.key_migrated(100, b"key1".to_vec());
        assert!(manager.is_key_migrated(100, b"key1"));
        assert!(!manager.is_key_migrated(100, b"key2"));
    }

    #[test]
    fn manager_complete_migration() {
        let mut manager = MigrationManager::new();
        let local = node_id();
        let target = node_id();

        manager.start_migrate(100, local, target).unwrap();
        manager.key_migrated(100, b"key1".to_vec());

        let completed = manager.complete_migration(100);
        assert!(completed.is_some());
        assert_eq!(completed.unwrap().state, MigrationState::Complete);

        // State should be cleaned up
        assert!(!manager.is_migrating(100));
        assert!(!manager.is_key_migrated(100, b"key1"));
    }

    #[test]
    fn manager_abort_migration() {
        let mut manager = MigrationManager::new();
        let local = node_id();
        let target = node_id();

        manager.start_migrate(100, local, target).unwrap();

        let aborted = manager.abort_migration(100);
        assert!(aborted.is_some());

        assert!(!manager.is_migrating(100));
    }

    #[test]
    fn batch_operations() {
        let mut batch = MigrationBatch::new(100, 1);

        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        batch.add_entry(b"key1".to_vec(), b"value1".to_vec(), 0);
        batch.add_entry(b"key2".to_vec(), b"value2".to_vec(), 5000);

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_final);

        batch.mark_final();
        assert!(batch.is_final);
    }

    #[test]
    fn redirect_formatting() {
        let moved = MigrationRedirect::Moved {
            slot: 100,
            addr: "127.0.0.1:6379".parse().unwrap(),
        };
        assert_eq!(
            moved.to_error_string(),
            Some("MOVED 100 127.0.0.1:6379".to_string())
        );

        let ask = MigrationRedirect::Ask {
            slot: 200,
            addr: "127.0.0.1:6380".parse().unwrap(),
        };
        assert_eq!(
            ask.to_error_string(),
            Some("ASK 200 127.0.0.1:6380".to_string())
        );

        assert_eq!(MigrationRedirect::None.to_error_string(), None);
    }

    #[test]
    fn migration_state_display() {
        assert_eq!(MigrationState::Importing.to_string(), "importing");
        assert_eq!(MigrationState::Migrating.to_string(), "migrating");
        assert_eq!(MigrationState::Streaming.to_string(), "streaming");
        assert_eq!(MigrationState::Finalizing.to_string(), "finalizing");
        assert_eq!(MigrationState::Complete.to_string(), "complete");
    }

    #[test]
    fn config_defaults() {
        let config = MigrationConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.batch_bytes, 1024 * 1024);
        assert_eq!(config.key_timeout, Duration::from_secs(5));
    }
}
