//! The keyspace: Ember's core key-value store.
//!
//! A `Keyspace` owns a flat `AHashMap<Box<str>, Entry>` and handles
//! get, set, delete, existence checks, and TTL management. Expired
//! keys are removed lazily on access. Memory usage is tracked on
//! every mutation for eviction and stats reporting.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use ahash::AHashMap;
use bytes::Bytes;
use rand::seq::IteratorRandom;

use tracing::warn;

use crate::dropper::DropHandle;
use crate::memory::{self, MemoryTracker};
use crate::time;
use crate::types::sorted_set::{SortedSet, ZAddFlags};
use crate::types::{self, normalize_range, Value};

mod string;
mod list;
mod hash;
mod set;
mod zset;
#[cfg(feature = "vector")]
mod vector;
#[cfg(feature = "protobuf")]
mod proto;

const WRONGTYPE_MSG: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const OOM_MSG: &str = "OOM command not allowed when used memory > 'maxmemory'";

/// Error returned when a command is used against a key holding the wrong type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrongType;

impl std::fmt::Display for WrongType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{WRONGTYPE_MSG}")
    }
}

impl std::error::Error for WrongType {}

/// Error returned by write operations that may fail due to type mismatch
/// or memory limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteError {
    /// The key holds a different type than expected.
    WrongType,
    /// Memory limit reached and eviction couldn't free enough space.
    OutOfMemory,
}

impl From<WrongType> for WriteError {
    fn from(_: WrongType) -> Self {
        WriteError::WrongType
    }
}

/// Errors that can occur during INCR/DECR operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrError {
    /// Key holds a non-string type.
    WrongType,
    /// Value is not a valid integer.
    NotAnInteger,
    /// Increment or decrement would overflow i64.
    Overflow,
    /// Memory limit reached.
    OutOfMemory,
}

impl std::fmt::Display for IncrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncrError::WrongType => write!(f, "{WRONGTYPE_MSG}"),
            IncrError::NotAnInteger => write!(f, "ERR value is not an integer or out of range"),
            IncrError::Overflow => write!(f, "ERR increment or decrement would overflow"),
            IncrError::OutOfMemory => write!(f, "{OOM_MSG}"),
        }
    }
}

impl std::error::Error for IncrError {}

/// Errors that can occur during INCRBYFLOAT operations.
#[derive(Debug, Clone, PartialEq)]
pub enum IncrFloatError {
    /// Key holds a non-string type.
    WrongType,
    /// Value is not a valid float.
    NotAFloat,
    /// Result would be NaN or Infinity.
    NanOrInfinity,
    /// Memory limit reached.
    OutOfMemory,
}

impl std::fmt::Display for IncrFloatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IncrFloatError::WrongType => write!(f, "{WRONGTYPE_MSG}"),
            IncrFloatError::NotAFloat => write!(f, "ERR value is not a valid float"),
            IncrFloatError::NanOrInfinity => {
                write!(f, "ERR increment would produce NaN or Infinity")
            }
            IncrFloatError::OutOfMemory => write!(f, "{OOM_MSG}"),
        }
    }
}

impl std::error::Error for IncrFloatError {}

/// Error returned when RENAME fails because the source key doesn't exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameError {
    /// The source key does not exist.
    NoSuchKey,
}

impl std::fmt::Display for RenameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RenameError::NoSuchKey => write!(f, "ERR no such key"),
        }
    }
}

impl std::error::Error for RenameError {}

/// Result of a ZADD operation, containing both the client-facing count
/// and the list of members that were actually applied (for AOF correctness).
#[derive(Debug, Clone)]
pub struct ZAddResult {
    /// Number of members added (or added+updated if CH flag was set).
    pub count: usize,
    /// Members that were actually inserted or had their score updated.
    /// Only these should be persisted to the AOF.
    pub applied: Vec<(f64, String)>,
}

/// Result of a VADD operation, carrying the applied element for AOF persistence.
#[cfg(feature = "vector")]
#[derive(Debug, Clone)]
pub struct VAddResult {
    /// The element name that was added or updated.
    pub element: String,
    /// The vector that was stored.
    pub vector: Vec<f32>,
    /// Whether a new element was added (false = updated existing).
    pub added: bool,
}

/// Result of a VADD_BATCH operation.
#[cfg(feature = "vector")]
#[derive(Debug, Clone)]
pub struct VAddBatchResult {
    /// Number of newly added elements (not updates).
    pub added_count: usize,
    /// Elements that were actually inserted or updated, with their vectors.
    /// Only these should be persisted to the AOF.
    pub applied: Vec<(String, Vec<f32>)>,
}

/// Errors from vector write operations.
#[cfg(feature = "vector")]
#[derive(Debug, Clone)]
pub enum VectorWriteError {
    /// The key holds a different type than expected.
    WrongType,
    /// Memory limit reached.
    OutOfMemory,
    /// usearch index error (dimension mismatch, capacity, etc).
    IndexError(String),
    /// A batch insert partially succeeded before encountering an error.
    /// The applied vectors should still be persisted to the AOF.
    PartialBatch {
        message: String,
        applied: Vec<(String, Vec<f32>)>,
    },
}

/// How the keyspace should handle writes when the memory limit is reached.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Return an error on writes when memory is full.
    #[default]
    NoEviction,
    /// Evict the least-recently-used key (approximated via random sampling).
    AllKeysLru,
}

/// Configuration for a single keyspace / shard.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Maximum memory in bytes. `None` means unlimited.
    pub max_memory: Option<usize>,
    /// What to do when memory is full.
    pub eviction_policy: EvictionPolicy,
    /// Numeric identifier for this shard (used for persistence file naming).
    pub shard_id: u16,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            max_memory: None,
            eviction_policy: EvictionPolicy::NoEviction,
            shard_id: 0,
        }
    }
}

/// Result of a set operation that may fail under memory pressure.
#[derive(Debug, PartialEq, Eq)]
pub enum SetResult {
    /// The key was stored successfully.
    Ok,
    /// Memory limit reached and eviction policy is NoEviction.
    OutOfMemory,
    /// NX/XX condition was not met (key existed for NX, or didn't for XX).
    Blocked,
}

/// A single entry in the keyspace: a value plus optional expiration
/// and last access time for LRU approximation.
///
/// Memory optimized: uses u64 timestamps instead of Option<Instant>.
/// Saves 8 bytes per entry (24 bytes down from 32 for metadata).
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) value: Value,
    /// Monotonic expiry timestamp in ms. 0 = no expiry.
    pub(crate) expires_at_ms: u64,
    /// Monotonic last access timestamp in ms (for LRU).
    pub(crate) last_access_ms: u64,
}

impl Entry {
    fn new(value: Value, ttl: Option<Duration>) -> Self {
        Self {
            value,
            expires_at_ms: time::expiry_from_duration(ttl),
            last_access_ms: time::now_ms(),
        }
    }

    /// Returns `true` if this entry has passed its expiration time.
    fn is_expired(&self) -> bool {
        time::is_expired(self.expires_at_ms)
    }

    /// Marks this entry as accessed right now.
    fn touch(&mut self) {
        self.last_access_ms = time::now_ms();
    }
}

/// Result of a TTL query, matching Redis semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TtlResult {
    /// Key exists and has a TTL. Returns remaining seconds.
    Seconds(u64),
    /// Key exists and has a TTL. Returns remaining milliseconds.
    Milliseconds(u64),
    /// Key exists but has no expiration set.
    NoExpiry,
    /// Key does not exist.
    NotFound,
}

/// Aggregated statistics for a keyspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyspaceStats {
    /// Number of live keys.
    pub key_count: usize,
    /// Estimated memory usage in bytes.
    pub used_bytes: usize,
    /// Number of keys with an expiration set.
    pub keys_with_expiry: usize,
    /// Cumulative count of keys removed by expiration (lazy + active).
    pub keys_expired: u64,
    /// Cumulative count of keys removed by eviction.
    pub keys_evicted: u64,
}

/// Number of random keys to sample when looking for an eviction candidate.
///
/// Eviction uses sampling-based approximate LRU — we randomly select this many
/// keys and evict the least-recently-accessed among them. This trades perfect
/// LRU accuracy for O(1) eviction (no sorted structure to maintain).
///
/// Larger sample sizes give better LRU approximation but cost more per eviction.
/// 16 is a reasonable balance — similar to Redis's default sample size. With
/// 16 samples, we statistically find a good eviction candidate while keeping
/// eviction overhead low even at millions of keys.
const EVICTION_SAMPLE_SIZE: usize = 16;

/// The core key-value store.
///
/// All operations are single-threaded per shard — no internal locking.
/// Memory usage is tracked incrementally on every mutation.
pub struct Keyspace {
    entries: AHashMap<Box<str>, Entry>,
    memory: MemoryTracker,
    config: ShardConfig,
    /// Number of entries that currently have an expiration set.
    expiry_count: usize,
    /// Cumulative count of keys removed by expiration (lazy + active).
    expired_total: u64,
    /// Cumulative count of keys removed by eviction.
    evicted_total: u64,
    /// When set, large values are dropped on a background thread instead
    /// of inline on the shard thread. See [`crate::dropper`].
    drop_handle: Option<DropHandle>,
}

impl Keyspace {
    /// Creates a new, empty keyspace with default config (no memory limit).
    pub fn new() -> Self {
        Self::with_config(ShardConfig::default())
    }

    /// Creates a new, empty keyspace with the given config.
    pub fn with_config(config: ShardConfig) -> Self {
        Self {
            entries: AHashMap::new(),
            memory: MemoryTracker::new(),
            config,
            expiry_count: 0,
            expired_total: 0,
            evicted_total: 0,
            drop_handle: None,
        }
    }

    /// Attaches a background drop handle for lazy free. When set, large
    /// values removed by del/eviction/expiration are dropped on a
    /// background thread instead of blocking the shard.
    pub fn set_drop_handle(&mut self, handle: DropHandle) {
        self.drop_handle = Some(handle);
    }

    /// Decrements the expiry count if the entry had a TTL set.
    fn decrement_expiry_if_set(&mut self, entry: &Entry) {
        if entry.expires_at_ms != 0 {
            self.expiry_count = self.expiry_count.saturating_sub(1);
        }
    }

    /// Cleans up after removing an element from a collection (list, sorted
    /// set, hash, or set). If the collection is now empty, removes the key
    /// entirely and subtracts `old_size` from the memory tracker. Otherwise
    /// subtracts `removed_bytes` (the byte cost of the removed element(s))
    /// without rescanning the remaining collection.
    fn cleanup_after_remove(
        &mut self,
        key: &str,
        old_size: usize,
        is_empty: bool,
        removed_bytes: usize,
    ) {
        if is_empty {
            if let Some(removed) = self.entries.remove(key) {
                self.decrement_expiry_if_set(&removed);
            }
            self.memory.remove_with_size(old_size);
        } else {
            self.memory.shrink_by(removed_bytes);
        }
    }

    /// Checks whether a key either doesn't exist or holds the expected
    /// collection type. Returns `Ok(true)` if the key is new,
    /// `Ok(false)` if it exists with the right type, or `Err(WrongType)`
    /// if the key exists with a different type.
    fn ensure_collection_type(
        &self,
        key: &str,
        type_check: fn(&Value) -> bool,
    ) -> Result<bool, WriteError> {
        match self.entries.get(key) {
            None => Ok(true),
            Some(e) if type_check(&e.value) => Ok(false),
            Some(_) => Err(WriteError::WrongType),
        }
    }

    /// Estimates the memory cost of a collection write and enforces the
    /// limit. `base_overhead` is the fixed cost of a new collection
    /// (e.g. VECDEQUE_BASE_OVERHEAD). Returns `Ok(())` on success.
    fn reserve_memory(
        &mut self,
        is_new: bool,
        key: &str,
        base_overhead: usize,
        element_increase: usize,
    ) -> Result<(), WriteError> {
        let estimated_increase = if is_new {
            memory::ENTRY_OVERHEAD + key.len() + base_overhead + element_increase
        } else {
            element_increase
        };
        if self.enforce_memory_limit(estimated_increase) {
            Ok(())
        } else {
            Err(WriteError::OutOfMemory)
        }
    }

    /// Inserts a new key with an empty collection value. Used by
    /// collection-write methods after type-checking and memory reservation.
    fn insert_empty(&mut self, key: &str, value: Value) {
        self.memory.add(key, &value);
        self.entries.insert(Box::from(key), Entry::new(value, None));
    }

    /// Measures entry size before and after a mutation, adjusting the
    /// memory tracker for the difference. Touches the entry afterwards.
    fn track_size<T>(&mut self, key: &str, f: impl FnOnce(&mut Entry) -> T) -> Option<T> {
        let entry = self.entries.get_mut(key)?;
        let old_size = memory::entry_size(key, &entry.value);
        let result = f(entry);
        let entry = self.entries.get(key)?;
        let new_size = memory::entry_size(key, &entry.value);
        self.memory.adjust(old_size, new_size);
        Some(result)
    }

    /// Adjusts the expiry count when replacing an entry whose TTL status
    /// may have changed (e.g. SET overwriting an existing key).
    fn adjust_expiry_count(&mut self, had_expiry: bool, has_expiry: bool) {
        match (had_expiry, has_expiry) {
            (false, true) => self.expiry_count += 1,
            (true, false) => self.expiry_count = self.expiry_count.saturating_sub(1),
            _ => {}
        }
    }

    /// Tries to evict one key using LRU approximation.
    ///
    /// Randomly samples `EVICTION_SAMPLE_SIZE` keys and removes the one
    /// with the oldest `last_access` time. Returns `true` if a key was
    /// evicted, `false` if the keyspace is empty.
    ///
    /// Uses reservoir sampling with k=1 to avoid allocating a Vec on
    /// every eviction attempt. The victim key index is remembered
    /// rather than cloned, eliminating a heap allocation on the hot path.
    fn try_evict(&mut self) -> bool {
        if self.entries.is_empty() {
            return false;
        }

        let mut rng = rand::rng();

        // reservoir sample k=1 from the iterator, tracking the oldest
        // entry by last_access_ms. this replaces choose_multiple() which
        // allocates a Vec internally.
        let mut best_key: Option<&str> = None;
        let mut best_access = u64::MAX;
        let mut seen = 0usize;

        for (key, entry) in &self.entries {
            // reservoir sampling: include this item with probability
            // EVICTION_SAMPLE_SIZE / (seen + 1), capped once we have
            // enough candidates
            seen += 1;
            if seen <= EVICTION_SAMPLE_SIZE {
                if entry.last_access_ms < best_access {
                    best_access = entry.last_access_ms;
                    best_key = Some(&**key);
                }
            } else {
                use rand::Rng;
                let j = rng.random_range(0..seen);
                if j < EVICTION_SAMPLE_SIZE && entry.last_access_ms < best_access {
                    best_access = entry.last_access_ms;
                    best_key = Some(&**key);
                }
            }
        }

        if let Some(victim) = best_key {
            // own the key to break the immutable borrow on self.entries
            let victim = victim.to_owned();
            if let Some(entry) = self.entries.remove(victim.as_str()) {
                self.memory.remove(&victim, &entry.value);
                self.decrement_expiry_if_set(&entry);
                self.evicted_total += 1;
                self.defer_drop(entry.value);
                return true;
            }
        }
        false
    }

    /// Checks whether the memory limit allows a write that would increase
    /// usage by `estimated_increase` bytes. Attempts eviction if the
    /// policy allows it. Returns `true` if the write can proceed.
    ///
    /// The comparison uses [`memory::effective_limit`] rather than the raw
    /// configured maximum. This reserves headroom for allocator overhead
    /// and fragmentation that our per-entry estimates can't account for,
    /// preventing the OS from OOM-killing us before eviction triggers.
    fn enforce_memory_limit(&mut self, estimated_increase: usize) -> bool {
        if let Some(max) = self.config.max_memory {
            let limit = memory::effective_limit(max);
            while self.memory.used_bytes() + estimated_increase > limit {
                match self.config.eviction_policy {
                    EvictionPolicy::NoEviction => return false,
                    EvictionPolicy::AllKeysLru => {
                        if !self.try_evict() {
                            return false;
                        }
                    }
                }
            }
        }
        true
    }

    /// Removes a key. Returns `true` if the key existed (and wasn't expired).
    ///
    /// When a drop handle is set, large values are dropped on the
    /// background thread instead of inline.
    pub fn del(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        if let Some(entry) = self.entries.remove(key) {
            self.memory.remove(key, &entry.value);
            self.decrement_expiry_if_set(&entry);
            self.defer_drop(entry.value);
            true
        } else {
            false
        }
    }

    /// Removes a key like `del`, but always defers the value's destructor
    /// to the background drop thread (when available). Semantically
    /// identical to DEL — the key is gone immediately, memory is
    /// accounted for immediately, but the actual deallocation happens
    /// off the hot path.
    pub fn unlink(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        if let Some(entry) = self.entries.remove(key) {
            self.memory.remove(key, &entry.value);
            self.decrement_expiry_if_set(&entry);
            // always defer for UNLINK, regardless of value size
            if let Some(ref handle) = self.drop_handle {
                handle.defer_value(entry.value);
            }
            true
        } else {
            false
        }
    }

    /// Replaces the entries map with an empty one and resets memory
    /// tracking. Returns the old entries so the caller can send them
    /// to the background drop thread.
    pub(crate) fn flush_async(&mut self) -> AHashMap<Box<str>, Entry> {
        let old = std::mem::take(&mut self.entries);
        self.memory.reset();
        self.expiry_count = 0;
        old
    }

    /// Returns `true` if the key exists and hasn't expired.
    pub fn exists(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        self.entries.contains_key(key)
    }

    /// Sets an expiration on an existing key. Returns `true` if the key
    /// exists (and the TTL was set), `false` if the key doesn't exist.
    pub fn expire(&mut self, key: &str, seconds: u64) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                if entry.expires_at_ms == 0 {
                    self.expiry_count += 1;
                }
                entry.expires_at_ms = time::now_ms().saturating_add(seconds.saturating_mul(1000));
                true
            }
            None => false,
        }
    }

    /// Returns the TTL status for a key, following Redis semantics:
    /// - `Seconds(n)` if the key has a TTL
    /// - `NoExpiry` if the key exists without a TTL
    /// - `NotFound` if the key doesn't exist
    pub fn ttl(&mut self, key: &str) -> TtlResult {
        if self.remove_if_expired(key) {
            return TtlResult::NotFound;
        }
        match self.entries.get(key) {
            Some(entry) => match time::remaining_secs(entry.expires_at_ms) {
                Some(secs) => TtlResult::Seconds(secs),
                None => TtlResult::NoExpiry,
            },
            None => TtlResult::NotFound,
        }
    }

    /// Removes the expiration from a key.
    ///
    /// Returns `true` if the key existed and had a timeout that was removed.
    /// Returns `false` if the key doesn't exist or has no expiration.
    pub fn persist(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                if entry.expires_at_ms != 0 {
                    entry.expires_at_ms = 0;
                    self.expiry_count = self.expiry_count.saturating_sub(1);
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }

    /// Returns the TTL status for a key in milliseconds, following Redis semantics:
    /// - `Milliseconds(n)` if the key has a TTL
    /// - `NoExpiry` if the key exists without a TTL
    /// - `NotFound` if the key doesn't exist
    pub fn pttl(&mut self, key: &str) -> TtlResult {
        if self.remove_if_expired(key) {
            return TtlResult::NotFound;
        }
        match self.entries.get(key) {
            Some(entry) => match time::remaining_ms(entry.expires_at_ms) {
                Some(ms) => TtlResult::Milliseconds(ms),
                None => TtlResult::NoExpiry,
            },
            None => TtlResult::NotFound,
        }
    }

    /// Sets an expiration on an existing key in milliseconds.
    ///
    /// Returns `true` if the key exists (and the TTL was set),
    /// `false` if the key doesn't exist.
    pub fn pexpire(&mut self, key: &str, millis: u64) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                if entry.expires_at_ms == 0 {
                    self.expiry_count += 1;
                }
                entry.expires_at_ms = time::now_ms().saturating_add(millis);
                true
            }
            None => false,
        }
    }

    /// Returns all keys matching a glob pattern.
    ///
    /// Warning: O(n) scan of the entire keyspace. Use SCAN for production
    /// workloads with large key counts.
    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let len = self.entries.len();
        if len > 10_000 {
            warn!(
                key_count = len,
                "KEYS on large keyspace, consider SCAN instead"
            );
        }
        let compiled = GlobPattern::new(pattern);
        self.entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| compiled.matches(key))
            .map(|(key, _)| String::from(&**key))
            .collect()
    }

    /// Counts live keys in this keyspace that hash to the given cluster slot.
    ///
    /// O(n) scan over all entries — same cost as KEYS.
    pub fn count_keys_in_slot(&self, slot: u16) -> usize {
        self.entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| ember_cluster::key_slot(key.as_bytes()) == slot)
            .count()
    }

    /// Returns up to `count` live keys that hash to the given cluster slot.
    ///
    /// O(n) scan over all entries — same cost as KEYS.
    pub fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<String> {
        self.entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| ember_cluster::key_slot(key.as_bytes()) == slot)
            .take(count)
            .map(|(key, _)| String::from(&**key))
            .collect()
    }

    /// Renames a key to a new name. Returns an error if the source key
    /// doesn't exist. If the destination key already exists, it is overwritten.
    pub fn rename(&mut self, key: &str, newkey: &str) -> Result<(), RenameError> {
        self.remove_if_expired(key);
        self.remove_if_expired(newkey);

        let entry = match self.entries.remove(key) {
            Some(entry) => entry,
            None => return Err(RenameError::NoSuchKey),
        };

        // update memory tracking for old key removal
        self.memory.remove(key, &entry.value);
        self.decrement_expiry_if_set(&entry);

        // remove destination if it exists
        if let Some(old_dest) = self.entries.remove(newkey) {
            self.memory.remove(newkey, &old_dest.value);
            self.decrement_expiry_if_set(&old_dest);
        }

        // re-insert with the new key name, preserving value and expiry
        self.memory.add(newkey, &entry.value);
        if entry.expires_at_ms != 0 {
            self.expiry_count += 1;
        }
        self.entries.insert(Box::from(newkey), entry);
        Ok(())
    }

    /// Returns aggregated stats for this keyspace.
    ///
    /// All fields are tracked incrementally — this is O(1).
    pub fn stats(&self) -> KeyspaceStats {
        KeyspaceStats {
            key_count: self.memory.key_count(),
            used_bytes: self.memory.used_bytes(),
            keys_with_expiry: self.expiry_count,
            keys_expired: self.expired_total,
            keys_evicted: self.evicted_total,
        }
    }

    /// Returns the number of live keys.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Removes all keys from the keyspace.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.memory.reset();
        self.expiry_count = 0;
    }

    /// Returns `true` if the keyspace has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Scans keys starting from a cursor position.
    ///
    /// Returns the next cursor (0 if scan complete) and a batch of keys.
    /// The `pattern` argument supports glob-style matching (`*`, `?`, `[abc]`).
    pub fn scan_keys(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&str>,
    ) -> (u64, Vec<String>) {
        let mut keys = Vec::with_capacity(count);
        let mut position = 0u64;
        let target_count = if count == 0 { 10 } else { count };

        let compiled = pattern.map(GlobPattern::new);

        for (key, entry) in self.entries.iter() {
            // skip expired entries
            if entry.is_expired() {
                continue;
            }

            // skip entries before cursor
            if position < cursor {
                position += 1;
                continue;
            }

            // pattern matching
            if let Some(ref pat) = compiled {
                if !pat.matches(key) {
                    position += 1;
                    continue;
                }
            }

            keys.push(String::from(&**key));
            position += 1;

            if keys.len() >= target_count {
                // return position as next cursor
                return (position, keys);
            }
        }

        // scan complete
        (0, keys)
    }

    /// Returns the value and remaining TTL in milliseconds for a single key.
    ///
    /// Returns `None` if the key doesn't exist or is expired. TTL is -1 for
    /// entries with no expiration. Used by MIGRATE/DUMP to serialize a key
    /// for transfer to another node.
    pub fn dump(&mut self, key: &str) -> Option<(&Value, i64)> {
        if self.remove_if_expired(key) {
            return None;
        }
        let entry = self.entries.get(key)?;
        let ttl_ms = match time::remaining_ms(entry.expires_at_ms) {
            Some(ms) => ms.min(i64::MAX as u64) as i64,
            None => -1,
        };
        Some((&entry.value, ttl_ms))
    }

    /// Iterates over all live (non-expired) entries, yielding the key, a
    /// clone of the value, and the remaining TTL in milliseconds (-1 for
    /// entries with no expiration). Used by snapshot and AOF rewrite.
    pub fn iter_entries(&self) -> impl Iterator<Item = (&str, &Value, i64)> {
        self.entries.iter().filter_map(move |(key, entry)| {
            if entry.is_expired() {
                return None;
            }
            let ttl_ms = match time::remaining_ms(entry.expires_at_ms) {
                Some(ms) => ms.min(i64::MAX as u64) as i64,
                None => -1,
            };
            Some((&**key, &entry.value, ttl_ms))
        })
    }

    /// Restores an entry during recovery, bypassing memory limits.
    ///
    /// `ttl` is the remaining time-to-live. If `None`, the key has no expiry.
    /// This is used only during shard startup when loading from
    /// snapshot/AOF — normal writes should go through `set()`.
    pub fn restore(&mut self, key: String, value: Value, ttl: Option<Duration>) {
        let has_expiry = ttl.is_some();

        // if replacing an existing entry, adjust memory tracking
        if let Some(old) = self.entries.get(key.as_str()) {
            self.memory.replace(&key, &old.value, &value);
            self.adjust_expiry_count(old.expires_at_ms != 0, has_expiry);
        } else {
            self.memory.add(&key, &value);
            if has_expiry {
                self.expiry_count += 1;
            }
        }

        self.entries
            .insert(key.into_boxed_str(), Entry::new(value, ttl));
    }

    /// Randomly samples up to `count` keys and removes any that have expired.
    ///
    /// Returns the number of keys actually removed. Used by the active
    /// expiration cycle to clean up keys that no one is reading.
    pub fn expire_sample(&mut self, count: usize) -> usize {
        if self.entries.is_empty() {
            return 0;
        }

        let mut rng = rand::rng();

        let keys_to_check: Vec<String> = self
            .entries
            .keys()
            .choose_multiple(&mut rng, count)
            .into_iter()
            .map(|k| String::from(&**k))
            .collect();

        let mut removed = 0;
        for key in &keys_to_check {
            if self.remove_if_expired(key) {
                removed += 1;
            }
        }
        removed
    }

    /// Checks if a key is expired and removes it if so. Returns `true`
    /// if the key was removed (or didn't exist).
    fn remove_if_expired(&mut self, key: &str) -> bool {
        let expired = self
            .entries
            .get(key)
            .map(|e| e.is_expired())
            .unwrap_or(false);

        if expired {
            if let Some(entry) = self.entries.remove(key) {
                self.memory.remove(key, &entry.value);
                self.decrement_expiry_if_set(&entry);
                self.expired_total += 1;
                self.defer_drop(entry.value);
            }
        }
        expired
    }

    /// Sends a value to the background drop thread if one is configured
    /// and the value is large enough to justify the overhead.
    fn defer_drop(&self, value: Value) {
        if let Some(ref handle) = self.drop_handle {
            handle.defer_value(value);
        }
    }
}

impl Default for Keyspace {
    fn default() -> Self {
        Self::new()
    }
}

/// Formats a float value matching Redis behavior.
///
/// Uses up to 17 significant digits and strips unnecessary trailing zeros,
/// but always keeps at least one decimal place for non-integer results.
pub(crate) fn format_float(val: f64) -> String {
    if val == 0.0 {
        return "0".into();
    }
    // Use enough precision to round-trip
    let s = format!("{:.17e}", val);
    // Parse back to get the clean representation
    let reparsed: f64 = s.parse().unwrap_or(val);
    // If it's a whole number that fits in i64, format without decimals
    if reparsed == reparsed.trunc() && reparsed >= i64::MIN as f64 && reparsed <= i64::MAX as f64 {
        format!("{}", reparsed as i64)
    } else {
        // Use ryu-like formatting via Display which strips trailing zeros
        let formatted = format!("{}", reparsed);
        formatted
    }
}

/// Glob-style pattern matching for SCAN's MATCH option.
///
/// Supports:
/// - `*` matches any sequence of characters (including empty)
/// - `?` matches exactly one character
/// - `[abc]` matches one character from the set
/// - `[^abc]` or `[!abc]` matches one character NOT in the set
///
/// Uses an iterative two-pointer algorithm with backtracking for O(n*m)
/// worst-case performance, where n is pattern length and m is text length.
///
/// Prefer [`GlobPattern::new`] + [`GlobPattern::matches`] when matching
/// the same pattern against many strings (KEYS, SCAN) to avoid
/// re-collecting the pattern chars on every call.
pub(crate) fn glob_match(pattern: &str, text: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    glob_match_compiled(&pat, text)
}

/// Pre-compiled glob pattern that avoids re-allocating pattern chars
/// on every match call. Use for KEYS/SCAN where the same pattern is
/// tested against every key in the keyspace.
pub(crate) struct GlobPattern {
    chars: Vec<char>,
}

impl GlobPattern {
    pub(crate) fn new(pattern: &str) -> Self {
        Self {
            chars: pattern.chars().collect(),
        }
    }

    pub(crate) fn matches(&self, text: &str) -> bool {
        glob_match_compiled(&self.chars, text)
    }
}

/// Core glob matching against a pre-compiled pattern char slice.
fn glob_match_compiled(pat: &[char], text: &str) -> bool {
    let txt: Vec<char> = text.chars().collect();

    let mut pi = 0; // pattern index
    let mut ti = 0; // text index

    // backtracking state for the most recent '*'
    let mut star_pi: Option<usize> = None;
    let mut star_ti: usize = 0;

    while ti < txt.len() || pi < pat.len() {
        if pi < pat.len() {
            match pat[pi] {
                '*' => {
                    // record star position and try matching zero chars first
                    star_pi = Some(pi);
                    star_ti = ti;
                    pi += 1;
                    continue;
                }
                '?' if ti < txt.len() => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                '[' if ti < txt.len() => {
                    // parse character class
                    let tc = txt[ti];
                    let mut j = pi + 1;
                    let mut negated = false;
                    let mut matched = false;

                    if j < pat.len() && (pat[j] == '^' || pat[j] == '!') {
                        negated = true;
                        j += 1;
                    }

                    while j < pat.len() && pat[j] != ']' {
                        if pat[j] == tc {
                            matched = true;
                        }
                        j += 1;
                    }

                    if negated {
                        matched = !matched;
                    }

                    if matched && j < pat.len() {
                        pi = j + 1; // skip past ']'
                        ti += 1;
                        continue;
                    }
                    // fall through to backtrack
                }
                c if ti < txt.len() && c == txt[ti] => {
                    pi += 1;
                    ti += 1;
                    continue;
                }
                _ => {}
            }
        }

        // mismatch or end of pattern — try backtracking to last '*'
        if let Some(sp) = star_pi {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
            if ti > txt.len() {
                return false;
            }
        } else {
            return false;
        }
    }

    // skip trailing '*' in pattern
    while pi < pat.len() && pat[pi] == '*' {
        pi += 1;
    }

    pi == pat.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn del_existing() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);
        assert!(ks.del("key"));
        assert_eq!(ks.get("key").unwrap(), None);
    }

    #[test]
    fn del_missing() {
        let mut ks = Keyspace::new();
        assert!(!ks.del("nope"));
    }

    #[test]
    fn exists_present_and_absent() {
        let mut ks = Keyspace::new();
        ks.set("yes".into(), Bytes::from("here"), None, false, false);
        assert!(ks.exists("yes"));
        assert!(!ks.exists("no"));
    }

    #[test]
    fn ttl_no_expiry() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);
        assert_eq!(ks.ttl("key"), TtlResult::NoExpiry);
    }

    #[test]
    fn ttl_not_found() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.ttl("missing"), TtlResult::NotFound);
    }

    #[test]
    fn ttl_with_expiry() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::from_secs(100)),
            false,
            false,
        );
        match ks.ttl("key") {
            TtlResult::Seconds(s) => assert!((98..=100).contains(&s)),
            other => panic!("expected Seconds, got {other:?}"),
        }
    }

    #[test]
    fn ttl_expired_key() {
        let mut ks = Keyspace::new();
        ks.set(
            "temp".into(),
            Bytes::from("val"),
            Some(Duration::from_millis(10)),
            false,
            false,
        );
        thread::sleep(Duration::from_millis(30));
        assert_eq!(ks.ttl("temp"), TtlResult::NotFound);
    }

    #[test]
    fn expire_existing_key() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);
        assert!(ks.expire("key", 60));
        match ks.ttl("key") {
            TtlResult::Seconds(s) => assert!((58..=60).contains(&s)),
            other => panic!("expected Seconds, got {other:?}"),
        }
    }

    #[test]
    fn expire_missing_key() {
        let mut ks = Keyspace::new();
        assert!(!ks.expire("nope", 60));
    }

    #[test]
    fn del_expired_key_returns_false() {
        let mut ks = Keyspace::new();
        ks.set(
            "temp".into(),
            Bytes::from("val"),
            Some(Duration::from_millis(10)),
            false,
            false,
        );
        thread::sleep(Duration::from_millis(30));
        // key is expired, del should return false (not found)
        assert!(!ks.del("temp"));
    }

    // -- memory tracking tests --

    #[test]
    fn memory_increases_on_set() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.stats().used_bytes, 0);
        ks.set("key".into(), Bytes::from("value"), None, false, false);
        assert!(ks.stats().used_bytes > 0);
        assert_eq!(ks.stats().key_count, 1);
    }

    #[test]
    fn memory_decreases_on_del() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("value"), None, false, false);
        let after_set = ks.stats().used_bytes;
        ks.del("key");
        assert_eq!(ks.stats().used_bytes, 0);
        assert!(after_set > 0);
    }

    #[test]
    fn memory_adjusts_on_overwrite() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("short"), None, false, false);
        let small = ks.stats().used_bytes;

        ks.set(
            "key".into(),
            Bytes::from("a much longer value"),
            None,
            false,
            false,
        );
        let large = ks.stats().used_bytes;

        assert!(large > small);
        assert_eq!(ks.stats().key_count, 1);
    }

    #[test]
    fn memory_decreases_on_expired_removal() {
        let mut ks = Keyspace::new();
        ks.set(
            "temp".into(),
            Bytes::from("data"),
            Some(Duration::from_millis(10)),
            false,
            false,
        );
        assert!(ks.stats().used_bytes > 0);
        thread::sleep(Duration::from_millis(30));
        // trigger lazy expiration
        let _ = ks.get("temp");
        assert_eq!(ks.stats().used_bytes, 0);
        assert_eq!(ks.stats().key_count, 0);
    }

    #[test]
    fn stats_tracks_expiry_count() {
        let mut ks = Keyspace::new();
        ks.set("a".into(), Bytes::from("1"), None, false, false);
        ks.set(
            "b".into(),
            Bytes::from("2"),
            Some(Duration::from_secs(100)),
            false,
            false,
        );
        ks.set(
            "c".into(),
            Bytes::from("3"),
            Some(Duration::from_secs(200)),
            false,
            false,
        );

        let stats = ks.stats();
        assert_eq!(stats.key_count, 3);
        assert_eq!(stats.keys_with_expiry, 2);
    }

    // -- eviction tests --

    #[test]
    fn noeviction_returns_oom_when_full() {
        // one entry with key "a" + value "val" = 1 + 3 + 96 = 100 bytes
        // set limit so one entry fits but two don't
        let config = ShardConfig {
            max_memory: Some(150),
            eviction_policy: EvictionPolicy::NoEviction,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        // first key should fit
        assert_eq!(
            ks.set("a".into(), Bytes::from("val"), None, false, false),
            SetResult::Ok
        );

        // second key should push us over the limit
        let result = ks.set("b".into(), Bytes::from("val"), None, false, false);
        assert_eq!(result, SetResult::OutOfMemory);

        // original key should still be there
        assert!(ks.exists("a"));
    }

    #[test]
    fn lru_eviction_makes_room() {
        let config = ShardConfig {
            max_memory: Some(150),
            eviction_policy: EvictionPolicy::AllKeysLru,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        assert_eq!(
            ks.set("a".into(), Bytes::from("val"), None, false, false),
            SetResult::Ok
        );

        // this should trigger eviction of "a" to make room
        assert_eq!(
            ks.set("b".into(), Bytes::from("val"), None, false, false),
            SetResult::Ok
        );

        // "a" should have been evicted
        assert!(!ks.exists("a"));
        assert!(ks.exists("b"));
    }

    #[test]
    fn safety_margin_rejects_near_raw_limit() {
        // one entry = 1 (key) + 3 (val) + 128 (overhead) = 132 bytes.
        // configure max_memory = 147. effective limit = 147 * 90 / 100 = 132.
        // the entry fills exactly the effective limit, so a second entry should
        // be rejected even though the raw limit has 15 bytes of headroom.
        let config = ShardConfig {
            max_memory: Some(147),
            eviction_policy: EvictionPolicy::NoEviction,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        assert_eq!(
            ks.set("a".into(), Bytes::from("val"), None, false, false),
            SetResult::Ok
        );

        let result = ks.set("b".into(), Bytes::from("val"), None, false, false);
        assert_eq!(result, SetResult::OutOfMemory);
    }

    #[test]
    fn overwrite_same_size_succeeds_at_limit() {
        let config = ShardConfig {
            max_memory: Some(150),
            eviction_policy: EvictionPolicy::NoEviction,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        assert_eq!(
            ks.set("a".into(), Bytes::from("val"), None, false, false),
            SetResult::Ok
        );

        // overwriting with same-size value should succeed — no net increase
        assert_eq!(
            ks.set("a".into(), Bytes::from("new"), None, false, false),
            SetResult::Ok
        );
        assert_eq!(
            ks.get("a").unwrap(),
            Some(Value::String(Bytes::from("new")))
        );
    }

    #[test]
    fn overwrite_larger_value_respects_limit() {
        let config = ShardConfig {
            max_memory: Some(150),
            eviction_policy: EvictionPolicy::NoEviction,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        assert_eq!(
            ks.set("a".into(), Bytes::from("val"), None, false, false),
            SetResult::Ok
        );

        // overwriting with a much larger value should fail if it exceeds limit
        let big_value = "x".repeat(200);
        let result = ks.set("a".into(), Bytes::from(big_value), None, false, false);
        assert_eq!(result, SetResult::OutOfMemory);

        // original value should still be intact
        assert_eq!(
            ks.get("a").unwrap(),
            Some(Value::String(Bytes::from("val")))
        );
    }

    // -- iter_entries tests --

    #[test]
    fn iter_entries_returns_live_entries() {
        let mut ks = Keyspace::new();
        ks.set("a".into(), Bytes::from("1"), None, false, false);
        ks.set(
            "b".into(),
            Bytes::from("2"),
            Some(Duration::from_secs(100)),
            false,
            false,
        );

        let entries: Vec<_> = ks.iter_entries().collect();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn iter_entries_skips_expired() {
        let mut ks = Keyspace::new();
        ks.set(
            "dead".into(),
            Bytes::from("gone"),
            Some(Duration::from_millis(1)),
            false,
            false,
        );
        ks.set("alive".into(), Bytes::from("here"), None, false, false);
        thread::sleep(Duration::from_millis(10));

        let entries: Vec<_> = ks.iter_entries().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "alive");
    }

    #[test]
    fn iter_entries_ttl_for_no_expiry() {
        let mut ks = Keyspace::new();
        ks.set("permanent".into(), Bytes::from("val"), None, false, false);

        let entries: Vec<_> = ks.iter_entries().collect();
        assert_eq!(entries[0].2, -1);
    }

    // -- restore tests --

    #[test]
    fn restore_adds_entry() {
        let mut ks = Keyspace::new();
        ks.restore("restored".into(), Value::String(Bytes::from("data")), None);
        assert_eq!(
            ks.get("restored").unwrap(),
            Some(Value::String(Bytes::from("data")))
        );
        assert_eq!(ks.stats().key_count, 1);
    }

    #[test]
    fn restore_with_zero_ttl_expires_immediately() {
        let mut ks = Keyspace::new();
        // TTL of 0 should create entry that expires immediately
        ks.restore(
            "short-lived".into(),
            Value::String(Bytes::from("data")),
            Some(Duration::from_millis(1)),
        );
        // Entry exists but will be expired on access
        std::thread::sleep(Duration::from_millis(5));
        assert!(ks.get("short-lived").is_err() || ks.get("short-lived").unwrap().is_none());
    }

    #[test]
    fn restore_overwrites_existing() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("old"), None, false, false);
        ks.restore("key".into(), Value::String(Bytes::from("new")), None);
        assert_eq!(
            ks.get("key").unwrap(),
            Some(Value::String(Bytes::from("new")))
        );
        assert_eq!(ks.stats().key_count, 1);
    }

    #[test]
    fn restore_bypasses_memory_limit() {
        let config = ShardConfig {
            max_memory: Some(50), // very small
            eviction_policy: EvictionPolicy::NoEviction,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        // normal set would fail due to memory limit
        ks.restore(
            "big".into(),
            Value::String(Bytes::from("x".repeat(200))),
            None,
        );
        assert_eq!(ks.stats().key_count, 1);
    }

    #[test]
    fn no_limit_never_rejects() {
        // default config has no memory limit
        let mut ks = Keyspace::new();
        for i in 0..100 {
            assert_eq!(
                ks.set(format!("key:{i}"), Bytes::from("value"), None, false, false),
                SetResult::Ok
            );
        }
        assert_eq!(ks.len(), 100);
    }

    #[test]
    fn clear_removes_all_keys() {
        let mut ks = Keyspace::new();
        ks.set("a".into(), Bytes::from("1"), None, false, false);
        ks.set(
            "b".into(),
            Bytes::from("2"),
            Some(Duration::from_secs(60)),
            false,
            false,
        );
        ks.lpush("list", &[Bytes::from("x")]).unwrap();

        assert_eq!(ks.len(), 3);
        assert!(ks.stats().used_bytes > 0);
        assert_eq!(ks.stats().keys_with_expiry, 1);

        ks.clear();

        assert_eq!(ks.len(), 0);
        assert!(ks.is_empty());
        assert_eq!(ks.stats().used_bytes, 0);
        assert_eq!(ks.stats().keys_with_expiry, 0);
    }

    // --- scan ---

    #[test]
    fn scan_returns_keys() {
        let mut ks = Keyspace::new();
        ks.set("key1".into(), Bytes::from("a"), None, false, false);
        ks.set("key2".into(), Bytes::from("b"), None, false, false);
        ks.set("key3".into(), Bytes::from("c"), None, false, false);

        let (cursor, keys) = ks.scan_keys(0, 10, None);
        assert_eq!(cursor, 0); // complete in one pass
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn scan_empty_keyspace() {
        let ks = Keyspace::new();
        let (cursor, keys) = ks.scan_keys(0, 10, None);
        assert_eq!(cursor, 0);
        assert!(keys.is_empty());
    }

    #[test]
    fn scan_with_pattern() {
        let mut ks = Keyspace::new();
        ks.set("user:1".into(), Bytes::from("a"), None, false, false);
        ks.set("user:2".into(), Bytes::from("b"), None, false, false);
        ks.set("item:1".into(), Bytes::from("c"), None, false, false);

        let (cursor, keys) = ks.scan_keys(0, 10, Some("user:*"));
        assert_eq!(cursor, 0);
        assert_eq!(keys.len(), 2);
        for k in &keys {
            assert!(k.starts_with("user:"));
        }
    }

    #[test]
    fn scan_with_count_limit() {
        let mut ks = Keyspace::new();
        for i in 0..10 {
            ks.set(format!("k{i}"), Bytes::from("v"), None, false, false);
        }

        // first batch
        let (cursor, keys) = ks.scan_keys(0, 3, None);
        assert!(!keys.is_empty());
        assert!(keys.len() <= 3);

        // if there are more keys, cursor should be non-zero
        if cursor != 0 {
            let (cursor2, keys2) = ks.scan_keys(cursor, 3, None);
            assert!(!keys2.is_empty());
            // continue until complete
            let _ = (cursor2, keys2);
        }
    }

    #[test]
    fn scan_skips_expired_keys() {
        let mut ks = Keyspace::new();
        ks.set("live".into(), Bytes::from("a"), None, false, false);
        ks.set(
            "expired".into(),
            Bytes::from("b"),
            Some(Duration::from_millis(1)),
            false,
            false,
        );

        std::thread::sleep(Duration::from_millis(5));

        let (_, keys) = ks.scan_keys(0, 10, None);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], "live");
    }

    #[test]
    fn glob_match_star() {
        assert!(super::glob_match("user:*", "user:123"));
        assert!(super::glob_match("user:*", "user:"));
        assert!(super::glob_match("*:data", "foo:data"));
        assert!(!super::glob_match("user:*", "item:123"));
    }

    #[test]
    fn glob_match_question() {
        assert!(super::glob_match("key?", "key1"));
        assert!(super::glob_match("key?", "keya"));
        assert!(!super::glob_match("key?", "key"));
        assert!(!super::glob_match("key?", "key12"));
    }

    #[test]
    fn glob_match_brackets() {
        assert!(super::glob_match("key[abc]", "keya"));
        assert!(super::glob_match("key[abc]", "keyb"));
        assert!(!super::glob_match("key[abc]", "keyd"));
    }

    #[test]
    fn glob_match_literal() {
        assert!(super::glob_match("exact", "exact"));
        assert!(!super::glob_match("exact", "exactnot"));
        assert!(!super::glob_match("exact", "notexact"));
    }

    // --- persist/pttl/pexpire ---

    #[test]
    fn persist_removes_expiry() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::from_secs(60)),
            false,
            false,
        );
        assert!(matches!(ks.ttl("key"), TtlResult::Seconds(_)));

        assert!(ks.persist("key"));
        assert_eq!(ks.ttl("key"), TtlResult::NoExpiry);
        assert_eq!(ks.stats().keys_with_expiry, 0);
    }

    #[test]
    fn persist_returns_false_without_expiry() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);
        assert!(!ks.persist("key"));
    }

    #[test]
    fn persist_returns_false_for_missing_key() {
        let mut ks = Keyspace::new();
        assert!(!ks.persist("missing"));
    }

    #[test]
    fn pttl_returns_milliseconds() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::from_secs(60)),
            false,
            false,
        );
        match ks.pttl("key") {
            TtlResult::Milliseconds(ms) => assert!(ms > 59_000 && ms <= 60_000),
            other => panic!("expected Milliseconds, got {other:?}"),
        }
    }

    #[test]
    fn pttl_no_expiry() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);
        assert_eq!(ks.pttl("key"), TtlResult::NoExpiry);
    }

    #[test]
    fn pttl_not_found() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.pttl("missing"), TtlResult::NotFound);
    }

    #[test]
    fn pexpire_sets_ttl_in_millis() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);
        assert!(ks.pexpire("key", 5000));
        match ks.pttl("key") {
            TtlResult::Milliseconds(ms) => assert!(ms > 4000 && ms <= 5000),
            other => panic!("expected Milliseconds, got {other:?}"),
        }
        assert_eq!(ks.stats().keys_with_expiry, 1);
    }

    #[test]
    fn pexpire_missing_key_returns_false() {
        let mut ks = Keyspace::new();
        assert!(!ks.pexpire("missing", 5000));
    }

    #[test]
    fn pexpire_overwrites_existing_ttl() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::from_secs(60)),
            false,
            false,
        );
        assert!(ks.pexpire("key", 500));
        match ks.pttl("key") {
            TtlResult::Milliseconds(ms) => assert!(ms <= 500),
            other => panic!("expected Milliseconds, got {other:?}"),
        }
        // expiry count shouldn't double-count
        assert_eq!(ks.stats().keys_with_expiry, 1);
    }

    // --- keys tests ---

    #[test]
    fn keys_match_all() {
        let mut ks = Keyspace::new();
        ks.set("a".into(), Bytes::from("1"), None, false, false);
        ks.set("b".into(), Bytes::from("2"), None, false, false);
        ks.set("c".into(), Bytes::from("3"), None, false, false);
        let mut result = ks.keys("*");
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn keys_with_pattern() {
        let mut ks = Keyspace::new();
        ks.set("user:1".into(), Bytes::from("a"), None, false, false);
        ks.set("user:2".into(), Bytes::from("b"), None, false, false);
        ks.set("item:1".into(), Bytes::from("c"), None, false, false);
        let mut result = ks.keys("user:*");
        result.sort();
        assert_eq!(result, vec!["user:1", "user:2"]);
    }

    #[test]
    fn keys_skips_expired() {
        let mut ks = Keyspace::new();
        ks.set("live".into(), Bytes::from("a"), None, false, false);
        ks.set(
            "dead".into(),
            Bytes::from("b"),
            Some(Duration::from_millis(1)),
            false,
            false,
        );
        thread::sleep(Duration::from_millis(5));
        let result = ks.keys("*");
        assert_eq!(result, vec!["live"]);
    }

    #[test]
    fn keys_empty_keyspace() {
        let ks = Keyspace::new();
        assert!(ks.keys("*").is_empty());
    }

    // --- rename tests ---

    #[test]
    fn rename_basic() {
        let mut ks = Keyspace::new();
        ks.set("old".into(), Bytes::from("value"), None, false, false);
        ks.rename("old", "new").unwrap();
        assert!(!ks.exists("old"));
        assert_eq!(
            ks.get("new").unwrap(),
            Some(Value::String(Bytes::from("value")))
        );
    }

    #[test]
    fn rename_preserves_expiry() {
        let mut ks = Keyspace::new();
        ks.set(
            "old".into(),
            Bytes::from("val"),
            Some(Duration::from_secs(60)),
            false,
            false,
        );
        ks.rename("old", "new").unwrap();
        match ks.ttl("new") {
            TtlResult::Seconds(s) => assert!((58..=60).contains(&s)),
            other => panic!("expected TTL preserved, got {other:?}"),
        }
    }

    #[test]
    fn rename_overwrites_destination() {
        let mut ks = Keyspace::new();
        ks.set("src".into(), Bytes::from("new_val"), None, false, false);
        ks.set("dst".into(), Bytes::from("old_val"), None, false, false);
        ks.rename("src", "dst").unwrap();
        assert!(!ks.exists("src"));
        assert_eq!(
            ks.get("dst").unwrap(),
            Some(Value::String(Bytes::from("new_val")))
        );
        assert_eq!(ks.len(), 1);
    }

    #[test]
    fn rename_missing_key_returns_error() {
        let mut ks = Keyspace::new();
        let err = ks.rename("missing", "new").unwrap_err();
        assert_eq!(err, RenameError::NoSuchKey);
    }

    #[test]
    fn rename_same_key() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);
        // renaming to itself should succeed (Redis behavior)
        ks.rename("key", "key").unwrap();
        assert_eq!(
            ks.get("key").unwrap(),
            Some(Value::String(Bytes::from("val")))
        );
    }

    #[test]
    fn rename_tracks_memory() {
        let mut ks = Keyspace::new();
        ks.set("old".into(), Bytes::from("value"), None, false, false);
        let before = ks.stats().used_bytes;
        ks.rename("old", "new").unwrap();
        let after = ks.stats().used_bytes;
        // same key length, so memory should be the same
        assert_eq!(before, after);
        assert_eq!(ks.stats().key_count, 1);
    }

    #[test]
    fn zero_ttl_expires_immediately() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::ZERO),
            false,
            false,
        );

        // key should be expired immediately
        std::thread::sleep(Duration::from_millis(1));
        assert!(ks.get("key").unwrap().is_none());
    }

    #[test]
    fn very_small_ttl_expires_quickly() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::from_millis(1)),
            false,
            false,
        );

        std::thread::sleep(Duration::from_millis(5));
        assert!(ks.get("key").unwrap().is_none());
    }

    #[test]
    fn count_keys_in_slot_empty() {
        let ks = Keyspace::new();
        assert_eq!(ks.count_keys_in_slot(0), 0);
    }

    #[test]
    fn count_keys_in_slot_matches() {
        let mut ks = Keyspace::new();
        // insert a few keys and count those in a specific slot
        ks.set("a".into(), Bytes::from("1"), None, false, false);
        ks.set("b".into(), Bytes::from("2"), None, false, false);
        ks.set("c".into(), Bytes::from("3"), None, false, false);

        let slot_a = ember_cluster::key_slot(b"a");
        let count = ks.count_keys_in_slot(slot_a);
        // at minimum, "a" should be in its own slot
        assert!(count >= 1);
    }

    #[test]
    fn count_keys_in_slot_skips_expired() {
        let mut ks = Keyspace::new();
        let slot = ember_cluster::key_slot(b"temp");
        ks.set(
            "temp".into(),
            Bytes::from("gone"),
            Some(Duration::from_millis(0)),
            false,
            false,
        );
        // key is expired — should not be counted
        thread::sleep(Duration::from_millis(5));
        assert_eq!(ks.count_keys_in_slot(slot), 0);
    }

    #[test]
    fn get_keys_in_slot_returns_matching() {
        let mut ks = Keyspace::new();
        ks.set("x".into(), Bytes::from("1"), None, false, false);
        ks.set("y".into(), Bytes::from("2"), None, false, false);

        let slot_x = ember_cluster::key_slot(b"x");
        let keys = ks.get_keys_in_slot(slot_x, 100);
        assert!(keys.contains(&"x".to_string()));
    }

    #[test]
    fn get_keys_in_slot_respects_count_limit() {
        let mut ks = Keyspace::new();
        // insert several keys — some might share a slot
        for i in 0..100 {
            ks.set(format!("key:{i}"), Bytes::from("v"), None, false, false);
        }
        // ask for at most 3 keys from slot 0
        let keys = ks.get_keys_in_slot(0, 3);
        assert!(keys.len() <= 3);
    }
}
