//! The keyspace: Ember's core key-value store.
//!
//! A `Keyspace` owns a flat `EntryMap` (an `IndexMap` of keys to entries) and handles
//! get, set, delete, existence checks, and TTL management. Expired
//! keys are removed lazily on access. Memory usage is tracked on
//! every mutation for eviction and stats reporting.

use std::time::Duration;

use ahash::AHashMap;
use bytes::Bytes;
use compact_str::CompactString;
use rand::Rng;

use tracing::warn;

use crate::dropper::DropHandle;
use crate::glob::glob_match;
use crate::memory::{self, MemoryTracker};
use crate::time;
use crate::types::list::ListValue;
use crate::types::sorted_set::{ScoreBound, SortedSet, ZAddFlags};
use crate::types::{self, normalize_range, Value};

mod bitmap;
mod hash;
mod keys;
mod list;
#[cfg(feature = "protobuf")]
mod proto;
#[cfg(feature = "protobuf")]
pub use proto::ProtoFindOpts;
mod scan;
mod set;
mod string;
mod ttl;
#[cfg(feature = "vector")]
mod vector;
mod zset;

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

impl From<WriteError> for IncrError {
    fn from(e: WriteError) -> Self {
        match e {
            WriteError::WrongType => IncrError::WrongType,
            WriteError::OutOfMemory => IncrError::OutOfMemory,
        }
    }
}

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

impl From<WriteError> for IncrFloatError {
    fn from(e: WriteError) -> Self {
        match e {
            WriteError::WrongType => IncrFloatError::WrongType,
            WriteError::OutOfMemory => IncrFloatError::OutOfMemory,
        }
    }
}

/// Error returned when RENAME fails because the source key doesn't exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenameError {
    /// The source key does not exist.
    NoSuchKey,
}

/// Error returned by COPY.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyError {
    /// The source key does not exist.
    NoSuchKey,
    /// Memory limit reached and eviction couldn't free enough space.
    OutOfMemory,
}

/// Error returned by LSET.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LsetError {
    /// Key holds a different type.
    WrongType,
    /// Key does not exist.
    NoSuchKey,
    /// Index is beyond list bounds.
    IndexOutOfRange,
}

impl std::fmt::Display for LsetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LsetError::WrongType => write!(f, "{WRONGTYPE_MSG}"),
            LsetError::NoSuchKey => write!(f, "ERR no such key"),
            LsetError::IndexOutOfRange => write!(f, "ERR index out of range"),
        }
    }
}

impl std::error::Error for LsetError {}

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
    /// How often the shard samples keys for active expiration. 100ms
    /// matches Redis's hz=10 default.
    pub expiry_interval: Duration,
    /// How often the AOF is fsynced under the `EverySec` policy.
    pub fsync_interval: Duration,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            max_memory: None,
            eviction_policy: EvictionPolicy::NoEviction,
            shard_id: 0,
            expiry_interval: Duration::from_millis(100),
            fsync_interval: Duration::from_secs(1),
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

/// The key-to-entry map a shard keeps.
///
/// An `IndexMap` stores entries in a dense vector, so a random key or a
/// cursor position is an O(1) index. SCAN, eviction and active expiry rely
/// on that. Remove entries only with `swap_remove`: `shift_remove` is O(n).
pub(crate) type EntryMap = indexmap::IndexMap<CompactString, Entry, ahash::RandomState>;

/// The keys that have a TTL. Active expiry samples from this set, so it
/// finds expired keys even when most keys have no TTL. It is an `IndexSet`
/// for the same O(1) random picks as [`EntryMap`].
type ExpirySet = indexmap::IndexSet<CompactString, ahash::RandomState>;

/// A single entry in the keyspace: a value plus optional expiration
/// and last access time for LRU approximation.
///
/// Field order is chosen for cache-line packing: `value` and
/// `expires_at_ms` (the hot-path read fields) sit at the front so
/// they share the first L1 cache line with the HashMap key pointer.
/// `last_access_secs` is cold (only used during eviction sampling).
///
/// Version tracking for WATCH/EXEC lives in a separate side table on
/// Keyspace (`versions` map), not on every entry. This saves 8 bytes
/// per entry since <1% of keys are ever WATCHed.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) value: Value,
    /// Monotonic expiry timestamp in ms. 0 = no expiry.
    pub(crate) expires_at_ms: u64,
    /// Monotonic last access time in seconds since process start (for LRU).
    /// Using u32 saves 4 bytes per entry; wraps at ~136 years.
    pub(crate) last_access_secs: u32,
}

impl Entry {
    pub(crate) fn new(value: Value, ttl: Option<Duration>) -> Self {
        Self {
            value,
            expires_at_ms: time::expiry_from_duration(ttl),
            last_access_secs: time::now_secs(),
        }
    }

    /// Returns `true` if this entry has passed its expiration time.
    fn is_expired(&self) -> bool {
        time::is_expired(self.expires_at_ms)
    }

    /// Marks this entry as accessed right now. When `track` is false
    /// (NoEviction policy), this is a no-op — skipping the `now_secs()`
    /// call on every access.
    #[inline(always)]
    fn touch(&mut self, track: bool) {
        if track {
            self.last_access_secs = time::now_secs();
        }
    }

    /// Returns the estimated memory footprint of this entry: key, value
    /// and overhead. Every value type counts its own size, so this is O(1).
    fn entry_size(&self, key: &str) -> usize {
        key.len() + self.value_size() + memory::ENTRY_OVERHEAD
    }

    /// Returns the estimated size of the value.
    pub(crate) fn value_size(&self) -> usize {
        memory::value_size(&self.value)
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
    /// Cumulative count of write commands rejected due to memory limits.
    pub oom_rejections: u64,
    /// Cumulative count of successful key lookups.
    pub keyspace_hits: u64,
    /// Cumulative count of key lookups that found no key (expired or absent).
    pub keyspace_misses: u64,
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
    entries: EntryMap,
    memory: MemoryTracker,
    config: ShardConfig,
    /// Keys that currently have an expiration set.
    expiring: ExpirySet,
    /// Cumulative count of keys removed by expiration (lazy + active).
    expired_total: u64,
    /// Cumulative count of keys removed by eviction.
    evicted_total: u64,
    /// Keys evicted since the shard last called [`Keyspace::take_evicted`].
    evicted: Vec<String>,
    /// Cumulative count of write rejections due to memory limits.
    oom_rejections: u64,
    /// Cumulative count of successful key lookups.
    keyspace_hits: u64,
    /// Cumulative count of key lookups that found no key (expired or absent).
    keyspace_misses: u64,
    /// When set, large values are dropped on a background thread instead
    /// of inline on the shard thread. See [`crate::dropper`].
    drop_handle: Option<DropHandle>,
    /// Monotonic counter for entry versions. Each mutation gets the next
    /// value, giving WATCH a cheap way to detect concurrent writes.
    next_version: u64,
    /// Side table for WATCH/EXEC version tracking. Only populated for
    /// keys that have been WATCHed. On mutation, we bump the version
    /// only if the key exists in this map — which is almost never,
    /// so the hot path pays only a fast hash-miss lookup.
    versions: AHashMap<CompactString, u64>,
    /// Whether to update `last_access_secs` on every access. Only useful
    /// when the eviction policy needs LRU timestamps (AllKeysLru).
    /// When false (NoEviction), we skip the `now_secs()` call on every
    /// read/write — saving a syscall per operation on the hot path.
    track_access: bool,
}

impl Keyspace {
    /// Creates a new, empty keyspace with default config (no memory limit).
    pub fn new() -> Self {
        Self::with_config(ShardConfig::default())
    }

    /// Creates a new, empty keyspace with the given config.
    pub fn with_config(config: ShardConfig) -> Self {
        let track_access = config.eviction_policy == EvictionPolicy::AllKeysLru;
        Self {
            entries: EntryMap::default(),
            memory: MemoryTracker::new(),
            config,
            expiring: ExpirySet::default(),
            expired_total: 0,
            evicted_total: 0,
            evicted: Vec::new(),
            oom_rejections: 0,
            keyspace_hits: 0,
            keyspace_misses: 0,
            drop_handle: None,
            next_version: 0,
            versions: AHashMap::new(),
            track_access,
        }
    }

    /// Attaches a background drop handle for lazy free. When set, large
    /// values removed by del/eviction/expiration are dropped on a
    /// background thread instead of blocking the shard.
    pub fn set_drop_handle(&mut self, handle: DropHandle) {
        self.drop_handle = Some(handle);
    }

    /// Bumps the version for a key in the side table, but only if the
    /// key is being tracked (i.e. someone WATCHed it). When no keys
    /// are watched, the versions map is empty and this is a fast miss.
    fn bump_version(&mut self, key: &str) {
        if let Some(ver) = self.versions.get_mut(key) {
            self.next_version += 1;
            *ver = self.next_version;
        }
    }

    /// Returns the current version of a key, or `None` if the key is
    /// missing or expired. Used by WATCH/EXEC — cold path only.
    ///
    /// If the key hasn't been WATCHed yet, inserts the current
    /// `next_version` so that future mutations will be detected.
    pub fn key_version(&mut self, key: &str) -> Option<u64> {
        let entry = self.entries.get(key)?;
        if entry.is_expired() {
            return None;
        }
        let ver = *self
            .versions
            .entry(CompactString::from(key))
            .or_insert(self.next_version);
        Some(ver)
    }

    /// Removes version tracking for a key. Called on key deletion,
    /// expiration, and eviction so the side table doesn't leak.
    fn remove_version(&mut self, key: &str) {
        self.versions.remove(key);
    }

    /// Removes all version tracking entries. Called on UNWATCH/DISCARD
    /// or FLUSHDB.
    pub fn clear_versions(&mut self) {
        self.versions.clear();
    }

    /// Stops tracking the TTL of `key`, whose `entry` was just removed.
    fn untrack_expiry(&mut self, key: &str, entry: &Entry) {
        self.track_expiry(key, entry.expires_at_ms != 0, false);
    }

    /// Runs `f` on the collection at `key` and keeps the bookkeeping
    /// right: memory use, the access time, the WATCH version, and removing
    /// the key once the collection is empty. `pick` selects the collection
    /// type. `f` returns its result and whether it changed the collection.
    ///
    /// Returns `Ok(None)` when the key doesn't exist and `Err(WrongType)`
    /// when it holds another type.
    pub(crate) fn edit<C: ?Sized, T>(
        &mut self,
        key: &str,
        pick: fn(&mut Value) -> Option<&mut C>,
        f: impl FnOnce(&mut C) -> (T, bool),
    ) -> Result<Option<T>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }
        let track_access = self.track_access;
        let Some(entry) = self.entries.get_mut(key) else {
            return Ok(None);
        };
        let old_size = entry.entry_size(key);
        let collection = pick(&mut entry.value).ok_or(WrongType)?;
        let (result, changed) = f(collection);
        entry.touch(track_access);

        if entry.value.is_empty_collection() {
            if let Some(removed) = self.entries.swap_remove(key) {
                self.untrack_expiry(key, &removed);
            }
            self.memory.remove_with_size(old_size);
        } else if changed {
            let new_size = entry.entry_size(key);
            self.memory.adjust(old_size, new_size);
        }
        if changed {
            self.bump_version(key);
        }
        Ok(Some(result))
    }

    /// Like [`edit`](Self::edit), but creates the key with `empty()` when
    /// it doesn't exist. `increase` is the most the write can add to memory
    /// use; the write fails with `OutOfMemory` when the limit leaves no
    /// room for it, even after eviction.
    pub(crate) fn edit_or_create<C: ?Sized, T>(
        &mut self,
        key: &str,
        pick: fn(&mut Value) -> Option<&mut C>,
        empty: fn() -> Value,
        increase: usize,
        f: impl FnOnce(&mut C) -> (T, bool),
    ) -> Result<T, WriteError> {
        self.remove_if_expired(key);
        let is_new = match self.entries.get_mut(key) {
            None => true,
            Some(entry) => {
                pick(&mut entry.value).ok_or(WriteError::WrongType)?;
                false
            }
        };
        let new_value = is_new.then(empty);
        let increase = increase + new_value.as_ref().map_or(0, |v| memory::entry_size(key, v));
        if !self.enforce_memory_limit(key, increase) {
            return Err(WriteError::OutOfMemory);
        }
        if let Some(value) = new_value {
            self.insert_empty(key, value);
        }
        // the key was checked or created above, and eviction skips it, so
        // the edit always finds it
        match self.edit(key, pick, f) {
            Ok(Some(result)) => Ok(result),
            Ok(None) | Err(WrongType) => Err(WriteError::WrongType),
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
        if self.enforce_memory_limit(key, estimated_increase) {
            Ok(())
        } else {
            Err(WriteError::OutOfMemory)
        }
    }

    /// Inserts a new key with an empty collection value. Used by
    /// collection-write methods after type-checking and memory reservation.
    fn insert_empty(&mut self, key: &str, value: Value) {
        self.memory.add(key, &value);
        let entry = Entry::new(value, None);
        self.entries.insert(CompactString::from(key), entry);
        self.bump_version(key);
    }

    /// Updates the set of keys with a TTL when `key` goes from having one
    /// or not (`had_expiry`) to having one or not (`has_expiry`).
    fn track_expiry(&mut self, key: &str, had_expiry: bool, has_expiry: bool) {
        match (had_expiry, has_expiry) {
            (false, true) => {
                self.expiring.insert(key.into());
            }
            (true, false) => {
                self.expiring.swap_remove(key);
            }
            _ => {}
        }
    }

    /// Tries to evict one key using an approximate LRU.
    ///
    /// Picks `EVICTION_SAMPLE_SIZE` random entries (every entry when there
    /// are fewer) and removes the one with the oldest `last_access` time.
    /// `protect` is never chosen, because the caller is about to write to
    /// it. Returns `false` if no other key exists.
    fn try_evict(&mut self, protect: &str) -> bool {
        let len = self.entries.len();
        let victim = if len <= EVICTION_SAMPLE_SIZE {
            self.least_recently_used(0..len, protect)
        } else {
            let mut rng = rand::rng();
            let samples = (0..EVICTION_SAMPLE_SIZE).map(|_| rng.random_range(0..len));
            self.least_recently_used(samples, protect)
        };
        let Some((key, entry)) = victim.and_then(|i| self.entries.swap_remove_index(i)) else {
            return false;
        };
        self.memory.remove(&key, &entry.value);
        self.untrack_expiry(&key, &entry);
        self.evicted_total += 1;
        self.remove_version(&key);
        self.defer_drop(entry.value);
        self.evicted.push(key.into());
        true
    }

    /// Returns the keys evicted since the last call. The shard logs them as
    /// deletes so the AOF and replicas drop them too.
    pub(crate) fn take_evicted(&mut self) -> Vec<String> {
        std::mem::take(&mut self.evicted)
    }

    /// Returns the position, among `positions`, of the entry accessed least
    /// recently, skipping `protect`.
    fn least_recently_used(
        &self,
        positions: impl Iterator<Item = usize>,
        protect: &str,
    ) -> Option<usize> {
        positions
            .filter(|&i| {
                self.entries
                    .get_index(i)
                    .is_some_and(|(key, _)| key.as_str() != protect)
            })
            .min_by_key(|&i| self.entries[i].last_access_secs)
    }

    /// Checks whether the memory limit allows a write that would increase
    /// usage by `estimated_increase` bytes. Attempts eviction if the
    /// policy allows it. Returns `true` if the write can proceed.
    ///
    /// `protect` is the key being written. Eviction skips it, so the caller
    /// can rely on the entry it already checked still being there.
    ///
    /// The comparison uses [`memory::effective_limit`] rather than the raw
    /// configured maximum. This reserves headroom for allocator overhead
    /// and fragmentation that our per-entry estimates can't account for,
    /// preventing the OS from OOM-killing us before eviction triggers.
    fn enforce_memory_limit(&mut self, protect: &str, estimated_increase: usize) -> bool {
        if let Some(max) = self.config.max_memory {
            let limit = memory::effective_limit(max);
            while self.memory.used_bytes() + estimated_increase > limit {
                match self.config.eviction_policy {
                    EvictionPolicy::NoEviction => {
                        self.oom_rejections += 1;
                        // log first rejection, then every 1000th to avoid flooding
                        if self.oom_rejections == 1 || self.oom_rejections.is_multiple_of(1000) {
                            warn!(
                                used_bytes = self.memory.used_bytes(),
                                limit,
                                requested = estimated_increase,
                                total_rejections = self.oom_rejections,
                                "OOM: write rejected (policy: noeviction)"
                            );
                        }
                        return false;
                    }
                    EvictionPolicy::AllKeysLru => {
                        if !self.try_evict(protect) {
                            self.oom_rejections += 1;
                            if self.oom_rejections == 1 || self.oom_rejections.is_multiple_of(1000)
                            {
                                warn!(
                                    used_bytes = self.memory.used_bytes(),
                                    limit,
                                    requested = estimated_increase,
                                    total_rejections = self.oom_rejections,
                                    "OOM: write rejected (eviction exhausted)"
                                );
                            }
                            return false;
                        }
                    }
                }
            }
        }
        true
    }

    /// Replaces the entries map with an empty one and resets memory
    /// tracking. Returns the old entries so the caller can send them
    /// to the background drop thread.
    pub(crate) fn flush_async(&mut self) -> EntryMap {
        let old = std::mem::take(&mut self.entries);
        self.memory.reset();
        self.expiring.clear();
        self.versions.clear();
        old
    }

    /// Returns the memory limit and eviction policy.
    pub fn memory_config(&self) -> (Option<usize>, EvictionPolicy) {
        (self.config.max_memory, self.config.eviction_policy)
    }

    /// Updates the memory limit and eviction policy in-place.
    ///
    /// Takes effect immediately for all subsequent write commands.
    /// `track_access` is synchronized with the new policy so LRU
    /// sampling stays consistent.
    pub fn update_memory_config(
        &mut self,
        max_memory: Option<usize>,
        eviction_policy: EvictionPolicy,
    ) {
        self.config.max_memory = max_memory;
        self.config.eviction_policy = eviction_policy;
        self.track_access = matches!(eviction_policy, EvictionPolicy::AllKeysLru);
    }

    /// Returns aggregated stats for this keyspace.
    ///
    /// All fields are tracked incrementally — this is O(1).
    pub fn stats(&self) -> KeyspaceStats {
        KeyspaceStats {
            key_count: self.memory.key_count(),
            used_bytes: self.memory.used_bytes(),
            keys_with_expiry: self.expiring.len(),
            keys_expired: self.expired_total,
            keys_evicted: self.evicted_total,
            oom_rejections: self.oom_rejections,
            keyspace_hits: self.keyspace_hits,
            keyspace_misses: self.keyspace_misses,
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
        self.expiring.clear();
        self.versions.clear();
    }

    /// Returns `true` if the keyspace has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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

    /// Samples up to `count` random keys that have a TTL and removes any
    /// that have expired.
    ///
    /// Expired key names are appended to `out` so the caller can emit
    /// keyspace notifications. Returns the number of keys removed.
    pub(crate) fn expire_sample(&mut self, count: usize, out: &mut Vec<String>) -> usize {
        let len = self.expiring.len();
        if len == 0 {
            return 0;
        }
        // distinct positions, so a pass over a small set sees every key
        let positions = rand::seq::index::sample(&mut rand::rng(), len, count.min(len));
        let expired: Vec<String> = positions
            .into_iter()
            .filter_map(|i| {
                let key = self.expiring.get_index(i)?;
                let entry = self.entries.get(key)?;
                entry.is_expired().then(|| key.to_string())
            })
            .collect();

        let mut removed = 0;
        for key in expired {
            if self.remove_if_expired(&key) {
                out.push(key);
                removed += 1;
            }
        }
        removed
    }

    /// Removes a key that is already known to be expired. Used by
    /// fused lookup paths that check expiry inline via `get_mut()` and
    /// need a second probe only on the rare expired path.
    fn remove_expired_entry(&mut self, key: &str) {
        if let Some(entry) = self.entries.swap_remove(key) {
            self.memory.remove(key, &entry.value);
            self.untrack_expiry(key, &entry);
            self.expired_total += 1;
            self.remove_version(key);
            self.defer_drop(entry.value);
        }
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
            self.remove_expired_entry(key);
        }
        expired
    }

    /// Returns a mutable reference to the entry for `key`, or `None` if the
    /// key doesn't exist or has expired.
    ///
    /// Combines the three steps that almost every read operation repeats:
    /// 1. Remove the key if it has expired (lazy expiration).
    /// 2. Look up the entry in the map.
    /// 3. Touch the entry to update its last-access timestamp for LRU.
    ///
    /// Callers still need to match on the entry's value type and handle
    /// `WrongType` themselves — this helper just eliminates the common
    /// expiry + lookup + touch boilerplate.
    fn get_live_entry(&mut self, key: &str) -> Option<&mut Entry> {
        self.remove_if_expired(key);
        let entry = self.entries.get_mut(key)?;
        entry.touch(self.track_access);
        Some(entry)
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

#[cfg(test)]
mod tests;
