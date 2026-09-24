//! The keyspace: Ember's core key-value store.
//!
//! A `Keyspace` owns a flat `EntryMap` (an `IndexMap` of keys to entries) and handles
//! get, set, delete, existence checks, and TTL management. Expired
//! keys are removed lazily on access. Memory usage is tracked on
//! every mutation for eviction and stats reporting.

use std::collections::VecDeque;
use std::time::Duration;

use ahash::AHashMap;
use bytes::Bytes;
use compact_str::CompactString;
use rand::seq::IteratorRandom;
use rand::Rng;

use tracing::warn;

use crate::dropper::DropHandle;
use crate::glob::glob_match;
use crate::memory::{self, MemoryTracker};
use crate::time;
use crate::types::sorted_set::{ScoreBound, SortedSet, ZAddFlags};
use crate::types::{self, normalize_range, Value};

mod bitmap;
mod hash;
mod list;
#[cfg(feature = "protobuf")]
mod proto;
#[cfg(feature = "protobuf")]
pub use proto::ProtoFindOpts;
mod set;
mod string;
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
/// `cached_value_size` is warm (used on writes). `last_access_secs`
/// is cold (only used during eviction sampling).
///
/// Version tracking for WATCH/EXEC lives in a separate side table on
/// Keyspace (`versions` map), not on every entry. This saves 8 bytes
/// per entry since <1% of keys are ever WATCHed.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) value: Value,
    /// Monotonic expiry timestamp in ms. 0 = no expiry.
    pub(crate) expires_at_ms: u64,
    /// Cached result of `memory::value_size(&self.value)`, so memory
    /// accounting is O(1) instead of walking whole collections. A u32
    /// saves 4 bytes per entry. Collections can outgrow it, so
    /// `SIZE_NOT_CACHED` marks a size read from the value instead. Access
    /// it only through `value_size` and the setters below.
    cached_value_size: u32,
    /// Monotonic last access time in seconds since process start (for LRU).
    /// Using u32 saves 4 bytes per entry; wraps at ~136 years.
    pub(crate) last_access_secs: u32,
}

impl Entry {
    pub(crate) fn new(value: Value, ttl: Option<Duration>) -> Self {
        let cached_value_size = cache_size(memory::value_size(&value));
        Self {
            value,
            expires_at_ms: time::expiry_from_duration(ttl),
            cached_value_size,
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

    /// Returns the full estimated memory footprint of this entry
    /// (key + value + overhead) using the cached value size.
    fn entry_size(&self, key: &str) -> usize {
        key.len() + self.value_size() + memory::ENTRY_OVERHEAD
    }

    /// Returns the estimated size of the value.
    pub(crate) fn value_size(&self) -> usize {
        match self.cached_value_size {
            SIZE_NOT_CACHED => memory::value_size(&self.value),
            size => size as usize,
        }
    }

    pub(crate) fn set_value_size(&mut self, size: usize) {
        self.cached_value_size = cache_size(size);
    }

    /// Adds `bytes` to the cached size. A size that is not cached is read
    /// from the value, which already includes the change.
    pub(crate) fn grow_value_size(&mut self, bytes: usize) {
        if self.cached_value_size != SIZE_NOT_CACHED {
            self.set_value_size(self.cached_value_size as usize + bytes);
        }
    }

    /// Subtracts `bytes` from the cached size, stopping at zero.
    pub(crate) fn shrink_value_size(&mut self, bytes: usize) {
        if self.cached_value_size != SIZE_NOT_CACHED {
            self.set_value_size((self.cached_value_size as usize).saturating_sub(bytes));
        }
    }
}

/// Marks a value size too large for `Entry::cached_value_size`.
const SIZE_NOT_CACHED: u32 = u32::MAX;

fn cache_size(size: usize) -> u32 {
    u32::try_from(size)
        .ok()
        .filter(|&s| s != SIZE_NOT_CACHED)
        .unwrap_or(SIZE_NOT_CACHED)
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

    /// Cleans up after removing an element from a collection (list, sorted
    /// set, hash, or set). If the collection is now empty, removes the key
    /// entirely and subtracts `old_size` from the memory tracker. Otherwise
    /// subtracts `removed_bytes` (the byte cost of the removed element(s))
    /// without rescanning the remaining collection, and updates the cached
    /// value size.
    fn cleanup_after_remove(
        &mut self,
        key: &str,
        old_size: usize,
        is_empty: bool,
        removed_bytes: usize,
    ) {
        if is_empty {
            if let Some(removed) = self.entries.swap_remove(key) {
                self.untrack_expiry(key, &removed);
            }
            self.memory.remove_with_size(old_size);
        } else {
            self.memory.shrink_by(removed_bytes);
            if let Some(entry) = self.entries.get_mut(key) {
                entry.shrink_value_size(removed_bytes);
            }
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

    /// Measures entry size before and after a mutation, adjusting the
    /// memory tracker for the difference.
    ///
    /// Uses `cached_value_size` for the pre-mutation size (O(1)) and
    /// recomputes after the mutation to update the cache. This halves
    /// the cost of memory tracking for large collections.
    fn track_size<T>(&mut self, key: &str, f: impl FnOnce(&mut Entry) -> T) -> Option<T> {
        let entry = self.entries.get_mut(key)?;
        let old_size = entry.entry_size(key);
        let result = f(entry);
        // re-lookup after mutation (f consumed the borrow)
        let entry = self.entries.get_mut(key)?;
        let new_value_size = memory::value_size(&entry.value);
        entry.set_value_size(new_value_size);
        let new_size = key.len() + new_value_size + memory::ENTRY_OVERHEAD;
        self.memory.adjust(old_size, new_size);
        self.bump_version(key);
        Some(result)
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
        true
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

    /// Removes a key. Returns `true` if the key existed (and wasn't expired).
    ///
    /// When a drop handle is set, large values are dropped on the
    /// background thread instead of inline.
    pub fn del(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        if let Some(entry) = self.entries.swap_remove(key) {
            self.memory.remove(key, &entry.value);
            self.untrack_expiry(key, &entry);
            self.remove_version(key);
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
        if let Some(entry) = self.entries.swap_remove(key) {
            self.memory.remove(key, &entry.value);
            self.untrack_expiry(key, &entry);
            self.remove_version(key);
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
    pub(crate) fn flush_async(&mut self) -> EntryMap {
        let old = std::mem::take(&mut self.entries);
        self.memory.reset();
        self.expiring.clear();
        self.versions.clear();
        old
    }

    /// Returns `true` if the key exists and hasn't expired.
    pub fn exists(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        self.entries.contains_key(key)
    }

    /// Returns a random key from the keyspace, or `None` if empty.
    ///
    /// Expired keys that come up are removed, with a bounded number of
    /// retries.
    pub fn random_key(&mut self) -> Option<String> {
        let mut rng = rand::rng();
        for _ in 0..5 {
            let len = self.entries.len();
            if len == 0 {
                return None;
            }
            let key = self.entries.get_index(rng.random_range(0..len))?.0.clone();
            if self.remove_if_expired(&key) {
                continue;
            }
            return Some(key.to_string());
        }
        None
    }

    /// Updates the last access time for a key. Returns `true` if the key exists.
    pub fn touch(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                entry.touch(self.track_access);
                true
            }
            None => false,
        }
    }

    /// Sorts elements from a list, set, or sorted set.
    ///
    /// Returns the sorted elements as byte strings, or an error if the
    /// key holds the wrong type or numeric parsing fails.
    pub fn sort(
        &mut self,
        key: &str,
        desc: bool,
        alpha: bool,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<Bytes>, &'static str> {
        if self.remove_if_expired(key) {
            return Ok(Vec::new());
        }
        let entry = match self.entries.get_mut(key) {
            Some(e) => {
                e.touch(self.track_access);
                e
            }
            None => return Ok(Vec::new()),
        };

        // collect elements from the appropriate type
        let mut items: Vec<Bytes> = match &entry.value {
            Value::List(deq) => deq.iter().cloned().collect(),
            Value::Set(set) => set.iter().map(|s| Bytes::from(s.clone())).collect(),
            Value::SortedSet(zset) => zset
                .iter()
                .map(|(m, _)| Bytes::from(m.to_owned()))
                .collect(),
            _ => return Err(WRONGTYPE_MSG),
        };

        // sort
        if alpha {
            items.sort();
            if desc {
                items.reverse();
            }
        } else {
            // numeric sort — parse all elements as f64
            let mut parse_err = false;
            items.sort_by(|a, b| {
                let a_str = std::str::from_utf8(a).unwrap_or("");
                let b_str = std::str::from_utf8(b).unwrap_or("");
                let a_val = a_str.parse::<f64>().unwrap_or_else(|_| {
                    parse_err = true;
                    0.0
                });
                let b_val = b_str.parse::<f64>().unwrap_or_else(|_| {
                    parse_err = true;
                    0.0
                });
                if desc {
                    b_val
                        .partial_cmp(&a_val)
                        .unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    a_val
                        .partial_cmp(&b_val)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
            });
            if parse_err {
                return Err("ERR One or more scores can't be converted into double");
            }
        }

        // apply limit
        if let Some((offset, count)) = limit {
            let offset = offset.max(0) as usize;
            let count = count.max(0) as usize;
            let end = offset.saturating_add(count).min(items.len());
            if offset < items.len() {
                items = items[offset..end].to_vec();
            } else {
                items.clear();
            }
        }

        Ok(items)
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
                    self.expiring.insert(key.into());
                }
                entry.expires_at_ms = time::now_ms().saturating_add(seconds.saturating_mul(1000));
                self.bump_version(key);
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

    /// Returns the estimated memory usage in bytes for the given key,
    /// or `None` if the key does not exist or is expired.
    ///
    /// Uses the cached value size for O(1) cost. The estimate covers the
    /// key string, the serialized value, and the per-entry overhead.
    pub fn memory_usage(&mut self, key: &str) -> Option<usize> {
        if self.remove_if_expired(key) {
            return None;
        }
        let entry = self.entries.get(key)?;
        Some(entry.entry_size(key))
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
                    self.expiring.swap_remove(key);
                    self.bump_version(key);
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
                    self.expiring.insert(key.into());
                }
                entry.expires_at_ms = time::now_ms().saturating_add(millis);
                self.bump_version(key);
                true
            }
            None => false,
        }
    }

    /// Sets an expiration at an absolute Unix timestamp (seconds).
    ///
    /// Returns `true` if the key exists and the expiry was set,
    /// `false` if the key doesn't exist.
    pub fn expireat(&mut self, key: &str, unix_secs: u64) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                if entry.expires_at_ms == 0 {
                    self.expiring.insert(key.into());
                }
                entry.expires_at_ms = time::unix_ms_to_monotonic_ms(unix_secs.saturating_mul(1000));
                self.bump_version(key);
                true
            }
            None => false,
        }
    }

    /// Sets an expiration at an absolute Unix timestamp (milliseconds).
    ///
    /// Returns `true` if the key exists and the expiry was set,
    /// `false` if the key doesn't exist.
    pub fn pexpireat(&mut self, key: &str, unix_ms: u64) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                if entry.expires_at_ms == 0 {
                    self.expiring.insert(key.into());
                }
                entry.expires_at_ms = time::unix_ms_to_monotonic_ms(unix_ms);
                self.bump_version(key);
                true
            }
            None => false,
        }
    }

    /// Returns the absolute Unix timestamp (seconds) when the key expires.
    ///
    /// Returns `-2` if the key doesn't exist, `-1` if it has no expiry.
    pub fn expiretime(&mut self, key: &str) -> i64 {
        if self.remove_if_expired(key) {
            return -2;
        }
        match self.entries.get(key) {
            None => -2,
            Some(entry) => match time::monotonic_to_unix_ms(entry.expires_at_ms) {
                None => -1,
                Some(unix_ms) => (unix_ms / 1000) as i64,
            },
        }
    }

    /// Returns the absolute Unix timestamp (milliseconds) when the key expires.
    ///
    /// Returns `-2` if the key doesn't exist, `-1` if it has no expiry.
    pub fn pexpiretime(&mut self, key: &str) -> i64 {
        if self.remove_if_expired(key) {
            return -2;
        }
        match self.entries.get(key) {
            None => -2,
            Some(entry) => match time::monotonic_to_unix_ms(entry.expires_at_ms) {
                None => -1,
                Some(unix_ms) => unix_ms as i64,
            },
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
        self.entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| glob_match(pattern, key))
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
            .filter(|(key, _)| ember_protocol::slots::key_slot(key.as_bytes()) == slot)
            .count()
    }

    /// Returns up to `count` live keys that hash to the given cluster slot.
    ///
    /// O(n) scan over all entries — same cost as KEYS.
    pub fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<String> {
        self.entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| ember_protocol::slots::key_slot(key.as_bytes()) == slot)
            .take(count)
            .map(|(key, _)| String::from(&**key))
            .collect()
    }

    /// Renames a key to a new name. Returns an error if the source key
    /// doesn't exist. If the destination key already exists, it is overwritten.
    pub fn rename(&mut self, key: &str, newkey: &str) -> Result<(), RenameError> {
        self.remove_if_expired(key);
        self.remove_if_expired(newkey);

        let entry = match self.entries.swap_remove(key) {
            Some(entry) => entry,
            None => return Err(RenameError::NoSuchKey),
        };

        // update memory tracking for old key removal
        self.memory.remove(key, &entry.value);
        self.untrack_expiry(key, &entry);

        // remove destination if it exists
        if let Some(old_dest) = self.entries.swap_remove(newkey) {
            self.memory.remove(newkey, &old_dest.value);
            self.untrack_expiry(newkey, &old_dest);
        }

        // re-insert with the new key name, preserving value and expiry
        self.memory.add(newkey, &entry.value);
        self.track_expiry(newkey, false, entry.expires_at_ms != 0);
        self.remove_version(key);
        self.entries.insert(CompactString::from(newkey), entry);
        self.bump_version(newkey);
        Ok(())
    }

    /// Copies the value at `source` to `destination`. If `replace` is false and
    /// the destination already exists, returns `Ok(false)`. Returns `Ok(true)` on
    /// success.
    pub fn copy(&mut self, source: &str, dest: &str, replace: bool) -> Result<bool, CopyError> {
        self.remove_if_expired(source);
        self.remove_if_expired(dest);

        let src_entry = match self.entries.get(source) {
            Some(e) => e,
            None => return Err(CopyError::NoSuchKey),
        };

        // if destination exists and replace not set, return 0
        if !replace && self.entries.contains_key(dest) {
            return Ok(false);
        }

        // clone value and expiry from source
        let cloned_value = src_entry.value.clone();
        let cloned_expire = if src_entry.expires_at_ms != 0 {
            Some(src_entry.expires_at_ms)
        } else {
            None
        };

        // estimate memory for the new entry
        let new_size = memory::entry_size(dest, &cloned_value);

        // if replacing, account for the old destination's size
        let old_dest_size = self
            .entries
            .get(dest)
            .map(|e| e.entry_size(dest))
            .unwrap_or(0);
        let net_increase = new_size.saturating_sub(old_dest_size);
        if !self.enforce_memory_limit(dest, net_increase) {
            return Err(CopyError::OutOfMemory);
        }

        // remove old destination if replacing
        if let Some(old_dest) = self.entries.swap_remove(dest) {
            self.memory.remove(dest, &old_dest.value);
            self.untrack_expiry(dest, &old_dest);
            self.defer_drop(old_dest.value);
        }

        // insert the clone
        self.memory.add(dest, &cloned_value);
        let has_expiry = cloned_expire.is_some();
        self.track_expiry(dest, false, has_expiry);
        let mut entry = Entry::new(cloned_value, None);
        // preserve the source's absolute expiry timestamp
        if let Some(ts) = cloned_expire {
            entry.expires_at_ms = ts;
        }
        self.entries.insert(CompactString::from(dest), entry);
        self.bump_version(dest);
        Ok(true)
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

    /// Scans proto keys starting from a cursor position.
    ///
    /// Returns only keys holding `Value::Proto` values. If `type_name` is
    /// provided, further restricts to keys whose message type matches exactly.
    /// Pattern matching follows the same glob rules as `scan_keys`.
    #[cfg(feature = "protobuf")]
    pub fn scan_proto_keys(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&str>,
        type_name: Option<&str>,
    ) -> (u64, Vec<String>) {
        self.scan_entries(cursor, count, |key, entry| {
            let Value::Proto { type_name: t, .. } = &entry.value else {
                return false;
            };
            type_name.is_none_or(|wanted| t.as_str() == wanted)
                && pattern.is_none_or(|pat| glob_match(pat, key))
        })
    }

    /// Scans keys starting from a cursor position.
    ///
    /// Returns the next cursor (0 if scan complete) and a batch of keys.
    /// The `pattern` argument uses the glob rules in [`crate::glob`].
    pub fn scan_keys(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&str>,
    ) -> (u64, Vec<String>) {
        self.scan_entries(cursor, count, |key, _| {
            pattern.is_none_or(|pat| glob_match(pat, key))
        })
    }

    /// Walks entries down from the cursor, collecting live keys that pass
    /// `keep`, until `count` are found or the walk reaches the start.
    ///
    /// The cursor is the number of positions left to visit: 0 starts a
    /// scan, and a returned 0 means it is done. Each call costs O(count)
    /// plus the entries skipped. Walking down from the end keeps Redis's
    /// guarantee that a key present for the whole scan is returned:
    /// `swap_remove` only moves the last entry, which the walk has already
    /// passed, into a lower slot. It may be returned twice. Keys added
    /// during the scan go at the end and may be missed, as Redis allows.
    fn scan_entries(
        &self,
        cursor: u64,
        count: usize,
        keep: impl Fn(&str, &Entry) -> bool,
    ) -> (u64, Vec<String>) {
        let target = if count == 0 { 10 } else { count };
        let len = self.entries.len();
        let mut position = match cursor {
            0 => len,
            c => usize::try_from(c).map_or(len, |c| c.min(len)),
        };
        let mut keys = Vec::with_capacity(target.min(position));
        while position > 0 && keys.len() < target {
            position -= 1;
            let (key, entry) = &self
                .entries
                .get_index(position)
                .expect("position is below len");
            if !entry.is_expired() && keep(key, entry) {
                keys.push(key.to_string());
            }
        }
        (position as u64, keys)
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
            self.track_expiry(&key, old.expires_at_ms != 0, has_expiry);
        } else {
            self.memory.add(&key, &value);
            self.track_expiry(&key, false, has_expiry);
        }

        let entry = Entry::new(value, ttl);
        self.entries.insert(CompactString::from(key.clone()), entry);
        self.bump_version(&key);
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
