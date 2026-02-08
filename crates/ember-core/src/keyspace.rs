//! The keyspace: Ember's core key-value store.
//!
//! A `Keyspace` owns a flat `HashMap<String, Entry>` and handles
//! get, set, delete, existence checks, and TTL management. Expired
//! keys are removed lazily on access. Memory usage is tracked on
//! every mutation for eviction and stats reporting.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use bytes::Bytes;
use rand::seq::IteratorRandom;

use tracing::warn;

use crate::dropper::DropHandle;
use crate::memory::{self, MemoryTracker};
use crate::time;
use crate::types::sorted_set::{SortedSet, ZAddFlags};
use crate::types::{self, normalize_range, Value};

/// Error returned when a command is used against a key holding the wrong type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrongType;

impl std::fmt::Display for WrongType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WRONGTYPE Operation against a key holding the wrong kind of value"
        )
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
            IncrError::WrongType => write!(
                f,
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            ),
            IncrError::NotAnInteger => write!(f, "ERR value is not an integer or out of range"),
            IncrError::Overflow => write!(f, "ERR increment or decrement would overflow"),
            IncrError::OutOfMemory => {
                write!(f, "OOM command not allowed when used memory > 'maxmemory'")
            }
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
            IncrFloatError::WrongType => write!(
                f,
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            ),
            IncrFloatError::NotAFloat => {
                write!(f, "ERR value is not a valid float")
            }
            IncrFloatError::NanOrInfinity => {
                write!(f, "ERR increment would produce NaN or Infinity")
            }
            IncrFloatError::OutOfMemory => {
                write!(f, "OOM command not allowed when used memory > 'maxmemory'")
            }
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
    entries: HashMap<String, Entry>,
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
            entries: HashMap::new(),
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

    /// Retrieves the string value for `key`, or `None` if missing/expired.
    ///
    /// Returns `Err(WrongType)` if the key holds a non-string value.
    /// Expired keys are removed lazily on access. Successful reads update
    /// the entry's last access time for LRU tracking.
    pub fn get(&mut self, key: &str) -> Result<Option<Value>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }
        match self.entries.get_mut(key) {
            Some(e) => match &e.value {
                Value::String(_) => {
                    e.touch();
                    Ok(Some(e.value.clone()))
                }
                _ => Err(WrongType),
            },
            None => Ok(None),
        }
    }

    /// Returns the type name of the value at `key`, or "none" if missing.
    pub fn value_type(&mut self, key: &str) -> &'static str {
        if self.remove_if_expired(key) {
            return "none";
        }
        match self.entries.get(key) {
            Some(e) => types::type_name(&e.value),
            None => "none",
        }
    }

    /// Stores a key-value pair. If the key already existed, the old entry
    /// (including any TTL) is replaced entirely.
    ///
    /// `expire` sets an optional TTL as a duration from now.
    ///
    /// Returns `SetResult::OutOfMemory` if the memory limit is reached
    /// and the eviction policy is `NoEviction`. With `AllKeysLru`, this
    /// will evict keys to make room before inserting.
    pub fn set(&mut self, key: String, value: Bytes, expire: Option<Duration>) -> SetResult {
        let has_expiry = expire.is_some();
        let new_value = Value::String(value);

        // check memory limit — for overwrites, only the net increase matters
        let new_size = memory::entry_size(&key, &new_value);
        let old_size = self
            .entries
            .get(&key)
            .map(|e| memory::entry_size(&key, &e.value))
            .unwrap_or(0);
        let net_increase = new_size.saturating_sub(old_size);

        if !self.enforce_memory_limit(net_increase) {
            return SetResult::OutOfMemory;
        }

        if let Some(old_entry) = self.entries.get(&key) {
            self.memory.replace(&key, &old_entry.value, &new_value);
            // adjust expiry count if the TTL status changed
            let had_expiry = old_entry.expires_at_ms != 0;
            match (had_expiry, has_expiry) {
                (false, true) => self.expiry_count += 1,
                (true, false) => self.expiry_count = self.expiry_count.saturating_sub(1),
                _ => {}
            }
        } else {
            self.memory.add(&key, &new_value);
            if has_expiry {
                self.expiry_count += 1;
            }
        }

        self.entries.insert(key, Entry::new(new_value, expire));
        SetResult::Ok
    }

    /// Tries to evict one key using LRU approximation.
    ///
    /// Randomly samples `EVICTION_SAMPLE_SIZE` keys and removes the one
    /// with the oldest `last_access` time. Returns `true` if a key was
    /// evicted, `false` if the keyspace is empty.
    fn try_evict(&mut self) -> bool {
        if self.entries.is_empty() {
            return false;
        }

        let mut rng = rand::rng();

        // randomly sample keys and find the least recently accessed one
        let victim = self
            .entries
            .iter()
            .choose_multiple(&mut rng, EVICTION_SAMPLE_SIZE)
            .into_iter()
            .min_by_key(|(_, entry)| entry.last_access_ms)
            .map(|(k, _)| k.clone());

        if let Some(key) = victim {
            if let Some(entry) = self.entries.remove(&key) {
                self.memory.remove(&key, &entry.value);
                if entry.expires_at_ms != 0 {
                    self.expiry_count = self.expiry_count.saturating_sub(1);
                }
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
            if entry.expires_at_ms != 0 {
                self.expiry_count = self.expiry_count.saturating_sub(1);
            }
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
            if entry.expires_at_ms != 0 {
                self.expiry_count = self.expiry_count.saturating_sub(1);
            }
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
    pub(crate) fn flush_async(&mut self) -> HashMap<String, Entry> {
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
                entry.expires_at_ms = time::now_ms() + seconds * 1000;
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
                entry.expires_at_ms = time::now_ms() + millis;
                true
            }
            None => false,
        }
    }

    /// Increments the integer value of a key by 1.
    ///
    /// If the key doesn't exist, it's initialized to 0 before incrementing.
    /// Returns the new value after the operation.
    pub fn incr(&mut self, key: &str) -> Result<i64, IncrError> {
        self.incr_by(key, 1)
    }

    /// Decrements the integer value of a key by 1.
    ///
    /// If the key doesn't exist, it's initialized to 0 before decrementing.
    /// Returns the new value after the operation.
    pub fn decr(&mut self, key: &str) -> Result<i64, IncrError> {
        self.incr_by(key, -1)
    }

    /// Adds `delta` to the current integer value of the key, creating it
    /// if necessary. Used by INCR, DECR, INCRBY, and DECRBY.
    ///
    /// Preserves the existing TTL when updating an existing key.
    pub fn incr_by(&mut self, key: &str, delta: i64) -> Result<i64, IncrError> {
        self.remove_if_expired(key);

        // read current value and TTL
        let (current, existing_expire) = match self.entries.get(key) {
            Some(entry) => {
                let val = match &entry.value {
                    Value::String(data) => {
                        let s = std::str::from_utf8(data).map_err(|_| IncrError::NotAnInteger)?;
                        s.parse::<i64>().map_err(|_| IncrError::NotAnInteger)?
                    }
                    _ => return Err(IncrError::WrongType),
                };
                let expire = time::remaining_ms(entry.expires_at_ms).map(Duration::from_millis);
                (val, expire)
            }
            None => (0, None),
        };

        let new_val = current.checked_add(delta).ok_or(IncrError::Overflow)?;
        let new_bytes = Bytes::from(new_val.to_string());

        match self.set(key.to_owned(), new_bytes, existing_expire) {
            SetResult::Ok => Ok(new_val),
            SetResult::OutOfMemory => Err(IncrError::OutOfMemory),
        }
    }

    /// Adds a float `delta` to the current value of the key, creating it
    /// if necessary. Used by INCRBYFLOAT.
    ///
    /// Preserves the existing TTL when updating an existing key.
    /// Returns the new value as a string (matching Redis behavior).
    pub fn incr_by_float(&mut self, key: &str, delta: f64) -> Result<String, IncrFloatError> {
        self.remove_if_expired(key);

        let (current, existing_expire) = match self.entries.get(key) {
            Some(entry) => {
                let val = match &entry.value {
                    Value::String(data) => {
                        let s = std::str::from_utf8(data).map_err(|_| IncrFloatError::NotAFloat)?;
                        s.parse::<f64>().map_err(|_| IncrFloatError::NotAFloat)?
                    }
                    _ => return Err(IncrFloatError::WrongType),
                };
                let expire = time::remaining_ms(entry.expires_at_ms).map(Duration::from_millis);
                (val, expire)
            }
            None => (0.0, None),
        };

        let new_val = current + delta;
        if new_val.is_nan() || new_val.is_infinite() {
            return Err(IncrFloatError::NanOrInfinity);
        }

        // Redis strips trailing zeros: "10.5" not "10.50000..."
        // but keeps at least one decimal if the result is a whole number
        let formatted = format_float(new_val);
        let new_bytes = Bytes::from(formatted.clone());

        match self.set(key.to_owned(), new_bytes, existing_expire) {
            SetResult::Ok => Ok(formatted),
            SetResult::OutOfMemory => Err(IncrFloatError::OutOfMemory),
        }
    }

    /// Appends a value to an existing string key, or creates a new key if
    /// it doesn't exist. Returns the new string length.
    pub fn append(&mut self, key: &str, value: &[u8]) -> Result<usize, WriteError> {
        self.remove_if_expired(key);

        match self.entries.get(key) {
            Some(entry) => match &entry.value {
                Value::String(existing) => {
                    let mut new_data = Vec::with_capacity(existing.len() + value.len());
                    new_data.extend_from_slice(existing);
                    new_data.extend_from_slice(value);
                    let new_len = new_data.len();
                    let expire = time::remaining_ms(entry.expires_at_ms).map(Duration::from_millis);
                    match self.set(key.to_owned(), Bytes::from(new_data), expire) {
                        SetResult::Ok => Ok(new_len),
                        SetResult::OutOfMemory => Err(WriteError::OutOfMemory),
                    }
                }
                _ => Err(WriteError::WrongType),
            },
            None => {
                let new_len = value.len();
                match self.set(key.to_owned(), Bytes::copy_from_slice(value), None) {
                    SetResult::Ok => Ok(new_len),
                    SetResult::OutOfMemory => Err(WriteError::OutOfMemory),
                }
            }
        }
    }

    /// Returns the length of the string value stored at key.
    /// Returns 0 if the key does not exist.
    pub fn strlen(&mut self, key: &str) -> Result<usize, WrongType> {
        self.remove_if_expired(key);

        match self.entries.get(key) {
            Some(entry) => match &entry.value {
                Value::String(data) => Ok(data.len()),
                _ => Err(WrongType),
            },
            None => Ok(0),
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
            .map(|(key, _)| key.clone())
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
        if entry.expires_at_ms != 0 {
            self.expiry_count = self.expiry_count.saturating_sub(1);
        }

        // remove destination if it exists
        if let Some(old_dest) = self.entries.remove(newkey) {
            self.memory.remove(newkey, &old_dest.value);
            if old_dest.expires_at_ms != 0 {
                self.expiry_count = self.expiry_count.saturating_sub(1);
            }
        }

        // re-insert with the new key name, preserving value and expiry
        self.memory.add(newkey, &entry.value);
        if entry.expires_at_ms != 0 {
            self.expiry_count += 1;
        }
        self.entries.insert(newkey.to_owned(), entry);
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
            if let Some(pat) = pattern {
                if !glob_match(pat, key) {
                    position += 1;
                    continue;
                }
            }

            keys.push(key.clone());
            position += 1;

            if keys.len() >= target_count {
                // return position as next cursor
                return (position, keys);
            }
        }

        // scan complete
        (0, keys)
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
                Some(ms) => ms as i64,
                None => -1,
            };
            Some((key.as_str(), &entry.value, ttl_ms))
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
        if let Some(old) = self.entries.get(&key) {
            self.memory.replace(&key, &old.value, &value);
            let had_expiry = old.expires_at_ms != 0;
            match (had_expiry, has_expiry) {
                (false, true) => self.expiry_count += 1,
                (true, false) => self.expiry_count = self.expiry_count.saturating_sub(1),
                _ => {}
            }
        } else {
            self.memory.add(&key, &value);
            if has_expiry {
                self.expiry_count += 1;
            }
        }

        self.entries.insert(key, Entry::new(value, ttl));
    }

    // -- list operations --

    /// Pushes one or more values to the head (left) of a list.
    ///
    /// Creates the list if the key doesn't exist. Returns `Err(WriteError::WrongType)`
    /// if the key exists but holds a non-list value, or
    /// `Err(WriteError::OutOfMemory)` if the memory limit is reached.
    /// Returns the new length on success.
    pub fn lpush(&mut self, key: &str, values: &[Bytes]) -> Result<usize, WriteError> {
        self.list_push(key, values, true)
    }

    /// Pushes one or more values to the tail (right) of a list.
    ///
    /// Creates the list if the key doesn't exist. Returns `Err(WriteError::WrongType)`
    /// if the key exists but holds a non-list value, or
    /// `Err(WriteError::OutOfMemory)` if the memory limit is reached.
    /// Returns the new length on success.
    pub fn rpush(&mut self, key: &str, values: &[Bytes]) -> Result<usize, WriteError> {
        self.list_push(key, values, false)
    }

    /// Pops a value from the head (left) of a list.
    ///
    /// Returns `Ok(None)` if the key doesn't exist. Removes the key if
    /// the list becomes empty. Returns `Err(WrongType)` on type mismatch.
    pub fn lpop(&mut self, key: &str) -> Result<Option<Bytes>, WrongType> {
        self.list_pop(key, true)
    }

    /// Pops a value from the tail (right) of a list.
    ///
    /// Returns `Ok(None)` if the key doesn't exist. Removes the key if
    /// the list becomes empty. Returns `Err(WrongType)` on type mismatch.
    pub fn rpop(&mut self, key: &str) -> Result<Option<Bytes>, WrongType> {
        self.list_pop(key, false)
    }

    /// Returns a range of elements from a list by index.
    ///
    /// Supports negative indices (e.g. -1 = last element). Out-of-bounds
    /// indices are clamped to the list boundaries. Returns `Err(WrongType)`
    /// on type mismatch. Missing keys return an empty vec.
    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }
        match self.entries.get_mut(key) {
            None => Ok(vec![]),
            Some(entry) => {
                let result = match &entry.value {
                    Value::List(deque) => {
                        let len = deque.len() as i64;
                        let (s, e) = normalize_range(start, stop, len);
                        if s > e {
                            return Ok(vec![]);
                        }
                        Ok(deque
                            .iter()
                            .skip(s as usize)
                            .take((e - s + 1) as usize)
                            .cloned()
                            .collect())
                    }
                    _ => Err(WrongType),
                };
                if result.is_ok() {
                    entry.touch();
                }
                result
            }
        }
    }

    /// Returns the length of a list, or 0 if the key doesn't exist.
    ///
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn llen(&mut self, key: &str) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }
        match self.entries.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                Value::List(deque) => Ok(deque.len()),
                _ => Err(WrongType),
            },
        }
    }

    /// Internal push implementation shared by lpush/rpush.
    fn list_push(&mut self, key: &str, values: &[Bytes], left: bool) -> Result<usize, WriteError> {
        self.remove_if_expired(key);

        let is_new = !self.entries.contains_key(key);

        if !is_new && !matches!(self.entries[key].value, Value::List(_)) {
            return Err(WriteError::WrongType);
        }

        // estimate the memory increase and enforce the limit before mutating
        let element_increase: usize = values
            .iter()
            .map(|v| memory::VECDEQUE_ELEMENT_OVERHEAD + v.len())
            .sum();
        let estimated_increase = if is_new {
            memory::ENTRY_OVERHEAD + key.len() + memory::VECDEQUE_BASE_OVERHEAD + element_increase
        } else {
            element_increase
        };
        if !self.enforce_memory_limit(estimated_increase) {
            return Err(WriteError::OutOfMemory);
        }

        if is_new {
            let value = Value::List(VecDeque::new());
            self.memory.add(key, &value);
            self.entries.insert(key.to_owned(), Entry::new(value, None));
        }

        let entry = self
            .entries
            .get_mut(key)
            .expect("just inserted or verified");
        let old_entry_size = memory::entry_size(key, &entry.value);

        if let Value::List(ref mut deque) = entry.value {
            for val in values {
                if left {
                    deque.push_front(val.clone());
                } else {
                    deque.push_back(val.clone());
                }
            }
        }
        entry.touch();

        let new_entry_size = memory::entry_size(key, &entry.value);
        self.memory.adjust(old_entry_size, new_entry_size);

        let len = match &entry.value {
            Value::List(d) => d.len(),
            _ => unreachable!(),
        };
        Ok(len)
    }

    /// Internal pop implementation shared by lpop/rpop.
    fn list_pop(&mut self, key: &str, left: bool) -> Result<Option<Bytes>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }

        match self.entries.get(key) {
            None => return Ok(None),
            Some(e) => {
                if !matches!(e.value, Value::List(_)) {
                    return Err(WrongType);
                }
            }
        };

        let old_entry_size = memory::entry_size(key, &self.entries[key].value);
        let entry = self.entries.get_mut(key).expect("verified above");
        let popped = if let Value::List(ref mut deque) = entry.value {
            if left {
                deque.pop_front()
            } else {
                deque.pop_back()
            }
        } else {
            unreachable!()
        };
        entry.touch();

        // check if list is now empty — if so, delete the key entirely
        let is_empty = matches!(&entry.value, Value::List(d) if d.is_empty());
        if is_empty {
            let removed = self.entries.remove(key).expect("verified above");
            // use remove_with_size since the value was already mutated
            self.memory.remove_with_size(old_entry_size);
            if removed.expires_at_ms != 0 {
                self.expiry_count = self.expiry_count.saturating_sub(1);
            }
        } else {
            let new_entry_size = memory::entry_size(key, &self.entries[key].value);
            self.memory.adjust(old_entry_size, new_entry_size);
        }

        Ok(popped)
    }

    // -- sorted set operations --

    /// Adds members with scores to a sorted set, with optional ZADD flags.
    ///
    /// Creates the sorted set if the key doesn't exist. Returns a
    /// `ZAddResult` containing the count for the client response and the
    /// list of members that were actually applied (for AOF correctness).
    /// Returns `Err(WriteError::WrongType)` on type mismatch, or
    /// `Err(WriteError::OutOfMemory)` if the memory limit is reached.
    pub fn zadd(
        &mut self,
        key: &str,
        members: &[(f64, String)],
        flags: &ZAddFlags,
    ) -> Result<ZAddResult, WriteError> {
        self.remove_if_expired(key);

        let is_new = !self.entries.contains_key(key);
        if !is_new && !matches!(self.entries[key].value, Value::SortedSet(_)) {
            return Err(WriteError::WrongType);
        }

        // worst-case estimate: assume all members are new
        let member_increase: usize = members
            .iter()
            .map(|(_, m)| SortedSet::estimated_member_cost(m))
            .sum();
        let estimated_increase = if is_new {
            memory::ENTRY_OVERHEAD + key.len() + SortedSet::BASE_OVERHEAD + member_increase
        } else {
            member_increase
        };
        if !self.enforce_memory_limit(estimated_increase) {
            return Err(WriteError::OutOfMemory);
        }

        if is_new {
            let value = Value::SortedSet(SortedSet::new());
            self.memory.add(key, &value);
            self.entries.insert(key.to_owned(), Entry::new(value, None));
        }

        let entry = self
            .entries
            .get_mut(key)
            .expect("just inserted or verified");
        let old_entry_size = memory::entry_size(key, &entry.value);

        let mut count = 0;
        let mut applied = Vec::new();
        if let Value::SortedSet(ref mut ss) = entry.value {
            for (score, member) in members {
                let result = ss.add_with_flags(member.clone(), *score, flags);
                if result.added || result.updated {
                    applied.push((*score, member.clone()));
                }
                if flags.ch {
                    if result.added || result.updated {
                        count += 1;
                    }
                } else if result.added {
                    count += 1;
                }
            }
        }
        entry.touch();

        let new_entry_size = memory::entry_size(key, &entry.value);
        self.memory.adjust(old_entry_size, new_entry_size);

        // clean up if the set is still empty (e.g. XX on a new key)
        if let Value::SortedSet(ref ss) = entry.value {
            if ss.is_empty() {
                self.memory
                    .remove_with_size(memory::entry_size(key, &entry.value));
                self.entries.remove(key);
            }
        }

        Ok(ZAddResult { count, applied })
    }

    /// Removes members from a sorted set. Returns the names of members
    /// that were actually removed (for AOF correctness). Deletes the key
    /// if the set becomes empty.
    ///
    /// Returns `Err(WrongType)` if the key holds a non-sorted-set value.
    pub fn zrem(&mut self, key: &str, members: &[String]) -> Result<Vec<String>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }

        match self.entries.get(key) {
            None => return Ok(vec![]),
            Some(e) => {
                if !matches!(e.value, Value::SortedSet(_)) {
                    return Err(WrongType);
                }
            }
        }

        let old_entry_size = memory::entry_size(key, &self.entries[key].value);
        let entry = self.entries.get_mut(key).expect("verified above");
        let mut removed = Vec::new();
        if let Value::SortedSet(ref mut ss) = entry.value {
            for member in members {
                if ss.remove(member) {
                    removed.push(member.clone());
                }
            }
        }
        entry.touch();

        let is_empty = matches!(&entry.value, Value::SortedSet(ss) if ss.is_empty());
        if is_empty {
            let removed_entry = self.entries.remove(key).expect("verified above");
            self.memory.remove_with_size(old_entry_size);
            if removed_entry.expires_at_ms != 0 {
                self.expiry_count = self.expiry_count.saturating_sub(1);
            }
        } else {
            let new_entry_size = memory::entry_size(key, &self.entries[key].value);
            self.memory.adjust(old_entry_size, new_entry_size);
        }

        Ok(removed)
    }

    /// Returns the score for a member in a sorted set.
    ///
    /// Returns `Ok(None)` if the key or member doesn't exist.
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn zscore(&mut self, key: &str, member: &str) -> Result<Option<f64>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }
        match self.entries.get_mut(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                Value::SortedSet(ss) => {
                    let score = ss.score(member);
                    entry.touch();
                    Ok(score)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Returns the 0-based rank of a member in a sorted set (lowest score = 0).
    ///
    /// Returns `Ok(None)` if the key or member doesn't exist.
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn zrank(&mut self, key: &str, member: &str) -> Result<Option<usize>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }
        match self.entries.get_mut(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                Value::SortedSet(ss) => {
                    let rank = ss.rank(member);
                    entry.touch();
                    Ok(rank)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Returns a range of members from a sorted set by rank.
    ///
    /// Supports negative indices. If `with_scores` is true, the result
    /// includes `(member, score)` pairs; otherwise just members.
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn zrange(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<(String, f64)>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }
        match self.entries.get_mut(key) {
            None => Ok(vec![]),
            Some(entry) => {
                let result = match &entry.value {
                    Value::SortedSet(ss) => {
                        let items = ss.range_by_rank(start, stop);
                        Ok(items.into_iter().map(|(m, s)| (m.to_owned(), s)).collect())
                    }
                    _ => Err(WrongType),
                };
                if result.is_ok() {
                    entry.touch();
                }
                result
            }
        }
    }

    /// Returns the number of members in a sorted set, or 0 if the key doesn't exist.
    ///
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn zcard(&mut self, key: &str) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }
        match self.entries.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                Value::SortedSet(ss) => Ok(ss.len()),
                _ => Err(WrongType),
            },
        }
    }

    // -------------------------------------------------------------------------
    // Hash operations
    // -------------------------------------------------------------------------

    /// Sets one or more field-value pairs in a hash.
    ///
    /// Creates the hash if the key doesn't exist. Returns the number of
    /// new fields added (fields that were updated don't count).
    pub fn hset(&mut self, key: &str, fields: &[(String, Bytes)]) -> Result<usize, WriteError> {
        if fields.is_empty() {
            return Ok(0);
        }

        self.remove_if_expired(key);

        let is_new = !self.entries.contains_key(key);

        if !is_new && !matches!(self.entries[key].value, Value::Hash(_)) {
            return Err(WriteError::WrongType);
        }

        // estimate memory increase before mutating
        let field_increase: usize = fields
            .iter()
            .map(|(f, v)| f.len() + v.len() + memory::HASHMAP_ENTRY_OVERHEAD)
            .sum();
        let estimated_increase = if is_new {
            memory::ENTRY_OVERHEAD + key.len() + memory::HASHMAP_BASE_OVERHEAD + field_increase
        } else {
            field_increase
        };
        if !self.enforce_memory_limit(estimated_increase) {
            return Err(WriteError::OutOfMemory);
        }

        if is_new {
            let value = Value::Hash(HashMap::new());
            self.memory.add(key, &value);
            self.entries.insert(key.to_owned(), Entry::new(value, None));
        }

        let entry = self
            .entries
            .get_mut(key)
            .expect("just inserted or verified");
        let old_entry_size = memory::entry_size(key, &entry.value);

        let mut added = 0;
        if let Value::Hash(ref mut map) = entry.value {
            for (field, value) in fields {
                if map.insert(field.clone(), value.clone()).is_none() {
                    added += 1;
                }
            }
        }
        entry.touch();

        let new_entry_size = memory::entry_size(key, &entry.value);
        self.memory.adjust(old_entry_size, new_entry_size);

        Ok(added)
    }

    /// Gets the value of a field in a hash.
    ///
    /// Returns `None` if the key or field doesn't exist.
    pub fn hget(&mut self, key: &str, field: &str) -> Result<Option<Bytes>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }
        match self.entries.get_mut(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let result = map.get(field).cloned();
                    entry.touch();
                    Ok(result)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Gets all field-value pairs from a hash.
    ///
    /// Returns an empty vec if the key doesn't exist.
    pub fn hgetall(&mut self, key: &str) -> Result<Vec<(String, Bytes)>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }
        match self.entries.get_mut(key) {
            None => Ok(vec![]),
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let result: Vec<_> = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                    entry.touch();
                    Ok(result)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Deletes one or more fields from a hash.
    ///
    /// Returns the fields that were actually removed.
    pub fn hdel(&mut self, key: &str, fields: &[String]) -> Result<Vec<String>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }

        match self.entries.get(key) {
            None => return Ok(vec![]),
            Some(e) => {
                if !matches!(e.value, Value::Hash(_)) {
                    return Err(WrongType);
                }
            }
        }

        let old_entry_size = memory::entry_size(key, &self.entries[key].value);
        let entry = self.entries.get_mut(key).expect("verified above");

        let mut removed = Vec::new();
        let is_empty = if let Value::Hash(ref mut map) = entry.value {
            for field in fields {
                if map.remove(field).is_some() {
                    removed.push(field.clone());
                }
            }
            map.is_empty()
        } else {
            false
        };

        if is_empty {
            self.memory.remove_with_size(old_entry_size);
            self.entries.remove(key);
        } else {
            let new_entry_size = memory::entry_size(key, &self.entries[key].value);
            self.memory.adjust(old_entry_size, new_entry_size);
        }

        Ok(removed)
    }

    /// Checks if a field exists in a hash.
    pub fn hexists(&mut self, key: &str, field: &str) -> Result<bool, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(false);
        }
        match self.entries.get_mut(key) {
            None => Ok(false),
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let result = map.contains_key(field);
                    entry.touch();
                    Ok(result)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Returns the number of fields in a hash.
    pub fn hlen(&mut self, key: &str) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }
        match self.entries.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                Value::Hash(map) => Ok(map.len()),
                _ => Err(WrongType),
            },
        }
    }

    /// Increments a field's integer value by the given amount.
    ///
    /// Creates the hash and field if they don't exist, starting from 0.
    pub fn hincrby(&mut self, key: &str, field: &str, delta: i64) -> Result<i64, IncrError> {
        self.remove_if_expired(key);

        let is_new = !self.entries.contains_key(key);

        if !is_new {
            match &self.entries[key].value {
                Value::Hash(_) => {}
                _ => return Err(IncrError::WrongType),
            }
        }

        // estimate memory for new field (worst case: new hash + new field)
        let val_str_len = 20; // max i64 string length
        let estimated_increase = if is_new {
            memory::ENTRY_OVERHEAD
                + key.len()
                + memory::HASHMAP_BASE_OVERHEAD
                + field.len()
                + val_str_len
                + memory::HASHMAP_ENTRY_OVERHEAD
        } else {
            field.len() + val_str_len + memory::HASHMAP_ENTRY_OVERHEAD
        };

        if !self.enforce_memory_limit(estimated_increase) {
            return Err(IncrError::OutOfMemory);
        }

        if is_new {
            let value = Value::Hash(HashMap::new());
            self.memory.add(key, &value);
            self.entries.insert(key.to_owned(), Entry::new(value, None));
        }

        let entry = self
            .entries
            .get_mut(key)
            .expect("just inserted or verified");
        let old_entry_size = memory::entry_size(key, &entry.value);

        let new_val = if let Value::Hash(ref mut map) = entry.value {
            let current_val = match map.get(field) {
                Some(data) => {
                    let s = std::str::from_utf8(data).map_err(|_| IncrError::NotAnInteger)?;
                    s.parse::<i64>().map_err(|_| IncrError::NotAnInteger)?
                }
                None => 0,
            };

            let new_val = current_val.checked_add(delta).ok_or(IncrError::Overflow)?;
            map.insert(field.to_owned(), Bytes::from(new_val.to_string()));
            new_val
        } else {
            unreachable!()
        };
        entry.touch();

        let new_entry_size = memory::entry_size(key, &entry.value);
        self.memory.adjust(old_entry_size, new_entry_size);

        Ok(new_val)
    }

    /// Returns all field names in a hash.
    pub fn hkeys(&mut self, key: &str) -> Result<Vec<String>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }
        match self.entries.get_mut(key) {
            None => Ok(vec![]),
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let result = map.keys().cloned().collect();
                    entry.touch();
                    Ok(result)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Returns all values in a hash.
    pub fn hvals(&mut self, key: &str) -> Result<Vec<Bytes>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }
        match self.entries.get_mut(key) {
            None => Ok(vec![]),
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let result = map.values().cloned().collect();
                    entry.touch();
                    Ok(result)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Gets multiple field values from a hash.
    ///
    /// Returns `None` for fields that don't exist.
    pub fn hmget(&mut self, key: &str, fields: &[String]) -> Result<Vec<Option<Bytes>>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(fields.iter().map(|_| None).collect());
        }
        match self.entries.get_mut(key) {
            None => Ok(fields.iter().map(|_| None).collect()),
            Some(entry) => match &entry.value {
                Value::Hash(map) => {
                    let result = fields.iter().map(|f| map.get(f).cloned()).collect();
                    entry.touch();
                    Ok(result)
                }
                _ => Err(WrongType),
            },
        }
    }

    // -------------------------------------------------------------------------
    // Set operations
    // -------------------------------------------------------------------------

    /// Adds one or more members to a set.
    ///
    /// Creates the set if the key doesn't exist. Returns the number of
    /// new members added (existing members don't count).
    pub fn sadd(&mut self, key: &str, members: &[String]) -> Result<usize, WriteError> {
        if members.is_empty() {
            return Ok(0);
        }

        self.remove_if_expired(key);

        let is_new = !self.entries.contains_key(key);

        if !is_new && !matches!(self.entries[key].value, Value::Set(_)) {
            return Err(WriteError::WrongType);
        }

        // estimate memory increase before mutating
        let member_increase: usize = members
            .iter()
            .map(|m| m.len() + memory::HASHSET_MEMBER_OVERHEAD)
            .sum();
        let estimated_increase = if is_new {
            memory::ENTRY_OVERHEAD + key.len() + memory::HASHSET_BASE_OVERHEAD + member_increase
        } else {
            member_increase
        };
        if !self.enforce_memory_limit(estimated_increase) {
            return Err(WriteError::OutOfMemory);
        }

        if is_new {
            let value = Value::Set(std::collections::HashSet::new());
            self.memory.add(key, &value);
            self.entries.insert(key.to_owned(), Entry::new(value, None));
        }

        let entry = self
            .entries
            .get_mut(key)
            .expect("just inserted or verified");
        let old_entry_size = memory::entry_size(key, &entry.value);

        let mut added = 0;
        if let Value::Set(ref mut set) = entry.value {
            for member in members {
                if set.insert(member.clone()) {
                    added += 1;
                }
            }
        }
        entry.touch();

        let new_entry_size = memory::entry_size(key, &entry.value);
        self.memory.adjust(old_entry_size, new_entry_size);

        Ok(added)
    }

    /// Removes one or more members from a set.
    ///
    /// Returns the number of members that were actually removed.
    pub fn srem(&mut self, key: &str, members: &[String]) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }

        match self.entries.get(key) {
            None => return Ok(0),
            Some(e) => {
                if !matches!(e.value, Value::Set(_)) {
                    return Err(WrongType);
                }
            }
        }

        let old_entry_size = memory::entry_size(key, &self.entries[key].value);
        let entry = self.entries.get_mut(key).expect("verified above");

        let mut removed = 0;
        let is_empty = if let Value::Set(ref mut set) = entry.value {
            for member in members {
                if set.remove(member) {
                    removed += 1;
                }
            }
            set.is_empty()
        } else {
            false
        };

        if is_empty {
            self.memory.remove_with_size(old_entry_size);
            self.entries.remove(key);
        } else {
            let new_entry_size = memory::entry_size(key, &self.entries[key].value);
            self.memory.adjust(old_entry_size, new_entry_size);
        }

        Ok(removed)
    }

    /// Returns all members of a set.
    pub fn smembers(&mut self, key: &str) -> Result<Vec<String>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }
        match self.entries.get_mut(key) {
            None => Ok(vec![]),
            Some(entry) => match &entry.value {
                Value::Set(set) => {
                    let result = set.iter().cloned().collect();
                    entry.touch();
                    Ok(result)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Checks if a member exists in a set.
    pub fn sismember(&mut self, key: &str, member: &str) -> Result<bool, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(false);
        }
        match self.entries.get_mut(key) {
            None => Ok(false),
            Some(entry) => match &entry.value {
                Value::Set(set) => {
                    let result = set.contains(member);
                    entry.touch();
                    Ok(result)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Returns the cardinality (number of elements) of a set.
    pub fn scard(&mut self, key: &str) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }
        match self.entries.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                Value::Set(set) => Ok(set.len()),
                _ => Err(WrongType),
            },
        }
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
            .cloned()
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
                if entry.expires_at_ms != 0 {
                    self.expiry_count = self.expiry_count.saturating_sub(1);
                }
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
fn format_float(val: f64) -> String {
    if val == 0.0 {
        return "0".into();
    }
    // Use enough precision to round-trip
    let s = format!("{:.17e}", val);
    // Parse back to get the clean representation
    let reparsed: f64 = s.parse().unwrap_or(val);
    // If it's a whole number, format without decimals
    if reparsed == reparsed.trunc() && reparsed.abs() < 1e15 {
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
fn glob_match(pattern: &str, text: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
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
    fn set_and_get() {
        let mut ks = Keyspace::new();
        ks.set("hello".into(), Bytes::from("world"), None);
        assert_eq!(
            ks.get("hello").unwrap(),
            Some(Value::String(Bytes::from("world")))
        );
    }

    #[test]
    fn get_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.get("nope").unwrap(), None);
    }

    #[test]
    fn overwrite_replaces_value() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("first"), None);
        ks.set("key".into(), Bytes::from("second"), None);
        assert_eq!(
            ks.get("key").unwrap(),
            Some(Value::String(Bytes::from("second")))
        );
    }

    #[test]
    fn overwrite_clears_old_ttl() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("v1"),
            Some(Duration::from_secs(100)),
        );
        // overwrite without TTL — should clear the old one
        ks.set("key".into(), Bytes::from("v2"), None);
        assert_eq!(ks.ttl("key"), TtlResult::NoExpiry);
    }

    #[test]
    fn del_existing() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None);
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
        ks.set("yes".into(), Bytes::from("here"), None);
        assert!(ks.exists("yes"));
        assert!(!ks.exists("no"));
    }

    #[test]
    fn expired_key_returns_none() {
        let mut ks = Keyspace::new();
        ks.set(
            "temp".into(),
            Bytes::from("gone"),
            Some(Duration::from_millis(10)),
        );
        // wait for expiration
        thread::sleep(Duration::from_millis(30));
        assert_eq!(ks.get("temp").unwrap(), None);
        // should also be gone from exists
        assert!(!ks.exists("temp"));
    }

    #[test]
    fn ttl_no_expiry() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None);
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
        );
        thread::sleep(Duration::from_millis(30));
        assert_eq!(ks.ttl("temp"), TtlResult::NotFound);
    }

    #[test]
    fn expire_existing_key() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None);
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
        ks.set("key".into(), Bytes::from("value"), None);
        assert!(ks.stats().used_bytes > 0);
        assert_eq!(ks.stats().key_count, 1);
    }

    #[test]
    fn memory_decreases_on_del() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("value"), None);
        let after_set = ks.stats().used_bytes;
        ks.del("key");
        assert_eq!(ks.stats().used_bytes, 0);
        assert!(after_set > 0);
    }

    #[test]
    fn memory_adjusts_on_overwrite() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("short"), None);
        let small = ks.stats().used_bytes;

        ks.set("key".into(), Bytes::from("a much longer value"), None);
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
        ks.set("a".into(), Bytes::from("1"), None);
        ks.set("b".into(), Bytes::from("2"), Some(Duration::from_secs(100)));
        ks.set("c".into(), Bytes::from("3"), Some(Duration::from_secs(200)));

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
        assert_eq!(ks.set("a".into(), Bytes::from("val"), None), SetResult::Ok);

        // second key should push us over the limit
        let result = ks.set("b".into(), Bytes::from("val"), None);
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

        assert_eq!(ks.set("a".into(), Bytes::from("val"), None), SetResult::Ok);

        // this should trigger eviction of "a" to make room
        assert_eq!(ks.set("b".into(), Bytes::from("val"), None), SetResult::Ok);

        // "a" should have been evicted
        assert!(!ks.exists("a"));
        assert!(ks.exists("b"));
    }

    #[test]
    fn safety_margin_rejects_near_raw_limit() {
        // one entry = 1 (key) + 3 (val) + 96 (overhead) = 100 bytes.
        // configure max_memory = 112. effective limit = 112 * 90 / 100 = 100.
        // the entry fills exactly the effective limit, so a second entry should
        // be rejected even though the raw limit has 12 bytes of headroom.
        let config = ShardConfig {
            max_memory: Some(112),
            eviction_policy: EvictionPolicy::NoEviction,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        assert_eq!(ks.set("a".into(), Bytes::from("val"), None), SetResult::Ok);

        let result = ks.set("b".into(), Bytes::from("val"), None);
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

        assert_eq!(ks.set("a".into(), Bytes::from("val"), None), SetResult::Ok);

        // overwriting with same-size value should succeed — no net increase
        assert_eq!(ks.set("a".into(), Bytes::from("new"), None), SetResult::Ok);
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

        assert_eq!(ks.set("a".into(), Bytes::from("val"), None), SetResult::Ok);

        // overwriting with a much larger value should fail if it exceeds limit
        let big_value = "x".repeat(200);
        let result = ks.set("a".into(), Bytes::from(big_value), None);
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
        ks.set("a".into(), Bytes::from("1"), None);
        ks.set("b".into(), Bytes::from("2"), Some(Duration::from_secs(100)));

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
        );
        ks.set("alive".into(), Bytes::from("here"), None);
        thread::sleep(Duration::from_millis(10));

        let entries: Vec<_> = ks.iter_entries().collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "alive");
    }

    #[test]
    fn iter_entries_ttl_for_no_expiry() {
        let mut ks = Keyspace::new();
        ks.set("permanent".into(), Bytes::from("val"), None);

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
        ks.set("key".into(), Bytes::from("old"), None);
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
                ks.set(format!("key:{i}"), Bytes::from("value"), None),
                SetResult::Ok
            );
        }
        assert_eq!(ks.len(), 100);
    }

    // -- list tests --

    #[test]
    fn lpush_creates_list() {
        let mut ks = Keyspace::new();
        let len = ks
            .lpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(len, 2);
        // lpush pushes each to front, so order is b, a
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(items, vec![Bytes::from("b"), Bytes::from("a")]);
    }

    #[test]
    fn rpush_creates_list() {
        let mut ks = Keyspace::new();
        let len = ks
            .rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(len, 2);
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(items, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn push_to_existing_list() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a")]).unwrap();
        let len = ks.rpush("list", &[Bytes::from("b")]).unwrap();
        assert_eq!(len, 2);
    }

    #[test]
    fn lpop_returns_front() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(ks.lpop("list").unwrap(), Some(Bytes::from("a")));
        assert_eq!(ks.lpop("list").unwrap(), Some(Bytes::from("b")));
        assert_eq!(ks.lpop("list").unwrap(), None); // empty, key deleted
    }

    #[test]
    fn rpop_returns_back() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(ks.rpop("list").unwrap(), Some(Bytes::from("b")));
    }

    #[test]
    fn pop_from_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.lpop("nope").unwrap(), None);
        assert_eq!(ks.rpop("nope").unwrap(), None);
    }

    #[test]
    fn empty_list_auto_deletes_key() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("only")]).unwrap();
        ks.lpop("list").unwrap();
        assert!(!ks.exists("list"));
        assert_eq!(ks.stats().key_count, 0);
        assert_eq!(ks.stats().used_bytes, 0);
    }

    #[test]
    fn lrange_negative_indices() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        )
        .unwrap();
        // -2 to -1 => last two elements
        let items = ks.lrange("list", -2, -1).unwrap();
        assert_eq!(items, vec![Bytes::from("b"), Bytes::from("c")]);
    }

    #[test]
    fn lrange_out_of_bounds_clamps() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        let items = ks.lrange("list", -100, 100).unwrap();
        assert_eq!(items, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn lrange_missing_key_returns_empty() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.lrange("nope", 0, -1).unwrap(), Vec::<Bytes>::new());
    }

    #[test]
    fn llen_returns_length() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.llen("nope").unwrap(), 0);
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(ks.llen("list").unwrap(), 2);
    }

    #[test]
    fn list_memory_tracked_on_push_pop() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("hello")]).unwrap();
        let after_push = ks.stats().used_bytes;
        assert!(after_push > 0);

        ks.rpush("list", &[Bytes::from("world")]).unwrap();
        let after_second = ks.stats().used_bytes;
        assert!(after_second > after_push);

        ks.lpop("list").unwrap();
        let after_pop = ks.stats().used_bytes;
        assert!(after_pop < after_second);
    }

    #[test]
    fn lpush_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None);
        assert!(ks.lpush("s", &[Bytes::from("nope")]).is_err());
    }

    #[test]
    fn lrange_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None);
        assert!(ks.lrange("s", 0, -1).is_err());
    }

    #[test]
    fn llen_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None);
        assert!(ks.llen("s").is_err());
    }

    // -- wrongtype tests --

    #[test]
    fn get_on_list_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        let mut list = std::collections::VecDeque::new();
        list.push_back(Bytes::from("item"));
        ks.restore("mylist".into(), Value::List(list), None);

        assert!(ks.get("mylist").is_err());
    }

    #[test]
    fn value_type_returns_correct_types() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.value_type("missing"), "none");

        ks.set("s".into(), Bytes::from("val"), None);
        assert_eq!(ks.value_type("s"), "string");

        let mut list = std::collections::VecDeque::new();
        list.push_back(Bytes::from("item"));
        ks.restore("l".into(), Value::List(list), None);
        assert_eq!(ks.value_type("l"), "list");

        ks.zadd("z", &[(1.0, "a".into())], &ZAddFlags::default())
            .unwrap();
        assert_eq!(ks.value_type("z"), "zset");
    }

    // -- sorted set tests --

    #[test]
    fn zadd_creates_sorted_set() {
        let mut ks = Keyspace::new();
        let result = ks
            .zadd(
                "board",
                &[(100.0, "alice".into()), (200.0, "bob".into())],
                &ZAddFlags::default(),
            )
            .unwrap();
        assert_eq!(result.count, 2);
        assert_eq!(result.applied.len(), 2);
        assert_eq!(ks.value_type("board"), "zset");
    }

    #[test]
    fn zadd_updates_existing_score() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(100.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        // update score — default flags don't count updates
        let result = ks
            .zadd("z", &[(200.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        assert_eq!(result.count, 0);
        // score was updated, so applied should have the member
        assert_eq!(result.applied.len(), 1);
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(200.0));
    }

    #[test]
    fn zadd_ch_flag_counts_changes() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(100.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        let flags = ZAddFlags {
            ch: true,
            ..Default::default()
        };
        let result = ks
            .zadd(
                "z",
                &[(200.0, "alice".into()), (50.0, "bob".into())],
                &flags,
            )
            .unwrap();
        // 1 updated + 1 added = 2
        assert_eq!(result.count, 2);
        assert_eq!(result.applied.len(), 2);
    }

    #[test]
    fn zadd_nx_skips_existing() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(100.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        let flags = ZAddFlags {
            nx: true,
            ..Default::default()
        };
        let result = ks.zadd("z", &[(999.0, "alice".into())], &flags).unwrap();
        assert_eq!(result.count, 0);
        assert!(result.applied.is_empty());
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(100.0));
    }

    #[test]
    fn zadd_xx_skips_new() {
        let mut ks = Keyspace::new();
        let flags = ZAddFlags {
            xx: true,
            ..Default::default()
        };
        let result = ks.zadd("z", &[(100.0, "alice".into())], &flags).unwrap();
        assert_eq!(result.count, 0);
        assert!(result.applied.is_empty());
        // key should be cleaned up since nothing was added
        assert_eq!(ks.value_type("z"), "none");
    }

    #[test]
    fn zadd_gt_only_increases() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(100.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        let flags = ZAddFlags {
            gt: true,
            ..Default::default()
        };
        ks.zadd("z", &[(50.0, "alice".into())], &flags).unwrap();
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(100.0));
        ks.zadd("z", &[(200.0, "alice".into())], &flags).unwrap();
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(200.0));
    }

    #[test]
    fn zadd_lt_only_decreases() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(100.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        let flags = ZAddFlags {
            lt: true,
            ..Default::default()
        };
        ks.zadd("z", &[(200.0, "alice".into())], &flags).unwrap();
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(100.0));
        ks.zadd("z", &[(50.0, "alice".into())], &flags).unwrap();
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(50.0));
    }

    #[test]
    fn zrem_removes_members() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into()), (3.0, "c".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        let removed = ks
            .zrem("z", &["a".into(), "c".into(), "nonexistent".into()])
            .unwrap();
        assert_eq!(removed.len(), 2);
        assert!(removed.contains(&"a".to_owned()));
        assert!(removed.contains(&"c".to_owned()));
        assert_eq!(ks.zscore("z", "a").unwrap(), None);
        assert_eq!(ks.zscore("z", "b").unwrap(), Some(2.0));
    }

    #[test]
    fn zrem_auto_deletes_empty() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(1.0, "only".into())], &ZAddFlags::default())
            .unwrap();
        ks.zrem("z", &["only".into()]).unwrap();
        assert!(!ks.exists("z"));
        assert_eq!(ks.stats().key_count, 0);
    }

    #[test]
    fn zrem_missing_key() {
        let mut ks = Keyspace::new();
        assert!(ks.zrem("nope", &["a".into()]).unwrap().is_empty());
    }

    #[test]
    fn zscore_returns_score() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(42.5, "member".into())], &ZAddFlags::default())
            .unwrap();
        assert_eq!(ks.zscore("z", "member").unwrap(), Some(42.5));
        assert_eq!(ks.zscore("z", "missing").unwrap(), None);
    }

    #[test]
    fn zscore_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.zscore("nope", "m").unwrap(), None);
    }

    #[test]
    fn zrank_returns_rank() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[
                (300.0, "c".into()),
                (100.0, "a".into()),
                (200.0, "b".into()),
            ],
            &ZAddFlags::default(),
        )
        .unwrap();
        assert_eq!(ks.zrank("z", "a").unwrap(), Some(0));
        assert_eq!(ks.zrank("z", "b").unwrap(), Some(1));
        assert_eq!(ks.zrank("z", "c").unwrap(), Some(2));
        assert_eq!(ks.zrank("z", "d").unwrap(), None);
    }

    #[test]
    fn zrange_returns_range() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into()), (3.0, "c".into())],
            &ZAddFlags::default(),
        )
        .unwrap();

        let all = ks.zrange("z", 0, -1).unwrap();
        assert_eq!(
            all,
            vec![
                ("a".to_owned(), 1.0),
                ("b".to_owned(), 2.0),
                ("c".to_owned(), 3.0),
            ]
        );

        let middle = ks.zrange("z", 1, 1).unwrap();
        assert_eq!(middle, vec![("b".to_owned(), 2.0)]);

        let last_two = ks.zrange("z", -2, -1).unwrap();
        assert_eq!(last_two, vec![("b".to_owned(), 2.0), ("c".to_owned(), 3.0)]);
    }

    #[test]
    fn zrange_missing_key() {
        let mut ks = Keyspace::new();
        assert!(ks.zrange("nope", 0, -1).unwrap().is_empty());
    }

    #[test]
    fn zadd_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None);
        assert!(ks
            .zadd("s", &[(1.0, "m".into())], &ZAddFlags::default())
            .is_err());
    }

    #[test]
    fn zrem_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None);
        assert!(ks.zrem("s", &["m".into()]).is_err());
    }

    #[test]
    fn zscore_on_list_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.rpush("l", &[Bytes::from("item")]).unwrap();
        assert!(ks.zscore("l", "m").is_err());
    }

    #[test]
    fn zrank_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None);
        assert!(ks.zrank("s", "m").is_err());
    }

    #[test]
    fn zrange_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None);
        assert!(ks.zrange("s", 0, -1).is_err());
    }

    #[test]
    fn sorted_set_memory_tracked() {
        let mut ks = Keyspace::new();
        let before = ks.stats().used_bytes;
        ks.zadd("z", &[(1.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        let after_add = ks.stats().used_bytes;
        assert!(after_add > before);

        ks.zadd("z", &[(2.0, "bob".into())], &ZAddFlags::default())
            .unwrap();
        let after_second = ks.stats().used_bytes;
        assert!(after_second > after_add);

        ks.zrem("z", &["alice".into()]).unwrap();
        let after_remove = ks.stats().used_bytes;
        assert!(after_remove < after_second);
    }

    #[test]
    fn incr_new_key_defaults_to_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.incr("counter").unwrap(), 1);
        // verify the stored value
        match ks.get("counter").unwrap() {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("1")),
            other => panic!("expected String(\"1\"), got {other:?}"),
        }
    }

    #[test]
    fn incr_existing_value() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None);
        assert_eq!(ks.incr("n").unwrap(), 11);
    }

    #[test]
    fn decr_new_key_defaults_to_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.decr("counter").unwrap(), -1);
    }

    #[test]
    fn decr_existing_value() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None);
        assert_eq!(ks.decr("n").unwrap(), 9);
    }

    #[test]
    fn incr_non_integer_returns_error() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("notanum"), None);
        assert_eq!(ks.incr("s").unwrap_err(), IncrError::NotAnInteger);
    }

    #[test]
    fn incr_on_list_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.lpush("list", &[Bytes::from("a")]).unwrap();
        assert_eq!(ks.incr("list").unwrap_err(), IncrError::WrongType);
    }

    #[test]
    fn incr_overflow_returns_error() {
        let mut ks = Keyspace::new();
        ks.set("max".into(), Bytes::from(i64::MAX.to_string()), None);
        assert_eq!(ks.incr("max").unwrap_err(), IncrError::Overflow);
    }

    #[test]
    fn decr_overflow_returns_error() {
        let mut ks = Keyspace::new();
        ks.set("min".into(), Bytes::from(i64::MIN.to_string()), None);
        assert_eq!(ks.decr("min").unwrap_err(), IncrError::Overflow);
    }

    #[test]
    fn incr_preserves_ttl() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("5"), Some(Duration::from_secs(60)));
        ks.incr("n").unwrap();
        match ks.ttl("n") {
            TtlResult::Seconds(s) => assert!((58..=60).contains(&s)),
            other => panic!("expected TTL preserved, got {other:?}"),
        }
    }

    #[test]
    fn zrem_returns_actually_removed_members() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        // "a" exists, "ghost" doesn't — only "a" should be in the result
        let removed = ks.zrem("z", &["a".into(), "ghost".into()]).unwrap();
        assert_eq!(removed, vec!["a".to_owned()]);
    }

    #[test]
    fn zcard_returns_count() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        assert_eq!(ks.zcard("z").unwrap(), 2);
    }

    #[test]
    fn zcard_missing_key_returns_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.zcard("missing").unwrap(), 0);
    }

    #[test]
    fn zcard_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None);
        assert!(ks.zcard("s").is_err());
    }

    #[test]
    fn persist_removes_expiry() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::from_secs(60)),
        );
        assert!(matches!(ks.ttl("key"), TtlResult::Seconds(_)));

        assert!(ks.persist("key"));
        assert_eq!(ks.ttl("key"), TtlResult::NoExpiry);
        assert_eq!(ks.stats().keys_with_expiry, 0);
    }

    #[test]
    fn persist_returns_false_without_expiry() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None);
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
        );
        match ks.pttl("key") {
            TtlResult::Milliseconds(ms) => assert!(ms > 59_000 && ms <= 60_000),
            other => panic!("expected Milliseconds, got {other:?}"),
        }
    }

    #[test]
    fn pttl_no_expiry() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None);
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
        ks.set("key".into(), Bytes::from("val"), None);
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
        );
        assert!(ks.pexpire("key", 500));
        match ks.pttl("key") {
            TtlResult::Milliseconds(ms) => assert!(ms <= 500),
            other => panic!("expected Milliseconds, got {other:?}"),
        }
        // expiry count shouldn't double-count
        assert_eq!(ks.stats().keys_with_expiry, 1);
    }

    // -- memory limit enforcement for list/zset --

    #[test]
    fn lpush_rejects_when_memory_full() {
        let config = ShardConfig {
            max_memory: Some(150),
            eviction_policy: EvictionPolicy::NoEviction,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        // first key eats up most of the budget
        assert_eq!(ks.set("a".into(), Bytes::from("val"), None), SetResult::Ok);

        // lpush should be rejected — not enough room
        let result = ks.lpush("list", &[Bytes::from("big-value-here")]);
        assert_eq!(result, Err(WriteError::OutOfMemory));

        // original key should be untouched
        assert!(ks.exists("a"));
    }

    #[test]
    fn rpush_rejects_when_memory_full() {
        let config = ShardConfig {
            max_memory: Some(150),
            eviction_policy: EvictionPolicy::NoEviction,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        assert_eq!(ks.set("a".into(), Bytes::from("val"), None), SetResult::Ok);

        let result = ks.rpush("list", &[Bytes::from("big-value-here")]);
        assert_eq!(result, Err(WriteError::OutOfMemory));
    }

    #[test]
    fn zadd_rejects_when_memory_full() {
        let config = ShardConfig {
            max_memory: Some(150),
            eviction_policy: EvictionPolicy::NoEviction,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        assert_eq!(ks.set("a".into(), Bytes::from("val"), None), SetResult::Ok);

        let result = ks.zadd("z", &[(1.0, "member".into())], &ZAddFlags::default());
        assert!(matches!(result, Err(WriteError::OutOfMemory)));

        // original key should be untouched
        assert!(ks.exists("a"));
    }

    #[test]
    fn lpush_evicts_under_lru_policy() {
        let config = ShardConfig {
            max_memory: Some(200),
            eviction_policy: EvictionPolicy::AllKeysLru,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        assert_eq!(ks.set("a".into(), Bytes::from("val"), None), SetResult::Ok);

        // should evict "a" to make room for the list
        assert!(ks.lpush("list", &[Bytes::from("item")]).is_ok());
        assert!(!ks.exists("a"));
    }

    #[test]
    fn clear_removes_all_keys() {
        let mut ks = Keyspace::new();
        ks.set("a".into(), Bytes::from("1"), None);
        ks.set("b".into(), Bytes::from("2"), Some(Duration::from_secs(60)));
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
        ks.set("key1".into(), Bytes::from("a"), None);
        ks.set("key2".into(), Bytes::from("b"), None);
        ks.set("key3".into(), Bytes::from("c"), None);

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
        ks.set("user:1".into(), Bytes::from("a"), None);
        ks.set("user:2".into(), Bytes::from("b"), None);
        ks.set("item:1".into(), Bytes::from("c"), None);

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
            ks.set(format!("k{i}"), Bytes::from("v"), None);
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
        ks.set("live".into(), Bytes::from("a"), None);
        ks.set(
            "expired".into(),
            Bytes::from("b"),
            Some(Duration::from_millis(1)),
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

    // --- hash tests ---

    #[test]
    fn hset_creates_hash() {
        let mut ks = Keyspace::new();
        let count = ks
            .hset("h", &[("field1".into(), Bytes::from("value1"))])
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(ks.value_type("h"), "hash");
    }

    #[test]
    fn hset_returns_new_field_count() {
        let mut ks = Keyspace::new();
        // add two new fields
        let count = ks
            .hset(
                "h",
                &[
                    ("f1".into(), Bytes::from("v1")),
                    ("f2".into(), Bytes::from("v2")),
                ],
            )
            .unwrap();
        assert_eq!(count, 2);

        // update one, add one new
        let count = ks
            .hset(
                "h",
                &[
                    ("f1".into(), Bytes::from("updated")),
                    ("f3".into(), Bytes::from("v3")),
                ],
            )
            .unwrap();
        assert_eq!(count, 1); // only f3 is new
    }

    #[test]
    fn hget_returns_value() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("name".into(), Bytes::from("alice"))])
            .unwrap();
        let val = ks.hget("h", "name").unwrap();
        assert_eq!(val, Some(Bytes::from("alice")));
    }

    #[test]
    fn hget_missing_field_returns_none() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("a".into(), Bytes::from("1"))]).unwrap();
        assert_eq!(ks.hget("h", "b").unwrap(), None);
    }

    #[test]
    fn hget_missing_key_returns_none() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.hget("missing", "field").unwrap(), None);
    }

    #[test]
    fn hgetall_returns_all_fields() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
            ],
        )
        .unwrap();
        let mut fields = ks.hgetall("h").unwrap();
        fields.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], ("a".into(), Bytes::from("1")));
        assert_eq!(fields[1], ("b".into(), Bytes::from("2")));
    }

    #[test]
    fn hdel_removes_fields() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
                ("c".into(), Bytes::from("3")),
            ],
        )
        .unwrap();
        let removed = ks.hdel("h", &["a".into(), "c".into()]).unwrap();
        assert_eq!(removed.len(), 2);
        assert!(removed.contains(&"a".into()));
        assert!(removed.contains(&"c".into()));
        assert_eq!(ks.hlen("h").unwrap(), 1);
    }

    #[test]
    fn hdel_auto_deletes_empty_hash() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("only".into(), Bytes::from("field"))])
            .unwrap();
        ks.hdel("h", &["only".into()]).unwrap();
        assert_eq!(ks.value_type("h"), "none");
    }

    #[test]
    fn hexists_returns_true_for_existing_field() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("field".into(), Bytes::from("val"))])
            .unwrap();
        assert!(ks.hexists("h", "field").unwrap());
    }

    #[test]
    fn hexists_returns_false_for_missing_field() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("a".into(), Bytes::from("1"))]).unwrap();
        assert!(!ks.hexists("h", "missing").unwrap());
    }

    #[test]
    fn hlen_returns_field_count() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
            ],
        )
        .unwrap();
        assert_eq!(ks.hlen("h").unwrap(), 2);
    }

    #[test]
    fn hlen_missing_key_returns_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.hlen("missing").unwrap(), 0);
    }

    #[test]
    fn hincrby_new_field() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("x".into(), Bytes::from("ignored"))])
            .unwrap();
        let val = ks.hincrby("h", "counter", 5).unwrap();
        assert_eq!(val, 5);
    }

    #[test]
    fn hincrby_existing_field() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("n".into(), Bytes::from("10"))]).unwrap();
        let val = ks.hincrby("h", "n", 3).unwrap();
        assert_eq!(val, 13);
    }

    #[test]
    fn hincrby_negative_delta() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("n".into(), Bytes::from("10"))]).unwrap();
        let val = ks.hincrby("h", "n", -7).unwrap();
        assert_eq!(val, 3);
    }

    #[test]
    fn hincrby_non_integer_returns_error() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("s".into(), Bytes::from("notanumber"))])
            .unwrap();
        assert_eq!(
            ks.hincrby("h", "s", 1).unwrap_err(),
            IncrError::NotAnInteger
        );
    }

    #[test]
    fn hkeys_returns_field_names() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("alpha".into(), Bytes::from("1")),
                ("beta".into(), Bytes::from("2")),
            ],
        )
        .unwrap();
        let mut keys = ks.hkeys("h").unwrap();
        keys.sort();
        assert_eq!(keys, vec!["alpha", "beta"]);
    }

    #[test]
    fn hvals_returns_values() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("x")),
                ("b".into(), Bytes::from("y")),
            ],
        )
        .unwrap();
        let mut vals = ks.hvals("h").unwrap();
        vals.sort();
        assert_eq!(vals, vec![Bytes::from("x"), Bytes::from("y")]);
    }

    #[test]
    fn hmget_returns_values_for_existing_fields() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
            ],
        )
        .unwrap();
        let vals = ks
            .hmget("h", &["a".into(), "missing".into(), "b".into()])
            .unwrap();
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0], Some(Bytes::from("1")));
        assert_eq!(vals[1], None);
        assert_eq!(vals[2], Some(Bytes::from("2")));
    }

    #[test]
    fn hash_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("string"), None);
        assert!(ks.hset("s", &[("f".into(), Bytes::from("v"))]).is_err());
        assert!(ks.hget("s", "f").is_err());
        assert!(ks.hgetall("s").is_err());
        assert!(ks.hdel("s", &["f".into()]).is_err());
        assert!(ks.hexists("s", "f").is_err());
        assert!(ks.hlen("s").is_err());
        assert!(ks.hincrby("s", "f", 1).is_err());
        assert!(ks.hkeys("s").is_err());
        assert!(ks.hvals("s").is_err());
        assert!(ks.hmget("s", &["f".into()]).is_err());
    }

    // --- set tests ---

    #[test]
    fn sadd_creates_set() {
        let mut ks = Keyspace::new();
        let added = ks.sadd("s", &["a".into(), "b".into()]).unwrap();
        assert_eq!(added, 2);
        assert_eq!(ks.value_type("s"), "set");
    }

    #[test]
    fn sadd_returns_new_member_count() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into()]).unwrap();
        // add one existing, one new
        let added = ks.sadd("s", &["b".into(), "c".into()]).unwrap();
        assert_eq!(added, 1); // only "c" is new
    }

    #[test]
    fn srem_removes_members() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into(), "c".into()]).unwrap();
        let removed = ks.srem("s", &["a".into(), "c".into()]).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(ks.scard("s").unwrap(), 1);
    }

    #[test]
    fn srem_auto_deletes_empty_set() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["only".into()]).unwrap();
        ks.srem("s", &["only".into()]).unwrap();
        assert_eq!(ks.value_type("s"), "none");
    }

    #[test]
    fn smembers_returns_all_members() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into(), "c".into()]).unwrap();
        let mut members = ks.smembers("s").unwrap();
        members.sort();
        assert_eq!(members, vec!["a", "b", "c"]);
    }

    #[test]
    fn smembers_missing_key_returns_empty() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.smembers("missing").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn sismember_returns_true_for_existing() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["member".into()]).unwrap();
        assert!(ks.sismember("s", "member").unwrap());
    }

    #[test]
    fn sismember_returns_false_for_missing() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into()]).unwrap();
        assert!(!ks.sismember("s", "missing").unwrap());
    }

    #[test]
    fn scard_returns_count() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into(), "c".into()]).unwrap();
        assert_eq!(ks.scard("s").unwrap(), 3);
    }

    #[test]
    fn scard_missing_key_returns_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.scard("missing").unwrap(), 0);
    }

    #[test]
    fn set_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("string"), None);
        assert!(ks.sadd("s", &["m".into()]).is_err());
        assert!(ks.srem("s", &["m".into()]).is_err());
        assert!(ks.smembers("s").is_err());
        assert!(ks.sismember("s", "m").is_err());
        assert!(ks.scard("s").is_err());
    }

    // --- edge case tests ---

    #[test]
    fn zero_ttl_expires_immediately() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), Some(Duration::ZERO));

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
        );

        std::thread::sleep(Duration::from_millis(5));
        assert!(ks.get("key").unwrap().is_none());
    }

    #[test]
    fn list_auto_deleted_when_empty() {
        let mut ks = Keyspace::new();
        ks.lpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(ks.len(), 1);

        // pop all elements
        let _ = ks.lpop("list");
        let _ = ks.lpop("list");

        // list should be auto-deleted
        assert_eq!(ks.len(), 0);
        assert!(!ks.exists("list"));
    }

    #[test]
    fn set_auto_deleted_when_empty() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into()]).unwrap();
        assert_eq!(ks.len(), 1);

        // remove all members
        ks.srem("s", &["a".into(), "b".into()]).unwrap();

        // set should be auto-deleted
        assert_eq!(ks.len(), 0);
        assert!(!ks.exists("s"));
    }

    #[test]
    fn hash_auto_deleted_when_empty() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("f1".into(), Bytes::from("v1")),
                ("f2".into(), Bytes::from("v2")),
            ],
        )
        .unwrap();
        assert_eq!(ks.len(), 1);

        // delete all fields
        ks.hdel("h", &["f1".into(), "f2".into()]).unwrap();

        // hash should be auto-deleted
        assert_eq!(ks.len(), 0);
        assert!(!ks.exists("h"));
    }

    #[test]
    fn sadd_duplicate_members_counted_once() {
        let mut ks = Keyspace::new();
        // add same member twice in one call
        let count = ks.sadd("s", &["a".into(), "a".into()]).unwrap();
        // should only count as 1 new member
        assert_eq!(count, 1);
        assert_eq!(ks.scard("s").unwrap(), 1);
    }

    #[test]
    fn srem_non_existent_member_returns_zero() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into()]).unwrap();
        let removed = ks.srem("s", &["nonexistent".into()]).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn hincrby_overflow_returns_error() {
        let mut ks = Keyspace::new();
        // set field to near max
        ks.hset("h", &[("count".into(), Bytes::from(i64::MAX.to_string()))])
            .unwrap();

        // try to increment by 1 - should overflow
        let result = ks.hincrby("h", "count", 1);
        assert!(result.is_err());
    }

    #[test]
    fn hincrby_on_non_integer_returns_error() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("field".into(), Bytes::from("not_a_number"))])
            .unwrap();

        let result = ks.hincrby("h", "field", 1);
        assert!(result.is_err());
    }

    #[test]
    fn incr_at_max_value_overflows() {
        let mut ks = Keyspace::new();
        ks.set("counter".into(), Bytes::from(i64::MAX.to_string()), None);

        let result = ks.incr("counter");
        assert!(matches!(result, Err(IncrError::Overflow)));
    }

    #[test]
    fn decr_at_min_value_underflows() {
        let mut ks = Keyspace::new();
        ks.set("counter".into(), Bytes::from(i64::MIN.to_string()), None);

        let result = ks.decr("counter");
        assert!(matches!(result, Err(IncrError::Overflow)));
    }

    #[test]
    fn lrange_inverted_start_stop_returns_empty() {
        let mut ks = Keyspace::new();
        ks.lpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        )
        .unwrap();

        // start > stop with positive indices
        let result = ks.lrange("list", 2, 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn lrange_large_stop_clamps_to_len() {
        let mut ks = Keyspace::new();
        ks.lpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();

        // large indices should clamp to list bounds
        let result = ks.lrange("list", 0, 1000).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn empty_string_key_works() {
        let mut ks = Keyspace::new();
        ks.set("".into(), Bytes::from("value"), None);
        assert_eq!(
            ks.get("").unwrap(),
            Some(Value::String(Bytes::from("value")))
        );
        assert!(ks.exists(""));
    }

    #[test]
    fn empty_value_works() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from(""), None);
        assert_eq!(ks.get("key").unwrap(), Some(Value::String(Bytes::from(""))));
    }

    #[test]
    fn binary_data_in_value() {
        let mut ks = Keyspace::new();
        // value with null bytes and other binary data
        let binary = Bytes::from(vec![0u8, 1, 2, 255, 0, 128]);
        ks.set("binary".into(), binary.clone(), None);
        assert_eq!(ks.get("binary").unwrap(), Some(Value::String(binary)));
    }

    #[test]
    fn incr_by_float_basic() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10.5"), None);
        let result = ks.incr_by_float("n", 2.3).unwrap();
        let f: f64 = result.parse().unwrap();
        assert!((f - 12.8).abs() < 0.001);
    }

    #[test]
    fn incr_by_float_new_key() {
        let mut ks = Keyspace::new();
        let result = ks.incr_by_float("new", 2.72).unwrap();
        let f: f64 = result.parse().unwrap();
        assert!((f - 2.72).abs() < 0.001);
    }

    #[test]
    fn incr_by_float_negative() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None);
        let result = ks.incr_by_float("n", -3.5).unwrap();
        let f: f64 = result.parse().unwrap();
        assert!((f - 6.5).abs() < 0.001);
    }

    #[test]
    fn incr_by_float_wrong_type() {
        let mut ks = Keyspace::new();
        ks.lpush("mylist", &[Bytes::from("a")]).unwrap();
        let err = ks.incr_by_float("mylist", 1.0).unwrap_err();
        assert_eq!(err, IncrFloatError::WrongType);
    }

    #[test]
    fn incr_by_float_not_a_float() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("hello"), None);
        let err = ks.incr_by_float("s", 1.0).unwrap_err();
        assert_eq!(err, IncrFloatError::NotAFloat);
    }

    #[test]
    fn append_to_existing_key() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("hello"), None);
        let len = ks.append("key", b" world").unwrap();
        assert_eq!(len, 11);
        assert_eq!(
            ks.get("key").unwrap(),
            Some(Value::String(Bytes::from("hello world")))
        );
    }

    #[test]
    fn append_to_new_key() {
        let mut ks = Keyspace::new();
        let len = ks.append("new", b"value").unwrap();
        assert_eq!(len, 5);
        assert_eq!(
            ks.get("new").unwrap(),
            Some(Value::String(Bytes::from("value")))
        );
    }

    #[test]
    fn append_wrong_type() {
        let mut ks = Keyspace::new();
        ks.lpush("mylist", &[Bytes::from("a")]).unwrap();
        let err = ks.append("mylist", b"value").unwrap_err();
        assert_eq!(err, WriteError::WrongType);
    }

    #[test]
    fn strlen_existing_key() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("hello"), None);
        assert_eq!(ks.strlen("key").unwrap(), 5);
    }

    #[test]
    fn strlen_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.strlen("missing").unwrap(), 0);
    }

    #[test]
    fn strlen_wrong_type() {
        let mut ks = Keyspace::new();
        ks.lpush("mylist", &[Bytes::from("a")]).unwrap();
        let err = ks.strlen("mylist").unwrap_err();
        assert_eq!(err, WrongType);
    }

    #[test]
    fn format_float_integers() {
        assert_eq!(super::format_float(10.0), "10");
        assert_eq!(super::format_float(0.0), "0");
        assert_eq!(super::format_float(-5.0), "-5");
    }

    #[test]
    fn format_float_decimals() {
        assert_eq!(super::format_float(2.72), "2.72");
        assert_eq!(super::format_float(10.5), "10.5");
    }

    // --- keys tests ---

    #[test]
    fn keys_match_all() {
        let mut ks = Keyspace::new();
        ks.set("a".into(), Bytes::from("1"), None);
        ks.set("b".into(), Bytes::from("2"), None);
        ks.set("c".into(), Bytes::from("3"), None);
        let mut result = ks.keys("*");
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn keys_with_pattern() {
        let mut ks = Keyspace::new();
        ks.set("user:1".into(), Bytes::from("a"), None);
        ks.set("user:2".into(), Bytes::from("b"), None);
        ks.set("item:1".into(), Bytes::from("c"), None);
        let mut result = ks.keys("user:*");
        result.sort();
        assert_eq!(result, vec!["user:1", "user:2"]);
    }

    #[test]
    fn keys_skips_expired() {
        let mut ks = Keyspace::new();
        ks.set("live".into(), Bytes::from("a"), None);
        ks.set(
            "dead".into(),
            Bytes::from("b"),
            Some(Duration::from_millis(1)),
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
        ks.set("old".into(), Bytes::from("value"), None);
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
        ks.set("src".into(), Bytes::from("new_val"), None);
        ks.set("dst".into(), Bytes::from("old_val"), None);
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
        ks.set("key".into(), Bytes::from("val"), None);
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
        ks.set("old".into(), Bytes::from("value"), None);
        let before = ks.stats().used_bytes;
        ks.rename("old", "new").unwrap();
        let after = ks.stats().used_bytes;
        // same key length, so memory should be the same
        assert_eq!(before, after);
        assert_eq!(ks.stats().key_count, 1);
    }
}
