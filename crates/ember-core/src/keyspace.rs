//! The keyspace: Ember's core key-value store.
//!
//! A `Keyspace` owns a flat `HashMap<String, Entry>` and handles
//! get, set, delete, existence checks, and TTL management. Expired
//! keys are removed lazily on access. Memory usage is tracked on
//! every mutation for eviction and stats reporting.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::seq::IteratorRandom;

use crate::memory::{self, MemoryTracker};
use crate::types::Value;

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
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) value: Value,
    pub(crate) expires_at: Option<Instant>,
    pub(crate) last_access: Instant,
}

impl Entry {
    fn new(value: Value, expires_at: Option<Instant>) -> Self {
        Self {
            value,
            expires_at,
            last_access: Instant::now(),
        }
    }

    /// Returns `true` if this entry has passed its expiration time.
    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(deadline) => Instant::now() >= deadline,
            None => false,
        }
    }

    /// Marks this entry as accessed right now.
    fn touch(&mut self) {
        self.last_access = Instant::now();
    }
}

/// Result of a TTL query, matching Redis semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TtlResult {
    /// Key exists and has a TTL. Returns remaining seconds.
    Seconds(u64),
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
}

/// Number of random keys to sample when looking for an eviction candidate.
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
        }
    }

    /// Retrieves the value for `key`, or `None` if the key doesn't exist
    /// or has expired.
    ///
    /// Expired keys are removed lazily on access. Successful reads update
    /// the entry's last access time for LRU tracking.
    pub fn get(&mut self, key: &str) -> Option<Value> {
        if self.remove_if_expired(key) {
            return None;
        }
        self.entries.get_mut(key).map(|e| {
            e.touch();
            e.value.clone()
        })
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
        let expires_at = expire.map(|d| Instant::now() + d);
        let new_value = Value::String(value);

        // check memory limit — for overwrites, only the net increase matters
        if let Some(max) = self.config.max_memory {
            let new_size = memory::entry_size(&key, &new_value);
            let old_size = self
                .entries
                .get(&key)
                .map(|e| memory::entry_size(&key, &e.value))
                .unwrap_or(0);
            let net_increase = new_size.saturating_sub(old_size);

            while self.memory.used_bytes() + net_increase > max {
                match self.config.eviction_policy {
                    EvictionPolicy::NoEviction => return SetResult::OutOfMemory,
                    EvictionPolicy::AllKeysLru => {
                        if !self.try_evict() {
                            return SetResult::OutOfMemory;
                        }
                    }
                }
            }
        }

        if let Some(old_entry) = self.entries.get(&key) {
            self.memory.replace(&key, &old_entry.value, &new_value);
            // adjust expiry count if the TTL status changed
            let had_expiry = old_entry.expires_at.is_some();
            let has_expiry = expires_at.is_some();
            match (had_expiry, has_expiry) {
                (false, true) => self.expiry_count += 1,
                (true, false) => self.expiry_count = self.expiry_count.saturating_sub(1),
                _ => {}
            }
        } else {
            self.memory.add(&key, &new_value);
            if expires_at.is_some() {
                self.expiry_count += 1;
            }
        }

        self.entries.insert(key, Entry::new(new_value, expires_at));
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

        let mut rng = rand::thread_rng();

        // randomly sample keys and find the least recently accessed one
        let victim = self
            .entries
            .iter()
            .choose_multiple(&mut rng, EVICTION_SAMPLE_SIZE)
            .into_iter()
            .min_by_key(|(_, entry)| entry.last_access)
            .map(|(k, _)| k.clone());

        if let Some(key) = victim {
            if let Some(entry) = self.entries.remove(&key) {
                self.memory.remove(&key, &entry.value);
                if entry.expires_at.is_some() {
                    self.expiry_count = self.expiry_count.saturating_sub(1);
                }
                return true;
            }
        }
        false
    }

    /// Removes a key. Returns `true` if the key existed (and wasn't expired).
    pub fn del(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        if let Some(entry) = self.entries.remove(key) {
            self.memory.remove(key, &entry.value);
            if entry.expires_at.is_some() {
                self.expiry_count = self.expiry_count.saturating_sub(1);
            }
            true
        } else {
            false
        }
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
                if entry.expires_at.is_none() {
                    self.expiry_count += 1;
                }
                entry.expires_at = Some(Instant::now() + Duration::from_secs(seconds));
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
            Some(entry) => match entry.expires_at {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    TtlResult::Seconds(remaining.as_secs())
                }
                None => TtlResult::NoExpiry,
            },
            None => TtlResult::NotFound,
        }
    }

    /// Returns aggregated stats for this keyspace.
    ///
    /// All fields are tracked incrementally — this is O(1).
    pub fn stats(&self) -> KeyspaceStats {
        KeyspaceStats {
            key_count: self.memory.key_count(),
            used_bytes: self.memory.used_bytes(),
            keys_with_expiry: self.expiry_count,
        }
    }

    /// Returns the number of live keys.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the keyspace has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates over all live (non-expired) entries, yielding the key, a
    /// clone of the value, and the remaining TTL in milliseconds (-1 for
    /// entries with no expiration). Used by snapshot and AOF rewrite.
    pub fn iter_entries(&self) -> impl Iterator<Item = (&str, &Value, i64)> {
        let now = Instant::now();
        self.entries.iter().filter_map(move |(key, entry)| {
            if entry.is_expired() {
                return None;
            }
            let ttl_ms = match entry.expires_at {
                Some(deadline) => {
                    let remaining = deadline.saturating_duration_since(now);
                    remaining.as_millis() as i64
                }
                None => -1,
            };
            Some((key.as_str(), &entry.value, ttl_ms))
        })
    }

    /// Restores an entry during recovery, bypassing memory limits.
    ///
    /// If `expires_at` is in the past, the entry is silently skipped.
    /// This is used only during shard startup when loading from
    /// snapshot/AOF — normal writes should go through `set()`.
    pub fn restore(
        &mut self,
        key: String,
        value: Value,
        expires_at: Option<Instant>,
    ) {
        // skip entries that already expired
        if let Some(deadline) = expires_at {
            if Instant::now() >= deadline {
                return;
            }
        }

        // if replacing an existing entry, adjust memory tracking
        if let Some(old) = self.entries.get(&key) {
            self.memory.replace(&key, &old.value, &value);
            let had_expiry = old.expires_at.is_some();
            let has_expiry = expires_at.is_some();
            match (had_expiry, has_expiry) {
                (false, true) => self.expiry_count += 1,
                (true, false) => self.expiry_count = self.expiry_count.saturating_sub(1),
                _ => {}
            }
        } else {
            self.memory.add(&key, &value);
            if expires_at.is_some() {
                self.expiry_count += 1;
            }
        }

        self.entries.insert(key, Entry::new(value, expires_at));
    }

    /// Randomly samples up to `count` keys and removes any that have expired.
    ///
    /// Returns the number of keys actually removed. Used by the active
    /// expiration cycle to clean up keys that no one is reading.
    pub fn expire_sample(&mut self, count: usize) -> usize {
        if self.entries.is_empty() {
            return 0;
        }

        let mut rng = rand::thread_rng();

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
                if entry.expires_at.is_some() {
                    self.expiry_count = self.expiry_count.saturating_sub(1);
                }
            }
        }
        expired
    }
}

impl Default for Keyspace {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn set_and_get() {
        let mut ks = Keyspace::new();
        ks.set("hello".into(), Bytes::from("world"), None);
        assert_eq!(ks.get("hello"), Some(Value::String(Bytes::from("world"))));
    }

    #[test]
    fn get_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.get("nope"), None);
    }

    #[test]
    fn overwrite_replaces_value() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("first"), None);
        ks.set("key".into(), Bytes::from("second"), None);
        assert_eq!(ks.get("key"), Some(Value::String(Bytes::from("second"))));
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
        assert_eq!(ks.get("key"), None);
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
        assert_eq!(ks.get("temp"), None);
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
            TtlResult::Seconds(s) => assert!(s >= 98 && s <= 100),
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
            TtlResult::Seconds(s) => assert!(s >= 58 && s <= 60),
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
        ks.get("temp");
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
        assert_eq!(ks.get("a"), Some(Value::String(Bytes::from("new"))));
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
        assert_eq!(ks.get("a"), Some(Value::String(Bytes::from("val"))));
    }

    // -- iter_entries tests --

    #[test]
    fn iter_entries_returns_live_entries() {
        let mut ks = Keyspace::new();
        ks.set("a".into(), Bytes::from("1"), None);
        ks.set(
            "b".into(),
            Bytes::from("2"),
            Some(Duration::from_secs(100)),
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
        ks.restore(
            "restored".into(),
            Value::String(Bytes::from("data")),
            None,
        );
        assert_eq!(
            ks.get("restored"),
            Some(Value::String(Bytes::from("data")))
        );
        assert_eq!(ks.stats().key_count, 1);
    }

    #[test]
    fn restore_skips_past_deadline() {
        let mut ks = Keyspace::new();
        // deadline already passed
        let past = Instant::now() - Duration::from_secs(1);
        ks.restore("expired".into(), Value::String(Bytes::from("old")), Some(past));
        assert!(ks.is_empty());
    }

    #[test]
    fn restore_overwrites_existing() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("old"), None);
        ks.restore(
            "key".into(),
            Value::String(Bytes::from("new")),
            None,
        );
        assert_eq!(ks.get("key"), Some(Value::String(Bytes::from("new"))));
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
}
