//! The keyspace: Ember's core key-value store.
//!
//! A `Keyspace` owns a flat `HashMap<String, Entry>` and handles
//! get, set, delete, existence checks, and TTL management. Expired
//! keys are removed lazily on access. Memory usage is tracked on
//! every mutation for eviction and stats reporting.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::memory::MemoryTracker;
use crate::types::Value;

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

/// The core key-value store.
///
/// All operations are single-threaded per shard — no internal locking.
/// Memory usage is tracked incrementally on every mutation.
pub struct Keyspace {
    entries: HashMap<String, Entry>,
    memory: MemoryTracker,
}

impl Keyspace {
    /// Creates a new, empty keyspace.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            memory: MemoryTracker::new(),
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
    pub fn set(&mut self, key: String, value: Bytes, expire: Option<Duration>) {
        let expires_at = expire.map(|d| Instant::now() + d);
        let new_value = Value::String(value);

        if let Some(old_entry) = self.entries.get(&key) {
            self.memory.replace(&key, &old_entry.value, &new_value);
        } else {
            self.memory.add(&key, &new_value);
        }

        self.entries.insert(key, Entry::new(new_value, expires_at));
    }

    /// Removes a key. Returns `true` if the key existed (and wasn't expired).
    pub fn del(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        if let Some(entry) = self.entries.remove(key) {
            self.memory.remove(key, &entry.value);
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
    pub fn stats(&self) -> KeyspaceStats {
        let keys_with_expiry = self
            .entries
            .values()
            .filter(|e| e.expires_at.is_some())
            .count();

        KeyspaceStats {
            key_count: self.memory.key_count(),
            used_bytes: self.memory.used_bytes(),
            keys_with_expiry,
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

    /// Samples up to `count` random keys and removes any that have expired.
    ///
    /// Returns the number of keys actually removed. Used by the active
    /// expiration cycle to clean up keys that no one is reading.
    pub fn expire_sample(&mut self, count: usize) -> usize {
        if self.entries.is_empty() {
            return 0;
        }

        // collect keys to check — we sample by iterating (HashMap order
        // is effectively random due to hashing)
        let keys_to_check: Vec<String> = self
            .entries
            .keys()
            .take(count)
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
        assert_eq!(
            ks.get("key"),
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
        ks.set(
            "b".into(),
            Bytes::from("2"),
            Some(Duration::from_secs(100)),
        );
        ks.set(
            "c".into(),
            Bytes::from("3"),
            Some(Duration::from_secs(200)),
        );

        let stats = ks.stats();
        assert_eq!(stats.key_count, 3);
        assert_eq!(stats.keys_with_expiry, 2);
    }
}
