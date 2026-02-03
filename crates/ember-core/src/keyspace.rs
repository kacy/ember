//! The keyspace: Ember's core key-value store.
//!
//! A `Keyspace` owns a flat `HashMap<String, Entry>` and handles
//! get, set, delete, existence checks, and TTL management. Expired
//! keys are removed lazily on access — no background threads needed
//! at this stage.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::types::Value;

/// A single entry in the keyspace: a value plus optional expiration.
#[derive(Debug, Clone)]
struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}

impl Entry {
    fn new(value: Value, expires_at: Option<Instant>) -> Self {
        Self { value, expires_at }
    }

    /// Returns `true` if this entry has passed its expiration time.
    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(deadline) => Instant::now() >= deadline,
            None => false,
        }
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

/// The core key-value store.
///
/// All operations are single-threaded per shard — no internal locking.
/// Callers are responsible for synchronization when sharing across threads.
pub struct Keyspace {
    entries: HashMap<String, Entry>,
}

impl Keyspace {
    /// Creates a new, empty keyspace.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Retrieves the value for `key`, or `None` if the key doesn't exist
    /// or has expired.
    ///
    /// Expired keys are removed lazily on access.
    pub fn get(&mut self, key: &str) -> Option<Value> {
        if self.remove_if_expired(key) {
            return None;
        }
        self.entries.get(key).map(|e| e.value.clone())
    }

    /// Stores a key-value pair. If the key already existed, the old entry
    /// (including any TTL) is replaced entirely.
    ///
    /// `expire` sets an optional TTL as a duration from now.
    pub fn set(&mut self, key: String, value: Bytes, expire: Option<Duration>) {
        let expires_at = expire.map(|d| Instant::now() + d);
        self.entries
            .insert(key, Entry::new(Value::String(value), expires_at));
    }

    /// Removes a key. Returns `true` if the key existed (and wasn't expired).
    pub fn del(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        self.entries.remove(key).is_some()
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

    /// Checks if a key is expired and removes it if so. Returns `true`
    /// if the key was removed (or didn't exist).
    fn remove_if_expired(&mut self, key: &str) -> bool {
        let expired = self
            .entries
            .get(key)
            .map(|e| e.is_expired())
            .unwrap_or(false);

        if expired {
            self.entries.remove(key);
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
}
