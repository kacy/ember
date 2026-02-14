//! Concurrent keyspace using DashMap for lock-free multi-threaded access.
//!
//! This is an alternative to the sharded architecture that eliminates channel
//! overhead by allowing direct access from multiple connection handlers.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;

use crate::keyspace::{format_float, EvictionPolicy, TtlResult};
use crate::memory;
use crate::time;

/// Errors from integer/float operations on the concurrent keyspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcurrentOpError {
    /// Value cannot be parsed as a number.
    NotAnInteger,
    /// Increment or decrement would overflow i64.
    Overflow,
}

impl std::fmt::Display for ConcurrentOpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAnInteger => write!(f, "ERR value is not an integer or out of range"),
            Self::Overflow => write!(f, "ERR increment or decrement would overflow"),
        }
    }
}

impl std::error::Error for ConcurrentOpError {}

/// Errors from float operations on the concurrent keyspace.
#[derive(Debug, Clone, PartialEq)]
pub enum ConcurrentFloatError {
    /// Value cannot be parsed as a float.
    NotAFloat,
    /// Result would be NaN or Infinity.
    NanOrInfinity,
}

impl std::fmt::Display for ConcurrentFloatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotAFloat => write!(f, "ERR value is not a valid float"),
            Self::NanOrInfinity => write!(f, "ERR increment would produce NaN or Infinity"),
        }
    }
}

impl std::error::Error for ConcurrentFloatError {}

/// An entry in the concurrent keyspace.
/// Optimized for memory: 40 bytes (down from 56).
#[derive(Debug, Clone)]
struct Entry {
    value: Bytes,
    /// Monotonic expiry timestamp in ms. 0 = no expiry.
    expires_at_ms: u64,
}

impl Entry {
    #[inline]
    fn is_expired(&self) -> bool {
        time::is_expired(self.expires_at_ms)
    }

    /// Compute entry size on demand (key_len passed in).
    #[inline]
    fn size(&self, key_len: usize) -> usize {
        // key heap + value heap + entry struct overhead
        key_len + self.value.len() + 48
    }
}

/// A concurrent keyspace backed by DashMap.
///
/// Provides thread-safe access to key-value data without channel overhead.
/// All operations are lock-free for non-conflicting keys.
#[derive(Debug)]
pub struct ConcurrentKeyspace {
    /// Using Box<str> instead of String saves 8 bytes per key (no capacity field).
    data: DashMap<Box<str>, Entry>,
    memory_used: AtomicUsize,
    max_memory: Option<usize>,
    eviction_policy: EvictionPolicy,
    ops_count: AtomicU64,
}

impl ConcurrentKeyspace {
    /// Creates a new concurrent keyspace with optional memory limit.
    pub fn new(max_memory: Option<usize>, eviction_policy: EvictionPolicy) -> Self {
        Self {
            data: DashMap::new(),
            memory_used: AtomicUsize::new(0),
            max_memory,
            eviction_policy,
            ops_count: AtomicU64::new(0),
        }
    }

    /// Gets a value by key, returning None if not found or expired.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        let entry = self.data.get(key)?;

        if entry.is_expired() {
            let key_len = entry.key().len();
            let size = entry.size(key_len);
            drop(entry);
            // Remove expired entry
            if self.data.remove(key).is_some() {
                self.memory_used.fetch_sub(size, Ordering::Relaxed);
            }
            return None;
        }

        Some(entry.value.clone())
    }

    /// Sets a key-value pair with optional TTL.
    pub fn set(&self, key: String, value: Bytes, ttl: Option<Duration>) -> bool {
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        let key: Box<str> = key.into_boxed_str();
        let entry_size = key.len() + value.len() + 48;
        let expires_at_ms = time::expiry_from_duration(ttl);

        // Check memory limit (with safety margin for allocator overhead)
        if let Some(max) = self.max_memory {
            let limit = memory::effective_limit(max);
            let current = self.memory_used.load(Ordering::Relaxed);
            if current + entry_size > limit {
                if self.eviction_policy == EvictionPolicy::NoEviction {
                    return false;
                }
                // Simple eviction: remove some entries
                self.evict_entries(entry_size);
            }
        }

        let entry = Entry {
            value,
            expires_at_ms,
        };

        // Update memory tracking
        if let Some(old) = self.data.insert(key.clone(), entry) {
            // Replace: adjust memory
            let old_size = old.size(key.len());
            let diff = entry_size as isize - old_size as isize;
            if diff > 0 {
                self.memory_used.fetch_add(diff as usize, Ordering::Relaxed);
            } else {
                self.memory_used
                    .fetch_sub((-diff) as usize, Ordering::Relaxed);
            }
        } else {
            self.memory_used.fetch_add(entry_size, Ordering::Relaxed);
        }

        true
    }

    /// Deletes a key, returning true if it existed.
    pub fn del(&self, key: &str) -> bool {
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        if let Some((k, removed)) = self.data.remove(key) {
            self.memory_used
                .fetch_sub(removed.size(k.len()), Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Checks if a key exists (and is not expired).
    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    /// Returns the TTL of a key.
    pub fn ttl(&self, key: &str) -> TtlResult {
        match self.data.get(key) {
            None => TtlResult::NotFound,
            Some(entry) => {
                if entry.is_expired() {
                    TtlResult::NotFound
                } else {
                    match time::remaining_secs(entry.expires_at_ms) {
                        None => TtlResult::NoExpiry,
                        Some(secs) => TtlResult::Seconds(secs),
                    }
                }
            }
        }
    }

    /// Sets expiration on a key.
    pub fn expire(&self, key: &str, seconds: u64) -> bool {
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                return false;
            }
            entry.expires_at_ms = time::now_ms().saturating_add(seconds.saturating_mul(1000));
            true
        } else {
            false
        }
    }

    /// Increments the integer value of a key by 1.
    /// If the key doesn't exist, it's initialized to 0 before incrementing.
    pub fn incr(&self, key: &str) -> Result<i64, ConcurrentOpError> {
        self.incr_by(key, 1)
    }

    /// Decrements the integer value of a key by 1.
    /// If the key doesn't exist, it's initialized to 0 before decrementing.
    pub fn decr(&self, key: &str) -> Result<i64, ConcurrentOpError> {
        self.incr_by(key, -1)
    }

    /// Adds `delta` to the integer value of a key, creating it if missing.
    /// Preserves existing TTL when updating.
    pub fn incr_by(&self, key: &str, delta: i64) -> Result<i64, ConcurrentOpError> {
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        // try to update in-place via get_mut
        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                let key_len = entry.key().len();
                let old_size = entry.size(key_len);
                drop(entry);
                if self.data.remove(key).is_some() {
                    self.memory_used.fetch_sub(old_size, Ordering::Relaxed);
                }
                // treat as missing — fall through to insert below
            } else {
                let s = std::str::from_utf8(&entry.value)
                    .map_err(|_| ConcurrentOpError::NotAnInteger)?;
                let current: i64 = s.parse().map_err(|_| ConcurrentOpError::NotAnInteger)?;
                let new_val = current
                    .checked_add(delta)
                    .ok_or(ConcurrentOpError::Overflow)?;
                let new_bytes = Bytes::from(new_val.to_string());

                let key_len = entry.key().len();
                let old_size = entry.size(key_len);
                entry.value = new_bytes;
                let new_size = entry.size(key_len);
                let diff = new_size as isize - old_size as isize;
                if diff > 0 {
                    self.memory_used.fetch_add(diff as usize, Ordering::Relaxed);
                } else if diff < 0 {
                    self.memory_used
                        .fetch_sub((-diff) as usize, Ordering::Relaxed);
                }
                return Ok(new_val);
            }
        }

        // key doesn't exist — treat as 0
        let new_val = (0i64)
            .checked_add(delta)
            .ok_or(ConcurrentOpError::Overflow)?;
        self.set(key.to_owned(), Bytes::from(new_val.to_string()), None);
        Ok(new_val)
    }

    /// Adds `delta` to the float value of a key, creating it if missing.
    /// Preserves existing TTL when updating.
    pub fn incr_by_float(&self, key: &str, delta: f64) -> Result<f64, ConcurrentFloatError> {
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                let key_len = entry.key().len();
                let old_size = entry.size(key_len);
                drop(entry);
                if self.data.remove(key).is_some() {
                    self.memory_used.fetch_sub(old_size, Ordering::Relaxed);
                }
            } else {
                let s = std::str::from_utf8(&entry.value)
                    .map_err(|_| ConcurrentFloatError::NotAFloat)?;
                let current: f64 = s.parse().map_err(|_| ConcurrentFloatError::NotAFloat)?;
                let new_val = current + delta;
                if new_val.is_nan() || new_val.is_infinite() {
                    return Err(ConcurrentFloatError::NanOrInfinity);
                }
                let new_bytes = Bytes::from(format_float(new_val));

                let key_len = entry.key().len();
                let old_size = entry.size(key_len);
                entry.value = new_bytes;
                let new_size = entry.size(key_len);
                let diff = new_size as isize - old_size as isize;
                if diff > 0 {
                    self.memory_used.fetch_add(diff as usize, Ordering::Relaxed);
                } else if diff < 0 {
                    self.memory_used
                        .fetch_sub((-diff) as usize, Ordering::Relaxed);
                }
                return Ok(new_val);
            }
        }

        // key doesn't exist — treat as 0.0
        let new_val = delta;
        if new_val.is_nan() || new_val.is_infinite() {
            return Err(ConcurrentFloatError::NanOrInfinity);
        }
        self.set(key.to_owned(), Bytes::from(format_float(new_val)), None);
        Ok(new_val)
    }

    /// Appends a value to an existing string key, or creates a new key.
    /// Returns the new string length.
    pub fn append(&self, key: &str, suffix: &[u8]) -> usize {
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        if let Some(mut entry) = self.data.get_mut(key) {
            if !entry.is_expired() {
                let mut new_data = Vec::with_capacity(entry.value.len() + suffix.len());
                new_data.extend_from_slice(&entry.value);
                new_data.extend_from_slice(suffix);
                let new_len = new_data.len();

                let key_len = entry.key().len();
                let old_size = entry.size(key_len);
                entry.value = Bytes::from(new_data);
                let new_size = entry.size(key_len);
                let diff = new_size as isize - old_size as isize;
                if diff > 0 {
                    self.memory_used.fetch_add(diff as usize, Ordering::Relaxed);
                } else if diff < 0 {
                    self.memory_used
                        .fetch_sub((-diff) as usize, Ordering::Relaxed);
                }
                return new_len;
            }
            // expired — remove and fall through to create
            let key_len = entry.key().len();
            let old_size = entry.size(key_len);
            drop(entry);
            if self.data.remove(key).is_some() {
                self.memory_used.fetch_sub(old_size, Ordering::Relaxed);
            }
        }

        // key doesn't exist — create with just the suffix
        let new_len = suffix.len();
        self.set(key.to_owned(), Bytes::copy_from_slice(suffix), None);
        new_len
    }

    /// Returns the length of the string value stored at key.
    /// Returns 0 if the key doesn't exist.
    pub fn strlen(&self, key: &str) -> usize {
        match self.get(key) {
            Some(data) => data.len(),
            None => 0,
        }
    }

    /// Removes the expiration from a key.
    /// Returns true if the timeout was successfully removed.
    pub fn persist(&self, key: &str) -> bool {
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                return false;
            }
            if entry.expires_at_ms != 0 {
                entry.expires_at_ms = 0;
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Sets expiration in milliseconds on an existing key.
    pub fn pexpire(&self, key: &str, millis: u64) -> bool {
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                return false;
            }
            entry.expires_at_ms = time::now_ms().saturating_add(millis);
            true
        } else {
            false
        }
    }

    /// Returns the remaining TTL in milliseconds.
    pub fn pttl(&self, key: &str) -> TtlResult {
        match self.data.get(key) {
            None => TtlResult::NotFound,
            Some(entry) => {
                if entry.is_expired() {
                    TtlResult::NotFound
                } else {
                    match time::remaining_ms(entry.expires_at_ms) {
                        None => TtlResult::NoExpiry,
                        Some(ms) => TtlResult::Milliseconds(ms),
                    }
                }
            }
        }
    }

    /// Returns all keys matching a glob pattern.
    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let len = self.data.len();
        if len > 10_000 {
            tracing::warn!(
                key_count = len,
                "KEYS on large keyspace, consider SCAN instead"
            );
        }
        self.data
            .iter()
            .filter(|entry| !entry.value().is_expired())
            .filter(|entry| crate::keyspace::glob_match(pattern, entry.key()))
            .map(|entry| entry.key().to_string())
            .collect()
    }

    /// Iterates keys using a cursor. Returns (next_cursor, keys).
    /// A next_cursor of 0 means the iteration is complete.
    pub fn scan_keys(
        &self,
        cursor: u64,
        count: usize,
        pattern: Option<&str>,
    ) -> (u64, Vec<String>) {
        let target_count = if count == 0 { 10 } else { count };
        let mut keys = Vec::with_capacity(target_count);
        let mut position = 0u64;

        for entry in self.data.iter() {
            if entry.value().is_expired() {
                continue;
            }
            if position < cursor {
                position += 1;
                continue;
            }
            if let Some(pat) = pattern {
                if !crate::keyspace::glob_match(pat, entry.key()) {
                    position += 1;
                    continue;
                }
            }
            keys.push(entry.key().to_string());
            position += 1;
            if keys.len() >= target_count {
                // there may be more keys — return position as next cursor
                return (position, keys);
            }
        }

        // iteration complete
        (0, keys)
    }

    /// Renames a key. Returns true if the source key existed.
    pub fn rename(&self, key: &str, newkey: &str) -> Result<(), &'static str> {
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        let (_, entry) = self.data.remove(key).ok_or("ERR no such key")?;

        if entry.is_expired() {
            let size = entry.size(key.len());
            self.memory_used.fetch_sub(size, Ordering::Relaxed);
            return Err("ERR no such key");
        }

        // remove destination if it exists
        if let Some((k, old_dest)) = self.data.remove(newkey) {
            self.memory_used
                .fetch_sub(old_dest.size(k.len()), Ordering::Relaxed);
        }

        // adjust memory: old key removed, new key added
        let old_key_len = key.len();
        let new_key_len = newkey.len();
        let old_mem = old_key_len + entry.value.len() + 48;
        let new_mem = new_key_len + entry.value.len() + 48;

        self.memory_used.fetch_sub(old_mem, Ordering::Relaxed);
        self.memory_used.fetch_add(new_mem, Ordering::Relaxed);

        self.data.insert(newkey.into(), entry);
        Ok(())
    }

    /// Returns the number of keys.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the keyspace is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns memory usage in bytes.
    pub fn memory_used(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Returns the operation count.
    pub fn ops_count(&self) -> u64 {
        self.ops_count.load(Ordering::Relaxed)
    }

    /// Clears all keys.
    pub fn clear(&self) {
        self.data.clear();
        self.memory_used.store(0, Ordering::Relaxed);
    }

    /// Simple eviction: remove approximately `needed` bytes worth of entries.
    fn evict_entries(&self, needed: usize) {
        let mut freed = 0usize;
        let mut keys_to_remove = Vec::new();

        // Collect keys to remove (can't remove while iterating)
        for entry in self.data.iter() {
            if freed >= needed {
                break;
            }
            let key_len = entry.key().len();
            keys_to_remove.push(entry.key().clone());
            freed += entry.value().size(key_len);
        }

        // Remove collected keys
        for key in keys_to_remove {
            if let Some((k, removed)) = self.data.remove(&key) {
                self.memory_used
                    .fetch_sub(removed.size(k.len()), Ordering::Relaxed);
            }
        }
    }
}

impl Default for ConcurrentKeyspace {
    fn default() -> Self {
        Self::new(None, EvictionPolicy::NoEviction)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_get() {
        let ks = ConcurrentKeyspace::default();
        assert!(ks.set("key".into(), Bytes::from("value"), None));
        assert_eq!(ks.get("key"), Some(Bytes::from("value")));
    }

    #[test]
    fn get_missing() {
        let ks = ConcurrentKeyspace::default();
        assert_eq!(ks.get("missing"), None);
    }

    #[test]
    fn del_existing() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from("value"), None);
        assert!(ks.del("key"));
        assert_eq!(ks.get("key"), None);
    }

    #[test]
    fn del_missing() {
        let ks = ConcurrentKeyspace::default();
        assert!(!ks.del("missing"));
    }

    #[test]
    fn exists_check() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from("value"), None);
        assert!(ks.exists("key"));
        assert!(!ks.exists("missing"));
    }

    #[test]
    fn ttl_expires() {
        let ks = ConcurrentKeyspace::default();
        ks.set(
            "key".into(),
            Bytes::from("value"),
            Some(Duration::from_millis(10)),
        );
        assert!(matches!(ks.ttl("key"), TtlResult::Seconds(_)));
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(ks.get("key"), None);
    }

    #[test]
    fn incr_new_key() {
        let ks = ConcurrentKeyspace::default();
        assert_eq!(ks.incr("counter").unwrap(), 1);
        assert_eq!(ks.get("counter"), Some(Bytes::from("1")));
    }

    #[test]
    fn incr_existing_key() {
        let ks = ConcurrentKeyspace::default();
        ks.set("counter".into(), Bytes::from("10"), None);
        assert_eq!(ks.incr("counter").unwrap(), 11);
    }

    #[test]
    fn decr_below_zero() {
        let ks = ConcurrentKeyspace::default();
        assert_eq!(ks.decr("counter").unwrap(), -1);
        assert_eq!(ks.decr("counter").unwrap(), -2);
    }

    #[test]
    fn incr_by_delta() {
        let ks = ConcurrentKeyspace::default();
        assert_eq!(ks.incr_by("counter", 5).unwrap(), 5);
        assert_eq!(ks.incr_by("counter", -3).unwrap(), 2);
    }

    #[test]
    fn incr_non_integer_value() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from("not_a_number"), None);
        assert_eq!(ks.incr("key"), Err(ConcurrentOpError::NotAnInteger));
    }

    #[test]
    fn incr_overflow() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from(i64::MAX.to_string()), None);
        assert_eq!(ks.incr("key"), Err(ConcurrentOpError::Overflow));
    }

    #[test]
    fn incr_by_float_new_key() {
        let ks = ConcurrentKeyspace::default();
        let val = ks.incr_by_float("key", 2.5).unwrap();
        assert!((val - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn incr_by_float_existing() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from("10.5"), None);
        let val = ks.incr_by_float("key", 1.5).unwrap();
        assert!((val - 12.0).abs() < f64::EPSILON);
    }

    #[test]
    fn incr_by_float_not_a_float() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from("hello"), None);
        assert_eq!(
            ks.incr_by_float("key", 1.0),
            Err(ConcurrentFloatError::NotAFloat)
        );
    }

    #[test]
    fn incr_by_float_infinity() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from(f64::MAX.to_string()), None);
        assert_eq!(
            ks.incr_by_float("key", f64::MAX),
            Err(ConcurrentFloatError::NanOrInfinity)
        );
    }

    #[test]
    fn append_new_key() {
        let ks = ConcurrentKeyspace::default();
        assert_eq!(ks.append("key", b"hello"), 5);
        assert_eq!(ks.get("key"), Some(Bytes::from("hello")));
    }

    #[test]
    fn append_existing_key() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from("hello"), None);
        assert_eq!(ks.append("key", b" world"), 11);
        assert_eq!(ks.get("key"), Some(Bytes::from("hello world")));
    }

    #[test]
    fn strlen_existing() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from("hello"), None);
        assert_eq!(ks.strlen("key"), 5);
    }

    #[test]
    fn strlen_missing() {
        let ks = ConcurrentKeyspace::default();
        assert_eq!(ks.strlen("missing"), 0);
    }

    #[test]
    fn persist_removes_ttl() {
        let ks = ConcurrentKeyspace::default();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::from_secs(60)),
        );
        assert!(ks.persist("key"));
        assert!(matches!(ks.ttl("key"), TtlResult::NoExpiry));
    }

    #[test]
    fn persist_no_ttl() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from("val"), None);
        assert!(!ks.persist("key")); // no TTL to remove
    }

    #[test]
    fn persist_missing_key() {
        let ks = ConcurrentKeyspace::default();
        assert!(!ks.persist("missing"));
    }

    #[test]
    fn pexpire_and_pttl() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from("val"), None);
        assert!(ks.pexpire("key", 5000));
        match ks.pttl("key") {
            TtlResult::Milliseconds(ms) => assert!(ms > 0 && ms <= 5000),
            other => panic!("expected Milliseconds, got {other:?}"),
        }
    }

    #[test]
    fn pttl_no_expiry() {
        let ks = ConcurrentKeyspace::default();
        ks.set("key".into(), Bytes::from("val"), None);
        assert!(matches!(ks.pttl("key"), TtlResult::NoExpiry));
    }

    #[test]
    fn pttl_missing() {
        let ks = ConcurrentKeyspace::default();
        assert!(matches!(ks.pttl("missing"), TtlResult::NotFound));
    }

    #[test]
    fn keys_match_pattern() {
        let ks = ConcurrentKeyspace::default();
        ks.set("user:1".into(), Bytes::from("a"), None);
        ks.set("user:2".into(), Bytes::from("b"), None);
        ks.set("item:1".into(), Bytes::from("c"), None);
        let mut result = ks.keys("user:*");
        result.sort();
        assert_eq!(result, vec!["user:1", "user:2"]);
    }

    #[test]
    fn keys_match_all() {
        let ks = ConcurrentKeyspace::default();
        ks.set("a".into(), Bytes::from("1"), None);
        ks.set("b".into(), Bytes::from("2"), None);
        let result = ks.keys("*");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn scan_basic() {
        let ks = ConcurrentKeyspace::default();
        ks.set("a".into(), Bytes::from("1"), None);
        ks.set("b".into(), Bytes::from("2"), None);
        ks.set("c".into(), Bytes::from("3"), None);
        let (cursor, keys) = ks.scan_keys(0, 10, None);
        assert_eq!(cursor, 0); // complete in one pass
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn scan_with_pattern() {
        let ks = ConcurrentKeyspace::default();
        ks.set("user:1".into(), Bytes::from("a"), None);
        ks.set("user:2".into(), Bytes::from("b"), None);
        ks.set("item:1".into(), Bytes::from("c"), None);
        let (_, keys) = ks.scan_keys(0, 10, Some("user:*"));
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn scan_with_count() {
        let ks = ConcurrentKeyspace::default();
        for i in 0..10 {
            ks.set(format!("k{i}"), Bytes::from("v"), None);
        }
        let (cursor, keys) = ks.scan_keys(0, 3, None);
        assert!(keys.len() <= 3);
        // if cursor > 0, there are more keys
        if cursor > 0 {
            let (_, keys2) = ks.scan_keys(cursor, 3, None);
            assert!(!keys2.is_empty());
        }
    }

    #[test]
    fn rename_basic() {
        let ks = ConcurrentKeyspace::default();
        ks.set("old".into(), Bytes::from("value"), None);
        ks.rename("old", "new").unwrap();
        assert_eq!(ks.get("old"), None);
        assert_eq!(ks.get("new"), Some(Bytes::from("value")));
    }

    #[test]
    fn rename_missing_key() {
        let ks = ConcurrentKeyspace::default();
        assert!(ks.rename("missing", "new").is_err());
    }

    #[test]
    fn rename_overwrites_destination() {
        let ks = ConcurrentKeyspace::default();
        ks.set("src".into(), Bytes::from("new_val"), None);
        ks.set("dst".into(), Bytes::from("old_val"), None);
        ks.rename("src", "dst").unwrap();
        assert_eq!(ks.get("src"), None);
        assert_eq!(ks.get("dst"), Some(Bytes::from("new_val")));
    }

    #[test]
    fn concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let ks = Arc::new(ConcurrentKeyspace::default());
        let mut handles = vec![];

        // Spawn multiple threads doing concurrent sets
        for i in 0..8 {
            let ks = Arc::clone(&ks);
            handles.push(thread::spawn(move || {
                for j in 0..1000 {
                    let key = format!("key-{}-{}", i, j);
                    ks.set(key, Bytes::from("value"), None);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(ks.len(), 8000);
    }
}
