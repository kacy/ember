//! Concurrent keyspace using DashMap for lock-free multi-threaded access.
//!
//! This is an alternative to the sharded architecture that eliminates channel
//! overhead by allowing direct access from multiple connection handlers.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;

use crate::keyspace::{EvictionPolicy, TtlResult};

/// An entry in the concurrent keyspace.
#[derive(Debug, Clone)]
struct Entry {
    value: Bytes,
    expires_at: Option<Instant>,
    size: usize,
}

impl Entry {
    fn is_expired(&self) -> bool {
        self.expires_at
            .map(|t| Instant::now() >= t)
            .unwrap_or(false)
    }
}

/// A concurrent keyspace backed by DashMap.
///
/// Provides thread-safe access to key-value data without channel overhead.
/// All operations are lock-free for non-conflicting keys.
#[derive(Debug)]
pub struct ConcurrentKeyspace {
    data: DashMap<String, Entry>,
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
            drop(entry);
            // Remove expired entry
            if let Some((_, removed)) = self.data.remove(key) {
                self.memory_used.fetch_sub(removed.size, Ordering::Relaxed);
            }
            return None;
        }

        Some(entry.value.clone())
    }

    /// Sets a key-value pair with optional TTL.
    pub fn set(&self, key: String, value: Bytes, ttl: Option<Duration>) -> bool {
        self.ops_count.fetch_add(1, Ordering::Relaxed);

        let entry_size = key.len() + value.len() + 64; // rough overhead estimate
        let expires_at = ttl.map(|d| Instant::now() + d);

        // Check memory limit
        if let Some(max) = self.max_memory {
            let current = self.memory_used.load(Ordering::Relaxed);
            if current + entry_size > max {
                if self.eviction_policy == EvictionPolicy::NoEviction {
                    return false;
                }
                // Simple eviction: remove some entries
                self.evict_entries(entry_size);
            }
        }

        let entry = Entry {
            value,
            expires_at,
            size: entry_size,
        };

        // Update memory tracking
        if let Some(old) = self.data.insert(key, entry) {
            // Replace: adjust memory
            let diff = entry_size as isize - old.size as isize;
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

        if let Some((_, removed)) = self.data.remove(key) {
            self.memory_used.fetch_sub(removed.size, Ordering::Relaxed);
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
                    match entry.expires_at {
                        None => TtlResult::NoExpiry,
                        Some(t) => {
                            let remaining = t.saturating_duration_since(Instant::now());
                            TtlResult::Seconds(remaining.as_secs())
                        }
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
            entry.expires_at = Some(Instant::now() + Duration::from_secs(seconds));
            true
        } else {
            false
        }
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
            keys_to_remove.push(entry.key().clone());
            freed += entry.value().size;
        }

        // Remove collected keys
        for key in keys_to_remove {
            if let Some((_, removed)) = self.data.remove(&key) {
                self.memory_used.fetch_sub(removed.size, Ordering::Relaxed);
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
