//! Memory tracking for the keyspace.
//!
//! Provides byte-level accounting of memory used by entries. Updated
//! on every mutation so the engine can enforce memory limits and
//! report stats without scanning the entire keyspace.

use crate::types::Value;

/// Estimated overhead per entry in the HashMap.
///
/// Accounts for: HashMap bucket pointer (8), Entry struct fields
/// (Option<Instant> = 16, last_access Instant = 8, Value enum tag + padding),
/// plus HashMap per-entry bookkeeping.
///
/// This is an approximation measured empirically on x86-64 linux. The exact
/// value varies by platform and compiler version, but precision isn't critical —
/// we use this for eviction triggers and memory reporting, not for correctness.
/// Overestimating is fine (triggers eviction earlier); underestimating could
/// theoretically let memory grow slightly beyond the configured limit.
pub(crate) const ENTRY_OVERHEAD: usize = 96;

/// Tracks memory usage for a single keyspace.
///
/// All updates are explicit — callers must call `add` / `remove` on every
/// mutation. This avoids any hidden scanning cost.
#[derive(Debug)]
pub struct MemoryTracker {
    used_bytes: usize,
    key_count: usize,
}

impl MemoryTracker {
    /// Creates a tracker with zero usage.
    pub fn new() -> Self {
        Self {
            used_bytes: 0,
            key_count: 0,
        }
    }

    /// Resets tracking to zero. Used by FLUSHDB.
    pub fn reset(&mut self) {
        self.used_bytes = 0;
        self.key_count = 0;
    }

    /// Returns the current estimated memory usage in bytes.
    pub fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    /// Returns the number of tracked keys.
    pub fn key_count(&self) -> usize {
        self.key_count
    }

    /// Records the addition of a new entry.
    pub fn add(&mut self, key: &str, value: &Value) {
        self.used_bytes += entry_size(key, value);
        self.key_count += 1;
    }

    /// Records the removal of an entry.
    pub fn remove(&mut self, key: &str, value: &Value) {
        let size = entry_size(key, value);
        self.used_bytes = self.used_bytes.saturating_sub(size);
        self.key_count = self.key_count.saturating_sub(1);
    }

    /// Adjusts tracking when a key's value is overwritten.
    ///
    /// Removes the old value's contribution and adds the new one.
    /// Key count stays the same.
    pub fn replace(&mut self, key: &str, old_value: &Value, new_value: &Value) {
        let old_size = entry_size(key, old_value);
        let new_size = entry_size(key, new_value);
        self.used_bytes = self
            .used_bytes
            .saturating_sub(old_size)
            .saturating_add(new_size);
    }

    /// Adjusts used bytes for an in-place mutation (e.g. list push/pop)
    /// without changing the key count.
    ///
    /// `old_entry_size` and `new_entry_size` are the full entry sizes
    /// (as returned by `entry_size`) before and after the mutation.
    pub fn adjust(&mut self, old_entry_size: usize, new_entry_size: usize) {
        self.used_bytes = self
            .used_bytes
            .saturating_sub(old_entry_size)
            .saturating_add(new_entry_size);
    }

    /// Removes an entry with an explicit size, useful when the value has
    /// already been mutated and the original size was captured beforehand.
    pub fn remove_with_size(&mut self, size: usize) {
        self.used_bytes = self.used_bytes.saturating_sub(size);
        self.key_count = self.key_count.saturating_sub(1);
    }
}

impl Default for MemoryTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Estimates the total memory footprint of a single entry.
///
/// key heap allocation + value bytes + fixed per-entry overhead.
pub fn entry_size(key: &str, value: &Value) -> usize {
    key.len() + value_size(value) + ENTRY_OVERHEAD
}

/// Estimated overhead per element in a VecDeque.
///
/// Each slot holds a `Bytes` (pointer + len + capacity = 24 bytes on 64-bit)
/// plus VecDeque's internal bookkeeping per slot.
pub(crate) const VECDEQUE_ELEMENT_OVERHEAD: usize = 32;

/// Base overhead for an empty VecDeque (internal buffer pointer + head/len).
pub(crate) const VECDEQUE_BASE_OVERHEAD: usize = 24;

/// Returns the byte size of a value's payload.
pub fn value_size(value: &Value) -> usize {
    match value {
        Value::String(data) => data.len(),
        Value::List(deque) => {
            let element_bytes: usize = deque
                .iter()
                .map(|b| b.len() + VECDEQUE_ELEMENT_OVERHEAD)
                .sum();
            VECDEQUE_BASE_OVERHEAD + element_bytes
        }
        Value::SortedSet(ss) => ss.memory_usage(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn string_val(s: &str) -> Value {
        Value::String(Bytes::from(s.to_string()))
    }

    #[test]
    fn new_tracker_is_empty() {
        let t = MemoryTracker::new();
        assert_eq!(t.used_bytes(), 0);
        assert_eq!(t.key_count(), 0);
    }

    #[test]
    fn add_increases_usage() {
        let mut t = MemoryTracker::new();
        let val = string_val("hello");
        t.add("key", &val);
        assert_eq!(t.key_count(), 1);
        assert_eq!(t.used_bytes(), entry_size("key", &val));
    }

    #[test]
    fn remove_decreases_usage() {
        let mut t = MemoryTracker::new();
        let val = string_val("data");
        t.add("k", &val);
        t.remove("k", &val);
        assert_eq!(t.used_bytes(), 0);
        assert_eq!(t.key_count(), 0);
    }

    #[test]
    fn replace_adjusts_usage() {
        let mut t = MemoryTracker::new();
        let old = string_val("short");
        let new = string_val("a much longer value here");
        t.add("k", &old);

        let before = t.used_bytes();
        t.replace("k", &old, &new);

        assert_eq!(t.key_count(), 1);
        // new value is longer, so usage should increase
        assert!(t.used_bytes() > before);
        assert_eq!(t.used_bytes(), entry_size("k", &new),);
    }

    #[test]
    fn remove_saturates_at_zero() {
        let mut t = MemoryTracker::new();
        let val = string_val("x");
        // remove without add — should not underflow
        t.remove("k", &val);
        assert_eq!(t.used_bytes(), 0);
        assert_eq!(t.key_count(), 0);
    }

    #[test]
    fn entry_size_accounts_for_key_and_value() {
        let val = string_val("test");
        let size = entry_size("mykey", &val);
        // 5 (key) + 4 (value) + 96 (overhead)
        assert_eq!(size, 5 + 4 + ENTRY_OVERHEAD);
    }

    #[test]
    fn list_value_size() {
        let mut deque = std::collections::VecDeque::new();
        deque.push_back(Bytes::from("hello"));
        deque.push_back(Bytes::from("world"));
        let val = Value::List(deque);

        let size = value_size(&val);
        // base overhead + 2 elements (each: data len + element overhead)
        let expected = VECDEQUE_BASE_OVERHEAD
            + (5 + VECDEQUE_ELEMENT_OVERHEAD)
            + (5 + VECDEQUE_ELEMENT_OVERHEAD);
        assert_eq!(size, expected);
    }

    #[test]
    fn empty_list_value_size() {
        let val = Value::List(std::collections::VecDeque::new());
        assert_eq!(value_size(&val), VECDEQUE_BASE_OVERHEAD);
    }

    #[test]
    fn multiple_entries() {
        let mut t = MemoryTracker::new();
        let v1 = string_val("aaa");
        let v2 = string_val("bbbbb");
        t.add("k1", &v1);
        t.add("k2", &v2);

        assert_eq!(t.key_count(), 2);
        assert_eq!(
            t.used_bytes(),
            entry_size("k1", &v1) + entry_size("k2", &v2),
        );

        t.remove("k1", &v1);
        assert_eq!(t.key_count(), 1);
        assert_eq!(t.used_bytes(), entry_size("k2", &v2));
    }
}
