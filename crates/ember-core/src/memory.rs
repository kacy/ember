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
/// plus HashMap per-entry bookkeeping. This doesn't need to be exact —
/// it's close enough for eviction triggers and stats reporting.
const ENTRY_OVERHEAD: usize = 96;

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

/// Returns the byte size of a value's payload.
fn value_size(value: &Value) -> usize {
    match value {
        Value::String(data) => data.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(
            t.used_bytes(),
            entry_size("k", &new),
        );
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
