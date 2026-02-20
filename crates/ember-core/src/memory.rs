//! Memory tracking for the keyspace.
//!
//! Provides byte-level accounting of memory used by entries. Updated
//! on every mutation so the engine can enforce memory limits and
//! report stats without scanning the entire keyspace.
//!
//! # Platform notes
//!
//! Overhead constants are empirical estimates for 64-bit platforms (x86-64,
//! aarch64). On 32-bit systems these would be smaller; the effect is that
//! we'd overestimate memory usage, which triggers eviction earlier than
//! necessary but doesn't cause correctness issues.
//!
//! The constants assume Rust's standard library allocator. Custom allocators
//! (jemalloc, mimalloc) may have different per-allocation overhead.
//!
//! # Safety margin
//!
//! Because overhead constants are estimates and allocator fragmentation is
//! unpredictable, we apply a safety margin when enforcing memory limits.
//! The effective limit is set to [`MEMORY_SAFETY_MARGIN_PERCENT`]% of the
//! configured max, reserving headroom so the process doesn't OOM before
//! eviction has a chance to kick in.

use crate::types::Value;

/// Percentage of the configured `max_memory` that we actually use as the
/// effective write limit. The remaining headroom absorbs allocator overhead,
/// internal fragmentation, and estimation error in our per-entry constants.
///
/// 90% is conservative — it means a server configured with 1 GB will start
/// rejecting writes (or evicting) at ~922 MB of estimated usage, leaving
/// ~100 MB of breathing room for the allocator.
pub const MEMORY_SAFETY_MARGIN_PERCENT: usize = 90;

/// Computes the effective memory limit after applying the safety margin.
///
/// Returns the number of bytes at which writes should be rejected or
/// eviction should begin — always less than the raw configured limit.
pub fn effective_limit(max_bytes: usize) -> usize {
    // use u128 intermediate to avoid overflow on large max_bytes values
    // while preserving precision for small values
    ((max_bytes as u128) * (MEMORY_SAFETY_MARGIN_PERCENT as u128) / 100) as usize
}

/// Estimated overhead per entry in the HashMap.
///
/// Accounts for: the Box<str> key struct (16 bytes ptr+len on 64-bit),
/// Entry struct fields (Value enum tag + Bytes/collection inline storage
/// + expires_at_ms + cached_value_size + last_access_secs), plus hashbrown per-entry bookkeeping
/// (1 control byte + empty slot waste at ~87.5% load factor).
///
/// This is calibrated from `std::mem::size_of` on 64-bit platforms. The
/// exact value varies by compiler version, but precision isn't critical —
/// we use this for eviction triggers and memory reporting, not correctness.
/// Overestimating is fine (triggers eviction earlier); underestimating could
/// let memory grow slightly beyond the configured limit.
///
/// The `entry_overhead_not_too_small` test validates this constant against
/// the actual struct sizes on each platform.
pub(crate) const ENTRY_OVERHEAD: usize = 120;

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

    /// Increases used bytes by `delta` without scanning the entry.
    ///
    /// Use this when the caller already knows the exact number of bytes
    /// being added (e.g. list push where element sizes are precomputed).
    /// Does not change the key count.
    pub fn grow_by(&mut self, delta: usize) {
        self.used_bytes = self.used_bytes.saturating_add(delta);
    }

    /// Decreases used bytes by `delta` without scanning the entry.
    ///
    /// Use this when the caller already knows the exact number of bytes
    /// being removed (e.g. list pop where the popped element length is known).
    /// Does not change the key count.
    pub fn shrink_by(&mut self, delta: usize) {
        self.used_bytes = self.used_bytes.saturating_sub(delta);
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

/// Element count threshold below which values are dropped inline rather
/// than sent to the background drop thread. Strings are always inline
/// (Bytes::drop is O(1)), but collections with more than this many
/// elements get deferred.
pub const LAZY_FREE_THRESHOLD: usize = 64;

/// Returns `true` if dropping this value is expensive enough to justify
/// sending it to the background drop thread.
///
/// Strings are always cheap to drop (reference-counted `Bytes`).
/// Collections are considered large when they exceed [`LAZY_FREE_THRESHOLD`]
/// elements.
pub fn is_large_value(value: &Value) -> bool {
    match value {
        Value::String(_) => false,
        Value::List(d) => d.len() > LAZY_FREE_THRESHOLD,
        Value::SortedSet(ss) => ss.len() > LAZY_FREE_THRESHOLD,
        Value::Hash(m) => m.len() > LAZY_FREE_THRESHOLD,
        Value::Set(s) => s.len() > LAZY_FREE_THRESHOLD,
        // Vector sets contain usearch Index (C++ object) + hashmaps.
        // Large sets should be deferred.
        #[cfg(feature = "vector")]
        Value::Vector(vs) => vs.len() > LAZY_FREE_THRESHOLD,
        // Proto values use Bytes (ref-counted, O(1) drop) + a String.
        // Neither is expensive to drop.
        #[cfg(feature = "protobuf")]
        Value::Proto { .. } => false,
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

/// Estimated overhead per entry in a HashMap (for hash type).
///
/// Each entry has: key String (24 bytes ptr+len+cap), value Bytes (24 bytes),
/// plus HashMap bucket overhead (~16 bytes for hash + next pointer).
pub(crate) const HASHMAP_ENTRY_OVERHEAD: usize = 64;

/// Base overhead for an empty HashMap (bucket array pointer + len + capacity).
pub(crate) const HASHMAP_BASE_OVERHEAD: usize = 48;

/// Estimated overhead per member in a HashSet.
///
/// Each member is a String (24 bytes ptr+len+cap) plus bucket overhead.
pub(crate) const HASHSET_MEMBER_OVERHEAD: usize = 40;

/// Base overhead for an empty HashSet (bucket array pointer + len + capacity).
pub(crate) const HASHSET_BASE_OVERHEAD: usize = 48;

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
        Value::Hash(map) => {
            let entry_bytes: usize = map
                .iter()
                .map(|(k, v)| k.len() + v.len() + HASHMAP_ENTRY_OVERHEAD)
                .sum();
            HASHMAP_BASE_OVERHEAD + entry_bytes
        }
        Value::Set(set) => {
            let member_bytes: usize = set.iter().map(|m| m.len() + HASHSET_MEMBER_OVERHEAD).sum();
            HASHSET_BASE_OVERHEAD + member_bytes
        }
        #[cfg(feature = "vector")]
        Value::Vector(vs) => vs.memory_usage(),
        // type_name: String struct = 24 bytes (ptr+len+cap) on 64-bit.
        // data: Bytes struct = ~24 bytes (ptr+len+vtable/arc).
        #[cfg(feature = "protobuf")]
        Value::Proto { type_name, data } => type_name.len() + data.len() + 48,
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
        // 5 (key) + 4 (value) + ENTRY_OVERHEAD
        assert_eq!(size, 5 + 4 + ENTRY_OVERHEAD);
    }

    /// Validates that ENTRY_OVERHEAD is at least as large as the actual
    /// struct sizes, so we never underestimate memory usage.
    #[test]
    fn entry_overhead_not_too_small() {
        use crate::keyspace::Entry;

        let entry_size = std::mem::size_of::<Entry>();
        let key_struct_size = std::mem::size_of::<Box<str>>();
        // hashbrown uses 1 control byte per slot + ~14% empty slot waste.
        // 8 bytes is a conservative lower bound for per-entry hash overhead.
        let hashmap_per_entry = 8;
        let minimum = entry_size + key_struct_size + hashmap_per_entry;

        assert!(
            ENTRY_OVERHEAD >= minimum,
            "ENTRY_OVERHEAD ({ENTRY_OVERHEAD}) is less than measured minimum \
             ({minimum} = Entry({entry_size}) + Box<str>({key_struct_size}) + \
             hashmap({hashmap_per_entry}))"
        );
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

    #[test]
    fn effective_limit_applies_margin() {
        // 1000 bytes configured → 900 effective at 90%
        assert_eq!(effective_limit(1000), 900);
    }

    #[test]
    fn effective_limit_rounds_down() {
        // 1001 * 90 / 100 = 900 (integer division truncates)
        assert_eq!(effective_limit(1001), 900);
    }

    #[test]
    fn effective_limit_zero() {
        assert_eq!(effective_limit(0), 0);
    }

    #[test]
    fn string_is_never_large() {
        let val = Value::String(Bytes::from(vec![0u8; 10_000]));
        assert!(!is_large_value(&val));
    }

    #[test]
    fn small_list_is_not_large() {
        let mut d = std::collections::VecDeque::new();
        for _ in 0..LAZY_FREE_THRESHOLD {
            d.push_back(Bytes::from("x"));
        }
        assert!(!is_large_value(&Value::List(d)));
    }

    #[test]
    fn big_list_is_large() {
        let mut d = std::collections::VecDeque::new();
        for _ in 0..=LAZY_FREE_THRESHOLD {
            d.push_back(Bytes::from("x"));
        }
        assert!(is_large_value(&Value::List(d)));
    }

    #[test]
    fn big_hash_is_large() {
        let mut m = std::collections::HashMap::new();
        for i in 0..=LAZY_FREE_THRESHOLD {
            m.insert(format!("f{i}"), Bytes::from("v"));
        }
        assert!(is_large_value(&Value::Hash(Box::new(m))));
    }

    #[test]
    fn big_set_is_large() {
        let mut s = std::collections::HashSet::new();
        for i in 0..=LAZY_FREE_THRESHOLD {
            s.insert(format!("m{i}"));
        }
        assert!(is_large_value(&Value::Set(Box::new(s))));
    }
}
