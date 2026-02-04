//! Data type representations for stored values.
//!
//! Each variant maps to a Redis-like data type. Strings, lists, and
//! sorted sets are supported; plain sets and hashes will come later.

pub mod sorted_set;

use std::collections::VecDeque;

use bytes::Bytes;

use sorted_set::SortedSet;

/// A stored value in the keyspace.
///
/// Each variant maps to a Redis-like data type. We implement `PartialEq`
/// manually because `SortedSet` contains `OrderedFloat` and `BTreeMap`
/// which need custom comparison.
#[derive(Debug, Clone)]
pub enum Value {
    /// Binary-safe string data. Uses `Bytes` for cheap cloning
    /// and zero-copy slicing.
    String(Bytes),

    /// Ordered list of binary-safe elements. `VecDeque` gives us
    /// O(1) push/pop at both ends and good cache locality.
    List(VecDeque<Bytes>),

    /// Sorted set of unique string members, each with a float score.
    /// Members are ordered by (score, member_name).
    SortedSet(SortedSet),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::String(a), Value::String(b)) => a == b,
            (Value::List(a), Value::List(b)) => a == b,
            (Value::SortedSet(a), Value::SortedSet(b)) => {
                a.len() == b.len()
                    && a.iter()
                        .zip(b.iter())
                        .all(|((m1, s1), (m2, s2))| m1 == m2 && s1 == s2)
            }
            _ => false,
        }
    }
}

/// Returns the type name for a value, matching Redis TYPE command output.
pub fn type_name(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "string",
        Value::List(_) => "list",
        Value::SortedSet(_) => "zset",
    }
}

/// Converts Redis-style indices (supporting negative values) to a
/// clamped `(start, stop)` pair.
///
/// Negative indices count back from `len` (e.g. -1 = last element).
/// Out-of-bounds stop is clamped to `len - 1`; out-of-bounds negative
/// stop clamps to -1 so the caller sees `start > stop` (empty range).
/// Returns `(0, -1)` for empty collections.
pub fn normalize_range(start: i64, stop: i64, len: i64) -> (i64, i64) {
    if len == 0 {
        return (0, -1);
    }

    // resolve negative indices, clamp floor to 0
    let s = if start < 0 {
        (len + start).max(0)
    } else {
        start
    };

    // resolve negative indices, clamp floor to -1 so that a
    // hugely-negative stop produces an empty range
    let e = if stop < 0 {
        (len + stop).max(-1)
    } else {
        stop.min(len - 1)
    };

    (s, e)
}
