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
