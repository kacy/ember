//! Data type representations for stored values.
//!
//! Each variant maps to a Redis-like data type. Strings and lists
//! are supported; sets, hashes, and sorted sets will come later.

use std::collections::VecDeque;

use bytes::Bytes;

/// A stored value in the keyspace.
///
/// Each variant maps to a Redis-like data type. We intentionally
/// derive `Clone` and implement `PartialEq` manually — `VecDeque`
/// supports both, so this stays straightforward.
#[derive(Debug, Clone)]
pub enum Value {
    /// Binary-safe string data. Uses `Bytes` for cheap cloning
    /// and zero-copy slicing.
    String(Bytes),

    /// Ordered list of binary-safe elements. `VecDeque` gives us
    /// O(1) push/pop at both ends and good cache locality.
    List(VecDeque<Bytes>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::String(a), Value::String(b)) => a == b,
            (Value::List(a), Value::List(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

/// Returns the type name for a value, matching Redis TYPE command output.
pub fn type_name(value: &Value) -> &'static str {
    match value {
        Value::String(_) => "string",
        Value::List(_) => "list",
    }
}
