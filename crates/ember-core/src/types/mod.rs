//! Data type representations for stored values.
//!
//! Currently only strings — lists, sets, hashes, and sorted sets
//! will be added in later phases.

use bytes::Bytes;

/// A stored value in the keyspace.
///
/// Each variant maps to a Redis-like data type. For now we only
/// support strings; the other variants will land as we build out
/// the command surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// Binary-safe string data. Uses `Bytes` for cheap cloning
    /// and zero-copy slicing.
    String(Bytes),
}
