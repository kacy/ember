//! ember-core: the storage engine.
//!
//! Owns the keyspace, data types, expiration, and memory management.
//! Designed around a thread-per-core, shared-nothing architecture
//! where each shard independently manages a partition of keys.

pub mod error;
pub mod keyspace;
pub mod types;

pub use error::KeyspaceError;
pub use keyspace::{Keyspace, TtlResult};
pub use types::Value;
