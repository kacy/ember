//! ember-core: the storage engine.
//!
//! Owns the keyspace, data types, expiration, and memory management.
//! Designed around a thread-per-core, shared-nothing architecture
//! where each shard independently manages a partition of keys.

pub mod concurrent;
pub mod dropper;
pub mod engine;
pub mod error;
pub mod expiry;
pub mod keyspace;
pub mod memory;
pub mod shard;
pub mod time;
pub mod types;

#[cfg(feature = "protobuf")]
pub mod schema;

pub use concurrent::{ConcurrentFloatError, ConcurrentKeyspace, ConcurrentOpError};
pub use engine::{Engine, EngineConfig};
pub use error::ShardError;
pub use keyspace::{
    EvictionPolicy, IncrError, IncrFloatError, Keyspace, KeyspaceStats, RenameError, ShardConfig,
    TtlResult, WriteError, WrongType, ZAddResult,
};
#[cfg(feature = "vector")]
pub use keyspace::{VAddResult, VectorWriteError};
pub use shard::{ReplicationEvent, ShardHandle, ShardPersistenceConfig, ShardRequest, ShardResponse};
pub use types::Value;
