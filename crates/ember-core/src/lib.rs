//! ember-core: the storage engine.
//!
//! Owns the keyspace, data types, expiration, and memory management.
//! Designed around a thread-per-core, shared-nothing architecture
//! where each shard independently manages a partition of keys.

pub mod engine;
pub mod error;
pub mod expiry;
pub mod keyspace;
pub mod memory;
pub mod shard;
pub mod types;

pub use engine::{Engine, EngineConfig};
pub use error::ShardError;
pub use keyspace::{
    EvictionPolicy, IncrError, Keyspace, KeyspaceStats, ShardConfig, TtlResult, WriteError,
    WrongType, ZAddResult,
};
pub use shard::{ShardPersistenceConfig, ShardRequest, ShardResponse};
pub use types::Value;
