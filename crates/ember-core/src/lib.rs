//! ember-core: the storage engine.
//!
//! Owns the keyspace, data types, expiration, and memory management.
//! Designed around a thread-per-core, shared-nothing architecture
//! where each shard independently manages a partition of keys.
