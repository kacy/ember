//! ember-persistence: durability layer.
//!
//! Handles append-only file logging, point-in-time snapshots,
//! and crash recovery.

pub mod aof;
pub mod format;
pub mod recovery;
pub mod snapshot;
