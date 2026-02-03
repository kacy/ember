//! Error types for the core engine.

use thiserror::Error;

/// Errors returned by keyspace operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum KeyspaceError {
    /// The operation was attempted on a key holding a value of the wrong type.
    /// For example, running a list command against a string key.
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,
}
