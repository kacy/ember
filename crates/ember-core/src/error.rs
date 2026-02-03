//! Error types for the core engine.

use thiserror::Error;

/// Errors returned by shard or engine operations.
#[derive(Debug, Error)]
pub enum ShardError {
    /// The target shard is no longer running (channel closed).
    #[error("shard unavailable")]
    Unavailable,
}
