//! Error types for cluster operations.

use std::net::SocketAddr;

use crate::NodeId;

/// Errors that can occur during cluster operations.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// The slot is not assigned to any node.
    #[error("slot {0} is not assigned to any node")]
    SlotNotAssigned(u16),

    /// The key belongs to a slot on a different node.
    #[error("MOVED {slot} {addr}")]
    Moved { slot: u16, addr: SocketAddr },

    /// The slot is being migrated; client should retry with ASK.
    #[error("ASK {slot} {addr}")]
    Ask { slot: u16, addr: SocketAddr },

    /// Node not found in the cluster.
    #[error("node {0} not found in cluster")]
    NodeNotFound(NodeId),

    /// Cluster is not in a healthy state.
    #[error("cluster is down")]
    ClusterDown,

    /// Operation requires a different node role.
    #[error("operation not supported on {role} node")]
    WrongRole { role: String },

    /// Cross-slot operation with keys in different slots.
    #[error("cross-slot keys not allowed (keys span slots {0} and {1})")]
    CrossSlot(u16, u16),

    /// Network error during cluster communication.
    #[error("cluster communication error: {0}")]
    Network(String),

    /// Timeout waiting for cluster operation.
    #[error("cluster operation timed out")]
    Timeout,

    /// Configuration error.
    #[error("invalid cluster configuration: {0}")]
    Configuration(String),
}

impl ClusterError {
    /// Returns true if this is a redirect error (MOVED or ASK).
    pub fn is_redirect(&self) -> bool {
        matches!(self, ClusterError::Moved { .. } | ClusterError::Ask { .. })
    }

    /// Creates a MOVED error for a slot redirect.
    pub fn moved(slot: u16, addr: SocketAddr) -> Self {
        ClusterError::Moved { slot, addr }
    }

    /// Creates an ASK error for a slot migration redirect.
    pub fn ask(slot: u16, addr: SocketAddr) -> Self {
        ClusterError::Ask { slot, addr }
    }
}
