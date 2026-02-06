//! ember-cluster: distributed coordination for ember.
//!
//! This crate provides the building blocks for running ember as a distributed
//! cluster with automatic failover and horizontal scaling.
//!
//! # Architecture
//!
//! The cluster layer sits between the protocol layer and the storage engine,
//! handling:
//!
//! - **Slot management**: 16384 hash slots distributed across nodes
//! - **Topology tracking**: Node membership and health monitoring
//! - **Failure detection**: SWIM gossip protocol for quick detection
//! - **Consensus**: Raft for cluster configuration changes
//! - **Migration**: Live slot resharding without downtime
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use ember_cluster::{ClusterState, ClusterNode, NodeId, key_slot};
//!
//! // Create a single-node cluster
//! let node_id = NodeId::new();
//! let node = ClusterNode::new_primary(node_id, "127.0.0.1:6379".parse().unwrap());
//! let cluster = ClusterState::single_node(node);
//!
//! // Route a key to its slot
//! let slot = key_slot(b"mykey");
//! assert!(cluster.owns_slot(slot));
//! ```

mod error;
mod message;
mod slots;
mod topology;

pub use error::ClusterError;
pub use message::{GossipMessage, MemberInfo, NodeUpdate};
pub use slots::{key_slot, SlotMap, SlotRange, SLOT_COUNT};
pub use topology::{ClusterHealth, ClusterNode, ClusterState, NodeFlags, NodeId, NodeRole};
