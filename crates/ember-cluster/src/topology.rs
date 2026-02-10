//! Cluster topology management.
//!
//! Defines the structure of a cluster: nodes, their roles, health states,
//! and the overall cluster configuration.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::slots::{SlotMap, SlotRange, SLOT_COUNT};
use crate::ClusterError;

/// Unique identifier for a cluster node.
///
/// Wraps a UUID v4 for guaranteed uniqueness across the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub Uuid);

impl NodeId {
    /// Generates a new random node ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Creates a node ID from a UUID string.
    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Show first 8 chars for readability (similar to git short hashes)
        write!(f, "{}", &self.0.to_string()[..8])
    }
}

/// The role of a node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    /// Primary node that owns slots and accepts writes.
    Primary,
    /// Replica node that mirrors a primary's data.
    Replica,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::Primary => write!(f, "primary"),
            NodeRole::Replica => write!(f, "replica"),
        }
    }
}

/// Status flags for a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct NodeFlags {
    /// Node is the local node (myself).
    pub myself: bool,
    /// Node is suspected to be failing.
    pub pfail: bool,
    /// Node has been confirmed as failed by the cluster.
    pub fail: bool,
    /// Node is performing a handshake (not yet part of cluster).
    pub handshake: bool,
    /// Node has no address yet.
    pub noaddr: bool,
}

impl NodeFlags {
    /// Returns true if the node is considered healthy.
    pub fn is_healthy(&self) -> bool {
        !self.fail && !self.pfail
    }
}

impl std::fmt::Display for NodeFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut flags = Vec::new();
        if self.myself {
            flags.push("myself");
        }
        if self.pfail {
            flags.push("pfail");
        }
        if self.fail {
            flags.push("fail");
        }
        if self.handshake {
            flags.push("handshake");
        }
        if self.noaddr {
            flags.push("noaddr");
        }
        if flags.is_empty() {
            write!(f, "-")
        } else {
            write!(f, "{}", flags.join(","))
        }
    }
}

/// Information about a single node in the cluster.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    /// Unique node identifier.
    pub id: NodeId,
    /// Address for client connections.
    pub addr: SocketAddr,
    /// Address for cluster bus (gossip) connections.
    /// Typically addr.port + 10000.
    pub cluster_bus_addr: SocketAddr,
    /// Node's role in the cluster.
    pub role: NodeRole,
    /// Slot ranges assigned to this node (only for primaries).
    pub slots: Vec<SlotRange>,
    /// If this is a replica, the ID of its primary.
    pub replicates: Option<NodeId>,
    /// IDs of nodes replicating this one (if primary).
    pub replicas: Vec<NodeId>,
    /// Last time we received a message from this node.
    pub last_seen: Instant,
    /// Last time we sent a ping to this node.
    pub last_ping_sent: Option<Instant>,
    /// Last time we received a pong from this node.
    pub last_pong_received: Option<Instant>,
    /// Status flags.
    pub flags: NodeFlags,
    /// Configuration epoch (used for conflict resolution).
    pub config_epoch: u64,
}

impl ClusterNode {
    /// Creates a new primary node.
    pub fn new_primary(id: NodeId, addr: SocketAddr) -> Self {
        let cluster_bus_addr = SocketAddr::new(addr.ip(), addr.port() + 10000);
        Self {
            id,
            addr,
            cluster_bus_addr,
            role: NodeRole::Primary,
            slots: Vec::new(),
            replicates: None,
            replicas: Vec::new(),
            last_seen: Instant::now(),
            last_ping_sent: None,
            last_pong_received: None,
            flags: NodeFlags::default(),
            config_epoch: 0,
        }
    }

    /// Creates a new replica node.
    pub fn new_replica(id: NodeId, addr: SocketAddr, primary_id: NodeId) -> Self {
        let cluster_bus_addr = SocketAddr::new(addr.ip(), addr.port() + 10000);
        Self {
            id,
            addr,
            cluster_bus_addr,
            role: NodeRole::Replica,
            slots: Vec::new(),
            replicates: Some(primary_id),
            replicas: Vec::new(),
            last_seen: Instant::now(),
            last_ping_sent: None,
            last_pong_received: None,
            flags: NodeFlags::default(),
            config_epoch: 0,
        }
    }

    /// Marks this node as the local node.
    pub fn set_myself(&mut self) {
        self.flags.myself = true;
    }

    /// Returns true if this node is healthy and can serve requests.
    pub fn is_healthy(&self) -> bool {
        self.flags.is_healthy()
    }

    /// Returns the total number of slots owned by this node.
    pub fn slot_count(&self) -> u16 {
        self.slots.iter().map(|r| r.len()).sum()
    }

    /// Formats the node in CLUSTER NODES output format.
    pub fn to_cluster_nodes_line(&self, slot_map: &SlotMap) -> String {
        let slots_str = if self.role == NodeRole::Primary {
            let ranges = slot_map.slots_for_node(self.id);
            if ranges.is_empty() {
                String::new()
            } else {
                ranges
                    .iter()
                    .map(|r| r.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            }
        } else {
            String::new()
        };

        let replicates_str = self
            .replicates
            .map(|id| id.0.to_string())
            .unwrap_or_else(|| "-".to_string());

        // Format: <id> <addr>@<bus-port> <flags> <master-id> <ping-sent> <pong-recv> <config-epoch> <link-state> <slots>
        format!(
            "{} {}@{} {} {} {} {} {} connected {}",
            self.id.0,
            self.addr,
            self.cluster_bus_addr.port(),
            self.format_flags(),
            replicates_str,
            self.last_ping_sent
                .map(|t| t.elapsed().as_millis() as u64)
                .unwrap_or(0),
            self.last_pong_received
                .map(|t| t.elapsed().as_millis() as u64)
                .unwrap_or(0),
            self.config_epoch,
            slots_str
        )
        .trim()
        .to_string()
    }

    fn format_flags(&self) -> String {
        let mut flags = Vec::new();

        if self.flags.myself {
            flags.push("myself");
        }

        match self.role {
            NodeRole::Primary => flags.push("master"),
            NodeRole::Replica => flags.push("slave"),
        }

        if self.flags.fail {
            flags.push("fail");
        } else if self.flags.pfail {
            flags.push("fail?");
        }

        if self.flags.handshake {
            flags.push("handshake");
        }

        if self.flags.noaddr {
            flags.push("noaddr");
        }

        flags.join(",")
    }
}

/// The complete state of the cluster as seen by a node.
#[derive(Debug)]
pub struct ClusterState {
    /// All known nodes in the cluster, indexed by ID.
    pub nodes: HashMap<NodeId, ClusterNode>,
    /// This node's ID.
    pub local_id: NodeId,
    /// Current configuration epoch (increases on topology changes).
    pub config_epoch: u64,
    /// Slot-to-node mapping.
    pub slot_map: SlotMap,
    /// Cluster state: ok, fail, or unknown.
    pub state: ClusterHealth,
}

/// Overall cluster health status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterHealth {
    /// Cluster is operational and all slots are covered.
    Ok,
    /// Cluster has failed nodes or uncovered slots.
    Fail,
    /// Cluster state is being computed.
    Unknown,
}

impl std::fmt::Display for ClusterHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterHealth::Ok => write!(f, "ok"),
            ClusterHealth::Fail => write!(f, "fail"),
            ClusterHealth::Unknown => write!(f, "unknown"),
        }
    }
}

impl ClusterState {
    /// Creates a new cluster state for a single-node cluster.
    pub fn single_node(local_node: ClusterNode) -> Self {
        let local_id = local_node.id;
        let slot_map = SlotMap::single_node(local_id);
        let mut nodes = HashMap::new();
        nodes.insert(local_id, local_node);

        Self {
            nodes,
            local_id,
            config_epoch: 1,
            slot_map,
            state: ClusterHealth::Ok,
        }
    }

    /// Creates a new empty cluster state (for joining an existing cluster).
    pub fn new(local_id: NodeId) -> Self {
        Self {
            nodes: HashMap::new(),
            local_id,
            config_epoch: 0,
            slot_map: SlotMap::new(),
            state: ClusterHealth::Unknown,
        }
    }

    /// Returns the local node.
    pub fn local_node(&self) -> Option<&ClusterNode> {
        self.nodes.get(&self.local_id)
    }

    /// Returns a mutable reference to the local node.
    pub fn local_node_mut(&mut self) -> Option<&mut ClusterNode> {
        self.nodes.get_mut(&self.local_id)
    }

    /// Adds a node to the cluster.
    pub fn add_node(&mut self, node: ClusterNode) {
        self.nodes.insert(node.id, node);
    }

    /// Removes a node from the cluster.
    pub fn remove_node(&mut self, node_id: NodeId) -> Option<ClusterNode> {
        self.nodes.remove(&node_id)
    }

    /// Returns the node that owns the given slot.
    pub fn slot_owner(&self, slot: u16) -> Option<&ClusterNode> {
        let node_id = self.slot_map.owner(slot)?;
        self.nodes.get(&node_id)
    }

    /// Returns true if the local node owns the given slot.
    pub fn owns_slot(&self, slot: u16) -> bool {
        self.slot_map.owner(slot) == Some(self.local_id)
    }

    /// Returns all primary nodes.
    pub fn primaries(&self) -> impl Iterator<Item = &ClusterNode> {
        self.nodes.values().filter(|n| n.role == NodeRole::Primary)
    }

    /// Returns all replica nodes.
    pub fn replicas(&self) -> impl Iterator<Item = &ClusterNode> {
        self.nodes.values().filter(|n| n.role == NodeRole::Replica)
    }

    /// Returns replicas of a specific primary.
    pub fn replicas_of(&self, primary_id: NodeId) -> impl Iterator<Item = &ClusterNode> {
        self.nodes
            .values()
            .filter(move |n| n.replicates == Some(primary_id))
    }

    /// Computes and updates the cluster health state.
    pub fn update_health(&mut self) {
        // Check if all slots are covered by healthy primaries
        if !self.slot_map.is_complete() {
            self.state = ClusterHealth::Fail;
            return;
        }

        // Check if any slot's owner is unhealthy
        for slot in 0..crate::slots::SLOT_COUNT {
            if let Some(owner_id) = self.slot_map.owner(slot) {
                if let Some(node) = self.nodes.get(&owner_id) {
                    if !node.is_healthy() {
                        self.state = ClusterHealth::Fail;
                        return;
                    }
                } else {
                    // Owner node not found
                    self.state = ClusterHealth::Fail;
                    return;
                }
            }
        }

        self.state = ClusterHealth::Ok;
    }

    /// Generates the response for CLUSTER INFO command.
    pub fn cluster_info(&self) -> String {
        let assigned_slots = (SLOT_COUNT as usize - self.slot_map.unassigned_count()) as u16;
        let primaries_count = self.primaries().count();

        format!(
            "cluster_state:{}\r\n\
             cluster_slots_assigned:{}\r\n\
             cluster_slots_ok:{}\r\n\
             cluster_slots_pfail:0\r\n\
             cluster_slots_fail:0\r\n\
             cluster_known_nodes:{}\r\n\
             cluster_size:{}\r\n\
             cluster_current_epoch:{}\r\n\
             cluster_my_epoch:{}\r\n",
            self.state,
            assigned_slots,
            if self.state == ClusterHealth::Ok {
                assigned_slots
            } else {
                0
            },
            self.nodes.len(),
            primaries_count,
            self.config_epoch,
            self.local_node().map(|n| n.config_epoch).unwrap_or(0),
        )
    }

    /// Generates the response for CLUSTER NODES command.
    pub fn cluster_nodes(&self) -> String {
        let mut lines: Vec<String> = self
            .nodes
            .values()
            .map(|node| node.to_cluster_nodes_line(&self.slot_map))
            .collect();
        lines.sort(); // Consistent ordering
        lines.join("\n")
    }

    /// Generates MOVED redirect information for a slot.
    pub fn moved_redirect(&self, slot: u16) -> Result<(u16, SocketAddr), ClusterError> {
        let node = self
            .slot_owner(slot)
            .ok_or(ClusterError::SlotNotAssigned(slot))?;
        Ok((slot, node.addr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    #[test]
    fn node_id_display() {
        let id = NodeId::new();
        let display = id.to_string();
        assert_eq!(display.len(), 8);
    }

    #[test]
    fn node_id_parse() {
        let id = NodeId::new();
        let parsed = NodeId::parse(&id.0.to_string()).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn node_flags_display() {
        let mut flags = NodeFlags::default();
        assert_eq!(flags.to_string(), "-");

        flags.myself = true;
        assert_eq!(flags.to_string(), "myself");

        flags.pfail = true;
        assert_eq!(flags.to_string(), "myself,pfail");
    }

    #[test]
    fn cluster_node_primary() {
        let id = NodeId::new();
        let node = ClusterNode::new_primary(id, test_addr(6379));

        assert_eq!(node.id, id);
        assert_eq!(node.role, NodeRole::Primary);
        assert_eq!(node.addr.port(), 6379);
        assert_eq!(node.cluster_bus_addr.port(), 16379);
        assert!(node.replicates.is_none());
        assert!(node.is_healthy());
    }

    #[test]
    fn cluster_node_replica() {
        let primary_id = NodeId::new();
        let replica_id = NodeId::new();
        let node = ClusterNode::new_replica(replica_id, test_addr(6380), primary_id);

        assert_eq!(node.id, replica_id);
        assert_eq!(node.role, NodeRole::Replica);
        assert_eq!(node.replicates, Some(primary_id));
    }

    #[test]
    fn cluster_state_single_node() {
        let id = NodeId::new();
        let mut node = ClusterNode::new_primary(id, test_addr(6379));
        node.set_myself();

        let state = ClusterState::single_node(node);

        assert_eq!(state.local_id, id);
        assert!(state.owns_slot(0));
        assert!(state.owns_slot(16383));
        assert_eq!(state.state, ClusterHealth::Ok);
    }

    #[test]
    fn cluster_state_slot_owner() {
        let id = NodeId::new();
        let mut node = ClusterNode::new_primary(id, test_addr(6379));
        node.set_myself();

        let state = ClusterState::single_node(node);

        let owner = state.slot_owner(100).unwrap();
        assert_eq!(owner.id, id);
    }

    #[test]
    fn cluster_state_health_check() {
        let id = NodeId::new();
        let mut node = ClusterNode::new_primary(id, test_addr(6379));
        node.set_myself();

        let mut state = ClusterState::single_node(node);
        state.update_health();
        assert_eq!(state.state, ClusterHealth::Ok);

        // Unassign a slot
        state.slot_map.unassign(0);
        state.update_health();
        assert_eq!(state.state, ClusterHealth::Fail);
    }

    #[test]
    fn cluster_info_format() {
        let id = NodeId::new();
        let mut node = ClusterNode::new_primary(id, test_addr(6379));
        node.set_myself();

        let state = ClusterState::single_node(node);
        let info = state.cluster_info();

        assert!(info.contains("cluster_state:ok"));
        assert!(info.contains("cluster_slots_assigned:16384"));
        assert!(info.contains("cluster_known_nodes:1"));
    }

    #[test]
    fn moved_redirect() {
        let id = NodeId::new();
        let mut node = ClusterNode::new_primary(id, test_addr(6379));
        node.set_myself();

        let state = ClusterState::single_node(node);

        let (slot, addr) = state.moved_redirect(100).unwrap();
        assert_eq!(slot, 100);
        assert_eq!(addr.port(), 6379);
    }

    #[test]
    fn primaries_and_replicas() {
        let primary_id = NodeId::new();
        let replica_id = NodeId::new();

        let mut primary = ClusterNode::new_primary(primary_id, test_addr(6379));
        primary.set_myself();

        let mut state = ClusterState::single_node(primary);

        let replica = ClusterNode::new_replica(replica_id, test_addr(6380), primary_id);
        state.add_node(replica);

        assert_eq!(state.primaries().count(), 1);
        assert_eq!(state.replicas().count(), 1);
        assert_eq!(state.replicas_of(primary_id).count(), 1);
    }
}
