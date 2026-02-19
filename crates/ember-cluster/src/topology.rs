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

/// Error returned when parsing a `nodes.conf` file fails.
#[derive(Debug, thiserror::Error)]
pub enum ConfigParseError {
    #[error("missing vars header line")]
    MissingVarsLine,
    #[error("invalid vars line: {0}")]
    InvalidVarsLine(String),
    #[error("no node with 'myself' flag found")]
    NoMyselfNode,
    #[error("invalid node line: {0}")]
    InvalidNodeLine(String),
    #[error("invalid node id: {0}")]
    InvalidNodeId(String),
    #[error("invalid address: {0}")]
    InvalidAddress(String),
    #[error("invalid slot range: {0}")]
    InvalidSlotRange(String),
}

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

    /// Returns the full UUID string for use as a map key.
    ///
    /// Note: `Display` for `NodeId` shows only the first 8 characters (for
    /// readability in logs). This method returns the full UUID needed when
    /// `NodeId` is used as a key in `BTreeMap<String, ...>` storage.
    pub(crate) fn as_key(&self) -> String {
        self.0.to_string()
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
    /// Creates a new primary node with the default bus port offset (10000).
    pub fn new_primary(id: NodeId, addr: SocketAddr) -> Self {
        Self::new_primary_with_offset(id, addr, 10000)
    }

    /// Creates a new primary node with a custom bus port offset.
    pub fn new_primary_with_offset(id: NodeId, addr: SocketAddr, bus_port_offset: u16) -> Self {
        let cluster_bus_addr =
            SocketAddr::new(addr.ip(), addr.port().saturating_add(bus_port_offset));
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
        let cluster_bus_addr = SocketAddr::new(addr.ip(), addr.port().saturating_add(10000));
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

    /// Formats the node flags for the CLUSTER NODES wire response.
    ///
    /// Intentionally diverges from `NodeFlags::Display` in two ways:
    /// 1. Role (`master`/`slave`) is included here but is not a flag field.
    /// 2. `pfail` renders as `fail?` per the Redis cluster protocol spec,
    ///    whereas `NodeFlags::Display` uses the field name `pfail`.
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

    /// Promotes a replica to primary, transferring slots from its current primary.
    ///
    /// Performs the full state transition for a failover:
    /// - Transfers all slots from the old primary to the promoted replica.
    /// - Demotes the old primary to a replica of the new primary.
    /// - Updates replica lists on both nodes.
    /// - Bumps the global config epoch.
    ///
    /// Returns an error if the target is not a replica with a configured primary.
    pub fn promote_replica(&mut self, replica_id: NodeId) -> Result<(), String> {
        // locate the replica and find its current primary
        let primary_id = {
            let replica = self
                .nodes
                .get(&replica_id)
                .ok_or_else(|| format!("node {replica_id} not found in cluster state"))?;
            if replica.role != NodeRole::Replica {
                return Err(format!("node {replica_id} is not a replica"));
            }
            replica
                .replicates
                .ok_or_else(|| format!("replica {replica_id} has no primary configured"))?
        };

        // transfer every slot currently owned by the old primary
        for slot in 0..SLOT_COUNT {
            if self.slot_map.owner(slot) == Some(primary_id) {
                self.slot_map.assign(slot, replica_id);
            }
        }
        let new_primary_slots = self.slot_map.slots_for_node(replica_id);

        // bump epoch before touching node state
        self.config_epoch += 1;
        let new_epoch = self.config_epoch;

        // demote old primary → it now replicates the new primary
        if let Some(old_primary) = self.nodes.get_mut(&primary_id) {
            old_primary.role = NodeRole::Replica;
            old_primary.replicates = Some(replica_id);
            old_primary.replicas.retain(|&id| id != replica_id);
            old_primary.slots.clear();
            old_primary.config_epoch = new_epoch;
        }

        // promote the replica → it becomes the new primary
        if let Some(new_primary) = self.nodes.get_mut(&replica_id) {
            new_primary.role = NodeRole::Primary;
            new_primary.replicates = None;
            if !new_primary.replicas.contains(&primary_id) {
                new_primary.replicas.push(primary_id);
            }
            new_primary.slots = new_primary_slots;
            new_primary.config_epoch = new_epoch;
        }

        self.update_health();
        Ok(())
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

    /// Serializes the cluster state to the `nodes.conf` format.
    ///
    /// The output consists of a comment header, a `vars` line with the current
    /// epoch and gossip incarnation, followed by one line per node in the
    /// standard `CLUSTER NODES` format.
    pub fn to_nodes_conf(&self, incarnation: u64) -> String {
        let mut out = String::new();
        out.push_str("# ember cluster config — do not edit\n");
        out.push_str(&format!(
            "vars currentEpoch {} lastIncarnation {}\n",
            self.config_epoch, incarnation
        ));

        // sort by node id for deterministic output
        let mut nodes: Vec<&ClusterNode> = self.nodes.values().collect();
        nodes.sort_by_key(|n| n.id.0);

        for node in nodes {
            out.push_str(&node.to_cluster_nodes_line(&self.slot_map));
            out.push('\n');
        }

        out
    }

    /// Parses a `nodes.conf` file and reconstructs the cluster state.
    ///
    /// Returns the reconstructed state and the saved gossip incarnation number.
    /// `Instant` fields on nodes are initialized to `Instant::now()` since
    /// wall-clock timestamps aren't persisted.
    pub fn from_nodes_conf(data: &str) -> Result<(Self, u64), ConfigParseError> {
        let mut config_epoch = 0u64;
        let mut incarnation = 0u64;
        let mut found_vars = false;
        let mut local_id = None;
        let mut nodes = HashMap::new();
        let mut slot_map = SlotMap::new();

        for line in data.lines() {
            let line = line.trim();

            // skip blank lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // parse the vars header
            if line.starts_with("vars ") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                // vars currentEpoch <n> lastIncarnation <n>
                if parts.len() < 5 {
                    return Err(ConfigParseError::InvalidVarsLine(line.to_string()));
                }
                config_epoch = parts[2]
                    .parse()
                    .map_err(|_| ConfigParseError::InvalidVarsLine(line.to_string()))?;
                incarnation = parts[4]
                    .parse()
                    .map_err(|_| ConfigParseError::InvalidVarsLine(line.to_string()))?;
                found_vars = true;
                continue;
            }

            // parse a node line:
            // <id> <ip:port@bus-port> <flags> <master-id> <ping-sent> <pong-recv> <config-epoch> <link-state> [slots...]
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 8 {
                return Err(ConfigParseError::InvalidNodeLine(line.to_string()));
            }

            let node_id = NodeId::parse(parts[0])
                .map_err(|_| ConfigParseError::InvalidNodeId(parts[0].to_string()))?;

            // parse address: "ip:port@bus-port"
            let addr_str = parts[1];
            let (client_addr, bus_port) = parse_addr_field(addr_str)?;

            let flags_str = parts[2];
            let is_myself = flags_str.contains("myself");
            let is_replica = flags_str.contains("slave");

            let replicates = if parts[3] == "-" {
                None
            } else {
                Some(
                    NodeId::parse(parts[3])
                        .map_err(|_| ConfigParseError::InvalidNodeId(parts[3].to_string()))?,
                )
            };

            let node_config_epoch: u64 = parts[6]
                .parse()
                .map_err(|_| ConfigParseError::InvalidNodeLine(line.to_string()))?;

            // parse slot ranges (fields 8+)
            let mut slot_ranges = Vec::new();
            for part in &parts[8..] {
                let range = parse_slot_range(part)?;
                slot_ranges.push(range);
            }

            // assign slots in the slot map
            for range in &slot_ranges {
                for slot in range.iter() {
                    slot_map.assign(slot, node_id);
                }
            }

            let bus_addr = SocketAddr::new(client_addr.ip(), bus_port);
            let role = if is_replica {
                NodeRole::Replica
            } else {
                NodeRole::Primary
            };

            let node = ClusterNode {
                id: node_id,
                addr: client_addr,
                cluster_bus_addr: bus_addr,
                role,
                slots: slot_ranges,
                replicates,
                replicas: Vec::new(),
                last_seen: Instant::now(),
                last_ping_sent: None,
                last_pong_received: None,
                flags: NodeFlags {
                    myself: is_myself,
                    ..NodeFlags::default()
                },
                config_epoch: node_config_epoch,
            };

            if is_myself {
                local_id = Some(node_id);
            }

            nodes.insert(node_id, node);
        }

        if !found_vars {
            return Err(ConfigParseError::MissingVarsLine);
        }

        let local_id = local_id.ok_or(ConfigParseError::NoMyselfNode)?;

        // rebuild replica links
        let replica_links: Vec<(NodeId, NodeId)> = nodes
            .values()
            .filter_map(|n| n.replicates.map(|primary| (primary, n.id)))
            .collect();

        for (primary_id, replica_id) in replica_links {
            if let Some(primary) = nodes.get_mut(&primary_id) {
                primary.replicas.push(replica_id);
            }
        }

        let mut state = ClusterState {
            nodes,
            local_id,
            config_epoch,
            slot_map,
            state: ClusterHealth::Unknown,
        };
        state.update_health();

        Ok((state, incarnation))
    }
}

/// Parses the `ip:port@bus-port` field from a nodes.conf line.
fn parse_addr_field(s: &str) -> Result<(SocketAddr, u16), ConfigParseError> {
    // format: "ip:port@bus-port"
    let at_pos = s
        .find('@')
        .ok_or_else(|| ConfigParseError::InvalidAddress(s.to_string()))?;

    let client_part = &s[..at_pos];
    let bus_part = &s[at_pos + 1..];

    let client_addr: SocketAddr = client_part
        .parse()
        .map_err(|_| ConfigParseError::InvalidAddress(s.to_string()))?;

    let bus_port: u16 = bus_part
        .parse()
        .map_err(|_| ConfigParseError::InvalidAddress(s.to_string()))?;

    Ok((client_addr, bus_port))
}

/// Parses a slot range string like "0-5460" or "100".
fn parse_slot_range(s: &str) -> Result<SlotRange, ConfigParseError> {
    if let Some((start_str, end_str)) = s.split_once('-') {
        let start: u16 = start_str
            .parse()
            .map_err(|_| ConfigParseError::InvalidSlotRange(s.to_string()))?;
        let end: u16 = end_str
            .parse()
            .map_err(|_| ConfigParseError::InvalidSlotRange(s.to_string()))?;
        SlotRange::try_new(start, end)
            .map_err(|_| ConfigParseError::InvalidSlotRange(s.to_string()))
    } else {
        let slot: u16 = s
            .parse()
            .map_err(|_| ConfigParseError::InvalidSlotRange(s.to_string()))?;
        SlotRange::try_new(slot, slot)
            .map_err(|_| ConfigParseError::InvalidSlotRange(s.to_string()))
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
    fn nodes_conf_roundtrip_single_node() {
        let id = NodeId::new();
        let mut node = ClusterNode::new_primary(id, test_addr(6379));
        node.set_myself();

        let state = ClusterState::single_node(node);
        let conf = state.to_nodes_conf(42);

        let (restored, incarnation) = ClusterState::from_nodes_conf(&conf).unwrap();
        assert_eq!(incarnation, 42);
        assert_eq!(restored.local_id, id);
        assert_eq!(restored.config_epoch, 1);
        assert!(restored.owns_slot(0));
        assert!(restored.owns_slot(16383));
        assert_eq!(restored.nodes.len(), 1);
        assert!(restored.nodes[&id].flags.myself);
    }

    #[test]
    fn nodes_conf_roundtrip_multi_node() {
        let id1 = NodeId::new();
        let id2 = NodeId::new();

        let mut node1 = ClusterNode::new_primary(id1, test_addr(6379));
        node1.set_myself();

        let mut state = ClusterState::new(id1);
        state.add_node(node1);

        let node2 = ClusterNode::new_primary(id2, test_addr(6380));
        state.add_node(node2);

        // assign slots to each
        for slot in 0..8192 {
            state.slot_map.assign(slot, id1);
        }
        for slot in 8192..16384 {
            state.slot_map.assign(slot, id2);
        }
        if let Some(n) = state.nodes.get_mut(&id1) {
            n.slots = state.slot_map.slots_for_node(id1);
        }
        if let Some(n) = state.nodes.get_mut(&id2) {
            n.slots = state.slot_map.slots_for_node(id2);
        }
        state.config_epoch = 5;
        state.update_health();

        let conf = state.to_nodes_conf(10);
        let (restored, incarnation) = ClusterState::from_nodes_conf(&conf).unwrap();

        assert_eq!(incarnation, 10);
        assert_eq!(restored.config_epoch, 5);
        assert_eq!(restored.local_id, id1);
        assert_eq!(restored.nodes.len(), 2);
        assert_eq!(restored.slot_map.owner(0), Some(id1));
        assert_eq!(restored.slot_map.owner(8192), Some(id2));
        assert_eq!(restored.slot_map.owner(16383), Some(id2));
        assert_eq!(restored.state, ClusterHealth::Ok);
    }

    #[test]
    fn nodes_conf_roundtrip_with_replica() {
        let primary_id = NodeId::new();
        let replica_id = NodeId::new();

        let mut primary = ClusterNode::new_primary(primary_id, test_addr(6379));
        primary.set_myself();

        let mut state = ClusterState::single_node(primary);

        let replica = ClusterNode::new_replica(replica_id, test_addr(6380), primary_id);
        state.add_node(replica);

        // add replica to primary's replica list
        if let Some(p) = state.nodes.get_mut(&primary_id) {
            p.replicas.push(replica_id);
        }

        let conf = state.to_nodes_conf(1);
        let (restored, _) = ClusterState::from_nodes_conf(&conf).unwrap();

        assert_eq!(restored.nodes.len(), 2);
        let restored_replica = &restored.nodes[&replica_id];
        assert_eq!(restored_replica.role, NodeRole::Replica);
        assert_eq!(restored_replica.replicates, Some(primary_id));

        // replica links are rebuilt
        let restored_primary = &restored.nodes[&primary_id];
        assert!(restored_primary.replicas.contains(&replica_id));
    }

    #[test]
    fn nodes_conf_parse_errors() {
        // missing vars line
        let result = ClusterState::from_nodes_conf("# comment\n");
        assert!(result.is_err());

        // no myself flag
        let result = ClusterState::from_nodes_conf(
            "vars currentEpoch 0 lastIncarnation 0\n\
             00000000-0000-0000-0000-000000000001 127.0.0.1:6379@16379 master - 0 0 0 connected\n",
        );
        assert!(matches!(
            result.unwrap_err(),
            ConfigParseError::NoMyselfNode
        ));

        // malformed vars
        let result = ClusterState::from_nodes_conf("vars bad\n");
        assert!(result.is_err());
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

    #[test]
    fn promote_replica_transfers_slots() {
        let primary_id = NodeId::new();
        let replica_id = NodeId::new();

        let mut primary = ClusterNode::new_primary(primary_id, test_addr(6379));
        primary.set_myself();

        let mut state = ClusterState::single_node(primary);
        let replica = ClusterNode::new_replica(replica_id, test_addr(6380), primary_id);
        state.add_node(replica);

        // register the replica in the primary's replica list
        state.nodes.get_mut(&primary_id).unwrap().replicas.push(replica_id);

        let initial_epoch = state.config_epoch;
        state.promote_replica(replica_id).unwrap();

        // epoch should have been bumped
        assert_eq!(state.config_epoch, initial_epoch + 1);

        // the promoted node is now a primary with all slots
        let new_primary = state.nodes.get(&replica_id).unwrap();
        assert_eq!(new_primary.role, NodeRole::Primary);
        assert_eq!(new_primary.replicates, None);
        assert!(new_primary.replicas.contains(&primary_id));
        assert!(!new_primary.slots.is_empty());

        // the old primary is now a replica
        let old_primary = state.nodes.get(&primary_id).unwrap();
        assert_eq!(old_primary.role, NodeRole::Replica);
        assert_eq!(old_primary.replicates, Some(replica_id));
        assert!(old_primary.slots.is_empty());

        // slot ownership must be transferred
        for slot in 0..SLOT_COUNT {
            assert_eq!(state.slot_map.owner(slot), Some(replica_id));
        }

        // cluster should still be healthy
        assert_eq!(state.state, ClusterHealth::Ok);
    }

    #[test]
    fn promote_replica_rejects_non_replica() {
        let id = NodeId::new();
        let mut node = ClusterNode::new_primary(id, test_addr(6379));
        node.set_myself();
        let mut state = ClusterState::single_node(node);

        let err = state.promote_replica(id).unwrap_err();
        assert!(err.contains("not a replica"), "unexpected error: {err}");
    }

    #[test]
    fn promote_replica_rejects_unknown_node() {
        let id = NodeId::new();
        let mut node = ClusterNode::new_primary(id, test_addr(6379));
        node.set_myself();
        let mut state = ClusterState::single_node(node);

        let missing = NodeId::new();
        let err = state.promote_replica(missing).unwrap_err();
        assert!(err.contains("not found"), "unexpected error: {err}");
    }
}
