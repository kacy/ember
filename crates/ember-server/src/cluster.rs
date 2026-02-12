//! Cluster coordination layer for the ember server.
//!
//! Wraps the ember-cluster crate's types into a server-integrated
//! coordinator that handles gossip networking, cluster commands,
//! and slot ownership validation.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use ember_cluster::{
    key_slot, ClusterNode, ClusterState, GossipConfig, GossipEngine, GossipEvent, GossipMessage,
    MigrationManager, NodeId, SLOT_COUNT,
};
use ember_protocol::Frame;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

/// Integration struct wrapping cluster crate types for the running server.
///
/// Thread-safe via interior mutability: `RwLock` for state (many readers,
/// rare writers) and `Mutex` for gossip (single writer during ticks).
pub struct ClusterCoordinator {
    state: RwLock<ClusterState>,
    gossip: Mutex<GossipEngine>,
    migration: Mutex<MigrationManager>,
    local_id: NodeId,
    gossip_port_offset: u16,
    /// bound UDP socket for gossip, set after spawn_gossip
    udp_socket: Mutex<Option<Arc<UdpSocket>>>,
}

impl std::fmt::Debug for ClusterCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterCoordinator")
            .field("local_id", &self.local_id)
            .finish_non_exhaustive()
    }
}

impl ClusterCoordinator {
    /// Creates a new cluster coordinator.
    ///
    /// Returns the coordinator and a receiver for gossip events that
    /// should be consumed by a background task.
    pub fn new(
        local_id: NodeId,
        bind_addr: SocketAddr,
        gossip_config: GossipConfig,
        bootstrap: bool,
    ) -> (Self, mpsc::Receiver<GossipEvent>) {
        let (event_tx, event_rx) = mpsc::channel(256);

        let port_offset = gossip_config.gossip_port_offset;
        let gossip_addr =
            SocketAddr::new(bind_addr.ip(), bind_addr.port().saturating_add(port_offset));

        let gossip = GossipEngine::new(local_id, gossip_addr, gossip_config, event_tx);

        let state = if bootstrap {
            let mut node = ClusterNode::new_primary_with_offset(local_id, bind_addr, port_offset);
            node.set_myself();
            ClusterState::single_node(node)
        } else {
            let mut cs = ClusterState::new(local_id);
            let mut node = ClusterNode::new_primary_with_offset(local_id, bind_addr, port_offset);
            node.set_myself();
            cs.add_node(node);
            cs
        };

        let coordinator = Self {
            state: RwLock::new(state),
            gossip: Mutex::new(gossip),
            migration: Mutex::new(MigrationManager::new()),
            local_id,
            gossip_port_offset: port_offset,
            udp_socket: Mutex::new(None),
        };

        (coordinator, event_rx)
    }

    // -- cluster command handlers --

    /// CLUSTER INFO
    pub async fn cluster_info(&self) -> Frame {
        let state = self.state.read().await;
        Frame::Bulk(Bytes::from(state.cluster_info()))
    }

    /// CLUSTER NODES
    pub async fn cluster_nodes(&self) -> Frame {
        let state = self.state.read().await;
        Frame::Bulk(Bytes::from(state.cluster_nodes()))
    }

    /// CLUSTER MYID
    pub fn cluster_myid(&self) -> Frame {
        Frame::Bulk(Bytes::from(self.local_id.0.to_string()))
    }

    /// CLUSTER SLOTS — returns slot ranges in the Redis array format.
    pub async fn cluster_slots(&self) -> Frame {
        let state = self.state.read().await;

        let mut result = Vec::new();
        for node in state.primaries() {
            let ranges = state.slot_map.slots_for_node(node.id);
            for range in ranges {
                let mut entry = vec![
                    Frame::Integer(range.start as i64),
                    Frame::Integer(range.end as i64),
                    // node info: [ip, port, id]
                    Frame::Array(vec![
                        Frame::Bulk(Bytes::from(node.addr.ip().to_string())),
                        Frame::Integer(node.addr.port() as i64),
                        Frame::Bulk(Bytes::from(node.id.0.to_string())),
                    ]),
                ];

                // add replicas
                for replica in state.replicas_of(node.id) {
                    entry.push(Frame::Array(vec![
                        Frame::Bulk(Bytes::from(replica.addr.ip().to_string())),
                        Frame::Integer(replica.addr.port() as i64),
                        Frame::Bulk(Bytes::from(replica.id.0.to_string())),
                    ]));
                }

                result.push(Frame::Array(entry));
            }
        }

        Frame::Array(result)
    }

    /// CLUSTER MEET ip port
    pub async fn cluster_meet(&self, ip: &str, port: u16) -> Frame {
        let addr: SocketAddr = match format!("{ip}:{port}").parse() {
            Ok(a) => a,
            Err(e) => return Frame::Error(format!("ERR invalid address: {e}")),
        };

        let mut gossip = self.gossip.lock().await;
        let new_id = NodeId::new();
        let gossip_port = match port.checked_add(self.gossip_port_offset) {
            Some(p) => p,
            None => {
                return Frame::Error(format!(
                    "ERR port {port} + offset {} overflows",
                    self.gossip_port_offset
                ))
            }
        };
        let gossip_addr = SocketAddr::new(addr.ip(), gossip_port);

        gossip.add_seed(new_id, gossip_addr);

        // send join message via UDP
        let join_msg = gossip.create_join_message();
        let encoded = join_msg.encode();

        let socket = self.udp_socket.lock().await;
        if let Some(ref sock) = *socket {
            if let Err(e) = sock.send_to(&encoded, gossip_addr).await {
                warn!("failed to send join to {gossip_addr}: {e}");
                return Frame::Error(format!("ERR failed to send join: {e}"));
            }
        } else {
            return Frame::Error("ERR gossip socket not ready".into());
        }

        // add to cluster state as well
        let mut state = self.state.write().await;
        let node = ClusterNode::new_primary_with_offset(new_id, addr, self.gossip_port_offset);
        state.add_node(node);

        Frame::Simple("OK".into())
    }

    /// CLUSTER ADDSLOTS slot [slot ...]
    pub async fn cluster_addslots(&self, slots: &[u16]) -> Frame {
        let mut state = self.state.write().await;

        // validate: all slots must be unassigned
        for &slot in slots {
            if slot >= SLOT_COUNT {
                return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
            }
            if state.slot_map.owner(slot).is_some() {
                return Frame::Error(format!("ERR Slot {slot} is already busy"));
            }
        }

        // assign all slots to local node
        for &slot in slots {
            state.slot_map.assign(slot, self.local_id);
        }

        // update node.slots from slot_map
        let new_slots = state.slot_map.slots_for_node(self.local_id);
        if let Some(node) = state.nodes.get_mut(&self.local_id) {
            node.slots = new_slots;
        }

        state.update_health();
        Frame::Simple("OK".into())
    }

    /// CLUSTER DELSLOTS slot [slot ...]
    pub async fn cluster_delslots(&self, slots: &[u16]) -> Frame {
        let mut state = self.state.write().await;

        // validate: all slots must be owned by us
        for &slot in slots {
            if slot >= SLOT_COUNT {
                return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
            }
            match state.slot_map.owner(slot) {
                Some(owner) if owner != self.local_id => {
                    return Frame::Error(format!("ERR Slot {slot} is not owned by this node"));
                }
                None => {
                    return Frame::Error(format!("ERR Slot {slot} is already unassigned"));
                }
                _ => {}
            }
        }

        for &slot in slots {
            state.slot_map.unassign(slot);
        }

        // update node.slots from slot_map
        let new_slots = state.slot_map.slots_for_node(self.local_id);
        if let Some(node) = state.nodes.get_mut(&self.local_id) {
            node.slots = new_slots;
        }

        state.update_health();
        Frame::Simple("OK".into())
    }

    /// CLUSTER FORGET node-id
    pub async fn cluster_forget(&self, node_id_str: &str) -> Frame {
        let node_id = match NodeId::parse(node_id_str) {
            Ok(id) => id,
            Err(_) => return Frame::Error("ERR Invalid node ID".into()),
        };

        if node_id == self.local_id {
            return Frame::Error("ERR I tried hard but I can't forget myself...".into());
        }

        let mut state = self.state.write().await;
        match state.remove_node(node_id) {
            Some(_) => Frame::Simple("OK".into()),
            None => Frame::Error("ERR Unknown node ID".into()),
        }
    }

    // -- slot migration (SETSLOT) commands --

    /// CLUSTER SETSLOT <slot> IMPORTING <node-id>
    ///
    /// Marks a slot as importing from the given source node. The local node
    /// becomes the target of the migration.
    pub async fn cluster_setslot_importing(&self, slot: u16, node_id_str: &str) -> Frame {
        if slot >= SLOT_COUNT {
            return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
        }
        let source_id = match NodeId::parse(node_id_str) {
            Ok(id) => id,
            Err(_) => return Frame::Error("ERR Invalid node ID".into()),
        };
        if source_id == self.local_id {
            return Frame::Error("ERR can't import from myself".into());
        }

        let mut migration = self.migration.lock().await;
        match migration.start_import(slot, source_id, self.local_id) {
            Ok(_) => Frame::Simple("OK".into()),
            Err(e) => Frame::Error(format!("ERR {e}")),
        }
    }

    /// CLUSTER SETSLOT <slot> MIGRATING <node-id>
    ///
    /// Marks a slot as migrating to the given target node. The local node
    /// must currently own the slot.
    pub async fn cluster_setslot_migrating(&self, slot: u16, node_id_str: &str) -> Frame {
        if slot >= SLOT_COUNT {
            return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
        }
        let target_id = match NodeId::parse(node_id_str) {
            Ok(id) => id,
            Err(_) => return Frame::Error("ERR Invalid node ID".into()),
        };
        if target_id == self.local_id {
            return Frame::Error("ERR can't migrate to myself".into());
        }

        // verify we own the slot before allowing migration
        {
            let state = self.state.read().await;
            if !state.owns_slot(slot) {
                return Frame::Error(format!("ERR I'm not the owner of hash slot {slot}"));
            }
        }

        let mut migration = self.migration.lock().await;
        match migration.start_migrate(slot, self.local_id, target_id) {
            Ok(_) => Frame::Simple("OK".into()),
            Err(e) => Frame::Error(format!("ERR {e}")),
        }
    }

    /// CLUSTER SETSLOT <slot> NODE <node-id>
    ///
    /// Completes migration by assigning the slot to the given node.
    /// Cleans up any in-progress migration state.
    pub async fn cluster_setslot_node(&self, slot: u16, node_id_str: &str) -> Frame {
        if slot >= SLOT_COUNT {
            return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
        }
        let node_id = match NodeId::parse(node_id_str) {
            Ok(id) => id,
            Err(_) => return Frame::Error("ERR Invalid node ID".into()),
        };

        // complete any in-progress migration for this slot
        {
            let mut migration = self.migration.lock().await;
            migration.complete_migration(slot);
        }

        // assign the slot to the specified node
        let mut state = self.state.write().await;
        state.slot_map.assign(slot, node_id);

        // update the node's slot list
        let new_slots = state.slot_map.slots_for_node(node_id);
        if let Some(node) = state.nodes.get_mut(&node_id) {
            node.slots = new_slots;
        }

        // also update the local node's slot list if it changed
        if node_id != self.local_id {
            let local_slots = state.slot_map.slots_for_node(self.local_id);
            if let Some(node) = state.nodes.get_mut(&self.local_id) {
                node.slots = local_slots;
            }
        }

        state.update_health();
        Frame::Simple("OK".into())
    }

    /// CLUSTER SETSLOT <slot> STABLE
    ///
    /// Aborts any in-progress migration for the slot, clearing
    /// importing/migrating state without changing slot ownership.
    pub async fn cluster_setslot_stable(&self, slot: u16) -> Frame {
        if slot >= SLOT_COUNT {
            return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
        }

        let mut migration = self.migration.lock().await;
        migration.abort_migration(slot);
        Frame::Simple("OK".into())
    }

    // -- slot ownership check --

    /// Checks if the local node owns the slot for the given key.
    ///
    /// Returns `None` if local node owns the slot (proceed normally).
    /// Returns `Some(Frame::Error("MOVED ..."))` if another node owns it.
    /// Returns `Some(Frame::Error("CLUSTERDOWN ..."))` if slot is unassigned.
    pub async fn check_slot(&self, key: &[u8]) -> Option<Frame> {
        let slot = key_slot(key);
        let state = self.state.read().await;

        if state.owns_slot(slot) {
            return None;
        }

        match state.slot_owner(slot) {
            Some(owner) => Some(Frame::Error(format!("MOVED {} {}", slot, owner.addr))),
            None => Some(Frame::Error("CLUSTERDOWN Hash slot not served".into())),
        }
    }

    /// Checks that all keys hash to the same slot.
    ///
    /// Returns `Ok(())` if all keys are in the same slot.
    /// Returns `Err(Frame)` with a CROSSSLOT error if they span multiple slots.
    pub fn check_crossslot(&self, keys: &[String]) -> Result<(), Frame> {
        if keys.len() <= 1 {
            return Ok(());
        }
        let first_slot = key_slot(keys[0].as_bytes());
        for key in &keys[1..] {
            if key_slot(key.as_bytes()) != first_slot {
                return Err(Frame::Error(
                    "CROSSSLOT Keys in request don't hash to the same slot".into(),
                ));
            }
        }
        Ok(())
    }

    // -- gossip networking --

    /// Spawns the gossip network tasks: UDP send/receive and event consumer.
    pub async fn spawn_gossip(
        self: &Arc<Self>,
        bind_addr: SocketAddr,
        mut event_rx: mpsc::Receiver<GossipEvent>,
    ) {
        let gossip_addr = SocketAddr::new(
            bind_addr.ip(),
            bind_addr.port().saturating_add(self.gossip_port_offset),
        );

        let socket = match UdpSocket::bind(gossip_addr).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                error!("failed to bind gossip UDP socket on {gossip_addr}: {e}");
                return;
            }
        };

        info!("gossip listening on {gossip_addr}");

        // store socket for cluster_meet
        {
            let mut guard = self.udp_socket.lock().await;
            *guard = Some(Arc::clone(&socket));
        }

        // task 1: gossip tick + UDP recv/send loop
        let coordinator = Arc::clone(self);
        let sock = Arc::clone(&socket);
        tokio::spawn(async move {
            let mut recv_buf = vec![0u8; 65535];
            let mut tick_interval = tokio::time::interval(std::time::Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = tick_interval.tick() => {
                        let mut gossip = coordinator.gossip.lock().await;
                        if let Some((target_addr, msg)) = gossip.tick() {
                            let encoded = msg.encode();
                            if let Err(e) = sock.send_to(&encoded, target_addr).await {
                                debug!("gossip send error to {target_addr}: {e}");
                            }
                        }
                    }

                    result = sock.recv_from(&mut recv_buf) => {
                        match result {
                            Ok((len, from)) => {
                                match GossipMessage::decode(&recv_buf[..len]) {
                                    Ok(msg) => {
                                        let mut gossip = coordinator.gossip.lock().await;
                                        if let Some(reply) = gossip.handle_message(msg, from).await {
                                            let encoded = reply.encode();
                                            if let Err(e) = sock.send_to(&encoded, from).await {
                                                debug!("gossip reply error to {from}: {e}");
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        debug!("gossip decode error from {from}: {e}");
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("gossip recv error: {e}");
                            }
                        }
                    }
                }
            }
        });

        // task 2: gossip event consumer — updates cluster state
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                let mut state = coordinator.state.write().await;
                match event {
                    GossipEvent::MemberJoined(id, addr) => {
                        info!("cluster: node {} joined at {}", id, addr);
                        if !state.nodes.contains_key(&id) {
                            let node = ClusterNode::new_primary_with_offset(
                                id,
                                addr,
                                coordinator.gossip_port_offset,
                            );
                            state.add_node(node);
                        }
                    }
                    GossipEvent::MemberSuspected(id) => {
                        info!("cluster: node {} suspected", id);
                        if let Some(node) = state.nodes.get_mut(&id) {
                            node.flags.pfail = true;
                        }
                        state.update_health();
                    }
                    GossipEvent::MemberFailed(id) => {
                        warn!("cluster: node {} confirmed failed", id);
                        if let Some(node) = state.nodes.get_mut(&id) {
                            node.flags.fail = true;
                            node.flags.pfail = false;
                        }
                        state.update_health();
                    }
                    GossipEvent::MemberLeft(id) => {
                        info!("cluster: node {} left", id);
                        state.remove_node(id);
                        state.update_health();
                    }
                    GossipEvent::MemberAlive(id) => {
                        debug!("cluster: node {} alive", id);
                        if let Some(node) = state.nodes.get_mut(&id) {
                            node.flags.pfail = false;
                            node.flags.fail = false;
                        }
                        state.update_health();
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a test coordinator with a single node that owns no slots.
    fn test_coordinator() -> (ClusterCoordinator, mpsc::Receiver<GossipEvent>) {
        let local_id = NodeId::new();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let config = GossipConfig::default();
        ClusterCoordinator::new(local_id, addr, config, false)
    }

    /// Creates a test coordinator bootstrapped with all 16384 slots.
    fn test_coordinator_bootstrapped() -> (ClusterCoordinator, mpsc::Receiver<GossipEvent>) {
        let local_id = NodeId::new();
        let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
        let config = GossipConfig::default();
        ClusterCoordinator::new(local_id, addr, config, true)
    }

    #[tokio::test]
    async fn setslot_importing_valid() {
        let (coord, _rx) = test_coordinator();
        let source = NodeId::new();
        let resp = coord
            .cluster_setslot_importing(100, &source.0.to_string())
            .await;
        assert!(matches!(resp, Frame::Simple(_)));
    }

    #[tokio::test]
    async fn setslot_importing_invalid_slot() {
        let (coord, _rx) = test_coordinator();
        let source = NodeId::new();
        let resp = coord
            .cluster_setslot_importing(16384, &source.0.to_string())
            .await;
        assert!(matches!(resp, Frame::Error(_)));
    }

    #[tokio::test]
    async fn setslot_importing_self_rejected() {
        let (coord, _rx) = test_coordinator();
        let resp = coord
            .cluster_setslot_importing(100, &coord.local_id.0.to_string())
            .await;
        match resp {
            Frame::Error(msg) => assert!(msg.contains("can't import from myself")),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn setslot_importing_duplicate_rejected() {
        let (coord, _rx) = test_coordinator();
        let source = NodeId::new();
        let id_str = source.0.to_string();
        coord.cluster_setslot_importing(100, &id_str).await;
        let resp = coord.cluster_setslot_importing(100, &id_str).await;
        assert!(matches!(resp, Frame::Error(_)));
    }

    #[tokio::test]
    async fn setslot_migrating_valid() {
        let (coord, _rx) = test_coordinator_bootstrapped();
        let target = NodeId::new();
        let resp = coord
            .cluster_setslot_migrating(0, &target.0.to_string())
            .await;
        assert!(matches!(resp, Frame::Simple(_)));
    }

    #[tokio::test]
    async fn setslot_migrating_not_owner() {
        let (coord, _rx) = test_coordinator(); // no slots owned
        let target = NodeId::new();
        let resp = coord
            .cluster_setslot_migrating(100, &target.0.to_string())
            .await;
        match resp {
            Frame::Error(msg) => assert!(msg.contains("not the owner")),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn setslot_migrating_self_rejected() {
        let (coord, _rx) = test_coordinator_bootstrapped();
        let resp = coord
            .cluster_setslot_migrating(0, &coord.local_id.0.to_string())
            .await;
        match resp {
            Frame::Error(msg) => assert!(msg.contains("can't migrate to myself")),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn setslot_node_assigns_slot() {
        let (coord, _rx) = test_coordinator();
        let target = NodeId::new();

        // add the target node to cluster state
        {
            let mut state = coord.state.write().await;
            let node = ClusterNode::new_primary(target, "127.0.0.1:6380".parse().unwrap());
            state.add_node(node);
        }

        let resp = coord.cluster_setslot_node(100, &target.0.to_string()).await;
        assert!(matches!(resp, Frame::Simple(_)));

        // verify the slot is now owned by the target
        let state = coord.state.read().await;
        assert_eq!(state.slot_map.owner(100), Some(target));
    }

    #[tokio::test]
    async fn setslot_node_completes_migration() {
        let (coord, _rx) = test_coordinator_bootstrapped();
        let target = NodeId::new();

        // start a migration
        coord
            .cluster_setslot_migrating(0, &target.0.to_string())
            .await;

        // add target to state
        {
            let mut state = coord.state.write().await;
            let node = ClusterNode::new_primary(target, "127.0.0.1:6380".parse().unwrap());
            state.add_node(node);
        }

        // complete with NODE — should clean up migration state
        let resp = coord.cluster_setslot_node(0, &target.0.to_string()).await;
        assert!(matches!(resp, Frame::Simple(_)));

        // migration should be cleaned up
        let migration = coord.migration.lock().await;
        assert!(!migration.is_migrating(0));
    }

    #[tokio::test]
    async fn setslot_stable_aborts_migration() {
        let (coord, _rx) = test_coordinator();
        let source = NodeId::new();
        coord
            .cluster_setslot_importing(100, &source.0.to_string())
            .await;

        let resp = coord.cluster_setslot_stable(100).await;
        assert!(matches!(resp, Frame::Simple(_)));

        // migration should be cleaned up
        let migration = coord.migration.lock().await;
        assert!(!migration.is_importing(100));
    }

    #[tokio::test]
    async fn setslot_stable_noop_when_no_migration() {
        let (coord, _rx) = test_coordinator();
        // should succeed even with no active migration
        let resp = coord.cluster_setslot_stable(100).await;
        assert!(matches!(resp, Frame::Simple(_)));
    }
}
