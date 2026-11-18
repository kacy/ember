//! The CLUSTER subcommands and the per-command slot checks.

use super::*;
use ember_protocol::Command;

impl ClusterCoordinator {
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

        // build the join message, then release the gossip lock before any awaits
        let encoded = {
            let mut gossip = self.gossip.lock().await;
            gossip.add_seed(new_id, gossip_addr);
            let msg = gossip.create_join_message();
            self.encode_gossip(&msg)
        };

        // Insert placeholder before sending UDP so the gossip receive task always
        // finds an entry during MemberJoined resolution. Without this, the Welcome
        // reply can arrive and be processed before we add the placeholder, leaving
        // both a real entry and a stale placeholder in state.nodes.
        {
            let mut state = self.state.write().await;
            let mut node =
                ClusterNode::new_primary_with_offset(new_id, addr, self.gossip_port_offset);
            // a placeholder until the node answers with its real id
            node.flags.handshake = true;
            state.add_node(node);
        }

        // send join message via UDP
        {
            let socket = self.udp_socket.lock().await;
            if let Some(ref sock) = *socket {
                if let Err(e) = sock.send_to(&encoded, gossip_addr).await {
                    warn!("failed to send join to {gossip_addr}: {e}");
                    return Frame::Error(format!("ERR failed to send join: {e}"));
                }
            } else {
                return Frame::Error("ERR gossip socket not ready".into());
            }
        }

        self.save_config().await;

        Frame::Simple("OK".into())
    }

    /// CLUSTER ADDSLOTS slot [slot ...]
    pub async fn cluster_addslots(&self, slots: &[u16]) -> Frame {
        // validate inputs against the current routing state
        {
            let state = self.state.read().await;
            for &slot in slots {
                if slot >= SLOT_COUNT {
                    return Frame::Error(format!("ERR Invalid or out of range slot {slot}"));
                }
                if state.slot_map.owner(slot).is_some() {
                    return Frame::Error(format!("ERR Slot {slot} is already busy"));
                }
            }
        }

        let (new_slots, epoch) = {
            let mut state = self.state.write().await;
            for &slot in slots {
                state.slot_map.assign(slot, self.local_id);
            }
            let new_slots = state.slot_map.slots_for_node(self.local_id);
            if let Some(node) = state.nodes.get_mut(&self.local_id) {
                node.slots = new_slots.clone();
            }
            state.update_health();
            (new_slots, state.local_epoch())
        };
        self.broadcast_local_slots(new_slots, epoch).await;
        self.save_config().await;
        Frame::Simple("OK".into())
    }

    /// CLUSTER DELSLOTS slot [slot ...]
    pub async fn cluster_delslots(&self, slots: &[u16]) -> Frame {
        // validate
        {
            let state = self.state.read().await;
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
        }

        let (new_slots, epoch) = {
            let mut state = self.state.write().await;
            for &slot in slots {
                state.slot_map.unassign(slot);
            }
            let new_slots = state.slot_map.slots_for_node(self.local_id);
            if let Some(node) = state.nodes.get_mut(&self.local_id) {
                node.slots = new_slots.clone();
            }
            state.update_health();
            (new_slots, state.local_epoch())
        };
        self.broadcast_local_slots(new_slots, epoch).await;
        self.save_config().await;
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

        // verify the node exists before proposing
        {
            let state = self.state.read().await;
            if !state.nodes.contains_key(&node_id) {
                return Frame::Error("ERR Unknown node ID".into());
            }
        }

        let mut state = self.state.write().await;
        match state.remove_node(node_id) {
            Some(_) => {
                drop(state);
                self.save_config().await;
                Frame::Simple("OK".into())
            }
            // already removed by reconciliation — that's fine
            None => Frame::Simple("OK".into()),
        }
    }

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

        let mut migration = self.migration.write().unwrap();
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

        // record in-node migration state so ASK redirects work during transfer
        {
            let mut migration = self.migration.write().unwrap();
            if let Err(e) = migration.start_migrate(slot, self.local_id, target_id) {
                return Frame::Error(format!("ERR {e}"));
            }
        }

        Frame::Simple("OK".into())
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

        {
            let mut migration = self.migration.write().unwrap();
            migration.complete_migration(slot);
        }

        let (local_slots, epoch) = {
            let mut state = self.state.write().await;
            state.slot_map.assign(slot, node_id);
            // the previous owner still claims the slot at its epoch
            // until it hears of this, so claim it at a newer one
            if node_id == self.local_id {
                state.bump_local_epoch();
            }

            let new_slots = state.slot_map.slots_for_node(node_id);
            if let Some(node) = state.nodes.get_mut(&node_id) {
                node.slots = new_slots;
            }

            let local_slots = state.slot_map.slots_for_node(self.local_id);
            if node_id != self.local_id {
                if let Some(node) = state.nodes.get_mut(&self.local_id) {
                    node.slots = local_slots.clone();
                }
            }

            state.update_health();
            (local_slots, state.local_epoch())
        };

        self.broadcast_local_slots(local_slots, epoch).await;
        self.save_config().await;
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

        let mut migration = self.migration.write().unwrap();
        migration.abort_migration(slot);
        Frame::Simple("OK".into())
    }

    /// Decides whether this node may run `cmd`, from one read of the
    /// cluster state. The connection calls it for every command, so it
    /// takes each lock once.
    ///
    /// In order, it replies with:
    /// - READONLY for a write while failover has paused writes
    /// - MOVED to the slot's primary for a write on a replica, or READONLY
    ///   if that primary is unknown. Replicas serve every read
    /// - CLUSTERDOWN for a write on a primary that can't reach a majority
    ///   of the primaries
    /// - CROSSSLOT when the keys hash to different slots
    /// - the result of [`slot_check`] for the first key
    pub async fn check_command(&self, cmd: &Command, asking: bool) -> SlotCheck {
        let keys = cmd.keys();
        let first = keys.iter().next();
        let state = self.state.read().await;
        let is_replica = state
            .nodes
            .get(&self.local_id)
            .is_some_and(|n| n.role == NodeRole::Replica);

        if cmd.is_write() {
            if self.is_writes_paused() {
                return SlotCheck::Reply(Frame::Error(
                    "READONLY Failover in progress; writes are temporarily paused.".into(),
                ));
            }
            if is_replica {
                let slot = first.map(|key| key_slot(key.as_bytes()));
                return SlotCheck::Reply(Frame::Error(
                    match slot.and_then(|slot| primary_for_slot(&state, slot).map(|a| (slot, a))) {
                        Some((slot, addr)) => format!("MOVED {slot} {addr}"),
                        None => "READONLY You can't write against a read only replica.".into(),
                    },
                ));
            }
            if !self.reaches_majority_of_primaries(&state) {
                return SlotCheck::Reply(Frame::Error(
                    "CLUSTERDOWN this primary can't reach a majority of the primaries".into(),
                ));
            }
        }

        let Some(first) = first else {
            return SlotCheck::Local;
        };
        if is_replica {
            return SlotCheck::Local;
        }
        let slot = key_slot(first.as_bytes());
        if keys.iter().any(|key| key_slot(key.as_bytes()) != slot) {
            return SlotCheck::Reply(Frame::Error(
                "CROSSSLOT Keys in request don't hash to the same slot".into(),
            ));
        }
        let migration = self.migration.read().unwrap_or_else(|e| e.into_inner());
        slot_check(&state, &migration, first.as_bytes(), slot, asking)
    }

    /// Whether this node can reach a majority of the cluster's primaries,
    /// counting itself.
    ///
    /// A primary that can't is on the minority side of a partition, where
    /// the majority may already be promoting one of its replicas. It stops
    /// taking writes then, as Redis Cluster does, so writes aren't
    /// acknowledged on both sides and lost when the partition heals.
    fn reaches_majority_of_primaries(&self, state: &ClusterState) -> bool {
        let primaries = state
            .nodes
            .values()
            .filter(|n| n.role == NodeRole::Primary && !n.flags.handshake);
        let (mut total, mut reachable) = (0, 0);
        for node in primaries {
            total += 1;
            if node.id == self.local_id || node.is_healthy() {
                reachable += 1;
            }
        }
        reachable * 2 > total
    }

    /// Counts a key MIGRATE moved out of `slot`, for the migration's
    /// progress.
    pub async fn mark_key_migrated(&self, slot: u16) {
        self.migration.write().unwrap().key_migrated(slot);
    }

    /// Marks a key as being copied by MIGRATE, so commands for it get
    /// TRYAGAIN until [`end_key_transfer`](Self::end_key_transfer). Returns
    /// `false` if another MIGRATE is already copying it.
    pub async fn begin_key_transfer(&self, key: &[u8]) -> bool {
        self.migration.write().unwrap().begin_key_transfer(key)
    }

    pub async fn end_key_transfer(&self, key: &[u8]) {
        self.migration.write().unwrap().end_key_transfer(key);
    }

    /// CLUSTER REPLICATE node-id
    ///
    /// Makes this node a replica of the given primary. Updates local topology,
    /// queues a role-change gossip announcement, and persists nodes.conf.
    pub async fn cluster_replicate(&self, primary_id_str: &str) -> Frame {
        let primary_id = match NodeId::parse(primary_id_str) {
            Ok(id) => id,
            Err(_) => return Frame::Error("ERR Invalid node ID".into()),
        };

        if primary_id == self.local_id {
            return Frame::Error("ERR Cannot replicate self".into());
        }

        // verify the target exists and is a primary
        {
            let state = self.state.read().await;
            match state.nodes.get(&primary_id) {
                None => return Frame::Error("ERR Unknown node ID".into()),
                Some(node) if node.role != NodeRole::Primary => {
                    return Frame::Error("ERR Target node is not a primary".into())
                }
                _ => {}
            }
        }

        // update local cluster state
        {
            let mut state = self.state.write().await;
            if let Some(node) = state.nodes.get_mut(&self.local_id) {
                node.role = NodeRole::Replica;
                node.replicates = Some(primary_id);
            }
            // register ourselves in the primary's replica list
            if let Some(primary) = state.nodes.get_mut(&primary_id) {
                if !primary.replicas.contains(&self.local_id) {
                    primary.replicas.push(self.local_id);
                }
            }
            state.update_health();
        }

        // queue the role change for gossip, under a new incarnation so
        // peers do not discard it as already seen
        {
            let mut gossip = self.gossip.lock().await;
            let incarnation = gossip.bump_incarnation();
            gossip.queue_role_update(self.local_id, incarnation, false, Some(primary_id));
        }

        self.save_config().await;

        // connect to the primary's replication port and start streaming
        self.start_replication_client(primary_id).await;

        Frame::Simple("OK".into())
    }
}

/// Where the key `key` in `slot` is served, for a node that may serve it.
///
/// During a migration, as in Redis Cluster:
/// - the source serves the keys it still has, and sends a client whose
///   keys are gone to the target with ASK ([`SlotCheck::AskIfMissing`]).
///   Keys written during the move so land on the target too
/// - the target serves the slot to a client that sent ASKING
/// - a key MIGRATE is copying right now gets TRYAGAIN
fn slot_check(
    state: &ClusterState,
    migration: &MigrationManager,
    key: &[u8],
    slot: u16,
    asking: bool,
) -> SlotCheck {
    if state.owns_slot(slot) {
        // MIGRATE is copying this key: a write now would be lost when the
        // local copy is deleted, so the client retries shortly
        if migration.is_key_in_flight(key) {
            return SlotCheck::Reply(Frame::Error(
                "TRYAGAIN the key is being migrated, retry shortly".into(),
            ));
        }
        let target = migration
            .get_outgoing(slot)
            .and_then(|m| state.nodes.get(&m.target));
        return match target {
            Some(target) => SlotCheck::AskIfMissing {
                slot,
                addr: target.addr.to_string(),
            },
            None => SlotCheck::Local,
        };
    }

    // not our slot, but we are importing it and the client sent ASKING
    if migration.is_importing(slot) && asking {
        return SlotCheck::Local;
    }

    SlotCheck::Reply(match state.slot_owner(slot) {
        Some(owner) => Frame::Error(format!("MOVED {} {}", slot, owner.addr)),
        None => Frame::Error("CLUSTERDOWN Hash slot not served".into()),
    })
}

/// The address of the primary that owns `slot`, if that owner is a known
/// primary.
fn primary_for_slot(state: &ClusterState, slot: u16) -> Option<std::net::SocketAddr> {
    let node = state.nodes.get(&state.slot_map.owner(slot)?)?;
    (node.role == NodeRole::Primary).then_some(node.addr)
}
