//! Raft integration: proposing topology changes and applying committed ones.

use super::*;

impl ClusterCoordinator {
    /// Attaches a `RaftNode` so topology mutations go through consensus.
    ///
    /// Must be called at most once, after the coordinator is wrapped in `Arc`.
    /// If not called, all commands fall back to direct writes (useful in tests).
    pub fn attach_raft(&self, node: Arc<RaftNode>) {
        // ignore the error — it just means attach_raft was called twice
        let _ = self.raft_node.set(node);
    }

    /// Spawns a background task that reconciles `ClusterState` with the Raft
    /// state machine whenever committed entries are applied.
    ///
    /// The watch receiver fires after every `apply_to_state_machine` call on
    /// the Raft storage. Each update applies the slot owners that changed in
    /// Raft since the one before; the state Raft already had at startup is
    /// in the saved node config.
    pub fn spawn_raft_reconciliation(
        self: &Arc<Self>,
        mut state_rx: watch::Receiver<ClusterStateData>,
    ) {
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            let mut applied = state_rx.borrow().slots.clone();
            while state_rx.changed().await.is_ok() {
                let data = state_rx.borrow().clone();
                coordinator.apply_raft_state(&data, &applied).await;
                applied = data.slots;
            }
        });
    }

    /// Reconciles the local routing table with a committed Raft state.
    /// `applied` is the Raft slot map from the previous update.
    pub(super) async fn apply_raft_state(
        &self,
        data: &ClusterStateData,
        applied: &BTreeMap<u16, String>,
    ) {
        let mut state = self.state.write().await;

        // add nodes that are in raft state but not yet in the routing table
        for (key, info) in &data.nodes {
            let node_id = match NodeId::parse(key).ok() {
                Some(id) => id,
                None => continue,
            };
            if !state.nodes.contains_key(&node_id) {
                let addr: SocketAddr = match info.addr.parse() {
                    Ok(a) => a,
                    Err(_) => continue,
                };
                let node =
                    ClusterNode::new_primary_with_offset(node_id, addr, self.gossip_port_offset);
                state.add_node(node);
            }
        }

        // remove nodes that raft has dropped (never remove ourselves)
        let raft_ids: HashSet<NodeId> = data
            .nodes
            .keys()
            .filter_map(|k| NodeId::parse(k).ok())
            .collect();
        let to_remove: Vec<NodeId> = state
            .nodes
            .keys()
            .filter(|id| !raft_ids.contains(*id) && **id != self.local_id)
            .copied()
            .collect();
        for id in to_remove {
            state.nodes.remove(&id);
        }

        // apply only the slots whose owner changed in raft. raft runs on
        // the bootstrap node alone, and failovers between other nodes go
        // through gossip, so for the slots raft didn't touch it may still
        // name an old primary. overwriting every slot would route them back.
        for slot in 0..SLOT_COUNT {
            let raft_owner = data.slots.get(&slot);
            if raft_owner == applied.get(&slot) {
                continue;
            }
            match raft_owner.and_then(|k| NodeId::parse(k).ok()) {
                Some(owner) => state.slot_map.assign(slot, owner),
                None => state.slot_map.unassign(slot),
            }
        }

        // rebuild each node's slot list from the updated slot map
        let all_ids: Vec<NodeId> = state.nodes.keys().copied().collect();
        for id in all_ids {
            let slots = state.slot_map.slots_for_node(id);
            if let Some(node) = state.nodes.get_mut(&id) {
                node.slots = slots;
            }
        }

        state.update_health();
        drop(state);
        self.save_config().await;
    }

    /// Returns an error `Frame` for the given `RaftProposalError`.
    pub(super) fn raft_error_frame(e: RaftProposalError) -> Frame {
        Frame::Error(format!("ERR {e}"))
    }
}
