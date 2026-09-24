//! The gossip socket and the loop that applies gossip events.

use super::*;

impl ClusterCoordinator {
    /// Pushes the local node's current slot ownership into the gossip engine
    /// so it propagates to the rest of the cluster.
    /// Sends gossip messages straight to their addresses.
    pub(super) async fn send_direct(
        &self,
        messages: impl IntoIterator<Item = (SocketAddr, GossipMessage)>,
    ) {
        let socket = self.udp_socket.lock().await;
        let Some(ref sock) = *socket else {
            return;
        };
        for (addr, msg) in messages {
            let encoded = match &self.secret {
                Some(s) => msg.encode_authenticated(s),
                None => msg.encode(),
            };
            if let Err(e) = sock.send_to(&encoded, addr).await {
                debug!("gossip send to {addr} failed: {e}");
            }
        }
    }

    pub(super) async fn broadcast_local_slots(&self, slots: Vec<SlotRange>, config_epoch: u64) {
        // Gather peer addresses and build the announce message while holding
        // the gossip lock, then release it before taking the socket lock.
        let (peer_addrs, encoded) = {
            let mut gossip = self.gossip.lock().await;
            gossip.set_local_slots(slots.clone(), config_epoch);
            let incarnation = gossip.local_incarnation();
            // Queue for piggybacking on future ticks as a fallback.
            gossip.queue_slots_update(self.local_id, incarnation, config_epoch, slots.clone());

            // Build an eager push to every known peer so they learn immediately
            // rather than waiting for the probabilistic gossip tick to select them.
            let msg = GossipMessage::SlotsAnnounce {
                sender: self.local_id,
                incarnation,
                config_epoch,
                slots,
            };
            let encoded = match &self.secret {
                Some(s) => msg.encode_authenticated(s),
                None => msg.encode(),
            };
            (gossip.alive_member_addrs(), encoded)
        };

        // Send directly to all alive peers (gossip lock released).
        let socket = self.udp_socket.lock().await;
        if let Some(ref sock) = *socket {
            for addr in &peer_addrs {
                if let Err(e) = sock.send_to(&encoded, addr).await {
                    debug!("slot broadcast to {addr} failed: {e}");
                }
            }
        }
    }

    /// Spawns the gossip network tasks: UDP send/receive and event consumer.
    pub async fn spawn_gossip(
        self: &Arc<Self>,
        bind_addr: SocketAddr,
        mut event_rx: mpsc::Receiver<GossipEvent>,
    ) {
        let gossip_port = match bind_addr.port().checked_add(self.gossip_port_offset) {
            Some(p) => p,
            None => {
                error!("gossip port offset overflows u16");
                return;
            }
        };
        let gossip_addr = SocketAddr::new(bind_addr.ip(), gossip_port);

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
                        // take events while locked, send them after the lock
                        // is released: the event consumer takes this lock too
                        let (outgoing, events) = {
                            let mut gossip = coordinator.gossip.lock().await;
                            (gossip.tick(), gossip.take_events())
                        };
                        events.send().await;
                        for (target_addr, msg) in outgoing {
                            let encoded = match &coordinator.secret {
                                Some(s) => msg.encode_authenticated(s),
                                None => msg.encode(),
                            };
                            if let Err(e) = sock.send_to(&encoded, target_addr).await {
                                debug!("gossip send error to {target_addr}: {e}");
                            }
                        }
                    }

                    result = sock.recv_from(&mut recv_buf) => {
                        match result {
                            Ok((len, from)) => {
                                let decode_result = match &coordinator.secret {
                                    Some(s) => GossipMessage::decode_authenticated(&recv_buf[..len], s),
                                    None => GossipMessage::decode(&recv_buf[..len]),
                                };
                                match decode_result {
                                    Ok(msg) => {
                                        let (replies, events) = {
                                            let mut gossip = coordinator.gossip.lock().await;
                                            (gossip.handle_message(msg, from), gossip.take_events())
                                        };
                                        events.send().await;
                                        for (addr, reply) in replies {
                                            let encoded = match &coordinator.secret {
                                                Some(s) => reply.encode_authenticated(s),
                                                None => reply.encode(),
                                            };
                                            if let Err(e) = sock.send_to(&encoded, addr).await {
                                                debug!("gossip reply error to {addr}: {e}");
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
            // PostAction defers work that must run after the state write-lock is released.
            // Holding the lock while calling gossip or async failover methods would deadlock.
            enum PostAction {
                None,
                StartElection(NodeId),
                HandleVoteRequest {
                    candidate: NodeId,
                    failed_primary: NodeId,
                    epoch: u64,
                },
                HandleVoteGranted {
                    from: NodeId,
                    candidate: NodeId,
                    epoch: u64,
                },
                /// Another node took slots from this one with a newer epoch.
                AnnounceLocalSlots,
            }

            while let Some(event) = event_rx.recv().await {
                let mut post_action = PostAction::None;

                let needs_save = {
                    let mut state = coordinator.state.write().await;
                    match event {
                        GossipEvent::MemberJoined(id, gossip_addr, slots, epoch) => {
                            info!("cluster: node {} joined at {}", id, gossip_addr);
                            if state.nodes.contains_key(&id) {
                                false
                            } else {
                                // cluster_meet creates a handshake placeholder with a
                                // random id but the right addresses. Only such a
                                // placeholder at this exact gossip address is replaced:
                                // matching on the port alone replaced real nodes on
                                // other hosts that use the same port.
                                let stale_id = state
                                    .nodes
                                    .values()
                                    .find(|n| {
                                        n.flags.handshake
                                            && n.id != id
                                            && n.cluster_bus_addr == gossip_addr
                                    })
                                    .map(|n| n.id);

                                let data_addr = if let Some(stale) = stale_id {
                                    // Preserve the data-port address from the placeholder.
                                    let saved = state.nodes[&stale].addr;
                                    state.nodes.remove(&stale);
                                    saved
                                } else {
                                    // No placeholder — derive data addr from gossip port.
                                    let data_port = gossip_addr
                                        .port()
                                        .saturating_sub(coordinator.gossip_port_offset);
                                    SocketAddr::new(gossip_addr.ip(), data_port)
                                };

                                let mut node = ClusterNode::new_primary_with_offset(
                                    id,
                                    data_addr,
                                    coordinator.gossip_port_offset,
                                );
                                node.config_epoch = epoch;
                                state.add_node(node);
                                if state.apply_slot_claim(id, &slots, epoch).local_lost {
                                    post_action = PostAction::AnnounceLocalSlots;
                                }
                                state.update_health();

                                // replicate the new node into raft state and update
                                // raft membership if we are the current leader
                                if let Some(raft) = coordinator.raft_node.get() {
                                    let raft = Arc::clone(raft);
                                    let raft_id = raft_id_from_node_id(id);
                                    let data_addr_str = data_addr.to_string();
                                    let raft_port_offset = coordinator.gossip_port_offset + 1;
                                    let raft_addr = SocketAddr::new(
                                        data_addr.ip(),
                                        data_addr.port().saturating_add(raft_port_offset),
                                    );
                                    tokio::spawn(async move {
                                        // add to application state machine
                                        let _ = raft
                                            .propose(ClusterCommand::AddNode {
                                                node_id: id,
                                                raft_id,
                                                addr: data_addr_str,
                                                is_primary: true,
                                            })
                                            .await;

                                        // add to raft membership if we're the leader
                                        if raft.is_leader() {
                                            let node = BasicNode {
                                                addr: raft_addr.to_string(),
                                            };
                                            let handle = raft.raft_handle();
                                            if handle.add_learner(raft_id, node, true).await.is_ok()
                                            {
                                                let m = handle.metrics().borrow().clone();
                                                let mut new_members: std::collections::BTreeSet<
                                                    u64,
                                                > = m
                                                    .membership_config
                                                    .membership()
                                                    .voter_ids()
                                                    .collect();
                                                new_members.insert(raft_id);
                                                let _ = handle
                                                    .change_membership(new_members, false)
                                                    .await;
                                            }
                                        }
                                    });
                                }

                                true
                            }
                        }
                        GossipEvent::MemberSuspected(id) => {
                            info!("cluster: node {} suspected", id);
                            if let Some(node) = state.nodes.get_mut(&id) {
                                node.flags.pfail = true;
                            }
                            state.update_health();
                            false // suspicion is transient, don't persist
                        }
                        GossipEvent::MemberFailed(id) => {
                            warn!("cluster: node {} confirmed failed", id);
                            if let Some(node) = state.nodes.get_mut(&id) {
                                node.flags.fail = true;
                                node.flags.pfail = false;
                            }
                            state.update_health();
                            // if we are a replica of the failed node, start an election
                            let replicates_failed = state
                                .nodes
                                .get(&coordinator.local_id)
                                .and_then(|n| n.replicates)
                                .map(|primary_id| primary_id == id)
                                .unwrap_or(false);
                            if replicates_failed {
                                post_action = PostAction::StartElection(id);
                            }
                            true
                        }
                        GossipEvent::MemberLeft(id) => {
                            info!("cluster: node {} left", id);
                            state.remove_node(id);
                            state.update_health();
                            true
                        }
                        GossipEvent::MemberAlive(id) => {
                            debug!("cluster: node {} alive", id);
                            if let Some(node) = state.nodes.get_mut(&id) {
                                node.flags.pfail = false;
                                node.flags.fail = false;
                            }
                            state.update_health();
                            false
                        }
                        GossipEvent::SlotsChanged(id, slots, epoch) => {
                            // The local node is authoritative for its own slot ownership;
                            // external gossip about it must never overwrite canonical state.
                            if id == coordinator.local_id {
                                false
                            } else {
                                debug!(
                                    "cluster: node {} claims {} slot ranges at epoch {}",
                                    id,
                                    slots.len(),
                                    epoch
                                );
                                let result = state.apply_slot_claim(id, &slots, epoch);
                                if result.local_lost {
                                    warn!(
                                        "cluster: node {} took slots from this node at epoch {}",
                                        id, epoch
                                    );
                                    post_action = PostAction::AnnounceLocalSlots;
                                }
                                state.update_health();
                                result.changed
                            }
                        }
                        GossipEvent::RoleChanged(id, is_primary, replicates) => {
                            debug!(
                                "cluster: node {} role changed to {}",
                                id,
                                if is_primary { "primary" } else { "replica" }
                            );
                            if let Some(node) = state.nodes.get_mut(&id) {
                                node.role = if is_primary {
                                    NodeRole::Primary
                                } else {
                                    NodeRole::Replica
                                };
                                node.replicates = replicates;
                            }
                            state.update_health();
                            true
                        }
                        GossipEvent::VoteRequested {
                            candidate,
                            failed_primary,
                            epoch,
                            offset: _,
                        } => {
                            // Handle outside the lock so we can call gossip.
                            post_action = PostAction::HandleVoteRequest {
                                candidate,
                                failed_primary,
                                epoch,
                            };
                            false
                        }
                        GossipEvent::VoteGranted {
                            from,
                            candidate,
                            epoch,
                        } => {
                            post_action = PostAction::HandleVoteGranted {
                                from,
                                candidate,
                                epoch,
                            };
                            false
                        }
                    }
                };

                // Handle post-lock actions outside the state write-lock.
                match post_action {
                    PostAction::None => {}
                    PostAction::StartElection(primary_id) => {
                        let coord = Arc::clone(&coordinator);
                        tokio::spawn(async move {
                            coord.start_election(primary_id).await;
                        });
                    }
                    PostAction::HandleVoteRequest {
                        candidate,
                        failed_primary,
                        epoch,
                    } => {
                        coordinator
                            .handle_vote_request(candidate, failed_primary, epoch)
                            .await;
                    }
                    PostAction::HandleVoteGranted {
                        from,
                        candidate,
                        epoch,
                    } => {
                        coordinator
                            .handle_vote_granted(from, candidate, epoch)
                            .await;
                    }
                    PostAction::AnnounceLocalSlots => {
                        let (slots, epoch) = {
                            let state = coordinator.state.read().await;
                            (
                                state.slot_map.slots_for_node(coordinator.local_id),
                                state.local_epoch(),
                            )
                        };
                        coordinator.broadcast_local_slots(slots, epoch).await;
                    }
                }

                if needs_save {
                    coordinator.save_config().await;
                }
            }
        });
    }
}
