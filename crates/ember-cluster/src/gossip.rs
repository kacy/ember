//! SWIM gossip protocol implementation.
//!
//! Implements the Scalable Weakly-consistent Infection-style Membership
//! protocol for failure detection and cluster membership management.
//!
//! # Protocol Overview
//!
//! Each protocol period:
//! 1. Pick a random node to probe with PING
//! 2. If no ACK within timeout, send PING-REQ to k random nodes
//! 3. If still no ACK, mark node as SUSPECT
//! 4. After suspicion timeout, mark as DEAD
//! 5. Piggyback state updates on all messages

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use rand::prelude::IndexedRandom;
use tokio::sync::mpsc;
use tracing::{debug, info, trace, warn};

use crate::message::{GossipMessage, MemberInfo, NodeUpdate};
use crate::{NodeId, SlotRange};

/// Maximum allowed incarnation value. Rejects gossip updates with
/// incarnation numbers beyond this to prevent a malicious node from
/// sending u64::MAX and permanently disabling suspicion refutation.
const MAX_INCARNATION: u64 = u64::MAX / 2;

/// Maximum allowed single-hop incarnation jump. A legitimate node increments
/// its incarnation by 1 when refuting suspicion, so jumps larger than this
/// threshold indicate either a bug or a DoS attempt trying to exhaust the
/// incarnation space.
const MAX_INCARNATION_JUMP: u64 = 1000;

/// Each queued update is piggybacked on this many messages per doubling of
/// the cluster size, as in SWIM, so it survives lost packets and reaches
/// every member with high probability.
const RETRANSMIT_MULT: u32 = 3;

/// The most updates kept waiting to be piggybacked. When more are queued,
/// the ones sent most often are dropped first.
const MAX_PENDING_UPDATES: usize = 256;

/// Configuration for the gossip protocol.
#[derive(Debug, Clone)]
pub struct GossipConfig {
    /// How often to run the protocol period (probe a random node).
    pub protocol_period: Duration,
    /// How long to wait for a direct probe response.
    pub probe_timeout: Duration,
    /// Multiplier for suspicion timeout (protocol_period * suspicion_mult).
    pub suspicion_mult: u32,
    /// Number of nodes to ask for indirect probes.
    pub indirect_probes: usize,
    /// Maximum number of updates to piggyback per message.
    pub max_piggyback: usize,
    /// Port offset for gossip (data_port + gossip_port_offset).
    pub gossip_port_offset: u16,
}

impl Default for GossipConfig {
    fn default() -> Self {
        Self {
            protocol_period: Duration::from_secs(1),
            probe_timeout: Duration::from_millis(500),
            suspicion_mult: 5,
            indirect_probes: 3,
            max_piggyback: 10,
            gossip_port_offset: 10000,
        }
    }
}

/// Internal state of a cluster member as tracked by gossip.
#[derive(Debug, Clone)]
pub struct MemberState {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub incarnation: u64,
    pub state: MemberStatus,
    pub state_change: Instant,
    pub is_primary: bool,
    /// The primary this member replicates from, if it is a replica.
    pub replicates: Option<NodeId>,
    pub slots: Vec<SlotRange>,
    /// The config epoch of the member's latest slot claim.
    pub config_epoch: u64,
}

/// Health status of a member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberStatus {
    Alive,
    Suspect,
    Dead,
    Left,
}

/// Events emitted by the gossip engine.
#[derive(Debug, Clone)]
pub enum GossipEvent {
    /// A new node joined the cluster, with the slots it claims and the
    /// config epoch of the claim.
    MemberJoined(NodeId, SocketAddr, Vec<SlotRange>, u64),
    /// A node is suspected to be failing.
    MemberSuspected(NodeId),
    /// A node has been confirmed dead.
    MemberFailed(NodeId),
    /// A node left gracefully.
    MemberLeft(NodeId),
    /// A node that was suspected is now alive.
    MemberAlive(NodeId),
    /// A node's slot claim changed. Fields: node ID, slots, config epoch.
    SlotsChanged(NodeId, Vec<SlotRange>, u64),
    /// A node's role changed. Fields: node ID, is_primary, replicates.
    RoleChanged(NodeId, bool, Option<NodeId>),
    /// A replica requested votes for a failover election.
    VoteRequested {
        candidate: NodeId,
        /// The primary the candidate wants to replace.
        failed_primary: NodeId,
        epoch: u64,
        /// Candidate's replication offset at the time of the request.
        offset: u64,
    },
    /// A primary granted its vote to a candidate.
    VoteGranted {
        from: NodeId,
        candidate: NodeId,
        epoch: u64,
    },
}

/// The gossip engine manages cluster membership and failure detection.
pub struct GossipEngine {
    /// Our node's identity.
    local_id: NodeId,
    /// Our advertised address.
    local_addr: SocketAddr,
    /// Our incarnation number (incremented to refute suspicion).
    incarnation: u64,
    /// Protocol configuration.
    config: GossipConfig,
    /// Known cluster members.
    members: HashMap<NodeId, MemberState>,
    /// Updates to piggyback on outgoing messages, each with the number of
    /// messages it still rides on.
    pending_updates: Vec<PendingUpdate>,
    /// Sequence number for protocol messages.
    next_seq: u64,
    /// Pending probes awaiting acknowledgment.
    pending_probes: HashMap<u64, PendingProbe>,
    /// Channel the events go to. See [`GossipEngine::take_events`].
    event_tx: mpsc::Sender<GossipEvent>,
    /// Events produced since the last `take_events` call.
    pending_events: Vec<GossipEvent>,
    /// Slot ranges owned by the local node, included in Welcome replies.
    local_slots: Vec<SlotRange>,
    /// The local node's config epoch, sent with its slot claims.
    local_config_epoch: u64,
    /// The primary the local node replicates, if it is a replica. Sent in
    /// Welcome replies so a joining node learns this node's role.
    local_replicates: Option<NodeId>,
    /// Active PingReq relays waiting for an Ack from the target.
    relay_pending: HashMap<u64, RelayEntry>,
}

struct PendingUpdate {
    update: NodeUpdate,
    sends_left: u32,
}

/// What an update is about. A newer update replaces a queued one with the
/// same subject: a node's liveness, its slots, or its role.
#[derive(PartialEq, Eq)]
enum Subject {
    Membership(NodeId),
    Slots(NodeId),
    Role(NodeId),
    Other,
}

fn subject(update: &NodeUpdate) -> Subject {
    match update {
        NodeUpdate::Alive { node, .. }
        | NodeUpdate::Suspect { node, .. }
        | NodeUpdate::Dead { node, .. }
        | NodeUpdate::Left { node } => Subject::Membership(*node),
        NodeUpdate::SlotsChanged { node, .. } => Subject::Slots(*node),
        NodeUpdate::RoleChanged { node, .. } => Subject::Role(*node),
        NodeUpdate::VoteRequest { .. } | NodeUpdate::VoteGranted { .. } => Subject::Other,
    }
}

struct PendingProbe {
    target: NodeId,
    sent_at: Instant,
    indirect: bool,
}

/// Tracks a PingReq relay in progress.
///
/// When we forward a Ping on behalf of another node (via PingReq), we
/// store this entry so we can relay the Ack back to the original requester.
struct RelayEntry {
    requester: SocketAddr,
    original_seq: u64,
    sent_at: Instant,
}

impl GossipEngine {
    /// Creates a new gossip engine.
    pub fn new(
        local_id: NodeId,
        local_addr: SocketAddr,
        config: GossipConfig,
        event_tx: mpsc::Sender<GossipEvent>,
    ) -> Self {
        Self {
            local_id,
            local_addr,
            incarnation: 1,
            config,
            members: HashMap::new(),
            pending_updates: Vec::new(),
            next_seq: 1,
            pending_probes: HashMap::new(),
            event_tx,
            pending_events: Vec::new(),
            local_slots: Vec::new(),
            local_config_epoch: 0,
            local_replicates: None,
            relay_pending: HashMap::new(),
        }
    }

    /// Returns the local node ID.
    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    /// How long a node stays unanswered before it is declared dead.
    pub fn failure_timeout(&self) -> Duration {
        self.config.protocol_period * self.config.suspicion_mult
    }

    /// Returns the local node's incarnation number.
    pub fn local_incarnation(&self) -> u64 {
        self.incarnation
    }

    /// Increments the local incarnation and returns the new value.
    ///
    /// Peers apply an update about this node only when its incarnation is
    /// newer than the last one they saw, so a change to this node's own role
    /// or slots must bump it first.
    pub fn bump_incarnation(&mut self) -> u64 {
        self.incarnation += 1;
        self.incarnation
    }

    /// Restores the incarnation number from a previous session.
    ///
    /// Used when loading persisted config so the node doesn't regress
    /// to a lower incarnation, which would make it lose suspicion refutations.
    pub fn set_incarnation(&mut self, n: u64) {
        self.incarnation = n;
    }

    /// Sets the local node's role for Welcome replies: the primary it
    /// replicates, or `None` for a primary.
    pub fn set_local_role(&mut self, replicates: Option<NodeId>) {
        self.local_replicates = replicates;
    }

    /// Returns all known members.
    pub fn members(&self) -> impl Iterator<Item = &MemberState> {
        self.members.values()
    }

    /// Returns gossip addresses for all currently alive members.
    pub fn alive_member_addrs(&self) -> Vec<std::net::SocketAddr> {
        self.members
            .values()
            .filter(|m| m.state == MemberStatus::Alive || m.state == MemberStatus::Suspect)
            .map(|m| m.addr)
            .collect()
    }

    /// Returns the number of alive members (excluding self).
    pub fn alive_count(&self) -> usize {
        self.members
            .values()
            .filter(|m| m.state == MemberStatus::Alive)
            .count()
    }

    /// Updates the local node's slot ownership.
    ///
    /// Called after ADDSLOTS/DELSLOTS/SETSLOT NODE to keep the gossip
    /// engine's view in sync. The updated slots are included in Welcome
    /// replies so joining nodes learn the full slot map.
    pub fn set_local_slots(&mut self, slots: Vec<SlotRange>, config_epoch: u64) {
        self.local_slots = slots;
        self.local_config_epoch = config_epoch;
    }

    /// Queues a slot ownership update for gossip propagation.
    ///
    /// The update will be piggybacked on the next outgoing Ping or Ack
    /// message, spreading to the cluster via epidemic dissemination.
    pub fn queue_slots_update(
        &mut self,
        node: NodeId,
        incarnation: u64,
        config_epoch: u64,
        slots: Vec<SlotRange>,
    ) {
        self.queue_update(NodeUpdate::SlotsChanged {
            node,
            incarnation,
            config_epoch,
            slots,
        });
    }

    /// Queues a role change for gossip propagation.
    ///
    /// Called after this node changes from primary to replica (or vice versa).
    /// The update will be piggybacked on the next outgoing Ping or Ack.
    pub fn queue_role_update(
        &mut self,
        node: NodeId,
        incarnation: u64,
        is_primary: bool,
        replicates: Option<NodeId>,
    ) {
        if node == self.local_id {
            self.local_replicates = replicates;
        }
        self.queue_update(NodeUpdate::RoleChanged {
            node,
            incarnation,
            is_primary,
            replicates,
        });
    }

    /// Builds the message asking `voter` for its vote. Election messages go
    /// straight to the node they are for, never piggybacked, so the
    /// receiver can check the sender. `None` if `voter` is unknown.
    pub fn vote_request(
        &self,
        voter: NodeId,
        failed_primary: NodeId,
        epoch: u64,
        offset: u64,
    ) -> Option<(SocketAddr, GossipMessage)> {
        self.election_message(
            voter,
            NodeUpdate::VoteRequest {
                candidate: self.local_id,
                failed_primary,
                epoch,
                offset,
            },
        )
    }

    /// Builds the message granting `candidate` this node's vote for `epoch`.
    pub fn vote_granted(
        &self,
        candidate: NodeId,
        epoch: u64,
    ) -> Option<(SocketAddr, GossipMessage)> {
        self.election_message(
            candidate,
            NodeUpdate::VoteGranted {
                from: self.local_id,
                candidate,
                epoch,
            },
        )
    }

    fn election_message(
        &self,
        to: NodeId,
        update: NodeUpdate,
    ) -> Option<(SocketAddr, GossipMessage)> {
        let addr = self.members.get(&to)?.addr;
        let msg = GossipMessage::Election {
            sender: self.local_id,
            update,
        };
        Some((addr, msg))
    }

    /// Adds a seed node to bootstrap cluster discovery.
    pub fn add_seed(&mut self, id: NodeId, addr: SocketAddr) {
        if id == self.local_id {
            return;
        }
        self.members.entry(id).or_insert_with(|| MemberState {
            id,
            addr,
            incarnation: 0,
            state: MemberStatus::Alive,
            state_change: Instant::now(),
            is_primary: false,
            replicates: None,
            slots: Vec::new(),
            config_epoch: 0,
        });
    }

    /// Handles an incoming gossip message.
    ///
    /// Returns a list of `(address, message)` pairs to send. Most messages
    /// produce a single reply back to `from`, but PingReq forwards a Ping
    /// to a different host, and relayed Acks route back to the original
    /// requester.
    pub fn handle_message(
        &mut self,
        msg: GossipMessage,
        from: SocketAddr,
    ) -> Vec<(SocketAddr, GossipMessage)> {
        match msg {
            GossipMessage::Ping {
                seq,
                sender,
                updates,
            } => {
                trace!("received ping seq={} from {}", seq, sender);
                self.apply_updates(&updates);
                self.ensure_member(sender, from);

                // Reply with ACK
                let response_updates = self.updates_for(sender);
                vec![(
                    from,
                    GossipMessage::Ack {
                        seq,
                        sender: self.local_id,
                        updates: response_updates,
                    },
                )]
            }

            GossipMessage::PingReq {
                seq,
                sender,
                target,
                target_addr,
            } => {
                trace!(
                    "received ping-req seq={} from {} for {}",
                    seq,
                    sender,
                    target
                );
                self.ensure_member(sender, from);

                // forward a fresh Ping to the target on behalf of the requester
                let relay_seq = self.next_seq;
                self.next_seq += 1;

                self.relay_pending.insert(
                    relay_seq,
                    RelayEntry {
                        requester: from,
                        original_seq: seq,
                        sent_at: Instant::now(),
                    },
                );

                vec![(
                    target_addr,
                    GossipMessage::Ping {
                        seq: relay_seq,
                        sender: self.local_id,
                        updates: vec![],
                    },
                )]
            }

            GossipMessage::Ack {
                seq,
                sender,
                updates,
            } => {
                trace!("received ack seq={} from {}", seq, sender);
                self.apply_updates(&updates);
                self.ensure_member(sender, from);

                let mut outgoing = Vec::new();

                // Clear pending probe
                if let Some(probe) = self.pending_probes.remove(&seq) {
                    if self.members.get(&probe.target).map(|m| m.state)
                        == Some(MemberStatus::Suspect)
                    {
                        // Node recovered from suspicion
                        self.mark_alive(probe.target);
                    }
                }

                // Check if this is a relayed Ack — forward it back to the requester
                if let Some(relay) = self.relay_pending.remove(&seq) {
                    outgoing.push((
                        relay.requester,
                        GossipMessage::Ack {
                            seq: relay.original_seq,
                            sender: self.local_id,
                            updates: vec![],
                        },
                    ));
                }

                outgoing
            }

            GossipMessage::Join {
                sender,
                sender_addr,
            } => {
                info!("node {} joining from {}", sender, sender_addr);
                let sender_is_new = !self.members.contains_key(&sender);
                self.ensure_member(sender, sender_addr);
                if sender_is_new {
                    self.emit(GossipEvent::MemberJoined(
                        sender,
                        sender_addr,
                        Vec::new(),
                        0,
                    ));
                }

                // Broadcast alive update
                self.queue_update(NodeUpdate::Alive {
                    node: sender,
                    addr: sender_addr,
                    incarnation: 1,
                });

                // send welcome with current members, including ourselves
                let mut members: Vec<MemberInfo> = self
                    .members
                    .values()
                    .filter(|m| m.state == MemberStatus::Alive)
                    .map(|m| MemberInfo {
                        id: m.id,
                        addr: m.addr,
                        incarnation: m.incarnation,
                        is_primary: m.is_primary,
                        replicates: m.replicates,
                        config_epoch: m.config_epoch,
                        slots: m.slots.clone(),
                    })
                    .collect();

                // include ourselves so the joiner learns the full map and
                // our role
                members.push(MemberInfo {
                    id: self.local_id,
                    addr: self.local_addr,
                    incarnation: self.incarnation,
                    is_primary: self.local_replicates.is_none(),
                    replicates: self.local_replicates,
                    config_epoch: self.local_config_epoch,
                    slots: self.local_slots.clone(),
                });

                vec![(
                    from,
                    GossipMessage::Welcome {
                        sender: self.local_id,
                        members,
                    },
                )]
            }

            GossipMessage::Welcome { sender, members } => {
                info!(
                    "received welcome from {} with {} members",
                    sender,
                    members.len()
                );
                // the sender lists itself among the members, with its role
                // and slots
                for member in members {
                    if member.id == self.local_id {
                        continue;
                    }
                    if let std::collections::hash_map::Entry::Vacant(e) =
                        self.members.entry(member.id)
                    {
                        let slots = member.slots.clone();
                        e.insert(MemberState {
                            id: member.id,
                            addr: member.addr,
                            incarnation: member.incarnation,
                            state: MemberStatus::Alive,
                            state_change: Instant::now(),
                            is_primary: member.is_primary,
                            replicates: member.replicates,
                            slots: slots.clone(),
                            config_epoch: member.config_epoch,
                        });
                        self.emit(GossipEvent::MemberJoined(
                            member.id,
                            member.addr,
                            slots,
                            member.config_epoch,
                        ));
                        // a joining node starts out as a primary; tell the
                        // cluster state about a replica, or it counts it
                        // among the primaries that vote in elections
                        if !member.is_primary {
                            self.emit(GossipEvent::RoleChanged(
                                member.id,
                                false,
                                member.replicates,
                            ));
                        }
                    }
                }
                if !self.members.contains_key(&sender) {
                    self.ensure_member(sender, from);
                    self.emit(GossipEvent::MemberJoined(sender, from, Vec::new(), 0));
                }
                vec![]
            }

            GossipMessage::Election { sender, update } => {
                // the node the vote is from must have sent it, from the
                // address this node knows for it. every node shares the
                // cluster secret, so the message alone doesn't prove that.
                let from_sender = self.members.get(&sender).is_some_and(|m| m.addr == from);
                let names_sender = match update {
                    NodeUpdate::VoteRequest { candidate, .. } => candidate == sender,
                    NodeUpdate::VoteGranted { from: voter, .. } => voter == sender,
                    _ => false,
                };
                if from_sender && names_sender {
                    self.apply_vote(update);
                } else {
                    debug!(
                        "ignoring election message claiming to be from {sender} sent from {from}"
                    );
                }
                vec![]
            }

            GossipMessage::SlotsAnnounce {
                sender,
                incarnation,
                config_epoch,
                slots,
            } => {
                // Treat as a piggybacked SlotsChanged update from the sender.
                self.ensure_member(sender, from);
                self.apply_updates(&[NodeUpdate::SlotsChanged {
                    node: sender,
                    incarnation,
                    config_epoch,
                    slots,
                }]);
                vec![]
            }
        }
    }

    /// Runs one protocol period: probe a random node.
    ///
    /// Returns all messages to send this tick: the direct probe plus any
    /// PingReq messages generated by timed-out direct probes.
    pub fn tick(&mut self) -> Vec<(SocketAddr, GossipMessage)> {
        let mut outgoing = Vec::new();

        // Check for timed-out probes (may generate PingReq messages)
        outgoing.extend(self.check_probe_timeouts());

        // Check for expired suspicions
        self.check_suspicion_timeouts();

        // Clean up stale relay entries
        self.cleanup_stale_relays();

        // Select a random member to probe. Dead members stay in the
        // rotation: after a partition heals, a probe is how a node learns
        // it was declared dead and refutes it.
        let target_info = {
            let alive_members: Vec<_> = self
                .members
                .values()
                .filter(|m| m.state != MemberStatus::Left)
                .map(|m| (m.id, m.addr))
                .collect();

            if alive_members.is_empty() {
                return outgoing;
            }

            match alive_members.choose(&mut rand::rng()) {
                Some(info) => *info,
                None => return outgoing,
            }
        };

        let (target_id, target_addr) = target_info;
        let seq = self.next_seq;
        self.next_seq += 1;

        let updates = self.updates_for(target_id);

        let msg = GossipMessage::Ping {
            seq,
            sender: self.local_id,
            updates,
        };

        self.pending_probes.insert(
            seq,
            PendingProbe {
                target: target_id,
                sent_at: Instant::now(),
                indirect: false,
            },
        );

        outgoing.push((target_addr, msg));
        outgoing
    }

    /// Creates a join message to send to a seed node.
    pub fn create_join_message(&self) -> GossipMessage {
        GossipMessage::Join {
            sender: self.local_id,
            sender_addr: self.local_addr,
        }
    }

    fn ensure_member(&mut self, id: NodeId, addr: SocketAddr) {
        if id == self.local_id {
            return;
        }
        self.members.entry(id).or_insert_with(|| MemberState {
            id,
            addr,
            incarnation: 0,
            state: MemberStatus::Alive,
            state_change: Instant::now(),
            is_primary: false,
            replicates: None,
            slots: Vec::new(),
            config_epoch: 0,
        });
    }

    /// Records an event for the next [`take_events`](Self::take_events).
    fn emit(&mut self, event: GossipEvent) {
        self.pending_events.push(event);
    }

    /// Takes the events produced since the last call, with the channel they
    /// go to.
    ///
    /// The engine never sends events itself. Callers hold its lock while
    /// they call `tick` or `handle_message`, and the event consumer takes
    /// the same lock, so sending while locked could deadlock once the
    /// channel fills. Callers release the lock, then call
    /// [`PendingEvents::send`].
    pub fn take_events(&mut self) -> PendingEvents {
        PendingEvents {
            tx: self.event_tx.clone(),
            events: std::mem::take(&mut self.pending_events),
        }
    }

    /// Whether an update about `node` at `incarnation` is believable. Nodes
    /// raise their own incarnation one step at a time, so a claim far ahead
    /// of what we know is a bug or an attempt to use up the incarnation
    /// space. That includes claims about the local node: refuting one would
    /// copy its incarnation. A node we haven't heard of yet has no baseline.
    fn plausible_incarnation(&self, node: NodeId, incarnation: u64) -> bool {
        let known = if node == self.local_id {
            Some(self.incarnation)
        } else {
            self.members.get(&node).map(|member| member.incarnation)
        };
        incarnation <= MAX_INCARNATION
            && known.is_none_or(|known| incarnation <= known.saturating_add(MAX_INCARNATION_JUMP))
    }

    fn apply_updates(&mut self, updates: &[NodeUpdate]) {
        for update in updates {
            let subject = match update {
                NodeUpdate::Alive {
                    node, incarnation, ..
                }
                | NodeUpdate::Suspect { node, incarnation }
                | NodeUpdate::Dead { node, incarnation }
                | NodeUpdate::SlotsChanged {
                    node, incarnation, ..
                }
                | NodeUpdate::RoleChanged {
                    node, incarnation, ..
                } => Some((*node, *incarnation)),
                _ => None,
            };
            if let Some((node, incarnation)) = subject {
                if !self.plausible_incarnation(node, incarnation) {
                    warn!("rejecting gossip update for {node} with incarnation {incarnation}");
                    continue;
                }
            }

            match update {
                NodeUpdate::Alive {
                    node,
                    addr,
                    incarnation,
                } => {
                    if *node == self.local_id {
                        // Someone thinks we're alive, good
                        continue;
                    }
                    if let Some(member) = self.members.get_mut(node) {
                        if *incarnation > member.incarnation {
                            member.incarnation = *incarnation;
                            member.addr = *addr;
                            if member.state != MemberStatus::Alive {
                                member.state = MemberStatus::Alive;
                                member.state_change = Instant::now();
                                self.emit(GossipEvent::MemberAlive(*node));
                            }
                        }
                    } else {
                        self.members.insert(
                            *node,
                            MemberState {
                                id: *node,
                                addr: *addr,
                                incarnation: *incarnation,
                                state: MemberStatus::Alive,
                                state_change: Instant::now(),
                                is_primary: false,
                                replicates: None,
                                slots: Vec::new(),
                                config_epoch: 0,
                            },
                        );
                        self.emit(GossipEvent::MemberJoined(*node, *addr, Vec::new(), 0));
                    }
                }

                NodeUpdate::Suspect { node, incarnation } => {
                    if *node == self.local_id {
                        // Refute suspicion by incrementing our incarnation
                        if *incarnation >= self.incarnation {
                            self.incarnation = incarnation.saturating_add(1);
                            self.queue_update(NodeUpdate::Alive {
                                node: self.local_id,
                                addr: self.local_addr,
                                incarnation: self.incarnation,
                            });
                        }
                        continue;
                    }
                    if let Some(member) = self.members.get_mut(node) {
                        if *incarnation >= member.incarnation && member.state == MemberStatus::Alive
                        {
                            member.state = MemberStatus::Suspect;
                            member.state_change = Instant::now();
                            self.emit(GossipEvent::MemberSuspected(*node));
                        }
                    }
                }

                NodeUpdate::Dead { node, incarnation } => {
                    if *node == self.local_id {
                        // refute the death claim with an incarnation newer
                        // than both the claim's and our own, never older
                        self.incarnation = self.incarnation.max(incarnation.saturating_add(1));
                        self.queue_update(NodeUpdate::Alive {
                            node: self.local_id,
                            addr: self.local_addr,
                            incarnation: self.incarnation,
                        });
                        continue;
                    }
                    if let Some(member) = self.members.get_mut(node) {
                        if *incarnation >= member.incarnation && member.state != MemberStatus::Dead
                        {
                            member.state = MemberStatus::Dead;
                            member.state_change = Instant::now();
                            self.emit(GossipEvent::MemberFailed(*node));
                        }
                    }
                }

                NodeUpdate::Left { node } => {
                    if *node == self.local_id {
                        continue;
                    }
                    if let Some(member) = self.members.get_mut(node) {
                        if member.state != MemberStatus::Left {
                            member.state = MemberStatus::Left;
                            member.state_change = Instant::now();
                            self.emit(GossipEvent::MemberLeft(*node));
                        }
                    }
                }

                NodeUpdate::SlotsChanged {
                    node,
                    incarnation,
                    config_epoch,
                    slots,
                } => {
                    if *node == self.local_id {
                        continue;
                    }
                    // clone the slot list once before the mutable borrow so we
                    // can move it into the event after the borrow is released
                    let owned = slots.clone();
                    let should_emit = match self.members.get_mut(node) {
                        Some(member) if *incarnation >= member.incarnation => {
                            member.slots = owned.clone();
                            member.config_epoch = *config_epoch;
                            true
                        }
                        _ => false,
                    };
                    if should_emit {
                        self.emit(GossipEvent::SlotsChanged(*node, owned, *config_epoch));
                    }
                }

                NodeUpdate::RoleChanged {
                    node,
                    incarnation,
                    is_primary,
                    replicates,
                } => {
                    if *node == self.local_id {
                        // we know our own role
                        continue;
                    }
                    if let Some(member) = self.members.get_mut(node) {
                        if *incarnation > member.incarnation {
                            member.incarnation = *incarnation;
                            member.is_primary = *is_primary;
                            member.replicates = *replicates;
                            self.emit(GossipEvent::RoleChanged(*node, *is_primary, *replicates));
                        }
                    }
                }

                // votes only count when sent directly, see GossipMessage::Election
                NodeUpdate::VoteRequest { .. } | NodeUpdate::VoteGranted { .. } => {}
            }
        }
    }

    /// Reports a vote request or grant that arrived in an election message.
    fn apply_vote(&mut self, update: NodeUpdate) {
        match update {
            NodeUpdate::VoteRequest {
                candidate,
                failed_primary,
                epoch,
                offset,
            } => self.emit(GossipEvent::VoteRequested {
                candidate,
                failed_primary,
                epoch,
                offset,
            }),
            NodeUpdate::VoteGranted {
                from,
                candidate,
                epoch,
            } => self.emit(GossipEvent::VoteGranted {
                from,
                candidate,
                epoch,
            }),
            _ => {}
        }
    }

    fn mark_alive(&mut self, node: NodeId) {
        if let Some(member) = self.members.get_mut(&node) {
            if member.state == MemberStatus::Suspect {
                member.state = MemberStatus::Alive;
                member.state_change = Instant::now();
                self.emit(GossipEvent::MemberAlive(node));
            }
        }
    }

    /// Checks for timed-out probes and implements two-phase failure detection.
    ///
    /// Phase 1: direct ping timeout → send PingReq to `indirect_probes` random
    ///          alive members, and register an indirect probe for the target.
    /// Phase 2: indirect probe timeout → mark the target Suspect.
    ///
    /// Returns PingReq messages to send.
    fn check_probe_timeouts(&mut self) -> Vec<(SocketAddr, GossipMessage)> {
        let timeout = self.config.probe_timeout;
        let now = Instant::now();
        let mut outgoing = Vec::new();

        // Single pass: collect timed-out entries by kind and remove them.
        let mut timed_out_indirect: Vec<(u64, NodeId)> = Vec::new();
        let mut timed_out_direct: Vec<(u64, NodeId)> = Vec::new();
        self.pending_probes.retain(|seq, probe| {
            if now.duration_since(probe.sent_at) <= timeout {
                return true;
            }
            if probe.indirect {
                timed_out_indirect.push((*seq, probe.target));
            } else {
                timed_out_direct.push((*seq, probe.target));
            }
            false
        });

        // Phase 2: indirect probe timeouts → mark Suspect
        for (_seq, target) in timed_out_indirect {
            let incarnation = self
                .members
                .get(&target)
                .filter(|m| m.state == MemberStatus::Alive)
                .map(|m| m.incarnation);

            if let Some(inc) = incarnation {
                if let Some(member) = self.members.get_mut(&target) {
                    debug!("node {} failed indirect probe, marking suspect", target);
                    member.state = MemberStatus::Suspect;
                    member.state_change = Instant::now();
                }
                self.queue_update(NodeUpdate::Suspect {
                    node: target,
                    incarnation: inc,
                });
                self.emit(GossipEvent::MemberSuspected(target));
            }
        }

        // Phase 1: direct ping timeouts → send PingReq
        for (_seq, target) in timed_out_direct {
            let target_addr = match self.members.get(&target) {
                Some(m) if m.state == MemberStatus::Alive => m.addr,
                _ => continue,
            };

            // pick random alive members (excluding target) to relay through
            let relay_nodes: Vec<(NodeId, SocketAddr)> = self
                .members
                .values()
                .filter(|m| m.state == MemberStatus::Alive && m.id != target)
                .map(|m| (m.id, m.addr))
                .collect();

            if relay_nodes.is_empty() {
                // no relays available — fall back to immediate suspect
                let incarnation = self
                    .members
                    .get(&target)
                    .map(|m| m.incarnation)
                    .unwrap_or(0);

                if let Some(member) = self.members.get_mut(&target) {
                    debug!("node {} timed out with no relays, marking suspect", target);
                    member.state = MemberStatus::Suspect;
                    member.state_change = Instant::now();
                }
                self.queue_update(NodeUpdate::Suspect {
                    node: target,
                    incarnation,
                });
                self.emit(GossipEvent::MemberSuspected(target));
                continue;
            }

            let k = self.config.indirect_probes.min(relay_nodes.len());
            let chosen: Vec<_> = relay_nodes
                .choose_multiple(&mut rand::rng(), k)
                .copied()
                .collect();

            debug!(
                "node {} direct ping timed out, sending PingReq to {} relays",
                target,
                chosen.len()
            );

            // register an indirect probe — if this times out, we mark Suspect
            let indirect_seq = self.next_seq;
            self.next_seq += 1;
            self.pending_probes.insert(
                indirect_seq,
                PendingProbe {
                    target,
                    sent_at: Instant::now(),
                    indirect: true,
                },
            );

            for (_, relay_addr) in chosen {
                outgoing.push((
                    relay_addr,
                    GossipMessage::PingReq {
                        seq: indirect_seq,
                        sender: self.local_id,
                        target,
                        target_addr,
                    },
                ));
            }
        }

        outgoing
    }

    /// Removes stale relay entries that have timed out.
    ///
    /// If the target never responds, the relay entry just sits there.
    /// The original prober handles its own timeout via the indirect probe.
    fn cleanup_stale_relays(&mut self) {
        let timeout = self.config.probe_timeout;
        let now = Instant::now();
        self.relay_pending
            .retain(|_, entry| now.duration_since(entry.sent_at) <= timeout);
    }

    fn check_suspicion_timeouts(&mut self) {
        let suspicion_timeout = self.config.protocol_period * self.config.suspicion_mult;
        let now = Instant::now();
        let mut to_mark_dead = Vec::new();

        for member in self.members.values() {
            if member.state == MemberStatus::Suspect
                && now.duration_since(member.state_change) > suspicion_timeout
            {
                to_mark_dead.push((member.id, member.incarnation));
            }
        }

        for (id, incarnation) in to_mark_dead {
            if let Some(member) = self.members.get_mut(&id) {
                warn!("node {} confirmed dead after suspicion timeout", id);
                member.state = MemberStatus::Dead;
                member.state_change = Instant::now();
                self.queue_update(NodeUpdate::Dead {
                    node: id,
                    incarnation,
                });
                // this node reached the verdict itself, so act on it here
                // rather than wait for a peer's Dead update to come back
                self.emit(GossipEvent::MemberFailed(id));
            }
        }
    }

    fn queue_update(&mut self, update: NodeUpdate) {
        let about = subject(&update);
        if about != Subject::Other {
            self.pending_updates.retain(|p| subject(&p.update) != about);
        }
        let sends_left = self.retransmit_limit();
        self.pending_updates
            .push(PendingUpdate { update, sends_left });
        if self.pending_updates.len() > MAX_PENDING_UPDATES {
            // drop the update that has already gone out the most times
            if let Some(i) =
                (0..self.pending_updates.len()).min_by_key(|&i| self.pending_updates[i].sends_left)
            {
                self.pending_updates.swap_remove(i);
            }
        }
    }

    /// How many messages a new update rides on: `RETRANSMIT_MULT` times
    /// the number of bits in the cluster size.
    fn retransmit_limit(&self) -> u32 {
        let size = self.members.len() as u32 + 1;
        RETRANSMIT_MULT * (u32::BITS - size.leading_zeros())
    }

    /// Picks the updates for one outgoing message, those sent least so far
    /// first, and counts the send against each.
    fn collect_updates(&mut self) -> Vec<NodeUpdate> {
        self.pending_updates
            .sort_by_key(|p| std::cmp::Reverse(p.sends_left));
        let count = self.pending_updates.len().min(self.config.max_piggyback);
        let picked: Vec<NodeUpdate> = self.pending_updates[..count]
            .iter_mut()
            .map(|p| {
                p.sends_left -= 1;
                p.update.clone()
            })
            .collect();
        self.pending_updates.retain(|p| p.sends_left > 0);
        picked
    }

    /// The local node's slot claim, sent on every Ping and Ack so peers
    /// converge even when they missed the update queued at the change.
    fn local_slots_update(&self) -> Option<NodeUpdate> {
        (!self.local_slots.is_empty()).then(|| NodeUpdate::SlotsChanged {
            node: self.local_id,
            incarnation: self.incarnation,
            config_epoch: self.local_config_epoch,
            slots: self.local_slots.clone(),
        })
    }

    /// Updates for a message to `node`: the queued ones, the local slot
    /// claim, and, when we believe `node` is dead, that verdict, so it can
    /// refute it with a higher incarnation once it is reachable again.
    fn updates_for(&mut self, node: NodeId) -> Vec<NodeUpdate> {
        let mut updates = self.collect_updates();
        updates.extend(self.local_slots_update());
        if let Some(member) = self.members.get(&node) {
            if member.state == MemberStatus::Dead {
                updates.push(NodeUpdate::Dead {
                    node,
                    incarnation: member.incarnation,
                });
            }
        }
        updates
    }
}

/// Events taken from a [`GossipEngine`], ready to send once its lock is
/// released.
pub struct PendingEvents {
    tx: mpsc::Sender<GossipEvent>,
    events: Vec<GossipEvent>,
}

impl PendingEvents {
    /// Sends the events in order. A closed channel normally means shutdown;
    /// in steady state it points to a bug in the event consumer.
    pub async fn send(self) {
        for event in self.events {
            if self.tx.send(event).await.is_err() {
                warn!("gossip event channel closed, dropping event");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn test_addr(port: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::new(127, 0, 0, 1), port))
    }

    #[tokio::test]
    async fn engine_creation() {
        let (tx, _rx) = mpsc::channel(16);
        let engine = GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);
        assert_eq!(engine.alive_count(), 0);
    }

    #[tokio::test]
    async fn add_seed() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let seed_id = NodeId::new();
        engine.add_seed(seed_id, test_addr(6380));
        assert_eq!(engine.alive_count(), 1);
    }

    #[tokio::test]
    async fn handle_ping() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let sender = NodeId::new();
        let msg = GossipMessage::Ping {
            seq: 1,
            sender,
            updates: vec![],
        };

        let responses = engine.handle_message(msg, test_addr(6380));
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0].1, GossipMessage::Ack { .. }));
        assert_eq!(responses[0].0.port(), 6380);
        assert_eq!(engine.alive_count(), 1);
    }

    fn engine(port: u16) -> GossipEngine {
        let (tx, _rx) = mpsc::channel(64);
        GossipEngine::new(NodeId::new(), test_addr(port), GossipConfig::default(), tx)
    }

    #[tokio::test]
    async fn queued_update_rides_on_several_messages() {
        let mut a = engine(7000);
        let (b, c) = (NodeId::new(), NodeId::new());
        a.add_seed(b, test_addr(7001));
        a.add_seed(c, test_addr(7002));
        a.queue_update(NodeUpdate::Suspect {
            node: c,
            incarnation: 0,
        });

        // three members: 2 bits of cluster size, so 6 messages carry it
        let carried = (0..10)
            .filter(|_| {
                a.collect_updates()
                    .iter()
                    .any(|u| matches!(u, NodeUpdate::Suspect { node, .. } if *node == c))
            })
            .count();
        assert_eq!(carried, 6);
    }

    #[tokio::test]
    async fn newer_update_replaces_a_queued_one_about_the_same_node() {
        let mut a = engine(7000);
        let b = NodeId::new();
        a.add_seed(b, test_addr(7001));
        a.queue_update(NodeUpdate::Suspect {
            node: b,
            incarnation: 0,
        });
        a.queue_update(NodeUpdate::Dead {
            node: b,
            incarnation: 0,
        });
        let updates = a.collect_updates();
        assert_eq!(updates.len(), 1);
        assert!(matches!(updates[0], NodeUpdate::Dead { .. }));
    }

    #[tokio::test]
    async fn ack_carries_the_responders_slot_claim() {
        let mut a = engine(7000);
        a.set_local_slots(vec![SlotRange::new(0, 99)], 3);
        let ping = GossipMessage::Ping {
            seq: 1,
            sender: NodeId::new(),
            updates: vec![],
        };
        let replies = a.handle_message(ping, test_addr(7001));
        let GossipMessage::Ack { updates, .. } = &replies[0].1 else {
            panic!("expected an ack");
        };
        assert!(updates.iter().any(|u| matches!(
            u,
            NodeUpdate::SlotsChanged {
                config_epoch: 3,
                ..
            }
        )));
    }

    /// After a partition heals, a node the others declared dead must learn
    /// of it and come back alive, so slot claims flow to it again.
    #[tokio::test]
    async fn a_node_declared_dead_refutes_it_once_reachable() {
        let mut a = engine(7000);
        let mut b = engine(7001);
        let (a_id, b_id) = (a.local_id(), b.local_id());
        a.add_seed(b_id, test_addr(7001));
        b.add_seed(a_id, test_addr(7000));
        a.members.get_mut(&b_id).unwrap().state = MemberStatus::Dead;

        // a still probes b, and tells it that it is dead
        let probes = a.tick();
        let (to, ping) = probes
            .into_iter()
            .find(|(to, _)| *to == test_addr(7001))
            .expect("dead members are still probed");
        assert_eq!(to, test_addr(7001));
        let GossipMessage::Ping { updates, .. } = &ping else {
            panic!("expected a ping");
        };
        assert!(updates
            .iter()
            .any(|u| matches!(u, NodeUpdate::Dead { node, .. } if *node == b_id)));

        // b refutes with a higher incarnation in its ack
        let replies = b.handle_message(ping, test_addr(7000));
        assert_eq!(b.local_incarnation(), 1);
        for (_, reply) in replies {
            a.handle_message(reply, test_addr(7001));
        }
        assert_eq!(a.members[&b_id].state, MemberStatus::Alive);
        assert!(a
            .take_events()
            .events
            .iter()
            .any(|e| matches!(e, GossipEvent::MemberAlive(id) if *id == b_id)));
    }

    #[tokio::test]
    async fn handle_join() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let joiner = NodeId::new();
        let msg = GossipMessage::Join {
            sender: joiner,
            sender_addr: test_addr(6380),
        };

        let responses = engine.handle_message(msg, test_addr(6380));
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0].1, GossipMessage::Welcome { .. }));
        assert_eq!(engine.alive_count(), 1);
    }

    #[tokio::test]
    async fn tick_with_no_members() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let messages = engine.tick();
        assert!(messages.is_empty());
    }

    #[tokio::test]
    async fn tick_with_members() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        engine.add_seed(NodeId::new(), test_addr(6380));
        let messages = engine.tick();
        assert_eq!(messages.len(), 1);

        let (addr, msg) = &messages[0];
        assert_eq!(addr.port(), 6380);
        assert!(matches!(msg, GossipMessage::Ping { .. }));
    }

    #[tokio::test]
    async fn create_join_message() {
        let (tx, _rx) = mpsc::channel(16);
        let id = NodeId::new();
        let addr = test_addr(6379);
        let engine = GossipEngine::new(id, addr, GossipConfig::default(), tx);

        let msg = engine.create_join_message();
        match msg {
            GossipMessage::Join {
                sender,
                sender_addr,
            } => {
                assert_eq!(sender, id);
                assert_eq!(sender_addr, addr);
            }
            _ => panic!("expected Join message"),
        }
    }

    #[tokio::test]
    async fn apply_slots_changed_updates_member() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let remote = NodeId::new();
        engine.add_seed(remote, test_addr(6380));

        let slots = vec![SlotRange::new(0, 5460)];
        let updates = vec![NodeUpdate::SlotsChanged {
            node: remote,
            incarnation: 1,
            config_epoch: 1,
            slots: slots.clone(),
        }];

        let msg = GossipMessage::Ping {
            seq: 1,
            sender: remote,
            updates,
        };
        engine.handle_message(msg, test_addr(6380));

        // member should have updated slots
        let member = engine.members.get(&remote).unwrap();
        assert_eq!(member.slots, slots);

        // should have emitted a SlotsChanged event
        engine.take_events().send().await;
        let event = rx.try_recv().unwrap();
        assert!(matches!(event, GossipEvent::SlotsChanged(id, _, _) if id == remote));
    }

    #[tokio::test]
    async fn stale_slots_changed_ignored() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let remote = NodeId::new();
        // add member with incarnation 5
        engine.members.insert(
            remote,
            MemberState {
                id: remote,
                addr: test_addr(6380),
                incarnation: 5,
                state: MemberStatus::Alive,
                state_change: Instant::now(),
                is_primary: true,
                replicates: None,
                slots: vec![SlotRange::new(0, 5460)],
                config_epoch: 0,
            },
        );

        // send slots update with stale incarnation (lower)
        let msg = GossipMessage::Ping {
            seq: 1,
            sender: remote,
            updates: vec![NodeUpdate::SlotsChanged {
                node: remote,
                incarnation: 3, // stale
                config_epoch: 1,
                slots: vec![],
            }],
        };
        engine.handle_message(msg, test_addr(6380));

        // slots should NOT have been cleared
        let member = engine.members.get(&remote).unwrap();
        assert_eq!(member.slots.len(), 1);

        // no SlotsChanged event should have been emitted
        engine.take_events().send().await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn welcome_includes_local_slots() {
        let (tx, _rx) = mpsc::channel(16);
        let local_id = NodeId::new();
        let mut engine = GossipEngine::new(local_id, test_addr(6379), GossipConfig::default(), tx);

        engine.set_local_slots(vec![SlotRange::new(0, 16383)], 1);

        let joiner = NodeId::new();
        let msg = GossipMessage::Join {
            sender: joiner,
            sender_addr: test_addr(6380),
        };

        let responses = engine.handle_message(msg, test_addr(6380));
        assert_eq!(responses.len(), 1);
        match &responses[0].1 {
            GossipMessage::Welcome { members, .. } => {
                let local_member = members.iter().find(|m| m.id == local_id);
                assert!(local_member.is_some(), "welcome should include local node");
                assert_eq!(local_member.unwrap().slots, vec![SlotRange::new(0, 16383)]);
            }
            other => panic!("expected Welcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn welcome_reports_the_senders_replica_role() {
        let (tx, _rx) = mpsc::channel(16);
        let local = NodeId::new();
        let mut engine = GossipEngine::new(local, test_addr(6379), GossipConfig::default(), tx);
        let primary = NodeId::new();
        engine.queue_role_update(local, 1, false, Some(primary));

        let msg = GossipMessage::Join {
            sender: NodeId::new(),
            sender_addr: test_addr(6380),
        };
        let responses = engine.handle_message(msg, test_addr(6380));
        let GossipMessage::Welcome { members, .. } = &responses[0].1 else {
            panic!("expected Welcome");
        };
        let me = members.iter().find(|m| m.id == local).unwrap();
        assert!(!me.is_primary);
        assert_eq!(me.replicates, Some(primary));
    }

    #[tokio::test]
    async fn a_joiner_learns_replica_roles_from_the_welcome() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);
        let (sender, primary) = (NodeId::new(), NodeId::new());
        let msg = GossipMessage::Welcome {
            sender,
            members: vec![MemberInfo {
                id: sender,
                addr: test_addr(6380),
                incarnation: 1,
                is_primary: false,
                replicates: Some(primary),
                config_epoch: 0,
                slots: vec![],
            }],
        };
        engine.handle_message(msg, test_addr(6380));
        assert_eq!(engine.members[&sender].replicates, Some(primary));

        engine.take_events().send().await;
        let mut role = None;
        while let Ok(event) = rx.try_recv() {
            if let GossipEvent::RoleChanged(id, is_primary, replicates) = event {
                role = Some((id, is_primary, replicates));
            }
        }
        assert_eq!(role, Some((sender, false, Some(primary))));
    }

    #[tokio::test]
    async fn refuting_a_stale_death_claim_never_lowers_the_incarnation() {
        let (tx, _rx) = mpsc::channel(16);
        let local = NodeId::new();
        let mut engine = GossipEngine::new(local, test_addr(6379), GossipConfig::default(), tx);
        engine.set_incarnation(10);
        let msg = GossipMessage::Ping {
            seq: 1,
            sender: NodeId::new(),
            updates: vec![NodeUpdate::Dead {
                node: local,
                incarnation: 3,
            }],
        };
        engine.handle_message(msg, test_addr(6380));
        assert!(engine.local_incarnation() >= 10);
    }

    #[tokio::test]
    async fn welcome_propagates_member_slots() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let sender = NodeId::new();
        let member_id = NodeId::new();
        let slots = vec![SlotRange::new(0, 5460)];

        let msg = GossipMessage::Welcome {
            sender,
            members: vec![MemberInfo {
                id: member_id,
                addr: test_addr(6381),
                incarnation: 1,
                is_primary: true,
                replicates: None,
                config_epoch: 1,
                slots: slots.clone(),
            }],
        };

        engine.handle_message(msg, test_addr(6380));

        // member should be added with slots
        let member = engine.members.get(&member_id).unwrap();
        assert_eq!(member.slots, slots);

        // Fix 1: the handler now emits MemberJoined for the sender first,
        // then for each new member in the members list.
        // Drain until we find the one for member_id.
        let mut found = false;
        engine.take_events().send().await;
        while let Ok(event) = rx.try_recv() {
            if let GossipEvent::MemberJoined(id, _, s, _) = event {
                if id == member_id {
                    assert_eq!(s, slots);
                    found = true;
                    break;
                }
            }
        }
        assert!(found, "expected MemberJoined for member_id with slots");
    }

    #[tokio::test]
    async fn direct_ping_timeout_sends_pingreq() {
        let (tx, _rx) = mpsc::channel(16);
        let config = GossipConfig {
            probe_timeout: Duration::from_millis(0), // expire immediately
            ..GossipConfig::default()
        };
        let mut engine = GossipEngine::new(NodeId::new(), test_addr(6379), config, tx);

        let target = NodeId::new();
        let relay = NodeId::new();
        engine.add_seed(target, test_addr(6380));
        engine.add_seed(relay, test_addr(6381));

        // send a direct probe to target
        let messages = engine.tick();
        // tick sends the direct ping plus checks timeouts (but probe just started)
        assert!(!messages.is_empty());

        // the direct probe is pending — now on next tick it should time out
        // and generate PingReq messages to the relay
        let messages = engine.tick();

        // should have at least one PingReq in the outgoing messages
        let pingreqs: Vec<_> = messages
            .iter()
            .filter(|(_, msg)| matches!(msg, GossipMessage::PingReq { .. }))
            .collect();

        // the first tick's probe should have timed out (0ms timeout)
        // and generated PingReq(s) to the relay node
        assert!(
            !pingreqs.is_empty(),
            "expected PingReq after direct probe timeout"
        );
    }

    #[tokio::test]
    async fn pingreq_handler_forwards_to_target() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let requester = NodeId::new();
        let target = NodeId::new();
        let target_addr = test_addr(6381);

        let msg = GossipMessage::PingReq {
            seq: 42,
            sender: requester,
            target,
            target_addr,
        };

        let responses = engine.handle_message(msg, test_addr(6380));

        // should forward a Ping to the target address
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].0, target_addr);
        assert!(matches!(responses[0].1, GossipMessage::Ping { .. }));

        // relay entry should be registered
        assert_eq!(engine.relay_pending.len(), 1);
    }

    #[tokio::test]
    async fn relayed_ack_forwarded_to_requester() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let requester = NodeId::new();
        let requester_addr = test_addr(6380);
        let target = NodeId::new();
        let target_addr = test_addr(6381);

        // step 1: receive PingReq
        let msg = GossipMessage::PingReq {
            seq: 42,
            sender: requester,
            target,
            target_addr,
        };
        let responses = engine.handle_message(msg, requester_addr);
        let relay_seq = match &responses[0].1 {
            GossipMessage::Ping { seq, .. } => *seq,
            other => panic!("expected Ping, got {other:?}"),
        };

        // step 2: target responds with Ack for the relay_seq
        let target_sender = NodeId::new();
        let ack = GossipMessage::Ack {
            seq: relay_seq,
            sender: target_sender,
            updates: vec![],
        };
        let responses = engine.handle_message(ack, target_addr);

        // should forward an Ack with the original seq back to the requester
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].0, requester_addr);
        match &responses[0].1 {
            GossipMessage::Ack { seq, .. } => assert_eq!(*seq, 42),
            other => panic!("expected Ack, got {other:?}"),
        }

        // relay entry should be cleaned up
        assert!(engine.relay_pending.is_empty());
    }

    #[tokio::test]
    async fn indirect_probe_timeout_marks_suspect() {
        let (tx, _rx) = mpsc::channel(16);
        let config = GossipConfig {
            probe_timeout: Duration::from_millis(0), // expire immediately
            indirect_probes: 1,
            ..GossipConfig::default()
        };
        let mut engine = GossipEngine::new(NodeId::new(), test_addr(6379), config, tx);

        let target = NodeId::new();
        let relay = NodeId::new();
        engine.add_seed(target, test_addr(6380));
        engine.add_seed(relay, test_addr(6381));

        // tick 1: send direct ping
        engine.tick();

        // tick 2: direct probe timed out → PingReq sent, indirect probe registered
        engine.tick();

        // tick 3: indirect probe also timed out → should mark Suspect
        engine.tick();

        // verify at least one member is now Suspect
        let suspect_count = engine
            .members
            .values()
            .filter(|m| m.state == MemberStatus::Suspect)
            .count();
        assert!(suspect_count > 0, "expected at least one Suspect member");
    }

    #[tokio::test]
    async fn stale_relay_entries_cleaned_up() {
        let (tx, _rx) = mpsc::channel(16);
        let config = GossipConfig {
            probe_timeout: Duration::from_millis(0), // expire immediately
            ..GossipConfig::default()
        };
        let mut engine = GossipEngine::new(NodeId::new(), test_addr(6379), config, tx);

        // manually insert a relay entry
        engine.relay_pending.insert(
            999,
            RelayEntry {
                requester: test_addr(6380),
                original_seq: 1,
                sent_at: Instant::now() - Duration::from_secs(10),
            },
        );

        engine.tick();

        // stale entry should be cleaned up
        assert!(engine.relay_pending.is_empty());
    }

    #[tokio::test]
    async fn apply_role_changed_updates_member() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let remote = NodeId::new();
        let primary = NodeId::new();
        engine.add_seed(remote, test_addr(6380));

        // send a role change: remote becomes a replica of primary
        let msg = GossipMessage::Ping {
            seq: 1,
            sender: remote,
            updates: vec![NodeUpdate::RoleChanged {
                node: remote,
                incarnation: 2,
                is_primary: false,
                replicates: Some(primary),
            }],
        };
        engine.handle_message(msg, test_addr(6380));

        let member = engine.members.get(&remote).unwrap();
        assert!(!member.is_primary);
        assert_eq!(member.replicates, Some(primary));
        assert_eq!(member.incarnation, 2);

        // should have emitted RoleChanged
        let mut found = false;
        engine.take_events().send().await;
        while let Ok(event) = rx.try_recv() {
            if let GossipEvent::RoleChanged(id, is_primary, replicates) = event {
                if id == remote {
                    assert!(!is_primary);
                    assert_eq!(replicates, Some(primary));
                    found = true;
                    break;
                }
            }
        }
        assert!(found, "expected RoleChanged event for remote");
    }

    #[tokio::test]
    async fn stale_role_changed_ignored() {
        let (tx, mut rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let remote = NodeId::new();
        engine.members.insert(
            remote,
            MemberState {
                id: remote,
                addr: test_addr(6380),
                incarnation: 10,
                state: MemberStatus::Alive,
                state_change: Instant::now(),
                is_primary: true,
                replicates: None,
                slots: vec![],
                config_epoch: 0,
            },
        );

        // send role change with old incarnation
        let msg = GossipMessage::Ping {
            seq: 1,
            sender: remote,
            updates: vec![NodeUpdate::RoleChanged {
                node: remote,
                incarnation: 5, // stale
                is_primary: false,
                replicates: None,
            }],
        };
        engine.handle_message(msg, test_addr(6380));

        // member should still be primary
        let member = engine.members.get(&remote).unwrap();
        assert!(member.is_primary, "stale update should not change role");

        // drain events (MemberAlive from the Ping sender, but no RoleChanged)
        let mut role_changed = false;
        engine.take_events().send().await;
        while let Ok(event) = rx.try_recv() {
            if matches!(event, GossipEvent::RoleChanged(..)) {
                role_changed = true;
            }
        }
        assert!(
            !role_changed,
            "stale role update should not emit RoleChanged"
        );
    }

    #[tokio::test]
    async fn excessive_incarnation_jump_rejected() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let remote = NodeId::new();
        engine.add_seed(remote, test_addr(6380));
        // seed starts at incarnation 0

        // a jump larger than MAX_INCARNATION_JUMP should be rejected
        let msg = GossipMessage::Ping {
            seq: 1,
            sender: remote,
            updates: vec![NodeUpdate::Alive {
                node: remote,
                addr: test_addr(6380),
                incarnation: MAX_INCARNATION_JUMP + 1,
            }],
        };
        engine.handle_message(msg, test_addr(6380));

        // incarnation should NOT have changed
        let member = engine.members.get(&remote).unwrap();
        assert_eq!(
            member.incarnation, 0,
            "incarnation should not change after excessive jump"
        );
    }

    #[tokio::test]
    async fn valid_incarnation_jump_accepted() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let remote = NodeId::new();
        engine.add_seed(remote, test_addr(6380));

        // a jump within the limit should be accepted
        let msg = GossipMessage::Ping {
            seq: 1,
            sender: remote,
            updates: vec![NodeUpdate::Alive {
                node: remote,
                addr: test_addr(6380),
                incarnation: MAX_INCARNATION_JUMP,
            }],
        };
        engine.handle_message(msg, test_addr(6380));

        let member = engine.members.get(&remote).unwrap();
        assert_eq!(
            member.incarnation, MAX_INCARNATION_JUMP,
            "valid incarnation jump should be accepted"
        );
    }

    #[tokio::test]
    async fn dead_update_far_ahead_is_ignored() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);
        let remote = NodeId::new();
        engine.add_seed(remote, test_addr(6380));

        let msg = GossipMessage::Ping {
            seq: 1,
            sender: NodeId::new(),
            updates: vec![NodeUpdate::Dead {
                node: remote,
                incarnation: MAX_INCARNATION_JUMP + 1,
            }],
        };
        engine.handle_message(msg, test_addr(6381));

        assert_ne!(engine.members[&remote].state, MemberStatus::Dead);
    }

    #[tokio::test]
    async fn suspicion_of_self_far_ahead_does_not_raise_incarnation() {
        let (tx, _rx) = mpsc::channel(16);
        let local = NodeId::new();
        let mut engine = GossipEngine::new(local, test_addr(6379), GossipConfig::default(), tx);
        let before = engine.incarnation;

        let msg = GossipMessage::Ping {
            seq: 1,
            sender: NodeId::new(),
            updates: vec![NodeUpdate::Suspect {
                node: local,
                incarnation: MAX_INCARNATION / 2,
            }],
        };
        engine.handle_message(msg, test_addr(6381));

        assert_eq!(engine.incarnation, before);
    }

    /// Delivers `msg` from `from` to an engine that knows `voter` at
    /// port 6380, and returns the events it produced.
    async fn deliver_vote(msg: GossipMessage, voter: NodeId, from: SocketAddr) -> Vec<GossipEvent> {
        let (tx, mut rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);
        engine.add_seed(voter, test_addr(6380));
        engine.handle_message(msg, from);
        engine.take_events().send().await;
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    fn grant(from: NodeId) -> NodeUpdate {
        NodeUpdate::VoteGranted {
            from,
            candidate: NodeId::new(),
            epoch: 2,
        }
    }

    #[tokio::test]
    async fn vote_from_the_voters_address_counts() {
        let voter = NodeId::new();
        let msg = GossipMessage::Election {
            sender: voter,
            update: grant(voter),
        };
        let events = deliver_vote(msg, voter, test_addr(6380)).await;
        assert!(matches!(events[..], [GossipEvent::VoteGranted { from, .. }] if from == voter));
    }

    #[tokio::test]
    async fn vote_from_another_address_is_ignored() {
        let voter = NodeId::new();
        let msg = GossipMessage::Election {
            sender: voter,
            update: grant(voter),
        };
        assert!(deliver_vote(msg, voter, test_addr(6390)).await.is_empty());
    }

    #[tokio::test]
    async fn vote_for_someone_else_is_ignored() {
        // the sender is known, but the vote names another voter
        let voter = NodeId::new();
        let msg = GossipMessage::Election {
            sender: voter,
            update: grant(NodeId::new()),
        };
        assert!(deliver_vote(msg, voter, test_addr(6380)).await.is_empty());
    }

    #[tokio::test]
    async fn piggybacked_vote_is_ignored() {
        let voter = NodeId::new();
        let msg = GossipMessage::Ping {
            seq: 1,
            sender: voter,
            updates: vec![grant(voter)],
        };
        let events = deliver_vote(msg, voter, test_addr(6380)).await;
        assert!(!events
            .iter()
            .any(|e| matches!(e, GossipEvent::VoteGranted { .. })));
    }
}
