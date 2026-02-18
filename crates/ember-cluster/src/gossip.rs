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
    pub slots: Vec<SlotRange>,
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
    /// A new node joined the cluster.
    MemberJoined(NodeId, SocketAddr, Vec<SlotRange>),
    /// A node is suspected to be failing.
    MemberSuspected(NodeId),
    /// A node has been confirmed dead.
    MemberFailed(NodeId),
    /// A node left gracefully.
    MemberLeft(NodeId),
    /// A node that was suspected is now alive.
    MemberAlive(NodeId),
    /// A node's slot ownership changed.
    SlotsChanged(NodeId, Vec<SlotRange>),
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
    /// Pending updates to piggyback on outgoing messages.
    pending_updates: Vec<NodeUpdate>,
    /// Sequence number for protocol messages.
    next_seq: u64,
    /// Pending probes awaiting acknowledgment.
    pending_probes: HashMap<u64, PendingProbe>,
    /// Channel for emitting events.
    event_tx: mpsc::Sender<GossipEvent>,
    /// Slot ranges owned by the local node, included in Welcome replies.
    local_slots: Vec<SlotRange>,
    /// Active PingReq relays waiting for an Ack from the target.
    relay_pending: HashMap<u64, RelayEntry>,
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
            local_slots: Vec::new(),
            relay_pending: HashMap::new(),
        }
    }

    /// Returns the local node ID.
    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    /// Returns the local node's incarnation number.
    pub fn local_incarnation(&self) -> u64 {
        self.incarnation
    }

    /// Restores the incarnation number from a previous session.
    ///
    /// Used when loading persisted config so the node doesn't regress
    /// to a lower incarnation, which would make it lose suspicion refutations.
    pub fn set_incarnation(&mut self, n: u64) {
        self.incarnation = n;
    }

    /// Returns all known members.
    pub fn members(&self) -> impl Iterator<Item = &MemberState> {
        self.members.values()
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
    pub fn set_local_slots(&mut self, slots: Vec<SlotRange>) {
        self.local_slots = slots;
    }

    /// Queues a slot ownership update for gossip propagation.
    ///
    /// The update will be piggybacked on the next outgoing Ping or Ack
    /// message, spreading to the cluster via epidemic dissemination.
    pub fn queue_slots_update(&mut self, node: NodeId, incarnation: u64, slots: Vec<SlotRange>) {
        self.queue_update(NodeUpdate::SlotsChanged {
            node,
            incarnation,
            slots,
        });
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
            slots: Vec::new(),
        });
    }

    /// Handles an incoming gossip message.
    ///
    /// Returns a list of `(address, message)` pairs to send. Most messages
    /// produce a single reply back to `from`, but PingReq forwards a Ping
    /// to a different host, and relayed Acks route back to the original
    /// requester.
    pub async fn handle_message(
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
                self.apply_updates(&updates).await;
                self.ensure_member(sender, from);

                // Reply with ACK
                let response_updates = self.collect_updates();
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
                self.apply_updates(&updates).await;
                self.ensure_member(sender, from);

                let mut outgoing = Vec::new();

                // Clear pending probe
                if let Some(probe) = self.pending_probes.remove(&seq) {
                    if self.members.get(&probe.target).map(|m| m.state)
                        == Some(MemberStatus::Suspect)
                    {
                        // Node recovered from suspicion
                        self.mark_alive(probe.target).await;
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
                self.ensure_member(sender, sender_addr);

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
                        slots: m.slots.clone(),
                    })
                    .collect();

                // include our own slot info so the joiner learns the full map
                members.push(MemberInfo {
                    id: self.local_id,
                    addr: self.local_addr,
                    incarnation: self.incarnation,
                    is_primary: true,
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
                self.ensure_member(sender, from);

                for member in members {
                    if member.id == self.local_id {
                        continue;
                    }
                    let slots = member.slots.clone();
                    if let std::collections::hash_map::Entry::Vacant(e) =
                        self.members.entry(member.id)
                    {
                        e.insert(MemberState {
                            id: member.id,
                            addr: member.addr,
                            incarnation: member.incarnation,
                            state: MemberStatus::Alive,
                            state_change: Instant::now(),
                            is_primary: member.is_primary,
                            slots: slots.clone(),
                        });
                        self.emit(GossipEvent::MemberJoined(member.id, member.addr, slots))
                            .await;
                    }
                }
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

        // Select a random alive member to probe
        let target_info = {
            let alive_members: Vec<_> = self
                .members
                .values()
                .filter(|m| m.state == MemberStatus::Alive || m.state == MemberStatus::Suspect)
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

        let updates = self.collect_updates();
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
            slots: Vec::new(),
        });
    }

    /// Sends a gossip event to the external event channel.
    ///
    /// Logs a warning when the channel is closed (receiver dropped). This
    /// normally only happens during shutdown, so seeing the message in steady
    /// state indicates a bug in the event consumer.
    async fn emit(&self, event: GossipEvent) {
        if self.event_tx.send(event).await.is_err() {
            warn!("gossip event channel closed, dropping event");
        }
    }

    async fn apply_updates(&mut self, updates: &[NodeUpdate]) {
        for update in updates {
            match update {
                NodeUpdate::Alive {
                    node,
                    addr,
                    incarnation,
                } => {
                    if *incarnation > MAX_INCARNATION {
                        warn!(
                            "rejecting alive update for {} with excessive incarnation {}",
                            node, incarnation
                        );
                        continue;
                    }
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
                                self.emit(GossipEvent::MemberAlive(*node)).await;
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
                                slots: Vec::new(),
                            },
                        );
                        self.emit(GossipEvent::MemberJoined(*node, *addr, Vec::new()))
                            .await;
                    }
                }

                NodeUpdate::Suspect { node, incarnation } => {
                    if *incarnation > MAX_INCARNATION {
                        warn!(
                            "rejecting suspect update for {} with excessive incarnation {}",
                            node, incarnation
                        );
                        continue;
                    }
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
                            self.emit(GossipEvent::MemberSuspected(*node)).await;
                        }
                    }
                }

                NodeUpdate::Dead { node, incarnation } => {
                    if *incarnation > MAX_INCARNATION {
                        warn!(
                            "rejecting dead update for {} with excessive incarnation {}",
                            node, incarnation
                        );
                        continue;
                    }
                    if *node == self.local_id {
                        // Refute death claim
                        self.incarnation = incarnation.saturating_add(1);
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
                            self.emit(GossipEvent::MemberFailed(*node)).await;
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
                            self.emit(GossipEvent::MemberLeft(*node)).await;
                        }
                    }
                }

                NodeUpdate::SlotsChanged {
                    node,
                    incarnation,
                    slots,
                } => {
                    if *incarnation > MAX_INCARNATION {
                        warn!(
                            "rejecting slots update for {} with excessive incarnation {}",
                            node, incarnation
                        );
                        continue;
                    }
                    if *node == self.local_id {
                        continue;
                    }
                    if let Some(member) = self.members.get_mut(node) {
                        // only accept if incarnation is at least as recent
                        if *incarnation >= member.incarnation {
                            member.slots = slots.clone();
                            self.emit(GossipEvent::SlotsChanged(*node, slots.clone()))
                                .await;
                        }
                    }
                }
            }
        }
    }

    async fn mark_alive(&mut self, node: NodeId) {
        if let Some(member) = self.members.get_mut(&node) {
            if member.state == MemberStatus::Suspect {
                member.state = MemberStatus::Alive;
                member.state_change = Instant::now();
                self.emit(GossipEvent::MemberAlive(node)).await;
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

        // Phase 2: indirect probe timeouts → mark Suspect
        let indirect_timed_out: Vec<_> = self
            .pending_probes
            .iter()
            .filter(|(_, probe)| probe.indirect && now.duration_since(probe.sent_at) > timeout)
            .map(|(seq, probe)| (*seq, probe.target))
            .collect();

        for (seq, target) in indirect_timed_out {
            self.pending_probes.remove(&seq);

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
            }
        }

        // Phase 1: direct ping timeouts → send PingReq
        let direct_timed_out: Vec<_> = self
            .pending_probes
            .iter()
            .filter(|(_, probe)| !probe.indirect && now.duration_since(probe.sent_at) > timeout)
            .map(|(seq, probe)| (*seq, probe.target))
            .collect();

        for (seq, target) in direct_timed_out {
            self.pending_probes.remove(&seq);

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
            }
        }
    }

    fn queue_update(&mut self, update: NodeUpdate) {
        self.pending_updates.push(update);
        // When the queue overflows, drop the oldest pending updates.
        // This is safe: gossip convergence doesn't require every update to be
        // delivered. Members re-gossip their state on each protocol period, so
        // a dropped update will be re-sent in the next round.
        if self.pending_updates.len() > self.config.max_piggyback * 2 {
            self.pending_updates.drain(0..self.config.max_piggyback);
        }
    }

    fn collect_updates(&mut self) -> Vec<NodeUpdate> {
        let count = self.pending_updates.len().min(self.config.max_piggyback);
        self.pending_updates.drain(0..count).collect()
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

        let responses = engine.handle_message(msg, test_addr(6380)).await;
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0].1, GossipMessage::Ack { .. }));
        assert_eq!(responses[0].0.port(), 6380);
        assert_eq!(engine.alive_count(), 1);
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

        let responses = engine.handle_message(msg, test_addr(6380)).await;
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
            slots: slots.clone(),
        }];

        let msg = GossipMessage::Ping {
            seq: 1,
            sender: remote,
            updates,
        };
        engine.handle_message(msg, test_addr(6380)).await;

        // member should have updated slots
        let member = engine.members.get(&remote).unwrap();
        assert_eq!(member.slots, slots);

        // should have emitted a SlotsChanged event
        let event = rx.try_recv().unwrap();
        assert!(matches!(event, GossipEvent::SlotsChanged(id, _) if id == remote));
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
                slots: vec![SlotRange::new(0, 5460)],
            },
        );

        // send slots update with stale incarnation (lower)
        let msg = GossipMessage::Ping {
            seq: 1,
            sender: remote,
            updates: vec![NodeUpdate::SlotsChanged {
                node: remote,
                incarnation: 3, // stale
                slots: vec![],
            }],
        };
        engine.handle_message(msg, test_addr(6380)).await;

        // slots should NOT have been cleared
        let member = engine.members.get(&remote).unwrap();
        assert_eq!(member.slots.len(), 1);

        // no SlotsChanged event should have been emitted
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn welcome_includes_local_slots() {
        let (tx, _rx) = mpsc::channel(16);
        let local_id = NodeId::new();
        let mut engine = GossipEngine::new(local_id, test_addr(6379), GossipConfig::default(), tx);

        engine.set_local_slots(vec![SlotRange::new(0, 16383)]);

        let joiner = NodeId::new();
        let msg = GossipMessage::Join {
            sender: joiner,
            sender_addr: test_addr(6380),
        };

        let responses = engine.handle_message(msg, test_addr(6380)).await;
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
                slots: slots.clone(),
            }],
        };

        engine.handle_message(msg, test_addr(6380)).await;

        // member should be added with slots
        let member = engine.members.get(&member_id).unwrap();
        assert_eq!(member.slots, slots);

        // should emit MemberJoined with slots
        let event = rx.try_recv().unwrap();
        match event {
            GossipEvent::MemberJoined(id, _, s) => {
                assert_eq!(id, member_id);
                assert_eq!(s, slots);
            }
            other => panic!("expected MemberJoined, got {other:?}"),
        }
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

        let responses = engine.handle_message(msg, test_addr(6380)).await;

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
        let responses = engine.handle_message(msg, requester_addr).await;
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
        let responses = engine.handle_message(ack, target_addr).await;

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
}
