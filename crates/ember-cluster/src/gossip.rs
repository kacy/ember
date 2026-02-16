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
    MemberJoined(NodeId, SocketAddr),
    /// A node is suspected to be failing.
    MemberSuspected(NodeId),
    /// A node has been confirmed dead.
    MemberFailed(NodeId),
    /// A node left gracefully.
    MemberLeft(NodeId),
    /// A node that was suspected is now alive.
    MemberAlive(NodeId),
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
}

struct PendingProbe {
    target: NodeId,
    sent_at: Instant,
    indirect: bool,
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
        }
    }

    /// Returns the local node ID.
    pub fn local_id(&self) -> NodeId {
        self.local_id
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
    pub async fn handle_message(
        &mut self,
        msg: GossipMessage,
        from: SocketAddr,
    ) -> Option<GossipMessage> {
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
                Some(GossipMessage::Ack {
                    seq,
                    sender: self.local_id,
                    updates: response_updates,
                })
            }

            GossipMessage::PingReq {
                seq,
                sender,
                target,
                target_addr: _,
            } => {
                trace!(
                    "received ping-req seq={} from {} for {}",
                    seq,
                    sender,
                    target
                );
                self.ensure_member(sender, from);

                // Forward ping to target (handled externally)
                // For now, we just record that we might need to relay
                None
            }

            GossipMessage::Ack {
                seq,
                sender,
                updates,
            } => {
                trace!("received ack seq={} from {}", seq, sender);
                self.apply_updates(&updates).await;
                self.ensure_member(sender, from);

                // Clear pending probe
                if let Some(probe) = self.pending_probes.remove(&seq) {
                    if self.members.get(&probe.target).map(|m| m.state)
                        == Some(MemberStatus::Suspect)
                    {
                        // Node recovered from suspicion
                        self.mark_alive(probe.target).await;
                    }
                }
                None
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

                // Send welcome with current members
                let members: Vec<MemberInfo> = self
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

                Some(GossipMessage::Welcome {
                    sender: self.local_id,
                    members,
                })
            }

            GossipMessage::Welcome { sender, members } => {
                info!(
                    "received welcome from {} with {} members",
                    sender,
                    members.len()
                );
                self.ensure_member(sender, from);

                for member in members {
                    if member.id != self.local_id {
                        self.members
                            .entry(member.id)
                            .or_insert_with(|| MemberState {
                                id: member.id,
                                addr: member.addr,
                                incarnation: member.incarnation,
                                state: MemberStatus::Alive,
                                state_change: Instant::now(),
                                is_primary: member.is_primary,
                                slots: member.slots,
                            });
                    }
                }
                None
            }
        }
    }

    /// Runs one protocol period: probe a random node.
    pub fn tick(&mut self) -> Option<(SocketAddr, GossipMessage)> {
        // Check for timed-out probes
        self.check_probe_timeouts();

        // Check for expired suspicions
        self.check_suspicion_timeouts();

        // Select a random alive member to probe
        let target_info = {
            let alive_members: Vec<_> = self
                .members
                .values()
                .filter(|m| m.state == MemberStatus::Alive || m.state == MemberStatus::Suspect)
                .map(|m| (m.id, m.addr))
                .collect();

            if alive_members.is_empty() {
                return None;
            }

            *alive_members.choose(&mut rand::rng())?
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

        Some((target_addr, msg))
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
                                if self
                                    .event_tx
                                    .send(GossipEvent::MemberAlive(*node))
                                    .await
                                    .is_err()
                                {
                                    warn!("event channel closed, cannot send MemberAlive event");
                                }
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
                        let _ = self
                            .event_tx
                            .send(GossipEvent::MemberJoined(*node, *addr))
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
                            let _ = self
                                .event_tx
                                .send(GossipEvent::MemberSuspected(*node))
                                .await;
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
                            if self
                                .event_tx
                                .send(GossipEvent::MemberFailed(*node))
                                .await
                                .is_err()
                            {
                                warn!("event channel closed, cannot send MemberFailed event");
                            }
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
                            if self
                                .event_tx
                                .send(GossipEvent::MemberLeft(*node))
                                .await
                                .is_err()
                            {
                                warn!("event channel closed, cannot send MemberLeft event");
                            }
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
                if self
                    .event_tx
                    .send(GossipEvent::MemberAlive(node))
                    .await
                    .is_err()
                {
                    warn!("event channel closed, cannot send MemberAlive event");
                }
            }
        }
    }

    fn check_probe_timeouts(&mut self) {
        let timeout = self.config.probe_timeout;
        let now = Instant::now();

        // Collect timed out probes first
        let timed_out: Vec<_> = self
            .pending_probes
            .iter()
            .filter(|(_, probe)| now.duration_since(probe.sent_at) > timeout && !probe.indirect)
            .map(|(seq, probe)| (*seq, probe.target))
            .collect();

        // Now process them
        for (seq, target) in timed_out {
            self.pending_probes.remove(&seq);

            // Get incarnation before mutating
            let incarnation = self
                .members
                .get(&target)
                .filter(|m| m.state == MemberStatus::Alive)
                .map(|m| m.incarnation);

            if let Some(inc) = incarnation {
                if let Some(member) = self.members.get_mut(&target) {
                    debug!("node {} failed to respond, marking suspect", target);
                    member.state = MemberStatus::Suspect;
                    member.state_change = Instant::now();
                }
                self.queue_update(NodeUpdate::Suspect {
                    node: target,
                    incarnation: inc,
                });
            }
        }
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
        // Keep bounded
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

        let response = engine.handle_message(msg, test_addr(6380)).await;
        assert!(matches!(response, Some(GossipMessage::Ack { .. })));
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

        let response = engine.handle_message(msg, test_addr(6380)).await;
        assert!(matches!(response, Some(GossipMessage::Welcome { .. })));
        assert_eq!(engine.alive_count(), 1);
    }

    #[tokio::test]
    async fn tick_with_no_members() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        let probe = engine.tick();
        assert!(probe.is_none());
    }

    #[tokio::test]
    async fn tick_with_members() {
        let (tx, _rx) = mpsc::channel(16);
        let mut engine =
            GossipEngine::new(NodeId::new(), test_addr(6379), GossipConfig::default(), tx);

        engine.add_seed(NodeId::new(), test_addr(6380));
        let probe = engine.tick();
        assert!(probe.is_some());

        let (addr, msg) = probe.unwrap();
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
}
