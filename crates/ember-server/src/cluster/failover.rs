//! Automatic failover: elections, votes and promotion, plus CLUSTER FAILOVER.

use super::*;

/// Stagger delay before broadcasting a vote request so replicas with more
/// data wait less (replaced with a fixed value until offset tracking lands).
const ELECTION_STAGGER_MS: u64 = 500;

/// Timeout for an in-progress election; clears state if quorum not reached.
const ELECTION_TIMEOUT_SECS: u64 = 5;

/// Election rounds a replica runs before giving up on a failover.
const ELECTION_ROUNDS: u64 = 5;

/// How far past this node's current epoch a vote request may be. Candidates
/// ask at most `ELECTION_ROUNDS` past theirs, and nodes see each other's
/// epochs through gossip, so a request much further ahead is bogus.
pub(super) const MAX_VOTE_EPOCH_AHEAD: u64 = 100;

/// Grace period given to the primary during a default (non-FORCE) failover.
const FAILOVER_GRACE_MS: u64 = 500;

impl ClusterCoordinator {
    /// Runs an automatic failover election after `failed_primary` is
    /// confirmed dead.
    ///
    /// A round can fail when voters have not yet seen the failure
    /// themselves, so it retries while the primary stays down and this node
    /// is still a replica. Each round uses a higher epoch, since a primary
    /// votes at most once per epoch.
    pub(super) async fn start_election(self: &Arc<Self>, failed_primary: NodeId) {
        for round in 1..=ELECTION_ROUNDS {
            if !self.election_round(failed_primary, round).await || !self.is_replica().await {
                return;
            }
        }
        warn!(
            "election: gave up on failed primary {failed_primary} after {ELECTION_ROUNDS} rounds"
        );
    }

    /// Runs one election round. Applies a brief stagger delay, broadcasts a
    /// `VoteRequest` via gossip, and waits up to 5 seconds for a quorum of
    /// primaries to respond with `VoteGranted`. Returns `true` if the round
    /// timed out and another may follow.
    pub(super) async fn election_round(
        self: &Arc<Self>,
        failed_primary: NodeId,
        round: u64,
    ) -> bool {
        // fixed 500 ms stagger; a production implementation should compute
        // (max_offset - my_offset) * scale so the most up-to-date replica wins.
        // this requires a shared offset oracle (e.g., gossip-advertised offset).
        tokio::time::sleep(std::time::Duration::from_millis(ELECTION_STAGGER_MS)).await;

        // Re-read cluster state — the primary may have recovered during our sleep.
        let (epoch, voters, still_failed) = {
            let state = self.state.read().await;
            let epoch = state.config_epoch + round;
            // every other primary votes, reachable or not. counting only
            // the ones this node can reach would let a replica cut off from
            // the cluster win with its own tiny view of it (split brain).
            let voters: HashSet<NodeId> = state
                .nodes
                .values()
                .filter(|n| {
                    n.role == NodeRole::Primary && n.id != failed_primary && !n.flags.handshake
                })
                .map(|n| n.id)
                .collect();
            let still_failed = state
                .nodes
                .get(&failed_primary)
                .map(|n| n.flags.fail)
                .unwrap_or(true);
            (epoch, voters, still_failed)
        };

        if !still_failed {
            debug!(
                "election: primary {} recovered before election started",
                failed_primary
            );
            return false;
        }

        if voters.is_empty() {
            // nobody can confirm the primary is really gone, so promoting
            // could leave two primaries serving the same slots
            warn!(
                "election: no other primaries to vote; automatic failover of {} needs at \
                 least one more primary in the cluster",
                failed_primary
            );
            return false;
        }
        let total_primaries = voters.len();

        // ask every voter directly
        let requests: Vec<_> = {
            let gossip = self.gossip.lock().await;
            voters
                .iter()
                .filter_map(|&voter| gossip.vote_request(voter, failed_primary, epoch, 0))
                .collect()
        };

        // Initialize the election.
        {
            let mut guard = self.election.lock().await;
            *guard = Some(ElectionAttempt {
                inner: Election::new(epoch),
                voters,
            });
        }

        info!(
            "election: starting for epoch {} ({} primary voters needed)",
            epoch,
            Election::quorum(total_primaries)
        );

        self.send_direct(requests).await;

        // Wait for votes; give the cluster time to respond.
        tokio::time::sleep(std::time::Duration::from_secs(ELECTION_TIMEOUT_SECS)).await;

        // Timed out without quorum — clear election state.
        let mut guard = self.election.lock().await;
        match *guard {
            Some(ref e) if e.inner.epoch == epoch && !e.inner.is_promoted() => {
                warn!(
                    "election: timed out for epoch {} without reaching quorum",
                    epoch
                );
                *guard = None;
                true
            }
            _ => false,
        }
    }

    /// Handles an incoming `VoteRequest` gossip event.
    ///
    /// If this node is a primary and hasn't voted in the given epoch, it
    /// grants its vote to the candidate and broadcasts `VoteGranted` via gossip.
    pub(super) async fn handle_vote_request(
        &self,
        candidate: NodeId,
        failed_primary: NodeId,
        epoch: u64,
    ) {
        // Only primaries vote, and only when this node also sees the
        // primary being replaced as failing. Otherwise a replica that merely
        // lost its own link to a healthy primary could take over its slots.
        // The candidate names that primary in the request, since news of
        // who replicates whom may not have reached this node yet; if it has,
        // the two must agree.
        {
            let mut state = self.state.write().await;
            let is_primary = state
                .nodes
                .get(&self.local_id)
                .is_some_and(|n| n.role == NodeRole::Primary);
            let primary_failing = state
                .nodes
                .get(&failed_primary)
                .is_some_and(|primary| primary.flags.fail || primary.flags.pfail);
            let known_primary = state.nodes.get(&candidate).and_then(|c| c.replicates);
            if !(is_primary && primary_failing && known_primary.is_none_or(|p| p == failed_primary))
            {
                debug!("election: not voting for {candidate}; its primary looks healthy from here");
                return;
            }
            // a request far ahead of every epoch seen here would use up the
            // epochs that later elections need
            if epoch > state.config_epoch.saturating_add(MAX_VOTE_EPOCH_AHEAD) {
                debug!(
                    "election: not voting in epoch {epoch}, far past the current {}",
                    state.config_epoch
                );
                return;
            }
            // one vote per epoch
            if epoch <= state.last_vote_epoch {
                debug!(
                    "election: already voted in epoch {} (requested {}); ignoring",
                    state.last_vote_epoch, epoch
                );
                return;
            }
            state.last_vote_epoch = epoch;
        }
        // the vote goes to disk before it is granted, so a restart can't
        // lead to a second vote in the same epoch
        self.save_config().await;

        info!(
            "election: granting vote to candidate {} for epoch {}",
            candidate, epoch
        );

        let grant = self.gossip.lock().await.vote_granted(candidate, epoch);
        self.send_direct(grant).await;
    }

    /// Handles an incoming `VoteGranted` gossip event.
    ///
    /// If this node is the intended candidate and has an in-progress election,
    /// records the vote. Triggers promotion when quorum is reached.
    pub(super) async fn handle_vote_granted(
        self: &Arc<Self>,
        from: NodeId,
        candidate: NodeId,
        epoch: u64,
    ) {
        if candidate != self.local_id {
            return; // not meant for us
        }

        let should_promote = {
            let mut guard = self.election.lock().await;
            match guard.as_mut() {
                Some(attempt) if attempt.inner.epoch == epoch && attempt.voters.contains(&from) => {
                    attempt.inner.record_vote(from, attempt.voters.len())
                }
                _ => false,
            }
        };

        if should_promote {
            info!(
                "election: quorum reached for epoch {}; promoting self",
                epoch
            );
            let _ = self.cluster_failover(true, false).await;
        }
    }

    /// CLUSTER FAILOVER [FORCE|TAKEOVER]
    ///
    /// Promotes this replica to primary. Must be run on a replica node.
    ///
    /// Three modes:
    /// - **Default**: waits 500ms for replication to catch up, then promotes
    ///   via Raft so all nodes agree on the new topology.
    /// - **FORCE**: skips the grace period; promotes via Raft immediately.
    ///   Use when the primary is unreachable and you accept possible data loss.
    /// - **TAKEOVER**: bypasses Raft entirely. Updates local state and
    ///   announces the new role via gossip. Use when Raft quorum is lost.
    pub async fn cluster_failover(&self, force: bool, takeover: bool) -> Frame {
        // verify we are a replica with a configured primary
        let primary_id = {
            let state = self.state.read().await;
            let local = match state.nodes.get(&self.local_id) {
                Some(n) => n,
                None => return Frame::Error("ERR local node not found in cluster state".into()),
            };
            if local.role != NodeRole::Replica {
                return Frame::Error("ERR You should send CLUSTER FAILOVER to a replica".into());
            }
            match local.replicates {
                Some(id) => id,
                None => return Frame::Error("ERR No primary configured for this replica".into()),
            }
        };

        if takeover {
            // TAKEOVER: immediate local promotion, no Raft or primary coordination.
            // The replica asserts itself as primary and gossips the change;
            // the rest of the cluster learns via gossip convergence.
            {
                let mut state = self.state.write().await;
                if let Err(e) = state.promote_replica(self.local_id) {
                    return Frame::Error(format!("ERR {e}"));
                }
            }
            self.finish_promotion().await;
            info!(local_id = %self.local_id, %primary_id, "TAKEOVER: promoted to primary");
            return Frame::Simple("OK".into());
        }

        // Default / FORCE: use Raft for cluster-wide agreement.
        if !force {
            // give the replication stream a brief window to deliver
            // any in-flight records before we cut over. a future improvement
            // could track the exact offset and wait for full catchup.
            tokio::time::sleep(std::time::Duration::from_millis(FAILOVER_GRACE_MS)).await;
        }

        // get the primary's current slot ranges so we can hand them over
        let primary_slots = {
            let state = self.state.read().await;
            state
                .nodes
                .get(&primary_id)
                .map(|n| n.slots.clone())
                .unwrap_or_default()
        };

        if let Some(raft) = self.raft_node.get() {
            // promote this replica in the Raft state machine
            let promote_cmd = ClusterCommand::PromoteReplica {
                replica_id: self.local_id,
            };
            if let Err(e) = raft.propose(promote_cmd).await {
                return Self::raft_error_frame(e);
            }

            // move the primary's slots to this node in one step
            if !primary_slots.is_empty() {
                let transfer_cmd = ClusterCommand::TransferSlots {
                    from: primary_id,
                    to: self.local_id,
                    slots: primary_slots,
                };
                if let Err(e) = raft.propose(transfer_cmd).await {
                    return Self::raft_error_frame(e);
                }
            }
        }

        // apply locally right away so this node can start accepting writes
        // without waiting for the async Raft reconciliation to complete
        {
            let mut state = self.state.write().await;
            if let Err(e) = state.promote_replica(self.local_id) {
                warn!(%e, "local promote_replica after Raft proposal failed");
            }
        }

        self.finish_promotion().await;

        let mode = if force { "FORCE" } else { "default" };
        info!(local_id = %self.local_id, %primary_id, mode, "promoted to primary");
        Frame::Simple("OK".into())
    }

    /// Completes a promotion that has already updated the local state:
    /// stops pulling from the old primary, announces the new role, and
    /// saves the config.
    pub(super) async fn finish_promotion(&self) {
        self.stop_replication_client().await;
        self.announce_promotion().await;
        self.save_config().await;
    }

    /// Tells the cluster this node is now a primary and which slots it owns.
    ///
    /// The incarnation is bumped first: peers had already seen the current
    /// one on this node's earlier role update and would ignore a new update
    /// that reused it. The slots go straight to every peer as well, so no
    /// peer keeps routing them to the old primary.
    pub(super) async fn announce_promotion(&self) {
        let (slots, epoch) = {
            let state = self.state.read().await;
            (
                state.slot_map.slots_for_node(self.local_id),
                state.local_epoch(),
            )
        };
        {
            let mut gossip = self.gossip.lock().await;
            let incarnation = gossip.bump_incarnation();
            gossip.queue_role_update(self.local_id, incarnation, true, None);
        }
        self.broadcast_local_slots(slots, epoch).await;
    }
}
