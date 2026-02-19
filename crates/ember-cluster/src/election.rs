//! Automatic failover election state machine.
//!
//! When gossip confirms a primary has failed, its replicas compete to replace
//! it. Each replica starts a timed election: it broadcasts a `VoteRequest`
//! via gossip and waits for primary nodes to respond with `VoteGranted`.
//!
//! The first replica to collect votes from a majority of the alive primaries
//! wins the election and promotes itself via `CLUSTER FAILOVER FORCE`.
//!
//! # Fairness
//!
//! Replicas that are more up-to-date (higher replication offset) are
//! preferred by waiting less before broadcasting their request. This is
//! achieved by the caller applying a stagger delay before calling
//! `queue_vote_request`.

use std::collections::HashSet;

use crate::NodeId;

/// State for an in-progress automatic failover election.
///
/// Created when a replica detects its primary has failed.  Discarded when
/// the election succeeds (promotion triggered) or times out.
pub struct Election {
    /// Config epoch this election is contesting.
    pub epoch: u64,
    /// Node IDs of primaries that have voted for us.
    votes: HashSet<NodeId>,
    /// Whether promotion has already been triggered for this election.
    promoted: bool,
}

impl Election {
    /// Creates a new election for the given epoch.
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            votes: HashSet::new(),
            promoted: false,
        }
    }

    /// Records a vote from `from`. Returns `true` if quorum is newly reached.
    ///
    /// `total_primaries` is the number of alive primaries (excluding the
    /// failed one) at the time the election started.  Quorum is a simple
    /// majority: `total_primaries / 2 + 1`.
    ///
    /// Once quorum is reached this returns `true` exactly once; subsequent
    /// calls return `false` so the caller promotes exactly once.
    pub fn record_vote(&mut self, from: NodeId, total_primaries: usize) -> bool {
        if self.promoted {
            return false;
        }
        self.votes.insert(from);
        self.promoted = self.votes.len() >= Self::quorum(total_primaries);
        self.promoted
    }

    /// Minimum votes required for a majority.
    pub fn quorum(total_primaries: usize) -> usize {
        total_primaries / 2 + 1
    }

    /// Returns `true` if this election has already succeeded.
    pub fn is_promoted(&self) -> bool {
        self.promoted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_single_primary() {
        assert_eq!(Election::quorum(1), 1);
    }

    #[test]
    fn quorum_three_primaries() {
        assert_eq!(Election::quorum(3), 2);
    }

    #[test]
    fn quorum_five_primaries() {
        assert_eq!(Election::quorum(5), 3);
    }

    #[test]
    fn record_vote_reaches_quorum() {
        let mut e = Election::new(1);
        let n1 = NodeId::new();
        let n2 = NodeId::new();
        // 3 primaries → need 2 votes
        assert!(!e.record_vote(n1, 3), "first vote should not reach quorum");
        assert!(e.record_vote(n2, 3), "second vote should reach quorum");
        assert!(e.is_promoted());
    }

    #[test]
    fn record_vote_deduplicates() {
        let mut e = Election::new(1);
        let n1 = NodeId::new();
        // 3 primaries need 2 votes; same voter counted only once
        assert!(!e.record_vote(n1, 3));
        assert!(!e.record_vote(n1, 3), "duplicate vote must not count");
        assert!(!e.is_promoted());
    }

    #[test]
    fn no_votes_after_promotion() {
        let mut e = Election::new(1);
        let n1 = NodeId::new();
        // single primary — one vote is enough
        assert!(e.record_vote(n1, 1));
        // further votes return false
        assert!(!e.record_vote(NodeId::new(), 1));
    }

    #[test]
    fn single_node_quorum() {
        let mut e = Election::new(42);
        assert!(e.record_vote(NodeId::new(), 1));
        assert!(e.is_promoted());
    }
}
