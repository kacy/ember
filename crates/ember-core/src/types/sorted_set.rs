//! Sorted set data structure: dual-indexed by score and member.
//!
//! Each member has a unique name and an associated `f64` score. Members
//! are ordered by (score, member) — ties in score are broken
//! lexicographically, matching Redis semantics.
//!
//! Implementation uses a `BTreeMap<(OrderedFloat<f64>, String), ()>` for
//! ordered iteration and a `HashMap<String, OrderedFloat<f64>>` for O(1)
//! member→score lookups. This is simpler and more correct than a
//! hand-rolled skip list.

use std::collections::{BTreeMap, HashMap};

use ordered_float::OrderedFloat;

/// Flags that control ZADD behavior.
#[derive(Debug, Clone, Default)]
pub struct ZAddFlags {
    /// Only add new members, don't update existing scores.
    pub nx: bool,
    /// Only update existing members, don't add new ones.
    pub xx: bool,
    /// Only update when new score > current score.
    pub gt: bool,
    /// Only update when new score < current score.
    pub lt: bool,
    /// Return count of changed members (added + updated) instead of just added.
    pub ch: bool,
}

/// Result of a single ZADD member operation.
#[derive(Debug, Clone, Copy)]
pub struct AddResult {
    /// Whether a new member was added.
    pub added: bool,
    /// Whether an existing member's score was changed.
    pub updated: bool,
}

impl AddResult {
    /// No change: member was neither added nor updated.
    pub const UNCHANGED: Self = Self {
        added: false,
        updated: false,
    };
}

/// A sorted set of unique string members, each with a floating-point score.
///
/// Members are ordered by `(score, member_name)`. Rank is determined by
/// position in this ordering (0-based, lowest score first).
#[derive(Debug, Clone)]
pub struct SortedSet {
    /// Score→member index for ordered iteration.
    tree: BTreeMap<(OrderedFloat<f64>, String), ()>,
    /// Member→score index for O(1) lookups.
    scores: HashMap<String, OrderedFloat<f64>>,
}

impl SortedSet {
    /// Creates an empty sorted set.
    pub fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
            scores: HashMap::new(),
        }
    }

    /// Adds or updates a member with the given score. Returns whether the
    /// member was newly added and/or updated.
    pub fn add(&mut self, member: String, score: f64) -> AddResult {
        self.add_with_flags(member, score, &ZAddFlags::default())
    }

    /// Adds or updates a member with ZADD flag semantics.
    pub fn add_with_flags(&mut self, member: String, score: f64, flags: &ZAddFlags) -> AddResult {
        let new_score = OrderedFloat(score);

        if let Some(&old_score) = self.scores.get(&member) {
            // member exists — skip if any flag condition prevents the update
            if flags.nx
                || (flags.gt && new_score <= old_score)
                || (flags.lt && new_score >= old_score)
                || new_score == old_score
            {
                return AddResult::UNCHANGED;
            }
            // update: remove old entry, insert new
            self.tree.remove(&(old_score, member.clone()));
            self.scores.insert(member.clone(), new_score);
            self.tree.insert((new_score, member), ());
            AddResult {
                added: false,
                updated: true,
            }
        } else {
            // new member — XX means only update, so skip
            if flags.xx {
                return AddResult::UNCHANGED;
            }
            self.scores.insert(member.clone(), new_score);
            self.tree.insert((new_score, member), ());
            AddResult {
                added: true,
                updated: false,
            }
        }
    }

    /// Removes a member from the sorted set. Returns `true` if it existed.
    pub fn remove(&mut self, member: &str) -> bool {
        if let Some(score) = self.scores.remove(member) {
            self.tree.remove(&(score, member.to_owned()));
            true
        } else {
            false
        }
    }

    /// Returns the score for a member, or `None` if not present.
    pub fn score(&self, member: &str) -> Option<f64> {
        self.scores.get(member).map(|s| s.0)
    }

    /// Returns the 0-based rank of a member (lowest score = rank 0).
    /// Returns `None` if the member is not present.
    ///
    /// O(n) — walks the BTreeMap up to the target entry. Acceptable for
    /// small-to-medium sets; a skip list with rank counts would give
    /// O(log n) if this becomes a bottleneck.
    pub fn rank(&self, member: &str) -> Option<usize> {
        let score = self.scores.get(member)?;
        let key = (*score, member.to_owned());
        // count entries before this one
        Some(self.tree.range(..&key).count())
    }

    /// Returns members in the given rank range, inclusive on both ends.
    /// Supports negative indices: -1 = last, -2 = second to last, etc.
    pub fn range_by_rank(&self, start: i64, stop: i64) -> Vec<(&str, f64)> {
        let len = self.tree.len() as i64;
        let (s, e) = super::normalize_range(start, stop, len);
        if s > e {
            return Vec::new();
        }

        let s = s as usize;
        let e = e as usize;

        self.tree
            .keys()
            .skip(s)
            .take(e - s + 1)
            .map(|(score, member)| (member.as_str(), score.0))
            .collect()
    }

    /// Returns the number of members.
    pub fn len(&self) -> usize {
        self.scores.len()
    }

    /// Returns `true` if the sorted set has no members.
    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    /// Returns an iterator over (member, score) pairs in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, f64)> {
        self.tree
            .keys()
            .map(|(score, member)| (member.as_str(), score.0))
    }

    /// Estimates memory usage in bytes.
    ///
    /// This is an approximation: BTreeMap and HashMap have internal
    /// overhead that varies, but we account for the per-entry costs
    /// and the string data.
    pub fn memory_usage(&self) -> usize {
        let per_entry: usize = self
            .scores
            .keys()
            .map(|k| Self::estimated_member_cost(k))
            .sum();

        Self::BASE_OVERHEAD + per_entry
    }

    /// Base overhead of an empty sorted set (BTreeMap + HashMap shells).
    pub const BASE_OVERHEAD: usize = 24 + 48; // BTREE_BASE + HASHMAP_BASE

    /// Estimates the memory cost of storing a single member.
    ///
    /// Includes BTreeMap entry overhead (64), HashMap entry overhead (56),
    /// the member string stored in both collections, and the OrderedFloat.
    pub fn estimated_member_cost(member: &str) -> usize {
        const BTREE_ENTRY: usize = 64;
        const HASHMAP_ENTRY: usize = 56;
        BTREE_ENTRY + HASHMAP_ENTRY + member.len() * 2 + 8
    }
}

impl Default for SortedSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Converts a possibly-negative index to a non-negative index.
/// Negative indices count back from `len` (-1 = len-1).
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_score() {
        let mut ss = SortedSet::new();
        let result = ss.add("alice".into(), 100.0);
        assert!(result.added);
        assert!(!result.updated);
        assert_eq!(ss.score("alice"), Some(100.0));
        assert_eq!(ss.len(), 1);
    }

    #[test]
    fn update_existing_score() {
        let mut ss = SortedSet::new();
        ss.add("alice".into(), 100.0);
        let result = ss.add("alice".into(), 200.0);
        assert!(!result.added);
        assert!(result.updated);
        assert_eq!(ss.score("alice"), Some(200.0));
        assert_eq!(ss.len(), 1);
    }

    #[test]
    fn same_score_no_update() {
        let mut ss = SortedSet::new();
        ss.add("alice".into(), 100.0);
        let result = ss.add("alice".into(), 100.0);
        assert!(!result.added);
        assert!(!result.updated);
    }

    #[test]
    fn remove_existing() {
        let mut ss = SortedSet::new();
        ss.add("alice".into(), 100.0);
        assert!(ss.remove("alice"));
        assert!(ss.is_empty());
        assert_eq!(ss.score("alice"), None);
    }

    #[test]
    fn remove_nonexistent() {
        let mut ss = SortedSet::new();
        assert!(!ss.remove("nobody"));
    }

    #[test]
    fn rank_ordering() {
        let mut ss = SortedSet::new();
        ss.add("c".into(), 300.0);
        ss.add("a".into(), 100.0);
        ss.add("b".into(), 200.0);

        assert_eq!(ss.rank("a"), Some(0));
        assert_eq!(ss.rank("b"), Some(1));
        assert_eq!(ss.rank("c"), Some(2));
        assert_eq!(ss.rank("d"), None);
    }

    #[test]
    fn equal_scores_lexicographic_order() {
        let mut ss = SortedSet::new();
        ss.add("charlie".into(), 100.0);
        ss.add("alice".into(), 100.0);
        ss.add("bob".into(), 100.0);

        // same score: should be alphabetical
        assert_eq!(ss.rank("alice"), Some(0));
        assert_eq!(ss.rank("bob"), Some(1));
        assert_eq!(ss.rank("charlie"), Some(2));
    }

    #[test]
    fn range_by_rank_basic() {
        let mut ss = SortedSet::new();
        ss.add("a".into(), 10.0);
        ss.add("b".into(), 20.0);
        ss.add("c".into(), 30.0);

        let result = ss.range_by_rank(0, -1);
        assert_eq!(result, vec![("a", 10.0), ("b", 20.0), ("c", 30.0)]);

        let result = ss.range_by_rank(1, 1);
        assert_eq!(result, vec![("b", 20.0)]);

        let result = ss.range_by_rank(-2, -1);
        assert_eq!(result, vec![("b", 20.0), ("c", 30.0)]);
    }

    #[test]
    fn range_by_rank_out_of_bounds() {
        let mut ss = SortedSet::new();
        ss.add("a".into(), 10.0);

        // start > stop
        assert!(ss.range_by_rank(2, 1).is_empty());
        // start beyond length
        assert!(ss.range_by_rank(5, 10).is_empty());
        // empty set
        let empty = SortedSet::new();
        assert!(empty.range_by_rank(0, -1).is_empty());
    }

    #[test]
    fn nx_flag_skips_existing() {
        let mut ss = SortedSet::new();
        ss.add("alice".into(), 100.0);

        let flags = ZAddFlags {
            nx: true,
            ..Default::default()
        };
        let result = ss.add_with_flags("alice".into(), 999.0, &flags);
        assert!(!result.added);
        assert!(!result.updated);
        assert_eq!(ss.score("alice"), Some(100.0));

        // but adding a new member works
        let result = ss.add_with_flags("bob".into(), 50.0, &flags);
        assert!(result.added);
    }

    #[test]
    fn xx_flag_skips_new() {
        let mut ss = SortedSet::new();
        let flags = ZAddFlags {
            xx: true,
            ..Default::default()
        };

        let result = ss.add_with_flags("alice".into(), 100.0, &flags);
        assert!(!result.added);
        assert!(ss.is_empty());

        // but updating an existing member works
        ss.add("bob".into(), 50.0);
        let result = ss.add_with_flags("bob".into(), 75.0, &flags);
        assert!(result.updated);
        assert_eq!(ss.score("bob"), Some(75.0));
    }

    #[test]
    fn gt_flag_only_increases() {
        let mut ss = SortedSet::new();
        ss.add("alice".into(), 100.0);

        let flags = ZAddFlags {
            gt: true,
            ..Default::default()
        };

        // lower score — skip
        let result = ss.add_with_flags("alice".into(), 50.0, &flags);
        assert!(!result.updated);
        assert_eq!(ss.score("alice"), Some(100.0));

        // higher score — update
        let result = ss.add_with_flags("alice".into(), 200.0, &flags);
        assert!(result.updated);
        assert_eq!(ss.score("alice"), Some(200.0));
    }

    #[test]
    fn lt_flag_only_decreases() {
        let mut ss = SortedSet::new();
        ss.add("alice".into(), 100.0);

        let flags = ZAddFlags {
            lt: true,
            ..Default::default()
        };

        // higher score — skip
        let result = ss.add_with_flags("alice".into(), 200.0, &flags);
        assert!(!result.updated);
        assert_eq!(ss.score("alice"), Some(100.0));

        // lower score — update
        let result = ss.add_with_flags("alice".into(), 50.0, &flags);
        assert!(result.updated);
        assert_eq!(ss.score("alice"), Some(50.0));
    }

    #[test]
    fn memory_usage_grows_with_members() {
        let mut ss = SortedSet::new();
        let base = ss.memory_usage();
        ss.add("alice".into(), 100.0);
        let with_one = ss.memory_usage();
        assert!(with_one > base);
        ss.add("bob".into(), 200.0);
        assert!(ss.memory_usage() > with_one);
    }

    #[test]
    fn iter_sorted_order() {
        let mut ss = SortedSet::new();
        ss.add("c".into(), 3.0);
        ss.add("a".into(), 1.0);
        ss.add("b".into(), 2.0);

        let items: Vec<_> = ss.iter().collect();
        assert_eq!(items, vec![("a", 1.0), ("b", 2.0), ("c", 3.0)]);
    }

    #[test]
    fn update_score_changes_rank() {
        let mut ss = SortedSet::new();
        ss.add("a".into(), 10.0);
        ss.add("b".into(), 20.0);
        ss.add("c".into(), 30.0);

        assert_eq!(ss.rank("a"), Some(0));

        // move "a" to the top
        ss.add("a".into(), 50.0);
        assert_eq!(ss.rank("a"), Some(2));
        assert_eq!(ss.rank("b"), Some(0));
    }

    #[test]
    fn positive_infinity_score() {
        let mut ss = SortedSet::new();
        ss.add("normal".into(), 100.0);
        ss.add("infinite".into(), f64::INFINITY);
        ss.add("large".into(), 1e308);

        // infinity should sort after everything
        assert_eq!(ss.rank("infinite"), Some(2));
        assert_eq!(ss.rank("large"), Some(1));
        assert_eq!(ss.rank("normal"), Some(0));
    }

    #[test]
    fn negative_infinity_score() {
        let mut ss = SortedSet::new();
        ss.add("normal".into(), 100.0);
        ss.add("neg_inf".into(), f64::NEG_INFINITY);
        ss.add("small".into(), -1e308);

        // negative infinity should sort before everything
        assert_eq!(ss.rank("neg_inf"), Some(0));
        assert_eq!(ss.rank("small"), Some(1));
        assert_eq!(ss.rank("normal"), Some(2));
    }

    #[test]
    fn zero_score() {
        let mut ss = SortedSet::new();
        ss.add("positive".into(), 1.0);
        ss.add("zero".into(), 0.0);
        ss.add("negative".into(), -1.0);

        assert_eq!(ss.rank("negative"), Some(0));
        assert_eq!(ss.rank("zero"), Some(1));
        assert_eq!(ss.rank("positive"), Some(2));
    }

    #[test]
    fn range_by_rank_on_empty_set() {
        let ss = SortedSet::new();
        assert!(ss.range_by_rank(0, -1).is_empty());
        assert!(ss.range_by_rank(0, 100).is_empty());
    }

    #[test]
    fn range_by_rank_inverted_indices() {
        let mut ss = SortedSet::new();
        ss.add("a".into(), 1.0);
        ss.add("b".into(), 2.0);
        ss.add("c".into(), 3.0);

        // start > stop with positive indices should return empty
        let result = ss.range_by_rank(2, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn remove_all_members_leaves_empty() {
        let mut ss = SortedSet::new();
        ss.add("a".into(), 1.0);
        ss.add("b".into(), 2.0);

        ss.remove("a");
        ss.remove("b");

        assert_eq!(ss.len(), 0);
        assert!(ss.range_by_rank(0, -1).is_empty());
    }
}
