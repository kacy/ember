//! Sorted set data structure: dual-indexed by score and member.
//!
//! Each member has a unique name and an associated `f64` score. Members
//! are ordered by (score, member) — ties in score are broken
//! lexicographically, matching Redis semantics.
//!
//! Implementation uses a sorted `Vec<(OrderedFloat<f64>, Arc<str>)>` for
//! O(log n) rank queries and fast iteration, plus a
//! `HashMap<Arc<str>, OrderedFloat<f64>>` for O(1) member→score lookups.
//! Member strings are shared via `Arc<str>` between both structures,
//! so each string is stored once on the heap.
//!
//! Compared to a BTreeMap-based design, the sorted Vec gives:
//! - O(log n) `rank()` via binary search (was O(n) with BTreeMap::range.count())
//! - Better cache locality for iteration (contiguous memory vs pointer-chasing)
//! - Lower memory per member (~24 bytes for a Vec slot vs ~64 for a BTreeMap node)
//!   The tradeoff is O(n) insert/remove due to Vec shifting, but for typical
//!   sorted set sizes that shifting is cache-friendly memmove and faster than
//!   BTreeMap's O(log n) with high constant factor.

use std::collections::HashMap;
use std::sync::Arc;

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
///
/// Member strings are shared between the score index and the sorted index
/// via `Arc<str>`, so each string is stored once on the heap.
#[derive(Debug, Clone)]
pub struct SortedSet {
    /// Score-ordered index for rank queries and iteration.
    /// Kept sorted by `(score, member_name)` at all times.
    sorted: Vec<(OrderedFloat<f64>, Arc<str>)>,
    /// Member→score index for O(1) lookups.
    scores: HashMap<Arc<str>, OrderedFloat<f64>>,
    /// Cached sum of member string lengths for O(1) `memory_usage()`.
    data_bytes: usize,
}

impl SortedSet {
    /// Creates an empty sorted set.
    pub fn new() -> Self {
        Self {
            sorted: Vec::new(),
            scores: HashMap::new(),
            data_bytes: 0,
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

        if let Some(&old_score) = self.scores.get(member.as_str()) {
            // member exists — skip if any flag condition prevents the update
            if flags.nx
                || (flags.gt && new_score <= old_score)
                || (flags.lt && new_score >= old_score)
                || new_score == old_score
            {
                return AddResult::UNCHANGED;
            }
            // reuse the existing Arc from scores to avoid a new heap allocation
            let name: Arc<str> = self
                .scores
                .get_key_value(member.as_str())
                .unwrap()
                .0
                .clone();
            // remove old position in sorted vec
            let old_idx = self.search_idx(old_score, &name).unwrap();
            self.sorted.remove(old_idx);
            // update score and insert at new sorted position
            self.scores.insert(name.clone(), new_score);
            let new_idx = self.search_idx(new_score, &name).unwrap_err();
            self.sorted.insert(new_idx, (new_score, name));
            // data_bytes unchanged — member string stays the same
            AddResult {
                added: false,
                updated: true,
            }
        } else {
            // new member — XX means only update existing, so skip
            if flags.xx {
                return AddResult::UNCHANGED;
            }
            let name: Arc<str> = Arc::from(member.as_str());
            self.data_bytes += member.len();
            self.scores.insert(name.clone(), new_score);
            let idx = self.search_idx(new_score, &name).unwrap_err();
            self.sorted.insert(idx, (new_score, name));
            AddResult {
                added: true,
                updated: false,
            }
        }
    }

    /// Removes a member from the sorted set. Returns `true` if it existed.
    pub fn remove(&mut self, member: &str) -> bool {
        if let Some((name, score)) = self.scores.remove_entry(member) {
            let idx = self.search_idx(score, &name).unwrap();
            self.sorted.remove(idx);
            self.data_bytes -= name.len();
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
    /// O(log n) — binary search over the sorted Vec.
    pub fn rank(&self, member: &str) -> Option<usize> {
        let (name, &score) = self.scores.get_key_value(member)?;
        Some(self.search_idx(score, name).unwrap())
    }

    /// Returns members in the given rank range, inclusive on both ends.
    /// Supports negative indices: -1 = last, -2 = second to last, etc.
    pub fn range_by_rank(&self, start: i64, stop: i64) -> Vec<(&str, f64)> {
        let len = self.sorted.len() as i64;
        let (s, e) = super::normalize_range(start, stop, len);
        if s > e {
            return Vec::new();
        }
        let s = s as usize;
        let e = e as usize;

        self.sorted[s..=e]
            .iter()
            .map(|(score, member)| (&**member, score.0))
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
        self.sorted
            .iter()
            .map(|(score, member)| (&**member, score.0))
    }

    /// Estimates memory usage in bytes.
    ///
    /// O(1) — uses the cached `data_bytes` sum plus a fixed overhead per member.
    pub fn memory_usage(&self) -> usize {
        Self::BASE_OVERHEAD + self.scores.len() * Self::MEMBER_FIXED_OVERHEAD + self.data_bytes
    }

    /// Base overhead of an empty sorted set (Vec shell + HashMap shell + usize).
    pub const BASE_OVERHEAD: usize = 24 + 48 + 8; // VEC_BASE + HASHMAP_BASE + data_bytes field

    /// Fixed per-member overhead, excluding the variable-length string data.
    ///
    /// Accounts for:
    /// - Vec entry: `(OrderedFloat<f64>, Arc<str>)` = 8 + 16 = 24 bytes
    /// - HashMap entry: Arc<str> key + OrderedFloat value + bucket overhead = 56 bytes
    /// - Arc heap header: 16 bytes (strong + weak counts, stored once)
    /// - Second Arc pointer: 8 bytes (Arc<str> clone shared between sorted and scores)
    const MEMBER_FIXED_OVERHEAD: usize = 24 + 56 + 16 + 8;

    /// Estimates the memory cost of storing a single member.
    ///
    /// Includes fixed structural overhead plus the variable string length.
    /// Used for worst-case memory reservation during ZADD.
    pub fn estimated_member_cost(member: &str) -> usize {
        Self::MEMBER_FIXED_OVERHEAD + member.len()
    }

    /// Finds the position of `(score, name)` in the sorted Vec.
    ///
    /// Returns `Ok(idx)` if found, `Err(insertion_point)` if not found.
    /// The sort key is `(score, member_name)` — same ordering as the set itself.
    fn search_idx(&self, score: OrderedFloat<f64>, name: &Arc<str>) -> Result<usize, usize> {
        self.sorted
            .binary_search_by(|(s, m)| s.cmp(&score).then_with(|| (**m).cmp(&**name)))
    }
}

impl Default for SortedSet {
    fn default() -> Self {
        Self::new()
    }
}

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
    fn memory_usage_shrinks_on_remove() {
        let mut ss = SortedSet::new();
        ss.add("alice".into(), 100.0);
        ss.add("bob".into(), 200.0);
        let before = ss.memory_usage();
        ss.remove("alice");
        assert!(ss.memory_usage() < before);
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

    #[test]
    fn data_bytes_stays_consistent() {
        let mut ss = SortedSet::new();
        assert_eq!(ss.data_bytes, 0);

        ss.add("hello".into(), 1.0); // len = 5
        assert_eq!(ss.data_bytes, 5);

        ss.add("world".into(), 2.0); // len = 5
        assert_eq!(ss.data_bytes, 10);

        // update doesn't change data_bytes
        ss.add("hello".into(), 99.0);
        assert_eq!(ss.data_bytes, 10);

        ss.remove("hello");
        assert_eq!(ss.data_bytes, 5);

        ss.remove("world");
        assert_eq!(ss.data_bytes, 0);
    }

    #[test]
    fn rank_is_o_log_n() {
        // verify rank is correct for a larger set (regression guard for binary search)
        let mut ss = SortedSet::new();
        for i in 0..100 {
            ss.add(format!("member:{i:03}"), i as f64);
        }
        for i in 0..100 {
            assert_eq!(ss.rank(&format!("member:{i:03}")), Some(i));
        }
    }
}
