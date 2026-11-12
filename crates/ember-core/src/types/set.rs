//! The set type.
//!
//! Members live in an `IndexSet`, so a random member is one index away
//! (SPOP, SRANDMEMBER) and SSCAN can resume from a position. The set keeps
//! a running count of member bytes, so its memory estimate is O(1).

use std::ops::Deref;

use indexmap::IndexSet;

use crate::memory::{HASHSET_BASE_OVERHEAD, HASHSET_MEMBER_OVERHEAD};

type Members = IndexSet<String, ahash::RandomState>;

/// A set of string members. Reads go through `Deref` to the `IndexSet`;
/// writes go through the methods here so the byte count stays right.
#[derive(Debug, Clone, Default)]
pub struct SetValue {
    members: Members,
    /// Total length of all members.
    data_bytes: usize,
}

impl SetValue {
    /// Adds `member`. Returns `true` if it was not already present.
    pub fn insert(&mut self, member: String) -> bool {
        let len = member.len();
        let added = self.members.insert(member);
        if added {
            self.data_bytes += len;
        }
        added
    }

    /// Removes `member`. Returns `true` if it was present. The last member
    /// takes the removed member's position.
    pub fn remove(&mut self, member: &str) -> bool {
        let removed = self.members.swap_remove(member);
        if removed {
            self.data_bytes -= member.len();
        }
        removed
    }

    /// Removes and returns the member at `index`. The last member takes
    /// its position.
    pub fn swap_remove_index(&mut self, index: usize) -> Option<String> {
        let member = self.members.swap_remove_index(index)?;
        self.data_bytes -= member.len();
        Some(member)
    }

    /// Estimated memory used by the set, excluding the keyspace entry.
    pub fn memory_usage(&self) -> usize {
        HASHSET_BASE_OVERHEAD + self.members.len() * HASHSET_MEMBER_OVERHEAD + self.data_bytes
    }
}

impl Deref for SetValue {
    type Target = Members;

    fn deref(&self) -> &Members {
        &self.members
    }
}

impl PartialEq for SetValue {
    /// Sets are equal when they hold the same members, in any order.
    fn eq(&self, other: &Self) -> bool {
        self.members.len() == other.members.len()
            && self.members.iter().all(|m| other.members.contains(m))
    }
}

impl FromIterator<String> for SetValue {
    fn from_iter<I: IntoIterator<Item = String>>(iter: I) -> Self {
        let mut set = SetValue::default();
        for member in iter {
            set.insert(member);
        }
        set
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_count_follows_inserts_and_removes() {
        let mut set = SetValue::default();
        let empty = set.memory_usage();
        assert!(set.insert("abc".into()));
        assert!(!set.insert("abc".into()));
        assert!(set.insert("de".into()));
        assert_eq!(set.memory_usage(), empty + 2 * HASHSET_MEMBER_OVERHEAD + 5);
        assert!(set.remove("abc"));
        assert!(!set.remove("abc"));
        assert_eq!(set.swap_remove_index(0).as_deref(), Some("de"));
        assert_eq!(set.memory_usage(), empty);
    }

    #[test]
    fn equality_ignores_order() {
        let a: SetValue = ["x", "y"].map(String::from).into_iter().collect();
        let b: SetValue = ["y", "x"].map(String::from).into_iter().collect();
        let c: SetValue = ["x"].map(String::from).into_iter().collect();
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
