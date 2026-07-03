//! Hash slot management for Redis Cluster-compatible key distribution.
//!
//! Implements CRC16 hashing (XMODEM polynomial) and 16384-slot mapping
//! following the Redis Cluster specification.
//!
//! The slot-hashing primitives (`key_slot`, `SLOT_COUNT`) live in
//! `ember_protocol::slots` so the core engine can use them without
//! depending on this crate; they are re-exported here for compatibility.

pub use ember_protocol::slots::{key_slot, SLOT_COUNT};

use crate::NodeId;

/// A contiguous range of slots assigned to a node.
///
/// # Invariants
///
/// A valid `SlotRange` always satisfies `start <= end`, meaning it contains
/// at least one slot. This is enforced by debug assertions in the constructor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16, // inclusive
}

impl SlotRange {
    /// Creates a new slot range (end is inclusive).
    ///
    /// # Panics
    ///
    /// Panics if `start > end` or if `end >= SLOT_COUNT`.
    pub fn new(start: u16, end: u16) -> Self {
        assert!(start <= end, "SlotRange requires start <= end");
        assert!(end < SLOT_COUNT, "slot must be < {SLOT_COUNT}");
        Self { start, end }
    }

    /// Creates a new slot range with runtime validation.
    ///
    /// Returns an error if `start > end` or `end >= SLOT_COUNT`.
    /// Use this for untrusted input (e.g. network-decoded data).
    pub fn try_new(start: u16, end: u16) -> Result<Self, std::io::Error> {
        if start > end {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("SlotRange requires start <= end, got {start}..{end}"),
            ));
        }
        if end >= SLOT_COUNT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("slot {end} out of range (max {})", SLOT_COUNT - 1),
            ));
        }
        Ok(Self { start, end })
    }

    /// Creates a range containing a single slot.
    pub fn single(slot: u16) -> Self {
        Self::new(slot, slot)
    }

    /// Returns the number of slots in this range (always >= 1 for valid ranges).
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u16 {
        self.end - self.start + 1
    }

    /// Returns true if this range contains the given slot.
    pub fn contains(&self, slot: u16) -> bool {
        slot >= self.start && slot <= self.end
    }

    /// Returns an iterator over all slots in this range.
    pub fn iter(&self) -> impl Iterator<Item = u16> {
        self.start..=self.end
    }
}

impl std::fmt::Display for SlotRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.start == self.end {
            write!(f, "{}", self.start)
        } else {
            write!(f, "{}-{}", self.start, self.end)
        }
    }
}

/// Maps each of the 16384 slots to a node ID.
///
/// When a slot is `None`, it means the slot is not assigned to any node,
/// which indicates an incomplete cluster configuration.
#[derive(Debug, Clone)]
pub struct SlotMap {
    slots: Box<[Option<NodeId>; SLOT_COUNT as usize]>,
}

impl Default for SlotMap {
    fn default() -> Self {
        Self::new()
    }
}

impl SlotMap {
    /// Creates an empty slot map with no assignments.
    pub fn new() -> Self {
        Self {
            // Box it to avoid 128KB stack allocation
            slots: Box::new([None; SLOT_COUNT as usize]),
        }
    }

    /// Creates a slot map with all slots assigned to a single node.
    ///
    /// Useful for single-node clusters.
    pub fn single_node(node: NodeId) -> Self {
        let mut map = Self::new();
        for slot in map.slots.iter_mut() {
            *slot = Some(node);
        }
        map
    }

    /// Returns the node that owns the given slot, if assigned.
    pub fn owner(&self, slot: u16) -> Option<NodeId> {
        self.slots.get(slot as usize).copied().flatten()
    }

    /// Assigns a slot to a node.
    pub fn assign(&mut self, slot: u16, node: NodeId) {
        if let Some(entry) = self.slots.get_mut(slot as usize) {
            *entry = Some(node);
        }
    }

    /// Assigns a range of slots to a node.
    pub fn assign_range(&mut self, range: SlotRange, node: NodeId) {
        for slot in range.iter() {
            self.assign(slot, node);
        }
    }

    /// Clears the assignment for a slot.
    pub fn unassign(&mut self, slot: u16) {
        if let Some(entry) = self.slots.get_mut(slot as usize) {
            *entry = None;
        }
    }

    /// Returns true if all slots are assigned to some node.
    pub fn is_complete(&self) -> bool {
        self.slots.iter().all(|s| s.is_some())
    }

    /// Returns the number of unassigned slots.
    pub fn unassigned_count(&self) -> usize {
        self.slots.iter().filter(|s| s.is_none()).count()
    }

    /// Returns all slots owned by a specific node as a list of ranges.
    ///
    /// Consecutive slots are merged into ranges for compact representation.
    pub fn slots_for_node(&self, node: NodeId) -> Vec<SlotRange> {
        let mut ranges = Vec::new();
        let mut range_start: Option<u16> = None;
        let mut prev_slot: Option<u16> = None;

        for (slot_idx, owner) in self.slots.iter().enumerate() {
            let slot = slot_idx as u16;
            let owned = *owner == Some(node);

            match (owned, range_start) {
                (true, None) => {
                    // Start a new range
                    range_start = Some(slot);
                    prev_slot = Some(slot);
                }
                (true, Some(_)) => {
                    // Continue the current range
                    prev_slot = Some(slot);
                }
                (false, Some(start)) => {
                    // End the current range
                    if let Some(end) = prev_slot {
                        ranges.push(SlotRange::new(start, end));
                    }
                    range_start = None;
                    prev_slot = None;
                }
                (false, None) => {
                    // Not in a range, not owned by this node
                }
            }
        }

        // Close any open range at the end
        if let (Some(start), Some(end)) = (range_start, prev_slot) {
            ranges.push(SlotRange::new(start, end));
        }

        ranges
    }

    /// Returns a count of slots per node.
    pub fn slot_counts(&self) -> std::collections::HashMap<NodeId, usize> {
        let mut counts = std::collections::HashMap::new();
        for owner in self.slots.iter().flatten() {
            *counts.entry(*owner).or_insert(0) += 1;
        }
        counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn slot_range_basics() {
        let range = SlotRange::new(0, 5460);
        assert_eq!(range.len(), 5461);
        assert!(range.contains(0));
        assert!(range.contains(5460));
        assert!(!range.contains(5461));

        let single = SlotRange::single(100);
        assert_eq!(single.len(), 1);
        assert!(single.contains(100));
        assert!(!single.contains(99));
        assert!(!single.contains(101));
    }

    #[test]
    fn slot_range_display() {
        assert_eq!(SlotRange::new(0, 5460).to_string(), "0-5460");
        assert_eq!(SlotRange::single(100).to_string(), "100");
    }

    #[test]
    fn slot_map_single_node() {
        let node = NodeId(Uuid::new_v4());
        let map = SlotMap::single_node(node);

        assert!(map.is_complete());
        assert_eq!(map.unassigned_count(), 0);
        assert_eq!(map.owner(0), Some(node));
        assert_eq!(map.owner(SLOT_COUNT - 1), Some(node));

        let ranges = map.slots_for_node(node);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], SlotRange::new(0, SLOT_COUNT - 1));
    }

    #[test]
    fn slot_map_multi_node() {
        let node1 = NodeId(Uuid::new_v4());
        let node2 = NodeId(Uuid::new_v4());
        let node3 = NodeId(Uuid::new_v4());

        let mut map = SlotMap::new();
        assert!(!map.is_complete());
        assert_eq!(map.unassigned_count(), SLOT_COUNT as usize);

        // Assign slots evenly: 0-5460 to node1, 5461-10922 to node2, 10923-16383 to node3
        map.assign_range(SlotRange::new(0, 5460), node1);
        map.assign_range(SlotRange::new(5461, 10922), node2);
        map.assign_range(SlotRange::new(10923, 16383), node3);

        assert!(map.is_complete());

        assert_eq!(map.owner(0), Some(node1));
        assert_eq!(map.owner(5460), Some(node1));
        assert_eq!(map.owner(5461), Some(node2));
        assert_eq!(map.owner(10922), Some(node2));
        assert_eq!(map.owner(10923), Some(node3));
        assert_eq!(map.owner(16383), Some(node3));

        let counts = map.slot_counts();
        assert_eq!(counts.get(&node1), Some(&5461));
        assert_eq!(counts.get(&node2), Some(&5462));
        assert_eq!(counts.get(&node3), Some(&5461));
    }

    #[test]
    fn slot_map_unassign() {
        let node = NodeId(Uuid::new_v4());
        let mut map = SlotMap::single_node(node);

        map.unassign(100);
        assert_eq!(map.owner(100), None);
        assert!(!map.is_complete());
        assert_eq!(map.unassigned_count(), 1);
    }

    #[test]
    fn slot_range_try_new_validates() {
        assert!(SlotRange::try_new(0, 5460).is_ok());
        assert!(SlotRange::try_new(100, 100).is_ok());
        // start > end
        assert!(SlotRange::try_new(5000, 100).is_err());
        // end >= SLOT_COUNT
        assert!(SlotRange::try_new(0, 16384).is_err());
        assert!(SlotRange::try_new(0, u16::MAX).is_err());
    }

    #[test]
    fn slots_for_node_ranges() {
        let node = NodeId(Uuid::new_v4());
        let mut map = SlotMap::new();

        // Assign non-contiguous ranges
        map.assign_range(SlotRange::new(0, 10), node);
        map.assign_range(SlotRange::new(100, 110), node);
        map.assign_range(SlotRange::new(200, 200), node); // single slot

        let ranges = map.slots_for_node(node);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], SlotRange::new(0, 10));
        assert_eq!(ranges[1], SlotRange::new(100, 110));
        assert_eq!(ranges[2], SlotRange::new(200, 200));
    }
}
