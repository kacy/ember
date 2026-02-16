//! Hash slot management for Redis Cluster-compatible key distribution.
//!
//! Implements CRC16 hashing (XMODEM polynomial) and 16384-slot mapping
//! following the Redis Cluster specification.

use crate::NodeId;

/// Total number of hash slots in the cluster (Redis Cluster standard).
pub const SLOT_COUNT: u16 = 16384;

/// CRC16 lookup table from Redis source code (crc16.c).
/// Uses CCITT polynomial for Redis Cluster slot calculation.
#[rustfmt::skip]
static CRC16_TABLE: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

/// Computes CRC16 checksum using XMODEM polynomial.
///
/// This is the same algorithm Redis uses for slot calculation.
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        let idx = ((crc >> 8) ^ (byte as u16)) as usize;
        crc = (crc << 8) ^ CRC16_TABLE[idx];
    }
    crc
}

/// Extracts the hashable portion of a key, handling hash tags.
///
/// Hash tags allow multiple keys to be assigned to the same slot.
/// The tag is the content between the first `{` and the first `}` after it.
///
/// Examples:
/// - `user:{123}:profile` → hashes `123`
/// - `{user}:123` → hashes `user`
/// - `foo{}{bar}` → hashes `foo{}{bar}` (empty tag, no match)
/// - `foo{bar` → hashes `foo{bar` (no closing brace)
/// - `foobar` → hashes `foobar` (no tag)
fn extract_hash_tag(key: &[u8]) -> &[u8] {
    // Find first '{'
    let Some(open) = key.iter().position(|&b| b == b'{') else {
        return key;
    };

    // Find first '}' after the '{'
    let after_open = &key[open + 1..];
    let Some(close) = after_open.iter().position(|&b| b == b'}') else {
        return key;
    };

    // Empty tag (e.g., "foo{}bar") means use the whole key
    if close == 0 {
        return key;
    }

    &after_open[..close]
}

/// Computes the hash slot for a key.
///
/// Returns a value in the range [0, 16383].
pub fn key_slot(key: &[u8]) -> u16 {
    let hash_input = extract_hash_tag(key);
    crc16(hash_input) % SLOT_COUNT
}

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

    // Test vectors verified against Redis CLUSTER KEYSLOT command
    #[test]
    fn crc16_matches_redis() {
        // These are known Redis slot assignments
        assert_eq!(key_slot(b""), 0);
        assert_eq!(key_slot(b"foo"), 12182);
        assert_eq!(key_slot(b"bar"), 5061);
        assert_eq!(key_slot(b"hello"), 866);
        // CRC16 CCITT/XMODEM of "123456789" is 0x31C3 = 12739
        assert_eq!(key_slot(b"123456789"), 12739);
    }

    #[test]
    fn hash_tag_extraction() {
        // Basic hash tag
        assert_eq!(key_slot(b"user:{123}:profile"), key_slot(b"123"));
        assert_eq!(key_slot(b"order:{123}:items"), key_slot(b"123"));

        // Tag at start
        assert_eq!(key_slot(b"{user}:123"), key_slot(b"user"));

        // Empty tag uses whole key
        assert_eq!(key_slot(b"foo{}bar"), key_slot(b"foo{}bar"));

        // No closing brace uses whole key
        assert_eq!(key_slot(b"foo{bar"), key_slot(b"foo{bar"));

        // Only first tag matters
        assert_eq!(key_slot(b"{a}{b}"), key_slot(b"a"));
    }

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
