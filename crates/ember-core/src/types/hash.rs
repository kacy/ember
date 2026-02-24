//! Dual-representation hash value for memory-efficient storage.
//!
//! Small hashes (≤32 fields) use a packed byte buffer for minimal per-field
//! overhead (6 bytes vs 48 bytes for the old Vec<(CompactString, Bytes)>
//! approach). Larger hashes auto-promote to a HashMap for O(1) field access.
//!
//! ## Packed format
//!
//! ```text
//! [num_fields: u16][{name_len: u16, name: bytes, val_len: u32, val: bytes}...]
//! ```
//!
//! Per-field overhead is just 6 bytes (2B name_len + 4B value_len). The buffer
//! is a contiguous `Vec<u8>` with excellent cache locality for small hashes.
//! All reads use checked accessors — a truncated or corrupt buffer causes
//! iteration to stop gracefully rather than panic.

use bytes::Bytes;
use compact_str::CompactString;
use std::collections::HashMap;

/// Hashes with more fields than this are stored as a HashMap.
/// 32 is chosen to balance linear-scan cost against HashMap overhead
/// (48 bytes base + 16 bytes/bucket).
const COMPACT_THRESHOLD: usize = 32;

/// Size of the field count header in the packed buffer.
const HEADER_SIZE: usize = 2;

/// A hash value stored as either a packed byte buffer or a full hashmap.
///
/// Most Redis-style hashes have fewer than 32 fields (user profiles,
/// session data, config objects). For these, a packed buffer is both smaller
/// and faster than a HashMap due to cache locality.
#[derive(Debug, Clone)]
pub enum HashValue {
    /// Packed byte buffer for small hashes. Each lookup is O(n) but
    /// n ≤ 32 and the entries are contiguous in memory with only 6 bytes
    /// of framing per field.
    Packed(Vec<u8>),
    /// Hash-indexed storage for large hashes. Promoted automatically
    /// when field count exceeds the threshold.
    Full(HashMap<CompactString, Bytes>),
}

impl Default for HashValue {
    fn default() -> Self {
        HashValue::Packed(vec![0, 0])
    }
}

/// Reads a u16 from the buffer at the given offset, returning None if
/// there aren't enough bytes.
fn read_u16(buf: &[u8], offset: usize) -> Option<u16> {
    let end = offset.checked_add(2)?;
    let slice = buf.get(offset..end)?;
    Some(u16::from_le_bytes([slice[0], slice[1]]))
}

/// Reads a u32 from the buffer at the given offset, returning None if
/// there aren't enough bytes.
fn read_u32(buf: &[u8], offset: usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let slice = buf.get(offset..end)?;
    Some(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

/// Writes a u16 in little-endian at the given offset. Caller must ensure
/// the buffer is large enough.
fn write_u16(buf: &mut [u8], offset: usize, val: u16) {
    let bytes = val.to_le_bytes();
    buf[offset] = bytes[0];
    buf[offset + 1] = bytes[1];
}

impl HashValue {
    /// Inserts a field-value pair. Returns the old value if the field existed.
    ///
    /// Auto-promotes from Packed to Full when the field count exceeds
    /// the threshold after insertion.
    pub fn insert(&mut self, field: CompactString, value: Bytes) -> Option<Bytes> {
        let result = match self {
            HashValue::Packed(buf) => {
                // scan for existing field
                let field_str = field.as_str();
                let mut iter = PackedIter::new(buf);
                while let Some((offset, name, val)) = iter.next_with_offset() {
                    if name == field_str {
                        // found — rebuild buffer with the updated value
                        let old = Bytes::copy_from_slice(val);
                        rebuild_with_update(buf, offset, iter.offset, field_str, &value);
                        return Some(old);
                    }
                }
                // new field — append in place
                let name_bytes = field_str.as_bytes();
                let name_len = name_bytes.len() as u16;
                let val_len = value.len() as u32;
                buf.extend_from_slice(&name_len.to_le_bytes());
                buf.extend_from_slice(name_bytes);
                buf.extend_from_slice(&val_len.to_le_bytes());
                buf.extend_from_slice(&value);
                // increment field count in header
                if let Some(count) = read_u16(buf, 0) {
                    if let Some(new_count) = count.checked_add(1) {
                        write_u16(buf, 0, new_count);
                    }
                }
                None
            }
            HashValue::Full(map) => return map.insert(field, value),
        };
        // check if packed representation exceeded threshold
        if let HashValue::Packed(buf) = self {
            if let Some(count) = read_u16(buf, 0) {
                if count as usize > COMPACT_THRESHOLD {
                    let map = drain_packed_to_map(buf);
                    *self = HashValue::Full(map);
                }
            }
        }
        result
    }

    /// Gets a field's value by name. Returns a slice into the packed buffer
    /// (zero-copy for Packed variant) or a slice of the Bytes (for Full).
    pub fn get(&self, field: &str) -> Option<&[u8]> {
        match self {
            HashValue::Packed(buf) => {
                for (name, val) in PackedIter::new(buf) {
                    if name == field {
                        return Some(val);
                    }
                }
                None
            }
            HashValue::Full(map) => map.get(field).map(|b| b.as_ref()),
        }
    }

    /// Removes a field, returning its value if it existed.
    pub fn remove(&mut self, field: &str) -> Option<Bytes> {
        match self {
            HashValue::Packed(buf) => {
                let mut iter = PackedIter::new(buf);
                while let Some((offset, name, val)) = iter.next_with_offset() {
                    if name == field {
                        let removed = Bytes::copy_from_slice(val);
                        let next_offset = iter.offset;
                        rebuild_without(buf, offset, next_offset);
                        // decrement field count
                        if let Some(count) = read_u16(buf, 0) {
                            write_u16(buf, 0, count.saturating_sub(1));
                        }
                        return Some(removed);
                    }
                }
                None
            }
            HashValue::Full(map) => map.remove(field),
        }
    }

    /// Checks if a field exists.
    pub fn contains_key(&self, field: &str) -> bool {
        match self {
            HashValue::Packed(buf) => PackedIter::new(buf).any(|(name, _)| name == field),
            HashValue::Full(map) => map.contains_key(field),
        }
    }

    /// Returns the number of fields.
    pub fn len(&self) -> usize {
        match self {
            HashValue::Packed(buf) => read_u16(buf, 0).unwrap_or(0) as usize,
            HashValue::Full(map) => map.len(),
        }
    }

    /// Returns true if the hash has no fields.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterates over all field-value pairs.
    pub fn iter(&self) -> HashIter<'_> {
        match self {
            HashValue::Packed(buf) => HashIter::Packed(PackedIter::new(buf)),
            HashValue::Full(map) => HashIter::Full(map.iter()),
        }
    }
}

impl PartialEq for HashValue {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        // every field in self must exist in other with the same value
        for (k, v) in self.iter() {
            match other.get(k) {
                Some(ov) if ov == v => {}
                _ => return false,
            }
        }
        true
    }
}

/// Constructs a HashValue from a HashMap, choosing packed or full
/// representation based on field count.
impl From<HashMap<String, Bytes>> for HashValue {
    fn from(map: HashMap<String, Bytes>) -> Self {
        if map.len() <= COMPACT_THRESHOLD {
            let mut buf = Vec::with_capacity(HEADER_SIZE + map.len() * 16);
            buf.extend_from_slice(&(map.len() as u16).to_le_bytes());
            for (k, v) in &map {
                let name_bytes = k.as_bytes();
                buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(name_bytes);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
            }
            HashValue::Packed(buf)
        } else {
            HashValue::Full(
                map.into_iter()
                    .map(|(k, v)| (CompactString::from(k), v))
                    .collect(),
            )
        }
    }
}

impl HashValue {
    /// Converts to a standard HashMap for serialization.
    pub fn to_hash_map(&self) -> HashMap<String, Bytes> {
        self.iter()
            .map(|(k, v)| (k.to_string(), Bytes::copy_from_slice(v)))
            .collect()
    }
}

/// Iterator over hash fields. For packed buffers this walks the contiguous
/// byte buffer; for full maps it wraps the HashMap iterator.
pub enum HashIter<'a> {
    Packed(PackedIter<'a>),
    Full(std::collections::hash_map::Iter<'a, CompactString, Bytes>),
}

impl<'a> Iterator for HashIter<'a> {
    type Item = (&'a str, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            HashIter::Packed(iter) => iter.next(),
            HashIter::Full(iter) => iter.next().map(|(k, v)| (k.as_str(), v.as_ref())),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            HashIter::Packed(iter) => iter.size_hint(),
            HashIter::Full(iter) => iter.size_hint(),
        }
    }
}

impl<'a> ExactSizeIterator for HashIter<'a> {}

/// Zero-copy iterator over the packed byte buffer.
///
/// All reads use checked accessors — if the buffer is truncated or corrupt,
/// iteration simply stops (returns `None`) rather than panicking.
pub struct PackedIter<'a> {
    buf: &'a [u8],
    offset: usize,
    remaining: usize,
}

impl<'a> PackedIter<'a> {
    fn new(buf: &'a [u8]) -> Self {
        let count = read_u16(buf, 0).unwrap_or(0) as usize;
        PackedIter {
            buf,
            offset: HEADER_SIZE,
            remaining: count,
        }
    }

    /// Advances to the next field, returning (entry_start_offset, name, value).
    /// The entry_start_offset is the position where this field's framing begins.
    fn next_with_offset(&mut self) -> Option<(usize, &'a str, &'a [u8])> {
        if self.remaining == 0 {
            return None;
        }
        let entry_start = self.offset;

        // read name_len (u16)
        let name_len = read_u16(self.buf, self.offset)? as usize;
        self.offset = self.offset.checked_add(2)?;

        // read name bytes
        let name_end = self.offset.checked_add(name_len)?;
        let name_bytes = self.buf.get(self.offset..name_end)?;
        let name = std::str::from_utf8(name_bytes).ok()?;
        self.offset = name_end;

        // read val_len (u32)
        let val_len = read_u32(self.buf, self.offset)? as usize;
        self.offset = self.offset.checked_add(4)?;

        // read value bytes
        let val_end = self.offset.checked_add(val_len)?;
        let val = self.buf.get(self.offset..val_end)?;
        self.offset = val_end;

        self.remaining -= 1;
        Some((entry_start, name, val))
    }
}

impl<'a> Iterator for PackedIter<'a> {
    type Item = (&'a str, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_with_offset().map(|(_, name, val)| (name, val))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<'a> ExactSizeIterator for PackedIter<'a> {}

/// Rebuilds the packed buffer with a single field updated in-place.
/// `field_start` is where the old field entry begins, `field_end` is
/// where the next field starts (or buf.len() if it was the last field).
fn rebuild_with_update(buf: &mut Vec<u8>, field_start: usize, field_end: usize, name: &str, new_val: &Bytes) {
    let name_bytes = name.as_bytes();
    let name_len = name_bytes.len() as u16;
    let val_len = new_val.len() as u32;

    // build the replacement entry
    let mut entry = Vec::with_capacity(2 + name_bytes.len() + 4 + new_val.len());
    entry.extend_from_slice(&name_len.to_le_bytes());
    entry.extend_from_slice(name_bytes);
    entry.extend_from_slice(&val_len.to_le_bytes());
    entry.extend_from_slice(new_val);

    // splice it in
    buf.splice(field_start..field_end, entry);
}

/// Rebuilds the packed buffer with a single field removed.
fn rebuild_without(buf: &mut Vec<u8>, field_start: usize, field_end: usize) {
    buf.drain(field_start..field_end);
}

/// Drains a packed buffer into a HashMap for promotion.
fn drain_packed_to_map(buf: &[u8]) -> HashMap<CompactString, Bytes> {
    let mut map = HashMap::new();
    for (name, val) in PackedIter::new(buf) {
        map.insert(CompactString::from(name), Bytes::copy_from_slice(val));
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_insert_and_get() {
        let mut h = HashValue::default();
        h.insert("name".into(), Bytes::from("alice"));
        assert_eq!(h.get("name"), Some(b"alice".as_slice()));
        assert!(matches!(h, HashValue::Packed(_)));
    }

    #[test]
    fn insert_returns_old_value() {
        let mut h = HashValue::default();
        assert!(h.insert("k".into(), Bytes::from("v1")).is_none());
        let old = h.insert("k".into(), Bytes::from("v2"));
        assert_eq!(old, Some(Bytes::from("v1")));
        assert_eq!(h.get("k"), Some(b"v2".as_slice()));
    }

    #[test]
    fn promotes_to_full_above_threshold() {
        let mut h = HashValue::default();
        for i in 0..=COMPACT_THRESHOLD {
            h.insert(format!("f{i}").into(), Bytes::from("v"));
        }
        assert!(matches!(h, HashValue::Full(_)));
        // all fields still accessible
        for i in 0..=COMPACT_THRESHOLD {
            assert!(h.contains_key(&format!("f{i}")));
        }
    }

    #[test]
    fn remove_returns_value() {
        let mut h = HashValue::default();
        h.insert("x".into(), Bytes::from("1"));
        let removed = h.remove("x");
        assert_eq!(removed, Some(Bytes::from("1")));
        assert!(h.is_empty());
    }

    #[test]
    fn equality() {
        let mut a = HashValue::default();
        a.insert("x".into(), Bytes::from("1"));
        a.insert("y".into(), Bytes::from("2"));

        let mut b = HashValue::default();
        b.insert("y".into(), Bytes::from("2"));
        b.insert("x".into(), Bytes::from("1"));

        assert_eq!(a, b);
    }

    #[test]
    fn from_hashmap_small() {
        let mut map = HashMap::new();
        map.insert("a".into(), Bytes::from("1"));
        let h = HashValue::from(map);
        assert!(matches!(h, HashValue::Packed(_)));
        assert_eq!(h.get("a"), Some(b"1".as_slice()));
    }

    #[test]
    fn from_hashmap_large() {
        let mut map = HashMap::new();
        for i in 0..=COMPACT_THRESHOLD {
            map.insert(format!("f{i}"), Bytes::from("v"));
        }
        let h = HashValue::from(map);
        assert!(matches!(h, HashValue::Full(_)));
    }

    #[test]
    fn iter_yields_all_fields() {
        let mut h = HashValue::default();
        h.insert("a".into(), Bytes::from("1"));
        h.insert("b".into(), Bytes::from("2"));
        let mut fields: Vec<_> = h.iter().map(|(k, _)| k.to_string()).collect();
        fields.sort();
        assert_eq!(fields, vec!["a", "b"]);
    }

    #[test]
    fn remove_middle_field() {
        let mut h = HashValue::default();
        h.insert("a".into(), Bytes::from("1"));
        h.insert("b".into(), Bytes::from("2"));
        h.insert("c".into(), Bytes::from("3"));
        h.remove("b");
        assert_eq!(h.len(), 2);
        assert_eq!(h.get("a"), Some(b"1".as_slice()));
        assert_eq!(h.get("b"), None);
        assert_eq!(h.get("c"), Some(b"3".as_slice()));
    }

    #[test]
    fn update_existing_field() {
        let mut h = HashValue::default();
        h.insert("k".into(), Bytes::from("short"));
        h.insert("k".into(), Bytes::from("a much longer value"));
        assert_eq!(h.get("k"), Some(b"a much longer value".as_slice()));
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn empty_buffer_is_valid() {
        let h = HashValue::default();
        assert!(h.is_empty());
        assert_eq!(h.len(), 0);
        assert_eq!(h.get("anything"), None);
        assert_eq!(h.iter().count(), 0);
    }

    #[test]
    fn to_hash_map_roundtrip() {
        let mut h = HashValue::default();
        h.insert("x".into(), Bytes::from("1"));
        h.insert("y".into(), Bytes::from("2"));
        let map = h.to_hash_map();
        let h2 = HashValue::from(map);
        assert_eq!(h, h2);
    }

    #[test]
    fn truncated_buffer_does_not_panic() {
        // a buffer with count=5 but no actual field data
        let h = HashValue::Packed(vec![5, 0]);
        assert_eq!(h.get("anything"), None);
        assert_eq!(h.iter().count(), 0);
    }
}
