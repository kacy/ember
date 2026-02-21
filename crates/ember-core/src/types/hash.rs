//! Dual-representation hash value for memory-efficient storage.
//!
//! Small hashes (≤32 fields) use a Vec for compact storage and
//! cache-friendly linear scans. Larger hashes auto-promote to a HashMap
//! for O(1) field access. CompactString inlines field names ≤24 bytes,
//! avoiding a heap allocation per field.

use bytes::Bytes;
use compact_str::CompactString;
use std::collections::HashMap;

/// Hashes with more fields than this are stored as a HashMap.
/// 32 is chosen to balance linear-scan cost against HashMap overhead
/// (48 bytes base + 16 bytes/bucket).
const COMPACT_THRESHOLD: usize = 32;

/// A hash value stored as either a compact vec or a full hashmap.
///
/// Most Redis-style hashes have fewer than 32 fields (user profiles,
/// session data, config objects). For these, a Vec is both smaller
/// and faster than a HashMap due to cache locality.
#[derive(Debug, Clone)]
pub enum HashValue {
    /// Linear-scan storage for small hashes. Each lookup is O(n) but
    /// n ≤ 32 and the entries are contiguous in memory.
    Compact(Vec<(CompactString, Bytes)>),
    /// Hash-indexed storage for large hashes. Promoted automatically
    /// when field count exceeds the threshold.
    Full(HashMap<CompactString, Bytes>),
}

impl Default for HashValue {
    fn default() -> Self {
        HashValue::Compact(Vec::new())
    }
}

impl HashValue {
    /// Inserts a field-value pair. Returns the old value if the field existed.
    ///
    /// Auto-promotes from Compact to Full when the field count exceeds
    /// the threshold after insertion.
    pub fn insert(&mut self, field: CompactString, value: Bytes) -> Option<Bytes> {
        let result = match self {
            HashValue::Compact(vec) => {
                for (k, v) in vec.iter_mut() {
                    if *k == field {
                        return Some(std::mem::replace(v, value));
                    }
                }
                vec.push((field, value));
                None
            }
            HashValue::Full(map) => return map.insert(field, value),
        };
        // check if compact representation exceeded threshold
        if let HashValue::Compact(vec) = self {
            if vec.len() > COMPACT_THRESHOLD {
                let map = std::mem::take(vec).into_iter().collect();
                *self = HashValue::Full(map);
            }
        }
        result
    }

    /// Gets a field's value by name.
    pub fn get(&self, field: &str) -> Option<&Bytes> {
        match self {
            HashValue::Compact(vec) => vec
                .iter()
                .find(|(k, _)| k.as_str() == field)
                .map(|(_, v)| v),
            HashValue::Full(map) => map.get(field),
        }
    }

    /// Removes a field, returning its value if it existed.
    pub fn remove(&mut self, field: &str) -> Option<Bytes> {
        match self {
            HashValue::Compact(vec) => {
                if let Some(pos) = vec.iter().position(|(k, _)| k.as_str() == field) {
                    Some(vec.swap_remove(pos).1)
                } else {
                    None
                }
            }
            HashValue::Full(map) => map.remove(field),
        }
    }

    /// Checks if a field exists.
    pub fn contains_key(&self, field: &str) -> bool {
        match self {
            HashValue::Compact(vec) => vec.iter().any(|(k, _)| k.as_str() == field),
            HashValue::Full(map) => map.contains_key(field),
        }
    }

    /// Returns the number of fields.
    pub fn len(&self) -> usize {
        match self {
            HashValue::Compact(vec) => vec.len(),
            HashValue::Full(map) => map.len(),
        }
    }

    /// Returns true if the hash has no fields.
    pub fn is_empty(&self) -> bool {
        match self {
            HashValue::Compact(vec) => vec.is_empty(),
            HashValue::Full(map) => map.is_empty(),
        }
    }

    /// Iterates over all field-value pairs.
    pub fn iter(&self) -> HashIter<'_> {
        match self {
            HashValue::Compact(vec) => HashIter::Compact(vec.iter()),
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
            if other.get(k) != Some(v) {
                return false;
            }
        }
        true
    }
}

/// Constructs a HashValue from a HashMap, choosing compact or full
/// representation based on field count.
impl From<HashMap<String, Bytes>> for HashValue {
    fn from(map: HashMap<String, Bytes>) -> Self {
        if map.len() <= COMPACT_THRESHOLD {
            HashValue::Compact(
                map.into_iter()
                    .map(|(k, v)| (CompactString::from(k), v))
                    .collect(),
            )
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
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }
}

/// Zero-allocation iterator over hash fields.
pub enum HashIter<'a> {
    Compact(std::slice::Iter<'a, (CompactString, Bytes)>),
    Full(std::collections::hash_map::Iter<'a, CompactString, Bytes>),
}

impl<'a> Iterator for HashIter<'a> {
    type Item = (&'a str, &'a Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            HashIter::Compact(iter) => iter.next().map(|(k, v)| (k.as_str(), v)),
            HashIter::Full(iter) => iter.next().map(|(k, v)| (k.as_str(), v)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            HashIter::Compact(iter) => iter.size_hint(),
            HashIter::Full(iter) => iter.size_hint(),
        }
    }
}

impl<'a> ExactSizeIterator for HashIter<'a> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_insert_and_get() {
        let mut h = HashValue::default();
        h.insert("name".into(), Bytes::from("alice"));
        assert_eq!(h.get("name"), Some(&Bytes::from("alice")));
        assert!(matches!(h, HashValue::Compact(_)));
    }

    #[test]
    fn insert_returns_old_value() {
        let mut h = HashValue::default();
        assert!(h.insert("k".into(), Bytes::from("v1")).is_none());
        let old = h.insert("k".into(), Bytes::from("v2"));
        assert_eq!(old, Some(Bytes::from("v1")));
        assert_eq!(h.get("k"), Some(&Bytes::from("v2")));
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
        assert!(matches!(h, HashValue::Compact(_)));
        assert_eq!(h.get("a"), Some(&Bytes::from("1")));
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
}
