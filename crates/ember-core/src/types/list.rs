//! The list type.
//!
//! A `VecDeque` of elements that keeps a running count of element bytes,
//! so its memory estimate is O(1).

use std::collections::VecDeque;
use std::ops::Deref;

use bytes::Bytes;

use crate::memory::{VECDEQUE_BASE_OVERHEAD, VECDEQUE_ELEMENT_OVERHEAD};

/// A list of binary-safe elements. Reads go through `Deref` to the
/// `VecDeque`; writes go through the methods here so the byte count stays
/// right.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ListValue {
    items: VecDeque<Bytes>,
    /// Total length of all elements.
    data_bytes: usize,
}

impl ListValue {
    pub fn push_front(&mut self, value: Bytes) {
        self.data_bytes += value.len();
        self.items.push_front(value);
    }

    pub fn push_back(&mut self, value: Bytes) {
        self.data_bytes += value.len();
        self.items.push_back(value);
    }

    pub fn pop_front(&mut self) -> Option<Bytes> {
        let value = self.items.pop_front()?;
        self.data_bytes -= value.len();
        Some(value)
    }

    pub fn pop_back(&mut self) -> Option<Bytes> {
        let value = self.items.pop_back()?;
        self.data_bytes -= value.len();
        Some(value)
    }

    /// Inserts `value` at `index`, shifting later elements back.
    pub fn insert(&mut self, index: usize, value: Bytes) {
        self.data_bytes += value.len();
        self.items.insert(index, value);
    }

    /// Removes and returns the element at `index`.
    pub fn remove(&mut self, index: usize) -> Option<Bytes> {
        let value = self.items.remove(index)?;
        self.data_bytes -= value.len();
        Some(value)
    }

    /// Replaces the element at `index`. Returns `false` if `index` is out
    /// of range.
    pub fn set(&mut self, index: usize, value: Bytes) -> bool {
        let Some(slot) = self.items.get_mut(index) else {
            return false;
        };
        self.data_bytes = self.data_bytes - slot.len() + value.len();
        *slot = value;
        true
    }

    /// Keeps only the elements from `start` to `end`, inclusive. An empty
    /// or out-of-range span empties the list.
    pub fn keep_range(&mut self, start: usize, end: usize) {
        if start > end || start >= self.items.len() {
            self.items.clear();
            self.data_bytes = 0;
            return;
        }
        let dropped_tail = self
            .items
            .drain(end.saturating_add(1).min(self.items.len())..);
        let tail_bytes: usize = dropped_tail.map(|v| v.len()).sum();
        let head_bytes: usize = self.items.drain(..start).map(|v| v.len()).sum();
        self.data_bytes -= tail_bytes + head_bytes;
    }

    /// Estimated memory used by the list, excluding the keyspace entry.
    pub fn memory_usage(&self) -> usize {
        VECDEQUE_BASE_OVERHEAD + self.items.len() * VECDEQUE_ELEMENT_OVERHEAD + self.data_bytes
    }
}

impl Deref for ListValue {
    type Target = VecDeque<Bytes>;

    fn deref(&self) -> &VecDeque<Bytes> {
        &self.items
    }
}

impl FromIterator<Bytes> for ListValue {
    fn from_iter<I: IntoIterator<Item = Bytes>>(iter: I) -> Self {
        let items: VecDeque<Bytes> = iter.into_iter().collect();
        let data_bytes = items.iter().map(Bytes::len).sum();
        Self { items, data_bytes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list(items: &[&'static str]) -> ListValue {
        items
            .iter()
            .map(|s| Bytes::from_static(s.as_bytes()))
            .collect()
    }

    fn data_bytes(list: &ListValue) -> usize {
        list.iter().map(Bytes::len).sum()
    }

    #[test]
    fn byte_count_follows_every_write() {
        let mut l = list(&["a", "bb", "ccc"]);
        l.push_front(Bytes::from_static(b"dddd"));
        l.push_back(Bytes::from_static(b"e"));
        l.insert(2, Bytes::from_static(b"ff"));
        assert!(l.set(0, Bytes::from_static(b"g")));
        assert!(!l.set(99, Bytes::from_static(b"g")));
        assert_eq!(l.remove(1).as_deref(), Some(&b"a"[..]));
        l.pop_front();
        l.pop_back();
        assert_eq!(l.data_bytes, data_bytes(&l));
        assert_eq!(
            l.memory_usage(),
            VECDEQUE_BASE_OVERHEAD + l.len() * VECDEQUE_ELEMENT_OVERHEAD + data_bytes(&l)
        );
    }

    #[test]
    fn keep_range_trims_both_ends() {
        let mut l = list(&["a", "bb", "ccc", "dddd"]);
        l.keep_range(1, 2);
        assert_eq!(l, list(&["bb", "ccc"]));
        assert_eq!(l.data_bytes, 5);
        l.keep_range(0, 10);
        assert_eq!(l, list(&["bb", "ccc"]));
        l.keep_range(1, 0);
        assert!(l.is_empty());
        assert_eq!(l.data_bytes, 0);
    }
}
