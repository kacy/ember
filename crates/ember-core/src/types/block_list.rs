//! A sequence stored as a list of blocks.
//!
//! Sorted sets keep their members in order by score. In one `Vec`, every
//! insert or remove shifts all the later members, which is O(n) per write
//! on a large set. Here the members are split into blocks of at most
//! [`MAX_BLOCK`] entries, so a write shifts at most one block. Finding a
//! position sums the block lengths, which is O(n / MAX_BLOCK).
//!
//! Blocks are never empty, and the elements read in order across them.

use std::cmp::Ordering;

/// A block grows to this many elements before it splits in half.
const MAX_BLOCK: usize = 1024;

/// A sequence with `Vec`-like positions and O(MAX_BLOCK) inserts and
/// removes at any position.
#[derive(Debug, Clone)]
pub struct BlockList<T> {
    blocks: Vec<Vec<T>>,
    len: usize,
}

impl<T> Default for BlockList<T> {
    fn default() -> Self {
        Self {
            blocks: Vec::new(),
            len: 0,
        }
    }
}

impl<T> BlockList<T> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the block and offset of position `index`. Positions at or
    /// past the end map to `(number of blocks, index - len)`.
    fn locate(&self, mut index: usize) -> (usize, usize) {
        for (b, block) in self.blocks.iter().enumerate() {
            if index < block.len() {
                return (b, index);
            }
            index -= block.len();
        }
        (self.blocks.len(), index)
    }

    /// The number of elements in the blocks before block `b`.
    fn count_before(&self, b: usize) -> usize {
        self.blocks[..b].iter().map(Vec::len).sum()
    }

    /// Like [`slice::partition_point`]: the elements must be partitioned so
    /// that `pred` holds for a prefix of them.
    pub fn partition_point(&self, mut pred: impl FnMut(&T) -> bool) -> usize {
        let b = self.blocks.partition_point(|block| pred(last(block)));
        let inside = self
            .blocks
            .get(b)
            .map_or(0, |block| block.partition_point(&mut pred));
        self.count_before(b) + inside
    }

    /// Like [`slice::binary_search_by`] over the whole sequence.
    pub fn binary_search_by(&self, mut f: impl FnMut(&T) -> Ordering) -> Result<usize, usize> {
        let b = self
            .blocks
            .partition_point(|block| f(last(block)) == Ordering::Less);
        let before = self.count_before(b);
        match self.blocks.get(b) {
            None => Err(before),
            Some(block) => block
                .binary_search_by(f)
                .map(|i| before + i)
                .map_err(|i| before + i),
        }
    }

    /// Inserts `value` at position `index`, shifting later elements back.
    ///
    /// # Panics
    ///
    /// Panics if `index > len`.
    pub fn insert(&mut self, index: usize, value: T) {
        assert!(index <= self.len, "insert index out of bounds");
        if self.blocks.is_empty() {
            self.blocks.push(Vec::with_capacity(MAX_BLOCK));
        }
        let (mut b, mut i) = self.locate(index);
        if b == self.blocks.len() {
            // appending: add to the end of the last block
            b -= 1;
            i = self.blocks[b].len();
        }
        self.blocks[b].insert(i, value);
        self.len += 1;
        if self.blocks[b].len() > MAX_BLOCK {
            let tail = self.blocks[b].split_off(MAX_BLOCK / 2);
            self.blocks.insert(b + 1, tail);
        }
    }

    /// Removes and returns the element at position `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index >= len`.
    pub fn remove(&mut self, index: usize) -> T {
        assert!(index < self.len, "remove index out of bounds");
        let (b, i) = self.locate(index);
        let value = self.blocks[b].remove(i);
        self.len -= 1;
        self.tidy(b);
        value
    }

    /// Drops block `b` when empty, or merges it into the next block when
    /// both are small, so blocks stay reasonably full after removals.
    fn tidy(&mut self, b: usize) {
        if self.blocks[b].is_empty() {
            self.blocks.remove(b);
        } else if b + 1 < self.blocks.len()
            && self.blocks[b].len() + self.blocks[b + 1].len() <= MAX_BLOCK / 2
        {
            let next = self.blocks.remove(b + 1);
            self.blocks[b].extend(next);
        }
    }

    /// Removes and returns the first `n` elements, in order.
    pub fn drain_front(&mut self, n: usize) -> Vec<T> {
        let n = n.min(self.len);
        let mut out = Vec::with_capacity(n);
        while out.len() < n {
            let want = n - out.len();
            if self.blocks[0].len() <= want {
                out.extend(self.blocks.remove(0));
            } else {
                out.extend(self.blocks[0].drain(..want));
            }
        }
        self.len -= n;
        out
    }

    /// Removes and returns the last `n` elements, in order.
    pub fn drain_back(&mut self, n: usize) -> Vec<T> {
        let n = n.min(self.len);
        let mut taken = Vec::new();
        let mut count = 0;
        while count < n {
            let want = n - count;
            let block = self.blocks.last_mut().expect("count < len");
            let chunk = if block.len() <= want {
                self.blocks.pop().expect("checked above")
            } else {
                block.split_off(block.len() - want)
            };
            count += chunk.len();
            taken.push(chunk);
        }
        self.len -= n;
        taken.into_iter().rev().flatten().collect()
    }

    /// Iterates over the elements in positions `start..end`.
    pub fn range(&self, start: usize, end: usize) -> impl DoubleEndedIterator<Item = &T> {
        let end = end.min(self.len);
        let start = start.min(end);
        let (first, from) = self.locate(start);
        let (last, to) = self.locate(end);
        (first..=last.min(self.blocks.len().saturating_sub(1)))
            .filter(move |_| start < end)
            .flat_map(move |b| {
                let block = &self.blocks[b];
                let lo = if b == first { from } else { 0 };
                let hi = if b == last { to } else { block.len() };
                block[lo..hi].iter()
            })
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> {
        self.blocks.iter().flatten()
    }
}

fn last<T>(block: &[T]) -> &T {
    block.last().expect("blocks are never empty")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks the list against a `Vec` doing the same operations.
    fn assert_same(list: &BlockList<u32>, model: &[u32]) {
        assert_eq!(list.len(), model.len());
        assert!(list.blocks.iter().all(|b| !b.is_empty()));
        assert!(list.iter().eq(model.iter()));
        assert!(list.iter().rev().eq(model.iter().rev()));
    }

    #[test]
    fn matches_a_vec_through_inserts_and_removes() {
        let mut list = BlockList::default();
        let mut model = Vec::new();
        // a simple deterministic sequence of positions
        let mut x: u64 = 7;
        let mut next = |bound: usize| {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (x >> 33) as usize % bound.max(1)
        };
        for v in 0..5000u32 {
            let i = next(model.len() + 1);
            list.insert(i, v);
            model.insert(i, v);
        }
        assert_same(&list, &model);
        assert!(list.blocks.len() > 4);
        for _ in 0..4000 {
            let i = next(model.len());
            assert_eq!(list.remove(i), model.remove(i));
        }
        assert_same(&list, &model);

        for (start, end) in [
            (0, 0),
            (0, 1),
            (3, 700),
            (500, 1000),
            (999, 2000),
            (0, 1000),
        ] {
            let want: Vec<_> = model[start.min(model.len())..end.min(model.len())].to_vec();
            let got: Vec<_> = list.range(start, end).copied().collect();
            assert_eq!(got, want, "range {start}..{end}");
            let got_rev: Vec<_> = list.range(start, end).rev().copied().collect();
            assert!(got_rev.into_iter().eq(want.into_iter().rev()));
        }
    }

    #[test]
    fn searches_match_a_sorted_vec() {
        let model: Vec<u32> = (0..3000).map(|i| i * 2).collect();
        let mut list = BlockList::default();
        for &v in &model {
            list.insert(list.len(), v);
        }
        for target in [0, 1, 2, 999, 1000, 5998, 5999, 6000, 7000] {
            assert_eq!(
                list.binary_search_by(|v| v.cmp(&target)),
                model.binary_search(&target)
            );
            assert_eq!(
                list.partition_point(|v| *v < target),
                model.partition_point(|v| *v < target)
            );
        }
    }

    #[test]
    fn drains_from_both_ends() {
        let mut list = BlockList::default();
        let mut model: Vec<u32> = (0..2500).collect();
        for &v in &model {
            list.insert(list.len(), v);
        }
        assert_eq!(
            list.drain_front(1100),
            model.drain(..1100).collect::<Vec<_>>()
        );
        let tail = model.split_off(model.len() - 700);
        assert_eq!(list.drain_back(700), tail);
        assert_same(&list, &model);
        assert_eq!(list.drain_back(10_000).len(), model.len());
        assert!(list.is_empty());
        assert!(list.drain_front(5).is_empty());
    }
}
