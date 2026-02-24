use super::*;

/// Resolves a signed index into a VecDeque position.
/// Negative indices count from the end (-1 = last element).
/// Returns `None` if the resolved index is out of bounds.
fn resolve_index(index: i64, len: usize) -> Option<usize> {
    let resolved = if index < 0 {
        (len as i64).checked_add(index)?
    } else {
        index
    };
    if resolved < 0 || resolved >= len as i64 {
        None
    } else {
        Some(resolved as usize)
    }
}

impl Keyspace {
    /// Pushes one or more values to the head (left) of a list.
    ///
    /// Creates the list if the key doesn't exist. Returns `Err(WriteError::WrongType)`
    /// if the key exists but holds a non-list value, or
    /// `Err(WriteError::OutOfMemory)` if the memory limit is reached.
    /// Returns the new length on success.
    pub fn lpush(&mut self, key: &str, values: &[Bytes]) -> Result<usize, WriteError> {
        self.list_push(key, values, true)
    }

    /// Pushes one or more values to the tail (right) of a list.
    ///
    /// Creates the list if the key doesn't exist. Returns `Err(WriteError::WrongType)`
    /// if the key exists but holds a non-list value, or
    /// `Err(WriteError::OutOfMemory)` if the memory limit is reached.
    /// Returns the new length on success.
    pub fn rpush(&mut self, key: &str, values: &[Bytes]) -> Result<usize, WriteError> {
        self.list_push(key, values, false)
    }

    /// Pops a value from the head (left) of a list.
    ///
    /// Returns `Ok(None)` if the key doesn't exist. Removes the key if
    /// the list becomes empty. Returns `Err(WrongType)` on type mismatch.
    pub fn lpop(&mut self, key: &str) -> Result<Option<Bytes>, WrongType> {
        self.list_pop(key, true)
    }

    /// Pops a value from the tail (right) of a list.
    ///
    /// Returns `Ok(None)` if the key doesn't exist. Removes the key if
    /// the list becomes empty. Returns `Err(WrongType)` on type mismatch.
    pub fn rpop(&mut self, key: &str) -> Result<Option<Bytes>, WrongType> {
        self.list_pop(key, false)
    }

    /// Returns a range of elements from a list by index.
    ///
    /// Supports negative indices (e.g. -1 = last element). Out-of-bounds
    /// indices are clamped to the list boundaries. Returns `Err(WrongType)`
    /// on type mismatch. Missing keys return an empty vec.
    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<Bytes>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }
        match self.entries.get_mut(key) {
            None => Ok(vec![]),
            Some(entry) => {
                let result = match &entry.value {
                    Value::List(deque) => {
                        let len = deque.len() as i64;
                        let (s, e) = normalize_range(start, stop, len);
                        // empty range: inverted indices or both out of bounds
                        if s > e {
                            return Ok(vec![]);
                        }
                        Ok(deque
                            .iter()
                            .skip(s as usize)
                            .take((e - s + 1) as usize)
                            .cloned()
                            .collect())
                    }
                    _ => Err(WrongType),
                };
                if result.is_ok() {
                    entry.touch();
                }
                result
            }
        }
    }

    /// Returns the length of a list, or 0 if the key doesn't exist.
    ///
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn llen(&mut self, key: &str) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }
        match self.entries.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                Value::List(deque) => Ok(deque.len()),
                _ => Err(WrongType),
            },
        }
    }

    /// Returns the element at `index` in the list, or `None` if out of bounds.
    ///
    /// Negative indices count from the tail (-1 = last element).
    /// Returns `Err(WrongType)` if the key holds a non-list value.
    pub fn lindex(&mut self, key: &str, index: i64) -> Result<Option<Bytes>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }
        let entry = match self.entries.get_mut(key) {
            None => return Ok(None),
            Some(e) => e,
        };
        let result = match &entry.value {
            Value::List(deque) => {
                Ok(resolve_index(index, deque.len()).and_then(|i| deque.get(i).cloned()))
            }
            _ => Err(WrongType),
        };
        if result.is_ok() {
            entry.touch();
        }
        result
    }

    /// Sets the element at `index` to `value`.
    ///
    /// Negative indices count from the tail. Returns an error if the key
    /// doesn't exist, the index is out of range, or the key holds the
    /// wrong type.
    pub fn lset(&mut self, key: &str, index: i64, value: Bytes) -> Result<(), LsetError> {
        self.remove_if_expired(key);

        match self.entries.get(key) {
            None => return Err(LsetError::NoSuchKey),
            Some(e) if !matches!(e.value, Value::List(_)) => return Err(LsetError::WrongType),
            _ => {}
        }

        // mutate in a scoped block so entry borrow is released for memory updates
        let (old_len, new_len) = {
            let entry = self.entries.get_mut(key).expect("verified above");
            let Value::List(ref mut deque) = entry.value else {
                unreachable!("type checked");
            };
            let pos = resolve_index(index, deque.len())
                .ok_or(LsetError::IndexOutOfRange)?;
            let elem = deque.get_mut(pos).ok_or(LsetError::IndexOutOfRange)?;
            let old_len = elem.len();
            let new_len = value.len();
            *elem = value;
            (old_len, new_len)
        };

        let ver = self.next_ver();
        let delta = new_len as isize - old_len as isize;
        {
            let entry = self.entries.get_mut(key).expect("key confirmed to exist");
            entry.touch();
            entry.version = ver;
            if delta > 0 {
                entry.cached_value_size += delta as usize;
            } else if delta < 0 {
                entry.cached_value_size = entry
                    .cached_value_size
                    .saturating_sub((-delta) as usize);
            }
        }
        if delta > 0 {
            self.memory.grow_by(delta as usize);
        } else if delta < 0 {
            self.memory.shrink_by((-delta) as usize);
        }

        Ok(())
    }

    /// Trims a list to keep only elements in the range [start, stop].
    ///
    /// Supports negative indices. If the range is empty or inverted, the
    /// key is deleted. Missing keys are a no-op.
    pub fn ltrim(&mut self, key: &str, start: i64, stop: i64) -> Result<(), WrongType> {
        if self.remove_if_expired(key) {
            return Ok(());
        }

        let (len, old_entry_size) = match self.entries.get(key) {
            None => return Ok(()),
            Some(e) => match &e.value {
                Value::List(d) => (d.len(), e.entry_size(key)),
                _ => return Err(WrongType),
            },
        };

        let (s, e) = normalize_range(start, stop, len as i64);

        let (is_empty, new_value_size) = {
            let ver = self.next_ver();
            let entry = self.entries.get_mut(key).expect("verified above");
            {
                let Value::List(ref mut deque) = entry.value else {
                    unreachable!("type checked");
                };
                if s > e || s >= len as i64 {
                    deque.clear();
                } else {
                    let s_usize = s as usize;
                    let e_usize = (e as usize).min(len - 1);
                    deque.truncate(e_usize + 1);
                    deque.drain(..s_usize);
                }
            }

            let empty = matches!(&entry.value, Value::List(d) if d.is_empty());
            entry.touch();
            entry.version = ver;
            let nvs = if !empty {
                let size = memory::value_size(&entry.value);
                entry.cached_value_size = size;
                size
            } else {
                0
            };

            (empty, nvs)
        };

        if is_empty {
            if let Some(removed) = self.entries.remove(key) {
                self.decrement_expiry_if_set(&removed);
            }
            self.memory.remove_with_size(old_entry_size);
        } else {
            let new_entry_size = key.len() + new_value_size + memory::ENTRY_OVERHEAD;
            self.memory.adjust(old_entry_size, new_entry_size);
        }

        Ok(())
    }

    /// Inserts `value` before or after the first occurrence of `pivot`.
    ///
    /// Returns the new list length on success, -1 if the pivot was not
    /// found, or 0 if the key doesn't exist.
    pub fn linsert(
        &mut self,
        key: &str,
        before: bool,
        pivot: &[u8],
        value: Bytes,
    ) -> Result<i64, WriteError> {
        self.remove_if_expired(key);

        match self.entries.get(key) {
            None => return Ok(0),
            Some(e) if !matches!(e.value, Value::List(_)) => {
                return Err(WriteError::WrongType);
            }
            _ => {}
        }

        // find pivot position
        let pivot_pos = {
            let entry = self.entries.get(key).expect("exists");
            match &entry.value {
                Value::List(deque) => deque.iter().position(|e| e.as_ref() == pivot),
                _ => unreachable!("type checked"),
            }
        };

        let pivot_pos = match pivot_pos {
            Some(p) => p,
            None => return Ok(-1),
        };

        let element_cost = memory::VECDEQUE_ELEMENT_OVERHEAD + value.len();
        if !self.enforce_memory_limit(element_cost) {
            return Err(WriteError::OutOfMemory);
        }

        let ver = self.next_ver();
        let new_len = {
            let entry = match self.entries.get_mut(key) {
                Some(e) => e,
                None => return Ok(0), // evicted during memory enforcement
            };
            let Value::List(ref mut deque) = entry.value else {
                unreachable!("type checked");
            };

            let insert_pos = if before { pivot_pos } else { pivot_pos + 1 };
            if insert_pos > deque.len() {
                return Ok(-1); // safety: pivot position no longer valid
            }

            deque.insert(insert_pos, value);
            let len = deque.len() as i64;
            entry.touch();
            entry.version = ver;
            entry.cached_value_size += element_cost;
            len
        };

        self.memory.grow_by(element_cost);
        Ok(new_len)
    }

    /// Removes elements equal to `value` from the list.
    ///
    /// - `count > 0`: remove first `count` matches from head
    /// - `count < 0`: remove last `|count|` matches from tail
    /// - `count == 0`: remove all matches
    ///
    /// Returns the number of elements removed.
    pub fn lrem(&mut self, key: &str, count: i64, value: &[u8]) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }

        match self.entries.get(key) {
            None => return Ok(0),
            Some(e) if !matches!(e.value, Value::List(_)) => return Err(WrongType),
            _ => {}
        }

        let old_entry_size = self.entries.get(key).expect("exists").entry_size(key);
        let ver = self.next_ver();

        let (removed_count, removed_bytes, is_empty) = {
            let entry = self.entries.get_mut(key).expect("exists");
            let Value::List(ref mut deque) = entry.value else {
                unreachable!("type checked");
            };

            let limit = if count == 0 {
                usize::MAX
            } else {
                count.unsigned_abs() as usize
            };
            let mut indices: Vec<usize> = Vec::new();

            if count >= 0 {
                for (i, elem) in deque.iter().enumerate() {
                    if elem.as_ref() == value {
                        indices.push(i);
                        if indices.len() >= limit {
                            break;
                        }
                    }
                }
            } else {
                for (i, elem) in deque.iter().enumerate().rev() {
                    if elem.as_ref() == value {
                        indices.push(i);
                        if indices.len() >= limit {
                            break;
                        }
                    }
                }
            }

            let n = indices.len();
            if n == 0 {
                return Ok(0);
            }

            // remove from highest index first to preserve lower indices
            indices.sort_unstable_by(|a, b| b.cmp(a));

            let mut bytes = 0usize;
            for idx in &indices {
                if let Some(elem) = deque.remove(*idx) {
                    bytes += elem.len() + memory::VECDEQUE_ELEMENT_OVERHEAD;
                }
            }

            let empty = deque.is_empty();
            entry.touch();
            entry.version = ver;

            if !empty {
                entry.cached_value_size = entry.cached_value_size.saturating_sub(bytes);
            }

            (n, bytes, empty)
        };

        if is_empty {
            if let Some(removed) = self.entries.remove(key) {
                self.decrement_expiry_if_set(&removed);
            }
            self.memory.remove_with_size(old_entry_size);
        } else {
            self.memory.shrink_by(removed_bytes);
        }

        Ok(removed_count)
    }

    /// Finds the positions of elements matching `element` in the list.
    ///
    /// - `rank`: 1-based. Positive scans from head, negative from tail.
    ///   Rank 2 means "start from the 2nd match".
    /// - `count`: 0 returns all matches, N returns at most N.
    /// - `maxlen`: limits the scan to the first (or last) N entries.
    pub fn lpos(
        &mut self,
        key: &str,
        element: &[u8],
        rank: i64,
        count: usize,
        maxlen: usize,
    ) -> Result<Vec<i64>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }

        let entry = match self.entries.get_mut(key) {
            None => return Ok(vec![]),
            Some(e) => e,
        };

        let result = {
            let Value::List(ref deque) = entry.value else {
                return Err(WrongType);
            };

            let len = deque.len();
            let limit = if maxlen == 0 { len } else { maxlen.min(len) };
            let rank = if rank == 0 { 1 } else { rank };
            let forward = rank > 0;
            let skip = (rank.unsigned_abs() as usize).saturating_sub(1);

            let mut skipped = 0usize;
            let mut results = Vec::new();

            if forward {
                for i in 0..limit {
                    if deque.get(i).map(|e| e.as_ref() == element).unwrap_or(false) {
                        if skipped < skip {
                            skipped += 1;
                        } else {
                            results.push(i as i64);
                            if count > 0 && results.len() >= count {
                                break;
                            }
                        }
                    }
                }
            } else {
                let start = if maxlen == 0 { 0 } else { len.saturating_sub(maxlen) };
                for i in (start..len).rev() {
                    if deque.get(i).map(|e| e.as_ref() == element).unwrap_or(false) {
                        if skipped < skip {
                            skipped += 1;
                        } else {
                            results.push(i as i64);
                            if count > 0 && results.len() >= count {
                                break;
                            }
                        }
                    }
                }
            }

            results
        };

        entry.touch();
        Ok(result)
    }

    /// Internal push implementation shared by lpush/rpush.
    pub(super) fn list_push(
        &mut self,
        key: &str,
        values: &[Bytes],
        left: bool,
    ) -> Result<usize, WriteError> {
        self.remove_if_expired(key);

        let is_new = self.ensure_collection_type(key, |v| matches!(v, Value::List(_)))?;

        let element_increase: usize = values
            .iter()
            .map(|v| memory::VECDEQUE_ELEMENT_OVERHEAD + v.len())
            .sum();
        self.reserve_memory(
            is_new,
            key,
            memory::VECDEQUE_BASE_OVERHEAD,
            element_increase,
        )?;

        if is_new {
            self.insert_empty(key, Value::List(VecDeque::new()));
        }

        // safe: key was just inserted or confirmed to exist
        let ver = self.next_ver();
        let entry = self.entries.get_mut(key).unwrap();
        let Value::List(ref mut deque) = entry.value else {
            unreachable!("type verified by ensure_collection_type");
        };
        for val in values {
            if left {
                deque.push_front(val.clone());
            } else {
                deque.push_back(val.clone());
            }
        }
        let len = deque.len();
        entry.touch();
        entry.version = ver;
        entry.cached_value_size += element_increase;

        // apply the known delta — no need to rescan the entire list
        self.memory.grow_by(element_increase);

        Ok(len)
    }

    /// Internal pop implementation shared by lpop/rpop.
    pub(super) fn list_pop(&mut self, key: &str, left: bool) -> Result<Option<Bytes>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }

        let ver = self.next_ver();
        let Some(entry) = self.entries.get_mut(key) else {
            return Ok(None);
        };
        if !matches!(entry.value, Value::List(_)) {
            return Err(WrongType);
        }

        let Value::List(ref mut deque) = entry.value else {
            // checked above
            return Err(WrongType);
        };
        let popped = if left {
            deque.pop_front()
        } else {
            deque.pop_back()
        };
        entry.touch();
        entry.version = ver;

        let is_empty = matches!(&entry.value, Value::List(d) if d.is_empty());
        if let Some(ref elem) = popped {
            let element_size = elem.len() + memory::VECDEQUE_ELEMENT_OVERHEAD;
            let old_size = if is_empty {
                // the list held exactly this one element — compute exact old size
                // from constants rather than scanning
                key.len() + memory::ENTRY_OVERHEAD + memory::VECDEQUE_BASE_OVERHEAD + element_size
            } else {
                0 // unused in the non-empty branch of cleanup_after_remove
            };
            self.cleanup_after_remove(key, old_size, is_empty, element_size);
        }

        Ok(popped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lpush_creates_list() {
        let mut ks = Keyspace::new();
        let len = ks
            .lpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(len, 2);
        // lpush pushes each to front, so order is b, a
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(items, vec![Bytes::from("b"), Bytes::from("a")]);
    }

    #[test]
    fn rpush_creates_list() {
        let mut ks = Keyspace::new();
        let len = ks
            .rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(len, 2);
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(items, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn push_to_existing_list() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a")]).unwrap();
        let len = ks.rpush("list", &[Bytes::from("b")]).unwrap();
        assert_eq!(len, 2);
    }

    #[test]
    fn lpop_returns_front() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(ks.lpop("list").unwrap(), Some(Bytes::from("a")));
        assert_eq!(ks.lpop("list").unwrap(), Some(Bytes::from("b")));
        assert_eq!(ks.lpop("list").unwrap(), None); // empty, key deleted
    }

    #[test]
    fn rpop_returns_back() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(ks.rpop("list").unwrap(), Some(Bytes::from("b")));
    }

    #[test]
    fn pop_from_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.lpop("nope").unwrap(), None);
        assert_eq!(ks.rpop("nope").unwrap(), None);
    }

    #[test]
    fn empty_list_auto_deletes_key() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("only")]).unwrap();
        ks.lpop("list").unwrap();
        assert!(!ks.exists("list"));
        assert_eq!(ks.stats().key_count, 0);
        assert_eq!(ks.stats().used_bytes, 0);
    }

    #[test]
    fn lrange_negative_indices() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        )
        .unwrap();
        // -2 to -1 => last two elements
        let items = ks.lrange("list", -2, -1).unwrap();
        assert_eq!(items, vec![Bytes::from("b"), Bytes::from("c")]);
    }

    #[test]
    fn lrange_out_of_bounds_clamps() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        let items = ks.lrange("list", -100, 100).unwrap();
        assert_eq!(items, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn lrange_missing_key_returns_empty() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.lrange("nope", 0, -1).unwrap(), Vec::<Bytes>::new());
    }

    #[test]
    fn llen_returns_length() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.llen("nope").unwrap(), 0);
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(ks.llen("list").unwrap(), 2);
    }

    #[test]
    fn list_memory_tracked_on_push_pop() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("hello")]).unwrap();
        let after_push = ks.stats().used_bytes;
        assert!(after_push > 0);

        ks.rpush("list", &[Bytes::from("world")]).unwrap();
        let after_second = ks.stats().used_bytes;
        assert!(after_second > after_push);

        ks.lpop("list").unwrap();
        let after_pop = ks.stats().used_bytes;
        assert!(after_pop < after_second);
    }

    #[test]
    fn lpush_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.lpush("s", &[Bytes::from("nope")]).is_err());
    }

    #[test]
    fn lrange_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.lrange("s", 0, -1).is_err());
    }

    #[test]
    fn llen_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.llen("s").is_err());
    }

    #[test]
    fn lpush_rejects_when_memory_full() {
        let config = ShardConfig {
            max_memory: Some(150),
            eviction_policy: EvictionPolicy::NoEviction,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        // first key eats up most of the budget
        assert_eq!(
            ks.set("a".into(), Bytes::from("val"), None, false, false),
            SetResult::Ok
        );

        // lpush should be rejected — not enough room
        let result = ks.lpush("list", &[Bytes::from("big-value-here")]);
        assert_eq!(result, Err(WriteError::OutOfMemory));

        // original key should be untouched
        assert!(ks.exists("a"));
    }

    #[test]
    fn rpush_rejects_when_memory_full() {
        let config = ShardConfig {
            max_memory: Some(150),
            eviction_policy: EvictionPolicy::NoEviction,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        assert_eq!(
            ks.set("a".into(), Bytes::from("val"), None, false, false),
            SetResult::Ok
        );

        let result = ks.rpush("list", &[Bytes::from("big-value-here")]);
        assert_eq!(result, Err(WriteError::OutOfMemory));
    }

    #[test]
    fn lpush_evicts_under_lru_policy() {
        // "a" entry = 1 + 3 + 128 = 132 bytes.
        // list entry = 4 + 24 + (4 + 32) + 128 = 192 bytes.
        // effective limit = 250 * 90 / 100 = 225. fits one entry but not both,
        // so lpush should evict "a" to make room for the list.
        let config = ShardConfig {
            max_memory: Some(250),
            eviction_policy: EvictionPolicy::AllKeysLru,
            ..ShardConfig::default()
        };
        let mut ks = Keyspace::with_config(config);

        assert_eq!(
            ks.set("a".into(), Bytes::from("val"), None, false, false),
            SetResult::Ok
        );

        // should evict "a" to make room for the list
        assert!(ks.lpush("list", &[Bytes::from("item")]).is_ok());
        assert!(!ks.exists("a"));
    }

    #[test]
    fn list_auto_deleted_when_empty() {
        let mut ks = Keyspace::new();
        ks.lpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(ks.len(), 1);

        // pop all elements
        let _ = ks.lpop("list");
        let _ = ks.lpop("list");

        // list should be auto-deleted
        assert_eq!(ks.len(), 0);
        assert!(!ks.exists("list"));
    }

    #[test]
    fn lrange_inverted_start_stop_returns_empty() {
        let mut ks = Keyspace::new();
        ks.lpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        )
        .unwrap();

        // start > stop with positive indices
        let result = ks.lrange("list", 2, 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn lrange_large_stop_clamps_to_len() {
        let mut ks = Keyspace::new();
        ks.lpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();

        // large indices should clamp to list bounds
        let result = ks.lrange("list", 0, 1000).unwrap();
        assert_eq!(result.len(), 2);
    }

    // --- lindex ---

    #[test]
    fn lindex_returns_element() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        )
        .unwrap();
        assert_eq!(ks.lindex("list", 0).unwrap(), Some(Bytes::from("a")));
        assert_eq!(ks.lindex("list", 2).unwrap(), Some(Bytes::from("c")));
    }

    #[test]
    fn lindex_negative() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        )
        .unwrap();
        assert_eq!(ks.lindex("list", -1).unwrap(), Some(Bytes::from("c")));
        assert_eq!(ks.lindex("list", -3).unwrap(), Some(Bytes::from("a")));
    }

    #[test]
    fn lindex_out_of_bounds() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a")]).unwrap();
        assert_eq!(ks.lindex("list", 1).unwrap(), None);
        assert_eq!(ks.lindex("list", -2).unwrap(), None);
        assert_eq!(ks.lindex("list", 100).unwrap(), None);
    }

    #[test]
    fn lindex_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.lindex("nope", 0).unwrap(), None);
    }

    #[test]
    fn lindex_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.lindex("s", 0).is_err());
    }

    // --- lset ---

    #[test]
    fn lset_replaces_element() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        )
        .unwrap();
        ks.lset("list", 1, Bytes::from("B")).unwrap();
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(
            items,
            vec![Bytes::from("a"), Bytes::from("B"), Bytes::from("c")]
        );
    }

    #[test]
    fn lset_negative_index() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        ks.lset("list", -1, Bytes::from("B")).unwrap();
        assert_eq!(ks.lindex("list", 1).unwrap(), Some(Bytes::from("B")));
    }

    #[test]
    fn lset_out_of_range() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a")]).unwrap();
        assert_eq!(
            ks.lset("list", 5, Bytes::from("x")),
            Err(LsetError::IndexOutOfRange)
        );
    }

    #[test]
    fn lset_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(
            ks.lset("nope", 0, Bytes::from("x")),
            Err(LsetError::NoSuchKey)
        );
    }

    #[test]
    fn lset_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert_eq!(
            ks.lset("s", 0, Bytes::from("x")),
            Err(LsetError::WrongType)
        );
    }

    #[test]
    fn lset_tracks_memory() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("short")]).unwrap();
        let before = ks.stats().used_bytes;
        ks.lset("list", 0, Bytes::from("a much longer value"))
            .unwrap();
        let after = ks.stats().used_bytes;
        assert!(after > before);
    }

    // --- ltrim ---

    #[test]
    fn ltrim_keeps_range() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("c"),
                Bytes::from("d"),
            ],
        )
        .unwrap();
        ks.ltrim("list", 1, 2).unwrap();
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(items, vec![Bytes::from("b"), Bytes::from("c")]);
    }

    #[test]
    fn ltrim_negative_indices() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        )
        .unwrap();
        ks.ltrim("list", -2, -1).unwrap();
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(items, vec![Bytes::from("b"), Bytes::from("c")]);
    }

    #[test]
    fn ltrim_inverted_range_deletes() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        ks.ltrim("list", 2, 0).unwrap();
        assert!(!ks.exists("list"));
        assert_eq!(ks.stats().key_count, 0);
    }

    #[test]
    fn ltrim_out_of_bounds_clamps() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        ks.ltrim("list", -100, 100).unwrap();
        assert_eq!(ks.llen("list").unwrap(), 2);
    }

    #[test]
    fn ltrim_missing_key_noop() {
        let mut ks = Keyspace::new();
        ks.ltrim("nope", 0, 1).unwrap();
    }

    #[test]
    fn ltrim_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.ltrim("s", 0, 1).is_err());
    }

    #[test]
    fn ltrim_tracks_memory() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("c"),
                Bytes::from("d"),
            ],
        )
        .unwrap();
        let before = ks.stats().used_bytes;
        ks.ltrim("list", 0, 0).unwrap(); // keep only first element
        let after = ks.stats().used_bytes;
        assert!(after < before);
    }

    // --- linsert ---

    #[test]
    fn linsert_before() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("c")])
            .unwrap();
        let len = ks
            .linsert("list", true, b"c", Bytes::from("b"))
            .unwrap();
        assert_eq!(len, 3);
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(
            items,
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
    }

    #[test]
    fn linsert_after() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("c")])
            .unwrap();
        let len = ks
            .linsert("list", false, b"a", Bytes::from("b"))
            .unwrap();
        assert_eq!(len, 3);
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(
            items,
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
    }

    #[test]
    fn linsert_pivot_not_found() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a")]).unwrap();
        assert_eq!(
            ks.linsert("list", true, b"missing", Bytes::from("x"))
                .unwrap(),
            -1
        );
    }

    #[test]
    fn linsert_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(
            ks.linsert("nope", true, b"a", Bytes::from("x")).unwrap(),
            0
        );
    }

    #[test]
    fn linsert_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.linsert("s", true, b"a", Bytes::from("x")).is_err());
    }

    // --- lrem ---

    #[test]
    fn lrem_positive_count_from_head() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("a"),
                Bytes::from("c"),
                Bytes::from("a"),
            ],
        )
        .unwrap();
        let removed = ks.lrem("list", 2, b"a").unwrap();
        assert_eq!(removed, 2);
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(
            items,
            vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("a")]
        );
    }

    #[test]
    fn lrem_negative_count_from_tail() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("a"),
                Bytes::from("c"),
                Bytes::from("a"),
            ],
        )
        .unwrap();
        let removed = ks.lrem("list", -2, b"a").unwrap();
        assert_eq!(removed, 2);
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(
            items,
            vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
        );
    }

    #[test]
    fn lrem_zero_removes_all() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("a")],
        )
        .unwrap();
        let removed = ks.lrem("list", 0, b"a").unwrap();
        assert_eq!(removed, 2);
        let items = ks.lrange("list", 0, -1).unwrap();
        assert_eq!(items, vec![Bytes::from("b")]);
    }

    #[test]
    fn lrem_no_matches() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(ks.lrem("list", 0, b"x").unwrap(), 0);
    }

    #[test]
    fn lrem_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.lrem("nope", 0, b"x").unwrap(), 0);
    }

    #[test]
    fn lrem_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.lrem("s", 0, b"x").is_err());
    }

    #[test]
    fn lrem_empties_list_deletes_key() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a"), Bytes::from("a")])
            .unwrap();
        ks.lrem("list", 0, b"a").unwrap();
        assert!(!ks.exists("list"));
        assert_eq!(ks.stats().key_count, 0);
        assert_eq!(ks.stats().used_bytes, 0);
    }

    // --- lpos ---

    #[test]
    fn lpos_finds_first_match() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("a")],
        )
        .unwrap();
        let result = ks.lpos("list", b"a", 1, 1, 0).unwrap();
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn lpos_with_count_all() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("a"),
                Bytes::from("c"),
                Bytes::from("a"),
            ],
        )
        .unwrap();
        let result = ks.lpos("list", b"a", 1, 0, 0).unwrap();
        assert_eq!(result, vec![0, 2, 4]);
    }

    #[test]
    fn lpos_with_rank() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("a")],
        )
        .unwrap();
        // rank 2 = second match from head
        let result = ks.lpos("list", b"a", 2, 1, 0).unwrap();
        assert_eq!(result, vec![2]);
    }

    #[test]
    fn lpos_negative_rank() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[Bytes::from("a"), Bytes::from("b"), Bytes::from("a")],
        )
        .unwrap();
        // rank -1 = last match
        let result = ks.lpos("list", b"a", -1, 1, 0).unwrap();
        assert_eq!(result, vec![2]);
    }

    #[test]
    fn lpos_with_maxlen() {
        let mut ks = Keyspace::new();
        ks.rpush(
            "list",
            &[
                Bytes::from("b"),
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("a"),
            ],
        )
        .unwrap();
        // maxlen=2: only scan first 2 elements
        let result = ks.lpos("list", b"a", 1, 0, 2).unwrap();
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn lpos_no_match() {
        let mut ks = Keyspace::new();
        ks.rpush("list", &[Bytes::from("a")]).unwrap();
        let result = ks.lpos("list", b"x", 1, 1, 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn lpos_missing_key() {
        let mut ks = Keyspace::new();
        let result = ks.lpos("nope", b"a", 1, 1, 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn lpos_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.lpos("s", b"a", 1, 1, 0).is_err());
    }
}
