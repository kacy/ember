use super::*;

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

        // apply the known delta — no need to rescan the entire list
        self.memory.grow_by(element_increase);

        Ok(len)
    }

    /// Internal pop implementation shared by lpop/rpop.
    pub(super) fn list_pop(
        &mut self,
        key: &str,
        left: bool,
    ) -> Result<Option<Bytes>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }

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
}
