use super::*;

impl Keyspace {
    /// Adds members with scores to a sorted set, with optional ZADD flags.
    ///
    /// Creates the sorted set if the key doesn't exist. Returns a
    /// `ZAddResult` containing the count for the client response and the
    /// list of members that were actually applied (for AOF correctness).
    /// Returns `Err(WriteError::WrongType)` on type mismatch, or
    /// `Err(WriteError::OutOfMemory)` if the memory limit is reached.
    pub fn zadd(
        &mut self,
        key: &str,
        members: &[(f64, String)],
        flags: &ZAddFlags,
    ) -> Result<ZAddResult, WriteError> {
        self.remove_if_expired(key);

        let is_new = self.ensure_collection_type(key, |v| matches!(v, Value::SortedSet(_)))?;

        // worst-case estimate: assume all members are new
        let member_increase: usize = members
            .iter()
            .map(|(_, m)| SortedSet::estimated_member_cost(m))
            .sum();
        self.reserve_memory(is_new, key, SortedSet::BASE_OVERHEAD, member_increase)?;

        if is_new {
            self.insert_empty(key, Value::SortedSet(Box::default()));
        }

        let (count, applied) = self
            .track_size(key, |entry| {
                let Value::SortedSet(ref mut ss) = entry.value else {
                    unreachable!("type verified by ensure_collection_type");
                };
                let mut count = 0;
                let mut applied = Vec::new();
                for (score, member) in members {
                    let result = ss.add_with_flags(member.clone(), *score, flags);
                    if result.added || result.updated {
                        applied.push((*score, member.clone()));
                    }
                    if flags.ch {
                        if result.added || result.updated {
                            count += 1;
                        }
                    } else if result.added {
                        count += 1;
                    }
                }
                entry.touch();
                (count, applied)
            })
            .unwrap_or_default();

        // clean up if the set is still empty (e.g. XX flag on a new key)
        if let Some(entry) = self.entries.get(key) {
            if matches!(&entry.value, Value::SortedSet(ss) if ss.is_empty()) {
                self.memory.remove_with_size(entry.entry_size(key));
                self.entries.remove(key);
            }
        }

        Ok(ZAddResult { count, applied })
    }

    /// Removes members from a sorted set. Returns the names of members
    /// that were actually removed (for AOF correctness). Deletes the key
    /// if the set becomes empty.
    ///
    /// Returns `Err(WrongType)` if the key holds a non-sorted-set value.
    pub fn zrem(&mut self, key: &str, members: &[String]) -> Result<Vec<String>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }

        let Some(entry) = self.entries.get(key) else {
            return Ok(vec![]);
        };
        if !matches!(entry.value, Value::SortedSet(_)) {
            return Err(WrongType);
        }

        let Some(entry) = self.entries.get_mut(key) else {
            return Ok(vec![]);
        };
        let old_entry_size = entry.entry_size(key);
        let mut removed = Vec::new();
        let mut removed_bytes: usize = 0;
        if let Value::SortedSet(ref mut ss) = entry.value {
            for member in members {
                if ss.remove(member) {
                    removed_bytes += SortedSet::estimated_member_cost(member);
                    removed.push(member.clone());
                }
            }
        }
        entry.touch();

        let is_empty = matches!(&entry.value, Value::SortedSet(ss) if ss.is_empty());
        self.cleanup_after_remove(key, old_entry_size, is_empty, removed_bytes);

        Ok(removed)
    }

    /// Returns the score for a member in a sorted set.
    ///
    /// Returns `Ok(None)` if the key or member doesn't exist.
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn zscore(&mut self, key: &str, member: &str) -> Result<Option<f64>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }
        match self.entries.get_mut(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                Value::SortedSet(ss) => {
                    let score = ss.score(member);
                    entry.touch();
                    Ok(score)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Returns the 0-based rank of a member in a sorted set (lowest score = 0).
    ///
    /// Returns `Ok(None)` if the key or member doesn't exist.
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn zrank(&mut self, key: &str, member: &str) -> Result<Option<usize>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }
        match self.entries.get_mut(key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                Value::SortedSet(ss) => {
                    let rank = ss.rank(member);
                    entry.touch();
                    Ok(rank)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Returns a range of members from a sorted set by rank.
    ///
    /// Supports negative indices. If `with_scores` is true, the result
    /// includes `(member, score)` pairs; otherwise just members.
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn zrange(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<(String, f64)>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }
        match self.entries.get_mut(key) {
            None => Ok(vec![]),
            Some(entry) => {
                let result = match &entry.value {
                    Value::SortedSet(ss) => {
                        let items = ss.range_by_rank(start, stop);
                        Ok(items.into_iter().map(|(m, s)| (m.to_owned(), s)).collect())
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

    /// Returns the number of members in a sorted set, or 0 if the key doesn't exist.
    ///
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn zcard(&mut self, key: &str) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }
        match self.entries.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                Value::SortedSet(ss) => Ok(ss.len()),
                _ => Err(WrongType),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zadd_creates_sorted_set() {
        let mut ks = Keyspace::new();
        let result = ks
            .zadd(
                "board",
                &[(100.0, "alice".into()), (200.0, "bob".into())],
                &ZAddFlags::default(),
            )
            .unwrap();
        assert_eq!(result.count, 2);
        assert_eq!(result.applied.len(), 2);
        assert_eq!(ks.value_type("board"), "zset");
    }

    #[test]
    fn zadd_updates_existing_score() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(100.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        // update score — default flags don't count updates
        let result = ks
            .zadd("z", &[(200.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        assert_eq!(result.count, 0);
        // score was updated, so applied should have the member
        assert_eq!(result.applied.len(), 1);
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(200.0));
    }

    #[test]
    fn zadd_ch_flag_counts_changes() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(100.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        let flags = ZAddFlags {
            ch: true,
            ..Default::default()
        };
        let result = ks
            .zadd(
                "z",
                &[(200.0, "alice".into()), (50.0, "bob".into())],
                &flags,
            )
            .unwrap();
        // 1 updated + 1 added = 2
        assert_eq!(result.count, 2);
        assert_eq!(result.applied.len(), 2);
    }

    #[test]
    fn zadd_nx_skips_existing() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(100.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        let flags = ZAddFlags {
            nx: true,
            ..Default::default()
        };
        let result = ks.zadd("z", &[(999.0, "alice".into())], &flags).unwrap();
        assert_eq!(result.count, 0);
        assert!(result.applied.is_empty());
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(100.0));
    }

    #[test]
    fn zadd_xx_skips_new() {
        let mut ks = Keyspace::new();
        let flags = ZAddFlags {
            xx: true,
            ..Default::default()
        };
        let result = ks.zadd("z", &[(100.0, "alice".into())], &flags).unwrap();
        assert_eq!(result.count, 0);
        assert!(result.applied.is_empty());
        // key should be cleaned up since nothing was added
        assert_eq!(ks.value_type("z"), "none");
    }

    #[test]
    fn zadd_gt_only_increases() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(100.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        let flags = ZAddFlags {
            gt: true,
            ..Default::default()
        };
        ks.zadd("z", &[(50.0, "alice".into())], &flags).unwrap();
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(100.0));
        ks.zadd("z", &[(200.0, "alice".into())], &flags).unwrap();
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(200.0));
    }

    #[test]
    fn zadd_lt_only_decreases() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(100.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        let flags = ZAddFlags {
            lt: true,
            ..Default::default()
        };
        ks.zadd("z", &[(200.0, "alice".into())], &flags).unwrap();
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(100.0));
        ks.zadd("z", &[(50.0, "alice".into())], &flags).unwrap();
        assert_eq!(ks.zscore("z", "alice").unwrap(), Some(50.0));
    }

    #[test]
    fn zrem_removes_members() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into()), (3.0, "c".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        let removed = ks
            .zrem("z", &["a".into(), "c".into(), "nonexistent".into()])
            .unwrap();
        assert_eq!(removed.len(), 2);
        assert!(removed.contains(&"a".to_owned()));
        assert!(removed.contains(&"c".to_owned()));
        assert_eq!(ks.zscore("z", "a").unwrap(), None);
        assert_eq!(ks.zscore("z", "b").unwrap(), Some(2.0));
    }

    #[test]
    fn zrem_auto_deletes_empty() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(1.0, "only".into())], &ZAddFlags::default())
            .unwrap();
        ks.zrem("z", &["only".into()]).unwrap();
        assert!(!ks.exists("z"));
        assert_eq!(ks.stats().key_count, 0);
    }

    #[test]
    fn zrem_missing_key() {
        let mut ks = Keyspace::new();
        assert!(ks.zrem("nope", &["a".into()]).unwrap().is_empty());
    }

    #[test]
    fn zscore_returns_score() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(42.5, "member".into())], &ZAddFlags::default())
            .unwrap();
        assert_eq!(ks.zscore("z", "member").unwrap(), Some(42.5));
        assert_eq!(ks.zscore("z", "missing").unwrap(), None);
    }

    #[test]
    fn zscore_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.zscore("nope", "m").unwrap(), None);
    }

    #[test]
    fn zrank_returns_rank() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[
                (300.0, "c".into()),
                (100.0, "a".into()),
                (200.0, "b".into()),
            ],
            &ZAddFlags::default(),
        )
        .unwrap();
        assert_eq!(ks.zrank("z", "a").unwrap(), Some(0));
        assert_eq!(ks.zrank("z", "b").unwrap(), Some(1));
        assert_eq!(ks.zrank("z", "c").unwrap(), Some(2));
        assert_eq!(ks.zrank("z", "d").unwrap(), None);
    }

    #[test]
    fn zrange_returns_range() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into()), (3.0, "c".into())],
            &ZAddFlags::default(),
        )
        .unwrap();

        let all = ks.zrange("z", 0, -1).unwrap();
        assert_eq!(
            all,
            vec![
                ("a".to_owned(), 1.0),
                ("b".to_owned(), 2.0),
                ("c".to_owned(), 3.0),
            ]
        );

        let middle = ks.zrange("z", 1, 1).unwrap();
        assert_eq!(middle, vec![("b".to_owned(), 2.0)]);

        let last_two = ks.zrange("z", -2, -1).unwrap();
        assert_eq!(last_two, vec![("b".to_owned(), 2.0), ("c".to_owned(), 3.0)]);
    }

    #[test]
    fn zrange_missing_key() {
        let mut ks = Keyspace::new();
        assert!(ks.zrange("nope", 0, -1).unwrap().is_empty());
    }

    #[test]
    fn zadd_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks
            .zadd("s", &[(1.0, "m".into())], &ZAddFlags::default())
            .is_err());
    }

    #[test]
    fn zrem_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.zrem("s", &["m".into()]).is_err());
    }

    #[test]
    fn zscore_on_list_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.rpush("l", &[Bytes::from("item")]).unwrap();
        assert!(ks.zscore("l", "m").is_err());
    }

    #[test]
    fn zrank_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.zrank("s", "m").is_err());
    }

    #[test]
    fn zrange_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.zrange("s", 0, -1).is_err());
    }

    #[test]
    fn sorted_set_memory_tracked() {
        let mut ks = Keyspace::new();
        let before = ks.stats().used_bytes;
        ks.zadd("z", &[(1.0, "alice".into())], &ZAddFlags::default())
            .unwrap();
        let after_add = ks.stats().used_bytes;
        assert!(after_add > before);

        ks.zadd("z", &[(2.0, "bob".into())], &ZAddFlags::default())
            .unwrap();
        let after_second = ks.stats().used_bytes;
        assert!(after_second > after_add);

        ks.zrem("z", &["alice".into()]).unwrap();
        let after_remove = ks.stats().used_bytes;
        assert!(after_remove < after_second);
    }

    #[test]
    fn zrem_returns_actually_removed_members() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        // "a" exists, "ghost" doesn't — only "a" should be in the result
        let removed = ks.zrem("z", &["a".into(), "ghost".into()]).unwrap();
        assert_eq!(removed, vec!["a".to_owned()]);
    }

    #[test]
    fn zcard_returns_count() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        assert_eq!(ks.zcard("z").unwrap(), 2);
    }

    #[test]
    fn zcard_missing_key_returns_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.zcard("missing").unwrap(), 0);
    }

    #[test]
    fn zcard_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.zcard("s").is_err());
    }

    #[test]
    fn zadd_rejects_when_memory_full() {
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

        let result = ks.zadd("z", &[(1.0, "member".into())], &ZAddFlags::default());
        assert!(matches!(result, Err(WriteError::OutOfMemory)));

        // original key should be untouched
        assert!(ks.exists("a"));
    }
}
