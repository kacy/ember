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

        let track_access = self.track_access;
        let (count, applied) = self
            .track_size(key, |entry| {
                let Value::SortedSet(ref mut ss) = entry.value else {
                    unreachable!("type verified by ensure_collection_type");
                };
                let mut count = 0;
                let mut applied = Vec::new();
                for (score, member) in members {
                    let result = ss.add_with_flags(member, *score, flags);
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
                entry.touch(track_access);
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
        let track_access = self.track_access;
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
        entry.touch(track_access);

        let is_empty = matches!(&entry.value, Value::SortedSet(ss) if ss.is_empty());
        self.cleanup_after_remove(key, old_entry_size, is_empty, removed_bytes);

        if !removed.is_empty() {
            self.bump_version(key);
        }

        Ok(removed)
    }

    /// Returns the score for a member in a sorted set.
    ///
    /// Returns `Ok(None)` if the key or member doesn't exist.
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn zscore(&mut self, key: &str, member: &str) -> Result<Option<f64>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(None);
        };
        match &entry.value {
            Value::SortedSet(ss) => Ok(ss.score(member)),
            _ => Err(WrongType),
        }
    }

    /// Returns the 0-based rank of a member in a sorted set (lowest score = 0).
    ///
    /// Returns `Ok(None)` if the key or member doesn't exist.
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn zrank(&mut self, key: &str, member: &str) -> Result<Option<usize>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(None);
        };
        match &entry.value {
            Value::SortedSet(ss) => Ok(ss.rank(member)),
            _ => Err(WrongType),
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
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(vec![]);
        };
        match &entry.value {
            Value::SortedSet(ss) => {
                let items = ss.range_by_rank(start, stop);
                Ok(items.into_iter().map(|(m, s)| (m.to_owned(), s)).collect())
            }
            _ => Err(WrongType),
        }
    }

    /// Incrementally iterates members of a sorted set.
    ///
    /// Returns the next cursor and a batch of member-score pairs. A returned
    /// cursor of `0` means the iteration is complete. Pattern matching
    /// (MATCH) filters on member names.
    pub fn scan_sorted_set(
        &mut self,
        key: &str,
        cursor: u64,
        count: usize,
        pattern: Option<&str>,
    ) -> Result<(u64, Vec<(String, f64)>), WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok((0, vec![]));
        };
        let Value::SortedSet(ref ss) = entry.value else {
            return Err(WrongType);
        };

        let target = if count == 0 { 10 } else { count };
        let compiled = pattern.map(GlobPattern::new);
        let mut result = Vec::with_capacity(target);
        let mut pos = 0u64;
        let mut done = true;

        for (member, score) in ss.iter() {
            if pos < cursor {
                pos += 1;
                continue;
            }
            if let Some(ref pat) = compiled {
                if !pat.matches(member) {
                    pos += 1;
                    continue;
                }
            }
            result.push((member.to_owned(), score));
            pos += 1;
            if result.len() >= target {
                done = false;
                break;
            }
        }

        Ok(if done { (0, result) } else { (pos, result) })
    }

    /// Returns the reverse rank of a member (highest score = 0).
    ///
    /// Returns `Ok(None)` if the key or member doesn't exist.
    /// Returns `Err(WrongType)` on type mismatch.
    pub fn zrevrank(&mut self, key: &str, member: &str) -> Result<Option<usize>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(None);
        };
        match &entry.value {
            Value::SortedSet(ss) => Ok(ss.rev_rank(member)),
            _ => Err(WrongType),
        }
    }

    /// Returns a range of members in reverse rank order (highest first).
    pub fn zrevrange(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<(String, f64)>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(vec![]);
        };
        match &entry.value {
            Value::SortedSet(ss) => {
                let items = ss.rev_range_by_rank(start, stop);
                Ok(items.into_iter().map(|(m, s)| (m.to_owned(), s)).collect())
            }
            _ => Err(WrongType),
        }
    }

    /// Counts members with scores in the given bounds.
    pub fn zcount(
        &mut self,
        key: &str,
        min: ScoreBound,
        max: ScoreBound,
    ) -> Result<usize, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(0);
        };
        match &entry.value {
            Value::SortedSet(ss) => Ok(ss.count_by_score(min, max)),
            _ => Err(WrongType),
        }
    }

    /// Increments the score of a member in a sorted set. If the member
    /// doesn't exist, it is added with the increment as its score.
    /// If the key doesn't exist, a new sorted set is created.
    ///
    /// Returns the new score.
    pub fn zincrby(&mut self, key: &str, increment: f64, member: &str) -> Result<f64, WriteError> {
        self.remove_if_expired(key);

        let is_new = self.ensure_collection_type(key, |v| matches!(v, Value::SortedSet(_)))?;

        // worst case: member is new
        self.reserve_memory(
            is_new,
            key,
            SortedSet::BASE_OVERHEAD,
            SortedSet::estimated_member_cost(member),
        )?;

        if is_new {
            self.insert_empty(key, Value::SortedSet(Box::default()));
        }

        let track_access = self.track_access;
        let new_score = self
            .track_size(key, |entry| {
                let Value::SortedSet(ref mut ss) = entry.value else {
                    unreachable!("type verified by ensure_collection_type");
                };
                let score = ss.incr(member, increment);
                entry.touch(track_access);
                score
            })
            .unwrap_or(increment);

        Ok(new_score)
    }

    /// Returns members with scores in the given range, in ascending order.
    pub fn zrangebyscore(
        &mut self,
        key: &str,
        min: ScoreBound,
        max: ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Result<Vec<(String, f64)>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(vec![]);
        };
        match &entry.value {
            Value::SortedSet(ss) => {
                let items = ss.range_by_score(min, max, offset, count);
                Ok(items.into_iter().map(|(m, s)| (m.to_owned(), s)).collect())
            }
            _ => Err(WrongType),
        }
    }

    /// Returns members with scores in the given range, in descending order.
    pub fn zrevrangebyscore(
        &mut self,
        key: &str,
        min: ScoreBound,
        max: ScoreBound,
        offset: usize,
        count: Option<usize>,
    ) -> Result<Vec<(String, f64)>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(vec![]);
        };
        match &entry.value {
            Value::SortedSet(ss) => {
                let items = ss.rev_range_by_score(min, max, offset, count);
                Ok(items.into_iter().map(|(m, s)| (m.to_owned(), s)).collect())
            }
            _ => Err(WrongType),
        }
    }

    /// Removes and returns up to `count` members with the lowest scores.
    /// Deletes the key if the set becomes empty.
    pub fn zpopmin(&mut self, key: &str, count: usize) -> Result<Vec<(String, f64)>, WrongType> {
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
        let track_access = self.track_access;
        let mut removed_bytes = 0usize;
        let popped = if let Value::SortedSet(ref mut ss) = entry.value {
            let items = ss.pop_min(count);
            for (member, _) in &items {
                removed_bytes += SortedSet::estimated_member_cost(member);
            }
            items
        } else {
            vec![]
        };

        if !popped.is_empty() {
            entry.touch(track_access);
        }

        let is_empty = matches!(&entry.value, Value::SortedSet(ss) if ss.is_empty());
        self.cleanup_after_remove(key, old_entry_size, is_empty, removed_bytes);

        if !popped.is_empty() {
            self.bump_version(key);
        }

        Ok(popped)
    }

    /// Removes and returns up to `count` members with the highest scores.
    /// Deletes the key if the set becomes empty.
    pub fn zpopmax(&mut self, key: &str, count: usize) -> Result<Vec<(String, f64)>, WrongType> {
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
        let track_access = self.track_access;
        let mut removed_bytes = 0usize;
        let popped = if let Value::SortedSet(ref mut ss) = entry.value {
            let items = ss.pop_max(count);
            for (member, _) in &items {
                removed_bytes += SortedSet::estimated_member_cost(member);
            }
            items
        } else {
            vec![]
        };

        if !popped.is_empty() {
            entry.touch(track_access);
        }

        let is_empty = matches!(&entry.value, Value::SortedSet(ss) if ss.is_empty());
        self.cleanup_after_remove(key, old_entry_size, is_empty, removed_bytes);

        if !popped.is_empty() {
            self.bump_version(key);
        }

        Ok(popped)
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

    /// Returns members in the first sorted set that are not in any of the others.
    ///
    /// Missing keys are treated as empty sets. Results are ordered by score
    /// then by member name.
    pub fn zdiff(&mut self, keys: &[String]) -> Result<Vec<(String, f64)>, WrongType> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        for key in keys {
            self.remove_if_expired(key);
        }
        let first: Vec<(String, f64)> = match self.entries.get(keys[0].as_str()) {
            None => return Ok(vec![]),
            Some(e) => match &e.value {
                Value::SortedSet(ss) => ss.iter().map(|(m, s)| (m.to_owned(), s)).collect(),
                _ => return Err(WrongType),
            },
        };
        let mut excluded: AHashMap<String, ()> = AHashMap::new();
        for key in &keys[1..] {
            match self.entries.get(key.as_str()) {
                None => {}
                Some(e) => match &e.value {
                    Value::SortedSet(ss) => {
                        for (member, _) in ss.iter() {
                            excluded.insert(member.to_owned(), ());
                        }
                    }
                    _ => return Err(WrongType),
                },
            }
        }
        Ok(first
            .into_iter()
            .filter(|(m, _)| !excluded.contains_key(m))
            .collect())
    }

    /// Returns members present in all of the given sorted sets, with scores summed.
    ///
    /// If any key is missing the result is empty. Results are ordered by
    /// score then member name.
    pub fn zinter(&mut self, keys: &[String]) -> Result<Vec<(String, f64)>, WrongType> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        for key in keys {
            self.remove_if_expired(key);
        }
        let mut candidates: Vec<(String, f64)> = match self.entries.get(keys[0].as_str()) {
            None => return Ok(vec![]),
            Some(e) => match &e.value {
                Value::SortedSet(ss) => ss.iter().map(|(m, s)| (m.to_owned(), s)).collect(),
                _ => return Err(WrongType),
            },
        };
        for key in &keys[1..] {
            match self.entries.get(key.as_str()) {
                None => return Ok(vec![]),
                Some(e) => match &e.value {
                    Value::SortedSet(ss) => {
                        let lookup: AHashMap<String, f64> =
                            ss.iter().map(|(m, s)| (m.to_owned(), s)).collect();
                        candidates = candidates
                            .into_iter()
                            .filter_map(|(m, score)| lookup.get(&m).map(|&s| (m, score + s)))
                            .collect();
                    }
                    _ => return Err(WrongType),
                },
            }
        }
        candidates.sort_by(|(am, as_), (bm, bs)| {
            as_.partial_cmp(bs)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| am.cmp(bm))
        });
        Ok(candidates)
    }

    /// Returns the union of all given sorted sets, with scores summed across keys.
    ///
    /// Missing keys contribute no members. Results are ordered by score then
    /// member name.
    pub fn zunion(&mut self, keys: &[String]) -> Result<Vec<(String, f64)>, WrongType> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        for key in keys {
            self.remove_if_expired(key);
        }
        let mut totals: AHashMap<String, f64> = AHashMap::new();
        for key in keys {
            match self.entries.get(key.as_str()) {
                None => {}
                Some(e) => match &e.value {
                    Value::SortedSet(ss) => {
                        for (member, score) in ss.iter() {
                            *totals.entry(member.to_owned()).or_insert(0.0) += score;
                        }
                    }
                    _ => return Err(WrongType),
                },
            }
        }
        let mut result: Vec<(String, f64)> = totals.into_iter().collect();
        result.sort_by(|(am, as_), (bm, bs)| {
            as_.partial_cmp(bs)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| am.cmp(bm))
        });
        Ok(result)
    }

    /// Computes the diff of sorted sets and stores the result in `dest`.
    ///
    /// Equivalent to calling `zdiff` then writing the result to `dest`.
    /// Replaces any existing value at `dest`. Returns the count of stored
    /// members and the scored pairs (for AOF persistence).
    pub fn zdiffstore(
        &mut self,
        dest: &str,
        keys: &[String],
    ) -> Result<(usize, Vec<(f64, String)>), WrongType> {
        let members = self.zdiff(keys)?;
        self.zstore_result(dest, members)
    }

    /// Computes the intersection of sorted sets and stores the result in `dest`.
    ///
    /// Equivalent to calling `zinter` then writing the result to `dest`.
    /// Replaces any existing value at `dest`. Returns the count of stored
    /// members and the scored pairs (for AOF persistence).
    pub fn zinterstore(
        &mut self,
        dest: &str,
        keys: &[String],
    ) -> Result<(usize, Vec<(f64, String)>), WrongType> {
        let members = self.zinter(keys)?;
        self.zstore_result(dest, members)
    }

    /// Computes the union of sorted sets and stores the result in `dest`.
    ///
    /// Equivalent to calling `zunion` then writing the result to `dest`.
    /// Replaces any existing value at `dest`. Returns the count of stored
    /// members and the scored pairs (for AOF persistence).
    pub fn zunionstore(
        &mut self,
        dest: &str,
        keys: &[String],
    ) -> Result<(usize, Vec<(f64, String)>), WrongType> {
        let members = self.zunion(keys)?;
        self.zstore_result(dest, members)
    }

    /// Writes a computed sorted set result to `dest`, replacing any existing key.
    ///
    /// Returns the cardinality and the stored members (score, member) pairs for AOF.
    fn zstore_result(
        &mut self,
        dest: &str,
        members: Vec<(String, f64)>,
    ) -> Result<(usize, Vec<(f64, String)>), WrongType> {
        // remove any existing entry at dest (any type)
        self.remove_if_expired(dest);
        if let Some(old) = self.entries.remove(dest) {
            self.memory.remove(dest, &old.value);
            self.decrement_expiry_if_set(&old);
            self.defer_drop(old.value);
        }

        let count = members.len();
        if count == 0 {
            return Ok((0, vec![]));
        }

        let mut ss = SortedSet::default();
        let flags = ZAddFlags::default();
        for (member, score) in &members {
            ss.add_with_flags(member, *score, &flags);
        }

        let value = Value::SortedSet(Box::new(ss));
        self.memory.add(dest, &value);
        let entry = Entry::new(value, None);
        self.entries.insert(CompactString::from(dest), entry);
        self.bump_version(dest);

        // return as (score, member) to match the ZAdd AOF record convention
        let stored: Vec<(f64, String)> = members.into_iter().map(|(m, s)| (s, m)).collect();
        Ok((count, stored))
    }

    /// Returns random member(s) from a sorted set.
    ///
    /// - `count = None`: return one random member as a single string (no score)
    /// - `count > 0`: return up to count distinct members
    /// - `count < 0`: return |count| members, allowing duplicates
    ///
    /// If `with_scores` is true and count is `Some`, returns `(member, Some(score))` pairs.
    /// When `count` is `None`, the score field is always `None`.
    pub fn zrandmember(
        &mut self,
        key: &str,
        count: Option<i64>,
        with_scores: bool,
    ) -> Result<Vec<(String, Option<f64>)>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(vec![]);
        };
        let Value::SortedSet(ref zset) = entry.value else {
            return Err(WrongType);
        };
        if zset.is_empty() {
            return Ok(vec![]);
        }

        // collect into a vec for indexed random access
        let members: Vec<(&str, f64)> = zset.iter().collect();
        let mut rng = rand::rng();

        let result = match count {
            None => {
                // single random member — no score even if with_scores is set
                use rand::seq::IteratorRandom;
                members
                    .iter()
                    .choose(&mut rng)
                    .map(|(m, _)| ((*m).to_owned(), None))
                    .into_iter()
                    .collect()
            }
            Some(n) if n > 0 => {
                use rand::seq::IteratorRandom;
                let n = (n as usize).min(members.len());
                members
                    .iter()
                    .choose_multiple(&mut rng, n)
                    .into_iter()
                    .map(|(m, s)| {
                        let score = if with_scores { Some(*s) } else { None };
                        ((*m).to_owned(), score)
                    })
                    .collect()
            }
            Some(n) => {
                // negative count: allow duplicates, return |n| entries
                use rand::Rng;
                let n = n.unsigned_abs() as usize;
                (0..n)
                    .map(|_| {
                        let idx = rng.random_range(0..members.len());
                        let (m, s) = members[idx];
                        let score = if with_scores { Some(s) } else { None };
                        (m.to_owned(), score)
                    })
                    .collect()
            }
        };

        Ok(result)
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

    // --- scan_sorted_set ---

    #[test]
    fn scan_zset_returns_all() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into()), (3.0, "c".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        let (cursor, members) = ks.scan_sorted_set("z", 0, 100, None).unwrap();
        assert_eq!(cursor, 0);
        assert_eq!(members.len(), 3);
        // sorted set iteration is score-ordered
        assert_eq!(members[0].0, "a");
        assert_eq!(members[2].0, "c");
    }

    #[test]
    fn scan_zset_missing_key() {
        let mut ks = Keyspace::new();
        let (cursor, members) = ks.scan_sorted_set("missing", 0, 10, None).unwrap();
        assert_eq!(cursor, 0);
        assert!(members.is_empty());
    }

    #[test]
    fn scan_zset_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("z".into(), Bytes::from("string"), None, false, false);
        assert!(ks.scan_sorted_set("z", 0, 10, None).is_err());
    }

    #[test]
    fn scan_zset_with_pattern() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[
                (1.0, "player:1".into()),
                (2.0, "player:2".into()),
                (3.0, "enemy:1".into()),
            ],
            &ZAddFlags::default(),
        )
        .unwrap();
        let (_, members) = ks.scan_sorted_set("z", 0, 100, Some("player:*")).unwrap();
        assert_eq!(members.len(), 2);
        assert!(members.iter().all(|(m, _)| m.starts_with("player:")));
    }

    #[test]
    fn scan_zset_pagination() {
        let mut ks = Keyspace::new();
        let items: Vec<(f64, String)> = (0..20).map(|i| (i as f64, format!("m{i}"))).collect();
        ks.zadd("z", &items, &ZAddFlags::default()).unwrap();

        let mut collected = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (next, batch) = ks.scan_sorted_set("z", cursor, 5, None).unwrap();
            collected.extend(batch);
            if next == 0 {
                break;
            }
            cursor = next;
        }
        assert_eq!(collected.len(), 20);
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

    #[test]
    fn zdiff_returns_members_unique_to_first() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "a",
            &[(1.0, "x".into()), (2.0, "y".into()), (3.0, "z".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        ks.zadd(
            "b",
            &[(1.0, "y".into()), (1.0, "w".into())],
            &ZAddFlags::default(),
        )
        .unwrap();

        let keys = vec!["a".to_owned(), "b".to_owned()];
        let diff = ks.zdiff(&keys).unwrap();
        let members: Vec<&str> = diff.iter().map(|(m, _)| m.as_str()).collect();
        assert!(members.contains(&"x"));
        assert!(members.contains(&"z"));
        assert!(!members.contains(&"y"));
    }

    #[test]
    fn zdiff_with_missing_second_key_returns_all() {
        let mut ks = Keyspace::new();
        ks.zadd("a", &[(1.0, "x".into())], &ZAddFlags::default())
            .unwrap();
        let keys = vec!["a".to_owned(), "missing".to_owned()];
        let diff = ks.zdiff(&keys).unwrap();
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].0, "x");
    }

    #[test]
    fn zinter_returns_common_members_with_summed_scores() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "a",
            &[(1.0, "x".into()), (2.0, "y".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        ks.zadd(
            "b",
            &[(3.0, "x".into()), (4.0, "z".into())],
            &ZAddFlags::default(),
        )
        .unwrap();

        let keys = vec!["a".to_owned(), "b".to_owned()];
        let inter = ks.zinter(&keys).unwrap();
        assert_eq!(inter.len(), 1);
        assert_eq!(inter[0].0, "x");
        assert!((inter[0].1 - 4.0).abs() < f64::EPSILON); // 1.0 + 3.0
    }

    #[test]
    fn zinter_empty_when_no_common_members() {
        let mut ks = Keyspace::new();
        ks.zadd("a", &[(1.0, "x".into())], &ZAddFlags::default())
            .unwrap();
        ks.zadd("b", &[(1.0, "y".into())], &ZAddFlags::default())
            .unwrap();
        let keys = vec!["a".to_owned(), "b".to_owned()];
        assert!(ks.zinter(&keys).unwrap().is_empty());
    }

    #[test]
    fn zunion_combines_all_members_with_summed_scores() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "a",
            &[(1.0, "x".into()), (2.0, "y".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        ks.zadd(
            "b",
            &[(3.0, "x".into()), (4.0, "z".into())],
            &ZAddFlags::default(),
        )
        .unwrap();

        let keys = vec!["a".to_owned(), "b".to_owned()];
        let union = ks.zunion(&keys).unwrap();
        assert_eq!(union.len(), 3); // x, y, z
        let x = union.iter().find(|(m, _)| m == "x").unwrap();
        assert!((x.1 - 4.0).abs() < f64::EPSILON); // 1.0 + 3.0
    }

    #[test]
    fn zdiff_wrong_type_returns_error() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("v"), None, false, false);
        let keys = vec!["s".to_owned()];
        assert!(ks.zdiff(&keys).is_err());
    }

    // --- zrandmember ---

    #[test]
    fn zrandmember_no_count_returns_single_member() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into()), (3.0, "c".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        let result = ks.zrandmember("z", None, false).unwrap();
        assert_eq!(result.len(), 1);
        assert!(["a", "b", "c"].contains(&result[0].0.as_str()));
        // no count means no score
        assert!(result[0].1.is_none());
    }

    #[test]
    fn zrandmember_positive_count_distinct() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into()), (3.0, "c".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        let result = ks.zrandmember("z", Some(2), false).unwrap();
        assert_eq!(result.len(), 2);
        for (m, s) in &result {
            assert!(["a", "b", "c"].contains(&m.as_str()));
            assert!(s.is_none());
        }
        // distinct
        let unique: std::collections::HashSet<_> = result.iter().map(|(m, _)| m).collect();
        assert_eq!(unique.len(), 2);
    }

    #[test]
    fn zrandmember_positive_count_capped_at_set_size() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        let result = ks.zrandmember("z", Some(10), false).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn zrandmember_negative_count_allows_duplicates() {
        let mut ks = Keyspace::new();
        ks.zadd("z", &[(1.0, "only".into())], &ZAddFlags::default())
            .unwrap();
        let result = ks.zrandmember("z", Some(-5), false).unwrap();
        assert_eq!(result.len(), 5);
        assert!(result.iter().all(|(m, _)| m == "only"));
    }

    #[test]
    fn zrandmember_with_scores() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "z",
            &[(1.0, "a".into()), (2.0, "b".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        let result = ks.zrandmember("z", Some(2), true).unwrap();
        assert_eq!(result.len(), 2);
        for (m, s) in &result {
            assert!(["a", "b"].contains(&m.as_str()));
            assert!(s.is_some());
        }
    }

    #[test]
    fn zrandmember_missing_key_returns_empty() {
        let mut ks = Keyspace::new();
        assert!(ks.zrandmember("missing", None, false).unwrap().is_empty());
        assert!(ks.zrandmember("missing", Some(5), true).unwrap().is_empty());
    }

    #[test]
    fn zrandmember_wrong_type_returns_error() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.zrandmember("s", None, false).is_err());
    }

    // --- zdiffstore / zinterstore / zunionstore ---

    #[test]
    fn zdiffstore_basic() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "a",
            &[(1.0, "x".into()), (2.0, "y".into()), (3.0, "z".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        ks.zadd("b", &[(1.0, "y".into())], &ZAddFlags::default())
            .unwrap();

        let keys = vec!["a".to_owned(), "b".to_owned()];
        let (count, _stored) = ks.zdiffstore("dest", &keys).unwrap();
        assert_eq!(count, 2); // x, z

        // dest key should now hold a sorted set
        assert_eq!(ks.value_type("dest"), "zset");
        let members = ks.zrange("dest", 0, -1).unwrap();
        let names: Vec<&str> = members.iter().map(|(m, _)| m.as_str()).collect();
        assert!(names.contains(&"x"));
        assert!(names.contains(&"z"));
        assert!(!names.contains(&"y"));
    }

    #[test]
    fn zinterstore_basic() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "a",
            &[(1.0, "x".into()), (2.0, "y".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        ks.zadd(
            "b",
            &[(3.0, "x".into()), (4.0, "z".into())],
            &ZAddFlags::default(),
        )
        .unwrap();

        let keys = vec!["a".to_owned(), "b".to_owned()];
        let (count, _stored) = ks.zinterstore("dest", &keys).unwrap();
        assert_eq!(count, 1); // only x is in both

        let members = ks.zrange("dest", 0, -1).unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].0, "x");
        // score is summed: 1.0 + 3.0 = 4.0
        assert!((members[0].1 - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn zunionstore_basic() {
        let mut ks = Keyspace::new();
        ks.zadd(
            "a",
            &[(1.0, "x".into()), (2.0, "y".into())],
            &ZAddFlags::default(),
        )
        .unwrap();
        ks.zadd(
            "b",
            &[(3.0, "x".into()), (4.0, "z".into())],
            &ZAddFlags::default(),
        )
        .unwrap();

        let keys = vec!["a".to_owned(), "b".to_owned()];
        let (count, _stored) = ks.zunionstore("dest", &keys).unwrap();
        assert_eq!(count, 3); // x, y, z

        let members = ks.zrange("dest", 0, -1).unwrap();
        assert_eq!(members.len(), 3);
        let x = members.iter().find(|(m, _)| m == "x").unwrap();
        // score is summed: 1.0 + 3.0 = 4.0
        assert!((x.1 - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn zstore_overwrites_existing_dest() {
        let mut ks = Keyspace::new();
        ks.zadd("a", &[(1.0, "x".into())], &ZAddFlags::default())
            .unwrap();
        // put something at dest first
        ks.set("dest".into(), Bytes::from("old"), None, false, false);

        let keys = vec!["a".to_owned()];
        let (count, _) = ks.zunionstore("dest", &keys).unwrap();
        assert_eq!(count, 1);
        assert_eq!(ks.value_type("dest"), "zset");
    }

    #[test]
    fn zstore_empty_result_removes_dest() {
        let mut ks = Keyspace::new();
        ks.zadd("a", &[(1.0, "x".into())], &ZAddFlags::default())
            .unwrap();
        ks.zadd("b", &[(1.0, "x".into())], &ZAddFlags::default())
            .unwrap();
        // intersection of disjoint sets is empty
        ks.zadd("dest", &[(5.0, "old".into())], &ZAddFlags::default())
            .unwrap();

        let keys = vec!["a".to_owned(), "b".to_owned()];
        // zdiff of a and b where b has all of a's members → empty
        let keys_diff = vec!["a".to_owned(), "b".to_owned()];
        let (count, _) = ks.zdiffstore("dest", &keys_diff).unwrap();
        assert_eq!(count, 0);
        // dest should be removed when result is empty
        assert_eq!(ks.value_type("dest"), "none");

        // also check zinterstore on disjoint sets
        ks.zadd("c", &[(1.0, "p".into())], &ZAddFlags::default())
            .unwrap();
        ks.zadd("d", &[(1.0, "q".into())], &ZAddFlags::default())
            .unwrap();
        ks.zadd("dest2", &[(5.0, "old".into())], &ZAddFlags::default())
            .unwrap();
        let keys_inter = vec!["c".to_owned(), "d".to_owned()];
        let (count2, _) = ks.zinterstore("dest2", &keys_inter).unwrap();
        assert_eq!(count2, 0);
        assert_eq!(ks.value_type("dest2"), "none");
        _ = keys;
    }

    #[test]
    fn zstore_wrong_type_returns_error() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        let keys = vec!["s".to_owned()];
        assert!(ks.zunionstore("dest", &keys).is_err());
        assert!(ks.zinterstore("dest", &keys).is_err());
        assert!(ks.zdiffstore("dest", &keys).is_err());
    }
}
