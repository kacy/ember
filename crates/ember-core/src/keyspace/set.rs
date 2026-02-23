use super::*;

impl Keyspace {
    /// Adds one or more members to a set.
    ///
    /// Creates the set if the key doesn't exist. Returns the number of
    /// new members added (existing members don't count).
    pub fn sadd(&mut self, key: &str, members: &[String]) -> Result<usize, WriteError> {
        if members.is_empty() {
            return Ok(0);
        }

        self.remove_if_expired(key);

        let is_new = self.ensure_collection_type(key, |v| matches!(v, Value::Set(_)))?;

        let member_increase: usize = members
            .iter()
            .map(|m| m.len() + memory::HASHSET_MEMBER_OVERHEAD)
            .sum();
        self.reserve_memory(is_new, key, memory::HASHSET_BASE_OVERHEAD, member_increase)?;

        if is_new {
            self.insert_empty(key, Value::Set(Box::default()));
        }

        let added = self
            .track_size(key, |entry| {
                let Value::Set(ref mut set) = entry.value else {
                    unreachable!("type verified by ensure_collection_type");
                };
                let mut added = 0;
                for member in members {
                    if set.insert(member.clone()) {
                        added += 1;
                    }
                }
                entry.touch();
                added
            })
            .unwrap_or(0);

        Ok(added)
    }

    /// Removes one or more members from a set.
    ///
    /// Returns the number of members that were actually removed.
    pub fn srem(&mut self, key: &str, members: &[String]) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }

        let Some(entry) = self.entries.get_mut(key) else {
            return Ok(0);
        };
        if !matches!(entry.value, Value::Set(_)) {
            return Err(WrongType);
        }

        let old_entry_size = entry.entry_size(key);

        let mut removed = 0;
        let mut removed_bytes: usize = 0;
        let is_empty = if let Value::Set(ref mut set) = entry.value {
            for member in members {
                if set.remove(member) {
                    removed_bytes += member.len() + memory::HASHSET_MEMBER_OVERHEAD;
                    removed += 1;
                }
            }
            set.is_empty()
        } else {
            false
        };

        self.cleanup_after_remove(key, old_entry_size, is_empty, removed_bytes);

        Ok(removed)
    }

    /// Returns all members of a set.
    pub fn smembers(&mut self, key: &str) -> Result<Vec<String>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }
        match self.entries.get_mut(key) {
            None => Ok(vec![]),
            Some(entry) => match &entry.value {
                Value::Set(set) => {
                    let result = set.iter().cloned().collect();
                    entry.touch();
                    Ok(result)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Checks if a member exists in a set.
    pub fn sismember(&mut self, key: &str, member: &str) -> Result<bool, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(false);
        }
        match self.entries.get_mut(key) {
            None => Ok(false),
            Some(entry) => match &entry.value {
                Value::Set(set) => {
                    let result = set.contains(member);
                    entry.touch();
                    Ok(result)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Incrementally iterates members of a set.
    ///
    /// Returns the next cursor and a batch of members. A returned cursor
    /// of `0` means the iteration is complete. Pattern matching (MATCH)
    /// filters on member names.
    pub fn scan_set(
        &mut self,
        key: &str,
        cursor: u64,
        count: usize,
        pattern: Option<&str>,
    ) -> Result<(u64, Vec<String>), WrongType> {
        if self.remove_if_expired(key) {
            return Ok((0, vec![]));
        }
        match self.entries.get_mut(key) {
            None => Ok((0, vec![])),
            Some(entry) => {
                let Value::Set(ref set) = entry.value else {
                    return Err(WrongType);
                };

                let target = if count == 0 { 10 } else { count };
                let compiled = pattern.map(GlobPattern::new);
                let mut result = Vec::with_capacity(target);
                let mut pos = 0u64;
                let mut done = true;

                for member in set.iter() {
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
                    result.push(member.clone());
                    pos += 1;
                    if result.len() >= target {
                        done = false;
                        break;
                    }
                }

                entry.touch();
                Ok(if done { (0, result) } else { (pos, result) })
            }
        }
    }

    /// Returns the cardinality (number of elements) of a set.
    pub fn scard(&mut self, key: &str) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }
        match self.entries.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                Value::Set(set) => Ok(set.len()),
                _ => Err(WrongType),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sadd_creates_set() {
        let mut ks = Keyspace::new();
        let added = ks.sadd("s", &["a".into(), "b".into()]).unwrap();
        assert_eq!(added, 2);
        assert_eq!(ks.value_type("s"), "set");
    }

    #[test]
    fn sadd_returns_new_member_count() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into()]).unwrap();
        // add one existing, one new
        let added = ks.sadd("s", &["b".into(), "c".into()]).unwrap();
        assert_eq!(added, 1); // only "c" is new
    }

    #[test]
    fn srem_removes_members() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into(), "c".into()]).unwrap();
        let removed = ks.srem("s", &["a".into(), "c".into()]).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(ks.scard("s").unwrap(), 1);
    }

    #[test]
    fn srem_auto_deletes_empty_set() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["only".into()]).unwrap();
        ks.srem("s", &["only".into()]).unwrap();
        assert_eq!(ks.value_type("s"), "none");
    }

    #[test]
    fn smembers_returns_all_members() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into(), "c".into()]).unwrap();
        let mut members = ks.smembers("s").unwrap();
        members.sort();
        assert_eq!(members, vec!["a", "b", "c"]);
    }

    #[test]
    fn smembers_missing_key_returns_empty() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.smembers("missing").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn sismember_returns_true_for_existing() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["member".into()]).unwrap();
        assert!(ks.sismember("s", "member").unwrap());
    }

    #[test]
    fn sismember_returns_false_for_missing() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into()]).unwrap();
        assert!(!ks.sismember("s", "missing").unwrap());
    }

    #[test]
    fn scard_returns_count() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into(), "c".into()]).unwrap();
        assert_eq!(ks.scard("s").unwrap(), 3);
    }

    #[test]
    fn scard_missing_key_returns_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.scard("missing").unwrap(), 0);
    }

    #[test]
    fn set_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("string"), None, false, false);
        assert!(ks.sadd("s", &["m".into()]).is_err());
        assert!(ks.srem("s", &["m".into()]).is_err());
        assert!(ks.smembers("s").is_err());
        assert!(ks.sismember("s", "m").is_err());
        assert!(ks.scard("s").is_err());
    }

    #[test]
    fn sadd_duplicate_members_counted_once() {
        let mut ks = Keyspace::new();
        // add same member twice in one call
        let count = ks.sadd("s", &["a".into(), "a".into()]).unwrap();
        // should only count as 1 new member
        assert_eq!(count, 1);
        assert_eq!(ks.scard("s").unwrap(), 1);
    }

    #[test]
    fn srem_non_existent_member_returns_zero() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into()]).unwrap();
        let removed = ks.srem("s", &["nonexistent".into()]).unwrap();
        assert_eq!(removed, 0);
    }

    // --- scan_set ---

    #[test]
    fn scan_set_returns_all() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into(), "c".into()]).unwrap();
        let (cursor, members) = ks.scan_set("s", 0, 100, None).unwrap();
        assert_eq!(cursor, 0);
        assert_eq!(members.len(), 3);
    }

    #[test]
    fn scan_set_missing_key() {
        let mut ks = Keyspace::new();
        let (cursor, members) = ks.scan_set("missing", 0, 10, None).unwrap();
        assert_eq!(cursor, 0);
        assert!(members.is_empty());
    }

    #[test]
    fn scan_set_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("string"), None, false, false);
        assert!(ks.scan_set("s", 0, 10, None).is_err());
    }

    #[test]
    fn scan_set_with_pattern() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["user:1".into(), "user:2".into(), "item:1".into()])
            .unwrap();
        let (_, members) = ks.scan_set("s", 0, 100, Some("user:*")).unwrap();
        assert_eq!(members.len(), 2);
        assert!(members.iter().all(|m| m.starts_with("user:")));
    }

    #[test]
    fn scan_set_pagination() {
        let mut ks = Keyspace::new();
        let items: Vec<String> = (0..20).map(|i| format!("m{i}")).collect();
        ks.sadd("s", &items).unwrap();

        let mut collected = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (next, batch) = ks.scan_set("s", cursor, 5, None).unwrap();
            collected.extend(batch);
            if next == 0 {
                break;
            }
            cursor = next;
        }
        // all 20 members collected
        assert_eq!(collected.len(), 20);
    }

    #[test]
    fn set_auto_deleted_when_empty() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into()]).unwrap();
        assert_eq!(ks.len(), 1);

        // remove all members
        ks.srem("s", &["a".into(), "b".into()]).unwrap();

        // set should be auto-deleted
        assert_eq!(ks.len(), 0);
        assert!(!ks.exists("s"));
    }
}
