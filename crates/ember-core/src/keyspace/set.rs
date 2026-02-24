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

        let ver = self.next_ver();
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
        if removed > 0 {
            entry.version = ver;
        }

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

    /// Returns the union of all given sets.
    ///
    /// Keys that don't exist are treated as empty sets. Returns an error
    /// only if any key holds a non-set type.
    pub fn sunion(&mut self, keys: &[String]) -> Result<Vec<String>, WrongType> {
        let mut result = std::collections::HashSet::new();
        for key in keys {
            self.remove_if_expired(key);
            match self.entries.get_mut(key.as_str()) {
                None => {}
                Some(entry) => match &entry.value {
                    Value::Set(set) => {
                        result.extend(set.iter().cloned());
                        entry.touch();
                    }
                    _ => return Err(WrongType),
                },
            }
        }
        Ok(result.into_iter().collect())
    }

    /// Returns the intersection of all given sets.
    ///
    /// If any key doesn't exist, the result is empty. Returns an error
    /// only if any key holds a non-set type.
    pub fn sinter(&mut self, keys: &[String]) -> Result<Vec<String>, WrongType> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // check types and find missing keys first
        for key in keys {
            self.remove_if_expired(key);
            match self.entries.get(key.as_str()) {
                None => return Ok(vec![]), // any missing key → empty intersection
                Some(entry) => {
                    if !matches!(&entry.value, Value::Set(_)) {
                        return Err(WrongType);
                    }
                }
            }
        }

        // start with the first set, intersect with the rest
        let entry = self.entries.get_mut(keys[0].as_str()).expect("checked above");
        let Value::Set(ref base) = entry.value else {
            unreachable!("type checked above");
        };
        let candidates: Vec<String> = base.iter().cloned().collect();
        entry.touch();

        let result: Vec<String> = candidates
            .into_iter()
            .filter(|member| {
                keys[1..].iter().all(|key| {
                    self.entries
                        .get(key.as_str())
                        .and_then(|e| match &e.value {
                            Value::Set(s) => Some(s.contains(member)),
                            _ => None,
                        })
                        .unwrap_or(false)
                })
            })
            .collect();

        // touch remaining keys
        for key in &keys[1..] {
            if let Some(entry) = self.entries.get_mut(key.as_str()) {
                entry.touch();
            }
        }

        Ok(result)
    }

    /// Returns members of the first set that are not in any of the other sets.
    ///
    /// If the first key doesn't exist, the result is empty. Returns an error
    /// only if any key holds a non-set type.
    pub fn sdiff(&mut self, keys: &[String]) -> Result<Vec<String>, WrongType> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        // type-check all keys
        for key in keys {
            self.remove_if_expired(key);
            if let Some(entry) = self.entries.get(key.as_str()) {
                if !matches!(&entry.value, Value::Set(_)) {
                    return Err(WrongType);
                }
            }
        }

        let Some(first_entry) = self.entries.get_mut(keys[0].as_str()) else {
            return Ok(vec![]);
        };
        let Value::Set(ref base) = first_entry.value else {
            unreachable!("type checked above");
        };
        let candidates: Vec<String> = base.iter().cloned().collect();
        first_entry.touch();

        let result: Vec<String> = candidates
            .into_iter()
            .filter(|member| {
                !keys[1..].iter().any(|key| {
                    self.entries
                        .get(key.as_str())
                        .and_then(|e| match &e.value {
                            Value::Set(s) => Some(s.contains(member)),
                            _ => None,
                        })
                        .unwrap_or(false)
                })
            })
            .collect();

        // touch remaining keys
        for key in &keys[1..] {
            if let Some(entry) = self.entries.get_mut(key.as_str()) {
                entry.touch();
            }
        }

        Ok(result)
    }

    /// Stores the union of all source sets into `dest`.
    ///
    /// Overwrites the destination if it already exists. Returns the
    /// cardinality of the resulting set.
    pub fn sunionstore(
        &mut self,
        dest: &str,
        keys: &[String],
    ) -> Result<usize, WriteError> {
        let members = self.sunion(keys).map_err(|_| WriteError::WrongType)?;
        self.store_set_result(dest, members)
    }

    /// Stores the intersection of all source sets into `dest`.
    pub fn sinterstore(
        &mut self,
        dest: &str,
        keys: &[String],
    ) -> Result<usize, WriteError> {
        let members = self.sinter(keys).map_err(|_| WriteError::WrongType)?;
        self.store_set_result(dest, members)
    }

    /// Stores the difference of sets (first minus the rest) into `dest`.
    pub fn sdiffstore(
        &mut self,
        dest: &str,
        keys: &[String],
    ) -> Result<usize, WriteError> {
        let members = self.sdiff(keys).map_err(|_| WriteError::WrongType)?;
        self.store_set_result(dest, members)
    }

    /// Writes a computed set result to `dest`, replacing any existing key.
    fn store_set_result(
        &mut self,
        dest: &str,
        members: Vec<String>,
    ) -> Result<usize, WriteError> {
        // delete destination first
        self.remove_if_expired(dest);
        if let Some(old) = self.entries.remove(dest) {
            self.memory.remove(dest, &old.value);
            self.decrement_expiry_if_set(&old);
            self.defer_drop(old.value);
        }

        let count = members.len();
        if count == 0 {
            return Ok(0);
        }

        let member_bytes: usize = members
            .iter()
            .map(|m| m.len() + memory::HASHSET_MEMBER_OVERHEAD)
            .sum();
        self.reserve_memory(true, dest, memory::HASHSET_BASE_OVERHEAD, member_bytes)?;

        let set: std::collections::HashSet<String> = members.into_iter().collect();
        let value = Value::Set(Box::new(set));
        self.memory.add(dest, &value);
        let mut entry = Entry::new(value, None);
        entry.version = self.next_ver();
        self.entries.insert(CompactString::from(dest), entry);

        Ok(count)
    }

    /// Returns random members from a set without removing them.
    ///
    /// - `count > 0`: return up to `count` distinct members
    /// - `count < 0`: return `|count|` members, allowing duplicates
    /// - `count == 0`: return empty
    pub fn srandmember(
        &mut self,
        key: &str,
        count: i64,
    ) -> Result<Vec<String>, WrongType> {
        if self.remove_if_expired(key) || count == 0 {
            return Ok(vec![]);
        }
        match self.entries.get_mut(key) {
            None => Ok(vec![]),
            Some(entry) => {
                let Value::Set(ref set) = entry.value else {
                    return Err(WrongType);
                };
                if set.is_empty() {
                    entry.touch();
                    return Ok(vec![]);
                }

                let mut rng = rand::rng();
                let result = if count > 0 {
                    // distinct members, up to set size
                    let n = (count as usize).min(set.len());
                    set.iter().choose_multiple(&mut rng, n).into_iter().cloned().collect()
                } else {
                    // allow duplicates, return exactly |count| elements
                    let n = count.unsigned_abs() as usize;
                    let members: Vec<&String> = set.iter().collect();
                    use rand::Rng;
                    (0..n)
                        .map(|_| members[rng.random_range(0..members.len())].clone())
                        .collect()
                };

                entry.touch();
                Ok(result)
            }
        }
    }

    /// Removes and returns up to `count` random members from a set.
    pub fn spop(&mut self, key: &str, count: usize) -> Result<Vec<String>, WrongType> {
        if self.remove_if_expired(key) || count == 0 {
            return Ok(vec![]);
        }

        let ver = self.next_ver();
        let Some(entry) = self.entries.get_mut(key) else {
            return Ok(vec![]);
        };
        if !matches!(entry.value, Value::Set(_)) {
            return Err(WrongType);
        }

        let old_entry_size = entry.entry_size(key);

        // reborrow to get mutable access to the set
        let Value::Set(ref mut set) = entry.value else {
            unreachable!("type checked above");
        };
        if set.is_empty() {
            return Ok(vec![]);
        }

        let n = count.min(set.len());
        let mut rng = rand::rng();
        let chosen: Vec<String> = set.iter().choose_multiple(&mut rng, n).into_iter().cloned().collect();

        let mut removed_bytes = 0usize;
        for member in &chosen {
            set.remove(member);
            removed_bytes += member.len() + memory::HASHSET_MEMBER_OVERHEAD;
        }
        let is_empty = set.is_empty();

        if !chosen.is_empty() {
            entry.version = ver;
        }

        self.cleanup_after_remove(key, old_entry_size, is_empty, removed_bytes);

        Ok(chosen)
    }

    /// Checks membership for multiple members at once.
    ///
    /// Returns a boolean for each member in the same order.
    pub fn smismember(
        &mut self,
        key: &str,
        members: &[String],
    ) -> Result<Vec<bool>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![false; members.len()]);
        }
        match self.entries.get_mut(key) {
            None => Ok(vec![false; members.len()]),
            Some(entry) => match &entry.value {
                Value::Set(set) => {
                    let result = members.iter().map(|m| set.contains(m)).collect();
                    entry.touch();
                    Ok(result)
                }
                _ => Err(WrongType),
            },
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

    // --- sunion ---

    #[test]
    fn sunion_basic() {
        let mut ks = Keyspace::new();
        ks.sadd("s1", &["a".into(), "b".into()]).unwrap();
        ks.sadd("s2", &["b".into(), "c".into()]).unwrap();
        let mut result = ks.sunion(&["s1".into(), "s2".into()]).unwrap();
        result.sort();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn sunion_with_missing_key() {
        let mut ks = Keyspace::new();
        ks.sadd("s1", &["a".into()]).unwrap();
        let mut result = ks.sunion(&["s1".into(), "missing".into()]).unwrap();
        result.sort();
        assert_eq!(result, vec!["a"]);
    }

    #[test]
    fn sunion_empty_keys() {
        let mut ks = Keyspace::new();
        assert!(ks.sunion(&[]).unwrap().is_empty());
    }

    #[test]
    fn sunion_wrong_type() {
        let mut ks = Keyspace::new();
        ks.sadd("s1", &["a".into()]).unwrap();
        ks.set("str".into(), Bytes::from("val"), None, false, false);
        assert!(ks.sunion(&["s1".into(), "str".into()]).is_err());
    }

    // --- sinter ---

    #[test]
    fn sinter_basic() {
        let mut ks = Keyspace::new();
        ks.sadd("s1", &["a".into(), "b".into(), "c".into()]).unwrap();
        ks.sadd("s2", &["b".into(), "c".into(), "d".into()]).unwrap();
        let mut result = ks.sinter(&["s1".into(), "s2".into()]).unwrap();
        result.sort();
        assert_eq!(result, vec!["b", "c"]);
    }

    #[test]
    fn sinter_missing_key_returns_empty() {
        let mut ks = Keyspace::new();
        ks.sadd("s1", &["a".into()]).unwrap();
        let result = ks.sinter(&["s1".into(), "missing".into()]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn sinter_disjoint_sets() {
        let mut ks = Keyspace::new();
        ks.sadd("s1", &["a".into()]).unwrap();
        ks.sadd("s2", &["b".into()]).unwrap();
        let result = ks.sinter(&["s1".into(), "s2".into()]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn sinter_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("str".into(), Bytes::from("val"), None, false, false);
        assert!(ks.sinter(&["str".into()]).is_err());
    }

    // --- sdiff ---

    #[test]
    fn sdiff_basic() {
        let mut ks = Keyspace::new();
        ks.sadd("s1", &["a".into(), "b".into(), "c".into()]).unwrap();
        ks.sadd("s2", &["b".into(), "d".into()]).unwrap();
        let mut result = ks.sdiff(&["s1".into(), "s2".into()]).unwrap();
        result.sort();
        assert_eq!(result, vec!["a", "c"]);
    }

    #[test]
    fn sdiff_missing_first_key() {
        let mut ks = Keyspace::new();
        ks.sadd("s2", &["a".into()]).unwrap();
        let result = ks.sdiff(&["missing".into(), "s2".into()]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn sdiff_missing_second_key() {
        let mut ks = Keyspace::new();
        ks.sadd("s1", &["a".into(), "b".into()]).unwrap();
        let mut result = ks.sdiff(&["s1".into(), "missing".into()]).unwrap();
        result.sort();
        assert_eq!(result, vec!["a", "b"]);
    }

    // --- sunionstore / sinterstore / sdiffstore ---

    #[test]
    fn sunionstore_basic() {
        let mut ks = Keyspace::new();
        ks.sadd("s1", &["a".into(), "b".into()]).unwrap();
        ks.sadd("s2", &["b".into(), "c".into()]).unwrap();
        let count = ks.sunionstore("dest", &["s1".into(), "s2".into()]).unwrap();
        assert_eq!(count, 3);
        let mut members = ks.smembers("dest").unwrap();
        members.sort();
        assert_eq!(members, vec!["a", "b", "c"]);
    }

    #[test]
    fn sinterstore_basic() {
        let mut ks = Keyspace::new();
        ks.sadd("s1", &["a".into(), "b".into(), "c".into()]).unwrap();
        ks.sadd("s2", &["b".into(), "c".into(), "d".into()]).unwrap();
        let count = ks.sinterstore("dest", &["s1".into(), "s2".into()]).unwrap();
        assert_eq!(count, 2);
        let mut members = ks.smembers("dest").unwrap();
        members.sort();
        assert_eq!(members, vec!["b", "c"]);
    }

    #[test]
    fn sdiffstore_basic() {
        let mut ks = Keyspace::new();
        ks.sadd("s1", &["a".into(), "b".into(), "c".into()]).unwrap();
        ks.sadd("s2", &["b".into()]).unwrap();
        let count = ks.sdiffstore("dest", &["s1".into(), "s2".into()]).unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn store_overwrites_destination() {
        let mut ks = Keyspace::new();
        ks.sadd("dest", &["old".into()]).unwrap();
        ks.sadd("s1", &["new".into()]).unwrap();
        ks.sunionstore("dest", &["s1".into()]).unwrap();
        let members = ks.smembers("dest").unwrap();
        assert_eq!(members, vec!["new"]);
    }

    #[test]
    fn store_empty_result_deletes_dest() {
        let mut ks = Keyspace::new();
        ks.sadd("dest", &["old".into()]).unwrap();
        // intersect with missing key → empty result
        ks.sinterstore("dest", &["missing".into()]).unwrap();
        assert_eq!(ks.value_type("dest"), "none");
    }

    // --- srandmember ---

    #[test]
    fn srandmember_positive_count() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into(), "c".into()]).unwrap();
        let result = ks.srandmember("s", 2).unwrap();
        assert_eq!(result.len(), 2);
        // all returned members should be from the set
        for m in &result {
            assert!(["a", "b", "c"].contains(&m.as_str()));
        }
        // results should be distinct
        let unique: std::collections::HashSet<_> = result.iter().collect();
        assert_eq!(unique.len(), 2);
    }

    #[test]
    fn srandmember_count_larger_than_set() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into()]).unwrap();
        let result = ks.srandmember("s", 10).unwrap();
        assert_eq!(result.len(), 2); // capped at set size
    }

    #[test]
    fn srandmember_negative_count_allows_duplicates() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["only".into()]).unwrap();
        let result = ks.srandmember("s", -5).unwrap();
        assert_eq!(result.len(), 5);
        assert!(result.iter().all(|m| m == "only"));
    }

    #[test]
    fn srandmember_zero_returns_empty() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into()]).unwrap();
        assert!(ks.srandmember("s", 0).unwrap().is_empty());
    }

    #[test]
    fn srandmember_missing_key() {
        let mut ks = Keyspace::new();
        assert!(ks.srandmember("missing", 1).unwrap().is_empty());
    }

    #[test]
    fn srandmember_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("str".into(), Bytes::from("val"), None, false, false);
        assert!(ks.srandmember("str", 1).is_err());
    }

    // --- spop ---

    #[test]
    fn spop_basic() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into(), "c".into()]).unwrap();
        let result = ks.spop("s", 1).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(ks.scard("s").unwrap(), 2);
    }

    #[test]
    fn spop_all_members() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "b".into()]).unwrap();
        let result = ks.spop("s", 10).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(ks.value_type("s"), "none"); // auto-deleted
    }

    #[test]
    fn spop_missing_key() {
        let mut ks = Keyspace::new();
        assert!(ks.spop("missing", 1).unwrap().is_empty());
    }

    #[test]
    fn spop_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("str".into(), Bytes::from("val"), None, false, false);
        assert!(ks.spop("str", 1).is_err());
    }

    #[test]
    fn spop_zero_count() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into()]).unwrap();
        assert!(ks.spop("s", 0).unwrap().is_empty());
        assert_eq!(ks.scard("s").unwrap(), 1);
    }

    // --- smismember ---

    #[test]
    fn smismember_basic() {
        let mut ks = Keyspace::new();
        ks.sadd("s", &["a".into(), "c".into()]).unwrap();
        let result = ks.smismember("s", &["a".into(), "b".into(), "c".into()]).unwrap();
        assert_eq!(result, vec![true, false, true]);
    }

    #[test]
    fn smismember_missing_key() {
        let mut ks = Keyspace::new();
        let result = ks.smismember("missing", &["a".into()]).unwrap();
        assert_eq!(result, vec![false]);
    }

    #[test]
    fn smismember_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("str".into(), Bytes::from("val"), None, false, false);
        assert!(ks.smismember("str", &["a".into()]).is_err());
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
