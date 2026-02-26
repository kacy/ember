use compact_str::CompactString;

use super::*;

impl Keyspace {
    /// Sets one or more field-value pairs in a hash.
    ///
    /// Creates the hash if the key doesn't exist. Returns the number of
    /// new fields added (fields that were updated don't count).
    pub fn hset(&mut self, key: &str, fields: &[(String, Bytes)]) -> Result<usize, WriteError> {
        if fields.is_empty() {
            return Ok(0);
        }

        self.remove_if_expired(key);

        let is_new = self.ensure_collection_type(key, |v| matches!(v, Value::Hash(_)))?;

        let field_increase: usize = fields
            .iter()
            .map(|(f, v)| f.len() + v.len() + memory::PACKED_HASH_ENTRY_OVERHEAD)
            .sum();
        self.reserve_memory(
            is_new,
            key,
            memory::PACKED_HASH_BASE_OVERHEAD,
            field_increase,
        )?;

        if is_new {
            self.insert_empty(key, Value::Hash(Box::default()));
        }

        let track_access = self.track_access;
        let added = self
            .track_size(key, |entry| {
                let Value::Hash(ref mut hash) = entry.value else {
                    unreachable!("type verified by ensure_collection_type");
                };
                let mut added = 0;
                for (field, value) in fields {
                    if hash
                        .insert(CompactString::from(field.as_str()), value.clone())
                        .is_none()
                    {
                        added += 1;
                    }
                }
                entry.touch(track_access);
                added
            })
            .unwrap_or(0);

        Ok(added)
    }

    /// Gets the value of a field in a hash.
    ///
    /// Returns `None` if the key or field doesn't exist.
    pub fn hget(&mut self, key: &str, field: &str) -> Result<Option<Bytes>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(None);
        };
        match &entry.value {
            Value::Hash(hash) => Ok(hash.get(field).map(Bytes::copy_from_slice)),
            _ => Err(WrongType),
        }
    }

    /// Gets all field-value pairs from a hash.
    ///
    /// Returns an empty vec if the key doesn't exist.
    pub fn hgetall(&mut self, key: &str) -> Result<Vec<(String, Bytes)>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(vec![]);
        };
        match &entry.value {
            Value::Hash(hash) => Ok(hash
                .iter()
                .map(|(k, v)| (k.to_string(), Bytes::copy_from_slice(v)))
                .collect()),
            _ => Err(WrongType),
        }
    }

    /// Deletes one or more fields from a hash.
    ///
    /// Returns the fields that were actually removed.
    pub fn hdel(&mut self, key: &str, fields: &[String]) -> Result<Vec<String>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(vec![]);
        }

        let Some(entry) = self.entries.get_mut(key) else {
            return Ok(vec![]);
        };
        if !matches!(entry.value, Value::Hash(_)) {
            return Err(WrongType);
        }

        let old_entry_size = entry.entry_size(key);
        let mut removed = Vec::new();
        let mut removed_bytes: usize = 0;
        let is_empty = if let Value::Hash(ref mut hash) = entry.value {
            for field in fields {
                if let Some(val) = hash.remove(field) {
                    removed_bytes += field.len() + val.len() + memory::PACKED_HASH_ENTRY_OVERHEAD;
                    removed.push(field.clone());
                }
            }
            hash.is_empty()
        } else {
            false
        };
        if !removed.is_empty() {
            self.bump_version(key);
        }

        self.cleanup_after_remove(key, old_entry_size, is_empty, removed_bytes);

        Ok(removed)
    }

    /// Checks if a field exists in a hash.
    pub fn hexists(&mut self, key: &str, field: &str) -> Result<bool, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(false);
        };
        match &entry.value {
            Value::Hash(hash) => Ok(hash.contains_key(field)),
            _ => Err(WrongType),
        }
    }

    /// Returns the number of fields in a hash.
    pub fn hlen(&mut self, key: &str) -> Result<usize, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }
        match self.entries.get(key) {
            None => Ok(0),
            Some(entry) => match &entry.value {
                Value::Hash(hash) => Ok(hash.len()),
                _ => Err(WrongType),
            },
        }
    }

    /// Increments a field's integer value by the given amount.
    ///
    /// Creates the hash and field if they don't exist, starting from 0.
    pub fn hincrby(&mut self, key: &str, field: &str, delta: i64) -> Result<i64, IncrError> {
        self.remove_if_expired(key);

        let is_new = match self.entries.get(key) {
            None => true,
            Some(e) if matches!(e.value, Value::Hash(_)) => false,
            Some(_) => return Err(IncrError::WrongType),
        };

        // estimate memory for new field (worst case: new hash + new field)
        let val_str_len = 20; // max i64 string length
        let estimated_increase = if is_new {
            memory::ENTRY_OVERHEAD
                + key.len()
                + memory::PACKED_HASH_BASE_OVERHEAD
                + field.len()
                + val_str_len
                + memory::PACKED_HASH_ENTRY_OVERHEAD
        } else {
            field.len() + val_str_len + memory::PACKED_HASH_ENTRY_OVERHEAD
        };

        if !self.enforce_memory_limit(estimated_increase) {
            return Err(IncrError::OutOfMemory);
        }

        if is_new {
            let value = Value::Hash(Box::default());
            self.memory.add(key, &value);
            let entry = Entry::new(value, None);
            self.entries.insert(CompactString::from(key), entry);
            self.bump_version(key);
        }

        // safe: key was either just inserted above or verified to exist
        let Some(entry) = self.entries.get_mut(key) else {
            return Err(IncrError::WrongType);
        };
        let old_entry_size = entry.entry_size(key);

        let Value::Hash(ref mut hash) = entry.value else {
            return Err(IncrError::WrongType);
        };
        let current_val = match hash.get(field) {
            Some(data) => {
                let s = std::str::from_utf8(data).map_err(|_| IncrError::NotAnInteger)?;
                s.parse::<i64>().map_err(|_| IncrError::NotAnInteger)?
            }
            None => 0,
        };
        let new_val = current_val.checked_add(delta).ok_or(IncrError::Overflow)?;
        hash.insert(field.into(), Bytes::from(new_val.to_string()));
        entry.touch(self.track_access);

        let new_value_size = memory::value_size(&entry.value);
        entry.cached_value_size = new_value_size as u32;
        let new_entry_size = key.len() + new_value_size + memory::ENTRY_OVERHEAD;
        self.memory.adjust(old_entry_size, new_entry_size);
        self.bump_version(key);

        Ok(new_val)
    }

    /// Returns all field names in a hash.
    pub fn hkeys(&mut self, key: &str) -> Result<Vec<String>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(vec![]);
        };
        match &entry.value {
            Value::Hash(hash) => Ok(hash.iter().map(|(k, _)| k.to_string()).collect()),
            _ => Err(WrongType),
        }
    }

    /// Returns all values in a hash.
    pub fn hvals(&mut self, key: &str) -> Result<Vec<Bytes>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(vec![]);
        };
        match &entry.value {
            Value::Hash(hash) => Ok(hash
                .iter()
                .map(|(_, v)| Bytes::copy_from_slice(v))
                .collect()),
            _ => Err(WrongType),
        }
    }

    /// Incrementally iterates fields of a hash.
    ///
    /// Returns the next cursor and a batch of field-value pairs. A returned
    /// cursor of `0` means the iteration is complete. Pattern matching
    /// (MATCH) filters on field names.
    pub fn scan_hash(
        &mut self,
        key: &str,
        cursor: u64,
        count: usize,
        pattern: Option<&str>,
    ) -> Result<(u64, Vec<(String, Bytes)>), WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok((0, vec![]));
        };
        let Value::Hash(ref hash) = entry.value else {
            return Err(WrongType);
        };

        let target = if count == 0 { 10 } else { count };
        let compiled = pattern.map(GlobPattern::new);
        let mut result = Vec::with_capacity(target);
        let mut pos = 0u64;
        let mut done = true;

        for (field, value) in hash.iter() {
            if pos < cursor {
                pos += 1;
                continue;
            }
            if let Some(ref pat) = compiled {
                if !pat.matches(field) {
                    pos += 1;
                    continue;
                }
            }
            result.push((field.to_string(), Bytes::copy_from_slice(value)));
            pos += 1;
            if result.len() >= target {
                done = false;
                break;
            }
        }

        Ok(if done { (0, result) } else { (pos, result) })
    }

    /// Gets multiple field values from a hash.
    ///
    /// Returns `None` for fields that don't exist.
    pub fn hmget(&mut self, key: &str, fields: &[String]) -> Result<Vec<Option<Bytes>>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(fields.iter().map(|_| None).collect());
        };
        match &entry.value {
            Value::Hash(hash) => Ok(fields
                .iter()
                .map(|f| hash.get(f.as_str()).map(Bytes::copy_from_slice))
                .collect()),
            _ => Err(WrongType),
        }
    }

    /// Returns random field(s) from a hash.
    ///
    /// - `count = None`: return one random field (no value, even if `with_values` is set)
    /// - `count > 0`: return up to count distinct fields
    /// - `count < 0`: return |count| fields, allowing duplicates
    ///
    /// If `with_values` is true and count is `Some`, returns interleaved `(field, Some(value))`
    /// pairs. When `count` is `None`, only the field name is returned as `(field, None)`.
    pub fn hrandfield(
        &mut self,
        key: &str,
        count: Option<i64>,
        with_values: bool,
    ) -> Result<Vec<(String, Option<Bytes>)>, WrongType> {
        let Some(entry) = self.get_live_entry(key) else {
            return Ok(vec![]);
        };
        let Value::Hash(ref hash) = entry.value else {
            return Err(WrongType);
        };
        if hash.is_empty() {
            return Ok(vec![]);
        }

        // collect into a vec for indexed random access
        let fields: Vec<(&str, &[u8])> = hash.iter().collect();
        let mut rng = rand::rng();

        let result = match count {
            None => {
                // single random field — no value even with with_values
                use rand::seq::IteratorRandom;
                fields
                    .iter()
                    .choose(&mut rng)
                    .map(|(f, _)| ((*f).to_owned(), None))
                    .into_iter()
                    .collect()
            }
            Some(n) if n > 0 => {
                use rand::seq::IteratorRandom;
                let n = (n as usize).min(fields.len());
                fields
                    .iter()
                    .choose_multiple(&mut rng, n)
                    .into_iter()
                    .map(|(f, v)| {
                        let val = if with_values {
                            Some(Bytes::copy_from_slice(v))
                        } else {
                            None
                        };
                        ((*f).to_owned(), val)
                    })
                    .collect()
            }
            Some(n) => {
                // negative count: allow duplicates, return |n| entries
                use rand::Rng;
                let n = n.unsigned_abs() as usize;
                (0..n)
                    .map(|_| {
                        let idx = rng.random_range(0..fields.len());
                        let (f, v) = fields[idx];
                        let val = if with_values {
                            Some(Bytes::copy_from_slice(v))
                        } else {
                            None
                        };
                        (f.to_owned(), val)
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
    fn hset_creates_hash() {
        let mut ks = Keyspace::new();
        let count = ks
            .hset("h", &[("field1".into(), Bytes::from("value1"))])
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(ks.value_type("h"), "hash");
    }

    #[test]
    fn hset_returns_new_field_count() {
        let mut ks = Keyspace::new();
        // add two new fields
        let count = ks
            .hset(
                "h",
                &[
                    ("f1".into(), Bytes::from("v1")),
                    ("f2".into(), Bytes::from("v2")),
                ],
            )
            .unwrap();
        assert_eq!(count, 2);

        // update one, add one new
        let count = ks
            .hset(
                "h",
                &[
                    ("f1".into(), Bytes::from("updated")),
                    ("f3".into(), Bytes::from("v3")),
                ],
            )
            .unwrap();
        assert_eq!(count, 1); // only f3 is new
    }

    #[test]
    fn hget_returns_value() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("name".into(), Bytes::from("alice"))])
            .unwrap();
        let val = ks.hget("h", "name").unwrap();
        assert_eq!(val, Some(Bytes::from("alice")));
    }

    #[test]
    fn hget_missing_field_returns_none() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("a".into(), Bytes::from("1"))]).unwrap();
        assert_eq!(ks.hget("h", "b").unwrap(), None);
    }

    #[test]
    fn hget_missing_key_returns_none() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.hget("missing", "field").unwrap(), None);
    }

    #[test]
    fn hgetall_returns_all_fields() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
            ],
        )
        .unwrap();
        let mut fields = ks.hgetall("h").unwrap();
        fields.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], ("a".into(), Bytes::from("1")));
        assert_eq!(fields[1], ("b".into(), Bytes::from("2")));
    }

    #[test]
    fn hdel_removes_fields() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
                ("c".into(), Bytes::from("3")),
            ],
        )
        .unwrap();
        let removed = ks.hdel("h", &["a".into(), "c".into()]).unwrap();
        assert_eq!(removed.len(), 2);
        assert!(removed.contains(&"a".into()));
        assert!(removed.contains(&"c".into()));
        assert_eq!(ks.hlen("h").unwrap(), 1);
    }

    #[test]
    fn hdel_auto_deletes_empty_hash() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("only".into(), Bytes::from("field"))])
            .unwrap();
        ks.hdel("h", &["only".into()]).unwrap();
        assert_eq!(ks.value_type("h"), "none");
    }

    #[test]
    fn hexists_returns_true_for_existing_field() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("field".into(), Bytes::from("val"))])
            .unwrap();
        assert!(ks.hexists("h", "field").unwrap());
    }

    #[test]
    fn hexists_returns_false_for_missing_field() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("a".into(), Bytes::from("1"))]).unwrap();
        assert!(!ks.hexists("h", "missing").unwrap());
    }

    #[test]
    fn hlen_returns_field_count() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
            ],
        )
        .unwrap();
        assert_eq!(ks.hlen("h").unwrap(), 2);
    }

    #[test]
    fn hlen_missing_key_returns_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.hlen("missing").unwrap(), 0);
    }

    #[test]
    fn hincrby_new_field() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("x".into(), Bytes::from("ignored"))])
            .unwrap();
        let val = ks.hincrby("h", "counter", 5).unwrap();
        assert_eq!(val, 5);
    }

    #[test]
    fn hincrby_existing_field() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("n".into(), Bytes::from("10"))]).unwrap();
        let val = ks.hincrby("h", "n", 3).unwrap();
        assert_eq!(val, 13);
    }

    #[test]
    fn hincrby_negative_delta() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("n".into(), Bytes::from("10"))]).unwrap();
        let val = ks.hincrby("h", "n", -7).unwrap();
        assert_eq!(val, 3);
    }

    #[test]
    fn hincrby_non_integer_returns_error() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("s".into(), Bytes::from("notanumber"))])
            .unwrap();
        assert_eq!(
            ks.hincrby("h", "s", 1).unwrap_err(),
            IncrError::NotAnInteger
        );
    }

    #[test]
    fn hkeys_returns_field_names() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("alpha".into(), Bytes::from("1")),
                ("beta".into(), Bytes::from("2")),
            ],
        )
        .unwrap();
        let mut keys = ks.hkeys("h").unwrap();
        keys.sort();
        assert_eq!(keys, vec!["alpha", "beta"]);
    }

    #[test]
    fn hvals_returns_values() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("x")),
                ("b".into(), Bytes::from("y")),
            ],
        )
        .unwrap();
        let mut vals = ks.hvals("h").unwrap();
        vals.sort();
        assert_eq!(vals, vec![Bytes::from("x"), Bytes::from("y")]);
    }

    #[test]
    fn hmget_returns_values_for_existing_fields() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
            ],
        )
        .unwrap();
        let vals = ks
            .hmget("h", &["a".into(), "missing".into(), "b".into()])
            .unwrap();
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0], Some(Bytes::from("1")));
        assert_eq!(vals[1], None);
        assert_eq!(vals[2], Some(Bytes::from("2")));
    }

    #[test]
    fn hash_on_string_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("string"), None, false, false);
        assert!(ks.hset("s", &[("f".into(), Bytes::from("v"))]).is_err());
        assert!(ks.hget("s", "f").is_err());
        assert!(ks.hgetall("s").is_err());
        assert!(ks.hdel("s", &["f".into()]).is_err());
        assert!(ks.hexists("s", "f").is_err());
        assert!(ks.hlen("s").is_err());
        assert!(ks.hincrby("s", "f", 1).is_err());
        assert!(ks.hkeys("s").is_err());
        assert!(ks.hvals("s").is_err());
        assert!(ks.hmget("s", &["f".into()]).is_err());
    }

    #[test]
    fn hincrby_overflow_returns_error() {
        let mut ks = Keyspace::new();
        // set field to near max
        ks.hset("h", &[("count".into(), Bytes::from(i64::MAX.to_string()))])
            .unwrap();

        // try to increment by 1 - should overflow
        let result = ks.hincrby("h", "count", 1);
        assert!(result.is_err());
    }

    #[test]
    fn hincrby_on_non_integer_returns_error() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("field".into(), Bytes::from("not_a_number"))])
            .unwrap();

        let result = ks.hincrby("h", "field", 1);
        assert!(result.is_err());
    }

    // --- scan_hash ---

    #[test]
    fn scan_hash_returns_all() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
                ("c".into(), Bytes::from("3")),
            ],
        )
        .unwrap();
        let (cursor, fields) = ks.scan_hash("h", 0, 100, None).unwrap();
        assert_eq!(cursor, 0);
        assert_eq!(fields.len(), 3);
    }

    #[test]
    fn scan_hash_missing_key() {
        let mut ks = Keyspace::new();
        let (cursor, fields) = ks.scan_hash("missing", 0, 10, None).unwrap();
        assert_eq!(cursor, 0);
        assert!(fields.is_empty());
    }

    #[test]
    fn scan_hash_wrong_type() {
        let mut ks = Keyspace::new();
        ks.set("h".into(), Bytes::from("string"), None, false, false);
        assert!(ks.scan_hash("h", 0, 10, None).is_err());
    }

    #[test]
    fn scan_hash_with_pattern() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("name".into(), Bytes::from("alice")),
                ("age".into(), Bytes::from("30")),
                ("nickname".into(), Bytes::from("ali")),
            ],
        )
        .unwrap();
        let (_, fields) = ks.scan_hash("h", 0, 100, Some("n*")).unwrap();
        assert_eq!(fields.len(), 2);
        assert!(fields.iter().all(|(f, _)| f.starts_with('n')));
    }

    #[test]
    fn scan_hash_pagination() {
        let mut ks = Keyspace::new();
        let fields: Vec<(String, Bytes)> = (0..20)
            .map(|i| (format!("f{i}"), Bytes::from(format!("v{i}"))))
            .collect();
        ks.hset("h", &fields).unwrap();

        let mut collected = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (next, batch) = ks.scan_hash("h", cursor, 5, None).unwrap();
            collected.extend(batch);
            if next == 0 {
                break;
            }
            cursor = next;
        }
        assert_eq!(collected.len(), 20);
    }

    #[test]
    fn hash_auto_deleted_when_empty() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("f1".into(), Bytes::from("v1")),
                ("f2".into(), Bytes::from("v2")),
            ],
        )
        .unwrap();
        assert_eq!(ks.len(), 1);

        // delete all fields
        ks.hdel("h", &["f1".into(), "f2".into()]).unwrap();

        // hash should be auto-deleted
        assert_eq!(ks.len(), 0);
        assert!(!ks.exists("h"));
    }

    // --- hrandfield ---

    #[test]
    fn hrandfield_no_count_returns_single_field() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
                ("c".into(), Bytes::from("3")),
            ],
        )
        .unwrap();
        let result = ks.hrandfield("h", None, false).unwrap();
        assert_eq!(result.len(), 1);
        assert!(["a", "b", "c"].contains(&result[0].0.as_str()));
        // no count means no value, even with_values would be ignored
        assert!(result[0].1.is_none());
    }

    #[test]
    fn hrandfield_positive_count_distinct() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
                ("c".into(), Bytes::from("3")),
            ],
        )
        .unwrap();
        let result = ks.hrandfield("h", Some(2), false).unwrap();
        assert_eq!(result.len(), 2);
        // all returned should be valid fields
        for (f, v) in &result {
            assert!(["a", "b", "c"].contains(&f.as_str()));
            assert!(v.is_none());
        }
        // distinct
        let unique: std::collections::HashSet<_> = result.iter().map(|(f, _)| f).collect();
        assert_eq!(unique.len(), 2);
    }

    #[test]
    fn hrandfield_positive_count_capped_at_hash_size() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
            ],
        )
        .unwrap();
        let result = ks.hrandfield("h", Some(10), false).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn hrandfield_negative_count_allows_duplicates() {
        let mut ks = Keyspace::new();
        ks.hset("h", &[("only".into(), Bytes::from("v"))]).unwrap();
        let result = ks.hrandfield("h", Some(-5), false).unwrap();
        assert_eq!(result.len(), 5);
        assert!(result.iter().all(|(f, _)| f == "only"));
    }

    #[test]
    fn hrandfield_with_values() {
        let mut ks = Keyspace::new();
        ks.hset(
            "h",
            &[
                ("field".into(), Bytes::from("value")),
                ("other".into(), Bytes::from("data")),
            ],
        )
        .unwrap();
        let result = ks.hrandfield("h", Some(2), true).unwrap();
        assert_eq!(result.len(), 2);
        for (f, v) in &result {
            assert!(["field", "other"].contains(&f.as_str()));
            assert!(v.is_some());
        }
    }

    #[test]
    fn hrandfield_missing_key_returns_empty() {
        let mut ks = Keyspace::new();
        assert!(ks.hrandfield("missing", None, false).unwrap().is_empty());
        assert!(ks.hrandfield("missing", Some(5), true).unwrap().is_empty());
    }

    #[test]
    fn hrandfield_wrong_type_returns_error() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert!(ks.hrandfield("s", None, false).is_err());
    }
}
