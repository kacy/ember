//! Whole-key commands: delete, exists, rename, copy, sort, dump and
//! restore, and listing keys.

use super::*;

impl Keyspace {
    /// Removes a key. Returns `true` if the key existed (and wasn't expired).
    ///
    /// When a drop handle is set, large values are dropped on the
    /// background thread instead of inline.
    pub fn del(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        if let Some(entry) = self.entries.swap_remove(key) {
            self.memory.remove(key, &entry.value);
            self.untrack_expiry(key, &entry);
            self.remove_version(key);
            self.defer_drop(entry.value);
            true
        } else {
            false
        }
    }

    /// Removes a key like `del`, but always defers the value's destructor
    /// to the background drop thread (when available). Semantically
    /// identical to DEL — the key is gone immediately, memory is
    /// accounted for immediately, but the actual deallocation happens
    /// off the hot path.
    pub fn unlink(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        if let Some(entry) = self.entries.swap_remove(key) {
            self.memory.remove(key, &entry.value);
            self.untrack_expiry(key, &entry);
            self.remove_version(key);
            // always defer for UNLINK, regardless of value size
            if let Some(ref handle) = self.drop_handle {
                handle.defer_value(entry.value);
            }
            true
        } else {
            false
        }
    }

    /// Returns `true` if the key exists and hasn't expired.
    pub fn exists(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        self.entries.contains_key(key)
    }

    /// Returns a random key from the keyspace, or `None` if empty.
    ///
    /// Expired keys that come up are removed, with a bounded number of
    /// retries.
    pub fn random_key(&mut self) -> Option<String> {
        let mut rng = rand::rng();
        for _ in 0..5 {
            let len = self.entries.len();
            if len == 0 {
                return None;
            }
            let key = self.entries.get_index(rng.random_range(0..len))?.0.clone();
            if self.remove_if_expired(&key) {
                continue;
            }
            return Some(key.to_string());
        }
        None
    }

    /// Updates the last access time for a key. Returns `true` if the key exists.
    pub fn touch(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                entry.touch(self.track_access);
                true
            }
            None => false,
        }
    }

    /// Sorts elements from a list, set, or sorted set.
    ///
    /// Returns the sorted elements as byte strings, or an error if the
    /// key holds the wrong type or numeric parsing fails.
    pub fn sort(
        &mut self,
        key: &str,
        desc: bool,
        alpha: bool,
        limit: Option<(i64, i64)>,
    ) -> Result<Vec<Bytes>, &'static str> {
        if self.remove_if_expired(key) {
            return Ok(Vec::new());
        }
        let entry = match self.entries.get_mut(key) {
            Some(e) => {
                e.touch(self.track_access);
                e
            }
            None => return Ok(Vec::new()),
        };

        // collect elements from the appropriate type
        let mut items: Vec<Bytes> = match &entry.value {
            Value::List(deq) => deq.iter().cloned().collect(),
            Value::Set(set) => set.iter().map(|s| Bytes::from(s.clone())).collect(),
            Value::SortedSet(zset) => zset
                .iter()
                .map(|(m, _)| Bytes::from(m.to_owned()))
                .collect(),
            _ => return Err(WRONGTYPE_MSG),
        };

        // sort
        if alpha {
            items.sort();
            if desc {
                items.reverse();
            }
        } else {
            // numeric sort — parse all elements as f64
            let mut parse_err = false;
            items.sort_by(|a, b| {
                let a_str = std::str::from_utf8(a).unwrap_or("");
                let b_str = std::str::from_utf8(b).unwrap_or("");
                let a_val = a_str.parse::<f64>().unwrap_or_else(|_| {
                    parse_err = true;
                    0.0
                });
                let b_val = b_str.parse::<f64>().unwrap_or_else(|_| {
                    parse_err = true;
                    0.0
                });
                if desc {
                    b_val
                        .partial_cmp(&a_val)
                        .unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    a_val
                        .partial_cmp(&b_val)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
            });
            if parse_err {
                return Err("ERR One or more scores can't be converted into double");
            }
        }

        // apply limit
        if let Some((offset, count)) = limit {
            let offset = offset.max(0) as usize;
            let count = count.max(0) as usize;
            let end = offset.saturating_add(count).min(items.len());
            if offset < items.len() {
                items = items[offset..end].to_vec();
            } else {
                items.clear();
            }
        }

        Ok(items)
    }

    /// Returns the estimated memory usage in bytes for the given key,
    /// or `None` if the key does not exist or is expired.
    ///
    /// O(1), since every value type counts its own size. The estimate covers the
    /// key string, the serialized value, and the per-entry overhead.
    pub fn memory_usage(&mut self, key: &str) -> Option<usize> {
        if self.remove_if_expired(key) {
            return None;
        }
        let entry = self.entries.get(key)?;
        Some(entry.entry_size(key))
    }

    /// Returns all keys matching a glob pattern.
    ///
    /// Warning: O(n) scan of the entire keyspace. Use SCAN for production
    /// workloads with large key counts.
    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let len = self.entries.len();
        if len > 10_000 {
            warn!(
                key_count = len,
                "KEYS on large keyspace, consider SCAN instead"
            );
        }
        self.entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| glob_match(pattern, key))
            .map(|(key, _)| String::from(&**key))
            .collect()
    }

    /// Counts live keys in this keyspace that hash to the given cluster slot.
    ///
    /// O(n) scan over all entries — same cost as KEYS.
    pub fn count_keys_in_slot(&self, slot: u16) -> usize {
        self.entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| ember_protocol::slots::key_slot(key.as_bytes()) == slot)
            .count()
    }

    /// Returns up to `count` live keys that hash to the given cluster slot.
    ///
    /// O(n) scan over all entries — same cost as KEYS.
    pub fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<String> {
        self.entries
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| ember_protocol::slots::key_slot(key.as_bytes()) == slot)
            .take(count)
            .map(|(key, _)| String::from(&**key))
            .collect()
    }

    /// Renames a key to a new name. Returns an error if the source key
    /// doesn't exist. If the destination key already exists, it is overwritten.
    pub fn rename(&mut self, key: &str, newkey: &str) -> Result<(), RenameError> {
        self.remove_if_expired(key);
        self.remove_if_expired(newkey);

        let entry = match self.entries.swap_remove(key) {
            Some(entry) => entry,
            None => return Err(RenameError::NoSuchKey),
        };

        // update memory tracking for old key removal
        self.memory.remove(key, &entry.value);
        self.untrack_expiry(key, &entry);

        // remove destination if it exists
        if let Some(old_dest) = self.entries.swap_remove(newkey) {
            self.memory.remove(newkey, &old_dest.value);
            self.untrack_expiry(newkey, &old_dest);
        }

        // re-insert with the new key name, preserving value and expiry
        self.memory.add(newkey, &entry.value);
        self.track_expiry(newkey, false, entry.expires_at_ms != 0);
        self.remove_version(key);
        self.entries.insert(CompactString::from(newkey), entry);
        self.bump_version(newkey);
        Ok(())
    }

    /// Copies the value at `source` to `destination`. If `replace` is false and
    /// the destination already exists, returns `Ok(false)`. Returns `Ok(true)` on
    /// success.
    pub fn copy(&mut self, source: &str, dest: &str, replace: bool) -> Result<bool, CopyError> {
        self.remove_if_expired(source);
        self.remove_if_expired(dest);

        let src_entry = match self.entries.get(source) {
            Some(e) => e,
            None => return Err(CopyError::NoSuchKey),
        };

        // if destination exists and replace not set, return 0
        if !replace && self.entries.contains_key(dest) {
            return Ok(false);
        }

        // clone value and expiry from source
        let cloned_value = src_entry.value.clone();
        let cloned_expire = if src_entry.expires_at_ms != 0 {
            Some(src_entry.expires_at_ms)
        } else {
            None
        };

        // estimate memory for the new entry
        let new_size = memory::entry_size(dest, &cloned_value);

        // if replacing, account for the old destination's size
        let old_dest_size = self
            .entries
            .get(dest)
            .map(|e| e.entry_size(dest))
            .unwrap_or(0);
        let net_increase = new_size.saturating_sub(old_dest_size);
        if !self.enforce_memory_limit(dest, net_increase) {
            return Err(CopyError::OutOfMemory);
        }

        // remove old destination if replacing
        if let Some(old_dest) = self.entries.swap_remove(dest) {
            self.memory.remove(dest, &old_dest.value);
            self.untrack_expiry(dest, &old_dest);
            self.defer_drop(old_dest.value);
        }

        // insert the clone
        self.memory.add(dest, &cloned_value);
        let has_expiry = cloned_expire.is_some();
        self.track_expiry(dest, false, has_expiry);
        let mut entry = Entry::new(cloned_value, None);
        // preserve the source's absolute expiry timestamp
        if let Some(ts) = cloned_expire {
            entry.expires_at_ms = ts;
        }
        self.entries.insert(CompactString::from(dest), entry);
        self.bump_version(dest);
        Ok(true)
    }

    /// Returns the value and remaining TTL in milliseconds for a single key.
    ///
    /// Returns `None` if the key doesn't exist or is expired. TTL is -1 for
    /// entries with no expiration. Used by MIGRATE/DUMP to serialize a key
    /// for transfer to another node.
    pub fn dump(&mut self, key: &str) -> Option<(&Value, i64)> {
        if self.remove_if_expired(key) {
            return None;
        }
        let entry = self.entries.get(key)?;
        let ttl_ms = match time::remaining_ms(entry.expires_at_ms) {
            Some(ms) => ms.min(i64::MAX as u64) as i64,
            None => -1,
        };
        Some((&entry.value, ttl_ms))
    }

    /// Restores an entry during recovery, bypassing memory limits.
    ///
    /// `ttl` is the remaining time-to-live. If `None`, the key has no expiry.
    /// This is used only during shard startup when loading from
    /// snapshot/AOF — normal writes should go through `set()`.
    pub fn restore(&mut self, key: String, value: Value, ttl: Option<Duration>) {
        let has_expiry = ttl.is_some();

        // if replacing an existing entry, adjust memory tracking
        if let Some(old) = self.entries.get(key.as_str()) {
            self.memory.replace(&key, &old.value, &value);
            self.track_expiry(&key, old.expires_at_ms != 0, has_expiry);
        } else {
            self.memory.add(&key, &value);
            self.track_expiry(&key, false, has_expiry);
        }

        let entry = Entry::new(value, ttl);
        self.entries.insert(CompactString::from(key.clone()), entry);
        self.bump_version(&key);
    }
}
