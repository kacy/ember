#[cfg(feature = "protobuf")]
use super::*;

#[cfg(feature = "protobuf")]
impl Keyspace {
    /// Stores a protobuf value. No schema validation here — that's the
    /// server's responsibility. Follows the same pattern as `set()`.
    pub fn proto_set(
        &mut self,
        key: String,
        type_name: String,
        data: Bytes,
        expire: Option<Duration>,
    ) -> SetResult {
        let has_expiry = expire.is_some();
        let new_value = Value::Proto { type_name, data };

        let new_size = memory::entry_size(&key, &new_value);
        let old_size = self
            .entries
            .get(&key)
            .map(|e| e.entry_size(&key))
            .unwrap_or(0);
        let net_increase = new_size.saturating_sub(old_size);

        if !self.enforce_memory_limit(net_increase) {
            return SetResult::OutOfMemory;
        }

        if let Some(old_entry) = self.entries.get(&key) {
            self.memory.replace(&key, &old_entry.value, &new_value);
            let had_expiry = old_entry.expires_at_ms != 0;
            match (had_expiry, has_expiry) {
                (false, true) => self.expiry_count += 1,
                (true, false) => self.expiry_count = self.expiry_count.saturating_sub(1),
                _ => {}
            }
        } else {
            self.memory.add(&key, &new_value);
            if has_expiry {
                self.expiry_count += 1;
            }
        }

        self.entries.insert(key, Entry::new(new_value, expire));
        SetResult::Ok
    }

    /// Retrieves a proto value, returning `(type_name, data, remaining_ttl)`
    /// or `None`.
    ///
    /// The remaining TTL is `Some(duration)` if the key has an expiry set,
    /// or `None` for keys that never expire. This allows callers to preserve
    /// the TTL across read-modify-write cycles (e.g. SETFIELD/DELFIELD).
    ///
    /// Returns `Err(WrongType)` if the key holds a different value type.
    pub fn proto_get(
        &mut self,
        key: &str,
    ) -> Result<Option<(String, Bytes, Option<Duration>)>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }
        match self.entries.get_mut(key) {
            Some(e) => {
                if let Value::Proto { type_name, data } = &e.value {
                    let remaining = if e.expires_at_ms == 0 {
                        None
                    } else {
                        let now = time::now_ms();
                        Some(Duration::from_millis(e.expires_at_ms.saturating_sub(now)))
                    };
                    let result = (type_name.clone(), data.clone(), remaining);
                    e.touch();
                    Ok(Some(result))
                } else {
                    Err(WrongType)
                }
            }
            None => Ok(None),
        }
    }

    /// Returns the protobuf message type name for a key, or `None` if
    /// the key doesn't exist.
    ///
    /// Returns `Err(WrongType)` if the key holds a non-proto value.
    pub fn proto_type(&mut self, key: &str) -> Result<Option<String>, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(None);
        }
        match self.entries.get(key) {
            Some(e) => match &e.value {
                Value::Proto { type_name, .. } => Ok(Some(type_name.clone())),
                _ => Err(WrongType),
            },
            None => Ok(None),
        }
    }
}
