#[cfg(feature = "protobuf")]
use super::*;
#[cfg(feature = "protobuf")]
use crate::schema::SchemaRegistry;

/// Parameters for [`Keyspace::scan_proto_find`].
#[cfg(feature = "protobuf")]
pub struct ProtoFindOpts<'a> {
    pub cursor: u64,
    pub count: usize,
    pub pattern: Option<&'a str>,
    pub type_name: Option<&'a str>,
    pub field_path: &'a str,
    pub field_value: &'a str,
}

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
            .get(key.as_str())
            .map(|e| e.entry_size(&key))
            .unwrap_or(0);
        let net_increase = new_size.saturating_sub(old_size);

        if !self.enforce_memory_limit(net_increase) {
            return SetResult::OutOfMemory;
        }

        if let Some(old_entry) = self.entries.get(key.as_str()) {
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

        let entry = Entry::new(new_value, expire);
        self.entries
            .insert(CompactString::from(key.as_str()), entry);
        self.bump_version(&key);
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
                    e.touch(self.track_access);
                    Ok(Some(result))
                } else {
                    Err(WrongType)
                }
            }
            None => Ok(None),
        }
    }

    /// Scans all proto keys, returning those where the given field equals the
    /// given value. Walks the keyspace using the same position-cursor logic as
    /// `scan_proto_keys`. Skips keys where field decoding fails (e.g. wrong
    /// type, nested/repeated field) rather than returning an error.
    ///
    /// `opts.field_value` is compared against the field's string representation:
    /// booleans as `"true"/"false"`, integers and floats as their decimal
    /// string, strings verbatim.
    pub fn scan_proto_find(
        &self,
        opts: ProtoFindOpts<'_>,
        registry: &SchemaRegistry,
    ) -> (u64, Vec<String>) {
        let ProtoFindOpts {
            cursor,
            count,
            pattern,
            type_name,
            field_path,
            field_value,
        } = opts;
        let mut keys = Vec::with_capacity(count);
        let mut position = 0u64;
        let target_count = if count == 0 { 10 } else { count };
        let compiled = pattern.map(GlobPattern::new);

        for (key, entry) in self.entries.iter() {
            if entry.is_expired() {
                continue;
            }

            if position < cursor {
                position += 1;
                continue;
            }

            let (entry_type, data) = match &entry.value {
                Value::Proto { type_name: t, data } => (t.as_str(), data.as_ref()),
                _ => {
                    position += 1;
                    continue;
                }
            };

            // optional type filter
            if let Some(wanted) = type_name {
                if entry_type != wanted {
                    position += 1;
                    continue;
                }
            }

            // optional key pattern
            if let Some(ref pat) = compiled {
                if !pat.matches(key) {
                    position += 1;
                    continue;
                }
            }

            // field value comparison — skip on any error (wrong type,
            // non-scalar field, field not found, decode error, etc.)
            let matches = registry
                .get_field_str(entry_type, data, field_path)
                .map(|v| v == field_value)
                .unwrap_or(false);

            if matches {
                keys.push(String::from(&**key));
            }
            position += 1;

            if keys.len() >= target_count {
                return (position, keys);
            }
        }

        (0, keys)
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
