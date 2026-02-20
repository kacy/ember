use super::*;

impl Keyspace {
    /// Retrieves the string value for `key`, or `None` if missing/expired.
    ///
    /// Returns `Err(WrongType)` if the key holds a non-string value.
    /// Expired keys are removed lazily on access. Successful reads update
    /// the entry's last access time for LRU tracking.
    ///
    /// Uses a single hash probe on the common (non-expired) path.
    /// The expired path (rare) does a second probe to remove.
    pub fn get(&mut self, key: &str) -> Result<Option<Value>, WrongType> {
        let expired = match self.entries.get_mut(key) {
            Some(e) if e.is_expired() => true,
            Some(e) => {
                return match &e.value {
                    Value::String(_) => {
                        e.touch();
                        Ok(Some(e.value.clone()))
                    }
                    _ => Err(WrongType),
                };
            }
            None => return Ok(None),
        };
        if expired {
            self.remove_expired_entry(key);
        }
        Ok(None)
    }

    /// Retrieves the raw `Bytes` for a string key, avoiding the `Value`
    /// enum wrapper. `Bytes::clone()` is a cheap refcount increment.
    ///
    /// Returns `Err(WrongType)` if the key holds a non-string value.
    ///
    /// Uses a single hash probe on the common (non-expired) path.
    pub fn get_string(&mut self, key: &str) -> Result<Option<Bytes>, WrongType> {
        let expired = match self.entries.get_mut(key) {
            Some(e) if e.is_expired() => true,
            Some(e) => {
                return match &e.value {
                    Value::String(b) => {
                        let data = b.clone();
                        e.touch();
                        Ok(Some(data))
                    }
                    _ => Err(WrongType),
                };
            }
            None => return Ok(None),
        };
        if expired {
            self.remove_expired_entry(key);
        }
        Ok(None)
    }

    /// Returns the type name of the value at `key`, or "none" if missing.
    pub fn value_type(&mut self, key: &str) -> &'static str {
        if self.remove_if_expired(key) {
            return "none";
        }
        match self.entries.get(key) {
            Some(e) => types::type_name(&e.value),
            None => "none",
        }
    }

    /// Stores a key-value pair with optional NX/XX conditions.
    ///
    /// - `nx`: only set if the key does NOT already exist
    /// - `xx`: only set if the key DOES already exist
    ///
    /// `expire` sets an optional TTL as a duration from now.
    ///
    /// Uses a single lookup for the old entry to handle NX/XX checks,
    /// memory accounting, and expiry tracking together.
    pub fn set(
        &mut self,
        key: String,
        value: Bytes,
        expire: Option<Duration>,
        nx: bool,
        xx: bool,
    ) -> SetResult {
        let has_expiry = expire.is_some();
        let new_value = Value::String(value);
        let new_size = memory::entry_size(&key, &new_value);

        // single lookup: check existence, gather old size and expiry state.
        // treat expired entries as non-existent. uses cached_value_size
        // for O(1) size lookup instead of walking the value.
        let old_info = self.entries.get(key.as_str()).and_then(|e| {
            if e.is_expired() {
                None
            } else {
                Some((e.entry_size(&key), e.expires_at_ms != 0))
            }
        });

        // NX/XX condition checks
        let key_exists = old_info.is_some();
        if nx && key_exists {
            return SetResult::Blocked;
        }
        if xx && !key_exists {
            return SetResult::Blocked;
        }

        // memory limit check — for overwrites, only the net increase matters
        let old_size = old_info.map(|(size, _)| size).unwrap_or(0);
        let net_increase = new_size.saturating_sub(old_size);
        if !self.enforce_memory_limit(net_increase) {
            return SetResult::OutOfMemory;
        }

        // update memory tracking and expiry count
        if let Some((_, had_expiry)) = old_info {
            self.memory.adjust(old_size, new_size);
            self.adjust_expiry_count(had_expiry, has_expiry);
        } else {
            // clean up the expired entry if one exists
            self.remove_if_expired(&key);
            self.memory.add(&key, &new_value);
            if has_expiry {
                self.expiry_count += 1;
            }
        }

        self.entries
            .insert(key.into_boxed_str(), Entry::new(new_value, expire));
        SetResult::Ok
    }

    /// Increments the integer value of a key by 1.
    ///
    /// If the key doesn't exist, it's initialized to 0 before incrementing.
    /// Returns the new value after the operation.
    pub fn incr(&mut self, key: &str) -> Result<i64, IncrError> {
        self.incr_by(key, 1)
    }

    /// Decrements the integer value of a key by 1.
    ///
    /// If the key doesn't exist, it's initialized to 0 before decrementing.
    /// Returns the new value after the operation.
    pub fn decr(&mut self, key: &str) -> Result<i64, IncrError> {
        self.incr_by(key, -1)
    }

    /// Adds `delta` to the current integer value of the key, creating it
    /// if necessary. Used by INCR, DECR, INCRBY, and DECRBY.
    ///
    /// Preserves the existing TTL when updating an existing key.
    pub fn incr_by(&mut self, key: &str, delta: i64) -> Result<i64, IncrError> {
        self.remove_if_expired(key);

        // read current value and TTL
        let (current, existing_expire) = match self.entries.get(key) {
            Some(entry) => {
                let val = match &entry.value {
                    Value::String(data) => {
                        let s = std::str::from_utf8(data).map_err(|_| IncrError::NotAnInteger)?;
                        s.parse::<i64>().map_err(|_| IncrError::NotAnInteger)?
                    }
                    _ => return Err(IncrError::WrongType),
                };
                let expire = time::remaining_ms(entry.expires_at_ms).map(Duration::from_millis);
                (val, expire)
            }
            None => (0, None),
        };

        let new_val = current.checked_add(delta).ok_or(IncrError::Overflow)?;
        let new_bytes = Bytes::from(new_val.to_string());

        match self.set(key.to_owned(), new_bytes, existing_expire, false, false) {
            SetResult::Ok | SetResult::Blocked => Ok(new_val),
            SetResult::OutOfMemory => Err(IncrError::OutOfMemory),
        }
    }

    /// Adds a float `delta` to the current value of the key, creating it
    /// if necessary. Used by INCRBYFLOAT.
    ///
    /// Preserves the existing TTL when updating an existing key.
    /// Returns the new value as a string (matching Redis behavior).
    pub fn incr_by_float(&mut self, key: &str, delta: f64) -> Result<String, IncrFloatError> {
        self.remove_if_expired(key);

        let (current, existing_expire) = match self.entries.get(key) {
            Some(entry) => {
                let val = match &entry.value {
                    Value::String(data) => {
                        let s = std::str::from_utf8(data).map_err(|_| IncrFloatError::NotAFloat)?;
                        s.parse::<f64>().map_err(|_| IncrFloatError::NotAFloat)?
                    }
                    _ => return Err(IncrFloatError::WrongType),
                };
                let expire = time::remaining_ms(entry.expires_at_ms).map(Duration::from_millis);
                (val, expire)
            }
            None => (0.0, None),
        };

        let new_val = current + delta;
        if new_val.is_nan() || new_val.is_infinite() {
            return Err(IncrFloatError::NanOrInfinity);
        }

        // Redis strips trailing zeros: "10.5" not "10.50000..."
        // but keeps at least one decimal if the result is a whole number
        let formatted = format_float(new_val);
        let new_bytes = Bytes::copy_from_slice(formatted.as_bytes());

        match self.set(key.to_owned(), new_bytes, existing_expire, false, false) {
            SetResult::Ok | SetResult::Blocked => Ok(formatted),
            SetResult::OutOfMemory => Err(IncrFloatError::OutOfMemory),
        }
    }

    /// Appends a value to an existing string key, or creates a new key if
    /// it doesn't exist. Returns the new string length.
    pub fn append(&mut self, key: &str, value: &[u8]) -> Result<usize, WriteError> {
        self.remove_if_expired(key);

        match self.entries.get(key) {
            Some(entry) => match &entry.value {
                Value::String(existing) => {
                    let mut new_data = Vec::with_capacity(existing.len() + value.len());
                    new_data.extend_from_slice(existing);
                    new_data.extend_from_slice(value);
                    let new_len = new_data.len();
                    let expire = time::remaining_ms(entry.expires_at_ms).map(Duration::from_millis);
                    match self.set(key.to_owned(), Bytes::from(new_data), expire, false, false) {
                        SetResult::Ok | SetResult::Blocked => Ok(new_len),
                        SetResult::OutOfMemory => Err(WriteError::OutOfMemory),
                    }
                }
                _ => Err(WriteError::WrongType),
            },
            None => {
                let new_len = value.len();
                match self.set(
                    key.to_owned(),
                    Bytes::copy_from_slice(value),
                    None,
                    false,
                    false,
                ) {
                    SetResult::Ok | SetResult::Blocked => Ok(new_len),
                    SetResult::OutOfMemory => Err(WriteError::OutOfMemory),
                }
            }
        }
    }

    /// Returns the length of the string value stored at key.
    /// Returns 0 if the key does not exist.
    pub fn strlen(&mut self, key: &str) -> Result<usize, WrongType> {
        self.remove_if_expired(key);

        match self.entries.get(key) {
            Some(entry) => match &entry.value {
                Value::String(data) => Ok(data.len()),
                _ => Err(WrongType),
            },
            None => Ok(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn set_and_get() {
        let mut ks = Keyspace::new();
        ks.set("hello".into(), Bytes::from("world"), None, false, false);
        assert_eq!(
            ks.get("hello").unwrap(),
            Some(Value::String(Bytes::from("world")))
        );
    }

    #[test]
    fn get_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.get("nope").unwrap(), None);
    }

    #[test]
    fn overwrite_replaces_value() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("first"), None, false, false);
        ks.set("key".into(), Bytes::from("second"), None, false, false);
        assert_eq!(
            ks.get("key").unwrap(),
            Some(Value::String(Bytes::from("second")))
        );
    }

    #[test]
    fn overwrite_clears_old_ttl() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("v1"),
            Some(Duration::from_secs(100)),
            false,
            false,
        );
        // overwrite without TTL — should clear the old one
        ks.set("key".into(), Bytes::from("v2"), None, false, false);
        assert_eq!(ks.ttl("key"), TtlResult::NoExpiry);
    }

    #[test]
    fn expired_key_returns_none() {
        let mut ks = Keyspace::new();
        ks.set(
            "temp".into(),
            Bytes::from("gone"),
            Some(Duration::from_millis(10)),
            false,
            false,
        );
        // wait for expiration
        thread::sleep(Duration::from_millis(30));
        assert_eq!(ks.get("temp").unwrap(), None);
        // should also be gone from exists
        assert!(!ks.exists("temp"));
    }

    #[test]
    fn get_on_list_key_returns_wrongtype() {
        let mut ks = Keyspace::new();
        let mut list = std::collections::VecDeque::new();
        list.push_back(Bytes::from("item"));
        ks.restore("mylist".into(), Value::List(list), None);

        assert!(ks.get("mylist").is_err());
    }

    #[test]
    fn value_type_returns_correct_types() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.value_type("missing"), "none");

        ks.set("s".into(), Bytes::from("val"), None, false, false);
        assert_eq!(ks.value_type("s"), "string");

        let mut list = std::collections::VecDeque::new();
        list.push_back(Bytes::from("item"));
        ks.restore("l".into(), Value::List(list), None);
        assert_eq!(ks.value_type("l"), "list");

        ks.zadd("z", &[(1.0, "a".into())], &ZAddFlags::default())
            .unwrap();
        assert_eq!(ks.value_type("z"), "zset");
    }

    #[test]
    fn incr_new_key_defaults_to_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.incr("counter").unwrap(), 1);
        // verify the stored value
        match ks.get("counter").unwrap() {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("1")),
            other => panic!("expected String(\"1\"), got {other:?}"),
        }
    }

    #[test]
    fn incr_existing_value() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None, false, false);
        assert_eq!(ks.incr("n").unwrap(), 11);
    }

    #[test]
    fn decr_new_key_defaults_to_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.decr("counter").unwrap(), -1);
    }

    #[test]
    fn decr_existing_value() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None, false, false);
        assert_eq!(ks.decr("n").unwrap(), 9);
    }

    #[test]
    fn incr_non_integer_returns_error() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("notanum"), None, false, false);
        assert_eq!(ks.incr("s").unwrap_err(), IncrError::NotAnInteger);
    }

    #[test]
    fn incr_on_list_returns_wrongtype() {
        let mut ks = Keyspace::new();
        ks.lpush("list", &[Bytes::from("a")]).unwrap();
        assert_eq!(ks.incr("list").unwrap_err(), IncrError::WrongType);
    }

    #[test]
    fn incr_overflow_returns_error() {
        let mut ks = Keyspace::new();
        ks.set(
            "max".into(),
            Bytes::from(i64::MAX.to_string()),
            None,
            false,
            false,
        );
        assert_eq!(ks.incr("max").unwrap_err(), IncrError::Overflow);
    }

    #[test]
    fn decr_overflow_returns_error() {
        let mut ks = Keyspace::new();
        ks.set(
            "min".into(),
            Bytes::from(i64::MIN.to_string()),
            None,
            false,
            false,
        );
        assert_eq!(ks.decr("min").unwrap_err(), IncrError::Overflow);
    }

    #[test]
    fn incr_preserves_ttl() {
        let mut ks = Keyspace::new();
        ks.set(
            "n".into(),
            Bytes::from("5"),
            Some(Duration::from_secs(60)),
            false,
            false,
        );
        ks.incr("n").unwrap();
        match ks.ttl("n") {
            TtlResult::Seconds(s) => assert!((58..=60).contains(&s)),
            other => panic!("expected TTL preserved, got {other:?}"),
        }
    }

    #[test]
    fn incr_at_max_value_overflows() {
        let mut ks = Keyspace::new();
        ks.set(
            "counter".into(),
            Bytes::from(i64::MAX.to_string()),
            None,
            false,
            false,
        );

        let result = ks.incr("counter");
        assert!(matches!(result, Err(IncrError::Overflow)));
    }

    #[test]
    fn decr_at_min_value_underflows() {
        let mut ks = Keyspace::new();
        ks.set(
            "counter".into(),
            Bytes::from(i64::MIN.to_string()),
            None,
            false,
            false,
        );

        let result = ks.decr("counter");
        assert!(matches!(result, Err(IncrError::Overflow)));
    }

    #[test]
    fn incr_by_float_basic() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10.5"), None, false, false);
        let result = ks.incr_by_float("n", 2.3).unwrap();
        let f: f64 = result.parse().unwrap();
        assert!((f - 12.8).abs() < 0.001);
    }

    #[test]
    fn incr_by_float_new_key() {
        let mut ks = Keyspace::new();
        let result = ks.incr_by_float("new", 2.72).unwrap();
        let f: f64 = result.parse().unwrap();
        assert!((f - 2.72).abs() < 0.001);
    }

    #[test]
    fn incr_by_float_negative() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None, false, false);
        let result = ks.incr_by_float("n", -3.5).unwrap();
        let f: f64 = result.parse().unwrap();
        assert!((f - 6.5).abs() < 0.001);
    }

    #[test]
    fn incr_by_float_wrong_type() {
        let mut ks = Keyspace::new();
        ks.lpush("mylist", &[Bytes::from("a")]).unwrap();
        let err = ks.incr_by_float("mylist", 1.0).unwrap_err();
        assert_eq!(err, IncrFloatError::WrongType);
    }

    #[test]
    fn incr_by_float_not_a_float() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("hello"), None, false, false);
        let err = ks.incr_by_float("s", 1.0).unwrap_err();
        assert_eq!(err, IncrFloatError::NotAFloat);
    }

    #[test]
    fn append_to_existing_key() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("hello"), None, false, false);
        let len = ks.append("key", b" world").unwrap();
        assert_eq!(len, 11);
        assert_eq!(
            ks.get("key").unwrap(),
            Some(Value::String(Bytes::from("hello world")))
        );
    }

    #[test]
    fn append_to_new_key() {
        let mut ks = Keyspace::new();
        let len = ks.append("new", b"value").unwrap();
        assert_eq!(len, 5);
        assert_eq!(
            ks.get("new").unwrap(),
            Some(Value::String(Bytes::from("value")))
        );
    }

    #[test]
    fn append_wrong_type() {
        let mut ks = Keyspace::new();
        ks.lpush("mylist", &[Bytes::from("a")]).unwrap();
        let err = ks.append("mylist", b"value").unwrap_err();
        assert_eq!(err, WriteError::WrongType);
    }

    #[test]
    fn strlen_existing_key() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("hello"), None, false, false);
        assert_eq!(ks.strlen("key").unwrap(), 5);
    }

    #[test]
    fn strlen_missing_key() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.strlen("missing").unwrap(), 0);
    }

    #[test]
    fn strlen_wrong_type() {
        let mut ks = Keyspace::new();
        ks.lpush("mylist", &[Bytes::from("a")]).unwrap();
        let err = ks.strlen("mylist").unwrap_err();
        assert_eq!(err, WrongType);
    }

    #[test]
    fn empty_string_key_works() {
        let mut ks = Keyspace::new();
        ks.set("".into(), Bytes::from("value"), None, false, false);
        assert_eq!(
            ks.get("").unwrap(),
            Some(Value::String(Bytes::from("value")))
        );
        assert!(ks.exists(""));
    }

    #[test]
    fn empty_value_works() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from(""), None, false, false);
        assert_eq!(ks.get("key").unwrap(), Some(Value::String(Bytes::from(""))));
    }

    #[test]
    fn binary_data_in_value() {
        let mut ks = Keyspace::new();
        // value with null bytes and other binary data
        let binary = Bytes::from(vec![0u8, 1, 2, 255, 0, 128]);
        ks.set("binary".into(), binary.clone(), None, false, false);
        assert_eq!(ks.get("binary").unwrap(), Some(Value::String(binary)));
    }

    #[test]
    fn format_float_integers() {
        assert_eq!(super::format_float(10.0), "10");
        assert_eq!(super::format_float(0.0), "0");
        assert_eq!(super::format_float(-5.0), "-5");
    }

    #[test]
    fn format_float_decimals() {
        assert_eq!(super::format_float(2.72), "2.72");
        assert_eq!(super::format_float(10.5), "10.5");
    }
}
