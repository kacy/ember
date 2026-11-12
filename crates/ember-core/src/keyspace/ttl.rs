//! Key expiry: EXPIRE and its variants, PERSIST, and the TTL queries.

use super::*;

impl Keyspace {
    /// Sets an expiration on an existing key. Returns `true` if the key
    /// exists (and the TTL was set), `false` if the key doesn't exist.
    pub fn expire(&mut self, key: &str, seconds: u64) -> bool {
        self.set_deadline(
            key,
            time::now_ms().saturating_add(seconds.saturating_mul(1000)),
        )
    }

    /// Returns the TTL status for a key, following Redis semantics:
    /// - `Seconds(n)` if the key has a TTL
    /// - `NoExpiry` if the key exists without a TTL
    /// - `NotFound` if the key doesn't exist
    pub fn ttl(&mut self, key: &str) -> TtlResult {
        match self.deadline(key) {
            None => TtlResult::NotFound,
            Some(at) => time::remaining_secs(at).map_or(TtlResult::NoExpiry, TtlResult::Seconds),
        }
    }

    /// Removes the expiration from a key.
    ///
    /// Returns `true` if the key existed and had a timeout that was removed.
    /// Returns `false` if the key doesn't exist or has no expiration.
    pub fn persist(&mut self, key: &str) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                if entry.expires_at_ms != 0 {
                    entry.expires_at_ms = 0;
                    self.expiring.swap_remove(key);
                    self.bump_version(key);
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }

    /// Returns the TTL status for a key in milliseconds, following Redis semantics:
    /// - `Milliseconds(n)` if the key has a TTL
    /// - `NoExpiry` if the key exists without a TTL
    /// - `NotFound` if the key doesn't exist
    pub fn pttl(&mut self, key: &str) -> TtlResult {
        match self.deadline(key) {
            None => TtlResult::NotFound,
            Some(at) => time::remaining_ms(at).map_or(TtlResult::NoExpiry, TtlResult::Milliseconds),
        }
    }

    /// Sets an expiration on an existing key in milliseconds.
    ///
    /// Returns `true` if the key exists (and the TTL was set),
    /// `false` if the key doesn't exist.
    pub fn pexpire(&mut self, key: &str, millis: u64) -> bool {
        self.set_deadline(key, time::now_ms().saturating_add(millis))
    }

    /// Sets an expiration at an absolute Unix timestamp (seconds).
    ///
    /// Returns `true` if the key exists and the expiry was set,
    /// `false` if the key doesn't exist.
    pub fn expireat(&mut self, key: &str, unix_secs: u64) -> bool {
        self.set_deadline(
            key,
            time::unix_ms_to_monotonic_ms(unix_secs.saturating_mul(1000)),
        )
    }

    /// Sets an expiration at an absolute Unix timestamp (milliseconds).
    ///
    /// Returns `true` if the key exists and the expiry was set,
    /// `false` if the key doesn't exist.
    pub fn pexpireat(&mut self, key: &str, unix_ms: u64) -> bool {
        self.set_deadline(key, time::unix_ms_to_monotonic_ms(unix_ms))
    }

    /// Returns the absolute Unix timestamp (seconds) when the key expires.
    ///
    /// Returns `-2` if the key doesn't exist, `-1` if it has no expiry.
    pub fn expiretime(&mut self, key: &str) -> i64 {
        self.expiry_unix_ms(key)
            .map_or_else(|code| code, |ms| (ms / 1000) as i64)
    }

    /// Returns the absolute Unix timestamp (milliseconds) when the key expires.
    ///
    /// Returns `-2` if the key doesn't exist, `-1` if it has no expiry.
    pub fn pexpiretime(&mut self, key: &str) -> i64 {
        self.expiry_unix_ms(key)
            .map_or_else(|code| code, |ms| ms as i64)
    }

    /// Sets the monotonic deadline of `key`. Returns `false` if the key
    /// doesn't exist.
    fn set_deadline(&mut self, key: &str, deadline_ms: u64) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        let Some(entry) = self.entries.get_mut(key) else {
            return false;
        };
        let had_expiry = entry.expires_at_ms != 0;
        entry.expires_at_ms = deadline_ms;
        self.track_expiry(key, had_expiry, true);
        self.bump_version(key);
        true
    }

    /// Returns the monotonic deadline of `key` (0 for none), or `None` if
    /// the key doesn't exist.
    fn deadline(&mut self, key: &str) -> Option<u64> {
        if self.remove_if_expired(key) {
            return None;
        }
        self.entries.get(key).map(|e| e.expires_at_ms)
    }

    /// Returns when `key` expires as a Unix time in milliseconds, or the
    /// Redis reply code: -2 when the key doesn't exist, -1 when it has no
    /// expiry.
    fn expiry_unix_ms(&mut self, key: &str) -> Result<u64, i64> {
        let at = self.deadline(key).ok_or(-2)?;
        time::monotonic_to_unix_ms(at).ok_or(-1)
    }
}
