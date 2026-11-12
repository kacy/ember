//! Key expiry: EXPIRE and its variants, PERSIST, and the TTL queries.

use super::*;

impl Keyspace {
    /// Sets an expiration on an existing key. Returns `true` if the key
    /// exists (and the TTL was set), `false` if the key doesn't exist.
    pub fn expire(&mut self, key: &str, seconds: u64) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                if entry.expires_at_ms == 0 {
                    self.expiring.insert(key.into());
                }
                entry.expires_at_ms = time::now_ms().saturating_add(seconds.saturating_mul(1000));
                self.bump_version(key);
                true
            }
            None => false,
        }
    }

    /// Returns the TTL status for a key, following Redis semantics:
    /// - `Seconds(n)` if the key has a TTL
    /// - `NoExpiry` if the key exists without a TTL
    /// - `NotFound` if the key doesn't exist
    pub fn ttl(&mut self, key: &str) -> TtlResult {
        if self.remove_if_expired(key) {
            return TtlResult::NotFound;
        }
        match self.entries.get(key) {
            Some(entry) => match time::remaining_secs(entry.expires_at_ms) {
                Some(secs) => TtlResult::Seconds(secs),
                None => TtlResult::NoExpiry,
            },
            None => TtlResult::NotFound,
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
        if self.remove_if_expired(key) {
            return TtlResult::NotFound;
        }
        match self.entries.get(key) {
            Some(entry) => match time::remaining_ms(entry.expires_at_ms) {
                Some(ms) => TtlResult::Milliseconds(ms),
                None => TtlResult::NoExpiry,
            },
            None => TtlResult::NotFound,
        }
    }

    /// Sets an expiration on an existing key in milliseconds.
    ///
    /// Returns `true` if the key exists (and the TTL was set),
    /// `false` if the key doesn't exist.
    pub fn pexpire(&mut self, key: &str, millis: u64) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                if entry.expires_at_ms == 0 {
                    self.expiring.insert(key.into());
                }
                entry.expires_at_ms = time::now_ms().saturating_add(millis);
                self.bump_version(key);
                true
            }
            None => false,
        }
    }

    /// Sets an expiration at an absolute Unix timestamp (seconds).
    ///
    /// Returns `true` if the key exists and the expiry was set,
    /// `false` if the key doesn't exist.
    pub fn expireat(&mut self, key: &str, unix_secs: u64) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                if entry.expires_at_ms == 0 {
                    self.expiring.insert(key.into());
                }
                entry.expires_at_ms = time::unix_ms_to_monotonic_ms(unix_secs.saturating_mul(1000));
                self.bump_version(key);
                true
            }
            None => false,
        }
    }

    /// Sets an expiration at an absolute Unix timestamp (milliseconds).
    ///
    /// Returns `true` if the key exists and the expiry was set,
    /// `false` if the key doesn't exist.
    pub fn pexpireat(&mut self, key: &str, unix_ms: u64) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        match self.entries.get_mut(key) {
            Some(entry) => {
                if entry.expires_at_ms == 0 {
                    self.expiring.insert(key.into());
                }
                entry.expires_at_ms = time::unix_ms_to_monotonic_ms(unix_ms);
                self.bump_version(key);
                true
            }
            None => false,
        }
    }

    /// Returns the absolute Unix timestamp (seconds) when the key expires.
    ///
    /// Returns `-2` if the key doesn't exist, `-1` if it has no expiry.
    pub fn expiretime(&mut self, key: &str) -> i64 {
        if self.remove_if_expired(key) {
            return -2;
        }
        match self.entries.get(key) {
            None => -2,
            Some(entry) => match time::monotonic_to_unix_ms(entry.expires_at_ms) {
                None => -1,
                Some(unix_ms) => (unix_ms / 1000) as i64,
            },
        }
    }

    /// Returns the absolute Unix timestamp (milliseconds) when the key expires.
    ///
    /// Returns `-2` if the key doesn't exist, `-1` if it has no expiry.
    pub fn pexpiretime(&mut self, key: &str) -> i64 {
        if self.remove_if_expired(key) {
            return -2;
        }
        match self.entries.get(key) {
            None => -2,
            Some(entry) => match time::monotonic_to_unix_ms(entry.expires_at_ms) {
                None => -1,
                Some(unix_ms) => unix_ms as i64,
            },
        }
    }
}
