//! Compact monotonic time utilities.
//!
//! Uses a process-local monotonic clock for timestamps that are smaller
//! than std::time::Instant (8 bytes vs 16 bytes for Option<Instant>).

use std::sync::OnceLock;
use std::time::Instant;

/// Returns current monotonic time in milliseconds since process start.
#[inline]
pub fn now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_millis() as u64
}

/// Sentinel value meaning "no expiry".
pub const NO_EXPIRY: u64 = 0;

/// Returns true if the given expiry timestamp has passed.
#[inline]
pub fn is_expired(expires_at_ms: u64) -> bool {
    expires_at_ms != NO_EXPIRY && now_ms() >= expires_at_ms
}

/// Converts a Duration to an absolute expiry timestamp.
#[inline]
pub fn expiry_from_duration(ttl: Option<std::time::Duration>) -> u64 {
    ttl.map(|d| now_ms() + d.as_millis() as u64)
        .unwrap_or(NO_EXPIRY)
}

/// Returns remaining TTL in seconds, or None if no expiry.
#[inline]
pub fn remaining_secs(expires_at_ms: u64) -> Option<u64> {
    if expires_at_ms == NO_EXPIRY {
        None
    } else {
        let now = now_ms();
        Some(expires_at_ms.saturating_sub(now) / 1000)
    }
}

/// Returns remaining TTL in milliseconds, or None if no expiry.
#[inline]
pub fn remaining_ms(expires_at_ms: u64) -> Option<u64> {
    if expires_at_ms == NO_EXPIRY {
        None
    } else {
        let now = now_ms();
        Some(expires_at_ms.saturating_sub(now))
    }
}
