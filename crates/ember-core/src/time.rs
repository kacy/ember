//! Compact monotonic time utilities.
//!
//! Uses a process-local monotonic clock for timestamps that are smaller
//! than std::time::Instant (8 bytes vs 16 bytes for Option<Instant>).
//!
//! All internal expiry values are stored as monotonic milliseconds since
//! process start. Use `monotonic_to_unix_ms` to convert to wall-clock Unix
//! timestamps for commands like EXPIRETIME and PEXPIRETIME.

use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Returns current monotonic time in milliseconds since process start.
#[inline]
pub fn now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_millis() as u64
}

/// Returns current monotonic time in seconds since process start, as u32.
///
/// Used for LRU last-access tracking where millisecond precision isn't
/// needed and the smaller type improves cache-line packing. Wraps after
/// ~136 years — well beyond any realistic process lifetime.
#[inline]
pub fn now_secs() -> u32 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_secs() as u32
}

/// Anchors the monotonic clock to wall time once and caches it for all
/// subsequent conversions. Both `monotonic_to_unix_ms` and
/// `unix_ms_to_monotonic_ms` share this anchor so the two operations are
/// perfectly invertible.
struct ClockAnchor {
    /// Unix epoch ms at the moment we captured the anchor.
    unix_ms_at_capture: u64,
    /// Monotonic ms at the moment we captured the anchor.
    mono_ms_at_capture: u64,
}

fn clock_anchor() -> &'static ClockAnchor {
    static ANCHOR: OnceLock<ClockAnchor> = OnceLock::new();
    ANCHOR.get_or_init(|| {
        let unix_ms_at_capture = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let mono_ms_at_capture = now_ms();
        ClockAnchor {
            unix_ms_at_capture,
            mono_ms_at_capture,
        }
    })
}

/// Converts a monotonic expiry timestamp (ms since process start) to a Unix
/// epoch timestamp in milliseconds.
///
/// The conversion anchors the monotonic clock to wall time on first call using
/// a single `SystemTime::now()` sample. Subsequent calls use only the fast
/// monotonic clock and arithmetic — no system call.
///
/// Returns `None` if `expires_at_ms` is `NO_EXPIRY`.
#[inline]
pub fn monotonic_to_unix_ms(expires_at_ms: u64) -> Option<u64> {
    if expires_at_ms == NO_EXPIRY {
        return None;
    }
    let anchor = clock_anchor();
    // unix_ms = unix_at_capture + (mono_expiry - mono_at_capture)
    let offset = expires_at_ms.saturating_sub(anchor.mono_ms_at_capture);
    Some(anchor.unix_ms_at_capture.saturating_add(offset))
}

/// Converts a Unix epoch timestamp in milliseconds to a monotonic expiry
/// value suitable for storage in `Entry::expires_at_ms`.
///
/// The inverse of `monotonic_to_unix_ms`. Used by EXPIREAT and PEXPIREAT
/// to convert an absolute wall-clock timestamp into the internal monotonic
/// representation. Both functions share the same clock anchor so the
/// conversion is coherent.
///
/// A timestamp in the past results in a value less than or equal to
/// `now_ms()`, which means the key will be treated as already expired on
/// the next access.
#[inline]
pub fn unix_ms_to_monotonic_ms(unix_ms: u64) -> u64 {
    let anchor = clock_anchor();
    // mono_expiry = mono_at_capture + (unix_ms - unix_at_capture)
    let offset = unix_ms.saturating_sub(anchor.unix_ms_at_capture);
    anchor.mono_ms_at_capture.saturating_add(offset)
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
    ttl.map(|d| {
        let ms = d.as_millis().min(u64::MAX as u128) as u64;
        now_ms().saturating_add(ms)
    })
    .unwrap_or(NO_EXPIRY)
}

/// Returns remaining TTL in seconds, or None if no expiry.
#[inline]
pub fn remaining_secs(expires_at_ms: u64) -> Option<u64> {
    remaining(expires_at_ms, 1000)
}

/// Returns remaining TTL in milliseconds, or None if no expiry.
#[inline]
pub fn remaining_ms(expires_at_ms: u64) -> Option<u64> {
    remaining(expires_at_ms, 1)
}

/// Shared implementation for remaining TTL with a unit divisor.
#[inline]
fn remaining(expires_at_ms: u64, divisor: u64) -> Option<u64> {
    if expires_at_ms == NO_EXPIRY {
        None
    } else {
        let now = now_ms();
        Some(expires_at_ms.saturating_sub(now) / divisor)
    }
}
