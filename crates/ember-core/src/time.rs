//! Compact monotonic time utilities.
//!
//! Uses a process-local monotonic clock for timestamps that are smaller
//! than std::time::Instant (8 bytes vs 16 bytes for Option<Instant>).
//!
//! All internal expiry values are stored as monotonic milliseconds since
//! process start. Use `monotonic_to_unix_ms` to convert to wall-clock Unix
//! timestamps for commands like EXPIRETIME and PEXPIRETIME. The conversion
//! follows the wall clock when it is stepped.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Returns current monotonic time in milliseconds since process start.
///
/// The count starts at 1 rather than 0, so no deadline derived from it can
/// equal [`NO_EXPIRY`]. Otherwise a past EXPIREAT handled in the first
/// millisecond of the clock would be stored as 0 and the key would never
/// expire.
#[inline]
pub fn now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_millis() as u64 + 1
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

/// How far the wall clock may move against the monotonic clock before the
/// offset between them is measured again. Below this the cached offset is
/// kept, so converting a time one way and back gives the same value.
const MAX_CLOCK_DRIFT_MS: i64 = 1000;

/// Unix ms minus monotonic ms, as last measured. `i64::MIN` until the first
/// measurement.
static WALL_OFFSET_MS: AtomicI64 = AtomicI64::new(i64::MIN);

/// The offset that turns monotonic ms into Unix ms.
///
/// The two clocks drift apart when the wall clock is stepped, as by NTP, or
/// when a paused VM resumes. Measuring the offset once at startup carried
/// that drift into every EXPIREAT and EXPIRETIME for the life of the
/// process, so each call measures it again and keeps the new value once it
/// is more than [`MAX_CLOCK_DRIFT_MS`] from the cached one.
fn wall_offset_ms() -> i64 {
    let unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64;
    let measured = unix_ms - now_ms() as i64;
    let cached = WALL_OFFSET_MS.load(Ordering::Relaxed);
    let offset = next_offset(cached, measured);
    if offset != cached {
        WALL_OFFSET_MS.store(offset, Ordering::Relaxed);
    }
    offset
}

/// The offset to use, given the `cached` one and one just `measured`: the
/// cached one unless there is none yet or the clocks drifted too far.
fn next_offset(cached: i64, measured: i64) -> i64 {
    if cached != i64::MIN && (measured - cached).abs() <= MAX_CLOCK_DRIFT_MS {
        cached
    } else {
        measured
    }
}

/// Converts a monotonic expiry timestamp (ms since process start) to a Unix
/// epoch timestamp in milliseconds.
///
/// Returns `None` if `expires_at_ms` is `NO_EXPIRY`.
#[inline]
pub fn monotonic_to_unix_ms(expires_at_ms: u64) -> Option<u64> {
    if expires_at_ms == NO_EXPIRY {
        return None;
    }
    let unix = i128::from(expires_at_ms) + i128::from(wall_offset_ms());
    Some(unix.clamp(0, i128::from(u64::MAX)) as u64)
}

/// Converts a Unix epoch timestamp in milliseconds to a monotonic expiry
/// value suitable for storage in `Entry::expires_at_ms`.
///
/// The inverse of `monotonic_to_unix_ms`. Used by EXPIREAT and PEXPIREAT
/// to convert an absolute wall-clock timestamp into the internal monotonic
/// representation.
///
/// A timestamp in the past results in a value less than or equal to
/// `now_ms()`, which means the key will be treated as already expired on
/// the next access. The result is at least 1, since 0 means no expiry.
#[inline]
pub fn unix_ms_to_monotonic_ms(unix_ms: u64) -> u64 {
    let mono = i128::from(unix_ms) - i128::from(wall_offset_ms());
    mono.clamp(1, i128::from(u64::MAX)) as u64
}

/// Sentinel value meaning "no expiry".
pub const NO_EXPIRY: u64 = 0;

/// Returns true if the given expiry timestamp has passed. Nothing counts
/// as expired on a thread that holds [`hold_expiry`].
#[inline]
pub fn is_expired(expires_at_ms: u64) -> bool {
    expires_at_ms != NO_EXPIRY && now_ms() >= expires_at_ms && !HOLD_EXPIRY.get()
}

thread_local! {
    static HOLD_EXPIRY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Stops keys from expiring on this thread until the guard drops.
///
/// AOF replay holds it. Each record replays at the current time, not the
/// time it was written, so a key whose first TTL has passed would be
/// deleted before the later records that extended or removed that TTL
/// could apply.
pub(crate) fn hold_expiry() -> HoldExpiry {
    HOLD_EXPIRY.set(true);
    HoldExpiry
}

/// Guard returned by [`hold_expiry`].
pub(crate) struct HoldExpiry;

impl Drop for HoldExpiry {
    fn drop(&mut self) {
        HOLD_EXPIRY.set(false);
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_offset_follows_a_stepped_wall_clock() {
        assert_eq!(next_offset(i64::MIN, 5_000), 5_000);
        // small jitter keeps the cached offset, so conversions round-trip
        assert_eq!(next_offset(5_000, 5_400), 5_000);
        // a step past the limit, as after NTP or a paused VM, is taken
        assert_eq!(next_offset(5_000, 65_000), 65_000);
        assert_eq!(next_offset(5_000, -55_000), -55_000);
    }

    #[test]
    fn unix_times_round_trip_through_monotonic_time() {
        let unix_ms = 4_000_000_000_000;
        let mono = unix_ms_to_monotonic_ms(unix_ms);
        assert_eq!(monotonic_to_unix_ms(mono), Some(unix_ms));
    }
}
