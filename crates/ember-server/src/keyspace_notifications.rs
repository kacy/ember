//! Keyspace notifications — publish events to pub/sub channels.
//!
//! When `notify-keyspace-events` is configured, Ember publishes a message
//! for each matching event:
//!
//! - `__keyspace@0__:<key>` → message is the event name (e.g. "set")
//! - `__keyevent@0__:<event>` → message is the key name
//!
//! Both channels are only published when the corresponding K (keyspace) or
//! E (keyevent) flag is set, and the event-type flag matches (e.g. `$` for
//! string commands, `g` for generic, `x` for expired).
//!
//! The guard `flags == 0` is a single AtomicU32 load — true zero overhead
//! when notifications are disabled.

use bytes::Bytes;

use crate::pubsub::PubSubManager;

/// Emit keyspace events on `__keyspace@<db>__:<key>` channels.
pub const FLAG_K: u32 = 0x01;
/// Emit keyevent events on `__keyevent@<db>__:<event>` channels.
pub const FLAG_E: u32 = 0x02;
/// Generic commands: DEL, EXPIRE, RENAME, LPUSH (notify on key itself).
pub const FLAG_G: u32 = 0x04;
/// String commands: SET, GETSET, INCR, APPEND, etc.
pub const FLAG_DOLLAR: u32 = 0x08;
/// List commands: LPUSH, RPUSH, LPOP, RPOP, etc.
pub const FLAG_L: u32 = 0x10;
/// Sorted set commands: ZADD, ZINCRBY, ZREM, etc.
pub const FLAG_Z: u32 = 0x20;
/// Hash commands: HSET, HINCRBY, HDEL, etc.
pub const FLAG_H: u32 = 0x40;
/// Set commands: SADD, SREM, SPOP, etc.
pub const FLAG_S: u32 = 0x80;
/// Key expiration events.
pub const FLAG_X: u32 = 0x100;
/// Key eviction events (allkeys-lru policy).
pub const FLAG_D: u32 = 0x200;

/// Parses a keyspace event flag string into a bitmask.
///
/// Accepts any combination of:
/// - `K` — keyspace events
/// - `E` — keyevent events
/// - `g` — generic commands
/// - `$` — string commands
/// - `l` — list commands
/// - `z` — sorted set commands
/// - `h` — hash commands
/// - `s` — set commands
/// - `x` — expired events
/// - `d` — eviction events
/// - `A` — alias for `g$lzxhsd` (all event types)
/// - `""` or `"0"` — disable all notifications
///
/// Unknown characters are silently ignored (Redis compat).
pub fn parse_keyspace_event_flags(s: &str) -> u32 {
    if s.is_empty() || s == "0" {
        return 0;
    }

    let mut flags = 0u32;
    for ch in s.chars() {
        match ch {
            'K' => flags |= FLAG_K,
            'E' => flags |= FLAG_E,
            'g' => flags |= FLAG_G,
            '$' => flags |= FLAG_DOLLAR,
            'l' => flags |= FLAG_L,
            'z' => flags |= FLAG_Z,
            'h' => flags |= FLAG_H,
            's' => flags |= FLAG_S,
            'x' => flags |= FLAG_X,
            'd' => flags |= FLAG_D,
            'A' => {
                flags |= FLAG_G | FLAG_DOLLAR | FLAG_L | FLAG_Z | FLAG_X | FLAG_H | FLAG_S | FLAG_D
            }
            _ => {} // unknown flags silently ignored
        }
    }
    flags
}

/// Publishes keyspace and keyevent notifications for one event.
///
/// `flags` is the current `notify-keyspace-events` bitmask.
/// `event_flag` is the type flag for this specific event (e.g. `FLAG_X` for expired).
/// `event` is the event name (e.g. `"expired"`, `"set"`, `"del"`).
/// `key` is the key that was affected.
///
/// Emits to:
/// - `__keyspace@0__:<key>` with message `<event>` (when FLAG_K is set)
/// - `__keyevent@0__:<event>` with message `<key>` (when FLAG_E is set)
///
/// No-op when neither K nor E is set for this event type.
pub fn notify_keyspace_event(
    flags: u32,
    event_flag: u32,
    event: &str,
    key: &str,
    pubsub: &PubSubManager,
) {
    // both K and E are disabled — nothing to do
    if flags & (FLAG_K | FLAG_E) == 0 {
        return;
    }
    // event type not enabled
    if flags & event_flag == 0 {
        return;
    }

    if flags & FLAG_K != 0 {
        let channel = format!("__keyspace@0__:{key}");
        pubsub.publish(&channel, Bytes::copy_from_slice(event.as_bytes()));
    }

    if flags & FLAG_E != 0 {
        let channel = format!("__keyevent@0__:{event}");
        pubsub.publish(&channel, Bytes::copy_from_slice(key.as_bytes()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_string_returns_zero() {
        assert_eq!(parse_keyspace_event_flags(""), 0);
        assert_eq!(parse_keyspace_event_flags("0"), 0);
    }

    #[test]
    fn individual_flags_parsed() {
        assert_eq!(parse_keyspace_event_flags("K"), FLAG_K);
        assert_eq!(parse_keyspace_event_flags("E"), FLAG_E);
        assert_eq!(parse_keyspace_event_flags("x"), FLAG_X);
        assert_eq!(parse_keyspace_event_flags("$"), FLAG_DOLLAR);
    }

    #[test]
    fn uppercase_a_expands_all_events() {
        let flags = parse_keyspace_event_flags("A");
        assert!(flags & FLAG_G != 0);
        assert!(flags & FLAG_DOLLAR != 0);
        assert!(flags & FLAG_L != 0);
        assert!(flags & FLAG_Z != 0);
        assert!(flags & FLAG_X != 0);
        assert!(flags & FLAG_H != 0);
        assert!(flags & FLAG_S != 0);
        // K and E are NOT included in A
        assert_eq!(flags & FLAG_K, 0);
        assert_eq!(flags & FLAG_E, 0);
    }

    #[test]
    fn kex_is_common_config() {
        // KEA = keyspace + keyevent + all events
        let flags = parse_keyspace_event_flags("KEA");
        assert!(flags & FLAG_K != 0);
        assert!(flags & FLAG_E != 0);
        assert!(flags & FLAG_X != 0);
    }

    #[test]
    fn unknown_chars_ignored() {
        // 'Q' is not a valid flag
        assert_eq!(
            parse_keyspace_event_flags("Kx"),
            parse_keyspace_event_flags("KxQ")
        );
    }
}
