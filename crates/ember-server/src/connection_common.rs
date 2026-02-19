//! Shared constants and utilities for connection handlers.
//!
//! Both the sharded connection handler (`connection.rs`) and concurrent
//! handler (`concurrent_handler.rs`) use these constants to ensure
//! consistent behavior across execution modes.

use std::time::Duration;

use ember_protocol::types::Frame;
use ember_protocol::Command;
use subtle::ConstantTimeEq;

use crate::server::ServerContext;

/// Initial read buffer capacity. 4KB covers most commands comfortably
/// without over-allocating for simple PING/SET/GET workloads.
pub const BUF_CAPACITY: usize = 4096;

/// Maximum read buffer size before we disconnect the client. Prevents
/// a single slow or malicious client from consuming unbounded memory
/// with incomplete frames. Set to 64MB to allow very large pipelined
/// batches while still protecting against runaway growth.
pub const MAX_BUF_SIZE: usize = 64 * 1024 * 1024;

/// How long a connection can be idle (no data received) before we
/// close it. Prevents abandoned connections from leaking resources.
/// 5 minutes matches Redis default behavior.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Maximum number of failed AUTH attempts before the connection is closed.
/// Prevents brute-force password guessing. Matches Redis 6.2+ behavior.
pub const MAX_AUTH_FAILURES: u32 = 10;

/// Maximum number of pub/sub subscriptions a single connection can hold.
/// Prevents a malicious client from exhausting memory with thousands of
/// broadcast channels. 10,000 is generous for legitimate use cases.
pub const MAX_SUBSCRIPTIONS_PER_CONN: usize = 10_000;

/// Maximum length of a PSUBSCRIBE pattern string. Very long patterns can
/// cause pathological backtracking in glob matching. 256 bytes covers any
/// reasonable channel naming scheme.
pub const MAX_PATTERN_LEN: usize = 256;

/// Maximum number of commands parsed from a single read buffer before
/// flushing responses. Prevents a huge pipeline from consuming unbounded
/// memory for pending responses. 10,000 is generous for legitimate
/// pipelining (Redis typically handles hundreds at a time).
pub const MAX_PIPELINE_DEPTH: usize = 10_000;

/// Maximum key length in bytes (512KB). Same limit enforced on the gRPC path.
/// Redis allows up to 512MB keys but virtually no legitimate workload uses
/// keys longer than a few hundred bytes — 512KB is extremely generous.
pub const MAX_KEY_LEN: usize = 512 * 1024;

/// Maximum value length in bytes (512MB). Matches the RESP bulk string limit
/// and the gRPC path. Very large values should use chunked protocols instead.
pub const MAX_VALUE_LEN: usize = 512 * 1024 * 1024;

/// Checks if a raw frame is an AUTH command (before full parsing).
///
/// Peeks at the first bulk element to avoid a full `Command::from_frame`
/// round-trip on unauthenticated connections.
pub fn is_auth_frame(frame: &Frame) -> bool {
    if let Frame::Array(parts) = frame {
        if let Some(Frame::Bulk(name)) = parts.first() {
            return name.eq_ignore_ascii_case(b"AUTH");
        }
    }
    false
}

/// Checks if a raw frame represents a command allowed before authentication.
///
/// Per Redis semantics, only AUTH, PING, ECHO, and QUIT are permitted
/// on unauthenticated connections.
pub fn is_allowed_before_auth(frame: &Frame) -> bool {
    if let Frame::Array(parts) = frame {
        if let Some(Frame::Bulk(name)) = parts.first() {
            return name.eq_ignore_ascii_case(b"AUTH")
                || name.eq_ignore_ascii_case(b"PING")
                || name.eq_ignore_ascii_case(b"ECHO")
                || name.eq_ignore_ascii_case(b"QUIT");
        }
    }
    false
}

/// Attempts to authenticate using an AUTH frame.
///
/// Returns `(response_frame, authenticated)`. The caller should flip
/// their per-connection auth state when `authenticated` is true.
pub fn try_auth(frame: Frame, ctx: &ServerContext) -> (Frame, bool) {
    let cmd = match Command::from_frame(frame) {
        Ok(cmd) => cmd,
        Err(e) => return (Frame::Error(format!("ERR {e}")), false),
    };

    match cmd {
        Command::Auth { username, password } => match &ctx.requirepass {
            None => (
                Frame::Error(
                    "ERR Client sent AUTH, but no password is set. \
                     Did you mean ACL SETUSER with >password?"
                        .into(),
                ),
                false,
            ),
            Some(expected) => {
                // only the "default" username is accepted (no full ACL yet)
                if let Some(ref user) = username {
                    if user != "default" {
                        return (
                            Frame::Error(
                                "WRONGPASS invalid username-password pair \
                                 or user is disabled."
                                    .into(),
                            ),
                            false,
                        );
                    }
                }
                if bool::from(password.as_bytes().ct_eq(expected.as_bytes())) {
                    (Frame::Simple("OK".into()), true)
                } else {
                    (
                        Frame::Error(
                            "WRONGPASS invalid username-password pair \
                             or user is disabled."
                                .into(),
                        ),
                        false,
                    )
                }
            }
        },
        _ => (Frame::Error("ERR expected AUTH command".into()), false),
    }
}

/// Validates key and value sizes for a parsed command.
///
/// Returns an error frame if any key exceeds [`MAX_KEY_LEN`] or any value
/// exceeds [`MAX_VALUE_LEN`]. Returns `None` when the command passes validation.
/// Called on the RESP path to match the limits already enforced by gRPC.
pub fn validate_command_sizes(cmd: &Command) -> Option<Frame> {
    // check primary key length
    if let Some(key) = cmd.primary_key() {
        if key.len() > MAX_KEY_LEN {
            return Some(Frame::Error(format!(
                "ERR key length {} exceeds limit of {MAX_KEY_LEN} bytes",
                key.len()
            )));
        }
    }

    // check multi-key commands (DEL, UNLINK, EXISTS, MGET all have `keys`)
    match cmd {
        Command::Del { keys }
        | Command::Unlink { keys }
        | Command::Exists { keys }
        | Command::MGet { keys } => {
            for k in keys {
                if k.len() > MAX_KEY_LEN {
                    return Some(Frame::Error(format!(
                        "ERR key length {} exceeds limit of {MAX_KEY_LEN} bytes",
                        k.len()
                    )));
                }
            }
        }
        _ => {}
    }

    // check value sizes for commands that carry payloads
    match cmd {
        Command::Set { value, .. } | Command::Append { value, .. } => {
            if value.len() > MAX_VALUE_LEN {
                return Some(Frame::Error(format!(
                    "ERR value length {} exceeds limit of {MAX_VALUE_LEN} bytes",
                    value.len()
                )));
            }
        }
        Command::MSet { pairs } => {
            for (k, v) in pairs {
                if k.len() > MAX_KEY_LEN {
                    return Some(Frame::Error(format!(
                        "ERR key length {} exceeds limit of {MAX_KEY_LEN} bytes",
                        k.len()
                    )));
                }
                if v.len() > MAX_VALUE_LEN {
                    return Some(Frame::Error(format!(
                        "ERR value length {} exceeds limit of {MAX_VALUE_LEN} bytes",
                        v.len()
                    )));
                }
            }
        }
        Command::LPush { values, .. } | Command::RPush { values, .. } => {
            for v in values {
                if v.len() > MAX_VALUE_LEN {
                    return Some(Frame::Error(format!(
                        "ERR value length {} exceeds limit of {MAX_VALUE_LEN} bytes",
                        v.len()
                    )));
                }
            }
        }
        Command::HSet { fields, .. } => {
            for (_, v) in fields {
                if v.len() > MAX_VALUE_LEN {
                    return Some(Frame::Error(format!(
                        "ERR value length {} exceeds limit of {MAX_VALUE_LEN} bytes",
                        v.len()
                    )));
                }
            }
        }
        Command::Restore { data, .. } => {
            if data.len() > MAX_VALUE_LEN {
                return Some(Frame::Error(format!(
                    "ERR value length {} exceeds limit of {MAX_VALUE_LEN} bytes",
                    data.len()
                )));
            }
        }
        Command::Publish { message, .. } => {
            if message.len() > MAX_VALUE_LEN {
                return Some(Frame::Error(format!(
                    "ERR value length {} exceeds limit of {MAX_VALUE_LEN} bytes",
                    message.len()
                )));
            }
        }
        _ => {}
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn normal_set_passes_validation() {
        let cmd = Command::Set {
            key: "foo".into(),
            value: Bytes::from_static(b"bar"),
            expire: None,
            nx: false,
            xx: false,
        };
        assert!(validate_command_sizes(&cmd).is_none());
    }

    #[test]
    fn oversized_key_rejected() {
        let big_key = "x".repeat(MAX_KEY_LEN + 1);
        let cmd = Command::Get { key: big_key };
        let err = validate_command_sizes(&cmd).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("key length")));
    }

    #[test]
    fn key_at_limit_passes() {
        let key = "k".repeat(MAX_KEY_LEN);
        let cmd = Command::Get { key };
        assert!(validate_command_sizes(&cmd).is_none());
    }

    #[test]
    fn oversized_value_in_set_rejected() {
        let big_val = Bytes::from(vec![0u8; MAX_VALUE_LEN + 1]);
        let cmd = Command::Set {
            key: "k".into(),
            value: big_val,
            expire: None,
            nx: false,
            xx: false,
        };
        let err = validate_command_sizes(&cmd).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("value length")));
    }

    #[test]
    fn oversized_key_in_mset_rejected() {
        let big_key = "x".repeat(MAX_KEY_LEN + 1);
        let cmd = Command::MSet {
            pairs: vec![(big_key, Bytes::from_static(b"v"))],
        };
        let err = validate_command_sizes(&cmd).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("key length")));
    }

    #[test]
    fn oversized_value_in_lpush_rejected() {
        let big_val = Bytes::from(vec![0u8; MAX_VALUE_LEN + 1]);
        let cmd = Command::LPush {
            key: "mylist".into(),
            values: vec![Bytes::from_static(b"ok"), big_val],
        };
        let err = validate_command_sizes(&cmd).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("value length")));
    }

    #[test]
    fn oversized_key_in_del_rejected() {
        let big_key = "x".repeat(MAX_KEY_LEN + 1);
        let cmd = Command::Del {
            keys: vec!["ok".into(), big_key],
        };
        let err = validate_command_sizes(&cmd).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("key length")));
    }

    #[test]
    fn commands_without_keys_pass() {
        assert!(validate_command_sizes(&Command::Ping(None)).is_none());
        assert!(validate_command_sizes(&Command::DbSize).is_none());
    }
}
