//! Shared constants and utilities for connection handlers.
//!
//! Both the sharded connection handler (`connection.rs`) and concurrent
//! handler (`concurrent_handler.rs`) use these defaults and helpers.
//! Actual runtime values come from `ctx.limits` (derived from EmberConfig).

use std::time::Duration;

use ember_protocol::types::Frame;
use ember_protocol::Command;
use subtle::ConstantTimeEq;

use crate::server::ServerContext;

// Default values for connection limits. These serve as documentation
// fallbacks — the actual values used at runtime come from `ctx.limits`
// (derived from EmberConfig at startup). Used primarily in tests.

#[cfg(test)]
/// Default max key length (512KB).
pub const DEFAULT_MAX_KEY_LEN: usize = 512 * 1024;
#[cfg(test)]
/// Default max value length (512MB).
pub const DEFAULT_MAX_VALUE_LEN: usize = 512 * 1024 * 1024;

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
/// Returns an error frame if any key exceeds `max_key_len` or any value
/// exceeds `max_value_len`. Returns `None` when the command passes validation.
/// Called on the RESP path to match the limits already enforced by gRPC.
pub fn validate_command_sizes(
    cmd: &Command,
    max_key_len: usize,
    max_value_len: usize,
) -> Option<Frame> {
    // check primary key length
    if let Some(key) = cmd.primary_key() {
        if key.len() > max_key_len {
            return Some(Frame::Error(format!(
                "ERR key length {} exceeds limit of {max_key_len} bytes",
                key.len()
            )));
        }
    }

    // check multi-key commands (DEL, UNLINK, EXISTS, MGET all have `keys`)
    match cmd {
        Command::Del { keys }
        | Command::Unlink { keys }
        | Command::Exists { keys }
        | Command::MGet { keys }
        | Command::BLPop { keys, .. }
        | Command::BRPop { keys, .. } => {
            for k in keys {
                if k.len() > max_key_len {
                    return Some(Frame::Error(format!(
                        "ERR key length {} exceeds limit of {max_key_len} bytes",
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
            if value.len() > max_value_len {
                return Some(Frame::Error(format!(
                    "ERR value length {} exceeds limit of {max_value_len} bytes",
                    value.len()
                )));
            }
        }
        Command::MSet { pairs } => {
            for (k, v) in pairs {
                if k.len() > max_key_len {
                    return Some(Frame::Error(format!(
                        "ERR key length {} exceeds limit of {max_key_len} bytes",
                        k.len()
                    )));
                }
                if v.len() > max_value_len {
                    return Some(Frame::Error(format!(
                        "ERR value length {} exceeds limit of {max_value_len} bytes",
                        v.len()
                    )));
                }
            }
        }
        Command::LPush { values, .. } | Command::RPush { values, .. } => {
            for v in values {
                if v.len() > max_value_len {
                    return Some(Frame::Error(format!(
                        "ERR value length {} exceeds limit of {max_value_len} bytes",
                        v.len()
                    )));
                }
            }
        }
        Command::HSet { fields, .. } => {
            for (_, v) in fields {
                if v.len() > max_value_len {
                    return Some(Frame::Error(format!(
                        "ERR value length {} exceeds limit of {max_value_len} bytes",
                        v.len()
                    )));
                }
            }
        }
        Command::Restore { data, .. } => {
            if data.len() > max_value_len {
                return Some(Frame::Error(format!(
                    "ERR value length {} exceeds limit of {max_value_len} bytes",
                    data.len()
                )));
            }
        }
        Command::Publish { message, .. } => {
            if message.len() > max_value_len {
                return Some(Frame::Error(format!(
                    "ERR value length {} exceeds limit of {max_value_len} bytes",
                    message.len()
                )));
            }
        }
        _ => {}
    }

    None
}

/// Per-connection transaction state for MULTI/EXEC/DISCARD.
///
/// When a client sends MULTI, subsequent commands are queued as raw frames
/// rather than dispatched. EXEC replays them in order and collects results
/// into a single Array response. DISCARD drops the queue.
pub enum TransactionState {
    /// Normal mode — commands are dispatched immediately.
    None,
    /// Queuing mode — frames accumulate until EXEC or DISCARD.
    Queuing {
        queue: Vec<Frame>,
        /// Set when a command in the queue had a parse error. EXEC returns
        /// EXECABORT instead of executing.
        error: bool,
    },
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
        assert!(validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN).is_none());
    }

    #[test]
    fn oversized_key_rejected() {
        let big_key = "x".repeat(DEFAULT_MAX_KEY_LEN + 1);
        let cmd = Command::Get { key: big_key };
        let err = validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("key length")));
    }

    #[test]
    fn key_at_limit_passes() {
        let key = "k".repeat(DEFAULT_MAX_KEY_LEN);
        let cmd = Command::Get { key };
        assert!(validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN).is_none());
    }

    #[test]
    fn oversized_value_in_set_rejected() {
        let big_val = Bytes::from(vec![0u8; DEFAULT_MAX_VALUE_LEN + 1]);
        let cmd = Command::Set {
            key: "k".into(),
            value: big_val,
            expire: None,
            nx: false,
            xx: false,
        };
        let err = validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("value length")));
    }

    #[test]
    fn oversized_key_in_mset_rejected() {
        let big_key = "x".repeat(DEFAULT_MAX_KEY_LEN + 1);
        let cmd = Command::MSet {
            pairs: vec![(big_key, Bytes::from_static(b"v"))],
        };
        let err = validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("key length")));
    }

    #[test]
    fn oversized_value_in_lpush_rejected() {
        let big_val = Bytes::from(vec![0u8; DEFAULT_MAX_VALUE_LEN + 1]);
        let cmd = Command::LPush {
            key: "mylist".into(),
            values: vec![Bytes::from_static(b"ok"), big_val],
        };
        let err = validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("value length")));
    }

    #[test]
    fn oversized_key_in_del_rejected() {
        let big_key = "x".repeat(DEFAULT_MAX_KEY_LEN + 1);
        let cmd = Command::Del {
            keys: vec!["ok".into(), big_key],
        };
        let err = validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("key length")));
    }

    #[test]
    fn commands_without_keys_pass() {
        assert!(validate_command_sizes(&Command::Ping(None), DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN).is_none());
        assert!(validate_command_sizes(&Command::DbSize, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN).is_none());
    }
}
