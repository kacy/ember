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
