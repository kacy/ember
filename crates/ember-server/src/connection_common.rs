//! Shared constants and utilities for connection handlers.
//!
//! Both the sharded connection handler (`connection.rs`) and concurrent
//! handler (`concurrent_handler.rs`) use these defaults and helpers.
//! Actual runtime values come from `ctx.limits` (derived from EmberConfig).

use std::sync::Arc;

use ember_protocol::types::Frame;
use ember_protocol::Command;
use subtle::ConstantTimeEq;

use crate::acl::AclUser;
use crate::metrics::on_auth_failure;
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
#[cfg(test)]
/// Default max command memory (128MB total payload per command).
pub const DEFAULT_MAX_COMMAND_MEMORY: usize = 128 * 1024 * 1024;

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

/// Result of an AUTH attempt. Carries the user snapshot and username
/// on success so the connection can cache them for permission checks.
pub struct AuthResult {
    pub response: Frame,
    pub user: Option<Arc<AclUser>>,
    pub username: String,
}

impl AuthResult {
    fn fail(msg: &str) -> Self {
        Self {
            response: Frame::Error(msg.into()),
            user: None,
            username: String::new(),
        }
    }

    pub fn success(&self) -> bool {
        self.user.is_some()
    }
}

/// Attempts to authenticate using an AUTH frame.
///
/// When ACL is configured (`ctx.acl` is Some), looks up the user in
/// the ACL state and verifies the password against stored hashes.
/// When ACL is not configured, falls back to legacy `requirepass` comparison.
///
/// Returns an `AuthResult` containing the response frame, and on success,
/// an `Arc<AclUser>` snapshot and the username string.
pub fn try_auth(frame: Frame, ctx: &ServerContext) -> AuthResult {
    let cmd = match Command::from_frame(frame) {
        Ok(cmd) => cmd,
        Err(e) => return AuthResult::fail(&format!("ERR {e}")),
    };

    let (username, password) = match cmd {
        Command::Auth { username, password } => {
            (username.unwrap_or_else(|| "default".into()), password)
        }
        _ => return AuthResult::fail("ERR expected AUTH command"),
    };

    // ACL-aware path
    if let Some(ref acl) = ctx.acl {
        let state = match acl.read() {
            Ok(s) => s,
            Err(_) => return AuthResult::fail("ERR ACL state lock poisoned"),
        };

        let user = match state.get_user(&username) {
            Some(u) => u,
            None => {
                on_auth_failure("wrongpass");
                return AuthResult::fail(
                    "WRONGPASS invalid username-password pair or user is disabled.",
                );
            }
        };

        if !user.enabled {
            on_auth_failure("wrongpass");
            return AuthResult::fail(
                "WRONGPASS invalid username-password pair or user is disabled.",
            );
        }

        if !user.verify_password(&password) {
            on_auth_failure("wrongpass");
            return AuthResult::fail(
                "WRONGPASS invalid username-password pair or user is disabled.",
            );
        }

        return AuthResult {
            response: Frame::Simple("OK".into()),
            user: Some(Arc::new(user.clone())),
            username,
        };
    }

    // Legacy requirepass path
    match &ctx.requirepass {
        None => AuthResult::fail(
            "ERR Client sent AUTH, but no password is set. \
             Did you mean ACL SETUSER with >password?",
        ),
        Some(expected) => {
            if bool::from(password.as_bytes().ct_eq(expected.as_bytes())) {
                AuthResult {
                    response: Frame::Simple("OK".into()),
                    user: Some(Arc::new(AclUser::unrestricted())),
                    username,
                }
            } else {
                on_auth_failure("wrongpass");
                AuthResult::fail("WRONGPASS invalid username-password pair or user is disabled.")
            }
        }
    }
}

/// Returns the initial ACL user for a new connection.
///
/// - When no `requirepass` and no ACL: returns `Some(unrestricted)` (auto-authenticated)
/// - When `requirepass` set or ACL with password-required default: returns `None` (must AUTH)
/// - When ACL configured but default user is nopass+enabled: returns `Some(default_user)`
pub fn initial_acl_user(ctx: &ServerContext) -> (Option<Arc<AclUser>>, String) {
    if let Some(ref acl) = ctx.acl {
        if let Ok(state) = acl.read() {
            if let Some(user) = state.get_user("default") {
                if user.enabled && user.nopass {
                    return (Some(Arc::new(user.clone())), "default".into());
                }
            }
        }
        // ACL configured but default user requires a password
        return (None, String::new());
    }

    // Legacy mode
    if ctx.requirepass.is_none() {
        (Some(Arc::new(AclUser::unrestricted())), "default".into())
    } else {
        (None, String::new())
    }
}

/// Validates key and value sizes for a parsed command.
///
/// Returns an error frame if any key exceeds `max_key_len`, any value exceeds
/// `max_value_len`, or the combined payload of a bulk command exceeds
/// `max_command_memory`. Returns `None` when the command passes validation.
/// Called on the RESP path to match the limits already enforced by gRPC.
pub fn validate_command_sizes(
    cmd: &Command,
    max_key_len: usize,
    max_value_len: usize,
    max_command_memory: usize,
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
            let mut total = 0usize;
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
                total = total.saturating_add(k.len()).saturating_add(v.len());
            }
            if total > max_command_memory {
                return Some(Frame::Error(format!(
                    "ERR command payload {total} bytes exceeds limit of {max_command_memory} bytes"
                )));
            }
        }
        Command::LPush { values, .. } | Command::RPush { values, .. } => {
            let mut total = 0usize;
            for v in values {
                if v.len() > max_value_len {
                    return Some(Frame::Error(format!(
                        "ERR value length {} exceeds limit of {max_value_len} bytes",
                        v.len()
                    )));
                }
                total = total.saturating_add(v.len());
            }
            if total > max_command_memory {
                return Some(Frame::Error(format!(
                    "ERR command payload {total} bytes exceeds limit of {max_command_memory} bytes"
                )));
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

/// Checks if a raw frame is a MONITOR command.
pub fn is_monitor_frame(frame: &Frame) -> bool {
    if let Frame::Array(parts) = frame {
        if let Some(Frame::Bulk(name)) = parts.first() {
            return name.eq_ignore_ascii_case(b"MONITOR");
        }
    }
    false
}

/// Event broadcast to MONITOR subscribers.
#[derive(Clone, Debug)]
pub struct MonitorEvent {
    /// Unix timestamp with microsecond precision.
    pub timestamp: f64,
    /// Client address (e.g. "127.0.0.1:52431").
    pub client_addr: String,
    /// Raw command arguments as strings (e.g. ["SET", "key", "value"]).
    pub args: Vec<String>,
}

/// Formats a MONITOR event as a Redis-compatible status line.
///
/// Output matches Redis format:
///   +1234567890.123456 [127.0.0.1:52431] "SET" "key" "value"
pub fn format_monitor_event(event: &MonitorEvent) -> String {
    use std::fmt::Write;
    let mut out = format!("{:.6} [{}]", event.timestamp, event.client_addr);
    for arg in &event.args {
        write!(
            out,
            " \"{}\"",
            arg.replace('\\', "\\\\").replace('"', "\\\"")
        )
        .expect("write to string never fails");
    }
    out
}

/// Extracts command arguments from a raw frame as strings for MONITOR.
///
/// Returns an empty vec for non-array frames. Binary data is lossy-converted
/// to UTF-8 (matching Redis behavior for MONITOR output).
pub fn frame_to_monitor_args(frame: &Frame) -> Vec<String> {
    let Frame::Array(parts) = frame else {
        return Vec::new();
    };
    parts
        .iter()
        .map(|p| match p {
            Frame::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
            Frame::Simple(s) => s.clone(),
            Frame::Integer(n) => n.to_string(),
            _ => String::new(),
        })
        .collect()
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

/// Formats a byte count as a human-readable string (e.g. "1.23M").
pub fn human_bytes(bytes: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let b = bytes as f64;
    if b >= GB {
        format!("{:.2}G", b / GB)
    } else if b >= MB {
        format!("{:.2}M", b / MB)
    } else if b >= KB {
        format!("{:.2}K", b / KB)
    } else {
        format!("{bytes}B")
    }
}

/// Returns the resident set size (RSS) of the current process in bytes.
///
/// Platform-specific:
/// - **macOS**: uses `proc_pidinfo` with `PROC_PIDTASKINFO`
/// - **Linux**: reads `/proc/self/statm` (field 1 × page size)
/// - **other**: returns `None`
pub fn get_rss_bytes() -> Option<usize> {
    #[cfg(target_os = "macos")]
    {
        get_rss_macos()
    }
    #[cfg(target_os = "linux")]
    {
        get_rss_linux()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        None
    }
}

#[cfg(target_os = "macos")]
fn get_rss_macos() -> Option<usize> {
    use std::mem;
    // SAFETY: proc_pidinfo reads kernel info for our own PID.
    // We pass a correctly-sized buffer and check the return value.
    unsafe {
        let pid = libc::getpid();
        let mut info: libc::proc_taskinfo = mem::zeroed();
        let size = mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
        let ret = libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTASKINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size,
        );
        if ret <= 0 {
            return None;
        }
        Some(info.pti_resident_size as usize)
    }
}

#[cfg(target_os = "linux")]
fn get_rss_linux() -> Option<usize> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let rss_pages: usize = statm.split_whitespace().nth(1)?.parse().ok()?;
    // SAFETY: sysconf reads a kernel constant — no side effects.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    Some(rss_pages * page_size as usize)
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
        assert!(validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN, DEFAULT_MAX_COMMAND_MEMORY).is_none());
    }

    #[test]
    fn oversized_key_rejected() {
        let big_key = "x".repeat(DEFAULT_MAX_KEY_LEN + 1);
        let cmd = Command::Get { key: big_key };
        let err = validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN, DEFAULT_MAX_COMMAND_MEMORY).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("key length")));
    }

    #[test]
    fn key_at_limit_passes() {
        let key = "k".repeat(DEFAULT_MAX_KEY_LEN);
        let cmd = Command::Get { key };
        assert!(validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN, DEFAULT_MAX_COMMAND_MEMORY).is_none());
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
        let err = validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN, DEFAULT_MAX_COMMAND_MEMORY).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("value length")));
    }

    #[test]
    fn oversized_key_in_mset_rejected() {
        let big_key = "x".repeat(DEFAULT_MAX_KEY_LEN + 1);
        let cmd = Command::MSet {
            pairs: vec![(big_key, Bytes::from_static(b"v"))],
        };
        let err = validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN, DEFAULT_MAX_COMMAND_MEMORY).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("key length")));
    }

    #[test]
    fn oversized_value_in_lpush_rejected() {
        let big_val = Bytes::from(vec![0u8; DEFAULT_MAX_VALUE_LEN + 1]);
        let cmd = Command::LPush {
            key: "mylist".into(),
            values: vec![Bytes::from_static(b"ok"), big_val],
        };
        let err = validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN, DEFAULT_MAX_COMMAND_MEMORY).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("value length")));
    }

    #[test]
    fn oversized_key_in_del_rejected() {
        let big_key = "x".repeat(DEFAULT_MAX_KEY_LEN + 1);
        let cmd = Command::Del {
            keys: vec!["ok".into(), big_key],
        };
        let err = validate_command_sizes(&cmd, DEFAULT_MAX_KEY_LEN, DEFAULT_MAX_VALUE_LEN, DEFAULT_MAX_COMMAND_MEMORY).unwrap();
        assert!(matches!(err, Frame::Error(ref msg) if msg.contains("key length")));
    }

    #[test]
    fn commands_without_keys_pass() {
        assert!(validate_command_sizes(
            &Command::Ping(None),
            DEFAULT_MAX_KEY_LEN,
            DEFAULT_MAX_VALUE_LEN,
            DEFAULT_MAX_COMMAND_MEMORY
        )
        .is_none());
        assert!(validate_command_sizes(
            &Command::DbSize,
            DEFAULT_MAX_KEY_LEN,
            DEFAULT_MAX_VALUE_LEN,
            DEFAULT_MAX_COMMAND_MEMORY
        )
        .is_none());
    }

    #[test]
    fn is_monitor_frame_detects_monitor() {
        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"MONITOR"))]);
        assert!(is_monitor_frame(&frame));

        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"monitor"))]);
        assert!(is_monitor_frame(&frame));

        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"GET"))]);
        assert!(!is_monitor_frame(&frame));
    }

    #[test]
    fn format_monitor_event_matches_redis_format() {
        let event = MonitorEvent {
            timestamp: 1234567890.123456,
            client_addr: "127.0.0.1:52431".into(),
            args: vec!["SET".into(), "key".into(), "value".into()],
        };
        let output = format_monitor_event(&event);
        assert_eq!(
            output,
            "1234567890.123456 [127.0.0.1:52431] \"SET\" \"key\" \"value\""
        );
    }

    #[test]
    fn format_monitor_event_escapes_quotes() {
        let event = MonitorEvent {
            timestamp: 1.0,
            client_addr: "127.0.0.1:1".into(),
            args: vec!["SET".into(), "key".into(), "val\"ue".into()],
        };
        let output = format_monitor_event(&event);
        assert!(output.contains("\"val\\\"ue\""));
    }

    #[test]
    fn frame_to_monitor_args_extracts_bulk_strings() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"SET")),
            Frame::Bulk(Bytes::from_static(b"mykey")),
            Frame::Bulk(Bytes::from_static(b"myvalue")),
        ]);
        let args = frame_to_monitor_args(&frame);
        assert_eq!(args, vec!["SET", "mykey", "myvalue"]);
    }

    #[test]
    fn frame_to_monitor_args_non_array_is_empty() {
        let frame = Frame::Simple("OK".into());
        assert!(frame_to_monitor_args(&frame).is_empty());
    }
}
