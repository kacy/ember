//! AUTH and ACL command handlers.

use bytes::Bytes;
use ember_protocol::{Command, Frame};

use super::ExecCtx;

pub(in crate::connection) fn auth(
    username: Option<String>,
    password: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    let username = username.unwrap_or_else(|| "default".into());
    match crate::acl::authenticate(&cx.ctx.acl, &username, &password) {
        Ok(_) => Frame::Simple("OK".into()),
        Err(msg) => Frame::Error(msg),
    }
}

/// WHOAMI is handled at the connection level (needs current_username).
/// If it reaches here, return a generic response.
pub(in crate::connection) fn acl_whoami() -> Frame {
    Frame::Bulk(Bytes::from_static(b"default"))
}

pub(in crate::connection) fn acl_admin(cmd: Command, cx: &ExecCtx<'_>) -> Frame {
    crate::acl::run_admin_command(&cx.ctx.acl, cmd)
}
