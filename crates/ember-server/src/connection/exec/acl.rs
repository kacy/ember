//! ACL and AUTH command handlers.

use bytes::Bytes;
use ember_protocol::Frame;
use subtle::ConstantTimeEq;

use super::ExecCtx;

pub(in crate::connection) fn auth(
    username: Option<String>,
    password: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    let uname = username.unwrap_or_else(|| "default".into());
    if let Some(ref acl_state) = cx.ctx.acl {
        match acl_state.read() {
            Ok(state) => match state.get_user(&uname) {
                Some(user) if user.enabled && user.verify_password(&password) => {
                    Frame::Simple("OK".into())
                }
                _ => Frame::Error(
                    "WRONGPASS invalid username-password pair or user is disabled.".into(),
                ),
            },
            Err(_) => Frame::Error("ERR ACL state lock poisoned".into()),
        }
    } else {
        match &cx.ctx.requirepass {
            None => Frame::Error(
                "ERR Client sent AUTH, but no password is set. \
                 Did you mean ACL SETUSER with >password?"
                    .into(),
            ),
            Some(expected) => {
                if uname != "default" {
                    Frame::Error(
                        "WRONGPASS invalid username-password pair or user is disabled.".into(),
                    )
                } else if bool::from(password.as_bytes().ct_eq(expected.as_bytes())) {
                    Frame::Simple("OK".into())
                } else {
                    Frame::Error(
                        "WRONGPASS invalid username-password pair or user is disabled.".into(),
                    )
                }
            }
        }
    }
}

/// WHOAMI is handled at the connection level (needs current_username).
/// If it reaches here, return a generic response.
pub(in crate::connection) fn acl_whoami() -> Frame {
    Frame::Bulk(Bytes::from_static(b"default"))
}

pub(in crate::connection) fn acl_list(cx: &ExecCtx<'_>) -> Frame {
    if let Some(ref acl) = cx.ctx.acl {
        match acl.read() {
            Ok(state) => {
                let lines = state.list();
                Frame::Array(
                    lines
                        .into_iter()
                        .map(|l| Frame::Bulk(Bytes::from(l)))
                        .collect(),
                )
            }
            Err(_) => Frame::Error("ERR ACL state lock poisoned".into()),
        }
    } else {
        // legacy mode — synthesize a default user entry
        Frame::Array(vec![Frame::Bulk(Bytes::from(
            "user default on nopass +@all ~*",
        ))])
    }
}

pub(in crate::connection) fn acl_users(cx: &ExecCtx<'_>) -> Frame {
    if let Some(ref acl) = cx.ctx.acl {
        match acl.read() {
            Ok(state) => {
                let names = state.usernames();
                Frame::Array(
                    names
                        .into_iter()
                        .map(|n| Frame::Bulk(Bytes::from(n)))
                        .collect(),
                )
            }
            Err(_) => Frame::Error("ERR ACL state lock poisoned".into()),
        }
    } else {
        Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"default"))])
    }
}

pub(in crate::connection) fn acl_getuser(username: String, cx: &ExecCtx<'_>) -> Frame {
    if let Some(ref acl) = cx.ctx.acl {
        match acl.read() {
            Ok(state) => match state.get_user_detail(&username) {
                Some(detail) => detail,
                None => Frame::Error(format!("ERR no such user '{username}'")),
            },
            Err(_) => Frame::Error("ERR ACL state lock poisoned".into()),
        }
    } else if username == "default" {
        // legacy mode: synthesize default user detail
        crate::acl::AclState::new()
            .get_user_detail("default")
            .unwrap_or(Frame::Null)
    } else {
        Frame::Error(format!("ERR no such user '{username}'"))
    }
}

pub(in crate::connection) fn acl_deluser(usernames: Vec<String>, cx: &ExecCtx<'_>) -> Frame {
    if let Some(ref acl) = cx.ctx.acl {
        match acl.write() {
            Ok(mut state) => match state.del_users(&usernames) {
                Ok(count) => Frame::Integer(count as i64),
                Err(msg) => Frame::Error(msg),
            },
            Err(_) => Frame::Error("ERR ACL state lock poisoned".into()),
        }
    } else {
        Frame::Error("ERR ACL is not enabled. Configure ACL users to use this command.".into())
    }
}

pub(in crate::connection) fn acl_setuser(
    username: String,
    rules: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    if let Some(ref acl) = cx.ctx.acl {
        match acl.write() {
            Ok(mut state) => match state.set_user(&username, &rules) {
                Ok(()) => Frame::Simple("OK".into()),
                Err(msg) => Frame::Error(msg),
            },
            Err(_) => Frame::Error("ERR ACL state lock poisoned".into()),
        }
    } else {
        Frame::Error("ERR ACL is not enabled. Configure ACL users to use this command.".into())
    }
}

pub(in crate::connection) fn acl_cat(category: Option<String>) -> Frame {
    crate::acl::handle_acl_cat(category.as_deref())
}
