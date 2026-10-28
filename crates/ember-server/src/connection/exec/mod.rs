//! Shared execution context and helpers for command sub-modules.

use std::sync::Arc;

use bytes::Bytes;
use ember_core::{Engine, ShardRequest, ShardResponse, Value};
use ember_protocol::Frame;

use crate::pubsub::PubSubManager;
use crate::server::ServerContext;
use crate::slowlog::SlowLog;

pub(super) mod acl;
pub(super) mod cluster;
pub(super) mod hashes;
pub(super) mod keyspace;
pub(super) mod lists;
pub(super) mod protobuf;
pub(super) mod pubsub;
pub(super) mod server;
pub(super) mod sets;
pub(super) mod sorted_sets;
pub(super) mod strings;
pub(super) mod vector;

/// Shared execution context passed to every command handler.
///
/// Bundles the references that most commands need — engine for shard
/// routing, server context for config/cluster state, pub/sub manager
/// for notifications, slow log, and the current client ID.
pub(in crate::connection) struct ExecCtx<'a> {
    pub engine: &'a Engine,
    pub ctx: &'a Arc<ServerContext>,
    pub pubsub: &'a Arc<PubSubManager>,
    pub slow_log: &'a Arc<SlowLog>,
    /// Client identifier, available for commands that need per-connection context.
    #[allow(dead_code)]
    pub client_id: u64,
}

impl<'a> ExecCtx<'a> {
    /// Emits keyspace/keyevent notifications for a successful write command.
    ///
    /// No-op when `notify-keyspace-events` is `""` / zero (the common case).
    /// A single atomic load guards the allocation path.
    #[inline]
    pub fn notify_write(&self, event_flag: u32, event: &str, key: &str) {
        let flags = self
            .ctx
            .keyspace_event_flags
            .load(std::sync::atomic::Ordering::Relaxed);
        if flags != 0 {
            crate::keyspace_notifications::notify_keyspace_event(
                flags,
                event_flag,
                event,
                key,
                self.pubsub,
            );
        }
    }
}

/// Fans out a boolean-result command across shards for multiple keys
/// and returns the count of `true` results as an integer frame.
pub(in crate::connection) async fn multi_key_bool<F>(
    engine: &Engine,
    keys: &[String],
    make_req: F,
) -> Frame
where
    F: Fn(String) -> ShardRequest,
{
    match engine.route_multi(keys, make_req).await {
        Ok(responses) => {
            let count = responses
                .iter()
                .filter(|r| matches!(r, ShardResponse::Bool(true)))
                .count();
            Frame::Integer(count as i64)
        }
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

/// Returns the standard WRONGTYPE error frame.
#[inline]
pub(in crate::connection) fn wrongtype_error() -> Frame {
    Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}

/// Returns the standard OOM error frame.
#[inline]
pub(in crate::connection) fn oom_error() -> Frame {
    Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
}

// ---------------------------------------------------------------------------
// route_to_shard + response mapper helpers
//
// These eliminate the repetitive send → match → error-handling boilerplate
// from every single-key command handler. The caller only provides the success
// mapping via `map_ok`; common error variants are handled uniformly.
// ---------------------------------------------------------------------------

/// Routes a single-key command to its owning shard and maps the response.
///
/// Handles WrongType, OutOfMemory, Err, and channel errors uniformly.
/// The caller only maps the success case via `map_ok`. If `map_ok`
/// doesn't recognise the response variant, it should return a
/// `Frame::Error("ERR unexpected ...")` itself.
pub(in crate::connection) async fn route_to_shard<F>(
    cx: &ExecCtx<'_>,
    shard_idx: usize,
    req: ShardRequest,
    map_ok: F,
) -> Frame
where
    F: FnOnce(ShardResponse) -> Frame,
{
    match cx.engine.send_to_shard(shard_idx, req).await {
        Ok(ShardResponse::WrongType) => wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
        Ok(resp) => map_ok(resp),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

/// Maps `Value(Some(String(data)))` → `Bulk`, `Value(None)` → `Null`.
pub(in crate::connection) fn resp_string_value(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::Value(Some(Value::String(data))) => Frame::Bulk(data),
        ShardResponse::Value(None) => Frame::Null,
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    }
}

/// Maps `Integer(n)` → `Frame::Integer(n)`.
pub(in crate::connection) fn resp_integer(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::Integer(n) => Frame::Integer(n),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    }
}

/// Maps `Bool(b)` → `Frame::Integer(0|1)`.
pub(in crate::connection) fn resp_bool_int(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::Bool(b) => Frame::Integer(i64::from(b)),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    }
}

/// Maps `BulkString(val)` → `Frame::Bulk(Bytes::from(val))`.
pub(in crate::connection) fn resp_bulk_string(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::BulkString(val) => Frame::Bulk(Bytes::from(val)),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    }
}

/// A set operation over several keys, for SUNION/SINTER/SDIFF and the
/// sorted-set equivalents.
#[derive(Clone, Copy)]
pub(in crate::connection) enum SetOp {
    Union,
    Inter,
    Diff,
}

impl SetOp {
    /// What the shard holding the group of keys at position `group` runs.
    /// A difference subtracts everything outside the first key's group, so
    /// the other groups only need the union of their keys.
    fn for_group(self, group: usize) -> SetOp {
        match self {
            SetOp::Diff if group > 0 => SetOp::Union,
            op => op,
        }
    }
}

/// Splits `keys` by shard. Groups follow the order in which their shards
/// first appear, so the first group holds `keys[0]`.
fn group_by_shard(engine: &Engine, keys: Vec<String>) -> Vec<(usize, Vec<String>)> {
    let mut groups: Vec<(usize, Vec<String>)> = Vec::new();
    for key in keys {
        let idx = engine.shard_for_key(&key);
        match groups.iter_mut().find(|(i, _)| *i == idx) {
            Some((_, group)) => group.push(key),
            None => groups.push((idx, vec![key])),
        }
    }
    groups
}

/// Runs `op` over `keys` split by shard: each shard applies the operation
/// to its own keys through `make_req`, and the replies come back in group
/// order. With every key on one shard this is a single request.
pub(in crate::connection) async fn per_shard(
    cx: &ExecCtx<'_>,
    op: SetOp,
    keys: Vec<String>,
    make_req: impl Fn(SetOp, Vec<String>) -> ShardRequest,
) -> Result<Vec<ShardResponse>, Frame> {
    let mut pending = Vec::new();
    for (group, (idx, keys)) in group_by_shard(cx.engine, keys).into_iter().enumerate() {
        let req = make_req(op.for_group(group), keys);
        let rx = cx
            .engine
            .dispatch_to_shard(idx, req)
            .await
            .map_err(|e| Frame::Error(format!("ERR {e}")))?;
        pending.push(rx);
    }
    let mut replies = Vec::with_capacity(pending.len());
    for rx in pending {
        match rx.await {
            Ok(ShardResponse::WrongType) => return Err(wrongtype_error()),
            Ok(resp) => replies.push(resp),
            Err(_) => return Err(Frame::Error("ERR shard unavailable".into())),
        }
    }
    Ok(replies)
}

/// Replaces `dest` with the result of a multi-key command: deletes it, then
/// sends `write` to create the new value. `write` is `None` for an empty
/// result, which leaves `dest` deleted. Replies with `count`.
pub(in crate::connection) async fn store_result(
    cx: &ExecCtx<'_>,
    dest: &str,
    write: Option<ShardRequest>,
    count: usize,
) -> Frame {
    let idx = cx.engine.shard_for_key(dest);
    let del = ShardRequest::Del {
        key: dest.to_owned(),
    };
    if let Err(e) = cx.engine.send_to_shard(idx, del).await {
        return Frame::Error(format!("ERR {e}"));
    }
    match write {
        Some(req) => route_to_shard(cx, idx, req, |_| Frame::Integer(count as i64)).await,
        None => Frame::Integer(count as i64),
    }
}

/// Checks that `key` on shard `idx` is missing or holds `kind` (a TYPE
/// name such as "list"), so a cross-shard move can tell before it takes
/// anything from the source whether the destination write can succeed.
pub(in crate::connection) async fn check_type(
    cx: &ExecCtx<'_>,
    idx: usize,
    key: &str,
    kind: &str,
) -> Result<(), Frame> {
    let req = ShardRequest::Type {
        key: key.to_owned(),
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::TypeName(name)) if name == "none" || name == kind => Ok(()),
        Ok(ShardResponse::TypeName(_)) => Err(wrongtype_error()),
        Ok(other) => Err(Frame::Error(format!(
            "ERR unexpected shard response: {other:?}"
        ))),
        Err(e) => Err(Frame::Error(format!("ERR {e}"))),
    }
}

/// Converts a scored array into RESP frames, optionally interleaving scores.
pub(in crate::connection) fn scored_to_frame(
    items: Vec<(String, f64)>,
    with_scores: bool,
) -> Frame {
    let mut frames = Vec::with_capacity(items.len() * if with_scores { 2 } else { 1 });
    for (member, score) in items {
        frames.push(Frame::Bulk(Bytes::from(member)));
        if with_scores {
            frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
        }
    }
    Frame::Array(frames)
}
