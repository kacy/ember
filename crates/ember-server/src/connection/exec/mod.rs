//! Shared execution context and helpers for command sub-modules.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ember_core::{Engine, ShardRequest, ShardResponse, Value};
use ember_protocol::{Frame, SetExpire};

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

/// Converts a [`SetExpire`] option to a [`Duration`] relative to now.
///
/// EX/PX are relative; EXAT/PXAT are unix timestamps converted to a
/// duration by subtracting the current wall time. A past timestamp
/// results in a zero duration (the key expires immediately).
pub(in crate::connection) fn set_expire_to_duration(expire: SetExpire) -> Duration {
    use std::time::{SystemTime, UNIX_EPOCH};
    match expire {
        SetExpire::Ex(secs) => Duration::from_secs(secs),
        SetExpire::Px(ms) => Duration::from_millis(ms),
        SetExpire::ExAt(ts) => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| {
                    tracing::warn!(
                        "system clock is before UNIX epoch; EXAT TTL calculations may be incorrect"
                    );
                    Duration::ZERO
                })
                .as_secs();
            Duration::from_secs(ts.saturating_sub(now))
        }
        SetExpire::PxAt(ts_ms) => {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| {
                    tracing::warn!(
                        "system clock is before UNIX epoch; PXAT TTL calculations may be incorrect"
                    );
                    Duration::ZERO
                })
                .as_millis() as u64;
            Duration::from_millis(ts_ms.saturating_sub(now_ms))
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

/// Resolves a collection scan (SSCAN/HSCAN/ZSCAN) shard response into a RESP frame.
pub(in crate::connection) fn resolve_collection_scan(
    result: Result<ShardResponse, ember_core::ShardError>,
) -> Frame {
    match result {
        Ok(ShardResponse::CollectionScan { cursor, items }) => {
            let cursor_frame = Frame::Bulk(Bytes::from(cursor.to_string()));
            let item_frames = items.into_iter().map(Frame::Bulk).collect();
            Frame::Array(vec![cursor_frame, Frame::Array(item_frames)])
        }
        Ok(ShardResponse::WrongType) => wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
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

/// Maps `Ok` → `Simple("OK")`.
pub(in crate::connection) fn resp_ok(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::Ok => Frame::Simple("OK".into()),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    }
}

/// Maps `Ok` → `Simple("OK")`, `Value(None)` → `Null` (for SET with NX/XX).
pub(in crate::connection) fn resp_ok_or_null(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::Ok => Frame::Simple("OK".into()),
        ShardResponse::Value(None) => Frame::Null,
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    }
}

/// Maps `Len(n)` → `Frame::Integer(n as i64)`.
pub(in crate::connection) fn resp_len(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::Len(n) => Frame::Integer(n as i64),
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

/// Maps `Array(items)` → `Frame::Array` of `Bulk` frames.
pub(in crate::connection) fn resp_bulk_array(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::Array(items) => Frame::Array(items.into_iter().map(Frame::Bulk).collect()),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    }
}

/// Maps `StringArray(items)` → `Frame::Array` of `Bulk` frames.
pub(in crate::connection) fn resp_string_array(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::StringArray(members) => Frame::Array(
            members
                .into_iter()
                .map(|m| Frame::Bulk(Bytes::from(m)))
                .collect(),
        ),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
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

/// Maps `Rank(Some(r))` → `Integer`, `Rank(None)` → `Null`.
pub(in crate::connection) fn resp_rank(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::Rank(Some(r)) => Frame::Integer(r as i64),
        ShardResponse::Rank(None) => Frame::Null,
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    }
}

/// Maps `Score(Some(s))` → `Bulk`, `Score(None)` → `Null`.
pub(in crate::connection) fn resp_score(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::Score(Some(s)) => Frame::Bulk(Bytes::from(format!("{s}"))),
        ShardResponse::Score(None) => Frame::Null,
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    }
}

/// Maps `ZPopResult` → interleaved `[member, score, ...]` array.
pub(in crate::connection) fn resp_zpop(resp: ShardResponse) -> Frame {
    match resp {
        ShardResponse::ZPopResult(items) => {
            let mut frames = Vec::with_capacity(items.len() * 2);
            for (member, score) in items {
                frames.push(Frame::Bulk(Bytes::from(member)));
                frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
            }
            Frame::Array(frames)
        }
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    }
}
