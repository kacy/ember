//! Shared execution context and helpers for command sub-modules.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ember_core::{Engine, ShardRequest, ShardResponse};
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
