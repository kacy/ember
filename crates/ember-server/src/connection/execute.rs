//! Command execution — routes parsed commands to engine shards.

use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::connection_common::{get_rss_bytes, human_bytes};
use crate::pubsub::PubSubManager;
use crate::server::{format_client_list, ServerContext};
use crate::slowlog::SlowLog;
use bytes::{Bytes, BytesMut};
use ember_core::{Engine, KeyspaceStats, ShardRequest, ShardResponse, TtlResult, Value};
use ember_protocol::{command::BitOpKind, parse_frame, Command, Frame, SetExpire};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Converts a [`SetExpire`] option to a [`Duration`] relative to now.
///
/// EX/PX are relative; EXAT/PXAT are unix timestamps that are converted to
/// a duration by subtracting the current wall time. A past timestamp results
/// in a zero duration (the key expires immediately).
fn set_expire_to_duration(expire: SetExpire) -> Duration {
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

/// Emits keyspace/keyevent notifications for a successful write command.
///
/// No-op when `notify-keyspace-events` is `""` / zero (the common case).
/// The `flags == 0` check is a single atomic load — true zero overhead
/// when notifications are disabled.
#[inline]
fn notify_write(
    ctx: &Arc<ServerContext>,
    pubsub: &Arc<PubSubManager>,
    event_flag: u32,
    event: &str,
    key: &str,
) {
    let flags = ctx
        .keyspace_event_flags
        .load(std::sync::atomic::Ordering::Relaxed);
    if flags != 0 {
        crate::keyspace_notifications::notify_keyspace_event(flags, event_flag, event, key, pubsub);
    }
}

/// Executes a parsed command and returns the response frame.
///
/// Ping and Echo are handled inline (no shard routing needed).
/// Single-key commands route to the owning shard. Multi-key commands
/// (DEL, EXISTS) fan out across shards and aggregate results.
#[allow(clippy::too_many_arguments)]
pub(super) async fn execute(
    cmd: Command,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
    asking: bool,
    client_id: u64,
) -> Frame {
    // Write gating: reject mutations when the node is a replica or when
    // writes are temporarily paused (e.g. during failover coordination).
    if let Some(ref cluster) = ctx.cluster {
        if cluster.is_writes_paused() && cmd.is_write() {
            return Frame::Error(
                "READONLY Failover in progress; writes are temporarily paused.".into(),
            );
        }
        if cluster.is_replica().await && cmd.is_write() {
            if let Some(key) = cmd.primary_key() {
                use ember_cluster::key_slot;
                let slot = key_slot(key.as_bytes());
                if let Some(addr) = cluster.primary_addr_for_slot(slot).await {
                    return Frame::Error(format!("MOVED {slot} {addr}"));
                }
            }
            return Frame::Error("READONLY You can't write against a read only replica.".into());
        }
    }

    // cluster slot validation — check whether we own the slot for this key.
    // when `asking` is true, importing slots are allowed through.
    if let Some(redirect) = super::dispatch::cluster_slot_check(ctx, &cmd, asking).await {
        return redirect;
    }

    match cmd {
        // -- no shard needed --
        Command::Ping(None) => Frame::Simple("PONG".into()),
        Command::Ping(Some(msg)) => Frame::Bulk(msg),
        Command::Echo(msg) => Frame::Bulk(msg),
        Command::Command { subcommand, args } => handle_command_cmd(subcommand.as_deref(), &args),

        // -- client commands (connection-scoped, no shard needed) --
        Command::ClientId => Frame::Integer(client_id as i64),
        Command::ClientGetName => {
            let name = ctx
                .clients
                .lock()
                .ok()
                .and_then(|map| map.get(&client_id).and_then(|c| c.name.clone()));
            match name {
                Some(n) => Frame::Bulk(Bytes::from(n)),
                None => Frame::Null,
            }
        }
        Command::ClientSetName { name } => {
            if let Ok(mut map) = ctx.clients.lock() {
                if let Some(info) = map.get_mut(&client_id) {
                    info.name = if name.is_empty() { None } else { Some(name) };
                }
            }
            Frame::Simple("OK".into())
        }
        Command::ClientList => {
            let output = format_client_list(ctx);
            Frame::Bulk(Bytes::from(output))
        }

        // -- single-key commands --
        Command::Get { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Get { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Set {
            key,
            value,
            expire,
            nx,
            xx,
        } => {
            let duration = expire.map(set_expire_to_duration);
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Set {
                key: key.clone(),
                value,
                expire: duration,
                nx,
                xx,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Ok) => {
                    notify_write(
                        ctx,
                        pubsub,
                        crate::keyspace_notifications::FLAG_DOLLAR,
                        "set",
                        &key,
                    );
                    Frame::Simple("OK".into())
                }
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Expire { key, seconds } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Expire {
                key: key.clone(),
                seconds,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Bool(true)) => {
                    notify_write(
                        ctx,
                        pubsub,
                        crate::keyspace_notifications::FLAG_G,
                        "expire",
                        &key,
                    );
                    Frame::Integer(1)
                }
                Ok(ShardResponse::Bool(false)) => Frame::Integer(0),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Expireat { key, timestamp } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Expireat {
                key: key.clone(),
                timestamp,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Bool(true)) => {
                    notify_write(
                        ctx,
                        pubsub,
                        crate::keyspace_notifications::FLAG_G,
                        "expireat",
                        &key,
                    );
                    Frame::Integer(1)
                }
                Ok(ShardResponse::Bool(false)) => Frame::Integer(0),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Pexpireat { key, timestamp_ms } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Pexpireat {
                key: key.clone(),
                timestamp_ms,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Bool(true)) => {
                    notify_write(
                        ctx,
                        pubsub,
                        crate::keyspace_notifications::FLAG_G,
                        "pexpireat",
                        &key,
                    );
                    Frame::Integer(1)
                }
                Ok(ShardResponse::Bool(false)) => Frame::Integer(0),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Ttl { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Ttl { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Ttl(TtlResult::Seconds(s))) => Frame::Integer(s as i64),
                Ok(ShardResponse::Ttl(TtlResult::NoExpiry)) => Frame::Integer(-1),
                Ok(ShardResponse::Ttl(TtlResult::NotFound)) => Frame::Integer(-2),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Incr { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Incr { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Decr { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Decr { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::IncrBy { key, delta } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::IncrBy { key, delta };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::DecrBy { key, delta } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::DecrBy { key, delta };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Append { key, value } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Append { key, value };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Strlen { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Strlen { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::GetRange { key, start, end } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::GetRange { key, start, end };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SetRange { key, offset, value } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SetRange { key, offset, value };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::GetBit { key, offset } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::GetBit { key, offset };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(bit)) => Frame::Integer(bit),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SetBit { key, offset, value } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SetBit { key, offset, value };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(old_bit)) => Frame::Integer(old_bit),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::BitCount { key, range } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::BitCount { key, range };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::BitPos { key, bit, range } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::BitPos { key, bit, range };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(pos)) => Frame::Integer(pos),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::BitOp { op, dest, keys } => {
            // Read source keys from their respective shards, then compute the
            // bitwise result here and write it to dest's shard. This is
            // necessary because source keys may live on different shards.
            let responses = match engine
                .route_multi(&keys, |k| ShardRequest::Get { key: k })
                .await
            {
                Ok(r) => r,
                Err(e) => return Frame::Error(format!("ERR {e}")),
            };

            let mut sources: Vec<Bytes> = Vec::with_capacity(responses.len());
            for r in responses {
                match r {
                    ShardResponse::Value(Some(Value::String(b))) => sources.push(b),
                    ShardResponse::Value(None) => sources.push(Bytes::new()),
                    ShardResponse::WrongType => return wrongtype_error(),
                    _ => sources.push(Bytes::new()),
                }
            }

            let result_len = sources.iter().map(|s| s.len()).max().unwrap_or(0);
            let mut result = vec![0u8; result_len];
            match op {
                BitOpKind::Not => {
                    let src = sources.first().map(|b| b.as_ref()).unwrap_or(&[]);
                    for (i, b) in result.iter_mut().enumerate() {
                        *b = if i < src.len() { !src[i] } else { 0xFF };
                    }
                }
                BitOpKind::And => {
                    if let Some(first) = sources.first() {
                        for (i, b) in result.iter_mut().enumerate() {
                            *b = if i < first.len() { first[i] } else { 0 };
                        }
                    }
                    for src in sources.iter().skip(1) {
                        for (i, b) in result.iter_mut().enumerate() {
                            *b &= if i < src.len() { src[i] } else { 0 };
                        }
                    }
                }
                BitOpKind::Or => {
                    for src in &sources {
                        for (i, b) in result.iter_mut().enumerate() {
                            if i < src.len() {
                                *b |= src[i];
                            }
                        }
                    }
                }
                BitOpKind::Xor => {
                    for src in &sources {
                        for (i, b) in result.iter_mut().enumerate() {
                            if i < src.len() {
                                *b ^= src[i];
                            }
                        }
                    }
                }
            }

            let dest_idx = engine.shard_for_key(&dest);
            let req = ShardRequest::Set {
                key: dest,
                value: Bytes::from(result),
                expire: None,
                nx: false,
                xx: false,
            };
            match engine.send_to_shard(dest_idx, req).await {
                Ok(ShardResponse::Ok) | Ok(ShardResponse::Value(_)) => {
                    Frame::Integer(result_len as i64)
                }
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::IncrByFloat { key, delta } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::IncrByFloat { key, delta };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::BulkString(val)) => Frame::Bulk(Bytes::from(val)),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Persist { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Persist { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Bool(b)) => Frame::Integer(i64::from(b)),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Pttl { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Pttl { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Ttl(TtlResult::Milliseconds(ms))) => Frame::Integer(ms as i64),
                Ok(ShardResponse::Ttl(TtlResult::NoExpiry)) => Frame::Integer(-1),
                Ok(ShardResponse::Ttl(TtlResult::NotFound)) => Frame::Integer(-2),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Pexpire { key, milliseconds } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Pexpire { key, milliseconds };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Bool(b)) => Frame::Integer(i64::from(b)),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Expiretime { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Expiretime { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Pexpiretime { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Pexpiretime { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        // -- multi-key fan-out --
        Command::Del { keys } => {
            multi_key_bool(engine, &keys, |k| ShardRequest::Del { key: k }).await
        }

        Command::Unlink { keys } => {
            multi_key_bool(engine, &keys, |k| ShardRequest::Unlink { key: k }).await
        }

        Command::Exists { keys } => {
            multi_key_bool(engine, &keys, |k| ShardRequest::Exists { key: k }).await
        }

        Command::Touch { keys } => {
            multi_key_bool(engine, &keys, |k| ShardRequest::Touch { key: k }).await
        }

        Command::MGet { keys } => {
            match engine
                .route_multi(&keys, |k| ShardRequest::Get { key: k })
                .await
            {
                Ok(responses) => {
                    let frames: Vec<Frame> = responses
                        .into_iter()
                        .map(|r| match r {
                            ShardResponse::Value(Some(Value::String(data))) => Frame::Bulk(data),
                            ShardResponse::Value(None) => Frame::Null,
                            // MGET on wrong type returns null per Redis behavior
                            ShardResponse::WrongType => Frame::Null,
                            _ => Frame::Null,
                        })
                        .collect();
                    Frame::Array(frames)
                }
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::MSet { pairs } => {
            // Fan out individual SET requests — MSET always succeeds (or OOMs).
            // We build a HashMap for O(1) value lookups during routing. If there
            // are duplicate keys in pairs, the HashMap keeps the last value, which
            // matches Redis semantics (last write wins).
            let keys: Vec<String> = pairs.iter().map(|(k, _)| k.clone()).collect();
            let values: std::collections::HashMap<String, Bytes> = pairs.into_iter().collect();

            match engine
                .route_multi(&keys, |k| {
                    // Safe: k comes from keys, which came from pairs, so it exists in values.
                    let value = values.get(&k).cloned().unwrap_or_default();
                    ShardRequest::Set {
                        key: k,
                        value,
                        expire: None,
                        nx: false,
                        xx: false,
                    }
                })
                .await
            {
                Ok(responses) => {
                    // check if any SET failed due to OOM
                    for r in &responses {
                        if matches!(r, ShardResponse::OutOfMemory) {
                            return oom_error();
                        }
                    }
                    Frame::Simple("OK".into())
                }
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::MSetNx { pairs } => {
            // MSETNX is all-or-nothing: set all keys only if none exist.
            //
            // We implement this with two fan-out passes:
            //   1. Check existence of every key across all shards.
            //   2. If all are absent, write all pairs.
            //
            // This is not atomic across shards (no distributed transaction),
            // but matches Redis cluster semantics where MSETNX pairs must
            // share a hash slot. For single-node mode it is correct.
            if pairs.is_empty() {
                return Frame::Error("ERR wrong number of arguments for 'MSETNX'".into());
            }

            let keys: Vec<String> = pairs.iter().map(|(k, _)| k.clone()).collect();

            // phase 1: check existence
            let exists_responses = match engine
                .route_multi(&keys, |k| ShardRequest::Exists { key: k })
                .await
            {
                Ok(r) => r,
                Err(e) => return Frame::Error(format!("ERR {e}")),
            };

            let any_exists = exists_responses
                .iter()
                .any(|r| matches!(r, ShardResponse::Bool(true)));
            if any_exists {
                return Frame::Integer(0);
            }

            // phase 2: write all pairs
            let values: std::collections::HashMap<String, Bytes> = pairs.into_iter().collect();
            match engine
                .route_multi(&keys, |k| {
                    let value = values.get(&k).cloned().unwrap_or_default();
                    ShardRequest::Set {
                        key: k,
                        value,
                        expire: None,
                        nx: false,
                        xx: false,
                    }
                })
                .await
            {
                Ok(responses) => {
                    for r in &responses {
                        if matches!(r, ShardResponse::OutOfMemory) {
                            return oom_error();
                        }
                    }
                    Frame::Integer(1)
                }
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::GetSet { key, value } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::GetSet { key, value };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::RandomKey => match engine.broadcast(|| ShardRequest::RandomKey).await {
            Ok(responses) => {
                let keys: Vec<String> = responses
                    .into_iter()
                    .flat_map(|r| match r {
                        ShardResponse::StringArray(v) => v,
                        _ => vec![],
                    })
                    .collect();
                if keys.is_empty() {
                    Frame::Null
                } else {
                    use rand::seq::IndexedRandom;
                    let mut rng = rand::rng();
                    match keys.choose(&mut rng) {
                        Some(k) => Frame::Bulk(Bytes::from(k.to_owned())),
                        None => Frame::Null,
                    }
                }
            }
            Err(e) => Frame::Error(format!("ERR {e}")),
        },

        Command::Sort {
            key,
            desc,
            alpha,
            limit,
            store: Some(dest),
        } => {
            // phase 1: sort on the source shard
            let src_idx = engine.shard_for_key(&key);
            let sort_req = ShardRequest::Sort {
                key,
                desc,
                alpha,
                limit,
            };
            match engine.send_to_shard(src_idx, sort_req).await {
                Ok(ShardResponse::Array(items)) => {
                    let count = items.len() as i64;
                    // phase 2: delete dest + rpush sorted items
                    let dst_idx = engine.shard_for_key(&dest);
                    let del_req = ShardRequest::Del { key: dest.clone() };
                    let _ = engine.send_to_shard(dst_idx, del_req).await;
                    if !items.is_empty() {
                        let rpush_req = ShardRequest::RPush {
                            key: dest,
                            values: items,
                        };
                        let _ = engine.send_to_shard(dst_idx, rpush_req).await;
                    }
                    Frame::Integer(count)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        // -- broadcast commands --
        Command::DbSize => match engine.broadcast(|| ShardRequest::DbSize).await {
            Ok(responses) => {
                let total: usize = responses
                    .iter()
                    .map(|r| match r {
                        ShardResponse::KeyCount(n) => *n,
                        _ => 0,
                    })
                    .sum();
                Frame::Integer(total as i64)
            }
            Err(e) => Frame::Error(format!("ERR {e}")),
        },

        Command::Info { section } => render_info(engine, ctx, section.as_deref()).await,

        Command::ConfigGet { pattern } => {
            let pairs = ctx.config.get_matching(&pattern);
            let mut frames = Vec::with_capacity(pairs.len() * 2);
            for (key, value) in pairs {
                frames.push(Frame::Bulk(Bytes::from(key)));
                frames.push(Frame::Bulk(Bytes::from(value)));
            }
            Frame::Array(frames)
        }

        Command::ConfigSet { param, value } => {
            if let Err(e) = ctx.config.set(&param, &value) {
                Frame::Error(e)
            } else {
                // apply dynamic updates for known parameters
                let key = param.to_ascii_lowercase();
                if key == "slowlog-log-slower-than" {
                    if let Ok(us) = value.parse::<i64>() {
                        slow_log.update_threshold(us);
                    }
                } else if key == "slowlog-max-len" {
                    if let Ok(len) = value.parse::<usize>() {
                        slow_log.update_max_len(len);
                    }
                } else if key == "maxmemory" || key == "maxmemory-policy" {
                    let limit = ctx.config.memory_limit();
                    let policy = ctx.config.eviction_policy();
                    // broadcast is fallible but config is already stored — log and continue
                    let _ = engine
                        .broadcast(move || ShardRequest::UpdateMemoryConfig {
                            max_memory: limit,
                            eviction_policy: policy,
                        })
                        .await;
                    // keep the INFO-visible limit in sync
                    ctx.max_memory_limit.store(
                        limit.unwrap_or(0) as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                } else if key == "notify-keyspace-events" {
                    let flags = crate::keyspace_notifications::parse_keyspace_event_flags(&value);
                    ctx.keyspace_event_flags
                        .store(flags, std::sync::atomic::Ordering::Relaxed);
                }
                Frame::Simple("OK".into())
            }
        }

        Command::ConfigRewrite => match &ctx.config_path {
            Some(path) => match ctx.config.rewrite(path) {
                Ok(()) => Frame::Simple("OK".into()),
                Err(e) => Frame::Error(e),
            },
            None => Frame::Error("ERR The server is running without a config file".into()),
        },

        Command::BgSave => match engine.broadcast(|| ShardRequest::Snapshot).await {
            Ok(_) => {
                use std::time::{SystemTime, UNIX_EPOCH};
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                ctx.last_save_timestamp
                    .store(ts, std::sync::atomic::Ordering::Relaxed);
                Frame::Simple("Background saving started".into())
            }
            Err(e) => Frame::Error(format!("ERR {e}")),
        },

        Command::BgRewriteAof => match engine.broadcast(|| ShardRequest::RewriteAof).await {
            Ok(_) => Frame::Simple("Background append only file rewriting started".into()),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },

        Command::Time => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let dur = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default();
            Frame::Array(vec![
                Frame::Bulk(Bytes::from(dur.as_secs().to_string())),
                Frame::Bulk(Bytes::from(dur.subsec_micros().to_string())),
            ])
        }

        Command::LastSave => {
            let ts = ctx
                .last_save_timestamp
                .load(std::sync::atomic::Ordering::Relaxed);
            Frame::Integer(ts as i64)
        }

        Command::Role => {
            if let Some(ref cluster) = ctx.cluster {
                use ember_cluster::NodeRole;
                let info = cluster.replication_info().await;
                match info.role {
                    NodeRole::Primary => Frame::Array(vec![
                        Frame::Bulk(Bytes::from("master")),
                        Frame::Integer(0),
                        Frame::Array(vec![]),
                    ]),
                    NodeRole::Replica => {
                        let (host, port) = match info.primary_addr {
                            Some(addr) => (addr.ip().to_string(), addr.port() as i64),
                            None => (String::new(), 0),
                        };
                        Frame::Array(vec![
                            Frame::Bulk(Bytes::from("slave")),
                            Frame::Bulk(Bytes::from(host)),
                            Frame::Integer(port),
                            Frame::Bulk(Bytes::from("connected")),
                            Frame::Integer(0),
                        ])
                    }
                }
            } else {
                Frame::Array(vec![
                    Frame::Bulk(Bytes::from("master")),
                    Frame::Integer(0),
                    Frame::Array(vec![]),
                ])
            }
        }

        Command::Wait {
            numreplicas,
            timeout_ms,
        } => handle_wait(ctx, numreplicas, timeout_ms).await,

        Command::FlushDb { async_mode } => {
            let req = if async_mode {
                || ShardRequest::FlushDbAsync
            } else {
                || ShardRequest::FlushDb
            };
            match engine.broadcast(req).await {
                Ok(_) => Frame::Simple("OK".into()),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        // Ember is single-database, so FLUSHALL is identical to FLUSHDB.
        Command::FlushAll { async_mode } => {
            let req = if async_mode {
                || ShardRequest::FlushDbAsync
            } else {
                || ShardRequest::FlushDb
            };
            match engine.broadcast(req).await {
                Ok(_) => Frame::Simple("OK".into()),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::MemoryUsage { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::MemoryUsage { key: key.clone() };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(-1)) => Frame::Null,
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Keys { pattern } => {
            match engine
                .broadcast(|| ShardRequest::Keys {
                    pattern: pattern.clone(),
                })
                .await
            {
                Ok(responses) => {
                    let mut all_keys = Vec::new();
                    for r in responses {
                        if let ShardResponse::StringArray(keys) = r {
                            all_keys.extend(keys);
                        }
                    }
                    Frame::Array(
                        all_keys
                            .into_iter()
                            .map(|k| Frame::Bulk(Bytes::from(k)))
                            .collect(),
                    )
                }
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Rename { key, newkey } => {
            if !engine.same_shard(&key, &newkey) {
                Frame::Error("ERR source and destination keys must hash to the same shard".into())
            } else {
                let idx = engine.shard_for_key(&key);
                let req = ShardRequest::Rename { key, newkey };
                match engine.send_to_shard(idx, req).await {
                    Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
                    Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
                    Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                    Err(e) => Frame::Error(format!("ERR {e}")),
                }
            }
        }

        Command::Copy {
            source,
            destination,
            replace,
        } => {
            if !engine.same_shard(&source, &destination) {
                Frame::Error("ERR source and destination keys must hash to the same shard".into())
            } else {
                let idx = engine.shard_for_key(&source);
                let req = ShardRequest::Copy {
                    source,
                    destination,
                    replace,
                };
                match engine.send_to_shard(idx, req).await {
                    Ok(ShardResponse::Bool(b)) => Frame::Integer(i64::from(b)),
                    Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
                    Ok(ShardResponse::OutOfMemory) => oom_error(),
                    Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                    Err(e) => Frame::Error(format!("ERR {e}")),
                }
            }
        }

        Command::ObjectEncoding { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ObjectEncoding { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::EncodingName(Some(name))) => Frame::Bulk(Bytes::from(name)),
                Ok(ShardResponse::EncodingName(None)) => Frame::Null,
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ObjectRefcount { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Exists { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Bool(true)) => Frame::Integer(1),
                Ok(ShardResponse::Bool(false)) => Frame::Null,
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Scan {
            cursor,
            pattern,
            count,
        } => {
            // cursor encoding: (shard_id << 48) | position_within_shard
            //
            // this gives us 16 bits for shard_id (up to 65536 shards) and 48 bits
            // for position within each shard. cursor 0 always means "start fresh".
            //
            // the cursor is opaque to clients — they just pass back whatever we
            // returned last time. this lets us iterate across the sharded keyspace
            // without clients needing to know the topology.
            let shard_count = engine.shard_count();
            let count = count.unwrap_or(10);

            let (shard_id, position) = if cursor == 0 {
                (0usize, 0u64)
            } else {
                let shard_id = (cursor >> 48) as usize;
                let position = cursor & 0xFFFF_FFFF_FFFF;

                // guard against invalid cursor (shard_id out of range)
                if shard_id >= shard_count {
                    return Frame::Array(vec![Frame::Bulk(Bytes::from("0")), Frame::Array(vec![])]);
                }

                (shard_id, position)
            };

            // collect keys from current shard and possibly subsequent shards
            let mut all_keys = Vec::new();
            let mut current_shard = shard_id;
            let mut current_pos = position;

            while all_keys.len() < count && current_shard < shard_count {
                let req = ShardRequest::Scan {
                    cursor: current_pos,
                    count: count.saturating_sub(all_keys.len()),
                    pattern: pattern.clone(),
                };
                match engine.send_to_shard(current_shard, req).await {
                    Ok(ShardResponse::Scan {
                        cursor: next_pos,
                        keys,
                    }) => {
                        all_keys.extend(keys);
                        if next_pos == 0 {
                            // shard exhausted, move to next
                            current_shard += 1;
                            current_pos = 0;
                        } else {
                            current_pos = next_pos;
                            break; // have more in this shard, stop here
                        }
                    }
                    Ok(other) => {
                        return Frame::Error(format!("ERR unexpected shard response: {other:?}"));
                    }
                    Err(e) => {
                        return Frame::Error(format!("ERR {e}"));
                    }
                }
            }

            // compute next cursor
            let next_cursor = if current_shard >= shard_count {
                0 // scan complete
            } else {
                ((current_shard as u64) << 48) | current_pos
            };

            // return [cursor, [keys...]]
            let cursor_str = next_cursor.to_string();
            let keys_frames: Vec<Frame> = all_keys
                .into_iter()
                .map(|k| Frame::Bulk(Bytes::from(k)))
                .collect();
            Frame::Array(vec![
                Frame::Bulk(Bytes::from(cursor_str)),
                Frame::Array(keys_frames),
            ])
        }

        // -- list commands --
        Command::LPush { key, values } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::LPush {
                key: key.clone(),
                values,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => {
                    notify_write(
                        ctx,
                        pubsub,
                        crate::keyspace_notifications::FLAG_L,
                        "lpush",
                        &key,
                    );
                    Frame::Integer(n as i64)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::RPush { key, values } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::RPush {
                key: key.clone(),
                values,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => {
                    notify_write(
                        ctx,
                        pubsub,
                        crate::keyspace_notifications::FLAG_L,
                        "rpush",
                        &key,
                    );
                    Frame::Integer(n as i64)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LPop { key, count: None } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::LPop { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LPop {
            key,
            count: Some(count),
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::LPopCount { key, count };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Array(items)) => {
                    let frames = items.into_iter().map(Frame::Bulk).collect();
                    Frame::Array(frames)
                }
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::RPop { key, count: None } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::RPop { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::RPop {
            key,
            count: Some(count),
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::RPopCount { key, count };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Array(items)) => {
                    let frames = items.into_iter().map(Frame::Bulk).collect();
                    Frame::Array(frames)
                }
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LRange { key, start, stop } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::LRange { key, start, stop };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Array(items)) => {
                    let frames = items.into_iter().map(Frame::Bulk).collect();
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LLen { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::LLen { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LIndex { key, index } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::LIndex { key, index };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LSet { key, index, value } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::LSet { key, index, value };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LTrim { key, start, stop } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::LTrim { key, start, stop };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LInsert {
            key,
            before,
            pivot,
            value,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::LInsert {
                key,
                before,
                pivot,
                value,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LRem { key, count, value } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::LRem { key, count, value };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LPos {
            key,
            element,
            rank,
            count,
            maxlen,
        } => {
            let idx = engine.shard_for_key(&key);
            let shard_count = count.unwrap_or(1);
            let req = ShardRequest::LPos {
                key,
                element,
                rank,
                count: shard_count,
                maxlen,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::IntegerArray(positions)) => {
                    if count.is_some() {
                        Frame::Array(positions.into_iter().map(Frame::Integer).collect())
                    } else if let Some(&pos) = positions.first() {
                        Frame::Integer(pos)
                    } else {
                        Frame::Null
                    }
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        // blocking list ops are handled by handle_blocking_pop_cmd in the
        // main loop; reaching here means they're inside a transaction.
        Command::BLPop { .. } | Command::BRPop { .. } => {
            Frame::Error("ERR blocking commands are not allowed inside transactions".into())
        }

        Command::Type { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Type { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::TypeName(name)) => Frame::Simple(name.into()),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        // -- sorted set commands --
        Command::ZAdd {
            key,
            flags,
            members,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZAdd {
                key: key.clone(),
                members,
                nx: flags.nx,
                xx: flags.xx,
                gt: flags.gt,
                lt: flags.lt,
                ch: flags.ch,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ZAddLen { count, .. }) => {
                    if count > 0 {
                        notify_write(
                            ctx,
                            pubsub,
                            crate::keyspace_notifications::FLAG_Z,
                            "zadd",
                            &key,
                        );
                    }
                    Frame::Integer(count as i64)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZRem { key, members } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZRem { key, members };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ZRemLen { count, .. }) => Frame::Integer(count as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZScore { key, member } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZScore { key, member };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Score(Some(s))) => Frame::Bulk(Bytes::from(format!("{s}"))),
                Ok(ShardResponse::Score(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZRank { key, member } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZRank { key, member };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Rank(Some(r))) => Frame::Integer(r as i64),
                Ok(ShardResponse::Rank(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZRange {
            key,
            start,
            stop,
            with_scores,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZRange {
                key,
                start,
                stop,
                with_scores,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ScoredArray(items)) => {
                    let mut frames = Vec::new();
                    for (member, score) in items {
                        frames.push(Frame::Bulk(Bytes::from(member)));
                        if with_scores {
                            frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                        }
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZRevRank { key, member } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZRevRank { key, member };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Rank(Some(r))) => Frame::Integer(r as i64),
                Ok(ShardResponse::Rank(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZCard { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZCard { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZRevRange {
            key,
            start,
            stop,
            with_scores,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZRevRange {
                key,
                start,
                stop,
                with_scores,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ScoredArray(items)) => {
                    let mut frames = Vec::new();
                    for (member, score) in items {
                        frames.push(Frame::Bulk(Bytes::from(member)));
                        if with_scores {
                            frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                        }
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZCount { key, min, max } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZCount { key, min, max };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZIncrBy {
            key,
            increment,
            member,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZIncrBy {
                key,
                increment,
                member,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ZIncrByResult { new_score, .. }) => {
                    Frame::Bulk(Bytes::from(format!("{new_score}")))
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZRangeByScore {
            key,
            min,
            max,
            with_scores,
            offset,
            count,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZRangeByScore {
                key,
                min,
                max,
                offset,
                count,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ScoredArray(items)) => {
                    let mut frames = Vec::new();
                    for (member, score) in items {
                        frames.push(Frame::Bulk(Bytes::from(member)));
                        if with_scores {
                            frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                        }
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZRevRangeByScore {
            key,
            min,
            max,
            with_scores,
            offset,
            count,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZRevRangeByScore {
                key,
                min,
                max,
                offset,
                count,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ScoredArray(items)) => {
                    let mut frames = Vec::new();
                    for (member, score) in items {
                        frames.push(Frame::Bulk(Bytes::from(member)));
                        if with_scores {
                            frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                        }
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZPopMin { key, count } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZPopMin { key, count };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ZPopResult(items)) => {
                    let mut frames = Vec::with_capacity(items.len() * 2);
                    for (member, score) in items {
                        frames.push(Frame::Bulk(Bytes::from(member)));
                        frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZPopMax { key, count } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZPopMax { key, count };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ZPopResult(items)) => {
                    let mut frames = Vec::with_capacity(items.len() * 2);
                    for (member, score) in items {
                        frames.push(Frame::Bulk(Bytes::from(member)));
                        frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Lmpop { keys, left, count } => {
            for key in &keys {
                let idx = engine.shard_for_key(key);
                let req = ShardRequest::LmpopSingle {
                    key: key.clone(),
                    left,
                    count,
                };
                match engine.send_to_shard(idx, req).await {
                    Ok(ShardResponse::Array(items)) if !items.is_empty() => {
                        let elems = Frame::Array(items.into_iter().map(Frame::Bulk).collect());
                        return Frame::Array(vec![Frame::Bulk(Bytes::from(key.clone())), elems]);
                    }
                    Ok(ShardResponse::Array(_)) | Ok(ShardResponse::Value(None)) => continue,
                    Ok(ShardResponse::WrongType) => return wrongtype_error(),
                    Ok(other) => {
                        return Frame::Error(format!("ERR unexpected shard response: {other:?}"))
                    }
                    Err(e) => return Frame::Error(format!("ERR {e}")),
                }
            }
            Frame::Null
        }

        Command::Zmpop { keys, min, count } => {
            for key in &keys {
                let idx = engine.shard_for_key(key);
                let req = ShardRequest::ZmpopSingle {
                    key: key.clone(),
                    min,
                    count,
                };
                match engine.send_to_shard(idx, req).await {
                    Ok(ShardResponse::ZPopResult(members)) if !members.is_empty() => {
                        let pairs: Vec<Frame> = members
                            .into_iter()
                            .flat_map(|(m, s)| {
                                vec![
                                    Frame::Bulk(Bytes::from(m)),
                                    Frame::Bulk(Bytes::from(format!("{s}"))),
                                ]
                            })
                            .collect();
                        return Frame::Array(vec![
                            Frame::Bulk(Bytes::from(key.clone())),
                            Frame::Array(pairs),
                        ]);
                    }
                    Ok(ShardResponse::ZPopResult(_)) | Ok(ShardResponse::Value(None)) => continue,
                    Ok(ShardResponse::WrongType) => return wrongtype_error(),
                    Ok(other) => {
                        return Frame::Error(format!("ERR unexpected shard response: {other:?}"))
                    }
                    Err(e) => return Frame::Error(format!("ERR {e}")),
                }
            }
            Frame::Null
        }

        // --- hash commands ---
        Command::HSet { key, fields } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HSet {
                key: key.clone(),
                fields,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => {
                    notify_write(
                        ctx,
                        pubsub,
                        crate::keyspace_notifications::FLAG_H,
                        "hset",
                        &key,
                    );
                    Frame::Integer(n as i64)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HGet { key, field } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HGet { key, field };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HGetAll { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HGetAll { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::HashFields(fields)) => {
                    let mut frames = Vec::with_capacity(fields.len() * 2);
                    for (field, value) in fields {
                        frames.push(Frame::Bulk(Bytes::from(field)));
                        frames.push(Frame::Bulk(value));
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HDel { key, fields } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HDel { key, fields };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::HDelLen { count, .. }) => Frame::Integer(count as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HExists { key, field } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HExists { key, field };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Bool(b)) => Frame::Integer(if b { 1 } else { 0 }),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HLen { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HLen { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HIncrBy { key, field, delta } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HIncrBy { key, field, delta };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HIncrByFloat { key, field, delta } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HIncrByFloat { key, field, delta };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::BulkString(val)) => Frame::Bulk(Bytes::from(val)),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HKeys { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HKeys { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::StringArray(keys)) => Frame::Array(
                    keys.into_iter()
                        .map(|k| Frame::Bulk(Bytes::from(k)))
                        .collect(),
                ),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HVals { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HVals { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Array(vals)) => {
                    Frame::Array(vals.into_iter().map(Frame::Bulk).collect())
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HMGet { key, fields } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HMGet { key, fields };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::OptionalArray(vals)) => Frame::Array(
                    vals.into_iter()
                        .map(|v| match v {
                            Some(data) => Frame::Bulk(data),
                            None => Frame::Null,
                        })
                        .collect(),
                ),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HRandField {
            key,
            count,
            with_values,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HRandField {
                key,
                count,
                with_values,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::HRandFieldResult(pairs)) => {
                    if count.is_none() {
                        // no count: return a single bulk string (or nil if empty)
                        match pairs.into_iter().next() {
                            Some((field, _)) => Frame::Bulk(Bytes::from(field)),
                            None => Frame::Null,
                        }
                    } else {
                        // with count: return array, interleaved with values if requested
                        let frames: Vec<Frame> = pairs
                            .into_iter()
                            .flat_map(|(f, v)| {
                                let mut items = vec![Frame::Bulk(Bytes::from(f))];
                                if let Some(val) = v {
                                    items.push(Frame::Bulk(val));
                                }
                                items
                            })
                            .collect();
                        Frame::Array(frames)
                    }
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        // --- set commands ---
        Command::SAdd { key, members } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SAdd {
                key: key.clone(),
                members,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => {
                    if n > 0 {
                        notify_write(
                            ctx,
                            pubsub,
                            crate::keyspace_notifications::FLAG_S,
                            "sadd",
                            &key,
                        );
                    }
                    Frame::Integer(n as i64)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SRem { key, members } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SRem { key, members };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SMembers { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SMembers { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::StringArray(members)) => Frame::Array(
                    members
                        .into_iter()
                        .map(|m| Frame::Bulk(Bytes::from(m)))
                        .collect(),
                ),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SIsMember { key, member } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SIsMember { key, member };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Bool(b)) => Frame::Integer(if b { 1 } else { 0 }),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SCard { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SCard { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SUnion { keys } => {
            let key = keys.first().cloned().unwrap_or_default();
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SUnion { keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::StringArray(members)) => Frame::Array(
                    members
                        .into_iter()
                        .map(|m| Frame::Bulk(Bytes::from(m)))
                        .collect(),
                ),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SInter { keys } => {
            let key = keys.first().cloned().unwrap_or_default();
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SInter { keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::StringArray(members)) => Frame::Array(
                    members
                        .into_iter()
                        .map(|m| Frame::Bulk(Bytes::from(m)))
                        .collect(),
                ),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SDiff { keys } => {
            let key = keys.first().cloned().unwrap_or_default();
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SDiff { keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::StringArray(members)) => Frame::Array(
                    members
                        .into_iter()
                        .map(|m| Frame::Bulk(Bytes::from(m)))
                        .collect(),
                ),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SUnionStore { dest, keys } => {
            let idx = engine.shard_for_key(&dest);
            let req = ShardRequest::SUnionStore { dest, keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::SetStoreResult { count, .. }) => Frame::Integer(count as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SInterStore { dest, keys } => {
            let idx = engine.shard_for_key(&dest);
            let req = ShardRequest::SInterStore { dest, keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::SetStoreResult { count, .. }) => Frame::Integer(count as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SDiffStore { dest, keys } => {
            let idx = engine.shard_for_key(&dest);
            let req = ShardRequest::SDiffStore { dest, keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::SetStoreResult { count, .. }) => Frame::Integer(count as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SRandMember { key, count } => {
            let count = count.unwrap_or(1);
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SRandMember { key, count };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::StringArray(members)) => Frame::Array(
                    members
                        .into_iter()
                        .map(|m| Frame::Bulk(Bytes::from(m)))
                        .collect(),
                ),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SPop { key, count } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SPop { key, count };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::StringArray(members)) => Frame::Array(
                    members
                        .into_iter()
                        .map(|m| Frame::Bulk(Bytes::from(m)))
                        .collect(),
                ),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SMisMember { key, members } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SMisMember { key, members };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::BoolArray(arr)) => Frame::Array(
                    arr.into_iter()
                        .map(|b| Frame::Integer(i64::from(b)))
                        .collect(),
                ),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SMove {
            source,
            destination,
            member,
        } => {
            let src_idx = engine.shard_for_key(&source);
            let dst_idx = engine.shard_for_key(&destination);

            if src_idx == dst_idx {
                // same shard — single atomic operation
                let req = ShardRequest::SMove {
                    source,
                    destination,
                    member,
                };
                match engine.send_to_shard(src_idx, req).await {
                    Ok(ShardResponse::Bool(moved)) => Frame::Integer(if moved { 1 } else { 0 }),
                    Ok(ShardResponse::WrongType) => wrongtype_error(),
                    Ok(ShardResponse::OutOfMemory) => oom_error(),
                    Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                    Err(e) => Frame::Error(format!("ERR {e}")),
                }
            } else {
                // cross-shard: remove from source, then add to destination
                let rem_req = ShardRequest::SRem {
                    key: source,
                    members: vec![member.clone()],
                };
                let removed = match engine.send_to_shard(src_idx, rem_req).await {
                    Ok(ShardResponse::Len(n)) => n,
                    Ok(ShardResponse::WrongType) => return wrongtype_error(),
                    Ok(other) => {
                        return Frame::Error(format!("ERR unexpected shard response: {other:?}"))
                    }
                    Err(e) => return Frame::Error(format!("ERR {e}")),
                };

                if removed == 0 {
                    return Frame::Integer(0);
                }

                let add_req = ShardRequest::SAdd {
                    key: destination,
                    members: vec![member],
                };
                match engine.send_to_shard(dst_idx, add_req).await {
                    Ok(ShardResponse::Len(_)) => Frame::Integer(1),
                    Ok(ShardResponse::WrongType) => wrongtype_error(),
                    Ok(ShardResponse::OutOfMemory) => oom_error(),
                    Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                    Err(e) => Frame::Error(format!("ERR {e}")),
                }
            }
        }

        Command::SInterCard { keys, limit } => {
            // Fetch members for each key from its owning shard, then intersect.
            // This handles keys spread across shards without cross-shard calls
            // inside the keyspace layer.
            let mut sets: Vec<std::collections::HashSet<String>> = Vec::with_capacity(keys.len());
            for key in &keys {
                let idx = engine.shard_for_key(key);
                let req = ShardRequest::SMembers { key: key.clone() };
                match engine.send_to_shard(idx, req).await {
                    Ok(ShardResponse::StringArray(members)) => {
                        // an empty set (including missing key) short-circuits to 0
                        if members.is_empty() {
                            return Frame::Integer(0);
                        }
                        sets.push(members.into_iter().collect());
                    }
                    Ok(ShardResponse::WrongType) => return wrongtype_error(),
                    Ok(other) => {
                        return Frame::Error(format!("ERR unexpected shard response: {other:?}"))
                    }
                    Err(e) => return Frame::Error(format!("ERR {e}")),
                }
            }

            if sets.is_empty() {
                return Frame::Integer(0);
            }

            // Start with the smallest set to minimise comparisons.
            sets.sort_unstable_by_key(|s| s.len());
            let Some((first, rest)) = sets.split_first() else {
                return Frame::Integer(0);
            };
            let mut count = 0usize;
            'outer: for member in first {
                for other in rest {
                    if !other.contains(member.as_str()) {
                        continue 'outer;
                    }
                }
                count += 1;
                if limit > 0 && count >= limit {
                    break;
                }
            }

            Frame::Integer(count as i64)
        }

        Command::LMove {
            source,
            destination,
            src_left,
            dst_left,
        } => {
            // route to the source key's shard
            let idx = engine.shard_for_key(&source);
            let req = ShardRequest::LMove {
                source,
                destination,
                src_left,
                dst_left,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::GetDel { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::GetDel { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::GetEx { key, expire } => {
            let idx = engine.shard_for_key(&key);
            // convert SetExpire into an Option<Option<u64>> (milliseconds from now)
            let expire_ms: Option<Option<u64>> = expire.map(|opt| {
                opt.map(|se| match se {
                    SetExpire::Ex(s) => Duration::from_secs(s).as_millis() as u64,
                    SetExpire::Px(ms) => ms,
                    SetExpire::ExAt(ts) => {
                        use std::time::{SystemTime, UNIX_EPOCH};
                        let now_s = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        Duration::from_secs(ts.saturating_sub(now_s)).as_millis() as u64
                    }
                    SetExpire::PxAt(ts_ms) => {
                        use std::time::{SystemTime, UNIX_EPOCH};
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        ts_ms.saturating_sub(now_ms)
                    }
                })
            });
            let req = ShardRequest::GetEx {
                key,
                expire: expire_ms,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZDiff { keys, with_scores } => {
            let key = keys.first().cloned().unwrap_or_default();
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZDiff { keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ScoredArray(items)) => {
                    let mut frames = Vec::new();
                    for (member, score) in items {
                        frames.push(Frame::Bulk(Bytes::from(member)));
                        if with_scores {
                            frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                        }
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZInter { keys, with_scores } => {
            let key = keys.first().cloned().unwrap_or_default();
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZInter { keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ScoredArray(items)) => {
                    let mut frames = Vec::new();
                    for (member, score) in items {
                        frames.push(Frame::Bulk(Bytes::from(member)));
                        if with_scores {
                            frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                        }
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZUnion { keys, with_scores } => {
            let key = keys.first().cloned().unwrap_or_default();
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZUnion { keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ScoredArray(items)) => {
                    let mut frames = Vec::new();
                    for (member, score) in items {
                        frames.push(Frame::Bulk(Bytes::from(member)));
                        if with_scores {
                            frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                        }
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZDiffStore { dest, keys } => {
            let idx = engine.shard_for_key(&dest);
            let req = ShardRequest::ZDiffStore { dest: dest.clone(), keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ZStoreResult { count, .. }) => {
                    notify_write(
                        ctx,
                        pubsub,
                        crate::keyspace_notifications::FLAG_Z,
                        "zdiffstore",
                        &dest,
                    );
                    Frame::Integer(count as i64)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZInterStore { dest, keys } => {
            let idx = engine.shard_for_key(&dest);
            let req = ShardRequest::ZInterStore { dest: dest.clone(), keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ZStoreResult { count, .. }) => {
                    notify_write(
                        ctx,
                        pubsub,
                        crate::keyspace_notifications::FLAG_Z,
                        "zinterstore",
                        &dest,
                    );
                    Frame::Integer(count as i64)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZUnionStore { dest, keys } => {
            let idx = engine.shard_for_key(&dest);
            let req = ShardRequest::ZUnionStore { dest: dest.clone(), keys };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ZStoreResult { count, .. }) => {
                    notify_write(
                        ctx,
                        pubsub,
                        crate::keyspace_notifications::FLAG_Z,
                        "zunionstore",
                        &dest,
                    );
                    Frame::Integer(count as i64)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZRandMember {
            key,
            count,
            with_scores,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZRandMember {
                key,
                count,
                with_scores,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ZRandMemberResult(pairs)) => {
                    if count.is_none() {
                        // no count: return a single bulk string (or nil if empty)
                        match pairs.into_iter().next() {
                            Some((member, _)) => Frame::Bulk(Bytes::from(member)),
                            None => Frame::Null,
                        }
                    } else {
                        // with count: return array, interleaved with scores if requested
                        let frames: Vec<Frame> = pairs
                            .into_iter()
                            .flat_map(|(m, s)| {
                                let mut items = vec![Frame::Bulk(Bytes::from(m))];
                                if let Some(score) = s {
                                    items.push(Frame::Bulk(Bytes::from(score.to_string())));
                                }
                                items
                            })
                            .collect();
                        Frame::Array(frames)
                    }
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SScan {
            key,
            cursor,
            pattern,
            count,
        } => {
            let count = count.unwrap_or(10);
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SScan {
                key,
                cursor,
                count,
                pattern,
            };
            resolve_collection_scan(engine.send_to_shard(idx, req).await)
        }

        Command::HScan {
            key,
            cursor,
            pattern,
            count,
        } => {
            let count = count.unwrap_or(10);
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HScan {
                key,
                cursor,
                count,
                pattern,
            };
            resolve_collection_scan(engine.send_to_shard(idx, req).await)
        }

        Command::ZScan {
            key,
            cursor,
            pattern,
            count,
        } => {
            let count = count.unwrap_or(10);
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ZScan {
                key,
                cursor,
                count,
                pattern,
            };
            resolve_collection_scan(engine.send_to_shard(idx, req).await)
        }

        // --- cluster commands ---
        Command::ClusterKeySlot { key } => {
            let slot = ember_cluster::key_slot(key.as_bytes());
            Frame::Integer(slot as i64)
        }

        Command::ClusterInfo => match &ctx.cluster {
            Some(c) => c.cluster_info().await,
            None => Frame::Bulk(Bytes::from("cluster_enabled:0\r\n")),
        },

        Command::ClusterNodes => match &ctx.cluster {
            Some(c) => c.cluster_nodes().await,
            None => Frame::Bulk(Bytes::from("")),
        },

        Command::ClusterSlots => match &ctx.cluster {
            Some(c) => c.cluster_slots().await,
            None => Frame::Array(vec![]),
        },

        Command::ClusterMyId => match &ctx.cluster {
            Some(c) => c.cluster_myid(),
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterMeet { ip, port } => match &ctx.cluster {
            Some(c) => c.cluster_meet(&ip, port).await,
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterAddSlots { slots } => match &ctx.cluster {
            Some(c) => c.cluster_addslots(&slots).await,
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterAddSlotsRange { ranges } => match &ctx.cluster {
            Some(c) => {
                let slots: Vec<u16> = ranges.iter().flat_map(|&(s, e)| s..=e).collect();
                c.cluster_addslots(&slots).await
            }
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterDelSlots { slots } => match &ctx.cluster {
            Some(c) => c.cluster_delslots(&slots).await,
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterForget { node_id } => match &ctx.cluster {
            Some(c) => c.cluster_forget(&node_id).await,
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterSetSlotImporting { slot, node_id } => match &ctx.cluster {
            Some(c) => c.cluster_setslot_importing(slot, &node_id).await,
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterSetSlotMigrating { slot, node_id } => match &ctx.cluster {
            Some(c) => c.cluster_setslot_migrating(slot, &node_id).await,
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterSetSlotNode { slot, node_id } => match &ctx.cluster {
            Some(c) => c.cluster_setslot_node(slot, &node_id).await,
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterSetSlotStable { slot } => match &ctx.cluster {
            Some(c) => c.cluster_setslot_stable(slot).await,
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterCountKeysInSlot { slot } => {
            match engine
                .broadcast(|| ShardRequest::CountKeysInSlot { slot })
                .await
            {
                Ok(responses) => {
                    let total: usize = responses
                        .iter()
                        .map(|r| match r {
                            ShardResponse::KeyCount(n) => *n,
                            _ => 0,
                        })
                        .sum();
                    Frame::Integer(total as i64)
                }
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ClusterGetKeysInSlot { slot, count } => {
            let count = count as usize;
            match engine
                .broadcast(|| ShardRequest::GetKeysInSlot { slot, count })
                .await
            {
                Ok(responses) => {
                    let mut all_keys = Vec::new();
                    for r in responses {
                        if let ShardResponse::StringArray(keys) = r {
                            all_keys.extend(keys);
                        }
                    }
                    all_keys.truncate(count);
                    Frame::Array(
                        all_keys
                            .into_iter()
                            .map(|k| Frame::Bulk(Bytes::from(k)))
                            .collect(),
                    )
                }
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ClusterReplicate { node_id } => match &ctx.cluster {
            Some(c) => c.cluster_replicate(&node_id).await,
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterFailover { force, takeover } => match &ctx.cluster {
            Some(c) => c.cluster_failover(force, takeover).await,
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::Migrate {
            host,
            port,
            key,
            timeout_ms,
            replace,
            ..
        } => {
            // dump the key from the local shard
            let idx = engine.shard_for_key(&key);
            let dump_req = ShardRequest::DumpKey { key: key.clone() };
            let dump_resp = match engine.send_to_shard(idx, dump_req).await {
                Ok(r) => r,
                Err(e) => return Frame::Error(format!("ERR {e}")),
            };

            let (data, ttl_ms) = match dump_resp {
                ShardResponse::KeyDump { data, ttl_ms } => (data, ttl_ms),
                ShardResponse::Value(None) => {
                    return Frame::Error("ERR no such key".into());
                }
                _ => return Frame::Error("ERR internal error".into()),
            };

            // send RESTORE to the target node
            let ttl_arg = if ttl_ms < 0 { 0u64 } else { ttl_ms as u64 };
            let timeout = Duration::from_millis(timeout_ms.max(1000));
            let addr = format!("{host}:{port}");

            let result = tokio::time::timeout(timeout, async {
                let mut stream = tokio::net::TcpStream::connect(&addr).await?;

                // build RESTORE command as RESP3 array
                let mut parts = vec![
                    Frame::Bulk(Bytes::from("RESTORE")),
                    Frame::Bulk(Bytes::from(key.clone())),
                    Frame::Bulk(Bytes::from(ttl_arg.to_string())),
                    Frame::Bulk(Bytes::from(data)),
                ];
                if replace {
                    parts.push(Frame::Bulk(Bytes::from("REPLACE")));
                }
                let cmd_frame = Frame::Array(parts);

                let mut buf = BytesMut::new();
                cmd_frame.serialize(&mut buf);
                stream.write_all(&buf).await?;

                // read response
                let mut read_buf = BytesMut::with_capacity(256);
                loop {
                    let n = stream.read_buf(&mut read_buf).await?;
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "connection closed by target",
                        ));
                    }
                    match parse_frame(&read_buf) {
                        Ok(Some((frame, _))) => return Ok(frame),
                        Ok(None) => {} // need more data
                        Err(e) => {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
                        }
                    }
                }
            })
            .await;

            match result {
                Ok(Ok(Frame::Simple(_))) => {
                    // success — delete local key and mark as migrated
                    let del_req = ShardRequest::Del { key: key.clone() };
                    let _ = engine.send_to_shard(idx, del_req).await;

                    if let Some(c) = &ctx.cluster {
                        let slot = ember_cluster::key_slot(key.as_bytes());
                        c.mark_key_migrated(slot, key.as_bytes()).await;
                    }
                    Frame::Simple("OK".into())
                }
                Ok(Ok(Frame::Error(e))) => Frame::Error(format!("ERR target error: {e}")),
                Ok(Ok(_)) => Frame::Error("ERR unexpected response from target".into()),
                Ok(Err(e)) => Frame::Error(format!("ERR {e}")),
                Err(_) => Frame::Error("ERR timeout connecting to target".into()),
            }
        }

        Command::Restore {
            key,
            ttl_ms,
            data,
            replace,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::RestoreKey {
                key,
                ttl_ms,
                data,
                replace,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
                Ok(ShardResponse::Err(e)) => Frame::Error(e),
                Ok(_) => Frame::Error("ERR internal error".into()),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        // -- slow log commands --
        Command::SlowLogGet { count } => {
            let entries = slow_log.get(count);
            let frames: Vec<Frame> = entries
                .into_iter()
                .map(|e| {
                    let ts = e
                        .timestamp
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    Frame::Array(vec![
                        Frame::Integer(e.id.min(i64::MAX as u64) as i64),
                        Frame::Integer(ts.min(i64::MAX as u64) as i64),
                        Frame::Integer(e.duration.as_micros().min(i64::MAX as u128) as i64),
                        Frame::Array(vec![Frame::Bulk(Bytes::from(e.command))]),
                    ])
                })
                .collect();
            Frame::Array(frames)
        }

        Command::SlowLogLen => Frame::Integer(slow_log.len() as i64),

        Command::SlowLogReset => {
            slow_log.reset();
            Frame::Simple("OK".into())
        }

        // -- pub/sub --
        Command::Publish { channel, message } => {
            let count = pubsub.publish(&channel, message);
            Frame::Integer(count as i64)
        }

        Command::PubSubChannels { pattern } => {
            let names = pubsub.channel_names(pattern.as_deref());
            Frame::Array(names.into_iter().map(|n| Frame::Bulk(n.into())).collect())
        }

        Command::PubSubNumSub { channels } => {
            let pairs = pubsub.numsub(&channels);
            let mut frames = Vec::with_capacity(pairs.len() * 2);
            for (ch, count) in pairs {
                frames.push(Frame::Bulk(ch.into()));
                frames.push(Frame::Integer(count as i64));
            }
            Frame::Array(frames)
        }

        Command::PubSubNumPat => Frame::Integer(pubsub.active_patterns() as i64),

        // subscribe commands are handled in the connection loop, not here.
        // if we reach this point, something went wrong.
        Command::Subscribe { .. }
        | Command::Unsubscribe { .. }
        | Command::PSubscribe { .. }
        | Command::PUnsubscribe { .. } => {
            Frame::Error("ERR subscribe commands should not reach execute".into())
        }

        // --- vector commands ---
        #[cfg(feature = "vector")]
        Command::VAdd {
            key,
            element,
            vector,
            metric,
            quantization,
            connectivity,
            expansion_add,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::VAdd {
                key,
                element,
                vector,
                metric,
                quantization,
                connectivity,
                expansion_add,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::VAddResult { added, .. }) => {
                    Frame::Integer(if added { 1 } else { 0 })
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "vector")]
        Command::VAddBatch {
            key,
            entries,
            dim,
            metric,
            quantization,
            connectivity,
            expansion_add,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::VAddBatch {
                key,
                entries,
                dim,
                metric,
                quantization,
                connectivity,
                expansion_add,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::VAddBatchResult { added_count, .. }) => {
                    Frame::Integer(added_count as i64)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "vector")]
        Command::VSim {
            key,
            query,
            count,
            ef_search,
            with_scores,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::VSim {
                key,
                query,
                count,
                ef_search,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::VSimResult(results)) => {
                    let mut frames = Vec::new();
                    for (element, distance) in results {
                        frames.push(Frame::Bulk(Bytes::from(element)));
                        if with_scores {
                            frames.push(Frame::Bulk(Bytes::from(distance.to_string())));
                        }
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "vector")]
        Command::VRem { key, element } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::VRem { key, element };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Bool(removed)) => Frame::Integer(if removed { 1 } else { 0 }),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "vector")]
        Command::VGet { key, element } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::VGet { key, element };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::VectorData(Some(vector))) => Frame::Array(
                    vector
                        .into_iter()
                        .map(|v| Frame::Bulk(Bytes::from(v.to_string())))
                        .collect(),
                ),
                Ok(ShardResponse::VectorData(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "vector")]
        Command::VCard { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::VCard { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(count)) => Frame::Integer(count),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "vector")]
        Command::VDim { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::VDim { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Integer(dim)) => Frame::Integer(dim),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "vector")]
        Command::VInfo { key } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::VInfo { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::VectorInfo(Some(fields))) => {
                    let mut frames = Vec::with_capacity(fields.len() * 2);
                    for (k, v) in fields {
                        frames.push(Frame::Bulk(Bytes::from(k)));
                        frames.push(Frame::Bulk(Bytes::from(v)));
                    }
                    Frame::Array(frames)
                }
                Ok(ShardResponse::VectorInfo(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(not(feature = "vector"))]
        Command::VAdd { .. }
        | Command::VAddBatch { .. }
        | Command::VSim { .. }
        | Command::VRem { .. }
        | Command::VGet { .. }
        | Command::VCard { .. }
        | Command::VDim { .. }
        | Command::VInfo { .. } => {
            Frame::Error("ERR unknown command (vector support not compiled)".into())
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoRegister { name, descriptor } => {
            let registry = match engine.schema_registry() {
                Some(r) => r,
                None => return Frame::Error("ERR protobuf support is not enabled".into()),
            };
            let result = {
                let mut reg = match registry.write() {
                    Ok(r) => r,
                    Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
                };
                reg.register(name.clone(), descriptor.clone())
            };
            match result {
                Ok(types) => {
                    // persist the registration to all shards' AOF
                    if let Err(e) = engine
                        .broadcast(|| ShardRequest::ProtoRegisterAof {
                            name: name.clone(),
                            descriptor: descriptor.clone(),
                        })
                        .await
                    {
                        tracing::warn!("failed to persist proto registration to AOF: {e}");
                    }
                    Frame::Array(
                        types
                            .into_iter()
                            .map(|t| Frame::Bulk(Bytes::from(t)))
                            .collect(),
                    )
                }
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoSet {
            key,
            type_name,
            data,
            expire,
            nx,
            xx,
        } => {
            let registry = match engine.schema_registry() {
                Some(r) => r,
                None => return Frame::Error("ERR protobuf support is not enabled".into()),
            };
            // validate the bytes against the schema before storing
            {
                let reg = match registry.read() {
                    Ok(r) => r,
                    Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
                };
                if let Err(e) = reg.validate(&type_name, &data) {
                    return Frame::Error(format!("ERR {e}"));
                }
            }
            let duration = expire.map(|e| {
                use std::time::{SystemTime, UNIX_EPOCH};
                match e {
                    SetExpire::Ex(secs) => Duration::from_secs(secs),
                    SetExpire::Px(millis) => Duration::from_millis(millis),
                    SetExpire::ExAt(ts) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        Duration::from_secs(ts.saturating_sub(now))
                    }
                    SetExpire::PxAt(ts_ms) => {
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        Duration::from_millis(ts_ms.saturating_sub(now_ms))
                    }
                }
            });
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ProtoSet {
                key,
                type_name,
                data,
                expire: duration,
                nx,
                xx,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoGet { key } => {
            if engine.schema_registry().is_none() {
                return Frame::Error("ERR protobuf support is not enabled".into());
            }
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ProtoGet { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ProtoValue(Some((type_name, data, _ttl)))) => {
                    Frame::Array(vec![Frame::Bulk(Bytes::from(type_name)), Frame::Bulk(data)])
                }
                Ok(ShardResponse::ProtoValue(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoType { key } => {
            if engine.schema_registry().is_none() {
                return Frame::Error("ERR protobuf support is not enabled".into());
            }
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ProtoType { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ProtoTypeName(Some(name))) => Frame::Bulk(Bytes::from(name)),
                Ok(ShardResponse::ProtoTypeName(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoSchemas => {
            let registry = match engine.schema_registry() {
                Some(r) => r,
                None => return Frame::Error("ERR protobuf support is not enabled".into()),
            };
            let reg = match registry.read() {
                Ok(r) => r,
                Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
            };
            let names = reg.schema_names();
            Frame::Array(
                names
                    .into_iter()
                    .map(|n| Frame::Bulk(Bytes::from(n)))
                    .collect(),
            )
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoDescribe { name } => {
            let registry = match engine.schema_registry() {
                Some(r) => r,
                None => return Frame::Error("ERR protobuf support is not enabled".into()),
            };
            let reg = match registry.read() {
                Ok(r) => r,
                Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
            };
            match reg.describe(&name) {
                Some(types) => Frame::Array(
                    types
                        .into_iter()
                        .map(|t| Frame::Bulk(Bytes::from(t)))
                        .collect(),
                ),
                None => Frame::Error(format!("ERR unknown schema '{name}'")),
            }
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoGetField { key, field_path } => {
            let registry = match engine.schema_registry() {
                Some(r) => r,
                None => return Frame::Error("ERR protobuf support is not enabled".into()),
            };
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ProtoGet { key };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ProtoValue(Some((type_name, data, _ttl)))) => {
                    let reg = match registry.read() {
                        Ok(r) => r,
                        Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
                    };
                    match reg.get_field(&type_name, &data, &field_path) {
                        Ok(frame) => frame,
                        Err(e) => Frame::Error(format!("ERR {e}")),
                    }
                }
                Ok(ShardResponse::ProtoValue(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoSetField {
            key,
            field_path,
            value,
        } => {
            if engine.schema_registry().is_none() {
                return Frame::Error("ERR protobuf support is not enabled".into());
            }
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ProtoSetField {
                key,
                field_path,
                value,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ProtoFieldUpdated { .. }) => Frame::Simple("OK".into()),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoDelField { key, field_path } => {
            if engine.schema_registry().is_none() {
                return Frame::Error("ERR protobuf support is not enabled".into());
            }
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::ProtoDelField { key, field_path };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ProtoFieldUpdated { .. }) => Frame::Integer(1),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        // when protobuf feature is disabled, proto commands are unknown
        #[cfg(not(feature = "protobuf"))]
        Command::ProtoRegister { .. }
        | Command::ProtoSet { .. }
        | Command::ProtoGet { .. }
        | Command::ProtoType { .. }
        | Command::ProtoSchemas
        | Command::ProtoDescribe { .. }
        | Command::ProtoGetField { .. }
        | Command::ProtoSetField { .. }
        | Command::ProtoDelField { .. } => {
            Frame::Error("ERR unknown command (protobuf support not compiled)".into())
        }

        // AUTH on an already-authenticated connection (re-auth).
        // note: this verifies the password but doesn't update per-connection
        // ACL state — re-auth with a different user requires reconnecting.
        Command::Auth { username, password } => {
            let uname = username.unwrap_or_else(|| "default".into());
            if let Some(ref acl_state) = ctx.acl {
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
                match &ctx.requirepass {
                    None => Frame::Error(
                        "ERR Client sent AUTH, but no password is set. \
                         Did you mean ACL SETUSER with >password?"
                            .into(),
                    ),
                    Some(expected) => {
                        if uname != "default" {
                            Frame::Error(
                                "WRONGPASS invalid username-password pair or user is disabled."
                                    .into(),
                            )
                        } else if bool::from(password.as_bytes().ct_eq(expected.as_bytes())) {
                            Frame::Simple("OK".into())
                        } else {
                            Frame::Error(
                                "WRONGPASS invalid username-password pair or user is disabled."
                                    .into(),
                            )
                        }
                    }
                }
            }
        }

        Command::Quit => Frame::Simple("OK".into()),

        // ASKING is intercepted by process() and prepare_command() before
        // reaching here, but the match must be exhaustive.
        Command::Asking => Frame::Simple("OK".into()),

        // MULTI/EXEC/DISCARD are intercepted by handle_frame_with_tx() before
        // reaching here. If they arrive directly (e.g. EXEC without MULTI),
        // they should have been caught earlier — return an error for safety.
        Command::Multi => Frame::Error("ERR MULTI calls can not be nested".into()),
        Command::Exec => Frame::Error("ERR EXEC without MULTI".into()),
        Command::Discard => Frame::Error("ERR DISCARD without MULTI".into()),

        // MONITOR is handled at the frame level before process() is called.
        // If it arrives here (e.g. during a transaction), just return OK.
        Command::Monitor => Frame::Simple("OK".into()),

        // WATCH/UNWATCH are intercepted by handle_frame_with_tx() before
        // process(). If they reach here, return sensible defaults.
        Command::Watch { .. } | Command::Unwatch => Frame::Simple("OK".into()),

        // -- ACL commands --
        // WHOAMI is handled at the connection level (needs current_username).
        // If it reaches here, return a generic response.
        Command::AclWhoAmI => Frame::Bulk(Bytes::from_static(b"default")),

        Command::AclList => {
            if let Some(ref acl) = ctx.acl {
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

        Command::AclUsers => {
            if let Some(ref acl) = ctx.acl {
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

        Command::AclGetUser { username } => {
            if let Some(ref acl) = ctx.acl {
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

        Command::AclDelUser { usernames } => {
            if let Some(ref acl) = ctx.acl {
                match acl.write() {
                    Ok(mut state) => match state.del_users(&usernames) {
                        Ok(count) => Frame::Integer(count as i64),
                        Err(msg) => Frame::Error(msg),
                    },
                    Err(_) => Frame::Error("ERR ACL state lock poisoned".into()),
                }
            } else {
                Frame::Error(
                    "ERR ACL is not enabled. Configure ACL users to use this command.".into(),
                )
            }
        }

        Command::AclSetUser { username, rules } => {
            if let Some(ref acl) = ctx.acl {
                match acl.write() {
                    Ok(mut state) => match state.set_user(&username, &rules) {
                        Ok(()) => Frame::Simple("OK".into()),
                        Err(msg) => Frame::Error(msg),
                    },
                    Err(_) => Frame::Error("ERR ACL state lock poisoned".into()),
                }
            } else {
                Frame::Error(
                    "ERR ACL is not enabled. Configure ACL users to use this command.".into(),
                )
            }
        }

        Command::AclCat { category } => crate::acl::handle_acl_cat(category.as_deref()),

        // SORT without STORE is normally dispatched via route! in prepare_command,
        // but execute() must be exhaustive.
        Command::Sort {
            key,
            desc,
            alpha,
            limit,
            store: None,
        } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Sort {
                key,
                desc,
                alpha,
                limit,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Array(items)) => {
                    Frame::Array(items.into_iter().map(Frame::Bulk).collect())
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Unknown(name) => Frame::Error(format!("ERR unknown command '{name}'")),
    }
}

/// Fans out a boolean-result command across shards for multiple keys
/// and returns the count of `true` results as an integer frame.
///
/// Uses `route_multi` to dispatch all keys concurrently rather than
/// awaiting each one sequentially.
async fn multi_key_bool<F>(engine: &Engine, keys: &[String], make_req: F) -> Frame
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

/// Renders the INFO response with multiple sections.
///
/// With no argument, returns all sections. With a section name,
/// returns only that section. Matches Redis convention of `#` headers
/// followed by `key:value` pairs separated by `\r\n`.
async fn render_info(engine: &Engine, ctx: &Arc<ServerContext>, section: Option<&str>) -> Frame {
    let section_upper = section.map(|s| s.to_ascii_uppercase());
    let want_all = section_upper.is_none();
    let want = |name: &str| want_all || section_upper.as_deref() == Some(name);

    // only broadcast to shards if we need keyspace/memory/persistence sections
    let stats = if want("KEYSPACE") || want("MEMORY") || want("PERSISTENCE") || want("STATS") {
        match engine.broadcast(|| ShardRequest::Stats).await {
            Ok(responses) => {
                let mut total = KeyspaceStats {
                    key_count: 0,
                    used_bytes: 0,
                    keys_with_expiry: 0,
                    keys_expired: 0,
                    keys_evicted: 0,
                    oom_rejections: 0,
                    keyspace_hits: 0,
                    keyspace_misses: 0,
                };
                for r in &responses {
                    if let ShardResponse::Stats(s) = r {
                        total.key_count += s.key_count;
                        total.used_bytes += s.used_bytes;
                        total.keys_with_expiry += s.keys_with_expiry;
                        total.keys_expired += s.keys_expired;
                        total.keys_evicted += s.keys_evicted;
                        total.oom_rejections += s.oom_rejections;
                        total.keyspace_hits += s.keyspace_hits;
                        total.keyspace_misses += s.keyspace_misses;
                    }
                }
                Some(total)
            }
            Err(e) => return Frame::Error(format!("ERR {e}")),
        }
    } else {
        None
    };

    let mut out = String::with_capacity(512);

    if want("SERVER") {
        let uptime = ctx.start_time.elapsed().as_secs();
        out.push_str("# Server\r\n");
        out.push_str(&format!("ember_version:{}\r\n", ctx.version));
        out.push_str(&format!("process_id:{}\r\n", std::process::id()));
        out.push_str(&format!("uptime_in_seconds:{uptime}\r\n"));
        out.push_str(&format!("shard_count:{}\r\n", ctx.shard_count));
        out.push_str(&format!("tcp_port:{}\r\n", ctx.bind_addr.port()));
        out.push_str("hz:10\r\n");
        if let Some(ref path) = ctx.config_path {
            out.push_str(&format!("config_file:{}\r\n", path.display()));
        } else {
            out.push_str("config_file:\r\n");
        }
        out.push_str("\r\n");
    }

    if want("CLIENTS") {
        let connected = ctx.connections_active.load(Ordering::Relaxed);
        out.push_str("# Clients\r\n");
        out.push_str(&format!("connected_clients:{connected}\r\n"));
        out.push_str(&format!("max_clients:{}\r\n", ctx.max_connections));
        out.push_str("\r\n");
    }

    if want("MEMORY") {
        if let Some(ref stats) = stats {
            out.push_str("# Memory\r\n");
            out.push_str(&format!("used_memory:{}\r\n", stats.used_bytes));
            out.push_str(&format!(
                "used_memory_human:{}\r\n",
                human_bytes(stats.used_bytes)
            ));
            if let Some(rss) = get_rss_bytes() {
                out.push_str(&format!("used_memory_rss:{rss}\r\n"));
                out.push_str(&format!("used_memory_rss_human:{}\r\n", human_bytes(rss)));
            }
            let max_bytes = ctx
                .max_memory_limit
                .load(std::sync::atomic::Ordering::Relaxed) as usize;
            if max_bytes > 0 {
                let effective = ember_core::memory::effective_limit(max_bytes);
                out.push_str(&format!("max_memory:{max_bytes}\r\n"));
                out.push_str(&format!("max_memory_human:{}\r\n", human_bytes(max_bytes)));
                out.push_str(&format!("max_memory_effective:{effective}\r\n"));
                out.push_str(&format!(
                    "max_memory_effective_human:{}\r\n",
                    human_bytes(effective)
                ));
            } else {
                out.push_str("max_memory:0\r\n");
                out.push_str("max_memory_human:unlimited\r\n");
            }
            out.push_str("\r\n");
        }
    }

    if want("PERSISTENCE") {
        let last_save = ctx
            .last_save_timestamp
            .load(std::sync::atomic::Ordering::Relaxed);
        out.push_str("# Persistence\r\n");
        out.push_str(&format!(
            "aof_enabled:{}\r\n",
            if ctx.aof_enabled { 1 } else { 0 }
        ));
        out.push_str("aof_last_bgrewrite_status:ok\r\n");
        out.push_str(&format!("rdb_last_save_time:{last_save}\r\n"));
        out.push_str("\r\n");
    }

    if want("STATS") {
        let total_conns = ctx.connections_accepted.load(Ordering::Relaxed);
        let total_cmds = ctx.commands_processed.load(Ordering::Relaxed);
        out.push_str("# Stats\r\n");
        out.push_str(&format!("total_connections_received:{total_conns}\r\n"));
        out.push_str(&format!("total_commands_processed:{total_cmds}\r\n"));
        if let Some(ref stats) = stats {
            out.push_str(&format!("expired_keys:{}\r\n", stats.keys_expired));
            out.push_str(&format!("evicted_keys:{}\r\n", stats.keys_evicted));
            out.push_str(&format!("oom_rejections:{}\r\n", stats.oom_rejections));
            out.push_str(&format!("keyspace_hits:{}\r\n", stats.keyspace_hits));
            out.push_str(&format!("keyspace_misses:{}\r\n", stats.keyspace_misses));
        }
        out.push_str("\r\n");
    }

    if want("KEYSPACE") {
        if let Some(ref stats) = stats {
            out.push_str("# Keyspace\r\n");
            if stats.key_count > 0 {
                out.push_str(&format!(
                    "db0:keys={},expires={},used_bytes={}\r\n",
                    stats.key_count, stats.keys_with_expiry, stats.used_bytes
                ));
            }
            out.push_str("\r\n");
        }
    }

    if want("REPLICATION") {
        out.push_str("# Replication\r\n");
        if let Some(ref cluster) = ctx.cluster {
            let info = cluster.replication_info().await;
            use ember_cluster::NodeRole;
            match info.role {
                NodeRole::Primary => {
                    out.push_str("role:primary\r\n");
                    out.push_str(&format!("connected_replicas:{}\r\n", info.replica_count));
                }
                NodeRole::Replica => {
                    out.push_str("role:replica\r\n");
                    if let Some(addr) = info.primary_addr {
                        out.push_str(&format!("master_host:{}\r\n", addr.ip()));
                        out.push_str(&format!("master_port:{}\r\n", addr.port()));
                        out.push_str("master_link_status:up\r\n");
                    } else {
                        out.push_str("master_link_status:down\r\n");
                    }
                }
            }
        } else {
            out.push_str("role:primary\r\n");
            out.push_str("connected_replicas:0\r\n");
        }
        out.push_str("\r\n");
    }

    // trim trailing blank line
    if out.ends_with("\r\n\r\n") {
        out.truncate(out.len() - 2);
    }

    Frame::Bulk(Bytes::from(out))
}

/// Returns the standard WRONGTYPE error frame.
pub(super) fn wrongtype_error() -> Frame {
    Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}

/// Resolves a collection scan (SSCAN/HSCAN/ZSCAN) shard response into a RESP frame.
pub(super) fn resolve_collection_scan(
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

/// Returns the standard OOM error frame.
pub(super) fn oom_error() -> Frame {
    Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
}

/// Handles COMMAND [COUNT | INFO name... | DOCS name... | LIST].
///
/// Provides static command metadata for client library discovery.
/// The format matches Redis 7 conventions so clients can probe capabilities
/// without falling back to error-handling paths.
fn handle_command_cmd(subcommand: Option<&str>, args: &[String]) -> Frame {
    match subcommand {
        None | Some("LIST") => Frame::Array(COMMAND_TABLE.iter().map(command_entry).collect()),
        Some("COUNT") => Frame::Integer(COMMAND_TABLE.len() as i64),
        Some("INFO") => {
            if args.is_empty() {
                return Frame::Array(COMMAND_TABLE.iter().map(command_entry).collect());
            }
            let frames = args
                .iter()
                .map(|name| {
                    let upper = name.to_ascii_uppercase();
                    match COMMAND_TABLE.iter().find(|e| e.name == upper) {
                        Some(entry) => command_entry(entry),
                        None => Frame::Null,
                    }
                })
                .collect();
            Frame::Array(frames)
        }
        Some("DOCS") => {
            // return empty docs — clients use this for documentation display,
            // not capability detection. an empty map per command is valid.
            if args.is_empty() {
                return Frame::Array(vec![]);
            }
            let mut frames = Vec::with_capacity(args.len() * 2);
            for name in args {
                let upper = name.to_ascii_uppercase();
                frames.push(Frame::Bulk(Bytes::from(upper)));
                frames.push(Frame::Array(vec![]));
            }
            Frame::Array(frames)
        }
        Some("GETKEYS") => Frame::Array(vec![]),
        Some(other) => Frame::Error(format!("ERR unknown COMMAND subcommand '{other}'")),
    }
}

/// Builds a COMMAND entry array for a single command.
///
/// Format: [name, arity, [flags], first_key, last_key, step]
fn command_entry(e: &CommandEntry) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from(e.name.to_ascii_lowercase())),
        Frame::Integer(e.arity),
        Frame::Array(e.flags.iter().map(|f| Frame::Simple((*f).into())).collect()),
        Frame::Integer(e.first_key),
        Frame::Integer(e.last_key),
        Frame::Integer(e.step),
    ])
}

struct CommandEntry {
    name: &'static str,
    arity: i64,
    flags: &'static [&'static str],
    first_key: i64,
    last_key: i64,
    step: i64,
}

/// Static command table. Arity: positive = exact, negative = minimum.
/// Flags: write, readonly, denyoom, admin, pubsub, noscript, fast, loading, etc.
static COMMAND_TABLE: &[CommandEntry] = &[
    CommandEntry { name: "APPEND", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "AUTH", arity: -2, flags: &["noscript", "loading", "fast", "no_auth"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "BGREWRITEAOF", arity: 1, flags: &["admin"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "BGSAVE", arity: -1, flags: &["admin"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "BITCOUNT", arity: -2, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "BITOP", arity: -4, flags: &["write", "denyoom"], first_key: 2, last_key: -1, step: 1 },
    CommandEntry { name: "BITPOS", arity: -3, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "BLPOP", arity: -3, flags: &["write", "noscript"], first_key: 1, last_key: -2, step: 1 },
    CommandEntry { name: "BRPOP", arity: -3, flags: &["write", "noscript"], first_key: 1, last_key: -2, step: 1 },
    CommandEntry { name: "CLIENT", arity: -2, flags: &["admin", "noscript", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "CLUSTER", arity: -2, flags: &["admin"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "COMMAND", arity: -1, flags: &["loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "CONFIG", arity: -2, flags: &["admin", "loading", "noscript"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "COPY", arity: -3, flags: &["write"], first_key: 1, last_key: 2, step: 1 },
    CommandEntry { name: "DBSIZE", arity: 1, flags: &["readonly", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "DECR", arity: 2, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "DECRBY", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "DEL", arity: -2, flags: &["write"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "DISCARD", arity: 1, flags: &["noscript", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "ECHO", arity: 2, flags: &["fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "EXEC", arity: 1, flags: &["noscript", "loading"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "EXISTS", arity: -2, flags: &["readonly", "fast"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "EXPIRE", arity: 3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "EXPIREAT", arity: 3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "EXPIRETIME", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "FLUSHALL", arity: -1, flags: &["write"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "FLUSHDB", arity: -1, flags: &["write"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "GET", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "GETBIT", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "GETDEL", arity: 2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "GETEX", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "GETRANGE", arity: 4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "GETSET", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HDEL", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HEXISTS", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HGET", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HGETALL", arity: 2, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HINCRBY", arity: 4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HINCRBYFLOAT", arity: 4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HKEYS", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HLEN", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HMGET", arity: -3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HMSET", arity: -4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HRANDFIELD", arity: -2, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HSCAN", arity: -3, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HSET", arity: -4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "HVALS", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "INCR", arity: 2, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "INCRBY", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "INCRBYFLOAT", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "INFO", arity: -1, flags: &["loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "KEYS", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "LASTSAVE", arity: 1, flags: &["random", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "LINDEX", arity: 3, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "LINSERT", arity: 5, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "LLEN", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "LMOVE", arity: 5, flags: &["write", "denyoom"], first_key: 1, last_key: 2, step: 1 },
    CommandEntry { name: "LMPOP", arity: -4, flags: &["write", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "LPOS", arity: -3, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "LPOP", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "LPUSH", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "LPUSHX", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "LRANGE", arity: 4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "LREM", arity: 4, flags: &["write"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "LSET", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "LTRIM", arity: 4, flags: &["write"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "MEMORY", arity: -2, flags: &["readonly"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "MGET", arity: -2, flags: &["readonly", "fast"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "MIGRATE", arity: -6, flags: &["write"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "MONITOR", arity: 1, flags: &["admin", "loading"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "MSET", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 2 },
    CommandEntry { name: "MSETNX", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 2 },
    CommandEntry { name: "MULTI", arity: 1, flags: &["noscript", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "OBJECT", arity: -2, flags: &["slow"], first_key: 2, last_key: 2, step: 1 },
    CommandEntry { name: "PERSIST", arity: 2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "PEXPIRE", arity: 3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "PEXPIREAT", arity: 3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "PEXPIRETIME", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "PING", arity: -1, flags: &["fast", "loading"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "PSETEX", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "PSUBSCRIBE", arity: -2, flags: &["pubsub", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "PTTL", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "PUBLISH", arity: 3, flags: &["pubsub", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "PUBSUB", arity: -2, flags: &["pubsub", "random", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "PUNSUBSCRIBE", arity: -1, flags: &["pubsub", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "QUIT", arity: 1, flags: &["fast", "loading"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "RANDOMKEY", arity: 1, flags: &["readonly", "random"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "RENAME", arity: 3, flags: &["write"], first_key: 1, last_key: 2, step: 1 },
    CommandEntry { name: "ROLE", arity: 1, flags: &["noscript", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "RPOP", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "RPUSH", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "RPUSHX", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SADD", arity: -3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SCAN", arity: -2, flags: &["readonly"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "SCARD", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SDIFF", arity: -2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "SDIFFSTORE", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "SET", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SETBIT", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SETEX", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SETNX", arity: 3, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SETRANGE", arity: 4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SINTER", arity: -2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "SINTERCARD", arity: -3, flags: &["readonly"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "SINTERSTORE", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "SISMEMBER", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SLOWLOG", arity: -2, flags: &["admin", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "SMEMBERS", arity: 2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SMISMEMBER", arity: -3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SMOVE", arity: 4, flags: &["write", "fast"], first_key: 1, last_key: 2, step: 1 },
    CommandEntry { name: "SORT", arity: -2, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SPOP", arity: -2, flags: &["write", "random", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SRANDMEMBER", arity: -2, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SREM", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SSCAN", arity: -3, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "STRLEN", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "SUBSCRIBE", arity: -2, flags: &["pubsub", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "SUNION", arity: -2, flags: &["readonly", "sort_for_script"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "SUNIONSTORE", arity: -3, flags: &["write", "denyoom"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "TIME", arity: 1, flags: &["random", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "TOUCH", arity: -2, flags: &["readonly", "fast"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "TTL", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "TYPE", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "UNLINK", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "UNSUBSCRIBE", arity: -1, flags: &["pubsub", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "UNWATCH", arity: 1, flags: &["noscript", "loading", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "WAIT", arity: 3, flags: &["noscript"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "WATCH", arity: -2, flags: &["noscript", "loading", "fast"], first_key: 1, last_key: -1, step: 1 },
    CommandEntry { name: "ZADD", arity: -4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZCARD", arity: 2, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZCOUNT", arity: 4, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZDIFF", arity: -3, flags: &["readonly", "sort_for_script"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "ZDIFFSTORE", arity: -4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZINCRBY", arity: 4, flags: &["write", "denyoom", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZINTER", arity: -3, flags: &["readonly", "sort_for_script"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "ZINTERSTORE", arity: -4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZLEXCOUNT", arity: 4, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZMPOP", arity: -4, flags: &["write", "fast"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "ZPOPMAX", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZPOPMIN", arity: -2, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZRANDMEMBER", arity: -2, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZRANGE", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZRANGEBYSCORE", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZRANK", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZREM", arity: -3, flags: &["write", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZREVRANGE", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZREVRANGEBYSCORE", arity: -4, flags: &["readonly"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZREVRANK", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZSCAN", arity: -3, flags: &["readonly", "random"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZSCORE", arity: 3, flags: &["readonly", "fast"], first_key: 1, last_key: 1, step: 1 },
    CommandEntry { name: "ZUNION", arity: -3, flags: &["readonly", "sort_for_script"], first_key: 0, last_key: 0, step: 0 },
    CommandEntry { name: "ZUNIONSTORE", arity: -4, flags: &["write", "denyoom"], first_key: 1, last_key: 1, step: 1 },
];

/// Implements the WAIT command: blocks until `needed` replicas have
/// acknowledged all writes at or before the current primary offset,
/// or until `timeout_ms` milliseconds elapse.
///
/// Returns the count of replicas that acknowledged in time as a
/// RESP integer. When there are no replicas or no writes, returns
/// immediately without sleeping.
async fn handle_wait(ctx: &Arc<ServerContext>, numreplicas: u64, timeout_ms: u64) -> Frame {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let needed = numreplicas as usize;
    let tracker = &ctx.replica_tracker;

    // fast path: no replicas connected
    if tracker.connected_count() == 0 {
        return Frame::Integer(0);
    }

    let target = tracker.write_offset.load(Ordering::Relaxed);

    // fast path: already satisfied or no timeout needed
    let count = tracker.count_at_or_above(target);
    if count >= needed || timeout_ms == 0 {
        return Frame::Integer(count as i64);
    }

    // poll until enough replicas have caught up or the deadline passes
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let c = tracker.count_at_or_above(target);
        if c >= needed || tokio::time::Instant::now() >= deadline {
            return Frame::Integer(c as i64);
        }
    }
}
