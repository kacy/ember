//! Per-connection handler.
//!
//! Reads RESP3 frames from a TCP stream, routes them through the
//! sharded engine, and writes responses back. Supports pipelining
//! by processing multiple frames from a single read.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use ember_core::{Engine, KeyspaceStats, ShardRequest, ShardResponse, TtlResult, Value};
use ember_protocol::{parse_frame, Command, Frame, SetExpire};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::server::ServerContext;
use crate::slowlog::SlowLog;

/// Initial read buffer capacity. 4KB covers most commands comfortably
/// without over-allocating for simple PING/SET/GET workloads.
const BUF_CAPACITY: usize = 4096;

/// Maximum read buffer size before we disconnect the client. Prevents
/// a single slow or malicious client from consuming unbounded memory
/// with incomplete frames.
const MAX_BUF_SIZE: usize = 64 * 1024 * 1024; // 64 MB

/// How long a connection can be idle (no data received) before we
/// close it. Prevents abandoned connections from leaking resources.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes

/// Drives a single client connection to completion.
///
/// Reads data into a buffer, parses complete frames, dispatches commands
/// through the engine, and writes serialized responses back. The loop
/// exits when the client disconnects or a protocol error occurs.
pub async fn handle(
    mut stream: TcpStream,
    engine: Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
) -> Result<(), Box<dyn std::error::Error>> {
    // disable Nagle's algorithm — cache servers need low-latency writes,
    // and we already batch responses from pipelining into a single write
    stream.set_nodelay(true)?;

    let mut buf = BytesMut::with_capacity(BUF_CAPACITY);
    let mut out = BytesMut::with_capacity(BUF_CAPACITY);

    loop {
        // guard against unbounded buffer growth from incomplete frames
        if buf.len() > MAX_BUF_SIZE {
            let msg = "ERR max buffer size exceeded, closing connection";
            let mut err_buf = BytesMut::new();
            Frame::Error(msg.into()).serialize(&mut err_buf);
            let _ = stream.write_all(&err_buf).await;
            return Ok(());
        }

        // read some data — returns 0 on clean disconnect, times out
        // after IDLE_TIMEOUT to reclaim resources from abandoned connections
        match tokio::time::timeout(IDLE_TIMEOUT, stream.read_buf(&mut buf)).await {
            Ok(Ok(0)) => return Ok(()),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => return Ok(()), // idle timeout — close silently
        }

        // process as many complete frames as the buffer holds (pipelining),
        // batching all responses into a single write buffer
        out.clear();
        loop {
            match parse_frame(&buf) {
                Ok(Some((frame, consumed))) => {
                    let _ = buf.split_to(consumed);
                    let response =
                        process(frame, &engine, ctx, slow_log).await;
                    response.serialize(&mut out);
                }
                Ok(None) => break, // need more data
                Err(e) => {
                    let msg = format!("ERR protocol error: {e}");
                    Frame::Error(msg).serialize(&mut out);
                    stream.write_all(&out).await?;
                    return Ok(());
                }
            }
        }

        if !out.is_empty() {
            stream.write_all(&out).await?;
        }
    }
}

/// Converts a raw frame into a command and executes it.
///
/// When metrics or slowlog are enabled, brackets the command with
/// `Instant::now()` to measure latency. Skips timing entirely when
/// neither feature needs it.
async fn process(
    frame: Frame,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
) -> Frame {
    match Command::from_frame(frame) {
        Ok(cmd) => {
            let cmd_name = cmd.command_name();
            let needs_timing = ctx.metrics_enabled || slow_log.is_enabled();
            let start = if needs_timing {
                Some(Instant::now())
            } else {
                None
            };

            let response = execute(cmd, engine, ctx, slow_log).await;
            ctx.commands_processed.fetch_add(1, Ordering::Relaxed);

            if let Some(start) = start {
                let elapsed = start.elapsed();
                slow_log.maybe_record(elapsed, cmd_name);
                if ctx.metrics_enabled {
                    let is_error = matches!(&response, Frame::Error(_));
                    crate::metrics::record_command(cmd_name, elapsed, is_error);
                }
            }

            response
        }
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

/// Executes a parsed command and returns the response frame.
///
/// Ping and Echo are handled inline (no shard routing needed).
/// Single-key commands route to the owning shard. Multi-key commands
/// (DEL, EXISTS) fan out across shards and aggregate results.
async fn execute(
    cmd: Command,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
) -> Frame {
    match cmd {
        // -- no shard needed --
        Command::Ping(None) => Frame::Simple("PONG".into()),
        Command::Ping(Some(msg)) => Frame::Bulk(msg),
        Command::Echo(msg) => Frame::Bulk(msg),

        // -- single-key commands --
        Command::Get { key } => {
            let req = ShardRequest::Get { key: key.clone() };
            match engine.route(&key, req).await {
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
            let duration = expire.map(|e| match e {
                SetExpire::Ex(secs) => Duration::from_secs(secs),
                SetExpire::Px(millis) => Duration::from_millis(millis),
            });
            let req = ShardRequest::Set {
                key: key.clone(),
                value,
                expire: duration,
                nx,
                xx,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Expire { key, seconds } => {
            let req = ShardRequest::Expire {
                key: key.clone(),
                seconds,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Bool(b)) => Frame::Integer(i64::from(b)),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Ttl { key } => {
            let req = ShardRequest::Ttl { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Ttl(TtlResult::Seconds(s))) => Frame::Integer(s as i64),
                Ok(ShardResponse::Ttl(TtlResult::NoExpiry)) => Frame::Integer(-1),
                Ok(ShardResponse::Ttl(TtlResult::NotFound)) => Frame::Integer(-2),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Incr { key } => {
            let req = ShardRequest::Incr { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => {
                    Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
                }
                Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Decr { key } => {
            let req = ShardRequest::Decr { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => {
                    Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
                }
                Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Persist { key } => {
            let req = ShardRequest::Persist { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Bool(b)) => Frame::Integer(i64::from(b)),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Pttl { key } => {
            let req = ShardRequest::Pttl { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Ttl(TtlResult::Milliseconds(ms))) => Frame::Integer(ms as i64),
                Ok(ShardResponse::Ttl(TtlResult::NoExpiry)) => Frame::Integer(-1),
                Ok(ShardResponse::Ttl(TtlResult::NotFound)) => Frame::Integer(-2),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Pexpire { key, milliseconds } => {
            let req = ShardRequest::Pexpire {
                key: key.clone(),
                milliseconds,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Bool(b)) => Frame::Integer(i64::from(b)),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        // -- multi-key fan-out --
        Command::Del { keys } => {
            multi_key_bool(engine, &keys, |k| ShardRequest::Del { key: k }).await
        }

        Command::Exists { keys } => {
            multi_key_bool(engine, &keys, |k| ShardRequest::Exists { key: k }).await
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

        Command::Info { section } => {
            render_info(engine, ctx, section.as_deref()).await
        }

        Command::BgSave => match engine.broadcast(|| ShardRequest::Snapshot).await {
            Ok(_) => Frame::Simple("Background saving started".into()),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },

        Command::BgRewriteAof => match engine.broadcast(|| ShardRequest::RewriteAof).await {
            Ok(_) => Frame::Simple("Background append only file rewriting started".into()),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },

        Command::FlushDb => match engine.broadcast(|| ShardRequest::FlushDb).await {
            Ok(_) => Frame::Simple("OK".into()),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },

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
            let req = ShardRequest::LPush {
                key: key.clone(),
                values,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::RPush { key, values } => {
            let req = ShardRequest::RPush {
                key: key.clone(),
                values,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LPop { key } => {
            let req = ShardRequest::LPop { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::RPop { key } => {
            let req = ShardRequest::RPop { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LRange { key, start, stop } => {
            let req = ShardRequest::LRange {
                key: key.clone(),
                start,
                stop,
            };
            match engine.route(&key, req).await {
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
            let req = ShardRequest::LLen { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::Type { key } => {
            let req = ShardRequest::Type { key: key.clone() };
            match engine.route(&key, req).await {
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
            let req = ShardRequest::ZAdd {
                key: key.clone(),
                members,
                nx: flags.nx,
                xx: flags.xx,
                gt: flags.gt,
                lt: flags.lt,
                ch: flags.ch,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::ZAddLen { count, .. }) => Frame::Integer(count as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZRem { key, members } => {
            let req = ShardRequest::ZRem {
                key: key.clone(),
                members,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::ZRemLen { count, .. }) => Frame::Integer(count as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZScore { key, member } => {
            let req = ShardRequest::ZScore {
                key: key.clone(),
                member,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Score(Some(s))) => Frame::Bulk(Bytes::from(format!("{s}"))),
                Ok(ShardResponse::Score(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::ZRank { key, member } => {
            let req = ShardRequest::ZRank {
                key: key.clone(),
                member,
            };
            match engine.route(&key, req).await {
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
            let req = ShardRequest::ZRange {
                key: key.clone(),
                start,
                stop,
                with_scores,
            };
            match engine.route(&key, req).await {
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

        Command::ZCard { key } => {
            let req = ShardRequest::ZCard { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        // --- hash commands ---
        Command::HSet { key, fields } => {
            let req = ShardRequest::HSet {
                key: key.clone(),
                fields,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HGet { key, field } => {
            let req = ShardRequest::HGet {
                key: key.clone(),
                field,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HGetAll { key } => {
            let req = ShardRequest::HGetAll { key: key.clone() };
            match engine.route(&key, req).await {
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
            let req = ShardRequest::HDel {
                key: key.clone(),
                fields,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::HDelLen { count, .. }) => Frame::Integer(count as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HExists { key, field } => {
            let req = ShardRequest::HExists {
                key: key.clone(),
                field,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Bool(b)) => Frame::Integer(if b { 1 } else { 0 }),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HLen { key } => {
            let req = ShardRequest::HLen { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HIncrBy { key, field, delta } => {
            let req = ShardRequest::HIncrBy {
                key: key.clone(),
                field,
                delta,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HKeys { key } => {
            let req = ShardRequest::HKeys { key: key.clone() };
            match engine.route(&key, req).await {
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
            let req = ShardRequest::HVals { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Array(vals)) => {
                    Frame::Array(vals.into_iter().map(Frame::Bulk).collect())
                }
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::HMGet { key, fields } => {
            let req = ShardRequest::HMGet {
                key: key.clone(),
                fields,
            };
            match engine.route(&key, req).await {
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

        // --- set commands ---
        Command::SAdd { key, members } => {
            let req = ShardRequest::SAdd {
                key: key.clone(),
                members,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SRem { key, members } => {
            let req = ShardRequest::SRem {
                key: key.clone(),
                members,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SMembers { key } => {
            let req = ShardRequest::SMembers { key: key.clone() };
            match engine.route(&key, req).await {
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
            let req = ShardRequest::SIsMember {
                key: key.clone(),
                member,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Bool(b)) => Frame::Integer(if b { 1 } else { 0 }),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::SCard { key } => {
            let req = ShardRequest::SCard { key: key.clone() };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        // --- cluster commands ---
        // Note: Full cluster support requires integration with ember-cluster crate.
        // For now, CLUSTER KEYSLOT works, and other commands return stub responses.
        Command::ClusterKeySlot { key } => {
            let slot = ember_cluster::key_slot(key.as_bytes());
            Frame::Integer(slot as i64)
        }

        Command::ClusterInfo => {
            // Return minimal info indicating cluster mode is disabled
            let info = "cluster_enabled:0\r\n";
            Frame::Bulk(Bytes::from(info))
        }

        Command::ClusterNodes => {
            // In non-cluster mode, return empty string
            Frame::Bulk(Bytes::from(""))
        }

        Command::ClusterSlots => {
            // In non-cluster mode, return empty array
            Frame::Array(vec![])
        }

        Command::ClusterMyId => {
            // In non-cluster mode, return an error
            Frame::Error("ERR This instance has cluster support disabled".into())
        }

        Command::Asking => {
            // ASKING is a no-op in non-cluster mode, just return OK
            Frame::Simple("OK".into())
        }

        Command::ClusterSetSlotImporting { .. }
        | Command::ClusterSetSlotMigrating { .. }
        | Command::ClusterSetSlotNode { .. }
        | Command::ClusterSetSlotStable { .. }
        | Command::ClusterMeet { .. }
        | Command::ClusterAddSlots { .. }
        | Command::ClusterDelSlots { .. }
        | Command::ClusterForget { .. }
        | Command::ClusterReplicate { .. }
        | Command::ClusterFailover { .. }
        | Command::ClusterCountKeysInSlot { .. }
        | Command::ClusterGetKeysInSlot { .. } => {
            Frame::Error("ERR This instance has cluster support disabled".into())
        }

        Command::Migrate { .. } => {
            Frame::Error("ERR This instance has cluster support disabled".into())
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
                        Frame::Integer(e.id as i64),
                        Frame::Integer(ts as i64),
                        Frame::Integer(e.duration.as_micros() as i64),
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
async fn render_info(
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    section: Option<&str>,
) -> Frame {
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
                };
                for r in &responses {
                    if let ShardResponse::Stats(s) = r {
                        total.key_count += s.key_count;
                        total.used_bytes += s.used_bytes;
                        total.keys_with_expiry += s.keys_with_expiry;
                        total.keys_expired += s.keys_expired;
                        total.keys_evicted += s.keys_evicted;
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
            if let Some(max) = ctx.max_memory {
                out.push_str(&format!("max_memory:{max}\r\n"));
                out.push_str(&format!("max_memory_human:{}\r\n", human_bytes(max)));
            } else {
                out.push_str("max_memory:0\r\n");
                out.push_str("max_memory_human:unlimited\r\n");
            }
            out.push_str("\r\n");
        }
    }

    if want("PERSISTENCE") {
        out.push_str("# Persistence\r\n");
        out.push_str(&format!(
            "aof_enabled:{}\r\n",
            if ctx.aof_enabled { 1 } else { 0 }
        ));
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

    // trim trailing blank line
    if out.ends_with("\r\n\r\n") {
        out.truncate(out.len() - 2);
    }

    Frame::Bulk(Bytes::from(out))
}

/// Formats a byte count as a human-readable string (e.g. "1.23M").
fn human_bytes(bytes: usize) -> String {
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

/// Returns the standard WRONGTYPE error frame.
fn wrongtype_error() -> Frame {
    Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}

/// Returns the standard OOM error frame.
fn oom_error() -> Frame {
    Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
}
