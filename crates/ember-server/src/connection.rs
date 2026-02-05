//! Per-connection handler.
//!
//! Reads RESP3 frames from a TCP stream, routes them through the
//! sharded engine, and writes responses back. Supports pipelining
//! by processing multiple frames from a single read.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use ember_core::{Engine, KeyspaceStats, ShardRequest, ShardResponse, TtlResult, Value};
use ember_protocol::{parse_frame, Command, Frame, SetExpire};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
                    let response = process(frame, &engine).await;
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
async fn process(frame: Frame, engine: &Engine) -> Frame {
    match Command::from_frame(frame) {
        Ok(cmd) => execute(cmd, engine).await,
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

/// Executes a parsed command and returns the response frame.
///
/// Ping and Echo are handled inline (no shard routing needed).
/// Single-key commands route to the owning shard. Multi-key commands
/// (DEL, EXISTS) fan out across shards and aggregate results.
async fn execute(cmd: Command, engine: &Engine) -> Frame {
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

        Command::Set { key, value, expire } => {
            let duration = expire.map(|e| match e {
                SetExpire::Ex(secs) => Duration::from_secs(secs),
                SetExpire::Px(millis) => Duration::from_millis(millis),
            });
            let req = ShardRequest::Set {
                key: key.clone(),
                value,
                expire: duration,
            };
            match engine.route(&key, req).await {
                Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
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

        // -- multi-key fan-out --
        Command::Del { keys } => {
            multi_key_bool(engine, &keys, |k| ShardRequest::Del { key: k }).await
        }

        Command::Exists { keys } => {
            multi_key_bool(engine, &keys, |k| ShardRequest::Exists { key: k }).await
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
            let section_upper = section.as_deref().map(|s| s.to_ascii_uppercase());
            match section_upper.as_deref() {
                None | Some("KEYSPACE") => match engine.broadcast(|| ShardRequest::Stats).await {
                    Ok(responses) => {
                        let mut total = KeyspaceStats {
                            key_count: 0,
                            used_bytes: 0,
                            keys_with_expiry: 0,
                        };
                        for r in &responses {
                            if let ShardResponse::Stats(stats) = r {
                                total.key_count += stats.key_count;
                                total.used_bytes += stats.used_bytes;
                                total.keys_with_expiry += stats.keys_with_expiry;
                            }
                        }
                        let info = format!(
                            "# Keyspace\r\ndb0:keys={},expires={},used_bytes={}\r\n",
                            total.key_count, total.keys_with_expiry, total.used_bytes
                        );
                        Frame::Bulk(Bytes::from(info))
                    }
                    Err(e) => Frame::Error(format!("ERR {e}")),
                },
                Some(other) => Frame::Error(format!("ERR unsupported INFO section '{other}'")),
            }
        }

        Command::BgSave => match engine.broadcast(|| ShardRequest::Snapshot).await {
            Ok(_) => Frame::Simple("Background saving started".into()),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },

        Command::BgRewriteAof => match engine.broadcast(|| ShardRequest::RewriteAof).await {
            Ok(_) => Frame::Simple("Background append only file rewriting started".into()),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },

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

/// Returns the standard WRONGTYPE error frame.
fn wrongtype_error() -> Frame {
    Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}

/// Returns the standard OOM error frame.
fn oom_error() -> Frame {
    Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
}
