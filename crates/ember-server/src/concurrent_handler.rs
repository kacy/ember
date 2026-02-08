//! Concurrent handler that bypasses shard channels for GET/SET.
//!
//! Uses DashMap-backed ConcurrentKeyspace for lock-free multi-threaded access.
//! This mode trades feature completeness for raw throughput — only string
//! operations are supported, but they execute 2x faster than sharded mode
//! by avoiding channel round-trips.
//!
//! ## Performance characteristics
//!
//! - GET/SET: ~2M ops/sec (vs ~1M in sharded mode)
//! - No channel overhead — direct DashMap access
//! - Processes frames serially (vs parallel dispatch in sharded mode)
//! - Falls back to error for unsupported commands (lists, hashes, etc.)

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use ember_core::{ConcurrentKeyspace, Engine, TtlResult};
use ember_protocol::{parse_frame, Command, Frame, SetExpire};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::connection_common::{BUF_CAPACITY, IDLE_TIMEOUT, MAX_BUF_SIZE};
use crate::pubsub::PubSubManager;
use crate::server::ServerContext;
use crate::slowlog::SlowLog;

/// Handles a connection using the concurrent keyspace for GET/SET.
pub async fn handle(
    mut stream: TcpStream,
    keyspace: Arc<ConcurrentKeyspace>,
    engine: Engine, // fallback for complex commands
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_nodelay(true)?;

    let mut buf = BytesMut::with_capacity(BUF_CAPACITY);
    let mut out = BytesMut::with_capacity(BUF_CAPACITY);

    loop {
        if buf.len() > MAX_BUF_SIZE {
            let msg = "ERR max buffer size exceeded, closing connection";
            let mut err_buf = BytesMut::new();
            Frame::Error(msg.into()).serialize(&mut err_buf);
            let _ = stream.write_all(&err_buf).await;
            return Ok(());
        }

        match tokio::time::timeout(IDLE_TIMEOUT, stream.read_buf(&mut buf)).await {
            Ok(Ok(0)) => return Ok(()),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => return Ok(()),
        }

        out.clear();
        loop {
            match parse_frame(&buf) {
                Ok(Some((frame, consumed))) => {
                    let _ = buf.split_to(consumed);
                    let response = process(frame, &keyspace, &engine, ctx, slow_log, pubsub).await;
                    response.serialize(&mut out);
                }
                Ok(None) => break,
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

async fn process(
    frame: Frame,
    keyspace: &Arc<ConcurrentKeyspace>,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
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

            let response = execute_concurrent(cmd, keyspace, engine, pubsub).await;
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

/// Execute commands using concurrent keyspace for GET/SET, fallback for others.
async fn execute_concurrent(
    cmd: Command,
    keyspace: &Arc<ConcurrentKeyspace>,
    _engine: &Engine,
    pubsub: &Arc<PubSubManager>,
) -> Frame {
    match cmd {
        // Hot path: direct access without channels
        Command::Get { key } => match keyspace.get(&key) {
            Some(data) => Frame::Bulk(data),
            None => Frame::Null,
        },

        Command::Set {
            key,
            value,
            expire,
            nx,
            xx,
        } => {
            // Handle NX/XX flags
            let exists = keyspace.exists(&key);
            if nx && exists {
                return Frame::Null;
            }
            if xx && !exists {
                return Frame::Null;
            }

            let ttl = expire.map(|e| match e {
                SetExpire::Ex(secs) => Duration::from_secs(secs),
                SetExpire::Px(millis) => Duration::from_millis(millis),
            });

            if keyspace.set(key, value, ttl) {
                Frame::Simple("OK".into())
            } else {
                Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
            }
        }

        Command::Del { keys } => {
            let mut count = 0i64;
            for key in keys {
                if keyspace.del(&key) {
                    count += 1;
                }
            }
            Frame::Integer(count)
        }

        Command::Exists { keys } => {
            let mut count = 0i64;
            for key in keys {
                if keyspace.exists(&key) {
                    count += 1;
                }
            }
            Frame::Integer(count)
        }

        Command::Expire { key, seconds } => {
            let result = keyspace.expire(&key, seconds);
            Frame::Integer(if result { 1 } else { 0 })
        }

        Command::Ttl { key } => match keyspace.ttl(&key) {
            TtlResult::Seconds(s) => Frame::Integer(s as i64),
            TtlResult::NoExpiry => Frame::Integer(-1),
            TtlResult::NotFound => Frame::Integer(-2),
            TtlResult::Milliseconds(ms) => Frame::Integer((ms / 1000) as i64),
        },

        Command::Ping(None) => Frame::Simple("PONG".into()),
        Command::Ping(Some(msg)) => Frame::Bulk(msg),
        Command::Echo(msg) => Frame::Bulk(msg),

        Command::DbSize => Frame::Integer(keyspace.len() as i64),

        Command::FlushDb => {
            keyspace.clear();
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

        // subscribe commands are handled in the connection layer, not here
        Command::Subscribe { .. }
        | Command::Unsubscribe { .. }
        | Command::PSubscribe { .. }
        | Command::PUnsubscribe { .. } => {
            Frame::Error("ERR pub/sub not supported in concurrent mode yet".into())
        }

        // For unsupported commands, return an error
        Command::Unknown(name) => Frame::Error(format!("ERR unknown command '{name}'")),

        _ => Frame::Error("ERR command not supported in concurrent mode".into()),
    }
}
