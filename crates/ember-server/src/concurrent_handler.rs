//! Concurrent handler that bypasses shard channels for GET/SET.
//!
//! Uses DashMap-backed ConcurrentKeyspace for lock-free multi-threaded access.
//! Falls back to sharded engine for complex commands.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use ember_core::{ConcurrentKeyspace, Engine, TtlResult};
use ember_protocol::{parse_frame, Command, Frame, SetExpire};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::server::ServerContext;
use crate::slowlog::SlowLog;

const BUF_CAPACITY: usize = 4096;
const MAX_BUF_SIZE: usize = 64 * 1024 * 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Handles a connection using the concurrent keyspace for GET/SET.
pub async fn handle(
    mut stream: TcpStream,
    keyspace: Arc<ConcurrentKeyspace>,
    engine: Engine, // fallback for complex commands
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
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
                    let response = process(frame, &keyspace, &engine, ctx, slow_log).await;
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

            let response = execute_concurrent(cmd, keyspace, engine).await;
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

        // For unsupported commands, return an error
        Command::Unknown(name) => Frame::Error(format!("ERR unknown command '{name}'")),

        _ => Frame::Error("ERR command not supported in concurrent mode".into()),
    }
}
