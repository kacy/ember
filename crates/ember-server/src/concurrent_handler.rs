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

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use bytes::BytesMut;
use ember_core::{ConcurrentKeyspace, Engine, TtlResult};
use ember_protocol::{parse_frame, Command, Frame, SetExpire};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::connection_common::{
    frame_to_monitor_args, get_rss_bytes, human_bytes, initial_acl_user, is_allowed_before_auth,
    is_auth_frame, is_monitor_frame, try_auth, validate_command_sizes, MonitorEvent,
    TransactionState,
};
use crate::metrics::on_auth_failure;
use crate::pubsub::PubSubManager;
use crate::server::{format_client_list, ServerContext};
use crate::slowlog::SlowLog;

/// Handles a connection using the concurrent keyspace for GET/SET.
///
/// Generic over the stream type to support both plain TCP and TLS connections.
/// Callers should set TCP_NODELAY on the underlying socket before calling.
#[allow(clippy::too_many_arguments)]
pub async fn handle<S>(
    mut stream: S,
    peer_addr: SocketAddr,
    keyspace: Arc<ConcurrentKeyspace>,
    engine: Engine, // fallback for complex commands
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
    client_id: u64,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut acl_user, mut current_username) = initial_acl_user(ctx);
    let mut authenticated = acl_user.is_some();
    let mut auth_failures: u32 = 0;
    let mut tx_state = TransactionState::None;

    let mut buf = BytesMut::with_capacity(ctx.limits.buf_capacity);
    let mut out = BytesMut::with_capacity(ctx.limits.buf_capacity);

    loop {
        if buf.len() > ctx.limits.max_buf_size {
            let msg = "ERR max buffer size exceeded, closing connection";
            let mut err_buf = BytesMut::new();
            Frame::Error(msg.into()).serialize(&mut err_buf);
            let _ = stream.write_all(&err_buf).await;
            return Ok(());
        }

        match tokio::time::timeout(ctx.limits.idle_timeout, stream.read_buf(&mut buf)).await {
            Ok(Ok(0)) => return Ok(()),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => return Ok(()),
        }

        // parse frames using split_to — O(1) pointer adjustment, no
        // memcpy for unconsumed data. bulk strings are copied during
        // parsing rather than zero-copy sliced from a frozen buffer.
        out.clear();
        let mut pipeline_count: usize = 0;
        loop {
            if pipeline_count >= ctx.limits.max_pipeline_depth {
                break; // process this batch, remaining data stays in buf
            }
            if buf.is_empty() {
                break;
            }
            match parse_frame(&buf) {
                Ok(Some((frame, consumed))) => {
                    let _ = buf.split_to(consumed);
                    pipeline_count += 1;

                    if !authenticated {
                        if is_auth_frame(&frame) {
                            let result = try_auth(frame, ctx);
                            result.response.serialize(&mut out);
                            if result.success() {
                                authenticated = true;
                                acl_user = result.user;
                                current_username = result.username;
                            } else {
                                auth_failures = auth_failures.saturating_add(1);
                                if auth_failures >= ctx.limits.max_auth_failures {
                                    Frame::Error(
                                        "ERR too many AUTH failures, closing connection".into(),
                                    )
                                    .serialize(&mut out);
                                    let _ = stream.write_all(&out).await;
                                    return Ok(());
                                }
                            }
                        } else if is_allowed_before_auth(&frame) {
                            let response = process(
                                frame,
                                &keyspace,
                                &engine,
                                ctx,
                                slow_log,
                                pubsub,
                                client_id,
                                &acl_user,
                                &current_username,
                            )
                            .await;
                            response.serialize(&mut out);
                        } else {
                            on_auth_failure("noauth");
                            Frame::Error("NOAUTH Authentication required.".into())
                                .serialize(&mut out);
                        }
                    } else if is_monitor_frame(&frame) {
                        // enter monitor mode
                        Frame::Simple("OK".into()).serialize(&mut out);
                        stream.write_all(&out).await?;
                        out.clear();
                        handle_monitor_mode(&mut stream, &mut buf, ctx, peer_addr).await?;
                        return Ok(());
                    } else {
                        // broadcast to MONITOR subscribers
                        if ctx.monitor_tx.receiver_count() > 0 {
                            let args = frame_to_monitor_args(&frame);
                            if !args.is_empty() {
                                let ts = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs_f64();
                                let _ = ctx.monitor_tx.send(MonitorEvent {
                                    timestamp: ts,
                                    client_addr: peer_addr.to_string(),
                                    args,
                                });
                            }
                        }

                        let response = handle_frame_with_tx(
                            frame,
                            &mut tx_state,
                            &keyspace,
                            &engine,
                            ctx,
                            slow_log,
                            pubsub,
                            client_id,
                            &acl_user,
                            &current_username,
                        )
                        .await;
                        response.serialize(&mut out);
                    }
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
        // unconsumed bytes remain in buf for the next read — no copy needed

        if !out.is_empty() {
            stream.write_all(&out).await?;
        }
    }
}

/// Monitor mode for the concurrent handler. Identical to the sharded
/// variant — subscribes to the broadcast channel and streams events.
async fn handle_monitor_mode<S>(
    stream: &mut S,
    buf: &mut BytesMut,
    ctx: &Arc<ServerContext>,
    peer_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::connection_common::format_monitor_event;
    use tokio::sync::broadcast;

    let mut rx = ctx.monitor_tx.subscribe();
    let mut out = BytesMut::with_capacity(4096);
    let _ = peer_addr;

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(event) => {
                        let line = format_monitor_event(&event);
                        Frame::Simple(line).serialize(&mut out);
                        stream.write_all(&out).await?;
                        out.clear();
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        let msg = format!("monitor: skipped {n} events (slow consumer)");
                        Frame::Simple(msg).serialize(&mut out);
                        stream.write_all(&out).await?;
                        out.clear();
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Ok(());
                    }
                }
            }
            result = stream.read_buf(buf) => {
                match result {
                    Ok(0) | Err(_) => return Ok(()),
                    Ok(_) => { buf.clear(); }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process(
    frame: Frame,
    keyspace: &Arc<ConcurrentKeyspace>,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
    client_id: u64,
    acl_user: &Option<Arc<crate::acl::AclUser>>,
    current_username: &str,
) -> Frame {
    match Command::from_frame(frame) {
        Ok(cmd) => {
            // reject oversized keys/values before any further processing
            if let Some(err) = validate_command_sizes(
                &cmd,
                ctx.limits.max_key_len,
                ctx.limits.max_value_len,
                ctx.limits.max_command_memory,
            ) {
                return err;
            }

            // ACL permission check
            if let Some(ref user) = acl_user {
                if !user.allcommands || !user.allkeys {
                    if let Some(err) = crate::acl::check_permission(
                        user,
                        &cmd,
                        cmd.command_name(),
                        cmd.acl_categories(),
                    ) {
                        return err;
                    }
                }
            }

            // intercept AclWhoAmI — needs per-connection username
            if matches!(cmd, Command::AclWhoAmI) {
                return Frame::Bulk(Bytes::from(current_username.to_owned()));
            }

            let cmd_name = cmd.command_name();
            let needs_timing = ctx.metrics_enabled || slow_log.is_enabled();
            let start = if needs_timing {
                Some(Instant::now())
            } else {
                None
            };

            let response =
                execute_concurrent(cmd, keyspace, engine, ctx, slow_log, pubsub, client_id).await;
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

/// Handles a single frame with transaction awareness (concurrent mode).
#[allow(clippy::too_many_arguments)]
async fn handle_frame_with_tx(
    frame: Frame,
    tx_state: &mut TransactionState,
    keyspace: &Arc<ConcurrentKeyspace>,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
    client_id: u64,
    acl_user: &Option<Arc<crate::acl::AclUser>>,
    current_username: &str,
) -> Frame {
    let cmd_name = peek_command_name(&frame);

    match tx_state {
        TransactionState::None => {
            if cmd_name.as_deref() == Some("MULTI") {
                match Command::from_frame(frame) {
                    Ok(Command::Multi) => {
                        *tx_state = TransactionState::Queuing {
                            queue: Vec::new(),
                            error: false,
                        };
                        Frame::Simple("OK".into())
                    }
                    Ok(_) => unreachable!(),
                    Err(e) => Frame::Error(format!("ERR {e}")),
                }
            } else if cmd_name.as_deref() == Some("EXEC") {
                Frame::Error("ERR EXEC without MULTI".into())
            } else if cmd_name.as_deref() == Some("DISCARD") {
                Frame::Error("ERR DISCARD without MULTI".into())
            } else if cmd_name.as_deref() == Some("WATCH") {
                Frame::Error("ERR WATCH is not supported in concurrent mode".into())
            } else if cmd_name.as_deref() == Some("UNWATCH") {
                Frame::Simple("OK".into())
            } else {
                process(
                    frame,
                    keyspace,
                    engine,
                    ctx,
                    slow_log,
                    pubsub,
                    client_id,
                    acl_user,
                    current_username,
                )
                .await
            }
        }
        TransactionState::Queuing { queue, error } => match cmd_name.as_deref() {
            Some("MULTI") => Frame::Error("ERR MULTI calls can not be nested".into()),
            Some("WATCH") => Frame::Error("ERR WATCH inside MULTI is not allowed".into()),
            Some("EXEC") => {
                if *error {
                    let q = std::mem::take(queue);
                    *tx_state = TransactionState::None;
                    drop(q);
                    Frame::Error(
                        "EXECABORT Transaction discarded because of previous errors.".into(),
                    )
                } else {
                    let q = std::mem::take(queue);
                    *tx_state = TransactionState::None;
                    let mut results = Vec::with_capacity(q.len());
                    for queued_frame in q {
                        let response = process(
                            queued_frame,
                            keyspace,
                            engine,
                            ctx,
                            slow_log,
                            pubsub,
                            client_id,
                            acl_user,
                            current_username,
                        )
                        .await;
                        results.push(response);
                    }
                    Frame::Array(results)
                }
            }
            Some("DISCARD") => {
                let q = std::mem::take(queue);
                *tx_state = TransactionState::None;
                drop(q);
                Frame::Simple("OK".into())
            }
            _ => match Command::from_frame(frame.clone()) {
                Ok(cmd) => {
                    if matches!(cmd, Command::Auth { .. } | Command::Quit) {
                        process(
                            frame,
                            keyspace,
                            engine,
                            ctx,
                            slow_log,
                            pubsub,
                            client_id,
                            acl_user,
                            current_username,
                        )
                        .await
                    } else {
                        queue.push(frame);
                        Frame::Simple("QUEUED".into())
                    }
                }
                Err(e) => {
                    *error = true;
                    Frame::Error(format!("ERR {e}"))
                }
            },
        },
    }
}

/// Peeks at the command name from a raw frame without consuming it.
fn peek_command_name(frame: &Frame) -> Option<String> {
    if let Frame::Array(parts) = frame {
        if let Some(Frame::Bulk(name)) = parts.first() {
            return String::from_utf8(name.to_vec())
                .ok()
                .map(|s| s.to_ascii_uppercase());
        }
    }
    None
}

/// Execute commands using concurrent keyspace for GET/SET, fallback for others.
async fn execute_concurrent(
    cmd: Command,
    keyspace: &Arc<ConcurrentKeyspace>,
    _engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
    client_id: u64,
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

            let ttl = expire.map(|e| {
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

            if keyspace.set(key, value, ttl) {
                Frame::Simple("OK".into())
            } else {
                Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
            }
        }

        Command::Del { keys } | Command::Unlink { keys } => {
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

        Command::RandomKey => {
            // concurrent mode only has strings — pick a random non-expired key
            match keyspace.random_key() {
                Some(k) => Frame::Bulk(Bytes::from(k)),
                None => Frame::Null,
            }
        }

        Command::Touch { keys } => {
            // concurrent mode doesn't track last_access; just count existing keys
            let mut count = 0i64;
            for key in keys {
                if keyspace.exists(&key) {
                    count += 1;
                }
            }
            Frame::Integer(count)
        }

        Command::Sort { .. } => {
            // concurrent mode only supports strings — SORT requires list/set/zset
            Frame::Error("ERR SORT is not supported in concurrent mode".into())
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

        Command::Incr { key } => match keyspace.incr(&key) {
            Ok(val) => Frame::Integer(val),
            Err(e) => Frame::Error(e.to_string()),
        },

        Command::Decr { key } => match keyspace.decr(&key) {
            Ok(val) => Frame::Integer(val),
            Err(e) => Frame::Error(e.to_string()),
        },

        Command::IncrBy { key, delta } => match keyspace.incr_by(&key, delta) {
            Ok(val) => Frame::Integer(val),
            Err(e) => Frame::Error(e.to_string()),
        },

        Command::DecrBy { key, delta } => match keyspace.incr_by(&key, -delta) {
            Ok(val) => Frame::Integer(val),
            Err(e) => Frame::Error(e.to_string()),
        },

        Command::IncrByFloat { key, delta } => match keyspace.incr_by_float(&key, delta) {
            Ok(val) => Frame::Bulk(Bytes::from(val.to_string())),
            Err(e) => Frame::Error(e.to_string()),
        },

        Command::Append { key, value } => {
            let new_len = keyspace.append(&key, &value);
            Frame::Integer(new_len as i64)
        }

        Command::Strlen { key } => Frame::Integer(keyspace.strlen(&key) as i64),

        Command::Persist { key } => Frame::Integer(if keyspace.persist(&key) { 1 } else { 0 }),

        Command::Pexpire { key, milliseconds } => {
            Frame::Integer(if keyspace.pexpire(&key, milliseconds) {
                1
            } else {
                0
            })
        }

        Command::Pttl { key } => match keyspace.pttl(&key) {
            TtlResult::Milliseconds(ms) => Frame::Integer(ms as i64),
            TtlResult::NoExpiry => Frame::Integer(-1),
            TtlResult::NotFound => Frame::Integer(-2),
            TtlResult::Seconds(s) => {
                // convert seconds → milliseconds, capping at i64::MAX to
                // avoid overflow for pathologically large TTL values
                let ms = s.saturating_mul(1000).min(i64::MAX as u64);
                Frame::Integer(ms as i64)
            }
        },

        Command::Ping(None) => Frame::Simple("PONG".into()),
        Command::Ping(Some(msg)) => Frame::Bulk(msg),
        Command::Echo(msg) => Frame::Bulk(msg),

        // -- client commands (connection-scoped, no keyspace access) --
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

        Command::DbSize => Frame::Integer(keyspace.len() as i64),

        Command::FlushDb { .. } => {
            keyspace.clear();
            Frame::Simple("OK".into())
        }

        Command::MGet { keys } => {
            let mut frames = Vec::with_capacity(keys.len());
            for key in keys {
                match keyspace.get(&key) {
                    Some(data) => frames.push(Frame::Bulk(data)),
                    None => frames.push(Frame::Null),
                }
            }
            Frame::Array(frames)
        }

        Command::MSet { pairs } => {
            for (key, value) in pairs {
                keyspace.set(key, value, None);
            }
            Frame::Simple("OK".into())
        }

        Command::Type { key } => {
            if keyspace.exists(&key) {
                Frame::Simple("string".into())
            } else {
                Frame::Simple("none".into())
            }
        }

        Command::Keys { pattern } => {
            let matched = keyspace.keys(&pattern);
            Frame::Array(
                matched
                    .into_iter()
                    .map(|k| Frame::Bulk(Bytes::from(k)))
                    .collect(),
            )
        }

        Command::Scan {
            cursor,
            pattern,
            count,
        } => {
            let (next_cursor, keys) =
                keyspace.scan_keys(cursor, count.unwrap_or(10), pattern.as_deref());
            Frame::Array(vec![
                Frame::Bulk(Bytes::from(next_cursor.to_string())),
                Frame::Array(
                    keys.into_iter()
                        .map(|k| Frame::Bulk(Bytes::from(k)))
                        .collect(),
                ),
            ])
        }

        Command::Rename { key, newkey } => match keyspace.rename(&key, &newkey) {
            Ok(()) => Frame::Simple("OK".into()),
            Err(msg) => Frame::Error(msg.into()),
        },

        Command::Copy {
            source,
            destination,
            replace,
        } => {
            if let Some(data) = keyspace.get(&source) {
                if !replace && keyspace.exists(&destination) {
                    Frame::Integer(0)
                } else {
                    keyspace.set(destination, data, None);
                    Frame::Integer(1)
                }
            } else {
                Frame::Error("ERR no such key".into())
            }
        }

        Command::ObjectEncoding { key } => {
            if keyspace.exists(&key) {
                // concurrent mode only stores strings
                if let Some(data) = keyspace.get(&key) {
                    if let Ok(s) = std::str::from_utf8(&data) {
                        if s.parse::<i64>().is_ok() {
                            return Frame::Bulk(Bytes::from("int"));
                        }
                    }
                    if data.len() <= 24 {
                        Frame::Bulk(Bytes::from("embstr"))
                    } else {
                        Frame::Bulk(Bytes::from("raw"))
                    }
                } else {
                    Frame::Null
                }
            } else {
                Frame::Null
            }
        }

        Command::ObjectRefcount { key } => {
            if keyspace.exists(&key) {
                Frame::Integer(1)
            } else {
                Frame::Null
            }
        }

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

        Command::LastSave => Frame::Integer(0),

        Command::Role => Frame::Array(vec![
            Frame::Bulk(Bytes::from("master")),
            Frame::Integer(0),
            Frame::Array(vec![]),
        ]),

        Command::Info { section } => render_concurrent_info(keyspace, ctx, section.as_deref()),

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
                let key = param.to_ascii_lowercase();
                if key == "slowlog-log-slower-than" {
                    if let Ok(us) = value.parse::<i64>() {
                        slow_log.update_threshold(us);
                    }
                } else if key == "slowlog-max-len" {
                    if let Ok(len) = value.parse::<usize>() {
                        slow_log.update_max_len(len);
                    }
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

        // AUTH on an already-authenticated connection (re-auth)
        Command::Auth { username, password } => match &ctx.requirepass {
            None => Frame::Error(
                "ERR Client sent AUTH, but no password is set. \
                 Did you mean ACL SETUSER with >password?"
                    .into(),
            ),
            Some(expected) => {
                if let Some(ref user) = username {
                    if user != "default" {
                        return Frame::Error(
                            "WRONGPASS invalid username-password pair \
                             or user is disabled."
                                .into(),
                        );
                    }
                }
                if bool::from(password.as_bytes().ct_eq(expected.as_bytes())) {
                    Frame::Simple("OK".into())
                } else {
                    Frame::Error(
                        "WRONGPASS invalid username-password pair \
                         or user is disabled."
                            .into(),
                    )
                }
            }
        },

        // -- protobuf commands --
        #[cfg(feature = "protobuf")]
        Command::ProtoRegister { name, descriptor } => {
            let Some(registry) = _engine.schema_registry() else {
                return Frame::Error("ERR protobuf support is not enabled".into());
            };
            let result = {
                let Ok(mut reg) = registry.write() else {
                    return Frame::Error("ERR schema registry lock poisoned".into());
                };
                reg.register(name.clone(), descriptor.clone())
            };
            match result {
                Ok(types) => {
                    if let Err(e) = _engine
                        .broadcast(|| ember_core::ShardRequest::ProtoRegisterAof {
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
            let Some(registry) = _engine.schema_registry() else {
                return Frame::Error("ERR protobuf support is not enabled".into());
            };
            {
                let Ok(reg) = registry.read() else {
                    return Frame::Error("ERR schema registry lock poisoned".into());
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
            let req = ember_core::ShardRequest::ProtoSet {
                key: key.clone(),
                type_name,
                data,
                expire: duration,
                nx,
                xx,
            };
            match _engine.route(&key, req).await {
                Ok(ember_core::ShardResponse::Ok) => Frame::Simple("OK".into()),
                Ok(ember_core::ShardResponse::Value(None)) => Frame::Null,
                Ok(ember_core::ShardResponse::OutOfMemory) => {
                    Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
                }
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoGet { key } => {
            if _engine.schema_registry().is_none() {
                return Frame::Error("ERR protobuf support is not enabled".into());
            }
            let req = ember_core::ShardRequest::ProtoGet { key: key.clone() };
            match _engine.route(&key, req).await {
                Ok(ember_core::ShardResponse::ProtoValue(Some((type_name, data, _ttl)))) => {
                    Frame::Array(vec![Frame::Bulk(Bytes::from(type_name)), Frame::Bulk(data)])
                }
                Ok(ember_core::ShardResponse::ProtoValue(None)) => Frame::Null,
                Ok(ember_core::ShardResponse::WrongType) => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoType { key } => {
            if _engine.schema_registry().is_none() {
                return Frame::Error("ERR protobuf support is not enabled".into());
            }
            let req = ember_core::ShardRequest::ProtoType { key: key.clone() };
            match _engine.route(&key, req).await {
                Ok(ember_core::ShardResponse::ProtoTypeName(Some(name))) => {
                    Frame::Bulk(Bytes::from(name))
                }
                Ok(ember_core::ShardResponse::ProtoTypeName(None)) => Frame::Null,
                Ok(ember_core::ShardResponse::WrongType) => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoSchemas => {
            let Some(registry) = _engine.schema_registry() else {
                return Frame::Error("ERR protobuf support is not enabled".into());
            };
            let Ok(reg) = registry.read() else {
                return Frame::Error("ERR schema registry lock poisoned".into());
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
            let Some(registry) = _engine.schema_registry() else {
                return Frame::Error("ERR protobuf support is not enabled".into());
            };
            let Ok(reg) = registry.read() else {
                return Frame::Error("ERR schema registry lock poisoned".into());
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
            let Some(registry) = _engine.schema_registry() else {
                return Frame::Error("ERR protobuf support is not enabled".into());
            };
            let req = ember_core::ShardRequest::ProtoGet { key: key.clone() };
            match _engine.route(&key, req).await {
                Ok(ember_core::ShardResponse::ProtoValue(Some((type_name, data, _ttl)))) => {
                    let Ok(reg) = registry.read() else {
                        return Frame::Error("ERR schema registry lock poisoned".into());
                    };
                    match reg.get_field(&type_name, &data, &field_path) {
                        Ok(frame) => frame,
                        Err(e) => Frame::Error(format!("ERR {e}")),
                    }
                }
                Ok(ember_core::ShardResponse::ProtoValue(None)) => Frame::Null,
                Ok(ember_core::ShardResponse::WrongType) => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
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
            if _engine.schema_registry().is_none() {
                return Frame::Error("ERR protobuf support is not enabled".into());
            }
            let req = ember_core::ShardRequest::ProtoSetField {
                key: key.clone(),
                field_path,
                value,
            };
            match _engine.route(&key, req).await {
                Ok(ember_core::ShardResponse::ProtoFieldUpdated { .. }) => {
                    Frame::Simple("OK".into())
                }
                Ok(ember_core::ShardResponse::Value(None)) => Frame::Null,
                Ok(ember_core::ShardResponse::WrongType) => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
                Ok(ember_core::ShardResponse::OutOfMemory) => {
                    Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
                }
                Ok(ember_core::ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        #[cfg(feature = "protobuf")]
        Command::ProtoDelField { key, field_path } => {
            if _engine.schema_registry().is_none() {
                return Frame::Error("ERR protobuf support is not enabled".into());
            }
            let req = ember_core::ShardRequest::ProtoDelField {
                key: key.clone(),
                field_path,
            };
            match _engine.route(&key, req).await {
                Ok(ember_core::ShardResponse::ProtoFieldUpdated { .. }) => Frame::Integer(1),
                Ok(ember_core::ShardResponse::Value(None)) => Frame::Null,
                Ok(ember_core::ShardResponse::WrongType) => Frame::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
                Ok(ember_core::ShardResponse::OutOfMemory) => {
                    Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
                }
                Ok(ember_core::ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

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

        Command::Quit => Frame::Simple("OK".into()),

        // MONITOR is handled at the frame level
        Command::Monitor => Frame::Simple("OK".into()),

        // -- ACL commands --
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

        // For unsupported commands, return an error
        Command::Unknown(name) => Frame::Error(format!("ERR unknown command '{name}'")),

        _ => Frame::Error("ERR command not supported in concurrent mode".into()),
    }
}

/// Renders INFO output for the concurrent keyspace.
fn render_concurrent_info(
    keyspace: &ConcurrentKeyspace,
    ctx: &ServerContext,
    section: Option<&str>,
) -> Frame {
    let section_upper = section.map(|s| s.to_ascii_uppercase());
    let want_all = section_upper.is_none();
    let want = |name: &str| want_all || section_upper.as_deref() == Some(name);

    let mut out = String::with_capacity(512);

    if want("SERVER") {
        let uptime = ctx.start_time.elapsed().as_secs();
        out.push_str("# Server\r\n");
        out.push_str(&format!("ember_version:{}\r\n", ctx.version));
        out.push_str(&format!("process_id:{}\r\n", std::process::id()));
        out.push_str(&format!("uptime_in_seconds:{uptime}\r\n"));
        out.push_str("mode:concurrent\r\n");
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
        let used = keyspace.memory_used();
        out.push_str("# Memory\r\n");
        out.push_str(&format!("used_memory:{used}\r\n"));
        out.push_str(&format!("used_memory_human:{}\r\n", human_bytes(used)));
        if let Some(rss) = get_rss_bytes() {
            out.push_str(&format!("used_memory_rss:{rss}\r\n"));
            out.push_str(&format!("used_memory_rss_human:{}\r\n", human_bytes(rss)));
        }
        if let Some(max) = ctx.max_memory {
            out.push_str(&format!("max_memory:{max}\r\n"));
            out.push_str(&format!("max_memory_human:{}\r\n", human_bytes(max)));
        } else {
            out.push_str("max_memory:0\r\n");
            out.push_str("max_memory_human:unlimited\r\n");
        }
        out.push_str("\r\n");
    }

    if want("PERSISTENCE") {
        out.push_str("# Persistence\r\n");
        out.push_str("aof_enabled:0\r\n");
        out.push_str("\r\n");
    }

    if want("STATS") {
        let total_cmds = ctx.commands_processed.load(Ordering::Relaxed);
        let total_conns = ctx.connections_accepted.load(Ordering::Relaxed);
        out.push_str("# Stats\r\n");
        out.push_str(&format!("total_connections_received:{total_conns}\r\n"));
        out.push_str(&format!("total_commands_processed:{total_cmds}\r\n"));
        out.push_str("expired_keys:0\r\n");
        out.push_str("evicted_keys:0\r\n");
        out.push_str("\r\n");
    }

    if want("REPLICATION") {
        out.push_str("# Replication\r\n");
        out.push_str("role:primary\r\n");
        out.push_str("connected_replicas:0\r\n");
        out.push_str("\r\n");
    }

    if want("KEYSPACE") {
        let key_count = keyspace.len();
        out.push_str("# Keyspace\r\n");
        if key_count > 0 {
            out.push_str(&format!(
                "db0:keys={},used_bytes={}\r\n",
                key_count,
                keyspace.memory_used()
            ));
        }
        out.push_str("\r\n");
    }

    // trim trailing blank line
    if out.ends_with("\r\n\r\n") {
        out.truncate(out.len() - 2);
    }

    Frame::Bulk(Bytes::from(out))
}
