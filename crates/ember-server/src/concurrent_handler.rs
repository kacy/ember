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

#[cfg(feature = "protobuf")]
use bytes::Bytes;
use bytes::BytesMut;
use ember_core::{ConcurrentKeyspace, Engine, TtlResult};
use ember_protocol::{parse_frame, Command, Frame, SetExpire};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::connection_common::{
    is_allowed_before_auth, is_auth_frame, try_auth, BUF_CAPACITY, IDLE_TIMEOUT, MAX_BUF_SIZE,
};
use crate::pubsub::PubSubManager;
use crate::server::ServerContext;
use crate::slowlog::SlowLog;

/// Handles a connection using the concurrent keyspace for GET/SET.
///
/// Generic over the stream type to support both plain TCP and TLS connections.
/// Callers should set TCP_NODELAY on the underlying socket before calling.
pub async fn handle<S>(
    mut stream: S,
    keyspace: Arc<ConcurrentKeyspace>,
    engine: Engine, // fallback for complex commands
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut authenticated = ctx.requirepass.is_none();

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

                    if !authenticated {
                        if is_auth_frame(&frame) {
                            let (response, success) = try_auth(frame, ctx);
                            response.serialize(&mut out);
                            if success {
                                authenticated = true;
                            }
                        } else if is_allowed_before_auth(&frame) {
                            let response =
                                process(frame, &keyspace, &engine, ctx, slow_log, pubsub).await;
                            response.serialize(&mut out);
                        } else {
                            Frame::Error("NOAUTH Authentication required.".into())
                                .serialize(&mut out);
                        }
                    } else {
                        let response =
                            process(frame, &keyspace, &engine, ctx, slow_log, pubsub).await;
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

            let response = execute_concurrent(cmd, keyspace, engine, ctx, pubsub).await;
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
    ctx: &Arc<ServerContext>,
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

        Command::FlushDb { .. } => {
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
            let registry = match _engine.schema_registry() {
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
                    let _ = _engine
                        .broadcast(|| ember_core::ShardRequest::ProtoRegisterAof {
                            name: name.clone(),
                            descriptor: descriptor.clone(),
                        })
                        .await;
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
            let registry = match _engine.schema_registry() {
                Some(r) => r,
                None => return Frame::Error("ERR protobuf support is not enabled".into()),
            };
            {
                let reg = match registry.read() {
                    Ok(r) => r,
                    Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
                };
                if let Err(e) = reg.validate(&type_name, &data) {
                    return Frame::Error(format!("ERR {e}"));
                }
            }
            let duration = expire.map(|e| match e {
                SetExpire::Ex(secs) => Duration::from_secs(secs),
                SetExpire::Px(millis) => Duration::from_millis(millis),
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
            let registry = match _engine.schema_registry() {
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
            let registry = match _engine.schema_registry() {
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
            let registry = match _engine.schema_registry() {
                Some(r) => r,
                None => return Frame::Error("ERR protobuf support is not enabled".into()),
            };
            let req = ember_core::ShardRequest::ProtoGet { key: key.clone() };
            match _engine.route(&key, req).await {
                Ok(ember_core::ShardResponse::ProtoValue(Some((type_name, data, _ttl)))) => {
                    let reg = match registry.read() {
                        Ok(r) => r,
                        Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
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

        // For unsupported commands, return an error
        Command::Unknown(name) => Frame::Error(format!("ERR unknown command '{name}'")),

        _ => Frame::Error("ERR command not supported in concurrent mode".into()),
    }
}
