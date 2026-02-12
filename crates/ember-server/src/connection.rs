//! Per-connection handler for sharded engine mode.
//!
//! Reads RESP3 frames from a TCP/TLS stream, routes them through the
//! sharded engine, and writes responses back. Supports pipelining
//! via a two-phase dispatch-collect pattern: all commands in a batch
//! are dispatched to shards without waiting, then responses are
//! collected in order.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use ember_core::{Engine, KeyspaceStats, ShardRequest, ShardResponse, TtlResult, Value};
use ember_protocol::{parse_frame, Command, Frame, SetExpire};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{broadcast, oneshot};

use crate::connection_common::{
    is_allowed_before_auth, is_auth_frame, try_auth, BUF_CAPACITY, IDLE_TIMEOUT, MAX_AUTH_FAILURES,
    MAX_BUF_SIZE, MAX_PATTERN_LEN, MAX_SUBSCRIPTIONS_PER_CONN,
};
use crate::pubsub::{PubMessage, PubSubManager};
use crate::server::ServerContext;
use crate::slowlog::SlowLog;

/// A command that has been dispatched to a shard but not yet resolved.
///
/// Single-key commands are dispatched non-blocking: the request is sent
/// to the shard's mpsc channel and we hold the oneshot receiver. Complex
/// commands (broadcast, multi-key, cluster) are executed immediately.
enum PendingResponse {
    /// Response is already available (non-shard commands, errors, or
    /// commands that needed special handling like broadcast/multi-key).
    Immediate(Frame),
    /// Waiting on a shard's oneshot reply. The `ResponseTag` tells the
    /// collect phase how to convert ShardResponse → Frame.
    Pending {
        rx: oneshot::Receiver<ShardResponse>,
        tag: ResponseTag,
        /// When the command was dispatched, for latency tracking.
        start: Option<Instant>,
        /// Command name for metrics/slowlog.
        cmd_name: &'static str,
    },
}

/// Lightweight tag that guides ShardResponse → Frame conversion in the
/// collect phase. Avoids keeping the full Command alive while waiting.
#[derive(Debug, Clone, Copy)]
enum ResponseTag {
    /// GET: Value(Some(String)) → Bulk, Value(None) → Null, WrongType → error
    Get,
    /// SET: Ok → Simple("OK"), Value(None) → Null, OutOfMemory → error
    Set,
    /// EXPIRE/PERSIST/PEXPIRE: Bool → Integer(0/1)
    BoolToInt,
    /// TTL: Ttl(Seconds) → Integer, NoExpiry → -1, NotFound → -2
    Ttl,
    /// PTTL: Ttl(Milliseconds) → Integer, NoExpiry → -1, NotFound → -2
    Pttl,
    /// INCR/DECR/INCRBY/DECRBY: Integer → Integer, with WrongType/OOM/Err
    IntResult,
    /// APPEND/STRLEN/LPUSH/RPUSH/LLEN/HLEN/SADD/SREM/SCARD/ZCARD: Len → Integer
    LenResult,
    /// INCRBYFLOAT: BulkString → Bulk
    FloatResult,
    /// LPOP/RPOP: Value(Some(String)) → Bulk, Value(None) → Null
    PopResult,
    /// LRANGE: Array → Array of Bulk
    ArrayResult,
    /// TYPE: TypeName → Simple
    TypeResult,
    /// ZADD: ZAddLen → Integer
    ZAddResult,
    /// ZREM: ZRemLen → Integer
    ZRemResult,
    /// ZSCORE: Score(Some) → Bulk, Score(None) → Null
    ZScoreResult,
    /// ZRANK: Rank(Some) → Integer, Rank(None) → Null
    ZRankResult,
    /// ZRANGE: ScoredArray → Array (with_scores handled by tag variant)
    ZRangeResult { with_scores: bool },
    /// HSET: Len → Integer (with OOM)
    HSetResult,
    /// HGET: Value(Some(String)) → Bulk, Value(None) → Null
    HGetResult,
    /// HGETALL: HashFields → Array of alternating field/value
    HGetAllResult,
    /// HDEL: HDelLen → Integer
    HDelResult,
    /// HEXISTS: Bool → Integer(0/1) (with WrongType)
    HExistsResult,
    /// HINCRBY: Integer → Integer (with WrongType/OOM/Err prefixed)
    HIncrByResult,
    /// HKEYS/SMEMBERS: StringArray → Array of Bulk
    StringArrayResult,
    /// HVALS: Array → Array of Bulk
    HValsResult,
    /// HMGET: OptionalArray → Array of Bulk/Null
    HMGetResult,
    /// SISMEMBER: Bool → Integer(0/1) (with WrongType)
    SIsMemberResult,
    /// RENAME: Ok → Simple("OK"), Err → Error
    RenameResult,
    /// Len result with OOM possible (LPUSH/RPUSH/SADD)
    LenResultOom,
    /// Vector VADD result
    #[cfg(feature = "vector")]
    VAddResult,
    /// Vector VSIM result
    #[cfg(feature = "vector")]
    VSimResult { with_scores: bool },
    /// Vector VREM result
    #[cfg(feature = "vector")]
    VRemResult,
    /// Vector VGET result
    #[cfg(feature = "vector")]
    VGetResult,
    /// Vector VCARD/VDIM result
    #[cfg(feature = "vector")]
    VIntResult,
    /// Vector VINFO result
    #[cfg(feature = "vector")]
    VInfoResult,
    /// PROTO.SET: Ok → Simple("OK"), Value(None) → Null
    #[cfg(feature = "protobuf")]
    ProtoSetResult,
    /// PROTO.GET result
    #[cfg(feature = "protobuf")]
    ProtoGetResult,
    /// PROTO.TYPE result
    #[cfg(feature = "protobuf")]
    ProtoTypeResult,
    /// PROTO.SETFIELD result
    #[cfg(feature = "protobuf")]
    ProtoSetFieldResult,
    /// PROTO.DELFIELD result
    #[cfg(feature = "protobuf")]
    ProtoDelFieldResult,
}

/// Drives a single client connection to completion.
///
/// Reads data into a buffer, parses complete frames, dispatches commands
/// through the engine, and writes serialized responses back. The loop
/// exits when the client disconnects or a protocol error occurs.
///
/// Generic over the stream type to support both plain TCP and TLS connections.
/// Callers should set TCP_NODELAY on the underlying socket before calling.
pub async fn handle<S>(
    mut stream: S,
    engine: Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // per-connection auth state. auto-authenticated when no password is set.
    let mut authenticated = ctx.requirepass.is_none();
    let mut auth_failures: u32 = 0;

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

        // parse all complete frames from the buffer first, then dispatch
        // them concurrently to shards. this allows pipelined commands to
        // be processed in parallel rather than serially.
        out.clear();
        let mut frames = Vec::new();
        loop {
            match parse_frame(&buf) {
                Ok(Some((frame, consumed))) => {
                    let _ = buf.split_to(consumed);
                    frames.push(frame);
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

        // when not yet authenticated, process frames serially so that an
        // AUTH command in a pipeline takes effect for subsequent frames
        if !authenticated {
            for frame in frames {
                if is_auth_frame(&frame) {
                    let (response, success) = try_auth(frame, ctx);
                    response.serialize(&mut out);
                    if success {
                        authenticated = true;
                    } else {
                        auth_failures += 1;
                        if auth_failures >= MAX_AUTH_FAILURES {
                            Frame::Error("ERR too many AUTH failures, closing connection".into())
                                .serialize(&mut out);
                            let _ = stream.write_all(&out).await;
                            return Ok(());
                        }
                    }
                } else if is_allowed_before_auth(&frame) {
                    let response = process(frame, &engine, ctx, slow_log, pubsub).await;
                    response.serialize(&mut out);
                } else {
                    Frame::Error("NOAUTH Authentication required.".into()).serialize(&mut out);
                }
            }
            if !out.is_empty() {
                stream.write_all(&out).await?;
            }
            continue;
        }

        // check if any frame is a subscribe command — if so, we need
        // to enter subscriber mode which changes the connection loop
        let enter_sub = frames.iter().any(is_subscribe_frame);

        if enter_sub {
            // process any non-subscribe commands that came before
            let mut sub_frames = Vec::new();
            for frame in frames {
                if is_subscribe_frame(&frame) {
                    sub_frames.push(frame);
                } else {
                    let response = process(frame, &engine, ctx, slow_log, pubsub).await;
                    response.serialize(&mut out);
                }
            }
            if !out.is_empty() {
                stream.write_all(&out).await?;
                out.clear();
            }

            // enter subscriber mode — this blocks until all subscriptions
            // are removed or the client disconnects
            handle_subscriber_mode(&mut stream, &mut buf, &mut out, pubsub, sub_frames).await?;
            return Ok(());
        }

        // two-phase pipeline: dispatch all commands to shards first,
        // then collect responses in order. this avoids creating N large
        // async state machines (one per pipelined command) and lets
        // shards process in parallel while we wait.
        if !frames.is_empty() {
            // phase 1: dispatch — send each command to its shard.
            // each dispatch is just an mpsc send (fast, completes
            // immediately when the channel has capacity).
            let mut pending = Vec::with_capacity(frames.len());
            for frame in frames {
                let p = dispatch_command(frame, &engine, ctx, slow_log, pubsub).await;
                pending.push(p);
            }

            // phase 2: collect — await shard responses in order.
            for p in pending {
                let response = resolve_response(p, ctx, slow_log).await;
                ctx.commands_processed.fetch_add(1, Ordering::Relaxed);
                response.serialize(&mut out);
            }
        }

        if !out.is_empty() {
            stream.write_all(&out).await?;
        }
    }
}

/// Checks if a raw frame is a SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE/PUNSUBSCRIBE command.
fn is_subscribe_frame(frame: &Frame) -> bool {
    if let Frame::Array(parts) = frame {
        if let Some(Frame::Bulk(name)) = parts.first() {
            return name.eq_ignore_ascii_case(b"SUBSCRIBE")
                || name.eq_ignore_ascii_case(b"PSUBSCRIBE")
                || name.eq_ignore_ascii_case(b"UNSUBSCRIBE")
                || name.eq_ignore_ascii_case(b"PUNSUBSCRIBE");
        }
    }
    false
}

/// Subscriber mode: listens for both broadcast messages and client commands.
///
/// In this mode the connection can only process SUBSCRIBE, UNSUBSCRIBE,
/// PSUBSCRIBE, PUNSUBSCRIBE, and PING. All other commands return an error.
/// Returns to the caller when all subscriptions are removed or the client
/// disconnects.
async fn handle_subscriber_mode<S>(
    stream: &mut S,
    buf: &mut BytesMut,
    out: &mut BytesMut,
    pubsub: &Arc<PubSubManager>,
    initial_frames: Vec<Frame>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // track subscriptions: channel/pattern -> receiver
    let mut channel_rxs: HashMap<String, broadcast::Receiver<PubMessage>> = HashMap::new();
    let mut pattern_rxs: HashMap<String, broadcast::Receiver<PubMessage>> = HashMap::new();

    // process the initial subscribe commands
    for frame in initial_frames {
        if let Ok(cmd) = Command::from_frame(frame) {
            handle_sub_command(cmd, pubsub, &mut channel_rxs, &mut pattern_rxs, out);
        }
    }

    if !out.is_empty() {
        stream.write_all(out).await?;
        out.clear();
    }

    // main subscriber loop
    loop {
        let total_subs = channel_rxs.len() + pattern_rxs.len();
        if total_subs == 0 {
            // no more subscriptions — exit subscriber mode
            return Ok(());
        }

        tokio::select! {
            // check for incoming messages from any subscription
            msg = recv_any_message(&mut channel_rxs, &mut pattern_rxs) => {
                if let Some(msg) = msg {
                    serialize_push_message(&msg, out);
                    stream.write_all(out).await?;
                    out.clear();
                }
            }

            // check for new commands from the client (with idle timeout)
            result = tokio::time::timeout(IDLE_TIMEOUT, stream.read_buf(buf)) => {
                let result = match result {
                    Ok(inner) => inner,
                    Err(_) => {
                        // idle timeout — clean up and close
                        cleanup_subscriptions(pubsub, &channel_rxs, &pattern_rxs);
                        return Ok(());
                    }
                };
                // guard against unbounded buffer growth
                if buf.len() > MAX_BUF_SIZE {
                    cleanup_subscriptions(pubsub, &channel_rxs, &pattern_rxs);
                    return Ok(());
                }
                match result {
                    Ok(0) => {
                        // client disconnected — clean up subscriptions
                        cleanup_subscriptions(pubsub, &channel_rxs, &pattern_rxs);
                        return Ok(());
                    }
                    Ok(_) => {
                        // parse and handle subscriber commands
                        loop {
                            match parse_frame(buf) {
                                Ok(Some((frame, consumed))) => {
                                    let _ = buf.split_to(consumed);
                                    match Command::from_frame(frame) {
                                        Ok(cmd) => match &cmd {
                                            Command::Subscribe { .. }
                                            | Command::Unsubscribe { .. }
                                            | Command::PSubscribe { .. }
                                            | Command::PUnsubscribe { .. } => {
                                                handle_sub_command(
                                                    cmd, pubsub, &mut channel_rxs,
                                                    &mut pattern_rxs, out,
                                                );
                                            }
                                            Command::Ping(msg) => {
                                                let resp = match msg {
                                                    Some(m) => Frame::Bulk(m.clone()),
                                                    None => Frame::Simple("PONG".into()),
                                                };
                                                resp.serialize(out);
                                            }
                                            _ => {
                                                Frame::Error(
                                                    "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING are allowed in this context".into()
                                                ).serialize(out);
                                            }
                                        }
                                        Err(e) => {
                                            Frame::Error(format!("ERR {e}")).serialize(out);
                                        }
                                    }
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    Frame::Error(format!("ERR protocol error: {e}")).serialize(out);
                                    stream.write_all(out).await?;
                                    cleanup_subscriptions(pubsub, &channel_rxs, &pattern_rxs);
                                    return Ok(());
                                }
                            }
                        }

                        if !out.is_empty() {
                            stream.write_all(out).await?;
                            out.clear();
                        }
                    }
                    Err(e) => {
                        cleanup_subscriptions(pubsub, &channel_rxs, &pattern_rxs);
                        return Err(e.into());
                    }
                }
            }
        }
    }
}

/// Processes a subscribe/unsubscribe command, updating the subscription maps
/// and writing RESP3 responses.
fn handle_sub_command(
    cmd: Command,
    pubsub: &PubSubManager,
    channel_rxs: &mut HashMap<String, broadcast::Receiver<PubMessage>>,
    pattern_rxs: &mut HashMap<String, broadcast::Receiver<PubMessage>>,
    out: &mut BytesMut,
) {
    match cmd {
        Command::Subscribe { channels } => {
            for ch in channels {
                let total = channel_rxs.len() + pattern_rxs.len();
                if total >= MAX_SUBSCRIPTIONS_PER_CONN {
                    Frame::Error("ERR max subscriptions per connection reached".into())
                        .serialize(out);
                    continue;
                }
                let rx = pubsub.subscribe(&ch);
                channel_rxs.insert(ch.clone(), rx);
                let count = channel_rxs.len() + pattern_rxs.len();
                serialize_sub_response(b"subscribe", &ch, count, out);
            }
        }
        Command::Unsubscribe { channels } => {
            if channels.is_empty() {
                // unsubscribe from all channels
                let names: Vec<String> = channel_rxs.keys().cloned().collect();
                for ch in names {
                    channel_rxs.remove(&ch);
                    pubsub.unsubscribe(&ch);
                    let count = channel_rxs.len() + pattern_rxs.len();
                    serialize_sub_response(b"unsubscribe", &ch, count, out);
                }
                if channel_rxs.is_empty() && pattern_rxs.is_empty() {
                    // send a final response with count 0 if we had nothing
                    serialize_sub_response(b"unsubscribe", "", 0, out);
                }
            } else {
                for ch in channels {
                    channel_rxs.remove(&ch);
                    pubsub.unsubscribe(&ch);
                    let count = channel_rxs.len() + pattern_rxs.len();
                    serialize_sub_response(b"unsubscribe", &ch, count, out);
                }
            }
        }
        Command::PSubscribe { patterns } => {
            for pat in patterns {
                if pat.len() > MAX_PATTERN_LEN {
                    Frame::Error(format!(
                        "ERR pattern too long ({} bytes, max {})",
                        pat.len(),
                        MAX_PATTERN_LEN
                    ))
                    .serialize(out);
                    continue;
                }
                let total = channel_rxs.len() + pattern_rxs.len();
                if total >= MAX_SUBSCRIPTIONS_PER_CONN {
                    Frame::Error("ERR max subscriptions per connection reached".into())
                        .serialize(out);
                    continue;
                }
                let rx = pubsub.psubscribe(&pat);
                pattern_rxs.insert(pat.clone(), rx);
                let count = channel_rxs.len() + pattern_rxs.len();
                serialize_sub_response(b"psubscribe", &pat, count, out);
            }
        }
        Command::PUnsubscribe { patterns } => {
            if patterns.is_empty() {
                let names: Vec<String> = pattern_rxs.keys().cloned().collect();
                for pat in names {
                    pattern_rxs.remove(&pat);
                    pubsub.punsubscribe(&pat);
                    let count = channel_rxs.len() + pattern_rxs.len();
                    serialize_sub_response(b"punsubscribe", &pat, count, out);
                }
                if channel_rxs.is_empty() && pattern_rxs.is_empty() {
                    serialize_sub_response(b"punsubscribe", "", 0, out);
                }
            } else {
                for pat in patterns {
                    pattern_rxs.remove(&pat);
                    pubsub.punsubscribe(&pat);
                    let count = channel_rxs.len() + pattern_rxs.len();
                    serialize_sub_response(b"punsubscribe", &pat, count, out);
                }
            }
        }
        _ => {}
    }
}

/// Receives a message from any active subscription (channels or patterns).
///
/// Uses `FuturesUnordered` to efficiently await all broadcast receivers
/// concurrently, avoiding busy-wait polling. Returns `None` only when
/// there are no active subscriptions.
async fn recv_any_message(
    channel_rxs: &mut HashMap<String, broadcast::Receiver<PubMessage>>,
    pattern_rxs: &mut HashMap<String, broadcast::Receiver<PubMessage>>,
) -> Option<PubMessage> {
    use std::pin::Pin;

    use futures::stream::{FuturesUnordered, StreamExt};

    if channel_rxs.is_empty() && pattern_rxs.is_empty() {
        // no subscriptions — sleep forever (will be cancelled by select)
        return std::future::pending::<Option<PubMessage>>().await;
    }

    type RecvFuture<'a> = Pin<
        Box<
            dyn std::future::Future<Output = Result<PubMessage, broadcast::error::RecvError>>
                + Send
                + 'a,
        >,
    >;

    // collect all receivers into a FuturesUnordered so we await them
    // all concurrently without spinning. each future resolves when its
    // broadcast channel has a message (or reports lag).
    let mut pending: FuturesUnordered<RecvFuture<'_>> = FuturesUnordered::new();

    for rx in channel_rxs.values_mut() {
        pending.push(Box::pin(rx.recv()));
    }
    for rx in pattern_rxs.values_mut() {
        pending.push(Box::pin(rx.recv()));
    }

    while let Some(result) = pending.next().await {
        match result {
            Ok(msg) => return Some(msg),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("subscriber lagged, missed {n} messages");
                // the receiver auto-advances past the gap, so the next
                // call to recv() will return the oldest available message.
                // we drop through to re-poll on the next loop iteration.
            }
            Err(broadcast::error::RecvError::Closed) => {
                // sender was dropped — channel was removed. skip it.
            }
        }
    }

    None
}

/// Serializes a subscribe/unsubscribe response: ["type", channel, count]
fn serialize_sub_response(kind: &'static [u8], channel: &str, count: usize, out: &mut BytesMut) {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(kind)),
        Frame::Bulk(Bytes::copy_from_slice(channel.as_bytes())),
        Frame::Integer(count as i64),
    ])
    .serialize(out);
}

/// Serializes a pushed message for subscribers.
///
/// For exact subscriptions: ["message", channel, data]
/// For pattern subscriptions: ["pmessage", pattern, channel, data]
fn serialize_push_message(msg: &PubMessage, out: &mut BytesMut) {
    let frame = if let Some(ref pattern) = msg.pattern {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"pmessage")),
            Frame::Bulk(Bytes::copy_from_slice(pattern.as_bytes())),
            Frame::Bulk(Bytes::copy_from_slice(msg.channel.as_bytes())),
            Frame::Bulk(msg.data.clone()),
        ])
    } else {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"message")),
            Frame::Bulk(Bytes::copy_from_slice(msg.channel.as_bytes())),
            Frame::Bulk(msg.data.clone()),
        ])
    };
    frame.serialize(out);
}

/// Cleans up all subscriptions when a subscriber disconnects.
fn cleanup_subscriptions(
    pubsub: &PubSubManager,
    channel_rxs: &HashMap<String, broadcast::Receiver<PubMessage>>,
    pattern_rxs: &HashMap<String, broadcast::Receiver<PubMessage>>,
) {
    for ch in channel_rxs.keys() {
        pubsub.unsubscribe(ch);
    }
    for pat in pattern_rxs.keys() {
        pubsub.punsubscribe(pat);
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

            let response = execute(cmd, engine, ctx, slow_log, pubsub).await;
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

/// Dispatches a single frame as part of a pipeline batch.
///
/// For single-key commands, sends the request to the owning shard
/// without waiting for the response. For commands that need special
/// handling (broadcast, multi-key, cluster, pub/sub), falls back to
/// the full `execute()` path and returns the result immediately.
///
/// This is the "dispatch" half of the dispatch-collect pipeline.
/// Each dispatch is fast (just an mpsc send) so the serial loop
/// doesn't bottleneck.
async fn dispatch_command(
    frame: Frame,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
) -> PendingResponse {
    let cmd = match Command::from_frame(frame) {
        Ok(cmd) => cmd,
        Err(e) => return PendingResponse::Immediate(Frame::Error(format!("ERR {e}"))),
    };

    let cmd_name = cmd.command_name();
    let needs_timing = ctx.metrics_enabled || slow_log.is_enabled();
    let start = if needs_timing {
        Some(Instant::now())
    } else {
        None
    };

    // cluster slot validation
    if let Some(redirect) = cluster_slot_check(ctx, &cmd).await {
        return PendingResponse::Immediate(redirect);
    }

    // macro to reduce boilerplate for single-key dispatch
    macro_rules! dispatch {
        ($key:expr, $req:expr, $tag:expr) => {{
            let idx = engine.shard_for_key(&$key);
            match engine.dispatch_to_shard(idx, $req).await {
                Ok(rx) => PendingResponse::Pending { rx, tag: $tag, start, cmd_name },
                Err(e) => PendingResponse::Immediate(Frame::Error(format!("ERR {e}")))
            }
        }};
    }

    match cmd {
        // -- no shard needed --
        Command::Ping(None) => PendingResponse::Immediate(Frame::Simple("PONG".into())),
        Command::Ping(Some(msg)) => PendingResponse::Immediate(Frame::Bulk(msg)),
        Command::Echo(msg) => PendingResponse::Immediate(Frame::Bulk(msg)),

        // -- single-key string commands --
        Command::Get { key } => {
            dispatch!(key, ShardRequest::Get { key }, ResponseTag::Get)
        }
        Command::Set { key, value, expire, nx, xx } => {
            let duration = expire.map(|e| match e {
                SetExpire::Ex(secs) => Duration::from_secs(secs),
                SetExpire::Px(millis) => Duration::from_millis(millis),
            });
            dispatch!(key, ShardRequest::Set { key, value, expire: duration, nx, xx }, ResponseTag::Set)
        }
        Command::Incr { key } => {
            dispatch!(key, ShardRequest::Incr { key }, ResponseTag::IntResult)
        }
        Command::Decr { key } => {
            dispatch!(key, ShardRequest::Decr { key }, ResponseTag::IntResult)
        }
        Command::IncrBy { key, delta } => {
            dispatch!(key, ShardRequest::IncrBy { key, delta }, ResponseTag::IntResult)
        }
        Command::DecrBy { key, delta } => {
            dispatch!(key, ShardRequest::DecrBy { key, delta }, ResponseTag::IntResult)
        }
        Command::IncrByFloat { key, delta } => {
            dispatch!(key, ShardRequest::IncrByFloat { key, delta }, ResponseTag::FloatResult)
        }
        Command::Append { key, value } => {
            dispatch!(key, ShardRequest::Append { key, value }, ResponseTag::LenResultOom)
        }
        Command::Strlen { key } => {
            dispatch!(key, ShardRequest::Strlen { key }, ResponseTag::LenResult)
        }
        Command::Expire { key, seconds } => {
            dispatch!(key, ShardRequest::Expire { key, seconds }, ResponseTag::BoolToInt)
        }
        Command::Ttl { key } => {
            dispatch!(key, ShardRequest::Ttl { key }, ResponseTag::Ttl)
        }
        Command::Persist { key } => {
            dispatch!(key, ShardRequest::Persist { key }, ResponseTag::BoolToInt)
        }
        Command::Pttl { key } => {
            dispatch!(key, ShardRequest::Pttl { key }, ResponseTag::Pttl)
        }
        Command::Pexpire { key, milliseconds } => {
            dispatch!(key, ShardRequest::Pexpire { key, milliseconds }, ResponseTag::BoolToInt)
        }
        Command::Type { key } => {
            dispatch!(key, ShardRequest::Type { key }, ResponseTag::TypeResult)
        }

        // -- list commands --
        Command::LPush { key, values } => {
            dispatch!(key, ShardRequest::LPush { key, values }, ResponseTag::LenResultOom)
        }
        Command::RPush { key, values } => {
            dispatch!(key, ShardRequest::RPush { key, values }, ResponseTag::LenResultOom)
        }
        Command::LPop { key } => {
            dispatch!(key, ShardRequest::LPop { key }, ResponseTag::PopResult)
        }
        Command::RPop { key } => {
            dispatch!(key, ShardRequest::RPop { key }, ResponseTag::PopResult)
        }
        Command::LRange { key, start, stop } => {
            dispatch!(key, ShardRequest::LRange { key, start, stop }, ResponseTag::ArrayResult)
        }
        Command::LLen { key } => {
            dispatch!(key, ShardRequest::LLen { key }, ResponseTag::LenResult)
        }

        // -- sorted set commands --
        Command::ZAdd { key, flags, members } => {
            dispatch!(key, ShardRequest::ZAdd {
                key, members, nx: flags.nx, xx: flags.xx, gt: flags.gt, lt: flags.lt, ch: flags.ch
            }, ResponseTag::ZAddResult)
        }
        Command::ZRem { key, members } => {
            dispatch!(key, ShardRequest::ZRem { key, members }, ResponseTag::ZRemResult)
        }
        Command::ZScore { key, member } => {
            dispatch!(key, ShardRequest::ZScore { key, member }, ResponseTag::ZScoreResult)
        }
        Command::ZRank { key, member } => {
            dispatch!(key, ShardRequest::ZRank { key, member }, ResponseTag::ZRankResult)
        }
        Command::ZRange { key, start, stop, with_scores } => {
            dispatch!(key, ShardRequest::ZRange { key, start, stop, with_scores },
                ResponseTag::ZRangeResult { with_scores })
        }
        Command::ZCard { key } => {
            dispatch!(key, ShardRequest::ZCard { key }, ResponseTag::LenResult)
        }

        // -- hash commands --
        Command::HSet { key, fields } => {
            dispatch!(key, ShardRequest::HSet { key, fields }, ResponseTag::HSetResult)
        }
        Command::HGet { key, field } => {
            dispatch!(key, ShardRequest::HGet { key, field }, ResponseTag::HGetResult)
        }
        Command::HGetAll { key } => {
            dispatch!(key, ShardRequest::HGetAll { key }, ResponseTag::HGetAllResult)
        }
        Command::HDel { key, fields } => {
            dispatch!(key, ShardRequest::HDel { key, fields }, ResponseTag::HDelResult)
        }
        Command::HExists { key, field } => {
            dispatch!(key, ShardRequest::HExists { key, field }, ResponseTag::HExistsResult)
        }
        Command::HLen { key } => {
            dispatch!(key, ShardRequest::HLen { key }, ResponseTag::LenResult)
        }
        Command::HIncrBy { key, field, delta } => {
            dispatch!(key, ShardRequest::HIncrBy { key, field, delta }, ResponseTag::HIncrByResult)
        }
        Command::HKeys { key } => {
            dispatch!(key, ShardRequest::HKeys { key }, ResponseTag::StringArrayResult)
        }
        Command::HVals { key } => {
            dispatch!(key, ShardRequest::HVals { key }, ResponseTag::HValsResult)
        }
        Command::HMGet { key, fields } => {
            dispatch!(key, ShardRequest::HMGet { key, fields }, ResponseTag::HMGetResult)
        }

        // -- set commands --
        Command::SAdd { key, members } => {
            dispatch!(key, ShardRequest::SAdd { key, members }, ResponseTag::LenResultOom)
        }
        Command::SRem { key, members } => {
            dispatch!(key, ShardRequest::SRem { key, members }, ResponseTag::LenResult)
        }
        Command::SMembers { key } => {
            dispatch!(key, ShardRequest::SMembers { key }, ResponseTag::StringArrayResult)
        }
        Command::SIsMember { key, member } => {
            dispatch!(key, ShardRequest::SIsMember { key, member }, ResponseTag::SIsMemberResult)
        }
        Command::SCard { key } => {
            dispatch!(key, ShardRequest::SCard { key }, ResponseTag::LenResult)
        }

        // -- vector commands --
        #[cfg(feature = "vector")]
        Command::VAdd { key, element, vector, metric, quantization, connectivity, expansion_add } => {
            dispatch!(key, ShardRequest::VAdd {
                key, element, vector, metric, quantization, connectivity, expansion_add
            }, ResponseTag::VAddResult)
        }
        #[cfg(feature = "vector")]
        Command::VSim { key, query, count, ef_search, with_scores } => {
            dispatch!(key, ShardRequest::VSim { key, query, count, ef_search },
                ResponseTag::VSimResult { with_scores })
        }
        #[cfg(feature = "vector")]
        Command::VRem { key, element } => {
            dispatch!(key, ShardRequest::VRem { key, element }, ResponseTag::VRemResult)
        }
        #[cfg(feature = "vector")]
        Command::VGet { key, element } => {
            dispatch!(key, ShardRequest::VGet { key, element }, ResponseTag::VGetResult)
        }
        #[cfg(feature = "vector")]
        Command::VCard { key } => {
            dispatch!(key, ShardRequest::VCard { key }, ResponseTag::VIntResult)
        }
        #[cfg(feature = "vector")]
        Command::VDim { key } => {
            dispatch!(key, ShardRequest::VDim { key }, ResponseTag::VIntResult)
        }
        #[cfg(feature = "vector")]
        Command::VInfo { key } => {
            dispatch!(key, ShardRequest::VInfo { key }, ResponseTag::VInfoResult)
        }

        // -- rename (needs same-shard validation) --
        Command::Rename { key, newkey } => {
            if !engine.same_shard(&key, &newkey) {
                PendingResponse::Immediate(Frame::Error(
                    "ERR source and destination keys must hash to the same shard".into(),
                ))
            } else {
                dispatch!(key, ShardRequest::Rename { key, newkey }, ResponseTag::RenameResult)
            }
        }

        // -- proto commands that are single-key dispatches --
        #[cfg(feature = "protobuf")]
        Command::ProtoSet { key, type_name, data, expire, nx, xx } => {
            if engine.schema_registry().is_none() {
                return PendingResponse::Immediate(
                    Frame::Error("ERR protobuf support is not enabled".into()),
                );
            }
            let registry = engine.schema_registry().unwrap();
            {
                let reg = match registry.read() {
                    Ok(r) => r,
                    Err(_) => return PendingResponse::Immediate(
                        Frame::Error("ERR schema registry lock poisoned".into()),
                    ),
                };
                if let Err(e) = reg.validate(&type_name, &data) {
                    return PendingResponse::Immediate(Frame::Error(format!("ERR {e}")));
                }
            }
            let duration = expire.map(|e| match e {
                SetExpire::Ex(secs) => Duration::from_secs(secs),
                SetExpire::Px(millis) => Duration::from_millis(millis),
            });
            dispatch!(key, ShardRequest::ProtoSet {
                key, type_name, data, expire: duration, nx, xx
            }, ResponseTag::ProtoSetResult)
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoGet { key } => {
            if engine.schema_registry().is_none() {
                return PendingResponse::Immediate(
                    Frame::Error("ERR protobuf support is not enabled".into()),
                );
            }
            dispatch!(key, ShardRequest::ProtoGet { key }, ResponseTag::ProtoGetResult)
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoType { key } => {
            if engine.schema_registry().is_none() {
                return PendingResponse::Immediate(
                    Frame::Error("ERR protobuf support is not enabled".into()),
                );
            }
            dispatch!(key, ShardRequest::ProtoType { key }, ResponseTag::ProtoTypeResult)
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoSetField { key, field_path, value } => {
            if engine.schema_registry().is_none() {
                return PendingResponse::Immediate(
                    Frame::Error("ERR protobuf support is not enabled".into()),
                );
            }
            dispatch!(key, ShardRequest::ProtoSetField { key, field_path, value },
                ResponseTag::ProtoSetFieldResult)
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoDelField { key, field_path } => {
            if engine.schema_registry().is_none() {
                return PendingResponse::Immediate(
                    Frame::Error("ERR protobuf support is not enabled".into()),
                );
            }
            dispatch!(key, ShardRequest::ProtoDelField { key, field_path },
                ResponseTag::ProtoDelFieldResult)
        }

        // -- everything else falls back to the full execute() path --
        cmd => {
            let response = execute(cmd, engine, ctx, slow_log, pubsub).await;
            PendingResponse::Immediate(response)
        }
    }
}

/// Resolves a `PendingResponse` into a `Frame`, recording timing if applicable.
async fn resolve_response(
    pending: PendingResponse,
    ctx: &ServerContext,
    slow_log: &SlowLog,
) -> Frame {
    match pending {
        PendingResponse::Immediate(frame) => frame,
        PendingResponse::Pending { rx, tag, start, cmd_name } => {
            let frame = match rx.await {
                Ok(resp) => resolve_shard_response(resp, tag),
                Err(_) => Frame::Error("ERR shard unavailable".into()),
            };
            if let Some(start) = start {
                let elapsed = start.elapsed();
                slow_log.maybe_record(elapsed, cmd_name);
                if ctx.metrics_enabled {
                    let is_error = matches!(&frame, Frame::Error(_));
                    crate::metrics::record_command(cmd_name, elapsed, is_error);
                }
            }
            frame
        }
    }
}

/// Converts a `ShardResponse` to a `Frame` based on the response tag.
fn resolve_shard_response(resp: ShardResponse, tag: ResponseTag) -> Frame {
    match tag {
        ResponseTag::Get => match resp {
            ShardResponse::Value(Some(Value::String(data))) => Frame::Bulk(data),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::Set => match resp {
            ShardResponse::Ok => Frame::Simple("OK".into()),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::BoolToInt => match resp {
            ShardResponse::Bool(b) => Frame::Integer(i64::from(b)),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::Ttl => match resp {
            ShardResponse::Ttl(TtlResult::Seconds(s)) => Frame::Integer(s as i64),
            ShardResponse::Ttl(TtlResult::NoExpiry) => Frame::Integer(-1),
            ShardResponse::Ttl(TtlResult::NotFound) => Frame::Integer(-2),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::Pttl => match resp {
            ShardResponse::Ttl(TtlResult::Milliseconds(ms)) => Frame::Integer(ms as i64),
            ShardResponse::Ttl(TtlResult::NoExpiry) => Frame::Integer(-1),
            ShardResponse::Ttl(TtlResult::NotFound) => Frame::Integer(-2),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::IntResult => match resp {
            ShardResponse::Integer(n) => Frame::Integer(n),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(msg),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::LenResult => match resp {
            ShardResponse::Len(n) => Frame::Integer(n as i64),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::LenResultOom => match resp {
            ShardResponse::Len(n) => Frame::Integer(n as i64),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::FloatResult => match resp {
            ShardResponse::BulkString(val) => Frame::Bulk(Bytes::from(val)),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(msg),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::PopResult => match resp {
            ShardResponse::Value(Some(Value::String(data))) => Frame::Bulk(data),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::ArrayResult => match resp {
            ShardResponse::Array(items) => {
                Frame::Array(items.into_iter().map(Frame::Bulk).collect())
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::TypeResult => match resp {
            ShardResponse::TypeName(name) => Frame::Simple(name.into()),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::ZAddResult => match resp {
            ShardResponse::ZAddLen { count, .. } => Frame::Integer(count as i64),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::ZRemResult => match resp {
            ShardResponse::ZRemLen { count, .. } => Frame::Integer(count as i64),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::ZScoreResult => match resp {
            ShardResponse::Score(Some(s)) => Frame::Bulk(Bytes::from(format!("{s}"))),
            ShardResponse::Score(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::ZRankResult => match resp {
            ShardResponse::Rank(Some(r)) => Frame::Integer(r as i64),
            ShardResponse::Rank(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::ZRangeResult { with_scores } => match resp {
            ShardResponse::ScoredArray(items) => {
                let mut frames = Vec::new();
                for (member, score) in items {
                    frames.push(Frame::Bulk(Bytes::from(member)));
                    if with_scores {
                        frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                    }
                }
                Frame::Array(frames)
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::HSetResult => match resp {
            ShardResponse::Len(n) => Frame::Integer(n as i64),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::HGetResult => match resp {
            ShardResponse::Value(Some(Value::String(data))) => Frame::Bulk(data),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::HGetAllResult => match resp {
            ShardResponse::HashFields(fields) => {
                let mut frames = Vec::with_capacity(fields.len() * 2);
                for (field, value) in fields {
                    frames.push(Frame::Bulk(Bytes::from(field)));
                    frames.push(Frame::Bulk(value));
                }
                Frame::Array(frames)
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::HDelResult => match resp {
            ShardResponse::HDelLen { count, .. } => Frame::Integer(count as i64),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::HExistsResult => match resp {
            ShardResponse::Bool(b) => Frame::Integer(if b { 1 } else { 0 }),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::HIncrByResult => match resp {
            ShardResponse::Integer(n) => Frame::Integer(n),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(format!("ERR {msg}")),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::StringArrayResult => match resp {
            ShardResponse::StringArray(items) => Frame::Array(
                items
                    .into_iter()
                    .map(|s| Frame::Bulk(Bytes::from(s)))
                    .collect(),
            ),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::HValsResult => match resp {
            ShardResponse::Array(vals) => {
                Frame::Array(vals.into_iter().map(Frame::Bulk).collect())
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::HMGetResult => match resp {
            ShardResponse::OptionalArray(vals) => Frame::Array(
                vals.into_iter()
                    .map(|v| match v {
                        Some(data) => Frame::Bulk(data),
                        None => Frame::Null,
                    })
                    .collect(),
            ),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::SIsMemberResult => match resp {
            ShardResponse::Bool(b) => Frame::Integer(if b { 1 } else { 0 }),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::RenameResult => match resp {
            ShardResponse::Ok => Frame::Simple("OK".into()),
            ShardResponse::Err(msg) => Frame::Error(msg),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VAddResult => match resp {
            ShardResponse::VAddResult { added, .. } => {
                Frame::Integer(if added { 1 } else { 0 })
            }
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(format!("ERR {msg}")),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VSimResult { with_scores } => match resp {
            ShardResponse::VSimResult(results) => {
                let mut frames = Vec::new();
                for (element, distance) in results {
                    frames.push(Frame::Bulk(Bytes::from(element)));
                    if with_scores {
                        frames.push(Frame::Bulk(Bytes::from(distance.to_string())));
                    }
                }
                Frame::Array(frames)
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VRemResult => match resp {
            ShardResponse::Bool(removed) => Frame::Integer(if removed { 1 } else { 0 }),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VGetResult => match resp {
            ShardResponse::VectorData(Some(vector)) => Frame::Array(
                vector
                    .into_iter()
                    .map(|v| Frame::Bulk(Bytes::from(v.to_string())))
                    .collect(),
            ),
            ShardResponse::VectorData(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VIntResult => match resp {
            ShardResponse::Integer(n) => Frame::Integer(n),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VInfoResult => match resp {
            ShardResponse::VectorInfo(Some(fields)) => {
                let mut frames = Vec::with_capacity(fields.len() * 2);
                for (k, v) in fields {
                    frames.push(Frame::Bulk(Bytes::from(k)));
                    frames.push(Frame::Bulk(Bytes::from(v)));
                }
                Frame::Array(frames)
            }
            ShardResponse::VectorInfo(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "protobuf")]
        ResponseTag::ProtoSetResult => match resp {
            ShardResponse::Ok => Frame::Simple("OK".into()),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "protobuf")]
        ResponseTag::ProtoGetResult => match resp {
            ShardResponse::ProtoValue(Some((type_name, data, _ttl))) => {
                Frame::Array(vec![Frame::Bulk(Bytes::from(type_name)), Frame::Bulk(data)])
            }
            ShardResponse::ProtoValue(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "protobuf")]
        ResponseTag::ProtoTypeResult => match resp {
            ShardResponse::ProtoTypeName(Some(name)) => Frame::Bulk(Bytes::from(name)),
            ShardResponse::ProtoTypeName(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "protobuf")]
        ResponseTag::ProtoSetFieldResult => match resp {
            ShardResponse::ProtoFieldUpdated { .. } => Frame::Simple("OK".into()),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(format!("ERR {msg}")),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "protobuf")]
        ResponseTag::ProtoDelFieldResult => match resp {
            ShardResponse::ProtoFieldUpdated { .. } => Frame::Integer(1),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(format!("ERR {msg}")),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
    }
}

/// Validates cluster slot ownership for the given command.
///
/// Returns `None` if the command should proceed (not in cluster mode,
/// or the local node owns the slot). Returns `Some(Frame)` with a MOVED,
/// CLUSTERDOWN, or CROSSSLOT error if execution should be rejected.
async fn cluster_slot_check(ctx: &ServerContext, cmd: &Command) -> Option<Frame> {
    let cluster = ctx.cluster.as_ref()?;

    match cmd {
        // single-key commands — check slot ownership for the key
        Command::Get { ref key }
        | Command::Set { ref key, .. }
        | Command::Expire { ref key, .. }
        | Command::Ttl { ref key }
        | Command::Incr { ref key }
        | Command::Decr { ref key }
        | Command::IncrBy { ref key, .. }
        | Command::DecrBy { ref key, .. }
        | Command::Append { ref key, .. }
        | Command::Strlen { ref key }
        | Command::IncrByFloat { ref key, .. }
        | Command::Persist { ref key }
        | Command::Pttl { ref key }
        | Command::Pexpire { ref key, .. }
        | Command::Type { ref key }
        | Command::LPush { ref key, .. }
        | Command::RPush { ref key, .. }
        | Command::LPop { ref key }
        | Command::RPop { ref key }
        | Command::LRange { ref key, .. }
        | Command::LLen { ref key }
        | Command::ZAdd { ref key, .. }
        | Command::ZRem { ref key, .. }
        | Command::ZScore { ref key, .. }
        | Command::ZRank { ref key, .. }
        | Command::ZRange { ref key, .. }
        | Command::ZCard { ref key }
        | Command::HSet { ref key, .. }
        | Command::HGet { ref key, .. }
        | Command::HGetAll { ref key }
        | Command::HDel { ref key, .. }
        | Command::HExists { ref key, .. }
        | Command::HLen { ref key }
        | Command::HIncrBy { ref key, .. }
        | Command::HKeys { ref key }
        | Command::HVals { ref key }
        | Command::HMGet { ref key, .. }
        | Command::SAdd { ref key, .. }
        | Command::SRem { ref key, .. }
        | Command::SMembers { ref key }
        | Command::SIsMember { ref key, .. }
        | Command::SCard { ref key }
        | Command::ProtoSet { ref key, .. }
        | Command::ProtoGet { ref key }
        | Command::ProtoType { ref key }
        | Command::ProtoGetField { ref key, .. }
        | Command::ProtoSetField { ref key, .. }
        | Command::ProtoDelField { ref key, .. }
        | Command::VAdd { ref key, .. }
        | Command::VSim { ref key, .. }
        | Command::VRem { ref key, .. }
        | Command::VGet { ref key, .. }
        | Command::VCard { ref key }
        | Command::VDim { ref key }
        | Command::VInfo { ref key } => cluster.check_slot(key.as_bytes()).await,

        // multi-key commands — crossslot validation + slot ownership
        Command::Del { ref keys }
        | Command::Unlink { ref keys }
        | Command::Exists { ref keys }
        | Command::MGet { ref keys } => {
            if let Err(err) = cluster.check_crossslot(keys) {
                return Some(err);
            }
            if let Some(first) = keys.first() {
                return cluster.check_slot(first.as_bytes()).await;
            }
            None
        }

        // rename: crossslot check on both keys
        Command::Rename {
            ref key,
            ref newkey,
        } => {
            let pair = [key.clone(), newkey.clone()];
            if let Err(err) = cluster.check_crossslot(&pair) {
                return Some(err);
            }
            cluster.check_slot(key.as_bytes()).await
        }

        // mset: extract keys from pairs for crossslot check
        Command::MSet { ref pairs } => {
            let keys: Vec<String> = pairs.iter().map(|(k, _)| k.clone()).collect();
            if let Err(err) = cluster.check_crossslot(&keys) {
                return Some(err);
            }
            if let Some(first) = keys.first() {
                return cluster.check_slot(first.as_bytes()).await;
            }
            None
        }

        // everything else (PING, ECHO, INFO, DBSIZE, cluster commands,
        // pubsub, AUTH, etc.) doesn't need slot routing
        _ => None,
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
    pubsub: &Arc<PubSubManager>,
) -> Frame {
    // cluster slot validation — check once before dispatch
    if let Some(redirect) = cluster_slot_check(ctx, &cmd).await {
        return redirect;
    }

    match cmd {
        // -- no shard needed --
        Command::Ping(None) => Frame::Simple("PONG".into()),
        Command::Ping(Some(msg)) => Frame::Bulk(msg),
        Command::Echo(msg) => Frame::Bulk(msg),

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
            let duration = expire.map(|e| match e {
                SetExpire::Ex(secs) => Duration::from_secs(secs),
                SetExpire::Px(millis) => Duration::from_millis(millis),
            });
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Set {
                key,
                value,
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

        Command::Expire { key, seconds } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::Expire { key, seconds };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Bool(b)) => Frame::Integer(i64::from(b)),
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

        Command::Info { section } => render_info(engine, ctx, section.as_deref()).await,

        Command::BgSave => match engine.broadcast(|| ShardRequest::Snapshot).await {
            Ok(_) => Frame::Simple("Background saving started".into()),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },

        Command::BgRewriteAof => match engine.broadcast(|| ShardRequest::RewriteAof).await {
            Ok(_) => Frame::Simple("Background append only file rewriting started".into()),
            Err(e) => Frame::Error(format!("ERR {e}")),
        },

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
            let req = ShardRequest::LPush { key, values };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::RPush { key, values } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::RPush { key, values };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
                Ok(ShardResponse::WrongType) => wrongtype_error(),
                Ok(ShardResponse::OutOfMemory) => oom_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }

        Command::LPop { key } => {
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

        Command::RPop { key } => {
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
                key,
                members,
                nx: flags.nx,
                xx: flags.xx,
                gt: flags.gt,
                lt: flags.lt,
                ch: flags.ch,
            };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::ZAddLen { count, .. }) => Frame::Integer(count as i64),
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

        // --- hash commands ---
        Command::HSet { key, fields } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::HSet { key, fields };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
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

        // --- set commands ---
        Command::SAdd { key, members } => {
            let idx = engine.shard_for_key(&key);
            let req = ShardRequest::SAdd { key, members };
            match engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
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

        Command::Asking => Frame::Simple("OK".into()),

        Command::ClusterMeet { ip, port } => match &ctx.cluster {
            Some(c) => c.cluster_meet(&ip, port).await,
            None => Frame::Error("ERR This instance has cluster support disabled".into()),
        },

        Command::ClusterAddSlots { slots } => match &ctx.cluster {
            Some(c) => c.cluster_addslots(&slots).await,
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

        Command::ClusterReplicate { .. } => Frame::Error("ERR REPLICATE not yet supported".into()),

        Command::ClusterFailover { .. } => Frame::Error("ERR FAILOVER not yet supported".into()),

        Command::Migrate { .. } => Frame::Error("ERR not yet implemented".into()),

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
            let duration = expire.map(|e| match e {
                SetExpire::Ex(secs) => Duration::from_secs(secs),
                SetExpire::Px(millis) => Duration::from_millis(millis),
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

        Command::Quit => Frame::Simple("OK".into()),

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
                let effective = ember_core::memory::effective_limit(max);
                out.push_str(&format!("max_memory:{max}\r\n"));
                out.push_str(&format!("max_memory_human:{}\r\n", human_bytes(max)));
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
