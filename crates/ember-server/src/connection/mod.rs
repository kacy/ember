//! Per-connection handler for sharded engine mode.
//!
//! Reads RESP3 frames from a TCP/TLS stream, routes them through the
//! sharded engine, and writes responses back. Supports pipelining
//! via a two-phase dispatch-collect pattern: all commands in a batch
//! are dispatched to shards without waiting, then responses are
//! collected in order.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use bytes::{BufMut, BytesMut};
use ember_core::{Engine, ShardRequest, ShardResponse};
use ember_protocol::{parse_frame, Frame};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::connection_common::{
    initial_acl_user, is_allowed_before_auth, is_auth_frame, is_monitor_frame, try_auth,
    TransactionState,
};
use crate::metrics::on_auth_failure;
use crate::pubsub::PubSubManager;
use crate::server::ServerContext;
use crate::slowlog::SlowLog;

mod dispatch;
mod exec;
mod execute;
mod handler;
mod response;

/// A command that has been dispatched to a shard but not yet resolved.
///
/// Single-key commands are dispatched non-blocking: the request is sent
/// to the shard's mpsc channel and we hold the oneshot receiver. Complex
/// commands (broadcast, multi-key, cluster) are executed immediately.
pub(super) enum PendingResponse {
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
pub(super) enum ResponseTag {
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
    /// ZRANK/ZREVRANK: Rank(Some) → Integer, Rank(None) → Null
    ZRankResult,
    /// ZRANGE/ZREVRANGE/ZRANGEBYSCORE/ZREVRANGEBYSCORE: ScoredArray → Array
    ZRangeResult { with_scores: bool },
    /// ZINCRBY: ZIncrByResult → Bulk (score as string)
    ZIncrByResult,
    /// ZPOPMIN/ZPOPMAX: ZPopResult → Array of member/score pairs
    ZPopResult,
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
    /// SMISMEMBER: BoolArray → Array of Integer(0/1)
    SMisMemberResult,
    /// SUNIONSTORE/SINTERSTORE/SDIFFSTORE: SetStoreResult → Integer (count)
    SetStoreResult,
    /// SSCAN/HSCAN/ZSCAN: CollectionScan → cursor + array
    CollectionScanResult,
    /// RENAME: Ok → Simple("OK"), Err → Error
    RenameResult,
    /// COPY: Bool(true) → Integer(1), Bool(false) → Integer(0), Err → Error
    CopyResult,
    /// OBJECT ENCODING: EncodingName(Some) → Bulk, EncodingName(None) → Null
    EncodingResult,
    /// Len result with OOM possible (LPUSH/RPUSH/SADD)
    LenResultOom,
    /// Vector VADD result
    #[cfg(feature = "vector")]
    VAddResult,
    /// Vector VADD_BATCH result
    #[cfg(feature = "vector")]
    VAddBatchResult,
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
    /// LINDEX: Value(Some(String)) → Bulk, Value(None) → Null, WrongType
    LIndexResult,
    /// LSET: Ok → Simple("OK"), Err → Error, WrongType
    LSetResult,
    /// LTRIM: Ok → Simple("OK"), WrongType
    LTrimResult,
    /// LINSERT: Integer → Integer, WrongType, OOM
    LInsertResult,
    /// LREM: Len → Integer, WrongType
    LRemResult,
    /// LPOS: IntegerArray → Array/Null/Integer depending on COUNT
    LPosResult { count: Option<usize> },
    /// SORT: Array → Array of Bulk, WrongType → error
    SortResult,
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

/// Result of preparing a pipelined command before the batch dispatch phase.
///
/// `Immediate` commands (errors, PING, multi-key, broadcast) are resolved
/// during preparation. `Routed` commands carry the shard index and request
/// so the caller can group them by shard for batch dispatch.
pub(super) enum PreparedDispatch {
    /// Already resolved — no shard send needed.
    Immediate(PendingResponse),
    /// Needs to be dispatched to the given shard.
    Routed {
        shard_idx: usize,
        request: ShardRequest,
        tag: ResponseTag,
        start: Option<Instant>,
        cmd_name: &'static str,
    },
}

/// A command routed to a shard during the batch-dispatch prepare phase.
///
/// Bundles the output index (position in the result vec), the shard request,
/// the response conversion tag, and timing metadata.
pub(super) type ShardBucketEntry = (
    usize,
    ShardRequest,
    ResponseTag,
    Option<Instant>,
    &'static str,
);

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
    peer_addr: SocketAddr,
    engine: Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
    client_id: u64,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // per-connection auth + ACL state.
    let (mut acl_user, mut current_username) = initial_acl_user(ctx);
    let mut authenticated = acl_user.is_some();
    let mut auth_failures: u32 = 0;
    // ASKING flag: set by the ASKING command, consumed by the next command.
    // allows the target node to serve importing slots during migration.
    let mut asking = false;
    // per-connection transaction state for MULTI/EXEC/DISCARD
    let mut tx_state = TransactionState::None;
    // per-connection WATCH state: (key, version_at_watch_time)
    let mut watched_keys: Vec<(String, Option<u64>)> = Vec::new();

    // cache the peer address string once; avoids an allocation per command
    // on the MONITOR hot path where every command needs a string representation.
    let peer_addr_str = peer_addr.to_string();

    let mut buf = BytesMut::with_capacity(ctx.limits.buf_capacity);
    let mut out = BytesMut::with_capacity(ctx.limits.buf_capacity);
    let mut frames = Vec::new();

    // per-connection reusable reply channel for the P=1 fast path.
    // avoids allocating a new oneshot::channel() on every single command
    // when the client isn't pipelining.
    let (reusable_tx, mut reusable_rx) = mpsc::channel::<ShardResponse>(1);

    // set when try_read grabbed data after a write, so we can skip
    // the blocking read at the top of the next iteration.
    let mut skip_read = false;

    loop {
        // guard against unbounded buffer growth from incomplete frames
        if buf.len() > ctx.limits.max_buf_size {
            let msg = "ERR max buffer size exceeded, closing connection";
            let mut err_buf = BytesMut::new();
            Frame::Error(msg.into()).serialize(&mut err_buf);
            let _ = stream.write_all(&err_buf).await;
            return Ok(());
        }

        if skip_read {
            skip_read = false;
        } else {
            // read some data — returns 0 on clean disconnect, times out
            // after idle_timeout to reclaim resources from abandoned connections
            match tokio::time::timeout(ctx.limits.idle_timeout, stream.read_buf(&mut buf)).await {
                Ok(Ok(0)) => return Ok(()),
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => return Ok(()), // idle timeout — close silently
            }
        }

        // parse all complete frames from the buffer first, then dispatch
        // them concurrently to shards. this allows pipelined commands to
        // be processed in parallel rather than serially.
        //
        // uses split_to(consumed) which is O(1) pointer adjustment —
        // unconsumed bytes stay in the buffer without copying. bulk
        // strings are copied during parsing rather than zero-copy sliced,
        // which is negligible for small values (the common case).
        out.clear();
        frames.clear();
        loop {
            if buf.is_empty() {
                break;
            }
            match parse_frame(&buf) {
                Ok(Some((frame, consumed))) => {
                    let _ = buf.split_to(consumed);
                    frames.push(frame);
                    if frames.len() >= ctx.limits.max_pipeline_depth {
                        break; // process this batch, remaining data stays in buf
                    }
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
            for frame in frames.drain(..) {
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
                            Frame::Error("ERR too many AUTH failures, closing connection".into())
                                .serialize(&mut out);
                            let _ = stream.write_all(&out).await;
                            return Ok(());
                        }
                    }
                } else if is_allowed_before_auth(&frame) {
                    let response = dispatch::process(
                        frame,
                        &engine,
                        ctx,
                        slow_log,
                        pubsub,
                        &mut asking,
                        &peer_addr_str,
                        client_id,
                        &acl_user,
                        &current_username,
                    )
                    .await;
                    response.serialize(&mut out);
                } else {
                    on_auth_failure("noauth");
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
        // check for MONITOR — enters a dedicated output loop
        if frames.iter().any(is_monitor_frame) {
            // process any non-MONITOR frames first
            for frame in frames.drain(..) {
                if is_monitor_frame(&frame) {
                    // write +OK and enter monitor mode
                    Frame::Simple("OK".into()).serialize(&mut out);
                    stream.write_all(&out).await?;
                    out.clear();
                    handler::handle_monitor_mode(&mut stream, &mut buf, ctx, &peer_addr_str)
                        .await?;
                    return Ok(());
                }
                let response = dispatch::process(
                    frame,
                    &engine,
                    ctx,
                    slow_log,
                    pubsub,
                    &mut asking,
                    &peer_addr_str,
                    client_id,
                    &acl_user,
                    &current_username,
                )
                .await;
                response.serialize(&mut out);
            }
            if !out.is_empty() {
                stream.write_all(&out).await?;
            }
            continue;
        }

        let enter_sub = frames.iter().any(is_subscribe_frame);

        if enter_sub {
            // process any non-subscribe commands that came before
            let mut sub_frames = Vec::new();
            for frame in frames.drain(..) {
                if is_subscribe_frame(&frame) {
                    sub_frames.push(frame);
                } else {
                    let response = dispatch::process(
                        frame,
                        &engine,
                        ctx,
                        slow_log,
                        pubsub,
                        &mut asking,
                        &peer_addr_str,
                        client_id,
                        &acl_user,
                        &current_username,
                    )
                    .await;
                    response.serialize(&mut out);
                }
            }
            if !out.is_empty() {
                stream.write_all(&out).await?;
                out.clear();
            }

            // enter subscriber mode — this blocks until all subscriptions
            // are removed or the client disconnects
            handler::handle_subscriber_mode(
                &mut stream,
                &mut buf,
                &mut out,
                ctx,
                pubsub,
                sub_frames,
            )
            .await?;
            return Ok(());
        }

        // check for blocking list operations (BLPOP/BRPOP). these break
        // the pipeline model because they may block the connection. process
        // any preceding non-blocking frames first, then handle the blocking
        // op, then continue with any remaining frames.
        if frames.iter().any(is_blocking_pop_frame) {
            let mut remaining = Vec::new();
            let mut blocking_frame = None;

            for frame in frames.drain(..) {
                if blocking_frame.is_some() {
                    remaining.push(frame);
                } else if is_blocking_pop_frame(&frame) {
                    blocking_frame = Some(frame);
                } else {
                    let response = dispatch::process(
                        frame,
                        &engine,
                        ctx,
                        slow_log,
                        pubsub,
                        &mut asking,
                        &peer_addr_str,
                        client_id,
                        &acl_user,
                        &current_username,
                    )
                    .await;
                    response.serialize(&mut out);
                }
            }

            // flush any preceding responses
            if !out.is_empty() {
                stream.write_all(&out).await?;
                out.clear();
            }

            // handle the blocking pop
            if let Some(frame) = blocking_frame {
                let response =
                    handler::handle_blocking_pop_cmd(frame, &engine, ctx, slow_log, &mut asking)
                        .await;
                response.serialize(&mut out);
                stream.write_all(&out).await?;
                out.clear();
            }

            // process any remaining frames after the blocking op
            for frame in remaining {
                let response = dispatch::process(
                    frame,
                    &engine,
                    ctx,
                    slow_log,
                    pubsub,
                    &mut asking,
                    &peer_addr_str,
                    client_id,
                    &acl_user,
                    &current_username,
                )
                .await;
                response.serialize(&mut out);
            }
            if !out.is_empty() {
                stream.write_all(&out).await?;
            }
            continue;
        }

        // two-phase pipeline: dispatch all commands to shards first,
        // then collect responses in order. this avoids creating N large
        // async state machines (one per pipelined command) and lets
        // shards process in parallel while we wait.
        //
        // when a transaction is active (or a batch contains MULTI/EXEC),
        // fall back to serial execution to preserve ordering guarantees.
        if !frames.is_empty() {
            let needs_serial = !matches!(tx_state, TransactionState::None)
                || frames.iter().any(is_transaction_frame);

            if needs_serial {
                // serial path: required during transactions
                for frame in frames.drain(..) {
                    let response = handler::handle_frame_with_tx(
                        frame,
                        &mut tx_state,
                        &mut watched_keys,
                        &engine,
                        ctx,
                        slow_log,
                        pubsub,
                        &mut asking,
                        &peer_addr_str,
                        client_id,
                        &acl_user,
                        &current_username,
                    )
                    .await;
                    response.serialize(&mut out);
                }
            } else if frames.len() == 1 {
                // P=1 fast path: single non-transactional command. uses the
                // per-connection reusable mpsc channel instead of allocating
                // a oneshot per command, and avoids the pipeline machinery
                // (shard buckets, batch dispatch, pending result vec).
                // safe: branch entered only when frames.len() == 1
                let Some(frame) = frames.pop() else {
                    continue;
                };
                let prepared = dispatch::prepare_command(
                    frame,
                    &engine,
                    ctx,
                    slow_log,
                    pubsub,
                    &mut asking,
                    &peer_addr_str,
                    client_id,
                    &acl_user,
                    &current_username,
                )
                .await;
                match prepared {
                    PreparedDispatch::Immediate(pr) => {
                        let response = response::resolve_response(pr, ctx, slow_log).await;
                        ctx.commands_processed.fetch_add(1, Ordering::Relaxed);
                        response.serialize(&mut out);
                    }
                    PreparedDispatch::Routed {
                        shard_idx,
                        request,
                        tag,
                        start,
                        cmd_name,
                    } => {
                        let frame = match engine
                            .dispatch_reusable_to_shard(shard_idx, request, reusable_tx.clone())
                            .await
                        {
                            Ok(()) => match reusable_rx.recv().await {
                                Some(resp) => response::resolve_shard_response(resp, tag),
                                None => Frame::Error("ERR shard unavailable".into()),
                            },
                            Err(e) => Frame::Error(format!("ERR {e}")),
                        };
                        if let Some(start) = start {
                            let elapsed = start.elapsed();
                            slow_log.maybe_record(elapsed, cmd_name);
                            if ctx.metrics_enabled {
                                let is_error = matches!(&frame, Frame::Error(_));
                                crate::metrics::record_command(cmd_name, elapsed, is_error);
                            }
                        }
                        ctx.commands_processed.fetch_add(1, Ordering::Relaxed);
                        frame.serialize(&mut out);
                    }
                }
            } else {
                // pipeline path: batch dispatch to reduce channel contention.
                //
                // phase 1: prepare all commands (parse, validate, determine shard)
                // phase 2: group by shard and send one batch message per shard
                // phase 3: collect responses in original order
                let frame_count = frames.len();
                let shard_count = engine.shard_count();
                let mut result: Vec<PendingResponse> = Vec::with_capacity(frame_count);

                // per-shard buckets for grouping commands by target shard
                let mut shard_buckets: Vec<Vec<ShardBucketEntry>> =
                    (0..shard_count).map(|_| Vec::new()).collect();

                // phase 1: prepare
                for frame in frames.drain(..) {
                    let idx = result.len();
                    let prepared = dispatch::prepare_command(
                        frame,
                        &engine,
                        ctx,
                        slow_log,
                        pubsub,
                        &mut asking,
                        &peer_addr_str,
                        client_id,
                        &acl_user,
                        &current_username,
                    )
                    .await;
                    match prepared {
                        PreparedDispatch::Immediate(pr) => {
                            result.push(pr);
                        }
                        PreparedDispatch::Routed {
                            shard_idx,
                            request,
                            tag,
                            start,
                            cmd_name,
                        } => {
                            // placeholder — will be replaced in phase 2
                            result.push(PendingResponse::Immediate(Frame::Null));
                            shard_buckets[shard_idx].push((idx, request, tag, start, cmd_name));
                        }
                    }
                }

                // phase 2: batch dispatch — one channel send per shard
                for (shard_idx, bucket) in shard_buckets.into_iter().enumerate() {
                    if bucket.is_empty() {
                        continue;
                    }
                    let mut indices = Vec::with_capacity(bucket.len());
                    let mut requests = Vec::with_capacity(bucket.len());
                    let mut meta = Vec::with_capacity(bucket.len());
                    for (i, req, tag, start, cmd_name) in bucket {
                        indices.push(i);
                        requests.push(req);
                        meta.push((tag, start, cmd_name));
                    }
                    match engine.dispatch_batch_to_shard(shard_idx, requests).await {
                        Ok(receivers) => {
                            for ((rx, (tag, start, cmd_name)), i) in
                                receivers.into_iter().zip(meta).zip(indices)
                            {
                                result[i] = PendingResponse::Pending {
                                    rx,
                                    tag,
                                    start,
                                    cmd_name,
                                };
                            }
                        }
                        Err(e) => {
                            let err_msg = format!("ERR {e}");
                            for i in indices {
                                result[i] =
                                    PendingResponse::Immediate(Frame::Error(err_msg.clone()));
                            }
                        }
                    }
                }

                // phase 3: collect in original order
                for p in result {
                    let response = response::resolve_response(p, ctx, slow_log).await;
                    ctx.commands_processed.fetch_add(1, Ordering::Relaxed);
                    response.serialize(&mut out);
                }
            }
        }

        if !out.is_empty() {
            stream.write_all(&out).await?;

            // try a non-blocking read before re-entering the event loop.
            // at P=1, the client often has the next command queued in the
            // kernel buffer already. grabbing it here saves a full
            // epoll/kqueue round-trip through the tokio scheduler.
            buf.reserve(4096);
            skip_read = std::future::poll_fn(|cx| {
                let n = {
                    let dst = buf.chunk_mut();
                    // SAFETY: UninitSlice is repr(transparent) over
                    // [MaybeUninit<u8>]. this is the same cast tokio uses
                    // internally in its read_buf implementation.
                    let dst = unsafe { &mut *(dst as *mut _ as *mut [std::mem::MaybeUninit<u8>]) };
                    let mut rb = tokio::io::ReadBuf::uninit(dst);
                    match std::pin::Pin::new(&mut stream).poll_read(cx, &mut rb) {
                        std::task::Poll::Ready(Ok(())) => rb.filled().len(),
                        std::task::Poll::Pending => 0,
                        std::task::Poll::Ready(Err(_)) => {
                            // I/O error — the next blocking read will surface
                            // it properly and tear down the connection.
                            0
                        }
                    }
                };
                if n > 0 {
                    // SAFETY: poll_read filled exactly n bytes into the
                    // buffer's uninitialized tail. advance_mut marks them
                    // as initialized.
                    unsafe {
                        buf.advance_mut(n);
                    }
                    std::task::Poll::Ready(true)
                } else {
                    std::task::Poll::Ready(false)
                }
            })
            .await;
        }
    }
}

/// Peeks at the command name from a raw frame without consuming it.
///
/// Returns a `&'static str` for known transaction commands, avoiding
/// the heap allocations of to_vec + from_utf8 + to_ascii_uppercase.
pub(super) fn peek_command_name(frame: &Frame) -> Option<&'static str> {
    if let Frame::Array(parts) = frame {
        if let Some(Frame::Bulk(name)) = parts.first() {
            if name.eq_ignore_ascii_case(b"MULTI") {
                return Some("MULTI");
            }
            if name.eq_ignore_ascii_case(b"EXEC") {
                return Some("EXEC");
            }
            if name.eq_ignore_ascii_case(b"DISCARD") {
                return Some("DISCARD");
            }
            if name.eq_ignore_ascii_case(b"WATCH") {
                return Some("WATCH");
            }
            if name.eq_ignore_ascii_case(b"UNWATCH") {
                return Some("UNWATCH");
            }
        }
    }
    None
}

/// Checks if a raw frame is a transaction-related command that requires
/// serial execution (MULTI, EXEC, DISCARD, WATCH, UNWATCH).
pub(super) fn is_transaction_frame(frame: &Frame) -> bool {
    if let Frame::Array(parts) = frame {
        if let Some(Frame::Bulk(name)) = parts.first() {
            return name.eq_ignore_ascii_case(b"MULTI")
                || name.eq_ignore_ascii_case(b"EXEC")
                || name.eq_ignore_ascii_case(b"DISCARD")
                || name.eq_ignore_ascii_case(b"WATCH")
                || name.eq_ignore_ascii_case(b"UNWATCH");
        }
    }
    false
}

/// Checks if a raw frame is a BLPOP or BRPOP command.
pub(super) fn is_blocking_pop_frame(frame: &Frame) -> bool {
    if let Frame::Array(parts) = frame {
        if let Some(Frame::Bulk(name)) = parts.first() {
            return name.eq_ignore_ascii_case(b"BLPOP") || name.eq_ignore_ascii_case(b"BRPOP");
        }
    }
    false
}

/// Checks if a raw frame is a SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE/PUNSUBSCRIBE command.
pub(super) fn is_subscribe_frame(frame: &Frame) -> bool {
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
