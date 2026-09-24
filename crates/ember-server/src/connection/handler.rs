//! Specialized connection handlers — transactions, blocking ops, monitor, pub/sub.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use ember_core::{Engine, ShardRequest, ShardResponse};
use ember_protocol::{parse_request, Command, Frame};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc};

use crate::connection_common::{
    format_monitor_event, validate_command_sizes, Session, TransactionState,
};
use crate::pubsub::{PubMessage, PubSubManager};
use crate::server::ServerContext;
use crate::slowlog::SlowLog;

/// Handles a single frame with transaction awareness.
///
/// When not in a transaction, dispatches the frame normally (falling back to
/// serial execution since transactions break the pipeline model). When
/// queuing, most frames are buffered and `+QUEUED` is returned. EXEC replays
/// the queue through the normal dispatch path and returns an array of results.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_frame_with_tx(
    frame: Frame,
    tx_state: &mut TransactionState,
    watched_keys: &mut Vec<(String, Option<u64>)>,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
    asking: &mut bool,
    peer_addr: &str,
    client_id: u64,
    session: &Session,
) -> Frame {
    // peek at the command name without consuming the frame
    let cmd_name = super::peek_command_name(&frame);

    match tx_state {
        TransactionState::None => {
            if cmd_name == Some("MULTI") {
                // validate the frame parses correctly
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
            } else if cmd_name == Some("EXEC") {
                Frame::Error("ERR EXEC without MULTI".into())
            } else if cmd_name == Some("DISCARD") {
                Frame::Error("ERR DISCARD without MULTI".into())
            } else if cmd_name == Some("WATCH") {
                match Command::from_frame(frame) {
                    Ok(Command::Watch { keys }) => {
                        // query current version for each key
                        for key in keys {
                            let ver = match engine
                                .route(&key, ShardRequest::KeyVersion { key: key.clone() })
                                .await
                            {
                                Ok(ShardResponse::Version(v)) => v,
                                _ => None,
                            };
                            watched_keys.push((key, ver));
                        }
                        Frame::Simple("OK".into())
                    }
                    Err(e) => Frame::Error(format!("ERR {e}")),
                    _ => Frame::Error("ERR unexpected parse result".into()),
                }
            } else if cmd_name == Some("UNWATCH") {
                watched_keys.clear();
                Frame::Simple("OK".into())
            } else {
                // normal dispatch path (process() increments commands_processed)
                super::dispatch::process(
                    frame, engine, ctx, slow_log, pubsub, asking, peer_addr, client_id, session,
                )
                .await
            }
        }
        TransactionState::Queuing { queue, error } => {
            match cmd_name {
                Some("MULTI") => Frame::Error("ERR MULTI calls can not be nested".into()),
                Some("WATCH") => Frame::Error("ERR WATCH inside MULTI is not allowed".into()),
                Some("EXEC") => {
                    if *error {
                        let q = std::mem::take(queue);
                        *tx_state = TransactionState::None;
                        watched_keys.clear();
                        drop(q);
                        Frame::Error(
                            "EXECABORT Transaction discarded because of previous errors.".into(),
                        )
                    } else {
                        // check watched keys before executing
                        let watches_ok = check_watched_keys(watched_keys, engine).await;
                        watched_keys.clear();

                        if !watches_ok {
                            let q = std::mem::take(queue);
                            *tx_state = TransactionState::None;
                            drop(q);
                            return Frame::Null;
                        }

                        let q = std::mem::take(queue);
                        *tx_state = TransactionState::None;
                        // replay queued commands serially (process() handles metrics)
                        let mut results = Vec::with_capacity(q.len());
                        for queued_frame in q {
                            let response = super::dispatch::process(
                                queued_frame,
                                engine,
                                ctx,
                                slow_log,
                                pubsub,
                                asking,
                                peer_addr,
                                client_id,
                                session,
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
                    watched_keys.clear();
                    drop(q);
                    Frame::Simple("OK".into())
                }
                Some("UNWATCH") => {
                    // inside MULTI, UNWATCH is a no-op (Redis compat)
                    queue.push(frame);
                    Frame::Simple("QUEUED".into())
                }
                _ => {
                    // validate the command parses correctly before queuing
                    match Command::from_frame(frame.clone()) {
                        Ok(cmd) => {
                            // AUTH and QUIT execute immediately, not queued
                            if matches!(cmd, Command::Auth { .. } | Command::Quit) {
                                super::dispatch::process(
                                    frame, engine, ctx, slow_log, pubsub, asking, peer_addr,
                                    client_id, session,
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
                    }
                }
            }
        }
    }
}

/// Re-queries all watched key versions and returns true if none have changed.
async fn check_watched_keys(watched: &[(String, Option<u64>)], engine: &Engine) -> bool {
    for (key, original_ver) in watched {
        let current = match engine
            .route(key, ShardRequest::KeyVersion { key: key.clone() })
            .await
        {
            Ok(ShardResponse::Version(v)) => v,
            _ => None,
        };
        if current != *original_ver {
            return false;
        }
    }
    true
}

/// Handles a BLPOP or BRPOP command.
///
/// Blocking list pops break the pipeline model — the connection may need to
/// wait for data to arrive. For each key, a oneshot waiter channel is sent
/// to the owning shard. The first shard to respond wins; the rest are
/// dropped (shard detects dead waiters and discards them).
pub(super) async fn handle_blocking_pop_cmd(
    frame: Frame,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    asking: &mut bool,
) -> Frame {
    let cmd = match Command::from_frame(frame) {
        Ok(cmd) => cmd,
        Err(e) => return Frame::Error(format!("ERR {e}")),
    };

    if let Some(err) = validate_command_sizes(
        &cmd,
        ctx.limits.max_key_len,
        ctx.limits.max_value_len,
        ctx.limits.max_command_memory,
    ) {
        return err;
    }

    let was_asking = std::mem::take(asking);
    if let Some(redirect) = super::dispatch::cluster_slot_check(ctx, &cmd, was_asking).await {
        return redirect;
    }

    let (keys, timeout_secs, is_left) = match cmd {
        Command::BLPop { keys, timeout_secs } => (keys, timeout_secs, true),
        Command::BRPop { keys, timeout_secs } => (keys, timeout_secs, false),
        _ => return Frame::Error("ERR expected BLPOP or BRPOP".into()),
    };

    let needs_timing = ctx.metrics_enabled || slow_log.is_enabled();
    let start = if needs_timing {
        Some(Instant::now())
    } else {
        None
    };

    // create a single mpsc channel — all shards race to deliver the first
    // result. capacity of 1 ensures only the first pop is delivered.
    let (waiter_tx, mut waiter_rx) = mpsc::channel(1);

    for key in &keys {
        let idx = engine.shard_for_key(key);
        let req = if is_left {
            ShardRequest::BLPop {
                key: key.clone(),
                waiter: waiter_tx.clone(),
            }
        } else {
            ShardRequest::BRPop {
                key: key.clone(),
                waiter: waiter_tx.clone(),
            }
        };
        // dispatch — we ignore the reply channel since blocking ops
        // communicate through the waiter mpsc instead
        if let Err(e) = engine.dispatch_to_shard(idx, req).await {
            return Frame::Error(format!("ERR {e}"));
        }
    }

    // drop our copy of the sender so the channel closes when all shard
    // waiters are gone (timeout or immediate pop)
    drop(waiter_tx);

    // race the receiver against the timeout. timeout of 0 means block
    // indefinitely — cap at 300 seconds to prevent truly infinite waits.
    let timeout_dur = if timeout_secs == 0.0 {
        Duration::from_secs(300)
    } else {
        // the parser only allows finite, non-negative values, but a huge one
        // still overflows Duration, so saturate instead of panicking
        Duration::try_from_secs_f64(timeout_secs).unwrap_or(Duration::MAX)
    };

    let result = tokio::time::timeout(timeout_dur, waiter_rx.recv()).await;

    let cmd_name = if is_left { "blpop" } else { "brpop" };

    let response = match result {
        // got a result before timeout
        Ok(Some((key, data))) => Frame::Array(vec![
            Frame::Bulk(Bytes::from(key.into_bytes())),
            Frame::Bulk(data),
        ]),
        // channel closed (all shards had empty lists and no push arrived)
        // or timeout expired
        Ok(None) | Err(_) => Frame::Null,
    };

    if let Some(start) = start {
        let elapsed = start.elapsed();
        slow_log.maybe_record(elapsed, cmd_name);
        if ctx.metrics_enabled {
            let is_error = matches!(&response, Frame::Error(_));
            crate::metrics::record_command(cmd_name, elapsed, is_error);
        }
    }
    ctx.commands_processed.fetch_add(1, Ordering::Relaxed);

    response
}

/// Subscriber mode: listens for both broadcast messages and client commands.
///
/// Streams all commands processed by the server to this connection.
///
/// Subscribes to the `monitor_tx` broadcast channel and writes each
/// until the client disconnects.
pub(super) async fn handle_monitor_mode<S>(
    stream: &mut S,
    buf: &mut BytesMut,
    ctx: &Arc<ServerContext>,
    peer_addr: &str,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut rx = ctx.monitor_tx.subscribe();
    let mut out = BytesMut::with_capacity(4096);
    let _ = peer_addr; // available for future per-client filtering

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
                        // subscriber fell behind — skip missed events
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
            // detect client disconnect
            result = stream.read_buf(buf) => {
                match result {
                    Ok(0) | Err(_) => return Ok(()),
                    Ok(_) => {
                        // discard any data sent while in monitor mode
                        // (redis does the same — MONITOR clients can't send commands)
                        buf.clear();
                    }
                }
            }
        }
    }
}

/// How subscriber mode ended.
pub(super) enum SubscriberExit {
    /// The client dropped its last subscription and is back in normal mode.
    Unsubscribed,
    /// The connection is finished: closed, idle too long, or broken.
    Close,
}

/// In this mode the connection can only process SUBSCRIBE, UNSUBSCRIBE,
/// PSUBSCRIBE, PUNSUBSCRIBE, and PING. All other commands return an error.
/// `first` is the command that entered the mode.
///
/// Returns when the last subscription is removed, leaving any later frames
/// in `buf` for normal mode, or when the connection should close.
pub(super) async fn handle_subscriber_mode<S>(
    stream: &mut S,
    buf: &mut BytesMut,
    out: &mut BytesMut,
    ctx: &Arc<ServerContext>,
    pubsub: &Arc<PubSubManager>,
    session: &Session,
    first: Frame,
) -> Result<SubscriberExit, Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // track subscriptions: channel/pattern -> receiver. the guard releases
    // them on every return path below.
    let mut subs = Subscriptions {
        pubsub,
        channels: HashMap::new(),
        patterns: HashMap::new(),
    };
    handle_subscriber_frame(first, ctx, session, &mut subs, out);

    loop {
        // the client may have pipelined more commands behind the first one
        let exit = run_buffered_frames(buf, out, ctx, session, &mut subs);
        if !out.is_empty() {
            stream.write_all(out).await?;
            out.clear();
        }
        if let Some(exit) = exit {
            return Ok(exit);
        }

        tokio::select! {
            msg = recv_any_message(&mut subs.channels, &mut subs.patterns) => {
                if let Some(msg) = msg {
                    serialize_push_message(&msg, out);
                }
            }
            // no idle timeout here: a subscriber may only listen, and
            // Redis doesn't apply `timeout` to pub/sub clients either
            result = stream.read_buf(buf) => {
                match result {
                    Ok(0) => return Ok(SubscriberExit::Close),
                    Ok(_) if buf.len() > ctx.limits.max_buf_size => {
                        return Ok(SubscriberExit::Close);
                    }
                    Ok(_) => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
}

/// Runs the complete frames in `buf`. Stops once no subscriptions are
/// left, so the frames after the last UNSUBSCRIBE go to normal mode.
/// Returns how subscriber mode should end, or `None` to keep going.
fn run_buffered_frames(
    buf: &mut BytesMut,
    out: &mut BytesMut,
    ctx: &Arc<ServerContext>,
    session: &Session,
    subs: &mut Subscriptions<'_>,
) -> Option<SubscriberExit> {
    loop {
        if subs.channels.is_empty() && subs.patterns.is_empty() {
            // no more subscriptions: back to normal mode, as in Redis
            return Some(SubscriberExit::Unsubscribed);
        }
        match parse_request(buf) {
            Ok(Some((frame, consumed))) => {
                let _ = buf.split_to(consumed);
                handle_subscriber_frame(frame, ctx, session, subs, out);
            }
            Ok(None) => return None,
            Err(e) => {
                Frame::Error(format!("ERR protocol error: {e}")).serialize(out);
                return Some(SubscriberExit::Close);
            }
        }
    }
}

/// Runs one command in subscriber mode.
fn handle_subscriber_frame(
    frame: Frame,
    ctx: &Arc<ServerContext>,
    session: &Session,
    subs: &mut Subscriptions<'_>,
    out: &mut BytesMut,
) {
    let cmd = match Command::from_frame(frame) {
        Ok(cmd) => cmd,
        Err(e) => return Frame::Error(format!("ERR {e}")).serialize(out),
    };
    match cmd {
        Command::Subscribe { .. }
        | Command::Unsubscribe { .. }
        | Command::PSubscribe { .. }
        | Command::PUnsubscribe { .. } => match session.check(&cmd) {
            Some(err) => err.serialize(out),
            None => handle_sub_command(
                cmd,
                ctx,
                subs.pubsub,
                &mut subs.channels,
                &mut subs.patterns,
                out,
            ),
        },
        Command::Ping(msg) => match msg {
            Some(m) => Frame::Bulk(m).serialize(out),
            None => Frame::Simple("PONG".into()).serialize(out),
        },
        _ => Frame::Error(
            "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING are allowed in this context".into(),
        )
        .serialize(out),
    }
}

/// Processes a subscribe/unsubscribe command, updating the subscription maps
/// and writing RESP3 responses.
fn handle_sub_command(
    cmd: Command,
    ctx: &Arc<ServerContext>,
    pubsub: &PubSubManager,
    channel_rxs: &mut HashMap<String, broadcast::Receiver<PubMessage>>,
    pattern_rxs: &mut HashMap<String, broadcast::Receiver<PubMessage>>,
    out: &mut BytesMut,
) {
    match cmd {
        Command::Subscribe { channels } => {
            for ch in channels {
                let total = channel_rxs.len() + pattern_rxs.len();
                if total >= ctx.limits.max_subscriptions_per_conn {
                    Frame::Error("ERR max subscriptions per connection reached".into())
                        .serialize(out);
                    continue;
                }
                // subscribing twice to the same channel is a no-op, as in Redis
                if !channel_rxs.contains_key(&ch) {
                    channel_rxs.insert(ch.clone(), pubsub.subscribe(&ch));
                }
                let count = channel_rxs.len() + pattern_rxs.len();
                serialize_sub_response(b"subscribe", &ch, count, out);
            }
        }
        Command::Unsubscribe { channels } => unsubscribe(
            channels,
            channel_rxs,
            pattern_rxs.len(),
            b"unsubscribe",
            |ch, rx| pubsub.unsubscribe(ch, rx),
            out,
        ),
        Command::PSubscribe { patterns } => {
            for pat in patterns {
                if pat.len() > ctx.limits.max_pattern_len {
                    Frame::Error(format!(
                        "ERR pattern too long ({} bytes, max {})",
                        pat.len(),
                        ctx.limits.max_pattern_len
                    ))
                    .serialize(out);
                    continue;
                }
                let total = channel_rxs.len() + pattern_rxs.len();
                if total >= ctx.limits.max_subscriptions_per_conn {
                    Frame::Error("ERR max subscriptions per connection reached".into())
                        .serialize(out);
                    continue;
                }
                // psubscribe returns None if the pattern exceeds its internal
                // length cap — this is a backstop; the check above should have
                // already rejected oversized patterns.
                let Some(rx) = pubsub.psubscribe(&pat) else {
                    Frame::Error(format!("ERR pattern too long ({} bytes)", pat.len()))
                        .serialize(out);
                    continue;
                };
                pattern_rxs.insert(pat.clone(), rx);
                let count = channel_rxs.len() + pattern_rxs.len();
                serialize_sub_response(b"psubscribe", &pat, count, out);
            }
        }
        Command::PUnsubscribe { patterns } => unsubscribe(
            patterns,
            pattern_rxs,
            channel_rxs.len(),
            b"punsubscribe",
            |pat, rx| pubsub.punsubscribe(pat, rx),
            out,
        ),
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

/// Handles UNSUBSCRIBE and PUNSUBSCRIBE. With no names it drops every
/// subscription in `rxs`. Replies once per name, including names the client
/// was not subscribed to, as Redis does. `other_count` is the number of
/// subscriptions of the other kind, which the reply count includes.
fn unsubscribe(
    names: Vec<String>,
    rxs: &mut HashMap<String, broadcast::Receiver<PubMessage>>,
    other_count: usize,
    kind: &'static [u8],
    release: impl Fn(&str, broadcast::Receiver<PubMessage>),
    out: &mut BytesMut,
) {
    let names = if names.is_empty() {
        rxs.keys().cloned().collect()
    } else {
        names
    };
    if names.is_empty() {
        serialize_sub_response(kind, "", other_count, out);
        return;
    }
    for name in names {
        if let Some(rx) = rxs.remove(&name) {
            release(&name, rx);
        }
        serialize_sub_response(kind, &name, rxs.len() + other_count, out);
    }
}

/// The subscriptions one connection holds in subscriber mode. Dropping it
/// unsubscribes from everything, so the subscriptions are released on
/// every exit, including early returns on I/O errors.
struct Subscriptions<'a> {
    pubsub: &'a PubSubManager,
    channels: HashMap<String, broadcast::Receiver<PubMessage>>,
    patterns: HashMap<String, broadcast::Receiver<PubMessage>>,
}

impl Drop for Subscriptions<'_> {
    fn drop(&mut self) {
        for (ch, rx) in self.channels.drain() {
            self.pubsub.unsubscribe(&ch, rx);
        }
        for (pat, rx) in self.patterns.drain() {
            self.pubsub.punsubscribe(&pat, rx);
        }
    }
}
