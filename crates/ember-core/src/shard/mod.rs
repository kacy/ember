//! Shard: an independent partition of the keyspace.
//!
//! ## thread-per-core, shared-nothing model
//!
//! Each shard runs as a single tokio task that exclusively owns its [`Keyspace`]
//! partition. All mutations execute serially inside one task — no mutex, no
//! read-write lock, no cross-thread coordination on the hot path. This is the
//! core design choice that enables predictable latency: a shard thread can never
//! be stalled waiting for another thread to release a lock.
//!
//! ## backpressure via bounded channel
//!
//! The mpsc buffer (4,096 items) is the system's flow-control valve. When a shard
//! is fully loaded, `try_send` returns `Err(Full)` immediately, giving the caller a
//! clear signal to shed load or return an error to the client rather than quietly
//! growing an unbounded queue. This prevents memory blow-up under sustained overload.
//!
//! ## per-request oneshot channels
//!
//! Each [`ShardMessage`] carries a `oneshot::Sender<ShardResponse>`. The caller
//! blocks on the receiver while the shard processes the request. This gives O(1)
//! delivery with no shared response queue and no head-of-line blocking between
//! concurrent callers — each caller waits only on its own future.
//!
//! ## pipeline draining
//!
//! After waking from `select!`, the event loop drains the channel with `try_recv()`
//! before re-entering `select!`. This amortizes scheduler wake-up overhead across
//! burst traffic — essential for pipelined clients that send dozens of commands
//! back-to-back without waiting for individual responses.
//!
//! ## AOF linearizability
//!
//! Because all mutations execute serially in one task, the AOF record sequence is a
//! perfect linearized history of the keyspace. The per-shard monotonically increasing
//! offset is a consistent position marker that replicas use to detect gaps and
//! trigger re-sync when they fall behind.
//!
//! ## mechanical sympathy
//!
//! The hot path (`recv → dispatch → respond`) touches only shard-local memory. No
//! cross-thread cache-line contention. This is why sharded mode outperforms
//! mutex-per-command designs by 3–5× on write-heavy workloads — the CPU's cache
//! hierarchy can stay warm on data that belongs to this core.

mod aof;
pub use aof::from_aof_record;
mod blocking;
mod dispatch;
mod persistence;
mod request;

use dispatch::dispatch;
pub use request::{ShardMessage, ShardRequest, ShardResponse};

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ember_persistence::aof::{AofRecord, AofWriter, FsyncPolicy};
use ember_persistence::recovery;
use ember_persistence::snapshot::{self, SnapEntry, SnapValue, SnapshotWriter};
use smallvec::{smallvec, SmallVec};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, info, warn};

use crate::dropper::DropHandle;
use crate::error::ShardError;
use crate::expiry;
use crate::keyspace::{
    EvictionPolicy, IncrError, IncrFloatError, Keyspace, KeyspaceStats, LsetError, SetResult,
    ShardConfig, TtlResult, WriteError,
};
use crate::types::sorted_set::{ScoreBound, ZAddFlags};
use crate::types::Value;
use ember_protocol::command::{BitOpKind, BitRange};

/// A mutation event broadcast to replication subscribers.
///
/// Published after every successful mutation on the hot path. The
/// `offset` is per-shard and monotonically increasing — replicas use it
/// to detect gaps and trigger re-sync when they fall behind.
#[derive(Debug, Clone)]
pub struct ReplicationEvent {
    /// The shard that produced this event.
    pub shard_id: u16,
    /// Monotonically increasing per-shard offset.
    pub offset: u64,
    /// The mutation record, ready to replay on a replica.
    pub record: AofRecord,
}

/// Optional persistence configuration for a shard.
#[derive(Debug, Clone)]
pub struct ShardPersistenceConfig {
    /// Directory where AOF and snapshot files live.
    pub data_dir: PathBuf,
    /// Whether to write an AOF log of mutations.
    pub append_only: bool,
    /// When to fsync the AOF file.
    pub fsync_policy: FsyncPolicy,
    /// Optional encryption key for encrypting data at rest.
    /// When set, AOF and snapshot files use the v3 encrypted format.
    #[cfg(feature = "encryption")]
    pub encryption_key: Option<ember_persistence::encryption::EncryptionKey>,
}

/// A cloneable handle for sending commands to a shard task.
///
/// Wraps the mpsc sender so callers don't need to manage oneshot
/// channels directly.
#[derive(Debug, Clone)]
pub struct ShardHandle {
    tx: mpsc::Sender<ShardMessage>,
    replication_offset: Arc<AtomicU64>,
}

impl ShardHandle {
    /// Returns the offset of the last replication event this shard sent.
    ///
    /// The shard advances it before replying to a write, so once a client
    /// has its reply the offset already covers that write. WAIT uses this
    /// as the point replicas must reach.
    pub fn replication_offset(&self) -> u64 {
        self.replication_offset.load(Ordering::Acquire)
    }

    /// Sends a request and waits for the response.
    ///
    /// Returns `ShardError::Unavailable` if the shard task has stopped.
    pub async fn send(&self, request: ShardRequest) -> Result<ShardResponse, ShardError> {
        let rx = self.dispatch(request).await?;
        rx.await.map_err(|_| ShardError::Unavailable)
    }

    /// Sends a request and returns the reply channel without waiting
    /// for the response. Used by `Engine::broadcast` to fan out to
    /// all shards before collecting results, and by
    /// `Engine::dispatch_to_shard` for the dispatch-collect pipeline.
    pub async fn dispatch(
        &self,
        request: ShardRequest,
    ) -> Result<oneshot::Receiver<ShardResponse>, ShardError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let msg = ShardMessage::Single {
            request,
            reply: reply_tx,
        };
        self.tx
            .send(msg)
            .await
            .map_err(|_| ShardError::Unavailable)?;
        Ok(reply_rx)
    }

    /// Sends a request using a caller-owned reply channel.
    ///
    /// Unlike `dispatch()`, this doesn't allocate a oneshot per call.
    /// The caller reuses the same `mpsc::Sender` across commands, saving
    /// a heap allocation per P=1 round-trip.
    pub async fn dispatch_reusable(
        &self,
        request: ShardRequest,
        reply: mpsc::Sender<ShardResponse>,
    ) -> Result<(), ShardError> {
        self.tx
            .send(ShardMessage::SingleReusable { request, reply })
            .await
            .map_err(|_| ShardError::Unavailable)
    }

    /// Sends a batch of requests as a single channel message.
    ///
    /// Returns one receiver per request, in the same order. For a single
    /// request, falls through to `dispatch()` to avoid the batch overhead.
    ///
    /// This is the key optimization for pipelining: N commands targeting
    /// the same shard consume 1 channel slot instead of N.
    pub async fn dispatch_batch(
        &self,
        requests: Vec<ShardRequest>,
    ) -> Result<Vec<oneshot::Receiver<ShardResponse>>, ShardError> {
        if requests.len() == 1 {
            let rx = self
                .dispatch(requests.into_iter().next().expect("len == 1"))
                .await?;
            return Ok(vec![rx]);
        }
        let mut receivers = Vec::with_capacity(requests.len());
        let mut entries = Vec::with_capacity(requests.len());
        for request in requests {
            let (tx, rx) = oneshot::channel();
            entries.push((request, tx));
            receivers.push(rx);
        }
        self.tx
            .send(ShardMessage::Batch(entries))
            .await
            .map_err(|_| ShardError::Unavailable)?;
        Ok(receivers)
    }
}

/// Everything needed to run a shard on a specific runtime.
///
/// Created by [`prepare_shard`] without spawning any tasks. The caller
/// is responsible for passing this to [`run_prepared`] on the desired
/// tokio runtime — this is the hook that enables thread-per-core
/// deployment where each worker thread runs its own shard.
pub struct PreparedShard {
    rx: mpsc::Receiver<ShardMessage>,
    config: ShardConfig,
    persistence: Option<ShardPersistenceConfig>,
    drop_handle: Option<DropHandle>,
    replication_tx: Option<broadcast::Sender<ReplicationEvent>>,
    replication_offset: Arc<AtomicU64>,
    /// Optional channel to broadcast expired key names for keyspace notifications.
    expired_tx: Option<broadcast::Sender<String>>,
    #[cfg(feature = "protobuf")]
    schema_registry: Option<crate::schema::SharedSchemaRegistry>,
}

/// Creates the channel and prepared shard without spawning any tasks.
///
/// Returns the `ShardHandle` for sending commands and the `PreparedShard`
/// that must be driven on the target runtime via [`run_prepared`].
pub fn prepare_shard(
    buffer: usize,
    config: ShardConfig,
    persistence: Option<ShardPersistenceConfig>,
    drop_handle: Option<DropHandle>,
    replication_tx: Option<broadcast::Sender<ReplicationEvent>>,
    expired_tx: Option<broadcast::Sender<String>>,
    #[cfg(feature = "protobuf")] schema_registry: Option<crate::schema::SharedSchemaRegistry>,
) -> (ShardHandle, PreparedShard) {
    let (tx, rx) = mpsc::channel(buffer);
    let replication_offset = Arc::new(AtomicU64::new(0));
    let prepared = PreparedShard {
        rx,
        config,
        persistence,
        drop_handle,
        replication_tx,
        replication_offset: Arc::clone(&replication_offset),
        expired_tx,
        #[cfg(feature = "protobuf")]
        schema_registry,
    };
    (
        ShardHandle {
            tx,
            replication_offset,
        },
        prepared,
    )
}

/// Runs the shard's main loop. Call this on the target runtime.
///
/// Consumes the `PreparedShard` and enters the infinite recv/expiry/fsync
/// select loop. Returns when the channel is closed (all senders dropped).
pub async fn run_prepared(prepared: PreparedShard) {
    run_shard(prepared).await
}

/// Spawns a shard task and returns the handle for communicating with it.
///
/// `buffer` controls the mpsc channel capacity — higher values absorb
/// burst traffic at the cost of memory. When `drop_handle` is provided,
/// large value deallocations are deferred to the background drop thread.
///
/// This is a convenience wrapper around [`prepare_shard`] + [`run_prepared`]
/// for the common case where you want to spawn on the current runtime.
pub fn spawn_shard(
    buffer: usize,
    config: ShardConfig,
    persistence: Option<ShardPersistenceConfig>,
    drop_handle: Option<DropHandle>,
    replication_tx: Option<broadcast::Sender<ReplicationEvent>>,
    expired_tx: Option<broadcast::Sender<String>>,
    #[cfg(feature = "protobuf")] schema_registry: Option<crate::schema::SharedSchemaRegistry>,
) -> ShardHandle {
    let (handle, prepared) = prepare_shard(
        buffer,
        config,
        persistence,
        drop_handle,
        replication_tx,
        expired_tx,
        #[cfg(feature = "protobuf")]
        schema_registry,
    );
    tokio::spawn(run_prepared(prepared));
    handle
}

/// The shard's main loop. Processes messages and runs periodic
/// active expiration until the channel closes.
async fn run_shard(prepared: PreparedShard) {
    let PreparedShard {
        mut rx,
        config,
        persistence,
        drop_handle,
        replication_tx,
        replication_offset,
        expired_tx,
        #[cfg(feature = "protobuf")]
        schema_registry,
    } = prepared;
    let shard_id = config.shard_id;
    // tokio's interval panics on zero, so hold both to at least 1ms
    let expiry_interval = config.expiry_interval.max(Duration::from_millis(1));
    let fsync_interval = config.fsync_interval.max(Duration::from_millis(1));
    let mut keyspace = Keyspace::with_config(config);

    if let Some(handle) = drop_handle.clone() {
        keyspace.set_drop_handle(handle);
    }

    // -- recovery --
    let mut stale_aof = false;
    if let Some(ref pcfg) = persistence {
        stale_aof = persistence::recover(
            &mut keyspace,
            pcfg,
            shard_id,
            #[cfg(feature = "protobuf")]
            &schema_registry,
        );
    }

    // -- AOF writer --
    let mut aof_writer: Option<AofWriter> = match &persistence {
        Some(pcfg) if pcfg.append_only => {
            let path = ember_persistence::aof::aof_path(&pcfg.data_dir, shard_id);
            #[cfg(feature = "encryption")]
            let result = if let Some(ref key) = pcfg.encryption_key {
                AofWriter::open_encrypted(path, key.clone())
            } else {
                AofWriter::open(path)
            };
            #[cfg(not(feature = "encryption"))]
            let result = AofWriter::open(path);
            match result {
                Ok(w) => Some(w),
                Err(e) => {
                    warn!(shard_id, "failed to open AOF writer: {e}");
                    None
                }
            }
        }
        _ => None,
    };

    // the AOF is in another format, since encryption was turned on or off,
    // or recovery skipped it as older than the snapshot. either way its
    // contents are loaded, so save them as a snapshot and start the AOF
    // over before anything is appended.
    if stale_aof || aof_writer.as_ref().is_some_and(AofWriter::needs_rewrite) {
        info!(shard_id, "rewriting the aof from a snapshot");
        if let ShardResponse::Err(e) = persistence::handle_snapshot(
            &keyspace,
            &persistence,
            &mut aof_writer,
            shard_id,
            #[cfg(feature = "protobuf")]
            &schema_registry,
        ) {
            error!(shard_id, "aof rewrite failed, not writing to it: {e}");
            aof_writer = None;
        }
    }

    let fsync_policy = persistence
        .as_ref()
        .map(|p| p.fsync_policy)
        .unwrap_or(FsyncPolicy::No);

    // waiter registries for blocking list operations (BLPOP/BRPOP)
    let mut lpop_waiters: HashMap<String, VecDeque<mpsc::Sender<(String, Bytes)>>> = HashMap::new();
    let mut rpop_waiters: HashMap<String, VecDeque<mpsc::Sender<(String, Bytes)>>> = HashMap::new();

    // consecutive AOF write/sync failure counter for rate-limited logging
    let mut aof_errors: u32 = 0;

    // when true, write commands are rejected with an error until disk
    // space is available again (detected on the periodic fsync tick).
    let mut disk_full: bool = false;

    // -- tickers --
    let mut expiry_tick = tokio::time::interval(expiry_interval);
    expiry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut fsync_tick = tokio::time::interval(fsync_interval);
    fsync_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(msg) => {
                        let mut ctx = ProcessCtx {
                            keyspace: &mut keyspace,
                            aof_writer: &mut aof_writer,
                            fsync_policy,
                            persistence: &persistence,
                            drop_handle: &drop_handle,
                            shard_id,
                            replication_tx: &replication_tx,
                            replication_offset: &replication_offset,
                            lpop_waiters: &mut lpop_waiters,
                            rpop_waiters: &mut rpop_waiters,
                            aof_errors: &mut aof_errors,
                            disk_full: &mut disk_full,
                            #[cfg(feature = "protobuf")]
                            schema_registry: &schema_registry,
                        };
                        process_message(msg, &mut ctx);

                        // drain any pending messages without re-entering select!.
                        // this amortizes the select! overhead across bursts of
                        // pipelined commands that arrived while we processed the
                        // first message.
                        while let Ok(msg) = rx.try_recv() {
                            process_message(msg, &mut ctx);
                        }
                    }
                    None => break, // channel closed, shard shutting down
                }
            }
            _ = expiry_tick.tick() => {
                let expired_keys = expiry::run_expiration_cycle(&mut keyspace);
                if let Some(ref tx) = expired_tx {
                    if !expired_keys.is_empty() && tx.receiver_count() > 0 {
                        for key in &expired_keys {
                            let _ = tx.send(key.clone());
                        }
                    }
                }
                blocking::prune_waiters(&mut lpop_waiters);
                blocking::prune_waiters(&mut rpop_waiters);
            }
            // also tick while the disk is full, whatever the fsync policy:
            // writes are rejected then, so a successful sync here is the
            // only way the shard learns there is space again
            _ = fsync_tick.tick(), if fsync_policy == FsyncPolicy::EverySec || disk_full => {
                if let Some(ref mut writer) = aof_writer {
                    if let Err(e) = writer.sync() {
                        if aof::log_aof_error(shard_id, &mut aof_errors, "sync", &e) {
                            disk_full = true;
                        }
                    } else if aof_errors > 0 {
                        let missed = aof_errors;
                        aof_errors = 0;
                        if disk_full {
                            disk_full = false;
                            info!(shard_id, missed_errors = missed, "aof sync recovered, accepting writes again");
                        } else {
                            info!(shard_id, missed_errors = missed, "aof sync recovered");
                        }
                    }
                }
            }
        }
    }

    // flush AOF on clean shutdown
    if let Some(ref mut writer) = aof_writer {
        if let Err(e) = writer.sync() {
            error!(shard_id, "final aof sync failed on shutdown, writes since the last successful sync may be lost: {e}");
        }
    }
}

/// Per-shard processing context passed into `process_message`.
///
/// Groups the mutable and configuration fields so the call site stays
/// readable and the parameter count stays reasonable.
struct ProcessCtx<'a> {
    keyspace: &'a mut Keyspace,
    aof_writer: &'a mut Option<AofWriter>,
    fsync_policy: FsyncPolicy,
    persistence: &'a Option<ShardPersistenceConfig>,
    drop_handle: &'a Option<DropHandle>,
    shard_id: u16,
    replication_tx: &'a Option<broadcast::Sender<ReplicationEvent>>,
    /// Offset of the last replication event sent. Shared with the
    /// `ShardHandle` so WAIT can read it.
    replication_offset: &'a AtomicU64,
    /// Waiters for BLPOP — keyed by list name, FIFO order.
    lpop_waiters: &'a mut HashMap<String, VecDeque<mpsc::Sender<(String, Bytes)>>>,
    /// Waiters for BRPOP — keyed by list name, FIFO order.
    rpop_waiters: &'a mut HashMap<String, VecDeque<mpsc::Sender<(String, Bytes)>>>,
    /// Consecutive AOF write/sync failures. Used to rate-limit error logging
    /// so a sustained disk-full condition doesn't flood logs.
    aof_errors: &'a mut u32,
    /// When true, write commands are rejected until disk space recovers.
    disk_full: &'a mut bool,
    #[cfg(feature = "protobuf")]
    schema_registry: &'a Option<crate::schema::SharedSchemaRegistry>,
}

/// Dispatches a single or batched message to the shard's keyspace.
///
/// Called both in the main `recv()` path and in the `try_recv()` drain loop
/// to amortize tokio select overhead across pipelined commands.
fn process_message(msg: ShardMessage, ctx: &mut ProcessCtx<'_>) {
    match msg {
        ShardMessage::Single { request, reply } => {
            process_single(request, ReplySender::Oneshot(reply), ctx);
        }
        ShardMessage::SingleReusable { request, reply } => {
            process_single(request, ReplySender::Reusable(reply), ctx);
        }
        ShardMessage::Batch(entries) => {
            for (request, reply) in entries {
                process_single(request, ReplySender::Oneshot(reply), ctx);
            }
        }
    }
}

/// Wraps either a oneshot or reusable mpsc sender for reply delivery.
///
/// The oneshot path is used for pipelined and batch commands. The reusable
/// path avoids per-command allocation for the P=1 fast path.
enum ReplySender {
    Oneshot(oneshot::Sender<ShardResponse>),
    Reusable(mpsc::Sender<ShardResponse>),
}

impl ReplySender {
    fn send(self, response: ShardResponse) {
        match self {
            ReplySender::Oneshot(tx) => {
                let _ = tx.send(response);
            }
            ReplySender::Reusable(tx) => {
                // capacity is 1 and the receiver always drains before
                // sending the next command, so try_send won't fail
                // under normal operation.
                if let Err(e) = tx.try_send(response) {
                    debug!("reusable reply channel full or closed: {e}");
                }
            }
        }
    }
}

/// Processes a single request: dispatch, write AOF, broadcast replication,
/// and send the response on the reply channel.
fn process_single(mut request: ShardRequest, reply: ReplySender, ctx: &mut ProcessCtx<'_>) {
    // copy cheap fields upfront to avoid field-borrow conflicts below
    let fsync_policy = ctx.fsync_policy;
    let shard_id = ctx.shard_id;

    // reject writes when AOF is enabled and disk is full. reads and admin
    // commands still go through so operators can inspect and recover.
    if *ctx.disk_full && ctx.aof_writer.is_some() && request.is_write() {
        reply.send(ShardResponse::Err(
            "ERR disk full, write rejected — free disk space to resume writes".into(),
        ));
        return;
    }

    // handle blocking pop requests before dispatch — they carry a waiter
    // oneshot that must be consumed here rather than going through the
    // normal dispatch → response path.
    match request {
        ShardRequest::BLPop { key, waiter } => {
            blocking::handle_blocking_pop(&key, waiter, true, reply, ctx);
            return;
        }
        ShardRequest::BRPop { key, waiter } => {
            blocking::handle_blocking_pop(&key, waiter, false, reply, ctx);
            return;
        }
        _ => {}
    }

    let request_kind = describe_request(&request);
    let mut response = dispatch(
        ctx.keyspace,
        &mut request,
        #[cfg(feature = "protobuf")]
        ctx.schema_registry,
    );

    // a successful push, or an LMOVE, COPY or RESTORE into a key, may feed
    // blocked clients. remember the key now,
    // because the request is consumed below, but wake the waiters only after
    // the push is logged: their pops must follow it in the AOF and in the
    // replication stream.
    let pushed_key = match (&request, &response) {
        (
            ShardRequest::LPush { key, .. } | ShardRequest::RPush { key, .. },
            ShardResponse::Len(_),
        )
        | (
            ShardRequest::LMove {
                destination: key, ..
            },
            ShardResponse::Value(Some(_)),
        )
        | (
            ShardRequest::Copy {
                destination: key, ..
            },
            ShardResponse::Bool(true),
        )
        | (ShardRequest::RestoreKey { key, .. }, ShardResponse::Ok) => Some(key.clone()),
        _ => None,
    };

    // consume the request to move owned data into AOF records (avoids cloning).
    // response is &mut so VAddBatch can steal applied entries instead of cloning
    // vectors — the connection handler only uses added_count.
    let kept_ttl = aof::incr_float_ttl(ctx.keyspace, &request, &response);
    let mut records = aof::to_aof_records(request, &mut response);
    records.extend(kept_ttl);

    // write AOF records for successful mutations
    if let Some(ref mut writer) = *ctx.aof_writer {
        let mut batch_ok = true;
        for record in &records {
            if let Err(e) = writer.write_record(record) {
                if aof::log_aof_error(shard_id, ctx.aof_errors, "write", &e) {
                    *ctx.disk_full = true;
                }
                batch_ok = false;
            }
        }
        if !records.is_empty() && fsync_policy == FsyncPolicy::Always {
            if let Err(e) = writer.sync() {
                if aof::log_aof_error(shard_id, ctx.aof_errors, "sync", &e) {
                    *ctx.disk_full = true;
                }
                batch_ok = false;
            }
        }
        if batch_ok && *ctx.aof_errors > 0 {
            let missed = *ctx.aof_errors;
            *ctx.aof_errors = 0;
            *ctx.disk_full = false;
            info!(shard_id, missed_errors = missed, "aof writes recovered");
        }
        // with appendfsync always, OK promises the write is on disk
        if !batch_ok && fsync_policy == FsyncPolicy::Always {
            response = ShardResponse::Err(
                "ERR write applied in memory but could not be persisted to the AOF".into(),
            );
        }
    }

    // broadcast mutation events to replication subscribers
    if let Some(ref tx) = *ctx.replication_tx {
        for record in records {
            let offset = ctx.replication_offset.fetch_add(1, Ordering::Release) + 1;
            // ignore send errors — no subscribers or lagged consumers
            let _ = tx.send(ReplicationEvent {
                shard_id,
                offset,
                record,
            });
        }
    }

    if let Some(key) = pushed_key {
        blocking::wake_blocked_waiters(&key, ctx);
    }

    // handle special requests that need access to persistence state
    match request_kind {
        RequestKind::SerializeSnapshot => {
            // the snapshot covers every event up to the current offset,
            // since this task sends all of them and is busy doing this now
            let offset = ctx.replication_offset.load(Ordering::Acquire);
            let resp = persistence::handle_serialize_snapshot(ctx.keyspace, shard_id, offset);
            reply.send(resp);
            return;
        }
        RequestKind::Snapshot => {
            let resp = persistence::handle_snapshot(
                ctx.keyspace,
                ctx.persistence,
                ctx.aof_writer,
                shard_id,
                #[cfg(feature = "protobuf")]
                ctx.schema_registry,
            );
            reply.send(resp);
            return;
        }
        RequestKind::FlushDbAsync => {
            let old_entries = ctx.keyspace.flush_async();
            if let Some(ref handle) = *ctx.drop_handle {
                handle.defer_entries(old_entries);
            }
            reply.send(ShardResponse::Ok);
            return;
        }
        RequestKind::UpdateMemoryConfig {
            max_memory,
            eviction_policy,
        } => {
            ctx.keyspace
                .update_memory_config(max_memory, eviction_policy);
            reply.send(ShardResponse::Ok);
            return;
        }
        RequestKind::Other => {}
    }

    reply.send(response);
}

/// Lightweight tag so we can identify requests that need special
/// handling after dispatch without borrowing the request again.
enum RequestKind {
    /// BGSAVE and BGREWRITEAOF both write a snapshot and truncate the AOF.
    Snapshot,
    SerializeSnapshot,
    FlushDbAsync,
    UpdateMemoryConfig {
        max_memory: Option<usize>,
        eviction_policy: EvictionPolicy,
    },
    Other,
}

fn describe_request(req: &ShardRequest) -> RequestKind {
    match req {
        ShardRequest::Snapshot | ShardRequest::RewriteAof => RequestKind::Snapshot,
        ShardRequest::SerializeSnapshot => RequestKind::SerializeSnapshot,
        ShardRequest::FlushDbAsync => RequestKind::FlushDbAsync,
        ShardRequest::UpdateMemoryConfig {
            max_memory,
            eviction_policy,
        } => RequestKind::UpdateMemoryConfig {
            max_memory: *max_memory,
            eviction_policy: *eviction_policy,
        },
        _ => RequestKind::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper: dispatch without a schema registry.
    fn test_dispatch(ks: &mut Keyspace, mut req: ShardRequest) -> ShardResponse {
        dispatch(
            ks,
            &mut req,
            #[cfg(feature = "protobuf")]
            &None,
        )
    }

    #[test]
    fn dispatch_rejects_arguments_that_would_allocate_without_bound() {
        let mut ks = Keyspace::new();
        let requests = [
            ShardRequest::SetRange {
                key: "s".into(),
                offset: usize::MAX,
                value: Bytes::from("x"),
            },
            ShardRequest::SetBit {
                key: "b".into(),
                offset: u64::MAX,
                value: 1,
            },
            ShardRequest::SRandMember {
                key: "set".into(),
                count: i64::MIN,
            },
            ShardRequest::ZRandMember {
                key: "z".into(),
                count: Some(i64::MIN),
                with_scores: false,
            },
            ShardRequest::HRandField {
                key: "h".into(),
                count: Some(i64::MIN),
                with_values: false,
            },
        ];
        for req in requests {
            assert!(matches!(test_dispatch(&mut ks, req), ShardResponse::Err(_)));
        }
        assert_eq!(ks.len(), 0);
    }

    #[test]
    fn dispatch_set_and_get() {
        let mut ks = Keyspace::new();

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire: None,
                nx: false,
                xx: false,
            },
        );
        assert!(matches!(resp, ShardResponse::Ok));

        let resp = test_dispatch(&mut ks, ShardRequest::Get { key: "k".into() });
        match resp {
            ShardResponse::Value(Some(Value::String(data))) => {
                assert_eq!(data, Bytes::from("v"));
            }
            other => panic!("expected Value(Some(String)), got {other:?}"),
        }
    }

    #[test]
    fn dispatch_get_missing() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(&mut ks, ShardRequest::Get { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Value(None)));
    }

    #[test]
    fn dispatch_del() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);

        let resp = test_dispatch(&mut ks, ShardRequest::Del { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = test_dispatch(&mut ks, ShardRequest::Del { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Bool(false)));
    }

    #[test]
    fn dispatch_exists() {
        let mut ks = Keyspace::new();
        ks.set("yes".into(), Bytes::from("here"), None, false, false);

        let resp = test_dispatch(&mut ks, ShardRequest::Exists { key: "yes".into() });
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = test_dispatch(&mut ks, ShardRequest::Exists { key: "no".into() });
        assert!(matches!(resp, ShardResponse::Bool(false)));
    }

    #[test]
    fn dispatch_expire_and_ttl() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Expire {
                key: "key".into(),
                seconds: 60,
            },
        );
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = test_dispatch(&mut ks, ShardRequest::Ttl { key: "key".into() });
        match resp {
            ShardResponse::Ttl(TtlResult::Seconds(s)) => assert!((58..=60).contains(&s)),
            other => panic!("expected Ttl(Seconds), got {other:?}"),
        }
    }

    #[test]
    fn dispatch_ttl_missing() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(&mut ks, ShardRequest::Ttl { key: "gone".into() });
        assert!(matches!(resp, ShardResponse::Ttl(TtlResult::NotFound)));
    }

    #[test]
    fn dispatch_incr_new_key() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(&mut ks, ShardRequest::Incr { key: "c".into() });
        assert!(matches!(resp, ShardResponse::Integer(1)));
    }

    #[test]
    fn dispatch_decr_existing() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None, false, false);
        let resp = test_dispatch(&mut ks, ShardRequest::Decr { key: "n".into() });
        assert!(matches!(resp, ShardResponse::Integer(9)));
    }

    #[test]
    fn dispatch_incr_non_integer() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("hello"), None, false, false);
        let resp = test_dispatch(&mut ks, ShardRequest::Incr { key: "s".into() });
        assert!(matches!(resp, ShardResponse::Err(_)));
    }

    #[test]
    fn dispatch_incrby() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None, false, false);
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::IncrBy {
                key: "n".into(),
                delta: 5,
            },
        );
        assert!(matches!(resp, ShardResponse::Integer(15)));
    }

    #[test]
    fn dispatch_decrby() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None, false, false);
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::DecrBy {
                key: "n".into(),
                delta: 3,
            },
        );
        assert!(matches!(resp, ShardResponse::Integer(7)));
    }

    #[test]
    fn dispatch_incrby_new_key() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::IncrBy {
                key: "new".into(),
                delta: 42,
            },
        );
        assert!(matches!(resp, ShardResponse::Integer(42)));
    }

    #[test]
    fn dispatch_incrbyfloat() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10.5"), None, false, false);
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::IncrByFloat {
                key: "n".into(),
                delta: 2.3,
            },
        );
        match resp {
            ShardResponse::BulkString(val) => {
                let f: f64 = val.parse().unwrap();
                assert!((f - 12.8).abs() < 0.001);
            }
            other => panic!("expected BulkString, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_append() {
        let mut ks = Keyspace::new();
        ks.set("k".into(), Bytes::from("hello"), None, false, false);
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Append {
                key: "k".into(),
                value: Bytes::from(" world"),
            },
        );
        assert!(matches!(resp, ShardResponse::Len(11)));
    }

    #[test]
    fn dispatch_strlen() {
        let mut ks = Keyspace::new();
        ks.set("k".into(), Bytes::from("hello"), None, false, false);
        let resp = test_dispatch(&mut ks, ShardRequest::Strlen { key: "k".into() });
        assert!(matches!(resp, ShardResponse::Len(5)));
    }

    #[test]
    fn dispatch_strlen_missing() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(&mut ks, ShardRequest::Strlen { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Len(0)));
    }

    #[test]
    fn dispatch_incrbyfloat_new_key() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::IncrByFloat {
                key: "new".into(),
                delta: 2.72,
            },
        );
        match resp {
            ShardResponse::BulkString(val) => {
                let f: f64 = val.parse().unwrap();
                assert!((f - 2.72).abs() < 0.001);
            }
            other => panic!("expected BulkString, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_persist_removes_ttl() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::from_secs(60)),
            false,
            false,
        );

        let resp = test_dispatch(&mut ks, ShardRequest::Persist { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = test_dispatch(&mut ks, ShardRequest::Ttl { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Ttl(TtlResult::NoExpiry)));
    }

    #[test]
    fn dispatch_persist_missing_key() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(&mut ks, ShardRequest::Persist { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Bool(false)));
    }

    #[test]
    fn dispatch_pttl() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::from_secs(60)),
            false,
            false,
        );

        let resp = test_dispatch(&mut ks, ShardRequest::Pttl { key: "key".into() });
        match resp {
            ShardResponse::Ttl(TtlResult::Milliseconds(ms)) => {
                assert!(ms > 59_000 && ms <= 60_000);
            }
            other => panic!("expected Ttl(Milliseconds), got {other:?}"),
        }
    }

    #[test]
    fn dispatch_pttl_missing() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(&mut ks, ShardRequest::Pttl { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Ttl(TtlResult::NotFound)));
    }

    #[test]
    fn dispatch_pexpire() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Pexpire {
                key: "key".into(),
                milliseconds: 5000,
            },
        );
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = test_dispatch(&mut ks, ShardRequest::Pttl { key: "key".into() });
        match resp {
            ShardResponse::Ttl(TtlResult::Milliseconds(ms)) => {
                assert!(ms > 4000 && ms <= 5000);
            }
            other => panic!("expected Ttl(Milliseconds), got {other:?}"),
        }
    }

    #[test]
    fn dispatch_set_nx_when_key_missing() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire: None,
                nx: true,
                xx: false,
            },
        );
        assert!(matches!(resp, ShardResponse::Ok));
        assert!(ks.exists("k"));
    }

    #[test]
    fn dispatch_set_nx_when_key_exists() {
        let mut ks = Keyspace::new();
        ks.set("k".into(), Bytes::from("old"), None, false, false);

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Set {
                key: "k".into(),
                value: Bytes::from("new"),
                expire: None,
                nx: true,
                xx: false,
            },
        );
        // NX should block — returns nil
        assert!(matches!(resp, ShardResponse::Value(None)));
        // original value should remain
        match ks.get("k").unwrap() {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("old")),
            other => panic!("expected old value, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_set_xx_when_key_exists() {
        let mut ks = Keyspace::new();
        ks.set("k".into(), Bytes::from("old"), None, false, false);

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Set {
                key: "k".into(),
                value: Bytes::from("new"),
                expire: None,
                nx: false,
                xx: true,
            },
        );
        assert!(matches!(resp, ShardResponse::Ok));
        match ks.get("k").unwrap() {
            Some(Value::String(data)) => assert_eq!(data, Bytes::from("new")),
            other => panic!("expected new value, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_set_xx_when_key_missing() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire: None,
                nx: false,
                xx: true,
            },
        );
        // XX should block — returns nil
        assert!(matches!(resp, ShardResponse::Value(None)));
        assert!(!ks.exists("k"));
    }

    #[test]
    fn dispatch_flushdb_clears_all_keys() {
        let mut ks = Keyspace::new();
        ks.set("a".into(), Bytes::from("1"), None, false, false);
        ks.set("b".into(), Bytes::from("2"), None, false, false);

        assert_eq!(ks.len(), 2);

        let resp = test_dispatch(&mut ks, ShardRequest::FlushDb);
        assert!(matches!(resp, ShardResponse::Ok));
        assert_eq!(ks.len(), 0);
    }

    #[test]
    fn dispatch_scan_returns_keys() {
        let mut ks = Keyspace::new();
        ks.set("user:1".into(), Bytes::from("a"), None, false, false);
        ks.set("user:2".into(), Bytes::from("b"), None, false, false);
        ks.set("item:1".into(), Bytes::from("c"), None, false, false);

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Scan {
                cursor: 0,
                count: 10,
                pattern: None,
            },
        );

        match resp {
            ShardResponse::Scan { cursor, keys } => {
                assert_eq!(cursor, 0); // complete in one pass
                assert_eq!(keys.len(), 3);
            }
            _ => panic!("expected Scan response"),
        }
    }

    #[test]
    fn dispatch_scan_with_pattern() {
        let mut ks = Keyspace::new();
        ks.set("user:1".into(), Bytes::from("a"), None, false, false);
        ks.set("user:2".into(), Bytes::from("b"), None, false, false);
        ks.set("item:1".into(), Bytes::from("c"), None, false, false);

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Scan {
                cursor: 0,
                count: 10,
                pattern: Some("user:*".into()),
            },
        );

        match resp {
            ShardResponse::Scan { cursor, keys } => {
                assert_eq!(cursor, 0);
                assert_eq!(keys.len(), 2);
                for k in &keys {
                    assert!(k.starts_with("user:"));
                }
            }
            _ => panic!("expected Scan response"),
        }
    }

    #[test]
    fn dispatch_keys() {
        let mut ks = Keyspace::new();
        ks.set("user:1".into(), Bytes::from("a"), None, false, false);
        ks.set("user:2".into(), Bytes::from("b"), None, false, false);
        ks.set("item:1".into(), Bytes::from("c"), None, false, false);
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Keys {
                pattern: "user:*".into(),
            },
        );
        match resp {
            ShardResponse::StringArray(mut keys) => {
                keys.sort();
                assert_eq!(keys, vec!["user:1", "user:2"]);
            }
            other => panic!("expected StringArray, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_rename() {
        let mut ks = Keyspace::new();
        ks.set("old".into(), Bytes::from("value"), None, false, false);
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Rename {
                key: "old".into(),
                newkey: "new".into(),
            },
        );
        assert!(matches!(resp, ShardResponse::Ok));
        assert!(!ks.exists("old"));
        assert!(ks.exists("new"));
    }

    #[test]
    fn dispatch_rename_missing_key() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::Rename {
                key: "missing".into(),
                newkey: "new".into(),
            },
        );
        assert!(matches!(resp, ShardResponse::Err(_)));
    }

    #[test]
    fn dump_key_returns_serialized_value() {
        let mut ks = Keyspace::new();
        ks.set(
            "greeting".into(),
            Bytes::from("hello"),
            Some(Duration::from_secs(60)),
            false,
            false,
        );

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::DumpKey {
                key: "greeting".into(),
            },
        );
        match resp {
            ShardResponse::KeyDump { data, ttl_ms } => {
                assert!(!data.is_empty());
                assert!(ttl_ms > 0);
                // verify the data round-trips
                let snap = snapshot::deserialize_snap_value(&data).unwrap();
                assert!(matches!(snap, SnapValue::String(ref b) if b == &Bytes::from("hello")));
            }
            other => panic!("expected KeyDump, got {other:?}"),
        }
    }

    #[test]
    fn dump_key_missing_returns_none() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(&mut ks, ShardRequest::DumpKey { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Value(None)));
    }

    #[test]
    fn restore_key_inserts_value() {
        let mut ks = Keyspace::new();
        let snap = SnapValue::String(Bytes::from("restored"));
        let data = snapshot::serialize_snap_value(&snap).unwrap();

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::RestoreKey {
                key: "mykey".into(),
                ttl_ms: 0,
                data: Bytes::from(data),
                replace: false,
            },
        );
        assert!(matches!(resp, ShardResponse::Ok));
        assert_eq!(
            ks.get("mykey").unwrap(),
            Some(Value::String(Bytes::from("restored")))
        );
    }

    #[test]
    fn restore_key_with_ttl() {
        let mut ks = Keyspace::new();
        let snap = SnapValue::String(Bytes::from("temp"));
        let data = snapshot::serialize_snap_value(&snap).unwrap();

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::RestoreKey {
                key: "ttlkey".into(),
                ttl_ms: 30_000,
                data: Bytes::from(data),
                replace: false,
            },
        );
        assert!(matches!(resp, ShardResponse::Ok));
        match ks.pttl("ttlkey") {
            TtlResult::Milliseconds(ms) => assert!(ms > 29_000 && ms <= 30_000),
            other => panic!("expected Milliseconds, got {other:?}"),
        }
    }

    #[test]
    fn restore_key_rejects_duplicate_without_replace() {
        let mut ks = Keyspace::new();
        ks.set("existing".into(), Bytes::from("old"), None, false, false);

        let snap = SnapValue::String(Bytes::from("new"));
        let data = snapshot::serialize_snap_value(&snap).unwrap();

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::RestoreKey {
                key: "existing".into(),
                ttl_ms: 0,
                data: Bytes::from(data),
                replace: false,
            },
        );
        assert!(matches!(resp, ShardResponse::Err(_)));
        // original value unchanged
        assert_eq!(
            ks.get("existing").unwrap(),
            Some(Value::String(Bytes::from("old")))
        );
    }

    #[test]
    fn restore_key_replace_overwrites() {
        let mut ks = Keyspace::new();
        ks.set("existing".into(), Bytes::from("old"), None, false, false);

        let snap = SnapValue::String(Bytes::from("new"));
        let data = snapshot::serialize_snap_value(&snap).unwrap();

        let resp = test_dispatch(
            &mut ks,
            ShardRequest::RestoreKey {
                key: "existing".into(),
                ttl_ms: 0,
                data: Bytes::from(data),
                replace: true,
            },
        );
        assert!(matches!(resp, ShardResponse::Ok));
        assert_eq!(
            ks.get("existing").unwrap(),
            Some(Value::String(Bytes::from("new")))
        );
    }

    #[test]
    fn dump_and_restore_hash_roundtrip() {
        let mut ks = Keyspace::new();
        ks.hset(
            "myhash",
            &[
                ("f1".into(), Bytes::from("v1")),
                ("f2".into(), Bytes::from("v2")),
            ],
        )
        .unwrap();

        // dump
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::DumpKey {
                key: "myhash".into(),
            },
        );
        let (data, _ttl) = match resp {
            ShardResponse::KeyDump { data, ttl_ms } => (data, ttl_ms),
            other => panic!("expected KeyDump, got {other:?}"),
        };

        // restore to a new key
        let resp = test_dispatch(
            &mut ks,
            ShardRequest::RestoreKey {
                key: "myhash2".into(),
                ttl_ms: 0,
                data: Bytes::from(data),
                replace: false,
            },
        );
        assert!(matches!(resp, ShardResponse::Ok));

        // verify fields
        assert_eq!(ks.hget("myhash2", "f1").unwrap(), Some(Bytes::from("v1")));
        assert_eq!(ks.hget("myhash2", "f2").unwrap(), Some(Bytes::from("v2")));
    }

    #[test]
    fn is_write_classifies_correctly() {
        // write commands
        assert!(ShardRequest::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire: None,
            nx: false,
            xx: false,
        }
        .is_write());
        assert!(ShardRequest::Del { key: "k".into() }.is_write());
        assert!(ShardRequest::Incr { key: "k".into() }.is_write());
        assert!(ShardRequest::LPush {
            key: "k".into(),
            values: vec![],
        }
        .is_write());
        assert!(ShardRequest::HSet {
            key: "k".into(),
            fields: vec![],
        }
        .is_write());
        assert!(ShardRequest::SAdd {
            key: "k".into(),
            members: vec![],
        }
        .is_write());
        assert!(ShardRequest::FlushDb.is_write());

        // read commands
        assert!(!ShardRequest::Get { key: "k".into() }.is_write());
        assert!(!ShardRequest::Exists { key: "k".into() }.is_write());
        assert!(!ShardRequest::Ttl { key: "k".into() }.is_write());
        assert!(!ShardRequest::DbSize.is_write());
        assert!(!ShardRequest::Stats.is_write());
        assert!(!ShardRequest::LLen { key: "k".into() }.is_write());
        assert!(!ShardRequest::HGet {
            key: "k".into(),
            field: "f".into(),
        }
        .is_write());
        assert!(!ShardRequest::SMembers { key: "k".into() }.is_write());
    }
}
