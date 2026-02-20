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
mod blocking;
mod persistence;

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use ember_persistence::aof::{AofRecord, AofWriter, FsyncPolicy};
use ember_persistence::recovery::{self, RecoveredValue};
use ember_persistence::snapshot::{self, SnapEntry, SnapValue, SnapshotWriter};
use smallvec::{smallvec, SmallVec};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{info, warn};

use crate::dropper::DropHandle;
use crate::error::ShardError;
use crate::expiry;
use crate::keyspace::{
    IncrError, IncrFloatError, Keyspace, KeyspaceStats, SetResult, ShardConfig, TtlResult,
    WriteError,
};
use crate::types::sorted_set::ZAddFlags;
use crate::types::Value;

/// How often the shard runs active expiration. 100ms matches
/// Redis's hz=10 default and keeps CPU overhead negligible.
const EXPIRY_TICK: Duration = Duration::from_millis(100);

/// How often to fsync when using the `EverySec` policy.
const FSYNC_INTERVAL: Duration = Duration::from_secs(1);

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

/// A protocol-agnostic command sent to a shard.
#[derive(Debug)]
pub enum ShardRequest {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: Bytes,
        expire: Option<Duration>,
        /// Only set the key if it does not already exist.
        nx: bool,
        /// Only set the key if it already exists.
        xx: bool,
    },
    Incr {
        key: String,
    },
    Decr {
        key: String,
    },
    IncrBy {
        key: String,
        delta: i64,
    },
    DecrBy {
        key: String,
        delta: i64,
    },
    IncrByFloat {
        key: String,
        delta: f64,
    },
    Append {
        key: String,
        value: Bytes,
    },
    Strlen {
        key: String,
    },
    /// Returns all keys matching a glob pattern in this shard.
    Keys {
        pattern: String,
    },
    /// Renames a key within this shard.
    Rename {
        key: String,
        newkey: String,
    },
    Del {
        key: String,
    },
    /// Like DEL but defers value deallocation to the background drop thread.
    Unlink {
        key: String,
    },
    Exists {
        key: String,
    },
    Expire {
        key: String,
        seconds: u64,
    },
    Ttl {
        key: String,
    },
    Persist {
        key: String,
    },
    Pttl {
        key: String,
    },
    Pexpire {
        key: String,
        milliseconds: u64,
    },
    LPush {
        key: String,
        values: Vec<Bytes>,
    },
    RPush {
        key: String,
        values: Vec<Bytes>,
    },
    LPop {
        key: String,
    },
    RPop {
        key: String,
    },
    /// Blocking left-pop. If the list has elements, pops immediately and sends
    /// the result on `waiter`. If empty, the shard registers the waiter to be
    /// woken when an element is pushed. Uses an mpsc sender so multiple shards
    /// can race to deliver the first result to a single receiver.
    BLPop {
        key: String,
        waiter: mpsc::Sender<(String, Bytes)>,
    },
    /// Blocking right-pop. Same semantics as BLPop but pops from the tail.
    BRPop {
        key: String,
        waiter: mpsc::Sender<(String, Bytes)>,
    },
    LRange {
        key: String,
        start: i64,
        stop: i64,
    },
    LLen {
        key: String,
    },
    Type {
        key: String,
    },
    ZAdd {
        key: String,
        members: Vec<(f64, String)>,
        nx: bool,
        xx: bool,
        gt: bool,
        lt: bool,
        ch: bool,
    },
    ZRem {
        key: String,
        members: Vec<String>,
    },
    ZScore {
        key: String,
        member: String,
    },
    ZRank {
        key: String,
        member: String,
    },
    ZCard {
        key: String,
    },
    ZRange {
        key: String,
        start: i64,
        stop: i64,
        with_scores: bool,
    },
    HSet {
        key: String,
        fields: Vec<(String, Bytes)>,
    },
    HGet {
        key: String,
        field: String,
    },
    HGetAll {
        key: String,
    },
    HDel {
        key: String,
        fields: Vec<String>,
    },
    HExists {
        key: String,
        field: String,
    },
    HLen {
        key: String,
    },
    HIncrBy {
        key: String,
        field: String,
        delta: i64,
    },
    HKeys {
        key: String,
    },
    HVals {
        key: String,
    },
    HMGet {
        key: String,
        fields: Vec<String>,
    },
    SAdd {
        key: String,
        members: Vec<String>,
    },
    SRem {
        key: String,
        members: Vec<String>,
    },
    SMembers {
        key: String,
    },
    SIsMember {
        key: String,
        member: String,
    },
    SCard {
        key: String,
    },
    /// Returns the key count for this shard.
    DbSize,
    /// Returns keyspace stats for this shard.
    Stats,
    /// Triggers a snapshot write.
    Snapshot,
    /// Serializes the current shard state to bytes (in-memory snapshot).
    ///
    /// Used by the replication server to capture a consistent shard
    /// snapshot for transmission to a new replica without filesystem I/O.
    SerializeSnapshot,
    /// Triggers an AOF rewrite (snapshot + truncate AOF).
    RewriteAof,
    /// Clears all keys from the keyspace.
    FlushDb,
    /// Clears all keys, deferring deallocation to the background drop thread.
    FlushDbAsync,
    /// Scans keys in the keyspace.
    Scan {
        cursor: u64,
        count: usize,
        pattern: Option<String>,
    },
    /// Counts keys in this shard that hash to the given cluster slot.
    CountKeysInSlot {
        slot: u16,
    },
    /// Returns up to `count` keys that hash to the given cluster slot.
    GetKeysInSlot {
        slot: u16,
        count: usize,
    },
    /// Dumps a key's value as serialized bytes for MIGRATE.
    DumpKey {
        key: String,
    },
    /// Restores a key from serialized bytes (received via MIGRATE).
    RestoreKey {
        key: String,
        ttl_ms: u64,
        data: bytes::Bytes,
        replace: bool,
    },
    /// Adds a vector to a vector set.
    #[cfg(feature = "vector")]
    VAdd {
        key: String,
        element: String,
        vector: Vec<f32>,
        metric: u8,
        quantization: u8,
        connectivity: u32,
        expansion_add: u32,
    },
    /// Adds multiple vectors to a vector set in a single command.
    #[cfg(feature = "vector")]
    VAddBatch {
        key: String,
        entries: Vec<(String, Vec<f32>)>,
        dim: usize,
        metric: u8,
        quantization: u8,
        connectivity: u32,
        expansion_add: u32,
    },
    /// Searches for nearest neighbors in a vector set.
    #[cfg(feature = "vector")]
    VSim {
        key: String,
        query: Vec<f32>,
        count: usize,
        ef_search: usize,
    },
    /// Removes an element from a vector set.
    #[cfg(feature = "vector")]
    VRem {
        key: String,
        element: String,
    },
    /// Gets the stored vector for an element.
    #[cfg(feature = "vector")]
    VGet {
        key: String,
        element: String,
    },
    /// Returns the number of elements in a vector set.
    #[cfg(feature = "vector")]
    VCard {
        key: String,
    },
    /// Returns the dimensionality of a vector set.
    #[cfg(feature = "vector")]
    VDim {
        key: String,
    },
    /// Returns metadata about a vector set.
    #[cfg(feature = "vector")]
    VInfo {
        key: String,
    },
    /// Stores a validated protobuf value.
    #[cfg(feature = "protobuf")]
    ProtoSet {
        key: String,
        type_name: String,
        data: Bytes,
        expire: Option<Duration>,
        nx: bool,
        xx: bool,
    },
    /// Retrieves a protobuf value.
    #[cfg(feature = "protobuf")]
    ProtoGet {
        key: String,
    },
    /// Returns the protobuf message type name for a key.
    #[cfg(feature = "protobuf")]
    ProtoType {
        key: String,
    },
    /// Writes a ProtoRegister AOF record (no keyspace mutation).
    /// Broadcast to all shards after a schema registration so the
    /// schema is recovered from any shard's AOF on restart.
    #[cfg(feature = "protobuf")]
    ProtoRegisterAof {
        name: String,
        descriptor: Bytes,
    },
    /// Atomically reads a proto value, sets a field, and writes it back.
    /// Runs entirely within the shard's single-threaded dispatch.
    #[cfg(feature = "protobuf")]
    ProtoSetField {
        key: String,
        field_path: String,
        value: String,
    },
    /// Atomically reads a proto value, clears a field, and writes it back.
    /// Runs entirely within the shard's single-threaded dispatch.
    #[cfg(feature = "protobuf")]
    ProtoDelField {
        key: String,
        field_path: String,
    },
}

/// The shard's response to a request.
#[derive(Debug)]
pub enum ShardResponse {
    /// A value (or None for a cache miss).
    Value(Option<Value>),
    /// Simple acknowledgement (e.g. SET).
    Ok,
    /// Integer result (e.g. INCR, DECR).
    Integer(i64),
    /// Boolean result (e.g. DEL, EXISTS, EXPIRE).
    Bool(bool),
    /// TTL query result.
    Ttl(TtlResult),
    /// Memory limit reached and eviction policy is NoEviction.
    OutOfMemory,
    /// Key count for a shard (DBSIZE).
    KeyCount(usize),
    /// Full stats for a shard (INFO).
    Stats(KeyspaceStats),
    /// Integer length result (e.g. LPUSH, RPUSH, LLEN).
    Len(usize),
    /// Array of bulk values (e.g. LRANGE).
    Array(Vec<Bytes>),
    /// The type name of a stored value.
    TypeName(&'static str),
    /// ZADD result: count for the client + actually applied members for AOF.
    ZAddLen {
        count: usize,
        applied: Vec<(f64, String)>,
    },
    /// ZREM result: count for the client + actually removed members for AOF.
    ZRemLen { count: usize, removed: Vec<String> },
    /// Float score result (e.g. ZSCORE).
    Score(Option<f64>),
    /// Rank result (e.g. ZRANK).
    Rank(Option<usize>),
    /// Scored array of (member, score) pairs (e.g. ZRANGE).
    ScoredArray(Vec<(String, f64)>),
    /// A bulk string result (e.g. INCRBYFLOAT).
    BulkString(String),
    /// Command used against a key holding the wrong kind of value.
    WrongType,
    /// An error message.
    Err(String),
    /// Scan result: next cursor and list of keys.
    Scan { cursor: u64, keys: Vec<String> },
    /// HGETALL result: all field-value pairs.
    HashFields(Vec<(String, Bytes)>),
    /// HDEL result: removed count + field names for AOF.
    HDelLen { count: usize, removed: Vec<String> },
    /// Array of strings (e.g. HKEYS).
    StringArray(Vec<String>),
    /// Serialized key dump with remaining TTL (for MIGRATE/DUMP).
    KeyDump { data: Vec<u8>, ttl_ms: i64 },
    /// In-memory snapshot of the full shard state (for replication).
    SnapshotData { shard_id: u16, data: Vec<u8> },
    /// HMGET result: array of optional values.
    OptionalArray(Vec<Option<Bytes>>),
    /// VADD result: element, vector, and whether it was newly added.
    #[cfg(feature = "vector")]
    VAddResult {
        element: String,
        vector: Vec<f32>,
        added: bool,
    },
    /// VADD_BATCH result: count of newly added elements + applied entries for AOF.
    #[cfg(feature = "vector")]
    VAddBatchResult {
        added_count: usize,
        applied: Vec<(String, Vec<f32>)>,
    },
    /// VSIM result: nearest neighbors with distances.
    #[cfg(feature = "vector")]
    VSimResult(Vec<(String, f32)>),
    /// VGET result: stored vector or None.
    #[cfg(feature = "vector")]
    VectorData(Option<Vec<f32>>),
    /// VINFO result: vector set metadata.
    #[cfg(feature = "vector")]
    VectorInfo(Option<Vec<(String, String)>>),
    /// PROTO.GET result: (type_name, data, remaining_ttl) or None.
    #[cfg(feature = "protobuf")]
    ProtoValue(Option<(String, Bytes, Option<Duration>)>),
    /// PROTO.TYPE result: message type name or None.
    #[cfg(feature = "protobuf")]
    ProtoTypeName(Option<String>),
    /// Result of an atomic SETFIELD/DELFIELD: carries the updated value
    /// for AOF persistence.
    #[cfg(feature = "protobuf")]
    ProtoFieldUpdated {
        type_name: String,
        data: Bytes,
        expire: Option<Duration>,
    },
}

/// A request (or batch of requests) bundled with reply channels.
///
/// The `Batch` variant reduces channel traffic during pipelining: instead
/// of N individual sends (one per pipelined command), the connection handler
/// groups commands by target shard and sends one `Batch` message per shard.
/// This cuts channel contention from O(pipeline_depth) to O(shard_count).
#[derive(Debug)]
pub enum ShardMessage {
    /// A single request with its reply channel.
    Single {
        request: ShardRequest,
        reply: oneshot::Sender<ShardResponse>,
    },
    /// Multiple requests batched for a single channel send.
    Batch(Vec<(ShardRequest, oneshot::Sender<ShardResponse>)>),
}

/// A cloneable handle for sending commands to a shard task.
///
/// Wraps the mpsc sender so callers don't need to manage oneshot
/// channels directly.
#[derive(Debug, Clone)]
pub struct ShardHandle {
    tx: mpsc::Sender<ShardMessage>,
}

impl ShardHandle {
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
    #[cfg(feature = "protobuf")] schema_registry: Option<crate::schema::SharedSchemaRegistry>,
) -> (ShardHandle, PreparedShard) {
    let (tx, rx) = mpsc::channel(buffer);
    let prepared = PreparedShard {
        rx,
        config,
        persistence,
        drop_handle,
        replication_tx,
        #[cfg(feature = "protobuf")]
        schema_registry,
    };
    (ShardHandle { tx }, prepared)
}

/// Runs the shard's main loop. Call this on the target runtime.
///
/// Consumes the `PreparedShard` and enters the infinite recv/expiry/fsync
/// select loop. Returns when the channel is closed (all senders dropped).
pub async fn run_prepared(prepared: PreparedShard) {
    run_shard(
        prepared.rx,
        prepared.config,
        prepared.persistence,
        prepared.drop_handle,
        prepared.replication_tx,
        #[cfg(feature = "protobuf")]
        prepared.schema_registry,
    )
    .await
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
    #[cfg(feature = "protobuf")] schema_registry: Option<crate::schema::SharedSchemaRegistry>,
) -> ShardHandle {
    let (handle, prepared) = prepare_shard(
        buffer,
        config,
        persistence,
        drop_handle,
        replication_tx,
        #[cfg(feature = "protobuf")]
        schema_registry,
    );
    tokio::spawn(run_prepared(prepared));
    handle
}

/// The shard's main loop. Processes messages and runs periodic
/// active expiration until the channel closes.
async fn run_shard(
    mut rx: mpsc::Receiver<ShardMessage>,
    config: ShardConfig,
    persistence: Option<ShardPersistenceConfig>,
    drop_handle: Option<DropHandle>,
    replication_tx: Option<broadcast::Sender<ReplicationEvent>>,
    #[cfg(feature = "protobuf")] schema_registry: Option<crate::schema::SharedSchemaRegistry>,
) {
    let shard_id = config.shard_id;
    let mut keyspace = Keyspace::with_config(config);

    if let Some(handle) = drop_handle.clone() {
        keyspace.set_drop_handle(handle);
    }

    // -- recovery --
    if let Some(ref pcfg) = persistence {
        #[cfg(feature = "encryption")]
        let result = if let Some(ref key) = pcfg.encryption_key {
            recovery::recover_shard_encrypted(&pcfg.data_dir, shard_id, key.clone())
        } else {
            recovery::recover_shard(&pcfg.data_dir, shard_id)
        };
        #[cfg(not(feature = "encryption"))]
        let result = recovery::recover_shard(&pcfg.data_dir, shard_id);
        let count = result.entries.len();
        for entry in result.entries {
            let value = match entry.value {
                RecoveredValue::String(data) => Value::String(data),
                RecoveredValue::List(deque) => Value::List(deque),
                RecoveredValue::SortedSet(members) => {
                    let mut ss = crate::types::sorted_set::SortedSet::new();
                    for (score, member) in members {
                        ss.add(member, score);
                    }
                    Value::SortedSet(Box::new(ss))
                }
                RecoveredValue::Hash(map) => Value::Hash(Box::new(map)),
                RecoveredValue::Set(set) => Value::Set(Box::new(set)),
                #[cfg(feature = "vector")]
                RecoveredValue::Vector {
                    metric,
                    quantization,
                    connectivity,
                    expansion_add,
                    elements,
                } => {
                    use crate::types::vector::{DistanceMetric, QuantizationType, VectorSet};
                    let dim = elements.first().map(|(_, v)| v.len()).unwrap_or(0);
                    match VectorSet::new(
                        dim,
                        DistanceMetric::from_u8(metric),
                        QuantizationType::from_u8(quantization),
                        connectivity as usize,
                        expansion_add as usize,
                    ) {
                        Ok(mut vs) => {
                            for (element, vector) in elements {
                                if let Err(e) = vs.add(element, &vector) {
                                    warn!("vector recovery: failed to add element: {e}");
                                }
                            }
                            Value::Vector(vs)
                        }
                        Err(e) => {
                            warn!("vector recovery: failed to create index: {e}");
                            continue;
                        }
                    }
                }
                #[cfg(feature = "protobuf")]
                RecoveredValue::Proto { type_name, data } => Value::Proto { type_name, data },
            };
            keyspace.restore(entry.key, value, entry.ttl);
        }
        if count > 0 {
            info!(
                shard_id,
                recovered_keys = count,
                snapshot = result.loaded_snapshot,
                aof = result.replayed_aof,
                "recovered shard state"
            );
        }

        // restore schemas found in the AOF into the shared registry
        #[cfg(feature = "protobuf")]
        if let Some(ref registry) = schema_registry {
            if !result.schemas.is_empty() {
                if let Ok(mut reg) = registry.write() {
                    let schema_count = result.schemas.len();
                    for (name, descriptor) in result.schemas {
                        reg.restore(name, descriptor);
                    }
                    info!(
                        shard_id,
                        schemas = schema_count,
                        "restored schemas from AOF"
                    );
                }
            }
        }
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

    let fsync_policy = persistence
        .as_ref()
        .map(|p| p.fsync_policy)
        .unwrap_or(FsyncPolicy::No);

    // monotonically increasing per-shard replication offset
    let mut replication_offset: u64 = 0;

    // waiter registries for blocking list operations (BLPOP/BRPOP)
    let mut lpop_waiters: HashMap<String, VecDeque<mpsc::Sender<(String, Bytes)>>> = HashMap::new();
    let mut rpop_waiters: HashMap<String, VecDeque<mpsc::Sender<(String, Bytes)>>> = HashMap::new();

    // -- tickers --
    let mut expiry_tick = tokio::time::interval(EXPIRY_TICK);
    expiry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut fsync_tick = tokio::time::interval(FSYNC_INTERVAL);
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
                            replication_offset: &mut replication_offset,
                            lpop_waiters: &mut lpop_waiters,
                            rpop_waiters: &mut rpop_waiters,
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
                expiry::run_expiration_cycle(&mut keyspace);
            }
            _ = fsync_tick.tick(), if fsync_policy == FsyncPolicy::EverySec => {
                if let Some(ref mut writer) = aof_writer {
                    if let Err(e) = writer.sync() {
                        warn!(shard_id, "periodic aof sync failed: {e}");
                    }
                }
            }
        }
    }

    // flush AOF on clean shutdown
    if let Some(ref mut writer) = aof_writer {
        let _ = writer.sync();
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
    replication_offset: &'a mut u64,
    /// Waiters for BLPOP — keyed by list name, FIFO order.
    lpop_waiters: &'a mut HashMap<String, VecDeque<mpsc::Sender<(String, Bytes)>>>,
    /// Waiters for BRPOP — keyed by list name, FIFO order.
    rpop_waiters: &'a mut HashMap<String, VecDeque<mpsc::Sender<(String, Bytes)>>>,
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
            process_single(request, reply, ctx);
        }
        ShardMessage::Batch(entries) => {
            for (request, reply) in entries {
                process_single(request, reply, ctx);
            }
        }
    }
}

/// Processes a single request: dispatch, write AOF, broadcast replication,
/// and send the response on the oneshot channel.
fn process_single(
    request: ShardRequest,
    reply: oneshot::Sender<ShardResponse>,
    ctx: &mut ProcessCtx<'_>,
) {
    // copy cheap fields upfront to avoid field-borrow conflicts below
    let fsync_policy = ctx.fsync_policy;
    let shard_id = ctx.shard_id;

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
    let response = dispatch(
        ctx.keyspace,
        &request,
        #[cfg(feature = "protobuf")]
        ctx.schema_registry,
    );

    // after LPush/RPush, check if any blocked clients are waiting.
    // done before consuming the request so we can borrow the key.
    if let ShardRequest::LPush { ref key, .. } | ShardRequest::RPush { ref key, .. } = request {
        if matches!(response, ShardResponse::Len(_)) {
            blocking::wake_blocked_waiters(key, ctx);
        }
    }

    // consume the request to move owned data into AOF records (avoids cloning)
    let records = aof::to_aof_records(request, &response);

    // write AOF records for successful mutations
    if let Some(ref mut writer) = *ctx.aof_writer {
        for record in &records {
            if let Err(e) = writer.write_record(record) {
                warn!(shard_id, "aof write failed: {e}");
            }
        }
        if !records.is_empty() && fsync_policy == FsyncPolicy::Always {
            if let Err(e) = writer.sync() {
                warn!(shard_id, "aof sync failed: {e}");
            }
        }
    }

    // broadcast mutation events to replication subscribers
    if let Some(ref tx) = *ctx.replication_tx {
        for record in records {
            *ctx.replication_offset += 1;
            // ignore send errors — no subscribers or lagged consumers
            let _ = tx.send(ReplicationEvent {
                shard_id,
                offset: *ctx.replication_offset,
                record,
            });
        }
    }

    // handle special requests that need access to persistence state
    match request_kind {
        RequestKind::Snapshot => {
            let resp = persistence::handle_snapshot(ctx.keyspace, ctx.persistence, shard_id);
            let _ = reply.send(resp);
            return;
        }
        RequestKind::SerializeSnapshot => {
            let resp = persistence::handle_serialize_snapshot(ctx.keyspace, shard_id);
            let _ = reply.send(resp);
            return;
        }
        RequestKind::RewriteAof => {
            let resp = persistence::handle_rewrite(
                ctx.keyspace,
                ctx.persistence,
                ctx.aof_writer,
                shard_id,
                #[cfg(feature = "protobuf")]
                ctx.schema_registry,
            );
            let _ = reply.send(resp);
            return;
        }
        RequestKind::FlushDbAsync => {
            let old_entries = ctx.keyspace.flush_async();
            if let Some(ref handle) = *ctx.drop_handle {
                handle.defer_entries(old_entries);
            }
            let _ = reply.send(ShardResponse::Ok);
            return;
        }
        RequestKind::Other => {}
    }

    let _ = reply.send(response);
}

/// Lightweight tag so we can identify requests that need special
/// handling after dispatch without borrowing the request again.
enum RequestKind {
    Snapshot,
    SerializeSnapshot,
    RewriteAof,
    FlushDbAsync,
    Other,
}

fn describe_request(req: &ShardRequest) -> RequestKind {
    match req {
        ShardRequest::Snapshot => RequestKind::Snapshot,
        ShardRequest::SerializeSnapshot => RequestKind::SerializeSnapshot,
        ShardRequest::RewriteAof => RequestKind::RewriteAof,
        ShardRequest::FlushDbAsync => RequestKind::FlushDbAsync,
        _ => RequestKind::Other,
    }
}

/// Converts an `IncrError` result into a `ShardResponse::Integer`.
fn incr_result(result: Result<i64, IncrError>) -> ShardResponse {
    match result {
        Ok(val) => ShardResponse::Integer(val),
        Err(IncrError::WrongType) => ShardResponse::WrongType,
        Err(IncrError::OutOfMemory) => ShardResponse::OutOfMemory,
        Err(e) => ShardResponse::Err(e.to_string()),
    }
}

/// Converts a `WriteError` result into a `ShardResponse::Len`.
fn write_result_len(result: Result<usize, WriteError>) -> ShardResponse {
    match result {
        Ok(len) => ShardResponse::Len(len),
        Err(WriteError::WrongType) => ShardResponse::WrongType,
        Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
    }
}

/// Routes a request to the appropriate keyspace operation and returns a response.
///
/// This is the hot path — every read and write goes through here.
fn dispatch(
    ks: &mut Keyspace,
    req: &ShardRequest,
    #[cfg(feature = "protobuf")] schema_registry: &Option<crate::schema::SharedSchemaRegistry>,
) -> ShardResponse {
    match req {
        ShardRequest::Get { key } => match ks.get_string(key) {
            Ok(val) => ShardResponse::Value(val.map(Value::String)),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::Set {
            key,
            value,
            expire,
            nx,
            xx,
        } => match ks.set(key.clone(), value.clone(), *expire, *nx, *xx) {
            SetResult::Ok => ShardResponse::Ok,
            SetResult::Blocked => ShardResponse::Value(None),
            SetResult::OutOfMemory => ShardResponse::OutOfMemory,
        },
        ShardRequest::Incr { key } => incr_result(ks.incr(key)),
        ShardRequest::Decr { key } => incr_result(ks.decr(key)),
        ShardRequest::IncrBy { key, delta } => incr_result(ks.incr_by(key, *delta)),
        ShardRequest::DecrBy { key, delta } => match delta.checked_neg() {
            Some(neg) => incr_result(ks.incr_by(key, neg)),
            None => ShardResponse::Err("ERR increment or decrement would overflow".into()),
        },
        ShardRequest::IncrByFloat { key, delta } => match ks.incr_by_float(key, *delta) {
            Ok(val) => ShardResponse::BulkString(val),
            Err(IncrFloatError::WrongType) => ShardResponse::WrongType,
            Err(IncrFloatError::OutOfMemory) => ShardResponse::OutOfMemory,
            Err(e) => ShardResponse::Err(e.to_string()),
        },
        ShardRequest::Append { key, value } => write_result_len(ks.append(key, value)),
        ShardRequest::Strlen { key } => match ks.strlen(key) {
            Ok(len) => ShardResponse::Len(len),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::Keys { pattern } => {
            let keys = ks.keys(pattern);
            ShardResponse::StringArray(keys)
        }
        ShardRequest::Rename { key, newkey } => {
            use crate::keyspace::RenameError;
            match ks.rename(key, newkey) {
                Ok(()) => ShardResponse::Ok,
                Err(RenameError::NoSuchKey) => ShardResponse::Err("ERR no such key".into()),
            }
        }
        ShardRequest::Del { key } => ShardResponse::Bool(ks.del(key)),
        ShardRequest::Unlink { key } => ShardResponse::Bool(ks.unlink(key)),
        ShardRequest::Exists { key } => ShardResponse::Bool(ks.exists(key)),
        ShardRequest::Expire { key, seconds } => ShardResponse::Bool(ks.expire(key, *seconds)),
        ShardRequest::Ttl { key } => ShardResponse::Ttl(ks.ttl(key)),
        ShardRequest::Persist { key } => ShardResponse::Bool(ks.persist(key)),
        ShardRequest::Pttl { key } => ShardResponse::Ttl(ks.pttl(key)),
        ShardRequest::Pexpire { key, milliseconds } => {
            ShardResponse::Bool(ks.pexpire(key, *milliseconds))
        }
        ShardRequest::LPush { key, values } => write_result_len(ks.lpush(key, values)),
        ShardRequest::RPush { key, values } => write_result_len(ks.rpush(key, values)),
        ShardRequest::LPop { key } => match ks.lpop(key) {
            Ok(val) => ShardResponse::Value(val.map(Value::String)),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::RPop { key } => match ks.rpop(key) {
            Ok(val) => ShardResponse::Value(val.map(Value::String)),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::LRange { key, start, stop } => match ks.lrange(key, *start, *stop) {
            Ok(items) => ShardResponse::Array(items),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::LLen { key } => match ks.llen(key) {
            Ok(len) => ShardResponse::Len(len),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::Type { key } => ShardResponse::TypeName(ks.value_type(key)),
        ShardRequest::ZAdd {
            key,
            members,
            nx,
            xx,
            gt,
            lt,
            ch,
        } => {
            let flags = ZAddFlags {
                nx: *nx,
                xx: *xx,
                gt: *gt,
                lt: *lt,
                ch: *ch,
            };
            match ks.zadd(key, members, &flags) {
                Ok(result) => ShardResponse::ZAddLen {
                    count: result.count,
                    applied: result.applied,
                },
                Err(WriteError::WrongType) => ShardResponse::WrongType,
                Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
            }
        }
        ShardRequest::ZRem { key, members } => match ks.zrem(key, members) {
            Ok(removed) => ShardResponse::ZRemLen {
                count: removed.len(),
                removed,
            },
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZScore { key, member } => match ks.zscore(key, member) {
            Ok(score) => ShardResponse::Score(score),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZRank { key, member } => match ks.zrank(key, member) {
            Ok(rank) => ShardResponse::Rank(rank),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZCard { key } => match ks.zcard(key) {
            Ok(len) => ShardResponse::Len(len),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZRange {
            key, start, stop, ..
        } => match ks.zrange(key, *start, *stop) {
            Ok(items) => ShardResponse::ScoredArray(items),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::DbSize => ShardResponse::KeyCount(ks.len()),
        ShardRequest::Stats => ShardResponse::Stats(ks.stats()),
        ShardRequest::FlushDb => {
            ks.clear();
            ShardResponse::Ok
        }
        ShardRequest::Scan {
            cursor,
            count,
            pattern,
        } => {
            let (next_cursor, keys) = ks.scan_keys(*cursor, *count, pattern.as_deref());
            ShardResponse::Scan {
                cursor: next_cursor,
                keys,
            }
        }
        ShardRequest::HSet { key, fields } => write_result_len(ks.hset(key, fields)),
        ShardRequest::HGet { key, field } => match ks.hget(key, field) {
            Ok(val) => ShardResponse::Value(val.map(Value::String)),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HGetAll { key } => match ks.hgetall(key) {
            Ok(fields) => ShardResponse::HashFields(fields),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HDel { key, fields } => match ks.hdel(key, fields) {
            Ok(removed) => ShardResponse::HDelLen {
                count: removed.len(),
                removed,
            },
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HExists { key, field } => match ks.hexists(key, field) {
            Ok(exists) => ShardResponse::Bool(exists),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HLen { key } => match ks.hlen(key) {
            Ok(len) => ShardResponse::Len(len),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HIncrBy { key, field, delta } => incr_result(ks.hincrby(key, field, *delta)),
        ShardRequest::HKeys { key } => match ks.hkeys(key) {
            Ok(keys) => ShardResponse::StringArray(keys),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HVals { key } => match ks.hvals(key) {
            Ok(vals) => ShardResponse::Array(vals),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HMGet { key, fields } => match ks.hmget(key, fields) {
            Ok(vals) => ShardResponse::OptionalArray(vals),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SAdd { key, members } => write_result_len(ks.sadd(key, members)),
        ShardRequest::SRem { key, members } => match ks.srem(key, members) {
            Ok(count) => ShardResponse::Len(count),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SMembers { key } => match ks.smembers(key) {
            Ok(members) => ShardResponse::StringArray(members),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SIsMember { key, member } => match ks.sismember(key, member) {
            Ok(exists) => ShardResponse::Bool(exists),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SCard { key } => match ks.scard(key) {
            Ok(count) => ShardResponse::Len(count),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::CountKeysInSlot { slot } => {
            ShardResponse::KeyCount(ks.count_keys_in_slot(*slot))
        }
        ShardRequest::GetKeysInSlot { slot, count } => {
            ShardResponse::StringArray(ks.get_keys_in_slot(*slot, *count))
        }
        ShardRequest::DumpKey { key } => match ks.dump(key) {
            Some((value, ttl_ms)) => {
                let snap = persistence::value_to_snap(value);
                match snapshot::serialize_snap_value(&snap) {
                    Ok(data) => ShardResponse::KeyDump { data, ttl_ms },
                    Err(e) => ShardResponse::Err(format!("ERR snapshot serialization failed: {e}")),
                }
            }
            None => ShardResponse::Value(None),
        },
        ShardRequest::RestoreKey {
            key,
            ttl_ms,
            data,
            replace,
        } => match snapshot::deserialize_snap_value(data) {
            Ok(snap) => {
                let exists = ks.exists(key);
                if exists && !replace {
                    ShardResponse::Err("ERR Target key name already exists".into())
                } else {
                    let value = persistence::snap_to_value(snap);
                    let ttl = if *ttl_ms == 0 {
                        None
                    } else {
                        Some(Duration::from_millis(*ttl_ms))
                    };
                    ks.restore(key.clone(), value, ttl);
                    ShardResponse::Ok
                }
            }
            Err(e) => ShardResponse::Err(format!("ERR DUMP payload corrupted: {e}")),
        },
        #[cfg(feature = "vector")]
        ShardRequest::VAdd {
            key,
            element,
            vector,
            metric,
            quantization,
            connectivity,
            expansion_add,
        } => {
            use crate::types::vector::{DistanceMetric, QuantizationType};
            match ks.vadd(
                key,
                element.clone(),
                vector.clone(),
                DistanceMetric::from_u8(*metric),
                QuantizationType::from_u8(*quantization),
                *connectivity as usize,
                *expansion_add as usize,
            ) {
                Ok(result) => ShardResponse::VAddResult {
                    element: result.element,
                    vector: result.vector,
                    added: result.added,
                },
                Err(crate::keyspace::VectorWriteError::WrongType) => ShardResponse::WrongType,
                Err(crate::keyspace::VectorWriteError::OutOfMemory) => ShardResponse::OutOfMemory,
                Err(crate::keyspace::VectorWriteError::IndexError(e))
                | Err(crate::keyspace::VectorWriteError::PartialBatch { message: e, .. }) => {
                    ShardResponse::Err(format!("ERR vector index: {e}"))
                }
            }
        }
        #[cfg(feature = "vector")]
        ShardRequest::VAddBatch {
            key,
            entries,
            metric,
            quantization,
            connectivity,
            expansion_add,
            ..
        } => {
            use crate::types::vector::{DistanceMetric, QuantizationType};
            match ks.vadd_batch(
                key,
                entries,
                DistanceMetric::from_u8(*metric),
                QuantizationType::from_u8(*quantization),
                *connectivity as usize,
                *expansion_add as usize,
            ) {
                Ok(result) => ShardResponse::VAddBatchResult {
                    added_count: result.added_count,
                    applied: result.applied,
                },
                Err(crate::keyspace::VectorWriteError::WrongType) => ShardResponse::WrongType,
                Err(crate::keyspace::VectorWriteError::OutOfMemory) => ShardResponse::OutOfMemory,
                Err(crate::keyspace::VectorWriteError::IndexError(e)) => {
                    ShardResponse::Err(format!("ERR vector index: {e}"))
                }
                Err(crate::keyspace::VectorWriteError::PartialBatch { applied, .. }) => {
                    // partial success: return applied vectors for AOF persistence
                    ShardResponse::VAddBatchResult {
                        added_count: applied.len(),
                        applied,
                    }
                }
            }
        }
        #[cfg(feature = "vector")]
        ShardRequest::VSim {
            key,
            query,
            count,
            ef_search,
        } => match ks.vsim(key, query, *count, *ef_search) {
            Ok(results) => ShardResponse::VSimResult(
                results
                    .into_iter()
                    .map(|r| (r.element, r.distance))
                    .collect(),
            ),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "vector")]
        ShardRequest::VRem { key, element } => match ks.vrem(key, element) {
            Ok(removed) => ShardResponse::Bool(removed),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "vector")]
        ShardRequest::VGet { key, element } => match ks.vget(key, element) {
            Ok(data) => ShardResponse::VectorData(data),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "vector")]
        ShardRequest::VCard { key } => match ks.vcard(key) {
            Ok(count) => ShardResponse::Integer(count as i64),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "vector")]
        ShardRequest::VDim { key } => match ks.vdim(key) {
            Ok(dim) => ShardResponse::Integer(dim as i64),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "vector")]
        ShardRequest::VInfo { key } => match ks.vinfo(key) {
            Ok(Some(info)) => {
                let fields = vec![
                    ("dim".to_owned(), info.dim.to_string()),
                    ("count".to_owned(), info.count.to_string()),
                    ("metric".to_owned(), info.metric.to_string()),
                    ("quantization".to_owned(), info.quantization.to_string()),
                    ("connectivity".to_owned(), info.connectivity.to_string()),
                    ("expansion_add".to_owned(), info.expansion_add.to_string()),
                ];
                ShardResponse::VectorInfo(Some(fields))
            }
            Ok(None) => ShardResponse::VectorInfo(None),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoSet {
            key,
            type_name,
            data,
            expire,
            nx,
            xx,
        } => {
            if *nx && ks.exists(key) {
                return ShardResponse::Value(None);
            }
            if *xx && !ks.exists(key) {
                return ShardResponse::Value(None);
            }
            match ks.proto_set(key.clone(), type_name.clone(), data.clone(), *expire) {
                SetResult::Ok | SetResult::Blocked => ShardResponse::Ok,
                SetResult::OutOfMemory => ShardResponse::OutOfMemory,
            }
        }
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoGet { key } => match ks.proto_get(key) {
            Ok(val) => ShardResponse::ProtoValue(val),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoType { key } => match ks.proto_type(key) {
            Ok(name) => ShardResponse::ProtoTypeName(name),
            Err(_) => ShardResponse::WrongType,
        },
        // ProtoRegisterAof is a no-op for the keyspace — the AOF record
        // is written by the to_aof_record path after dispatch returns Ok.
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoRegisterAof { .. } => ShardResponse::Ok,
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoSetField {
            key,
            field_path,
            value,
        } => dispatch_proto_field_op(ks, schema_registry, key, |reg, type_name, data, ttl| {
            let new_data = reg.set_field(type_name, data, field_path, value)?;
            Ok(ShardResponse::ProtoFieldUpdated {
                type_name: type_name.to_owned(),
                data: new_data,
                expire: ttl,
            })
        }),
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoDelField { key, field_path } => {
            dispatch_proto_field_op(ks, schema_registry, key, |reg, type_name, data, ttl| {
                let new_data = reg.clear_field(type_name, data, field_path)?;
                Ok(ShardResponse::ProtoFieldUpdated {
                    type_name: type_name.to_owned(),
                    data: new_data,
                    expire: ttl,
                })
            })
        }
        // these requests are intercepted in process_message, not handled here
        ShardRequest::Snapshot
        | ShardRequest::SerializeSnapshot
        | ShardRequest::RewriteAof
        | ShardRequest::FlushDbAsync
        | ShardRequest::BLPop { .. }
        | ShardRequest::BRPop { .. } => ShardResponse::Ok,
    }
}

/// Shared logic for atomic proto field operations (SETFIELD/DELFIELD).
///
/// Reads the proto value, acquires the schema registry, calls the
/// provided mutation closure, then writes the result back to the keyspace
/// — all within the single-threaded shard dispatch.
#[cfg(feature = "protobuf")]
fn dispatch_proto_field_op<F>(
    ks: &mut Keyspace,
    schema_registry: &Option<crate::schema::SharedSchemaRegistry>,
    key: &str,
    mutate: F,
) -> ShardResponse
where
    F: FnOnce(
        &crate::schema::SchemaRegistry,
        &str,
        &[u8],
        Option<Duration>,
    ) -> Result<ShardResponse, crate::schema::SchemaError>,
{
    let registry = match schema_registry {
        Some(r) => r,
        None => return ShardResponse::Err("protobuf support is not enabled".into()),
    };

    let (type_name, data, remaining_ttl) = match ks.proto_get(key) {
        Ok(Some(tuple)) => tuple,
        Ok(None) => return ShardResponse::Value(None),
        Err(_) => return ShardResponse::WrongType,
    };

    let reg = match registry.read() {
        Ok(r) => r,
        Err(_) => return ShardResponse::Err("schema registry lock poisoned".into()),
    };

    let resp = match mutate(&reg, &type_name, &data, remaining_ttl) {
        Ok(r) => r,
        Err(e) => return ShardResponse::Err(e.to_string()),
    };

    // write the updated value back, preserving the original TTL
    if let ShardResponse::ProtoFieldUpdated {
        ref type_name,
        ref data,
        expire,
    } = resp
    {
        ks.proto_set(key.to_owned(), type_name.clone(), data.clone(), expire);
    }

    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test helper: dispatch without a schema registry.
    fn test_dispatch(ks: &mut Keyspace, req: &ShardRequest) -> ShardResponse {
        dispatch(
            ks,
            req,
            #[cfg(feature = "protobuf")]
            &None,
        )
    }

    #[test]
    fn dispatch_set_and_get() {
        let mut ks = Keyspace::new();

        let resp = test_dispatch(
            &mut ks,
            &ShardRequest::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire: None,
                nx: false,
                xx: false,
            },
        );
        assert!(matches!(resp, ShardResponse::Ok));

        let resp = test_dispatch(&mut ks, &ShardRequest::Get { key: "k".into() });
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
        let resp = test_dispatch(&mut ks, &ShardRequest::Get { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Value(None)));
    }

    #[test]
    fn dispatch_del() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);

        let resp = test_dispatch(&mut ks, &ShardRequest::Del { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = test_dispatch(&mut ks, &ShardRequest::Del { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Bool(false)));
    }

    #[test]
    fn dispatch_exists() {
        let mut ks = Keyspace::new();
        ks.set("yes".into(), Bytes::from("here"), None, false, false);

        let resp = test_dispatch(&mut ks, &ShardRequest::Exists { key: "yes".into() });
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = test_dispatch(&mut ks, &ShardRequest::Exists { key: "no".into() });
        assert!(matches!(resp, ShardResponse::Bool(false)));
    }

    #[test]
    fn dispatch_expire_and_ttl() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);

        let resp = test_dispatch(
            &mut ks,
            &ShardRequest::Expire {
                key: "key".into(),
                seconds: 60,
            },
        );
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = test_dispatch(&mut ks, &ShardRequest::Ttl { key: "key".into() });
        match resp {
            ShardResponse::Ttl(TtlResult::Seconds(s)) => assert!((58..=60).contains(&s)),
            other => panic!("expected Ttl(Seconds), got {other:?}"),
        }
    }

    #[test]
    fn dispatch_ttl_missing() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(&mut ks, &ShardRequest::Ttl { key: "gone".into() });
        assert!(matches!(resp, ShardResponse::Ttl(TtlResult::NotFound)));
    }

    #[test]
    fn dispatch_incr_new_key() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(&mut ks, &ShardRequest::Incr { key: "c".into() });
        assert!(matches!(resp, ShardResponse::Integer(1)));
    }

    #[test]
    fn dispatch_decr_existing() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None, false, false);
        let resp = test_dispatch(&mut ks, &ShardRequest::Decr { key: "n".into() });
        assert!(matches!(resp, ShardResponse::Integer(9)));
    }

    #[test]
    fn dispatch_incr_non_integer() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("hello"), None, false, false);
        let resp = test_dispatch(&mut ks, &ShardRequest::Incr { key: "s".into() });
        assert!(matches!(resp, ShardResponse::Err(_)));
    }

    #[test]
    fn dispatch_incrby() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None, false, false);
        let resp = test_dispatch(
            &mut ks,
            &ShardRequest::IncrBy {
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
            &ShardRequest::DecrBy {
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
            &ShardRequest::IncrBy {
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
            &ShardRequest::IncrByFloat {
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
            &ShardRequest::Append {
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
        let resp = test_dispatch(&mut ks, &ShardRequest::Strlen { key: "k".into() });
        assert!(matches!(resp, ShardResponse::Len(5)));
    }

    #[test]
    fn dispatch_strlen_missing() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(&mut ks, &ShardRequest::Strlen { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Len(0)));
    }

    #[test]
    fn dispatch_incrbyfloat_new_key() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(
            &mut ks,
            &ShardRequest::IncrByFloat {
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

        let resp = test_dispatch(&mut ks, &ShardRequest::Persist { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = test_dispatch(&mut ks, &ShardRequest::Ttl { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Ttl(TtlResult::NoExpiry)));
    }

    #[test]
    fn dispatch_persist_missing_key() {
        let mut ks = Keyspace::new();
        let resp = test_dispatch(&mut ks, &ShardRequest::Persist { key: "nope".into() });
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

        let resp = test_dispatch(&mut ks, &ShardRequest::Pttl { key: "key".into() });
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
        let resp = test_dispatch(&mut ks, &ShardRequest::Pttl { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Ttl(TtlResult::NotFound)));
    }

    #[test]
    fn dispatch_pexpire() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None, false, false);

        let resp = test_dispatch(
            &mut ks,
            &ShardRequest::Pexpire {
                key: "key".into(),
                milliseconds: 5000,
            },
        );
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = test_dispatch(&mut ks, &ShardRequest::Pttl { key: "key".into() });
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
            &ShardRequest::Set {
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
            &ShardRequest::Set {
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
            &ShardRequest::Set {
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
            &ShardRequest::Set {
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

        let resp = test_dispatch(&mut ks, &ShardRequest::FlushDb);
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
            &ShardRequest::Scan {
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
            &ShardRequest::Scan {
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
            &ShardRequest::Keys {
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
            &ShardRequest::Rename {
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
            &ShardRequest::Rename {
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
            &ShardRequest::DumpKey {
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
        let resp = test_dispatch(&mut ks, &ShardRequest::DumpKey { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Value(None)));
    }

    #[test]
    fn restore_key_inserts_value() {
        let mut ks = Keyspace::new();
        let snap = SnapValue::String(Bytes::from("restored"));
        let data = snapshot::serialize_snap_value(&snap).unwrap();

        let resp = test_dispatch(
            &mut ks,
            &ShardRequest::RestoreKey {
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
            &ShardRequest::RestoreKey {
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
            &ShardRequest::RestoreKey {
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
            &ShardRequest::RestoreKey {
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
            &ShardRequest::DumpKey {
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
            &ShardRequest::RestoreKey {
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
}
