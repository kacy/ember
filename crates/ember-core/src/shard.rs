//! Shard: an independent partition of the keyspace.
//!
//! Each shard runs as its own tokio task, owning a `Keyspace` with no
//! internal locking. Commands arrive over an mpsc channel and responses
//! go back on a per-request oneshot. A background tick drives active
//! expiration of TTL'd keys.

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use ember_persistence::aof::{AofRecord, AofWriter, FsyncPolicy};
use ember_persistence::recovery::{self, RecoveredValue};
use ember_persistence::snapshot::{self, SnapEntry, SnapValue, SnapshotWriter};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use crate::error::ShardError;
use crate::expiry;
use crate::keyspace::{
    IncrError, Keyspace, KeyspaceStats, SetResult, ShardConfig, TtlResult, WriteError,
};
use crate::types::sorted_set::ZAddFlags;
use crate::types::Value;

/// How often the shard runs active expiration. 100ms matches
/// Redis's hz=10 default and keeps CPU overhead negligible.
const EXPIRY_TICK: Duration = Duration::from_millis(100);

/// How often to fsync when using the `EverySec` policy.
const FSYNC_INTERVAL: Duration = Duration::from_secs(1);

/// Optional persistence configuration for a shard.
#[derive(Debug, Clone)]
pub struct ShardPersistenceConfig {
    /// Directory where AOF and snapshot files live.
    pub data_dir: PathBuf,
    /// Whether to write an AOF log of mutations.
    pub append_only: bool,
    /// When to fsync the AOF file.
    pub fsync_policy: FsyncPolicy,
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
    Del {
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
    /// Triggers an AOF rewrite (snapshot + truncate AOF).
    RewriteAof,
    /// Clears all keys from the keyspace.
    FlushDb,
    /// Scans keys in the keyspace.
    Scan {
        cursor: u64,
        count: usize,
        pattern: Option<String>,
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
    /// HMGET result: array of optional values.
    OptionalArray(Vec<Option<Bytes>>),
}

/// A request bundled with its reply channel.
#[derive(Debug)]
pub struct ShardMessage {
    pub request: ShardRequest,
    pub reply: oneshot::Sender<ShardResponse>,
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
    /// all shards before collecting results.
    pub(crate) async fn dispatch(
        &self,
        request: ShardRequest,
    ) -> Result<oneshot::Receiver<ShardResponse>, ShardError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let msg = ShardMessage {
            request,
            reply: reply_tx,
        };
        self.tx
            .send(msg)
            .await
            .map_err(|_| ShardError::Unavailable)?;
        Ok(reply_rx)
    }
}

/// Spawns a shard task and returns the handle for communicating with it.
///
/// `buffer` controls the mpsc channel capacity — higher values absorb
/// burst traffic at the cost of memory.
pub fn spawn_shard(
    buffer: usize,
    config: ShardConfig,
    persistence: Option<ShardPersistenceConfig>,
) -> ShardHandle {
    let (tx, rx) = mpsc::channel(buffer);
    tokio::spawn(run_shard(rx, config, persistence));
    ShardHandle { tx }
}

/// The shard's main loop. Processes messages and runs periodic
/// active expiration until the channel closes.
async fn run_shard(
    mut rx: mpsc::Receiver<ShardMessage>,
    config: ShardConfig,
    persistence: Option<ShardPersistenceConfig>,
) {
    let shard_id = config.shard_id;
    let mut keyspace = Keyspace::with_config(config);

    // -- recovery --
    if let Some(ref pcfg) = persistence {
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
                    Value::SortedSet(ss)
                }
                RecoveredValue::Hash(map) => Value::Hash(map),
                RecoveredValue::Set(set) => Value::Set(set),
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
    }

    // -- AOF writer --
    let mut aof_writer: Option<AofWriter> = match &persistence {
        Some(pcfg) if pcfg.append_only => {
            let path = ember_persistence::aof::aof_path(&pcfg.data_dir, shard_id);
            match AofWriter::open(path) {
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
                        let request_kind = describe_request(&msg.request);
                        let response = dispatch(&mut keyspace, &msg.request);

                        // write AOF record for successful mutations
                        if let Some(ref mut writer) = aof_writer {
                            if let Some(record) = to_aof_record(&msg.request, &response) {
                                if let Err(e) = writer.write_record(&record) {
                                    warn!(shard_id, "aof write failed: {e}");
                                }
                                if fsync_policy == FsyncPolicy::Always {
                                    if let Err(e) = writer.sync() {
                                        warn!(shard_id, "aof sync failed: {e}");
                                    }
                                }
                            }
                        }

                        // handle snapshot/rewrite (these need mutable access
                        // to both keyspace and aof_writer)
                        match request_kind {
                            RequestKind::Snapshot => {
                                let resp = handle_snapshot(
                                    &keyspace, &persistence, shard_id,
                                );
                                let _ = msg.reply.send(resp);
                                continue;
                            }
                            RequestKind::RewriteAof => {
                                let resp = handle_rewrite(
                                    &keyspace,
                                    &persistence,
                                    &mut aof_writer,
                                    shard_id,
                                );
                                let _ = msg.reply.send(resp);
                                continue;
                            }
                            RequestKind::Other => {}
                        }

                        let _ = msg.reply.send(response);
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

/// Lightweight tag so we can identify snapshot/rewrite requests after
/// dispatch without borrowing the request again.
enum RequestKind {
    Snapshot,
    RewriteAof,
    Other,
}

fn describe_request(req: &ShardRequest) -> RequestKind {
    match req {
        ShardRequest::Snapshot => RequestKind::Snapshot,
        ShardRequest::RewriteAof => RequestKind::RewriteAof,
        _ => RequestKind::Other,
    }
}

/// Executes a single request against the keyspace.
fn dispatch(ks: &mut Keyspace, req: &ShardRequest) -> ShardResponse {
    match req {
        ShardRequest::Get { key } => match ks.get(key) {
            Ok(val) => ShardResponse::Value(val),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::Set {
            key,
            value,
            expire,
            nx,
            xx,
        } => {
            // NX: only set if key does NOT already exist
            if *nx && ks.exists(key) {
                return ShardResponse::Value(None);
            }
            // XX: only set if key DOES already exist
            if *xx && !ks.exists(key) {
                return ShardResponse::Value(None);
            }
            match ks.set(key.clone(), value.clone(), *expire) {
                SetResult::Ok => ShardResponse::Ok,
                SetResult::OutOfMemory => ShardResponse::OutOfMemory,
            }
        }
        ShardRequest::Incr { key } => match ks.incr(key) {
            Ok(val) => ShardResponse::Integer(val),
            Err(IncrError::WrongType) => ShardResponse::WrongType,
            Err(IncrError::OutOfMemory) => ShardResponse::OutOfMemory,
            Err(e) => ShardResponse::Err(e.to_string()),
        },
        ShardRequest::Decr { key } => match ks.decr(key) {
            Ok(val) => ShardResponse::Integer(val),
            Err(IncrError::WrongType) => ShardResponse::WrongType,
            Err(IncrError::OutOfMemory) => ShardResponse::OutOfMemory,
            Err(e) => ShardResponse::Err(e.to_string()),
        },
        ShardRequest::IncrBy { key, delta } => match ks.incr_by(key, *delta) {
            Ok(val) => ShardResponse::Integer(val),
            Err(IncrError::WrongType) => ShardResponse::WrongType,
            Err(IncrError::OutOfMemory) => ShardResponse::OutOfMemory,
            Err(e) => ShardResponse::Err(e.to_string()),
        },
        ShardRequest::DecrBy { key, delta } => match ks.incr_by(key, -delta) {
            Ok(val) => ShardResponse::Integer(val),
            Err(IncrError::WrongType) => ShardResponse::WrongType,
            Err(IncrError::OutOfMemory) => ShardResponse::OutOfMemory,
            Err(e) => ShardResponse::Err(e.to_string()),
        },
        ShardRequest::Del { key } => ShardResponse::Bool(ks.del(key)),
        ShardRequest::Exists { key } => ShardResponse::Bool(ks.exists(key)),
        ShardRequest::Expire { key, seconds } => ShardResponse::Bool(ks.expire(key, *seconds)),
        ShardRequest::Ttl { key } => ShardResponse::Ttl(ks.ttl(key)),
        ShardRequest::Persist { key } => ShardResponse::Bool(ks.persist(key)),
        ShardRequest::Pttl { key } => ShardResponse::Ttl(ks.pttl(key)),
        ShardRequest::Pexpire { key, milliseconds } => {
            ShardResponse::Bool(ks.pexpire(key, *milliseconds))
        }
        ShardRequest::LPush { key, values } => match ks.lpush(key, values) {
            Ok(len) => ShardResponse::Len(len),
            Err(WriteError::WrongType) => ShardResponse::WrongType,
            Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
        },
        ShardRequest::RPush { key, values } => match ks.rpush(key, values) {
            Ok(len) => ShardResponse::Len(len),
            Err(WriteError::WrongType) => ShardResponse::WrongType,
            Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
        },
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
        ShardRequest::HSet { key, fields } => match ks.hset(key, fields) {
            Ok(count) => ShardResponse::Len(count),
            Err(WriteError::WrongType) => ShardResponse::WrongType,
            Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
        },
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
        ShardRequest::HIncrBy { key, field, delta } => match ks.hincrby(key, field, *delta) {
            Ok(val) => ShardResponse::Integer(val),
            Err(IncrError::WrongType) => ShardResponse::WrongType,
            Err(IncrError::OutOfMemory) => ShardResponse::OutOfMemory,
            Err(e) => ShardResponse::Err(e.to_string()),
        },
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
        ShardRequest::SAdd { key, members } => match ks.sadd(key, members) {
            Ok(count) => ShardResponse::Len(count),
            Err(WriteError::WrongType) => ShardResponse::WrongType,
            Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
        },
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
        // snapshot/rewrite are handled in the main loop, not here
        ShardRequest::Snapshot | ShardRequest::RewriteAof => ShardResponse::Ok,
    }
}

/// Converts a successful mutation request+response pair into an AOF record.
/// Returns None for non-mutation requests or failed mutations.
fn to_aof_record(req: &ShardRequest, resp: &ShardResponse) -> Option<AofRecord> {
    match (req, resp) {
        (
            ShardRequest::Set {
                key, value, expire, ..
            },
            ShardResponse::Ok,
        ) => {
            let expire_ms = expire.map(|d| d.as_millis() as i64).unwrap_or(-1);
            Some(AofRecord::Set {
                key: key.clone(),
                value: value.clone(),
                expire_ms,
            })
        }
        (ShardRequest::Del { key }, ShardResponse::Bool(true)) => {
            Some(AofRecord::Del { key: key.clone() })
        }
        (ShardRequest::Expire { key, seconds }, ShardResponse::Bool(true)) => {
            Some(AofRecord::Expire {
                key: key.clone(),
                seconds: *seconds,
            })
        }
        (ShardRequest::LPush { key, values }, ShardResponse::Len(_)) => Some(AofRecord::LPush {
            key: key.clone(),
            values: values.clone(),
        }),
        (ShardRequest::RPush { key, values }, ShardResponse::Len(_)) => Some(AofRecord::RPush {
            key: key.clone(),
            values: values.clone(),
        }),
        (ShardRequest::LPop { key }, ShardResponse::Value(Some(_))) => {
            Some(AofRecord::LPop { key: key.clone() })
        }
        (ShardRequest::RPop { key }, ShardResponse::Value(Some(_))) => {
            Some(AofRecord::RPop { key: key.clone() })
        }
        (ShardRequest::ZAdd { key, .. }, ShardResponse::ZAddLen { applied, .. })
            if !applied.is_empty() =>
        {
            Some(AofRecord::ZAdd {
                key: key.clone(),
                members: applied.clone(),
            })
        }
        (ShardRequest::ZRem { key, .. }, ShardResponse::ZRemLen { removed, .. })
            if !removed.is_empty() =>
        {
            Some(AofRecord::ZRem {
                key: key.clone(),
                members: removed.clone(),
            })
        }
        (ShardRequest::Incr { key }, ShardResponse::Integer(_)) => {
            Some(AofRecord::Incr { key: key.clone() })
        }
        (ShardRequest::Decr { key }, ShardResponse::Integer(_)) => {
            Some(AofRecord::Decr { key: key.clone() })
        }
        (ShardRequest::IncrBy { key, delta }, ShardResponse::Integer(_)) => {
            Some(AofRecord::IncrBy {
                key: key.clone(),
                delta: *delta,
            })
        }
        (ShardRequest::DecrBy { key, delta }, ShardResponse::Integer(_)) => {
            Some(AofRecord::DecrBy {
                key: key.clone(),
                delta: *delta,
            })
        }
        (ShardRequest::Persist { key }, ShardResponse::Bool(true)) => {
            Some(AofRecord::Persist { key: key.clone() })
        }
        (ShardRequest::Pexpire { key, milliseconds }, ShardResponse::Bool(true)) => {
            Some(AofRecord::Pexpire {
                key: key.clone(),
                milliseconds: *milliseconds,
            })
        }
        // Hash commands
        (ShardRequest::HSet { key, fields }, ShardResponse::Len(_)) => Some(AofRecord::HSet {
            key: key.clone(),
            fields: fields.clone(),
        }),
        (ShardRequest::HDel { key, .. }, ShardResponse::HDelLen { removed, .. })
            if !removed.is_empty() =>
        {
            Some(AofRecord::HDel {
                key: key.clone(),
                fields: removed.clone(),
            })
        }
        (ShardRequest::HIncrBy { key, field, delta }, ShardResponse::Integer(_)) => {
            Some(AofRecord::HIncrBy {
                key: key.clone(),
                field: field.clone(),
                delta: *delta,
            })
        }
        // Set commands
        (ShardRequest::SAdd { key, members }, ShardResponse::Len(count)) if *count > 0 => {
            Some(AofRecord::SAdd {
                key: key.clone(),
                members: members.clone(),
            })
        }
        (ShardRequest::SRem { key, members }, ShardResponse::Len(count)) if *count > 0 => {
            Some(AofRecord::SRem {
                key: key.clone(),
                members: members.clone(),
            })
        }
        _ => None,
    }
}

/// Writes a snapshot of the current keyspace.
fn handle_snapshot(
    keyspace: &Keyspace,
    persistence: &Option<ShardPersistenceConfig>,
    shard_id: u16,
) -> ShardResponse {
    let pcfg = match persistence {
        Some(p) => p,
        None => return ShardResponse::Err("persistence not configured".into()),
    };

    let path = snapshot::snapshot_path(&pcfg.data_dir, shard_id);
    match write_snapshot(keyspace, &path, shard_id) {
        Ok(count) => {
            info!(shard_id, entries = count, "snapshot written");
            ShardResponse::Ok
        }
        Err(e) => {
            warn!(shard_id, "snapshot failed: {e}");
            ShardResponse::Err(format!("snapshot failed: {e}"))
        }
    }
}

/// Writes a snapshot and then truncates the AOF.
fn handle_rewrite(
    keyspace: &Keyspace,
    persistence: &Option<ShardPersistenceConfig>,
    aof_writer: &mut Option<AofWriter>,
    shard_id: u16,
) -> ShardResponse {
    let pcfg = match persistence {
        Some(p) => p,
        None => return ShardResponse::Err("persistence not configured".into()),
    };

    let path = snapshot::snapshot_path(&pcfg.data_dir, shard_id);
    match write_snapshot(keyspace, &path, shard_id) {
        Ok(count) => {
            // truncate AOF after successful snapshot
            if let Some(ref mut writer) = aof_writer {
                if let Err(e) = writer.truncate() {
                    warn!(shard_id, "aof truncate after rewrite failed: {e}");
                }
            }
            info!(shard_id, entries = count, "aof rewrite complete");
            ShardResponse::Ok
        }
        Err(e) => {
            warn!(shard_id, "aof rewrite failed: {e}");
            ShardResponse::Err(format!("rewrite failed: {e}"))
        }
    }
}

/// Iterates the keyspace and writes all live entries to a snapshot file.
fn write_snapshot(
    keyspace: &Keyspace,
    path: &std::path::Path,
    shard_id: u16,
) -> Result<u32, ember_persistence::format::FormatError> {
    let mut writer = SnapshotWriter::create(path, shard_id)?;
    let mut count = 0u32;

    for (key, value, ttl_ms) in keyspace.iter_entries() {
        let snap_value = match value {
            Value::String(data) => SnapValue::String(data.clone()),
            Value::List(deque) => SnapValue::List(deque.clone()),
            Value::SortedSet(ss) => {
                let members: Vec<(f64, String)> = ss
                    .iter()
                    .map(|(member, score)| (score, member.to_owned()))
                    .collect();
                SnapValue::SortedSet(members)
            }
            Value::Hash(map) => SnapValue::Hash(map.clone()),
            Value::Set(set) => SnapValue::Set(set.clone()),
        };
        writer.write_entry(&SnapEntry {
            key: key.to_owned(),
            value: snap_value,
            expire_ms: ttl_ms,
        })?;
        count += 1;
    }

    writer.finish()?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_set_and_get() {
        let mut ks = Keyspace::new();

        let resp = dispatch(
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

        let resp = dispatch(&mut ks, &ShardRequest::Get { key: "k".into() });
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
        let resp = dispatch(&mut ks, &ShardRequest::Get { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Value(None)));
    }

    #[test]
    fn dispatch_del() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None);

        let resp = dispatch(&mut ks, &ShardRequest::Del { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = dispatch(&mut ks, &ShardRequest::Del { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Bool(false)));
    }

    #[test]
    fn dispatch_exists() {
        let mut ks = Keyspace::new();
        ks.set("yes".into(), Bytes::from("here"), None);

        let resp = dispatch(&mut ks, &ShardRequest::Exists { key: "yes".into() });
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = dispatch(&mut ks, &ShardRequest::Exists { key: "no".into() });
        assert!(matches!(resp, ShardResponse::Bool(false)));
    }

    #[test]
    fn dispatch_expire_and_ttl() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None);

        let resp = dispatch(
            &mut ks,
            &ShardRequest::Expire {
                key: "key".into(),
                seconds: 60,
            },
        );
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = dispatch(&mut ks, &ShardRequest::Ttl { key: "key".into() });
        match resp {
            ShardResponse::Ttl(TtlResult::Seconds(s)) => assert!((58..=60).contains(&s)),
            other => panic!("expected Ttl(Seconds), got {other:?}"),
        }
    }

    #[test]
    fn dispatch_ttl_missing() {
        let mut ks = Keyspace::new();
        let resp = dispatch(&mut ks, &ShardRequest::Ttl { key: "gone".into() });
        assert!(matches!(resp, ShardResponse::Ttl(TtlResult::NotFound)));
    }

    #[tokio::test]
    async fn shard_round_trip() {
        let handle = spawn_shard(16, ShardConfig::default(), None);

        let resp = handle
            .send(ShardRequest::Set {
                key: "hello".into(),
                value: Bytes::from("world"),
                expire: None,
                nx: false,
                xx: false,
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Ok));

        let resp = handle
            .send(ShardRequest::Get {
                key: "hello".into(),
            })
            .await
            .unwrap();
        match resp {
            ShardResponse::Value(Some(Value::String(data))) => {
                assert_eq!(data, Bytes::from("world"));
            }
            other => panic!("expected Value(Some(String)), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn expired_key_through_shard() {
        let handle = spawn_shard(16, ShardConfig::default(), None);

        handle
            .send(ShardRequest::Set {
                key: "temp".into(),
                value: Bytes::from("gone"),
                expire: Some(Duration::from_millis(10)),
                nx: false,
                xx: false,
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(30)).await;

        let resp = handle
            .send(ShardRequest::Get { key: "temp".into() })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Value(None)));
    }

    #[tokio::test]
    async fn active_expiration_cleans_up_without_access() {
        let handle = spawn_shard(16, ShardConfig::default(), None);

        // set a key with a short TTL
        handle
            .send(ShardRequest::Set {
                key: "ephemeral".into(),
                value: Bytes::from("temp"),
                expire: Some(Duration::from_millis(10)),
                nx: false,
                xx: false,
            })
            .await
            .unwrap();

        // also set a persistent key
        handle
            .send(ShardRequest::Set {
                key: "persistent".into(),
                value: Bytes::from("stays"),
                expire: None,
                nx: false,
                xx: false,
            })
            .await
            .unwrap();

        // wait long enough for the TTL to expire AND for the background
        // tick to fire (100ms interval + some slack)
        tokio::time::sleep(Duration::from_millis(250)).await;

        // the ephemeral key should be gone even though we never accessed it
        let resp = handle
            .send(ShardRequest::Exists {
                key: "ephemeral".into(),
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Bool(false)));

        // the persistent key should still be there
        let resp = handle
            .send(ShardRequest::Exists {
                key: "persistent".into(),
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Bool(true)));
    }

    #[tokio::test]
    async fn shard_with_persistence_snapshot_and_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let pcfg = ShardPersistenceConfig {
            data_dir: dir.path().to_owned(),
            append_only: true,
            fsync_policy: FsyncPolicy::Always,
        };
        let config = ShardConfig {
            shard_id: 0,
            ..ShardConfig::default()
        };

        // write some keys then trigger a snapshot
        {
            let handle = spawn_shard(16, config.clone(), Some(pcfg.clone()));
            handle
                .send(ShardRequest::Set {
                    key: "a".into(),
                    value: Bytes::from("1"),
                    expire: None,
                    nx: false,
                    xx: false,
                })
                .await
                .unwrap();
            handle
                .send(ShardRequest::Set {
                    key: "b".into(),
                    value: Bytes::from("2"),
                    expire: Some(Duration::from_secs(300)),
                    nx: false,
                    xx: false,
                })
                .await
                .unwrap();
            handle.send(ShardRequest::Snapshot).await.unwrap();
            // write one more key that goes only to AOF
            handle
                .send(ShardRequest::Set {
                    key: "c".into(),
                    value: Bytes::from("3"),
                    expire: None,
                    nx: false,
                    xx: false,
                })
                .await
                .unwrap();
            // drop handle to shut down shard
        }

        // give it a moment to flush
        tokio::time::sleep(Duration::from_millis(50)).await;

        // start a new shard with the same config — should recover
        {
            let handle = spawn_shard(16, config, Some(pcfg));
            // give it a moment to recover
            tokio::time::sleep(Duration::from_millis(50)).await;

            let resp = handle
                .send(ShardRequest::Get { key: "a".into() })
                .await
                .unwrap();
            match resp {
                ShardResponse::Value(Some(Value::String(data))) => {
                    assert_eq!(data, Bytes::from("1"));
                }
                other => panic!("expected a=1, got {other:?}"),
            }

            let resp = handle
                .send(ShardRequest::Get { key: "b".into() })
                .await
                .unwrap();
            assert!(matches!(resp, ShardResponse::Value(Some(_))));

            let resp = handle
                .send(ShardRequest::Get { key: "c".into() })
                .await
                .unwrap();
            match resp {
                ShardResponse::Value(Some(Value::String(data))) => {
                    assert_eq!(data, Bytes::from("3"));
                }
                other => panic!("expected c=3, got {other:?}"),
            }
        }
    }

    #[test]
    fn to_aof_record_for_set() {
        let req = ShardRequest::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire: Some(Duration::from_secs(60)),
            nx: false,
            xx: false,
        };
        let resp = ShardResponse::Ok;
        let record = to_aof_record(&req, &resp).unwrap();
        match record {
            AofRecord::Set { key, expire_ms, .. } => {
                assert_eq!(key, "k");
                assert_eq!(expire_ms, 60_000);
            }
            other => panic!("expected Set, got {other:?}"),
        }
    }

    #[test]
    fn to_aof_record_skips_failed_set() {
        let req = ShardRequest::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire: None,
            nx: false,
            xx: false,
        };
        let resp = ShardResponse::OutOfMemory;
        assert!(to_aof_record(&req, &resp).is_none());
    }

    #[test]
    fn to_aof_record_for_del() {
        let req = ShardRequest::Del { key: "k".into() };
        let resp = ShardResponse::Bool(true);
        let record = to_aof_record(&req, &resp).unwrap();
        assert!(matches!(record, AofRecord::Del { .. }));
    }

    #[test]
    fn to_aof_record_skips_failed_del() {
        let req = ShardRequest::Del { key: "k".into() };
        let resp = ShardResponse::Bool(false);
        assert!(to_aof_record(&req, &resp).is_none());
    }

    #[test]
    fn dispatch_incr_new_key() {
        let mut ks = Keyspace::new();
        let resp = dispatch(&mut ks, &ShardRequest::Incr { key: "c".into() });
        assert!(matches!(resp, ShardResponse::Integer(1)));
    }

    #[test]
    fn dispatch_decr_existing() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None);
        let resp = dispatch(&mut ks, &ShardRequest::Decr { key: "n".into() });
        assert!(matches!(resp, ShardResponse::Integer(9)));
    }

    #[test]
    fn dispatch_incr_non_integer() {
        let mut ks = Keyspace::new();
        ks.set("s".into(), Bytes::from("hello"), None);
        let resp = dispatch(&mut ks, &ShardRequest::Incr { key: "s".into() });
        assert!(matches!(resp, ShardResponse::Err(_)));
    }

    #[test]
    fn dispatch_incrby() {
        let mut ks = Keyspace::new();
        ks.set("n".into(), Bytes::from("10"), None);
        let resp = dispatch(
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
        ks.set("n".into(), Bytes::from("10"), None);
        let resp = dispatch(
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
        let resp = dispatch(
            &mut ks,
            &ShardRequest::IncrBy {
                key: "new".into(),
                delta: 42,
            },
        );
        assert!(matches!(resp, ShardResponse::Integer(42)));
    }

    #[test]
    fn to_aof_record_for_incr() {
        let req = ShardRequest::Incr { key: "c".into() };
        let resp = ShardResponse::Integer(1);
        let record = to_aof_record(&req, &resp).unwrap();
        assert!(matches!(record, AofRecord::Incr { .. }));
    }

    #[test]
    fn to_aof_record_for_decr() {
        let req = ShardRequest::Decr { key: "c".into() };
        let resp = ShardResponse::Integer(-1);
        let record = to_aof_record(&req, &resp).unwrap();
        assert!(matches!(record, AofRecord::Decr { .. }));
    }

    #[test]
    fn to_aof_record_for_incrby() {
        let req = ShardRequest::IncrBy {
            key: "c".into(),
            delta: 5,
        };
        let resp = ShardResponse::Integer(15);
        let record = to_aof_record(&req, &resp).unwrap();
        match record {
            AofRecord::IncrBy { key, delta } => {
                assert_eq!(key, "c");
                assert_eq!(delta, 5);
            }
            other => panic!("expected IncrBy, got {other:?}"),
        }
    }

    #[test]
    fn to_aof_record_for_decrby() {
        let req = ShardRequest::DecrBy {
            key: "c".into(),
            delta: 3,
        };
        let resp = ShardResponse::Integer(7);
        let record = to_aof_record(&req, &resp).unwrap();
        match record {
            AofRecord::DecrBy { key, delta } => {
                assert_eq!(key, "c");
                assert_eq!(delta, 3);
            }
            other => panic!("expected DecrBy, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_persist_removes_ttl() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::from_secs(60)),
        );

        let resp = dispatch(&mut ks, &ShardRequest::Persist { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = dispatch(&mut ks, &ShardRequest::Ttl { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Ttl(TtlResult::NoExpiry)));
    }

    #[test]
    fn dispatch_persist_missing_key() {
        let mut ks = Keyspace::new();
        let resp = dispatch(&mut ks, &ShardRequest::Persist { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Bool(false)));
    }

    #[test]
    fn dispatch_pttl() {
        let mut ks = Keyspace::new();
        ks.set(
            "key".into(),
            Bytes::from("val"),
            Some(Duration::from_secs(60)),
        );

        let resp = dispatch(&mut ks, &ShardRequest::Pttl { key: "key".into() });
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
        let resp = dispatch(&mut ks, &ShardRequest::Pttl { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Ttl(TtlResult::NotFound)));
    }

    #[test]
    fn dispatch_pexpire() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None);

        let resp = dispatch(
            &mut ks,
            &ShardRequest::Pexpire {
                key: "key".into(),
                milliseconds: 5000,
            },
        );
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = dispatch(&mut ks, &ShardRequest::Pttl { key: "key".into() });
        match resp {
            ShardResponse::Ttl(TtlResult::Milliseconds(ms)) => {
                assert!(ms > 4000 && ms <= 5000);
            }
            other => panic!("expected Ttl(Milliseconds), got {other:?}"),
        }
    }

    #[test]
    fn to_aof_record_for_persist() {
        let req = ShardRequest::Persist { key: "k".into() };
        let resp = ShardResponse::Bool(true);
        let record = to_aof_record(&req, &resp).unwrap();
        assert!(matches!(record, AofRecord::Persist { .. }));
    }

    #[test]
    fn to_aof_record_skips_failed_persist() {
        let req = ShardRequest::Persist { key: "k".into() };
        let resp = ShardResponse::Bool(false);
        assert!(to_aof_record(&req, &resp).is_none());
    }

    #[test]
    fn to_aof_record_for_pexpire() {
        let req = ShardRequest::Pexpire {
            key: "k".into(),
            milliseconds: 5000,
        };
        let resp = ShardResponse::Bool(true);
        let record = to_aof_record(&req, &resp).unwrap();
        match record {
            AofRecord::Pexpire { key, milliseconds } => {
                assert_eq!(key, "k");
                assert_eq!(milliseconds, 5000);
            }
            other => panic!("expected Pexpire, got {other:?}"),
        }
    }

    #[test]
    fn to_aof_record_skips_failed_pexpire() {
        let req = ShardRequest::Pexpire {
            key: "k".into(),
            milliseconds: 5000,
        };
        let resp = ShardResponse::Bool(false);
        assert!(to_aof_record(&req, &resp).is_none());
    }

    #[test]
    fn dispatch_set_nx_when_key_missing() {
        let mut ks = Keyspace::new();
        let resp = dispatch(
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
        ks.set("k".into(), Bytes::from("old"), None);

        let resp = dispatch(
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
        ks.set("k".into(), Bytes::from("old"), None);

        let resp = dispatch(
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
        let resp = dispatch(
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
    fn to_aof_record_skips_nx_blocked_set() {
        let req = ShardRequest::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire: None,
            nx: true,
            xx: false,
        };
        // when NX blocks, the shard returns Value(None), not Ok
        let resp = ShardResponse::Value(None);
        assert!(to_aof_record(&req, &resp).is_none());
    }

    #[test]
    fn dispatch_flushdb_clears_all_keys() {
        let mut ks = Keyspace::new();
        ks.set("a".into(), Bytes::from("1"), None);
        ks.set("b".into(), Bytes::from("2"), None);

        assert_eq!(ks.len(), 2);

        let resp = dispatch(&mut ks, &ShardRequest::FlushDb);
        assert!(matches!(resp, ShardResponse::Ok));
        assert_eq!(ks.len(), 0);
    }

    #[test]
    fn dispatch_scan_returns_keys() {
        let mut ks = Keyspace::new();
        ks.set("user:1".into(), Bytes::from("a"), None);
        ks.set("user:2".into(), Bytes::from("b"), None);
        ks.set("item:1".into(), Bytes::from("c"), None);

        let resp = dispatch(
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
        ks.set("user:1".into(), Bytes::from("a"), None);
        ks.set("user:2".into(), Bytes::from("b"), None);
        ks.set("item:1".into(), Bytes::from("c"), None);

        let resp = dispatch(
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
    fn to_aof_record_for_hset() {
        let req = ShardRequest::HSet {
            key: "h".into(),
            fields: vec![("f1".into(), Bytes::from("v1"))],
        };
        let resp = ShardResponse::Len(1);
        let record = to_aof_record(&req, &resp).unwrap();
        match record {
            AofRecord::HSet { key, fields } => {
                assert_eq!(key, "h");
                assert_eq!(fields.len(), 1);
            }
            _ => panic!("expected HSet record"),
        }
    }

    #[test]
    fn to_aof_record_for_hdel() {
        let req = ShardRequest::HDel {
            key: "h".into(),
            fields: vec!["f1".into(), "f2".into()],
        };
        let resp = ShardResponse::HDelLen {
            count: 2,
            removed: vec!["f1".into(), "f2".into()],
        };
        let record = to_aof_record(&req, &resp).unwrap();
        match record {
            AofRecord::HDel { key, fields } => {
                assert_eq!(key, "h");
                assert_eq!(fields.len(), 2);
            }
            _ => panic!("expected HDel record"),
        }
    }

    #[test]
    fn to_aof_record_skips_hdel_when_none_removed() {
        let req = ShardRequest::HDel {
            key: "h".into(),
            fields: vec!["f1".into()],
        };
        let resp = ShardResponse::HDelLen {
            count: 0,
            removed: vec![],
        };
        assert!(to_aof_record(&req, &resp).is_none());
    }

    #[test]
    fn to_aof_record_for_hincrby() {
        let req = ShardRequest::HIncrBy {
            key: "h".into(),
            field: "counter".into(),
            delta: 5,
        };
        let resp = ShardResponse::Integer(10);
        let record = to_aof_record(&req, &resp).unwrap();
        match record {
            AofRecord::HIncrBy { key, field, delta } => {
                assert_eq!(key, "h");
                assert_eq!(field, "counter");
                assert_eq!(delta, 5);
            }
            _ => panic!("expected HIncrBy record"),
        }
    }

    #[test]
    fn to_aof_record_for_sadd() {
        let req = ShardRequest::SAdd {
            key: "s".into(),
            members: vec!["m1".into(), "m2".into()],
        };
        let resp = ShardResponse::Len(2);
        let record = to_aof_record(&req, &resp).unwrap();
        match record {
            AofRecord::SAdd { key, members } => {
                assert_eq!(key, "s");
                assert_eq!(members.len(), 2);
            }
            _ => panic!("expected SAdd record"),
        }
    }

    #[test]
    fn to_aof_record_skips_sadd_when_none_added() {
        let req = ShardRequest::SAdd {
            key: "s".into(),
            members: vec!["m1".into()],
        };
        let resp = ShardResponse::Len(0);
        assert!(to_aof_record(&req, &resp).is_none());
    }

    #[test]
    fn to_aof_record_for_srem() {
        let req = ShardRequest::SRem {
            key: "s".into(),
            members: vec!["m1".into()],
        };
        let resp = ShardResponse::Len(1);
        let record = to_aof_record(&req, &resp).unwrap();
        match record {
            AofRecord::SRem { key, members } => {
                assert_eq!(key, "s");
                assert_eq!(members.len(), 1);
            }
            _ => panic!("expected SRem record"),
        }
    }

    #[test]
    fn to_aof_record_skips_srem_when_none_removed() {
        let req = ShardRequest::SRem {
            key: "s".into(),
            members: vec!["m1".into()],
        };
        let resp = ShardResponse::Len(0);
        assert!(to_aof_record(&req, &resp).is_none());
    }
}
