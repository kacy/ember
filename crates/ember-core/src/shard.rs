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
use ember_persistence::recovery;
use ember_persistence::snapshot::{self, SnapEntry, SnapshotWriter};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use crate::error::ShardError;
use crate::expiry;
use crate::keyspace::{Keyspace, KeyspaceStats, SetResult, ShardConfig, TtlResult};
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
    /// Returns the key count for this shard.
    DbSize,
    /// Returns keyspace stats for this shard.
    Stats,
    /// Triggers a snapshot write.
    Snapshot,
    /// Triggers an AOF rewrite (snapshot + truncate AOF).
    RewriteAof,
}

/// The shard's response to a request.
#[derive(Debug)]
pub enum ShardResponse {
    /// A value (or None for a cache miss).
    Value(Option<Value>),
    /// Simple acknowledgement (e.g. SET).
    Ok,
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
    /// Command used against a key holding the wrong kind of value.
    WrongType,
    /// An error message.
    Err(String),
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
            let value = Value::String(entry.value);
            keyspace.restore(entry.key, value, entry.expires_at);
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
        ShardRequest::Set { key, value, expire } => {
            match ks.set(key.clone(), value.clone(), *expire) {
                SetResult::Ok => ShardResponse::Ok,
                SetResult::OutOfMemory => ShardResponse::OutOfMemory,
            }
        }
        ShardRequest::Del { key } => ShardResponse::Bool(ks.del(key)),
        ShardRequest::Exists { key } => ShardResponse::Bool(ks.exists(key)),
        ShardRequest::Expire { key, seconds } => ShardResponse::Bool(ks.expire(key, *seconds)),
        ShardRequest::Ttl { key } => ShardResponse::Ttl(ks.ttl(key)),
        ShardRequest::DbSize => ShardResponse::KeyCount(ks.len()),
        ShardRequest::Stats => ShardResponse::Stats(ks.stats()),
        // snapshot/rewrite are handled in the main loop, not here
        ShardRequest::Snapshot | ShardRequest::RewriteAof => ShardResponse::Ok,
    }
}

/// Converts a successful mutation request+response pair into an AOF record.
/// Returns None for non-mutation requests or failed mutations.
fn to_aof_record(req: &ShardRequest, resp: &ShardResponse) -> Option<AofRecord> {
    match (req, resp) {
        (
            ShardRequest::Set { key, value, expire },
            ShardResponse::Ok,
        ) => {
            let expire_ms = expire
                .map(|d| d.as_millis() as i64)
                .unwrap_or(-1);
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
        // only persist string values for now — list and sorted set
        // persistence is added in later commits
        let value_bytes = match value {
            Value::String(data) => data.clone(),
            _ => continue,
        };
        writer.write_entry(&SnapEntry {
            key: key.to_owned(),
            value: value_bytes,
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
            ShardResponse::Ttl(TtlResult::Seconds(s)) => assert!(s >= 58 && s <= 60),
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
            })
            .await
            .unwrap();

        // also set a persistent key
        handle
            .send(ShardRequest::Set {
                key: "persistent".into(),
                value: Bytes::from("stays"),
                expire: None,
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
                })
                .await
                .unwrap();
            handle
                .send(ShardRequest::Set {
                    key: "b".into(),
                    value: Bytes::from("2"),
                    expire: Some(Duration::from_secs(300)),
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
}
