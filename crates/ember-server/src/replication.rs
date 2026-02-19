//! Replication stream: primary → replica data sync.
//!
//! The primary side (`ReplicationServer`) accepts TCP connections from
//! replicas and streams all mutations as `AofRecord` frames after an
//! initial full-sync snapshot. The replica side (`ReplicationClient`)
//! connects, loads the snapshot, and applies incremental records.
//!
//! # Wire protocol
//!
//! All integers are little-endian.
//!
//! ```text
//! // Replica → primary (handshake request):
//! [version: 1B][num_shards: 2B]
//!
//! // Primary → replica (handshake response):
//! [version: 1B][num_shards: 2B][primary_id_len: 1B][primary_id: N bytes]
//! [status: 1B]   (0 = ok, 1 = shard count mismatch)
//!
//! // For each shard (if status = 0):
//! [MSG_SHARD_SYNC: 1B][shard_id: 2B][snapshot_len: 4B][snapshot_bytes]
//! [MSG_SHARD_OFFSET: 1B][shard_id: 2B][offset: 8B]
//!
//! // Incremental records (unbounded stream):
//! [MSG_RECORD: 1B][shard_id: 2B][offset: 8B][record_len: 4B][record_bytes]
//!
//! // When replica falls behind (broadcast lag):
//! [MSG_RESYNC: 1B]    primary closes the connection; replica reconnects
//! ```

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use ember_core::{Engine, ShardRequest, ShardResponse};
use ember_persistence::aof::AofRecord;
use ember_persistence::snapshot::{self, SnapValue};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

// -- protocol constants --

const REPL_VERSION: u8 = 1;
const STATUS_OK: u8 = 0;
const STATUS_SHARD_MISMATCH: u8 = 1;

const MSG_SHARD_SYNC: u8 = 2;
const MSG_SHARD_OFFSET: u8 = 3;
const MSG_RECORD: u8 = 4;
const MSG_RESYNC: u8 = 5;

// -- framed I/O primitives --
//
// thin wrappers around AsyncReadExt/AsyncWriteExt for the little-endian binary
// wire protocol. each wraps exactly one syscall; no buffering happens here.

async fn write_u8(w: &mut TcpStream, val: u8) -> std::io::Result<()> {
    w.write_all(&[val]).await
}

async fn write_u16_le(w: &mut TcpStream, val: u16) -> std::io::Result<()> {
    w.write_all(&val.to_le_bytes()).await
}

async fn write_u32_le(w: &mut TcpStream, val: u32) -> std::io::Result<()> {
    w.write_all(&val.to_le_bytes()).await
}

async fn write_u64_le(w: &mut TcpStream, val: u64) -> std::io::Result<()> {
    w.write_all(&val.to_le_bytes()).await
}

async fn read_u8(r: &mut TcpStream) -> std::io::Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf).await?;
    Ok(buf[0])
}

async fn read_u16_le(r: &mut TcpStream) -> std::io::Result<u16> {
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf).await?;
    Ok(u16::from_le_bytes(buf))
}

async fn read_u32_le(r: &mut TcpStream) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf).await?;
    Ok(u32::from_le_bytes(buf))
}

async fn read_u64_le(r: &mut TcpStream) -> std::io::Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf).await?;
    Ok(u64::from_le_bytes(buf))
}

/// Reads one byte and returns an error if it doesn't match `expected`.
///
/// Used throughout the handshake and snapshot-load path to assert that
/// the primary sent the message type we expected at this point in the
/// protocol.
async fn expect_tag(stream: &mut TcpStream, expected: u8, label: &str) -> io::Result<()> {
    let tag = read_u8(stream).await?;
    if tag != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected {label} ({}), got {tag}", expected),
        ));
    }
    Ok(())
}

// -- primary-side server --

/// Listens for incoming replica connections and drives replication.
///
/// Each accepted connection performs a full-sync snapshot handshake
/// followed by an incremental stream from the broadcast channel.
pub struct ReplicationServer {
    engine: Arc<Engine>,
    primary_id: String,
}

impl ReplicationServer {
    /// Binds the TCP listener and starts accepting replica connections.
    ///
    /// Runs indefinitely in the background; returns immediately after
    /// spawning the accept loop task.
    pub async fn start(
        engine: Arc<Engine>,
        primary_id: String,
        port: u16,
    ) -> std::io::Result<()> {
        let bind_addr = format!("0.0.0.0:{port}");
        let listener = TcpListener::bind(&bind_addr).await?;
        info!(port, "replication server listening");

        let server = Arc::new(Self {
            engine,
            primary_id,
        });

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        debug!(%peer, "replica connected");
                        let server = Arc::clone(&server);
                        tokio::spawn(async move {
                            if let Err(e) = server.handle_replica(stream).await {
                                debug!(%peer, "replication connection closed: {e}");
                            }
                        });
                    }
                    Err(e) => {
                        error!("replication accept error: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });

        Ok(())
    }

    /// Handles a single replica connection: handshake + full sync + stream.
    async fn handle_replica(&self, mut stream: TcpStream) -> std::io::Result<()> {
        // read replica handshake
        let replica_version = read_u8(&mut stream).await?;
        if replica_version != REPL_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported replication version: {replica_version}"),
            ));
        }
        let replica_shards = read_u16_le(&mut stream).await?;
        let our_shards = self.engine.shard_count() as u16;

        // send primary handshake response
        write_u8(&mut stream, REPL_VERSION).await?;
        write_u16_le(&mut stream, our_shards).await?;
        let id_bytes = self.primary_id.as_bytes();
        write_u8(&mut stream, id_bytes.len() as u8).await?;
        stream.write_all(id_bytes).await?;

        if replica_shards != our_shards {
            write_u8(&mut stream, STATUS_SHARD_MISMATCH).await?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "shard count mismatch: replica={replica_shards} primary={our_shards}"
                ),
            ));
        }
        write_u8(&mut stream, STATUS_OK).await?;

        // subscribe to the broadcast channel before snapshotting so we don't
        // miss events that happen between snapshot and stream start
        let mut rx = match self.engine.subscribe_replication() {
            Some(rx) => rx,
            None => {
                return Err(std::io::Error::other("replication channel not configured on this engine"));
            }
        };

        // full sync: snapshot each shard and send
        for shard_idx in 0..self.engine.shard_count() {
            let resp = self
                .engine
                .send_to_shard(shard_idx, ShardRequest::SerializeSnapshot)
                .await
                .map_err(|e| {
                    std::io::Error::other(format!("shard {shard_idx} serialize failed: {e:?}"))
                })?;

            let (shard_id, data) = match resp {
                ShardResponse::SnapshotData { shard_id, data } => (shard_id, data),
                other => {
                    return Err(std::io::Error::other(format!("unexpected shard response: {other:?}")));
                }
            };

            let data_len = u32::try_from(data.len()).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "snapshot too large")
            })?;

            write_u8(&mut stream, MSG_SHARD_SYNC).await?;
            write_u16_le(&mut stream, shard_id).await?;
            write_u32_le(&mut stream, data_len).await?;
            stream.write_all(&data).await?;

            // send the current replication offset for this shard (0 until we
            // track per-shard offsets; good enough for gap detection)
            write_u8(&mut stream, MSG_SHARD_OFFSET).await?;
            write_u16_le(&mut stream, shard_id).await?;
            write_u64_le(&mut stream, 0u64).await?;
        }

        stream.flush().await?;
        info!("full sync complete, starting incremental stream");

        // incremental stream: relay events from the broadcast channel
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let record_bytes = event.record.to_bytes().map_err(|e| {
                        std::io::Error::other(format!("record serialization failed: {e}"))
                    })?;
                    let record_len = u32::try_from(record_bytes.len()).map_err(|_| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "record too large",
                        )
                    })?;

                    write_u8(&mut stream, MSG_RECORD).await?;
                    write_u16_le(&mut stream, event.shard_id).await?;
                    write_u64_le(&mut stream, event.offset).await?;
                    write_u32_le(&mut stream, record_len).await?;
                    stream.write_all(&record_bytes).await?;
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    warn!("replication stream lagged by {count} events; triggering resync");
                    let _ = write_u8(&mut stream, MSG_RESYNC).await;
                    let _ = stream.flush().await;
                    return Ok(());
                }
                Err(broadcast::error::RecvError::Closed) => {
                    info!("replication broadcast channel closed; disconnecting replica");
                    return Ok(());
                }
            }
        }
    }
}

// -- replica-side client --

/// Connects to a primary's replication port and applies the incoming
/// snapshot and incremental record stream to the local engine.
///
/// Reconnects automatically with exponential backoff on failure.
pub struct ReplicationClient {
    engine: Arc<Engine>,
    primary_addr: SocketAddr,
}

impl ReplicationClient {
    /// Starts the replication client in a background task.
    ///
    /// Connects to `primary_addr` and applies the stream indefinitely,
    /// reconnecting with backoff on any error.
    pub fn start(engine: Arc<Engine>, primary_addr: SocketAddr) {
        let client = Arc::new(Self {
            engine,
            primary_addr,
        });
        tokio::spawn(async move {
            client.run().await;
        });
    }

    async fn run(&self) {
        let mut backoff = Duration::from_millis(500);
        const MAX_BACKOFF: Duration = Duration::from_secs(30);

        loop {
            info!(primary = %self.primary_addr, "connecting to primary for replication");
            match TcpStream::connect(self.primary_addr).await {
                Ok(stream) => {
                    match self.sync(stream).await {
                        Ok(()) => {
                            info!("replication connection ended cleanly");
                        }
                        Err(e) => {
                            warn!("replication error: {e}");
                        }
                    }
                }
                Err(e) => {
                    warn!(primary = %self.primary_addr, "failed to connect to primary: {e}");
                }
            }

            // back off before reconnecting
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    /// Performs full sync + incremental stream for one connection session.
    async fn sync(&self, mut stream: TcpStream) -> std::io::Result<()> {
        let our_shards = self.engine.shard_count() as u16;

        // send handshake
        write_u8(&mut stream, REPL_VERSION).await?;
        write_u16_le(&mut stream, our_shards).await?;

        // read primary response
        let primary_version = read_u8(&mut stream).await?;
        if primary_version != REPL_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported primary replication version: {primary_version}"),
            ));
        }
        let primary_shards = read_u16_le(&mut stream).await?;
        let id_len = read_u8(&mut stream).await? as usize;
        let mut id_buf = vec![0u8; id_len];
        stream.read_exact(&mut id_buf).await?;
        let primary_id = String::from_utf8_lossy(&id_buf).into_owned();

        let status = read_u8(&mut stream).await?;
        if status == STATUS_SHARD_MISMATCH {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "shard count mismatch with primary {primary_id}: \
                     ours={our_shards} primary={primary_shards}"
                ),
            ));
        }

        info!(primary_id = %primary_id, "handshake ok, loading full sync");

        // receive per-shard snapshots
        let mut snap_buf = Vec::new();
        for _ in 0..primary_shards {
            expect_tag(&mut stream, MSG_SHARD_SYNC, "MSG_SHARD_SYNC").await?;
            let shard_id = read_u16_le(&mut stream).await?;
            let snap_len = read_u32_le(&mut stream).await? as usize;

            snap_buf.clear();
            snap_buf.resize(snap_len, 0);
            stream.read_exact(&mut snap_buf).await?;

            // apply snapshot to the engine
            self.apply_snapshot(shard_id, &snap_buf).await?;

            // read (and ignore for now) the shard offset tag
            expect_tag(&mut stream, MSG_SHARD_OFFSET, "MSG_SHARD_OFFSET").await?;
            let _recv_shard_id = read_u16_le(&mut stream).await?;
            let _recv_offset = read_u64_le(&mut stream).await?;
        }

        info!("full sync applied, starting incremental replay");

        // incremental stream
        loop {
            let msg = read_u8(&mut stream).await?;
            match msg {
                MSG_RECORD => {
                    let _shard_id = read_u16_le(&mut stream).await?;
                    let _offset = read_u64_le(&mut stream).await?;
                    let record_len = read_u32_le(&mut stream).await? as usize;
                    let mut record_bytes = vec![0u8; record_len];
                    stream.read_exact(&mut record_bytes).await?;

                    let record = AofRecord::from_bytes(&record_bytes).map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid AOF record: {e}"),
                        )
                    })?;

                    if let Some(request) = aof_record_to_shard_request(&record) {
                        if let Some(key) = primary_key_for_request(&request).map(str::to_owned) {
                            if let Err(e) = self.engine.route(&key, request).await {
                                warn!("replication apply failed: {e:?}");
                            }
                        }
                    }
                }
                MSG_RESYNC => {
                    info!("primary requested resync; reconnecting");
                    return Ok(());
                }
                other => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("unknown replication message type: {other}"),
                    ));
                }
            }
        }
    }

    /// Applies a snapshot blob to the local engine for the given shard.
    async fn apply_snapshot(&self, _shard_id: u16, data: &[u8]) -> std::io::Result<()> {
        let (_, entries) = snapshot::read_snapshot_from_bytes(data).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("snapshot parse error: {e}"),
            )
        })?;

        // flush the existing shard state before loading the snapshot
        // we route by key so route() picks the right shard automatically
        for entry in entries {
            let value: Bytes = match &entry.value {
                SnapValue::String(data) => data.clone(),
                // for non-string types, reconstruct via the appropriate request
                _ => {
                    self.apply_snap_entry(entry).await;
                    continue;
                }
            };

            let request = ShardRequest::Set {
                key: entry.key.clone(),
                value,
                expire: expire_from_ms(entry.expire_ms),
                nx: false,
                xx: false,
            };
            if let Err(e) = self.engine.route(&entry.key, request).await {
                warn!(key = %entry.key, "snapshot restore failed: {e:?}");
            }
        }

        Ok(())
    }

    /// Restores a non-string snapshot entry by converting it to the
    /// appropriate write request(s).
    async fn apply_snap_entry(&self, entry: ember_persistence::snapshot::SnapEntry) {
        use ember_persistence::snapshot::SnapValue;

        let key = entry.key.clone();
        let expire = expire_from_ms(entry.expire_ms);

        match entry.value {
            SnapValue::List(deque) => {
                let values: Vec<Bytes> = deque.into_iter().collect();
                let req = ShardRequest::RPush {
                    key: key.clone(),
                    values,
                };
                if let Err(e) = self.engine.route(&key, req).await {
                    warn!(%key, "list restore failed: {e:?}");
                }
            }
            SnapValue::SortedSet(members) => {
                let req = ShardRequest::ZAdd {
                    key: key.clone(),
                    members,
                    nx: false,
                    xx: false,
                    gt: false,
                    lt: false,
                    ch: false,
                };
                if let Err(e) = self.engine.route(&key, req).await {
                    warn!(%key, "sorted set restore failed: {e:?}");
                }
            }
            SnapValue::Hash(map) => {
                let fields: Vec<(String, Bytes)> = map.into_iter().collect();
                let req = ShardRequest::HSet {
                    key: key.clone(),
                    fields,
                };
                if let Err(e) = self.engine.route(&key, req).await {
                    warn!(%key, "hash restore failed: {e:?}");
                }
            }
            SnapValue::Set(set) => {
                let members: Vec<String> = set.into_iter().collect();
                let req = ShardRequest::SAdd {
                    key: key.clone(),
                    members,
                };
                if let Err(e) = self.engine.route(&key, req).await {
                    warn!(%key, "set restore failed: {e:?}");
                }
            }
            // strings are handled by the caller
            SnapValue::String(_) => {}
            #[cfg(feature = "vector")]
            SnapValue::Vector { .. } => {
                // vector restoration is complex; skip for now
                warn!(%key, "vector snapshot restore not yet supported in replication");
            }
            #[cfg(feature = "protobuf")]
            SnapValue::Proto { type_name: _, data } => {
                // proto values serialize to Set (raw bytes)
                let req = ShardRequest::Set {
                    key: key.clone(),
                    value: data,
                    expire,
                    nx: false,
                    xx: false,
                };
                if let Err(e) = self.engine.route(&key, req).await {
                    warn!(%key, "proto snapshot restore failed: {e:?}");
                }
            }
        }

        // apply TTL if any
        if let Some(expire_duration) = expire {
            let ms = expire_duration.as_millis() as u64;
            let req = ShardRequest::Pexpire {
                key: key.clone(),
                milliseconds: ms,
            };
            if let Err(e) = self.engine.route(&key, req).await {
                warn!(%key, "pexpire after restore failed: {e:?}");
            }
        }
    }
}

/// Converts a millisecond expiry field to an `Option<Duration>`.
///
/// AOF and snapshot records store `-1` (or any non-positive value) to
/// indicate "no expiry". A positive `ms` becomes `Some(Duration)`.
fn expire_from_ms(ms: i64) -> Option<Duration> {
    (ms > 0).then(|| Duration::from_millis(ms as u64))
}

// -- AofRecord → ShardRequest conversion --

/// Converts an `AofRecord` into the equivalent `ShardRequest` for replay.
///
/// Returns `None` for record types that have no meaningful replay action
/// (e.g. schema registration, which is handled at startup).
pub fn aof_record_to_shard_request(record: &AofRecord) -> Option<ShardRequest> {
    match record {
        AofRecord::Set {
            key,
            value,
            expire_ms,
        } => Some(ShardRequest::Set {
            key: key.clone(),
            value: value.clone(),
            expire: expire_from_ms(*expire_ms),
            nx: false,
            xx: false,
        }),
        AofRecord::Del { key } => Some(ShardRequest::Del { key: key.clone() }),
        AofRecord::Expire { key, seconds } => Some(ShardRequest::Expire {
            key: key.clone(),
            seconds: *seconds,
        }),
        AofRecord::LPush { key, values } => Some(ShardRequest::LPush {
            key: key.clone(),
            values: values.clone(),
        }),
        AofRecord::RPush { key, values } => Some(ShardRequest::RPush {
            key: key.clone(),
            values: values.clone(),
        }),
        AofRecord::LPop { key } => Some(ShardRequest::LPop { key: key.clone() }),
        AofRecord::RPop { key } => Some(ShardRequest::RPop { key: key.clone() }),
        AofRecord::ZAdd { key, members } => Some(ShardRequest::ZAdd {
            key: key.clone(),
            members: members.clone(),
            nx: false,
            xx: false,
            gt: false,
            lt: false,
            ch: false,
        }),
        AofRecord::ZRem { key, members } => Some(ShardRequest::ZRem {
            key: key.clone(),
            members: members.clone(),
        }),
        AofRecord::Persist { key } => Some(ShardRequest::Persist { key: key.clone() }),
        AofRecord::Pexpire { key, milliseconds } => Some(ShardRequest::Pexpire {
            key: key.clone(),
            milliseconds: *milliseconds,
        }),
        AofRecord::Incr { key } => Some(ShardRequest::Incr { key: key.clone() }),
        AofRecord::Decr { key } => Some(ShardRequest::Decr { key: key.clone() }),
        AofRecord::HSet { key, fields } => Some(ShardRequest::HSet {
            key: key.clone(),
            fields: fields.clone(),
        }),
        AofRecord::HDel { key, fields } => Some(ShardRequest::HDel {
            key: key.clone(),
            fields: fields.clone(),
        }),
        AofRecord::HIncrBy { key, field, delta } => Some(ShardRequest::HIncrBy {
            key: key.clone(),
            field: field.clone(),
            delta: *delta,
        }),
        AofRecord::SAdd { key, members } => Some(ShardRequest::SAdd {
            key: key.clone(),
            members: members.clone(),
        }),
        AofRecord::SRem { key, members } => Some(ShardRequest::SRem {
            key: key.clone(),
            members: members.clone(),
        }),
        AofRecord::IncrBy { key, delta } => Some(ShardRequest::IncrBy {
            key: key.clone(),
            delta: *delta,
        }),
        AofRecord::DecrBy { key, delta } => Some(ShardRequest::DecrBy {
            key: key.clone(),
            delta: *delta,
        }),
        AofRecord::Append { key, value } => Some(ShardRequest::Append {
            key: key.clone(),
            value: value.clone(),
        }),
        AofRecord::Rename { key, newkey } => Some(ShardRequest::Rename {
            key: key.clone(),
            newkey: newkey.clone(),
        }),
        #[cfg(feature = "vector")]
        AofRecord::VAdd { .. } | AofRecord::VRem { .. } => {
            // vector replication not yet supported
            None
        }
        #[cfg(feature = "protobuf")]
        AofRecord::ProtoSet { .. }
        | AofRecord::ProtoRegister { .. } => {
            // protobuf replication not yet supported
            None
        }
    }
}

/// Returns a reference to the primary key of a `ShardRequest` for routing.
fn primary_key_for_request(req: &ShardRequest) -> Option<&str> {
    match req {
        ShardRequest::Set { key, .. }
        | ShardRequest::Del { key }
        | ShardRequest::Unlink { key }
        | ShardRequest::Expire { key, .. }
        | ShardRequest::Persist { key }
        | ShardRequest::Pexpire { key, .. }
        | ShardRequest::Incr { key }
        | ShardRequest::Decr { key }
        | ShardRequest::IncrBy { key, .. }
        | ShardRequest::DecrBy { key, .. }
        | ShardRequest::IncrByFloat { key, .. }
        | ShardRequest::Append { key, .. }
        | ShardRequest::LPush { key, .. }
        | ShardRequest::RPush { key, .. }
        | ShardRequest::LPop { key }
        | ShardRequest::RPop { key }
        | ShardRequest::ZAdd { key, .. }
        | ShardRequest::ZRem { key, .. }
        | ShardRequest::HSet { key, .. }
        | ShardRequest::HDel { key, .. }
        | ShardRequest::HIncrBy { key, .. }
        | ShardRequest::SAdd { key, .. }
        | ShardRequest::SRem { key, .. }
        | ShardRequest::Rename { key, .. } => Some(key),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn aof_set_roundtrip() {
        let record = AofRecord::Set {
            key: "foo".into(),
            value: Bytes::from("bar"),
            expire_ms: 5000,
        };
        let req = aof_record_to_shard_request(&record).expect("Set should map to ShardRequest");
        match req {
            ShardRequest::Set { key, value, expire, nx, xx } => {
                assert_eq!(key, "foo");
                assert_eq!(value, Bytes::from("bar"));
                assert_eq!(expire, Some(Duration::from_millis(5000)));
                assert!(!nx && !xx);
            }
            other => panic!("expected ShardRequest::Set, got {other:?}"),
        }
    }

    #[test]
    fn aof_del_roundtrip() {
        let record = AofRecord::Del { key: "gone".into() };
        let req = aof_record_to_shard_request(&record).unwrap();
        assert!(matches!(req, ShardRequest::Del { key } if key == "gone"));
    }

    #[test]
    fn primary_key_set() {
        let req = ShardRequest::Set {
            key: "mykey".into(),
            value: Bytes::new(),
            expire: None,
            nx: false,
            xx: false,
        };
        assert_eq!(primary_key_for_request(&req), Some("mykey"));
    }
}
