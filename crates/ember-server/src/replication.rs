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

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use ember_core::{Engine, ShardRequest, ShardResponse};
use ember_persistence::aof::AofRecord;
use ember_persistence::snapshot::{self, SnapValue};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
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
const MSG_ACK: u8 = 6;

/// Tracks per-replica acknowledged write offsets for the WAIT command.
///
/// The primary increments `write_offset` for each record forwarded to
/// replicas. Each replica sends back MSG_ACK frames reporting its
/// current offset. WAIT polls `count_at_or_above(target)` with a
/// deadline to determine when enough replicas are in sync.
#[derive(Debug)]
pub struct ReplicaTracker {
    /// Monotonically increasing counter of records forwarded by this primary.
    pub write_offset: AtomicU64,
    /// Per-replica last acknowledged offset. Keyed by a u64 replica ID
    /// assigned sequentially at connection time.
    offsets: Mutex<HashMap<u64, u64>>,
    /// Next replica ID to assign.
    next_id: AtomicU64,
}

impl ReplicaTracker {
    pub fn new() -> Self {
        Self {
            write_offset: AtomicU64::new(0),
            offsets: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
        }
    }

    /// Registers a new replica connection. Returns its unique ID.
    pub fn register(&self) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut map) = self.offsets.lock() {
            map.insert(id, 0);
        }
        id
    }

    /// Removes a replica connection from tracking.
    pub fn remove(&self, replica_id: u64) {
        if let Ok(mut map) = self.offsets.lock() {
            map.remove(&replica_id);
        }
    }

    /// Updates the acknowledged offset for a replica.
    ///
    /// Only advances forward — never decrements.
    pub fn update(&self, replica_id: u64, offset: u64) {
        if let Ok(mut map) = self.offsets.lock() {
            let entry = map.entry(replica_id).or_insert(0);
            if offset > *entry {
                *entry = offset;
            }
        }
    }

    /// Returns the number of replicas whose acked offset is >= `target`.
    pub fn count_at_or_above(&self, target: u64) -> usize {
        self.offsets
            .lock()
            .map(|map| map.values().filter(|&&v| v >= target).count())
            .unwrap_or(0)
    }

    /// Returns the total number of currently connected replicas.
    pub fn connected_count(&self) -> usize {
        self.offsets.lock().map(|map| map.len()).unwrap_or(0)
    }

    /// Returns the record lag for each connected replica.
    ///
    /// Lag is `write_offset - acked_offset`. A lag of 0 means fully caught up.
    pub fn replica_lags(&self) -> Vec<u64> {
        let write = self.write_offset.load(Ordering::Relaxed);
        self.offsets
            .lock()
            .map(|map| map.values().map(|&ack| write.saturating_sub(ack)).collect())
            .unwrap_or_default()
    }
}

// -- framed I/O primitives --
//
// generic over AsyncRead/AsyncWrite so they work with both raw TcpStream
// and BufWriter<TcpStream>. the primary-side write path wraps the stream
// in a BufWriter, so small writes (tag, shard_id, offset, length) are
// coalesced into a single syscall per record rather than 4-5 separate ones.

async fn write_u8(w: &mut (impl AsyncWrite + Unpin), val: u8) -> std::io::Result<()> {
    w.write_all(&[val]).await
}

async fn write_u16_le(w: &mut (impl AsyncWrite + Unpin), val: u16) -> std::io::Result<()> {
    w.write_all(&val.to_le_bytes()).await
}

async fn write_u32_le(w: &mut (impl AsyncWrite + Unpin), val: u32) -> std::io::Result<()> {
    w.write_all(&val.to_le_bytes()).await
}

async fn write_u64_le(w: &mut (impl AsyncWrite + Unpin), val: u64) -> std::io::Result<()> {
    w.write_all(&val.to_le_bytes()).await
}

async fn read_u8(r: &mut (impl AsyncRead + Unpin)) -> std::io::Result<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf).await?;
    Ok(buf[0])
}

async fn read_u16_le(r: &mut (impl AsyncRead + Unpin)) -> std::io::Result<u16> {
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf).await?;
    Ok(u16::from_le_bytes(buf))
}

async fn read_u32_le(r: &mut (impl AsyncRead + Unpin)) -> std::io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf).await?;
    Ok(u32::from_le_bytes(buf))
}

async fn read_u64_le(r: &mut (impl AsyncRead + Unpin)) -> std::io::Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf).await?;
    Ok(u64::from_le_bytes(buf))
}

/// Reads one byte and returns an error if it doesn't match `expected`.
///
/// Used throughout the handshake and snapshot-load path to assert that
/// the primary sent the message type we expected at this point in the
/// protocol.
async fn expect_tag(
    stream: &mut (impl AsyncRead + Unpin),
    expected: u8,
    label: &str,
) -> io::Result<()> {
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
    tracker: Arc<ReplicaTracker>,
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
        tracker: Arc<ReplicaTracker>,
    ) -> std::io::Result<()> {
        let bind_addr = format!("0.0.0.0:{port}");
        let listener = TcpListener::bind(&bind_addr).await?;
        info!(port, "replication server listening");

        let server = Arc::new(Self {
            engine,
            primary_id,
            tracker,
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
    async fn handle_replica(&self, stream: TcpStream) -> std::io::Result<()> {
        // wrap in a 64 KiB write buffer so the 4-5 small framing writes
        // (tag, shard_id, offset, length) per record are coalesced into
        // a single syscall. reads are delegated to the inner TcpStream.
        let mut stream = BufWriter::with_capacity(65536, stream);

        // --- handshake ---
        let replica_version = read_u8(&mut stream).await?;
        if replica_version != REPL_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unsupported replication version: {replica_version}"),
            ));
        }
        let replica_shards = read_u16_le(&mut stream).await?;
        let our_shards = self.engine.shard_count() as u16;

        write_u8(&mut stream, REPL_VERSION).await?;
        write_u16_le(&mut stream, our_shards).await?;
        let id_bytes = self.primary_id.as_bytes();
        write_u8(&mut stream, id_bytes.len() as u8).await?;
        stream.write_all(id_bytes).await?;

        if replica_shards != our_shards {
            write_u8(&mut stream, STATUS_SHARD_MISMATCH).await?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("shard count mismatch: replica={replica_shards} primary={our_shards}"),
            ));
        }
        write_u8(&mut stream, STATUS_OK).await?;

        // subscribe before snapshotting so we don't miss events that
        // happen between snapshot and incremental stream start
        let mut rx = match self.engine.subscribe_replication() {
            Some(rx) => rx,
            None => {
                return Err(std::io::Error::other(
                    "replication channel not configured on this engine",
                ));
            }
        };

        // --- full sync ---
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
                    return Err(std::io::Error::other(format!(
                        "unexpected shard response: {other:?}"
                    )));
                }
            };

            let data_len = u32::try_from(data.len()).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "snapshot too large")
            })?;

            write_u8(&mut stream, MSG_SHARD_SYNC).await?;
            write_u16_le(&mut stream, shard_id).await?;
            write_u32_le(&mut stream, data_len).await?;
            stream.write_all(&data).await?;

            write_u8(&mut stream, MSG_SHARD_OFFSET).await?;
            write_u16_le(&mut stream, shard_id).await?;
            write_u64_le(&mut stream, 0u64).await?;
        }

        stream.flush().await?;
        info!("full sync complete, starting incremental stream");

        // --- split stream for concurrent read (ACKs) and write (records) ---
        // Take the inner TcpStream back from BufWriter, then split.
        let inner = stream.into_inner();
        let (read_half, write_inner) = tokio::io::split(inner);
        let mut writer = BufWriter::with_capacity(65536, write_inner);

        // Register this replica and spawn an ACK reader task.
        let replica_id = self.tracker.register();
        let tracker = Arc::clone(&self.tracker);
        let mut ack_reader = read_half;

        let ack_task = tokio::spawn(async move {
            loop {
                match read_u8(&mut ack_reader).await {
                    Ok(MSG_ACK) => match read_u64_le(&mut ack_reader).await {
                        Ok(offset) => tracker.update(replica_id, offset),
                        Err(_) => break,
                    },
                    Ok(_) => {} // unknown or future message types — ignore
                    Err(_) => break,
                }
            }
        });

        // --- incremental stream ---
        let result = self.stream_records(&mut writer, &mut rx).await;

        // clean up regardless of result
        ack_task.abort();
        self.tracker.remove(replica_id);

        result
    }

    /// Streams replication records to the replica until the connection closes
    /// or the broadcast channel is exhausted.
    async fn stream_records(
        &self,
        writer: &mut BufWriter<tokio::io::WriteHalf<TcpStream>>,
        rx: &mut broadcast::Receiver<ember_core::ReplicationEvent>,
    ) -> std::io::Result<()> {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let record_bytes = event.record.to_bytes().map_err(|e| {
                        std::io::Error::other(format!("record serialization failed: {e}"))
                    })?;
                    let record_len = u32::try_from(record_bytes.len()).map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, "record too large")
                    })?;

                    write_u8(writer, MSG_RECORD).await?;
                    write_u16_le(writer, event.shard_id).await?;
                    write_u64_le(writer, event.offset).await?;
                    write_u32_le(writer, record_len).await?;
                    writer.write_all(&record_bytes).await?;
                    writer.flush().await?;

                    // advance the primary's write offset AFTER successfully
                    // flushing to the replica's TCP buffer
                    self.tracker.write_offset.fetch_add(1, Ordering::Relaxed);
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    warn!("replication stream lagged by {count} events; triggering resync");
                    let _ = write_u8(writer, MSG_RESYNC).await;
                    let _ = writer.flush().await;
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
                Ok(stream) => match self.sync(stream).await {
                    Ok(()) => {
                        info!("replication connection ended cleanly");
                    }
                    Err(e) => {
                        warn!("replication error: {e}");
                    }
                },
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
        let mut local_offset: u64 = 0;
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

                    // acknowledge this record to the primary so WAIT can count us
                    local_offset += 1;
                    write_u8(&mut stream, MSG_ACK).await?;
                    write_u64_le(&mut stream, local_offset).await?;
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
        AofRecord::LSet { key, index, value } => Some(ShardRequest::LSet {
            key: key.clone(),
            index: *index,
            value: value.clone(),
        }),
        AofRecord::LTrim { key, start, stop } => Some(ShardRequest::LTrim {
            key: key.clone(),
            start: *start,
            stop: *stop,
        }),
        AofRecord::LInsert {
            key,
            before,
            pivot,
            value,
        } => Some(ShardRequest::LInsert {
            key: key.clone(),
            before: *before,
            pivot: pivot.clone(),
            value: value.clone(),
        }),
        AofRecord::LRem { key, count, value } => Some(ShardRequest::LRem {
            key: key.clone(),
            count: *count,
            value: value.clone(),
        }),
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
        AofRecord::SetRange { key, offset, value } => Some(ShardRequest::SetRange {
            key: key.clone(),
            offset: *offset,
            value: value.clone(),
        }),
        AofRecord::SetBit { key, offset, value } => Some(ShardRequest::SetBit {
            key: key.clone(),
            offset: *offset,
            value: *value,
        }),
        AofRecord::BitOp { op, dest, keys } => {
            use ember_protocol::command::BitOpKind;
            let op_kind = match op {
                0 => BitOpKind::And,
                1 => BitOpKind::Or,
                2 => BitOpKind::Xor,
                _ => BitOpKind::Not,
            };
            Some(ShardRequest::BitOp {
                op: op_kind,
                dest: dest.clone(),
                keys: keys.clone(),
            })
        }
        AofRecord::Rename { key, newkey } => Some(ShardRequest::Rename {
            key: key.clone(),
            newkey: newkey.clone(),
        }),
        AofRecord::Copy {
            source,
            destination,
            replace,
        } => Some(ShardRequest::Copy {
            source: source.clone(),
            destination: destination.clone(),
            replace: *replace,
        }),
        #[cfg(feature = "vector")]
        AofRecord::VAdd {
            key,
            element,
            vector,
            metric,
            quantization,
            connectivity,
            expansion_add,
        } => Some(ShardRequest::VAdd {
            key: key.clone(),
            element: element.clone(),
            vector: vector.clone(),
            metric: *metric,
            quantization: *quantization,
            connectivity: *connectivity,
            expansion_add: *expansion_add,
        }),
        #[cfg(feature = "vector")]
        AofRecord::VRem { key, element } => Some(ShardRequest::VRem {
            key: key.clone(),
            element: element.clone(),
        }),
        #[cfg(feature = "protobuf")]
        AofRecord::ProtoSet {
            key,
            type_name,
            data,
            expire_ms,
        } => Some(ShardRequest::ProtoSet {
            key: key.clone(),
            type_name: type_name.clone(),
            data: data.clone(),
            expire: expire_from_ms(*expire_ms),
            nx: false,
            xx: false,
        }),
        #[cfg(feature = "protobuf")]
        AofRecord::ProtoRegister { name, descriptor } => Some(ShardRequest::ProtoRegisterAof {
            name: name.clone(),
            descriptor: descriptor.clone(),
        }),
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
        | ShardRequest::SetRange { key, .. }
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
        | ShardRequest::Rename { key, .. }
        | ShardRequest::SetBit { key, .. }
        | ShardRequest::GetBit { key, .. }
        | ShardRequest::BitCount { key, .. }
        | ShardRequest::BitPos { key, .. } => Some(key),
        ShardRequest::BitOp { dest, .. } => Some(dest),
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
            ShardRequest::Set {
                key,
                value,
                expire,
                nx,
                xx,
            } => {
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
