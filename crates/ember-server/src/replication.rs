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
//! // Replica → primary, after applying each record:
//! [MSG_ACK: 1B][shard_id: 2B][offset: 8B]
//!
//! // When replica falls behind (broadcast lag):
//! [MSG_RESYNC: 1B]    primary closes the connection; replica reconnects
//! ```
//!
//! Offsets count replication events per shard. A snapshot's offset is the
//! last event it contains, so the replica skips records at or below it.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use ember_core::{Engine, ShardRequest, ShardResponse};
use ember_persistence::aof::AofRecord;
use ember_persistence::snapshot;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

// -- protocol constants --

/// Version 2 sends real snapshot offsets and acknowledges `(shard, offset)`.
const REPL_VERSION: u8 = 2;
const STATUS_OK: u8 = 0;
const STATUS_SHARD_MISMATCH: u8 = 1;

const MSG_SHARD_SYNC: u8 = 2;
const MSG_SHARD_OFFSET: u8 = 3;
const MSG_RECORD: u8 = 4;
const MSG_RESYNC: u8 = 5;
const MSG_ACK: u8 = 6;

/// Tracks each replica's acknowledged offset per shard for WAIT.
///
/// A replica starts at the offsets of the snapshot it received and moves
/// forward as it acknowledges records. WAIT compares these against the
/// shards' current offsets from [`Engine::replication_offsets`].
#[derive(Debug)]
pub struct ReplicaTracker {
    /// Acknowledged offset of each shard, per replica. Keyed by an id
    /// assigned when the replica connects.
    acked: Mutex<HashMap<u64, Vec<u64>>>,
    next_id: AtomicU64,
}

impl ReplicaTracker {
    pub fn new() -> Self {
        Self {
            acked: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
        }
    }

    /// Registers a replica that loaded snapshots at `snapshot_offsets`.
    /// Returns its id.
    pub fn register(&self, snapshot_offsets: Vec<u64>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut map) = self.acked.lock() {
            map.insert(id, snapshot_offsets);
        }
        id
    }

    /// Removes a replica connection from tracking.
    pub fn remove(&self, replica_id: u64) {
        if let Ok(mut map) = self.acked.lock() {
            map.remove(&replica_id);
        }
    }

    /// Records that a replica applied `shard`'s events up to `offset`.
    /// Offsets only move forward.
    pub fn update(&self, replica_id: u64, shard: usize, offset: u64) {
        if let Ok(mut map) = self.acked.lock() {
            if let Some(acked) = map.get_mut(&replica_id).and_then(|a| a.get_mut(shard)) {
                *acked = (*acked).max(offset);
            }
        }
    }

    /// Returns how many replicas have reached `target` on every shard.
    pub fn count_caught_up(&self, target: &[u64]) -> usize {
        self.acked
            .lock()
            .map(|map| {
                map.values()
                    .filter(|acked| acked.iter().zip(target).all(|(a, t)| a >= t))
                    .count()
            })
            .unwrap_or(0)
    }

    /// Returns the total number of currently connected replicas.
    pub fn connected_count(&self) -> usize {
        self.acked.lock().map(|map| map.len()).unwrap_or(0)
    }

    /// Returns how many events each replica is behind `current`, summed
    /// over all shards. A lag of 0 means fully caught up.
    pub fn replica_lags(&self, current: &[u64]) -> Vec<u64> {
        self.acked
            .lock()
            .map(|map| {
                map.values()
                    .map(|acked| {
                        acked
                            .iter()
                            .zip(current)
                            .map(|(a, c)| c.saturating_sub(*a))
                            .sum()
                    })
                    .collect()
            })
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
    /// Binds the TCP listener on `addr` and starts accepting replica
    /// connections. `addr` uses the server's configured bind IP, so a
    /// server bound to loopback does not expose its dataset on every
    /// interface.
    ///
    /// Runs indefinitely in the background; returns immediately after
    /// spawning the accept loop task.
    pub async fn start(
        engine: Arc<Engine>,
        primary_id: String,
        addr: SocketAddr,
        tracker: Arc<ReplicaTracker>,
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!(%addr, "replication server listening");

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
        let mut snapshot_offsets = Vec::with_capacity(self.engine.shard_count());
        for shard_idx in 0..self.engine.shard_count() {
            let resp = self
                .engine
                .send_to_shard(shard_idx, ShardRequest::SerializeSnapshot)
                .await
                .map_err(|e| {
                    std::io::Error::other(format!("shard {shard_idx} serialize failed: {e:?}"))
                })?;

            let (shard_id, offset, data) = match resp {
                ShardResponse::SnapshotData {
                    shard_id,
                    offset,
                    data,
                } => (shard_id, offset, data),
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
            write_u64_le(&mut stream, offset).await?;
            snapshot_offsets.push(offset);
        }

        stream.flush().await?;
        info!("full sync complete, starting incremental stream");

        // --- split stream for concurrent read (ACKs) and write (records) ---
        // Take the inner TcpStream back from BufWriter, then split.
        let inner = stream.into_inner();
        let (read_half, write_inner) = tokio::io::split(inner);
        let mut writer = BufWriter::with_capacity(65536, write_inner);

        // Register this replica and spawn an ACK reader task.
        let replica_id = self.tracker.register(snapshot_offsets);
        let tracker = Arc::clone(&self.tracker);
        let mut ack_reader = read_half;

        let ack_task = tokio::spawn(async move {
            loop {
                match read_u8(&mut ack_reader).await {
                    Ok(MSG_ACK) => {
                        let ack = async {
                            let shard = read_u16_le(&mut ack_reader).await?;
                            let offset = read_u64_le(&mut ack_reader).await?;
                            std::io::Result::Ok((shard, offset))
                        };
                        match ack.await {
                            Ok((shard, offset)) => tracker.update(replica_id, shard.into(), offset),
                            Err(_) => break,
                        }
                    }
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

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(30);

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
    /// reconnecting with backoff on any error. The caller must abort the
    /// returned task when this node stops replicating that primary, or it
    /// keeps pulling the old primary's data in.
    pub fn start(engine: Arc<Engine>, primary_addr: SocketAddr) -> tokio::task::JoinHandle<()> {
        let client = Self {
            engine,
            primary_addr,
        };
        tokio::spawn(async move {
            client.run().await;
        })
    }

    async fn run(&self) {
        let mut backoff = INITIAL_BACKOFF;

        loop {
            info!(primary = %self.primary_addr, "connecting to primary for replication");
            match TcpStream::connect(self.primary_addr).await {
                Ok(stream) => match self.sync(stream, &mut backoff).await {
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
    /// Resets `backoff` once the handshake succeeds, so a replica that ran
    /// fine for hours does not wait the maximum delay after one drop.
    async fn sync(&self, mut stream: TcpStream, backoff: &mut Duration) -> std::io::Result<()> {
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
        *backoff = INITIAL_BACKOFF;

        // a full sync replaces everything: keys the primary no longer has
        // must not survive it
        self.engine
            .broadcast(|| ShardRequest::FlushDb)
            .await
            .map_err(|e| std::io::Error::other(format!("flush before sync failed: {e:?}")))?;

        // receive per-shard snapshots and the offset each one covers
        let mut snapshot_offsets = vec![0u64; usize::from(primary_shards)];
        let mut snap_buf = Vec::new();
        for _ in 0..primary_shards {
            expect_tag(&mut stream, MSG_SHARD_SYNC, "MSG_SHARD_SYNC").await?;
            let shard_id = read_u16_le(&mut stream).await?;
            let snap_len = read_u32_le(&mut stream).await? as usize;

            snap_buf.clear();
            snap_buf.resize(snap_len, 0);
            stream.read_exact(&mut snap_buf).await?;
            self.apply_snapshot(shard_id, &snap_buf).await?;

            expect_tag(&mut stream, MSG_SHARD_OFFSET, "MSG_SHARD_OFFSET").await?;
            let offset_shard = read_u16_le(&mut stream).await?;
            let offset = read_u64_le(&mut stream).await?;
            if let Some(slot) = snapshot_offsets.get_mut(usize::from(offset_shard)) {
                *slot = offset;
            }
        }

        info!("full sync applied, starting incremental replay");

        // incremental stream
        loop {
            let msg = read_u8(&mut stream).await?;
            match msg {
                MSG_RECORD => {
                    let shard_id = read_u16_le(&mut stream).await?;
                    let offset = read_u64_le(&mut stream).await?;
                    let record_len = read_u32_le(&mut stream).await? as usize;
                    let mut record_bytes = vec![0u8; record_len];
                    stream.read_exact(&mut record_bytes).await?;

                    let record = AofRecord::from_bytes(&record_bytes).map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid AOF record: {e}"),
                        )
                    })?;

                    // the primary subscribed before taking the snapshots, so
                    // the stream repeats events a snapshot already holds.
                    // applying them again would double INCRs and pushes.
                    let in_snapshot = snapshot_offsets
                        .get(usize::from(shard_id))
                        .is_some_and(|&snapshot| offset <= snapshot);

                    // the handshake checked that both sides have the same shard
                    // count, so the primary's shard id picks the same shard a
                    // key lookup would, and it also covers keyless records
                    if !in_snapshot {
                        if let Some(request) = aof_record_to_shard_request(&record) {
                            if let Err(e) =
                                self.engine.send_to_shard(shard_id.into(), request).await
                            {
                                warn!("replication apply failed: {e:?}");
                            }
                        }
                    }

                    // acknowledge the record so WAIT can count this replica
                    write_u8(&mut stream, MSG_ACK).await?;
                    write_u16_le(&mut stream, shard_id).await?;
                    write_u64_le(&mut stream, offset).await?;
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

    /// Loads a snapshot blob into the given shard.
    ///
    /// Each entry goes in through RESTORE, which replaces the key with the
    /// snapshot's value as is, for every value type.
    async fn apply_snapshot(&self, shard_id: u16, data: &[u8]) -> std::io::Result<()> {
        let (_, entries) = snapshot::read_snapshot_from_bytes(data).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("snapshot parse error: {e}"),
            )
        })?;

        for entry in entries {
            // expire_ms is the time left, or negative for no expiry
            let ttl_ms = match entry.expire_ms {
                ms if ms < 0 => 0,
                0 => continue, // expired while the snapshot was in flight
                ms => ms as u64,
            };
            let data = snapshot::serialize_snap_value(&entry.value)
                .map_err(|e| std::io::Error::other(format!("snapshot value encode failed: {e}")))?;
            let request = ShardRequest::RestoreKey {
                key: entry.key,
                ttl_ms,
                data: Bytes::from(data),
                replace: true,
            };
            if let Err(e) = self.engine.send_to_shard(shard_id.into(), request).await {
                warn!("snapshot restore failed: {e:?}");
            }
        }
        Ok(())
    }
}

/// Converts a millisecond expiry field to an `Option<Duration>`.
///
/// AOF records store `-1` (or any non-positive value) to indicate "no
/// expiry". A positive `ms` becomes `Some(Duration)`.
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
        AofRecord::Pexpireat { key, timestamp_ms } => Some(ShardRequest::Pexpireat {
            key: key.clone(),
            timestamp_ms: *timestamp_ms,
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
        AofRecord::FlushAll => Some(ShardRequest::FlushDb),
        AofRecord::Restore { key, ttl_ms, data } => Some(ShardRequest::RestoreKey {
            key: key.clone(),
            ttl_ms: *ttl_ms,
            data: data.clone(),
            replace: true,
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn tracker_counts_replicas_that_reached_every_shard() {
        let tracker = ReplicaTracker::new();
        let a = tracker.register(vec![3, 0]);
        let b = tracker.register(vec![3, 0]);
        tracker.update(a, 1, 2);
        // a replica that is ahead on one shard but behind on another does not count
        tracker.update(b, 0, 9);

        assert_eq!(tracker.count_caught_up(&[3, 2]), 1);
        let mut lags = tracker.replica_lags(&[3, 2]);
        lags.sort();
        assert_eq!(lags, [0, 2]);
        tracker.update(a, 1, 1); // offsets never move backward
        assert_eq!(tracker.count_caught_up(&[3, 2]), 1);
        tracker.remove(a);
        assert_eq!(tracker.count_caught_up(&[3, 2]), 0);
    }

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
}
