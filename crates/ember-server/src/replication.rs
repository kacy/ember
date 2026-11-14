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
//! // Authentication (if status = 0):
//! // primary → replica: [nonce: 16B]
//! // replica → primary: [tag: 32B]   HMAC of the nonce with the cluster
//! //                                 secret, or zeros without one
//! // primary → replica: [status: 1B] (0 = ok, 2 = authentication failed)
//!
//! // For each shard (if status = 0):
//! [MSG_SHARD_SYNC: 1B][shard_id: 2B][snapshot_len: 4B][snapshot_bytes]
//! [MSG_SHARD_OFFSET: 1B][shard_id: 2B][offset: 8B]
//!
//! // Incremental records (unbounded stream):
//! [MSG_RECORD: 1B][shard_id: 2B][offset: 8B][record_len: 4B][record_bytes]
//!
//! // Replica → primary, after loading the snapshots and after applying
//! // records. offset is the newest one applied on that shard:
//! [MSG_ACK: 1B][shard_id: 2B][offset: 8B]
//!
//! // When the replica falls too far behind (see REPLICA_QUEUE_LIMIT):
//! [MSG_RESYNC: 1B]    primary closes the connection; replica reconnects
//! ```
//!
//! Offsets count replication records per shard. A snapshot's offset is the
//! last record it contains, so the replica skips records at or below it.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use ember_cluster::ClusterSecret;
use ember_core::{Engine, ShardRequest, ShardResponse};
use ember_persistence::aof::AofRecord;
use ember_persistence::snapshot;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

// -- protocol constants --

/// Version 2 sends real snapshot offsets and acknowledges `(shard, offset)`.
const REPL_VERSION: u8 = 2;
const STATUS_OK: u8 = 0;
const STATUS_SHARD_MISMATCH: u8 = 1;
const STATUS_AUTH_FAILED: u8 = 2;
const NONCE_LEN: usize = 16;

const MSG_SHARD_SYNC: u8 = 2;
const MSG_SHARD_OFFSET: u8 = 3;
const MSG_RECORD: u8 = 4;
const MSG_RESYNC: u8 = 5;
const MSG_ACK: u8 = 6;

/// The most record bytes the primary queues for one replica. Writes made
/// while the replica loads its full sync wait in this queue. A replica
/// that falls further behind is sent MSG_RESYNC.
const REPLICA_QUEUE_LIMIT: usize = 256 * 1024 * 1024;

/// The most requests a replica collects before sending them to its shards.
const MAX_BATCH: usize = 4096;

/// Tracks each replica's acknowledged offset per shard for WAIT.
///
/// A replica starts at offset 0 on every shard. It acknowledges the
/// snapshots' offsets once it has loaded them, then each record it
/// applies. WAIT compares these against the
/// shards' current offsets from [`Engine::replication_offsets`].
#[derive(Debug)]
pub struct ReplicaTracker {
    /// Acknowledged offset of each shard, per replica. Keyed by an id
    /// assigned when the replica connects.
    acked: Mutex<HashMap<u64, Vec<u64>>>,
    next_id: AtomicU64,
    /// Full syncs started since the server started.
    full_syncs: AtomicU64,
}

impl ReplicaTracker {
    pub fn new() -> Self {
        Self {
            acked: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
            full_syncs: AtomicU64::new(0),
        }
    }

    /// Registers a replica of a primary with `shards` shards. Returns its
    /// id.
    pub fn register(&self, shards: usize) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut map) = self.acked.lock() {
            map.insert(id, vec![0; shards]);
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

    /// Counts a full sync sent to a replica.
    pub fn full_sync_started(&self) {
        self.full_syncs.fetch_add(1, Ordering::Relaxed);
    }

    /// Full syncs started since the server started.
    pub fn full_syncs(&self) -> u64 {
        self.full_syncs.load(Ordering::Relaxed)
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
    /// Replicas must prove they hold this secret before receiving data.
    secret: Option<Arc<ClusterSecret>>,
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
        secret: Option<Arc<ClusterSecret>>,
    ) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!(%addr, "replication server listening");

        let server = Arc::new(Self {
            engine,
            primary_id,
            tracker,
            secret,
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

        // the replica proves it holds the cluster secret before it gets
        // any data. a fresh random nonce means a captured answer cannot be
        // replayed.
        let nonce: [u8; NONCE_LEN] = rand::random();
        stream.write_all(&nonce).await?;
        stream.flush().await?;
        let mut tag = [0u8; ember_cluster::TAG_LEN];
        stream.read_exact(&mut tag).await?;
        let authenticated = self
            .secret
            .as_ref()
            .is_none_or(|secret| secret.verify(&nonce, &tag));
        if !authenticated {
            write_u8(&mut stream, STATUS_AUTH_FAILED).await?;
            stream.flush().await?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "replica failed cluster authentication",
            ));
        }
        write_u8(&mut stream, STATUS_OK).await?;

        // subscribe before snapshotting so we don't miss events that
        // happen between snapshot and incremental stream start
        let Some(rx) = self.engine.subscribe_replication() else {
            return Err(std::io::Error::other(
                "replication channel not configured on this engine",
            ));
        };
        // move events into this replica's own queue as they happen. the
        // broadcast ring holds a fixed number of events, too few to cover a
        // full sync under write load
        let (queue_tx, mut queue) = mpsc::unbounded_channel();
        let queued = Arc::new(AtomicUsize::new(0));
        let forwarder = tokio::spawn(forward_events(rx, queue_tx, Arc::clone(&queued)));

        // --- full sync ---
        self.tracker.full_sync_started();
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
        }

        stream.flush().await?;
        info!("full sync complete, starting incremental stream");

        // --- split stream for concurrent read (ACKs) and write (records) ---
        // Take the inner TcpStream back from BufWriter, then split.
        let inner = stream.into_inner();
        let (read_half, write_inner) = tokio::io::split(inner);
        let mut writer = BufWriter::with_capacity(65536, write_inner);

        // Register this replica and spawn an ACK reader task.
        // WAIT counts the replica once it acknowledges having loaded these
        let replica_id = self.tracker.register(self.engine.shard_count());
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
        let result = stream_records(&mut writer, &mut queue, &queued).await;

        // clean up regardless of result
        forwarder.abort();
        ack_task.abort();
        self.tracker.remove(replica_id);

        result
    }
}

/// Encodes the records of each replication event as MSG_RECORD frames and
/// queues them for one replica. Stops, closing the queue, when the replica
/// falls more than [`REPLICA_QUEUE_LIMIT`] bytes behind or the broadcast
/// ring overruns.
async fn forward_events(
    mut rx: broadcast::Receiver<ember_core::ReplicationEvent>,
    queue: mpsc::UnboundedSender<Bytes>,
    queued: Arc<AtomicUsize>,
) {
    loop {
        let event = match rx.recv().await {
            Ok(event) => event,
            Err(broadcast::error::RecvError::Lagged(count)) => {
                warn!("replication stream lagged by {count} events; triggering resync");
                return;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };
        let mut frames = BytesMut::new();
        for (offset, record) in (event.offset..).zip(event.records.iter()) {
            let bytes = match record.to_bytes() {
                Ok(bytes) => bytes,
                Err(e) => {
                    error!("replication record serialization failed: {e}");
                    return;
                }
            };
            let Ok(len) = u32::try_from(bytes.len()) else {
                error!("replication record too large: {} bytes", bytes.len());
                return;
            };
            frames.put_u8(MSG_RECORD);
            frames.put_u16_le(event.shard_id);
            frames.put_u64_le(offset);
            frames.put_u32_le(len);
            frames.put_slice(&bytes);
        }

        let size = frames.len();
        if queued.fetch_add(size, Ordering::Relaxed) + size > REPLICA_QUEUE_LIMIT {
            warn!("replica is over {REPLICA_QUEUE_LIMIT} bytes behind; triggering resync");
            return;
        }
        if queue.send(frames.freeze()).is_err() {
            return;
        }
    }
}

/// Writes queued records to the replica, flushing whenever the queue runs
/// dry. Once the forwarder closes the queue, the rest of it is stale: it
/// sends MSG_RESYNC instead, so the replica starts over with a full sync.
async fn stream_records(
    writer: &mut BufWriter<tokio::io::WriteHalf<TcpStream>>,
    queue: &mut mpsc::UnboundedReceiver<Bytes>,
    queued: &AtomicUsize,
) -> std::io::Result<()> {
    while let Some(frame) = queue.recv().await {
        if queue.is_closed() {
            break;
        }
        queued.fetch_sub(frame.len(), Ordering::Relaxed);
        writer.write_all(&frame).await?;
        if queue.is_empty() {
            writer.flush().await?;
        }
    }
    write_u8(writer, MSG_RESYNC).await?;
    writer.flush().await
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
    /// Proves this replica may pull the primary's data.
    secret: Option<Arc<ClusterSecret>>,
}

impl ReplicationClient {
    /// Starts the replication client in a background task.
    ///
    /// Connects to `primary_addr` and applies the stream indefinitely,
    /// reconnecting with backoff on any error. The caller must abort the
    /// returned task when this node stops replicating that primary, or it
    /// keeps pulling the old primary's data in.
    pub fn start(
        engine: Arc<Engine>,
        primary_addr: SocketAddr,
        secret: Option<Arc<ClusterSecret>>,
    ) -> tokio::task::JoinHandle<()> {
        let client = Self {
            engine,
            primary_addr,
            secret,
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
    async fn sync(&self, stream: TcpStream, backoff: &mut Duration) -> std::io::Result<()> {
        // reads come through a buffer; writes go straight to the socket
        let mut stream = BufReader::with_capacity(65536, stream);
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

        // answer the primary's challenge with the cluster secret
        let mut nonce = [0u8; NONCE_LEN];
        stream.read_exact(&mut nonce).await?;
        let tag = self
            .secret
            .as_ref()
            .map(|secret| secret.sign(&nonce))
            .unwrap_or([0u8; ember_cluster::TAG_LEN]);
        stream.write_all(&tag).await?;
        if read_u8(&mut stream).await? == STATUS_AUTH_FAILED {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("primary {primary_id} rejected our cluster credentials"),
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
        let mut batch = Batch::new(usize::from(primary_shards));
        let mut snapshot_offsets = vec![0u64; usize::from(primary_shards)];
        let mut snap_buf = Vec::new();
        for _ in 0..primary_shards {
            expect_tag(&mut stream, MSG_SHARD_SYNC, "MSG_SHARD_SYNC").await?;
            let shard_id = read_u16_le(&mut stream).await?;
            let snap_len = read_u32_le(&mut stream).await? as usize;

            snap_buf.clear();
            snap_buf.resize(snap_len, 0);
            stream.read_exact(&mut snap_buf).await?;
            self.apply_snapshot(&mut batch, shard_id, &snap_buf).await?;

            expect_tag(&mut stream, MSG_SHARD_OFFSET, "MSG_SHARD_OFFSET").await?;
            let offset_shard = read_u16_le(&mut stream).await?;
            let offset = read_u64_le(&mut stream).await?;
            if let Some(slot) = snapshot_offsets.get_mut(usize::from(offset_shard)) {
                *slot = offset;
            }
        }
        batch.apply(&self.engine).await;
        let mut acks = Vec::new();
        for (shard, offset) in snapshot_offsets.iter().enumerate() {
            acks.push(MSG_ACK);
            acks.extend_from_slice(&(shard as u16).to_le_bytes());
            acks.extend_from_slice(&offset.to_le_bytes());
        }
        stream.write_all(&acks).await?;

        info!("full sync applied, starting incremental replay");

        // incremental stream. records are collected into a batch, which
        // is applied and acknowledged once the buffered input is used up
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
                    let request = (!in_snapshot)
                        .then(|| ember_core::from_aof_record(&record))
                        .flatten();
                    batch.push(shard_id, request, Some(offset));
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

            // acknowledge once the records are applied, so WAIT can count
            // this replica
            if stream.buffer().is_empty() || batch.is_full() {
                let mut acks = Vec::new();
                for (shard, offset) in batch.apply(&self.engine).await {
                    acks.push(MSG_ACK);
                    acks.extend_from_slice(&shard.to_le_bytes());
                    acks.extend_from_slice(&offset.to_le_bytes());
                }
                stream.write_all(&acks).await?;
            }
        }
    }

    /// Loads a snapshot blob into the given shard.
    ///
    /// Each entry goes in through RESTORE, which replaces the key with the
    /// snapshot's value as is, for every value type.
    async fn apply_snapshot(
        &self,
        batch: &mut Batch,
        shard_id: u16,
        data: &[u8],
    ) -> std::io::Result<()> {
        let (_, entries) = snapshot::read_snapshot_from_bytes(data).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("snapshot parse error: {e}"),
            )
        })?;

        for entry in entries {
            // the time the key expires at, or 0 for no expiry. RESTORE
            // takes the time left, with 0 for none
            let ttl_ms = match entry.expires_at_ms {
                0 => 0,
                at => match ember_persistence::aof::ms_until(at) {
                    Some(ms) => ms,
                    None => continue, // expired while the snapshot was in flight
                },
            };
            let data = snapshot::serialize_snap_value(&entry.value)
                .map_err(|e| std::io::Error::other(format!("snapshot value encode failed: {e}")))?;
            let request = ShardRequest::RestoreKey {
                key: entry.key,
                ttl_ms,
                data: Bytes::from(data),
                replace: true,
            };
            batch.push(shard_id, Some(request), None);
            if batch.is_full() {
                batch.apply(&self.engine).await;
            }
        }
        Ok(())
    }
}

/// Requests a replica has collected for its shards, and the offsets to
/// acknowledge once they are applied.
///
/// Each shard gets its requests as one message, and the shards apply them
/// in parallel.
struct Batch {
    requests: Vec<Vec<ShardRequest>>,
    /// The newest offset collected for each shard.
    acks: Vec<Option<u64>>,
    len: usize,
}

impl Batch {
    fn new(shards: usize) -> Self {
        Self {
            requests: (0..shards).map(|_| Vec::new()).collect(),
            acks: vec![None; shards],
            len: 0,
        }
    }

    /// Adds `request` for `shard`, if there is one, and `offset` to
    /// acknowledge once it is applied.
    fn push(&mut self, shard: u16, request: Option<ShardRequest>, offset: Option<u64>) {
        let shard = usize::from(shard);
        if let (Some(request), Some(requests)) = (request, self.requests.get_mut(shard)) {
            requests.push(request);
            self.len += 1;
        }
        if let Some(ack) = self.acks.get_mut(shard) {
            *ack = offset.or(*ack);
        }
    }

    fn is_full(&self) -> bool {
        self.len >= MAX_BATCH
    }

    /// Sends the requests to their shards and waits until all are applied.
    /// Returns the `(shard, offset)` pairs this covers.
    async fn apply(&mut self, engine: &Engine) -> Vec<(u16, u64)> {
        let mut replies = Vec::with_capacity(self.len);
        for (shard, requests) in self.requests.iter_mut().enumerate() {
            if requests.is_empty() {
                continue;
            }
            match engine
                .dispatch_batch_to_shard(shard, std::mem::take(requests))
                .await
            {
                Ok(batch) => replies.extend(batch),
                Err(e) => warn!("replication apply failed: {e:?}"),
            }
        }
        self.len = 0;
        for reply in replies {
            if reply.await.is_err() {
                warn!("replication apply failed: shard dropped the request");
            }
        }
        self.acks
            .iter_mut()
            .enumerate()
            .filter_map(|(shard, ack)| Some((shard as u16, ack.take()?)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_counts_replicas_that_reached_every_shard() {
        let tracker = ReplicaTracker::new();
        let a = tracker.register(2);
        let b = tracker.register(2);
        // both loaded snapshots at offsets [3, 0]
        tracker.update(a, 0, 3);
        tracker.update(b, 0, 3);
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
}
