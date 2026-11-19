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
//! [version: 1B][num_shards: 2B][replica_nonce: 16B]
//!
//! // Primary → replica (handshake response):
//! [version: 1B][num_shards: 2B][primary_id_len: 1B][primary_id: N bytes]
//! [status: 1B]   (0 = ok, 1 = shard count mismatch)
//!
//! // Authentication (if status = 0). a tag is the HMAC of the role's
//! // label and the other side's nonce with the cluster secret, or zeros
//! // without one:
//! // primary → replica: [primary_nonce: 16B][primary tag: 32B]
//! // replica → primary: [replica tag: 32B]  (sent once the primary's tag checks out)
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
//!
//! // Primary → replica after HEARTBEAT_INTERVAL with nothing to send, and
//! // the replica's answer:
//! [MSG_PING: 1B]
//! ```
//!
//! Either side drops a connection on which a read or write waits longer
//! than REPL_TIMEOUT, and the primary drops a replica that hasn't finished
//! the handshake within HANDSHAKE_TIMEOUT.
//!
//! Offsets count replication records per shard. A snapshot's offset is the
//! last record it contains, so the replica skips records at or below it.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use ember_cluster::ClusterSecret;
use ember_core::{Engine, ShardRequest, ShardResponse};
use ember_persistence::aof::AofRecord;
use ember_persistence::snapshot;
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, ReadBuf,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{Instant, Sleep};
use tracing::{debug, error, info, warn};

// -- protocol constants --

/// Version 3 authenticates both sides and adds heartbeats. Version 4 sends
/// the protobuf schemas at the start of a full sync.
const REPL_VERSION: u8 = 4;
const STATUS_OK: u8 = 0;
const STATUS_SHARD_MISMATCH: u8 = 1;
const STATUS_AUTH_FAILED: u8 = 2;
const NONCE_LEN: usize = 16;

const MSG_SHARD_SYNC: u8 = 2;
const MSG_SHARD_OFFSET: u8 = 3;
const MSG_RECORD: u8 = 4;
const MSG_RESYNC: u8 = 5;
const MSG_ACK: u8 = 6;
const MSG_PING: u8 = 7;
/// A protobuf schema, sent before the snapshots of a full sync:
/// `[name_len: u32][name][descriptor_len: u32][descriptor]`.
const MSG_SCHEMA: u8 = 8;

/// Each side signs the other's nonce after a label for its own role, so
/// the answer one side gives can't be passed off as the other's.
const PRIMARY_ROLE: &[u8] = b"ember replication primary";
const REPLICA_ROLE: &[u8] = b"ember replication replica";

/// How long the primary gives a connecting replica to finish the
/// handshake. Until then the peer is unauthenticated. Unit tests use
/// shorter times here and below.
const HANDSHAKE_TIMEOUT: Duration = test_or(Duration::from_secs(10), Duration::from_millis(200));

/// The most handshakes the primary runs at once. Connections beyond it
/// are closed.
const MAX_HANDSHAKES: usize = 16;

/// How long either side waits on a read or write that makes no progress
/// before it drops the connection, as Redis's `repl-timeout`.
const REPL_TIMEOUT: Duration = test_or(Duration::from_secs(60), Duration::from_millis(600));

/// How long the primary goes without sending before it sends MSG_PING.
/// The replica answers with MSG_PING, so both directions carry data well
/// within [`REPL_TIMEOUT`].
const HEARTBEAT_INTERVAL: Duration = test_or(Duration::from_secs(10), Duration::from_millis(100));

const fn test_or(normal: Duration, in_tests: Duration) -> Duration {
    if cfg!(test) {
        in_tests
    } else {
        normal
    }
}

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
    /// Acknowledged offset of each shard, per replica, or `None` before the
    /// first acknowledgement. Keyed by an id assigned when the replica
    /// connects.
    acked: Mutex<HashMap<u64, Vec<Option<u64>>>>,
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
            map.insert(id, vec![None; shards]);
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
                *acked = Some(acked.map_or(offset, |a| a.max(offset)));
            }
        }
    }

    /// Returns how many replicas have reached `target` on every shard.
    pub fn count_caught_up(&self, target: &[u64]) -> usize {
        self.acked
            .lock()
            .map(|map| {
                map.values()
                    .filter(|acked| {
                        acked
                            .iter()
                            .zip(target)
                            .all(|(a, t)| a.is_some_and(|a| a >= *t))
                    })
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
                            .map(|(a, c)| c.saturating_sub(a.unwrap_or(0)))
                            .sum()
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// The tag that proves the holder of `secret` speaks as `role`, answering
/// the other side's `nonce`. Without a secret it is all zeros.
fn auth_tag(
    secret: Option<&ClusterSecret>,
    role: &[u8],
    nonce: &[u8; NONCE_LEN],
) -> [u8; ember_cluster::TAG_LEN] {
    secret.map_or([0; ember_cluster::TAG_LEN], |secret| {
        secret.sign(&[role, nonce].concat())
    })
}

/// Whether `tag` is [`auth_tag`] for `role` and `nonce`. Without a secret
/// any tag is accepted.
fn auth_ok(
    secret: Option<&ClusterSecret>,
    role: &[u8],
    nonce: &[u8; NONCE_LEN],
    tag: &[u8],
) -> bool {
    secret.is_none_or(|secret| secret.verify(&[role, nonce].concat(), tag))
}

/// Runs `fut`, failing with `TimedOut` if it takes longer than `limit`.
async fn within<T>(
    limit: Duration,
    what: &str,
    fut: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    tokio::time::timeout(limit, fut).await.unwrap_or_else(|_| {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("{what} timed out"),
        ))
    })
}

/// A connection whose reads and writes fail with `TimedOut` after waiting
/// [`REPL_TIMEOUT`] without progress.
///
/// Replication connections stay open indefinitely, so a peer that stops
/// responding must not hold one forever. Heartbeats keep a quiet but
/// healthy connection busy.
struct IdleTimeout<S> {
    inner: S,
    read: Deadline,
    write: Deadline,
}

type Conn = IdleTimeout<TcpStream>;

impl<S> IdleTimeout<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            read: Deadline::new(),
            write: Deadline::new(),
        }
    }
}

/// The point at which one direction of an [`IdleTimeout`] gives up.
struct Deadline {
    sleep: Pin<Box<Sleep>>,
    /// Set while an operation is waiting, from its first `Pending`.
    armed: bool,
}

impl Deadline {
    fn new() -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            armed: false,
        }
    }

    /// Passes on the inner operation's `poll`, or fails it once it has
    /// been pending for [`REPL_TIMEOUT`].
    fn check<T>(&mut self, cx: &mut Context<'_>, poll: Poll<io::Result<T>>) -> Poll<io::Result<T>> {
        if poll.is_ready() {
            self.armed = false;
            return poll;
        }
        if !self.armed {
            self.sleep.as_mut().reset(Instant::now() + REPL_TIMEOUT);
            self.armed = true;
        }
        match self.sleep.as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.armed = false;
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "replication peer stopped responding",
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for IdleTimeout<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        this.read.check(cx, poll)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for IdleTimeout<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.write.check(cx, poll)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_flush(cx);
        this.write.check(cx, poll)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
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

/// Reads a `len`-byte body into `buf`. The buffer grows as the bytes
/// arrive, so a corrupt length can't make the reader allocate memory the
/// sender never fills.
async fn read_body(
    r: &mut (impl AsyncRead + Unpin),
    len: u32,
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    buf.clear();
    let read = r.take(u64::from(len)).read_to_end(buf).await?;
    if read != len as usize {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(())
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
    /// Runs indefinitely in the background; returns the bound address
    /// immediately after spawning the accept loop task.
    pub async fn start(
        engine: Arc<Engine>,
        primary_id: String,
        addr: SocketAddr,
        tracker: Arc<ReplicaTracker>,
        secret: Option<Arc<ClusterSecret>>,
    ) -> std::io::Result<SocketAddr> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        info!(%addr, "replication server listening");

        let server = Arc::new(Self {
            engine,
            primary_id,
            tracker,
            secret,
        });

        let handshakes = Arc::new(Semaphore::new(MAX_HANDSHAKES));
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let Ok(permit) = Arc::clone(&handshakes).try_acquire_owned() else {
                            warn!(%peer, "too many replication handshakes; closing connection");
                            continue;
                        };
                        debug!(%peer, "replica connected");
                        let server = Arc::clone(&server);
                        tokio::spawn(async move {
                            if let Err(e) = server.handle_replica(stream, permit).await {
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

        Ok(addr)
    }

    /// Handles a single replica connection: handshake + full sync + stream.
    /// `permit` holds one of the [`MAX_HANDSHAKES`] places until the
    /// handshake ends.
    async fn handle_replica(
        &self,
        stream: TcpStream,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> std::io::Result<()> {
        // wrap in a 64 KiB write buffer so the 4-5 small framing writes
        // (tag, shard_id, offset, length) per record are coalesced into
        // a single syscall. reads are delegated to the inner stream.
        let mut stream = BufWriter::with_capacity(65536, IdleTimeout::new(stream));
        let handshake = within(HANDSHAKE_TIMEOUT, "handshake", self.handshake(&mut stream)).await;
        // the status or error reply may still be in the buffer
        let flushed = stream.flush().await;
        handshake?;
        flushed?;
        drop(permit);

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
        // aborted on every return, including an error during the full sync
        let forwarder = AbortOnDrop(tokio::spawn(forward_events(
            rx,
            queue_tx,
            Arc::clone(&queued),
        )));

        // --- full sync ---
        self.tracker.full_sync_started();
        // the schemas go first: the snapshots hold PROTO values of their
        // types, and later PROTO commands need them on the replica too
        for (name, descriptor) in self.schemas() {
            write_u8(&mut stream, MSG_SCHEMA).await?;
            write_u32_le(&mut stream, name.len() as u32).await?;
            stream.write_all(name.as_bytes()).await?;
            write_u32_le(&mut stream, descriptor.len() as u32).await?;
            stream.write_all(&descriptor).await?;
        }
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

        let mut ack_task = AbortOnDrop(tokio::spawn(async move {
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
        }));

        // --- incremental stream ---
        // the ACK reader ends when the replica goes quiet for REPL_TIMEOUT
        // or disconnects. stop streaming then too: heartbeats alone would
        // keep a dead replica's connection open
        let result = tokio::select! {
            result = stream_records(&mut writer, &mut queue, &queued) => result,
            _ = &mut ack_task.0 => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "the replica stopped acknowledging",
            )),
        };

        drop(forwarder);
        self.tracker.remove(replica_id);

        result
    }

    /// The protobuf schemas registered on this primary.
    fn schemas(&self) -> Vec<(String, Bytes)> {
        #[cfg(feature = "protobuf")]
        if let Some(registry) = self.engine.schema_registry() {
            if let Ok(registry) = registry.read() {
                return registry
                    .iter_schemas()
                    .map(|(name, descriptor)| (name.to_owned(), descriptor.clone()))
                    .collect();
            }
        }
        Vec::new()
    }

    /// Checks the replica's version and shard count, and that both sides
    /// hold the cluster secret.
    async fn handshake(&self, stream: &mut BufWriter<Conn>) -> io::Result<()> {
        let replica_version = read_u8(stream).await?;
        if replica_version != REPL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported replication version: {replica_version}"),
            ));
        }
        let replica_shards = read_u16_le(stream).await?;
        let mut replica_nonce = [0u8; NONCE_LEN];
        stream.read_exact(&mut replica_nonce).await?;
        let our_shards = self.engine.shard_count() as u16;

        write_u8(stream, REPL_VERSION).await?;
        write_u16_le(stream, our_shards).await?;
        let id_bytes = self.primary_id.as_bytes();
        write_u8(stream, id_bytes.len() as u8).await?;
        stream.write_all(id_bytes).await?;

        if replica_shards != our_shards {
            write_u8(stream, STATUS_SHARD_MISMATCH).await?;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("shard count mismatch: replica={replica_shards} primary={our_shards}"),
            ));
        }
        write_u8(stream, STATUS_OK).await?;

        // each side proves it holds the cluster secret by signing a fresh
        // nonce from the other, so a captured answer can't be replayed.
        // the primary answers first, and the replica gets no data until
        // it has answered too
        let secret = self.secret.as_deref();
        let nonce: [u8; NONCE_LEN] = rand::random();
        stream.write_all(&nonce).await?;
        stream
            .write_all(&auth_tag(secret, PRIMARY_ROLE, &replica_nonce))
            .await?;
        stream.flush().await?;
        let mut tag = [0u8; ember_cluster::TAG_LEN];
        stream.read_exact(&mut tag).await?;
        if !auth_ok(secret, REPLICA_ROLE, &nonce, &tag) {
            write_u8(stream, STATUS_AUTH_FAILED).await?;
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "replica failed cluster authentication",
            ));
        }
        write_u8(stream, STATUS_OK).await
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
/// dry, and MSG_PING when there has been nothing to send for
/// [`HEARTBEAT_INTERVAL`]. Once the forwarder closes the queue, the rest of
/// it is stale: it sends MSG_RESYNC instead, so the replica starts over
/// with a full sync.
async fn stream_records(
    writer: &mut BufWriter<tokio::io::WriteHalf<Conn>>,
    queue: &mut mpsc::UnboundedReceiver<Bytes>,
    queued: &AtomicUsize,
) -> std::io::Result<()> {
    loop {
        let frame = match tokio::time::timeout(HEARTBEAT_INTERVAL, queue.recv()).await {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(_) => {
                write_u8(writer, MSG_PING).await?;
                writer.flush().await?;
                continue;
            }
        };
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
    /// The offsets this replica has applied and acknowledged, summed over
    /// the shards.
    applied: Arc<AtomicU64>,
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
        applied: Arc<AtomicU64>,
    ) -> tokio::task::JoinHandle<()> {
        let client = Self {
            engine,
            primary_addr,
            secret,
            applied,
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
        let mut stream = BufReader::with_capacity(65536, IdleTimeout::new(stream));
        let (primary_id, primary_shards) = self.handshake(&mut stream).await?;

        info!(primary_id = %primary_id, "handshake ok, loading full sync");
        *backoff = INITIAL_BACKOFF;

        // a full sync replaces everything: keys the primary no longer has
        // must not survive it
        self.engine
            .broadcast_ok(|| ShardRequest::FlushDb)
            .await
            .map_err(|e| std::io::Error::other(format!("flush before sync failed: {e}")))?;

        // receive per-shard snapshots and the offset each one covers
        let mut batch = Batch::new(usize::from(primary_shards));
        let mut snapshot_offsets = vec![0u64; usize::from(primary_shards)];
        // the snapshot buffer holds the largest shard's snapshot, so it is
        // freed once the full sync is in, not kept for the whole stream
        let mut snap_buf = Vec::new();
        let mut tag = read_u8(&mut stream).await?;
        while tag == MSG_SCHEMA {
            self.load_schema(&mut stream, &mut snap_buf).await?;
            tag = read_u8(&mut stream).await?;
        }
        for i in 0..primary_shards {
            if i > 0 {
                tag = read_u8(&mut stream).await?;
            }
            if tag != MSG_SHARD_SYNC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("expected MSG_SHARD_SYNC ({MSG_SHARD_SYNC}), got {tag}"),
                ));
            }
            let shard_id = read_u16_le(&mut stream).await?;
            let snap_len = read_u32_le(&mut stream).await?;
            read_body(&mut stream, snap_len, &mut snap_buf).await?;
            self.apply_snapshot(&mut batch, shard_id, &snap_buf).await?;

            expect_tag(&mut stream, MSG_SHARD_OFFSET, "MSG_SHARD_OFFSET").await?;
            let offset_shard = read_u16_le(&mut stream).await?;
            let offset = read_u64_le(&mut stream).await?;
            if let Some(slot) = snapshot_offsets.get_mut(usize::from(offset_shard)) {
                *slot = offset;
            }
        }
        drop(snap_buf);
        batch.apply(&self.engine).await?;
        let mut applied = snapshot_offsets.clone();
        let mut acks = Vec::new();
        for (shard, offset) in snapshot_offsets.iter().enumerate() {
            acks.push(MSG_ACK);
            acks.extend_from_slice(&(shard as u16).to_le_bytes());
            acks.extend_from_slice(&offset.to_le_bytes());
        }
        stream.write_all(&acks).await?;
        self.applied.store(applied.iter().sum(), Ordering::Relaxed);

        info!("full sync applied, starting incremental replay");

        // incremental stream. records are collected into a batch, which
        // is applied and acknowledged once the buffered input is used up
        let mut record_bytes = Vec::new();
        loop {
            let msg = read_u8(&mut stream).await?;
            match msg {
                MSG_RECORD => {
                    let shard_id = read_u16_le(&mut stream).await?;
                    let offset = read_u64_le(&mut stream).await?;
                    let record_len = read_u32_le(&mut stream).await?;
                    read_body(&mut stream, record_len, &mut record_bytes).await?;

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
                MSG_PING => write_u8(&mut stream, MSG_PING).await?,
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
                for (shard, offset) in batch.apply(&self.engine).await? {
                    acks.push(MSG_ACK);
                    acks.extend_from_slice(&shard.to_le_bytes());
                    acks.extend_from_slice(&offset.to_le_bytes());
                    if let Some(slot) = applied.get_mut(usize::from(shard)) {
                        *slot = offset;
                    }
                }
                stream.write_all(&acks).await?;
                self.applied.store(applied.iter().sum(), Ordering::Relaxed);
            }
        }
    }

    /// Checks the primary's version and shard count, and that both sides
    /// hold the cluster secret. Returns the primary's id and shard count.
    async fn handshake(&self, stream: &mut BufReader<Conn>) -> io::Result<(String, u16)> {
        let our_shards = self.engine.shard_count() as u16;
        let nonce: [u8; NONCE_LEN] = rand::random();
        write_u8(stream, REPL_VERSION).await?;
        write_u16_le(stream, our_shards).await?;
        stream.write_all(&nonce).await?;

        let primary_version = read_u8(stream).await?;
        if primary_version != REPL_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported primary replication version: {primary_version}"),
            ));
        }
        let primary_shards = read_u16_le(stream).await?;
        let id_len = read_u8(stream).await? as usize;
        let mut id_buf = vec![0u8; id_len];
        stream.read_exact(&mut id_buf).await?;
        let primary_id = String::from_utf8_lossy(&id_buf).into_owned();

        if read_u8(stream).await? == STATUS_SHARD_MISMATCH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "shard count mismatch with primary {primary_id}: \
                     ours={our_shards} primary={primary_shards}"
                ),
            ));
        }

        // the primary proves it holds the cluster secret before this node
        // flushes its data for the primary's, then this node proves it
        let secret = self.secret.as_deref();
        let mut primary_nonce = [0u8; NONCE_LEN];
        stream.read_exact(&mut primary_nonce).await?;
        let mut tag = [0u8; ember_cluster::TAG_LEN];
        stream.read_exact(&mut tag).await?;
        if !auth_ok(secret, PRIMARY_ROLE, &nonce, &tag) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("primary {primary_id} failed cluster authentication"),
            ));
        }
        stream
            .write_all(&auth_tag(secret, REPLICA_ROLE, &primary_nonce))
            .await?;
        if read_u8(stream).await? == STATUS_AUTH_FAILED {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("primary {primary_id} rejected our cluster credentials"),
            ));
        }
        Ok((primary_id, primary_shards))
    }

    /// Reads one MSG_SCHEMA body and registers the schema, unless it is
    /// already registered. `buf` is scratch space.
    async fn load_schema(
        &self,
        stream: &mut (impl AsyncRead + Unpin),
        buf: &mut Vec<u8>,
    ) -> io::Result<()> {
        let name_len = read_u32_le(stream).await?;
        read_body(stream, name_len, buf).await?;
        let name = String::from_utf8(std::mem::take(buf))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "schema name isn't UTF-8"))?;
        let descriptor_len = read_u32_le(stream).await?;
        read_body(stream, descriptor_len, buf).await?;
        #[cfg(feature = "protobuf")]
        if let Some(registry) = self.engine.schema_registry() {
            if let Ok(mut registry) = registry.write() {
                registry.restore(name, Bytes::copy_from_slice(buf));
            }
        }
        #[cfg(not(feature = "protobuf"))]
        let _ = name;
        Ok(())
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
                batch.apply(&self.engine).await?;
            }
        }
        Ok(())
    }
}

/// Aborts the task when dropped, so it doesn't outlive the connection it
/// serves.
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
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
    ///
    /// The primary only streams writes that succeeded there, so one that
    /// fails here means this copy differs from the primary's. That's an
    /// error: the offsets aren't acknowledged, and the caller drops the
    /// connection, which starts a full resync.
    async fn apply(&mut self, engine: &Engine) -> io::Result<Vec<(u16, u64)>> {
        let mut replies = Vec::with_capacity(self.len);
        for (shard, requests) in self.requests.iter_mut().enumerate() {
            if requests.is_empty() {
                continue;
            }
            let batch = engine
                .dispatch_batch_to_shard(shard, std::mem::take(requests))
                .await
                .map_err(|e| io::Error::other(format!("replication apply failed: {e:?}")))?;
            replies.extend(batch);
        }
        self.len = 0;
        for reply in replies {
            match reply.await {
                Ok(ShardResponse::Err(e)) => {
                    return Err(io::Error::other(format!("replication apply failed: {e}")))
                }
                Ok(ShardResponse::OutOfMemory) => {
                    return Err(io::Error::other(
                        "replication apply failed: out of memory; raise maxmemory on the replica",
                    ))
                }
                Ok(ShardResponse::WrongType) => {
                    return Err(io::Error::other(
                        "replication apply failed: a key holds another type than on the primary",
                    ))
                }
                Ok(_) => {}
                Err(_) => {
                    return Err(io::Error::other(
                        "replication apply failed: shard dropped the request",
                    ))
                }
            }
        }
        Ok(self
            .acks
            .iter_mut()
            .enumerate()
            .filter_map(|(shard, ack)| Some((shard as u16, ack.take()?)))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ember_core::{EngineConfig, Value};

    fn secret(password: &str) -> Option<Arc<ClusterSecret>> {
        Some(Arc::new(ClusterSecret::from_password(password)))
    }

    fn set(key: &str, value: &'static str) -> ShardRequest {
        ShardRequest::Set {
            key: key.into(),
            value: Bytes::from_static(value.as_bytes()),
            expire: None,
            nx: false,
            xx: false,
        }
    }

    /// The value of `key` on `engine`, if it is a string.
    async fn get(engine: &Engine, key: &str) -> Option<Bytes> {
        let get = ShardRequest::Get { key: key.into() };
        match engine.send_to_shard(0, get).await.unwrap() {
            ShardResponse::Value(Some(Value::String(v))) => Some(v),
            _ => None,
        }
    }

    /// Starts a primary with one shard holding `greeting = hello`. Returns
    /// its replication address and engine.
    async fn start_primary(secret: Option<Arc<ClusterSecret>>) -> (SocketAddr, Arc<Engine>) {
        let (tx, _) = broadcast::channel(16);
        let config = EngineConfig {
            replication_tx: Some(tx),
            ..Default::default()
        };
        let engine = Arc::new(Engine::with_config(1, config));
        engine
            .send_to_shard(0, set("greeting", "hello"))
            .await
            .unwrap();
        let addr = "127.0.0.1:0".parse().unwrap();
        let tracker = Arc::new(ReplicaTracker::new());
        let addr =
            ReplicationServer::start(Arc::clone(&engine), "primary".into(), addr, tracker, secret)
                .await
                .unwrap();
        (addr, engine)
    }

    fn client(secret: Option<Arc<ClusterSecret>>) -> ReplicationClient {
        ReplicationClient {
            engine: Arc::new(Engine::new(1)),
            primary_addr: "127.0.0.1:1".parse().unwrap(),
            secret,
            applied: Arc::default(),
        }
    }

    /// Runs one replication session against `addr`, for at most two
    /// replication timeouts.
    async fn sync_once(client: &ReplicationClient, addr: SocketAddr) -> io::Result<()> {
        let stream = TcpStream::connect(addr).await?;
        let mut backoff = INITIAL_BACKOFF;
        within(REPL_TIMEOUT * 2, "sync", client.sync(stream, &mut backoff)).await
    }

    #[tokio::test]
    async fn each_side_checks_that_the_other_holds_the_secret() {
        let denied = |result: io::Result<()>| matches!(result, Err(e) if e.kind() == io::ErrorKind::PermissionDenied);

        // a primary with another secret, or none, can't feed this replica
        let (primary, _) = start_primary(secret("other")).await;
        assert!(denied(sync_once(&client(secret("ours")), primary).await));
        let (open_primary, _) = start_primary(None).await;
        assert!(denied(
            sync_once(&client(secret("ours")), open_primary).await
        ));

        // a replica without the secret gets nothing
        let (primary, _) = start_primary(secret("ours")).await;
        assert!(denied(sync_once(&client(None), primary).await));

        // with the same secret the replica syncs, then streams until the
        // test stops it
        let replica = client(secret("ours"));
        let result = sync_once(&replica, primary).await;
        assert!(matches!(result, Err(e) if e.kind() == io::ErrorKind::TimedOut));
        assert_eq!(get(&replica.engine, "greeting").await.unwrap(), "hello");
        // the snapshot's offset is advertised for failover elections
        assert!(replica.applied.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn heartbeats_keep_a_quiet_connection_open() {
        let (primary, primary_engine) = start_primary(None).await;
        let replica = Arc::new(client(None));
        let session = tokio::spawn({
            let replica = Arc::clone(&replica);
            async move {
                let stream = TcpStream::connect(primary).await?;
                let mut backoff = INITIAL_BACKOFF;
                replica.sync(stream, &mut backoff).await
            }
        });

        tokio::time::sleep(REPL_TIMEOUT * 3).await;
        assert!(!session.is_finished(), "{:?}", session.await);

        // the connection still carries writes
        primary_engine
            .send_to_shard(0, set("later", "still here"))
            .await
            .unwrap();
        for _ in 0..100 {
            if get(&replica.engine, "later").await.is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the write never reached the replica");
    }

    #[tokio::test]
    async fn the_primary_drops_a_peer_that_never_finishes_the_handshake() {
        let (primary, _) = start_primary(None).await;
        let start = Instant::now();
        let mut peer = TcpStream::connect(primary).await.unwrap();
        // the primary closes the connection without a byte
        let mut buf = [0u8; 1];
        assert_eq!(peer.read(&mut buf).await.unwrap(), 0);
        assert!(start.elapsed() >= HANDSHAKE_TIMEOUT);
    }

    #[tokio::test]
    async fn a_replica_drops_a_primary_that_goes_quiet() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // a primary with no shards that completes the handshake, then
        // sends nothing, not even heartbeats
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 3 + NONCE_LEN];
            stream.read_exact(&mut request).await.unwrap();
            let mut reply = vec![REPL_VERSION, 0, 0, 1, b'p', STATUS_OK];
            reply.extend_from_slice(&[0; NONCE_LEN]);
            reply.extend_from_slice(&[0; ember_cluster::TAG_LEN]);
            stream.write_all(&reply).await.unwrap();
            let mut tag = [0u8; ember_cluster::TAG_LEN];
            stream.read_exact(&mut tag).await.unwrap();
            stream.write_all(&[STATUS_OK]).await.unwrap();
            std::future::pending::<()>().await;
        });

        let replica = client(None);
        let start = Instant::now();
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut backoff = INITIAL_BACKOFF;
        let result = replica.sync(stream, &mut backoff).await;
        assert!(matches!(result, Err(e) if e.kind() == io::ErrorKind::TimedOut));
        assert!(start.elapsed() >= REPL_TIMEOUT);
    }

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

    #[test]
    fn a_replica_counts_for_wait_only_after_acknowledging() {
        // after a primary restart every offset is 0, and a replica still
        // loading its full sync must not count yet
        let tracker = ReplicaTracker::new();
        let replica = tracker.register(2);
        assert_eq!(tracker.count_caught_up(&[0, 0]), 0);
        tracker.update(replica, 0, 0);
        assert_eq!(tracker.count_caught_up(&[0, 0]), 0);
        tracker.update(replica, 1, 0);
        assert_eq!(tracker.count_caught_up(&[0, 0]), 1);
    }

    #[tokio::test]
    async fn a_write_that_fails_on_the_replica_is_not_acknowledged() {
        let engine = Engine::new(1);
        engine.route("k", set("k", "v")).await.unwrap();
        let mut batch = Batch::new(1);
        // a list push onto the string fails with WRONGTYPE
        let push = ShardRequest::LPush {
            key: "k".into(),
            values: vec![Bytes::from_static(b"x")],
        };
        batch.push(0, Some(push), Some(7));
        assert!(batch.apply(&engine).await.is_err());
    }
}
