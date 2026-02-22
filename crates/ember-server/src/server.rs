//! TCP/TLS server that accepts client connections and spawns handler tasks.
//!
//! Handles graceful shutdown on SIGINT/SIGTERM: stops accepting new
//! connections and waits for in-flight requests to drain before exiting.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ember_core::{ConcurrentKeyspace, Engine, EngineConfig, EvictionPolicy};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

use crate::cluster::ClusterCoordinator;
use crate::config::{ConfigRegistry, ConnectionLimits};
use crate::connection;
use crate::connection_common::MonitorEvent;
use crate::metrics::MemoryUsedBytes;
use crate::pubsub::PubSubManager;
use crate::slowlog::{SlowLog, SlowLogConfig};
use crate::tls::TlsConfig;

/// Default maximum number of concurrent client connections.
const DEFAULT_MAX_CONNECTIONS: usize = 10_000;

/// Shared server state for INFO and observability.
///
/// Created once at startup and shared (via `Arc`) across all connection
/// handlers. Atomic counters avoid any locking on the hot path.
#[derive(Debug)]
pub struct ServerContext {
    pub start_time: Instant,
    pub version: &'static str,
    pub shard_count: usize,
    pub max_connections: usize,
    pub max_memory: Option<usize>,
    pub aof_enabled: bool,
    pub metrics_enabled: bool,
    pub connections_accepted: AtomicU64,
    pub connections_active: AtomicU64,
    pub commands_processed: AtomicU64,
    /// Password required for AUTH. None means no authentication needed.
    pub requirepass: Option<String>,
    /// The address the server is bound to (for protected mode checks).
    pub bind_addr: SocketAddr,
    /// Runtime configuration registry for CONFIG GET/SET.
    pub config: Arc<ConfigRegistry>,
    /// Path to the config file loaded at startup (for CONFIG REWRITE).
    pub config_path: Option<PathBuf>,
    /// Cluster coordinator, present when --cluster-enabled is set.
    pub cluster: Option<Arc<ClusterCoordinator>>,
    /// Runtime limits derived from EmberConfig.
    pub limits: ConnectionLimits,
    /// Memory used bytes, updated by the stats poller.
    ///
    /// Read by the /health endpoint to avoid querying shards per HTTP request.
    pub memory_used_bytes: MemoryUsedBytes,
    /// Broadcast channel for MONITOR subscribers.
    ///
    /// When `sender.receiver_count() == 0`, the per-command check is a
    /// single atomic load — true zero overhead when nobody is monitoring.
    pub monitor_tx: tokio::sync::broadcast::Sender<MonitorEvent>,
}

/// Sets nodelay, acquires a semaphore permit, and updates connection metrics.
/// Returns `None` if the connection limit was reached — the caller should
/// `continue`. Protected mode must be checked by the caller before this.
fn accept_connection(
    stream: &TcpStream,
    peer: SocketAddr,
    ctx: &Arc<ServerContext>,
    semaphore: &Arc<Semaphore>,
) -> Option<OwnedSemaphorePermit> {
    if let Err(e) = stream.set_nodelay(true) {
        warn!("failed to set TCP_NODELAY: {e}");
    }

    let permit = match semaphore.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            warn!("connection limit reached, dropping connection from {peer}");
            if ctx.metrics_enabled {
                crate::metrics::on_connection_rejected();
            }
            return None;
        }
    };

    if ctx.metrics_enabled {
        crate::metrics::on_connection_accepted();
    }
    ctx.connections_accepted.fetch_add(1, Ordering::Relaxed);
    ctx.connections_active.fetch_add(1, Ordering::Relaxed);

    Some(permit)
}

/// Performs TLS handshake with a 10-second timeout to prevent slowloris attacks.
/// Returns `None` on handshake failure or timeout.
async fn tls_handshake(
    acceptor: TlsAcceptor,
    stream: TcpStream,
    peer: SocketAddr,
) -> Option<tokio_rustls::server::TlsStream<TcpStream>> {
    let handshake = tokio::time::timeout(Duration::from_secs(10), acceptor.accept(stream));
    match handshake.await {
        Ok(Ok(tls_stream)) => Some(tls_stream),
        Ok(Err(e)) => {
            warn!("TLS handshake failed from {peer}: {e}");
            None
        }
        Err(_) => {
            warn!("TLS handshake timed out from {peer}");
            None
        }
    }
}

/// Decrements active connection count and records the close metric.
fn on_connection_done(ctx: &ServerContext) {
    ctx.connections_active.fetch_sub(1, Ordering::Relaxed);
    if ctx.metrics_enabled {
        crate::metrics::on_connection_closed();
    }
}

/// Waits for all connections to drain with a 30-second timeout.
async fn drain_connections(semaphore: &Arc<Semaphore>, max_conn: usize) {
    info!("waiting for active connections to close...");
    let max_conn_u32 = u32::try_from(max_conn).unwrap_or(u32::MAX);
    let drain = semaphore.acquire_many(max_conn_u32);
    match tokio::time::timeout(Duration::from_secs(30), drain).await {
        Ok(_) => info!("all connections drained, shutting down"),
        Err(_) => warn!("shutdown timeout after 30s, forcing exit"),
    }
}

/// Bundles the Arc handles cloned into every connection handler task.
///
/// Avoids repeating the same four clone lines in each arm of the accept loop.
struct ConnectionHandles {
    engine: Engine,
    ctx: Arc<ServerContext>,
    slow_log: Arc<SlowLog>,
    pubsub: Arc<PubSubManager>,
}

impl ConnectionHandles {
    fn new(
        engine: &Engine,
        ctx: &Arc<ServerContext>,
        slow_log: &Arc<SlowLog>,
        pubsub: &Arc<PubSubManager>,
    ) -> Self {
        Self {
            engine: engine.clone(),
            ctx: Arc::clone(ctx),
            slow_log: Arc::clone(slow_log),
            pubsub: Arc::clone(pubsub),
        }
    }
}

/// Sends the protected mode rejection message and closes the connection.
async fn reject_protected_mode(mut stream: TcpStream) {
    let msg = "-DENIED Ember is running in protected mode \
               because no password is set. In this mode \
               connections are only accepted from the loopback \
               interface. Set a password with --requirepass or \
               bind to 127.0.0.1 to resolve this.\r\n";
    let _ = stream.write_all(msg.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// Runs the server with a concurrent keyspace (DashMap-backed).
///
/// This mode bypasses shard channels for GET/SET operations, accessing
/// the keyspace directly from connection handlers. Falls back to the
/// sharded engine for complex commands.
///
/// If `tls` is provided, also binds a TLS listener on the specified address.
#[allow(clippy::too_many_arguments)]
pub async fn run_concurrent(
    addr: SocketAddr,
    shard_count: usize,
    config: EngineConfig,
    max_memory: Option<usize>,
    eviction_policy: EvictionPolicy,
    max_connections: Option<usize>,
    metrics: Option<(SocketAddr, metrics_exporter_prometheus::PrometheusHandle)>,
    slowlog_config: SlowLogConfig,
    requirepass: Option<String>,
    tls: Option<(SocketAddr, TlsConfig)>,
    config_registry: Arc<ConfigRegistry>,
    limits: ConnectionLimits,
    config_path: Option<PathBuf>,
    #[cfg(feature = "grpc")] grpc_addr: Option<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>> {
    let aof_enabled = config
        .persistence
        .as_ref()
        .map(|p| p.append_only)
        .unwrap_or(false);

    // Create the concurrent keyspace
    let keyspace = Arc::new(ConcurrentKeyspace::new(max_memory, eviction_policy));

    // Also create the sharded engine for fallback on complex commands
    let engine = Engine::with_config(shard_count, config);

    let listener = TcpListener::bind(addr).await?;
    let max_conn = max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
    let semaphore = Arc::new(Semaphore::new(max_conn));

    let tls_listener = setup_tls_listener(tls).await?;

    let metrics_enabled = metrics.is_some();
    let stats_poll_interval = limits.stats_poll_interval;
    let ctx = Arc::new(ServerContext {
        start_time: Instant::now(),
        version: env!("CARGO_PKG_VERSION"),
        shard_count,
        max_connections: max_conn,
        max_memory,
        aof_enabled,
        metrics_enabled,
        connections_accepted: AtomicU64::new(0),
        connections_active: AtomicU64::new(0),
        commands_processed: AtomicU64::new(0),
        requirepass,
        bind_addr: addr,
        config: config_registry,
        config_path,
        cluster: None,
        limits,
        memory_used_bytes: MemoryUsedBytes::new(),
        monitor_tx: tokio::sync::broadcast::channel(256).0,
    });

    if let Some((metrics_addr, handle)) = metrics {
        crate::metrics::spawn_http_server(metrics_addr, handle, Arc::clone(&ctx));
        crate::metrics::spawn_stats_poller(engine.clone(), Arc::clone(&ctx), stats_poll_interval);
    }

    let slow_log = Arc::new(SlowLog::new(slowlog_config));
    let pubsub = Arc::new(PubSubManager::new());

    // spawn gRPC listener if configured
    #[cfg(feature = "grpc")]
    let _grpc_handle = if let Some(grpc_addr) = grpc_addr {
        let svc = crate::grpc::EmberService::new(
            engine.clone(),
            Arc::clone(&ctx),
            Arc::clone(&slow_log),
            Arc::clone(&pubsub),
        );
        info!("gRPC listening on {grpc_addr}");
        let server = tonic::transport::Server::builder()
            .concurrency_limit_per_connection(256)
            .add_service(svc.into_service())
            .serve(grpc_addr);
        Some(tokio::spawn(async move {
            if let Err(e) = server.await {
                error!("gRPC server error: {e}");
            }
        }))
    } else {
        None
    };

    info!("listening on {addr} with concurrent keyspace (max {max_conn} connections)");

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    // helper to accept from TLS listener or pend forever if disabled
    let tls_accept = || async {
        match &tls_listener {
            Some((listener, acceptor)) => {
                let (stream, addr) = listener.accept().await?;
                Ok::<_, std::io::Error>((stream, addr, acceptor.clone()))
            }
            None => std::future::pending().await,
        }
    };

    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown => {
                info!("shutdown signal received, draining connections...");
                break;
            }

            // plain TCP accept
            result = listener.accept() => {
                let (stream, peer) = result?;

                if is_protected_mode_violation(&ctx, &peer) {
                    reject_protected_mode(stream).await;
                    continue;
                }

                let permit = match accept_connection(&stream, peer, &ctx, &semaphore) {
                    Some(p) => p,
                    None => continue,
                };

                let keyspace = Arc::clone(&keyspace);
                let h = ConnectionHandles::new(&engine, &ctx, &slow_log, &pubsub);

                tokio::spawn(async move {
                    if let Err(e) = crate::concurrent_handler::handle(
                        stream, peer, keyspace, h.engine, &h.ctx, &h.slow_log, &h.pubsub
                    ).await {
                        error!("connection error from {peer}: {e}");
                    }
                    on_connection_done(&h.ctx);
                    drop(permit);
                });
            }

            // TLS accept (pends forever if TLS not configured)
            result = tls_accept() => {
                let (stream, peer, acceptor) = result?;

                if is_protected_mode_violation(&ctx, &peer) {
                    reject_protected_mode(stream).await;
                    continue;
                }

                let permit = match accept_connection(&stream, peer, &ctx, &semaphore) {
                    Some(p) => p,
                    None => continue,
                };

                let keyspace = Arc::clone(&keyspace);
                let h = ConnectionHandles::new(&engine, &ctx, &slow_log, &pubsub);

                tokio::spawn(async move {
                    if let Some(tls_stream) = tls_handshake(acceptor, stream, peer).await {
                        if let Err(e) = crate::concurrent_handler::handle(
                            tls_stream, peer, keyspace, h.engine, &h.ctx, &h.slow_log, &h.pubsub
                        ).await {
                            error!("TLS connection error from {peer}: {e}");
                        }
                    }
                    on_connection_done(&h.ctx);
                    drop(permit);
                });
            }
        }
    }

    drain_connections(&semaphore, max_conn).await;
    Ok(())
}

/// Pins the calling thread to the CPU core matching `worker_id`.
///
/// On Linux, uses `sched_setaffinity` to bind the thread to a single core,
/// reducing L1/L2 cache misses from OS thread migration. On other platforms
/// (macOS, Windows) this is a no-op — macOS doesn't support thread affinity
/// and the performance impact is negligible on smaller core counts.
///
/// If `worker_id` exceeds the number of configurable CPUs (typically 1024),
/// pinning is skipped with a warning.
fn pin_to_core(worker_id: usize) {
    #[cfg(target_os = "linux")]
    {
        // CPU_SET indexes into a fixed-size bitset (CPU_SETSIZE, typically
        // 1024). Guard against buffer overflow if worker_id is too large.
        const CPU_SETSIZE: usize = 1024;
        if worker_id >= CPU_SETSIZE {
            tracing::warn!(
                worker_id,
                "worker_id exceeds CPU_SETSIZE ({CPU_SETSIZE}), skipping affinity"
            );
            return;
        }

        // SAFETY: cpu_set_t is a plain C struct with no invariants beyond
        // zeroing. CPU_SET and sched_setaffinity are standard POSIX APIs.
        // We pass tid=0 (current thread) and a properly sized set.
        // worker_id is bounds-checked above against CPU_SETSIZE.
        unsafe {
            let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(worker_id, &mut cpuset);
            let result = libc::sched_setaffinity(0, std::mem::size_of_val(&cpuset), &cpuset);
            if result != 0 {
                let err = std::io::Error::last_os_error();
                tracing::warn!(worker_id, %err, "failed to pin worker to core");
            } else {
                tracing::debug!(worker_id, "pinned to core");
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = worker_id;
    }
}

/// Thread-per-core server: each OS thread gets its own single-threaded
/// tokio runtime, SO_REUSEPORT listener, and runs one shard.
///
/// Management tasks (metrics, gRPC, stats poller) run on the caller's
/// runtime. Worker threads are joined after shutdown is signaled.
///
/// This replaces `run()` for the sharded path — same semantics but with
/// pinned shard-per-thread affinity and parallel accepts.
#[allow(clippy::too_many_arguments)]
pub async fn run_threaded(
    addr: SocketAddr,
    shard_count: usize,
    config: EngineConfig,
    max_connections: Option<usize>,
    metrics: Option<(SocketAddr, metrics_exporter_prometheus::PrometheusHandle)>,
    slowlog_config: SlowLogConfig,
    requirepass: Option<String>,
    tls: Option<(SocketAddr, TlsConfig)>,
    cluster: Option<Arc<ClusterCoordinator>>,
    config_registry: Arc<ConfigRegistry>,
    limits: ConnectionLimits,
    config_path: Option<PathBuf>,
    #[cfg(feature = "grpc")] grpc_addr: Option<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // ensure data directory exists if persistence is configured
    if let Some(ref pcfg) = config.persistence {
        std::fs::create_dir_all(&pcfg.data_dir)?;
    }

    let aof_enabled = config
        .persistence
        .as_ref()
        .map(|p| p.append_only)
        .unwrap_or(false);
    let max_memory = config
        .shard
        .max_memory
        .map(|per_shard| per_shard * shard_count);

    let (engine, prepared_shards) = Engine::prepare(shard_count, config);

    // wire replication: give the cluster coordinator access to the engine
    // and start the replication server so replicas can connect
    if let Some(ref coordinator) = cluster {
        coordinator.set_engine(Arc::new(engine.clone()));
        coordinator.start_replication_server().await;
    }

    let max_conn = max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
    let semaphore = Arc::new(Semaphore::new(max_conn));

    let metrics_enabled = metrics.is_some();
    let stats_poll_interval = limits.stats_poll_interval;
    let ctx = Arc::new(ServerContext {
        start_time: Instant::now(),
        version: env!("CARGO_PKG_VERSION"),
        shard_count,
        max_connections: max_conn,
        max_memory,
        aof_enabled,
        metrics_enabled,
        connections_accepted: AtomicU64::new(0),
        connections_active: AtomicU64::new(0),
        commands_processed: AtomicU64::new(0),
        requirepass,
        bind_addr: addr,
        config: config_registry,
        config_path,
        cluster,
        limits,
        memory_used_bytes: MemoryUsedBytes::new(),
        monitor_tx: tokio::sync::broadcast::channel(256).0,
    });

    // management tasks run on the caller's runtime (not the hot path)
    if let Some((metrics_addr, handle)) = metrics {
        crate::metrics::spawn_http_server(metrics_addr, handle, Arc::clone(&ctx));
        crate::metrics::spawn_stats_poller(engine.clone(), Arc::clone(&ctx), stats_poll_interval);
    }

    let slow_log = Arc::new(SlowLog::new(slowlog_config));
    let pubsub = Arc::new(PubSubManager::new());

    #[cfg(feature = "grpc")]
    let _grpc_handle = if let Some(grpc_addr) = grpc_addr {
        let svc = crate::grpc::EmberService::new(
            engine.clone(),
            Arc::clone(&ctx),
            Arc::clone(&slow_log),
            Arc::clone(&pubsub),
        );
        info!("gRPC listening on {grpc_addr}");
        let server = tonic::transport::Server::builder()
            .concurrency_limit_per_connection(256)
            .add_service(svc.into_service())
            .serve(grpc_addr);
        Some(tokio::spawn(async move {
            if let Err(e) = server.await {
                error!("gRPC server error: {e}");
            }
        }))
    } else {
        None
    };

    // build shared TLS acceptor (if configured) — each worker clones it
    let tls_acceptor: Option<(SocketAddr, TlsAcceptor)> = match tls {
        Some((tls_addr, tls_config)) => {
            let acceptor = crate::tls::load_tls_acceptor(&tls_config)?;
            info!("TLS on {tls_addr} (thread-per-core)");
            Some((tls_addr, acceptor))
        }
        None => None,
    };

    let shutdown = Arc::new(tokio::sync::Notify::new());

    // spawn one OS thread per shard, each with its own single-threaded runtime
    let workers: Vec<std::thread::JoinHandle<()>> = prepared_shards
        .into_iter()
        .enumerate()
        .map(|(id, prepared)| {
            let engine = engine.clone();
            let ctx = Arc::clone(&ctx);
            let slow_log = Arc::clone(&slow_log);
            let pubsub = Arc::clone(&pubsub);
            let semaphore = Arc::clone(&semaphore);
            let shutdown = Arc::clone(&shutdown);
            let tls = tls_acceptor.as_ref().map(|(a, b)| (*a, b.clone()));

            std::thread::Builder::new()
                .name(format!("ember-worker-{id}"))
                .spawn(move || {
                    pin_to_core(id);

                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build worker runtime");

                    rt.block_on(worker_main(
                        id, prepared, addr, engine, ctx, slow_log, pubsub, semaphore, tls, shutdown,
                    ));

                    // give in-flight connection handlers time to finish
                    rt.shutdown_timeout(Duration::from_secs(30));
                })
                .expect("failed to spawn worker thread")
        })
        .collect();

    info!(
        "listening on {addr} with {shard_count} shards, thread-per-core (max {max_conn} connections)"
    );

    // await shutdown signal on the management runtime
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received, stopping workers...");
    shutdown.notify_waiters();

    // join worker threads (blocking — use spawn_blocking to avoid starving
    // the management runtime while threads drain)
    tokio::task::spawn_blocking(move || {
        for (id, handle) in workers.into_iter().enumerate() {
            if let Err(e) = handle.join() {
                error!("worker {id} panicked: {e:?}");
            }
        }
    })
    .await?;

    info!("all workers stopped");
    Ok(())
}

/// Per-worker accept loop: runs one shard and accepts connections on a
/// SO_REUSEPORT listener pinned to this thread's single-threaded runtime.
#[allow(clippy::too_many_arguments)]
async fn worker_main(
    id: usize,
    prepared: ember_core::PreparedShard,
    addr: SocketAddr,
    engine: Engine,
    ctx: Arc<ServerContext>,
    slow_log: Arc<SlowLog>,
    pubsub: Arc<PubSubManager>,
    semaphore: Arc<Semaphore>,
    tls: Option<(SocketAddr, TlsAcceptor)>,
    shutdown: Arc<tokio::sync::Notify>,
) {
    // run the shard task on this worker's runtime
    tokio::spawn(ember_core::run_prepared(prepared));

    // bind SO_REUSEPORT listener — the kernel distributes connections
    // across all workers listening on the same port
    let listener = match bind_reuse_port(addr) {
        Ok(l) => l,
        Err(e) => {
            error!("worker {id}: failed to bind {addr}: {e}");
            return;
        }
    };

    let tls_listener: Option<(TcpListener, TlsAcceptor)> = match tls {
        Some((tls_addr, acceptor)) => match bind_reuse_port(tls_addr) {
            Ok(l) => Some((l, acceptor)),
            Err(e) => {
                error!("worker {id}: failed to bind TLS {tls_addr}: {e}");
                return;
            }
        },
        None => None,
    };

    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    // helper to accept from TLS listener or pend forever if disabled
    let tls_accept = || async {
        match &tls_listener {
            Some((listener, acceptor)) => {
                let (stream, addr) = listener.accept().await?;
                Ok::<_, std::io::Error>((stream, addr, acceptor.clone()))
            }
            None => std::future::pending().await,
        }
    };

    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown_fut => break,

            result = listener.accept() => {
                let (stream, peer) = match result {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("worker {id}: accept error: {e}");
                        continue;
                    }
                };

                if is_protected_mode_violation(&ctx, &peer) {
                    reject_protected_mode(stream).await;
                    continue;
                }

                let permit = match accept_connection(&stream, peer, &ctx, &semaphore) {
                    Some(p) => p,
                    None => continue,
                };

                let h = ConnectionHandles::new(&engine, &ctx, &slow_log, &pubsub);

                tokio::spawn(async move {
                    if let Err(e) = connection::handle(
                        stream, peer, h.engine, &h.ctx, &h.slow_log, &h.pubsub
                    ).await {
                        error!("connection error from {peer}: {e}");
                    }
                    on_connection_done(&h.ctx);
                    drop(permit);
                });
            }

            result = tls_accept() => {
                let (stream, peer, acceptor) = match result {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("worker {id}: TLS accept error: {e}");
                        continue;
                    }
                };

                if is_protected_mode_violation(&ctx, &peer) {
                    reject_protected_mode(stream).await;
                    continue;
                }

                let permit = match accept_connection(&stream, peer, &ctx, &semaphore) {
                    Some(p) => p,
                    None => continue,
                };

                let h = ConnectionHandles::new(&engine, &ctx, &slow_log, &pubsub);

                tokio::spawn(async move {
                    if let Some(tls_stream) = tls_handshake(acceptor, stream, peer).await {
                        if let Err(e) = connection::handle(
                            tls_stream, peer, h.engine, &h.ctx, &h.slow_log, &h.pubsub
                        ).await {
                            error!("TLS connection error from {peer}: {e}");
                        }
                    }
                    on_connection_done(&h.ctx);
                    drop(permit);
                });
            }
        }
    }
}

/// Creates a TCP listener with SO_REUSEPORT so multiple workers can
/// accept on the same address. The kernel distributes incoming connections
/// across all listeners, giving us parallel accepts without contention.
fn bind_reuse_port(addr: SocketAddr) -> std::io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};

    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_port(true)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;

    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

/// Binds a TLS listener if TLS is configured, otherwise returns `None`.
///
/// Extracted to avoid repeating the same bind + acceptor creation in both
/// `run` and `run_concurrent`.
async fn setup_tls_listener(
    tls: Option<(SocketAddr, TlsConfig)>,
) -> Result<Option<(TcpListener, TlsAcceptor)>, Box<dyn std::error::Error>> {
    match tls {
        Some((tls_addr, tls_config)) => {
            let acceptor = crate::tls::load_tls_acceptor(&tls_config)?;
            let listener = TcpListener::bind(tls_addr).await?;
            info!("TLS listening on {tls_addr}");
            Ok(Some((listener, acceptor)))
        }
        None => Ok(None),
    }
}

/// Returns true if the connection should be rejected by protected mode.
///
/// Protected mode activates when all three conditions hold:
/// 1. No password is configured (requirepass is None)
/// 2. The server is bound to a non-loopback address (e.g. 0.0.0.0)
/// 3. The connecting client is from a non-loopback address
fn is_protected_mode_violation(ctx: &ServerContext, peer: &SocketAddr) -> bool {
    if ctx.requirepass.is_some() {
        return false;
    }
    if ctx.bind_addr.ip().is_loopback() {
        return false;
    }
    !peer.ip().is_loopback()
}
