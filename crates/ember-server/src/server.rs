//! TCP/TLS server that accepts client connections and spawns handler tasks.
//!
//! Handles graceful shutdown on SIGINT/SIGTERM: stops accepting new
//! connections and waits for in-flight requests to drain before exiting.

use std::net::SocketAddr;
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
use crate::connection;
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
    /// Cluster coordinator, present when --cluster-enabled is set.
    pub cluster: Option<Arc<ClusterCoordinator>>,
}

/// Binds to `addr` and runs the accept loop.
///
/// Spawns a sharded engine with the given shard count and config, then
/// hands each incoming connection a cheap clone of the engine handle.
/// Limits concurrent connections to `max_connections` — excess clients
/// are dropped immediately.
///
/// If `tls` is provided, also binds a TLS listener on the specified address.
/// Both plain TCP and TLS connections share the same engine and connection limits.
///
/// On SIGINT or SIGTERM the server stops accepting new connections,
/// waits for existing handlers to finish, then exits cleanly.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    addr: SocketAddr,
    shard_count: usize,
    config: EngineConfig,
    max_connections: Option<usize>,
    metrics_enabled: bool,
    slowlog_config: SlowLogConfig,
    requirepass: Option<String>,
    tls: Option<(SocketAddr, TlsConfig)>,
    cluster: Option<Arc<ClusterCoordinator>>,
    #[cfg(feature = "grpc")] grpc_addr: Option<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>> {
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

    let engine = Engine::with_config(shard_count, config);

    if metrics_enabled {
        crate::metrics::spawn_stats_poller(engine.clone());
    }

    let listener = TcpListener::bind(addr).await?;
    let max_conn = max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
    let semaphore = Arc::new(Semaphore::new(max_conn));

    // set up TLS listener if configured
    let tls_listener: Option<(TcpListener, TlsAcceptor)> = if let Some((tls_addr, tls_config)) = tls
    {
        let acceptor = crate::tls::load_tls_acceptor(&tls_config)?;
        let tls_tcp = TcpListener::bind(tls_addr).await?;
        info!("TLS listening on {tls_addr}");
        Some((tls_tcp, acceptor))
    } else {
        None
    };

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
        cluster,
    });

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

    info!(
        "listening on {addr} with {} shards (max {max_conn} connections)",
        engine.shard_count()
    );

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

                let engine = engine.clone();
                let ctx = Arc::clone(&ctx);
                let slow_log = Arc::clone(&slow_log);
                let pubsub = Arc::clone(&pubsub);

                tokio::spawn(async move {
                    if let Err(e) = connection::handle(stream, engine, &ctx, &slow_log, &pubsub).await {
                        error!("connection error from {peer}: {e}");
                    }
                    on_connection_done(&ctx);
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

                let engine = engine.clone();
                let ctx = Arc::clone(&ctx);
                let slow_log = Arc::clone(&slow_log);
                let pubsub = Arc::clone(&pubsub);

                tokio::spawn(async move {
                    if let Some(tls_stream) = tls_handshake(acceptor, stream, peer).await {
                        if let Err(e) = connection::handle(tls_stream, engine, &ctx, &slow_log, &pubsub).await {
                            error!("TLS connection error from {peer}: {e}");
                        }
                    }
                    on_connection_done(&ctx);
                    drop(permit);
                });
            }
        }
    }

    drain_connections(&semaphore, max_conn).await;
    Ok(())
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
    metrics_enabled: bool,
    slowlog_config: SlowLogConfig,
    requirepass: Option<String>,
    tls: Option<(SocketAddr, TlsConfig)>,
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

    if metrics_enabled {
        crate::metrics::spawn_stats_poller(engine.clone());
    }

    let listener = TcpListener::bind(addr).await?;
    let max_conn = max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
    let semaphore = Arc::new(Semaphore::new(max_conn));

    // set up TLS listener if configured
    let tls_listener: Option<(TcpListener, TlsAcceptor)> = if let Some((tls_addr, tls_config)) = tls
    {
        let acceptor = crate::tls::load_tls_acceptor(&tls_config)?;
        let tls_tcp = TcpListener::bind(tls_addr).await?;
        info!("TLS listening on {tls_addr}");
        Some((tls_tcp, acceptor))
    } else {
        None
    };

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
        cluster: None,
    });

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
                let engine = engine.clone();
                let ctx = Arc::clone(&ctx);
                let slow_log = Arc::clone(&slow_log);
                let pubsub = Arc::clone(&pubsub);

                tokio::spawn(async move {
                    if let Err(e) = crate::concurrent_handler::handle(
                        stream, keyspace, engine, &ctx, &slow_log, &pubsub
                    ).await {
                        error!("connection error from {peer}: {e}");
                    }
                    on_connection_done(&ctx);
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
                let engine = engine.clone();
                let ctx = Arc::clone(&ctx);
                let slow_log = Arc::clone(&slow_log);
                let pubsub = Arc::clone(&pubsub);

                tokio::spawn(async move {
                    if let Some(tls_stream) = tls_handshake(acceptor, stream, peer).await {
                        if let Err(e) = crate::concurrent_handler::handle(
                            tls_stream, keyspace, engine, &ctx, &slow_log, &pubsub
                        ).await {
                            error!("TLS connection error from {peer}: {e}");
                        }
                    }
                    on_connection_done(&ctx);
                    drop(permit);
                });
            }
        }
    }

    drain_connections(&semaphore, max_conn).await;
    Ok(())
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
