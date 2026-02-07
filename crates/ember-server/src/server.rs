//! TCP server that accepts client connections and spawns handler tasks.
//!
//! Supports two modes:
//! - Single-listener: one accept loop in the main tokio runtime (default)
//! - Worker-per-core: multiple accept loops via SO_REUSEPORT (better scaling)
//!
//! Handles graceful shutdown on SIGINT/SIGTERM: stops accepting new
//! connections and waits for in-flight requests to drain before exiting.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use ember_core::{Engine, EngineConfig};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use crate::connection;
use crate::slowlog::{SlowLog, SlowLogConfig};

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
}

/// Binds to `addr` and runs the accept loop.
///
/// Spawns a sharded engine with the given shard count and config, then
/// hands each incoming connection a cheap clone of the engine handle.
/// Limits concurrent connections to `max_connections` — excess clients
/// are dropped immediately.
///
/// On SIGINT or SIGTERM the server stops accepting new connections,
/// waits for existing handlers to finish, then exits cleanly.
pub async fn run(
    addr: SocketAddr,
    shard_count: usize,
    config: EngineConfig,
    max_connections: Option<usize>,
    metrics_enabled: bool,
    slowlog_config: SlowLogConfig,
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
    });

    let slow_log = Arc::new(SlowLog::new(slowlog_config));

    info!(
        "listening on {addr} with {} shards (max {max_conn} connections)",
        engine.shard_count()
    );

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;

            _ = &mut shutdown => {
                info!("shutdown signal received, draining connections...");
                break;
            }

            result = listener.accept() => {
                let (stream, peer) = result?;

                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        warn!("connection limit reached, dropping connection from {peer}");
                        if metrics_enabled {
                            crate::metrics::on_connection_rejected();
                        }
                        drop(stream);
                        continue;
                    }
                };

                if metrics_enabled {
                    crate::metrics::on_connection_accepted();
                }
                ctx.connections_accepted.fetch_add(1, Ordering::Relaxed);
                ctx.connections_active.fetch_add(1, Ordering::Relaxed);

                let engine = engine.clone();
                let ctx = Arc::clone(&ctx);
                let slow_log = Arc::clone(&slow_log);

                tokio::spawn(async move {
                    if let Err(e) = connection::handle(stream, engine, &ctx, &slow_log).await {
                        error!("connection error from {peer}: {e}");
                    }
                    ctx.connections_active.fetch_sub(1, Ordering::Relaxed);
                    if ctx.metrics_enabled {
                        crate::metrics::on_connection_closed();
                    }
                    // permit is dropped here, releasing the slot
                    drop(permit);
                });
            }
        }
    }

    // wait for all connection handlers to finish by acquiring all permits
    info!("waiting for active connections to close...");
    let _ = semaphore.acquire_many(max_conn as u32).await;
    info!("all connections drained, shutting down");

    Ok(())
}

/// Runs the server with multiple accept loops using SO_REUSEPORT.
///
/// Spawns multiple accept tasks on the tokio runtime, each with its own
/// TcpListener bound to the same port via SO_REUSEPORT. The kernel
/// distributes incoming connections across accept loops.
///
/// This reduces contention on the single accept loop while keeping
/// everything on the same tokio runtime as the shards.
pub async fn run_with_workers(
    addr: SocketAddr,
    shard_count: usize,
    config: EngineConfig,
    max_connections: Option<usize>,
    metrics_enabled: bool,
    slowlog_config: SlowLogConfig,
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
    let max_conn = max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);

    if metrics_enabled {
        crate::metrics::spawn_stats_poller(engine.clone());
    }

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
    });

    let slow_log = Arc::new(SlowLog::new(slowlog_config));
    let semaphore = Arc::new(Semaphore::new(max_conn));

    info!(
        "starting {} accept loops on {addr} with SO_REUSEPORT (max {max_conn} connections)",
        shard_count
    );

    // Spawn multiple accept loops, each with its own listener via SO_REUSEPORT
    let mut accept_handles = Vec::with_capacity(shard_count);
    for acceptor_id in 0..shard_count {
        let listener = create_reuseport_listener(addr)?;
        let engine = engine.clone();
        let ctx = Arc::clone(&ctx);
        let slow_log = Arc::clone(&slow_log);
        let semaphore = Arc::clone(&semaphore);

        let handle = tokio::spawn(async move {
            run_accept_loop(acceptor_id, listener, engine, ctx, slow_log, semaphore).await
        });
        accept_handles.push(handle);
    }

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received, stopping accept loops...");

    // Cancel all accept loops
    for handle in &accept_handles {
        handle.abort();
    }

    // Wait for connection drain
    info!("waiting for active connections to close...");
    let _ = semaphore.acquire_many(max_conn as u32).await;
    info!("all connections drained, shutting down");

    Ok(())
}

/// Creates a TcpListener with SO_REUSEPORT enabled.
fn create_reuseport_listener(addr: SocketAddr) -> Result<TcpListener, Box<dyn std::error::Error>> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;

    socket.set_reuse_port(true)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;

    Ok(TcpListener::from_std(socket.into())?)
}

/// Runs a single accept loop.
async fn run_accept_loop(
    acceptor_id: usize,
    listener: TcpListener,
    engine: Engine,
    ctx: Arc<ServerContext>,
    slow_log: Arc<SlowLog>,
    semaphore: Arc<Semaphore>,
) {
    info!(acceptor_id, "accept loop started");

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(result) => result,
            Err(e) => {
                error!(acceptor_id, "accept error: {e}");
                continue;
            }
        };

        let permit = match semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!(acceptor_id, "connection limit reached, dropping {peer}");
                if ctx.metrics_enabled {
                    crate::metrics::on_connection_rejected();
                }
                drop(stream);
                continue;
            }
        };

        if ctx.metrics_enabled {
            crate::metrics::on_connection_accepted();
        }
        ctx.connections_accepted.fetch_add(1, Ordering::Relaxed);
        ctx.connections_active.fetch_add(1, Ordering::Relaxed);

        let engine = engine.clone();
        let ctx = Arc::clone(&ctx);
        let slow_log = Arc::clone(&slow_log);

        tokio::spawn(async move {
            if let Err(e) = connection::handle(stream, engine, &ctx, &slow_log).await {
                error!("connection error from {peer}: {e}");
            }
            ctx.connections_active.fetch_sub(1, Ordering::Relaxed);
            if ctx.metrics_enabled {
                crate::metrics::on_connection_closed();
            }
            drop(permit);
        });
    }
}
