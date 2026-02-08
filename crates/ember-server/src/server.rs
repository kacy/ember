//! TCP server that accepts client connections and spawns handler tasks.
//!
//! Handles graceful shutdown on SIGINT/SIGTERM: stops accepting new
//! connections and waits for in-flight requests to drain before exiting.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use ember_core::{ConcurrentKeyspace, Engine, EngineConfig, EvictionPolicy};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use crate::connection;
use crate::pubsub::PubSubManager;
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
    let pubsub = Arc::new(PubSubManager::new());

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
                let pubsub = Arc::clone(&pubsub);

                tokio::spawn(async move {
                    if let Err(e) = connection::handle(stream, engine, &ctx, &slow_log, &pubsub).await {
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

/// Runs the server with a concurrent keyspace (DashMap-backed).
///
/// This mode bypasses shard channels for GET/SET operations, accessing
/// the keyspace directly from connection handlers. Falls back to the
/// sharded engine for complex commands.
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
    let pubsub = Arc::new(PubSubManager::new());

    info!("listening on {addr} with concurrent keyspace (max {max_conn} connections)");

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
                    ctx.connections_active.fetch_sub(1, Ordering::Relaxed);
                    if ctx.metrics_enabled {
                        crate::metrics::on_connection_closed();
                    }
                    drop(permit);
                });
            }
        }
    }

    info!("waiting for active connections to close...");
    let _ = semaphore.acquire_many(max_conn as u32).await;
    info!("all connections drained, shutting down");

    Ok(())
}
