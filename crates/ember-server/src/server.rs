//! TCP server that accepts client connections and spawns handler tasks.
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
    pub connections_accepted: AtomicU64,
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
    let max_memory = config.shard.max_memory.map(|per_shard| per_shard * shard_count);

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
        connections_accepted: AtomicU64::new(0),
        commands_processed: AtomicU64::new(0),
    });

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

                let engine = engine.clone();
                let ctx = Arc::clone(&ctx);
                let metrics = metrics_enabled;

                tokio::spawn(async move {
                    if let Err(e) = connection::handle(stream, engine, &ctx, metrics).await {
                        error!("connection error from {peer}: {e}");
                    }
                    if metrics {
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
