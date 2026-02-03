//! TCP server that accepts client connections and spawns handler tasks.

use std::net::SocketAddr;
use std::sync::Arc;

use ember_core::{Engine, EngineConfig};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use crate::connection;

/// Default maximum number of concurrent client connections.
const DEFAULT_MAX_CONNECTIONS: usize = 10_000;

/// Binds to `addr` and runs the accept loop.
///
/// Spawns a sharded engine with the given shard count and config, then
/// hands each incoming connection a cheap clone of the engine handle.
/// Limits concurrent connections to `max_connections` — excess clients
/// are dropped immediately.
pub async fn run(
    addr: SocketAddr,
    shard_count: usize,
    config: EngineConfig,
    max_connections: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    // ensure data directory exists if persistence is configured
    if let Some(ref pcfg) = config.persistence {
        std::fs::create_dir_all(&pcfg.data_dir)?;
    }

    let engine = Engine::with_config(shard_count, config);
    let listener = TcpListener::bind(addr).await?;
    let max_conn = max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS);
    let semaphore = Arc::new(Semaphore::new(max_conn));

    info!(
        "listening on {addr} with {} shards (max {max_conn} connections)",
        engine.shard_count()
    );

    loop {
        let (stream, peer) = listener.accept().await?;

        let permit = match semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!("connection limit reached, dropping connection from {peer}");
                drop(stream);
                continue;
            }
        };

        let engine = engine.clone();

        tokio::spawn(async move {
            if let Err(e) = connection::handle(stream, engine).await {
                error!("connection error from {peer}: {e}");
            }
            // permit is dropped here, releasing the slot
            drop(permit);
        });
    }
}
