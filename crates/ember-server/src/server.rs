//! TCP server that accepts client connections and spawns handler tasks.

use std::net::SocketAddr;

use ember_core::Engine;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::connection;

/// Binds to `addr` and runs the accept loop.
///
/// Spawns a sharded engine with one shard per CPU core, then hands
/// each incoming connection a cheap clone of the engine handle.
pub async fn run(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::with_available_cores();
    let listener = TcpListener::bind(addr).await?;

    info!(
        "listening on {addr} with {} shards",
        engine.shard_count()
    );

    loop {
        let (stream, peer) = listener.accept().await?;
        let engine = engine.clone();

        tokio::spawn(async move {
            if let Err(e) = connection::handle(stream, engine).await {
                error!("connection error from {peer}: {e}");
            }
        });
    }
}
