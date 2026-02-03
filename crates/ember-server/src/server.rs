//! TCP server that accepts client connections and spawns handler tasks.

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tracing::{error, info};

use crate::connection;

/// Binds to `addr` and runs the accept loop.
///
/// Each incoming connection is handed off to a spawned tokio task.
/// This is intentionally minimal — no graceful shutdown, no SO_REUSEPORT,
/// no connection limits. Those come later.
pub async fn run(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    info!("listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;

        tokio::spawn(async move {
            if let Err(e) = connection::handle(stream).await {
                error!("connection error from {peer}: {e}");
            }
        });
    }
}
