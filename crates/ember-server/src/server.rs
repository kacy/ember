//! TCP server that accepts client connections and spawns handler tasks.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use ember_core::Keyspace;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::connection;

/// Binds to `addr` and runs the accept loop.
///
/// Each incoming connection is handed off to a spawned tokio task.
/// All connections share a single keyspace behind an `Arc<Mutex>`.
/// We use `std::sync::Mutex` (not tokio's) because the lock is only
/// held during fast, synchronous HashMap operations — never across
/// an `.await` point.
pub async fn run(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let keyspace = Arc::new(Mutex::new(Keyspace::new()));
    let listener = TcpListener::bind(addr).await?;

    info!("listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        let ks = Arc::clone(&keyspace);

        tokio::spawn(async move {
            if let Err(e) = connection::handle(stream, ks).await {
                error!("connection error from {peer}: {e}");
            }
        });
    }
}
