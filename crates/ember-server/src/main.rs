mod config;
mod connection;
mod server;

use std::net::SocketAddr;

use clap::Parser;
use tracing::info;

use crate::config::{build_engine_config, parse_byte_size, parse_eviction_policy};

#[derive(Parser)]
#[command(name = "ember-server", about = "ember cache server")]
struct Args {
    /// address to bind to
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// port to listen on
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// maximum memory limit (e.g. "100M", "1G", "512K"). default: unlimited
    #[arg(long)]
    max_memory: Option<String>,

    /// eviction policy when memory limit is reached: noeviction or allkeys-lru
    #[arg(long, default_value = "noeviction")]
    eviction_policy: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ember=info".into()),
        )
        .init();

    let args = Args::parse();

    let addr: SocketAddr = format!("{}:{}", args.host, args.port)
        .parse()
        .expect("invalid bind address");

    let max_memory = args.max_memory.as_deref().map(|s| {
        parse_byte_size(s).unwrap_or_else(|e| {
            eprintln!("invalid --max-memory value: {e}");
            std::process::exit(1);
        })
    });

    let eviction_policy = parse_eviction_policy(&args.eviction_policy).unwrap_or_else(|e| {
        eprintln!("invalid --eviction-policy value: {e}");
        std::process::exit(1);
    });

    let shard_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let engine_config = build_engine_config(max_memory, eviction_policy, shard_count);

    if let Some(limit) = max_memory {
        info!(
            "memory limit: {} bytes ({} per shard)",
            limit,
            limit / shard_count
        );
    }

    info!("ember server starting...");

    if let Err(e) = server::run(addr, shard_count, engine_config, None).await {
        eprintln!("server error: {e}");
        std::process::exit(1);
    }
}
