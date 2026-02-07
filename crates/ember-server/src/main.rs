// use jemalloc as the global allocator for better multi-threaded performance.
// reduces allocation contention compared to the default system allocator.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod config;
mod connection;
mod metrics;
mod server;
mod slowlog;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use ember_core::ShardPersistenceConfig;
use tracing::info;

use crate::config::{
    build_engine_config, parse_byte_size, parse_eviction_policy, parse_fsync_policy,
};

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

    /// directory for AOF and snapshot files. required if --appendonly is set
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// enable append-only file logging for durability
    #[arg(long)]
    appendonly: bool,

    /// fsync policy for the AOF: always, everysec, or no
    #[arg(long, default_value = "everysec")]
    appendfsync: String,

    /// port for prometheus metrics HTTP endpoint. disabled when not set
    #[arg(long)]
    metrics_port: Option<u16>,

    /// log commands slower than this many microseconds. default: 10000 (10ms).
    /// set to -1 to disable, 0 to log every command
    #[arg(long, default_value_t = 10_000)]
    slowlog_log_slower_than: i64,

    /// maximum number of entries in the slow log ring buffer
    #[arg(long, default_value_t = 128)]
    slowlog_max_len: usize,

    /// number of shards (worker threads). defaults to available CPU cores
    #[arg(long)]
    shards: Option<usize>,

    /// use worker-per-core architecture with SO_REUSEPORT for better scaling.
    /// each worker gets its own accept loop and tokio runtime.
    #[arg(long)]
    workers: bool,
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

    let shard_count = args.shards.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });

    if shard_count == 0 {
        eprintln!("--shards must be at least 1");
        std::process::exit(1);
    }

    // build persistence config if data-dir is set or appendonly is enabled
    let persistence = if args.appendonly || args.data_dir.is_some() {
        let data_dir = args.data_dir.unwrap_or_else(|| {
            if args.appendonly {
                eprintln!("--data-dir is required when --appendonly is set");
                std::process::exit(1);
            }
            PathBuf::from(".")
        });

        let fsync_policy = parse_fsync_policy(&args.appendfsync).unwrap_or_else(|e| {
            eprintln!("invalid --appendfsync value: {e}");
            std::process::exit(1);
        });

        Some(ShardPersistenceConfig {
            data_dir,
            append_only: args.appendonly,
            fsync_policy,
        })
    } else {
        None
    };

    let engine_config = build_engine_config(max_memory, eviction_policy, shard_count, persistence);

    if let Some(limit) = max_memory {
        info!(
            "memory limit: {} bytes ({} per shard)",
            limit,
            limit / shard_count
        );
    }

    if let Some(ref p) = engine_config.persistence {
        info!(
            data_dir = %p.data_dir.display(),
            appendonly = p.append_only,
            fsync = ?p.fsync_policy,
            "persistence enabled"
        );
    }

    // install prometheus metrics exporter if --metrics-port is set
    if let Some(metrics_port) = args.metrics_port {
        let metrics_addr: std::net::SocketAddr = format!("{}:{}", args.host, metrics_port)
            .parse()
            .expect("invalid metrics bind address");
        if let Err(e) = metrics::install_exporter(metrics_addr) {
            eprintln!("failed to start metrics exporter: {e}");
            std::process::exit(1);
        }
    }

    let slowlog_config = slowlog::SlowLogConfig {
        slower_than: std::time::Duration::from_micros(args.slowlog_log_slower_than.max(0) as u64),
        max_len: args.slowlog_max_len,
        enabled: args.slowlog_log_slower_than >= 0,
    };

    info!(shards = shard_count, workers = args.workers, "ember server starting...");

    let result = if args.workers {
        // Worker mode: multiple accept loops with SO_REUSEPORT.
        server::run_with_workers(
            addr,
            shard_count,
            engine_config,
            None,
            args.metrics_port.is_some(),
            slowlog_config,
        )
        .await
    } else {
        // Default mode: single accept loop in tokio multi-threaded runtime.
        server::run(
            addr,
            shard_count,
            engine_config,
            None,
            args.metrics_port.is_some(),
            slowlog_config,
        )
        .await
    };

    if let Err(e) = result {
        eprintln!("server error: {e}");
        std::process::exit(1);
    }
}
