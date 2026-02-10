// use jemalloc as the global allocator for better multi-threaded performance.
// reduces allocation contention compared to the default system allocator.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod cluster;
mod concurrent_handler;
mod config;
mod connection;
mod connection_common;
mod metrics;
mod pubsub;
mod server;
mod slowlog;
mod tls;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use ember_cluster::{GossipConfig, NodeId};
use ember_core::ShardPersistenceConfig;
use tracing::info;

use crate::cluster::ClusterCoordinator;
use crate::config::{
    build_engine_config, parse_byte_size, parse_eviction_policy, parse_fsync_policy,
};

#[derive(Parser)]
#[command(name = "ember-server", about = "ember cache server")]
struct Args {
    /// address to bind to
    #[arg(long, default_value = "127.0.0.1", env = "EMBER_HOST")]
    host: String,

    /// port to listen on
    #[arg(short, long, default_value_t = 6379, env = "EMBER_PORT")]
    port: u16,

    /// maximum memory limit (e.g. "100M", "1G", "512K"). default: unlimited
    #[arg(long, env = "EMBER_MAX_MEMORY")]
    max_memory: Option<String>,

    /// eviction policy when memory limit is reached: noeviction or allkeys-lru
    #[arg(long, default_value = "noeviction", env = "EMBER_EVICTION_POLICY")]
    eviction_policy: String,

    /// directory for AOF and snapshot files. required if --appendonly is set
    #[arg(long, env = "EMBER_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// enable append-only file logging for durability
    #[arg(long, env = "EMBER_APPENDONLY")]
    appendonly: bool,

    /// fsync policy for the AOF: always, everysec, or no
    #[arg(long, default_value = "everysec", env = "EMBER_APPENDFSYNC")]
    appendfsync: String,

    /// port for prometheus metrics HTTP endpoint. disabled when not set
    #[arg(long, env = "EMBER_METRICS_PORT")]
    metrics_port: Option<u16>,

    /// log commands slower than this many microseconds. default: 10000 (10ms).
    /// set to -1 to disable, 0 to log every command
    #[arg(long, default_value_t = 10_000, env = "EMBER_SLOWLOG_LOG_SLOWER_THAN")]
    slowlog_log_slower_than: i64,

    /// maximum number of entries in the slow log ring buffer
    #[arg(long, default_value_t = 128, env = "EMBER_SLOWLOG_MAX_LEN")]
    slowlog_max_len: usize,

    /// number of shards (worker threads). defaults to available CPU cores
    #[arg(long, env = "EMBER_SHARDS")]
    shards: Option<usize>,

    /// use concurrent keyspace (DashMap) instead of sharded channels.
    /// experimental: bypasses channel overhead for GET/SET commands.
    #[arg(long, env = "EMBER_CONCURRENT")]
    concurrent: bool,

    /// require clients to AUTH with this password before running commands.
    /// when set, connections must authenticate before executing any data commands.
    #[arg(long, env = "EMBER_REQUIREPASS")]
    requirepass: Option<String>,

    /// path to a file containing the password (alternative to --requirepass).
    /// avoids exposing the password in /proc/cmdline. the file contents are
    /// trimmed of trailing whitespace.
    #[arg(long, env = "EMBER_REQUIREPASS_FILE")]
    requirepass_file: Option<PathBuf>,

    // -- TLS options (matching redis) --
    /// port for TLS connections. when set, enables TLS alongside plain TCP
    #[arg(long, env = "EMBER_TLS_PORT")]
    tls_port: Option<u16>,

    /// path to server certificate file (PEM format)
    #[arg(long, env = "EMBER_TLS_CERT_FILE")]
    tls_cert_file: Option<String>,

    /// path to server private key file (PEM format)
    #[arg(long, env = "EMBER_TLS_KEY_FILE")]
    tls_key_file: Option<String>,

    /// path to CA certificate for client verification (enables mTLS)
    #[arg(long, env = "EMBER_TLS_CA_CERT_FILE")]
    tls_ca_cert_file: Option<String>,

    /// require client certificates when CA cert is configured.
    /// accepts: yes, no. default: no
    #[arg(long, default_value = "no", env = "EMBER_TLS_AUTH_CLIENTS")]
    tls_auth_clients: String,

    // -- encryption at rest --
    /// path to a 32-byte key file for encrypting AOF and snapshot files.
    /// accepts 32 raw bytes or 64 hex characters. when set, new persistence
    /// files use AES-256-GCM encryption. existing plaintext files are read
    /// normally and migrated on the next BGREWRITEAOF/BGSAVE.
    #[cfg(feature = "encryption")]
    #[arg(long, env = "EMBER_ENCRYPTION_KEY_FILE")]
    encryption_key_file: Option<PathBuf>,

    // -- protobuf support --
    /// enable protobuf value storage. when set, PROTO.* commands become
    /// available for schema-validated structured data.
    #[cfg(feature = "protobuf")]
    #[arg(long, env = "EMBER_PROTOBUF")]
    protobuf: bool,

    // -- cluster options --
    /// enable cluster mode with gossip-based discovery and slot routing
    #[arg(long, env = "EMBER_CLUSTER_ENABLED")]
    cluster_enabled: bool,

    /// bootstrap a new cluster as a single node owning all 16384 slots
    #[arg(long, env = "EMBER_CLUSTER_BOOTSTRAP")]
    cluster_bootstrap: bool,

    /// port offset for the cluster gossip bus (data_port + offset)
    #[arg(long, default_value_t = 10000, env = "EMBER_CLUSTER_PORT_OFFSET")]
    cluster_port_offset: u16,

    /// node timeout in milliseconds for failure detection
    #[arg(long, default_value_t = 5000, env = "EMBER_CLUSTER_NODE_TIMEOUT")]
    cluster_node_timeout: u64,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ember=info".into()),
        )
        .init();

    let mut args = Args::parse();

    // resolve password: --requirepass-file takes the same role as --requirepass
    if args.requirepass.is_some() && args.requirepass_file.is_some() {
        eprintln!("error: --requirepass and --requirepass-file are mutually exclusive");
        std::process::exit(1);
    }
    if let Some(ref path) = args.requirepass_file {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let password = contents.trim_end().to_string();
                if password.is_empty() {
                    eprintln!("error: --requirepass-file is empty: {}", path.display());
                    std::process::exit(1);
                }
                args.requirepass = Some(password);
            }
            Err(e) => {
                eprintln!(
                    "error: failed to read --requirepass-file '{}': {e}",
                    path.display()
                );
                std::process::exit(1);
            }
        }
    }

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

    // load encryption key if configured
    #[cfg(feature = "encryption")]
    let encryption_key = if let Some(ref key_path) = args.encryption_key_file {
        match ember_persistence::encryption::EncryptionKey::from_file(key_path) {
            Ok(key) => Some(key),
            Err(e) => {
                eprintln!("failed to load encryption key: {e}");
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    #[cfg(feature = "encryption")]
    if encryption_key.is_some() && !args.appendonly && args.data_dir.is_none() {
        eprintln!("--encryption-key-file requires --data-dir and --appendonly");
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
            #[cfg(feature = "encryption")]
            encryption_key,
        })
    } else {
        None
    };

    #[allow(unused_mut)]
    let mut engine_config =
        build_engine_config(max_memory, eviction_policy, shard_count, persistence);

    #[cfg(feature = "protobuf")]
    if args.protobuf {
        engine_config.schema_registry = Some(ember_core::schema::SchemaRegistry::shared());
        info!("protobuf value storage enabled");
    }

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

        #[cfg(feature = "encryption")]
        if p.encryption_key.is_some() {
            info!("encryption at rest enabled (AES-256-GCM)");
        }
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

    info!(
        shards = shard_count,
        concurrent = args.concurrent,
        "ember server starting..."
    );

    if args.requirepass.is_some() {
        info!("authentication enabled (requirepass set)");
    }

    // build TLS config if --tls-port is set
    let tls_config = if let Some(tls_port) = args.tls_port {
        let cert_file = args.tls_cert_file.unwrap_or_else(|| {
            eprintln!("--tls-port requires --tls-cert-file and --tls-key-file");
            std::process::exit(1);
        });
        let key_file = args.tls_key_file.unwrap_or_else(|| {
            eprintln!("--tls-port requires --tls-cert-file and --tls-key-file");
            std::process::exit(1);
        });

        let auth_clients = match args.tls_auth_clients.to_lowercase().as_str() {
            "yes" | "true" | "1" => true,
            "no" | "false" | "0" => false,
            _ => {
                eprintln!("--tls-auth-clients must be 'yes' or 'no'");
                std::process::exit(1);
            }
        };

        let tls_addr: SocketAddr = format!("{}:{}", args.host, tls_port)
            .parse()
            .expect("invalid TLS bind address");

        info!(
            tls_port = tls_port,
            cert = %cert_file,
            "TLS enabled"
        );

        Some((
            tls_addr,
            tls::TlsConfig {
                cert_file,
                key_file,
                ca_cert_file: args.tls_ca_cert_file,
                auth_clients,
            },
        ))
    } else {
        None
    };

    // validate cluster mode
    if args.cluster_enabled && args.concurrent {
        eprintln!("error: --cluster-enabled and --concurrent are mutually exclusive");
        std::process::exit(1);
    }

    if args.cluster_bootstrap && !args.cluster_enabled {
        eprintln!("error: --cluster-bootstrap requires --cluster-enabled");
        std::process::exit(1);
    }

    // build cluster coordinator if cluster mode is enabled
    let cluster: Option<Arc<ClusterCoordinator>> = if args.cluster_enabled {
        let local_id = NodeId::new();
        let gossip_config = GossipConfig {
            gossip_port_offset: args.cluster_port_offset,
            probe_timeout: std::time::Duration::from_millis(args.cluster_node_timeout / 2),
            ..GossipConfig::default()
        };

        let (coordinator, event_rx) =
            ClusterCoordinator::new(local_id, addr, gossip_config, args.cluster_bootstrap);
        let coordinator = Arc::new(coordinator);

        // spawn gossip networking tasks
        coordinator.spawn_gossip(addr, event_rx).await;

        if args.cluster_bootstrap {
            info!("cluster mode: bootstrapped with all 16384 slots");
        } else {
            info!("cluster mode: waiting for CLUSTER MEET");
        }

        Some(coordinator)
    } else {
        None
    };

    let result = if args.concurrent {
        server::run_concurrent(
            addr,
            shard_count,
            engine_config,
            max_memory,
            eviction_policy,
            None,
            args.metrics_port.is_some(),
            slowlog_config,
            args.requirepass,
            tls_config,
        )
        .await
    } else {
        server::run(
            addr,
            shard_count,
            engine_config,
            None,
            args.metrics_port.is_some(),
            slowlog_config,
            args.requirepass,
            tls_config,
            cluster,
        )
        .await
    };

    if let Err(e) = result {
        eprintln!("server error: {e}");
        std::process::exit(1);
    }
}
