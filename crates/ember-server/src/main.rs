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
#[cfg(feature = "grpc")]
mod grpc;
mod metrics;
mod pubsub;
mod replication;
mod server;
mod slowlog;
mod tls;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use ember_cluster::{raft_id_from_node_id, GossipConfig, NodeId, RaftNode, RaftStorage};
use ember_core::{ReplicationEvent, ShardPersistenceConfig};
use tracing::info;
#[cfg(feature = "protobuf")]
use tracing::warn;

use crate::cluster::ClusterCoordinator;
use crate::config::{
    build_engine_config, parse_byte_size, parse_eviction_policy, parse_fsync_policy,
    ConfigRegistry,
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

    // -- gRPC options --
    /// port for gRPC connections. set to 0 to disable even when compiled with grpc.
    #[cfg(feature = "grpc")]
    #[arg(long, default_value_t = 6380, env = "EMBER_GRPC_PORT")]
    grpc_port: u16,

    /// disable gRPC listener even when compiled with the grpc feature
    #[cfg(feature = "grpc")]
    #[arg(long, env = "EMBER_NO_GRPC")]
    no_grpc: bool,

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

    /// port offset for the Raft TCP listener (data_port + offset).
    /// defaults to cluster_port_offset + 1 (e.g. 10001)
    #[arg(long, env = "EMBER_CLUSTER_RAFT_PORT_OFFSET")]
    cluster_raft_port_offset: Option<u16>,

    /// node timeout in milliseconds for failure detection
    #[arg(long, default_value_t = 5000, env = "EMBER_CLUSTER_NODE_TIMEOUT")]
    cluster_node_timeout: u64,

    /// shared secret for authenticating cluster transport (gossip + raft).
    /// when set, unauthenticated messages are silently dropped.
    #[arg(long, env = "EMBER_CLUSTER_AUTH_PASS")]
    cluster_auth_pass: Option<String>,

    /// path to a file containing the cluster auth password (alternative to
    /// --cluster-auth-pass). the file contents are trimmed of trailing whitespace.
    #[arg(long, env = "EMBER_CLUSTER_AUTH_PASS_FILE")]
    cluster_auth_pass_file: Option<std::path::PathBuf>,
}

/// Prints `msg` to stderr and exits with code 1.
///
/// Used throughout `main` to normalize the error-and-exit pattern for
/// argument validation failures. The `!` return type lets the compiler
/// verify that call sites don't need to produce a value after the call.
fn exit_err(msg: impl std::fmt::Display) -> ! {
    eprintln!("{msg}");
    std::process::exit(1);
}

/// Resolves the password from either `--requirepass` or `--requirepass-file`.
/// The two options are mutually exclusive. Exits on error.
fn resolve_password(args: &mut Args) {
    if args.requirepass.is_some() && args.requirepass_file.is_some() {
        exit_err("error: --requirepass and --requirepass-file are mutually exclusive");
    }
    if let Some(ref path) = args.requirepass_file {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let password = contents.trim_end().to_string();
                if password.is_empty() {
                    exit_err(format!(
                        "error: --requirepass-file is empty: {}",
                        path.display()
                    ));
                }
                args.requirepass = Some(password);
            }
            Err(e) => {
                exit_err(format!(
                    "error: failed to read --requirepass-file '{}': {e}",
                    path.display()
                ));
            }
        }
    }
}

/// Resolves the cluster auth secret from either `--cluster-auth-pass` or
/// `--cluster-auth-pass-file`. The two options are mutually exclusive.
fn resolve_cluster_secret(args: &Args) -> Option<std::sync::Arc<ember_cluster::ClusterSecret>> {
    if args.cluster_auth_pass.is_some() && args.cluster_auth_pass_file.is_some() {
        exit_err("error: --cluster-auth-pass and --cluster-auth-pass-file are mutually exclusive");
    }
    if let Some(ref password) = args.cluster_auth_pass {
        return Some(std::sync::Arc::new(
            ember_cluster::ClusterSecret::from_password(password),
        ));
    }
    if let Some(ref path) = args.cluster_auth_pass_file {
        match ember_cluster::ClusterSecret::from_file(path) {
            Ok(secret) => return Some(std::sync::Arc::new(secret)),
            Err(e) => exit_err(format!(
                "error: failed to read --cluster-auth-pass-file '{}': {e}",
                path.display()
            )),
        }
    }
    None
}

/// Parses a `host:port` pair into a `SocketAddr`. Exits with a message on failure.
fn parse_bind_addr(host: &str, port: u16, label: &str) -> SocketAddr {
    match format!("{host}:{port}").parse() {
        Ok(a) => a,
        Err(e) => {
            if label.is_empty() {
                exit_err(format!("invalid bind address '{host}:{port}': {e}"));
            } else {
                exit_err(format!("invalid {label} bind address '{host}:{port}': {e}"));
            }
        }
    }
}

/// Builds the persistence config from CLI args. Returns `None` if persistence
/// is not enabled. Exits on validation errors.
fn build_persistence_config(
    args: &mut Args,
    #[cfg(feature = "encryption")] encryption_key: Option<
        ember_persistence::encryption::EncryptionKey,
    >,
) -> Option<ShardPersistenceConfig> {
    if !args.appendonly && args.data_dir.is_none() {
        return None;
    }

    let data_dir = args.data_dir.take().unwrap_or_else(|| {
        if args.appendonly {
            exit_err("--data-dir is required when --appendonly is set");
        }
        PathBuf::from(".")
    });

    let fsync_policy = parse_fsync_policy(&args.appendfsync)
        .unwrap_or_else(|e| exit_err(format!("invalid --appendfsync value: {e}")));

    Some(ShardPersistenceConfig {
        data_dir,
        append_only: args.appendonly,
        fsync_policy,
        #[cfg(feature = "encryption")]
        encryption_key,
    })
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

    resolve_password(&mut args);
    let cluster_secret = resolve_cluster_secret(&args);

    let addr = parse_bind_addr(&args.host, args.port, "");

    let max_memory = args.max_memory.as_deref().map(|s| {
        parse_byte_size(s).unwrap_or_else(|e| exit_err(format!("invalid --max-memory value: {e}")))
    });

    let eviction_policy = parse_eviction_policy(&args.eviction_policy)
        .unwrap_or_else(|e| exit_err(format!("invalid --eviction-policy value: {e}")));

    let shard_count = args.shards.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });

    if shard_count == 0 {
        exit_err("--shards must be at least 1");
    }

    // load encryption key if configured
    #[cfg(feature = "encryption")]
    let encryption_key = if let Some(ref key_path) = args.encryption_key_file {
        match ember_persistence::encryption::EncryptionKey::from_file(key_path) {
            Ok(key) => Some(key),
            Err(e) => exit_err(format!("failed to load encryption key: {e}")),
        }
    } else {
        None
    };

    #[cfg(feature = "encryption")]
    if encryption_key.is_some() && !args.appendonly && args.data_dir.is_none() {
        exit_err("--encryption-key-file requires --data-dir and --appendonly");
    }

    let persistence = build_persistence_config(
        &mut args,
        #[cfg(feature = "encryption")]
        encryption_key,
    );

    #[allow(unused_mut)]
    let mut engine_config =
        build_engine_config(max_memory, eviction_policy, shard_count, persistence);

    #[cfg(feature = "protobuf")]
    if args.protobuf {
        engine_config.schema_registry = Some(ember_core::schema::SchemaRegistry::shared());
        info!("protobuf value storage enabled");

        if args.concurrent {
            warn!(
                "concurrent mode with protobuf: DEL, TTL, EXPIRE, and other generic \
                 commands do not affect proto keys. use sharded mode for full proto support."
            );
        }
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
        let metrics_addr = parse_bind_addr(&args.host, metrics_port, "metrics");
        if let Err(e) = metrics::install_exporter(metrics_addr) {
            exit_err(format!("failed to start metrics exporter: {e}"));
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
    if cluster_secret.is_some() {
        info!("cluster transport authentication enabled");
    }

    // build TLS config if --tls-port is set
    let tls_config = if let Some(tls_port) = args.tls_port {
        let cert_file = args
            .tls_cert_file
            .unwrap_or_else(|| exit_err("--tls-port requires --tls-cert-file and --tls-key-file"));
        let key_file = args
            .tls_key_file
            .unwrap_or_else(|| exit_err("--tls-port requires --tls-cert-file and --tls-key-file"));

        let auth_clients = match args.tls_auth_clients.to_lowercase().as_str() {
            "yes" | "true" | "1" => true,
            "no" | "false" | "0" => false,
            _ => exit_err("--tls-auth-clients must be 'yes' or 'no'"),
        };

        let tls_addr = parse_bind_addr(&args.host, tls_port, "TLS");

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
        exit_err("error: --cluster-enabled and --concurrent are mutually exclusive");
    }

    if args.cluster_bootstrap && !args.cluster_enabled {
        exit_err("error: --cluster-bootstrap requires --cluster-enabled");
    }

    // set up the replication broadcast channel when cluster is enabled.
    // the sender goes into engine_config so shards can publish mutations;
    // replicas subscribe to it when they connect for full sync + streaming.
    if args.cluster_enabled {
        let (repl_tx, _) = tokio::sync::broadcast::channel::<ReplicationEvent>(65536);
        engine_config.replication_tx = Some(repl_tx);
    }

    // build cluster coordinator if cluster mode is enabled
    let cluster: Option<Arc<ClusterCoordinator>> = if args.cluster_enabled {
        if args.port.checked_add(args.cluster_port_offset).is_none() {
            exit_err(format!(
                "error: port {} + cluster-port-offset {} exceeds u16 range",
                args.port, args.cluster_port_offset
            ));
        }

        let raft_port_offset = args
            .cluster_raft_port_offset
            .unwrap_or_else(|| args.cluster_port_offset.saturating_add(1));

        // cluster requires a data directory for nodes.conf persistence.
        // persistence may have already consumed args.data_dir via .take(),
        // so we read from the engine config's copy instead.
        let cluster_data_dir = engine_config
            .persistence
            .as_ref()
            .map(|p| p.data_dir.clone())
            .unwrap_or_else(|| {
                exit_err("error: --data-dir is required when --cluster-enabled is set")
            });

        let gossip_config = GossipConfig {
            gossip_port_offset: args.cluster_port_offset,
            probe_timeout: std::time::Duration::from_millis(args.cluster_node_timeout / 2),
            ..GossipConfig::default()
        };

        let conf_path = cluster_data_dir.join("nodes.conf");

        let (coordinator, event_rx, local_id, is_bootstrap) = if conf_path.exists() {
            // restore from saved config
            let data = std::fs::read_to_string(&conf_path)
                .unwrap_or_else(|e| exit_err(format!("failed to read nodes.conf: {e}")));

            let (coord, rx) = ClusterCoordinator::from_config(
                &data,
                addr,
                gossip_config,
                cluster_data_dir.clone(),
                cluster_secret.clone(),
            )
            .unwrap_or_else(|e| exit_err(format!("failed to parse nodes.conf: {e}")));

            info!("cluster mode: restored from nodes.conf");
            let id = coord.local_id();
            (coord, rx, id, false)
        } else if args.cluster_bootstrap {
            let local_id = NodeId::new();
            let (coord, rx) = ClusterCoordinator::new(
                local_id,
                addr,
                gossip_config,
                true,
                Some(cluster_data_dir.clone()),
                cluster_secret.clone(),
            )
            .unwrap_or_else(|e| exit_err(format!("error: {e}")));
            info!("cluster mode: bootstrapped with all 16384 slots");
            (coord, rx, local_id, true)
        } else {
            let local_id = NodeId::new();
            let (coord, rx) = ClusterCoordinator::new(
                local_id,
                addr,
                gossip_config,
                false,
                Some(cluster_data_dir.clone()),
                cluster_secret.clone(),
            )
            .unwrap_or_else(|e| exit_err(format!("error: {e}")));
            info!("cluster mode: waiting for CLUSTER MEET");
            (coord, rx, local_id, false)
        };

        let coordinator = Arc::new(coordinator);

        // start the Raft node and attach it to the coordinator
        let raft_port = addr
            .port()
            .checked_add(raft_port_offset)
            .unwrap_or_else(|| exit_err("error: raft port offset overflows u16"));
        let raft_addr = std::net::SocketAddr::new(addr.ip(), raft_port);
        let raft_id = raft_id_from_node_id(local_id);

        let (storage, state_rx) = RaftStorage::new();
        let raft_node = match RaftNode::start(
            raft_id,
            raft_addr,
            storage.clone(),
            coordinator.cluster_secret(),
        )
        .await
        {
            Ok(node) => Arc::new(node),
            Err(e) => exit_err(format!("failed to start raft node: {e}")),
        };

        if is_bootstrap {
            // first boot of a brand-new cluster: initialize single-member raft
            // and add this node to the application state machine
            if let Err(e) = raft_node.bootstrap_single().await {
                exit_err(format!("failed to bootstrap raft: {e}"));
            }
            // give raft a moment to elect itself leader before proposing
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;

            if let Err(e) = raft_node
                .propose(ember_cluster::ClusterCommand::AddNode {
                    node_id: local_id,
                    raft_id,
                    addr: addr.to_string(),
                    is_primary: true,
                })
                .await
            {
                // non-fatal: the node will be added via reconciliation later
                tracing::warn!("bootstrap AddNode proposal failed: {e}");
            }
        }

        coordinator.attach_raft(Arc::clone(&raft_node));
        coordinator.spawn_raft_reconciliation(state_rx);

        // spawn gossip networking tasks
        coordinator.spawn_gossip(addr, event_rx).await;

        // save initial config (for new clusters)
        if !conf_path.exists() {
            coordinator.save_config().await;
        }

        Some(coordinator)
    } else {
        None
    };

    // build grpc address if enabled
    #[cfg(feature = "grpc")]
    let grpc_addr = if !args.no_grpc && args.grpc_port != 0 {
        let addr = parse_bind_addr(&args.host, args.grpc_port, "gRPC");
        info!(grpc_port = args.grpc_port, "gRPC enabled");
        Some(addr)
    } else {
        None
    };

    // build runtime config registry from parsed args
    let config_registry = {
        let mut params = std::collections::HashMap::new();
        params.insert("port".into(), args.port.to_string());
        params.insert("bind-address".into(), args.host.clone());
        if let Some(mem) = max_memory {
            params.insert("maxmemory".into(), mem.to_string());
        } else {
            params.insert("maxmemory".into(), "0".into());
        }
        params.insert("maxmemory-policy".into(), args.eviction_policy.clone());
        params.insert("appendonly".into(), if args.appendonly { "yes" } else { "no" }.into());
        params.insert("appendfsync".into(), args.appendfsync.clone());
        params.insert(
            "slowlog-log-slower-than".into(),
            args.slowlog_log_slower_than.to_string(),
        );
        params.insert("slowlog-max-len".into(), args.slowlog_max_len.to_string());
        params.insert(
            "maxclients".into(),
            "10000".into(), // DEFAULT_MAX_CONNECTIONS
        );
        Arc::new(ConfigRegistry::new(params))
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
            Arc::clone(&config_registry),
            #[cfg(feature = "grpc")]
            grpc_addr,
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
            Arc::clone(&config_registry),
            #[cfg(feature = "grpc")]
            grpc_addr,
        )
        .await
    };

    if let Err(e) = result {
        eprintln!("server error: {e}");
        std::process::exit(1);
    }
}
