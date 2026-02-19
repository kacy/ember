//! Server configuration parsing and runtime config registry.
//!
//! Handles conversion from CLI-friendly strings (like "100M", "1G")
//! to the internal config types used by the engine. Also provides a
//! `ConfigRegistry` for CONFIG GET/SET support at runtime.
//!
//! Configuration resolution order (highest priority wins):
//!   defaults → TOML file → env vars → CLI flags

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::Duration;

use ember_core::{EngineConfig, EvictionPolicy, ShardConfig, ShardPersistenceConfig};
use ember_persistence::aof::FsyncPolicy;
use serde::{Deserialize, Serialize};

/// Parses a human-readable byte size string into a number of bytes.
///
/// Supports suffixes: K/KB (kibibytes), M/MB (mebibytes), G/GB (gibibytes).
/// Plain numbers are treated as bytes. Case insensitive.
///
/// # Examples
///
/// - "1024" → 1024
/// - "100K" → 102400
/// - "50M" → 52428800
/// - "2G" → 2147483648
pub fn parse_byte_size(input: &str) -> Result<usize, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("empty byte size string".into());
    }

    let upper = input.to_ascii_uppercase();

    let (num_str, multiplier) = if let Some(n) = upper.strip_suffix("GB") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = upper.strip_suffix("MB") {
        (n, 1024 * 1024)
    } else if let Some(n) = upper.strip_suffix("KB") {
        (n, 1024)
    } else if let Some(n) = upper.strip_suffix('G') {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = upper.strip_suffix('M') {
        (n, 1024 * 1024)
    } else if let Some(n) = upper.strip_suffix('K') {
        (n, 1024)
    } else {
        (upper.as_str(), 1)
    };

    let num: usize = num_str
        .parse()
        .map_err(|_| format!("invalid byte size: '{input}'"))?;

    num.checked_mul(multiplier)
        .ok_or_else(|| format!("byte size overflow: '{input}'"))
}

/// Parses an eviction policy name from a CLI string.
pub fn parse_eviction_policy(input: &str) -> Result<EvictionPolicy, String> {
    match input.to_ascii_lowercase().as_str() {
        "noeviction" => Ok(EvictionPolicy::NoEviction),
        "allkeys-lru" => Ok(EvictionPolicy::AllKeysLru),
        _ => Err(format!(
            "unknown eviction policy '{input}'. valid options: noeviction, allkeys-lru"
        )),
    }
}

/// Parses an fsync policy name from a CLI string.
pub fn parse_fsync_policy(input: &str) -> Result<FsyncPolicy, String> {
    match input.to_ascii_lowercase().as_str() {
        "always" => Ok(FsyncPolicy::Always),
        "everysec" => Ok(FsyncPolicy::EverySec),
        "no" => Ok(FsyncPolicy::No),
        _ => Err(format!(
            "unknown fsync policy '{input}'. valid options: always, everysec, no"
        )),
    }
}

/// Builds an `EngineConfig` from parsed CLI options.
///
/// `max_memory` is the total server limit — it gets divided evenly
/// across shards so each shard enforces its own share.
pub fn build_engine_config(
    max_memory: Option<usize>,
    eviction_policy: EvictionPolicy,
    shard_count: usize,
    persistence: Option<ShardPersistenceConfig>,
    shard_channel_buffer: usize,
) -> EngineConfig {
    let per_shard_memory = max_memory.map(|total| {
        // divide evenly, rounding down — better to be slightly
        // conservative than to overshoot the total limit
        total / shard_count
    });

    EngineConfig {
        shard: ShardConfig {
            max_memory: per_shard_memory,
            eviction_policy,
            ..ShardConfig::default()
        },
        persistence,
        replication_tx: None,
        #[cfg(feature = "protobuf")]
        schema_registry: None,
        shard_channel_buffer,
    }
}

// ---------------------------------------------------------------------------
// EmberConfig: unified TOML configuration
// ---------------------------------------------------------------------------

/// Top-level configuration loaded from a TOML file.
///
/// Every field has a default matching the current hard-coded values so a
/// completely empty file is valid. Fields are grouped into sections that
/// mirror the TOML layout.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EmberConfig {
    // -- server --
    pub bind: String,
    pub port: u16,
    /// Number of shards. 0 means auto-detect CPU cores.
    pub shards: usize,
    pub concurrent: bool,
    pub requirepass: String,
    #[serde(rename = "data-dir")]
    pub data_dir: String,

    // -- connections --
    pub maxclients: usize,
    #[serde(rename = "idle-timeout-secs")]
    pub idle_timeout_secs: u64,
    #[serde(rename = "max-pipeline-depth")]
    pub max_pipeline_depth: usize,
    #[serde(rename = "max-auth-failures")]
    pub max_auth_failures: u32,

    // -- memory --
    pub maxmemory: String,
    #[serde(rename = "maxmemory-policy")]
    pub maxmemory_policy: String,

    // -- persistence --
    pub appendonly: bool,
    pub appendfsync: String,
    #[serde(rename = "active-expiry-interval-ms")]
    pub active_expiry_interval_ms: u64,
    #[serde(rename = "aof-fsync-interval-secs")]
    pub aof_fsync_interval_secs: u64,

    // -- monitoring --
    #[serde(rename = "metrics-port")]
    pub metrics_port: u16,
    #[serde(rename = "slowlog-log-slower-than")]
    pub slowlog_log_slower_than: i64,
    #[serde(rename = "slowlog-max-len")]
    pub slowlog_max_len: usize,

    // -- tls --
    #[serde(rename = "tls-port")]
    pub tls_port: u16,
    #[serde(rename = "tls-cert-file")]
    pub tls_cert_file: String,
    #[serde(rename = "tls-key-file")]
    pub tls_key_file: String,
    #[serde(rename = "tls-ca-cert-file")]
    pub tls_ca_cert_file: String,
    #[serde(rename = "tls-auth-clients")]
    pub tls_auth_clients: String,

    // -- protocol limits --
    #[serde(rename = "max-key-len")]
    pub max_key_len: String,
    #[serde(rename = "max-value-len")]
    pub max_value_len: String,
    #[serde(rename = "max-subscriptions-per-connection")]
    pub max_subscriptions_per_connection: usize,
    #[serde(rename = "max-pattern-len")]
    pub max_pattern_len: usize,
    #[serde(rename = "read-buffer-capacity")]
    pub read_buffer_capacity: usize,
    #[serde(rename = "max-buffer-size")]
    pub max_buffer_size: String,

    // -- cluster (nested table) --
    pub cluster: ClusterConfig,

    // -- engine internals (nested table) --
    pub engine: EngineInternalsConfig,
}

impl Default for EmberConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".into(),
            port: 6379,
            shards: 0,
            concurrent: false,
            requirepass: String::new(),
            data_dir: String::new(),

            maxclients: 10_000,
            idle_timeout_secs: 300,
            max_pipeline_depth: 10_000,
            max_auth_failures: 10,

            maxmemory: String::new(),
            maxmemory_policy: "noeviction".into(),

            appendonly: false,
            appendfsync: "everysec".into(),
            active_expiry_interval_ms: 100,
            aof_fsync_interval_secs: 1,

            metrics_port: 0,
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,

            tls_port: 0,
            tls_cert_file: String::new(),
            tls_key_file: String::new(),
            tls_ca_cert_file: String::new(),
            tls_auth_clients: "no".into(),

            max_key_len: "512kb".into(),
            max_value_len: "512mb".into(),
            max_subscriptions_per_connection: 10_000,
            max_pattern_len: 256,
            read_buffer_capacity: 4096,
            max_buffer_size: "64mb".into(),

            cluster: ClusterConfig::default(),
            engine: EngineInternalsConfig::default(),
        }
    }
}

/// Cluster-specific configuration (`[cluster]` section).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub bootstrap: bool,
    #[serde(rename = "port-offset")]
    pub port_offset: u16,
    #[serde(rename = "raft-port-offset")]
    pub raft_port_offset: u16,
    #[serde(rename = "node-timeout-ms")]
    pub node_timeout_ms: u64,
    #[serde(rename = "auth-pass")]
    pub auth_pass: String,
    pub gossip: GossipSection,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bootstrap: false,
            port_offset: 10_000,
            raft_port_offset: 10_001,
            node_timeout_ms: 5_000,
            auth_pass: String::new(),
            gossip: GossipSection::default(),
        }
    }
}

/// Gossip tuning parameters (`[cluster.gossip]` section).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GossipSection {
    #[serde(rename = "protocol-period-ms")]
    pub protocol_period_ms: u64,
    #[serde(rename = "probe-timeout-ms")]
    pub probe_timeout_ms: u64,
    #[serde(rename = "suspicion-multiplier")]
    pub suspicion_multiplier: u32,
    #[serde(rename = "indirect-probes")]
    pub indirect_probes: usize,
    #[serde(rename = "max-piggyback")]
    pub max_piggyback: usize,
}

impl Default for GossipSection {
    fn default() -> Self {
        Self {
            protocol_period_ms: 1_000,
            probe_timeout_ms: 500,
            suspicion_multiplier: 5,
            indirect_probes: 3,
            max_piggyback: 10,
        }
    }
}

/// Engine internal tuning (`[engine]` section).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EngineInternalsConfig {
    #[serde(rename = "shard-channel-buffer")]
    pub shard_channel_buffer: usize,
    #[serde(rename = "replication-broadcast-capacity")]
    pub replication_broadcast_capacity: usize,
    #[serde(rename = "stats-poll-interval-secs")]
    pub stats_poll_interval_secs: u64,
}

impl Default for EngineInternalsConfig {
    fn default() -> Self {
        Self {
            shard_channel_buffer: 256,
            replication_broadcast_capacity: 65_536,
            stats_poll_interval_secs: 5,
        }
    }
}

impl EmberConfig {
    /// Loads config from a TOML file, falling back to defaults for any
    /// missing fields. Returns an error if the file can't be read or
    /// contains unknown fields.
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file '{}': {e}", path.display()))?;
        toml::from_str(&contents)
            .map_err(|e| format!("failed to parse config file '{}': {e}", path.display()))
    }

    /// Serializes the current config to a TOML string.
    pub fn to_toml(&self) -> Result<String, String> {
        toml::to_string_pretty(self).map_err(|e| format!("failed to serialize config: {e}"))
    }

    /// Derives `ConnectionLimits` from this config, resolving byte-size
    /// strings to concrete numbers.
    pub fn connection_limits(&self) -> Result<ConnectionLimits, String> {
        let max_key_len = if self.max_key_len.is_empty() {
            512 * 1024
        } else {
            parse_byte_size(&self.max_key_len)?
        };
        let max_value_len = if self.max_value_len.is_empty() {
            512 * 1024 * 1024
        } else {
            parse_byte_size(&self.max_value_len)?
        };
        let max_buf_size = if self.max_buffer_size.is_empty() {
            64 * 1024 * 1024
        } else {
            parse_byte_size(&self.max_buffer_size)?
        };

        Ok(ConnectionLimits {
            buf_capacity: self.read_buffer_capacity,
            max_buf_size,
            idle_timeout: Duration::from_secs(self.idle_timeout_secs),
            max_auth_failures: self.max_auth_failures,
            max_subscriptions_per_conn: self.max_subscriptions_per_connection,
            max_pattern_len: self.max_pattern_len,
            max_pipeline_depth: self.max_pipeline_depth,
            max_key_len,
            max_value_len,
            stats_poll_interval: Duration::from_secs(self.engine.stats_poll_interval_secs),
        })
    }

    /// Builds the runtime `ConfigRegistry` from this config's values.
    pub fn to_registry(&self) -> ConfigRegistry {
        let mut params = HashMap::new();
        params.insert("port".into(), self.port.to_string());
        params.insert("bind-address".into(), self.bind.clone());
        params.insert("maxmemory".into(), if self.maxmemory.is_empty() {
            "0".into()
        } else {
            parse_byte_size(&self.maxmemory).map(|v| v.to_string()).unwrap_or_else(|_| "0".into())
        });
        params.insert("maxmemory-policy".into(), self.maxmemory_policy.clone());
        params.insert("appendonly".into(), if self.appendonly { "yes" } else { "no" }.into());
        params.insert("appendfsync".into(), self.appendfsync.clone());
        params.insert(
            "slowlog-log-slower-than".into(),
            self.slowlog_log_slower_than.to_string(),
        );
        params.insert("slowlog-max-len".into(), self.slowlog_max_len.to_string());
        params.insert("maxclients".into(), self.maxclients.to_string());
        params.insert("idle-timeout-secs".into(), self.idle_timeout_secs.to_string());
        params.insert("max-pipeline-depth".into(), self.max_pipeline_depth.to_string());
        params.insert("max-auth-failures".into(), self.max_auth_failures.to_string());
        params.insert("active-expiry-interval-ms".into(), self.active_expiry_interval_ms.to_string());
        params.insert("aof-fsync-interval-secs".into(), self.aof_fsync_interval_secs.to_string());
        params.insert("max-key-len".into(), self.max_key_len.clone());
        params.insert("max-value-len".into(), self.max_value_len.clone());
        params.insert("max-subscriptions-per-connection".into(), self.max_subscriptions_per_connection.to_string());
        params.insert("max-pattern-len".into(), self.max_pattern_len.to_string());
        params.insert("read-buffer-capacity".into(), self.read_buffer_capacity.to_string());
        params.insert("max-buffer-size".into(), self.max_buffer_size.clone());
        params.insert("shard-channel-buffer".into(), self.engine.shard_channel_buffer.to_string());
        params.insert("replication-broadcast-capacity".into(), self.engine.replication_broadcast_capacity.to_string());
        params.insert("stats-poll-interval-secs".into(), self.engine.stats_poll_interval_secs.to_string());
        ConfigRegistry::new(params)
    }

    /// Resolves the shard count: uses the configured value, or CPU count
    /// if set to 0.
    pub fn resolved_shard_count(&self) -> usize {
        if self.shards == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            self.shards
        }
    }

    /// Resolves the data directory as an optional `PathBuf`.
    pub fn data_dir_path(&self) -> Option<PathBuf> {
        if self.data_dir.is_empty() {
            None
        } else {
            Some(PathBuf::from(&self.data_dir))
        }
    }

    /// Parses `maxmemory` into an optional byte count.
    pub fn max_memory_bytes(&self) -> Result<Option<usize>, String> {
        if self.maxmemory.is_empty() || self.maxmemory == "0" {
            Ok(None)
        } else {
            parse_byte_size(&self.maxmemory).map(Some)
        }
    }

    /// Parses `metrics-port`, returning `None` when 0 (disabled).
    pub fn metrics_port(&self) -> Option<u16> {
        if self.metrics_port == 0 {
            None
        } else {
            Some(self.metrics_port)
        }
    }

    /// Parses `tls-port`, returning `None` when 0 (disabled).
    pub fn tls_port(&self) -> Option<u16> {
        if self.tls_port == 0 {
            None
        } else {
            Some(self.tls_port)
        }
    }

    /// Returns the requirepass value, or `None` if empty.
    pub fn requirepass(&self) -> Option<String> {
        if self.requirepass.is_empty() {
            None
        } else {
            Some(self.requirepass.clone())
        }
    }

    /// Returns the cluster auth-pass, or `None` if empty.
    pub fn cluster_auth_pass(&self) -> Option<String> {
        if self.cluster.auth_pass.is_empty() {
            None
        } else {
            Some(self.cluster.auth_pass.clone())
        }
    }
}

/// Runtime limits derived from `EmberConfig` at startup.
///
/// Stored on `ServerContext` and read by connection handlers on the
/// hot path. All values are plain types — no allocations per-access.
#[derive(Debug, Clone)]
pub struct ConnectionLimits {
    pub buf_capacity: usize,
    pub max_buf_size: usize,
    pub idle_timeout: Duration,
    pub max_auth_failures: u32,
    pub max_subscriptions_per_conn: usize,
    pub max_pattern_len: usize,
    pub max_pipeline_depth: usize,
    pub max_key_len: usize,
    pub max_value_len: usize,
    pub stats_poll_interval: Duration,
}

/// Runtime configuration registry for CONFIG GET/SET.
///
/// Stores all server parameters as strings (matching Redis convention).
/// Only a small whitelist of parameters are mutable at runtime — the
/// rest return an error on CONFIG SET.
pub struct ConfigRegistry {
    params: RwLock<HashMap<String, String>>,
}

impl std::fmt::Debug for ConfigRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigRegistry").finish_non_exhaustive()
    }
}

/// Parameters that can be changed at runtime via CONFIG SET.
const MUTABLE_PARAMS: &[&str] = &["slowlog-log-slower-than", "slowlog-max-len"];

impl ConfigRegistry {
    /// Creates a new registry from the initial parameter map.
    pub fn new(params: HashMap<String, String>) -> Self {
        Self {
            params: RwLock::new(params),
        }
    }

    /// Returns all parameters matching the glob pattern.
    ///
    /// Supports `*` as a wildcard prefix/suffix/standalone. Simple glob
    /// matching is sufficient — monitoring tools typically use `*` or
    /// exact names.
    pub fn get_matching(&self, pattern: &str) -> Vec<(String, String)> {
        let params = self.params.read().unwrap_or_else(|e| e.into_inner());
        let mut results: Vec<_> = params
            .iter()
            .filter(|(k, _)| glob_match(pattern, k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results
    }

    /// Sets a configuration parameter at runtime.
    ///
    /// Only parameters in the mutable whitelist can be changed. Returns
    /// the new value on success so the caller can apply it.
    pub fn set(&self, param: &str, value: &str) -> Result<(), String> {
        let key = param.to_ascii_lowercase();
        if !MUTABLE_PARAMS.contains(&key.as_str()) {
            return Err(format!(
                "ERR Unsupported CONFIG parameter: {param}"
            ));
        }
        let mut params = self.params.write().unwrap_or_else(|e| e.into_inner());
        params.insert(key, value.to_string());
        Ok(())
    }
}

/// Simple glob matching for CONFIG GET patterns.
///
/// Supports `*` (match everything), `foo*` (prefix), `*foo` (suffix),
/// and exact match. This covers the patterns that monitoring tools use.
fn glob_match(pattern: &str, name: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let lower_pattern = pattern.to_ascii_lowercase();
    let lower_name = name.to_ascii_lowercase();

    if let Some(prefix) = lower_pattern.strip_suffix('*') {
        lower_name.starts_with(prefix)
    } else if let Some(suffix) = lower_pattern.strip_prefix('*') {
        lower_name.ends_with(suffix)
    } else {
        lower_pattern == lower_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_plain_bytes() {
        assert_eq!(parse_byte_size("1024").unwrap(), 1024);
    }

    #[test]
    fn parse_kilobytes() {
        assert_eq!(parse_byte_size("100K").unwrap(), 100 * 1024);
        assert_eq!(parse_byte_size("100KB").unwrap(), 100 * 1024);
        assert_eq!(parse_byte_size("100k").unwrap(), 100 * 1024);
    }

    #[test]
    fn parse_megabytes() {
        assert_eq!(parse_byte_size("50M").unwrap(), 50 * 1024 * 1024);
        assert_eq!(parse_byte_size("50MB").unwrap(), 50 * 1024 * 1024);
    }

    #[test]
    fn parse_gigabytes() {
        assert_eq!(parse_byte_size("2G").unwrap(), 2 * 1024 * 1024 * 1024);
        assert_eq!(parse_byte_size("2GB").unwrap(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_empty_is_error() {
        assert!(parse_byte_size("").is_err());
    }

    #[test]
    fn parse_invalid_is_error() {
        assert!(parse_byte_size("abc").is_err());
        assert!(parse_byte_size("M").is_err());
    }

    #[test]
    fn parse_eviction_policies() {
        assert_eq!(
            parse_eviction_policy("noeviction").unwrap(),
            EvictionPolicy::NoEviction
        );
        assert_eq!(
            parse_eviction_policy("allkeys-lru").unwrap(),
            EvictionPolicy::AllKeysLru
        );
        assert_eq!(
            parse_eviction_policy("ALLKEYS-LRU").unwrap(),
            EvictionPolicy::AllKeysLru
        );
    }

    #[test]
    fn parse_unknown_policy_is_error() {
        assert!(parse_eviction_policy("random").is_err());
    }

    #[test]
    fn parse_fsync_policies() {
        assert_eq!(parse_fsync_policy("always").unwrap(), FsyncPolicy::Always);
        assert_eq!(
            parse_fsync_policy("everysec").unwrap(),
            FsyncPolicy::EverySec
        );
        assert_eq!(parse_fsync_policy("no").unwrap(), FsyncPolicy::No);
        assert_eq!(parse_fsync_policy("ALWAYS").unwrap(), FsyncPolicy::Always);
    }

    #[test]
    fn parse_unknown_fsync_policy_is_error() {
        assert!(parse_fsync_policy("sometimes").is_err());
    }

    #[test]
    fn build_config_divides_memory() {
        let cfg = build_engine_config(Some(400), EvictionPolicy::AllKeysLru, 4, None, 0);
        assert_eq!(cfg.shard.max_memory, Some(100));
        assert_eq!(cfg.shard.eviction_policy, EvictionPolicy::AllKeysLru);
    }

    #[test]
    fn build_config_no_limit() {
        let cfg = build_engine_config(None, EvictionPolicy::NoEviction, 4, None, 0);
        assert_eq!(cfg.shard.max_memory, None);
    }

    #[test]
    fn glob_match_wildcard() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("slow*", "slowlog-log-slower-than"));
        assert!(glob_match("slow*", "slowlog-max-len"));
        assert!(!glob_match("slow*", "maxmemory"));
        assert!(glob_match("*memory", "maxmemory"));
        assert!(!glob_match("*memory", "slowlog-max-len"));
    }

    #[test]
    fn glob_match_exact() {
        assert!(glob_match("port", "port"));
        assert!(glob_match("PORT", "port")); // case insensitive
        assert!(!glob_match("port", "maxmemory"));
    }

    #[test]
    fn config_registry_get_matching() {
        let mut params = HashMap::new();
        params.insert("port".into(), "6379".into());
        params.insert("maxmemory".into(), "0".into());
        params.insert("slowlog-log-slower-than".into(), "10000".into());
        let registry = ConfigRegistry::new(params);

        let all = registry.get_matching("*");
        assert_eq!(all.len(), 3);

        let slow = registry.get_matching("slow*");
        assert_eq!(slow.len(), 1);
        assert_eq!(slow[0].0, "slowlog-log-slower-than");
    }

    #[test]
    fn config_registry_set_mutable() {
        let mut params = HashMap::new();
        params.insert("slowlog-log-slower-than".into(), "10000".into());
        let registry = ConfigRegistry::new(params);

        assert!(registry.set("slowlog-log-slower-than", "5000").is_ok());
        let result = registry.get_matching("slowlog-log-slower-than");
        assert_eq!(result[0].1, "5000");
    }

    #[test]
    fn config_registry_set_immutable_rejected() {
        let mut params = HashMap::new();
        params.insert("maxmemory".into(), "100".into());
        let registry = ConfigRegistry::new(params);

        assert!(registry.set("maxmemory", "200").is_err());
    }
}
