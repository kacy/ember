//! Server configuration parsing and runtime config registry.
//!
//! Handles conversion from CLI-friendly strings (like "100M", "1G")
//! to the internal config types used by the engine. Also provides a
//! `ConfigRegistry` for CONFIG GET/SET support at runtime.

use std::collections::HashMap;
use std::sync::RwLock;

use ember_core::{EngineConfig, EvictionPolicy, ShardConfig, ShardPersistenceConfig};
use ember_persistence::aof::FsyncPolicy;

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
    }
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
        let cfg = build_engine_config(Some(400), EvictionPolicy::AllKeysLru, 4, None);
        assert_eq!(cfg.shard.max_memory, Some(100));
        assert_eq!(cfg.shard.eviction_policy, EvictionPolicy::AllKeysLru);
    }

    #[test]
    fn build_config_no_limit() {
        let cfg = build_engine_config(None, EvictionPolicy::NoEviction, 4, None);
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
