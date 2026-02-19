//! Server configuration parsing.
//!
//! Handles conversion from CLI-friendly strings (like "100M", "1G")
//! to the internal config types used by the engine.

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
}
