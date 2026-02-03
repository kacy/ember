//! The engine: coordinator for the sharded keyspace.
//!
//! Routes single-key operations to the correct shard based on a hash
//! of the key. Each shard is an independent tokio task — no locks on
//! the hot path.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::error::ShardError;
use crate::keyspace::ShardConfig;
use crate::shard::{self, ShardHandle, ShardRequest, ShardResponse};

/// Channel buffer size per shard. 256 is large enough to absorb
/// bursts without putting meaningful back-pressure on connections.
const SHARD_BUFFER: usize = 256;

/// Configuration for the engine, passed down to each shard.
#[derive(Debug, Clone, Default)]
pub struct EngineConfig {
    /// Per-shard configuration (memory limits, eviction policy).
    pub shard: ShardConfig,
}

/// The sharded engine. Owns handles to all shard tasks and routes
/// requests by key hash.
///
/// `Clone` is cheap — it just clones the `Vec<ShardHandle>` (which are
/// mpsc senders under the hood).
#[derive(Debug, Clone)]
pub struct Engine {
    shards: Vec<ShardHandle>,
}

impl Engine {
    /// Creates an engine with `shard_count` shards using default config.
    ///
    /// Each shard is spawned as a tokio task immediately.
    /// Panics if `shard_count` is zero.
    pub fn new(shard_count: usize) -> Self {
        Self::with_config(shard_count, EngineConfig::default())
    }

    /// Creates an engine with `shard_count` shards and the given config.
    ///
    /// Panics if `shard_count` is zero.
    pub fn with_config(shard_count: usize, config: EngineConfig) -> Self {
        assert!(shard_count > 0, "shard count must be at least 1");

        let shards = (0..shard_count)
            .map(|_| shard::spawn_shard(SHARD_BUFFER, config.shard.clone()))
            .collect();

        Self { shards }
    }

    /// Creates an engine with one shard per available CPU core.
    ///
    /// Falls back to a single shard if the core count can't be determined.
    pub fn with_available_cores() -> Self {
        Self::with_available_cores_config(EngineConfig::default())
    }

    /// Creates an engine with one shard per available CPU core and the
    /// given config.
    pub fn with_available_cores_config(config: EngineConfig) -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        Self::with_config(cores, config)
    }

    /// Returns the number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Routes a request to the shard that owns `key`.
    pub async fn route(
        &self,
        key: &str,
        request: ShardRequest,
    ) -> Result<ShardResponse, ShardError> {
        let idx = self.shard_for_key(key);
        self.shards[idx].send(request).await
    }

    /// Sends a request to every shard and collects all responses.
    ///
    /// Used for commands like DBSIZE and INFO that need data from all
    /// shards. The request factory `make_req` is called once per shard.
    pub async fn broadcast<F>(
        &self,
        make_req: F,
    ) -> Result<Vec<ShardResponse>, ShardError>
    where
        F: Fn() -> ShardRequest,
    {
        let mut results = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let resp = shard.send(make_req()).await?;
            results.push(resp);
        }
        Ok(results)
    }

    /// Determines which shard owns a given key.
    fn shard_for_key(&self, key: &str) -> usize {
        shard_index(key, self.shards.len())
    }
}

/// Pure function: maps a key to a shard index.
///
/// Uses `DefaultHasher` (SipHash) and modulo. Deterministic within a
/// single process — that's all we need for local sharding. CRC16 will
/// replace this when cluster-level slot assignment arrives.
fn shard_index(key: &str, shard_count: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::types::Value;

    #[test]
    fn same_key_same_shard() {
        let idx1 = shard_index("foo", 8);
        let idx2 = shard_index("foo", 8);
        assert_eq!(idx1, idx2);
    }

    #[test]
    fn keys_spread_across_shards() {
        let mut seen = std::collections::HashSet::new();
        // with enough keys, we should hit more than one shard
        for i in 0..100 {
            let key = format!("key:{i}");
            seen.insert(shard_index(&key, 4));
        }
        assert!(seen.len() > 1, "expected keys to spread across shards");
    }

    #[test]
    fn single_shard_always_zero() {
        assert_eq!(shard_index("anything", 1), 0);
        assert_eq!(shard_index("other", 1), 0);
    }

    #[tokio::test]
    async fn engine_round_trip() {
        let engine = Engine::new(4);

        let resp = engine
            .route(
                "greeting",
                ShardRequest::Set {
                    key: "greeting".into(),
                    value: Bytes::from("hello"),
                    expire: None,
                },
            )
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Ok));

        let resp = engine
            .route("greeting", ShardRequest::Get { key: "greeting".into() })
            .await
            .unwrap();
        match resp {
            ShardResponse::Value(Some(Value::String(data))) => {
                assert_eq!(data, Bytes::from("hello"));
            }
            other => panic!("expected Value(Some(String)), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn multi_shard_del() {
        let engine = Engine::new(4);

        // set several keys (likely landing on different shards)
        for key in &["a", "b", "c", "d"] {
            engine
                .route(
                    key,
                    ShardRequest::Set {
                        key: key.to_string(),
                        value: Bytes::from("v"),
                        expire: None,
                    },
                )
                .await
                .unwrap();
        }

        // delete them all and count successes
        let mut count = 0i64;
        for key in &["a", "b", "c", "d", "missing"] {
            let resp = engine
                .route(key, ShardRequest::Del { key: key.to_string() })
                .await
                .unwrap();
            if let ShardResponse::Bool(true) = resp {
                count += 1;
            }
        }
        assert_eq!(count, 4);
    }

    #[test]
    #[should_panic(expected = "shard count must be at least 1")]
    fn zero_shards_panics() {
        Engine::new(0);
    }
}
