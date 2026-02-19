//! The engine: coordinator for the sharded keyspace.
//!
//! Routes single-key operations to the correct shard based on a hash
//! of the key. Each shard is an independent tokio task — no locks on
//! the hot path.

use tokio::sync::broadcast;

use crate::dropper::DropHandle;
use crate::error::ShardError;
use crate::keyspace::ShardConfig;
use crate::shard::{self, ReplicationEvent, ShardHandle, ShardPersistenceConfig, ShardRequest, ShardResponse};

/// Channel buffer size per shard. 256 is large enough to absorb
/// bursts without putting meaningful back-pressure on connections.
const SHARD_BUFFER: usize = 256;

/// Configuration for the engine, passed down to each shard.
#[derive(Debug, Clone, Default)]
pub struct EngineConfig {
    /// Per-shard configuration (memory limits, eviction policy).
    pub shard: ShardConfig,
    /// Optional persistence configuration. When set, each shard gets
    /// its own AOF and snapshot files under this directory.
    pub persistence: Option<ShardPersistenceConfig>,
    /// Optional broadcast sender for replication events.
    ///
    /// When set, every successful mutation is published as a
    /// [`ReplicationEvent`] so replication clients can stream it to
    /// replicas.
    pub replication_tx: Option<broadcast::Sender<ReplicationEvent>>,
    /// Optional schema registry for protobuf value validation.
    /// When set, enables PROTO.* commands.
    #[cfg(feature = "protobuf")]
    pub schema_registry: Option<crate::schema::SharedSchemaRegistry>,
}

/// The sharded engine. Owns handles to all shard tasks and routes
/// requests by key hash.
///
/// `Clone` is cheap — it just clones the `Vec<ShardHandle>` (which are
/// mpsc senders under the hood).
#[derive(Debug, Clone)]
pub struct Engine {
    shards: Vec<ShardHandle>,
    replication_tx: Option<broadcast::Sender<ReplicationEvent>>,
    #[cfg(feature = "protobuf")]
    schema_registry: Option<crate::schema::SharedSchemaRegistry>,
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
    /// Spawns a single background drop thread shared by all shards for
    /// lazy-freeing large values.
    ///
    /// Panics if `shard_count` is zero.
    pub fn with_config(shard_count: usize, config: EngineConfig) -> Self {
        assert!(shard_count > 0, "shard count must be at least 1");
        assert!(
            shard_count <= u16::MAX as usize,
            "shard count must fit in u16"
        );

        let drop_handle = DropHandle::spawn();

        let shards = (0..shard_count)
            .map(|i| {
                let mut shard_config = config.shard.clone();
                shard_config.shard_id = i as u16;
                shard::spawn_shard(
                    SHARD_BUFFER,
                    shard_config,
                    config.persistence.clone(),
                    Some(drop_handle.clone()),
                    config.replication_tx.clone(),
                    #[cfg(feature = "protobuf")]
                    config.schema_registry.clone(),
                )
            })
            .collect();

        Self {
            shards,
            replication_tx: config.replication_tx,
            #[cfg(feature = "protobuf")]
            schema_registry: config.schema_registry,
        }
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

    /// Returns a reference to the schema registry, if protobuf is enabled.
    #[cfg(feature = "protobuf")]
    pub fn schema_registry(&self) -> Option<&crate::schema::SharedSchemaRegistry> {
        self.schema_registry.as_ref()
    }

    /// Returns the number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Creates a new broadcast receiver for replication events.
    ///
    /// Returns `None` if no replication channel was configured. Each
    /// caller gets an independent receiver starting from the current
    /// broadcast position — not from the beginning of the stream.
    pub fn subscribe_replication(&self) -> Option<broadcast::Receiver<ReplicationEvent>> {
        self.replication_tx.as_ref().map(|tx| tx.subscribe())
    }

    /// Sends a request to a specific shard by index.
    ///
    /// Used by SCAN to iterate through shards sequentially.
    pub async fn send_to_shard(
        &self,
        shard_idx: usize,
        request: ShardRequest,
    ) -> Result<ShardResponse, ShardError> {
        if shard_idx >= self.shards.len() {
            return Err(ShardError::Unavailable);
        }
        self.shards[shard_idx].send(request).await
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
    /// Dispatches to all shards first (so they start processing in
    /// parallel), then collects the replies. Used for commands like
    /// DBSIZE and INFO that need data from all shards.
    pub async fn broadcast<F>(&self, make_req: F) -> Result<Vec<ShardResponse>, ShardError>
    where
        F: Fn() -> ShardRequest,
    {
        // dispatch to all shards without waiting for responses
        let mut receivers = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            receivers.push(shard.dispatch(make_req()).await?);
        }

        // now collect all responses
        let mut results = Vec::with_capacity(receivers.len());
        for rx in receivers {
            results.push(rx.await.map_err(|_| ShardError::Unavailable)?);
        }
        Ok(results)
    }

    /// Routes requests for multiple keys concurrently.
    ///
    /// Dispatches all requests without waiting, then collects responses.
    /// The response order matches the key order. Used for multi-key
    /// commands like DEL and EXISTS.
    pub async fn route_multi<F>(
        &self,
        keys: &[String],
        make_req: F,
    ) -> Result<Vec<ShardResponse>, ShardError>
    where
        F: Fn(String) -> ShardRequest,
    {
        let mut receivers = Vec::with_capacity(keys.len());
        for key in keys {
            let idx = self.shard_for_key(key);
            let rx = self.shards[idx].dispatch(make_req(key.clone())).await?;
            receivers.push(rx);
        }

        let mut results = Vec::with_capacity(receivers.len());
        for rx in receivers {
            results.push(rx.await.map_err(|_| ShardError::Unavailable)?);
        }
        Ok(results)
    }

    /// Returns true if both keys are owned by the same shard.
    pub fn same_shard(&self, key1: &str, key2: &str) -> bool {
        self.shard_for_key(key1) == self.shard_for_key(key2)
    }

    /// Determines which shard owns a given key.
    pub fn shard_for_key(&self, key: &str) -> usize {
        shard_index(key, self.shards.len())
    }

    /// Sends a request to a shard and returns the reply channel without
    /// waiting for the response. Used by the connection handler to
    /// dispatch commands and collect responses separately.
    pub async fn dispatch_to_shard(
        &self,
        shard_idx: usize,
        request: ShardRequest,
    ) -> Result<tokio::sync::oneshot::Receiver<ShardResponse>, ShardError> {
        if shard_idx >= self.shards.len() {
            return Err(ShardError::Unavailable);
        }
        self.shards[shard_idx].dispatch(request).await
    }
}

/// Pure function: maps a key to a shard index.
///
/// Uses FNV-1a hashing for deterministic shard routing across restarts.
/// This is critical for AOF/snapshot recovery — keys must hash to the
/// same shard on every startup, otherwise recovered data lands in the
/// wrong shard.
///
/// FNV-1a is simple, fast for short keys, and completely deterministic
/// (no per-process randomization). Shard routing is trusted internal
/// logic so DoS-resistant hashing is unnecessary here.
fn shard_index(key: &str, shard_count: usize) -> usize {
    // FNV-1a 64-bit
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    (hash as usize) % shard_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use bytes::Bytes;

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
                    nx: false,
                    xx: false,
                },
            )
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Ok));

        let resp = engine
            .route(
                "greeting",
                ShardRequest::Get {
                    key: "greeting".into(),
                },
            )
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
                        nx: false,
                        xx: false,
                    },
                )
                .await
                .unwrap();
        }

        // delete them all and count successes
        let mut count = 0i64;
        for key in &["a", "b", "c", "d", "missing"] {
            let resp = engine
                .route(
                    key,
                    ShardRequest::Del {
                        key: key.to_string(),
                    },
                )
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
