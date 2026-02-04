# emberkv-core

the storage engine for [ember](https://github.com/kacy/ember). owns the keyspace, data types, sharding, expiration, and memory management.

this is the heart of ember — it implements the shared-nothing, shard-per-core architecture where each shard independently manages a partition of keys with no cross-thread synchronization on the hot path.

## what's in here

- **engine** — routes requests to shards by key hash, supports single-key, multi-key, and broadcast operations
- **shard** — the single-threaded event loop per partition: dispatch, AOF recording, expiration ticks, fsync ticks
- **keyspace** — the key-value store itself: strings, lists, sorted sets, TTL, LRU eviction
- **types** — `Value` enum with `String(Bytes)`, `List(VecDeque<Bytes>)`, `SortedSet` (BTreeMap + HashMap dual-index)
- **memory** — per-shard memory tracking and entry size estimation
- **expiry** — lazy (on access) and active (background sampling) TTL expiration

## key types

```rust
use ember_core::{Engine, EngineConfig, ShardRequest, ShardResponse};

// create a 4-shard engine
let engine = Engine::new(4);

// route a GET to the correct shard
let response = engine.route("mykey", ShardRequest::Get {
    key: "mykey".into(),
}).await?;
```

## related crates

| crate | what it does |
|-------|-------------|
| [ember-protocol](../ember-protocol) | RESP3 parsing and command dispatch |
| [ember-persistence](../ember-persistence) | AOF, snapshots, and crash recovery |
| [ember-server](../ember-server) | TCP server and connection handling |
| [ember-cluster](../ember-cluster) | distributed coordination (WIP) |
| [ember-cli](../ember-cli) | interactive command-line client (WIP) |
