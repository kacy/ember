# ember-server

the main server binary for [ember](https://github.com/kacy/ember). accepts TCP connections, parses RESP3 commands, routes them through the sharded engine, and writes responses back. supports pipelining.

## what's in here

- **main** — CLI arg parsing (host, port, max-memory, eviction policy, persistence config)
- **server** — TCP accept loop with configurable connection limits, spawns a handler task per client
- **connection** — per-connection event loop: read → parse frames → dispatch commands → write responses. handles idle timeouts, buffer limits, and protocol errors
- **config** — configuration helpers for byte sizes, eviction policies, fsync policies

## running

```bash
# basic — listens on 127.0.0.1:6379, no persistence
cargo run --release -p ember-server

# with memory limit and LRU eviction
cargo run --release -p ember-server -- --max-memory 256M --eviction-policy allkeys-lru

# with AOF persistence
cargo run --release -p ember-server -- --data-dir ./data --appendonly --appendfsync everysec
```

compatible with `redis-cli` and any RESP3 client.

## related crates

| crate | what it does |
|-------|-------------|
| [emberkv-core](../ember-core) | storage engine, keyspace, sharding |
| [ember-protocol](../ember-protocol) | RESP3 parsing and command dispatch |
| [ember-persistence](../ember-persistence) | AOF, snapshots, and crash recovery |
| [ember-cluster](../ember-cluster) | distributed coordination (WIP) |
| [ember-cli](../ember-cli) | interactive command-line client (WIP) |
