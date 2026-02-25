# ember-server

the main server binary for [ember](https://github.com/kacy/ember). accepts TCP connections, parses RESP3 commands, routes them through the sharded engine, and writes responses back. supports pipelining and graceful shutdown.

## what's in here

- **main** — CLI arg parsing (host, port, max-memory, eviction policy, persistence, metrics, slowlog config, TLS, cluster, ACL)
- **server** — TCP accept loop with configurable connection limits, graceful shutdown on SIGINT/SIGTERM, spawns a handler task per client
- **connection** — per-connection event loop: read → parse frames → dispatch commands → write responses. handles idle timeouts, buffer limits, protocol errors, pipelining, transactions (MULTI/EXEC/WATCH), and ACL permission checks
- **config** — TOML-based configuration with runtime CONFIG GET/SET/REWRITE, byte size parsing, eviction/fsync policies
- **acl** — per-user access control: command permissions, key pattern restrictions, SHA-256 password hashing with constant-time comparison
- **pubsub** — channel and pattern subscriptions with SUBSCRIBE, PSUBSCRIBE, PUBLISH
- **metrics** — prometheus exporter, per-command histogram/counter recording, background stats poller
- **slowlog** — ring buffer for slow command logging with configurable threshold and capacity
- **tls** — optional TLS support on a separate port, with mTLS for client certificate verification
- **cluster** — cluster mode integration: gossip event loop, raft consensus, slot routing, replication, failover
- **replication** — primary→replica data streaming with AOF-based sync

## running

```bash
# basic — listens on 127.0.0.1:6379, no persistence
cargo run --release -p ember-server

# with memory limit and LRU eviction
cargo run --release -p ember-server -- --max-memory 256M --eviction-policy allkeys-lru

# with AOF persistence
cargo run --release -p ember-server -- --data-dir ./data --appendonly --appendfsync everysec

# with prometheus metrics on port 9100
cargo run --release -p ember-server -- --metrics-port 9100

# with encryption at rest (requires --features encryption)
cargo run --release -p ember-server --features encryption -- \
  --data-dir ./data --appendonly --encryption-key-file /path/to/keyfile
```

compatible with `redis-cli` and any RESP3 client.

## related crates

| crate | what it does |
|-------|-------------|
| [emberkv-core](../ember-core) | storage engine, keyspace, sharding |
| [ember-protocol](../ember-protocol) | RESP3 parsing and command dispatch |
| [ember-persistence](../ember-persistence) | AOF, snapshots, and crash recovery |
| [ember-cluster](../ember-cluster) | distributed coordination |
| [ember-cli](../ember-cli) | interactive CLI client (REPL, cluster subcommands, benchmark) |
