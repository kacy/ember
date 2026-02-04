# ember-cluster

distributed coordination for [ember](https://github.com/kacy/ember). will handle cluster topology, gossip-based failure detection, raft consensus, and live slot migration.

> this crate is a stub — cluster support is planned for a future phase.

## planned features

- hash slot mapping (16384 slots) with `MOVED`/`ASK` redirects
- SWIM gossip protocol for failure detection
- raft consensus (via `openraft`) for leader election and log replication
- live slot migration for rebalancing without downtime

## related crates

| crate | what it does |
|-------|-------------|
| [emberkv-core](../ember-core) | storage engine, keyspace, sharding |
| [ember-protocol](../ember-protocol) | RESP3 parsing and command dispatch |
| [ember-persistence](../ember-persistence) | AOF, snapshots, and crash recovery |
| [ember-server](../ember-server) | TCP server and connection handling |
| [ember-cli](../ember-cli) | interactive command-line client (WIP) |
