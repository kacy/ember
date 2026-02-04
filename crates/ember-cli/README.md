# emberkv-cli

interactive command-line client for [ember](https://github.com/kacy/ember).

> this crate is a stub — the CLI is planned for a future phase.

## planned features

- interactive REPL with command history
- syntax highlighting and autocomplete
- inline help (`help SET`, `help ZADD`)
- cluster management subcommands
- built-in benchmarking

## related crates

| crate | what it does |
|-------|-------------|
| [emberkv-core](../ember-core) | storage engine, keyspace, sharding |
| [ember-protocol](../ember-protocol) | RESP3 parsing and command dispatch |
| [ember-persistence](../ember-persistence) | AOF, snapshots, and crash recovery |
| [ember-server](../ember-server) | TCP server and connection handling |
| [ember-cluster](../ember-cluster) | distributed coordination (WIP) |
