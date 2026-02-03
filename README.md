# ember

a low-latency, memory-efficient, distributed cache written in Rust.

## build

```bash
cargo build --release
```

## run

```bash
./target/release/ember-server
```

## development

```bash
make check   # fmt, clippy, tests
make build   # debug build
make release # release build
```

## project structure

```
crates/
  ember-server/       main server binary
  ember-core/         core engine (keyspace, types, sharding)
  ember-protocol/     RESP3 wire protocol
  ember-persistence/  AOF and snapshot durability
  ember-cluster/      raft, gossip, slot management
  ember-cli/          interactive CLI tool
```

## license

MIT
