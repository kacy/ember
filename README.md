<p align="center">
  <img src="ember-logo.png" alt="ember logo" width="200">
</p>

# ❤️‍🔥 ember

a low-latency, memory-efficient, distributed cache written in Rust. designed to outperform Redis on throughput, latency, and memory efficiency while keeping the codebase small and readable.

## features

- **resp3 protocol** — full compatibility with `redis-cli` and existing Redis clients
- **core commands** — GET, SET, DEL, EXISTS, EXPIRE, TTL with proper semantics
- **lazy expiration** — expired keys are cleaned up on access, no background overhead
- **pipelined connections** — multiple commands per read for high throughput
- **thread-safe** — shared keyspace behind `Arc<Mutex>` ready for concurrent clients

## quickstart

```bash
# build
cargo build --release

# run the server
./target/release/ember-server

# connect with redis-cli
redis-cli SET hello world    # => OK
redis-cli GET hello          # => "world"
redis-cli SET temp data EX 60
redis-cli TTL temp           # => 59
```

## build & development

```bash
make check   # fmt, clippy, tests
make build   # debug build
make release # release build
make test    # run all tests
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

## architecture

ember uses a shared-nothing, thread-per-core design inspired by [Dragonfly](https://github.com/dragonflydb/dragonfly). each core owns a partition of the keyspace with no cross-thread synchronization on the hot path.

| target | redis baseline | ember goal |
|--------|---------------|------------|
| throughput | ~100k ops/sec/core | 500k+ ops/sec/core |
| p99 latency | ~1ms | <200µs |
| memory/key | ~90 bytes overhead | <40 bytes |

## license

MIT
