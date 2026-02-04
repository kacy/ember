<p align="center">
  <img src="ember-logo.png" alt="ember logo" width="200">
</p>

# ❤️‍🔥 ember

a low-latency, memory-efficient, distributed cache written in Rust. designed to outperform Redis on throughput, latency, and memory efficiency while keeping the codebase small and readable.

## features

- **resp3 protocol** — full compatibility with `redis-cli` and existing Redis clients
- **core commands** — GET, SET, DEL, EXISTS, EXPIRE, TTL, TYPE, DBSIZE, INFO with proper semantics
- **list operations** — LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN with O(1) push/pop and auto-cleanup
- **sorted sets** — ZADD (with NX/XX/GT/LT/CH flags), ZREM, ZSCORE, ZRANK, ZRANGE with WITHSCORES
- **sharded engine** — shared-nothing, thread-per-core design with no cross-shard locking on the hot path
- **active expiration** — background sampling cleans up expired keys without client access
- **memory tracking** — per-shard byte-level accounting with configurable memory limits
- **lru eviction** — approximate LRU via random sampling when memory pressure hits
- **pipelined connections** — multiple commands per read for high throughput

## quickstart

```bash
# build
cargo build --release

# run the server (defaults to 127.0.0.1:6379, no memory limit)
./target/release/ember-server

# or with a memory limit and eviction
./target/release/ember-server --max-memory 256M --eviction-policy allkeys-lru

# connect with redis-cli
redis-cli SET hello world    # => OK
redis-cli GET hello          # => "world"
redis-cli SET temp data EX 60
redis-cli TTL temp           # => 59
redis-cli DBSIZE             # => (integer) 2

# lists
redis-cli LPUSH mylist a b c # => (integer) 3
redis-cli LRANGE mylist 0 -1 # => 1) "c" 2) "b" 3) "a"
redis-cli RPOP mylist        # => "a"

# sorted sets
redis-cli ZADD board 100 alice 200 bob 150 charlie
redis-cli ZRANGE board 0 -1 WITHSCORES
redis-cli ZRANK board alice  # => (integer) 0
redis-cli ZSCORE board bob   # => "200"
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
