<p align="center">
  <img src="ember-logo.png" alt="ember logo" width="200">
</p>

<p align="center">
  <a href="https://github.com/kacy/ember/actions"><img src="https://github.com/kacy/ember/workflows/ci/badge.svg" alt="build status"></a>
  <a href="https://crates.io/crates/ember-server"><img src="https://img.shields.io/crates/v/ember-server.svg" alt="crates.io"></a>
  <img src="https://img.shields.io/badge/rust-1.75%2B-blue.svg" alt="rust version">
  <a href="https://github.com/kacy/ember/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="license"></a>
</p>

# ember

a low-latency, memory-efficient, distributed cache written in Rust. designed to outperform Redis on throughput, latency, and memory efficiency while keeping the codebase small and readable.

## features

- **resp3 protocol** — full compatibility with `redis-cli` and existing Redis clients
- **string commands** — GET, SET (with NX/XX/EX/PX), MGET, MSET, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN
- **list operations** — LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN
- **sorted sets** — ZADD (with NX/XX/GT/LT/CH), ZREM, ZSCORE, ZRANK, ZRANGE, ZCARD
- **hashes** — HSET, HGET, HGETALL, HDEL, HEXISTS, HLEN, HINCRBY, HKEYS, HVALS, HMGET
- **sets** — SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- **key commands** — DEL, EXISTS, EXPIRE, TTL, PEXPIRE, PTTL, PERSIST, TYPE, SCAN, KEYS, RENAME
- **server commands** — PING, ECHO, INFO, DBSIZE, FLUSHDB, BGSAVE, BGREWRITEAOF, AUTH, QUIT
- **pub/sub** — SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, plus PUBSUB introspection
- **authentication** — `--requirepass` for redis-compatible AUTH (legacy and username/password forms)
- **tls support** — redis-compatible TLS on a separate port, with optional mTLS for client certificates
- **protected mode** — rejects non-loopback connections when no password is set on public binds
- **observability** — prometheus metrics (`--metrics-port`), enriched INFO with 6 sections, SLOWLOG command
- **sharded engine** — shared-nothing, thread-per-core design with no cross-shard locking
- **concurrent mode** — experimental DashMap-backed keyspace for lock-free GET/SET (2x faster than Redis)
- **active expiration** — background sampling cleans up expired keys without client access
- **memory limits** — per-shard byte-level accounting with configurable limits
- **lru eviction** — approximate LRU via random sampling when memory pressure hits
- **persistence** — append-only file (AOF) and point-in-time snapshots
- **pipelining** — multiple commands per read for high throughput
- **graceful shutdown** — drains active connections on SIGINT/SIGTERM before exiting

## quickstart

```bash
# build
cargo build --release

# run the server (defaults to 127.0.0.1:6379)
./target/release/ember-server

# with memory limit and eviction
./target/release/ember-server --max-memory 256M --eviction-policy allkeys-lru

# with persistence
./target/release/ember-server --data-dir ./data --appendonly

# concurrent mode (experimental, 2x faster for GET/SET)
./target/release/ember-server --concurrent

# with TLS (runs alongside plain TCP)
./target/release/ember-server --tls-port 6380 \
  --tls-cert-file cert.pem --tls-key-file key.pem
```

```bash
# connect with redis-cli
redis-cli SET hello world       # => OK
redis-cli GET hello             # => "world"
redis-cli MSET a 1 b 2 c 3      # => OK
redis-cli MGET a b c            # => 1) "1" 2) "2" 3) "3"

# expiration
redis-cli SET temp data EX 60
redis-cli TTL temp              # => 59
redis-cli PTTL temp             # => 59000
redis-cli PERSIST temp          # => (integer) 1

# counters
redis-cli SET counter 10
redis-cli INCR counter          # => (integer) 11
redis-cli DECR counter          # => (integer) 10

# lists
redis-cli LPUSH mylist a b c    # => (integer) 3
redis-cli LRANGE mylist 0 -1    # => 1) "c" 2) "b" 3) "a"

# sorted sets
redis-cli ZADD board 100 alice 200 bob
redis-cli ZRANGE board 0 -1 WITHSCORES
redis-cli ZCARD board           # => (integer) 2

# hashes
redis-cli HSET user:1 name alice age 30
redis-cli HGET user:1 name      # => "alice"
redis-cli HGETALL user:1        # => 1) "name" 2) "alice" 3) "age" 4) "30"
redis-cli HINCRBY user:1 age 1  # => (integer) 31

# sets
redis-cli SADD tags rust cache fast   # => (integer) 3
redis-cli SMEMBERS tags               # => 1) "cache" 2) "fast" 3) "rust"
redis-cli SISMEMBER tags rust         # => (integer) 1
redis-cli SCARD tags                  # => (integer) 3
redis-cli SREM tags fast              # => (integer) 1

# iteration
redis-cli SCAN 0 MATCH "user:*" COUNT 100
redis-cli DBSIZE                # => (integer) 6
redis-cli FLUSHDB               # => OK

# TLS connection
redis-cli -p 6380 --tls --insecure PING
# or with cert verification
redis-cli -p 6380 --tls --cacert cert.pem PING
```

## configuration

| flag | default | description |
|------|---------|-------------|
| `--host` | 127.0.0.1 | address to bind to |
| `--port` | 6379 | port to listen on |
| `--shards` | CPU cores | number of worker threads (shards) |
| `--max-memory` | unlimited | memory limit (e.g., 256M, 1G) |
| `--eviction-policy` | noeviction | `noeviction` or `allkeys-lru` |
| `--data-dir` | — | directory for persistence files |
| `--appendonly` | false | enable append-only file logging |
| `--appendfsync` | everysec | fsync policy: `always`, `everysec`, `no` |
| `--metrics-port` | — | prometheus metrics HTTP port (disabled when not set) |
| `--slowlog-log-slower-than` | 10000 | log commands slower than N microseconds (-1 disables) |
| `--slowlog-max-len` | 128 | max entries in slow log ring buffer |
| `--concurrent` | false | use DashMap-backed keyspace (experimental, faster GET/SET) |
| `--requirepass` | — | require AUTH with this password before running commands |
| `--tls-port` | — | port for TLS connections (enables TLS when set) |
| `--tls-cert-file` | — | path to server certificate (PEM) |
| `--tls-key-file` | — | path to server private key (PEM) |
| `--tls-ca-cert-file` | — | path to CA certificate for client verification |
| `--tls-auth-clients` | no | require client certificates (`yes` or `no`) |

## build & development

```bash
make check    # fmt, clippy, tests
make build    # debug build
make release  # release build
make test     # run all tests
make docker-build  # build docker image
```

see [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow and code standards.

## project structure

```
crates/
  ember-server/       main server binary
  ember-core/         core engine (keyspace, types, sharding)
  ember-protocol/     RESP3 wire protocol
  ember-persistence/  AOF and snapshot durability
  ember-cluster/      raft consensus, gossip, slot management, migration
  ember-cli/          interactive CLI tool
```

## architecture

ember uses a shared-nothing, thread-per-core design inspired by [Dragonfly](https://github.com/dragonflydb/dragonfly). each cpu core owns a partition of the keyspace with no cross-thread synchronization on the hot path.

| metric | redis baseline | ember target |
|--------|---------------|--------------|
| throughput | ~100k ops/sec/core | 500k+ ops/sec/core |
| p99 latency | ~1ms | <200µs |
| memory/key | ~90 bytes overhead | <40 bytes |

## benchmarks

tested on GCP c2-standard-8 (8 vCPU Intel Xeon @ 3.10GHz). see [bench/README.md](bench/README.md) for full results.

| mode | vs redis | vs dragonfly | best for |
|------|----------|--------------|----------|
| concurrent | **1.8-2.1x faster** | **3.3-3.8x faster**\* | simple GET/SET workloads |
| sharded | ~0.9x (channel overhead) | **1.5-1.6x faster**\* | all data types |

\*take these comparisons with a grain of salt. ember is a small indie project; Redis and Dragonfly are battle-tested systems built by large teams over many years. see [bench/README.md](bench/README.md) for important caveats.

**highlights**:
- concurrent mode: 1.86M SET/sec, 2.49M GET/sec (simple GET/SET only)
- p99 latency: 0.4ms (same as redis)
- memory: ~161 bytes/key (redis: ~105 bytes/key)

```bash
./bench/bench-quick.sh   # quick sanity check
./bench/bench.sh         # full comparison vs redis
```

## architecture

ember offers two execution modes:

**sharded mode** (default): thread-per-core with channel-based routing. supports all data types (lists, hashes, sets, sorted sets). has channel overhead but enables atomic multi-key operations.

**concurrent mode** (`--concurrent`): lock-free DashMap access. 2x faster than sharded mode but only supports string operations.

contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## status

| phase | description | status |
|-------|-------------|--------|
| 1 | foundation (protocol, engine, expiration) | ✅ complete |
| 2 | persistence (AOF, snapshots, recovery) | ✅ complete |
| 3 | data types (sorted sets, lists, hashes, sets) | ✅ complete |
| 4 | clustering (raft, gossip, slots, migration) | ✅ complete |
| 5 | developer experience (observability, CLI, clients) | 🚧 in progress |

**current**: 85 commands, 701 tests, ~23k lines of code

## security

see [SECURITY.md](SECURITY.md) for:
- reporting vulnerabilities
- security considerations for deployment
- recommended configuration

**note**: use `--requirepass` to enable authentication. protected mode is active by default when no password is set, rejecting non-loopback connections on public binds.

## license

MIT
