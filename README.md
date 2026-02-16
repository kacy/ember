<p align="center">
  <img src="ember-logo.png" alt="ember logo" width="200">
</p>

<p align="center">
  <a href="https://github.com/kacy/ember/actions"><img src="https://github.com/kacy/ember/workflows/ci/badge.svg" alt="build status"></a>
  <a href="https://crates.io/crates/ember-server"><img src="https://img.shields.io/crates/v/ember-server.svg" alt="crates.io"></a>
  <img src="https://img.shields.io/badge/rust-1.93%2B-blue.svg" alt="rust version">
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
- **vector similarity search** — HNSW-backed approximate nearest neighbor search with cosine, L2, and inner product metrics (compile with `--features vector`)
- **protobuf storage** — schema-validated protobuf values with field-level access (compile with `--features protobuf`)
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
- **encryption at rest** — optional AES-256-GCM encryption for AOF and snapshot files (compile with `--features encryption`)
- **pipelining** — multiple commands per read for high throughput
- **interactive CLI** — `ember-cli` with REPL, syntax highlighting, tab-completion, inline hints, cluster subcommands, and built-in benchmark
- **graceful shutdown** — drains active connections on SIGINT/SIGTERM before exiting

## quickstart

```bash
# build server and cli
cargo build --release

# run the server (defaults to 127.0.0.1:6379)
./target/release/ember-server

# with memory limit and eviction
./target/release/ember-server --max-memory 256M --eviction-policy allkeys-lru

# with persistence
./target/release/ember-server --data-dir ./data --appendonly

# with encryption at rest (requires --features encryption)
./target/release/ember-server --data-dir ./data --appendonly \
  --encryption-key-file /path/to/keyfile

# concurrent mode (experimental, 2x faster for GET/SET)
./target/release/ember-server --concurrent

# with TLS (runs alongside plain TCP)
./target/release/ember-server --tls-port 6380 \
  --tls-cert-file cert.pem --tls-key-file key.pem
```

ember speaks RESP3, so `redis-cli` works as a drop-in replacement — but `ember-cli` adds syntax highlighting, tab-completion, inline hints, and auto-reconnect.

```bash
# ember-cli: interactive REPL with autocomplete and help
ember-cli                       # starts REPL at 127.0.0.1:6379>
ember-cli -H 10.0.0.1 -p 6380  # connect to a different host
ember-cli -a mypassword         # authenticate

# one-shot mode
ember-cli SET hello world       # => OK
ember-cli GET hello             # => "hello"

# cluster management subcommands
ember-cli cluster info          # cluster state
ember-cli cluster nodes         # list nodes
ember-cli cluster meet 10.0.0.1 6379

# built-in benchmark
ember-cli benchmark -n 100000 -c 50 -P 16

# redis-cli works too — same protocol, same port
redis-cli SET hello world       # => OK
redis-cli GET hello             # => "world"
```

```bash
# everything below works with either ember-cli or redis-cli
SET counter 10
INCR counter                    # => (integer) 11

LPUSH mylist a b c              # => (integer) 3
LRANGE mylist 0 -1              # => 1) "c" 2) "b" 3) "a"

ZADD board 100 alice 200 bob
ZRANGE board 0 -1 WITHSCORES

HSET user:1 name alice age 30
HGETALL user:1                  # => 1) "name" 2) "alice" 3) "age" 4) "30"

SADD tags rust cache fast       # => (integer) 3
SMEMBERS tags                   # => 1) "cache" 2) "fast" 3) "rust"

SET temp data EX 60
TTL temp                        # => 59

SCAN 0 MATCH "user:*" COUNT 100
DBSIZE                          # => (integer) 6

# TLS
ember-cli -p 6380 --tls --tls-insecure PING
```

## protobuf storage

ember can store schema-validated protobuf messages and access individual fields server-side. compile with `--features protobuf` to enable.

```bash
# build with protobuf support
cargo build --release --features protobuf
```

**commands**:

| command | description |
|---------|-------------|
| `PROTO.REGISTER name <descriptor>` | register a compiled FileDescriptorSet |
| `PROTO.SET key type_name <data> [EX s] [PX ms] [NX\|XX]` | store a validated protobuf value |
| `PROTO.GET key` | retrieve the full encoded message |
| `PROTO.TYPE key` | return the message type name |
| `PROTO.SCHEMAS` | list all registered schema names |
| `PROTO.DESCRIBE name` | list message types in a schema |
| `PROTO.GETFIELD key field_path` | read a single field (dot-separated nested paths) |
| `PROTO.SETFIELD key field_path value` | update a single scalar field |
| `PROTO.DELFIELD key field_path` | clear a field to its default value |

field-level operations decode/mutate/re-encode on the server, so clients don't need protobuf libraries for simple reads and writes. nested paths use dot notation (e.g., `address.city`). complex types (repeated, map, nested messages) require `PROTO.GET`/`PROTO.SET` for full replacement.

## vector similarity search

ember supports HNSW-backed approximate nearest neighbor search for building recommendation systems, semantic search, and RAG pipelines. compile with `--features vector` to enable.

```bash
# build with vector support
cargo build --release --features vector
```

**commands**:

| command | description |
|---------|-------------|
| `VADD key element f32 [f32 ...] [METRIC COSINE\|L2\|IP] [QUANT F32\|F16\|Q8] [M n] [EF n]` | add a vector to the set |
| `VSIM key f32 [f32 ...] COUNT k [EF n] [WITHSCORES]` | k nearest neighbors |
| `VREM key element` | remove a vector |
| `VGET key element` | retrieve stored vector values |
| `VCARD key` | number of vectors in the set |
| `VDIM key` | dimensionality of the vector set |
| `VINFO key` | metadata: dim, count, metric, quantization, M, ef |

index configuration (METRIC, QUANT, M, EF) is set on the first VADD and locked after that. dimension is inferred from the first vector's length. each key owns its own independent HNSW index.

```bash
# store some embeddings
VADD docs doc1 0.1 0.2 0.3 METRIC COSINE
VADD docs doc2 0.9 0.1 0.0
VADD docs doc3 0.0 0.8 0.2

# find 2 nearest neighbors
VSIM docs 0.1 0.3 0.2 COUNT 2 WITHSCORES
# => 1) "doc1" 2) "0.05" 3) "doc3" 4) "0.12"

VCARD docs                      # => (integer) 3
VDIM docs                       # => (integer) 3
VINFO docs                      # => metric, quantization, dim, count, M, ef
```

distance metrics: **COSINE** (default), **L2** (squared euclidean), **IP** (inner product). quantization: **F32** (default), **F16** (half precision), **Q8** (8-bit integer). lower precision uses less memory at a small accuracy cost.

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
| `--encryption-key-file` | — | path to 32-byte key file for AES-256-GCM encryption at rest (requires `--features encryption`) |

## build & development

```bash
make check    # fmt, clippy, tests
make build    # debug build
make release  # release build
make test     # run all tests
make docker-build   # build docker image
make helm-lint      # validate helm chart
make helm-template  # render helm templates
```

see [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow and code standards.

## kubernetes

deploy to kubernetes with [Helm](https://helm.sh):

```bash
# install with defaults (single replica, ClusterIP service)
helm install ember helm/ember

# install with custom settings
helm install ember helm/ember \
  --set ember.maxMemory=512M \
  --set ember.evictionPolicy=allkeys-lru \
  --set ember.appendonly=true

# connect via port-forward
kubectl port-forward svc/ember 6379:6379
ember-cli
```

see [helm/ember/values.yaml](helm/ember/values.yaml) for all configurable values.

## project structure

```
crates/
  ember-server/       main server binary
  ember-core/         core engine (keyspace, types, sharding)
  ember-protocol/     RESP3 wire protocol
  ember-persistence/  AOF and snapshot durability
  ember-cluster/      raft consensus, gossip, slot management, migration
  ember-cli/          interactive CLI client (REPL, cluster subcommands, benchmark)
```

## benchmarks

tested on GCP c2-standard-8 (8 vCPU Intel Xeon @ 3.10GHz). see [bench/README.md](bench/README.md) for full results.

| mode | vs redis | vs dragonfly | best for |
|------|----------|--------------|----------|
| concurrent | **1.8x faster** | **2.0-2.4x faster**\* | simple GET/SET workloads |
| sharded | **1.2-1.3x faster** | **1.4-1.8x faster**\* | all data types |

\*redis-benchmark, 64B values, P=16, 8 threads. take these comparisons with a grain of salt — ember is a small indie project; Redis and Dragonfly are battle-tested systems built by large teams over many years. see [bench/README.md](bench/README.md) for important caveats.

**highlights**:
- sharded mode: 1.22M SET/sec, 1.52M GET/sec (redis-benchmark, P=16)
- concurrent mode: 1.76M SET/sec, 2.18M GET/sec (redis-benchmark, P=16)
- p99 latency: 0.61ms SET, 0.56ms GET (P=1, concurrent mode)
- vector queries: 1.5k queries/sec (gRPC), 3-5x less memory than chromadb/pgvector/qdrant
- memory: 128 bytes/key concurrent, 208 bytes/key sharded (redis: 173 bytes/key)

```bash
./bench/bench-quick.sh       # quick sanity check
./bench/compare-redis.sh     # redis-benchmark comparison
./bench/bench-memtier.sh     # memtier_benchmark comparison
./bench/bench-vector.sh      # vector similarity (ember vs chromadb vs pgvector vs qdrant)
./bench/bench-grpc.sh        # gRPC vs RESP3
./bench/bench-all.sh         # run everything
```

## architecture

ember offers two execution modes:

**sharded mode** (default): thread-per-core with channel-based routing. supports all data types (lists, hashes, sets, sorted sets). has channel overhead but enables atomic multi-key operations.

**concurrent mode** (`--concurrent`): lock-free DashMap access. 2-3x faster than Redis but only supports string operations.

contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## status

| phase | description | status |
|-------|-------------|--------|
| 1 | foundation (protocol, engine, expiration) | ✅ complete |
| 2 | persistence (AOF, snapshots, recovery) | ✅ complete |
| 3 | data types (sorted sets, lists, hashes, sets) | ✅ complete |
| 4 | clustering (raft, gossip, slots, migration) | ✅ complete |
| 5 | developer experience (observability, CLI, clients) | 🚧 in progress |

**current**: 106 commands, 796+ tests, ~31k lines of code (excluding tests)

## security

see [SECURITY.md](SECURITY.md) for:
- reporting vulnerabilities
- security considerations for deployment
- recommended configuration

**note**: use `--requirepass` to enable authentication. protected mode is active by default when no password is set, rejecting non-loopback connections on public binds. for encryption at rest, see `--encryption-key-file` — key loss means data loss, so back up your key file separately from your data.

## license

MIT
