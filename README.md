<p align="center">
  <img src="ember-logo.png" alt="ember logo" width="200">
</p>

<p align="center">
  <a href="https://github.com/kacy/ember/actions"><img src="https://github.com/kacy/ember/workflows/ci/badge.svg" alt="build status"></a>
  <a href="https://crates.io/crates/ember-server"><img src="https://img.shields.io/crates/v/ember-server.svg" alt="crates.io"></a>
  <img src="https://img.shields.io/badge/rust-1.93%2B-blue.svg" alt="rust version">
  <a href="https://github.com/kacy/ember/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-MIT-green.svg" alt="license"></a>
  <a href="https://charts.emberdb.com"><img src="https://img.shields.io/badge/helm-charts.emberdb.com-0f1689.svg" alt="helm chart"></a>
  <a href="https://github.com/kacy/homebrew-ember"><img src="https://img.shields.io/badge/homebrew-kacy%2Fember-f9a825.svg" alt="homebrew"></a>
</p>

# ember

a low-latency, memory-efficient, distributed cache written in Rust. designed to outperform Redis on throughput, latency, and memory efficiency while keeping the codebase small and readable.

## features

- **resp3 protocol** — full compatibility with `redis-cli` and existing Redis clients
- **string commands** — GET, SET (with NX/XX/EX/PX), MGET, MSET, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, STRLEN
- **list operations** — LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, BLPOP, BRPOP
- **sorted sets** — ZADD (with NX/XX/GT/LT/CH), ZREM, ZSCORE, ZRANK, ZRANGE, ZCARD
- **hashes** — HSET, HGET, HGETALL, HDEL, HEXISTS, HLEN, HINCRBY, HKEYS, HVALS, HMGET
- **sets** — SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- **key commands** — DEL, EXISTS, EXPIRE, TTL, PEXPIRE, PTTL, PERSIST, TYPE, SCAN, KEYS, RENAME
- **server commands** — PING, ECHO, INFO, DBSIZE, FLUSHDB, BGSAVE, BGREWRITEAOF, AUTH, QUIT, CONFIG GET/SET
- **transactions** — MULTI, EXEC, DISCARD for atomic command batching
- **pub/sub** — SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, plus PUBSUB introspection
- **vector similarity search** — HNSW-backed approximate nearest neighbor search with cosine, L2, and inner product metrics (compile with `--features vector`)
- **protobuf storage** — schema-validated protobuf values with field-level access (compile with `--features protobuf`)
- **authentication** — `--requirepass` for redis-compatible AUTH (legacy and username/password forms)
- **tls support** — redis-compatible TLS on a separate port, with optional mTLS for client certificates
- **protected mode** — rejects non-loopback connections when no password is set on public binds
- **observability** — prometheus metrics (`--metrics-port`), enriched INFO with 6 sections, SLOWLOG command
- **sharded engine** — shared-nothing, thread-per-core design with no cross-shard locking
- **concurrent mode** — experimental DashMap-backed keyspace for lock-free GET/SET (2x faster than Redis)
- **cluster mode** — distributed keyspace across multiple nodes: 16384 hash slots, SWIM gossip, Raft-based topology, MOVED/ASK redirects, live slot migration
- **active expiration** — background sampling cleans up expired keys without client access
- **memory limits** — per-shard byte-level accounting with configurable limits
- **lru eviction** — approximate LRU via random sampling when memory pressure hits
- **persistence** — append-only file (AOF) and point-in-time snapshots
- **encryption at rest** — optional AES-256-GCM encryption for AOF and snapshot files (compile with `--features encryption`)
- **pipelining** — multiple commands per read for high throughput
- **interactive CLI** — `ember-cli` with REPL, syntax highlighting, tab-completion, inline hints, cluster subcommands, and built-in benchmark
- **graceful shutdown** — drains active connections on SIGINT/SIGTERM before exiting

## install

**homebrew (macOS/Linux)**

```bash
brew tap kacy/ember
brew install ember-db
```

this installs both `ember-server` and `ember-cli`.

**install script**

```bash
curl -fsSL https://raw.githubusercontent.com/kacy/ember/main/scripts/install.sh | bash
```

installs the latest `ember-server` and `ember-cli` to `/usr/local/bin`. useful flags:

```bash
# server only (skip ember-cli)
curl -fsSL https://raw.githubusercontent.com/kacy/ember/main/scripts/install.sh | bash -s -- --server-only

# pin to a specific version
... | bash -s -- --version v0.4.6

# custom install directory
... | bash -s -- --prefix ~/.local/bin
```

**from source**

```bash
git clone https://github.com/kacy/ember
cd ember
cargo build --release
```

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

# blocking list ops (work queues)
BLPOP tasks 5                   # blocks up to 5 seconds for an element
# in another terminal: LPUSH tasks job1 => unblocks with ["tasks", "job1"]

# transactions
MULTI
SET account:a 100
SET account:b 200
EXEC                            # => [OK, OK] — atomic batch

# TLS
ember-cli -p 6380 --tls --tls-insecure PING
```

## clustering

ember distributes keys across 16384 hash slots using CRC16. each node owns a non-overlapping range of slots. gossip (SWIM) propagates membership and health; Raft commits topology changes; MOVED and ASK redirects keep clients pointing at the right node during normal operation and live slot migration.

### quickstart

the fastest way to get a 3-node cluster running locally:

```bash
make cluster
```

this builds the release binaries if needed, starts nodes on ports 6379–6381, joins them together, and assigns all 16384 hash slots evenly. you should see something like:

```
starting node 1 (port 6379, bootstrap)...
starting node 2 (port 6380)...
starting node 3 (port 6381)...
  node :6379 ready
  node :6380 ready
  node :6381 ready
joining cluster...
assigning slots...

cluster_state:ok
cluster_slots_assigned:16384
cluster_known_nodes:3
cluster_size:3
```

to start nodes manually:

```bash
# node A — bootstrap a new cluster and own all slots initially
./target/release/ember-server --port 6379 --cluster-enabled --cluster-bootstrap \
  --data-dir ./data/node-a

# node B and C — join with --cluster-enabled, then CLUSTER MEET into A
./target/release/ember-server --port 6380 --cluster-enabled --data-dir ./data/node-b
./target/release/ember-server --port 6381 --cluster-enabled --data-dir ./data/node-c

# join B and C into the cluster
ember-cli -p 6380 cluster meet 127.0.0.1 6379
ember-cli -p 6381 cluster meet 127.0.0.1 6379

# redistribute slots (5461 slots each after rebalancing)
ember-cli -p 6379 cluster addslots $(seq 0 5460 | tr '\n' ' ')
ember-cli -p 6380 cluster addslots $(seq 5461 10922 | tr '\n' ' ')
ember-cli -p 6381 cluster addslots $(seq 10923 16383 | tr '\n' ' ')
```

connect to any node and start writing:

```bash
ember-cli -p 6379 SET hello world   # => OK
ember-cli -p 6380 GET hello         # => "world" or MOVED redirect

ember-cli -p 6379 cluster info      # cluster_state:ok, 16384 slots assigned
ember-cli -p 6380 cluster nodes     # 3 nodes, each with correct slot range
```

clients that support cluster mode (including `redis-cli --cluster`) follow `MOVED` redirects automatically. `ember-cli` routes each command to the correct node based on the key's CRC16 hash slot.

stop the cluster and clean up:

```bash
make cluster-stop   # SIGTERM the nodes, keep data
make cluster-clean  # stop + delete ./data/cluster/
```

### cluster commands

| command | description |
|---------|-------------|
| `CLUSTER INFO` | cluster state, slot counts, node count |
| `CLUSTER NODES` | list all known nodes with their slot ranges |
| `CLUSTER SLOTS` | slot-to-node mapping in array form |
| `CLUSTER KEYSLOT key` | return the hash slot for a key |
| `CLUSTER MYID` | return this node's ID |
| `CLUSTER MEET host port` | join another node into the cluster |
| `CLUSTER ADDSLOTS slot [slot ...]` | assign slots to this node |
| `CLUSTER DELSLOTS slot [slot ...]` | remove slot assignments from this node |
| `CLUSTER FORGET node-id` | remove a node from the cluster view |
| `CLUSTER SETSLOT slot IMPORTING\|MIGRATING\|NODE\|STABLE ...` | manage slot migration state |
| `CLUSTER COUNTKEYSINSLOT slot` | count keys owned by a slot on this node |
| `CLUSTER GETKEYSINSLOT slot count` | list keys in a slot (used during migration) |
| `MIGRATE host port key db timeout [COPY] [REPLACE]` | move a key to another node |
| `RESTORE key ttl payload` | receive a migrated key |
| `ASKING` | tell the server to honor an ASK redirect for the next command |

replication commands (REPLICATE, FAILOVER) are implemented — see the status table below.

see [ARCHITECTURE.md](ARCHITECTURE.md) for how clustering works under the hood (raft, gossip, hash slots, migration).

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
| `VADD_BATCH key DIM n elem1 f32... elem2 f32... [METRIC\|QUANT\|M\|EF]` | add multiple vectors in one command |
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

ember supports a TOML config file, environment variables, and CLI flags. the resolution order is: **defaults → config file → env vars → CLI flags** (highest priority wins).

```bash
# generate a config file with all defaults
ember-server --config-template > ember.toml

# start with a config file
ember-server --config ember.toml

# config file + CLI override
ember-server --config ember.toml --port 7777

# env vars use the EMBER_ prefix
EMBER_PORT=8888 ember-server --config ember.toml
```

see [`ember.example.toml`](ember.example.toml) for the full annotated config with all available options.

runtime changes via `CONFIG SET` persist in memory. use `CONFIG REWRITE` to flush them back to the config file.

### common flags

| flag | default | description |
|------|---------|-------------|
| `--config` / `-c` | — | path to TOML config file |
| `--config-template` | — | print default config to stdout and exit |
| `--host` | 127.0.0.1 | address to bind to |
| `--port` | 6379 | port to listen on |
| `--shards` | CPU cores | number of worker threads (shards) |
| `--max-memory` | unlimited | memory limit (e.g., 256M, 1G) |
| `--eviction-policy` | noeviction | `noeviction` or `allkeys-lru` |
| `--data-dir` | — | directory for persistence files |
| `--appendonly` | false | enable append-only file logging |
| `--appendfsync` | everysec | fsync policy: `always`, `everysec`, `no` |
| `--metrics-port` | — | prometheus /metrics and /health HTTP port |
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
| `--cluster-enabled` | false | enable cluster mode with gossip-based discovery and slot routing |
| `--cluster-bootstrap` | false | bootstrap a new cluster as a single node owning all 16384 slots |
| `--cluster-port-offset` | 10000 | port offset for the cluster gossip bus (data_port + offset) |
| `--cluster-node-timeout` | 5000 | node timeout in milliseconds for failure detection |

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
# add the helm repo
helm repo add ember https://charts.emberdb.com

# install with defaults (single replica, ClusterIP service)
helm install ember ember/ember

# install with custom settings
helm install ember ember/ember \
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
| concurrent | **1.9x faster** | **2.2-2.5x faster**\* | simple GET/SET workloads |
| sharded | **1.8x faster** | **2.1-2.5x faster**\* | all data types |

\*redis-benchmark, 64B values, P=16, 8 threads. take these comparisons with a grain of salt — ember is a small indie project; Redis and Dragonfly are battle-tested systems built by large teams over many years. see [bench/README.md](bench/README.md) for important caveats.

**highlights**:
- sharded mode: 1.76M SET/sec, 2.14M GET/sec (redis-benchmark, P=16)
- concurrent mode: 1.79M SET/sec, 2.14M GET/sec (redis-benchmark, P=16)
- p99 latency: 0.74ms SET, 0.70ms GET (P=1, concurrent mode)
- vector queries: 1.6k queries/sec (gRPC), 4-6x less memory than chromadb/pgvector/qdrant
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

ember offers two execution modes for the local keyspace:

**sharded mode** (default): thread-per-core with channel-based routing. each shard owns a partition of the keyspace with no cross-shard locking on the hot path. supports all data types and enables atomic multi-key operations.

**concurrent mode** (`--concurrent`): lock-free DashMap access. 2-3x faster than Redis for simple GET/SET but limited to string operations.

**cluster mode** (`--cluster-enabled`): multiple ember nodes, each running in sharded or concurrent mode, form a single distributed cache. the keyspace is partitioned into 16384 hash slots using CRC16 — each node owns a non-overlapping range. SWIM gossip propagates membership and health across nodes; topology changes (adding nodes, reassigning slots) go through a Raft consensus group so the cluster view is always consistent. clients are routed via MOVED redirects when a key lands on a different node, and ASK redirects during live slot migration.

contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## status

| phase | description | status |
|-------|-------------|--------|
| 1 | foundation (protocol, engine, expiration) | ✅ complete |
| 2 | persistence (AOF, snapshots, recovery) | ✅ complete |
| 3 | data types (sorted sets, lists, hashes, sets) | ✅ complete |
| 4 | clustering (raft, gossip, slots, migration) | ✅ complete |
| 5 | developer experience (observability, CLI, clients) | 🚧 in progress |
| 6 | replication and high availability | ✅ complete |
| 7 | security hardening | ✅ complete |
| 8 | production gaps (transactions, blocking ops, config) | ✅ complete |

phase 6 added leader/replica data streaming, `CLUSTER REPLICATE`, automatic failover via epoch-based elections, and `CLUSTER FAILOVER` for manual promotion. phase 7 added RESP key/value size limits and cluster transport HMAC-SHA256 auth. phase 8 filled critical production gaps: MULTI/EXEC/DISCARD transactions, BLPOP/BRPOP blocking list ops, and CONFIG GET/SET for monitoring tool compatibility.

**current**: 114 commands, 1,100+ tests, ~23k lines of code (~43k including tests and comments)

## security

see [SECURITY.md](SECURITY.md) for:
- reporting vulnerabilities
- security considerations for deployment
- recommended configuration

**note**: use `--requirepass` to enable authentication. protected mode is active by default when no password is set, rejecting non-loopback connections on public binds. for encryption at rest, see `--encryption-key-file` — key loss means data loss, so back up your key file separately from your data.

## license

MIT
