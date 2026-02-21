# benchmarks

performance benchmarks for ember comparing against Redis and Dragonfly.

## results summary

tested on GCP c2-standard-8 (8 vCPU Intel Xeon @ 3.10GHz), Ubuntu 22.04.

### throughput (requests/sec)

#### redis-benchmark (100k requests, 50 clients, 8 threads)

| test | ember concurrent | ember sharded | redis | dragonfly |
|------|------------------|---------------|-------|-----------|
| SET (3B, P=16) | **1,894,037** | 1,542,261 | 1,042,833 | 910,981 |
| GET (3B, P=16) | **2,010,920** | 1,932,423 | 1,151,264 | 1,022,204 |
| SET (64B, P=16) | **1,794,571** | 1,541,569 | 953,447 | 811,871 |
| GET (64B, P=16) | **2,135,829** | 1,792,803 | 1,112,355 | 849,355 |
| SET (1KB, P=16) | **1,015,070** | 668,186 | 610,780 | 692,855 |
| GET (1KB, P=16) | **1,761,649** | 1,670,383 | 720,661 | 329,390 |
| SET (64B, P=1) | 132,802 | **200,000** | 99,900 | 200,000 |
| GET (64B, P=1) | 133,333 | **199,600** | 99,900 | 200,000 |

#### memtier_benchmark (4 threads, 12 clients/thread, 50k req/client)

| test | ember concurrent | ember sharded | redis | dragonfly |
|------|------------------|---------------|-------|-----------|
| SET (64B, P=16) | **1,483,771** | 950,250 | 1,073,362 | 965,371 |
| GET (64B, P=16) | **1,808,080** | 1,285,581 | 1,293,518 | 1,017,445 |
| mixed 1:10 (64B, P=16) | **1,722,906** | 1,109,614 | 1,296,554 | 999,935 |
| mixed 1:1 (64B, P=16) | 371,076 | — | **1,172,227** | 960,709 |
| SET (1KB, P=16) | **906,045** | 481,431 | 605,295 | 673,908 |
| GET (1KB, P=16) | 841,580 | 578,775 | 692,457 | **1,008,558** |
| SET (64B, P=1) | **283,101** | 191,251 | 214,235 | 119,570 |
| GET (64B, P=1) | 180,897 | **311,333** | 149,223 | 120,020 |

### vs redis (redis-benchmark, 64B P=16)

| mode | SET | GET | notes |
|------|-----|-----|-------|
| ember concurrent | **1.9x** | **1.9x** | best for simple GET/SET workloads |
| ember sharded | **1.6x** | **1.6x** | supports all data types, beats redis at all test points |

### vs dragonfly (redis-benchmark, 64B P=16)

| mode | SET | GET | notes |
|------|-----|-----|-------|
| ember concurrent | **2.2x** | **2.5x** | pipelined |
| ember sharded | **1.9x** | **2.1x** | consistent wins across value sizes |

**important caveat**: these benchmarks should be taken with a grain of salt. ember is a small indie project built for learning and experimentation. Redis and Dragonfly are production-grade systems developed by large teams over many years, battle-tested at massive scale.

dragonfly in particular offers features ember simply doesn't have:

- full Redis API compatibility (200+ commands vs ember's ~114)
- sophisticated memory management (dashtable for ~25% of Redis memory usage)
- Lua scripting
- fork-free snapshotting
- streams, modules, and much more

ember's concurrent mode shows higher throughput on simple GET/SET because it's architecturally minimal — essentially a concurrent hashmap with RESP3 parsing. this simplicity is a tradeoff, not an advantage. for anything resembling production use, Redis and Dragonfly are the sensible choices. ember exists primarily as a learning project and for workloads where simplicity matters more than features.

### latency (P=16, 48 clients, memtier_benchmark)

| server | p99 SET | p99 GET |
|--------|---------|---------|
| ember concurrent | 1.575ms | 1.423ms |
| ember sharded | 1.871ms | 1.655ms |
| redis | 1.215ms | 0.903ms |
| dragonfly | 1.575ms | 1.487ms |

### latency (P=1, 48 clients, memtier_benchmark)

| server | p99 SET | p99 GET |
|--------|---------|---------|
| ember concurrent | 0.735ms | 0.655ms |
| ember sharded | 1.007ms | 1.039ms |
| redis | 0.599ms | 0.583ms |
| dragonfly | 1.207ms | 1.215ms |

### memory usage (~1M keys, 64B values)

| server | per key |
|--------|---------|
| ember concurrent | 128 B |
| ember sharded | 208 B |
| redis | 173 B |

ember concurrent mode is more memory-efficient than redis for string keys. sharded mode uses additional metadata for shard routing.

### with persistence enabled

AOF with `appendfsync everysec` (default):

| mode | SET throughput | vs no-persistence |
|------|----------------|-------------------|
| ember concurrent | ~1.7M/s | ~91% of baseline |
| ember sharded | ~850K/s | ~95% of baseline |
| redis | ~950K/s | ~95% of baseline |

persistence overhead is minimal with everysec fsync. `appendfsync always` has significant impact (~50% reduction).

### with encryption enabled

AES-256-GCM encryption at rest (AOF and snapshots). requires building with `--features encryption`:

| test | pipeline | plaintext | encrypted | overhead |
|------|----------|-----------|-----------|----------|
| SET | P=16 | 1.01M/s | 439k/s | 56% |
| GET | P=16 | 1.95M/s | 1.87M/s | 3% |
| SET | P=1 | 200k/s | 160k/s | 20% |
| GET | P=1 | 200k/s | 200k/s | 0% |

encryption only affects persistence writes — GET throughput is unchanged at P=1 since reads come from the in-memory keyspace. at higher pipeline depths, the throughput difference reflects AES-256-GCM overhead on the write path.

### vector similarity

ember vs chromadb vs pgvector vs qdrant. 100k random vectors, 128 dimensions, cosine metric, k=10 kNN search.
HNSW index: M=16, ef_construction=64 for all systems. tested on GCP c2-standard-8.

| metric | ember (RESP) | ember (gRPC) | chromadb | pgvector | qdrant |
|--------|-------------|-------------|----------|----------|--------|
| insert (vectors/sec) | 1,488 | 2,443 | 3,753 | 1,590 | **7,500** |
| query (queries/sec) | 1,212 | **1,407** | 385 | 849 | 597 |
| query p50 (ms) | 0.82ms | **0.70ms** | 2.59ms | 1.16ms | 1.67ms |
| query p99 (ms) | 0.97ms | **0.91ms** | 2.84ms | 1.54ms | 1.91ms |
| memory (MB) | **34 MB** | — | 124 MB | 178 MB | 132 MB |

ember's query throughput is 3.2x chromadb, 1.4x pgvector, and 2.1x qdrant, with 3-5x lower memory usage. gRPC queries are 20% faster than RESP due to lower serialization overhead. insert throughput uses VADD_BATCH (batches of 500 vectors per command) — the gRPC path benefits most since packed floats avoid string parsing entirely.

#### SIFT1M recall accuracy (128-dim, 1M vectors, 10k queries)

*run `bench/bench-vector.sh --sift --qdrant` on a c2-standard-8 (32GB) to populate. SIFT1M is 1M × 128 × 4B = 512MB raw data; ember HNSW peaks around 1.5GB RSS, well within 32GB.*

### pipeline scaling (ember sharded)

throughput vs pipeline depth on sharded mode, showing how the dispatch-collect pattern scales with batching. tested with redis-benchmark, 50 clients, 8 threads.

| pipeline depth | SET (ops/sec) | GET (ops/sec) |
|----------------|---------------|---------------|
| P=1 | 200,000 | 199,920 |
| P=4 | 726,872 | 753,108 |
| P=16 | 1,506,946 | 1,932,479 |
| P=64 | 1,895,932 | 3,369,087 |
| P=256 | 2,014,410 | 4,064,911 |

throughput scales monotonically with pipeline depth. the batch dispatch optimization (PR #232) groups commands by target shard and sends one channel message per shard, eliminating head-of-line blocking at high pipeline depths.

### transaction overhead

MULTI/SET/EXEC vs bare SET to quantify the cost of transaction wrapping. transactions force serial execution (no parallel shard dispatch), so they represent a worst-case for the sharded architecture.

| test | ember sharded | redis |
|------|---------------|-------|
| bare SET (P=1) | 82,713 | 105,597 |
| MULTI/SET/EXEC (P=1) | 91,659 | 89,928 |
| overhead | -10.8% (faster) | 14.8% |

ember's transaction overhead is negative — MULTI/SET/EXEC is slightly *faster* than bare SET. this is likely because the transaction path avoids per-command channel dispatch overhead.

the built-in CLI benchmark also supports transaction workloads:

```bash
# compare SET vs MULTI/SET/EXEC
ember-cli benchmark -t set,multi -n 100000 -c 50
```

### scaling efficiency

| cores | ember sharded SET | scaling factor |
|-------|-------------------|----------------|
| 1 | ~100k | 1.0x |
| 8 | ~1.5M | 15x |

sharded mode scales super-linearly with cores for pipelined workloads thanks to the dispatch-collect pipeline pattern. concurrent mode uses a global DashMap and doesn't scale with core count but has lower per-request overhead.

### gRPC vs RESP3

standard SET/GET operations comparing RESP3 (redis-py) against gRPC (ember-py). 100k requests, 64B values.

| test | ops/sec | p50 (ms) | p99 (ms) |
|------|---------|----------|----------|
| RESP3 SET (sequential) | 11,677 | 0.084 | 0.117 |
| RESP3 GET (sequential) | 12,668 | 0.078 | 0.100 |
| RESP3 SET (pipelined) | 80,169 | 0.012 | 0.014 |
| RESP3 GET (pipelined) | **108,934** | 0.009 | 0.010 |
| gRPC SET (unary) | 5,229 | 0.184 | 0.258 |
| gRPC GET (unary) | 5,144 | 0.185 | 0.257 |

RESP3 pipelining is the fastest option for bulk operations (5-7x over sequential, 14-17x over gRPC unary). gRPC unary calls have higher per-request overhead from HTTP/2 framing but provide type-safe APIs. for vector queries where gRPC uses streaming RPCs, it's 16% faster than RESP (see vector table above).

### pub/sub throughput

publish throughput and fan-out delivery rate across subscriber counts and message sizes. 10k messages per test.

| test | pub msg/s | fanout msg/s | p99 (ms) |
|------|-----------|--------------|----------|
| 1 sub, 64B, SUBSCRIBE | 9,413 | 9,413 | 0.22 |
| 10 sub, 64B, SUBSCRIBE | 1,788 | 17,880 | 3.35 |
| 100 sub, 64B, SUBSCRIBE | 392 | 25,511 | 28.45 |
| 1 sub, 1KB, SUBSCRIBE | 8,629 | 8,629 | 0.23 |
| 10 sub, 1KB, SUBSCRIBE | 1,728 | 17,279 | 3.42 |
| 100 sub, 1KB, SUBSCRIBE | 394 | 24,748 | 29.57 |
| 10 sub, 64B, PSUBSCRIBE | 1,722 | 17,225 | 3.38 |
| 100 sub, 64B, PSUBSCRIBE | 395 | 24,506 | 29.83 |

fan-out throughput scales well — total message delivery rate increases from 9.4k to 25.5k msg/s as subscribers grow from 1 to 100. per-publisher throughput drops proportionally since each message fans out to more receivers. PSUBSCRIBE performs nearly identically to SUBSCRIBE. message size (64B vs 1KB) has minimal impact.

### protobuf storage overhead

PROTO.* commands vs raw SET/GET with identical data. measures the cost of server-side schema validation and field-level access. 100k requests, bench.User message (28 bytes).

| test | ops/sec | p50 (ms) | p99 (ms) |
|------|---------|----------|----------|
| raw SET | 11,667 | 0.085 | 0.109 |
| PROTO.SET | 12,387 | 0.080 | 0.114 |
| raw GET | 12,639 | 0.079 | 0.106 |
| PROTO.GET | 12,028 | 0.082 | 0.108 |
| PROTO.GETFIELD | 12,522 | 0.079 | 0.100 |
| PROTO.SETFIELD | 12,124 | 0.081 | 0.107 |

schema validation overhead is within noise (~0-10%). PROTO.SET is actually slightly faster than raw SET in this run (within variance). field-level access (GETFIELD/SETFIELD) adds negligible overhead vs full message operations.

### memory by data type

per-key memory overhead across data types. string: 1M keys, 64B values. hash: 100k keys, 5 fields each. sorted set: 100k members. vector: 100k 128-dim vectors.

| data type | ember concurrent | ember sharded | redis |
|-----------|------------------|---------------|-------|
| string (64B) | **128 B/key** | 208 B/key | 173 B/key |
| hash (5 fields) | — | 555 B/key | **170 B/key** |
| sorted set | — | 115 B/member | **111 B/member** |
| vector (128-dim) | — | 853 B/vector | — |

ember concurrent mode is the most memory-efficient for strings (128 B/key vs redis 173 B/key) due to the DashMap structure. sharded mode uses more memory per key due to channel routing metadata. hash and sorted set commands only run in sharded mode. redis is more memory-efficient for complex types thanks to ziplist/listpack compact encodings.

## execution modes

ember offers two modes with different tradeoffs:

**concurrent mode** (`--concurrent`):
- uses DashMap for lock-free access
- 1.9x faster than redis for GET/SET (redis-benchmark, pipelined)
- only supports string operations
- best for simple key-value workloads

**sharded mode** (default):
- each CPU core owns a keyspace partition
- requests routed via tokio channels with dispatch-collect pipelining
- supports all data types (lists, hashes, sets, sorted sets)
- 1.6x redis throughput with pipelining, 2.0x without

## running benchmarks

### quick start (local)

```bash
# build with jemalloc for best performance
cargo build --release -p ember-server --features jemalloc

# for encryption benchmarks, also enable the encryption feature
cargo build --release -p ember-server --features jemalloc,encryption

# quick sanity check (ember only)
./bench/bench-quick.sh

# full comparison vs redis
./bench/bench.sh

# memory usage test
./bench/bench-memory.sh

# encryption overhead (requires --features encryption)
./bench/bench-encryption.sh

# comprehensive comparison using redis-benchmark (redis + dragonfly)
./bench/compare-redis.sh

# comprehensive comparison using memtier_benchmark (redis + dragonfly)
./bench/bench-memtier.sh

# vector similarity benchmark (requires --features vector, docker for full comparison)
cargo build --release -p ember-server --features jemalloc,vector
./bench/bench-vector.sh

# vector benchmark (ember only, no docker required)
./bench/bench-vector.sh --ember-only

# vector benchmark with qdrant
./bench/bench-vector.sh --qdrant

# SIFT1M recall accuracy
./bench/bench-vector.sh --sift

# gRPC vs RESP3 comparison (requires --features grpc + ember-py)
cargo build --release -p ember-server --features jemalloc,grpc
./bench/bench-grpc.sh

# pub/sub throughput
./bench/bench-pubsub.sh

# protobuf storage overhead (requires --features protobuf + protoc)
cargo build --release -p ember-server --features jemalloc,protobuf
./bench/bench-proto.sh

# run everything (builds with all features automatically)
./bench/bench-all.sh
```

### cloud VM benchmarking

for reproducible results, use a dedicated VM:

```bash
# create GCP instance
gcloud compute instances create ember-bench \
  --zone=us-central1-a \
  --machine-type=c2-standard-8 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud

# bootstrap (installs rust, redis, memtier_benchmark, dragonfly)
gcloud compute ssh ember-bench --zone=us-central1-a -- 'bash -s' < ./bench/setup-vm.sh

# setup for vector benchmarks (docker, python deps, qdrant)
gcloud compute ssh ember-bench --zone=us-central1-a -- 'bash -s' < ./bench/setup-vm-vector.sh

# run all benchmarks
gcloud compute ssh ember-bench --zone=us-central1-a
cd ember
./bench/bench-all.sh          # runs everything sequentially

# or run individual suites
./bench/compare-redis.sh      # redis-benchmark suite
./bench/bench-memtier.sh      # memtier_benchmark suite
./bench/bench-memory.sh       # memory comparison
./bench/bench-vector.sh --qdrant  # vector comparison
./bench/bench-grpc.sh         # gRPC vs RESP3
./bench/bench-pubsub.sh       # pub/sub
./bench/bench-proto.sh        # protobuf overhead

# cleanup
gcloud compute instances delete ember-bench --zone=us-central1-a
```

## scripts

| script | description |
|--------|-------------|
| `bench-all.sh` | run all benchmarks sequentially (builds with all features) |
| `bench.sh` | full benchmark: ember (sharded + concurrent) vs redis |
| `bench-quick.sh` | quick sanity check (~10 seconds) |
| `bench-memory.sh` | memory usage across data types (string, hash, zset, vector) |
| `compare-redis.sh` | comprehensive comparison using redis-benchmark |
| `bench-memtier.sh` | comprehensive comparison using memtier_benchmark |
| `bench-encryption.sh` | encryption at rest overhead (plaintext vs AES-256-GCM) |
| `bench-vector.sh` | vector similarity: ember vs chromadb vs pgvector vs qdrant |
| `bench-grpc.sh` | gRPC vs RESP3 standard operations |
| `bench-pubsub.sh` | pub/sub throughput and fan-out latency |
| `bench-proto.sh` | protobuf storage overhead (PROTO.* vs raw SET/GET) |
| `setup-vm.sh` | bootstrap dependencies on fresh ubuntu VM |
| `setup-vm-vector.sh` | additional dependencies for vector benchmarks |

## configuration

```bash
# customize redis-benchmark parameters
BENCH_REQUESTS=1000000 BENCH_THREADS=16 ./bench/compare-redis.sh

# customize memtier_benchmark parameters
MEMTIER_THREADS=8 MEMTIER_CLIENTS=16 MEMTIER_REQUESTS=20000 ./bench/bench-memtier.sh

# customize memory test
STRING_KEYS=5000000 VALUE_SIZE=128 ./bench/bench-memory.sh
```

## environment variables

### compare-redis.sh (redis-benchmark)

| variable | default | description |
|----------|---------|-------------|
| `EMBER_CONCURRENT_PORT` | 6379 | ember concurrent mode port |
| `EMBER_SHARDED_PORT` | 6380 | ember sharded mode port |
| `REDIS_PORT` | 6399 | redis port |
| `DRAGONFLY_PORT` | 6389 | dragonfly port |
| `BENCH_REQUESTS` | 100000 | requests per test |
| `BENCH_CLIENTS` | 50 | concurrent connections |
| `BENCH_PIPELINE` | 16 | pipeline depth |
| `BENCH_THREADS` | CPU cores | redis-benchmark threads |

### bench-memtier.sh (memtier_benchmark)

| variable | default | description |
|----------|---------|-------------|
| `EMBER_CONCURRENT_PORT` | 6379 | ember concurrent mode port |
| `EMBER_SHARDED_PORT` | 6380 | ember sharded mode port |
| `REDIS_PORT` | 6399 | redis port |
| `DRAGONFLY_PORT` | 6389 | dragonfly port |
| `MEMTIER_THREADS` | 4 | memtier threads |
| `MEMTIER_CLIENTS` | 12 | clients per thread (48 total) |
| `MEMTIER_REQUESTS` | 10000 | requests per client (480k total) |
| `MEMTIER_PIPELINE` | 16 | pipeline depth |

## built-in benchmark

ember also ships a built-in benchmark tool that doesn't require external dependencies:

```bash
# basic benchmark (100k requests, 50 clients)
ember-cli benchmark

# high-throughput with pipelining
ember-cli benchmark -n 1000000 -c 50 -P 16

# specific workloads
ember-cli benchmark -t set,get,ping -d 128

# transaction overhead (MULTI/SET/EXEC vs bare SET)
ember-cli benchmark -t set,multi -n 100000 -c 50
```

see `ember-cli benchmark --help` for all options.

## micro-benchmarks

for criterion micro-benchmarks:

```bash
cargo bench -p emberkv-core   # keyspace + engine
cargo bench -p ember-protocol # RESP3 parse/serialize
```
