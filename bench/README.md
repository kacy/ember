# benchmarks

performance benchmarks for ember comparing against Redis and Dragonfly.

## results summary

tested on GCP c2-standard-8 (8 vCPU Intel Xeon @ 3.10GHz), Ubuntu 22.04.

### throughput (requests/sec)

#### redis-benchmark (100k requests, 50 clients, 8 threads)

| test | ember concurrent | ember sharded | redis | dragonfly |
|------|------------------|---------------|-------|-----------|
| SET (3B, P=16) | **1,735,172** | 1,152,574 | 1,032,082 | 903,207 |
| GET (3B, P=16) | **2,096,333** | 1,437,085 | 1,151,080 | 1,012,202 |
| SET (64B, P=16) | **1,760,000** | 1,221,707 | 971,961 | 830,942 |
| GET (64B, P=16) | **2,181,565** | 1,522,666 | 1,137,636 | 856,615 |
| SET (1KB, P=16) | **993,059** | 710,737 | 588,764 | 694,400 |
| GET (1KB, P=16) | **1,728,534** | 1,284,692 | 667,913 | 332,092 |
| SET (64B, P=1) | 133,155 | 133,155 | 99,900 | **199,600** |
| GET (64B, P=1) | 132,978 | 133,155 | 100,000 | **199,203** |

#### memtier_benchmark (4 threads, 12 clients/thread, 50k req/client)

| test | ember concurrent | ember sharded | redis | dragonfly |
|------|------------------|---------------|-------|-----------|
| SET (64B, P=16) | 1,587,466 | 1,152,867 | **2,073,339** | 982,182 |
| GET (64B, P=16) | **1,025,795** | 854,908 | 1,297,126 | 727,549 |
| mixed 1:10 (64B, P=16) | **7,174,609** | 842,783 | 1,338,078 | 1,027,003 |
| mixed 1:1 (64B, P=16) | 1,697,929 | 1,219,427 | **2,309,158** | 993,407 |
| SET (1KB, P=16) | **998,064** | 972,829 | 589,156 | 512,599 |
| GET (1KB, P=16) | 907,113 | **1,272,418** | 659,975 | 355,064 |
| SET (64B, P=1) | 173,970 | **176,141** | 173,095 | 182,963 |
| GET (64B, P=1) | **177,080** | 166,416 | 169,415 | 172,726 |

### vs redis (redis-benchmark, 64B P=16)

| mode | SET | GET | notes |
|------|-----|-----|-------|
| ember concurrent | **1.8x** | **1.8x** | best for simple GET/SET workloads |
| ember sharded | **1.2x** | **1.3x** | supports all data types, beats redis at all test points |

### vs dragonfly (redis-benchmark, 64B P=16)

| mode | SET | GET | notes |
|------|-----|-----|-------|
| ember concurrent | **2.0x** | **2.4x** | pipelined |
| ember sharded | **1.4x** | **1.8x** | consistent wins across value sizes |

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
| ember concurrent | 1.46ms | 1.49ms |
| ember sharded | 2.13ms | 1.78ms |
| redis | 1.18ms | 1.05ms |
| dragonfly | 1.53ms | 1.42ms |

### latency (P=1, 48 clients, memtier_benchmark)

| server | p99 SET | p99 GET |
|--------|---------|---------|
| ember concurrent | 0.61ms | 0.56ms |
| ember sharded | 0.82ms | 0.78ms |
| redis | 0.55ms | 0.54ms |
| dragonfly | 1.11ms | 1.10ms |

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
| SET | P=16 | 1.24M/s | 657k/s | 47% |
| GET | P=16 | 1.28M/s | 933k/s | 27% |
| SET | P=1 | 133k/s | 112k/s | 16% |
| GET | P=1 | 133k/s | 133k/s | 0% |

encryption only affects persistence writes — GET throughput is unchanged at P=1 since reads come from the in-memory keyspace. at higher pipeline depths, the throughput difference reflects AES-256-GCM overhead on the write path.

### vector similarity

ember vs chromadb vs pgvector vs qdrant. 100k random vectors, 128 dimensions, cosine metric, k=10 kNN search.
HNSW index: M=16, ef_construction=64 for all systems. tested on GCP c2-standard-8.

| metric | ember (RESP) | ember (gRPC) | chromadb | pgvector | qdrant |
|--------|-------------|-------------|----------|----------|--------|
| insert (vectors/sec) | 1,483 | 2,374 | 3,679 | 1,513 | **7,382** |
| query (queries/sec) | 1,212 | **1,452** | 383 | 843 | 589 |
| query p50 (ms) | 0.82ms | **0.68ms** | 2.60ms | 1.16ms | 1.69ms |
| query p99 (ms) | 1.00ms | **0.86ms** | 2.82ms | 1.61ms | 1.93ms |
| memory (MB) | **38 MB** | — | 123 MB | 179 MB | 122 MB |

ember's query throughput is 3.2x chromadb, 1.4x pgvector, and 2.1x qdrant, with 3-5x lower memory usage. gRPC queries are 20% faster than RESP due to lower serialization overhead. insert throughput uses VADD_BATCH (batches of 500 vectors per command) — the gRPC path benefits most since packed floats avoid string parsing entirely.

#### SIFT1M recall accuracy (128-dim, 1M vectors, 10k queries)

*run `bench/bench-vector.sh --sift --qdrant` on a c2-standard-8 (32GB) to populate. SIFT1M is 1M × 128 × 4B = 512MB raw data; ember HNSW peaks around 1.5GB RSS, well within 32GB.*

### pipeline scaling (ember sharded)

throughput vs pipeline depth on sharded mode, showing how the dispatch-collect pattern scales with batching. tested with redis-benchmark, 50 clients, 8 threads.

| pipeline depth | SET (ops/sec) | GET (ops/sec) |
|----------------|---------------|---------------|
| P=1 | — | — |
| P=4 | — | — |
| P=16 | — | — |
| P=64 | — | — |
| P=256 | — | — |

*numbers pending — re-run `bench/bench.sh` on GCP c2-standard-8 after thread-per-core merge.*

### transaction overhead

MULTI/SET/EXEC vs bare SET to quantify the cost of transaction wrapping. transactions force serial execution (no parallel shard dispatch), so they represent a worst-case for the sharded architecture.

| test | ember sharded | redis |
|------|---------------|-------|
| bare SET (P=1) | — | — |
| MULTI/SET/EXEC (P=1) | — | — |
| overhead | — | — |

*numbers pending — run `bench/bench.sh` or `ember-cli benchmark -t set,multi` on GCP.*

the built-in CLI benchmark also supports transaction workloads:

```bash
# compare SET vs MULTI/SET/EXEC
ember-cli benchmark -t set,multi -n 100000 -c 50
```

### scaling efficiency

| cores | ember sharded SET | scaling factor |
|-------|-------------------|----------------|
| 1 | ~100k | 1.0x |
| 8 | ~1.25M | 12.5x |

sharded mode scales super-linearly with cores for pipelined workloads thanks to the dispatch-collect pipeline pattern. concurrent mode uses a global DashMap and doesn't scale with core count but has lower per-request overhead.

### gRPC vs RESP3

standard SET/GET operations comparing RESP3 (redis-py) against gRPC (ember-py). 100k requests, 64B values.

| test | ops/sec | p50 (ms) | p99 (ms) |
|------|---------|----------|----------|
| RESP3 SET (sequential) | 15,447 | 0.060 | 0.100 |
| RESP3 GET (sequential) | 16,915 | 0.054 | 0.093 |
| RESP3 SET (pipelined) | 83,943 | 0.012 | 0.014 |
| RESP3 GET (pipelined) | **109,453** | 0.009 | 0.010 |
| gRPC SET (unary) | 6,354 | 0.148 | 0.235 |
| gRPC GET (unary) | 6,323 | 0.148 | 0.236 |

RESP3 pipelining is the fastest option for bulk operations (5-7x over sequential, 14-17x over gRPC unary). gRPC unary calls have higher per-request overhead from HTTP/2 framing but provide type-safe APIs. for vector queries where gRPC uses streaming RPCs, it's 16% faster than RESP (see vector table above).

### pub/sub throughput

publish throughput and fan-out delivery rate across subscriber counts and message sizes. 10k messages per test.

| test | pub msg/s | fanout msg/s | p99 (ms) |
|------|-----------|--------------|----------|
| 1 sub, 64B, SUBSCRIBE | 10,875 | 10,875 | 0.20 |
| 10 sub, 64B, SUBSCRIBE | 1,929 | 19,290 | 3.02 |
| 100 sub, 64B, SUBSCRIBE | 388 | 27,001 | 26.48 |
| 1 sub, 1KB, SUBSCRIBE | 10,522 | 10,522 | 0.20 |
| 10 sub, 1KB, SUBSCRIBE | 1,856 | 18,561 | 3.21 |
| 100 sub, 1KB, SUBSCRIBE | 389 | 26,008 | 27.80 |
| 10 sub, 64B, PSUBSCRIBE | 1,869 | 18,689 | 3.11 |
| 100 sub, 64B, PSUBSCRIBE | 390 | 26,250 | 27.45 |

fan-out throughput scales well — total message delivery rate increases from 10.9k to 27k msg/s as subscribers grow from 1 to 100. per-publisher throughput drops proportionally since each message fans out to more receivers. PSUBSCRIBE performs nearly identically to SUBSCRIBE. message size (64B vs 1KB) has minimal impact.

### protobuf storage overhead

PROTO.* commands vs raw SET/GET with identical data. measures the cost of server-side schema validation and field-level access. 100k requests, bench.User message (28 bytes).

| test | ops/sec | p50 (ms) | p99 (ms) |
|------|---------|----------|----------|
| raw SET | 15,698 | 0.060 | 0.099 |
| PROTO.SET | 17,145 | 0.055 | 0.091 |
| raw GET | 18,081 | 0.054 | 0.066 |
| PROTO.GET | 16,220 | 0.058 | 0.095 |
| PROTO.GETFIELD | 16,838 | 0.056 | 0.093 |
| PROTO.SETFIELD | 16,635 | 0.057 | 0.094 |

schema validation overhead is within noise (~0-10%). PROTO.SET is actually slightly faster than raw SET in this run (within variance). field-level access (GETFIELD/SETFIELD) adds negligible overhead vs full message operations.

### memory by data type

per-key memory overhead across data types. string: 1M keys, 64B values. hash: 100k keys, 5 fields each. sorted set: 100k members. vector: 100k 128-dim vectors.

| data type | ember concurrent | ember sharded | redis |
|-----------|------------------|---------------|-------|
| string (64B) | **128 B/key** | 208 B/key | 173 B/key |
| hash (5 fields) | — | 555 B/key | **170 B/key** |
| sorted set | — | 179 B/member | **111 B/member** |
| vector (128-dim) | — | 853 B/vector | — |

ember concurrent mode is the most memory-efficient for strings (128 B/key vs redis 173 B/key) due to the DashMap structure. sharded mode uses more memory per key due to channel routing metadata. hash and sorted set commands only run in sharded mode. redis is more memory-efficient for complex types thanks to ziplist/listpack compact encodings.

## execution modes

ember offers two modes with different tradeoffs:

**concurrent mode** (`--concurrent`):
- uses DashMap for lock-free access
- 1.8x faster than redis for GET/SET (redis-benchmark, pipelined)
- only supports string operations
- best for simple key-value workloads

**sharded mode** (default):
- each CPU core owns a keyspace partition
- requests routed via tokio channels with dispatch-collect pipelining
- supports all data types (lists, hashes, sets, sorted sets)
- 1.3-1.4x redis throughput with pipelining, 1.3x without

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
