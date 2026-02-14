# benchmarks

performance benchmarks for ember comparing against Redis and Dragonfly.

## results summary

tested on GCP c2-standard-8 (8 vCPU Intel Xeon @ 3.10GHz), Ubuntu 22.04, 2.4M total requests per test.

### throughput (requests/sec)

#### redis-benchmark (1M requests, 50 clients, 8 threads)

| test | ember concurrent | ember sharded | redis | dragonfly |
|------|------------------|---------------|-------|-----------|
| SET (3B, P=16) | **1,792,759** | 1,165,928 | 1,066,234 | 922,922 |
| GET (3B, P=16) | **2,193,614** | 1,520,003 | 1,227,150 | 1,048,603 |
| SET (64B, P=16) | **1,773,503** | 1,237,948 | 1,011,251 | 876,161 |
| GET (64B, P=16) | **2,160,345** | 1,560,841 | 1,179,377 | 883,604 |
| SET (1KB, P=16) | **1,089,576** | 745,991 | 639,872 | 712,398 |
| GET (1KB, P=16) | **1,789,533** | 1,341,132 | 780,142 | 351,096 |
| SET (64B, P=1) | 200,000 | 181,785 | 124,984 | **235,238** |
| GET (64B, P=1) | 199,960 | 181,785 | 124,984 | **235,238** |

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

- full Redis API compatibility (200+ commands vs ember's ~101)
- sophisticated memory management (dashtable for ~25% of Redis memory usage)
- transactional semantics (MULTI/EXEC, Lua scripting)
- fork-free snapshotting
- mature replication and clustering
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

| server | memory | per key |
|--------|--------|---------|
| ember | 166 MB | ~166 bytes |
| redis | 105 MB | ~105 bytes |

ember uses more memory per key due to storing additional metadata for LRU eviction and expiration tracking.

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

| mode | SET throughput | overhead vs plaintext |
|------|----------------|----------------------|
| ember concurrent | — | — |
| ember sharded | — | — |

*results pending — run `bench/bench-encryption.sh` on a dedicated VM to populate.*

note: encryption only affects persistence writes. GET throughput should be unchanged since reads come from the in-memory keyspace.

### vector similarity

ember vs chromadb vs pgvector. 100k random vectors, 128 dimensions, cosine metric, k=10 kNN search.
HNSW index: M=16, ef_construction=64 for all systems. tested on GCP c2-standard-8.

| metric | ember | chromadb | pgvector |
|--------|-------|----------|----------|
| insert (vectors/sec) | 917 | **3,738** | 1,562 |
| query (queries/sec) | **1,214** | 376 | 882 |
| query p99 (ms) | **1.09ms** | 2.90ms | 1.52ms |
| memory (MB) | **30 MB** | 122 MB | 178 MB |

ember's query throughput is 3.2x chromadb and 1.4x pgvector, with 4-6x lower memory usage. insert throughput is lower due to per-vector RESP protocol overhead — batched pipelining helps but each VADD is still a separate command.

#### SIFT1M recall accuracy (128-dim, 1M vectors, 10k queries)

| metric | ember | chromadb | pgvector |
|--------|-------|----------|----------|
| recall@10 | — | — | — |
| insert (vectors/sec) | — | — | — |
| query p99 (ms) | — | — | — |

*results pending — requires a larger VM (c2-standard-16 or higher) since the 1M-vector HNSW index exceeds 16GB RAM during construction. run `bench/bench-vector.sh --sift` to populate.*

### scaling efficiency

| cores | ember sharded SET | scaling factor |
|-------|-------------------|----------------|
| 1 | ~100k | 1.0x |
| 8 | ~1.25M | 12.5x |

sharded mode scales super-linearly with cores for pipelined workloads thanks to the dispatch-collect pipeline pattern. concurrent mode uses a global DashMap and doesn't scale with core count but has lower per-request overhead.

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

# SIFT1M recall accuracy
./bench/bench-vector.sh --sift
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

# run benchmarks
gcloud compute ssh ember-bench --zone=us-central1-a
cd ember
./bench/compare-redis.sh      # redis-benchmark suite
./bench/bench-memtier.sh      # memtier_benchmark suite
./bench/bench-memory.sh       # memory comparison

# cleanup
gcloud compute instances delete ember-bench --zone=us-central1-a
```

## scripts

| script | description |
|--------|-------------|
| `bench.sh` | full benchmark: ember (sharded + concurrent) vs redis |
| `bench-quick.sh` | quick sanity check (~10 seconds) |
| `bench-memory.sh` | memory usage with 1M keys |
| `compare-redis.sh` | comprehensive comparison using redis-benchmark |
| `bench-memtier.sh` | comprehensive comparison using memtier_benchmark |
| `bench-encryption.sh` | encryption at rest overhead (plaintext vs AES-256-GCM) |
| `bench-vector.sh` | vector similarity: ember vs chromadb vs pgvector |
| `setup-vm.sh` | bootstrap dependencies on fresh ubuntu VM |
| `setup-vm-vector.sh` | additional dependencies for vector benchmarks |

## configuration

```bash
# customize redis-benchmark parameters
BENCH_REQUESTS=1000000 BENCH_THREADS=16 ./bench/compare-redis.sh

# customize memtier_benchmark parameters
MEMTIER_THREADS=8 MEMTIER_CLIENTS=16 MEMTIER_REQUESTS=20000 ./bench/bench-memtier.sh

# customize memory test
KEY_COUNT=5000000 VALUE_SIZE=128 ./bench/bench-memory.sh
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
```

see `ember-cli benchmark --help` for all options.

## micro-benchmarks

for criterion micro-benchmarks:

```bash
cargo bench -p emberkv-core   # keyspace + engine
cargo bench -p ember-protocol # RESP3 parse/serialize
```
