# benchmarks

performance benchmarks for ember comparing against Redis and Dragonfly.

## results summary

tested on GCP c2-standard-8 (8 vCPU Intel Xeon @ 3.10GHz), Ubuntu 22.04, 2.4M total requests per test.

### throughput (requests/sec)

#### redis-benchmark (1M requests, 50 clients, 8 threads)

| test | ember concurrent | ember sharded | redis | dragonfly |
|------|------------------|---------------|-------|-----------|
| SET (3B, P=16) | **1,333,333** | 799,360 | 999,000 | 798,722 |
| GET (3B, P=16) | **1,996,008** | 799,360 | 999,000 | 798,722 |
| SET (64B, P=16) | **1,331,558** | 798,722 | 799,360 | 797,448 |
| GET (64B, P=16) | **1,996,008** | 798,722 | 999,000 | 798,084 |
| SET (1KB, P=16) | **999,000** | 570,125 | 798,722 | 664,893 |
| GET (1KB, P=16) | **1,333,333** | 798,722 | 798,722 | 332,446 |
| SET (64B, P=1) | **190,331** | 173,822 | 114,246 | 210,393 |
| GET (64B, P=1) | **190,403** | 173,761 | 117,605 | 222,172 |

#### memtier_benchmark (4 threads, 12 clients/thread, 50k req/client)

| test | ember concurrent | ember sharded | redis | dragonfly |
|------|------------------|---------------|-------|-----------|
| SET (64B, P=16) | **4,562,538** | 987,530 | 2,022,543 | 694,116 |
| GET (64B, P=16) | **6,624,838** | 762,401 | 2,225,336 | 1,021,986 |
| mixed 1:10 (64B, P=16) | **1,727,847** | 1,083,253 | 1,165,414 | 702,866 |
| mixed 1:1 (64B, P=16) | **1,680,994** | 1,039,764 | 1,035,432 | 959,765 |
| SET (1KB, P=16) | **933,385** | 764,879 | 626,130 | 896,308 |
| GET (1KB, P=16) | **903,312** | 724,275 | 593,297 | 337,179 |
| SET (64B, P=1) | 170,655 | **171,517** | 154,439 | 171,582 |
| GET (64B, P=1) | **188,114** | 185,528 | 160,935 | 167,916 |

### vs redis

| mode | SET | GET | notes |
|------|-----|-----|-------|
| ember concurrent | **2.3x** | **3.0x** | best for simple GET/SET workloads |
| ember sharded | 0.5x | 0.3x | channel overhead, but supports all data types |

### vs dragonfly

| mode | SET | GET | notes |
|------|-----|-----|-------|
| ember concurrent | **6.6x** | **6.5x** | memtier, pipelined |
| ember sharded | 1.4x | 0.7x | mixed results depending on workload |

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
| ember concurrent | 1.56ms | 1.56ms |
| ember sharded | 2.29ms | 1.98ms |
| redis | 1.16ms | 1.19ms |
| dragonfly | 1.60ms | 1.46ms |

### latency (P=1, 48 clients, memtier_benchmark)

| server | p99 SET | p99 GET |
|--------|---------|---------|
| ember concurrent | 0.64ms | 0.61ms |
| ember sharded | 0.88ms | 0.83ms |
| redis | 0.58ms | 0.56ms |
| dragonfly | 1.15ms | 1.14ms |

### memory usage (~1M keys, 64B values)

| server | memory | per key |
|--------|--------|---------|
| ember | 161 MB | ~161 bytes |
| redis | 95 MB | ~95 bytes |

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

### scaling efficiency

| cores | ember sharded SET | scaling factor |
|-------|-------------------|----------------|
| 1 | ~100k | 1.0x |
| 8 | ~860k | 8.6x |

sharded mode scales linearly with cores for pipelined workloads. concurrent mode uses a global DashMap and doesn't scale with core count but has lower per-request overhead.

## execution modes

ember offers two modes with different tradeoffs:

**concurrent mode** (`--concurrent`):
- uses DashMap for lock-free access
- 2.3-3.0x faster than redis for GET/SET (pipelined)
- only supports string operations
- best for simple key-value workloads

**sharded mode** (default):
- each CPU core owns a keyspace partition
- requests routed via tokio channels
- supports all data types (lists, hashes, sets, sorted sets)
- 1.1-1.2x redis throughput without pipelining, lower with heavy pipelining due to channel overhead

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
| `setup-vm.sh` | bootstrap dependencies on fresh ubuntu VM |

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
