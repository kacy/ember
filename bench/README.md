# benchmarks

performance benchmarks for ember comparing against Redis and Dragonfly.

## results summary

tested on GCP c2-standard-8 (8 vCPU Intel Xeon @ 3.10GHz), Ubuntu 22.04.

### throughput (requests/sec)

| test | ember concurrent | ember sharded | redis | dragonfly |
|------|------------------|---------------|-------|-----------|
| SET (64B, P=16) | **1,859,464** | 863,823 | 992,380 | 551,514 |
| GET (64B, P=16) | **2,489,950** | 965,532 | 1,163,051 | 640,389 |
| SET (64B, P=1) | **222,123** | 199,840 | 117,647 | 222,222 |
| GET (64B, P=1) | **222,222** | 222,123 | 124,937 | 222,222 |

### vs redis

| mode | SET | GET | notes |
|------|-----|-----|-------|
| ember concurrent | **1.8x** | **2.1x** | best for simple GET/SET workloads |
| ember sharded | 0.9x | 0.8x | channel overhead, but supports all data types |

### vs dragonfly

| mode | SET | GET | notes |
|------|-----|-----|-------|
| ember concurrent | **3.3x** | **3.8x** | dragonfly tested with default config |
| ember sharded | **1.6x** | **1.5x** | both use thread-per-core architecture |

**important caveat**: this comparison is not apples-to-apples. dragonfly is a production-ready Redis replacement with features ember doesn't have:

- full Redis API compatibility (100+ commands vs ember's 65)
- sophisticated memory management (dashtable for ~25% of Redis memory usage)
- transactional semantics (MULTI/EXEC, Lua scripting)
- fork-free snapshotting
- replication and clustering
- streams, pub/sub, and more

ember's concurrent mode wins on raw GET/SET throughput because it's architecturally simpler — essentially a concurrent hashmap with RESP3 parsing. this simplicity comes at the cost of features. for production Redis replacement, dragonfly is likely the better choice. ember is best suited for simple caching workloads where raw throughput matters more than feature completeness.

### latency (50 clients, no pipelining)

| server | p50 | p99 | p100 |
|--------|-----|-----|------|
| ember concurrent | 0.3ms | 0.4ms | 0.6ms |
| ember sharded | 0.3ms | 0.4ms | 0.6ms |
| redis | 0.3ms | 0.4ms | 0.5ms |

### memory usage (~1M keys, 64B values)

| server | memory | per key |
|--------|--------|---------|
| ember | 161 MB | ~161 bytes |
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
- 1.8-2.1x faster than redis for GET/SET
- only supports string operations
- best for simple key-value workloads

**sharded mode** (default):
- each CPU core owns a keyspace partition
- requests routed via tokio channels
- supports all data types (lists, hashes, sets, sorted sets)
- ~0.9x redis throughput with pipelining, but 1.7x faster without pipelining

## running benchmarks

### quick start (local)

```bash
# build with jemalloc for best performance
cargo build --release -p ember-server --features jemalloc

# quick sanity check (ember only)
./bench/bench-quick.sh

# full comparison vs redis
./bench/bench.sh

# memory usage test
./bench/bench-memory.sh

# comprehensive comparison (redis + dragonfly)
./bench/compare-redis.sh
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

# set up and run
gcloud compute ssh ember-bench --zone=us-central1-a
bash -s < ./bench/setup-vm.sh
cd ember && ./bench/bench.sh

# cleanup
gcloud compute instances delete ember-bench --zone=us-central1-a
```

## scripts

| script | description |
|--------|-------------|
| `bench.sh` | full benchmark: ember (sharded + concurrent) vs redis |
| `bench-quick.sh` | quick sanity check (~10 seconds) |
| `bench-memory.sh` | memory usage with 1M keys |
| `compare-redis.sh` | comprehensive comparison with dragonfly support |
| `setup-vm.sh` | install dependencies on fresh ubuntu VM |

## configuration

```bash
# customize benchmark parameters
BENCH_REQUESTS=1000000 BENCH_THREADS=16 ./bench/compare-redis.sh

# customize memory test
KEY_COUNT=5000000 VALUE_SIZE=128 ./bench/bench-memory.sh
```

## environment variables

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

## micro-benchmarks

for criterion micro-benchmarks:

```bash
cargo bench -p emberkv-core   # keyspace + engine
cargo bench -p ember-protocol # RESP3 parse/serialize
```
