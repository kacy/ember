# benchmarks

performance benchmarks for ember comparing against Redis and Dragonfly.

## results summary

tested on GCP c2-standard-8 (8 vCPU Intel Xeon @ 3.10GHz), Ubuntu 22.04.

### throughput (requests/sec)

| test | ember concurrent | ember sharded | redis | dragonfly |
|------|------------------|---------------|-------|-----------|
| SET (64B, P=16) | **1,859,152** | 896,276 | 1,005,185 | 557,000 |
| GET (64B, P=16) | **2,482,898** | 992,302 | 1,160,259 | 395,000 |
| SET (64B, P=1) | **199,600** | 104,712 | 100,000 | 87,000 |
| GET (64B, P=1) | **200,000** | 104,712 | 99,800 | 87,000 |

### vs redis

| mode | SET | GET | notes |
|------|-----|-----|-------|
| ember concurrent | **1.85x** | **2.14x** | best for simple GET/SET workloads |
| ember sharded | 0.89x | 0.86x | channel overhead, but supports all data types |

### vs dragonfly

| mode | SET | GET | notes |
|------|-----|-----|-------|
| ember concurrent | **3.3x** | **6.3x** | dragonfly tested with default config |
| ember sharded | **1.6x** | **2.5x** | both use thread-per-core architecture |

### latency (50 clients, no pipelining)

| server | p50 | p99 | p100 |
|--------|-----|-----|------|
| ember concurrent | 0.3ms | 0.4ms | 0.5ms |
| ember sharded | 0.3ms | 0.4ms | 0.5ms |
| redis | 0.3ms | 0.4ms | 0.7ms |
| dragonfly | 0.4ms | 0.5ms | 4.0ms |

### memory usage (~632k keys, 64B values)

| server | memory | per key |
|--------|--------|---------|
| ember concurrent | 161 MB | ~257 bytes |
| ember sharded | 231 MB | ~356 bytes |
| redis | 105 MB | ~165 bytes |

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
| 1 | 812,449 | 1.0x |
| 8 | 803,774 | 1.0x |

**note**: sharded mode currently shows 1.0x scaling on 8 cores. the architecture routes requests through channels which creates a bottleneck. concurrent mode avoids this by using lock-free DashMap access.

## execution modes

ember offers two modes with different tradeoffs:

**concurrent mode** (`--concurrent`):
- uses DashMap for lock-free access
- 2x faster than redis for GET/SET
- only supports string operations
- best for simple key-value workloads

**sharded mode** (default):
- each CPU core owns a keyspace partition
- requests routed via tokio channels
- supports all data types (lists, hashes, sets, sorted sets)
- channel overhead reduces throughput vs concurrent mode

## running benchmarks

### quick start (local)

```bash
# build with jemalloc for best performance
cargo build --release --features jemalloc

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
REQUESTS=1000000 THREADS=16 ./bench/bench.sh

# customize memory test
KEY_COUNT=5000000 VALUE_SIZE=128 ./bench/bench-memory.sh
```

## environment variables

| variable | default | description |
|----------|---------|-------------|
| `EMBER_PORT` | 6379 | ember multi-core port |
| `REDIS_PORT` | 6399 | redis port |
| `DRAGONFLY_PORT` | 6389 | dragonfly port |
| `BENCH_REQUESTS` | 100000 | requests per test |
| `BENCH_CLIENTS` | 50 | concurrent connections |
| `BENCH_PIPELINE` | 16 | pipeline depth |

## micro-benchmarks

for criterion micro-benchmarks:

```bash
cargo bench -p emberkv-core   # keyspace + engine
cargo bench -p ember-protocol # RESP3 parse/serialize
```
