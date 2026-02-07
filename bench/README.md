# benchmarks

performance benchmarks for ember, comparing against redis (single-threaded) and dragonfly (multi-threaded).

## prerequisites

- `redis-benchmark` (comes with redis: `brew install redis` on macOS)
- `redis-server` (optional, for single-threaded comparison)
- `dragonfly` (optional, for multi-threaded comparison — https://github.com/dragonflydb/dragonfly)
- ember built in release mode (`cargo build --release -p ember-server`)

## running

```bash
# full comparison (redis + dragonfly if available)
make bench-compare

# ember only (no other servers needed)
make bench-quick

# quick mode with reduced test matrix
bash bench/compare-redis.sh --ember-only --quick

# with custom parameters
BENCH_REQUESTS=500000 BENCH_CLIENTS=100 bash bench/compare-redis.sh

# JSON output for CI
bash bench/compare-redis.sh --json
```

## what's measured

the benchmark runs three comparisons:

### 1. single-threaded (ember 1 shard vs redis)

apples-to-apples comparison of single-threaded performance. measures protocol efficiency, data structure speed, and per-core throughput. ember runs with `--shards 1`.

### 2. multi-threaded (ember N shards vs dragonfly)

apples-to-apples comparison of multi-threaded architectures. both ember and dragonfly use thread-per-core designs. ember runs with all available CPU cores.

### 3. scaling efficiency

compares ember multi-core vs ember single-core to show how well the sharded architecture scales. ideal scaling would be Nx on N cores.

## test matrix

| test | what it measures |
|------|-----------------|
| SET (3B, P=16) | peak write throughput, pipelined |
| GET (3B, P=16) | peak read throughput, pipelined |
| SET/GET (64B, P=16) | throughput with realistic value sizes |
| SET/GET (1KB, P=16) | throughput with larger payloads |
| SET/GET (64B, P=1) | single-request latency (no pipelining) |

## environment variables

| variable | default | description |
|----------|---------|-------------|
| `EMBER_PORT` | 6379 | port for ember multi-core server |
| `EMBER_PORT_SINGLE` | 6378 | port for ember single-core server |
| `REDIS_PORT` | 6399 | port for redis server |
| `DRAGONFLY_PORT` | 6389 | port for dragonfly server |
| `BENCH_REQUESTS` | 100000 | total requests per test |
| `BENCH_CLIENTS` | 50 | concurrent client connections |
| `BENCH_PIPELINE` | 16 | pipeline depth for P>1 tests |
| `EMBER_BIN` | ./target/release/ember-server | path to ember binary |
| `DRAGONFLY_BIN` | dragonfly | path to dragonfly binary |

## command-line flags

| flag | description |
|------|-------------|
| `--ember-only` | skip redis and dragonfly, only benchmark ember |
| `--quick` | reduced test matrix (64B only, P=16 and P=1) |
| `--json` | JSON output for CI integration |

## ember server flags

the benchmark uses these ember-server flags:

```bash
# multi-core (uses all CPU cores by default)
./target/release/ember-server --port 6379

# single-core (for fair redis comparison)
./target/release/ember-server --port 6378 --shards 1
```

## results

raw CSV results are saved to `bench/results/` with timestamps. these are gitignored to keep the repo clean — copy them elsewhere for long-term tracking.

## micro-benchmarks

for criterion micro-benchmarks (keyspace, engine, protocol), see the `benches/` directories in `ember-core` and `ember-protocol`:

```bash
cargo bench -p emberkv-core   # keyspace + engine benchmarks
cargo bench -p ember-protocol # RESP3 parse/serialize benchmarks
```
