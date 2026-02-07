# benchmarks

performance benchmarks for ember, using `redis-benchmark` for system-level throughput/latency testing.

## prerequisites

- `redis-benchmark` (comes with redis: `brew install redis` on macOS)
- `redis-server` (optional, only needed for comparison mode)
- ember built in release mode (`cargo build --release -p ember-server`)

## running

```bash
# full comparison against redis
make bench-compare

# ember only (no redis needed)
make bench-quick

# with custom parameters
BENCH_REQUESTS=500000 BENCH_CLIENTS=100 bash bench/compare-redis.sh

# JSON output for CI
bash bench/compare-redis.sh --json
```

## what's measured

| test | what it measures |
|------|-----------------|
| SET (3B, P=16) | peak write throughput, pipelined |
| GET (3B, P=16) | peak read throughput, pipelined |
| SET/GET (64B, P=16) | throughput with realistic value sizes |
| SET/GET (1KB, P=16) | throughput with larger payloads |
| SET/GET (3B, P=1) | single-request latency (no pipelining) |

## environment variables

| variable | default | description |
|----------|---------|-------------|
| `EMBER_PORT` | 6379 | port for ember server |
| `REDIS_PORT` | 6399 | port for redis server |
| `BENCH_REQUESTS` | 100000 | total requests per test |
| `BENCH_CLIENTS` | 50 | concurrent client connections |
| `BENCH_PIPELINE` | 16 | pipeline depth for P>1 tests |
| `EMBER_BIN` | ./target/release/ember-server | path to ember binary |

## results

raw CSV results are saved to `bench/results/` with timestamps. these are gitignored to keep the repo clean — copy them elsewhere for long-term tracking.

## micro-benchmarks

for criterion micro-benchmarks (keyspace, engine, protocol), see the `benches/` directories in `ember-core` and `ember-protocol`:

```bash
cargo bench -p emberkv-core   # keyspace + engine benchmarks
cargo bench -p ember-protocol # RESP3 parse/serialize benchmarks
```
