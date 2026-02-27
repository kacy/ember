# benchmarks

performance benchmarks for ember comparing against Redis and Dragonfly.

## results summary

tested on GCP c2-standard-8 (8 vCPU Intel Xeon @ 3.10GHz), Ubuntu 22.04.

### throughput (requests/sec)

#### redis-benchmark (100k requests, 50 clients, 8 threads)

| test | ember | redis | dragonfly |
|------|-------|-------|-----------|
| SET (3B, P=16) | **1,791,142** | 991,366 | 931,259 |
| GET (3B, P=16) | **2,138,893** | 1,151,632 | 1,081,290 |
| SET (64B, P=16) | **1,789,428** | 962,615 | 829,487 |
| GET (64B, P=16) | **2,004,480** | 1,112,355 | 896,857 |
| SET (1KB, P=16) | **984,009** | 592,514 | 688,438 |
| GET (1KB, P=16) | **1,699,762** | 695,638 | 339,087 |
| SET (64B, P=1) | **199,203** | 99,900 | 199,203 |
| GET (64B, P=1) | **199,600** | 99,900 | 199,600 |

#### memtier_benchmark (4 threads, 12 clients/thread, 10k req/client)

| test | ember | redis | dragonfly |
|------|-------|-------|-----------|
| SET (64B, P=16) | **1,071,443** | 1,022,491 | 971,076 |
| GET (64B, P=16) | **1,154,265** | 350,831 | 327,923 |
| mixed 1:10 (64B, P=16) | 1,123,225 | **1,149,125** | 325,731 |
| mixed 1:1 (64B, P=16) | 1,084,817 | **1,152,945** | 970,160 |
| SET (1KB, P=16) | **813,809** | 733,819 | 279,004 |
| GET (1KB, P=16) | 472,169 | **655,053** | 190,791 |
| SET (64B, P=1) | **164,560** | 136,723 | 150,964 |
| GET (64B, P=1) | **164,395** | 109,042 | 145,118 |

### vs redis (redis-benchmark, 64B P=16)

| | SET | GET | notes |
|------|-----|-----|-------|
| ember | **1.9x** | **1.8x** | beats redis at all value sizes and pipeline depths |

### vs dragonfly (redis-benchmark, 64B P=16)

| | SET | GET | notes |
|------|-----|-----|-------|
| ember | **2.2x** | **2.2x** | consistent wins across value sizes |

**important caveat**: these benchmarks should be taken with a grain of salt. ember is a small indie project built for learning and experimentation. Redis and Dragonfly are production-grade systems developed by large teams over many years, battle-tested at massive scale.

dragonfly in particular offers features ember simply doesn't have:

- full Redis API compatibility (200+ commands vs ember's ~150)
- sophisticated memory management (dashtable for ~25% of Redis memory usage)
- Lua scripting
- fork-free snapshotting
- streams, modules, and much more

for anything resembling production use, Redis and Dragonfly are the sensible choices. ember exists primarily as a learning project and for workloads where simplicity matters more than features.

### latency (P=16, 48 clients, memtier_benchmark)

| server | p99 SET | p99 GET |
|--------|---------|---------|
| ember | 1.343ms | 1.223ms |
| redis | 1.295ms | 0.935ms |
| dragonfly | 1.543ms | 1.431ms |

### latency (P=1, 48 clients, memtier_benchmark)

| server | p99 SET | p99 GET |
|--------|---------|---------|
| ember | 1.247ms | 1.167ms |
| redis | 0.647ms | 0.623ms |
| dragonfly | 1.263ms | 1.279ms |

### memory usage (~1M keys, 64B values)

| server | per key |
|--------|---------|
| ember | 180 B |
| redis | 173 B |

per-entry metadata (expiry, LRU timestamp, cached value size) accounts for the small overhead relative to redis.

### with persistence enabled

AOF with `appendfsync everysec` (default), SET at P=16:

| server | SET throughput | vs no-persistence |
|--------|----------------|-------------------|
| ember | ~1.24M/s | ~70% of baseline |
| redis | ~667K/s | ~69% of baseline |

both systems show ~30% throughput reduction with AOF enabled at high pipeline depths, where sustained write volume stresses disk I/O. `appendfsync always` has even more significant impact.

### with encryption enabled

AES-256-GCM encryption at rest (AOF and snapshots). requires building with `--features encryption`:

| test | pipeline | plaintext | encrypted | overhead |
|------|----------|-----------|-----------|----------|
| SET | P=16 | 1.16M/s | 463k/s | 60% |
| GET | P=16 | 2.04M/s | 2.00M/s | 1% |
| SET | P=1 | 160k/s | 160k/s | 0% |
| GET | P=1 | 200k/s | 200k/s | 0% |

encryption only affects persistence writes — GET throughput is unchanged since reads come from the in-memory keyspace. SET overhead at P=16 reflects AES-256-GCM cost on the AOF write path under sustained load.

### vector similarity

ember vs chromadb vs pgvector vs qdrant. 100k random vectors, 128 dimensions, cosine metric, k=10 kNN search.
HNSW index: M=16, ef_construction=64 for all systems. tested on GCP c2-standard-8.

| metric | ember (1 key) | ember (8 shards) | chromadb | pgvector | qdrant |
|--------|--------------|------------------|----------|----------|--------|
| insert (vectors/sec) | 2,432 | 5,482 | 4,879 | 1,702 | **7,699** |
| query (queries/sec) | 1,217 | **1,793** | 381 | 782 | 560 |
| query p50 (ms) | 0.82ms | **0.56ms** | 2.61ms | 1.26ms | 1.77ms |
| query p99 (ms) | 0.99ms | **0.62ms** | 2.91ms | 1.67ms | 2.00ms |
| memory (MB) | **29 MB** | ~31 MB | 139 MB | 178 MB | 168 MB |

ember's query throughput is 4.7x chromadb, 2.3x pgvector, and 3.2x qdrant (8-shard mode), with 4-6x lower memory. insert throughput uses binary-encoded VADD_BATCH (packed LE f32 blobs + parallel HNSW construction). sharding distributes vectors across multiple keys so each shard builds an independent HNSW index in parallel — this is where ember's thread-per-core architecture pays off for vector workloads.

#### insert scaling by shard count

shows how vector insert throughput scales when distributing vectors across multiple keys (each key maps to an independent HNSW index on a separate shard). binary-encoded VADD_BATCH, pipeline depth = shard count.

| shards | insert (vectors/sec) | query (queries/sec) | query p99 (ms) |
|--------|---------------------|---------------------|----------------|
| 1 | 2,432 | 1,217 | 0.99ms |
| 4 | 3,742 | 1,175 | 0.97ms |
| 8 | 5,482 | 1,793 | 0.62ms |

insert throughput scales nearly linearly with shard count since each shard does its own HNSW graph construction independently. query latency also improves with sharding — smaller per-shard indexes mean faster kNN graph traversal.

#### SIFT1M (1M vectors, 128-dim, 10k queries)

real-world accuracy and throughput on the standard SIFT1M benchmark dataset. tested on GCP c2-standard-8.

| metric | ember |
|--------|-------|
| insert (vectors/sec) | 3,755 |
| query (queries/sec) | 1,596 |
| query p99 (ms) | 0.78ms |
| recall@10 | **0.9339** |

93.4% recall@10 with M=16, ef_construction=64 — competitive with dedicated vector databases at the same HNSW parameters. insert throughput is higher than random vectors because SIFT features are integer-valued and sparser, making HNSW graph construction cheaper.

### pipeline scaling

throughput vs pipeline depth, showing how the dispatch-collect pattern scales with batching. tested with redis-benchmark, 50 clients, 8 threads.

| pipeline depth | SET (ops/sec) | GET (ops/sec) |
|----------------|---------------|---------------|
| P=1 | 200,000 | 199,800 |
| P=4 | 757,924 | 752,346 |
| P=16 | 1,804,108 | 2,108,969 |
| P=64 | 2,860,252 | 3,476,000 |
| P=256 | 2,431,340 | 4,247,832 |

GET throughput scales monotonically with pipeline depth. SET peaks around P=64 then slightly decreases at P=256 due to write-path contention. the batch dispatch optimization (PR #232) groups commands by target shard and sends one channel message per shard, eliminating head-of-line blocking at high pipeline depths.

### data type throughput

per-command throughput across data types. redis-benchmark, 100k requests, 50 clients, pipeline depth 1.

| command | ops/sec |
|---------|---------|
| SET | 455,677 |
| GET | 501,924 |
| LPUSH | 465,777 |
| SADD | 462,665 |
| ZADD | 450,479 |
| HSET | 462,443 |
| HGET | 473,517 |

all data types achieve similar throughput at P=1, showing consistent per-command overhead regardless of the underlying data structure.

### transaction overhead

MULTI/SET/EXEC vs bare SET to quantify the cost of transaction wrapping. measured with ember's built-in CLI benchmark (50 clients, 100k requests, P=1).

| test | ember | redis |
|------|-------|-------|
| bare SET (P=1) | **167,064** | 108,698 |
| MULTI/SET/EXEC (P=1) | **111,195** | 76,798 |
| overhead | 33% | 29% |

both systems show ~30% overhead from transaction wrapping. ember is 1.5x faster than redis for both bare SET and MULTI/SET/EXEC at P=1.

the built-in CLI benchmark also supports transaction workloads:

```bash
# compare SET vs MULTI/SET/EXEC
ember-cli benchmark -t set,multi -n 100000 -c 50
```

### scaling efficiency

| cores | ember SET (P=16) | scaling factor |
|-------|------------------|----------------|
| 1 | ~200k | 1.0x |
| 8 | ~1.8M | 9x |

ember scales well with cores for pipelined workloads thanks to the dispatch-collect pipeline pattern, where each CPU core owns a keyspace partition and processes its shard's commands independently.

### gRPC vs RESP3

standard SET/GET operations comparing RESP3 (redis-py) against gRPC (ember-py). 100k requests, 64B values.

| test | ops/sec | p50 (ms) | p99 (ms) |
|------|---------|----------|----------|
| RESP3 SET (sequential) | 11,361 | 0.087 | 0.116 |
| RESP3 GET (sequential) | 12,244 | 0.081 | 0.108 |
| RESP3 SET (pipelined) | 75,726 | 0.013 | 0.015 |
| RESP3 GET (pipelined) | **106,422** | 0.009 | 0.011 |
| gRPC SET (unary) | 5,031 | 0.190 | 0.267 |
| gRPC GET (unary) | 5,119 | 0.187 | 0.262 |

RESP3 pipelining is the fastest option for bulk operations (7-9x over sequential, 15-21x over gRPC unary). gRPC unary calls have higher per-request overhead from HTTP/2 framing but provide type-safe APIs. for vector queries where gRPC uses streaming RPCs, it's 16% faster than RESP (see vector table above).

### pub/sub throughput

publish throughput and fan-out delivery rate across subscriber counts and message sizes. 10k messages per test.

| test | pub msg/s | fanout msg/s | p99 (ms) |
|------|-----------|--------------|----------|
| 1 sub, 64B, SUBSCRIBE | 8,639 | 8,639 | 0.23 |
| 10 sub, 64B, SUBSCRIBE | 1,751 | 17,511 | 3.39 |
| 100 sub, 64B, SUBSCRIBE | 396 | 24,731 | 29.58 |
| 1 sub, 1KB, SUBSCRIBE | 9,265 | 9,265 | 0.24 |
| 10 sub, 1KB, SUBSCRIBE | 1,712 | 17,117 | 3.50 |
| 100 sub, 1KB, SUBSCRIBE | 399 | 23,779 | 31.33 |
| 10 sub, 64B, PSUBSCRIBE | 1,702 | 17,023 | 3.53 |
| 100 sub, 64B, PSUBSCRIBE | 397 | 23,768 | 30.94 |

fan-out throughput scales well — total message delivery rate increases from 8.6k to 24.7k msg/s as subscribers grow from 1 to 100. per-publisher throughput drops proportionally since each message fans out to more receivers. PSUBSCRIBE performs nearly identically to SUBSCRIBE. message size (64B vs 1KB) has minimal impact.

### protobuf storage overhead

PROTO.* commands vs raw SET/GET with identical data. measures the cost of server-side schema validation and field-level access. 100k requests, bench.User message (28 bytes).

| test | ops/sec | p50 (ms) | p99 (ms) |
|------|---------|----------|----------|
| raw SET | 11,591 | 0.085 | 0.117 |
| PROTO.SET | 12,612 | 0.079 | 0.101 |
| raw GET | 12,755 | 0.078 | 0.102 |
| PROTO.GET | 12,250 | 0.081 | 0.104 |
| PROTO.GETFIELD | 12,522 | 0.079 | 0.102 |
| PROTO.SETFIELD | 11,926 | 0.083 | 0.109 |

schema validation overhead is within noise (~0-10%). PROTO.SET is actually slightly faster than raw SET in this run (within variance). field-level access (GETFIELD/SETFIELD) adds negligible overhead vs full message operations.

### memory by data type

per-key memory overhead across data types. string: 1M keys, 64B values. hash: 100k keys, 5 fields each. sorted set: 100k members. vector: 100k 128-dim vectors.

| data type | ember | redis |
|-----------|-------|-------|
| string (64B) | 180 B/key | **173 B/key** |
| hash (5 fields) | 215 B/key | **170 B/key** |
| sorted set | **115 B/member** | 111 B/member |
| vector (128-dim) | 853 B/vector | — |

redis is slightly more memory-efficient for string and hash types thanks to ziplist/listpack compact encodings. ember's per-entry metadata (expiry, LRU timestamp, cached value size) accounts for the small overhead. hash memory was reduced from 451 to 215 B/key by replacing per-field `(CompactString, Bytes)` tuples (48 bytes overhead each) with a packed byte buffer and moving version tracking to a lazy side table.

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
| `bench.sh` | full benchmark: ember vs redis |
| `bench-quick.sh` | quick sanity check (~10 seconds) |
| `bench-memory.sh` | memory usage across data types (string, hash, zset, vector) |
| `compare-redis.sh` | comprehensive comparison using redis-benchmark |
| `bench-memtier.sh` | comprehensive comparison using memtier_benchmark |
| `bench-encryption.sh` | encryption at rest overhead (plaintext vs AES-256-GCM) |
| `bench-vector.sh` | vector similarity: ember vs chromadb vs pgvector vs qdrant |
| `bench-grpc.sh` | gRPC vs RESP3 standard operations |
| `bench-pubsub.sh` | pub/sub throughput and fan-out latency |
| `bench-proto.sh` | protobuf storage overhead (PROTO.* vs raw SET/GET) |
| `bench-datatypes.sh` | data type throughput: lists, sets, sorted sets, hashes |
| `bench-stress.sh` | stress tests: large values, eviction, connection storm |
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
| `EMBER_SHARDED_PORT` | 6380 | ember port |
| `REDIS_PORT` | 6399 | redis port |
| `DRAGONFLY_PORT` | 6389 | dragonfly port |
| `BENCH_REQUESTS` | 100000 | requests per test |
| `BENCH_CLIENTS` | 50 | concurrent connections |
| `BENCH_PIPELINE` | 16 | pipeline depth |
| `BENCH_THREADS` | CPU cores | redis-benchmark threads |

### bench-memtier.sh (memtier_benchmark)

| variable | default | description |
|----------|---------|-------------|
| `EMBER_SHARDED_PORT` | 6380 | ember port |
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
