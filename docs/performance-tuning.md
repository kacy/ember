# performance tuning

this guide covers the knobs and trade-offs you can use to get the most out of ember. most defaults are reasonable for general workloads, but understanding how the engine works will help you tune for your specific case.

---

## shard count vs cpu cores

by default, ember auto-detects the number of cpu cores and creates one shard per core. this is almost always the right starting point.

to override:

```
# cli flag
ember-server --shards 8

# config file
shards = 8
```

**rule of thumb: one shard per core.** the shard count directly controls parallelism — each shard owns a partition of the keyspace and runs on its own thread.

- **fewer shards than cores**: lower overhead but you leave parallelism on the table. reasonable if your workload is not cpu-bound.
- **more shards than cores**: increases context switching overhead with no throughput gain. avoid this.

if you're running ember inside a container with a cgroup cpu limit, set `--shards` explicitly to match the allocated cores — the auto-detect reads the full host core count and will over-provision.

---

## concurrent vs sharded mode

ember has two execution modes:

**sharded (default)**

thread-per-core architecture. each shard owns its keyspace partition and processes requests on a dedicated thread. supports all data types: strings, lists, sets, hashes, and sorted sets. best choice for mixed workloads or any workload that touches non-string types.

**concurrent (`--concurrent` flag)**

uses a DashMap-backed keyspace shared across all threads. string operations only. trades breadth for raw throughput — the absence of cross-thread routing overhead makes simple GET/SET slightly faster at high pipeline depths.

when to use concurrent mode:

- your workload is 95%+ string GET/SET operations
- you want maximum throughput and don't need other data types
- you're using ember as a pure string cache

when to use sharded mode (the default):

- you use lists, sets, hashes, or sorted sets
- you need cross-key operations like MSET, MGET
- you want the full command set
- you need TTL-based eviction and blocking commands like BLPOP

---

## pipeline depth

pipelining lets a client send multiple commands without waiting for each response. it is the single most effective lever for throughput.

| pipeline depth | typical set throughput | notes |
|---|---|---|
| P=1 | ~133k ops/sec | lowest latency (~0.7ms p99) |
| P=8 | ~900k ops/sec | good balance |
| P=16 | ~1.76M ops/sec | throughput sweet spot |
| P=64+ | diminishing returns | higher memory pressure per connection |

**recommendations:**

- use P=16 for throughput-focused workloads (batch ingestion, cache warming)
- use P=1 for latency-sensitive workloads (interactive sessions, real-time lookups)
- avoid pushing P much above 32 unless you've measured a benefit — the increased in-flight state raises memory usage without proportional gains

the `max-pipeline-depth` config option caps the number of in-flight commands per connection. the default is permissive; lower it if you see runaway memory growth from misbehaving clients.

---

## connection management

**maxclients** (default: 10000) sets the ceiling on simultaneous connections. connections beyond this limit are rejected with an error. each accepted connection holds a read buffer (4KB default) and a write buffer, so thousands of idle connections carry real memory cost.

tips for managing connections well:

- **use connection pooling in your client.** a pool of 20-50 connections per application process is usually enough. opening a new connection per request is expensive — tcp handshake overhead adds up fast.
- **set `idle-timeout-secs`** (recommended: 300) to automatically close stale connections and reclaim their buffers. this is especially important in environments where application processes come and go.
- **watch the `connected_clients` metric** in `INFO stats` or the `/metrics` prometheus endpoint. a steady climb without a corresponding traffic increase usually indicates a connection leak.

---

## memory overhead per data type

understanding per-key overhead helps size your cluster and interpret memory stats.

| data type | approximate overhead |
|---|---|
| string | ~128 bytes (entry metadata + key + value) |
| list | ~72 bytes base + VecDeque backing array + element bytes |
| set | ~216 bytes base + element bytes per member |
| hash | ~216 bytes base + field-value pairs; smaller for compact hashes |
| sorted set | ~120 bytes base + member bytes + 8 bytes per score |

these are rough figures for 64-byte values on a 64-bit system. actual usage depends on key length and value size. use `INFO memory` or the prometheus `memory_used_bytes` gauge per shard to measure real usage in your workload.

the biggest lever for reducing memory is **shorter keys**. a key that is 8 bytes vs 64 bytes saves 56 bytes per entry — at 100M keys that is over 5GB.

---

## key design

a few design habits that compound at scale:

**keep keys short.** key length is part of every entry's allocation. `u:{uid}:sess` is meaningfully cheaper than `user:{user_id}:session` at millions of keys.

**use hash tags for co-location.** keys with `{tag}` in the name hash on the tag, not the full key. this lets you group related keys onto the same shard, which enables multi-key operations without cross-shard coordination:

```
SET {user:1234}:profile ...
SET {user:1234}:prefs ...
MGET {user:1234}:profile {user:1234}:prefs   # same shard, no fan-out
```

**avoid very large collections.** commands like `SMEMBERS`, `LRANGE 0 -1`, or `HGETALL` on collections with 100k+ members block the shard thread for the duration of serialization. break large collections up, or paginate with `SSCAN`, `HSCAN`, `ZSCAN`.

**set TTLs on ephemeral data.** ember's active expiry sampler continuously evicts expired keys, but only if TTLs are set. without TTLs, old data accumulates and the eviction policy falls back to LRU under memory pressure, which is less precise.

---

## benchmarking tips

ember's built-in benchmark tool is the fastest way to test your specific configuration:

```bash
# baseline GET/SET with pipeline depth 16, 50 connections
ember-cli benchmark -t set,get -P 16 -c 50

# test non-string data types
ember-cli benchmark -t lpush,sadd,zadd,hset,hget -P 16

# simulate larger values (1KB)
ember-cli benchmark -d 1024 -P 16

# test a realistic keyspace (avoids hot-key distortion)
ember-cli benchmark -t get,set --keyspace 1000000
```

for comparison against redis, `redis-benchmark` works directly against ember (full RESP3 compatibility):

```bash
redis-benchmark -h 127.0.0.1 -p 6379 -n 1000000 -c 50 -P 16 -t set,get
```

for stress testing eviction and connection storms:

```bash
./bench/bench-stress.sh
```

**a few things to keep in mind when reading results:**

- always warm the keyspace before measuring reads — a cold cache will show artificially low GET throughput
- use `--keyspace` to avoid all requests hitting the same key, which collapses to a single-key hot spot
- run benchmarks on the same hardware and jemalloc configuration as production for comparable numbers
- p99 latency matters more than mean for most applications — check the latency histogram, not just ops/sec
