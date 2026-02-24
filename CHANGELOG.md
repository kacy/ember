# changelog

## 0.4.x — launch readiness (february 2026)

the final push before a proper release. this milestone focused on correctness, operability, and the kinds of features that turn a cache into something production teams can actually trust. a lot of the work here is about closing gaps — the commands that weren't there yet, the error paths that weren't handled, and the defaults that needed to be locked down.

### features
- acl system with per-user access control (command and key permissions)
- `WATCH` / `MULTI` / `EXEC` / `DISCARD` — optimistic locking and transaction support
- `BLPOP` / `BRPOP` blocking list operations with per-shard waiter registries
- `MONITOR` command for real-time command stream inspection
- `/health` http endpoint for load balancer readiness checks
- `SSCAN`, `HSCAN`, `ZSCAN` cursor-based iteration for all collection types
- `CLIENT ID`, `SETNAME`, `GETNAME`, `LIST` — connection introspection
- `COPY`, `OBJECT`, `TIME`, `LASTSAVE`, `ROLE` utility commands
- `RANDOMKEY`, `TOUCH`, `SORT` key-space commands
- full sorted set coverage: `ZREVRANK`, `ZREVRANGE`, `ZCOUNT`, `ZINCRBY`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZPOPMIN`, `ZPOPMAX`
- full set algebra: `SUNION`, `SINTER`, `SDIFF` with `STORE` variants; `SPOP`, `SRANDMEMBER`, `SMISMEMBER`
- full list coverage: `LINDEX`, `LSET`, `LTRIM`, `LINSERT`, `LREM`, `LPOS`
- string aliases: `SETNX`, `SETEX`, `PSETEX`, `GETRANGE`, `SETRANGE`, `SUBSTR`
- `TOML` config file support with `CONFIG GET`, `CONFIG SET`, `CONFIG REWRITE`
- example config file and annotated docker-compose quickstart
- apache 2.0 license

### performance
- thread-per-core workers with `SO_REUSEPORT` — connections distributed across cores at the kernel level
- `split_to` zero-copy read loop — eliminates buffer copies on the receive path
- `CompactString` keys with small-string optimization (≤24 bytes inline, no heap allocation)
- batch pipeline dispatch — flush accumulated commands as a unit rather than one at a time
- p=1 latency improvements targeting sub-millisecond round-trips
- compact hash encoding for small hash maps
- cpu pinning on linux for cache-local shard execution
- persistent raft log storage for fast cluster restarts
- keyspace and shard refactored out of the server monolith for cleaner hot-path separation
- data type micro-benchmarks added to ci

### fixes
- `ENOSPC` and oom error handling — disk-full and memory-full conditions now return proper errors instead of panicking
- config validation on startup catches invalid combinations before binding
- graceful shutdown improvements — in-flight requests drain cleanly on sigterm
- `clone` audit across hot paths — removed unnecessary allocations found during review
- expiry deduplication — redundant expiry checks consolidated
- command dispatch and connection handling split into focused modules (was one large file)

---

## 0.3.x — distributed systems and security (february 2026)

this is the "make it real" milestone. ember went from a single-node cache to a clustered system with replication, automatic failover, and the kind of security hardening you'd want before putting it in front of production traffic.

### features
- full cluster replication: `CLUSTER REPLICATE`, primary-to-replica sync stream, `CLUSTER FAILOVER`
- automatic failover via epoch-based voting — replicas elect a new primary without operator intervention
- cluster server integration wiring — gossip, raft, and slot migrations connected to the request path
- `MIGRATE` / `RESTORE` for live key transfer during resharding
- `ASK` redirects during slot migration; `MOVED` redirects for misrouted keys
- `nodes.conf` persistence — cluster topology survives restarts
- gossip slot propagation and `PingReq` relay for indirect failure detection
- resp key/value size limits — 512kb max key, 512mb max value, enforced at parse time
- hmac-sha256 cluster transport authentication (`--cluster-auth-pass`)
- redis-compatible `AUTH` command and password configuration
- `ARCHITECTURE.md` with detailed design notes
- install script and docker-compose quickstart
- redis compatibility documentation
- `MONITOR`, `SLOWLOG`, enriched `INFO` command
- `CONFIG GET` / `CONFIG SET` with runtime reconfiguration
- cluster cli subcommands: `create`, `check`, `reshard`, `rebalance`

### performance
- zero-alloc resp parsing — no heap allocation on the command parsing hot path
- `AHashMap` keyspace — faster hashing for string keys
- `Box<str>` keys — smaller representation than `String`
- single-lookup `SET NX/XX` — avoided double hash lookup on conditional sets
- `BufWriter` on replication stream — fewer syscalls for replica sync
- bincode for raft rpcs — smaller wire format than json
- o(log n) sorted set rank via vec-based structure
- incremental memory tracking — `grow_by` / `shrink_by` instead of full recompute
- concurrent pipeline dispatch for multi-core throughput
- `now_ms()` caching to avoid repeated syscalls in expiry checks

### fixes
- cluster transport frame limit reduced from 64mb to 10mb
- gossip incarnation jump limit to prevent state amplification attacks
- cli panic prevention — `truncate_id`, div-by-zero guard, `expect` → `error`
- osc escape sanitization in cli output
- server auth counter uses `saturating_add`
- 0 cves from `cargo audit`; `overflow-checks = true` in release profile

---

## 0.2.x — data types, observability, and the cli (february 2026)

with the foundation solid, this milestone filled in everything that makes ember useful day-to-day: the full redis data model, a real command-line experience, metrics, benchmarks, and the more exotic features that set ember apart.

### features
- vector similarity search — hnsw index with `VADD`, `VSEARCH`, `VREM`, `VCARD`, `VINFO`, `VADD_BATCH`; benchmarked against chromadb, qdrant, and pgvector
- protobuf schema registry — `PROTO.SET/GET/GETFIELD/SETFIELD/DELFIELD` for typed value storage
- grpc server with proto definitions; go and python grpc clients
- pub/sub — `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `PSUBSCRIBE` with pattern matching
- tls for both client connections and the cli
- encryption at rest (aes-256-gcm)
- redis-compatible `AUTH` command
- interactive cli with syntax highlighting, fuzzy autocomplete, and inline help
- `cluster` cli subcommands (meet, nodes, info, slots, keyslot)
- built-in `benchmark` subcommand (get/set/ping workloads, configurable pipeline depth)
- prometheus metrics endpoint (`/metrics`)
- `SLOWLOG` equivalent for slow command tracking
- dockerfile and helm chart for kubernetes deployment
- `MGET` / `MSET` / `FLUSHDB` / `SCAN`
- `INCR` / `DECR` / `INCRBY` / `DECRBY`
- `SET NX/XX` conditional set semantics
- `PERSIST` / `PTTL` / `PEXPIRE` millisecond expiry precision
- full hash command set: `HSET`, `HGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HINCRBY`, `HKEYS`, `HVALS`, `HMGET`
- full set command set: `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`
- full sorted set range queries by score and rank
- ci workflows, security policy

### performance
- criterion micro-benchmarks and system benchmarks vs redis
- `--shards` flag to tune the number of keyspace partitions
- jemalloc as the global allocator
- pipeline dispatch optimization — batched command processing
- concurrent keyspace mode with dashmap for read-heavy workloads
- memory overhead reduction — keyspace entry footprint profiled and trimmed
- memtier benchmark integration
- bench-all.sh for running all benchmark suites in one pass
- grpc vs resp3 comparison benchmarks

### fixes
- memory limits enforced on list and sorted set growth
- various audit findings from internal security reviews

---

## 0.1.0 — foundation (february 3–4, 2026)

the initial build. started from a blank workspace and ended with a working single-node cache that could pass basic redis compatibility tests. the goal was to get the boring-but-critical pieces right before adding anything interesting.

### features
- resp3 protocol parser and serializer — zero-copy, hand-rolled, no dependencies on redis libraries
- tcp server with tokio — accept, parse, dispatch, respond
- thread-per-core sharded engine — each shard owns a keyspace partition; no cross-shard locks on the hot path
- key hashing with fnv-1a 64-bit for shard routing
- core commands: `GET`, `SET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`
- all five redis data types: string, list, set, hash, sorted set
- append-only file (aof) — binary tlv format with crc32 checksums, configurable fsync policy
- snapshot format — binary with header, per-entry records, and integrity checks
- hybrid recovery — snapshot + aof tail on startup, corruption detection
- `BGSAVE` and `BGREWRITEAOF` commands
- lazy expiration (checked on access) and active expiration (background sampling)
- lru-approximate eviction when memory limit is hit
- memory usage tracking per shard
- graceful shutdown on sigint/sigterm with connection draining
- crate readmes and initial project structure
