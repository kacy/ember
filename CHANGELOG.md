# changelog

## 0.4.10 (2026-07-04)

### features
- `PROTO.SCAN` and `PROTO.FIND` commands for the protobuf schema registry (#341)

### fixes
- AOF recovery: on mid-file corruption, roll back to the snapshot state instead of silently keeping a partially-applied (and possibly internally inconsistent) AOF prefix while logging "snapshot state only" (#355)
- the `vector` cargo feature (`VADD`/`VSIM`/`VADD_BATCH`/`VREM`/`VGET`/`VCARD`/`VDIM`/`VINFO`) failed to compile and is now fixed and built in CI so it can't rot again (#360)
- Dockerfile `HEALTHCHECK` fixed — the previous check used `wget` (not installed) against the metrics port (disabled by default); now uses `ember-server --healthcheck` (#355)
- helm: `requirepass` is now stored in a Secret and mounted as a file via `EMBER_REQUIREPASS_FILE` instead of a plaintext env var; added `ember.existingSecret` (#355)
- log the final AOF sync error on clean shutdown instead of swallowing it (#355)
- cluster test flakiness and a missing ember-cli binary in test builds (#343)
- security: bump `aws-lc-sys` to 0.42.0, clearing RUSTSEC-2026-0045/0047/0048 (and two more) (#363)

### deprecations
- concurrent mode (`--concurrent` / `EMBER_CONCURRENT`) is deprecated and will be removed in a future release; the server now logs a warning on startup. it only accelerates string commands and is mutually exclusive with cluster mode — the default sharded engine is the supported path (#358)

### internal
- regenerated the stale Python and TypeScript client stubs (were 17 rpcs behind the proto) and added CI gates: MSRV (1.93), python/ts client builds, a client proto-drift check, and a `vector`-feature build (#356, #360)
- moved `key_slot`/`SLOT_COUNT` slot-hashing to ember-protocol; ember-core no longer depends on ember-cluster (no longer compiles openraft). ember-cluster re-exports both names, so downstream code is unaffected (#358)
- split `execute.rs` into `exec/` sub-modules, moved `COMMAND_TABLE` to ember-protocol, and reduced command-wiring boilerplate via macros (#340, #344)
- split the 4,200-line `grpc.rs` into per-command-family modules and added an end-to-end cluster failover integration test (#359, #361)
- rewrote the architecture and docs guides; refreshed helm/compatibility/contributing docs (#362)

---

## 0.4.9 (2026-02-27)

### features
- bitmap commands: `GETBIT`, `SETBIT`, `BITCOUNT`, `BITPOS`, `BITOP` (#316, #330)
- `LMPOP` / `ZMPOP` multi-key pops (#317)
- `EXPIREAT`, `PEXPIREAT`, `EXPIRETIME`, `PEXPIRETIME`, `GETSET`, `MSETNX`, `SMOVE`, `SINTERCARD`, count args for `LPOP`/`RPOP` (#315, #318)
- `HRANDFIELD` and `ZRANDMEMBER` (#319)
- Redis 6.2+ commands: `LMOVE`, `GETDEL`, `GETEX`, `ZDIFF`, `ZINTER`, `ZUNION` (#299)
- `COMMAND`, `HINCRBYFLOAT`, `ZDIFFSTORE`/`ZINTERSTORE`/`ZUNIONSTORE`, `FLUSHALL`, `MEMORY USAGE`, `WAIT` (#312, #324, #331)
- keyspace notifications (`__keyevent@0__:expired` and write events) (#313)
- automatic snapshot scheduling (#300)
- `CONFIG SET` live-apply for maxmemory and maxmemory-policy (#312)
- ember-client: typed async RESP3 API with pipelining; CLI now reuses it (#297, #302, #303)
- ember-ts client with full API parity; npm package renamed to `emberdb` with OIDC trusted publishing (#308, #309, #332)
- grpc: 39 new rpcs closing the RESP/gRPC api gap; 17 new commands surfaced across cli, grpc, and clients (#306, #320)
- CLI watch mode and batch mode (#298)

### fixes
- BITOP cross-shard correctness (#330)
- cluster reports `cluster_state:ok` on bootstrap (#339)
- eliminated production panics in client decoder and server startup (#322)
- auth failure metrics, command memory budget, migration progress reporting (#323)
- replication send-failure counter; go client subscribe error propagation (#325)
- raft log persistence switched from bincode to postcard; dropped atomic-polyfill (#296, #329)
- prometheus counters, crash recovery hardening, TLS tests (#314)
- go client: regenerated protobuf stubs for the 17 new rpcs (#337)

### docs
- command count updated to 190+; compatibility guide refreshed (#335)

---

## 0.4.8 (2026-02-25)

### performance
- entry struct optimizations: version field moved to lazy side table, cached_value_size packed as u32, ENTRY_OVERHEAD tightened from 128 to 104 (#284-287)
- skip touch() timestamp updates when eviction is disabled (#287)
- packed hash encoding — hash memory reduced from ~451 to ~240 B/key (#276)
- vector insert throughput optimization with binary-encoded VADD_BATCH (#271-272)

### fixes
- ENTRY_OVERHEAD bumped from 100 to 104 for cross-platform CI compatibility (#292-294)
- rate-limited ENOSPC handling for AOF writes (#249)

### docs
- pruned concurrent mode from the docs (#275, #291) — correction: the mode itself was not removed and is still available behind `--concurrent`; sharded remains the default execution mode
- refreshed all benchmark numbers from 2026-02-25 GCP run (#288-290)
- added documentation section, code of conduct, performance tuning guide, production checklist (#270, #281-282)

---

## 0.4.7 — launch readiness (february 2026)

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
- parallel pipeline dispatch for multi-core throughput
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
