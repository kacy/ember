# Ember: A Modern Distributed Cache

> *"Simplicity is the ultimate sophistication."*

Ember is a low-latency, memory-efficient, distributed cache written in Rust. It aims to beat Redis on every metric while maintaining a small, elegant codebase that inspires good development practices.

---

## Core Philosophy

### Design Principles

1. **Simplicity over features** — Every line of code must earn its place. Resist feature creep. A 20k LOC codebase that does the essential things perfectly beats 200k LOC that does everything poorly.

2. **Correctness over cleverness** — Prefer boring, obviously-correct code. Clever optimizations come later, backed by benchmarks. Make invalid states unrepresentable through Rust's type system.

3. **Measure everything** — No optimization without profiling. Every design decision should be backed by data. Build benchmarking into the development workflow from day one.

4. **Documentation as code** — If it's not documented, it doesn't exist. Every public API, every architectural decision, every non-obvious implementation detail gets explained.

5. **Ecosystem first** — The cache itself is just the kernel. Client libraries, tooling, integrations, and documentation are equally important products.

---

* Use Rust best patterns, ensuring that things like unwrap are avoided unless absolutely necessary.
* This should be a bullet-proof, bug free key value store.
* Add unit tests, but only for core functionality. Avoid tests for the sake of tests. However, you will want to definitely cover edge cases.
* The code and documentation should feel like it was written by a human.
* Commit frequently. Commits should be clear, atomic, and consistent history of changes.
* Avoid spilling out a full file. Think of problems from a high level then dive in.
* Commits should feel like a natural development process, not a complete product the first attempt.
* Pull Requests for complete features. Avoid massive pull requests. I'll be here to unblock and review for you.
* Pull Requests should never have tasks. Remember to make this feel like a human wrote it.
* Opt for lower case things, both in documentation and commits. It feels more humand and dev focused.
* If there are collisions on naming, your second option is `emberkv` or `EmberKV`.
* Be sure to include a .gitignore, Dockerfile, and Makefile.
* The gitignore file will NOT have a reference to CLAUD, but you should also never commit that file.
* Focus on ease of readibility. Remember, this code should be highly performant, but ultimately will be read by many humans.
* Tasks should never be added to pull requests.
* For every Pull Request, you are required to list: a summary, what was tested, and optionally design considerations. Never list tasks in Pull Requests.
* Almost never commit directly to main. If you have commits, it's most likely best to add it to a different branch so that you can create a PR. Sometimes for small changes it makes sense. Use your best judgement.

## Competitive Advantages Over Redis

### Performance

| Dimension | Redis | Ember Target | Strategy |
|-----------|-------|--------------|----------|
| Throughput | ~100k ops/sec/core | 500k+ ops/sec/core | Thread-per-core, shared-nothing |
| P99 Latency | ~1ms | <200µs | io_uring, lock-free structures |
| Memory/key | ~90 bytes overhead | <40 bytes | Custom allocators, compact repr |
| Cold start | Seconds (large datasets) | Sub-second | Parallel loading, memory mapping |

### Architecture Wins

**Thread-per-core model**: Redis is single-threaded. Ember dedicates one thread to each CPU core, with each thread owning a partition of the keyspace. No cross-thread synchronization on the hot path. This is how Dragonfly achieves 25x Redis throughput.

**io_uring everywhere**: Linux's io_uring provides async I/O with minimal syscall overhead. Use `tokio-uring` for network I/O, persistence writes, and replication. Falls back gracefully on older kernels/macOS.

**Zero-copy networking**: Parse RESP3 directly from the receive buffer. Serialize responses directly into the send buffer. Use `bytes::Bytes` for reference-counted, zero-copy value passing.

**Smarter memory layout**: 
- Small String Optimization: Inline strings ≤23 bytes (no heap allocation)
- Tagged pointers for type discrimination (use the low bits)
- Slab allocators for fixed-size structures
- Optional compression for values >1KB (LZ4)

### Developer Experience Wins

**First-class observability**: Prometheus metrics built-in, structured logging, distributed tracing with OpenTelemetry. Redis requires external tooling; Ember has it native.

**Typed client libraries**: Generate clients from an IDL. Full TypeScript types, Rust derive macros, Python type hints. No more stringly-typed APIs.

**Modern CLI**: Syntax highlighting, fuzzy autocomplete, inline docs, `--watch` mode, cluster visualization. Make the terminal experience delightful.

**Hermetic local development**: Single binary runs everything. `ember dev` gives you a 3-node cluster locally with one command. Zero configuration to get started.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client Layer                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ Rust Client │  │  TS Client  │  │  Go Client  │  ...         │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Protocol Layer                           │
│         RESP3 (Redis compat)  │  gRPC (native, optional)        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Connection Layer                           │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Acceptor (SO_REUSEPORT) → Parser → Router → Responder   │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Engine Layer                              │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐                 │
│  │  Core 0    │  │  Core 1    │  │  Core N    │  (shared-nothing)│
│  │ ┌────────┐ │  │ ┌────────┐ │  │ ┌────────┐ │                 │
│  │ │Keyspace│ │  │ │Keyspace│ │  │ │Keyspace│ │                 │
│  │ └────────┘ │  │ └────────┘ │  │ └────────┘ │                 │
│  │ ┌────────┐ │  │ ┌────────┐ │  │ ┌────────┐ │                 │
│  │ │  AOF   │ │  │ │  AOF   │ │  │ │  AOF   │ │                 │
│  │ └────────┘ │  │ └────────┘ │  │ └────────┘ │                 │
│  └────────────┘  └────────────┘  └────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Cluster Layer                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │  Raft (openraft)│  │ Gossip (SWIM)   │  │ Slot Migration  │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
ember/
├── Cargo.toml
├── CLAUDE.md                    # This file - development guide
├── README.md                    # User-facing documentation
├── ARCHITECTURE.md              # Deep technical documentation
│
├── crates/
│   ├── ember-server/            # Main server binary (~3k LOC target)
│   │   ├── src/
│   │   │   ├── main.rs          # Entry point, CLI parsing
│   │   │   ├── config.rs        # Configuration management
│   │   │   ├── server.rs        # Connection acceptor, lifecycle
│   │   │   └── metrics.rs       # Prometheus exposition
│   │   └── Cargo.toml
│   │
│   ├── ember-core/              # Core engine (~5k LOC target)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── engine.rs        # Thread-per-core coordinator
│   │   │   ├── shard.rs         # Single-threaded shard logic
│   │   │   ├── keyspace.rs      # Main key-value store
│   │   │   ├── types/           # Data type implementations
│   │   │   │   ├── mod.rs
│   │   │   │   ├── string.rs    # String with SSO
│   │   │   │   ├── list.rs      # Doubly-linked list
│   │   │   │   ├── set.rs       # Hash set
│   │   │   │   ├── sorted_set.rs # Skip list implementation
│   │   │   │   └── hash.rs      # Hash map
│   │   │   ├── expiry.rs        # TTL management
│   │   │   └── memory.rs        # Allocator, memory tracking
│   │   └── Cargo.toml
│   │
│   ├── ember-protocol/          # Wire protocol (~2k LOC target)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── resp3/           # RESP3 parser/serializer
│   │   │   │   ├── mod.rs
│   │   │   │   ├── parse.rs     # Zero-copy parsing
│   │   │   │   └── serialize.rs # Direct-to-buffer writing
│   │   │   └── command.rs       # Command definitions
│   │   └── Cargo.toml
│   │
│   ├── ember-persistence/       # Durability (~2k LOC target)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── aof.rs           # Append-only file
│   │   │   ├── snapshot.rs      # Point-in-time snapshots
│   │   │   └── recovery.rs      # Startup recovery logic
│   │   └── Cargo.toml
│   │
│   ├── ember-cluster/           # Distribution (~4k LOC target)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── topology.rs      # Cluster state machine
│   │   │   ├── gossip.rs        # SWIM protocol
│   │   │   ├── raft.rs          # openraft integration
│   │   │   ├── slots.rs         # Hash slot management
│   │   │   └── migration.rs     # Live resharding
│   │   └── Cargo.toml
│   │
│   └── ember-cli/               # CLI tool (~1.5k LOC target)
│       ├── src/
│       │   ├── main.rs
│       │   ├── repl.rs          # Interactive shell
│       │   ├── commands.rs      # CLI commands
│       │   └── format.rs        # Pretty printing
│       └── Cargo.toml
│
├── clients/
│   ├── ember-rs/                # Official Rust client
│   ├── ember-ts/                # Official TypeScript client
│   ├── ember-go/                # Official Go client
│   └── ember-py/                # Official Python client
│
├── bench/                       # Benchmarking suite
│   ├── src/
│   │   ├── main.rs
│   │   └── workloads/           # Realistic workload generators
│   ├── compare-redis.sh         # Head-to-head comparison script
│   └── results/                 # Historical benchmark data
│
├── tests/
│   ├── integration/             # Integration tests
│   ├── chaos/                   # Chaos engineering tests
│   └── redis-compat/            # Redis compatibility tests
│
└── docs/
    ├── book/                    # mdBook user guide
    ├── api/                     # Generated API docs
    └── rfcs/                    # Design documents
```

**Total target: ~18k LOC** for the core server (excluding clients, tests, docs).

---

## Development Phases

### Phase 1: Foundation (Weeks 1-4) ✅

**Goal**: Single-node server that passes basic Redis compatibility tests.

#### Week 1: Protocol & Skeleton

- [x] Set up workspace with all crate stubs
- [x] Implement RESP3 parser (zero-copy, hand-rolled)
- [x] Implement RESP3 serializer (direct to `BytesMut`)
- [x] Basic TCP server with tokio (accept, parse, echo)
- [x] Command dispatch skeleton
- [x] Graceful shutdown on SIGINT/SIGTERM with connection draining

**Key files**: `ember-protocol/src/resp3/*.rs`, `ember-server/src/server.rs`

**Milestone**: `redis-cli PING` returns `PONG` ✅

#### Week 2: Core Data Types

- [ ] `SmallString` type with inline storage ≤23 bytes — *deferred (perf optimization)*
- [x] `Value` enum with String, List, Set, Hash, SortedSet variants
- [x] `Keyspace` with `HashMap<String, Entry>`
- [x] `Entry` struct: value + expiry + last_access metadata
- [x] Basic commands: GET, SET, DEL, EXISTS, EXPIRE, TTL

**Key files**: `ember-core/src/types/*.rs`, `ember-core/src/keyspace.rs`

**Milestone**: Pass Redis string command tests ✅

#### Week 3: Thread-Per-Core Engine

- [x] Shard structure (owns a keyspace partition)
- [x] Engine coordinator (routes requests to shards)
- [x] Key hashing (xxhash)
- [x] Cross-shard messaging (MGET/MSET fan-out)
- [ ] CPU pinning and NUMA awareness — *deferred (perf optimization)*

**Key files**: `ember-core/src/engine.rs`, `ember-core/src/shard.rs`

**Milestone**: Linear scaling up to core count on GET/SET benchmark ✅

#### Week 4: Expiration & Memory

- [x] Lazy expiration (check on access)
- [x] Active expiration (background sampling)
- [ ] Time wheel for efficient TTL tracking — *using simpler per-access check*
- [x] Memory usage tracking per shard
- [ ] `MEMORY STATS` command — *deferred*
- [x] Basic eviction (LRU approximation when memory limit hit)

**Key files**: `ember-core/src/expiry.rs`, `ember-core/src/memory.rs`

**Milestone**: Stable memory usage under sustained load with TTLs ✅

---

### Phase 2: Persistence (Weeks 5-7) ✅

**Goal**: Durable storage with fast recovery.

#### Week 5: Append-Only File

- [x] AOF writer (batched writes, configurable fsync: always/everysec/no)
- [x] AOF format (binary TLV with CRC32 checksums)
- [x] Background fsync thread
- [x] AOF replay on startup
- [x] `BGREWRITEAOF` command

**Design decision**: Binary AOF format with per-record CRC32. Supports all data types: strings, lists, sorted sets, hashes, sets.

**Key files**: `ember-persistence/src/aof.rs`

#### Week 6: Snapshots

- [x] Snapshot format (custom binary with header + entries)
- [x] Snapshot writer (iterate keyspace, serialize)
- [ ] Fork-based snapshotting (COW for consistency) — *deferred (using in-process)*
- [ ] Parallel snapshot loading — *deferred*
- [x] `BGSAVE` command

**Key files**: `ember-persistence/src/snapshot.rs`

#### Week 7: Hybrid Persistence & Recovery

- [x] Snapshot + AOF tail recovery
- [ ] Automatic snapshot scheduling — *deferred*
- [ ] Recovery progress reporting — *deferred*
- [x] Corruption detection (CRC32 checksums on AOF records and snapshots)
- [ ] `ember doctor` CLI for persistence health — *deferred*

**Key files**: `ember-persistence/src/recovery.rs`

**Milestone**: Survives kill -9, recovers full dataset ✅

---

### Phase 3: Additional Data Types (Weeks 8-9) ✅

**Goal**: Feature parity on data structures.

#### Week 8: Sorted Sets

- [x] BTreeMap-based sorted set (score ordering with member lookup)
- [x] ZADD (with NX/XX/GT/LT/CH flags), ZRANGE (with WITHSCORES), ZRANK, ZSCORE, ZREM, ZCARD
- [x] Range queries by score and rank
- [x] Memory tracking integration

**Note**: Using BTreeMap for simplicity; skip list would give O(log n) rank queries.

#### Week 9: Lists, Hashes, Sets & Polish

- [x] VecDeque-based list with O(1) head/tail access
- [x] LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN
- [ ] Blocking operations: BLPOP, BRPOP — *deferred (requires async coordination)*
- [x] Hashes: HSET, HGET, HGETALL, HDEL, HEXISTS, HLEN, HINCRBY, HKEYS, HVALS, HMGET
- [x] Sets: SADD, SREM, SMEMBERS, SISMEMBER, SCARD
- [x] Command audit: 62 commands implemented, all with AOF persistence

**Milestone**: All 5 data types fully functional with persistence ✅

---

### Phase 4: Clustering (Weeks 10-14) ✅

**Goal**: Horizontal scaling with automatic failover.

#### Week 10: Cluster Topology

- [x] Hash slot mapping (16384 slots)
- [x] Node identity (UUID + address)
- [x] Cluster state structure
- [x] `CLUSTER SLOTS`, `CLUSTER NODES`, `CLUSTER INFO`, `CLUSTER KEYSLOT`, `CLUSTER MYID` commands
- [x] `MOVED` and `ASK` redirect types

**Key files**: `ember-cluster/src/topology.rs`, `ember-cluster/src/slots.rs`

#### Week 11: Gossip Protocol

- [x] SWIM protocol implementation
- [x] Binary wire format for gossip messages
- [x] Failure detection (suspicion, confirmation)
- [x] Protocol period tuning (configurable)
- [x] Cluster state convergence via piggybacked updates

**Key files**: `ember-cluster/src/gossip.rs`, `ember-cluster/src/message.rs`

#### Week 12: Raft Integration

- [x] openraft integration for consensus
- [x] Single Raft group for cluster configuration
- [x] Cluster commands: AddNode, RemoveNode, AssignSlots, PromoteReplica
- [x] State machine with membership tracking
- [ ] Persistent log storage — *deferred (in-memory for now)*

**Design decision**: Single Raft group for the whole cluster. Raft log is in-memory; persistence deferred.

**Key files**: `ember-cluster/src/raft.rs`

#### Weeks 13-14: Resharding & Operations

- [x] Slot migration protocol and state machine
- [x] Migration types: Importing, Migrating, Streaming, Finalizing, Complete
- [x] CLUSTER SETSLOT commands (IMPORTING, MIGRATING, NODE, STABLE)
- [x] CLUSTER MEET, ADDSLOTS, DELSLOTS, FORGET, REPLICATE, FAILOVER
- [x] MIGRATE command for key transfer
- [x] Key tracking during migration for ASK redirects
- [ ] Server integration — *modules complete, wiring deferred*

**Key files**: `ember-cluster/src/migration.rs`

**Milestone**: Cluster layer complete (56 tests). Server integration pending.

---

### Phase 5: Developer Experience (Weeks 15-18)

**Goal**: Make Ember a joy to use.

#### Week 15: Observability

- [x] Prometheus metrics endpoint (`/metrics`)
- [x] Key metrics: ops/sec, latency histograms, memory, connections
- [x] Structured logging with `tracing`
- [ ] Distributed tracing (OpenTelemetry) — *deferred (heavy dependency tree, marginal value for a cache)*
- [x] `SLOWLOG` equivalent
- [x] `INFO` command with rich stats

#### Week 16: CLI

- [ ] Interactive REPL with rustyline
- [ ] Syntax highlighting
- [ ] Command autocomplete
- [ ] Inline help (`help SET`)
- [ ] `ember cluster` subcommands
- [ ] `ember benchmark` built-in

**Key files**: `ember-cli/src/*.rs`

#### Week 17: Client Libraries

- [ ] Rust client with connection pooling, pipelining
- [ ] TypeScript client (generated from schema)
- [ ] Python client
- [ ] Go client
- [ ] Client documentation

**Key files**: `clients/*/`

#### Week 18: Documentation & Polish

- [ ] mdBook user guide
- [ ] API documentation
- [ ] Migration guide from Redis
- [ ] Docker image (`FROM scratch` — single static binary)
- [ ] Helm chart for Kubernetes
- [ ] GitHub Actions CI/CD

**Milestone**: New user can go from zero to running cluster in 5 minutes

---

## Benchmarking Strategy

### Methodology

Every PR that touches performance-critical code must include benchmark results. Use `criterion` for micro-benchmarks and a custom harness for system benchmarks.

### Key Benchmarks

```bash
# quick sanity check (ember only)
./scripts/bench-quick.sh

# full comparison with Redis
./scripts/bench.sh

# memory usage comparison
./scripts/bench-memory.sh
```

### Metrics to Track

| Metric | Target | Achieved | Redis Baseline |
|--------|--------|----------|----------------|
| SET throughput (P=16) | 500k+ ops/sec | **1.86M ops/sec** | 1.0M ops/sec |
| GET throughput (P=16) | 500k+ ops/sec | **2.48M ops/sec** | 1.16M ops/sec |
| SET throughput (P=1) | 100k+ ops/sec | **200k ops/sec** | 100k ops/sec |
| GET throughput (P=1) | 100k+ ops/sec | **200k ops/sec** | 100k ops/sec |
| P99 latency | <1ms | **0.4ms** | 0.4ms |
| Memory per string key (64B value) | <200B | **257B** | ~165B |

*Benchmarked on GCP c2-standard-8 (8 vCPU Intel Xeon @ 3.1GHz) using concurrent mode with jemalloc.*

### Workloads

1. **GET-heavy** (95% GET, 5% SET) — read cache scenario
2. **Balanced** (50% GET, 50% SET) — session store
3. **Write-heavy** (20% GET, 80% SET) — logging/metrics
4. **Sorted set** (ZADD, ZRANGE) — leaderboard scenario
5. **Large values** (1KB-1MB values) — document cache
6. **High cardinality** (100M+ keys) — memory efficiency test

---

## Code Standards

### Style

- Run `cargo fmt` before every commit
- Run `cargo clippy` with warnings as errors
- No `unsafe` without a comment explaining why and a `// SAFETY:` annotation
- No `unwrap()` in library code; use proper error handling
- Prefer `thiserror` for error types

### Documentation

Every public item needs:
- A doc comment explaining what it does
- Examples for complex APIs
- Panic conditions documented
- Performance characteristics noted where relevant

```rust
/// Inserts a key-value pair into the keyspace.
///
/// If the key already exists, the old value is returned.
/// TTL is preserved if not explicitly set.
///
/// # Performance
/// O(1) average case. May trigger eviction if memory limit is reached.
///
/// # Examples
/// ```
/// let mut ks = Keyspace::new(config);
/// ks.set("greeting", "hello", None);
/// assert_eq!(ks.get("greeting"), Some(b"hello"));
/// ```
pub fn set(&mut self, key: impl Into<Key>, value: impl Into<Value>, ttl: Option<Duration>) -> Option<Value>
```

### Testing

- Unit tests next to implementation (`#[cfg(test)]` modules)
- Integration tests in `tests/integration/`
- Property-based tests with `proptest` for data structures
- Chaos tests for clustering (network partitions, node failures)
- Redis compatibility tests (run same test against Redis and Ember)

### Git Workflow

- `main` is always deployable
- Feature branches, squash merge
- PRs require: passing CI, benchmark comparison (if perf-relevant), one review
- Conventional commits: `feat:`, `fix:`, `perf:`, `docs:`, `refactor:`

---

## Ecosystem Building

### Community

- **Discord server** for real-time discussion
- **GitHub Discussions** for longer-form Q&A
- **Office hours** (monthly video call for contributors)
- **Good first issues** labeled and mentored

### Contributions We Want

- Client libraries in other languages
- Integrations (Spring, Django, Rails, etc.)
- Deployment guides (AWS, GCP, k8s, etc.)
- Performance investigations
- Documentation improvements

### What Success Looks Like

| Metric | 6 months | 1 year | 2 years |
|--------|----------|--------|---------|
| GitHub stars | 1k | 5k | 15k |
| Contributors | 10 | 30 | 100 |
| Production users | 5 | 50 | 500 |
| Client languages | 4 | 8 | 12 |

---

## Anti-Goals

Things we explicitly **will not** do:

1. **Lua scripting** — Complex, security surface, and Lua is showing its age. If we add scripting, it'll be WASM-based.

2. **Redis Modules API compatibility** — The API is too tied to Redis internals. We'll have our own plugin system.

3. **Every Redis command** — Focus on the 80% that covers 99% of use cases. Esoteric commands can wait.

4. **Sentinel compatibility** — Raft-based clustering is strictly better. No need for a separate Sentinel system.

5. **Multi-master writes to same key** — CRDTs are complex and often unnecessary. Last-write-wins with clear semantics is fine.

---

## Open Questions

Decisions to make during development:

1. **Client-side caching protocol** — Redis 6 introduced server-assisted client caching. Worth implementing? Adds complexity but can dramatically reduce traffic.

2. **WASM plugins** — Allow users to deploy custom logic to the server? High value but significant engineering investment.

3. **Tiered storage** — Automatic spill to SSD for cold data? Would differentiate from Redis but adds complexity.

4. **Native replication vs. Raft** — Raft is correct but has overhead. For read replicas, simpler async replication might be better.

5. **Query language** — Beyond key-value, should we support filtering/projection? Scope creep risk.

---

## Resources

### Inspiration

- [Dragonfly](https://github.com/dragonflydb/dragonfly) — Thread-per-core architecture
- [KeyDB](https://github.com/Snapchat/KeyDB) — Multi-threaded Redis fork
- [Skytable](https://github.com/skytable/skytable) — Rust key-value store
- [Redis source](https://github.com/redis/redis) — The original

### Technical References

- [RESP3 specification](https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md)
- [Redis Cluster specification](https://redis.io/docs/reference/cluster-spec/)
- [io_uring and networking](https://kernel.dk/io_uring.pdf)
- [SWIM protocol paper](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
- [Raft paper](https://raft.github.io/raft.pdf)
- [Skip list paper](https://www.cl.cam.ac.uk/teaching/2005/Algorithms/skiplists.pdf)

### Rust Libraries

- `tokio` / `tokio-uring` — Async runtime
- `bytes` — Zero-copy buffers
- `openraft` — Raft consensus
- `crossbeam` — Concurrent data structures
- `criterion` — Benchmarking
- `tracing` — Structured logging
- `clap` — CLI parsing
- `thiserror` / `anyhow` — Error handling

---

## Getting Started

```bash
# Clone and build
git clone https://github.com/yourorg/ember
cd ember
cargo build --release

# Run single node
./target/release/ember-server

# Run local cluster (3 nodes)
./target/release/ember-server --dev-cluster

# Connect with CLI
./target/release/ember-cli

# Run benchmarks
cargo bench
./bench/compare-redis.sh
```

---

*This document is the source of truth for Ember development. Update it as decisions are made and the project evolves.*
