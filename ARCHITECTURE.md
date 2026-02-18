# architecture

this document describes the internal design of ember: the decisions that were made, the alternatives that were considered, and the trade-offs that were accepted. it is intended for contributors, auditors, and anyone who wants to understand how the system actually works.

for a high-level introduction, see the [readme](README.md).

---

## table of contents

1. [execution model](#1-execution-model)
2. [protocol layer](#2-protocol-layer)
3. [shard main loop](#3-shard-main-loop)
4. [data model](#4-data-model)
5. [memory management](#5-memory-management)
6. [connection handling](#6-connection-handling)
7. [persistence](#7-persistence)
8. [cluster layer](#8-cluster-layer)
9. [optional features](#9-optional-features)
10. [background subsystems](#10-background-subsystems)
11. [compile-time features](#11-compile-time-features)

---

## 1. execution model

ember supports two execution modes that can be selected at startup.

### sharded mode (default)

the default mode assigns one tokio task to each logical CPU core, and each task owns an exclusive partition of the keyspace. there is no locking on the hot path — commands are routed to the correct shard over a bounded mpsc channel, processed, and replied to via a oneshot.

**key routing** uses FNV-1a 64-bit hashing rather than the xxhash family. the reason is determinism across restarts: FNV-1a uses fixed constants (`offset = 0xcbf29ce484222325`, `prime = 0x100000001b3`) whereas xxhash seeds itself per-process. since AOF and snapshot recovery must route recovered keys to the exact same shard they were written from, a non-deterministic hash would corrupt the keyspace on startup.

the channel buffer per shard is `SHARD_BUFFER = 256`. this is large enough to absorb pipelined command bursts without meaningful back-pressure on connections, but small enough that a stalled shard becomes visible quickly.

`Engine::with_available_cores()` queries `std::thread::available_parallelism()` and creates one shard per logical core, falling back to one shard if the count can't be determined.

the routing api on `Engine` has five methods:

| method | purpose |
|---|---|
| `route(key, req)` | single-key commands (GET, SET, ZADD, etc.) |
| `route_multi(keys, req_fn)` | multi-key commands (DEL, EXISTS) |
| `broadcast(req_fn)` | fan-out to all shards (DBSIZE, INFO, FLUSHDB) |
| `send_to_shard(idx, req)` | direct shard access by index (SCAN) |
| `dispatch_to_shard(idx, req)` | non-blocking dispatch returning a oneshot receiver |

sharded mode supports all five data types and all 107 commands.

### concurrent mode (`--concurrent`)

an alternative backed by `DashMap<Box<str>, Entry>`. connection handlers access the map directly, with no channels or per-shard tasks. `Box<str>` is used instead of `String` to save 8 bytes per key (no capacity field).

this mode is roughly 2× faster for pure string workloads because it eliminates the channel round-trip. the trade-off is that it only supports strings (GET, SET, and basic key commands). complex data types require shard-local ownership that DashMap can't provide.

the reason both modes exist: concurrent is strictly better for string-only workloads (session caches, simple counters), while sharded is necessary for applications using lists, sorted sets, hashes, or sets. rather than force a single compromise, both are available.

---

## 2. protocol layer

**location:** `crates/ember-protocol/src/`

### parser

the parser operates on a buffered byte slice. it uses a `Cursor<&[u8]>` to track position through the input without consuming or copying it — the caller retains ownership of the buffer and can retry once more data arrives from the network.

earlier versions used a two-pass approach: a `check()` function to verify that a complete frame existed, followed by a separate `parse()` call to build the `Frame` value. this scanned every byte twice. the current single-pass design builds `Frame` values directly, returning `Incomplete` if the buffer runs short. this eliminates the redundant scan.

`\r` scanning inside bulk strings uses `memchr::memchr`, which uses SIMD instructions on supported platforms (16–32 bytes per cycle vs. 1 byte naive). for typical command framing this is a small but measurable win.

four security limits prevent amplification attacks:

| constant | value | reason |
|---|---|---|
| `MAX_NESTING_DEPTH` | 64 | prevents stack overflow from deeply nested frames |
| `MAX_ARRAY_ELEMENTS` | 1,048,576 | prevents memory amplification (claimed count × sizeof allocation) |
| `MAX_BULK_LEN` | 512 MB | matches redis, prevents absurd single-value allocations |
| `PREALLOC_CAP` | 1,024 | caps `Vec::with_capacity` so a declared element count of 1M costs at most ~72KB upfront |

### serializer

the serializer writes frames directly into a `BytesMut` buffer with no intermediate allocations. integer-to-string conversion uses the `itoa` crate, which formats on the stack rather than allocating a temporary string.

hot-path responses are pre-cached as static byte slices:

- `OK` → `+OK\r\n`
- `PONG` → `+PONG\r\n`
- `NULL` → `$-1\r\n`
- integers 0, 1, -1 → `:0\r\n`, `:1\r\n`, `:-1\r\n`

these cover the vast majority of GET misses, SET replies, and boolean results without any formatting work.

---

## 3. shard main loop

**location:** `crates/ember-core/src/shard.rs`

each shard runs a `tokio::select!` loop over three events:

1. incoming message from the mpsc channel
2. expiry tick every 100ms (`EXPIRY_TICK`)
3. fsync tick every 1 second (`FSYNC_INTERVAL`, only active with `EverySec` policy)

both ticks use `MissedTickBehavior::Skip`. under heavy load a tick will be missed rather than queued. this prevents ticks from piling up during bursts and then flooding the loop afterward.

**burst drain**: after the first message is received from `select!`, the loop immediately tries `try_recv()` in a tight inner loop, processing messages without re-entering `select!` until the channel is empty. this amortizes the overhead of the select mechanism across entire pipeline batches rather than paying it per command.

some requests (snapshot, aof rewrite, async flush) need to execute outside the normal request/response path. these are identified by the `RequestKind` enum and handled with `continue` — they send their response through a different path and skip the normal response machinery.

---

## 4. data model

**location:** `crates/ember-core/src/types/`, `crates/ember-core/src/keyspace.rs`

### value types

```
Value::String(Bytes)
Value::List(VecDeque<Bytes>)
Value::SortedSet(SortedSet)
Value::Hash(HashMap<String, Bytes>)
Value::Set(HashSet<String>)
```

`Bytes` is an arc-backed reference-counted buffer from the `bytes` crate. passing a value between shard and connection never copies the underlying data — only the arc pointer is cloned.

`List` uses `VecDeque` for O(1) push and pop at both ends. a plain `Vec` was considered but rejected because `push_front` is O(n).

`SortedSet` is backed by a `BTreeMap` keyed by `(ordered_float::OrderedFloat, member_string)`. this naturally maintains score order. a skip list would allow O(log n) rank queries, but `BTreeMap` is simpler, correct, and fast enough for current workloads. the skip list is noted as a future improvement if rank queries become a bottleneck.

### entry struct

each keyspace entry holds:

```
expires_at_ms: u64   // monotonic ms timestamp; 0 = no expiry
last_access_ms: u64  // for LRU approximation
value: Value
```

`expires_at_ms` is `u64` with 0 as sentinel rather than `Option<Instant>`. this saves 8 bytes per entry (no option discriminant, no padding for niche optimization failures) and is a common trade-off in hot-path data structures.

### expiration

**lazy expiration**: every key access checks `expires_at_ms` against the current monotonic time. expired keys are removed immediately and treated as absent. there is zero overhead when no TTL is set.

**active expiration**: a background sampling cycle runs every 100ms (`EXPIRY_TICK`). each cycle randomly samples up to `SAMPLE_SIZE = 20` keys and removes any that are expired. if more than 25% (`EXPIRED_THRESHOLD`) of the sample was expired, the cycle runs again immediately, up to `MAX_ROUNDS = 3` times per tick. this is the same algorithm redis uses — it is simple, requires no auxiliary data structure, and works across all TTL ranges.

the alternative (a time wheel or sorted expiry index) would give O(1) expiry in the best case but adds per-entry memory and write-path overhead. the sampling approach stays efficient as long as the expired fraction is not extremely high.

---

## 5. memory management

**location:** `crates/ember-core/src/memory.rs`

memory usage is tracked incrementally via `MemoryTracker` — callers call `add`, `remove`, or `replace` on every mutation. there is no periodic full scan.

**per-entry overhead** is estimated at `ENTRY_OVERHEAD = 128` bytes. this accounts for:
- the `String` key struct: 24 bytes (pointer + length + capacity) on 64-bit
- the `Entry` struct fields: value enum tag + inline `Bytes` struct + two `u64` timestamps
- hashbrown bookkeeping: 1 control byte per slot plus ~12.5% empty slot waste at 87.5% load factor

the constant is calibrated for 64-bit x86-64 and aarch64. on 32-bit systems, estimates would be smaller; the effect is earlier eviction, not incorrect behavior. overestimating is preferred.

**effective memory limit**: the configured `max_memory` is multiplied by `MEMORY_SAFETY_MARGIN_PERCENT = 90` before being used as the write limit. a server configured with 1 GB starts evicting at ~922 MB of estimated usage. the 10% headroom absorbs allocator fragmentation and estimation error so the process doesn't OOM before eviction has a chance to kick in.

**lru eviction**: when the effective limit is reached, the keyspace samples `EVICTION_SAMPLE_SIZE = 16` random keys and evicts the one with the oldest `last_access_ms`. this is O(1) per eviction. a sorted eviction queue would give O(log n) but costs per-write overhead to maintain. the sampling trade-off — approximate lru accuracy for guaranteed constant eviction cost — is the same approach redis uses.

**lazy free**: collections with more than `LAZY_FREE_THRESHOLD = 64` elements are sent to a background drop thread rather than freed on the shard. this is covered in [background subsystems](#10-background-subsystems).

---

## 6. connection handling

**location:** `crates/ember-server/src/connection.rs`, `crates/ember-server/src/connection_common.rs`

### buffers and limits

| constant | value | meaning |
|---|---|---|
| `BUF_CAPACITY` | 4 KB | initial read and write buffer size |
| `MAX_BUF_SIZE` | 64 MB | read buffer disconnect threshold |
| `IDLE_TIMEOUT` | 300s | matches redis 6.2+ defaults |
| `MAX_PIPELINE_DEPTH` | 10,000 | max commands buffered before forced flush |
| `MAX_AUTH_FAILURES` | 10 | disconnect after this many failed auth attempts |
| `MAX_SUBSCRIPTIONS_PER_CONN` | 10,000 | cap on pub/sub subscriptions per connection |
| `MAX_PATTERN_LEN` | 256 | max pattern length for PSUBSCRIBE |

### pipelining

the connection handler uses a dispatch-collect pattern to handle pipelined commands:

1. parse all frames available in the read buffer
2. for each command, dispatch it to the appropriate shard non-blocking (sends to the mpsc channel and holds the oneshot receiver)
3. once all commands are dispatched, collect responses in order
4. serialize all responses into the write buffer
5. flush

this allows all shards involved in a pipeline batch to execute concurrently. a batch touching four different shards processes all four in parallel rather than serially.

if more than `MAX_PIPELINE_DEPTH` frames are pending, the handler flushes early to bound memory usage.

### response shaping

`ResponseTag` is a lightweight enum (~40 variants) that encodes how a `ShardResponse` should be converted to a wire `Frame`. rather than keeping the full `Command` alive while waiting for a shard reply, the handler stores just the tag. this centralizes all response-shaping logic and avoids cloning command data.

### authentication

password comparison uses `subtle::ConstantTimeEq` to prevent timing side-channels. after `MAX_AUTH_FAILURES = 10` failed attempts, the connection is terminated.

---

## 7. persistence

**location:** `crates/ember-persistence/src/`

### append-only file (aof)

each shard writes its own AOF file (`shard-{id}.aof`). file layout:

```
[EAOF magic: 4B][version: 1B]
[record]*
```

each record:

```
[tag: 1B][payload...][crc32: 4B]
```

the CRC32 (crc32fast) covers the tag and payload bytes. a corrupt record aborts replay gracefully rather than silently loading bad data.

tag values are stable and never reassigned. there are 26 defined tags covering all mutations:

- string: SET, INCR, DECR, INCRBY, DECRBY, APPEND
- list: LPUSH, RPUSH, LPOP, RPOP
- sorted set: ZADD, ZREM
- hash: HSET, HDEL, HINCRBY
- set: SADD, SREM
- key lifecycle: DEL, EXPIRE, PERSIST, PEXPIRE, RENAME
- optional (vector): VADD, VREM
- optional (protobuf): PROTO_SET, PROTO_REGISTER

three fsync modes are supported: `Always` (fsync after every write), `EverySec` (background thread wakes every second), `No` (OS buffer only).

format versions: v1 was strings only, v2 added type-tagged records, v3 adds AES-256-GCM encryption (requires `--features encryption`).

### snapshot

each shard writes its own snapshot file (`shard-{id}.snap`). file layout:

```
[ESNP magic: 4B][version: 1B][shard_id: 2B][entry_count: 4B]
[entries...]
[footer_crc32: 4B]
```

each entry:

```
[key_len: 4B][key][type_tag: 1B][type-specific payload][expire_ms: 8B]
```

seven type tags (0–6): string, list, sorted set, hash, set, proto, vector.

`expire_ms` stores the remaining TTL in milliseconds, or -1 for no expiry. this allows the recovery path to filter entries whose TTL expired while the server was down.

writes go to a `.tmp` file and are atomically renamed on completion. a partial or crashed snapshot write never corrupts the existing snapshot.

### recovery

on startup, each shard:

1. loads its snapshot (bulk restore)
2. replays its AOF tail on top
3. filters out entries whose TTL has already expired
4. if neither file exists, starts empty
5. if a file is corrupt, logs a warning and starts with whatever data could be read

because each shard has its own files and uses deterministic key routing (FNV-1a), shards can replay in parallel on multi-core systems.

### encryption at rest

enabled with `--features encryption`. each record (AOF or snapshot entry) is independently encrypted with AES-256-GCM and a random 12-byte OS-generated nonce. overhead per record: 12 bytes nonce + 16 bytes authentication tag.

per-record nonces allow individual records to be read without decrypting the whole file, which matters for incremental AOF replay.

the encryption key is stored as `[u8; 32]` — no heap allocation. on drop, the key bytes are zeroed using `write_volatile` to prevent the compiler from eliminating the zeroing as a dead store.

the key file is either 32 raw bytes or 64 hex characters.

---

## 8. cluster layer

**location:** `crates/ember-cluster/src/`

### hash slots

the cluster divides the keyspace into 16,384 slots (the redis cluster standard). a key's slot is `crc16(hash_input) mod 16384`, using the XMODEM polynomial — the same as redis.

**hash tags**: if a key contains `{...}`, only the content between the first `{` and the first `}` is hashed. this lets clients co-locate keys: `user:{123}:profile` and `session:{123}` land on the same slot. an empty tag (`foo{}bar`) falls back to hashing the full key.

`SlotMap` is stored as `Box<[Option<NodeId>; 16384]>`. boxing avoids placing 128KB on the stack when the type is initialized.

### node identity

each node has a `NodeId(Uuid)` generated at startup (UUID v4). `Display` shows the first 8 characters, similar to git short hashes, for readability in logs and CLI output. the full UUID string is used as a key in BTreeMap-backed storage.

### gossip / SWIM failure detection

ember uses the SWIM protocol for failure detection and membership management.

each protocol period (default 1 second):

1. pick a random node and send PING
2. if no ACK within `probe_timeout` (500ms), send PING-REQ to `indirect_probes = 3` random nodes asking them to probe on our behalf
3. if still no ACK, mark the node SUSPECT
4. after `suspicion_mult × protocol_period` (5 seconds by default), promote to DEAD

a node can refute its own suspicion by incrementing its incarnation number and gossiping an `Alive` message. `MAX_INCARNATION = u64::MAX / 2` caps incarnation values — a malicious node sending max values would permanently disable refutation.

up to `max_piggyback = 10` state updates are piggybacked on every message to converge cluster state without dedicated gossip rounds.

**wire format**: compact binary, little-endian. five message types: PING (1), PING-REQ (2), ACK (3), JOIN (4), WELCOME (5). read helpers (`safe_get_u8`, `safe_get_u16_le`, `safe_get_u64_le`) return `io::Error` on truncated input rather than panicking. decoded arrays are capped at `MAX_COLLECTION_COUNT = 1024` elements to prevent allocation bombs.

### raft

a single openraft group manages cluster configuration changes: adding or removing nodes, assigning slots, promoting replicas, and tracking migrations. the data path is not routed through raft.

the raft log is in-memory (`BTreeMap<u64, Entry>` behind `RwLock`). this is intentional: cluster configuration changes are rare and the cluster state can be re-learned from gossip on restart. persisting the raft log is deferred.

commands through raft: `AddNode`, `RemoveNode`, `AssignSlots`, `PromoteReplica`, `BeginMigration`, `CompleteMigration`.

### slot migration

live resharding moves slots from one node to another without downtime. the migration FSM has five states:

```
Importing → Migrating → Streaming → Finalizing → Complete
```

during migration:
- reads are served from the source. if the key has already moved, a `MOVED` or `ASK` redirect is returned to the client.
- writes to a migrating slot get an `ASK` redirect to the target.

`MOVED` means the slot is permanently at the new node. `ASK` means the key might still be at the source — the client should try the redirect but not update its slot map.

`MigrationId(u64)` is generated as `timestamp_ns XOR slot XOR random(16-bit)`. the random component handles clock granularity collisions.

---

## 9. optional features

these features are disabled by default and compiled away entirely when not enabled. binary size and hot-path performance are unaffected.

### vector similarity search (`--features vector`)

adds `Value::Vector(VectorSet)` and the `VEC.*` command family. the HNSW index is provided by the `usearch` crate (C++ library via FFI), which ships battle-tested SIMD implementations for distance computation.

supported distance metrics: cosine, l2 (squared euclidean), inner product.

supported quantization: f32 (4 bytes/element), f16 (2 bytes/element), i8 (1 byte/element).

HNSW parameters `M` (connectivity, default 12, max 1024) and `ef_construction` (default 32, max 1024) are locked after the first insert — dimension, metric, quantization, and graph structure cannot change once data exists.

### protobuf storage (`--features protobuf`)

adds `Value::Proto { type_name, data: Bytes }` and the `PROTO.*` command family.

clients register schemas via `PROTO.REGISTER` (a compiled `FileDescriptorSet`). the server can then decode, mutate specific fields, and re-encode protobuf values without the client needing to include protobuf libraries for simple field operations. `PROTO.GETFIELD` and `PROTO.SETFIELD` use dot-notation paths for nested fields.

### grpc (`--features grpc`)

exposes the same engine through a `tonic` gRPC service. the gRPC layer translates proto requests into `ShardRequest` values internally — the routing and shard execution paths are identical to RESP3.

auth is via the `authorization` metadata header with constant-time comparison.

input limits: keys ≤ 512 KB, values ≤ 512 MB, vector dimensions ≤ 65,536, batch counts ≤ 10,000. max message size 4 MB.

### encryption at rest (`--features encryption`)

covered in the [persistence section](#7-persistence).

---

## 10. background subsystems

### drop thread (`crates/ember-core/src/dropper.rs`)

dropping large collections (lists with thousands of elements, large hash maps) is CPU-bound work. running it on a shard's event loop would stall command processing for the duration of the destructor call.

`DropHandle` sends large values to a dedicated `std::thread` (named `ember-drop`) over a bounded `std::sync::mpsc` channel with capacity `DROP_CHANNEL_CAPACITY = 4096`. the thread simply drains the channel — receiving each item and letting its destructor run.

`try_send` is used rather than blocking send. if the channel is full (drop thread is behind), the value is dropped inline on the shard rather than blocking the hot path. this is the same lazyfree strategy redis uses.

a `DropHandle` is shared across all shards. when the last handle is dropped, the channel closes and the thread exits.

only large values are deferred. strings (`Bytes`) are always dropped inline because `Bytes::drop` is O(1) reference count decrement. collections are considered large when they exceed `LAZY_FREE_THRESHOLD = 64` elements.

### fsync thread

when `appendfsync = everysec`, a per-shard background thread wakes every `FSYNC_INTERVAL = 1` second and calls `fsync` on the AOF file. writes are batched into this thread rather than blocking on the shard, keeping the command loop unaffected by disk latency.

### stats poller

when `--metrics-port` is configured, a background task periodically polls per-shard stats atomically for prometheus exposition. the poller is separate from the shard loops so the metrics scrape does not add latency to command processing.

---

## 11. compile-time features

features are additive — disabling a feature compiles away its code entirely. there is no dead code or unused allocation in the binary for disabled features.

feature propagation: `ember-server` features flow down to `emberkv-core`, which flows to `ember-persistence`. enabling `encryption` on the server binary enables it through the whole stack.

`jemalloc` is a default feature on the server binary. it substitutes the system allocator with jemalloc, which has better fragmentation characteristics under concurrent allocation patterns. disabling it falls back to the system allocator.

---

*updated to reflect the state of the codebase as of the clustering milestone.*
