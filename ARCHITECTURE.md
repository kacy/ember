# architecture

this doc explains how ember is put together today: which crates own which parts of the system, how requests move through the server, and where the main trade-offs live.

if you only need the user-facing overview, start with the [readme](README.md).

---

## table of contents

1. [crate map](#crate-map)
2. [request flow](#request-flow)
3. [sharded engine](#sharded-engine)
4. [keyspace and data types](#keyspace-and-data-types)
5. [expiration, memory, and eviction](#expiration-memory-and-eviction)
6. [protocol and connection handling](#protocol-and-connection-handling)
7. [persistence and recovery](#persistence-and-recovery)
8. [clustering and replication](#clustering-and-replication)
9. [optional features](#optional-features)
10. [background work](#background-work)

---

## crate map

ember is split into a few crates with fairly clear boundaries:

| crate | role |
|---|---|
| `crates/ember-protocol` | RESP3 parsing, frame types, and command decoding |
| `crates/ember-core` | sharded engine, keyspace, data types, memory tracking, expiration |
| `crates/ember-persistence` | AOF, snapshots, recovery, and optional encryption-at-rest support |
| `crates/ember-cluster` | slot routing, gossip, Raft state, migration machinery |
| `crates/ember-server` | TCP/TLS listeners, auth, ACL, connection handlers, metrics, replication, cluster integration |
| `crates/ember-cli` | interactive client and benchmark tool |
| `crates/ember-client` | reusable client-side helpers used by other binaries and tools |

the split is intentional: `ember-core` does not know about sockets or RESP, and `ember-protocol` does not know about the keyspace.

## request flow

the common RESP path looks like this:

1. a connection handler reads bytes from TCP or TLS into a reusable buffer.
2. `ember-protocol` parses one or more RESP3 frames.
3. `ember-server` handles auth, ACL checks, command limits, pub/sub mode, and command dispatch.
4. `ember-core` routes the request to one shard, many shards, or all shards.
5. the shard updates its local keyspace, writes AOF records if needed, emits replication events, and returns a typed response.
6. the connection handler turns that response back into a RESP frame and writes it out.

that flow is the default "sharded engine" mode. there is also a special concurrent mode for string-heavy workloads, covered below.

## sharded engine

**main code:** `crates/ember-core/src/engine.rs`, `crates/ember-core/src/shard/mod.rs`

ember's default execution model is shared-nothing sharding:

- one shard owns one partition of the keyspace
- each shard processes requests serially
- the hot path does not take locks inside the shard
- callers talk to shards through bounded channels

by default, `Engine::with_available_cores()` uses `std::thread::available_parallelism()` and creates one shard per logical CPU. `Engine::prepare()` exists for thread-per-core deployment where each OS thread runs its own single-threaded Tokio runtime and one shard.

single-key routing uses deterministic FNV-1a hashing. that matters because persistence recovery has to send a key back to the same shard after restart. cluster slot routing is a separate layer and uses CRC16, not the engine hash.

the engine exposes a small set of routing primitives:

| method | use |
|---|---|
| `route()` | single-key commands like `GET`, `SET`, `HGET`, `ZADD` |
| `route_multi()` | multi-key commands that dispatch to several shards |
| `broadcast()` | fan-out commands like `DBSIZE`, `INFO`, or `FLUSHDB` |
| `send_to_shard()` | direct shard access, mainly for shard-aware scans |
| `dispatch_to_shard()` / `dispatch_batch_to_shard()` | non-blocking dispatch used by the connection layer |

inside each shard, the main loop waits on three kinds of work:

- inbound requests from the engine
- active expiry ticks
- periodic fsync ticks when AOF is running in `everysec` mode

after the loop wakes up for a request, it drains as much queued work as it can before going back to `select!`. that is a simple optimization, but it matters a lot for pipelined clients.

the other important optimization lives one layer up: the RESP connection handler groups commands by target shard and uses `dispatch_batch_to_shard()` when several commands in the same pipeline hit the same shard. that keeps deep pipelines from burning one channel slot per command.

## keyspace and data types

**main code:** `crates/ember-core/src/keyspace/`, `crates/ember-core/src/types/`

each shard owns a `Keyspace`, which is mostly an `AHashMap<CompactString, Entry>`.

an `Entry` stores:

- the `Value`
- `expires_at_ms`
- `cached_value_size`
- `last_access_secs`

that cached size is there so write-heavy operations can update memory accounting without re-walking the whole value every time.

WATCH support does not store a version counter on every key. instead, `Keyspace` keeps a lazy side table of watched-key versions. if nobody has watched a key, mutations do not pay for version tracking.

the value enum covers the Redis-style core types plus optional feature-gated ones:

| type | backing structure | notes |
|---|---|---|
| string | `Bytes` | cheap cloning and slicing |
| list | `VecDeque<Bytes>` | efficient push/pop at both ends |
| sorted set | boxed `SortedSet` | custom structure, exposed as `zset` |
| hash | boxed `HashValue` | packed representation for small hashes, hashmap for larger ones |
| set | boxed `HashSet<String>` | plain hash set storage |
| vector | `VectorSet` | only when `--features vector` is enabled |
| proto | `{ type_name, data }` | only when `--features protobuf` is enabled |

the hash implementation is worth calling out. small hashes stay in a compact packed layout for better locality and lower overhead, then promote to a full hashmap once they grow. that keeps common cases like user-profile style hashes cheap.

## expiration, memory, and eviction

**main code:** `crates/ember-core/src/expiry.rs`, `crates/ember-core/src/memory.rs`, `crates/ember-core/src/keyspace/mod.rs`

expiration happens in two ways:

- **lazy expiration**: any access checks whether the key has expired and removes it on the spot
- **active expiration**: every shard runs a periodic sampler that checks a small random set of keys and deletes expired ones in the background

the active expiry code uses the same general idea Redis uses: random sampling instead of a global time wheel or sorted expiry heap. it is boring, but it keeps the write path simple and avoids an extra index for every TTL-bearing key.

memory accounting is explicit. `MemoryTracker` updates on every mutation and keeps a running total of estimated bytes used by the shard. there is no full keyspace scan in the normal path.

the memory numbers are estimates, not allocator-trace precision. to keep that safe, ember applies a 90% safety margin to configured memory limits before it starts rejecting writes or evicting keys.

the only eviction policy today is approximate `allkeys-lru`. when a shard is over its effective limit, it samples a small number of keys and evicts the least recently accessed one from that sample. this keeps eviction cost predictable.

large collections are also freed lazily. if dropping a value would be expensive, the shard hands it to a background drop thread instead of doing destructor work inline.

## protocol and connection handling

**main code:** `crates/ember-protocol/src/parse.rs`, `crates/ember-server/src/connection/`, `crates/ember-server/src/connection_common.rs`

the RESP3 parser is single-pass and can work in zero-copy mode when the caller has a `Bytes` buffer. it also has hard limits for nesting depth, array size, and bulk string size so malformed input cannot force absurd allocations.

the sharded RESP handler in `crates/ember-server/src/connection/mod.rs` follows a two-phase pattern:

1. parse as many complete frames as are available
2. prepare and dispatch each command
3. collect shard replies in input order
4. serialize all responses into one output buffer
5. flush

that is why pipelining scales well even when a batch touches several shards.

authentication is handled before full command execution. ember supports:

- `requirepass` style password auth
- ACL-backed auth with per-user permissions
- constant-time password comparisons on the hot paths that matter

pub/sub is handled in a dedicated subscriber mode. once a connection subscribes, the handler parks in a loop that multiplexes incoming subscription messages and unsubscribe commands until the connection leaves subscriber mode.

monitor mode is similar: the connection subscribes to a broadcast stream of observed commands and stays there until disconnect.

TLS is not a separate server architecture. it uses the same connection machinery after the TLS handshake.

### concurrent mode

**main code:** `crates/ember-server/src/concurrent_handler.rs`

ember also has an optional concurrent path that bypasses shard channels for basic string operations. it uses `ConcurrentKeyspace` and direct concurrent access for `GET`/`SET` style workloads.

this mode is faster for that narrow case, but it is not the general execution model. the sharded engine is still the main design and the one the rest of the system is built around.

## persistence and recovery

**main code:** `crates/ember-persistence/src/aof.rs`, `crates/ember-persistence/src/snapshot.rs`, `crates/ember-persistence/src/recovery.rs`

persistence is per-shard:

- each shard has its own AOF file
- each shard has its own snapshot file
- recovery can run shard-by-shard in parallel

the AOF format is binary and append-only. every record has a stable tag plus a CRC32 checksum. snapshots are also binary, include a header and footer checksum, and are written by streaming the live keyspace once.

both snapshot writes and AOF rewrites use the usual safe pattern:

1. write a temporary file next to the real one
2. flush and fsync it
3. atomically rename it into place

that way a crash during the write does not corrupt the last good file.

recovery is straightforward:

1. load the snapshot if one exists
2. replay the AOF on top
3. drop entries whose TTL expired while the server was down
4. if a file is corrupt, warn and recover what can be recovered

`BGREWRITEAOF` works by writing a fresh snapshot of the current shard state and then truncating the shard's AOF back to just its header before new writes continue.

### encryption at rest

when the `encryption` feature is enabled, AOF records and snapshot entries can be stored in v3 encrypted form using AES-256-GCM.

encryption is done per record, not per file. that keeps incremental replay simple and avoids decrypting an entire file just to read one record.

## clustering and replication

**main code:** `crates/ember-cluster/src/`, `crates/ember-server/src/cluster.rs`, `crates/ember-server/src/replication.rs`

the cluster layer sits above the local shard engine.

### hash slots and routing

cluster routing follows the Redis Cluster model:

- 16,384 hash slots
- CRC16 slot calculation
- hash tags with `{...}` for colocating related keys
- `MOVED` and `ASK` redirects during topology changes

slot ownership is tracked separately from local shard ownership. first a request has to land on the right node, then the node's local engine sends it to the right shard.

### membership and topology changes

membership and failure detection use SWIM-style gossip. topology changes such as adding nodes, removing nodes, assigning slots, or promoting replicas go through Raft.

that split is deliberate:

- gossip is fast and cheap for "who is alive?"
- Raft is slower but gives a single agreed view for "who owns what?"

the data path does not go through Raft. normal reads and writes stay local to the node that owns the slot.

`crates/ember-server/src/cluster.rs` is the bridge between the generic cluster crate and the running server. it owns the local `ClusterState`, the gossip engine, migration manager, and optional attached `RaftNode`.

### replication

replication is a primary-to-replica stream built on top of the same persistence primitives used for local durability:

1. a replica connects and performs a handshake
2. the primary sends one in-memory snapshot per shard plus the current offset
3. the replica loads those snapshots
4. the primary streams incremental `AofRecord` updates

successful mutations inside a shard emit `ReplicationEvent`s with monotonically increasing per-shard offsets. the replication server uses those offsets for streaming and for the `WAIT` command's acknowledgement tracking.

### slot migration

live resharding moves slots between nodes without taking the cluster offline. while a slot is in motion, the source and target coordinate with migration state plus `ASK` redirects. once the move is final, clients see `MOVED` and can update their slot map permanently.

## optional features

ember keeps several subsystems behind compile-time features so the default binary stays smaller and simpler.

| feature | effect |
|---|---|
| `vector` | adds HNSW-backed vector similarity search and the `V*` command family |
| `protobuf` | adds schema-aware protobuf storage and `PROTO.*` commands |
| `grpc` | exposes the engine through a tonic gRPC service in addition to RESP |
| `encryption` | enables encrypted persistence files |

these features are wired through the same engine rather than creating a separate execution path.

for example:

- vector commands still route through `ShardRequest` and the normal shard loop
- protobuf values still live in the same keyspace as other values
- gRPC translates requests into the same engine operations the RESP path uses

## background work

a few subsystems run off the main request path:

| subsystem | purpose |
|---|---|
| drop thread | frees large collections without stalling a shard |
| fsync thread | handles `appendfsync everysec` work |
| metrics tasks | collect stats for Prometheus and health endpoints |
| keyspace notification task | listens for expired-key broadcasts when enabled |
| gossip / cluster tasks | drive membership, reconciliation, and failover work |
| replication server | streams snapshots and AOF records to replicas |

the general pattern in ember is consistent: keep the shard loop focused on request execution, and push blocking or cleanup-heavy work to dedicated background tasks when that makes latency more predictable.

---

if you are tracing a bug through the stack, the usual starting points are:

- `crates/ember-server/src/connection/` for command parsing and dispatch
- `crates/ember-core/src/shard/mod.rs` for shard execution
- `crates/ember-core/src/keyspace/` for data structure behavior
- `crates/ember-persistence/src/` for durability and replay
- `crates/ember-server/src/cluster.rs` and `crates/ember-cluster/src/` for distributed behavior
