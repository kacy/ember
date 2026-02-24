# troubleshooting

common problems and how to fix them.

---

## connection issues

**"DENIED: protected mode is enabled"**

ember runs in protected mode by default when no password is set and no explicit bind address is configured. this prevents accidental exposure on public interfaces.

fix: either set a password with `requirepass <password>` in your config, or explicitly bind to the loopback interface with `bind 127.0.0.1`.

---

**connection refused**

first, confirm the server is actually running and listening on the expected address and port:

```
ember-cli ping --host 127.0.0.1 --port 6379
```

if it is running, check:
- `bind` in your config — if set to `127.0.0.1`, connections from remote hosts will be refused
- `port` in your config — default is 6379
- `maxclients` — if the limit is reached, new connections are rejected; check `INFO clients` for `connected_clients` and `rejected_connections`

---

**TLS handshake failure**

verify that the paths in your config are correct and the files are readable by the ember process:

- `tls-cert-file` — server certificate (PEM)
- `tls-key-file` — private key (PEM)
- `tls-ca-cert-file` — required when mTLS is enabled; clients must present a cert signed by this CA

a mismatch between the server cert's common name / SAN and the hostname the client connects to will also cause failures. check with:

```
openssl s_client -connect host:port -CAfile ca.pem
```

---

**"too many connections"**

`INFO clients` shows current connection count. if you are hitting the `maxclients` limit:

- increase `maxclients` in your config (default is 10000)
- audit your application for connection leaks — connections that are opened but never closed accumulate quickly under load
- use a connection pool in your client library with a bounded size

---

## memory issues

**"ERR OOM command not allowed when used memory > 'maxmemory'"**

ember is refusing writes because it has hit the configured memory ceiling.

options:
- increase `maxmemory` if the host has headroom
- switch to an eviction policy that automatically frees space: `allkeys-lru` is a safe default for cache workloads, `volatile-lru` for workloads that mix cached and persistent data

```
CONFIG SET maxmemory 4gb
CONFIG SET maxmemory-policy allkeys-lru
```

---

**memory keeps growing**

the most common cause is keys without a TTL in a cache workload — they accumulate indefinitely.

audit key patterns with SCAN (do not use KEYS in production — it blocks):

```
SCAN 0 MATCH * COUNT 100
```

check TTL on a sample of keys:

```
TTL some-key
```

if large numbers of keys have TTL -1 (no expiry), update your application to set TTLs, or switch to an `allkeys-*` eviction policy so ember can reclaim them automatically.

---

**high memory fragmentation**

fragmentation builds up over time, especially with workloads that frequently delete and reallocate keys of varying sizes. `INFO memory` reports `mem_fragmentation_ratio` — values above 1.5 are worth investigating.

- a controlled restart compacts memory immediately
- ember is built with jemalloc by default, which has better fragmentation behavior than the system allocator; if you are running a custom build, verify jemalloc is enabled

---

## persistence issues

**"ERR disk full"**

the data directory is out of space. immediate steps:

1. free space on the current volume (clear logs, old snapshots, etc.)
2. or move `data-dir` to a larger volume and restart

if you are running AOF, the rewrite process creates a temporary file that can be as large as the current AOF — make sure the volume has at least 2x the current AOF size available.

---

**slow startup**

startup time is proportional to the size of the data being replayed. a large AOF file can take a while to process.

- run `BGREWRITEAOF` periodically to compact the AOF down to the minimal set of records needed to reconstruct the current state
- if you have both a snapshot and an AOF, ember uses the snapshot as a base and only replays the AOF tail — keeping snapshots recent reduces replay time significantly

---

**data loss after crash**

check your `appendfsync` setting:

| setting | behavior | risk |
|---|---|---|
| `always` | fsync after every write | zero data loss, lower throughput |
| `everysec` | fsync once per second | up to ~1 second of data loss |
| `no` | OS decides when to flush | data loss possible on crash |

if you need zero data loss guarantees, use `appendfsync always`. for most workloads, `everysec` is a reasonable tradeoff.

---

## performance issues

**high P99 latency**

high tail latency usually points to a slow command blocking the event loop. check SLOWLOG:

```
SLOWLOG GET 10
```

common culprits:
- `KEYS *` — scans the entire keyspace; replace with SCAN
- `SMEMBERS` on large sets — returns all members in one shot; use `SSCAN` for large sets
- `LRANGE` with a large range on a long list
- `SORT` without a LIMIT on a large collection

any O(N) command where N is large will spike latency for other clients sharing that connection or shard.

---

**throughput below expected**

a few things to check:

- **pipeline depth** — single-command-per-roundtrip throughput is network-bound. use pipelining (`-P` flag in ember-cli benchmark, or batching in your client library) to get close to peak throughput
- **number of shards** — shards default to the number of logical CPU cores. if you have explicitly set a lower value, throughput will be proportionally capped
- **client concurrency** — a single non-pipelined client will not saturate the server; use multiple concurrent clients or connections

---

**CPU not fully utilized**

ember uses a thread-per-shard model. if CPU usage is low:

- verify that `shards` in your config matches (or is close to) the number of CPU cores available
- check that `SO_REUSEPORT` is enabled for the listener — this allows multiple acceptor threads to distribute incoming connections without a bottleneck at the accept syscall
- look at `INFO stats` for `total_commands_processed` over time to confirm the server is actually receiving load

---

## cluster issues

**MOVED errors in client**

`MOVED` is ember's way of telling the client that a key lives on a different node. this is expected behavior in cluster mode — it is not an error in the server.

the fix is on the client side: use a cluster-aware client that automatically follows `MOVED` redirects and maintains a slot map. most Redis-compatible clients have a cluster mode option.

---

**cluster formation fails**

when bringing up a new cluster:

- every node needs to be reachable on two ports: the data port (default 6379) and the gossip port (data port + 10000, so 16379 by default). check firewall rules for both
- if `cluster-auth-pass` is set, it must be identical on every node — a mismatch will cause nodes to reject each other's gossip messages silently
- use `CLUSTER INFO` on each node to see its view of the cluster state and `CLUSTER NODES` to see which peers it knows about

---

**split-brain**

if a network partition separates nodes, the minority partition will stop accepting writes (raft requires a majority quorum for configuration changes). this is correct behavior — it prevents conflicting writes.

to recover:
1. restore network connectivity between partitions
2. the raft leader will re-establish authority once it can communicate with a majority
3. `CLUSTER INFO` will show `cluster_state:ok` when quorum is restored

if nodes are permanently lost and quorum cannot be reached organically, you will need to manually reconfigure the cluster. do not do this lightly — it can result in data inconsistency.

---

**slot migration stuck**

a migration can stall if the destination node becomes unreachable mid-transfer.

check the current state:

```
CLUSTER INFO
```

look for `cluster_migrating_slots` or `cluster_importing_slots` with non-zero values. to reset a stuck migration:

```
CLUSTER SETSLOT <slot> STABLE
```

run this on both the source and destination nodes. the slot will return to its original owner and you can retry the migration when the cluster is healthy.

---

## debugging tools

**INFO**

`INFO` returns a snapshot of server state. useful sections:

- `INFO server` — version, uptime, config file path
- `INFO memory` — used memory, fragmentation ratio, eviction stats
- `INFO clients` — connected clients, blocked clients, rejected connections
- `INFO stats` — ops/sec, hits vs misses, expired keys
- `INFO keyspace` — key counts and TTL stats per database
- `INFO persistence` — AOF and snapshot status

```
INFO memory
INFO all
```

---

**SLOWLOG**

records commands that exceeded the slowlog threshold (default 10ms):

```
SLOWLOG GET 25        -- last 25 slow commands
SLOWLOG LEN           -- total entries in log
SLOWLOG RESET         -- clear the log
CONFIG SET slowlog-log-slower-than 5000   -- threshold in microseconds
```

---

**MONITOR**

streams every command processed by the server in real time. useful for debugging unexpected writes or identifying which client is sending a particular command:

```
MONITOR
```

use this sparingly — it adds overhead proportional to command throughput and will slow down a loaded server.

---

**CONFIG GET**

inspect any runtime configuration value without restarting:

```
CONFIG GET maxmemory
CONFIG GET appendfsync
CONFIG GET "*"          -- dump all settings
```

---

**Prometheus metrics**

if the metrics endpoint is enabled (default `/metrics` on port 9090), you get labeled histograms for command latency, counters for hits/misses, and gauges for memory and connection state. this is the best way to understand behavior over time rather than point-in-time snapshots.

---

**CLUSTER INFO / CLUSTER NODES**

for cluster deployments, these two commands are the first thing to check when something looks wrong:

```
CLUSTER INFO       -- overall cluster health and slot coverage
CLUSTER NODES      -- per-node state, role, and assigned slots
```

a healthy cluster will show `cluster_state:ok` and `cluster_slots_assigned:16384`.
