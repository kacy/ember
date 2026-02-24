# production checklist

a practical guide for running ember in production. this covers the areas that most commonly cause problems: OS configuration, memory, persistence, monitoring, security, and clustering.

---

## 1. os tuning

before running ember under real load, a few kernel settings make a meaningful difference.

**memory overcommit**

```
vm.overcommit_memory = 1
```

linux's default heuristic overcommit can cause fork-based operations (snapshots, AOF rewrite) to fail even when there is plenty of physical memory. setting `= 1` allows all allocations unconditionally, which is the right behavior for a cache server that manages its own memory budget.

add to `/etc/sysctl.conf` and apply with `sysctl -p`.

**connection queue depth**

```
net.core.somaxconn = 65535
```

the kernel listen backlog caps how many connections can be queued before being accepted. the default (128 on many distros) is far too low for a busy cache. set this alongside `net.ipv4.tcp_max_syn_backlog = 65535`.

**file descriptor limit**

ember opens one file descriptor per client connection plus several for AOF and snapshot files. the default per-process limit of 1024 is not enough.

```bash
ulimit -n 65536
```

for systemd-managed deployments, set `LimitNOFILE=65536` in the service unit. for container deployments, set `ulimits` in your container runtime config.

**transparent hugepages**

THP interacts badly with the allocator patterns of long-running memory-intensive processes. disable it:

```bash
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag
```

make it persistent via `/etc/rc.local` or a systemd `ExecStartPre` in your ember service unit.

**tcp keepalive**

detect dead clients faster and reclaim their connections:

```
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6
```

this will mark a connection dead after roughly 60 + (10 * 6) = 120 seconds of silence from a broken peer.

---

## 2. memory sizing

**maxmemory guideline**

set `maxmemory` to approximately 80% of available RAM. the remaining 20% is needed for:

- the OS page cache (important for AOF and snapshot I/O)
- the AOF rewrite buffer, which accumulates writes during a rewrite cycle
- connection buffers and protocol parsing overhead

on a 32 GB host, a reasonable starting point is `maxmemory = 25gb`.

**per-key overhead**

in sharded mode, each key consumes roughly 128 bytes of overhead above the value itself. this includes the key string, entry metadata (expiry, access time), hash table bookkeeping, and shard-level tracking.

**capacity formula**

```
rough_keys = maxmemory / (128 + avg_value_size_in_bytes)
```

for 64-byte average values on a 25 GB limit:

```
rough_keys = 26_843_545_600 / (128 + 64) = ~139 million keys
```

treat this as an upper bound. real workloads with mixed value sizes will vary.

**internal eviction threshold**

ember begins eviction internally when usage reaches 90% of `maxmemory`. this gives the engine room to evict proactively rather than reacting right at the limit. you will see evictions in metrics before the hard limit is hit.

**eviction policy**

two options cover most production cases:

- `noeviction` — reject writes when the limit is reached. clients receive an error. use this when data loss is not acceptable and you would rather shed load than silently drop keys.
- `allkeys-lru` — evict the least recently used keys across all keyspaces. use this for pure cache workloads where stale eviction is acceptable.

set `maxmemory-policy` in your config file.

---

## 3. persistence

**aof (append-only file)**

AOF is recommended for production deployments that care about durability. enable it with:

```toml
appendonly = true
```

each write command is logged to the AOF before the response is sent. on restart, the AOF is replayed to reconstruct the dataset.

`appendfsync` controls when data is flushed to disk:

| value | behavior | risk |
|---|---|---|
| `always` | fsync after every write | safest, lowest throughput |
| `everysec` | fsync once per second (background) | good default, up to 1 second of data loss |
| `no` | let the OS decide | fastest, data loss window is unbounded |

`everysec` is the right default for most deployments.

**snapshots**

use `BGSAVE` to trigger a point-in-time snapshot of the full dataset. snapshots complement AOF: a recent snapshot plus the AOF tail gives fast recovery without replaying the entire log from the beginning.

**storage**

put `data-dir` on an SSD. AOF writes are sequential and fast, but an HDD will become a bottleneck at high write rates. snapshot I/O is also I/O-intensive during the write phase.

**aof compaction**

over time the AOF grows as commands accumulate. run `BGREWRITEAOF` to compact it. ember rewrites the log to represent only the current state of each key, discarding redundant operations. schedule this during low-traffic periods.

**parallel recovery**

ember writes one AOF file per shard. on startup, shards replay their own AOF files in parallel, which means recovery time scales with the size of the largest shard rather than the total dataset size. keep shard counts consistent across restarts.

---

## 4. monitoring

**prometheus metrics**

ember exposes a prometheus-compatible `/metrics` endpoint. configure the port in your config:

```toml
metrics-port = 9100
```

example prometheus scrape config:

```yaml
scrape_configs:
  - job_name: ember
    static_configs:
      - targets: ['localhost:9100']
```

**alerts to configure**

| metric | what it signals |
|---|---|
| `ember_used_memory_bytes` | approaching maxmemory limit |
| `ember_total_commands_processed_total` | ops/sec baseline and anomaly detection |
| `ember_connected_clients` | client connection exhaustion |
| `ember_rejected_connections_total` | hitting the connection limit or eviction policy |
| `ember_keyspace_hits_total` and `ember_keyspace_misses_total` | cache hit rate degradation |

a hit rate drop often signals a key expiry wave, a traffic shift, or a client-side bug before anything else does.

**slowlog**

use `SLOWLOG GET` to inspect commands that exceeded the slowlog threshold. this is the first place to look when latency spikes without an obvious cause. the threshold is configurable; `10000` microseconds (10ms) is a reasonable starting point.

**INFO command**

`INFO` returns a structured snapshot of server state. useful sections:

- `INFO server` — version, uptime, config file path
- `INFO clients` — connected clients, blocked clients
- `INFO memory` — used memory, peak memory, allocator stats
- `INFO stats` — ops/sec, rejected connections, evictions
- `INFO keyspace` — key count and TTL stats per db
- `INFO persistence` — AOF enabled, last save time, AOF rewrite status

run `INFO all` to get everything at once.

---

## 5. tls

ember supports TLS for encrypting client connections. configure it in your config file:

```toml
tls-port = 6380
tls-cert-file = /etc/ember/server.crt
tls-key-file  = /etc/ember/server.key
```

the plain RESP port and the TLS port operate independently. you can run both simultaneously during a migration, or disable the plain port entirely once clients are updated.

**client certificate verification (mtls)**

to require clients to present a certificate:

```toml
tls-ca-cert-file  = /etc/ember/ca.crt
tls-auth-clients  = yes
```

with `tls-auth-clients = yes`, connections without a valid client certificate are rejected at the TLS handshake before any commands are processed.

rotate certificates before they expire. ember does not automatically reload certificates at runtime; a rolling restart is required after rotation.

---

## 6. security

**authentication**

set a strong password with `requirepass`. all clients must issue `AUTH <password>` before any other commands are accepted.

```toml
requirepass = "a-long-random-string"
```

**acl**

for multi-tenant or multi-team environments, use ACLs to restrict which commands and keys each user can access. `ACL SETUSER`, `ACL LIST`, and `ACL WHOAMI` manage this at runtime. define users in the config file for persistence across restarts.

**protected mode**

when `requirepass` is not set and ember is bound to a public interface, protected mode will reject external connections. this is a safety net, not a substitute for proper authentication. do not rely on it in production.

**bind address**

bind to the specific interface that clients use, not `0.0.0.0`:

```toml
bind = "10.0.1.42"
```

if ember should accept connections only on localhost, bind to `127.0.0.1`. use a firewall (security group, iptables, etc.) as an additional layer.

**cluster transport auth**

in clustered deployments, inter-node gossip and Raft messages are authenticated with HMAC-SHA256. set the same key on every node:

```toml
cluster-auth-pass = "shared-cluster-secret"
```

nodes that do not present the correct HMAC are rejected at the transport layer.

**key and value size limits**

ember enforces maximum sizes by default:

- key length: 512 KB
- value length: 512 MB

these limits protect against accidentally storing oversized data that degrades memory efficiency. they are not configurable at runtime today.

---

## 7. clustering

**initial setup**

on the first node, enable clustering and bootstrap a new cluster:

```toml
[cluster]
enabled   = true
bootstrap = true
```

on subsequent nodes, set `enabled = true` without `bootstrap`. then use `CLUSTER MEET` from any existing node to introduce the new node:

```
CLUSTER MEET <ip> <port>
```

**hash slots**

ember uses 16384 hash slots. every key maps to exactly one slot via CRC16, and every slot belongs to exactly one primary. slot assignments are visible via `CLUSTER SLOTS` and `CLUSTER INFO`.

**production topology**

for a fault-tolerant cluster:

- minimum 3 primaries, each owning a portion of the 16384 slots
- at least one replica per primary
- primaries and replicas should be on separate physical hosts or availability zones

a 3-primary / 3-replica setup (6 nodes total) can survive the loss of any single node without data loss or service interruption.

**failover**

manual failover:

```
CLUSTER FAILOVER
```

run this on a replica to promote it to primary. the outgoing primary will demote itself cleanly if it is reachable.

automatic failover triggers when a primary is unreachable. replicas hold an epoch-based election; the replica with the most up-to-date replication offset wins. the cluster should recover within a few seconds of failure detection.

**resharding**

use the CLI to move slots between nodes:

```bash
ember-cli cluster reshard
ember-cli cluster rebalance
```

resharding migrates keys live without downtime. clients receive `MOVED` or `ASK` redirects during migration and should follow them automatically.

---

## 8. backup and restore

**taking a backup**

```
BGSAVE
```

this writes a snapshot of the current dataset to the `data-dir`. the snapshot is consistent as of the moment `BGSAVE` was issued. it runs in the background; use `LASTSAVE` to check when it completed.

for AOF-enabled deployments, the snapshot plus the AOF written after the snapshot together represent the full durable state.

**automated backups**

schedule a cron job or external orchestration to:

1. issue `BGSAVE` via the CLI or a client
2. wait for the save to complete (`LASTSAVE` changes)
3. copy the `data-dir` contents to remote storage (S3, GCS, etc.)

for AOF-enabled deployments, also copy the current AOF file. make sure to copy both files in a consistent order (snapshot first, then AOF) so that the AOF tail is always at least as recent as the snapshot.

**restore**

to restore from a backup:

1. stop the running ember instance (if any)
2. copy the snapshot and AOF files into `data-dir`
3. start ember pointing at that `data-dir`

ember will automatically detect the snapshot and AOF on startup, load the snapshot first, then replay the AOF tail. no manual intervention is needed.

---

*keep this checklist close when provisioning new instances. most production incidents trace back to one of these areas being missed during initial setup.*
