# migrating from redis to ember

Ember speaks RESP3, so the first thing to know is that your existing Redis client library almost certainly works today without any code changes. Point it at the same host and port and you're running. The steps below help you go from "it works" to "it's fully configured and production-ready."

---

## table of contents

1. [command compatibility](#command-compatibility)
2. [configuration mapping](#configuration-mapping)
3. [behavioral differences](#behavioral-differences)
4. [migration steps](#migration-steps)

---

## command compatibility

Ember implements over 135 Redis commands. The tables below show what's supported, what's missing, and any behavioral differences worth knowing about.

Legend: `✓` supported, `~` partial or with caveats, `✗` not supported.

### strings

| command | status | notes |
|---------|--------|-------|
| GET | ✓ | |
| SET | ✓ | EX, PX, NX, XX flags all supported |
| MGET | ✓ | |
| MSET | ✓ | |
| INCR | ✓ | |
| DECR | ✓ | |
| INCRBY | ✓ | |
| DECRBY | ✓ | |
| INCRBYFLOAT | ✓ | |
| APPEND | ✓ | |
| STRLEN | ✓ | |
| GETRANGE | ✓ | |
| SETRANGE | ✓ | |
| SETNX | ✓ | legacy alias; prefer `SET key value NX` |
| SETEX | ✓ | legacy alias; prefer `SET key value EX seconds` |
| PSETEX | ✓ | legacy alias; prefer `SET key value PX millis` |
| SUBSTR | ✓ | alias for GETRANGE |
| GETSET | ✗ | use `SET key value GET` instead |
| GETDEL | ✗ | not implemented |
| GETEX | ✗ | not implemented |
| MSETNX | ✗ | not implemented |

### lists

| command | status | notes |
|---------|--------|-------|
| LPUSH | ✓ | multi-value |
| RPUSH | ✓ | multi-value |
| LPOP | ✓ | |
| RPOP | ✓ | |
| LRANGE | ✓ | |
| LLEN | ✓ | |
| LINDEX | ✓ | |
| LSET | ✓ | |
| LTRIM | ✓ | |
| LINSERT | ✓ | BEFORE and AFTER pivot |
| LREM | ✓ | |
| LPOS | ✓ | COUNT and RANK options supported |
| BLPOP | ✓ | multi-key with timeout |
| BRPOP | ✓ | multi-key with timeout |
| LPUSHX | ✗ | not implemented |
| RPUSHX | ✗ | not implemented |
| LMOVE | ✗ | not implemented |
| LMPOP | ✗ | not implemented |
| BLMOVE | ✗ | not implemented |

### sets

| command | status | notes |
|---------|--------|-------|
| SADD | ✓ | multi-member |
| SREM | ✓ | multi-member |
| SMEMBERS | ✓ | |
| SISMEMBER | ✓ | |
| SMISMEMBER | ✓ | |
| SCARD | ✓ | |
| SUNION | ✓ | |
| SINTER | ✓ | |
| SDIFF | ✓ | |
| SUNIONSTORE | ✓ | |
| SINTERSTORE | ✓ | |
| SDIFFSTORE | ✓ | |
| SRANDMEMBER | ✓ | optional count |
| SPOP | ✓ | optional count |
| SSCAN | ✓ | |
| SMOVE | ✗ | not implemented |
| SINTERCARD | ✗ | not implemented |

### sorted sets

| command | status | notes |
|---------|--------|-------|
| ZADD | ✓ | NX, XX, GT, LT, CH flags all supported |
| ZREM | ✓ | multi-member |
| ZSCORE | ✓ | |
| ZRANK | ✓ | |
| ZREVRANK | ✓ | |
| ZCARD | ✓ | |
| ZRANGE | ✓ | rank-based with optional WITHSCORES |
| ZREVRANGE | ✓ | |
| ZCOUNT | ✓ | |
| ZINCRBY | ✓ | |
| ZRANGEBYSCORE | ✓ | |
| ZREVRANGEBYSCORE | ✓ | |
| ZPOPMIN | ✓ | optional count |
| ZPOPMAX | ✓ | optional count |
| ZSCAN | ✓ | |
| ZRANGEBYLEX | ✗ | not implemented |
| ZRANGESTORE | ✗ | not implemented |
| ZLEXCOUNT | ✗ | not implemented |
| BZPOPMIN | ✗ | not implemented |
| BZPOPMAX | ✗ | not implemented |
| ZRANDMEMBER | ✗ | not implemented |
| ZUNIONSTORE | ✗ | not implemented |
| ZINTERSTORE | ✗ | not implemented |
| ZDIFFSTORE | ✗ | not implemented |
| ZUNION | ✗ | not implemented |
| ZINTER | ✗ | not implemented |
| ZDIFF | ✗ | not implemented |
| ZMPOP | ✗ | not implemented |
| ZMSCORE | ✗ | not implemented |

### hashes

| command | status | notes |
|---------|--------|-------|
| HSET | ✓ | multi-field |
| HGET | ✓ | |
| HGETALL | ✓ | |
| HDEL | ✓ | multi-field |
| HEXISTS | ✓ | |
| HLEN | ✓ | |
| HINCRBY | ✓ | |
| HKEYS | ✓ | |
| HVALS | ✓ | |
| HMGET | ✓ | |
| HSCAN | ✓ | |
| HMSET | ✗ | use HSET with multiple fields instead |
| HINCRBYFLOAT | ✗ | not implemented |
| HRANDFIELD | ✗ | not implemented |

### keys

| command | status | notes |
|---------|--------|-------|
| DEL | ✓ | multi-key |
| UNLINK | ✓ | async background free for large collections |
| EXISTS | ✓ | multi-key |
| RENAME | ~ | source and destination must hash to the same shard |
| COPY | ✓ | |
| KEYS | ✓ | glob patterns supported |
| SCAN | ✓ | cursor is shard-aware but opaque — functionally identical to Redis |
| TYPE | ✓ | |
| EXPIRE | ✓ | |
| TTL | ✓ | |
| PERSIST | ✓ | |
| PEXPIRE | ✓ | |
| PTTL | ✓ | |
| OBJECT | ✓ | ENCODING and REFCOUNT subcommands |
| TOUCH | ✓ | multi-key |
| RANDOMKEY | ✓ | |
| SORT | ✓ | |
| RENAMENX | ✗ | not implemented |
| MOVE | ✗ | single database only |
| SELECT | ✗ | single database only |
| SWAPDB | ✗ | single database only |
| DUMP | ✗ | not implemented |
| RESTORE | ~ | supported for cluster MIGRATE only |
| WAIT | ✗ | not implemented |
| EXPIREAT | ✗ | not implemented |
| PEXPIREAT | ✗ | not implemented |
| EXPIRETIME | ✗ | not implemented |
| PEXPIRETIME | ✗ | not implemented |

### server

| command | status | notes |
|---------|--------|-------|
| DBSIZE | ✓ | |
| INFO | ✓ | server, keyspace, replication, clients, memory, stats sections |
| TIME | ✓ | |
| LASTSAVE | ✓ | |
| ROLE | ✓ | primary / replica |
| BGSAVE | ✓ | triggers background snapshot |
| BGREWRITEAOF | ✓ | rewrites AOF from current snapshot |
| FLUSHDB | ✓ | ASYNC mode supported |
| CONFIG GET | ✓ | glob pattern matching |
| CONFIG SET | ✓ | mutable: slowlog-log-slower-than, slowlog-max-len |
| CONFIG REWRITE | ✗ | not implemented |
| CONFIG RESETSTAT | ✗ | not implemented |
| SLOWLOG GET | ✓ | optional count argument |
| SLOWLOG LEN | ✓ | |
| SLOWLOG RESET | ✓ | |
| FLUSHALL | ✗ | use FLUSHDB instead |
| SAVE | ✗ | use BGSAVE instead |
| SHUTDOWN | ✗ | send SIGTERM to the process |
| DEBUG | ✗ | not implemented |
| COMMAND | ✗ | not implemented |
| COMMAND COUNT | ✗ | not implemented |
| COMMAND INFO | ✗ | not implemented |
| COMMAND DOCS | ✗ | not implemented |
| REPLICAOF | ✗ | use CLUSTER REPLICATE instead |
| SLAVEOF | ✗ | use CLUSTER REPLICATE instead |
| LATENCY | ✗ | use the Prometheus /metrics endpoint instead |
| MEMORY USAGE | ✗ | not implemented |
| MEMORY STATS | ✗ | not implemented |

### connection

| command | status | notes |
|---------|--------|-------|
| PING | ✓ | optional message echo |
| ECHO | ✓ | |
| AUTH | ✓ | password and username/password forms |
| QUIT | ✓ | |
| MONITOR | ✓ | real-time command stream |
| CLIENT ID | ✓ | |
| CLIENT SETNAME | ✓ | |
| CLIENT GETNAME | ✓ | |
| CLIENT LIST | ✓ | |
| CLIENT KILL | ✗ | not implemented |
| CLIENT PAUSE | ✗ | not implemented |
| CLIENT UNPAUSE | ✗ | not implemented |
| RESET | ✗ | not implemented |

### transactions

| command | status | notes |
|---------|--------|-------|
| MULTI | ✓ | starts command queuing |
| EXEC | ✓ | executes queued commands atomically per shard |
| DISCARD | ✓ | discards queued commands |
| WATCH | ✓ | optimistic locking |
| UNWATCH | ✓ | |

single-shard transactions are truly atomic — the shard is single-threaded. cross-shard transactions execute in order but are not globally atomic, which is the same behavior as Redis Cluster. blocking commands (BLPOP, BRPOP) inside a MULTI block return an error.

### pub/sub

| command | status | notes |
|---------|--------|-------|
| SUBSCRIBE | ✓ | |
| UNSUBSCRIBE | ✓ | |
| PSUBSCRIBE | ✓ | glob pattern matching |
| PUNSUBSCRIBE | ✓ | |
| PUBLISH | ✓ | |
| PUBSUB CHANNELS | ✓ | optional pattern filter |
| PUBSUB NUMSUB | ✓ | |
| PUBSUB NUMPAT | ✓ | |
| PUBSUB SHARDCHANNELS | ✗ | not implemented |
| PUBSUB SHARDNUMSUB | ✗ | not implemented |
| SSUBSCRIBE | ✗ | not implemented |
| SUNSUBSCRIBE | ✗ | not implemented |
| SPUBLISH | ✗ | not implemented |

### cluster

| command | status | notes |
|---------|--------|-------|
| CLUSTER INFO | ✓ | |
| CLUSTER NODES | ✓ | |
| CLUSTER SLOTS | ✓ | |
| CLUSTER KEYSLOT | ✓ | |
| CLUSTER MYID | ✓ | |
| CLUSTER MEET | ✓ | |
| CLUSTER ADDSLOTS | ✓ | |
| CLUSTER ADDSLOTSRANGE | ✓ | |
| CLUSTER DELSLOTS | ✓ | |
| CLUSTER SETSLOT | ✓ | IMPORTING, MIGRATING, NODE, STABLE |
| CLUSTER FORGET | ✓ | |
| CLUSTER REPLICATE | ✓ | |
| CLUSTER FAILOVER | ✓ | FORCE and TAKEOVER modes |
| CLUSTER COUNTKEYSINSLOT | ✓ | |
| CLUSTER GETKEYSINSLOT | ✓ | |
| MIGRATE | ✓ | used internally during resharding |
| RESTORE | ~ | used internally by MIGRATE |
| ASKING | ✓ | access to importing slots during migration |
| CLUSTER RESET | ✗ | not implemented |
| CLUSTER SAVECONFIG | ✗ | not implemented |
| CLUSTER FLUSHSLOTS | ✗ | not implemented |
| CLUSTER LINKS | ✗ | not implemented |
| CLUSTER SLAVES | ✗ | deprecated alias — use CLUSTER REPLICAS |

### ACL

| command | status | notes |
|---------|--------|-------|
| ACL WHOAMI | ✓ | |
| ACL LIST | ✓ | |
| ACL USERS | ✓ | |
| ACL GETUSER | ✓ | |
| ACL DELUSER | ✓ | |
| ACL SETUSER | ✓ | |
| ACL CAT | ✓ | |
| ACL LOG | ✗ | not implemented |
| ACL SAVE | ✗ | not implemented |
| ACL LOAD | ✗ | not implemented |

### not planned

A few Redis command families are explicitly out of scope:

- **lua scripting** — EVAL, EVALSHA, EVALRO, SCRIPT LOAD/EXISTS/FLUSH, FCALL, and the FUNCTION family. Lua scripting is an anti-goal. WASM-based extensions may come in a future release.
- **streams** — XADD, XREAD, XRANGE, and the full Streams family. Ember focuses on caching workloads; use a dedicated stream store for this.
- **bit operations** — BITCOUNT, SETBIT, GETBIT, BITOP, BITPOS, BITFIELD. Not implemented yet, may be added later.
- **geo** — GEOADD, GEOPOS, GEODIST, GEORADIUS, GEOSEARCH, GEOHASH, and variants. Not implemented.
- **hyperloglog** — PFADD, PFCOUNT, PFMERGE. Not implemented.

---

## configuration mapping

Ember uses TOML instead of Redis's custom config format. The resolution order is: built-in defaults → config file → environment variables (EMBER_ prefix) → CLI flags. So you can always override any config value with an environment variable without touching the file.

```bash
# equivalent of --config in redis-server
ember-server --config /etc/ember/ember.toml

# or via env var
EMBER_PORT=6380 ember-server
```

### server and connections

| redis.conf | ember.toml | notes |
|------------|------------|-------|
| `bind 127.0.0.1` | `bind = "127.0.0.1"` | default is 127.0.0.1 |
| `port 6379` | `port = 6379` | same default |
| `requirepass secret` | `requirepass = "secret"` | |
| `maxclients 10000` | `maxclients = 10000` | same default |
| `timeout 300` | `idle-timeout-secs = 300` | |
| `tcp-backlog 511` | — | managed by tokio, not configurable |
| `databases 16` | — | ember has a single database |
| `hz 10` | — | not applicable |

### memory

| redis.conf | ember.toml | notes |
|------------|------------|-------|
| `maxmemory 256mb` | `maxmemory = "256mb"` | accepts kb, mb, gb suffixes |
| `maxmemory-policy noeviction` | `maxmemory-policy = "noeviction"` | supports `noeviction` and `allkeys-lru` |
| `active-defrag-enabled yes` | — | not implemented |

eviction starts when memory usage reaches 90% of `maxmemory`. the LRU sampler checks 16 keys per eviction pass, which matches Redis's default `maxmemory-samples`.

### persistence

| redis.conf | ember.toml | notes |
|------------|------------|-------|
| `appendonly yes` | `appendonly = true` | |
| `appendfsync everysec` | `appendfsync = "everysec"` | `always`, `everysec`, `no` |
| `appendfilename` | — | determined by `data-dir` |
| `dir /var/lib/redis` | `data-dir = "/var/lib/ember"` | |
| `save 900 1` | — | no RDB auto-save; use manual BGSAVE |
| `rdbcompression yes` | — | not applicable |
| `dbfilename dump.rdb` | — | snapshot name is auto-managed |

ember writes one AOF file per shard (not a single global file) when `appendonly = true` and `data-dir` is set. on restart it replays all shard AOF files in parallel.

there is no equivalent to Redis's RDB auto-save scheduling. use BGSAVE to take a manual snapshot, or rely on AOF for durability.

### monitoring

| redis.conf | ember.toml | notes |
|------------|------------|-------|
| `slowlog-log-slower-than 10000` | `slowlog-log-slower-than = 10000` | microseconds |
| `slowlog-max-len 128` | `slowlog-max-len = 128` | |
| `latency-tracking yes` | — | use Prometheus metrics instead |
| `loglevel notice` | — | use RUST_LOG env var (e.g. `RUST_LOG=info`) |

ember has a built-in Prometheus endpoint. set `metrics-port = 9090` (or any port) and scrape `/metrics`. this covers ops/sec, latency histograms, memory, connection counts, and more — no external exporter needed.

### TLS

| redis.conf | ember.toml | notes |
|------------|------------|-------|
| `tls-port 6380` | `tls-port = 6380` | |
| `tls-cert-file /path/to/cert.pem` | `tls-cert-file = "/path/to/cert.pem"` | |
| `tls-key-file /path/to/key.pem` | `tls-key-file = "/path/to/key.pem"` | |
| `tls-ca-cert-file /path/to/ca.pem` | `tls-ca-cert-file = "/path/to/ca.pem"` | |
| `tls-auth-clients yes` | `tls-auth-clients = "yes"` | |

### cluster

| redis.conf | ember.toml | notes |
|------------|------------|-------|
| `cluster-enabled yes` | `[cluster]` + `enabled = true` | |
| `cluster-config-file nodes.conf` | — | state managed internally |
| `cluster-node-timeout 15000` | `node-timeout-ms = 5000` | different default |
| `cluster-announce-ip` | — | ember uses the bind address |

a minimal ember cluster config:

```toml
bind = "0.0.0.0"
port = 6379
data-dir = "/var/lib/ember"

[cluster]
enabled = true
bootstrap = true   # only on the first node
auth-pass = "shared-cluster-secret"
```

---

## behavioral differences

these are the things that will surprise you if you're used to Redis. none of them are bugs — they're intentional design tradeoffs.

### threading model

Redis is single-threaded per process (with some background threads for persistence). Ember runs a dedicated thread per CPU core by default. each shard owns a partition of the keyspace and processes commands without holding any cross-shard locks.

the practical effect: throughput scales linearly with core count for workloads where keys are distributed across shards. single-key workloads (one key with many operations) won't see a difference.

### per-shard AOF

Redis writes a single `appendonly.aof` file. Ember writes one AOF file per shard into the `data-dir`. this means recovery is parallelized across cores, which dramatically speeds up startup with large datasets.

the tradeoff: you can't just copy a single file. a backup needs the full `data-dir`.

### RENAME cross-shard restriction

RENAME only works when the source and destination keys hash to the same shard. if they don't, Ember returns an error. this is the same constraint Redis Cluster imposes. use hashtags (`{user:1}`) to ensure related keys land on the same shard if you need to rename them.

### cluster consensus

Redis Cluster uses a gossip protocol and epoch-based voting for leader election, which can result in split-brain under certain network partition scenarios. Ember uses Raft (via openraft) for cluster configuration changes, which provides stronger consistency guarantees at the cost of requiring a quorum for slot assignments and node membership changes.

the failure detector is SWIM-based gossip, same conceptual model as Redis, but the cluster configuration itself goes through Raft.

### memory eviction

Ember starts evicting at 90% of `maxmemory`, not 100%. this gives a safety margin to avoid OOM kills under burst traffic. the LRU sampler uses 16 keys per pass (Redis default is also 16, but it's configurable there).

collections with more than 64 elements are freed in the background via `UNLINK`-style lazy deletion, even when you use `DEL`. this keeps the command latency predictable for large values.

### protected mode

ember runs in protected mode by default. if `bind` is not set to a non-loopback address and `requirepass` is empty, ember will refuse connections from outside localhost. to disable: set `requirepass` to a non-empty value or explicitly bind to your network interface.

### no multi-database support

Redis supports 16 logical databases (SELECT 0 through SELECT 15). Ember has a single database. there's no SELECT, SWAPDB, or MOVE. if you rely on multiple databases to isolate namespaces, you'll need to use key prefixes or run separate Ember instances.

### observability

Redis exposes metrics through INFO and requires an external exporter (like redis_exporter) to get Prometheus metrics. Ember ships a Prometheus endpoint natively. set `metrics-port` in config and point your scraper at it — no additional tooling needed.

---

## migration steps

### 1. audit your command usage

before anything else, check which commands your application uses. the compatibility tables above will tell you if any need substitutions.

common substitutions:

- `GETSET key value` → `SET key value GET`
- `SETNX key value` → `SET key value NX`
- `SETEX key seconds value` → `SET key value EX seconds`
- `HMSET key field value ...` → `HSET key field value ...`
- `FLUSHALL` → `FLUSHDB`
- `SHUTDOWN` → send SIGTERM to the ember-server process

if you use SELECT for multi-database isolation, plan to migrate to key prefixes or separate instances.

### 2. translate your config

write an `ember.toml` that maps your `redis.conf` settings using the table in the [configuration mapping](#configuration-mapping) section. a minimal starting point:

```toml
bind = "127.0.0.1"
port = 6379
requirepass = "your-password"
maxmemory = "256mb"
maxmemory-policy = "allkeys-lru"

appendonly = true
appendfsync = "everysec"
data-dir = "/var/lib/ember"
```

### 3. migrate your data

the simplest option is to let the application warm up a fresh Ember instance. for most caching workloads this is fine — the cache is not the source of truth.

if you need to copy existing data:

```bash
# on the source (Redis), dump a snapshot
redis-cli BGSAVE

# use redis-cli --pipe or a migration script to replay keys into Ember
redis-cli --no-auth-warning -a yourpassword \
  --scan --pattern '*' | \
  xargs -I{} redis-cli GET {} | \
  # pipe into your application's re-population logic
```

there's no built-in RDB import tool yet. the most reliable approach is to replay traffic from your application rather than migrating raw data, especially since key expiries would need to be recalculated anyway.

### 4. start ember

```bash
# single node
ember-server --config /etc/ember/ember.toml

# or minimal, no config file
ember-server --port 6379 --requirepass yourpassword
```

ember starts on port 6379 by default, same as Redis. if you're running side-by-side for testing, start Ember on a different port:

```bash
ember-server --port 6380 --config ember.toml
```

### 5. verify with the CLI

```bash
ember-cli --port 6380
```

the ember CLI supports syntax highlighting, autocomplete, and inline help. `help SET` shows usage for any command. run `help` to browse all commands grouped by category.

a quick smoke test:

```
> PING
PONG
> SET migration-test "hello"
OK
> GET migration-test
"hello"
> INFO server
# server
ember_version:...
```

### 6. update your client connection string

since Ember speaks RESP3, change your connection string host/port and you're done for most clients. if you're using Redis Cluster mode in your client, enable cluster mode in Ember and use the same cluster client configuration.

### 7. set up monitoring

enable the Prometheus endpoint to get visibility from day one:

```toml
metrics-port = 9090
```

then scrape `http://your-host:9090/metrics` from Prometheus. key metrics to watch:

- `ember_commands_total` — ops/sec by command
- `ember_commands_duration_seconds` — latency histograms
- `ember_memory_used_bytes` — memory consumption per shard
- `ember_connections_active` — active connections
- `ember_keyspace_hits_total` / `ember_keyspace_misses_total` — cache hit rate

### 8. run under traffic

if you can, shadow traffic to Ember before fully cutting over. run both Redis and Ember in parallel, send reads to Redis, and write to both. compare responses. once you're confident, shift reads to Ember and decommission Redis.

if a hard cutover is required, plan for a brief maintenance window: stop writes, let the cache drain or warm up, then switch the connection string.
