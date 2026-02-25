# redis compatibility

Ember speaks RESP3, so any Redis client works out of the box. Point your existing client at the same host and port and it connects — no code changes needed.

## how it works

Ember parses the Redis serialization protocol (RESP3) on the wire. Clients that speak RESP3 or RESP2 (the vast majority of Redis clients) are fully compatible. The server advertises itself as protocol-compatible so clients that auto-negotiate protocol version work without configuration.

Ember also exposes port `6379` by default, the same as Redis, so most default configurations work without any change at all.

---

## strings

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
| GETSET | ✗ | use `SET key value GET` pattern instead |
| GETDEL | ✗ | not implemented |
| GETEX | ✗ | not implemented |
| SETNX | ✗ | use `SET key value NX` instead |
| MSETNX | ✗ | not implemented |
| SUBSTR | ✗ | deprecated; use GETRANGE instead |

---

## key lifecycle

| command | status | notes |
|---------|--------|-------|
| DEL | ✓ | multi-key supported |
| UNLINK | ✓ | async background free |
| EXISTS | ✓ | multi-key supported |
| EXPIRE | ✓ | |
| TTL | ✓ | |
| PERSIST | ✓ | |
| PTTL | ✓ | |
| PEXPIRE | ✓ | |
| RENAME | ~ | source and destination must hash to the same shard in sharded mode |
| TYPE | ✓ | |
| KEYS | ✓ | glob patterns supported |
| SCAN | ✓ | cursor encoding is shard-aware but opaque to clients — functionally identical |
| COPY | ✓ | |
| TOUCH | ✓ | |
| RANDOMKEY | ✓ | |
| SORT | ✓ | basic sorting with ALPHA, ASC/DESC, LIMIT |
| OBJECT ENCODING | ✓ | |
| OBJECT REFCOUNT | ✓ | |
| RESTORE | ~ | supported for cluster MIGRATE only |
| RENAMENX | ✗ | not implemented |
| MOVE | ✗ | single database only |
| SELECT | ✗ | single database only |
| SWAPDB | ✗ | single database only |
| DUMP | ✗ | not implemented |
| WAIT | ✗ | not implemented |
| EXPIREAT | ✗ | not implemented |
| PEXPIREAT | ✗ | not implemented |
| EXPIRETIME | ✗ | not implemented |
| PEXPIRETIME | ✗ | not implemented |

---

## lists

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
| LINSERT | ✓ | BEFORE and AFTER |
| LREM | ✓ | |
| LPOS | ✓ | |
| BLPOP | ✓ | multi-key with timeout, blocks until element available |
| BRPOP | ✓ | multi-key with timeout, blocks until element available |
| LMOVE | ✗ | not implemented |
| LMPOP | ✗ | not implemented |
| BLMOVE | ✗ | not implemented |
| LPUSHX | ✗ | not implemented |
| RPUSHX | ✗ | not implemented |

---

## hashes

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
| HMSET | ✗ | deprecated; use HSET with multiple fields instead |
| HINCRBYFLOAT | ✗ | not implemented |
| HRANDFIELD | ✗ | not implemented |

---

## sets

| command | status | notes |
|---------|--------|-------|
| SADD | ✓ | multi-member |
| SREM | ✓ | multi-member |
| SMEMBERS | ✓ | |
| SISMEMBER | ✓ | |
| SCARD | ✓ | |
| SMISMEMBER | ✓ | |
| SUNION | ✓ | |
| SINTER | ✓ | |
| SDIFF | ✓ | |
| SUNIONSTORE | ✓ | |
| SINTERSTORE | ✓ | |
| SDIFFSTORE | ✓ | |
| SRANDMEMBER | ✓ | optional count argument |
| SPOP | ✓ | optional count argument |
| SSCAN | ✓ | |
| SMOVE | ✗ | not implemented |
| SINTERCARD | ✗ | not implemented |

---

## sorted sets

| command | status | notes |
|---------|--------|-------|
| ZADD | ✓ | NX, XX, GT, LT, CH flags all supported |
| ZREM | ✓ | multi-member |
| ZSCORE | ✓ | |
| ZRANK | ✓ | |
| ZREVRANK | ✓ | |
| ZCARD | ✓ | |
| ZRANGE | ✓ | rank-based with optional WITHSCORES |
| ZREVRANGE | ✓ | with optional WITHSCORES |
| ZINCRBY | ✓ | |
| ZRANGEBYSCORE | ✓ | with optional WITHSCORES and LIMIT |
| ZREVRANGEBYSCORE | ✓ | with optional WITHSCORES and LIMIT |
| ZCOUNT | ✓ | |
| ZPOPMIN | ✓ | optional count argument |
| ZPOPMAX | ✓ | optional count argument |
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

---

## pub/sub

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

---

## server & connection

| command | status | notes |
|---------|--------|-------|
| PING | ✓ | optional message echo |
| ECHO | ✓ | |
| AUTH | ✓ | password and username/password forms |
| QUIT | ✓ | |
| INFO | ✓ | server, keyspace, replication, clients, memory, stats sections |
| DBSIZE | ✓ | |
| FLUSHDB | ✓ | ASYNC mode supported |
| BGSAVE | ✓ | triggers background snapshot |
| BGREWRITEAOF | ✓ | rewrites AOF from current snapshot |
| TIME | ✓ | |
| LASTSAVE | ✓ | unix timestamp of last successful save |
| ROLE | ✓ | returns master/slave role and replication info |
| MONITOR | ✓ | real-time command stream |
| SLOWLOG GET | ✓ | optional count argument |
| SLOWLOG LEN | ✓ | |
| SLOWLOG RESET | ✓ | |
| CONFIG GET | ✓ | glob pattern matching |
| CONFIG SET | ✓ | runtime-mutable parameters |
| CONFIG REWRITE | ✓ | flushes runtime config back to file |
| CLIENT ID | ✓ | |
| CLIENT SETNAME | ✓ | |
| CLIENT GETNAME | ✓ | |
| CLIENT LIST | ✓ | |
| FLUSHALL | ✗ | use FLUSHDB instead |
| SAVE | ✗ | use BGSAVE instead |
| SHUTDOWN | ✗ | use SIGTERM instead |
| DEBUG | ✗ | not implemented |
| CONFIG RESETSTAT | ✗ | not implemented |
| COMMAND | ✗ | not implemented |
| COMMAND COUNT | ✗ | not implemented |
| COMMAND INFO | ✗ | not implemented |
| COMMAND DOCS | ✗ | not implemented |
| CLIENT KILL | ✗ | not implemented |
| CLIENT PAUSE | ✗ | not implemented |
| CLIENT UNPAUSE | ✗ | not implemented |
| CLIENT NO-EVICT | ✗ | not implemented |
| CLIENT NO-TOUCH | ✗ | not implemented |
| LATENCY | ✗ | not implemented |
| MEMORY USAGE | ✗ | not implemented |
| MEMORY STATS | ✗ | not implemented |
| MEMORY DOCTOR | ✗ | not implemented |
| RESET | ✗ | not implemented |
| REPLICAOF | ✗ | use CLUSTER REPLICATE instead |
| SLAVEOF | ✗ | use CLUSTER REPLICATE instead |
| PSYNC | ✗ | internal protocol, not for client use |

---

## transactions

| command | status | notes |
|---------|--------|-------|
| MULTI | ✓ | starts command queuing |
| EXEC | ✓ | executes queued commands atomically (single-shard) |
| DISCARD | ✓ | discards queued commands |
| WATCH | ✓ | accepts keys for optimistic locking |
| UNWATCH | ✓ | clears watched keys |

single-shard transactions are truly atomic (the shard is single-threaded). cross-shard transactions execute in order but are not globally atomic — same limitation as Redis Cluster. blocking commands (BLPOP, BRPOP) inside MULTI return an error.

---

## cluster commands

all cluster commands are implemented. see the cluster documentation for operational details.

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
| ASKING | ✓ | allows access to importing slots during migration |
| CLUSTER RESET | ✗ | not implemented |
| CLUSTER SAVECONFIG | ✗ | not implemented |
| CLUSTER FLUSHSLOTS | ✗ | not implemented |
| CLUSTER LINKS | ✗ | not implemented |

---

## acl

| command | status | notes |
|---------|--------|-------|
| ACL WHOAMI | ✓ | |
| ACL LIST | ✓ | |
| ACL USERS | ✓ | |
| ACL GETUSER | ✓ | |
| ACL SETUSER | ✓ | permissions, key patterns, command categories |
| ACL DELUSER | ✓ | |
| ACL CAT | ✓ | lists command categories |
| ACL LOG | ✗ | not implemented |
| ACL SAVE | ✗ | not implemented |
| ACL LOAD | ✗ | not implemented |

---

## commands out of scope

some Redis commands are explicitly not planned for Ember:

**scripting**
- `EVAL`, `EVALSHA`, `EVALRO`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH` — Lua scripting is an anti-goal. We may support WASM-based extensions in the future instead.
- `FCALL`, `FUNCTION LOAD`, `FUNCTION LIST`, `FUNCTION DELETE` — same reasoning as EVAL.

**streams**
- `XADD`, `XREAD`, `XRANGE`, `XREVRANGE`, `XLEN`, `XACK`, `XGROUP`, `XDEL`, `XTRIM`, `XINFO` — streams are a complex, purpose-built data type. Ember focuses on caching workloads; use a dedicated stream store for this.

**bit operations**
- `BITCOUNT`, `SETBIT`, `GETBIT`, `BITOP`, `BITPOS`, `BITFIELD`, `BITFIELD_RO` — not implemented yet; may be added in a future release.

**geo**
- `GEOADD`, `GEOPOS`, `GEODIST`, `GEORADIUS`, `GEORADIUSBYMEMBER`, `GEOSEARCH`, `GEOSEARCHSTORE`, `GEOHASH` — not implemented yet.

**other**
- `LOLWUT` — not implemented.
