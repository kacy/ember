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
| GETSET | ✗ | use `SET key value GET` pattern instead |
| GETDEL | ✗ | not implemented |
| GETEX | ✗ | not implemented |
| SETNX | ✗ | use `SET key value NX` instead |
| MSETNX | ✗ | not implemented |
| SETRANGE | ✗ | not implemented |
| GETRANGE | ✗ | not implemented |
| SUBSTR | ✗ | not implemented |

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
| RENAMENX | ✗ | not implemented |
| TYPE | ✓ | |
| KEYS | ✓ | glob patterns supported |
| SCAN | ✓ | cursor encoding is shard-aware but opaque to clients — functionally identical |
| COPY | ✗ | not implemented |
| MOVE | ✗ | single database only |
| SELECT | ✗ | single database only |
| SWAPDB | ✗ | single database only |
| DUMP | ✗ | not implemented |
| RESTORE | ~ | supported for cluster MIGRATE only |
| OBJECT | ✗ | not implemented |
| WAIT | ✗ | not implemented |
| TOUCH | ✗ | not implemented |
| EXPIREAT | ✗ | not implemented |
| PEXPIREAT | ✗ | not implemented |
| EXPIRETIME | ✗ | not implemented |
| PEXPIRETIME | ✗ | not implemented |
| RANDOMKEY | ✗ | not implemented |
| SORT | ✗ | not implemented |

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
| LINSERT | ✗ | not implemented |
| LSET | ✗ | not implemented |
| LINDEX | ✗ | not implemented |
| LTRIM | ✗ | not implemented |
| LPOS | ✗ | not implemented |
| LMOVE | ✗ | not implemented |
| LMPOP | ✗ | not implemented |
| BLPOP | ✗ | blocking operations not implemented |
| BRPOP | ✗ | blocking operations not implemented |
| BLMOVE | ✗ | blocking operations not implemented |
| LPUSHX | ✗ | not implemented |
| RPUSHX | ✗ | not implemented |
| LREM | ✗ | not implemented |

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
| HMSET | ✗ | use HSET with multiple fields instead |
| HINCRBYFLOAT | ✗ | not implemented |
| HSCAN | ✗ | not implemented |
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
| SMISMEMBER | ✗ | not implemented |
| SMOVE | ✗ | not implemented |
| SUNION | ✗ | not implemented |
| SINTER | ✗ | not implemented |
| SDIFF | ✗ | not implemented |
| SUNIONSTORE | ✗ | not implemented |
| SINTERSTORE | ✗ | not implemented |
| SDIFFSTORE | ✗ | not implemented |
| SINTERCARD | ✗ | not implemented |
| SRANDMEMBER | ✗ | not implemented |
| SPOP | ✗ | not implemented |
| SSCAN | ✗ | not implemented |

---

## sorted sets

| command | status | notes |
|---------|--------|-------|
| ZADD | ✓ | NX, XX, GT, LT, CH flags all supported |
| ZREM | ✓ | multi-member |
| ZSCORE | ✓ | |
| ZRANK | ✓ | |
| ZCARD | ✓ | |
| ZRANGE | ✓ | rank-based with optional WITHSCORES |
| ZINCRBY | ✗ | not implemented |
| ZREVRANK | ✗ | not implemented |
| ZREVRANGE | ✗ | not implemented |
| ZRANGEBYSCORE | ✗ | not implemented |
| ZREVRANGEBYSCORE | ✗ | not implemented |
| ZRANGEBYLEX | ✗ | not implemented |
| ZRANGESTORE | ✗ | not implemented |
| ZCOUNT | ✗ | not implemented |
| ZLEXCOUNT | ✗ | not implemented |
| ZPOPMIN | ✗ | not implemented |
| ZPOPMAX | ✗ | not implemented |
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
| ZSCAN | ✗ | not implemented |

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
| FLUSHALL | ✗ | use FLUSHDB instead |
| BGSAVE | ✓ | triggers background snapshot |
| BGREWRITEAOF | ✓ | rewrites AOF from current snapshot |
| SAVE | ✗ | not implemented |
| LASTSAVE | ✗ | not implemented |
| SLOWLOG GET | ✓ | optional count argument |
| SLOWLOG LEN | ✓ | |
| SLOWLOG RESET | ✓ | |
| DEBUG | ✗ | not implemented |
| CONFIG GET | ✗ | not implemented |
| CONFIG SET | ✗ | not implemented |
| CONFIG REWRITE | ✗ | not implemented |
| CONFIG RESETSTAT | ✗ | not implemented |
| COMMAND | ✗ | not implemented |
| COMMAND COUNT | ✗ | not implemented |
| COMMAND INFO | ✗ | not implemented |
| COMMAND DOCS | ✗ | not implemented |
| CLIENT LIST | ✗ | not implemented |
| CLIENT SETNAME | ✗ | not implemented |
| CLIENT GETNAME | ✗ | not implemented |
| CLIENT ID | ✗ | not implemented |
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
| SHUTDOWN | ✗ | use SIGTERM instead |
| REPLICAOF | ✗ | use CLUSTER REPLICATE instead |
| SLAVEOF | ✗ | use CLUSTER REPLICATE instead |
| PSYNC | ✗ | internal protocol, not for client use |

---

## cluster commands

All cluster commands are implemented. See the cluster documentation for operational details.

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
| CLUSTER RESET | ✗ | not implemented |
| CLUSTER SAVECONFIG | ✗ | not implemented |
| CLUSTER FLUSHSLOTS | ✗ | not implemented |
| CLUSTER LINKS | ✗ | not implemented |
| MIGRATE | ✓ | used internally during resharding |
| RESTORE | ~ | used internally by MIGRATE |
| ASKING | ✓ | allows access to importing slots during migration |

---

## commands that are out of scope

Some Redis commands are explicitly not planned for Ember:

**scripting**
- `EVAL`, `EVALSHA`, `EVALRO`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH` — Lua scripting is an anti-goal. We may support WASM-based extensions in the future instead.
- `FCALL`, `FUNCTION LOAD`, `FUNCTION LIST`, `FUNCTION DELETE` — same reasoning as EVAL.

**streams**
- `XADD`, `XREAD`, `XRANGE`, `XREVRANGE`, `XLEN`, `XACK`, `XGROUP`, `XDEL`, `XTRIM`, `XINFO` — streams are a complex, purpose-built data type. Ember focuses on caching workloads; use a dedicated stream store for this.

**bit operations**
- `BITCOUNT`, `SETBIT`, `GETBIT`, `BITOP`, `BITPOS`, `BITFIELD`, `BITFIELD_RO` — not implemented yet; may be added in a future release.

**geo**
- `GEOADD`, `GEOPOS`, `GEODIST`, `GEORADIUS`, `GEORADIUSBYMEMBER`, `GEOSEARCH`, `GEOSEARCHSTORE`, `GEOHASH` — not implemented yet.

**multi/exec**
- `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` — transactions not yet implemented.

**other**
- `LOLWUT` — not implemented.
