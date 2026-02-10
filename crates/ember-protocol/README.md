# ember-protocol

RESP3 wire protocol implementation for [ember](https://github.com/kacy/ember). handles zero-copy parsing, direct-to-buffer serialization, and typed command dispatch.

## what's in here

- **parse** — zero-copy RESP3 frame parser that works directly on byte slices, returning `(Frame, bytes_consumed)` for pipelining support
- **serialize** — writes frames directly into `BytesMut` with no intermediate allocations
- **command** — converts raw frames into typed `Command` enums with argument validation, arity checks, and flag parsing
- **types** — `Frame` enum: `Simple`, `Error`, `Integer`, `Bulk`, `Null`, `Array`, `Map`

## quick start

```rust
use bytes::{Bytes, BytesMut};
use ember_protocol::{Frame, parse_frame, Command};

// parse a RESP3 frame from raw bytes
let input = b"+OK\r\n";
let (frame, consumed) = parse_frame(input).unwrap().unwrap();
assert_eq!(frame, Frame::Simple("OK".into()));

// serialize a frame back to bytes
let mut buf = BytesMut::new();
frame.serialize(&mut buf);
assert_eq!(&buf[..], b"+OK\r\n");

// parse a command from an array frame
let frame = Frame::Array(vec![
    Frame::Bulk(Bytes::from("SET")),
    Frame::Bulk(Bytes::from("key")),
    Frame::Bulk(Bytes::from("value")),
]);
let cmd = Command::from_frame(frame).unwrap();
```

## supported commands

**strings**: `GET`, `SET` (with NX/XX/EX/PX), `INCR`, `DECR`, `MGET`, `MSET`

**lists**: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`

**sorted sets**: `ZADD` (with NX/XX/GT/LT/CH flags), `ZREM`, `ZSCORE`, `ZRANK`, `ZRANGE` (with WITHSCORES), `ZCARD`

**hashes**: `HSET`, `HGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HINCRBY`, `HKEYS`, `HVALS`, `HMGET`

**sets**: `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`

**keys**: `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `TYPE`, `SCAN`

**server**: `PING`, `ECHO`, `DBSIZE`, `INFO`, `BGSAVE`, `BGREWRITEAOF`, `FLUSHDB`, `SLOWLOG GET`, `SLOWLOG LEN`, `SLOWLOG RESET`

**cluster**: `CLUSTER INFO`, `NODES`, `SLOTS`, `KEYSLOT`, `MYID`, `MEET`, `ADDSLOTS`, `DELSLOTS`, `SETSLOT`, `FORGET`, `REPLICATE`, `FAILOVER`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT`, `MIGRATE`, `ASKING`

## related crates

| crate | what it does |
|-------|-------------|
| [emberkv-core](../ember-core) | storage engine, keyspace, sharding |
| [ember-persistence](../ember-persistence) | AOF, snapshots, and crash recovery |
| [ember-server](../ember-server) | TCP server and connection handling |
| [ember-cluster](../ember-cluster) | distributed coordination |
| [ember-cli](../ember-cli) | interactive CLI client (REPL, cluster subcommands, benchmark) |
