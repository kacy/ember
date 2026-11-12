# ember-persistence

durability layer for [ember](https://github.com/kacy/ember). handles append-only file logging, point-in-time snapshots, and crash recovery.

each shard gets its own persistence files (`shard-{id}.aof` and `shard-{id}.snap`), keeping the shared-nothing architecture all the way down to disk.

## what's in here

- **aof**: the append-only file writer and reader. the writer queues records in memory and writes each batch with one call. a write that fails part way is cut back to the last whole record. fsync runs after every write, once a second, or when the OS decides
- **snapshot**: a point-in-time copy of one shard's keyspace. it is written to a `.tmp` file, fsynced, then renamed into place, so a crash while writing leaves the previous snapshot as it was
- **recovery**: the startup sequence. it loads the snapshot without the keys that have expired, then replays the AOF records written after it. a file that can't be read stops the start with an error and is left untouched. a partial record at the end of the AOF, left by a crash, is cut off
- **frame**: the file header and the record framing that both files share
- **encryption**: optional AES-256-GCM (`--features encryption`) with a 32-byte key file, as raw bytes or hex. each file is encrypted with its own key, derived from the master key and a salt in the file's header. plaintext files are still read, and are rewritten encrypted at startup
- **format**: binary helpers for length-prefixed bytes, integers, floats and checksums

## file formats

every file starts with `[magic: 4B][version: 1B][flags: 1B]`, followed by a 16-byte salt when the encrypted flag is set. the current layout is version 5, and files in other versions are refused. records and snapshot entries are stored as frames:

```text
[len: 4B][!len: 4B][body][crc32 of body: 4B]
```

the second copy of the length, inverted, lets a reader tell a last frame cut short by a crash, which it drops, from a damaged length, which is an error. with encryption, the body is `[nonce: 12B][ciphertext with its 16-byte tag]`.

**AOF** (`EAOF`): the header, then one frame per record.

supported record types: SET, DEL, EXPIRE, PERSIST, PEXPIRE, INCR, DECR, INCRBY, DECRBY, APPEND, RENAME, COPY, LPUSH, RPUSH, LPOP, RPOP, ZADD, ZREM, HSET, HDEL, HINCRBY, SADD, SREM (plus VADD, VREM, PROTO.SET, PROTO.REGISTER when compiled with those features)

**snapshot** (`ESNP`): `[header][shard_id: 2B][one frame per entry][empty frame][entry count: 4B][crc32: 4B]`. each entry holds the key, a type-tagged value (string=0, list=1, sorted set=2, hash=3, set=4, protobuf=5, vector=6) and the unix time in milliseconds when it expires, or 0 for never. the trailing CRC covers the entry frames and identifies the snapshot: the AOF's checkpoint record names the snapshot it follows by this CRC.

## usage

```rust
use ember_persistence::recovery::{recover_shard, Recover};

// the shard implements `Recover` with its keyspace, so recovered entries
// and replayed records go through the same code as live writes
let result = recover_shard(data_dir, shard_id, &mut keyspace)?;
```

## related crates

| crate | what it does |
|-------|-------------|
| [emberkv-core](../ember-core) | storage engine, keyspace, sharding |
| [ember-protocol](../ember-protocol) | RESP3 parsing and command dispatch |
| [ember-server](../ember-server) | TCP server and connection handling |
| [ember-cluster](../ember-cluster) | distributed coordination |
| [ember-cli](../ember-cli) | interactive CLI client (REPL, cluster subcommands, benchmark) |
