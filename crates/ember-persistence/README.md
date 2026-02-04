# ember-persistence

durability layer for [ember](https://github.com/kacy/ember). handles append-only file logging, point-in-time snapshots, and crash recovery.

each shard gets its own persistence files (`shard-{id}.aof` and `shard-{id}.snap`), keeping the shared-nothing architecture all the way down to disk.

## what's in here

- **aof** — append-only file writer/reader with CRC32 integrity checks. binary TLV format, configurable fsync (always, every-second, OS-managed). gracefully handles truncated records from mid-write crashes
- **snapshot** — point-in-time serialization of an entire shard's keyspace. writes to a `.tmp` file first, then atomic rename to prevent partial snapshots from corrupting existing data
- **recovery** — startup sequence: load snapshot, replay AOF tail, skip expired entries. handles corrupt files gracefully (logs warning, starts empty)
- **format** — low-level binary serialization helpers: length-prefixed bytes, integers, floats, checksums, header validation

## file formats

**AOF** — `[EAOF magic][version][record...]` where each record is `[tag][payload][crc32]`

**snapshot (v2)** — `[ESNP magic][version][shard_id][entry_count][entries...][footer_crc32]` where entries are type-tagged (string=0, list=1, sorted set=2). v1 snapshots (no type tags) are still readable.

## usage

```rust
use ember_persistence::aof::{AofWriter, AofRecord};
use ember_persistence::snapshot::{SnapshotWriter, SnapshotReader, SnapEntry, SnapValue};
use ember_persistence::recovery::recover_shard;

// recovery is typically called by the shard on startup
let result = recover_shard(data_dir, shard_id);
for entry in result.entries {
    // insert into keyspace...
}
```

## related crates

| crate | what it does |
|-------|-------------|
| [emberkv-core](../ember-core) | storage engine, keyspace, sharding |
| [ember-protocol](../ember-protocol) | RESP3 parsing and command dispatch |
| [ember-server](../ember-server) | TCP server and connection handling |
| [ember-cluster](../ember-cluster) | distributed coordination (WIP) |
| [ember-cli](../ember-cli) | interactive command-line client (WIP) |
