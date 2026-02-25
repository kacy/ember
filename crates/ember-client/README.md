# ember-client

async Rust client for [ember](https://github.com/yourorg/ember), a high-performance distributed cache.

## features

- typed API for all common commands (strings, keys, lists, hashes, sets, sorted sets, server, pub/sub, slowlog)
- pipelining — batch commands into a single round-trip
- optional TLS via the `tls` cargo feature (enabled by default)
- optional vector commands via the `vector` cargo feature
- binary-safe values: inputs accept `&str`, `String`, `Vec<u8>`, or `bytes::Bytes`
- outputs return `Bytes` — zero-copy where possible
- `Subscriber` type for pub/sub with typed message frames

## quick start

```toml
[dependencies]
ember-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

```rust
use ember_client::Client;

#[tokio::main]
async fn main() -> Result<(), ember_client::ClientError> {
    let mut client = Client::connect("127.0.0.1", 6379).await?;

    client.set("greeting", "hello").await?;
    if let Some(value) = client.get("greeting").await? {
        println!("{}", String::from_utf8_lossy(&value));
    }

    Ok(())
}
```

## pipelining

Send multiple commands in a single network round-trip:

```rust
use ember_client::{Client, Pipeline};

#[tokio::main]
async fn main() -> Result<(), ember_client::ClientError> {
    let mut client = Client::connect("127.0.0.1", 6379).await?;

    let frames = client.execute_pipeline(
        Pipeline::new()
            .set("a", "1")
            .set("b", "2")
            .get("a")
            .get("b")
            .incr("counter")
            .zadd("scores", &[(9.5, "alice"), (8.0, "bob")])
            .hset("user:1", &[("name", "alice"), ("age", "30")]),
    ).await?;

    println!("{} responses", frames.len());
    Ok(())
}
```

## commands

### strings

| command | signature | returns |
|---------|-----------|---------|
| `get` | `get(key)` | `Option<Bytes>` |
| `set` | `set(key, value)` | `()` |
| `set_ex` | `set_ex(key, value, seconds)` | `()` |
| `getdel` | `getdel(key)` | `Option<Bytes>` |
| `append` | `append(key, value)` | `i64` (new length) |
| `strlen` | `strlen(key)` | `i64` |
| `incr` | `incr(key)` | `i64` |
| `decr` | `decr(key)` | `i64` |
| `incrby` | `incrby(key, delta)` | `i64` |
| `decrby` | `decrby(key, delta)` | `i64` |
| `incr_by_float` | `incr_by_float(key, delta)` | `f64` |
| `mget` | `mget(&[key, ...])` | `Vec<Option<Bytes>>` |
| `mset` | `mset(&[(key, value), ...])` | `()` |

### keys

| command | signature | returns |
|---------|-----------|---------|
| `del` | `del(&[key, ...])` | `i64` (count deleted) |
| `unlink` | `unlink(&[key, ...])` | `i64` |
| `exists` | `exists(&[key, ...])` | `i64` (count found) |
| `expire` | `expire(key, seconds)` | `bool` |
| `pexpire` | `pexpire(key, millis)` | `bool` |
| `persist` | `persist(key)` | `bool` |
| `ttl` | `ttl(key)` | `i64` (-2 missing, -1 no expiry) |
| `pttl` | `pttl(key)` | `i64` |
| `key_type` | `key_type(key)` | `String` |
| `keys` | `keys(pattern)` | `Vec<Bytes>` |
| `rename` | `rename(key, newkey)` | `()` |
| `scan` | `scan(cursor, match, count)` | `ScanPage` |

### lists

| command | signature | returns |
|---------|-----------|---------|
| `lpush` | `lpush(key, &[value, ...])` | `i64` (new length) |
| `rpush` | `rpush(key, &[value, ...])` | `i64` |
| `lpop` | `lpop(key)` | `Option<Bytes>` |
| `rpop` | `rpop(key)` | `Option<Bytes>` |
| `lrange` | `lrange(key, start, stop)` | `Vec<Bytes>` |
| `llen` | `llen(key)` | `i64` |

### hashes

| command | signature | returns |
|---------|-----------|---------|
| `hset` | `hset(key, &[(field, value), ...])` | `i64` (new fields added) |
| `hget` | `hget(key, field)` | `Option<Bytes>` |
| `hgetall` | `hgetall(key)` | `Vec<(Bytes, Bytes)>` |
| `hmget` | `hmget(key, &[field, ...])` | `Vec<Option<Bytes>>` |
| `hdel` | `hdel(key, &[field, ...])` | `i64` |
| `hexists` | `hexists(key, field)` | `bool` |
| `hlen` | `hlen(key)` | `i64` |
| `hincrby` | `hincrby(key, field, delta)` | `i64` |
| `hkeys` | `hkeys(key)` | `Vec<Bytes>` |
| `hvals` | `hvals(key)` | `Vec<Bytes>` |

### sets

| command | signature | returns |
|---------|-----------|---------|
| `sadd` | `sadd(key, &[member, ...])` | `i64` (new members) |
| `srem` | `srem(key, &[member, ...])` | `i64` |
| `smembers` | `smembers(key)` | `Vec<Bytes>` |
| `sismember` | `sismember(key, member)` | `bool` |
| `scard` | `scard(key)` | `i64` |

### sorted sets

| command | signature | returns |
|---------|-----------|---------|
| `zadd` | `zadd(key, &[(score, member), ...])` | `i64` (new members) |
| `zrange` | `zrange(key, start, stop)` | `Vec<Bytes>` |
| `zrange_withscores` | `zrange_withscores(key, start, stop)` | `Vec<(Bytes, f64)>` |
| `zscore` | `zscore(key, member)` | `Option<f64>` |
| `zrank` | `zrank(key, member)` | `Option<i64>` |
| `zrem` | `zrem(key, &[member, ...])` | `i64` |
| `zcard` | `zcard(key)` | `i64` |

### server

| command | signature | returns |
|---------|-----------|---------|
| `ping` | `ping()` | `()` |
| `echo` | `echo(message)` | `Bytes` |
| `dbsize` | `dbsize()` | `i64` |
| `flushdb` | `flushdb()` | `()` |
| `info` | `info(section)` | `String` |
| `bgsave` | `bgsave()` | `String` |
| `bgrewriteaof` | `bgrewriteaof()` | `String` |

### slowlog

| command | signature | returns |
|---------|-----------|---------|
| `slowlog_get` | `slowlog_get(count)` | `Vec<SlowlogEntry>` |
| `slowlog_len` | `slowlog_len()` | `i64` |
| `slowlog_reset` | `slowlog_reset()` | `()` |

### pub/sub

| command | signature | returns |
|---------|-----------|---------|
| `publish` | `publish(channel, message)` | `i64` (receivers) |
| `pubsub_channels` | `pubsub_channels(pattern)` | `Vec<Bytes>` |
| `pubsub_numsub` | `pubsub_numsub(&[channel, ...])` | `Vec<(Bytes, i64)>` |
| `pubsub_numpat` | `pubsub_numpat()` | `i64` |
| `subscribe` | `subscribe(&[channel, ...])` — consumes `self` | `Subscriber` |
| `psubscribe` | `psubscribe(&[pattern, ...])` — consumes `self` | `Subscriber` |

### raw

```rust
client.send(&["COMMAND", "arg1", "arg2"]).await?;
```

## pub/sub

`subscribe` and `psubscribe` consume the `Client`, returning a dedicated `Subscriber` handle. This prevents accidentally mixing pub/sub and regular commands on the same connection.

```rust
use ember_client::{Client, Message};

let client = Client::connect("127.0.0.1", 6379).await?;
let mut sub = client.subscribe(&["events", "alerts"]).await?;

loop {
    match sub.recv().await? {
        Some(Message { channel, payload, .. }) => {
            println!("[{}] {}", channel, String::from_utf8_lossy(&payload));
        }
        None => break,
    }
}
```

Pattern subscriptions work the same way via `psubscribe`. The `Message` type also carries the `pattern` field when using pattern subscriptions.

## vector commands

Requires the `vector` cargo feature:

```toml
ember-client = { version = "0.1", features = ["vector"] }
```

| command | signature | returns |
|---------|-----------|---------|
| `vadd` | `vadd(key, element, vector)` | `bool` |
| `vadd_batch` | `vadd_batch(key, &[(element, vector), ...])` | `i64` |
| `vsim` | `vsim(key, query_vector, count)` | `Vec<SimResult>` |
| `vrem` | `vrem(key, element)` | `bool` |
| `vget` | `vget(key, element)` | `Option<Vec<f32>>` |
| `vcard` | `vcard(key)` | `i64` |
| `vdim` | `vdim(key)` | `i64` |
| `vinfo` | `vinfo(key)` | `Vec<(Bytes, Bytes)>` |

```rust
use ember_client::Client;

let mut client = Client::connect("127.0.0.1", 6379).await?;

// index vectors
client.vadd("embeddings", "doc-1", &[0.1, 0.9, 0.2]).await?;
client.vadd("embeddings", "doc-2", &[0.8, 0.1, 0.5]).await?;

// similarity search
let results = client.vsim("embeddings", &[0.1, 0.8, 0.3], 5).await?;
for r in results {
    println!("{}: {:.4}", r.element, r.score);
}
```

## public types

### `ScanPage`

```rust
pub struct ScanPage {
    pub cursor: u64,  // 0 means iteration is complete
    pub keys: Vec<Bytes>,
}
```

### `SlowlogEntry`

```rust
pub struct SlowlogEntry {
    pub id: i64,
    pub timestamp: i64,
    pub duration_us: i64,
    pub args: Vec<Bytes>,
}
```

### `Message`

```rust
pub struct Message {
    pub kind: String,    // "message" or "pmessage"
    pub pattern: Option<Bytes>,
    pub channel: Bytes,
    pub payload: Bytes,
}
```

### `SimResult`

```rust
pub struct SimResult {
    pub element: String,
    pub score: f32,
}
```

## pipeline builder

All pipeline methods mirror the async API. The builder is chainable and zero-alloc until `execute_pipeline` is called.

| method | notes |
|--------|-------|
| `get(key)` | |
| `set(key, value)` | |
| `del(&[key, ...])` | |
| `exists(&[key, ...])` | |
| `expire(key, seconds)` | |
| `pexpire(key, millis)` | |
| `ttl(key)` | |
| `incr(key)` | |
| `decr(key)` | |
| `incrby(key, delta)` | |
| `strlen(key)` | |
| `incr_by_float(key, delta)` | |
| `unlink(&[key, ...])` | |
| `rename(key, newkey)` | |
| `key_type(key)` | |
| `keys(pattern)` | |
| `echo(message)` | |
| `lpush(key, &[value, ...])` | |
| `rpush(key, &[value, ...])` | |
| `lpop(key)` | |
| `rpop(key)` | |
| `llen(key)` | |
| `hget(key, field)` | |
| `hset(key, &[(field, value), ...])` | |
| `hdel(key, &[field, ...])` | |
| `hincrby(key, field, delta)` | |
| `hmget(key, &[field, ...])` | |
| `sadd(key, &[member, ...])` | |
| `srem(key, &[member, ...])` | |
| `scard(key)` | |
| `zadd(key, &[(score, member), ...])` | |
| `zcard(key)` | |
| `zscore(key, member)` | |
| `publish(channel, message)` | |
| `ping()` | |
| `send(&[arg, ...])` | raw escape hatch |

## errors

`ClientError` covers six cases:

| variant | meaning |
|---------|---------|
| `Io` | TCP-level failure |
| `Protocol` | unexpected RESP3 frame shape |
| `Server` | server returned an error reply (`WRONGTYPE`, `NOAUTH`, etc.) |
| `Disconnected` | server closed the connection |
| `Timeout` | connect or read timed out (5 s / 10 s defaults) |

## tls

```toml
ember-client = { version = "0.1", features = ["tls"] }
```

```rust
use ember_client::{Client, tls::TlsClientConfig};

let tls = TlsClientConfig::default(); // uses native root certs
let mut client = Client::connect_tls("my-ember-host", 6380, &tls).await?;
```
