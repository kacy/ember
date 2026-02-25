# ember-client

async Rust client for [ember](https://github.com/yourorg/ember), a high-performance distributed cache.

## features

- typed API for all common commands (strings, lists, hashes, sets, sorted sets)
- pipelining — batch commands into a single round-trip
- optional TLS via the `tls` feature (enabled by default)
- binary-safe values: inputs accept `&str`, `String`, `Vec<u8>`, or `bytes::Bytes`
- outputs return `Bytes` — zero-copy where possible

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
            .get("b"),
    ).await?;

    println!("{} responses", frames.len());
    Ok(())
}
```

## commands

| group       | commands |
|-------------|----------|
| strings     | `get`, `set`, `set_ex`, `del`, `exists`, `expire`, `persist`, `ttl`, `pttl`, `incr`, `decr`, `incrby`, `decrby`, `append`, `mget`, `mset`, `getdel` |
| lists       | `lpush`, `rpush`, `lpop`, `rpop`, `lrange`, `llen` |
| hashes      | `hset`, `hget`, `hgetall`, `hdel`, `hexists`, `hlen`, `hkeys`, `hvals` |
| sets        | `sadd`, `srem`, `smembers`, `sismember`, `scard` |
| sorted sets | `zadd`, `zrange`, `zrange_withscores`, `zscore`, `zrank`, `zrem`, `zcard` |
| server      | `ping`, `dbsize`, `flushdb` |
| raw         | `send` — pass any command as `&[&str]` |

## errors

`ClientError` covers five cases:

| variant       | meaning |
|---------------|---------|
| `Io`          | TCP-level failure |
| `Protocol`    | unexpected RESP3 frame shape |
| `Server`      | server returned an error reply (`WRONGTYPE`, `NOAUTH`, etc.) |
| `Disconnected`| server closed the connection |
| `Timeout`     | connect or read timed out (5 s / 10 s defaults) |

## tls

```toml
ember-client = { version = "0.1", features = ["tls"] }
```

```rust
use ember_client::{Client, tls::TlsClientConfig};

let tls = TlsClientConfig::default(); // uses native root certs
let mut client = Client::connect_tls("my-ember-host", 6380, &tls).await?;
```
