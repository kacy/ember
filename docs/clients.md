# client integration guide

Ember speaks RESP3 on the same default port as Redis (`6379`). If you have a working Redis setup, connecting to Ember is usually a one-line change.

---

## node.js — ioredis

```javascript
import Redis from 'ioredis';

const client = new Redis({
  host: 'localhost',
  port: 6379,
});

await client.set('hello', 'world');
const val = await client.get('hello');
console.log(val); // "world"
```

ioredis auto-negotiates RESP3 on newer versions and falls back to RESP2 transparently. No configuration needed for this.

---

## node.js — node-redis

```javascript
import { createClient } from 'redis';

const client = createClient({
  socket: { host: 'localhost', port: 6379 },
});

await client.connect();
await client.set('hello', 'world');
const val = await client.get('hello');
console.log(val); // "world"
```

node-redis v4+ uses RESP3 by default. Ember is fully compatible.

---

## python — redis-py

```python
import redis

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

r.set('hello', 'world')
val = r.get('hello')
print(val)  # "world"
```

redis-py supports RESP3 via the `RESP3Protocol` class on newer versions. The default RESP2 mode works fine with Ember either way.

For async usage:

```python
import redis.asyncio as redis

async def main():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    await r.set('hello', 'world')
    val = await r.get('hello')
    print(val)
```

---

## go — go-redis/v9

```go
package main

import (
    "context"
    "fmt"
    "github.com/redis/go-redis/v9"
)

func main() {
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    ctx := context.Background()
    err := rdb.Set(ctx, "hello", "world", 0).Err()
    if err != nil {
        panic(err)
    }

    val, err := rdb.Get(ctx, "hello").Result()
    if err != nil {
        panic(err)
    }
    fmt.Println(val) // "world"
}
```

go-redis/v9 sends a HELLO command on connect to negotiate RESP3. Ember responds correctly.

---

## ruby — redis-rb

```ruby
require 'redis'

redis = Redis.new(host: 'localhost', port: 6379)

redis.set('hello', 'world')
val = redis.get('hello')
puts val  # "world"
```

redis-rb 5.x defaults to RESP3. Ember is fully compatible with both RESP2 and RESP3 modes.

---

## java — jedis

```java
import redis.clients.jedis.Jedis;

try (Jedis jedis = new Jedis("localhost", 6379)) {
    jedis.set("hello", "world");
    String val = jedis.get("hello");
    System.out.println(val); // "world"
}
```

For production use with connection pooling:

```java
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost", 6379);

try (Jedis jedis = pool.getResource()) {
    jedis.set("hello", "world");
    String val = jedis.get("hello");
    System.out.println(val);
}
```

---

## c# — StackExchange.Redis

```csharp
using StackExchange.Redis;

var connection = await ConnectionMultiplexer.ConnectAsync("localhost:6379");
var db = connection.GetDatabase();

await db.StringSetAsync("hello", "world");
var val = await db.StringGetAsync("hello");
Console.WriteLine(val); // "world"
```

StackExchange.Redis uses connection multiplexing by default. One `ConnectionMultiplexer` instance is typically shared across the application. The library probes server capabilities on connect; Ember responds to those probes correctly.

---

## connecting with tls

Ember supports TLS on a separate port (default offset `6379 + tls_port`). Start the server with:

```
ember-server --tls-port 6380 --tls-cert-file server.crt --tls-key-file server.key
```

**ioredis + tls**

```javascript
import Redis from 'ioredis';
import { readFileSync } from 'fs';

const client = new Redis({
  host: 'localhost',
  port: 6380,
  tls: {
    ca: readFileSync('ca.crt'),    // omit if using a trusted CA
    rejectUnauthorized: true,
  },
});
```

**redis-py + tls**

```python
import redis
import ssl

r = redis.Redis(
    host='localhost',
    port=6380,
    ssl=True,
    ssl_ca_certs='ca.crt',   # omit if using a trusted CA
    decode_responses=True,
)

r.set('hello', 'world')
```

For mutual TLS (mTLS), start the server with `--tls-ca-cert-file ca.crt --tls-auth-clients yes` and pass client cert/key files in the client config.

---

## redis-cli

`redis-cli` connects to Ember without any changes:

```
redis-cli -h localhost -p 6379
```

With authentication:

```
redis-cli -h localhost -p 6379 -a yourpassword
```

With TLS:

```
redis-cli -h localhost -p 6380 --tls --cacert ca.crt
```

The `ember-cli` tool ships with Ember and adds syntax highlighting, autocomplete, and inline help. It also understands cluster topology natively. Use it when you want a richer terminal experience.

---

## authentication

When the server is started with `--requirepass`, clients must send `AUTH` before any data commands:

```javascript
// ioredis
const client = new Redis({ host: 'localhost', port: 6379, password: 'secret' });

// node-redis
const client = createClient({ socket: { host: 'localhost', port: 6379 }, password: 'secret' });

// redis-py
r = redis.Redis(host='localhost', port=6379, password='secret', decode_responses=True)

// go-redis
rdb := redis.NewClient(&redis.Options{Addr: 'localhost:6379', Password: 'secret'})
```

All clients handle AUTH automatically when a password is configured.
