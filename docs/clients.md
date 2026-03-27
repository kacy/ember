# client integration guide

for most applications, using ember from a client looks exactly like using redis: point your client at ember's host and port, then keep the rest of your code the same.

ember listens on `6379` by default, speaks the redis wire protocol, and works with normal redis clients. if a client prefers RESP3, that's fine. if it stays on RESP2, that's usually fine too.

---

## quick rules

- use your existing redis client first before looking for an ember-specific one
- reuse connections or a shared client object instead of reconnecting per request
- if you enable `--requirepass`, set the password in the client config and let the library handle `AUTH`
- if you enable TLS, connect to the TLS port, not the plain TCP port
- in cluster mode, use a client that understands `MOVED` and `ASK` redirects

## node.js

### ioredis

```javascript
import Redis from 'ioredis';

const client = new Redis({
  host: '127.0.0.1',
  port: 6379,
});

await client.set('hello', 'world');
console.log(await client.get('hello')); // "world"
```

`ioredis` is a good default if you want solid cluster support and familiar redis semantics.

### node-redis

```javascript
import { createClient } from 'redis';

const client = createClient({
  socket: {
    host: '127.0.0.1',
    port: 6379,
  },
});

await client.connect();
await client.set('hello', 'world');
console.log(await client.get('hello')); // "world"
```

`node-redis` works well for straightforward app integration. if you already use it with redis, ember usually does not need any special handling.

## python

### redis-py

```python
import redis

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

r.set("hello", "world")
print(r.get("hello"))  # "world"
```

async usage:

```python
import redis.asyncio as redis

async def main():
    r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
    await r.set("hello", "world")
    print(await r.get("hello"))
```

if you already use `redis-py`, start with the same settings you use for redis. only add ember-specific changes when you actually need them.

## go

### go-redis

```go
package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	if err := rdb.Set(ctx, "hello", "world", 0).Err(); err != nil {
		panic(err)
	}

	val, err := rdb.Get(ctx, "hello").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println(val) // world
}
```

for services, keep one shared `*redis.Client` per process unless you have a reason to do something more complex.

## ruby

### redis-rb

```ruby
require "redis"

redis = Redis.new(host: "127.0.0.1", port: 6379)

redis.set("hello", "world")
puts redis.get("hello") # "world"
```

## java

### jedis

```java
import redis.clients.jedis.Jedis;

try (Jedis jedis = new Jedis("127.0.0.1", 6379)) {
    jedis.set("hello", "world");
    System.out.println(jedis.get("hello"));
}
```

with pooling:

```java
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

JedisPool pool = new JedisPool(new JedisPoolConfig(), "127.0.0.1", 6379);

try (Jedis jedis = pool.getResource()) {
    jedis.set("hello", "world");
    System.out.println(jedis.get("hello"));
}
```

## c#

### StackExchange.Redis

```csharp
using StackExchange.Redis;

var mux = await ConnectionMultiplexer.ConnectAsync("127.0.0.1:6379");
var db = mux.GetDatabase();

await db.StringSetAsync("hello", "world");
Console.WriteLine(await db.StringGetAsync("hello")); // world
```

one `ConnectionMultiplexer` per application is the normal pattern here. do not create one per request.

## tls

ember serves TLS on a separate port that you choose with `--tls-port`.

example server:

```bash
ember-server \
  --tls-port 6380 \
  --tls-cert-file server.crt \
  --tls-key-file server.key
```

### ioredis over tls

```javascript
import Redis from 'ioredis';
import { readFileSync } from 'fs';

const client = new Redis({
  host: '127.0.0.1',
  port: 6380,
  tls: {
    ca: readFileSync('ca.crt'),
    rejectUnauthorized: true,
  },
});
```

### redis-py over tls

```python
import redis

r = redis.Redis(
    host="127.0.0.1",
    port=6380,
    ssl=True,
    ssl_ca_certs="ca.crt",
    decode_responses=True,
)
```

for mTLS, start ember with `--tls-ca-cert-file` and `--tls-auth-clients yes`, then pass the client certificate and key in your library's TLS settings.

## authentication

if you start the server with `--requirepass`, set the password in the client config and let the library send `AUTH` for you.

examples:

```javascript
// ioredis
const client = new Redis({
  host: '127.0.0.1',
  port: 6379,
  password: 'secret',
});

// node-redis
const client = createClient({
  socket: { host: '127.0.0.1', port: 6379 },
  password: 'secret',
});
```

```python
r = redis.Redis(
    host="127.0.0.1",
    port=6379,
    password="secret",
    decode_responses=True,
)
```

```go
rdb := redis.NewClient(&redis.Options{
	Addr:     "127.0.0.1:6379",
	Password: "secret",
})
```

if you are using ACL users instead of a single password, use the client library's username + password fields.

## cluster mode

in cluster mode, the important requirement is not "an ember client." it is "a redis client that understands cluster redirects."

good rule of thumb:

- if the client already works with redis cluster, it is the right place to start
- if the client does not follow `MOVED` and `ASK`, it will be painful in cluster mode

`ember-cli` and `redis-cli --cluster` are both useful for local testing and operational work.

## redis-cli and ember-cli

plain `redis-cli` works against ember:

```bash
redis-cli -h 127.0.0.1 -p 6379
redis-cli -h 127.0.0.1 -p 6379 -a secret
redis-cli -h 127.0.0.1 -p 6380 --tls --cacert ca.crt
```

`ember-cli` is the nicer option when you want autocomplete, inline help, and cluster-aware subcommands:

```bash
ember-cli
ember-cli -a secret
ember-cli cluster info
ember-cli cluster nodes
```

## troubleshooting

if a client connects but commands fail, check these first:

1. wrong port: plain TCP and TLS use different ports
2. auth mismatch: `NOAUTH` usually means the password was not configured in the client
3. cluster-unaware client: look for `MOVED` errors
4. certificate trust issues: common when testing TLS with self-signed certs

if you need more detail, see [troubleshooting](troubleshooting.md) and [migration from redis](migration-from-redis.md).
