# ember-py

python gRPC client for the [ember](https://github.com/kacy/ember) cache server.

## installation

```bash
pip install ember-py
```

or install from source:

```bash
cd clients/ember-py
pip install .
```

for development (includes grpc tools and pytest):

```bash
pip install -e ".[dev]"
```

## quickstart

```python
from ember import EmberClient

client = EmberClient("localhost:6380")
client.set("greeting", b"hello world")

value = client.get("greeting")
print(value)  # b"hello world"

client.close()
```

or use the context manager for automatic cleanup:

```python
with EmberClient("localhost:6380") as client:
    client.set("counter", b"0")
    client.incr("counter")
    print(client.get("counter"))  # b"1"
```

## authentication

pass a password to the client constructor. it's sent as gRPC metadata on every request.

```python
client = EmberClient("localhost:6380", password="secret")
```

## API reference

### strings

| method | description |
|--------|-------------|
| `get(key) -> bytes \| None` | get value by key |
| `set(key, value, ex=None, px=None, nx=False, xx=False) -> bool` | set key-value pair |
| `delete(*keys) -> int` | delete keys, returns count removed |
| `exists(*keys) -> int` | count how many keys exist |
| `incr(key) -> int` | increment by 1 |
| `incr_by(key, delta) -> int` | increment by delta |
| `expire(key, seconds) -> bool` | set TTL in seconds |
| `ttl(key) -> int` | get remaining TTL (-1 = no expiry, -2 = not found) |

### lists

| method | description |
|--------|-------------|
| `lpush(key, *values) -> int` | prepend to list, returns new length |
| `rpush(key, *values) -> int` | append to list, returns new length |
| `lpop(key) -> bytes \| None` | pop from head |
| `rpop(key) -> bytes \| None` | pop from tail |
| `lrange(key, start, stop) -> list[bytes]` | get range of elements |
| `llen(key) -> int` | get list length |

### hashes

| method | description |
|--------|-------------|
| `hset(key, fields) -> int` | set fields (dict), returns new field count |
| `hget(key, field) -> bytes \| None` | get a single field |
| `hgetall(key) -> dict[str, bytes]` | get all fields |
| `hdel(key, *fields) -> int` | delete fields, returns count removed |

### sets

| method | description |
|--------|-------------|
| `sadd(key, *members) -> int` | add members, returns new member count |
| `smembers(key) -> set[str]` | get all members |
| `scard(key) -> int` | get set cardinality |

### sorted sets

| method | description |
|--------|-------------|
| `zadd(key, members) -> int` | add scored members (dict of member→score) |
| `zrange(key, start, stop, with_scores=False) -> list[tuple]` | get range by rank |

### vectors

| method | description |
|--------|-------------|
| `vadd(key, element, vector, metric="cosine", m=16, ef=64) -> bool` | add vector to HNSW index |
| `vsim(key, query, count=10, ef_search=None) -> list[tuple]` | kNN similarity search, returns (element, distance) pairs |

vectors are sent as packed IEEE 754 floats over gRPC — no string parsing overhead compared to RESP.

### server

| method | description |
|--------|-------------|
| `ping() -> str` | health check, returns "PONG" |
| `flushdb(async_mode=False)` | remove all keys |
| `dbsize() -> int` | total key count |

## connection details

the gRPC server runs on port **6380** by default (separate from the RESP3 port 6379). start the ember server with gRPC enabled:

```bash
ember-server --grpc-port 6380
```

the client uses an insecure channel by default. TLS support can be configured at the server level.

## development

regenerate protobuf stubs after changing `proto/ember/v1/ember.proto`:

```bash
make proto-gen
```

run tests:

```bash
make test
```

install in editable mode with dev dependencies:

```bash
make install-dev
```
