# ember-go

go gRPC client for the [ember](https://github.com/kacy/ember) cache server.

## installation

```bash
go get github.com/kacy/ember-go
```

## quickstart

```go
package main

import (
    "context"
    "fmt"
    "log"

    ember "github.com/kacy/ember-go"
)

func main() {
    client, err := ember.Dial("localhost:6380")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    client.Set(ctx, "greeting", []byte("hello world"))
    value, _ := client.Get(ctx, "greeting")
    fmt.Println(string(value)) // hello world
}
```

## authentication

use `WithPassword` to authenticate on every request:

```go
client, err := ember.Dial("localhost:6380", ember.WithPassword("secret"))
```

the password is sent as gRPC metadata (`authorization` header) with each RPC call.

## API reference

### strings

```go
Get(ctx, key) ([]byte, error)
Set(ctx, key, value, ...SetOption) (bool, error)
Del(ctx, keys...) (int64, error)
Exists(ctx, keys...) (int64, error)
Incr(ctx, key) (int64, error)
IncrBy(ctx, key, delta) (int64, error)
Expire(ctx, key, seconds) (bool, error)
TTL(ctx, key) (int64, error)
```

set options: `WithEX(seconds)`, `WithPX(millis)`, `WithNX()`, `WithXX()`

### lists

```go
LPush(ctx, key, values...) (int64, error)
RPush(ctx, key, values...) (int64, error)
LPop(ctx, key) ([]byte, error)
RPop(ctx, key) ([]byte, error)
LRange(ctx, key, start, stop) ([][]byte, error)
LLen(ctx, key) (int64, error)
```

### hashes

```go
HSet(ctx, key, fields) (int64, error)    // fields: map[string][]byte
HGet(ctx, key, field) ([]byte, error)
HGetAll(ctx, key) (map[string][]byte, error)
HDel(ctx, key, fields...) (int64, error)
```

### sets

```go
SAdd(ctx, key, members...) (int64, error)
SMembers(ctx, key) ([]string, error)
SCard(ctx, key) (int64, error)
```

### sorted sets

```go
ZAdd(ctx, key, members...) (int64, error)  // members: ScoreMember{Member, Score}
ZRange(ctx, key, start, stop, withScores) ([]ScoreMember, error)
```

### vectors

```go
VAdd(ctx, key, element, vector, ...VAddOption) (bool, error)
VSim(ctx, key, query, count, ...VSimOption) ([]VSimResult, error)
```

vector options: `WithMetric(metric)`, `WithConnectivity(m)`, `WithEfConstruction(ef)`

query options: `WithEfSearch(ef)`

vectors are passed as `[]float32` — binary transport with no string parsing overhead.

### server

```go
Ping(ctx) (string, error)
FlushDB(ctx) error
DBSize(ctx) (int64, error)
```

## context and error handling

every method takes a `context.Context` as its first argument, following Go conventions. use contexts for timeouts and cancellation:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

value, err := client.Get(ctx, "key")
if err != nil {
    // gRPC errors can be inspected with status.FromError(err)
    log.Printf("get failed: %v", err)
}
```

errors are standard gRPC status errors. use `google.golang.org/grpc/status` to inspect error codes.

## connection details

the gRPC server runs on port **6380** by default (separate from the RESP3 port 6379). the client uses an insecure connection by default.

## development

regenerate protobuf stubs after changing `proto/ember/v1/ember.proto`:

```bash
make proto-gen
```

requires `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc`.

run tests:

```bash
make test
```
