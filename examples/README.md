# examples

four self-contained demos showing how to use ember in real applications. each targets a concrete use case where ember's capabilities provide a meaningful edge.

| example | language | features |
|---------|----------|---------|
| [semantic-search](./semantic-search/) | python (gRPC) | vector similarity, HSET metadata, string caching |
| [live-leaderboard](./live-leaderboard/) | go (gRPC) | sorted sets, pub/sub push, expiring match keys |
| [event-router](./event-router/) | python (RESP3) | pattern subscriptions, multi-tenant isolation |
| [protobuf-profiles](./protobuf-profiles/) | python (gRPC) | protobuf serialization, field-level hash index, TTL |

## prerequisites

all examples expect a running ember server. build and start one:

```sh
cargo build --release
./target/release/ember-server
```

the gRPC examples (semantic-search, live-leaderboard, protobuf-profiles) connect to port `6380`. the event-router example connects to the RESP3 port `6379`.

for vector support, build with the vector feature:

```sh
cargo build --release --features grpc,vector
```
