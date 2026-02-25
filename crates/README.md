# crates

this directory contains all ember workspace crates. each crate is self-contained with its own `Cargo.toml` and README.

---

## crate index

| directory | package | description |
|-----------|---------|-------------|
| `ember-server` | `ember-server` | tcp server, connection handling, RESP3 dispatch, gRPC, TLS, ACL, metrics |
| `ember-core` | `emberkv-core` | shared-nothing storage engine, keyspace, sharding, expiry, eviction |
| `ember-protocol` | `ember-protocol` | RESP3 parser/serializer, typed `Command` enum, 170+ commands |
| `ember-persistence` | `ember-persistence` | AOF, snapshots, crash recovery, optional AES-256-GCM encryption |
| `ember-cluster` | `ember-cluster` | SWIM gossip, openraft consensus, slot migration, failover |
| `ember-client` | `ember-client` | async Rust client with pipelining, TLS, pub/sub, vector |
| `ember-cli` | `emberkv-cli` | interactive REPL, cluster subcommands, built-in benchmark |

> **note on package names**: the directory name and the cargo package name differ for two crates. use the package name with `-p` flags: `-p emberkv-core`, `-p emberkv-cli`.

---

## dependency graph

```
ember-server
  ├── emberkv-core
  │     ├── ember-protocol
  │     └── ember-persistence
  └── ember-cluster

ember-client      (no internal deps — standalone)
emberkv-cli       (no internal deps — standalone)
```

`ember-client` and `emberkv-cli` speak RESP3 directly over TCP; they share no library code with the server internals.

---

## cargo cheat-sheet

```bash
# build everything
cargo build --workspace

# build a single crate
cargo build -p emberkv-core

# test everything
cargo test --workspace

# test a single crate
cargo test -p ember-server

# run with grpc and protobuf features
cargo test --workspace --features protobuf,grpc

# lint
cargo clippy --workspace -- -D warnings

# format
cargo fmt --all
```

---

## makefile targets

run these from the workspace root (see `Makefile` for full definitions):

| target | what it does |
|--------|-------------|
| `make build` | `cargo build --release` |
| `make test` | `cargo test --workspace` |
| `make fmt` | `cargo fmt --all` |
| `make clippy` | clippy with warnings as errors |
| `make check` | fmt-check + clippy + test |
| `make bench-quick` | quick single-node throughput check |
| `make bench-compare` | full comparison vs redis |
| `make cluster` | start a local 3-node cluster |
| `make cluster-stop` | stop the local cluster |
| `make cluster-status` | show cluster node status |
| `make proto-gen` | compile proto definitions (used by the server) |
| `make proto-go` | generate Go gRPC bindings → `clients/ember-go/ember/v1/` |
| `make proto-py` | generate Python gRPC bindings → `clients/ember-py/ember/proto/ember/v1/` |
| `make proto-ts` | print instructions for TypeScript gRPC codegen |

### proto generation prerequisites

**Go** (`make proto-go`):
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```
requires `protoc` on `$PATH`.

**Python** (`make proto-py`):
```bash
pip install grpcio-tools
```

---

## crate READMEs

- [ember-server](ember-server/README.md)
- [ember-core](ember-core/README.md)
- [ember-protocol](ember-protocol/README.md)
- [ember-persistence](ember-persistence/README.md)
- [ember-cluster](ember-cluster/README.md)
- [ember-client](ember-client/README.md)
- [ember-cli](ember-cli/README.md)
