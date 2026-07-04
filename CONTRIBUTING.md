# contributing to ember

thanks for your interest in contributing to ember. this document covers the
development workflow and standards we follow.

## getting started

```bash
# clone the repo
git clone https://github.com/kacy/ember
cd ember

# build and test
make check
```

## development workflow

1. **create a branch** from `main` for your work
   - use descriptive names: `feat/add-pubsub`, `fix/memory-leak`, `docs/api-examples`

2. **make changes** following the code standards below

3. **run checks** before committing:
   ```bash
   make check  # runs fmt, clippy, and tests
   ```

   `make check` uses `--features protobuf,grpc`. CI additionally builds with
   the `vector` feature enabled, so if you touch vector code (`V*` commands),
   also run:
   ```bash
   cargo clippy --workspace --features protobuf,grpc,vector -- -D warnings
   ```

4. **regenerate clients if you changed the proto.** The canonical gRPC schema
   is `proto/ember/v1/ember.proto`. If you change it, regenerate all three
   client stubs or the `client proto drift` CI job will fail your PR:
   ```bash
   make -C clients/ember-go proto-gen
   make -C clients/ember-py proto-gen
   make -C clients/ember-ts proto-gen
   ```
   (`scripts/check-client-drift.sh` verifies every rpc in the proto is present
   in each client.)

5. **commit** with clear messages:
   - use lowercase, present tense: `add pubsub support`, `fix memory tracking bug`
   - keep commits atomic and focused

6. **open a pull request** against `main`
   - include a summary of changes
   - describe what was tested
   - note any design considerations

## continuous integration

Every PR runs the following jobs (see `.github/workflows/ci.yml`); they must
pass before merge:

- **check** — `cargo fmt --check`, `clippy -D warnings`, and `cargo check`,
  all with `--features protobuf,grpc,vector`
- **test** — unit and integration tests on Ubuntu and macOS
- **build** — release build + binary artifact
- **msrv (1.93)** — builds against the pinned minimum supported Rust version
- **go / python / ts client** — build and test each client library
- **client proto drift** — asserts the clients are regenerated from the proto
- **helm lint**, **docker build**, and a **security** advisory audit

Run `make check` locally to cover the core Rust checks before pushing.

## code standards

### style

- run `cargo fmt` before committing
- run `cargo clippy` with warnings as errors
- no `unwrap()` in library code — use proper error handling
- no `unsafe` without a comment explaining why

### documentation

- every public item needs a doc comment
- include examples for complex apis
- document panic conditions and performance characteristics

### testing

- add tests for new functionality
- focus on edge cases and error paths
- run the full test suite before submitting

## project structure

```
crates/
├── ember-server/     # main server binary
├── ember-core/       # sharded engine and data structures
├── ember-protocol/   # resp3 parsing and commands
├── ember-persistence/# aof and snapshots
├── ember-cluster/    # distributed clustering (raft, gossip, slots)
└── ember-cli/        # command-line client
```

## questions?

open an issue or start a discussion — we're happy to help.
