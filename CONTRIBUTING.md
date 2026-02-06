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

4. **commit** with clear messages:
   - use lowercase, present tense: `add pubsub support`, `fix memory tracking bug`
   - keep commits atomic and focused

5. **open a pull request** against `main`
   - include a summary of changes
   - describe what was tested
   - note any design considerations

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
├── ember-cluster/    # distributed clustering (wip)
└── ember-cli/        # command-line client
```

## questions?

open an issue or start a discussion — we're happy to help.
