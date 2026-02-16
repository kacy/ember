# security audit — february 2026

## executive summary

comprehensive security audit across all 6 crates in the ember workspace. the codebase is well-defended overall: zero `unsafe` code, clean `cargo audit`, constant-time auth, per-connection limits, CRC checksums, AES-256-GCM encryption at rest. the findings below are hardening opportunities organized by impact and effort.

**audit scope**: ember-protocol, emberkv-core, ember-server, ember-persistence, ember-cluster, emberkv-cli

**methodology**: manual code review of all `.rs` source files, `cargo audit`, `cargo clippy`, `cargo outdated`, web research for common Rust vulnerability patterns.

**totals**: 6 critical, 16 high, 30 medium, 28 low

---

## tier 1 — high impact, low effort

quick wins that significantly improve security posture.

| # | finding | crate | severity | est. LOC |
|---|---------|-------|----------|----------|
| 1 | gRPC has no authentication — bypasses `requirepass` | ember-server | CRITICAL | ~50 |
| 2 | `expect()` in `track_size` can crash shard | emberkv-core | HIGH | ~10 |
| 3 | `effective_limit` integer overflow on large `max_bytes` | emberkv-core | HIGH | ~3 |
| 4 | NaN/infinity allowed in VADD/VSIM vector components | ember-protocol | MEDIUM | ~10 |
| 5 | VADD_BATCH element name collision with flags | ember-protocol | MEDIUM | ~15 |
| 6 | VADD_BATCH off-by-one in batch size check | ember-protocol | LOW | ~2 |
| 7 | SCAN COUNT accepted as unbounded usize | ember-protocol | MEDIUM | ~4 |
| 8 | gRPC subscribe has no subscription/pattern limits | ember-server | HIGH | ~10 |
| 9 | AUTH commands saved to plaintext history file | emberkv-cli | HIGH | ~10 |
| 10 | history file created with world-readable permissions | emberkv-cli | MEDIUM | ~5 |

**1. gRPC no auth**: when `--requirepass` is set, RESP3 connections must AUTH first. the gRPC service has zero authentication — any client on the gRPC port can GET, SET, DEL, FLUSHDB without a password. add a tonic interceptor that validates a token from gRPC metadata against `ctx.requirepass`.

**2. `track_size` expect**: two `.expect()` calls in the keyspace hot path. if a caller passes a key that was just deleted (possible via future refactoring), the shard task panics. replace with `let-else` + early return.

**3. `effective_limit` overflow**: `max_bytes * 90` overflows `usize` when `max_bytes > usize::MAX / 90`. use `max_bytes / 100 * 90` to avoid intermediate overflow.

**4. NaN/infinity in vectors**: `parse_vadd`, `parse_vadd_batch`, `parse_vsim` accept `NaN`/`inf` as f32 values. NaN corrupts HNSW graph traversal (all comparisons return false). add `v.is_nan() || v.is_infinite()` check.

**5. VADD_BATCH name collision**: if an element name matches a flag (METRIC, QUANT, M, EF — case-insensitive), parsing stops early. elements named "metric" silently vanish. use dim-based counting to know when entries end.

**6. off-by-one**: `entries.len() > MAX_VADD_BATCH_SIZE` allows 10,001 entries. should be `>=`.

**7. SCAN COUNT unbounded**: `SCAN 0 COUNT 18446744073709551615` passes through as `usize`. cap at protocol layer.

**8. gRPC subscribe limits**: RESP3 enforces `MAX_SUBSCRIPTIONS_PER_CONN = 10,000` and `MAX_PATTERN_LEN = 256`. gRPC subscribe has neither check.

**9. AUTH in history**: typing `AUTH password` in the REPL saves it to `~/.emberkv_history`. filter AUTH commands before adding to history.

**10. history permissions**: `rustyline` creates history with default umask (often 0644). set 0600 after save.

---

## tier 2 — high impact, medium effort

important fixes that require more careful implementation.

| # | finding | crate | severity | est. LOC |
|---|---------|-------|----------|----------|
| 11 | ConcurrentKeyspace TOCTOU race bypasses memory limit | emberkv-core | CRITICAL | ~25 |
| 12 | non-atomic AOF truncation — crash loses all AOF data | ember-persistence | HIGH | ~40 |
| 13 | partial AOF replay uses inconsistent state | ember-persistence | HIGH | ~30 |
| 14 | serialize recursion can stack overflow | ember-protocol | HIGH | ~20 |
| 15 | Vec::with_capacity pre-allocates up to 72MB per frame | ember-protocol | MEDIUM | ~5 |
| 16 | RESP3 pipeline depth unbounded within 64MB buffer | ember-server | MEDIUM | ~8 |
| 17 | gRPC server has no connection limit | ember-server | MEDIUM | ~5 |
| 18 | encryption key not zeroized on drop | ember-persistence | MEDIUM | ~15 |
| 19 | vadd_batch partial failure loses applied vectors from AOF | emberkv-core | MEDIUM | ~20 |
| 20 | concurrent keyspace incr_by double-decrement race | emberkv-core | HIGH | ~15 |

**11. TOCTOU memory bypass**: multiple threads can pass the memory limit check concurrently using `Relaxed` ordering, each seeing stale `current`. under heavy concurrent writes, actual memory can exceed `max_memory` by `N * entry_size`. use `fetch_add` to atomically reserve memory.

**12. non-atomic AOF truncation**: `truncate()` opens with `.truncate(true)` and rewrites in-place. crash between truncation and sync loses everything. use write-to-temp-then-rename (like snapshots).

**13. partial AOF replay**: if CRC fails on record 50000 of 100000, the function returns `Err` but partial mutations already applied to the map are silently kept. snapshot says `balance=1000`, first AOF record sets `balance=500`, second record (setting `credit=500`) is corrupt — result: 500 lost.

**14. serialize stack overflow**: `Frame::serialize()` is recursive with no depth limit. parser allows 64 levels. deeply nested response frames crash the server.

**15. pre-alloc amplification**: `*1048576\r\n` header (12 bytes) causes `Vec::with_capacity(1_048_576)` = ~72MB allocation. cap pre-allocation to `count.min(1024)`.

**16. pipeline depth**: a client can pipeline millions of small commands (e.g., PING) within the 64MB buffer. add max pipeline depth (e.g., 10,000).

**17. gRPC no connection limit**: RESP3 enforces 10,000 connections via semaphore. gRPC has no limit. configure tonic `concurrency_limit_per_connection`.

**18. encryption key zeroize**: `EncryptionKey` derives `Clone` and doesn't zero on drop. key material persists in freed memory. add `zeroize::ZeroizeOnDrop`.

**19. vadd_batch AOF partial**: on partial failure, `applied` vectors are lost because `Err` loses the list. applied vectors not persisted to AOF. on restart they disappear.

**20. concurrent incr_by race**: after `drop(entry)` releases the DashMap lock, another thread can concurrently remove the same expired key, double-decrementing `memory_used`. use `remove_if` instead.

---

## tier 3 — cluster security (design-level)

cluster layer findings are mostly design-level concerns. the cluster integration with the server is not yet wired, but these should be addressed before production cluster deployments.

| # | finding | crate | severity | est. LOC |
|---|---------|-------|----------|----------|
| 21 | raft vote/log not persisted — split-brain after restart | ember-cluster | CRITICAL | ~100+ |
| 22 | gossip messages have no authentication | ember-cluster | CRITICAL | ~80+ |
| 23 | forged Dead with u64::MAX incarnation kills nodes permanently | ember-cluster | CRITICAL | ~15 |
| 24 | no auth/authz on raft commands | ember-cluster | HIGH | ~40 |
| 25 | AssignSlots accepts unvalidated slot ranges | ember-cluster | HIGH | ~15 |
| 26 | CompleteMigration doesn't verify migration in progress | ember-cluster | HIGH | ~10 |
| 27 | no limit on gossip member list growth | ember-cluster | HIGH | ~10 |
| 28 | migration source/target identity not validated | ember-cluster | HIGH | ~15 |
| 29 | migration batches have no integrity verification | ember-cluster | HIGH | ~40+ |
| 30 | AssignSlots via deserialization bypasses SlotRange validation | ember-cluster | HIGH | ~10 |

**21. raft persistence**: vote and log are in-memory only. after restart, a node forgets its vote and can vote for a different leader in the same term. this violates raft safety and enables split-brain.

**22-23. gossip auth**: any node on the network can inject gossip messages. a `Dead { incarnation: u64::MAX }` message permanently kills a legitimate node because `saturating_add(1)` at max produces `u64::MAX` — the refutation can never exceed the forged incarnation.

**24-26, 30. raft command validation**: no authorization on who can propose commands. `AssignSlots` doesn't validate ranges. `CompleteMigration` doesn't verify an active migration exists. deserialized `SlotRange` bypasses `try_new`.

**27. member list unbounded**: every gossip message from a new NodeId creates a member entry. flood with unique IDs for OOM.

**28-29. migration safety**: no validation that source/target exist or own the slot. batches have no checksums, no auth, no sequence validation.

---

## tier 4 — medium impact, various effort

| # | finding | crate | severity | est. LOC |
|---|---------|-------|----------|----------|
| 31 | KEYS command unbounded allocation (DoS) | emberkv-core | MEDIUM | ~10 |
| 32 | SCAN cursor unstable with HashMap rehashing | emberkv-core | MEDIUM | design |
| 33 | no active expiration in concurrent keyspace | emberkv-core | MEDIUM | ~40 |
| 34 | VectorSet::clone silently loses data on failure | emberkv-core | MEDIUM | ~10 |
| 35 | snapshot footer CRC doesn't cover header | ember-persistence | MEDIUM | ~15 |
| 36 | no file locking on persistence files | ember-persistence | MEDIUM | ~15 |
| 37 | snapshot shard_id not validated during recovery | ember-persistence | MEDIUM | ~5 |
| 38 | VADD dim not validated in AOF read_payload_for_tag | ember-persistence | MEDIUM | ~3 |
| 39 | SlotRange::new uses debug_assert only | ember-cluster | MEDIUM | ~6 |
| 40 | encoding truncates collection lengths via `as u16` | ember-cluster | MEDIUM | ~6 |
| 41 | pending_keys HashSet unbounded during migration | ember-cluster | MEDIUM | ~10 |
| 42 | migration state transitions not enforced | ember-cluster | MEDIUM | ~20 |
| 43 | raft snapshot deserialization has no size limits | ember-cluster | MEDIUM | ~10 |
| 44 | ANSI escape injection from server responses in CLI | emberkv-cli | MEDIUM | ~15 |
| 45 | ConcurrentKeyspace rename not atomic — data loss window | emberkv-core | HIGH | design |
| 46 | no key/value size validation on RESP3 | ember-server | LOW | ~10 |
| 47 | variadic commands accept up to 1M arguments | ember-protocol | MEDIUM | ~15 |
| 48 | shard_id truncation `i as u16` with >65535 shards | emberkv-core | MEDIUM | ~3 |
| 49 | TTL u64→i64 cast overflow in iter_entries | emberkv-core | MEDIUM | ~3 |

---

## tier 5 — low impact / informational

| # | finding | crate | severity | est. LOC |
|---|---------|-------|----------|----------|
| 50 | password visible in process arguments (server) | ember-server | LOW | ~3 |
| 51 | password visible in process arguments (CLI) | emberkv-cli | LOW | ~3 |
| 52 | metrics endpoint has no access control | ember-server | LOW | design |
| 53 | `.expect()` in cluster coordinator | ember-server | LOW | ~5 |
| 54 | CRC32 is not tamper-resistant | ember-persistence | LOW | docs |
| 55 | VectorSet next_key wrapping after u64::MAX | emberkv-core | LOW | ~10 |
| 56 | `cursor.position() as usize` truncation on 32-bit | ember-protocol | LOW | ~5 |
| 57 | benchmark read buffer unbounded (CLI) | emberkv-cli | LOW | ~5 |
| 58 | password not zeroed from memory (CLI) | emberkv-cli | LOW | ~10 |
| 59 | NodeId::Display unchecked string slicing | ember-cluster | LOW | ~3 |
| 60 | pending_probes indirect entries never cleaned | ember-cluster | LOW | ~10 |

---

## positive findings

the codebase demonstrates strong security practices:

- **zero `unsafe` blocks** across the entire workspace
- **`cargo audit` clean** — no known CVEs in dependencies
- **`cargo clippy` clean** — no warnings
- **constant-time auth** using `subtle::ConstantTimeEq`
- **per-connection buffer limits** (64MB max, 5min idle timeout)
- **CRC32 checksums** on all persistence records
- **AES-256-GCM encryption** for at-rest data
- **RESP3 parser limits** (64 nesting depth, 1M array elements, 512MB bulk strings)
- **subscription limits** per connection (10,000 max, 256-byte patterns)
- **protected mode** for unauthenticated remote connections
- **TLS support** with mTLS option
- **AUTH failure lockout** (10 attempts per connection)
- **graceful shutdown** with connection draining
- **idle timeout** (300s) prevents slowloris

---

## recommended PR sequence

| PR | crate | findings addressed | est. LOC |
|----|-------|-------------------|----------|
| 1 | ember-protocol | #4, #5, #6, #7, #14, #15, #47 | ~70 |
| 2 | emberkv-core | #2, #3, #11, #19, #20, #48, #49 | ~80 |
| 3 | ember-server | #1, #8, #16, #17 | ~75 |
| 4 | ember-persistence | #12, #13, #18, #37, #38 | ~95 |
| 5 | emberkv-cli | #9, #10, #44 | ~30 |
| 6 | ember-cluster | #23, #25, #26, #30, #39, #40 | ~60 |

total estimated change: ~410 LOC across 6 PRs

cluster-level design issues (#21, #22, #24, #27-29) are tracked separately as they require architectural decisions.

---

## dependency status

- `cargo audit`: clean (no CVEs)
- `cargo clippy`: clean (no warnings)
- outdated: futures 0.3.31→0.3.32 (minor), pin-utils removed from registry
- all security-relevant deps (aes-gcm, rustls, subtle, crc32fast) at latest
