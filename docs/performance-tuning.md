# performance tuning

most ember deployments do not need heroic tuning. the defaults are reasonable. when performance is off, the usual culprits are pipeline depth, connection behavior, key layout, or a memory limit that was set without much margin.

this guide focuses on the knobs that actually matter first.

---

## start with the default shard count

by default, ember uses one shard per available CPU core. that is usually the right place to start.

override it only when you have a specific reason:

```bash
# cli
ember-server --shards 8

# config file
shards = 8
```

practical advice:

- if the host has 8 real cores available to ember, start with 8 shards
- if you run inside a container with a CPU limit, set `shards` explicitly
- do not crank the shard count above the cores you can actually use and expect a free win

more shards than cores usually just buys you more scheduler noise. fewer shards than cores can be fine for light workloads, but it leaves throughput on the table once the server gets busy.

## pipeline depth is the biggest lever

if you care about throughput, pipeline depth matters more than almost anything else.

one command at a time is easy to reason about, but it leaves a lot of performance sitting on the floor. once you start batching commands, ember can keep shards busy instead of waiting on round trips.

illustrative numbers from the repo benchmarks:

| pipeline depth | typical SET throughput | notes |
|---|---|---|
| `P=1` | ~133k ops/sec | lowest latency |
| `P=8` | ~900k ops/sec | good general-purpose setting |
| `P=16` | ~1.76M ops/sec | common throughput sweet spot |
| `P=64+` | diminishing returns | more in-flight memory, smaller gains |

good defaults:

- use `P=1` when latency matters more than raw throughput
- use `P=8` or `P=16` for bulk writes, cache warmups, and batch-heavy services
- be skeptical of very deep pipelines unless you measured a real gain

`max-pipeline-depth` exists to keep one noisy client from buffering an absurd amount of work on a single connection. leave it alone unless you have a reason to tighten it.

## connection behavior matters more than people expect

ember can handle a lot of clients, but lots of bad client behavior still adds up.

the basics:

- reuse connections
- use a bounded client-side pool
- stop opening one connection per request
- close dead or idle clients with `idle-timeout-secs` if your environment tends to leak them

`maxclients` defaults to `10000`. that is generous, but it is not free. idle clients still hold buffers, and thousands of pointless connections are just wasted memory.

watch these when things look odd:

- `INFO clients` for current connection counts
- `/metrics` for `ember_connected_clients`
- `/metrics` for `ember_rejected_connections_total`

if connection count keeps climbing while traffic does not, assume a leak until proven otherwise.

## memory tuning starts with key shape

the easiest memory win is usually shorter keys, not a different server setting.

an 8-byte key and a 64-byte key both point to the same value, but the longer one keeps costing you memory on every entry. at scale, that difference is real.

rough rules of thumb:

- strings are the cheapest data type
- lists, sets, hashes, and sorted sets have extra container overhead
- hashes are pretty efficient when you have a handful of related fields
- very large collections are convenient, but they are not cheap

the exact number depends on key length, value length, and data type, so treat any fixed "bytes per key" number as a ballpark figure, not gospel.

what to do in practice:

- keep keys short and boring
- pack related fields into hashes when it makes sense
- avoid giant one-key collections if you care about memory or latency
- validate your assumptions with `INFO memory` and `ember_memory_used_bytes`

## design keys for the execution model

ember is sharded. if related keys always land on different shards, some operations get more expensive than they need to be.

use hash tags when you want related keys to stay together:

```text
{user:1234}:profile
{user:1234}:prefs
{user:1234}:sessions
```

that keeps those keys on the same shard and helps commands like `MGET` avoid extra fan-out.

two more habits help a lot:

- set TTLs on data that is supposed to expire
- paginate large collections instead of reading them all in one command

`SMEMBERS`, `HGETALL`, and `LRANGE 0 -1` are fine on small collections. they are a bad surprise on huge ones.

if a collection can grow without a hard ceiling, plan on using:

- `SSCAN`
- `HSCAN`
- `ZSCAN`

## benchmark like you mean it

the built-in benchmark command is the fastest way to get a feel for how your setup behaves:

```bash
# simple GET/SET run
ember-cli benchmark -t set,get -P 16 -c 50

# mix in other data types
ember-cli benchmark -t lpush,sadd,zadd,hset,hget -P 16

# larger payloads
ember-cli benchmark -d 1024 -P 16

# avoid hammering the same tiny keyset
ember-cli benchmark -t get,set --keyspace 1000000
```

`redis-benchmark` also works directly against ember:

```bash
redis-benchmark -h 127.0.0.1 -p 6379 -n 1000000 -c 50 -P 16 -t set,get
```

when reading results:

- warm the keyspace before judging read performance
- use a realistic keyspace size so one hot key does not fake a great result
- compare on the same hardware, not across random machines
- care about p99, not just average latency

if you only remember one thing from this section, make it this: benchmark the shape of traffic you actually have, not the traffic that makes the graph look nicest.

## a short checklist

if ember is slower than you expected, check these in order:

1. are clients pipelining at all?
2. are you using one shard per available core?
3. are clients reusing connections instead of reconnecting constantly?
4. are large collections or all-at-once reads blocking shards?
5. are keys longer than they need to be?
6. are you benchmarking a real keyspace or one hot key?

that usually gets you to the answer faster than hunting for obscure kernel flags.
