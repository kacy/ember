# emberkv-cli

interactive command-line client for [ember](https://github.com/kacy/ember). a thin client that tokenizes input, sends RESP3 frames to the server, and pretty-prints responses — no client-side command validation.

## usage

```bash
# start interactive REPL (connects to 127.0.0.1:6379)
ember-cli

# connect to a specific host and port
ember-cli -H 10.0.0.1 -p 6380

# authenticate
ember-cli -a mypassword

# one-shot mode — run a single command and exit
ember-cli PING
ember-cli SET greeting hello
ember-cli GET greeting

# quoted strings work in both modes
ember-cli SET msg "hello world"
```

## TLS

connect to a TLS-enabled server:

```bash
# with system CA trust store
ember-cli -p 6380 --tls PING

# with a custom CA certificate
ember-cli -p 6380 --tls --tls-ca-cert /path/to/ca.pem PING

# skip certificate verification (self-signed certs, development only)
ember-cli -p 6380 --tls --tls-insecure PING

# REPL over TLS
ember-cli -p 6380 --tls --tls-insecure

# benchmark over TLS
ember-cli -p 6380 --tls --tls-insecure benchmark -n 10000
```

## options

| flag | default | description |
|------|---------|-------------|
| `-H`, `--host` | 127.0.0.1 | server hostname |
| `-p`, `--port` | 6379 | server port |
| `-a`, `--password` | — | password for AUTH |
| `--tls` | — | enable TLS for the connection |
| `--tls-ca-cert` | — | path to CA certificate (PEM) for server verification |
| `--tls-insecure` | — | skip server certificate verification |

## repl features

- **tab completion** — press tab to autocomplete command names and subcommands
- **syntax highlighting** — known commands in cyan, unknown in red, quoted strings in green
- **inline hints** — shows argument synopsis and subcommand options as you type
- **history** — command history persisted to `~/.emberkv_history`
- **inline help** — type `help` for all commands, `help SET` for details
- **reconnection** — automatically reconnects if the server disconnects
- **quoted strings** — double and single quoted arguments with backslash escapes
- **timeouts** — 5s connect timeout, 10s read timeout to avoid hanging

## cluster subcommands

manage a cluster directly from the CLI without manually typing `CLUSTER` commands:

```bash
ember-cli cluster info
ember-cli cluster nodes
ember-cli cluster slots
ember-cli cluster keyslot mykey
ember-cli cluster myid
ember-cli cluster meet 10.0.0.1 6379
ember-cli cluster forget <node-id>
ember-cli cluster addslots 0 1 2 3
ember-cli cluster delslots 100 200
ember-cli cluster setslot 42 importing <node-id>
ember-cli cluster setslot 42 migrating <node-id>
ember-cli cluster setslot 42 node <node-id>
ember-cli cluster setslot 42 stable
ember-cli cluster replicate <node-id>
ember-cli cluster failover --force
ember-cli cluster failover --takeover
ember-cli cluster countkeysinslot 42
ember-cli cluster getkeysinslot 42 10
```

## built-in benchmark

run a built-in benchmark with pipelining support and latency percentile reporting:

```bash
# basic benchmark (100k requests, 50 clients)
ember-cli benchmark

# high-throughput test with pipelining
ember-cli benchmark -n 1000000 -c 50 -P 16

# test specific workloads
ember-cli benchmark -t set,get,ping

# customize data size and keyspace
ember-cli benchmark -d 128 --keyspace 1000000

# quiet mode — summary lines only
ember-cli benchmark -q
```

| flag | default | description |
|------|---------|-------------|
| `-n`, `--requests` | 100,000 | total number of requests |
| `-c`, `--clients` | 50 | concurrent client connections |
| `-P`, `--pipeline` | 1 | commands per pipeline batch |
| `-d`, `--data-size` | 64 | value payload size in bytes |
| `-t`, `--tests` | set,get | comma-separated workloads (`set`, `get`, `ping`) |
| `--keyspace` | 100,000 | number of unique keys |
| `-q`, `--quiet` | — | only print summary lines |

output:

```
=== ember benchmark ===
server:     127.0.0.1:6379
requests:   100,000
clients:    50
pipeline:   16
data size:  64 bytes

SET: 523,809 rps    p50: 120us  p99: 410us  p99.9: 1.23ms  max: 4.56ms
GET: 612,345 rps    p50: 100us  p99: 380us  p99.9: 1.01ms  max: 3.21ms
```

## local commands

these are handled by the client and not sent to the server:

| command | description |
|---------|-------------|
| `help` | show all commands grouped by category |
| `help <command>` | show usage for a specific command |
| `quit` / `exit` | exit the REPL |
| `clear` | clear the terminal screen |

## related crates

| crate | what it does |
|-------|-------------|
| [emberkv-core](../ember-core) | storage engine, keyspace, sharding |
| [ember-protocol](../ember-protocol) | RESP3 parsing and command dispatch |
| [ember-persistence](../ember-persistence) | AOF, snapshots, and crash recovery |
| [ember-server](../ember-server) | TCP server and connection handling |
| [ember-cluster](../ember-cluster) | distributed coordination |
