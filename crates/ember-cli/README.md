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

## options

| flag | default | description |
|------|---------|-------------|
| `-H`, `--host` | 127.0.0.1 | server hostname |
| `-p`, `--port` | 6379 | server port |
| `-a`, `--password` | — | password for AUTH |
| `--tls` | — | enable TLS (not yet supported) |

## repl features

- **tab completion** — press tab to autocomplete command names
- **history** — command history persisted to `~/.emberkv_history`
- **inline help** — type `help` for all commands, `help SET` for details
- **reconnection** — automatically reconnects if the server disconnects
- **quoted strings** — double and single quoted arguments with backslash escapes
- **timeouts** — 5s connect timeout, 10s read timeout to avoid hanging

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
