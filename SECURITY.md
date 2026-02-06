# security policy

## supported versions

| version | supported |
|---------|-----------|
| 0.1.x   | yes       |

## reporting a vulnerability

if you discover a security vulnerability in ember, please report it responsibly:

1. **do not** open a public issue
2. email the maintainers directly at kacy@kacyfortner.com (or open a private security advisory on github)
3. include:
   - a description of the vulnerability
   - steps to reproduce
   - potential impact
   - any suggested fixes (optional)

we aim to respond within 48 hours and will work with you to understand and address the issue.

## security considerations

### network exposure

ember binds to `127.0.0.1` by default. if you expose it to a network:

- use a firewall to restrict access to trusted clients
- consider running behind a reverse proxy with TLS termination
- ember does not currently support authentication (planned for future releases)

### memory limits

always configure `--max-memory` in production to prevent unbounded memory growth:

```bash
ember-server --max-memory 1G --eviction-policy allkeys-lru
```

### untrusted input

- command parsing is defensive and rejects malformed input
- buffer sizes are capped to prevent memory exhaustion attacks
- pattern matching (SCAN MATCH) uses an iterative algorithm to avoid regex DOS

### persistence

if using AOF or snapshots:

- ensure the data directory has appropriate permissions
- aof files contain all write commands in binary format
- snapshots contain the full keyspace state

## security updates

security fixes are released as patch versions. we recommend staying up to date with the latest release.
