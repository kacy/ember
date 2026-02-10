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

### encryption at rest

ember supports optional AES-256-GCM encryption for AOF and snapshot files. enable it by building with `--features encryption` and passing `--encryption-key-file`:

```bash
# generate a 32-byte random key
dd if=/dev/urandom bs=32 count=1 > /path/to/ember.key
chmod 600 /path/to/ember.key

ember-server --data-dir ./data --appendonly --encryption-key-file /path/to/ember.key
```

key management considerations:

- **key loss = data loss** — there is no recovery mechanism if the key file is lost. back it up separately from your data directory
- **use a secrets manager** in production (e.g., HashiCorp Vault, AWS Secrets Manager) rather than storing the key file on the same disk as the data
- **key file permissions** — restrict to `600` (owner read/write only)
- **key rotation** — run `BGREWRITEAOF` and `BGSAVE` after swapping the key file to re-encrypt all persistence files with the new key. the old key is no longer needed once rewriting completes
- **transparent migration** — existing plaintext files are read normally even after enabling encryption. they are migrated to the encrypted format on the next `BGREWRITEAOF` or `BGSAVE`

## security updates

security fixes are released as patch versions. we recommend staying up to date with the latest release.
