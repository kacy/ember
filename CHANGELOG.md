# changelog

## unreleased

### added
- ACL channel rules, as in Redis 7: `&pattern` lets a user publish and subscribe to matching channels, `allchannels` (or `&*`) to every channel, and `resetchannels` to none. PSUBSCRIBE needs a pattern that is one of the user's patterns exactly. a user created with ACL SETUSER or the ACL file starts without channels, while the default user has all of them. any user who could run SUBSCRIBE could read every channel, keyspace notifications included. ACL LIST and GETUSER show the channel rules, and a subscriber whose user loses a channel it listens to is disconnected (#483)
- `protected-mode` setting (`--protected-mode`, `EMBER_PROTECTED_MODE`), `yes` by default, as in Redis. `no` turns protected mode off for a network that is already private, such as a container's. the docker compose quickstart and the helm chart (`ember.protectedMode`) set it to `no`: their clients, and the helm chart's probes, arrive over the container network, so with no password set the server refused them all (#479)
- TLS and ACL username options for the Go, Python and TypeScript clients: `WithTLS` and `WithUsername`, `tls`/`root_certificates` and `username`, and `tls`/`rootCerts` and `username`. a server with `tls-port` serves gRPC over TLS only, which the clients couldn't connect to (#478)

### changed
- failover elections rank a failed primary's replicas by how much of its data they hold, as in Redis Cluster. gossip pings and acks carry each node's replication offset, and a replica waits 500 ms plus one second per replica of the same primary that is further ahead before it asks for votes, plus up to 500 ms of jitter. every replica waited the same 500 ms, so the one with the least data could win and lose the writes the others had. the gossip message format changed, so nodes running an earlier version can't join the cluster (#486)
- keys go to local shards by their hash tag, as they go to cluster slots, so `{user:1}:a` and `{user:1}:b` share a shard and RENAME, COPY and the other multi-key commands work on them. they failed with "must hash to the same shard". a key with a tag may now map to another shard, so data written with tagged keys before this change isn't found after it (#482)
- file layout version 6 for AOF and snapshot files. files from version 5 are no longer read (#469):
  - an encrypted file's key is derived from its header too: the file type, the version and the flags. an encrypted AOF relabeled as a snapshot, or a header with a changed version or flags, no longer decrypts
  - a snapshot's trailer, holding the entry count, the CRC and the shard ID, is stored in a frame sealed like an entry. in an encrypted snapshot a forged trailer could drop every entry, which wiped the shard on startup, and two edited bytes could load one shard's snapshot into another
- new layout for AOF and snapshot files, version 5. every record and entry is a frame with its length stored twice and a CRC, so recovery tells a record cut short by a crash from damage in the middle of the file. a header flag marks encryption. files from earlier versions are no longer read (#450)
- snapshots store each key's expiry as a unix time. they stored the TTL left and recovery subtracted the time since the file's mtime, so a snapshot restored from a backup brought back keys that had expired (#450)

### removed
- concurrent mode: `--concurrent`, `EMBER_CONCURRENT`, the `concurrent` config key and the helm `ember.concurrent` value. the default mode is faster, serving about 35% more requests without pipelining and 2.6× more with a pipeline of 16 on a 32-core machine. concurrent mode also left strings out of the AOF and replication, rejected collection commands, and ran SET NX/XX and INCR as separate read and write steps (#446)
- raft and the `openraft` dependency. raft only ever ran on the bootstrap node, and gossip with config epochs already decided slot ownership and failover, as in Redis Cluster. `raft-port-offset` and `--cluster-raft-port-offset` are still accepted and ignored, so existing configs keep working (#443)

### performance
- replication keeps up with the primary under heavy writes. on a 32-core machine a replica loads a full sync of 1.26M keys in about 1 second instead of 13.5, and stays in step with 1.1M writes a second. a replica that fell behind, as every one did under load, was resynced from scratch in a loop and served a partial dataset each time (#455):
  - the primary queues each replica's records itself, up to 256 MiB, so writes during a full sync no longer overrun the 65,536-event broadcast channel. a replica further behind gets a resync at once instead of first receiving the stale backlog
  - shards send their records once per burst instead of once per record. every send locks the broadcast channel, which held the primary to about 600k writes a second with a replica attached
  - the replica applies records and snapshot keys in batches, one message per shard, instead of waiting for each one
- cluster mode is about as fast as standalone without pipelining: about 860k requests a second against 320k on a 32-core machine. with a pipeline of 16 it serves about 2.3M against 320k, 15% below standalone. every command locked the migration state, an async mutex shared by every connection, and read the cluster state up to three times. it now reads each once, and the migration state takes a shared lock (#453)

### fixes
- a node that CLUSTER REPLICATE turns into a replica forgets its clients blocked in BLPOP and BRPOP, whose waits then end at their timeouts. a push streamed from the new primary could pop an element for one of them, so the replica's list no longer matched the primary's (#485)
- expiry and MIGRATE fixes (#484):
  - a key found expired when a command reads it emits a keyspace `expired` event, as one found by the background sweep does. only the sweep's keys produced events
  - EXPIREAT and EXPIRETIME follow the wall clock when it is stepped, as by NTP or after a paused VM resumes. the offset between the wall clock and the monotonic clock was measured once at startup, so every later EXPIREAT and EXPIRETIME carried the drift since then
  - MIGRATE deletes the key on the source only if it hasn't changed since it was copied, and copies it again if it has. a write that passed its slot check just before the transfer began could land between the copy and the delete and be lost
- config and command edge cases (#481):
  - `stats-poll-interval-secs = 0` and `replication-broadcast-capacity = 0` are refused at startup. both panicked
  - `--host ::` and `--host ::1` bind to IPv6. an IPv6 address failed to parse
  - CONFIG SET maxmemory refuses a limit below one byte per shard, which left every shard a limit of 0 and refused every write. CONFIG REWRITEs run one at a time, since they share a temp file
  - `slowlog-max-len 0` keeps no entries. it kept one
  - with `timeout 0`, a client that stops reading its replies is still disconnected after 300 seconds. turning the idle timeout off turned the write timeout off too
  - BITOP over keys that hold no data deletes the destination, as in Redis, instead of storing an empty string
  - PEXPIRETIME of a time past `i64::MAX` ms stays positive. it wrapped to a negative number
  - INCRBYFLOAT and the other float replies print 2^63 exactly. they printed `9223372036854775807`
  - VADD_BATCH reads trailing `METRIC`, `QUANT`, `M` and `EF` options as options with a dimension of 1, where they read as one more element. with other dimensions, `M` or `EF` after the elements failed to parse
- a node that sees a replica win a slot claim at a newer epoch now records it as a primary. the role update a promoted replica sends could arrive after a newer update about the same node, such as one refuting a suspicion, and was then dropped as stale, leaving peers listing the new primary as a replica (#480)
- CLI, Rust client and SDK fixes (#478):
  - `ember-cli cluster reshard` stops when GETKEYSINSLOT or SETSLOT NODE fails. a GETKEYSINSLOT error read as an empty slot, so the slot was handed over with its keys left behind. a slot whose MIGRATING step fails goes back to STABLE on the target, a node id prefix must match exactly one node, and IPv6 addresses such as `[::1]:6379` lose their brackets before connecting
  - `ember-cli benchmark` no longer hangs when a client fails to connect or authenticate, or when there are fewer requests than clients. `--clients 0` and `--keyspace 0` are refused instead of panicking, prepopulate writes every key in the range with `-P` above 1, and a wrong password is reported
  - `--tls-ca-cert` and `--tls-insecure` turn TLS on. without `--tls` the CLI connected in plaintext and sent the password unprotected
  - the REPL leaves AUTH, HELLO and MIGRATE with AUTH, ACL SETUSER, and CONFIG SET of a password out of its history, quoted or not
  - `ember-client` keeps messages that arrive while SUBSCRIBE or PSUBSCRIBE waits for its confirmations, and `unsubscribe(&[])` reads one confirmation per channel. a message could be taken for a confirmation and lost, and the confirmations left over then came back as replies
  - `ember-client` and `ember-cli` skip OS certificates that can't be parsed instead of refusing to connect
- pub/sub fixes (#477):
  - a subscriber whose receiver lags on one channel keeps listening to it. it stopped listening to that channel and, with its other channels quiet, never received another message
  - PSUBSCRIBE to a pattern the connection already has is a no-op, as in Redis. a repeated PSUBSCRIBE replaced the subscription without releasing the first, so PUBSUB NUMPAT never dropped back
  - gRPC Subscribe waits for messages instead of checking every millisecond, and ends when the client disconnects. on a quiet channel the stream and its subscriptions stayed behind forever
- startup and shutdown fixes (#476):
  - the server exits with an error when a worker can't bind its port. it logged the error, stopped that worker's shard, and reported that it was listening with the shard missing
  - a worker that reaches its accept loop after the shutdown signal still stops. the signal only reached workers already waiting for it, and one that wasn't kept the server from exiting
- replication fixes (#475):
  - the primary stops streaming to a replica that has stopped acknowledging. heartbeats kept a dead replica's connection open, so WAIT and INFO replication went on counting it
  - a replica that fails to apply a write from the primary, with an error, OOM or WRONGTYPE, doesn't acknowledge it and resyncs. it acknowledged the write as applied and carried on with different data
  - WAIT counts a replica only once it has acknowledged every shard. after a primary restart, a replica still loading its full sync counted as caught up
  - protobuf schemas reach replicas: a full sync sends the schemas first, and a replica registers the schemas the stream carries. replicas had none, so every PROTO command failed on them
  - the replica frees the buffer for the largest shard snapshot once the full sync is in
  - ASKING covers only the next command, whether that command succeeds, fails to parse, is refused, or is AUTH or part of a transaction. a command that failed kept the flag for the one after it
  - EXEC during a failover pause refuses the whole transaction instead of running it with its writes refused one by one
  - MIGRATE reports a failed delete on the source after copying the key, and never sends a TTL of 0 for a key about to expire, which RESTORE would have kept forever
- cluster command fixes (#474):
  - a replica sends reads of slots owned by another primary to that primary with MOVED. it served them from its own data, which only holds its primary's slots, and so answered as if the keys didn't exist
  - CLUSTER FORGET removes the node's slots and stops gossip from probing it. the slots stayed assigned to the forgotten node
  - the random id CLUSTER MEET uses until the node answers is dropped from gossip once the real id arrives. gossip kept probing it, then reported it failed
  - CLUSTER REPLICATE refuses a node that serves slots, as Redis does, since those slots would be left without a primary
  - CLUSTER SETSLOT NODE refuses an unknown node or a replica
- election and gossip fixes (#473):
  - a replica that wins an election takes the epoch it won in. it took the next local epoch, which could be older than the one the voters granted
  - CLUSTER FAILOVER returns an error when the promotion fails. it logged a warning, announced the promotion anyway and replied OK
  - failure reports that overlap start one election, not several at once
  - CLUSTER ADDSLOTS and DELSLOTS check and change the slots under one lock and take a new config epoch, and a node ignores slot claims older than the last one it saw from the same node. replaying an old gossip packet could take back slots a node had added
  - a gossip Alive update no longer changes the address of a node already known. any node with the cluster secret could point another node's address at itself and receive its election messages
  - at most 1024 PingReq relays are in flight, at most 16 Joins a second get a Welcome, and the updates added to a Ping or Ack stay under 40 KiB. a Ping carrying several large slot claims could exceed the size of a UDP datagram and be dropped silently
- cluster role and quorum fixes (#472):
  - gossip counts a newly seen node as a primary until it hears otherwise, as the cluster state does, and a restarted node seeds its peers with the roles and slots saved for them. a node described every primary it had only heard of as a replica in the Welcome it sent to a joining node, which then demoted that primary in its own view
  - a Welcome updates a member the joiner already knows when it carries a newer role or newer slots
  - only primaries that serve slots count toward the write quorum and vote in failover elections, as in Redis Cluster. a primary replaced in a failover still counted, so after one failover in a three-primary cluster, losing another primary left the cluster down for good
  - a primary only votes for a known replica of the failed primary, and only once that primary is marked failed. it voted for unknown nodes and for primaries, and on a mere suspicion
  - a primary whose last slots another primary takes at a newer epoch, as when it comes back after being replaced, becomes a replica of that primary
- MIGRATE, request parsing and CLIENT SETNAME fixes (#471):
  - MIGRATE only runs in cluster mode and only connects to the client address of a node in the cluster. it connected to any host and port a client named, and sent it the key and any AUTH credentials given. MIGRATE and RESTORE are in `@admin`, and MIGRATE reads at most 64 KiB of replies from the target
  - a RESP request must be an array of bulk strings, as in Redis, and is only allocated once it is complete. requests that arrived a few bytes at a time were parsed from the start on every read, and nested arrays could build about a million frames from one request. a length line such as `$5` must end within 32 bytes, and more than 64 KiB of blank lines before a command closes the connection
  - CLIENT SETNAME refuses names with spaces, newlines or other characters outside printable ASCII, and names over 1024 bytes. such a name could add fields or lines to CLIENT LIST
- TLS, protected mode and metrics fixes (#470):
  - `port = 0` turns the plaintext listener off, for a server that only serves TLS. it bound a random plaintext port on every worker, outside of mTLS, although the production checklist said the plain port could be turned off. a server with `port = 0` needs `tls-port` and can't run in cluster mode
  - protected mode looks at the `default` ACL user and applies when that user is on and needs no password. it only looked at `requirepass`, so an ACL file that left `default` open without a password exposed the server
  - the metrics and health endpoint follows protected mode, serves at most 64 connections at once, gives a client 5 seconds to send its headers and closes a connection after 30 seconds. clients that opened connections and sent nothing could use up the server's file descriptors
  - the RESP, TLS and metrics listeners wait 50 ms after a failed accept. errors such as running out of file descriptors repeat at once, and retrying straight away kept a core busy
  - `/health` reports the `maxmemory` currently in force, not the one the server started with
- ACL fixes (#468):
  - command rules apply in the order given, and the last rule that matches a command decides, as in Redis. `-@all +@read` refused every read command, because a denied category always beat an allowed one. ACL LIST and GETUSER print the rules in order
  - ACL SETUSER and `--aclfile` accept `#<sha256>` and `!<sha256>` password rules, so ACL LIST output loads back
  - ACL DELUSER with `default` among the names deletes nobody. it deleted the names before `default` and then failed
  - a session without a user may only run AUTH, PING, ECHO and QUIT. the permission check let it run anything
  - a deleted or disabled user, or one that lost the command, is disconnected from MONITOR and subscriber mode at the next event, and a gRPC pipeline stream checks the current user for each command. they kept running with the old permissions
  - PROTO.REGISTER is in the `@admin` and `@dangerous` categories, so `-@admin` or `-@dangerous` keeps users from changing server-wide schemas
- VADD batch fixes (#467):
  - a batch for an existing vector set must match its dimension. a different one went to the index unchecked
  - an element named more than once in a batch takes its last vector. the parallel path assigned it two index slots
  - entries whose index insert fails are removed from the set. they stayed in the element maps with no vector in the index, and an update that failed lost its old vector while the element still showed as present
  - a batch with failures replies with an error. it replied with a count, and the AOF and replicas received every entry, including the ones that failed. now they receive only the entries that went in, plus a removal for each update that failed
- maxmemory and eviction fixes (#466):
  - under `allkeys-lru`, a write too large for the limit is refused before anything is evicted. it evicted every other key in the shard and then failed anyway
  - GETSET, MSETNX and PROTO field writes reply OOM when the value doesn't fit. they replied as if it was written and logged it to the AOF and replicas. MSETNX makes room for every pair up front, so it can't stop part way
  - SUNIONSTORE, SINTERSTORE, SDIFFSTORE and the ZSTORE commands check memory before replacing the destination, so an OOM leaves it as it was. SUNIONSTORE and its siblings deleted it first, and the ZSTORE commands didn't check memory at all
  - LMOVE and SMOVE make room before taking the element, and eviction keeps both keys. COPY keeps its source from eviction. eviction could remove the source in the middle of the write, which the AOF then replayed differently
  - LMOVE from a list to itself rotates in place, so a one-element list keeps its TTL
  - LSET, SETRANGE and SETBIT check `maxmemory` before building the new value. SETRANGE and SETBIT allocated up to 512 MiB first. SETRANGE with an empty value changes nothing and creates no key, as in Redis
- BLPOP and BRPOP fixes (#465):
  - a client blocked on keys in several shards takes exactly one element. a second shard could pop an element after the first one was delivered, and the element was lost. a client that timed out could also lose an element popped for it at the same moment
  - BLPOP and BRPOP reply WRONGTYPE when they reach a key holding another type before a list with elements, as in Redis. they returned nil after the timeout
  - a blocked pop is written to the AOF before the client receives the element
  - RENAME of a list into a key that clients are blocked on wakes them
- AOF durability fixes (#464):
  - under `appendfsync everysec` and `no`, a write whose AOF records couldn't be written at the end of its burst replies with an error. it replied OK
  - BGSAVE, BGREWRITEAOF, FLUSHDB, FLUSHALL and the automatic save report a shard's failure. they replied OK or logged success whatever the shards said. BGSAVE without a data directory is now an error
  - recovery reads AOF and snapshot frames of any size the writer produces. a record over 512 MiB, such as a large string with its framing, was written and then stopped every later start
  - a new AOF's header and the shard count manifest are fsynced, and the manifest is only rewritten when it changes. recovery treats an empty AOF as missing. a power loss could leave an empty file that stopped startup
  - BGREWRITEAOF works when the disk is too full to append to the old AOF. it tried to write the queued records to the old file first and failed, so the rewrite that would free space never ran
  - a shard whose AOF can't be opened or rewritten at startup doesn't start, as when recovery fails. it ran without an AOF and accepted writes a restart would lose
  - the server refuses a `tls-port` equal to `port`. both listeners bound to the same port and split connections between plaintext and TLS
- ordering and lost-write fixes (#463):
  - in a pipeline, multi-key and server commands such as DEL, MGET, MSET and WAIT run after the commands before them. they ran while single-key commands were still being grouped by shard, so `SET k v` then `DEL k` in one pipeline deleted nothing and left the key set
  - WATCH notices a key that was deleted and created again, a FLUSHDB, and a watched key that didn't exist and was then created. EXEC committed in those cases
  - AOF replay keeps keys whose first TTL has passed until every record has applied, and removes the keys that are still expired at the end. a key written with a TTL and later given a new one with EXPIRE, PERSIST or RENAME was deleted on restart
- memory and overflow limits (#462):
  - AOF, snapshot and RESTORE payloads read each length-prefixed field as its data arrives. a 5-byte RESTORE payload could make the server allocate and zero 512 MiB first
  - RESTORE respects `maxmemory` and replies OOM when the value doesn't fit. snapshot and RESTORE payloads with sorted set scores that aren't finite, or HNSW parameters above VADD's limit of 1024, are refused
  - HSCAN, SSCAN, ZSCAN and PROTO.FIND size their result buffer by the collection instead of COUNT. a gRPC HScan with a huge count tried to allocate hundreds of gigabytes
  - inside MULTI, UNWATCH counts toward the transaction size limit like other commands. a command refused by the ACL or arity check inside MULTI makes EXEC abort, as in Redis
  - CLUSTER ADDSLOTSRANGE refuses ranges covering more slots than exist, so repeated ranges can't expand to gigabytes. COUNTKEYSINSLOT and GETKEYSINSLOT refuse slots above 16383
  - a config epoch from gossip is capped at `u64::MAX / 2`. a forged `u64::MAX` made the next slot move or failover overflow and abort the server
- crash fixes (#461):
  - SORT, ZINTER, ZUNION and their STORE forms sort with a total order. a NaN score, such as `inf + -inf` from two sets, made Rust's sort panic and abort the server. SORT refuses NaN elements
  - ZINCRBY refuses a result that isn't finite, so scores can't overflow to infinity. gRPC ZAdd and ZIncrBy refuse NaN and infinite scores, as RESP does
  - `WAIT n 0` blocks until enough replicas catch up, as in Redis. it returned at once. a timeout too large to add to the clock also waits without a limit instead of risking an overflow panic
  - PROTO.SETFIELD and PROTO.DELFIELD refuse paths that go through a repeated or map field
- gossip hardening (#460):
  - with a cluster secret, each gossip message is signed with the time it was sent, and nodes drop messages more than 30 seconds off their own clock, so a recorded message can't be replayed later. nodes need clocks that agree within that
  - a Welcome reply is split across as many datagrams as it needs. a large cluster could build one bigger than a UDP datagram holds, and the joining node never received the member list
  - the Left update is gone. nothing sent it, but a node that received one removed the member for good
- security fixes (#459):
  - simple string and error replies turn CR and LF into spaces. many errors repeat client input, so a key or argument holding a line break could add replies of its own
  - MULTI, EXEC, DISCARD, WATCH and UNWATCH check ACL permissions. they skipped the check, so a user could WATCH keys outside their key patterns
  - an encryption key file with non-ASCII characters is refused at startup. the parser panicked on it
  - with an encryption key, plaintext AOF and snapshot files are refused. anyone who could write to the data directory could swap in a plaintext file with data of their own. existing data can no longer be encrypted in place; see the troubleshooting guide
  - encrypted records are authenticated with their position in the file, so they can't be reordered, repeated or removed from the middle. encrypted files from earlier builds fail to decrypt
- connections and auth (#458):
  - MULTI, EXEC and WATCH work in a batch that also holds AUTH. such a batch skipped transaction handling: MULTI was refused, its commands ran at once, and an open transaction stopped queueing
  - ACL SETUSER applies all of its rules or, when one is invalid, none. it kept the rules before the invalid one, and created the user
  - a transaction's queued commands and watched keys may hold at most 128 MiB, the most one command may. the queue had no limit. a command past it is refused, and EXEC then discards the transaction
  - CONFIG REWRITE keeps the config file's permissions, and writes it readable by the owner only until then. it wrote the file, which can hold requirepass, with the default permissions, often readable by everyone
  - gRPC connections count toward `maxclients`
- gRPC uses TLS when `tls-port` is set, with the same certificate, key and client certificate settings. it served plaintext on its own port even then, so a server that encrypted RESP still sent the same data in the clear over gRPC. the server refuses to start when the TLS port and the gRPC port are the same (#457)
- replication hardening (#456):
  - the replica checks that the primary holds the cluster secret before taking its data. only the primary checked the replica, so anything that could pose as the primary's address could replace a replica's data. each side signs the other's nonce after a label for its role, so one side's answer can't be relayed as the other's. the replication protocol version is now 3
  - the primary drops a connection that hasn't finished the handshake within 10 seconds, and runs at most 16 handshakes at once. an idle connection held a task forever before authenticating
  - either side drops a connection on which a read or write makes no progress for 60 seconds, as Redis's `repl-timeout`. the primary sends a heartbeat after 10 seconds without data and the replica answers it. a primary that stopped responding left its replica waiting forever
  - the replica reads snapshot and record bodies into buffers that grow as the bytes arrive. it allocated the full length from the header, up to 4 GiB, before reading any of it
  - on a shard count mismatch the primary sends its status byte before closing. it was left unflushed, so the replica saw only a closed connection
- WAIT counts a replica only once it has loaded its full sync. it counted the replica as soon as the primary had sent the snapshot, while the replica was still loading it, or had just flushed its data to start over (#455)
- `INFO replication` on a primary counts the replicas streaming from it in `connected_replicas`. it counted the replicas gossip had reported, which could read 0 while one streamed. a new `sync_full` field counts the full syncs sent (#455)
- failover safety (#452):
  - an election needs a majority of all primaries, the failed one included, as in Redis Cluster. it counted only the other primaries, so with two primaries a replica cut off with one of them won with a single vote while its primary kept serving
  - a primary votes for one replica per failed primary for two failure timeouts, so two replicas of the same primary can't both win. a replica stops its election once another node owns the failed primary's slots
  - a primary that can't reach a majority of the primaries rejects writes with CLUSTERDOWN, so a cut-off primary stops accepting writes the majority is about to replace
  - a node that joins learns which nodes are replicas from the Welcome reply. it saw them as primaries before, which put them in the election quorum, and the node sending the reply always described itself as a primary
  - refuting a Dead claim no longer lowers the node's incarnation
- slot migration (#451):
  - MIGRATE sends ASKING before RESTORE, so the target accepts the key for a slot it is importing. every MIGRATE during a reshard failed with MOVED before
  - MIGRATE takes `AUTH password` and `AUTH2 username password` for a target with a password, honors `COPY`, and replies `NOKEY` for a missing key, as in Redis
  - while a slot migrates, the source sends ASK for any key it no longer has, not only for keys MIGRATE moved, so keys written during the move land on the target instead of being left behind. a command with keys on both sides gets TRYAGAIN
  - `ember-cli cluster reshard` passes its password to MIGRATE, and stops on a failed key instead of listing it forever and then reassigning a slot that still held keys
- AOF durability (#449):
  - under `appendfsync everysec` and `no`, writes now reach the OS before their replies, so a crash of the process loses no acknowledged write. before, they sat in an 8 KB buffer, and `no` never flushed it
  - a write that fails part way, as on a full disk, no longer leaves a partial record that made the rest of the file unreadable
  - any failed AOF write or fsync now rejects writes until a retry succeeds, not only a full disk
  - an AOF rewrite fsyncs the directory and keeps its file handle instead of reopening the file
  - shutdown fsyncs the AOF even when the runtime drops the shard task
- GETEX with a deadline already past now deletes the key, as in Redis. it was logged to the AOF as PERSIST, so the key came back without a TTL after a restart and on replicas (#448)
- jemalloc no longer uses transparent huge pages. on a host with THP set to `always`, an idle 32-shard server used 290 MB instead of 19 MB, and 2.5M keys took 975 MB instead of 665 MB. `_RJEM_MALLOC_CONF` still overrides the setting. builds without jemalloc log a warning at startup when THP is `always` (#447)
- gossip now converges after a partition heals: updates are resent several times, nodes declared dead keep being probed so they can refute it, and acks carry the sender's slot claims (#444)

## 0.4.10 (2026-07-04)

### features
- `PROTO.SCAN` and `PROTO.FIND` commands for the protobuf schema registry (#341)

### fixes
- AOF recovery: on mid-file corruption, roll back to the snapshot state instead of silently keeping a partially-applied (and possibly internally inconsistent) AOF prefix while logging "snapshot state only" (#355)
- the `vector` cargo feature (`VADD`/`VSIM`/`VADD_BATCH`/`VREM`/`VGET`/`VCARD`/`VDIM`/`VINFO`) failed to compile and is now fixed and built in CI so it can't rot again (#360)
- Dockerfile `HEALTHCHECK` fixed — the previous check used `wget` (not installed) against the metrics port (disabled by default); now uses `ember-server --healthcheck` (#355)
- helm: `requirepass` is now stored in a Secret and mounted as a file via `EMBER_REQUIREPASS_FILE` instead of a plaintext env var; added `ember.existingSecret` (#355)
- log the final AOF sync error on clean shutdown instead of swallowing it (#355)
- cluster test flakiness and a missing ember-cli binary in test builds (#343)
- security: bump `aws-lc-sys` to 0.42.0, clearing RUSTSEC-2026-0045/0047/0048 (and two more) (#363)

### deprecations
- concurrent mode (`--concurrent` / `EMBER_CONCURRENT`) is deprecated and will be removed in a future release; the server now logs a warning on startup. it only accelerates string commands and is mutually exclusive with cluster mode — the default sharded engine is the supported path (#358)

### internal
- regenerated the stale Python and TypeScript client stubs (were 17 rpcs behind the proto) and added CI gates: MSRV (1.93), python/ts client builds, a client proto-drift check, and a `vector`-feature build (#356, #360)
- moved `key_slot`/`SLOT_COUNT` slot-hashing to ember-protocol; ember-core no longer depends on ember-cluster (no longer compiles openraft). ember-cluster re-exports both names, so downstream code is unaffected (#358)
- split `execute.rs` into `exec/` sub-modules, moved `COMMAND_TABLE` to ember-protocol, and reduced command-wiring boilerplate via macros (#340, #344)
- split the 4,200-line `grpc.rs` into per-command-family modules and added an end-to-end cluster failover integration test (#359, #361)
- rewrote the architecture and docs guides; refreshed helm/compatibility/contributing docs (#362)

---

## 0.4.9 (2026-02-27)

### features
- bitmap commands: `GETBIT`, `SETBIT`, `BITCOUNT`, `BITPOS`, `BITOP` (#316, #330)
- `LMPOP` / `ZMPOP` multi-key pops (#317)
- `EXPIREAT`, `PEXPIREAT`, `EXPIRETIME`, `PEXPIRETIME`, `GETSET`, `MSETNX`, `SMOVE`, `SINTERCARD`, count args for `LPOP`/`RPOP` (#315, #318)
- `HRANDFIELD` and `ZRANDMEMBER` (#319)
- Redis 6.2+ commands: `LMOVE`, `GETDEL`, `GETEX`, `ZDIFF`, `ZINTER`, `ZUNION` (#299)
- `COMMAND`, `HINCRBYFLOAT`, `ZDIFFSTORE`/`ZINTERSTORE`/`ZUNIONSTORE`, `FLUSHALL`, `MEMORY USAGE`, `WAIT` (#312, #324, #331)
- keyspace notifications (`__keyevent@0__:expired` and write events) (#313)
- automatic snapshot scheduling (#300)
- `CONFIG SET` live-apply for maxmemory and maxmemory-policy (#312)
- ember-client: typed async RESP3 API with pipelining; CLI now reuses it (#297, #302, #303)
- ember-ts client with full API parity; npm package renamed to `emberdb` with OIDC trusted publishing (#308, #309, #332)
- grpc: 39 new rpcs closing the RESP/gRPC api gap; 17 new commands surfaced across cli, grpc, and clients (#306, #320)
- CLI watch mode and batch mode (#298)

### fixes
- BITOP cross-shard correctness (#330)
- cluster reports `cluster_state:ok` on bootstrap (#339)
- eliminated production panics in client decoder and server startup (#322)
- auth failure metrics, command memory budget, migration progress reporting (#323)
- replication send-failure counter; go client subscribe error propagation (#325)
- raft log persistence switched from bincode to postcard; dropped atomic-polyfill (#296, #329)
- prometheus counters, crash recovery hardening, TLS tests (#314)
- go client: regenerated protobuf stubs for the 17 new rpcs (#337)

### docs
- command count updated to 190+; compatibility guide refreshed (#335)

---

## 0.4.8 (2026-02-25)

### performance
- entry struct optimizations: version field moved to lazy side table, cached_value_size packed as u32, ENTRY_OVERHEAD tightened from 128 to 104 (#284-287)
- skip touch() timestamp updates when eviction is disabled (#287)
- packed hash encoding — hash memory reduced from ~451 to ~240 B/key (#276)
- vector insert throughput optimization with binary-encoded VADD_BATCH (#271-272)

### fixes
- ENTRY_OVERHEAD bumped from 100 to 104 for cross-platform CI compatibility (#292-294)
- rate-limited ENOSPC handling for AOF writes (#249)

### docs
- pruned concurrent mode from the docs (#275, #291) — correction: the mode itself was not removed and is still available behind `--concurrent`; sharded remains the default execution mode
- refreshed all benchmark numbers from 2026-02-25 GCP run (#288-290)
- added documentation section, code of conduct, performance tuning guide, production checklist (#270, #281-282)

---

## 0.4.7 — launch readiness (february 2026)

the final push before a proper release. this milestone focused on correctness, operability, and the kinds of features that turn a cache into something production teams can actually trust. a lot of the work here is about closing gaps — the commands that weren't there yet, the error paths that weren't handled, and the defaults that needed to be locked down.

### features
- acl system with per-user access control (command and key permissions)
- `WATCH` / `MULTI` / `EXEC` / `DISCARD` — optimistic locking and transaction support
- `BLPOP` / `BRPOP` blocking list operations with per-shard waiter registries
- `MONITOR` command for real-time command stream inspection
- `/health` http endpoint for load balancer readiness checks
- `SSCAN`, `HSCAN`, `ZSCAN` cursor-based iteration for all collection types
- `CLIENT ID`, `SETNAME`, `GETNAME`, `LIST` — connection introspection
- `COPY`, `OBJECT`, `TIME`, `LASTSAVE`, `ROLE` utility commands
- `RANDOMKEY`, `TOUCH`, `SORT` key-space commands
- full sorted set coverage: `ZREVRANK`, `ZREVRANGE`, `ZCOUNT`, `ZINCRBY`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZPOPMIN`, `ZPOPMAX`
- full set algebra: `SUNION`, `SINTER`, `SDIFF` with `STORE` variants; `SPOP`, `SRANDMEMBER`, `SMISMEMBER`
- full list coverage: `LINDEX`, `LSET`, `LTRIM`, `LINSERT`, `LREM`, `LPOS`
- string aliases: `SETNX`, `SETEX`, `PSETEX`, `GETRANGE`, `SETRANGE`, `SUBSTR`
- `TOML` config file support with `CONFIG GET`, `CONFIG SET`, `CONFIG REWRITE`
- example config file and annotated docker-compose quickstart
- apache 2.0 license

### performance
- thread-per-core workers with `SO_REUSEPORT` — connections distributed across cores at the kernel level
- `split_to` zero-copy read loop — eliminates buffer copies on the receive path
- `CompactString` keys with small-string optimization (≤24 bytes inline, no heap allocation)
- batch pipeline dispatch — flush accumulated commands as a unit rather than one at a time
- p=1 latency improvements targeting sub-millisecond round-trips
- compact hash encoding for small hash maps
- cpu pinning on linux for cache-local shard execution
- persistent raft log storage for fast cluster restarts
- keyspace and shard refactored out of the server monolith for cleaner hot-path separation
- data type micro-benchmarks added to ci

### fixes
- `ENOSPC` and oom error handling — disk-full and memory-full conditions now return proper errors instead of panicking
- config validation on startup catches invalid combinations before binding
- graceful shutdown improvements — in-flight requests drain cleanly on sigterm
- `clone` audit across hot paths — removed unnecessary allocations found during review
- expiry deduplication — redundant expiry checks consolidated
- command dispatch and connection handling split into focused modules (was one large file)

---

## 0.3.x — distributed systems and security (february 2026)

this is the "make it real" milestone. ember went from a single-node cache to a clustered system with replication, automatic failover, and the kind of security hardening you'd want before putting it in front of production traffic.

### features
- full cluster replication: `CLUSTER REPLICATE`, primary-to-replica sync stream, `CLUSTER FAILOVER`
- automatic failover via epoch-based voting — replicas elect a new primary without operator intervention
- cluster server integration wiring — gossip, raft, and slot migrations connected to the request path
- `MIGRATE` / `RESTORE` for live key transfer during resharding
- `ASK` redirects during slot migration; `MOVED` redirects for misrouted keys
- `nodes.conf` persistence — cluster topology survives restarts
- gossip slot propagation and `PingReq` relay for indirect failure detection
- resp key/value size limits — 512kb max key, 512mb max value, enforced at parse time
- hmac-sha256 cluster transport authentication (`--cluster-auth-pass`)
- redis-compatible `AUTH` command and password configuration
- `ARCHITECTURE.md` with detailed design notes
- install script and docker-compose quickstart
- redis compatibility documentation
- `MONITOR`, `SLOWLOG`, enriched `INFO` command
- `CONFIG GET` / `CONFIG SET` with runtime reconfiguration
- cluster cli subcommands: `create`, `check`, `reshard`, `rebalance`

### performance
- zero-alloc resp parsing — no heap allocation on the command parsing hot path
- `AHashMap` keyspace — faster hashing for string keys
- `Box<str>` keys — smaller representation than `String`
- single-lookup `SET NX/XX` — avoided double hash lookup on conditional sets
- `BufWriter` on replication stream — fewer syscalls for replica sync
- bincode for raft rpcs — smaller wire format than json
- o(log n) sorted set rank via vec-based structure
- incremental memory tracking — `grow_by` / `shrink_by` instead of full recompute
- parallel pipeline dispatch for multi-core throughput
- `now_ms()` caching to avoid repeated syscalls in expiry checks

### fixes
- cluster transport frame limit reduced from 64mb to 10mb
- gossip incarnation jump limit to prevent state amplification attacks
- cli panic prevention — `truncate_id`, div-by-zero guard, `expect` → `error`
- osc escape sanitization in cli output
- server auth counter uses `saturating_add`
- 0 cves from `cargo audit`; `overflow-checks = true` in release profile

---

## 0.2.x — data types, observability, and the cli (february 2026)

with the foundation solid, this milestone filled in everything that makes ember useful day-to-day: the full redis data model, a real command-line experience, metrics, benchmarks, and the more exotic features that set ember apart.

### features
- vector similarity search — hnsw index with `VADD`, `VSEARCH`, `VREM`, `VCARD`, `VINFO`, `VADD_BATCH`; benchmarked against chromadb, qdrant, and pgvector
- protobuf schema registry — `PROTO.SET/GET/GETFIELD/SETFIELD/DELFIELD` for typed value storage
- grpc server with proto definitions; go and python grpc clients
- pub/sub — `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `PSUBSCRIBE` with pattern matching
- tls for both client connections and the cli
- encryption at rest (aes-256-gcm)
- redis-compatible `AUTH` command
- interactive cli with syntax highlighting, fuzzy autocomplete, and inline help
- `cluster` cli subcommands (meet, nodes, info, slots, keyslot)
- built-in `benchmark` subcommand (get/set/ping workloads, configurable pipeline depth)
- prometheus metrics endpoint (`/metrics`)
- `SLOWLOG` equivalent for slow command tracking
- dockerfile and helm chart for kubernetes deployment
- `MGET` / `MSET` / `FLUSHDB` / `SCAN`
- `INCR` / `DECR` / `INCRBY` / `DECRBY`
- `SET NX/XX` conditional set semantics
- `PERSIST` / `PTTL` / `PEXPIRE` millisecond expiry precision
- full hash command set: `HSET`, `HGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HINCRBY`, `HKEYS`, `HVALS`, `HMGET`
- full set command set: `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`
- full sorted set range queries by score and rank
- ci workflows, security policy

### performance
- criterion micro-benchmarks and system benchmarks vs redis
- `--shards` flag to tune the number of keyspace partitions
- jemalloc as the global allocator
- pipeline dispatch optimization — batched command processing
- memory overhead reduction — keyspace entry footprint profiled and trimmed
- memtier benchmark integration
- bench-all.sh for running all benchmark suites in one pass
- grpc vs resp3 comparison benchmarks

### fixes
- memory limits enforced on list and sorted set growth
- various audit findings from internal security reviews

---

## 0.1.0 — foundation (february 3–4, 2026)

the initial build. started from a blank workspace and ended with a working single-node cache that could pass basic redis compatibility tests. the goal was to get the boring-but-critical pieces right before adding anything interesting.

### features
- resp3 protocol parser and serializer — zero-copy, hand-rolled, no dependencies on redis libraries
- tcp server with tokio — accept, parse, dispatch, respond
- thread-per-core sharded engine — each shard owns a keyspace partition; no cross-shard locks on the hot path
- key hashing with fnv-1a 64-bit for shard routing
- core commands: `GET`, `SET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`
- all five redis data types: string, list, set, hash, sorted set
- append-only file (aof) — binary tlv format with crc32 checksums, configurable fsync policy
- snapshot format — binary with header, per-entry records, and integrity checks
- hybrid recovery — snapshot + aof tail on startup, corruption detection
- `BGSAVE` and `BGREWRITEAOF` commands
- lazy expiration (checked on access) and active expiration (background sampling)
- lru-approximate eviction when memory limit is hit
- memory usage tracking per shard
- graceful shutdown on sigint/sigterm with connection draining
- crate readmes and initial project structure
