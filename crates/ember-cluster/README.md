# ember-cluster

distributed coordination for [ember](https://github.com/kacy/ember). provides cluster topology management, gossip-based failure detection, raft consensus, and live slot migration.

## features

- **slot management** — 16384 hash slots with CRC16 hashing (Redis Cluster compatible)
- **topology tracking** — node identity, roles (primary/replica), health status
- **SWIM gossip** — failure detection with configurable probe intervals and suspicion timeouts
- **raft consensus** — cluster configuration changes via [openraft](https://github.com/datafuselabs/openraft)
- **live migration** — slot resharding without downtime, MOVED/ASK redirects

## modules

| module | description |
|--------|-------------|
| `slots` | CRC16 hash function, slot-to-node mapping, slot ranges |
| `topology` | `NodeId`, `ClusterNode`, `ClusterState`, health tracking |
| `gossip` | SWIM protocol engine, membership events, probe management |
| `raft` | openraft integration, cluster commands, state machine |
| `migration` | migration state machine, batch streaming, key tracking |
| `message` | binary wire format for gossip messages |
| `error` | cluster-specific error types with MOVED/ASK support |

## usage

```rust
use ember_cluster::{ClusterState, ClusterNode, NodeId, key_slot};

// create a single-node cluster
let node_id = NodeId::new();
let node = ClusterNode::new_primary(node_id, "127.0.0.1:6379".parse().unwrap());
let cluster = ClusterState::single_node(node);

// route a key to its slot
let slot = key_slot(b"mykey");
assert!(cluster.owns_slot(slot));
```

## cluster commands

the following CLUSTER commands are supported at the protocol layer:

| command | description |
|---------|-------------|
| `CLUSTER INFO` | cluster state and configuration |
| `CLUSTER NODES` | list of cluster nodes |
| `CLUSTER SLOTS` | slot distribution across nodes |
| `CLUSTER KEYSLOT <key>` | compute slot for a key |
| `CLUSTER MYID` | local node's ID |
| `CLUSTER MEET <ip> <port>` | add a node to the cluster |
| `CLUSTER ADDSLOTS <slot>...` | assign slots to local node |
| `CLUSTER DELSLOTS <slot>...` | remove slots from local node |
| `CLUSTER SETSLOT <slot> IMPORTING/MIGRATING/NODE/STABLE` | migration control |
| `CLUSTER FORGET <node-id>` | remove a node |
| `CLUSTER REPLICATE <node-id>` | become a replica |
| `CLUSTER FAILOVER [FORCE\|TAKEOVER]` | manual failover |
| `CLUSTER COUNTKEYSINSLOT <slot>` | key count in slot |
| `CLUSTER GETKEYSINSLOT <slot> <count>` | get keys in slot |
| `ASKING` | follow ASK redirect |
| `MIGRATE <host> <port> <key> <db> <timeout>` | migrate a key |

## design notes

- **in-memory raft log** — persistence is deferred; cluster state survives via gossip
- **single raft group** — all cluster config changes go through one raft group (simpler than multi-raft)
- **server integration pending** — modules are complete but not yet wired into ember-server

## related crates

| crate | what it does |
|-------|-------------|
| [emberkv-core](../ember-core) | storage engine, keyspace, sharding |
| [ember-protocol](../ember-protocol) | RESP3 parsing and command dispatch |
| [ember-persistence](../ember-persistence) | AOF, snapshots, and crash recovery |
| [ember-server](../ember-server) | TCP server and connection handling |
| [ember-cli](../ember-cli) | interactive CLI client (REPL, cluster subcommands, benchmark) |
