//! Command attribute methods — name, write classification, ACL categories.
//!
//! Separated from the enum definition for readability. Each method is a
//! simple match over all variants.

use super::Command;

impl Command {
    /// Returns the lowercase command name as a static string.
    ///
    /// Used for metrics labels and slow log entries. Zero allocation —
    /// returns a `&'static str` for every known variant.
    ///
    /// The match is explicit rather than derive-generated: a proc macro would
    /// obscure the string mappings, which are the thing most worth seeing at
    /// a glance when auditing command names.
    pub fn command_name(&self) -> &'static str {
        match self {
            // connection
            Command::Ping(_) => "ping",
            Command::Echo(_) => "echo",
            Command::Auth { .. } => "auth",
            Command::Quit => "quit",
            Command::Monitor => "monitor",
            Command::ClientId => "client",
            Command::ClientSetName { .. } => "client",
            Command::ClientGetName => "client",
            Command::ClientList => "client",

            // strings
            Command::Get { .. } => "get",
            Command::Set { .. } => "set",
            Command::MGet { .. } => "mget",
            Command::MSet { .. } => "mset",
            Command::Incr { .. } => "incr",
            Command::Decr { .. } => "decr",
            Command::IncrBy { .. } => "incrby",
            Command::DecrBy { .. } => "decrby",
            Command::IncrByFloat { .. } => "incrbyfloat",
            Command::Append { .. } => "append",
            Command::Strlen { .. } => "strlen",
            Command::GetRange { .. } => "getrange",
            Command::SetRange { .. } => "setrange",

            // bitmaps
            Command::GetBit { .. } => "getbit",
            Command::SetBit { .. } => "setbit",
            Command::BitCount { .. } => "bitcount",
            Command::BitPos { .. } => "bitpos",
            Command::BitOp { .. } => "bitop",

            // key lifecycle
            Command::Del { .. } => "del",
            Command::Unlink { .. } => "unlink",
            Command::Exists { .. } => "exists",
            Command::Rename { .. } => "rename",
            Command::Copy { .. } => "copy",
            Command::ObjectEncoding { .. } => "object",
            Command::ObjectRefcount { .. } => "object",
            Command::Keys { .. } => "keys",
            Command::Scan { .. } => "scan",
            Command::SScan { .. } => "sscan",
            Command::HScan { .. } => "hscan",
            Command::ZScan { .. } => "zscan",
            Command::Type { .. } => "type",
            Command::Expire { .. } => "expire",
            Command::Expireat { .. } => "expireat",
            Command::Ttl { .. } => "ttl",
            Command::Persist { .. } => "persist",
            Command::Pttl { .. } => "pttl",
            Command::Pexpire { .. } => "pexpire",
            Command::Pexpireat { .. } => "pexpireat",
            Command::Expiretime { .. } => "expiretime",
            Command::Pexpiretime { .. } => "pexpiretime",

            // server
            Command::DbSize => "dbsize",
            Command::Info { .. } => "info",
            Command::Time => "time",
            Command::LastSave => "lastsave",
            Command::Role => "role",
            Command::Wait { .. } => "wait",
            Command::BgSave => "bgsave",
            Command::BgRewriteAof => "bgrewriteaof",
            Command::FlushDb { .. } => "flushdb",
            Command::ConfigGet { .. } => "config",
            Command::ConfigSet { .. } => "config",
            Command::ConfigRewrite => "config",
            Command::Multi => "multi",
            Command::Exec => "exec",
            Command::Discard => "discard",
            Command::Watch { .. } => "watch",
            Command::Unwatch => "unwatch",

            // list
            Command::LPush { .. } => "lpush",
            Command::RPush { .. } => "rpush",
            Command::LPop { .. } => "lpop",
            Command::RPop { .. } => "rpop",
            Command::LRange { .. } => "lrange",
            Command::LLen { .. } => "llen",
            Command::BLPop { .. } => "blpop",
            Command::BRPop { .. } => "brpop",
            Command::LIndex { .. } => "lindex",
            Command::LSet { .. } => "lset",
            Command::LTrim { .. } => "ltrim",
            Command::LInsert { .. } => "linsert",
            Command::LRem { .. } => "lrem",
            Command::LPos { .. } => "lpos",
            Command::LMove { .. } => "lmove",
            Command::GetDel { .. } => "getdel",
            Command::GetEx { .. } => "getex",
            Command::GetSet { .. } => "getset",
            Command::MSetNx { .. } => "msetnx",

            // sorted set
            Command::ZAdd { .. } => "zadd",
            Command::ZRem { .. } => "zrem",
            Command::ZScore { .. } => "zscore",
            Command::ZRank { .. } => "zrank",
            Command::ZRevRank { .. } => "zrevrank",
            Command::ZCard { .. } => "zcard",
            Command::ZRange { .. } => "zrange",
            Command::ZRevRange { .. } => "zrevrange",
            Command::ZCount { .. } => "zcount",
            Command::ZIncrBy { .. } => "zincrby",
            Command::ZRangeByScore { .. } => "zrangebyscore",
            Command::ZRevRangeByScore { .. } => "zrevrangebyscore",
            Command::ZPopMin { .. } => "zpopmin",
            Command::ZPopMax { .. } => "zpopmax",
            Command::Lmpop { .. } => "lmpop",
            Command::Zmpop { .. } => "zmpop",
            Command::ZDiff { .. } => "zdiff",
            Command::ZInter { .. } => "zinter",
            Command::ZUnion { .. } => "zunion",
            Command::ZRandMember { .. } => "zrandmember",

            // hash
            Command::HSet { .. } => "hset",
            Command::HGet { .. } => "hget",
            Command::HGetAll { .. } => "hgetall",
            Command::HDel { .. } => "hdel",
            Command::HExists { .. } => "hexists",
            Command::HLen { .. } => "hlen",
            Command::HIncrBy { .. } => "hincrby",
            Command::HKeys { .. } => "hkeys",
            Command::HVals { .. } => "hvals",
            Command::HMGet { .. } => "hmget",
            Command::HRandField { .. } => "hrandfield",

            // set
            Command::SAdd { .. } => "sadd",
            Command::SRem { .. } => "srem",
            Command::SMembers { .. } => "smembers",
            Command::SIsMember { .. } => "sismember",
            Command::SCard { .. } => "scard",
            Command::SUnion { .. } => "sunion",
            Command::SInter { .. } => "sinter",
            Command::SDiff { .. } => "sdiff",
            Command::SUnionStore { .. } => "sunionstore",
            Command::SInterStore { .. } => "sinterstore",
            Command::SDiffStore { .. } => "sdiffstore",
            Command::SRandMember { .. } => "srandmember",
            Command::SPop { .. } => "spop",
            Command::SMisMember { .. } => "smismember",
            Command::SMove { .. } => "smove",
            Command::SInterCard { .. } => "sintercard",

            // cluster
            Command::ClusterInfo => "cluster_info",
            Command::ClusterNodes => "cluster_nodes",
            Command::ClusterSlots => "cluster_slots",
            Command::ClusterKeySlot { .. } => "cluster_keyslot",
            Command::ClusterMyId => "cluster_myid",
            Command::ClusterSetSlotImporting { .. } => "cluster_setslot",
            Command::ClusterSetSlotMigrating { .. } => "cluster_setslot",
            Command::ClusterSetSlotNode { .. } => "cluster_setslot",
            Command::ClusterSetSlotStable { .. } => "cluster_setslot",
            Command::ClusterMeet { .. } => "cluster_meet",
            Command::ClusterAddSlots { .. } => "cluster_addslots",
            Command::ClusterAddSlotsRange { .. } => "cluster_addslotsrange",
            Command::ClusterDelSlots { .. } => "cluster_delslots",
            Command::ClusterForget { .. } => "cluster_forget",
            Command::ClusterReplicate { .. } => "cluster_replicate",
            Command::ClusterFailover { .. } => "cluster_failover",
            Command::ClusterCountKeysInSlot { .. } => "cluster_countkeysinslot",
            Command::ClusterGetKeysInSlot { .. } => "cluster_getkeysinslot",
            Command::Migrate { .. } => "migrate",
            Command::Restore { .. } => "restore",
            Command::Asking => "asking",

            // slow log
            Command::SlowLogGet { .. } => "slowlog",
            Command::SlowLogLen => "slowlog",
            Command::SlowLogReset => "slowlog",

            // pub/sub
            Command::Subscribe { .. } => "subscribe",
            Command::Unsubscribe { .. } => "unsubscribe",
            Command::PSubscribe { .. } => "psubscribe",
            Command::PUnsubscribe { .. } => "punsubscribe",
            Command::Publish { .. } => "publish",
            Command::PubSubChannels { .. } => "pubsub",
            Command::PubSubNumSub { .. } => "pubsub",
            Command::PubSubNumPat => "pubsub",

            // vector
            Command::VAdd { .. } => "vadd",
            Command::VAddBatch { .. } => "vadd_batch",
            Command::VSim { .. } => "vsim",
            Command::VRem { .. } => "vrem",
            Command::VGet { .. } => "vget",
            Command::VCard { .. } => "vcard",
            Command::VDim { .. } => "vdim",
            Command::VInfo { .. } => "vinfo",

            // protobuf
            Command::ProtoRegister { .. } => "proto.register",
            Command::ProtoSet { .. } => "proto.set",
            Command::ProtoGet { .. } => "proto.get",
            Command::ProtoType { .. } => "proto.type",
            Command::ProtoSchemas => "proto.schemas",
            Command::ProtoDescribe { .. } => "proto.describe",
            Command::ProtoGetField { .. } => "proto.getfield",
            Command::ProtoSetField { .. } => "proto.setfield",
            Command::ProtoDelField { .. } => "proto.delfield",

            // acl
            Command::AclWhoAmI => "acl",
            Command::AclList => "acl",
            Command::AclUsers => "acl",
            Command::AclGetUser { .. } => "acl",
            Command::AclDelUser { .. } => "acl",
            Command::AclSetUser { .. } => "acl",
            Command::AclCat { .. } => "acl",

            Command::RandomKey => "randomkey",
            Command::Touch { .. } => "touch",
            Command::Sort { .. } => "sort",

            Command::Unknown(_) => "unknown",
        }
    }

    /// Returns `true` if this command mutates state.
    ///
    /// Used by the replica write-rejection layer: any command matching this
    /// predicate is redirected to the primary via MOVED rather than executed
    /// locally on a read-only replica.
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            // string mutations
            Command::Set { .. }
                | Command::MSet { .. }
                | Command::MSetNx { .. }
                | Command::GetSet { .. }
                | Command::Append { .. }
                | Command::Incr { .. }
                | Command::Decr { .. }
                | Command::IncrBy { .. }
                | Command::DecrBy { .. }
                | Command::IncrByFloat { .. }
                | Command::SetBit { .. }
                | Command::BitOp { .. }
            // key lifecycle
                | Command::Del { .. }
                | Command::Unlink { .. }
                | Command::Rename { .. }
                | Command::Copy { .. }
                | Command::Expire { .. }
                | Command::Expireat { .. }
                | Command::Pexpire { .. }
                | Command::Pexpireat { .. }
                | Command::Persist { .. }
            // list
                | Command::LPush { .. }
                | Command::RPush { .. }
                | Command::LPop { .. }
                | Command::RPop { .. }
                | Command::BLPop { .. }
                | Command::BRPop { .. }
                | Command::LSet { .. }
                | Command::LTrim { .. }
                | Command::LInsert { .. }
                | Command::LRem { .. }
                | Command::LMove { .. }
            // string extras
                | Command::GetDel { .. }
                | Command::GetEx { .. }
            // sorted set
                | Command::ZAdd { .. }
                | Command::ZRem { .. }
                | Command::ZIncrBy { .. }
                | Command::ZPopMin { .. }
                | Command::ZPopMax { .. }
                | Command::Lmpop { .. }
                | Command::Zmpop { .. }
            // hash
                | Command::HSet { .. }
                | Command::HDel { .. }
                | Command::HIncrBy { .. }
            // set
                | Command::SAdd { .. }
                | Command::SRem { .. }
                | Command::SUnionStore { .. }
                | Command::SInterStore { .. }
                | Command::SDiffStore { .. }
                | Command::SPop { .. }
                | Command::SMove { .. }
            // server / persistence
                | Command::FlushDb { .. }
                | Command::ConfigSet { .. }
                | Command::Exec
                | Command::BgRewriteAof
                | Command::BgSave
                | Command::Restore { .. }
            // vector
                | Command::VAdd { .. }
                | Command::VAddBatch { .. }
                | Command::VRem { .. }
            // protobuf
                | Command::ProtoRegister { .. }
                | Command::ProtoSet { .. }
                | Command::ProtoSetField { .. }
                | Command::ProtoDelField { .. }
            // acl mutations
                | Command::AclSetUser { .. }
                | Command::AclDelUser { .. }
        ) || matches!(self, Command::Sort { store: Some(_), .. })
    }

    /// Returns the ACL category bitmask for this command.
    ///
    /// Each bit corresponds to a category (read, write, string, list, etc.)
    /// as defined in `ember-server/src/acl.rs`. Used by the permission check
    /// to determine whether a user has access to a command.
    ///
    /// The bit values match the constants in the acl module:
    /// READ=1<<0, WRITE=1<<1, STRING=1<<2, LIST=1<<3, SET=1<<4,
    /// SORTEDSET=1<<5, HASH=1<<6, KEYSPACE=1<<7, SERVER=1<<8,
    /// CONNECTION=1<<9, TRANSACTION=1<<10, PUBSUB=1<<11, FAST=1<<12,
    /// SLOW=1<<13, ADMIN=1<<14, DANGEROUS=1<<15, CLUSTER=1<<16
    pub fn acl_categories(&self) -> u64 {
        // bitmask constants (duplicated from acl.rs to avoid cross-crate dep)
        const READ: u64 = 1 << 0;
        const WRITE: u64 = 1 << 1;
        const STRING: u64 = 1 << 2;
        const LIST: u64 = 1 << 3;
        const SET: u64 = 1 << 4;
        const SORTEDSET: u64 = 1 << 5;
        const HASH: u64 = 1 << 6;
        const KEYSPACE: u64 = 1 << 7;
        const SERVER: u64 = 1 << 8;
        const CONNECTION: u64 = 1 << 9;
        const TRANSACTION: u64 = 1 << 10;
        const PUBSUB: u64 = 1 << 11;
        const FAST: u64 = 1 << 12;
        const SLOW: u64 = 1 << 13;
        const ADMIN: u64 = 1 << 14;
        const DANGEROUS: u64 = 1 << 15;
        const CLUSTER: u64 = 1 << 16;

        match self {
            // connection
            Command::Ping(_) | Command::Echo(_) => CONNECTION | FAST,
            Command::Auth { .. } | Command::Quit => CONNECTION | FAST,
            Command::ClientId | Command::ClientGetName | Command::ClientSetName { .. } => {
                CONNECTION | SLOW
            }
            Command::ClientList => CONNECTION | ADMIN | SLOW,
            Command::Monitor => CONNECTION | ADMIN | SLOW,

            // string — reads
            Command::Get { .. }
            | Command::MGet { .. }
            | Command::Strlen { .. }
            | Command::GetRange { .. }
            | Command::GetBit { .. }
            | Command::BitCount { .. }
            | Command::BitPos { .. } => READ | STRING | FAST,

            // string — writes
            Command::Set { .. }
            | Command::MSet { .. }
            | Command::MSetNx { .. }
            | Command::GetSet { .. }
            | Command::Append { .. }
            | Command::SetRange { .. }
            | Command::SetBit { .. }
            | Command::BitOp { .. } => WRITE | STRING | SLOW,
            Command::Incr { .. }
            | Command::Decr { .. }
            | Command::IncrBy { .. }
            | Command::DecrBy { .. }
            | Command::IncrByFloat { .. } => WRITE | STRING | FAST,

            // keyspace — reads
            Command::Exists { .. } | Command::Type { .. } | Command::Touch { .. } => {
                READ | KEYSPACE | FAST
            }
            Command::RandomKey => READ | KEYSPACE | FAST,
            Command::Keys { .. } => READ | KEYSPACE | DANGEROUS | SLOW,
            Command::Sort { .. } => WRITE | SET | SORTEDSET | LIST | SLOW,
            Command::Scan { .. }
            | Command::SScan { .. }
            | Command::HScan { .. }
            | Command::ZScan { .. } => READ | KEYSPACE | SLOW,
            Command::Ttl { .. }
            | Command::Pttl { .. }
            | Command::Expiretime { .. }
            | Command::Pexpiretime { .. }
            | Command::ObjectEncoding { .. }
            | Command::ObjectRefcount { .. } => READ | KEYSPACE | FAST,

            // keyspace — writes
            Command::Del { .. } | Command::Unlink { .. } => WRITE | KEYSPACE | FAST,
            Command::Rename { .. } | Command::Copy { .. } => WRITE | KEYSPACE | SLOW,
            Command::Expire { .. }
            | Command::Expireat { .. }
            | Command::Pexpire { .. }
            | Command::Pexpireat { .. }
            | Command::Persist { .. } => WRITE | KEYSPACE | FAST,

            // list — reads
            Command::LRange { .. } | Command::LLen { .. } => READ | LIST | SLOW,
            Command::LIndex { .. } => READ | LIST | FAST,
            Command::LPos { .. } => READ | LIST | SLOW,

            // list — writes
            Command::LPush { .. }
            | Command::RPush { .. }
            | Command::LPop { .. }
            | Command::RPop { .. }
            | Command::LSet { .. } => WRITE | LIST | FAST,
            Command::LTrim { .. } | Command::LInsert { .. } | Command::LRem { .. } => {
                WRITE | LIST | SLOW
            }
            Command::BLPop { .. } | Command::BRPop { .. } => WRITE | LIST | SLOW,

            // sorted set — reads
            Command::ZScore { .. }
            | Command::ZRank { .. }
            | Command::ZRevRank { .. }
            | Command::ZCard { .. }
            | Command::ZCount { .. } => READ | SORTEDSET | FAST,
            Command::ZRange { .. }
            | Command::ZRevRange { .. }
            | Command::ZRangeByScore { .. }
            | Command::ZRevRangeByScore { .. } => READ | SORTEDSET | SLOW,

            // sorted set — writes
            Command::ZAdd { .. }
            | Command::ZRem { .. }
            | Command::ZIncrBy { .. }
            | Command::ZPopMin { .. }
            | Command::ZPopMax { .. }
            | Command::Zmpop { .. } => WRITE | SORTEDSET | SLOW,

            // list — multi-key pop (Redis 7.0+)
            Command::Lmpop { .. } => WRITE | LIST | SLOW,

            // sorted set — reads (Redis 6.2+)
            Command::ZDiff { .. }
            | Command::ZInter { .. }
            | Command::ZUnion { .. }
            | Command::ZRandMember { .. } => READ | SORTEDSET | SLOW,

            // string extras (Redis 6.2+)
            Command::GetDel { .. } | Command::GetEx { .. } => WRITE | STRING | FAST,

            // list extras (Redis 6.2+)
            Command::LMove { .. } => WRITE | LIST | FAST,

            // hash — reads
            Command::HGet { .. } | Command::HExists { .. } | Command::HLen { .. } => {
                READ | HASH | FAST
            }
            Command::HGetAll { .. }
            | Command::HKeys { .. }
            | Command::HVals { .. }
            | Command::HMGet { .. }
            | Command::HRandField { .. } => READ | HASH | SLOW,

            // hash — writes
            Command::HSet { .. } | Command::HDel { .. } | Command::HIncrBy { .. } => {
                WRITE | HASH | FAST
            }

            // set — reads
            Command::SIsMember { .. } | Command::SCard { .. } | Command::SMisMember { .. } => {
                READ | SET | FAST
            }
            Command::SMembers { .. } | Command::SRandMember { .. } => READ | SET | SLOW,
            Command::SUnion { .. } | Command::SInter { .. } | Command::SDiff { .. } => {
                READ | SET | SLOW
            }
            Command::SInterCard { .. } => READ | SET | SLOW,

            // set — writes
            Command::SAdd { .. } | Command::SRem { .. } | Command::SPop { .. } => {
                WRITE | SET | FAST
            }
            Command::SMove { .. } => WRITE | SET | FAST,
            Command::SUnionStore { .. }
            | Command::SInterStore { .. }
            | Command::SDiffStore { .. } => WRITE | SET | SLOW,

            // server
            Command::DbSize => SERVER | KEYSPACE | READ | FAST,
            Command::Info { .. } => SERVER | SLOW,
            Command::Time | Command::LastSave | Command::Role | Command::Wait { .. } => {
                SERVER | FAST
            }
            Command::BgSave | Command::BgRewriteAof => SERVER | ADMIN | SLOW,
            Command::FlushDb { .. } => KEYSPACE | WRITE | ADMIN | DANGEROUS | SLOW,
            Command::ConfigGet { .. } => SERVER | ADMIN | SLOW,
            Command::ConfigSet { .. } | Command::ConfigRewrite => SERVER | ADMIN | SLOW,
            Command::SlowLogGet { .. } | Command::SlowLogLen | Command::SlowLogReset => {
                SERVER | ADMIN | SLOW
            }

            // transactions
            Command::Multi | Command::Exec | Command::Discard => TRANSACTION | FAST,
            Command::Watch { .. } | Command::Unwatch => TRANSACTION | FAST,

            // cluster
            Command::ClusterInfo
            | Command::ClusterNodes
            | Command::ClusterSlots
            | Command::ClusterKeySlot { .. }
            | Command::ClusterMyId => CLUSTER | SLOW,
            Command::ClusterSetSlotImporting { .. }
            | Command::ClusterSetSlotMigrating { .. }
            | Command::ClusterSetSlotNode { .. }
            | Command::ClusterSetSlotStable { .. }
            | Command::ClusterMeet { .. }
            | Command::ClusterAddSlots { .. }
            | Command::ClusterAddSlotsRange { .. }
            | Command::ClusterDelSlots { .. }
            | Command::ClusterForget { .. }
            | Command::ClusterReplicate { .. }
            | Command::ClusterFailover { .. }
            | Command::ClusterCountKeysInSlot { .. }
            | Command::ClusterGetKeysInSlot { .. } => CLUSTER | ADMIN | SLOW,
            Command::Migrate { .. } | Command::Restore { .. } => CLUSTER | KEYSPACE | WRITE | SLOW,
            Command::Asking => CLUSTER | FAST,

            // pub/sub
            Command::Subscribe { .. }
            | Command::Unsubscribe { .. }
            | Command::PSubscribe { .. }
            | Command::PUnsubscribe { .. } => PUBSUB | SLOW,
            Command::Publish { .. } => PUBSUB | FAST,
            Command::PubSubChannels { .. }
            | Command::PubSubNumSub { .. }
            | Command::PubSubNumPat => PUBSUB | SLOW,

            // vector
            Command::VAdd { .. } | Command::VAddBatch { .. } | Command::VRem { .. } => WRITE | SLOW,
            Command::VSim { .. }
            | Command::VGet { .. }
            | Command::VCard { .. }
            | Command::VDim { .. }
            | Command::VInfo { .. } => READ | SLOW,

            // protobuf
            Command::ProtoRegister { .. } => WRITE | SERVER | SLOW,
            Command::ProtoSet { .. }
            | Command::ProtoSetField { .. }
            | Command::ProtoDelField { .. } => WRITE | STRING | SLOW,
            Command::ProtoGet { .. }
            | Command::ProtoType { .. }
            | Command::ProtoSchemas
            | Command::ProtoDescribe { .. }
            | Command::ProtoGetField { .. } => READ | STRING | SLOW,

            // ACL commands
            Command::AclWhoAmI => CONNECTION | FAST,
            Command::AclList | Command::AclUsers | Command::AclGetUser { .. } => {
                SERVER | ADMIN | SLOW
            }
            Command::AclSetUser { .. } | Command::AclDelUser { .. } => SERVER | ADMIN | SLOW,
            Command::AclCat { .. } => SERVER | SLOW,

            Command::Unknown(_) => 0,
        }
    }

    /// Returns the primary key for this command, if there is one.
    ///
    /// Used to calculate the hash slot for MOVED redirects on replicas.
    /// For multi-key commands, returns the first key.
    pub fn primary_key(&self) -> Option<&str> {
        match self {
            Command::Get { key }
            | Command::Set { key, .. }
            | Command::Incr { key }
            | Command::Decr { key }
            | Command::IncrBy { key, .. }
            | Command::DecrBy { key, .. }
            | Command::IncrByFloat { key, .. }
            | Command::Append { key, .. }
            | Command::Strlen { key }
            | Command::GetRange { key, .. }
            | Command::SetRange { key, .. }
            | Command::Persist { key }
            | Command::Expire { key, .. }
            | Command::Expireat { key, .. }
            | Command::Pexpire { key, .. }
            | Command::Pexpireat { key, .. }
            | Command::Ttl { key }
            | Command::Pttl { key }
            | Command::Expiretime { key }
            | Command::Pexpiretime { key }
            | Command::Type { key }
            | Command::Rename { key, .. }
            | Command::ObjectEncoding { key }
            | Command::ObjectRefcount { key }
            | Command::GetSet { key, .. }
            | Command::LPush { key, .. }
            | Command::RPush { key, .. }
            | Command::LPop { key, .. }
            | Command::RPop { key, .. }
            | Command::LRange { key, .. }
            | Command::LLen { key }
            | Command::LIndex { key, .. }
            | Command::LSet { key, .. }
            | Command::LTrim { key, .. }
            | Command::LInsert { key, .. }
            | Command::LRem { key, .. }
            | Command::LPos { key, .. }
            | Command::ZAdd { key, .. }
            | Command::ZRem { key, .. }
            | Command::ZScore { key, .. }
            | Command::ZRank { key, .. }
            | Command::ZRange { key, .. }
            | Command::ZCard { key }
            | Command::HSet { key, .. }
            | Command::HGet { key, .. }
            | Command::HGetAll { key }
            | Command::HDel { key, .. }
            | Command::HExists { key, .. }
            | Command::HLen { key }
            | Command::HIncrBy { key, .. }
            | Command::HKeys { key }
            | Command::HVals { key }
            | Command::HMGet { key, .. }
            | Command::HRandField { key, .. }
            | Command::ZRandMember { key, .. }
            | Command::SAdd { key, .. }
            | Command::SRem { key, .. }
            | Command::SMembers { key }
            | Command::SIsMember { key, .. }
            | Command::SCard { key }
            | Command::SScan { key, .. }
            | Command::SRandMember { key, .. }
            | Command::SPop { key, .. }
            | Command::SMisMember { key, .. }
            | Command::HScan { key, .. }
            | Command::ZScan { key, .. }
            | Command::VAdd { key, .. }
            | Command::VAddBatch { key, .. }
            | Command::VSim { key, .. }
            | Command::VRem { key, .. }
            | Command::VGet { key, .. }
            | Command::VCard { key }
            | Command::VDim { key }
            | Command::VInfo { key }
            | Command::ProtoSet { key, .. }
            | Command::ProtoGet { key }
            | Command::ProtoType { key }
            | Command::ProtoGetField { key, .. }
            | Command::ProtoSetField { key, .. }
            | Command::ProtoDelField { key, .. }
            | Command::Restore { key, .. }
            | Command::Sort { key, .. }
            | Command::GetDel { key }
            | Command::GetEx { key, .. } => Some(key),
            Command::LMove { source, .. } => Some(source),
            Command::Copy { source, .. } => Some(source),
            Command::SMove { source, .. } => Some(source),
            Command::Del { keys }
            | Command::Unlink { keys }
            | Command::Exists { keys }
            | Command::Touch { keys }
            | Command::MGet { keys }
            | Command::BLPop { keys, .. }
            | Command::BRPop { keys, .. }
            | Command::SUnion { keys }
            | Command::SInter { keys }
            | Command::SDiff { keys }
            | Command::SInterCard { keys, .. }
            | Command::ZDiff { keys, .. }
            | Command::ZInter { keys, .. }
            | Command::ZUnion { keys, .. }
            | Command::Lmpop { keys, .. }
            | Command::Zmpop { keys, .. } => keys.first().map(String::as_str),
            Command::SUnionStore { dest, .. }
            | Command::SInterStore { dest, .. }
            | Command::SDiffStore { dest, .. } => Some(dest),
            Command::MSet { pairs } | Command::MSetNx { pairs } => {
                pairs.first().map(|(k, _)| k.as_str())
            }
            _ => None,
        }
    }
}
