//! Command parsing from RESP3 frames.
//!
//! Converts a parsed [`Frame`] (expected to be an array) into a typed
//! [`Command`] enum. This keeps protocol-level concerns separate from
//! the engine that actually executes commands.

use bytes::Bytes;

use crate::error::ProtocolError;
use crate::types::Frame;

/// Maximum number of dimensions in a vector. 65,536 is generous for any
/// real-world embedding model (OpenAI: 1536, Cohere: 4096) while preventing
/// memory abuse from absurdly large vectors.
const MAX_VECTOR_DIMS: usize = 65_536;

/// Maximum value for HNSW connectivity (M) and expansion parameters.
/// Values above 1024 give no practical benefit and waste memory.
const MAX_HNSW_PARAM: u64 = 1024;

/// Expiration option for the SET command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetExpire {
    /// EX seconds — expire after N seconds.
    Ex(u64),
    /// PX milliseconds — expire after N milliseconds.
    Px(u64),
}

/// A parsed client command, ready for execution.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// PING with an optional message. Returns PONG or echoes the message.
    Ping(Option<Bytes>),

    /// ECHO `message`. Returns the message back to the client.
    Echo(Bytes),

    /// GET `key`. Returns the value or nil.
    Get { key: String },

    /// SET `key` `value` \[EX seconds | PX milliseconds\] \[NX | XX\].
    Set {
        key: String,
        value: Bytes,
        expire: Option<SetExpire>,
        /// Only set the key if it does not already exist.
        nx: bool,
        /// Only set the key if it already exists.
        xx: bool,
    },

    /// INCR `key`. Increments the integer value of a key by 1.
    Incr { key: String },

    /// DECR `key`. Decrements the integer value of a key by 1.
    Decr { key: String },

    /// INCRBY `key` `increment`. Increments the integer value of a key by the given amount.
    IncrBy { key: String, delta: i64 },

    /// DECRBY `key` `decrement`. Decrements the integer value of a key by the given amount.
    DecrBy { key: String, delta: i64 },

    /// INCRBYFLOAT `key` `increment`. Increments the float value of a key by the given amount.
    IncrByFloat { key: String, delta: f64 },

    /// APPEND `key` `value`. Appends a value to a string key. Returns the new length.
    Append { key: String, value: Bytes },

    /// STRLEN `key`. Returns the length of the string value stored at key.
    Strlen { key: String },

    /// KEYS `pattern`. Returns all keys matching a glob pattern.
    Keys { pattern: String },

    /// RENAME `key` `newkey`. Renames a key.
    Rename { key: String, newkey: String },

    /// DEL `key` \[key ...\]. Returns the number of keys removed.
    Del { keys: Vec<String> },

    /// UNLINK `key` \[key ...\]. Like DEL but frees memory in the background.
    Unlink { keys: Vec<String> },

    /// EXISTS `key` \[key ...\]. Returns the number of keys that exist.
    Exists { keys: Vec<String> },

    /// MGET `key` \[key ...\]. Returns the values for all specified keys.
    MGet { keys: Vec<String> },

    /// MSET `key` `value` \[key value ...\]. Sets multiple key-value pairs.
    MSet { pairs: Vec<(String, Bytes)> },

    /// EXPIRE `key` `seconds`. Sets a TTL on an existing key.
    Expire { key: String, seconds: u64 },

    /// TTL `key`. Returns remaining time-to-live in seconds.
    Ttl { key: String },

    /// PERSIST `key`. Removes the expiration from a key.
    Persist { key: String },

    /// PTTL `key`. Returns remaining time-to-live in milliseconds.
    Pttl { key: String },

    /// PEXPIRE `key` `milliseconds`. Sets a TTL in milliseconds on an existing key.
    Pexpire { key: String, milliseconds: u64 },

    /// DBSIZE. Returns the number of keys in the database.
    DbSize,

    /// INFO \[section\]. Returns server info. Currently only supports "keyspace".
    Info { section: Option<String> },

    /// BGSAVE. Triggers a background snapshot.
    BgSave,

    /// BGREWRITEAOF. Triggers an AOF rewrite (snapshot + truncate).
    BgRewriteAof,

    /// FLUSHDB \[ASYNC\]. Removes all keys from the database.
    FlushDb { async_mode: bool },

    /// SCAN `cursor` \[MATCH pattern\] \[COUNT count\]. Iterates keys.
    Scan {
        cursor: u64,
        pattern: Option<String>,
        count: Option<usize>,
    },

    /// LPUSH `key` `value` \[value ...\]. Pushes values to the head of a list.
    LPush { key: String, values: Vec<Bytes> },

    /// RPUSH `key` `value` \[value ...\]. Pushes values to the tail of a list.
    RPush { key: String, values: Vec<Bytes> },

    /// LPOP `key`. Pops a value from the head of a list.
    LPop { key: String },

    /// RPOP `key`. Pops a value from the tail of a list.
    RPop { key: String },

    /// LRANGE `key` `start` `stop`. Returns a range of elements by index.
    LRange { key: String, start: i64, stop: i64 },

    /// LLEN `key`. Returns the length of a list.
    LLen { key: String },

    /// TYPE `key`. Returns the type of the value stored at key.
    Type { key: String },

    /// ZADD `key` \[NX|XX\] \[GT|LT\] \[CH\] `score` `member` \[score member ...\].
    ZAdd {
        key: String,
        flags: ZAddFlags,
        members: Vec<(f64, String)>,
    },

    /// ZREM `key` `member` \[member ...\]. Removes members from a sorted set.
    ZRem { key: String, members: Vec<String> },

    /// ZSCORE `key` `member`. Returns the score of a member.
    ZScore { key: String, member: String },

    /// ZRANK `key` `member`. Returns the rank of a member (0-based).
    ZRank { key: String, member: String },

    /// ZCARD `key`. Returns the cardinality (number of members) of a sorted set.
    ZCard { key: String },

    /// ZRANGE `key` `start` `stop` \[WITHSCORES\]. Returns a range by rank.
    ZRange {
        key: String,
        start: i64,
        stop: i64,
        with_scores: bool,
    },

    /// HSET `key` `field` `value` \[field value ...\]. Sets field-value pairs in a hash.
    HSet {
        key: String,
        fields: Vec<(String, Bytes)>,
    },

    /// HGET `key` `field`. Gets a field's value from a hash.
    HGet { key: String, field: String },

    /// HGETALL `key`. Gets all field-value pairs from a hash.
    HGetAll { key: String },

    /// HDEL `key` `field` \[field ...\]. Deletes fields from a hash.
    HDel { key: String, fields: Vec<String> },

    /// HEXISTS `key` `field`. Checks if a field exists in a hash.
    HExists { key: String, field: String },

    /// HLEN `key`. Returns the number of fields in a hash.
    HLen { key: String },

    /// HINCRBY `key` `field` `increment`. Increments a hash field's integer value.
    HIncrBy {
        key: String,
        field: String,
        delta: i64,
    },

    /// HKEYS `key`. Returns all field names in a hash.
    HKeys { key: String },

    /// HVALS `key`. Returns all values in a hash.
    HVals { key: String },

    /// HMGET `key` `field` \[field ...\]. Gets multiple field values from a hash.
    HMGet { key: String, fields: Vec<String> },

    /// SADD `key` `member` \[member ...\]. Adds members to a set.
    SAdd { key: String, members: Vec<String> },

    /// SREM `key` `member` \[member ...\]. Removes members from a set.
    SRem { key: String, members: Vec<String> },

    /// SMEMBERS `key`. Returns all members of a set.
    SMembers { key: String },

    /// SISMEMBER `key` `member`. Checks if a member exists in a set.
    SIsMember { key: String, member: String },

    /// SCARD `key`. Returns the cardinality (number of members) of a set.
    SCard { key: String },

    // --- cluster commands ---
    /// CLUSTER INFO. Returns cluster state and configuration information.
    ClusterInfo,

    /// CLUSTER NODES. Returns the list of cluster nodes.
    ClusterNodes,

    /// CLUSTER SLOTS. Returns the slot distribution across nodes.
    ClusterSlots,

    /// CLUSTER KEYSLOT `key`. Returns the hash slot for a key.
    ClusterKeySlot { key: String },

    /// CLUSTER MYID. Returns the node's ID.
    ClusterMyId,

    /// CLUSTER SETSLOT `slot` IMPORTING `node-id`. Mark slot as importing from node.
    ClusterSetSlotImporting { slot: u16, node_id: String },

    /// CLUSTER SETSLOT `slot` MIGRATING `node-id`. Mark slot as migrating to node.
    ClusterSetSlotMigrating { slot: u16, node_id: String },

    /// CLUSTER SETSLOT `slot` NODE `node-id`. Assign slot to node.
    ClusterSetSlotNode { slot: u16, node_id: String },

    /// CLUSTER SETSLOT `slot` STABLE. Clear importing/migrating state.
    ClusterSetSlotStable { slot: u16 },

    /// CLUSTER MEET `ip` `port`. Add a node to the cluster.
    ClusterMeet { ip: String, port: u16 },

    /// CLUSTER ADDSLOTS `slot` \[slot...\]. Assign slots to the local node.
    ClusterAddSlots { slots: Vec<u16> },

    /// CLUSTER DELSLOTS `slot` \[slot...\]. Remove slots from the local node.
    ClusterDelSlots { slots: Vec<u16> },

    /// CLUSTER FORGET `node-id`. Remove a node from the cluster.
    ClusterForget { node_id: String },

    /// CLUSTER REPLICATE `node-id`. Make this node a replica of another.
    ClusterReplicate { node_id: String },

    /// CLUSTER FAILOVER [FORCE|TAKEOVER]. Trigger a manual failover.
    ClusterFailover { force: bool, takeover: bool },

    /// CLUSTER COUNTKEYSINSLOT `slot`. Return the number of keys in a slot.
    ClusterCountKeysInSlot { slot: u16 },

    /// CLUSTER GETKEYSINSLOT `slot` `count`. Return keys in a slot.
    ClusterGetKeysInSlot { slot: u16, count: u32 },

    /// MIGRATE `host` `port` `key` `db` `timeout` \[COPY\] \[REPLACE\] \[KEYS key...\].
    /// Migrate a key to another node.
    Migrate {
        host: String,
        port: u16,
        key: String,
        db: u32,
        timeout_ms: u64,
        copy: bool,
        replace: bool,
    },

    /// ASKING. Signals that the next command is for a migrating slot.
    Asking,

    /// SLOWLOG GET [count]. Returns recent slow log entries.
    SlowLogGet { count: Option<usize> },

    /// SLOWLOG LEN. Returns the number of entries in the slow log.
    SlowLogLen,

    /// SLOWLOG RESET. Clears the slow log.
    SlowLogReset,

    // --- pub/sub commands ---
    /// SUBSCRIBE `channel` \[channel ...\]. Subscribe to one or more channels.
    Subscribe { channels: Vec<String> },

    /// UNSUBSCRIBE \[channel ...\]. Unsubscribe from channels (all if none given).
    Unsubscribe { channels: Vec<String> },

    /// PSUBSCRIBE `pattern` \[pattern ...\]. Subscribe to channels matching patterns.
    PSubscribe { patterns: Vec<String> },

    /// PUNSUBSCRIBE \[pattern ...\]. Unsubscribe from patterns (all if none given).
    PUnsubscribe { patterns: Vec<String> },

    /// PUBLISH `channel` `message`. Publish a message to a channel.
    Publish { channel: String, message: Bytes },

    /// PUBSUB CHANNELS \[pattern\]. List active channels, optionally matching a glob.
    PubSubChannels { pattern: Option<String> },

    /// PUBSUB NUMSUB \[channel ...\]. Returns subscriber counts for given channels.
    PubSubNumSub { channels: Vec<String> },

    /// PUBSUB NUMPAT. Returns the number of active pattern subscriptions.
    PubSubNumPat,

    // --- vector commands ---
    /// VADD key element f32 [f32 ...] [METRIC COSINE|L2|IP] [QUANT F32|F16|I8]
    /// [M n] [EF n]. Adds a vector to a vector set.
    VAdd {
        key: String,
        element: String,
        vector: Vec<f32>,
        /// 0 = cosine (default), 1 = l2, 2 = inner product
        metric: u8,
        /// 0 = f32 (default), 1 = f16, 2 = i8
        quantization: u8,
        /// HNSW connectivity parameter (default 16)
        connectivity: u32,
        /// HNSW construction beam width (default 64)
        expansion_add: u32,
    },

    /// VSIM key f32 [f32 ...] COUNT k [EF n] [WITHSCORES].
    /// Searches for k nearest neighbors.
    VSim {
        key: String,
        query: Vec<f32>,
        count: usize,
        ef_search: usize,
        with_scores: bool,
    },

    /// VREM key element. Removes a vector from a vector set.
    VRem { key: String, element: String },

    /// VGET key element. Retrieves the stored vector for an element.
    VGet { key: String, element: String },

    /// VCARD key. Returns the number of elements in a vector set.
    VCard { key: String },

    /// VDIM key. Returns the dimensionality of a vector set.
    VDim { key: String },

    /// VINFO key. Returns metadata about a vector set.
    VInfo { key: String },

    // --- protobuf commands ---
    /// PROTO.REGISTER `name` `descriptor_bytes`. Registers a protobuf schema
    /// (pre-compiled FileDescriptorSet) under the given name.
    ProtoRegister { name: String, descriptor: Bytes },

    /// PROTO.SET `key` `type_name` `data` \[EX s | PX ms\] \[NX | XX\].
    /// Stores a validated protobuf value.
    ProtoSet {
        key: String,
        type_name: String,
        data: Bytes,
        expire: Option<SetExpire>,
        /// Only set the key if it does not already exist.
        nx: bool,
        /// Only set the key if it already exists.
        xx: bool,
    },

    /// PROTO.GET `key`. Returns \[type_name, data\] or nil.
    ProtoGet { key: String },

    /// PROTO.TYPE `key`. Returns the message type name or nil.
    ProtoType { key: String },

    /// PROTO.SCHEMAS. Lists all registered schema names.
    ProtoSchemas,

    /// PROTO.DESCRIBE `name`. Lists message types in a registered schema.
    ProtoDescribe { name: String },

    /// PROTO.GETFIELD `key` `field_path`. Reads a single field from a
    /// protobuf value, returning it as a native RESP3 type.
    ProtoGetField { key: String, field_path: String },

    /// PROTO.SETFIELD `key` `field_path` `value`. Updates a single scalar
    /// field in a stored protobuf value.
    ProtoSetField {
        key: String,
        field_path: String,
        value: String,
    },

    /// PROTO.DELFIELD `key` `field_path`. Clears a field to its default value.
    ProtoDelField { key: String, field_path: String },

    /// AUTH \[username\] password. Authenticate the connection.
    Auth {
        /// Username for ACL-style auth. None for legacy AUTH.
        username: Option<String>,
        /// The password to validate.
        password: String,
    },

    /// QUIT. Requests the server to close the connection.
    Quit,

    /// A command we don't recognize (yet).
    Unknown(String),
}

/// Flags for the ZADD command.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ZAddFlags {
    /// Only add new members, don't update existing scores.
    pub nx: bool,
    /// Only update existing members, don't add new ones.
    pub xx: bool,
    /// Only update when new score > current score.
    pub gt: bool,
    /// Only update when new score < current score.
    pub lt: bool,
    /// Return count of changed members (added + updated) instead of just added.
    pub ch: bool,
}

impl Eq for ZAddFlags {}

impl Command {
    /// Returns the lowercase command name as a static string.
    ///
    /// Used for metrics labels and slow log entries. Zero allocation —
    /// returns a `&'static str` for every known variant.
    pub fn command_name(&self) -> &'static str {
        match self {
            Command::Ping(_) => "ping",
            Command::Echo(_) => "echo",
            Command::Get { .. } => "get",
            Command::Set { .. } => "set",
            Command::Incr { .. } => "incr",
            Command::Decr { .. } => "decr",
            Command::IncrBy { .. } => "incrby",
            Command::DecrBy { .. } => "decrby",
            Command::IncrByFloat { .. } => "incrbyfloat",
            Command::Append { .. } => "append",
            Command::Strlen { .. } => "strlen",
            Command::Keys { .. } => "keys",
            Command::Rename { .. } => "rename",
            Command::Del { .. } => "del",
            Command::Unlink { .. } => "unlink",
            Command::Exists { .. } => "exists",
            Command::MGet { .. } => "mget",
            Command::MSet { .. } => "mset",
            Command::Expire { .. } => "expire",
            Command::Ttl { .. } => "ttl",
            Command::Persist { .. } => "persist",
            Command::Pttl { .. } => "pttl",
            Command::Pexpire { .. } => "pexpire",
            Command::DbSize => "dbsize",
            Command::Info { .. } => "info",
            Command::BgSave => "bgsave",
            Command::BgRewriteAof => "bgrewriteaof",
            Command::FlushDb { .. } => "flushdb",
            Command::Scan { .. } => "scan",
            Command::LPush { .. } => "lpush",
            Command::RPush { .. } => "rpush",
            Command::LPop { .. } => "lpop",
            Command::RPop { .. } => "rpop",
            Command::LRange { .. } => "lrange",
            Command::LLen { .. } => "llen",
            Command::Type { .. } => "type",
            Command::ZAdd { .. } => "zadd",
            Command::ZRem { .. } => "zrem",
            Command::ZScore { .. } => "zscore",
            Command::ZRank { .. } => "zrank",
            Command::ZCard { .. } => "zcard",
            Command::ZRange { .. } => "zrange",
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
            Command::SAdd { .. } => "sadd",
            Command::SRem { .. } => "srem",
            Command::SMembers { .. } => "smembers",
            Command::SIsMember { .. } => "sismember",
            Command::SCard { .. } => "scard",
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
            Command::ClusterDelSlots { .. } => "cluster_delslots",
            Command::ClusterForget { .. } => "cluster_forget",
            Command::ClusterReplicate { .. } => "cluster_replicate",
            Command::ClusterFailover { .. } => "cluster_failover",
            Command::ClusterCountKeysInSlot { .. } => "cluster_countkeysinslot",
            Command::ClusterGetKeysInSlot { .. } => "cluster_getkeysinslot",
            Command::Migrate { .. } => "migrate",
            Command::Asking => "asking",
            Command::SlowLogGet { .. } => "slowlog",
            Command::SlowLogLen => "slowlog",
            Command::SlowLogReset => "slowlog",
            Command::Subscribe { .. } => "subscribe",
            Command::Unsubscribe { .. } => "unsubscribe",
            Command::PSubscribe { .. } => "psubscribe",
            Command::PUnsubscribe { .. } => "punsubscribe",
            Command::Publish { .. } => "publish",
            Command::PubSubChannels { .. } => "pubsub",
            Command::PubSubNumSub { .. } => "pubsub",
            Command::PubSubNumPat => "pubsub",
            Command::VAdd { .. } => "vadd",
            Command::VSim { .. } => "vsim",
            Command::VRem { .. } => "vrem",
            Command::VGet { .. } => "vget",
            Command::VCard { .. } => "vcard",
            Command::VDim { .. } => "vdim",
            Command::VInfo { .. } => "vinfo",
            Command::ProtoRegister { .. } => "proto.register",
            Command::ProtoSet { .. } => "proto.set",
            Command::ProtoGet { .. } => "proto.get",
            Command::ProtoType { .. } => "proto.type",
            Command::ProtoSchemas => "proto.schemas",
            Command::ProtoDescribe { .. } => "proto.describe",
            Command::ProtoGetField { .. } => "proto.getfield",
            Command::ProtoSetField { .. } => "proto.setfield",
            Command::ProtoDelField { .. } => "proto.delfield",
            Command::Auth { .. } => "auth",
            Command::Quit => "quit",
            Command::Unknown(_) => "unknown",
        }
    }

    /// Parses a [`Frame`] into a [`Command`].
    ///
    /// Expects an array frame where the first element is the command name
    /// (as a bulk or simple string) and the rest are arguments.
    pub fn from_frame(frame: Frame) -> Result<Command, ProtocolError> {
        let frames = match frame {
            Frame::Array(frames) => frames,
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(
                    "expected array frame".into(),
                ));
            }
        };

        if frames.is_empty() {
            return Err(ProtocolError::InvalidCommandFrame(
                "empty command array".into(),
            ));
        }

        let name = extract_string(&frames[0])?;
        let name_upper = name.to_ascii_uppercase();

        match name_upper.as_str() {
            "PING" => parse_ping(&frames[1..]),
            "ECHO" => parse_echo(&frames[1..]),
            "GET" => parse_get(&frames[1..]),
            "SET" => parse_set(&frames[1..]),
            "INCR" => parse_incr(&frames[1..]),
            "DECR" => parse_decr(&frames[1..]),
            "INCRBY" => parse_incrby(&frames[1..]),
            "DECRBY" => parse_decrby(&frames[1..]),
            "INCRBYFLOAT" => parse_incrbyfloat(&frames[1..]),
            "APPEND" => parse_append(&frames[1..]),
            "STRLEN" => parse_strlen(&frames[1..]),
            "KEYS" => parse_keys(&frames[1..]),
            "RENAME" => parse_rename(&frames[1..]),
            "DEL" => parse_del(&frames[1..]),
            "UNLINK" => parse_unlink(&frames[1..]),
            "EXISTS" => parse_exists(&frames[1..]),
            "MGET" => parse_mget(&frames[1..]),
            "MSET" => parse_mset(&frames[1..]),
            "EXPIRE" => parse_expire(&frames[1..]),
            "TTL" => parse_ttl(&frames[1..]),
            "PERSIST" => parse_persist(&frames[1..]),
            "PTTL" => parse_pttl(&frames[1..]),
            "PEXPIRE" => parse_pexpire(&frames[1..]),
            "DBSIZE" => parse_dbsize(&frames[1..]),
            "INFO" => parse_info(&frames[1..]),
            "BGSAVE" => parse_bgsave(&frames[1..]),
            "BGREWRITEAOF" => parse_bgrewriteaof(&frames[1..]),
            "FLUSHDB" => parse_flushdb(&frames[1..]),
            "SCAN" => parse_scan(&frames[1..]),
            "LPUSH" => parse_lpush(&frames[1..]),
            "RPUSH" => parse_rpush(&frames[1..]),
            "LPOP" => parse_lpop(&frames[1..]),
            "RPOP" => parse_rpop(&frames[1..]),
            "LRANGE" => parse_lrange(&frames[1..]),
            "LLEN" => parse_llen(&frames[1..]),
            "TYPE" => parse_type(&frames[1..]),
            "ZADD" => parse_zadd(&frames[1..]),
            "ZREM" => parse_zrem(&frames[1..]),
            "ZSCORE" => parse_zscore(&frames[1..]),
            "ZRANK" => parse_zrank(&frames[1..]),
            "ZCARD" => parse_zcard(&frames[1..]),
            "ZRANGE" => parse_zrange(&frames[1..]),
            "HSET" => parse_hset(&frames[1..]),
            "HGET" => parse_hget(&frames[1..]),
            "HGETALL" => parse_hgetall(&frames[1..]),
            "HDEL" => parse_hdel(&frames[1..]),
            "HEXISTS" => parse_hexists(&frames[1..]),
            "HLEN" => parse_hlen(&frames[1..]),
            "HINCRBY" => parse_hincrby(&frames[1..]),
            "HKEYS" => parse_hkeys(&frames[1..]),
            "HVALS" => parse_hvals(&frames[1..]),
            "HMGET" => parse_hmget(&frames[1..]),
            "SADD" => parse_sadd(&frames[1..]),
            "SREM" => parse_srem(&frames[1..]),
            "SMEMBERS" => parse_smembers(&frames[1..]),
            "SISMEMBER" => parse_sismember(&frames[1..]),
            "SCARD" => parse_scard(&frames[1..]),
            "CLUSTER" => parse_cluster(&frames[1..]),
            "ASKING" => parse_asking(&frames[1..]),
            "MIGRATE" => parse_migrate(&frames[1..]),
            "SLOWLOG" => parse_slowlog(&frames[1..]),
            "SUBSCRIBE" => parse_subscribe(&frames[1..]),
            "UNSUBSCRIBE" => parse_unsubscribe(&frames[1..]),
            "PSUBSCRIBE" => parse_psubscribe(&frames[1..]),
            "PUNSUBSCRIBE" => parse_punsubscribe(&frames[1..]),
            "PUBLISH" => parse_publish(&frames[1..]),
            "PUBSUB" => parse_pubsub(&frames[1..]),
            "VADD" => parse_vadd(&frames[1..]),
            "VSIM" => parse_vsim(&frames[1..]),
            "VREM" => parse_vrem(&frames[1..]),
            "VGET" => parse_vget(&frames[1..]),
            "VCARD" => parse_vcard(&frames[1..]),
            "VDIM" => parse_vdim(&frames[1..]),
            "VINFO" => parse_vinfo(&frames[1..]),
            "PROTO.REGISTER" => parse_proto_register(&frames[1..]),
            "PROTO.SET" => parse_proto_set(&frames[1..]),
            "PROTO.GET" => parse_proto_get(&frames[1..]),
            "PROTO.TYPE" => parse_proto_type(&frames[1..]),
            "PROTO.SCHEMAS" => parse_proto_schemas(&frames[1..]),
            "PROTO.DESCRIBE" => parse_proto_describe(&frames[1..]),
            "PROTO.GETFIELD" => parse_proto_getfield(&frames[1..]),
            "PROTO.SETFIELD" => parse_proto_setfield(&frames[1..]),
            "PROTO.DELFIELD" => parse_proto_delfield(&frames[1..]),
            "AUTH" => parse_auth(&frames[1..]),
            "QUIT" => parse_quit(&frames[1..]),
            _ => Ok(Command::Unknown(name)),
        }
    }
}

/// Extracts a UTF-8 string from a Bulk or Simple frame.
///
/// Validates UTF-8 in-place on the Bytes buffer to avoid an
/// intermediate Vec<u8> allocation from `to_vec()`.
fn extract_string(frame: &Frame) -> Result<String, ProtocolError> {
    match frame {
        Frame::Bulk(data) => {
            let s = std::str::from_utf8(data).map_err(|_| {
                ProtocolError::InvalidCommandFrame("command name is not valid utf-8".into())
            })?;
            Ok(s.to_owned())
        }
        Frame::Simple(s) => Ok(s.clone()),
        _ => Err(ProtocolError::InvalidCommandFrame(
            "expected bulk or simple string for command name".into(),
        )),
    }
}

/// Extracts raw bytes from a Bulk or Simple frame.
fn extract_bytes(frame: &Frame) -> Result<Bytes, ProtocolError> {
    match frame {
        Frame::Bulk(data) => Ok(data.clone()),
        Frame::Simple(s) => Ok(Bytes::from(s.clone().into_bytes())),
        _ => Err(ProtocolError::InvalidCommandFrame(
            "expected bulk or simple string argument".into(),
        )),
    }
}

/// Extracts all frames in a slice as UTF-8 strings.
fn extract_strings(frames: &[Frame]) -> Result<Vec<String>, ProtocolError> {
    frames.iter().map(extract_string).collect()
}

/// Extracts all frames in a slice as raw byte buffers.
fn extract_bytes_vec(frames: &[Frame]) -> Result<Vec<Bytes>, ProtocolError> {
    frames.iter().map(extract_bytes).collect()
}

/// Parses a string argument as a positive u64.
fn parse_u64(frame: &Frame, cmd: &str) -> Result<u64, ProtocolError> {
    let s = extract_string(frame)?;
    s.parse::<u64>().map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!("value is not a valid integer for '{cmd}'"))
    })
}

fn parse_ping(args: &[Frame]) -> Result<Command, ProtocolError> {
    match args.len() {
        0 => Ok(Command::Ping(None)),
        1 => {
            let msg = extract_bytes(&args[0])?;
            Ok(Command::Ping(Some(msg)))
        }
        _ => Err(ProtocolError::WrongArity("PING".into())),
    }
}

fn parse_echo(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("ECHO".into()));
    }
    let msg = extract_bytes(&args[0])?;
    Ok(Command::Echo(msg))
}

fn parse_get(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("GET".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Get { key })
}

fn parse_set(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("SET".into()));
    }

    let key = extract_string(&args[0])?;
    let value = extract_bytes(&args[1])?;

    let mut expire = None;
    let mut nx = false;
    let mut xx = false;
    let mut idx = 2;

    while idx < args.len() {
        let flag = extract_string(&args[idx])?.to_ascii_uppercase();
        match flag.as_str() {
            "NX" => {
                nx = true;
                idx += 1;
            }
            "XX" => {
                xx = true;
                idx += 1;
            }
            "EX" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("SET".into()));
                }
                let amount = parse_u64(&args[idx], "SET")?;
                if amount == 0 {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "invalid expire time in 'SET' command".into(),
                    ));
                }
                expire = Some(SetExpire::Ex(amount));
                idx += 1;
            }
            "PX" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("SET".into()));
                }
                let amount = parse_u64(&args[idx], "SET")?;
                if amount == 0 {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "invalid expire time in 'SET' command".into(),
                    ));
                }
                expire = Some(SetExpire::Px(amount));
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported SET option '{flag}'"
                )));
            }
        }
    }

    if nx && xx {
        return Err(ProtocolError::InvalidCommandFrame(
            "XX and NX options at the same time are not compatible".into(),
        ));
    }

    Ok(Command::Set {
        key,
        value,
        expire,
        nx,
        xx,
    })
}

fn parse_incr(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("INCR".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Incr { key })
}

fn parse_decr(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("DECR".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Decr { key })
}

fn parse_incrby(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("INCRBY".into()));
    }
    let key = extract_string(&args[0])?;
    let delta = parse_i64(&args[1], "INCRBY")?;
    Ok(Command::IncrBy { key, delta })
}

fn parse_decrby(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("DECRBY".into()));
    }
    let key = extract_string(&args[0])?;
    let delta = parse_i64(&args[1], "DECRBY")?;
    Ok(Command::DecrBy { key, delta })
}

fn parse_incrbyfloat(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("INCRBYFLOAT".into()));
    }
    let key = extract_string(&args[0])?;
    let s = extract_string(&args[1])?;
    let delta: f64 = s.parse().map_err(|_| {
        ProtocolError::InvalidCommandFrame("value is not a valid float for 'INCRBYFLOAT'".into())
    })?;
    if delta.is_nan() || delta.is_infinite() {
        return Err(ProtocolError::InvalidCommandFrame(
            "increment would produce NaN or Infinity".into(),
        ));
    }
    Ok(Command::IncrByFloat { key, delta })
}

fn parse_append(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("APPEND".into()));
    }
    let key = extract_string(&args[0])?;
    let value = extract_bytes(&args[1])?;
    Ok(Command::Append { key, value })
}

fn parse_strlen(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("STRLEN".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Strlen { key })
}

fn parse_keys(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("KEYS".into()));
    }
    let pattern = extract_string(&args[0])?;
    Ok(Command::Keys { pattern })
}

fn parse_rename(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("RENAME".into()));
    }
    let key = extract_string(&args[0])?;
    let newkey = extract_string(&args[1])?;
    Ok(Command::Rename { key, newkey })
}

fn parse_del(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("DEL".into()));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Del { keys })
}

fn parse_exists(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("EXISTS".into()));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Exists { keys })
}

fn parse_mget(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("MGET".into()));
    }
    let keys = extract_strings(args)?;
    Ok(Command::MGet { keys })
}

fn parse_mset(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err(ProtocolError::WrongArity("MSET".into()));
    }
    let mut pairs = Vec::with_capacity(args.len() / 2);
    for chunk in args.chunks(2) {
        let key = extract_string(&chunk[0])?;
        let value = extract_bytes(&chunk[1])?;
        pairs.push((key, value));
    }
    Ok(Command::MSet { pairs })
}

fn parse_expire(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("EXPIRE".into()));
    }
    let key = extract_string(&args[0])?;
    let seconds = parse_u64(&args[1], "EXPIRE")?;

    if seconds == 0 {
        return Err(ProtocolError::InvalidCommandFrame(
            "invalid expire time in 'EXPIRE' command".into(),
        ));
    }

    Ok(Command::Expire { key, seconds })
}

fn parse_ttl(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("TTL".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Ttl { key })
}

fn parse_persist(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("PERSIST".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Persist { key })
}

fn parse_pttl(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("PTTL".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Pttl { key })
}

fn parse_pexpire(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("PEXPIRE".into()));
    }
    let key = extract_string(&args[0])?;
    let milliseconds = parse_u64(&args[1], "PEXPIRE")?;

    if milliseconds == 0 {
        return Err(ProtocolError::InvalidCommandFrame(
            "invalid expire time in 'PEXPIRE' command".into(),
        ));
    }

    Ok(Command::Pexpire { key, milliseconds })
}

fn parse_dbsize(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(ProtocolError::WrongArity("DBSIZE".into()));
    }
    Ok(Command::DbSize)
}

fn parse_info(args: &[Frame]) -> Result<Command, ProtocolError> {
    match args.len() {
        0 => Ok(Command::Info { section: None }),
        1 => {
            let section = extract_string(&args[0])?;
            Ok(Command::Info {
                section: Some(section),
            })
        }
        _ => Err(ProtocolError::WrongArity("INFO".into())),
    }
}

fn parse_bgsave(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(ProtocolError::WrongArity("BGSAVE".into()));
    }
    Ok(Command::BgSave)
}

fn parse_bgrewriteaof(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(ProtocolError::WrongArity("BGREWRITEAOF".into()));
    }
    Ok(Command::BgRewriteAof)
}

fn parse_flushdb(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Ok(Command::FlushDb { async_mode: false });
    }
    if args.len() == 1 {
        let arg = extract_string(&args[0])?;
        if arg.eq_ignore_ascii_case("ASYNC") {
            return Ok(Command::FlushDb { async_mode: true });
        }
    }
    Err(ProtocolError::WrongArity("FLUSHDB".into()))
}

fn parse_unlink(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("UNLINK".into()));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Unlink { keys })
}

fn parse_scan(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("SCAN".into()));
    }

    let cursor = parse_u64(&args[0], "SCAN")?;
    let mut pattern = None;
    let mut count = None;
    let mut idx = 1;

    while idx < args.len() {
        let flag = extract_string(&args[idx])?.to_ascii_uppercase();
        match flag.as_str() {
            "MATCH" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("SCAN".into()));
                }
                pattern = Some(extract_string(&args[idx])?);
                idx += 1;
            }
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("SCAN".into()));
                }
                let n = parse_u64(&args[idx], "SCAN")?;
                count = Some(n as usize);
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported SCAN option '{flag}'"
                )));
            }
        }
    }

    Ok(Command::Scan {
        cursor,
        pattern,
        count,
    })
}

/// Parses a string argument as an i64. Used by LRANGE for start/stop indices.
fn parse_i64(frame: &Frame, cmd: &str) -> Result<i64, ProtocolError> {
    let s = extract_string(frame)?;
    s.parse::<i64>().map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!("value is not a valid integer for '{cmd}'"))
    })
}

fn parse_lpush(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("LPUSH".into()));
    }
    let key = extract_string(&args[0])?;
    let values = extract_bytes_vec(&args[1..])?;
    Ok(Command::LPush { key, values })
}

fn parse_rpush(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("RPUSH".into()));
    }
    let key = extract_string(&args[0])?;
    let values = extract_bytes_vec(&args[1..])?;
    Ok(Command::RPush { key, values })
}

fn parse_lpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("LPOP".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::LPop { key })
}

fn parse_rpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("RPOP".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::RPop { key })
}

fn parse_lrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::WrongArity("LRANGE".into()));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "LRANGE")?;
    let stop = parse_i64(&args[2], "LRANGE")?;
    Ok(Command::LRange { key, start, stop })
}

fn parse_llen(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("LLEN".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::LLen { key })
}

fn parse_type(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("TYPE".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Type { key })
}

/// Parses a string argument as an f64 score.
fn parse_f64(frame: &Frame, cmd: &str) -> Result<f64, ProtocolError> {
    let s = extract_string(frame)?;
    let v = s.parse::<f64>().map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!("value is not a valid float for '{cmd}'"))
    })?;
    if v.is_nan() {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "NaN is not a valid score for '{cmd}'"
        )));
    }
    Ok(v)
}

fn parse_zadd(args: &[Frame]) -> Result<Command, ProtocolError> {
    // ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
    if args.len() < 3 {
        return Err(ProtocolError::WrongArity("ZADD".into()));
    }

    let key = extract_string(&args[0])?;
    let mut flags = ZAddFlags::default();
    let mut idx = 1;

    // parse optional flags before score/member pairs
    while idx < args.len() {
        let s = extract_string(&args[idx])?.to_ascii_uppercase();
        match s.as_str() {
            "NX" => {
                flags.nx = true;
                idx += 1;
            }
            "XX" => {
                flags.xx = true;
                idx += 1;
            }
            "GT" => {
                flags.gt = true;
                idx += 1;
            }
            "LT" => {
                flags.lt = true;
                idx += 1;
            }
            "CH" => {
                flags.ch = true;
                idx += 1;
            }
            _ => break,
        }
    }

    // NX and XX are mutually exclusive
    if flags.nx && flags.xx {
        return Err(ProtocolError::InvalidCommandFrame(
            "XX and NX options at the same time are not compatible".into(),
        ));
    }
    // GT and LT are mutually exclusive
    if flags.gt && flags.lt {
        return Err(ProtocolError::InvalidCommandFrame(
            "GT and LT options at the same time are not compatible".into(),
        ));
    }

    // remaining args must be score/member pairs
    let remaining = &args[idx..];
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
        return Err(ProtocolError::WrongArity("ZADD".into()));
    }

    let mut members = Vec::with_capacity(remaining.len() / 2);
    for pair in remaining.chunks(2) {
        let score = parse_f64(&pair[0], "ZADD")?;
        let member = extract_string(&pair[1])?;
        members.push((score, member));
    }

    Ok(Command::ZAdd {
        key,
        flags,
        members,
    })
}

fn parse_zcard(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("ZCARD".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::ZCard { key })
}

fn parse_zrem(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("ZREM".into()));
    }
    let key = extract_string(&args[0])?;
    let members = extract_strings(&args[1..])?;
    Ok(Command::ZRem { key, members })
}

fn parse_zscore(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("ZSCORE".into()));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::ZScore { key, member })
}

fn parse_zrank(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("ZRANK".into()));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::ZRank { key, member })
}

fn parse_zrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 || args.len() > 4 {
        return Err(ProtocolError::WrongArity("ZRANGE".into()));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "ZRANGE")?;
    let stop = parse_i64(&args[2], "ZRANGE")?;

    let with_scores = if args.len() == 4 {
        let opt = extract_string(&args[3])?.to_ascii_uppercase();
        if opt != "WITHSCORES" {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "unsupported ZRANGE option '{opt}'"
            )));
        }
        true
    } else {
        false
    };

    Ok(Command::ZRange {
        key,
        start,
        stop,
        with_scores,
    })
}

// --- hash commands ---

fn parse_hset(args: &[Frame]) -> Result<Command, ProtocolError> {
    // HSET key field value [field value ...]
    // args = [key, field, value, ...]
    // Need at least 3 args, and after key we need pairs (so remaining count must be even)
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err(ProtocolError::WrongArity("HSET".into()));
    }

    let key = extract_string(&args[0])?;
    let mut fields = Vec::with_capacity((args.len() - 1) / 2);

    for chunk in args[1..].chunks(2) {
        let field = extract_string(&chunk[0])?;
        let value = extract_bytes(&chunk[1])?;
        fields.push((field, value));
    }

    Ok(Command::HSet { key, fields })
}

fn parse_hget(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("HGET".into()));
    }
    let key = extract_string(&args[0])?;
    let field = extract_string(&args[1])?;
    Ok(Command::HGet { key, field })
}

fn parse_hgetall(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("HGETALL".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HGetAll { key })
}

fn parse_hdel(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("HDEL".into()));
    }
    let key = extract_string(&args[0])?;
    let fields = extract_strings(&args[1..])?;
    Ok(Command::HDel { key, fields })
}

fn parse_hexists(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("HEXISTS".into()));
    }
    let key = extract_string(&args[0])?;
    let field = extract_string(&args[1])?;
    Ok(Command::HExists { key, field })
}

fn parse_hlen(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("HLEN".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HLen { key })
}

fn parse_hincrby(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::WrongArity("HINCRBY".into()));
    }
    let key = extract_string(&args[0])?;
    let field = extract_string(&args[1])?;
    let delta = parse_i64(&args[2], "HINCRBY")?;
    Ok(Command::HIncrBy { key, field, delta })
}

fn parse_hkeys(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("HKEYS".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HKeys { key })
}

fn parse_hvals(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("HVALS".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HVals { key })
}

fn parse_hmget(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("HMGET".into()));
    }
    let key = extract_string(&args[0])?;
    let fields = extract_strings(&args[1..])?;
    Ok(Command::HMGet { key, fields })
}

// --- set commands ---

fn parse_sadd(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("SADD".into()));
    }
    let key = extract_string(&args[0])?;
    let members = extract_strings(&args[1..])?;
    Ok(Command::SAdd { key, members })
}

fn parse_srem(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("SREM".into()));
    }
    let key = extract_string(&args[0])?;
    let members = extract_strings(&args[1..])?;
    Ok(Command::SRem { key, members })
}

fn parse_smembers(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("SMEMBERS".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::SMembers { key })
}

fn parse_sismember(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("SISMEMBER".into()));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::SIsMember { key, member })
}

fn parse_scard(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("SCARD".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::SCard { key })
}

// --- cluster commands ---

fn parse_cluster(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("CLUSTER".into()));
    }

    let subcommand = extract_string(&args[0])?.to_ascii_uppercase();
    match subcommand.as_str() {
        "INFO" => {
            if args.len() != 1 {
                return Err(ProtocolError::WrongArity("CLUSTER INFO".into()));
            }
            Ok(Command::ClusterInfo)
        }
        "NODES" => {
            if args.len() != 1 {
                return Err(ProtocolError::WrongArity("CLUSTER NODES".into()));
            }
            Ok(Command::ClusterNodes)
        }
        "SLOTS" => {
            if args.len() != 1 {
                return Err(ProtocolError::WrongArity("CLUSTER SLOTS".into()));
            }
            Ok(Command::ClusterSlots)
        }
        "KEYSLOT" => {
            if args.len() != 2 {
                return Err(ProtocolError::WrongArity("CLUSTER KEYSLOT".into()));
            }
            let key = extract_string(&args[1])?;
            Ok(Command::ClusterKeySlot { key })
        }
        "MYID" => {
            if args.len() != 1 {
                return Err(ProtocolError::WrongArity("CLUSTER MYID".into()));
            }
            Ok(Command::ClusterMyId)
        }
        "SETSLOT" => parse_cluster_setslot(&args[1..]),
        "MEET" => {
            if args.len() != 3 {
                return Err(ProtocolError::WrongArity("CLUSTER MEET".into()));
            }
            let ip = extract_string(&args[1])?;
            let port_str = extract_string(&args[2])?;
            let port: u16 = port_str
                .parse()
                .map_err(|_| ProtocolError::InvalidCommandFrame("invalid port number".into()))?;
            Ok(Command::ClusterMeet { ip, port })
        }
        "ADDSLOTS" => {
            if args.len() < 2 {
                return Err(ProtocolError::WrongArity("CLUSTER ADDSLOTS".into()));
            }
            let slots = parse_slot_list(&args[1..])?;
            Ok(Command::ClusterAddSlots { slots })
        }
        "DELSLOTS" => {
            if args.len() < 2 {
                return Err(ProtocolError::WrongArity("CLUSTER DELSLOTS".into()));
            }
            let slots = parse_slot_list(&args[1..])?;
            Ok(Command::ClusterDelSlots { slots })
        }
        "FORGET" => {
            if args.len() != 2 {
                return Err(ProtocolError::WrongArity("CLUSTER FORGET".into()));
            }
            let node_id = extract_string(&args[1])?;
            Ok(Command::ClusterForget { node_id })
        }
        "REPLICATE" => {
            if args.len() != 2 {
                return Err(ProtocolError::WrongArity("CLUSTER REPLICATE".into()));
            }
            let node_id = extract_string(&args[1])?;
            Ok(Command::ClusterReplicate { node_id })
        }
        "FAILOVER" => {
            let mut force = false;
            let mut takeover = false;
            for arg in &args[1..] {
                let opt = extract_string(arg)?.to_ascii_uppercase();
                match opt.as_str() {
                    "FORCE" => force = true,
                    "TAKEOVER" => takeover = true,
                    _ => {
                        return Err(ProtocolError::InvalidCommandFrame(format!(
                            "unknown CLUSTER FAILOVER option '{opt}'"
                        )))
                    }
                }
            }
            Ok(Command::ClusterFailover { force, takeover })
        }
        "COUNTKEYSINSLOT" => {
            if args.len() != 2 {
                return Err(ProtocolError::WrongArity("CLUSTER COUNTKEYSINSLOT".into()));
            }
            let slot_str = extract_string(&args[1])?;
            let slot: u16 = slot_str
                .parse()
                .map_err(|_| ProtocolError::InvalidCommandFrame("invalid slot number".into()))?;
            Ok(Command::ClusterCountKeysInSlot { slot })
        }
        "GETKEYSINSLOT" => {
            if args.len() != 3 {
                return Err(ProtocolError::WrongArity("CLUSTER GETKEYSINSLOT".into()));
            }
            let slot_str = extract_string(&args[1])?;
            let slot: u16 = slot_str
                .parse()
                .map_err(|_| ProtocolError::InvalidCommandFrame("invalid slot number".into()))?;
            let count_str = extract_string(&args[2])?;
            let count: u32 = count_str
                .parse()
                .map_err(|_| ProtocolError::InvalidCommandFrame("invalid count".into()))?;
            Ok(Command::ClusterGetKeysInSlot { slot, count })
        }
        _ => Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown CLUSTER subcommand '{subcommand}'"
        ))),
    }
}

fn parse_asking(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(ProtocolError::WrongArity("ASKING".into()));
    }
    Ok(Command::Asking)
}

fn parse_slowlog(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("SLOWLOG".into()));
    }

    let subcmd = extract_string(&args[0])?.to_ascii_uppercase();
    match subcmd.as_str() {
        "GET" => {
            let count = if args.len() > 1 {
                let n: usize = extract_string(&args[1])?.parse().map_err(|_| {
                    ProtocolError::InvalidCommandFrame("invalid count for SLOWLOG GET".into())
                })?;
                Some(n)
            } else {
                None
            };
            Ok(Command::SlowLogGet { count })
        }
        "LEN" => Ok(Command::SlowLogLen),
        "RESET" => Ok(Command::SlowLogReset),
        other => Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown SLOWLOG subcommand '{other}'"
        ))),
    }
}

fn parse_slot_list(args: &[Frame]) -> Result<Vec<u16>, ProtocolError> {
    let mut slots = Vec::with_capacity(args.len());
    for arg in args {
        let slot_str = extract_string(arg)?;
        let slot: u16 = slot_str
            .parse()
            .map_err(|_| ProtocolError::InvalidCommandFrame("invalid slot number".into()))?;
        slots.push(slot);
    }
    Ok(slots)
}

fn parse_cluster_setslot(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("CLUSTER SETSLOT".into()));
    }

    let slot_str = extract_string(&args[0])?;
    let slot: u16 = slot_str
        .parse()
        .map_err(|_| ProtocolError::InvalidCommandFrame("invalid slot number".into()))?;

    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("CLUSTER SETSLOT".into()));
    }

    let action = extract_string(&args[1])?.to_ascii_uppercase();
    match action.as_str() {
        "IMPORTING" => {
            if args.len() != 3 {
                return Err(ProtocolError::WrongArity(
                    "CLUSTER SETSLOT IMPORTING".into(),
                ));
            }
            let node_id = extract_string(&args[2])?;
            Ok(Command::ClusterSetSlotImporting { slot, node_id })
        }
        "MIGRATING" => {
            if args.len() != 3 {
                return Err(ProtocolError::WrongArity(
                    "CLUSTER SETSLOT MIGRATING".into(),
                ));
            }
            let node_id = extract_string(&args[2])?;
            Ok(Command::ClusterSetSlotMigrating { slot, node_id })
        }
        "NODE" => {
            if args.len() != 3 {
                return Err(ProtocolError::WrongArity("CLUSTER SETSLOT NODE".into()));
            }
            let node_id = extract_string(&args[2])?;
            Ok(Command::ClusterSetSlotNode { slot, node_id })
        }
        "STABLE" => {
            if args.len() != 2 {
                return Err(ProtocolError::WrongArity("CLUSTER SETSLOT STABLE".into()));
            }
            Ok(Command::ClusterSetSlotStable { slot })
        }
        _ => Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown CLUSTER SETSLOT action '{action}'"
        ))),
    }
}

fn parse_migrate(args: &[Frame]) -> Result<Command, ProtocolError> {
    // MIGRATE host port key db timeout [COPY] [REPLACE]
    if args.len() < 5 {
        return Err(ProtocolError::WrongArity("MIGRATE".into()));
    }

    let host = extract_string(&args[0])?;
    let port_str = extract_string(&args[1])?;
    let port: u16 = port_str
        .parse()
        .map_err(|_| ProtocolError::InvalidCommandFrame("invalid port number".into()))?;
    let key = extract_string(&args[2])?;
    let db_str = extract_string(&args[3])?;
    let db: u32 = db_str
        .parse()
        .map_err(|_| ProtocolError::InvalidCommandFrame("invalid db number".into()))?;
    let timeout_str = extract_string(&args[4])?;
    let timeout_ms: u64 = timeout_str
        .parse()
        .map_err(|_| ProtocolError::InvalidCommandFrame("invalid timeout".into()))?;

    let mut copy = false;
    let mut replace = false;

    for arg in &args[5..] {
        let opt = extract_string(arg)?.to_ascii_uppercase();
        match opt.as_str() {
            "COPY" => copy = true,
            "REPLACE" => replace = true,
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unknown MIGRATE option '{opt}'"
                )))
            }
        }
    }

    Ok(Command::Migrate {
        host,
        port,
        key,
        db,
        timeout_ms,
        copy,
        replace,
    })
}

fn parse_subscribe(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("SUBSCRIBE".into()));
    }
    let channels: Vec<String> = args.iter().map(extract_string).collect::<Result<_, _>>()?;
    Ok(Command::Subscribe { channels })
}

fn parse_unsubscribe(args: &[Frame]) -> Result<Command, ProtocolError> {
    let channels: Vec<String> = args.iter().map(extract_string).collect::<Result<_, _>>()?;
    Ok(Command::Unsubscribe { channels })
}

fn parse_psubscribe(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("PSUBSCRIBE".into()));
    }
    let patterns: Vec<String> = args.iter().map(extract_string).collect::<Result<_, _>>()?;
    Ok(Command::PSubscribe { patterns })
}

fn parse_punsubscribe(args: &[Frame]) -> Result<Command, ProtocolError> {
    let patterns: Vec<String> = args.iter().map(extract_string).collect::<Result<_, _>>()?;
    Ok(Command::PUnsubscribe { patterns })
}

fn parse_publish(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("PUBLISH".into()));
    }
    let channel = extract_string(&args[0])?;
    let message = extract_bytes(&args[1])?;
    Ok(Command::Publish { channel, message })
}

fn parse_pubsub(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("PUBSUB".into()));
    }

    let subcmd = extract_string(&args[0])?.to_ascii_uppercase();
    match subcmd.as_str() {
        "CHANNELS" => {
            let pattern = if args.len() > 1 {
                Some(extract_string(&args[1])?)
            } else {
                None
            };
            Ok(Command::PubSubChannels { pattern })
        }
        "NUMSUB" => {
            let channels: Vec<String> = args[1..]
                .iter()
                .map(extract_string)
                .collect::<Result<_, _>>()?;
            Ok(Command::PubSubNumSub { channels })
        }
        "NUMPAT" => Ok(Command::PubSubNumPat),
        other => Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown PUBSUB subcommand '{other}'"
        ))),
    }
}

// --- vector command parsers ---

/// VADD key element f32 [f32 ...] [METRIC COSINE|L2|IP] [QUANT F32|F16|I8] [M n] [EF n]
fn parse_vadd(args: &[Frame]) -> Result<Command, ProtocolError> {
    // minimum: key + element + at least one float
    if args.len() < 3 {
        return Err(ProtocolError::WrongArity("VADD".into()));
    }

    let key = extract_string(&args[0])?;
    let element = extract_string(&args[1])?;

    // parse vector values until we hit a non-numeric argument or end
    let mut idx = 2;
    let mut vector = Vec::new();
    while idx < args.len() {
        let s = extract_string(&args[idx])?;
        if let Ok(v) = s.parse::<f32>() {
            vector.push(v);
            idx += 1;
        } else {
            break;
        }
    }

    if vector.is_empty() {
        return Err(ProtocolError::InvalidCommandFrame(
            "VADD: at least one vector dimension required".into(),
        ));
    }

    if vector.len() > MAX_VECTOR_DIMS {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "VADD: vector has {} dimensions, max is {MAX_VECTOR_DIMS}",
            vector.len()
        )));
    }

    // parse optional flags
    let mut metric: u8 = 0; // cosine default
    let mut quantization: u8 = 0; // f32 default
    let mut connectivity: u32 = 16;
    let mut expansion_add: u32 = 64;

    while idx < args.len() {
        let flag = extract_string(&args[idx])?.to_ascii_uppercase();
        match flag.as_str() {
            "METRIC" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VADD: METRIC requires a value".into(),
                    ));
                }
                let val = extract_string(&args[idx])?.to_ascii_uppercase();
                metric = match val.as_str() {
                    "COSINE" => 0,
                    "L2" => 1,
                    "IP" => 2,
                    _ => {
                        return Err(ProtocolError::InvalidCommandFrame(format!(
                            "VADD: unknown metric '{val}'"
                        )))
                    }
                };
                idx += 1;
            }
            "QUANT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VADD: QUANT requires a value".into(),
                    ));
                }
                let val = extract_string(&args[idx])?.to_ascii_uppercase();
                quantization = match val.as_str() {
                    "F32" => 0,
                    "F16" => 1,
                    "I8" | "Q8" => 2,
                    _ => {
                        return Err(ProtocolError::InvalidCommandFrame(format!(
                            "VADD: unknown quantization '{val}'"
                        )))
                    }
                };
                idx += 1;
            }
            "M" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VADD: M requires a value".into(),
                    ));
                }
                let m = parse_u64(&args[idx], "VADD")?;
                if m > MAX_HNSW_PARAM {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "VADD: M value {m} exceeds max {MAX_HNSW_PARAM}"
                    )));
                }
                connectivity = m as u32;
                idx += 1;
            }
            "EF" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VADD: EF requires a value".into(),
                    ));
                }
                let ef = parse_u64(&args[idx], "VADD")?;
                if ef > MAX_HNSW_PARAM {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "VADD: EF value {ef} exceeds max {MAX_HNSW_PARAM}"
                    )));
                }
                expansion_add = ef as u32;
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "VADD: unexpected argument '{flag}'"
                )));
            }
        }
    }

    Ok(Command::VAdd {
        key,
        element,
        vector,
        metric,
        quantization,
        connectivity,
        expansion_add,
    })
}

/// VSIM key f32 [f32 ...] COUNT k [EF n] [WITHSCORES]
fn parse_vsim(args: &[Frame]) -> Result<Command, ProtocolError> {
    // minimum: key + at least one float + COUNT + k
    if args.len() < 4 {
        return Err(ProtocolError::WrongArity("VSIM".into()));
    }

    let key = extract_string(&args[0])?;

    // parse query vector until we hit a non-numeric argument
    let mut idx = 1;
    let mut query = Vec::new();
    while idx < args.len() {
        let s = extract_string(&args[idx])?;
        if let Ok(v) = s.parse::<f32>() {
            query.push(v);
            idx += 1;
        } else {
            break;
        }
    }

    if query.is_empty() {
        return Err(ProtocolError::InvalidCommandFrame(
            "VSIM: at least one query dimension required".into(),
        ));
    }

    if query.len() > MAX_VECTOR_DIMS {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "VSIM: query has {} dimensions, max is {MAX_VECTOR_DIMS}",
            query.len()
        )));
    }

    // COUNT k is required
    let mut count: Option<usize> = None;
    let mut ef_search: usize = 0;
    let mut with_scores = false;

    while idx < args.len() {
        let flag = extract_string(&args[idx])?.to_ascii_uppercase();
        match flag.as_str() {
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VSIM: COUNT requires a value".into(),
                    ));
                }
                count = Some(parse_u64(&args[idx], "VSIM")? as usize);
                idx += 1;
            }
            "EF" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VSIM: EF requires a value".into(),
                    ));
                }
                ef_search = parse_u64(&args[idx], "VSIM")? as usize;
                idx += 1;
            }
            "WITHSCORES" => {
                with_scores = true;
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "VSIM: unexpected argument '{flag}'"
                )));
            }
        }
    }

    let count = count
        .ok_or_else(|| ProtocolError::InvalidCommandFrame("VSIM: COUNT is required".into()))?;

    Ok(Command::VSim {
        key,
        query,
        count,
        ef_search,
        with_scores,
    })
}

/// VREM key element
fn parse_vrem(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("VREM".into()));
    }
    let key = extract_string(&args[0])?;
    let element = extract_string(&args[1])?;
    Ok(Command::VRem { key, element })
}

/// VGET key element
fn parse_vget(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("VGET".into()));
    }
    let key = extract_string(&args[0])?;
    let element = extract_string(&args[1])?;
    Ok(Command::VGet { key, element })
}

/// VCARD key
fn parse_vcard(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("VCARD".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::VCard { key })
}

/// VDIM key
fn parse_vdim(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("VDIM".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::VDim { key })
}

/// VINFO key
fn parse_vinfo(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("VINFO".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::VInfo { key })
}

// --- proto command parsers ---

fn parse_proto_register(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("PROTO.REGISTER".into()));
    }
    let name = extract_string(&args[0])?;
    let descriptor = extract_bytes(&args[1])?;
    Ok(Command::ProtoRegister { name, descriptor })
}

fn parse_proto_set(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(ProtocolError::WrongArity("PROTO.SET".into()));
    }

    let key = extract_string(&args[0])?;
    let type_name = extract_string(&args[1])?;
    let data = extract_bytes(&args[2])?;

    let mut expire = None;
    let mut nx = false;
    let mut xx = false;
    let mut idx = 3;

    while idx < args.len() {
        let flag = extract_string(&args[idx])?.to_ascii_uppercase();
        match flag.as_str() {
            "NX" => {
                nx = true;
                idx += 1;
            }
            "XX" => {
                xx = true;
                idx += 1;
            }
            "EX" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("PROTO.SET".into()));
                }
                let amount = parse_u64(&args[idx], "PROTO.SET")?;
                if amount == 0 {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "invalid expire time in 'PROTO.SET' command".into(),
                    ));
                }
                expire = Some(SetExpire::Ex(amount));
                idx += 1;
            }
            "PX" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::WrongArity("PROTO.SET".into()));
                }
                let amount = parse_u64(&args[idx], "PROTO.SET")?;
                if amount == 0 {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "invalid expire time in 'PROTO.SET' command".into(),
                    ));
                }
                expire = Some(SetExpire::Px(amount));
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported PROTO.SET option '{flag}'"
                )));
            }
        }
    }

    if nx && xx {
        return Err(ProtocolError::InvalidCommandFrame(
            "XX and NX options at the same time are not compatible".into(),
        ));
    }

    Ok(Command::ProtoSet {
        key,
        type_name,
        data,
        expire,
        nx,
        xx,
    })
}

fn parse_proto_get(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("PROTO.GET".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::ProtoGet { key })
}

fn parse_proto_type(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("PROTO.TYPE".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::ProtoType { key })
}

fn parse_proto_schemas(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(ProtocolError::WrongArity("PROTO.SCHEMAS".into()));
    }
    Ok(Command::ProtoSchemas)
}

fn parse_proto_describe(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("PROTO.DESCRIBE".into()));
    }
    let name = extract_string(&args[0])?;
    Ok(Command::ProtoDescribe { name })
}

fn parse_proto_getfield(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("PROTO.GETFIELD".into()));
    }
    let key = extract_string(&args[0])?;
    let field_path = extract_string(&args[1])?;
    Ok(Command::ProtoGetField { key, field_path })
}

fn parse_proto_setfield(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::WrongArity("PROTO.SETFIELD".into()));
    }
    let key = extract_string(&args[0])?;
    let field_path = extract_string(&args[1])?;
    let value = extract_string(&args[2])?;
    Ok(Command::ProtoSetField {
        key,
        field_path,
        value,
    })
}

fn parse_proto_delfield(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("PROTO.DELFIELD".into()));
    }
    let key = extract_string(&args[0])?;
    let field_path = extract_string(&args[1])?;
    Ok(Command::ProtoDelField { key, field_path })
}

fn parse_auth(args: &[Frame]) -> Result<Command, ProtocolError> {
    match args.len() {
        1 => {
            let password = extract_string(&args[0])?;
            Ok(Command::Auth {
                username: None,
                password,
            })
        }
        2 => {
            let username = extract_string(&args[0])?;
            let password = extract_string(&args[1])?;
            Ok(Command::Auth {
                username: Some(username),
                password,
            })
        }
        _ => Err(ProtocolError::WrongArity("AUTH".into())),
    }
}

fn parse_quit(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(ProtocolError::WrongArity("QUIT".into()));
    }
    Ok(Command::Quit)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build an array frame from bulk strings.
    fn cmd(parts: &[&str]) -> Frame {
        Frame::Array(
            parts
                .iter()
                .map(|s| Frame::Bulk(Bytes::from(s.to_string())))
                .collect(),
        )
    }

    // --- ping ---

    #[test]
    fn ping_no_args() {
        assert_eq!(
            Command::from_frame(cmd(&["PING"])).unwrap(),
            Command::Ping(None),
        );
    }

    #[test]
    fn ping_with_message() {
        assert_eq!(
            Command::from_frame(cmd(&["PING", "hello"])).unwrap(),
            Command::Ping(Some(Bytes::from("hello"))),
        );
    }

    #[test]
    fn ping_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["ping"])).unwrap(),
            Command::Ping(None),
        );
        assert_eq!(
            Command::from_frame(cmd(&["Ping"])).unwrap(),
            Command::Ping(None),
        );
    }

    #[test]
    fn ping_too_many_args() {
        let err = Command::from_frame(cmd(&["PING", "a", "b"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- echo ---

    #[test]
    fn echo() {
        assert_eq!(
            Command::from_frame(cmd(&["ECHO", "test"])).unwrap(),
            Command::Echo(Bytes::from("test")),
        );
    }

    #[test]
    fn echo_missing_arg() {
        let err = Command::from_frame(cmd(&["ECHO"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- get ---

    #[test]
    fn get_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["GET", "mykey"])).unwrap(),
            Command::Get {
                key: "mykey".into()
            },
        );
    }

    #[test]
    fn get_no_args() {
        let err = Command::from_frame(cmd(&["GET"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn get_too_many_args() {
        let err = Command::from_frame(cmd(&["GET", "a", "b"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn get_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["get", "k"])).unwrap(),
            Command::Get { key: "k".into() },
        );
    }

    // --- set ---

    #[test]
    fn set_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["SET", "key", "value"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: Bytes::from("value"),
                expire: None,
                nx: false,
                xx: false,
            },
        );
    }

    #[test]
    fn set_with_ex() {
        assert_eq!(
            Command::from_frame(cmd(&["SET", "key", "val", "EX", "10"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: Bytes::from("val"),
                expire: Some(SetExpire::Ex(10)),
                nx: false,
                xx: false,
            },
        );
    }

    #[test]
    fn set_with_px() {
        assert_eq!(
            Command::from_frame(cmd(&["SET", "key", "val", "PX", "5000"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: Bytes::from("val"),
                expire: Some(SetExpire::Px(5000)),
                nx: false,
                xx: false,
            },
        );
    }

    #[test]
    fn set_ex_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["set", "k", "v", "ex", "5"])).unwrap(),
            Command::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire: Some(SetExpire::Ex(5)),
                nx: false,
                xx: false,
            },
        );
    }

    #[test]
    fn set_nx_flag() {
        assert_eq!(
            Command::from_frame(cmd(&["SET", "key", "val", "NX"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: Bytes::from("val"),
                expire: None,
                nx: true,
                xx: false,
            },
        );
    }

    #[test]
    fn set_xx_flag() {
        assert_eq!(
            Command::from_frame(cmd(&["SET", "key", "val", "XX"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: Bytes::from("val"),
                expire: None,
                nx: false,
                xx: true,
            },
        );
    }

    #[test]
    fn set_nx_with_ex() {
        assert_eq!(
            Command::from_frame(cmd(&["SET", "key", "val", "EX", "10", "NX"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: Bytes::from("val"),
                expire: Some(SetExpire::Ex(10)),
                nx: true,
                xx: false,
            },
        );
    }

    #[test]
    fn set_nx_before_ex() {
        assert_eq!(
            Command::from_frame(cmd(&["SET", "key", "val", "NX", "PX", "5000"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: Bytes::from("val"),
                expire: Some(SetExpire::Px(5000)),
                nx: true,
                xx: false,
            },
        );
    }

    #[test]
    fn set_nx_xx_conflict() {
        let err = Command::from_frame(cmd(&["SET", "k", "v", "NX", "XX"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn set_nx_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["set", "k", "v", "nx"])).unwrap(),
            Command::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire: None,
                nx: true,
                xx: false,
            },
        );
    }

    #[test]
    fn set_missing_value() {
        let err = Command::from_frame(cmd(&["SET", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn set_invalid_expire_value() {
        let err = Command::from_frame(cmd(&["SET", "k", "v", "EX", "notanum"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn set_zero_expire() {
        let err = Command::from_frame(cmd(&["SET", "k", "v", "EX", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn set_unknown_flag() {
        let err = Command::from_frame(cmd(&["SET", "k", "v", "ZZ", "10"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn set_incomplete_expire() {
        // EX without a value
        let err = Command::from_frame(cmd(&["SET", "k", "v", "EX"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- del ---

    #[test]
    fn del_single() {
        assert_eq!(
            Command::from_frame(cmd(&["DEL", "key"])).unwrap(),
            Command::Del {
                keys: vec!["key".into()]
            },
        );
    }

    #[test]
    fn del_multiple() {
        assert_eq!(
            Command::from_frame(cmd(&["DEL", "a", "b", "c"])).unwrap(),
            Command::Del {
                keys: vec!["a".into(), "b".into(), "c".into()]
            },
        );
    }

    #[test]
    fn del_no_args() {
        let err = Command::from_frame(cmd(&["DEL"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- exists ---

    #[test]
    fn exists_single() {
        assert_eq!(
            Command::from_frame(cmd(&["EXISTS", "key"])).unwrap(),
            Command::Exists {
                keys: vec!["key".into()]
            },
        );
    }

    #[test]
    fn exists_multiple() {
        assert_eq!(
            Command::from_frame(cmd(&["EXISTS", "a", "b"])).unwrap(),
            Command::Exists {
                keys: vec!["a".into(), "b".into()]
            },
        );
    }

    #[test]
    fn exists_no_args() {
        let err = Command::from_frame(cmd(&["EXISTS"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- mget ---

    #[test]
    fn mget_single() {
        assert_eq!(
            Command::from_frame(cmd(&["MGET", "key"])).unwrap(),
            Command::MGet {
                keys: vec!["key".into()]
            },
        );
    }

    #[test]
    fn mget_multiple() {
        assert_eq!(
            Command::from_frame(cmd(&["MGET", "a", "b", "c"])).unwrap(),
            Command::MGet {
                keys: vec!["a".into(), "b".into(), "c".into()]
            },
        );
    }

    #[test]
    fn mget_no_args() {
        let err = Command::from_frame(cmd(&["MGET"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- mset ---

    #[test]
    fn mset_single_pair() {
        assert_eq!(
            Command::from_frame(cmd(&["MSET", "key", "val"])).unwrap(),
            Command::MSet {
                pairs: vec![("key".into(), Bytes::from("val"))]
            },
        );
    }

    #[test]
    fn mset_multiple_pairs() {
        assert_eq!(
            Command::from_frame(cmd(&["MSET", "a", "1", "b", "2"])).unwrap(),
            Command::MSet {
                pairs: vec![
                    ("a".into(), Bytes::from("1")),
                    ("b".into(), Bytes::from("2")),
                ]
            },
        );
    }

    #[test]
    fn mset_no_args() {
        let err = Command::from_frame(cmd(&["MSET"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn mset_odd_args() {
        // missing value for second key
        let err = Command::from_frame(cmd(&["MSET", "a", "1", "b"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- expire ---

    #[test]
    fn expire_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["EXPIRE", "key", "60"])).unwrap(),
            Command::Expire {
                key: "key".into(),
                seconds: 60,
            },
        );
    }

    #[test]
    fn expire_wrong_arity() {
        let err = Command::from_frame(cmd(&["EXPIRE", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn expire_invalid_seconds() {
        let err = Command::from_frame(cmd(&["EXPIRE", "key", "abc"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn expire_zero_seconds() {
        let err = Command::from_frame(cmd(&["EXPIRE", "key", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    // --- ttl ---

    #[test]
    fn ttl_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["TTL", "key"])).unwrap(),
            Command::Ttl { key: "key".into() },
        );
    }

    #[test]
    fn ttl_wrong_arity() {
        let err = Command::from_frame(cmd(&["TTL"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- dbsize ---

    #[test]
    fn dbsize_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["DBSIZE"])).unwrap(),
            Command::DbSize,
        );
    }

    #[test]
    fn dbsize_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["dbsize"])).unwrap(),
            Command::DbSize,
        );
    }

    #[test]
    fn dbsize_extra_args() {
        let err = Command::from_frame(cmd(&["DBSIZE", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- info ---

    #[test]
    fn info_no_section() {
        assert_eq!(
            Command::from_frame(cmd(&["INFO"])).unwrap(),
            Command::Info { section: None },
        );
    }

    #[test]
    fn info_with_section() {
        assert_eq!(
            Command::from_frame(cmd(&["INFO", "keyspace"])).unwrap(),
            Command::Info {
                section: Some("keyspace".into())
            },
        );
    }

    #[test]
    fn info_too_many_args() {
        let err = Command::from_frame(cmd(&["INFO", "a", "b"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- bgsave ---

    #[test]
    fn bgsave_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["BGSAVE"])).unwrap(),
            Command::BgSave,
        );
    }

    #[test]
    fn bgsave_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["bgsave"])).unwrap(),
            Command::BgSave,
        );
    }

    #[test]
    fn bgsave_extra_args() {
        let err = Command::from_frame(cmd(&["BGSAVE", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- bgrewriteaof ---

    #[test]
    fn bgrewriteaof_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["BGREWRITEAOF"])).unwrap(),
            Command::BgRewriteAof,
        );
    }

    #[test]
    fn bgrewriteaof_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["bgrewriteaof"])).unwrap(),
            Command::BgRewriteAof,
        );
    }

    #[test]
    fn bgrewriteaof_extra_args() {
        let err = Command::from_frame(cmd(&["BGREWRITEAOF", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- flushdb ---

    #[test]
    fn flushdb_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["FLUSHDB"])).unwrap(),
            Command::FlushDb { async_mode: false },
        );
    }

    #[test]
    fn flushdb_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["flushdb"])).unwrap(),
            Command::FlushDb { async_mode: false },
        );
    }

    #[test]
    fn flushdb_async() {
        assert_eq!(
            Command::from_frame(cmd(&["FLUSHDB", "ASYNC"])).unwrap(),
            Command::FlushDb { async_mode: true },
        );
    }

    #[test]
    fn flushdb_async_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["flushdb", "async"])).unwrap(),
            Command::FlushDb { async_mode: true },
        );
    }

    #[test]
    fn flushdb_extra_args() {
        let err = Command::from_frame(cmd(&["FLUSHDB", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- unlink ---

    #[test]
    fn unlink_single() {
        assert_eq!(
            Command::from_frame(cmd(&["UNLINK", "mykey"])).unwrap(),
            Command::Unlink {
                keys: vec!["mykey".into()]
            },
        );
    }

    #[test]
    fn unlink_multiple() {
        assert_eq!(
            Command::from_frame(cmd(&["UNLINK", "a", "b", "c"])).unwrap(),
            Command::Unlink {
                keys: vec!["a".into(), "b".into(), "c".into()]
            },
        );
    }

    #[test]
    fn unlink_no_args() {
        let err = Command::from_frame(cmd(&["UNLINK"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- lpush ---

    #[test]
    fn lpush_single() {
        assert_eq!(
            Command::from_frame(cmd(&["LPUSH", "list", "val"])).unwrap(),
            Command::LPush {
                key: "list".into(),
                values: vec![Bytes::from("val")],
            },
        );
    }

    #[test]
    fn lpush_multiple() {
        assert_eq!(
            Command::from_frame(cmd(&["LPUSH", "list", "a", "b", "c"])).unwrap(),
            Command::LPush {
                key: "list".into(),
                values: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
        );
    }

    #[test]
    fn lpush_no_value() {
        let err = Command::from_frame(cmd(&["LPUSH", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn lpush_case_insensitive() {
        assert!(matches!(
            Command::from_frame(cmd(&["lpush", "k", "v"])).unwrap(),
            Command::LPush { .. }
        ));
    }

    // --- rpush ---

    #[test]
    fn rpush_single() {
        assert_eq!(
            Command::from_frame(cmd(&["RPUSH", "list", "val"])).unwrap(),
            Command::RPush {
                key: "list".into(),
                values: vec![Bytes::from("val")],
            },
        );
    }

    #[test]
    fn rpush_no_value() {
        let err = Command::from_frame(cmd(&["RPUSH", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- lpop ---

    #[test]
    fn lpop_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["LPOP", "list"])).unwrap(),
            Command::LPop { key: "list".into() },
        );
    }

    #[test]
    fn lpop_wrong_arity() {
        let err = Command::from_frame(cmd(&["LPOP"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- rpop ---

    #[test]
    fn rpop_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["RPOP", "list"])).unwrap(),
            Command::RPop { key: "list".into() },
        );
    }

    // --- lrange ---

    #[test]
    fn lrange_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["LRANGE", "list", "0", "-1"])).unwrap(),
            Command::LRange {
                key: "list".into(),
                start: 0,
                stop: -1,
            },
        );
    }

    #[test]
    fn lrange_wrong_arity() {
        let err = Command::from_frame(cmd(&["LRANGE", "list", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn lrange_invalid_index() {
        let err = Command::from_frame(cmd(&["LRANGE", "list", "abc", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    // --- llen ---

    #[test]
    fn llen_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["LLEN", "list"])).unwrap(),
            Command::LLen { key: "list".into() },
        );
    }

    #[test]
    fn llen_wrong_arity() {
        let err = Command::from_frame(cmd(&["LLEN"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- type ---

    #[test]
    fn type_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["TYPE", "key"])).unwrap(),
            Command::Type { key: "key".into() },
        );
    }

    #[test]
    fn type_wrong_arity() {
        let err = Command::from_frame(cmd(&["TYPE"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn type_case_insensitive() {
        assert!(matches!(
            Command::from_frame(cmd(&["type", "k"])).unwrap(),
            Command::Type { .. }
        ));
    }

    // --- general ---

    #[test]
    fn unknown_command() {
        assert_eq!(
            Command::from_frame(cmd(&["FOOBAR", "arg"])).unwrap(),
            Command::Unknown("FOOBAR".into()),
        );
    }

    #[test]
    fn non_array_frame() {
        let err = Command::from_frame(Frame::Simple("PING".into())).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn empty_array() {
        let err = Command::from_frame(Frame::Array(vec![])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    // --- zadd ---

    #[test]
    fn zadd_basic() {
        let parsed = Command::from_frame(cmd(&["ZADD", "board", "100", "alice"])).unwrap();
        match parsed {
            Command::ZAdd {
                key,
                flags,
                members,
            } => {
                assert_eq!(key, "board");
                assert_eq!(flags, ZAddFlags::default());
                assert_eq!(members, vec![(100.0, "alice".into())]);
            }
            other => panic!("expected ZAdd, got {other:?}"),
        }
    }

    #[test]
    fn zadd_multiple_members() {
        let parsed =
            Command::from_frame(cmd(&["ZADD", "board", "100", "alice", "200", "bob"])).unwrap();
        match parsed {
            Command::ZAdd { members, .. } => {
                assert_eq!(members.len(), 2);
                assert_eq!(members[0], (100.0, "alice".into()));
                assert_eq!(members[1], (200.0, "bob".into()));
            }
            other => panic!("expected ZAdd, got {other:?}"),
        }
    }

    #[test]
    fn zadd_with_flags() {
        let parsed = Command::from_frame(cmd(&["ZADD", "z", "NX", "CH", "100", "alice"])).unwrap();
        match parsed {
            Command::ZAdd { flags, .. } => {
                assert!(flags.nx);
                assert!(flags.ch);
                assert!(!flags.xx);
                assert!(!flags.gt);
                assert!(!flags.lt);
            }
            other => panic!("expected ZAdd, got {other:?}"),
        }
    }

    #[test]
    fn zadd_gt_flag() {
        let parsed = Command::from_frame(cmd(&["zadd", "z", "gt", "100", "alice"])).unwrap();
        match parsed {
            Command::ZAdd { flags, .. } => assert!(flags.gt),
            other => panic!("expected ZAdd, got {other:?}"),
        }
    }

    #[test]
    fn zadd_nx_xx_conflict() {
        let err = Command::from_frame(cmd(&["ZADD", "z", "NX", "XX", "100", "alice"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn zadd_gt_lt_conflict() {
        let err = Command::from_frame(cmd(&["ZADD", "z", "GT", "LT", "100", "alice"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn zadd_wrong_arity() {
        let err = Command::from_frame(cmd(&["ZADD", "z"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn zadd_odd_score_member_count() {
        // one score without a member
        let err = Command::from_frame(cmd(&["ZADD", "z", "100"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn zadd_invalid_score() {
        let err = Command::from_frame(cmd(&["ZADD", "z", "notanum", "alice"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn zadd_nan_score() {
        let err = Command::from_frame(cmd(&["ZADD", "z", "nan", "alice"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn zadd_negative_score() {
        let parsed = Command::from_frame(cmd(&["ZADD", "z", "-50.5", "alice"])).unwrap();
        match parsed {
            Command::ZAdd { members, .. } => {
                assert_eq!(members[0].0, -50.5);
            }
            other => panic!("expected ZAdd, got {other:?}"),
        }
    }

    // --- zrem ---

    #[test]
    fn zrem_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["ZREM", "z", "alice"])).unwrap(),
            Command::ZRem {
                key: "z".into(),
                members: vec!["alice".into()],
            },
        );
    }

    #[test]
    fn zrem_multiple() {
        let parsed = Command::from_frame(cmd(&["ZREM", "z", "a", "b", "c"])).unwrap();
        match parsed {
            Command::ZRem { members, .. } => assert_eq!(members.len(), 3),
            other => panic!("expected ZRem, got {other:?}"),
        }
    }

    #[test]
    fn zrem_wrong_arity() {
        let err = Command::from_frame(cmd(&["ZREM", "z"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- zscore ---

    #[test]
    fn zscore_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["ZSCORE", "z", "alice"])).unwrap(),
            Command::ZScore {
                key: "z".into(),
                member: "alice".into(),
            },
        );
    }

    #[test]
    fn zscore_wrong_arity() {
        let err = Command::from_frame(cmd(&["ZSCORE", "z"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- zrank ---

    #[test]
    fn zrank_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["ZRANK", "z", "alice"])).unwrap(),
            Command::ZRank {
                key: "z".into(),
                member: "alice".into(),
            },
        );
    }

    #[test]
    fn zrank_wrong_arity() {
        let err = Command::from_frame(cmd(&["ZRANK", "z"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- zcard ---

    #[test]
    fn zcard_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["ZCARD", "z"])).unwrap(),
            Command::ZCard { key: "z".into() },
        );
    }

    #[test]
    fn zcard_wrong_arity() {
        let err = Command::from_frame(cmd(&["ZCARD"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- zrange ---

    #[test]
    fn zrange_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["ZRANGE", "z", "0", "-1"])).unwrap(),
            Command::ZRange {
                key: "z".into(),
                start: 0,
                stop: -1,
                with_scores: false,
            },
        );
    }

    #[test]
    fn zrange_with_scores() {
        assert_eq!(
            Command::from_frame(cmd(&["ZRANGE", "z", "0", "-1", "WITHSCORES"])).unwrap(),
            Command::ZRange {
                key: "z".into(),
                start: 0,
                stop: -1,
                with_scores: true,
            },
        );
    }

    #[test]
    fn zrange_withscores_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["zrange", "z", "0", "-1", "withscores"])).unwrap(),
            Command::ZRange {
                key: "z".into(),
                start: 0,
                stop: -1,
                with_scores: true,
            },
        );
    }

    #[test]
    fn zrange_invalid_option() {
        let err = Command::from_frame(cmd(&["ZRANGE", "z", "0", "-1", "BADOPT"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn zrange_wrong_arity() {
        let err = Command::from_frame(cmd(&["ZRANGE", "z", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- incr ---

    #[test]
    fn incr_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["INCR", "counter"])).unwrap(),
            Command::Incr {
                key: "counter".into()
            },
        );
    }

    #[test]
    fn incr_wrong_arity() {
        let err = Command::from_frame(cmd(&["INCR"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- decr ---

    #[test]
    fn decr_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["DECR", "counter"])).unwrap(),
            Command::Decr {
                key: "counter".into()
            },
        );
    }

    #[test]
    fn decr_wrong_arity() {
        let err = Command::from_frame(cmd(&["DECR"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- persist ---

    #[test]
    fn persist_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PERSIST", "key"])).unwrap(),
            Command::Persist { key: "key".into() },
        );
    }

    #[test]
    fn persist_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["persist", "key"])).unwrap(),
            Command::Persist { key: "key".into() },
        );
    }

    #[test]
    fn persist_wrong_arity() {
        let err = Command::from_frame(cmd(&["PERSIST"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- pttl ---

    #[test]
    fn pttl_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PTTL", "key"])).unwrap(),
            Command::Pttl { key: "key".into() },
        );
    }

    #[test]
    fn pttl_wrong_arity() {
        let err = Command::from_frame(cmd(&["PTTL"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- pexpire ---

    #[test]
    fn pexpire_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PEXPIRE", "key", "5000"])).unwrap(),
            Command::Pexpire {
                key: "key".into(),
                milliseconds: 5000,
            },
        );
    }

    #[test]
    fn pexpire_wrong_arity() {
        let err = Command::from_frame(cmd(&["PEXPIRE", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn pexpire_zero_millis() {
        let err = Command::from_frame(cmd(&["PEXPIRE", "key", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn pexpire_invalid_millis() {
        let err = Command::from_frame(cmd(&["PEXPIRE", "key", "notanum"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    // --- scan ---

    #[test]
    fn scan_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["SCAN", "0"])).unwrap(),
            Command::Scan {
                cursor: 0,
                pattern: None,
                count: None,
            },
        );
    }

    #[test]
    fn scan_with_match() {
        assert_eq!(
            Command::from_frame(cmd(&["SCAN", "0", "MATCH", "user:*"])).unwrap(),
            Command::Scan {
                cursor: 0,
                pattern: Some("user:*".into()),
                count: None,
            },
        );
    }

    #[test]
    fn scan_with_count() {
        assert_eq!(
            Command::from_frame(cmd(&["SCAN", "42", "COUNT", "100"])).unwrap(),
            Command::Scan {
                cursor: 42,
                pattern: None,
                count: Some(100),
            },
        );
    }

    #[test]
    fn scan_with_match_and_count() {
        assert_eq!(
            Command::from_frame(cmd(&["SCAN", "0", "MATCH", "*:data", "COUNT", "50"])).unwrap(),
            Command::Scan {
                cursor: 0,
                pattern: Some("*:data".into()),
                count: Some(50),
            },
        );
    }

    #[test]
    fn scan_count_before_match() {
        assert_eq!(
            Command::from_frame(cmd(&["SCAN", "0", "COUNT", "10", "MATCH", "foo*"])).unwrap(),
            Command::Scan {
                cursor: 0,
                pattern: Some("foo*".into()),
                count: Some(10),
            },
        );
    }

    #[test]
    fn scan_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["scan", "0", "match", "x*", "count", "5"])).unwrap(),
            Command::Scan {
                cursor: 0,
                pattern: Some("x*".into()),
                count: Some(5),
            },
        );
    }

    #[test]
    fn scan_wrong_arity() {
        let err = Command::from_frame(cmd(&["SCAN"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn scan_invalid_cursor() {
        let err = Command::from_frame(cmd(&["SCAN", "notanum"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn scan_invalid_count() {
        let err = Command::from_frame(cmd(&["SCAN", "0", "COUNT", "bad"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn scan_unknown_flag() {
        let err = Command::from_frame(cmd(&["SCAN", "0", "BADOPT", "val"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn scan_match_missing_pattern() {
        let err = Command::from_frame(cmd(&["SCAN", "0", "MATCH"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn scan_count_missing_value() {
        let err = Command::from_frame(cmd(&["SCAN", "0", "COUNT"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- hash commands ---

    #[test]
    fn hset_single_field() {
        assert_eq!(
            Command::from_frame(cmd(&["HSET", "h", "field", "value"])).unwrap(),
            Command::HSet {
                key: "h".into(),
                fields: vec![("field".into(), Bytes::from("value"))],
            },
        );
    }

    #[test]
    fn hset_multiple_fields() {
        let parsed = Command::from_frame(cmd(&["HSET", "h", "f1", "v1", "f2", "v2"])).unwrap();
        match parsed {
            Command::HSet { key, fields } => {
                assert_eq!(key, "h");
                assert_eq!(fields.len(), 2);
            }
            other => panic!("expected HSet, got {other:?}"),
        }
    }

    #[test]
    fn hset_wrong_arity() {
        let err = Command::from_frame(cmd(&["HSET", "h"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
        let err = Command::from_frame(cmd(&["HSET", "h", "f"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn hget_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["HGET", "h", "field"])).unwrap(),
            Command::HGet {
                key: "h".into(),
                field: "field".into(),
            },
        );
    }

    #[test]
    fn hget_wrong_arity() {
        let err = Command::from_frame(cmd(&["HGET", "h"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn hgetall_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["HGETALL", "h"])).unwrap(),
            Command::HGetAll { key: "h".into() },
        );
    }

    #[test]
    fn hgetall_wrong_arity() {
        let err = Command::from_frame(cmd(&["HGETALL"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn hdel_single() {
        assert_eq!(
            Command::from_frame(cmd(&["HDEL", "h", "f"])).unwrap(),
            Command::HDel {
                key: "h".into(),
                fields: vec!["f".into()],
            },
        );
    }

    #[test]
    fn hdel_multiple() {
        let parsed = Command::from_frame(cmd(&["HDEL", "h", "f1", "f2", "f3"])).unwrap();
        match parsed {
            Command::HDel { fields, .. } => assert_eq!(fields.len(), 3),
            other => panic!("expected HDel, got {other:?}"),
        }
    }

    #[test]
    fn hdel_wrong_arity() {
        let err = Command::from_frame(cmd(&["HDEL", "h"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn hexists_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["HEXISTS", "h", "f"])).unwrap(),
            Command::HExists {
                key: "h".into(),
                field: "f".into(),
            },
        );
    }

    #[test]
    fn hlen_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["HLEN", "h"])).unwrap(),
            Command::HLen { key: "h".into() },
        );
    }

    #[test]
    fn hincrby_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["HINCRBY", "h", "f", "5"])).unwrap(),
            Command::HIncrBy {
                key: "h".into(),
                field: "f".into(),
                delta: 5,
            },
        );
    }

    #[test]
    fn hincrby_negative() {
        assert_eq!(
            Command::from_frame(cmd(&["HINCRBY", "h", "f", "-3"])).unwrap(),
            Command::HIncrBy {
                key: "h".into(),
                field: "f".into(),
                delta: -3,
            },
        );
    }

    #[test]
    fn hincrby_wrong_arity() {
        let err = Command::from_frame(cmd(&["HINCRBY", "h", "f"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn hkeys_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["HKEYS", "h"])).unwrap(),
            Command::HKeys { key: "h".into() },
        );
    }

    #[test]
    fn hvals_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["HVALS", "h"])).unwrap(),
            Command::HVals { key: "h".into() },
        );
    }

    #[test]
    fn hmget_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["HMGET", "h", "f1", "f2"])).unwrap(),
            Command::HMGet {
                key: "h".into(),
                fields: vec!["f1".into(), "f2".into()],
            },
        );
    }

    #[test]
    fn hmget_wrong_arity() {
        let err = Command::from_frame(cmd(&["HMGET", "h"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn hash_commands_case_insensitive() {
        assert!(matches!(
            Command::from_frame(cmd(&["hset", "h", "f", "v"])).unwrap(),
            Command::HSet { .. }
        ));
        assert!(matches!(
            Command::from_frame(cmd(&["hget", "h", "f"])).unwrap(),
            Command::HGet { .. }
        ));
        assert!(matches!(
            Command::from_frame(cmd(&["hgetall", "h"])).unwrap(),
            Command::HGetAll { .. }
        ));
    }

    // --- set commands ---

    #[test]
    fn sadd_single_member() {
        assert_eq!(
            Command::from_frame(cmd(&["SADD", "s", "member"])).unwrap(),
            Command::SAdd {
                key: "s".into(),
                members: vec!["member".into()],
            },
        );
    }

    #[test]
    fn sadd_multiple_members() {
        let parsed = Command::from_frame(cmd(&["SADD", "s", "a", "b", "c"])).unwrap();
        match parsed {
            Command::SAdd { key, members } => {
                assert_eq!(key, "s");
                assert_eq!(members.len(), 3);
            }
            other => panic!("expected SAdd, got {other:?}"),
        }
    }

    #[test]
    fn sadd_wrong_arity() {
        let err = Command::from_frame(cmd(&["SADD", "s"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn srem_single_member() {
        assert_eq!(
            Command::from_frame(cmd(&["SREM", "s", "member"])).unwrap(),
            Command::SRem {
                key: "s".into(),
                members: vec!["member".into()],
            },
        );
    }

    #[test]
    fn srem_multiple_members() {
        let parsed = Command::from_frame(cmd(&["SREM", "s", "a", "b"])).unwrap();
        match parsed {
            Command::SRem { key, members } => {
                assert_eq!(key, "s");
                assert_eq!(members.len(), 2);
            }
            other => panic!("expected SRem, got {other:?}"),
        }
    }

    #[test]
    fn srem_wrong_arity() {
        let err = Command::from_frame(cmd(&["SREM", "s"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn smembers_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["SMEMBERS", "s"])).unwrap(),
            Command::SMembers { key: "s".into() },
        );
    }

    #[test]
    fn smembers_wrong_arity() {
        let err = Command::from_frame(cmd(&["SMEMBERS"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn sismember_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["SISMEMBER", "s", "member"])).unwrap(),
            Command::SIsMember {
                key: "s".into(),
                member: "member".into(),
            },
        );
    }

    #[test]
    fn sismember_wrong_arity() {
        let err = Command::from_frame(cmd(&["SISMEMBER", "s"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn scard_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["SCARD", "s"])).unwrap(),
            Command::SCard { key: "s".into() },
        );
    }

    #[test]
    fn scard_wrong_arity() {
        let err = Command::from_frame(cmd(&["SCARD"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn set_commands_case_insensitive() {
        assert!(matches!(
            Command::from_frame(cmd(&["sadd", "s", "m"])).unwrap(),
            Command::SAdd { .. }
        ));
        assert!(matches!(
            Command::from_frame(cmd(&["srem", "s", "m"])).unwrap(),
            Command::SRem { .. }
        ));
        assert!(matches!(
            Command::from_frame(cmd(&["smembers", "s"])).unwrap(),
            Command::SMembers { .. }
        ));
    }

    // --- cluster commands ---

    #[test]
    fn cluster_info_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "INFO"])).unwrap(),
            Command::ClusterInfo,
        );
    }

    #[test]
    fn cluster_nodes_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "NODES"])).unwrap(),
            Command::ClusterNodes,
        );
    }

    #[test]
    fn cluster_slots_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "SLOTS"])).unwrap(),
            Command::ClusterSlots,
        );
    }

    #[test]
    fn cluster_keyslot_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "KEYSLOT", "mykey"])).unwrap(),
            Command::ClusterKeySlot {
                key: "mykey".into()
            },
        );
    }

    #[test]
    fn cluster_keyslot_wrong_arity() {
        let err = Command::from_frame(cmd(&["CLUSTER", "KEYSLOT"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn cluster_myid_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "MYID"])).unwrap(),
            Command::ClusterMyId,
        );
    }

    #[test]
    fn cluster_unknown_subcommand() {
        let err = Command::from_frame(cmd(&["CLUSTER", "BADCMD"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn cluster_no_subcommand() {
        let err = Command::from_frame(cmd(&["CLUSTER"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn cluster_case_insensitive() {
        assert!(matches!(
            Command::from_frame(cmd(&["cluster", "info"])).unwrap(),
            Command::ClusterInfo
        ));
        assert!(matches!(
            Command::from_frame(cmd(&["cluster", "keyslot", "k"])).unwrap(),
            Command::ClusterKeySlot { .. }
        ));
    }

    #[test]
    fn asking_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["ASKING"])).unwrap(),
            Command::Asking,
        );
    }

    #[test]
    fn asking_wrong_arity() {
        let err = Command::from_frame(cmd(&["ASKING", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn cluster_setslot_importing() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "100", "IMPORTING", "node123"]))
                .unwrap(),
            Command::ClusterSetSlotImporting {
                slot: 100,
                node_id: "node123".into()
            },
        );
    }

    #[test]
    fn cluster_setslot_migrating() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "200", "MIGRATING", "node456"]))
                .unwrap(),
            Command::ClusterSetSlotMigrating {
                slot: 200,
                node_id: "node456".into()
            },
        );
    }

    #[test]
    fn cluster_setslot_node() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "300", "NODE", "node789"])).unwrap(),
            Command::ClusterSetSlotNode {
                slot: 300,
                node_id: "node789".into()
            },
        );
    }

    #[test]
    fn cluster_setslot_stable() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "400", "STABLE"])).unwrap(),
            Command::ClusterSetSlotStable { slot: 400 },
        );
    }

    #[test]
    fn cluster_setslot_invalid_slot() {
        let err =
            Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "notanumber", "STABLE"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn cluster_setslot_wrong_arity() {
        let err = Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "100"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn migrate_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["MIGRATE", "127.0.0.1", "6379", "mykey", "0", "5000"]))
                .unwrap(),
            Command::Migrate {
                host: "127.0.0.1".into(),
                port: 6379,
                key: "mykey".into(),
                db: 0,
                timeout_ms: 5000,
                copy: false,
                replace: false,
            },
        );
    }

    #[test]
    fn migrate_with_options() {
        assert_eq!(
            Command::from_frame(cmd(&[
                "MIGRATE",
                "192.168.1.1",
                "6380",
                "testkey",
                "1",
                "10000",
                "COPY",
                "REPLACE"
            ]))
            .unwrap(),
            Command::Migrate {
                host: "192.168.1.1".into(),
                port: 6380,
                key: "testkey".into(),
                db: 1,
                timeout_ms: 10000,
                copy: true,
                replace: true,
            },
        );
    }

    #[test]
    fn migrate_wrong_arity() {
        let err = Command::from_frame(cmd(&["MIGRATE", "host", "port", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn migrate_invalid_port() {
        let err = Command::from_frame(cmd(&["MIGRATE", "host", "notaport", "key", "0", "1000"]))
            .unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn cluster_meet_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "MEET", "192.168.1.1", "6379"])).unwrap(),
            Command::ClusterMeet {
                ip: "192.168.1.1".into(),
                port: 6379
            },
        );
    }

    #[test]
    fn cluster_addslots_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "ADDSLOTS", "0", "1", "2"])).unwrap(),
            Command::ClusterAddSlots {
                slots: vec![0, 1, 2]
            },
        );
    }

    #[test]
    fn cluster_delslots_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "DELSLOTS", "100", "101"])).unwrap(),
            Command::ClusterDelSlots {
                slots: vec![100, 101]
            },
        );
    }

    #[test]
    fn cluster_forget_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "FORGET", "abc123"])).unwrap(),
            Command::ClusterForget {
                node_id: "abc123".into()
            },
        );
    }

    #[test]
    fn cluster_replicate_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "REPLICATE", "master-id"])).unwrap(),
            Command::ClusterReplicate {
                node_id: "master-id".into()
            },
        );
    }

    #[test]
    fn cluster_failover_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "FAILOVER"])).unwrap(),
            Command::ClusterFailover {
                force: false,
                takeover: false
            },
        );
    }

    #[test]
    fn cluster_failover_force() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "FAILOVER", "FORCE"])).unwrap(),
            Command::ClusterFailover {
                force: true,
                takeover: false
            },
        );
    }

    #[test]
    fn cluster_failover_takeover() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "FAILOVER", "TAKEOVER"])).unwrap(),
            Command::ClusterFailover {
                force: false,
                takeover: true
            },
        );
    }

    #[test]
    fn cluster_countkeysinslot_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "COUNTKEYSINSLOT", "100"])).unwrap(),
            Command::ClusterCountKeysInSlot { slot: 100 },
        );
    }

    #[test]
    fn cluster_getkeysinslot_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["CLUSTER", "GETKEYSINSLOT", "200", "10"])).unwrap(),
            Command::ClusterGetKeysInSlot {
                slot: 200,
                count: 10
            },
        );
    }

    // --- pub/sub ---

    #[test]
    fn subscribe_single_channel() {
        assert_eq!(
            Command::from_frame(cmd(&["SUBSCRIBE", "news"])).unwrap(),
            Command::Subscribe {
                channels: vec!["news".into()]
            },
        );
    }

    #[test]
    fn subscribe_multiple_channels() {
        assert_eq!(
            Command::from_frame(cmd(&["SUBSCRIBE", "ch1", "ch2", "ch3"])).unwrap(),
            Command::Subscribe {
                channels: vec!["ch1".into(), "ch2".into(), "ch3".into()]
            },
        );
    }

    #[test]
    fn subscribe_no_args() {
        let err = Command::from_frame(cmd(&["SUBSCRIBE"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn unsubscribe_all() {
        assert_eq!(
            Command::from_frame(cmd(&["UNSUBSCRIBE"])).unwrap(),
            Command::Unsubscribe { channels: vec![] },
        );
    }

    #[test]
    fn unsubscribe_specific() {
        assert_eq!(
            Command::from_frame(cmd(&["UNSUBSCRIBE", "news"])).unwrap(),
            Command::Unsubscribe {
                channels: vec!["news".into()]
            },
        );
    }

    #[test]
    fn psubscribe_pattern() {
        assert_eq!(
            Command::from_frame(cmd(&["PSUBSCRIBE", "news.*"])).unwrap(),
            Command::PSubscribe {
                patterns: vec!["news.*".into()]
            },
        );
    }

    #[test]
    fn psubscribe_no_args() {
        let err = Command::from_frame(cmd(&["PSUBSCRIBE"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn punsubscribe_all() {
        assert_eq!(
            Command::from_frame(cmd(&["PUNSUBSCRIBE"])).unwrap(),
            Command::PUnsubscribe { patterns: vec![] },
        );
    }

    #[test]
    fn publish_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PUBLISH", "news", "hello world"])).unwrap(),
            Command::Publish {
                channel: "news".into(),
                message: Bytes::from("hello world"),
            },
        );
    }

    #[test]
    fn publish_wrong_arity() {
        let err = Command::from_frame(cmd(&["PUBLISH", "news"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn subscribe_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["subscribe", "ch"])).unwrap(),
            Command::Subscribe {
                channels: vec!["ch".into()]
            },
        );
    }

    #[test]
    fn pubsub_channels_no_pattern() {
        assert_eq!(
            Command::from_frame(cmd(&["PUBSUB", "CHANNELS"])).unwrap(),
            Command::PubSubChannels { pattern: None },
        );
    }

    #[test]
    fn pubsub_channels_with_pattern() {
        assert_eq!(
            Command::from_frame(cmd(&["PUBSUB", "CHANNELS", "news.*"])).unwrap(),
            Command::PubSubChannels {
                pattern: Some("news.*".into())
            },
        );
    }

    #[test]
    fn pubsub_numsub_no_args() {
        assert_eq!(
            Command::from_frame(cmd(&["PUBSUB", "NUMSUB"])).unwrap(),
            Command::PubSubNumSub { channels: vec![] },
        );
    }

    #[test]
    fn pubsub_numsub_with_channels() {
        assert_eq!(
            Command::from_frame(cmd(&["PUBSUB", "NUMSUB", "ch1", "ch2"])).unwrap(),
            Command::PubSubNumSub {
                channels: vec!["ch1".into(), "ch2".into()]
            },
        );
    }

    #[test]
    fn pubsub_numpat() {
        assert_eq!(
            Command::from_frame(cmd(&["PUBSUB", "NUMPAT"])).unwrap(),
            Command::PubSubNumPat,
        );
    }

    #[test]
    fn pubsub_no_subcommand() {
        let err = Command::from_frame(cmd(&["PUBSUB"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn pubsub_unknown_subcommand() {
        let err = Command::from_frame(cmd(&["PUBSUB", "BOGUS"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    // --- INCRBY / DECRBY ---

    #[test]
    fn incrby_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["INCRBY", "counter", "5"])).unwrap(),
            Command::IncrBy {
                key: "counter".into(),
                delta: 5
            },
        );
    }

    #[test]
    fn incrby_negative() {
        assert_eq!(
            Command::from_frame(cmd(&["INCRBY", "counter", "-3"])).unwrap(),
            Command::IncrBy {
                key: "counter".into(),
                delta: -3
            },
        );
    }

    #[test]
    fn incrby_wrong_arity() {
        let err = Command::from_frame(cmd(&["INCRBY", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn incrby_not_integer() {
        let err = Command::from_frame(cmd(&["INCRBY", "key", "abc"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn decrby_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["DECRBY", "counter", "10"])).unwrap(),
            Command::DecrBy {
                key: "counter".into(),
                delta: 10
            },
        );
    }

    #[test]
    fn decrby_wrong_arity() {
        let err = Command::from_frame(cmd(&["DECRBY"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- INCRBYFLOAT ---

    #[test]
    fn incrbyfloat_basic() {
        let cmd = Command::from_frame(cmd(&["INCRBYFLOAT", "key", "2.5"])).unwrap();
        match cmd {
            Command::IncrByFloat { key, delta } => {
                assert_eq!(key, "key");
                assert!((delta - 2.5).abs() < f64::EPSILON);
            }
            other => panic!("expected IncrByFloat, got {other:?}"),
        }
    }

    #[test]
    fn incrbyfloat_negative() {
        let cmd = Command::from_frame(cmd(&["INCRBYFLOAT", "key", "-1.5"])).unwrap();
        match cmd {
            Command::IncrByFloat { key, delta } => {
                assert_eq!(key, "key");
                assert!((delta - (-1.5)).abs() < f64::EPSILON);
            }
            other => panic!("expected IncrByFloat, got {other:?}"),
        }
    }

    #[test]
    fn incrbyfloat_wrong_arity() {
        let err = Command::from_frame(cmd(&["INCRBYFLOAT", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn incrbyfloat_not_a_float() {
        let err = Command::from_frame(cmd(&["INCRBYFLOAT", "key", "abc"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    // --- APPEND / STRLEN ---

    #[test]
    fn append_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["APPEND", "key", "value"])).unwrap(),
            Command::Append {
                key: "key".into(),
                value: Bytes::from("value")
            },
        );
    }

    #[test]
    fn append_wrong_arity() {
        let err = Command::from_frame(cmd(&["APPEND", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn strlen_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["STRLEN", "key"])).unwrap(),
            Command::Strlen { key: "key".into() },
        );
    }

    #[test]
    fn strlen_wrong_arity() {
        let err = Command::from_frame(cmd(&["STRLEN"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- KEYS ---

    #[test]
    fn keys_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["KEYS", "user:*"])).unwrap(),
            Command::Keys {
                pattern: "user:*".into()
            },
        );
    }

    #[test]
    fn keys_wrong_arity() {
        let err = Command::from_frame(cmd(&["KEYS"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- RENAME ---

    #[test]
    fn rename_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["RENAME", "old", "new"])).unwrap(),
            Command::Rename {
                key: "old".into(),
                newkey: "new".into()
            },
        );
    }

    #[test]
    fn rename_wrong_arity() {
        let err = Command::from_frame(cmd(&["RENAME", "only"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- AUTH ---

    #[test]
    fn auth_legacy() {
        assert_eq!(
            Command::from_frame(cmd(&["AUTH", "secret"])).unwrap(),
            Command::Auth {
                username: None,
                password: "secret".into()
            },
        );
    }

    #[test]
    fn auth_with_username() {
        assert_eq!(
            Command::from_frame(cmd(&["AUTH", "default", "secret"])).unwrap(),
            Command::Auth {
                username: Some("default".into()),
                password: "secret".into()
            },
        );
    }

    #[test]
    fn auth_no_args() {
        let err = Command::from_frame(cmd(&["AUTH"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn auth_too_many_args() {
        let err = Command::from_frame(cmd(&["AUTH", "a", "b", "c"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- QUIT ---

    #[test]
    fn quit_basic() {
        assert_eq!(Command::from_frame(cmd(&["QUIT"])).unwrap(), Command::Quit,);
    }

    #[test]
    fn quit_wrong_arity() {
        let err = Command::from_frame(cmd(&["QUIT", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- PROTO.REGISTER ---

    #[test]
    fn proto_register_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.REGISTER", "myschema", "descriptor"])).unwrap(),
            Command::ProtoRegister {
                name: "myschema".into(),
                descriptor: Bytes::from("descriptor"),
            },
        );
    }

    #[test]
    fn proto_register_case_insensitive() {
        assert!(Command::from_frame(cmd(&["proto.register", "s", "d"])).is_ok());
    }

    #[test]
    fn proto_register_wrong_arity() {
        let err = Command::from_frame(cmd(&["PROTO.REGISTER", "only"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- PROTO.SET ---

    #[test]
    fn proto_set_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.SET", "key1", "my.Type", "data"])).unwrap(),
            Command::ProtoSet {
                key: "key1".into(),
                type_name: "my.Type".into(),
                data: Bytes::from("data"),
                expire: None,
                nx: false,
                xx: false,
            },
        );
    }

    #[test]
    fn proto_set_with_ex() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.SET", "k", "t", "d", "EX", "60"])).unwrap(),
            Command::ProtoSet {
                key: "k".into(),
                type_name: "t".into(),
                data: Bytes::from("d"),
                expire: Some(SetExpire::Ex(60)),
                nx: false,
                xx: false,
            },
        );
    }

    #[test]
    fn proto_set_with_px_and_nx() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.SET", "k", "t", "d", "PX", "5000", "NX"])).unwrap(),
            Command::ProtoSet {
                key: "k".into(),
                type_name: "t".into(),
                data: Bytes::from("d"),
                expire: Some(SetExpire::Px(5000)),
                nx: true,
                xx: false,
            },
        );
    }

    #[test]
    fn proto_set_nx_xx_conflict() {
        let err = Command::from_frame(cmd(&["PROTO.SET", "k", "t", "d", "NX", "XX"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn proto_set_wrong_arity() {
        let err = Command::from_frame(cmd(&["PROTO.SET", "k", "t"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn proto_set_zero_expiry() {
        let err = Command::from_frame(cmd(&["PROTO.SET", "k", "t", "d", "EX", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    // --- PROTO.GET ---

    #[test]
    fn proto_get_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.GET", "key1"])).unwrap(),
            Command::ProtoGet { key: "key1".into() },
        );
    }

    #[test]
    fn proto_get_wrong_arity() {
        let err = Command::from_frame(cmd(&["PROTO.GET"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- PROTO.TYPE ---

    #[test]
    fn proto_type_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.TYPE", "key1"])).unwrap(),
            Command::ProtoType { key: "key1".into() },
        );
    }

    #[test]
    fn proto_type_wrong_arity() {
        let err = Command::from_frame(cmd(&["PROTO.TYPE"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- PROTO.SCHEMAS ---

    #[test]
    fn proto_schemas_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.SCHEMAS"])).unwrap(),
            Command::ProtoSchemas,
        );
    }

    #[test]
    fn proto_schemas_wrong_arity() {
        let err = Command::from_frame(cmd(&["PROTO.SCHEMAS", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- PROTO.DESCRIBE ---

    #[test]
    fn proto_describe_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.DESCRIBE", "myschema"])).unwrap(),
            Command::ProtoDescribe {
                name: "myschema".into()
            },
        );
    }

    #[test]
    fn proto_describe_wrong_arity() {
        let err = Command::from_frame(cmd(&["PROTO.DESCRIBE"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- proto.getfield ---

    #[test]
    fn proto_getfield_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.GETFIELD", "user:1", "name"])).unwrap(),
            Command::ProtoGetField {
                key: "user:1".into(),
                field_path: "name".into(),
            },
        );
    }

    #[test]
    fn proto_getfield_nested_path() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.GETFIELD", "key", "address.city"])).unwrap(),
            Command::ProtoGetField {
                key: "key".into(),
                field_path: "address.city".into(),
            },
        );
    }

    #[test]
    fn proto_getfield_wrong_arity() {
        let err = Command::from_frame(cmd(&["PROTO.GETFIELD"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));

        let err = Command::from_frame(cmd(&["PROTO.GETFIELD", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));

        let err =
            Command::from_frame(cmd(&["PROTO.GETFIELD", "key", "field", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- proto.setfield ---

    #[test]
    fn proto_setfield_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.SETFIELD", "user:1", "name", "bob"])).unwrap(),
            Command::ProtoSetField {
                key: "user:1".into(),
                field_path: "name".into(),
                value: "bob".into(),
            },
        );
    }

    #[test]
    fn proto_setfield_wrong_arity() {
        let err = Command::from_frame(cmd(&["PROTO.SETFIELD"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));

        let err = Command::from_frame(cmd(&["PROTO.SETFIELD", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));

        let err = Command::from_frame(cmd(&["PROTO.SETFIELD", "key", "field"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));

        let err = Command::from_frame(cmd(&["PROTO.SETFIELD", "key", "field", "value", "extra"]))
            .unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- proto.delfield ---

    #[test]
    fn proto_delfield_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["PROTO.DELFIELD", "user:1", "name"])).unwrap(),
            Command::ProtoDelField {
                key: "user:1".into(),
                field_path: "name".into(),
            },
        );
    }

    #[test]
    fn proto_delfield_wrong_arity() {
        let err = Command::from_frame(cmd(&["PROTO.DELFIELD"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));

        let err = Command::from_frame(cmd(&["PROTO.DELFIELD", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));

        let err =
            Command::from_frame(cmd(&["PROTO.DELFIELD", "key", "field", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }
}
