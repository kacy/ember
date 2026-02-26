//! Command parsing from RESP3 frames.
//!
//! Converts a parsed [`Frame`] (expected to be an array) into a typed
//! [`Command`] enum. This keeps protocol-level concerns separate from
//! the engine that actually executes commands.

use bytes::Bytes;

/// Expiration option for the SET and GETEX commands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetExpire {
    /// EX seconds — expire after N seconds.
    Ex(u64),
    /// PX milliseconds — expire after N milliseconds.
    Px(u64),
    /// EXAT unix-seconds — expire at an absolute unix timestamp (seconds).
    ExAt(u64),
    /// PXAT unix-milliseconds — expire at an absolute unix timestamp (milliseconds).
    PxAt(u64),
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

    /// GETRANGE `key` `start` `end`. Returns a substring of the string at key.
    GetRange { key: String, start: i64, end: i64 },

    /// SETRANGE `key` `offset` `value`. Overwrites part of the string at key.
    SetRange {
        key: String,
        offset: usize,
        value: Bytes,
    },

    /// GETBIT `key` `offset`. Returns the bit at `offset` in the string stored at key.
    ///
    /// Bit ordering is big-endian (Redis compatible): byte 0 holds bits 0–7, MSB first.
    /// Returns 0 if the key does not exist.
    GetBit { key: String, offset: u64 },

    /// SETBIT `key` `offset` `value`. Sets or clears the bit at `offset`.
    ///
    /// The string is automatically grown to accommodate the offset.
    /// Returns the original bit value.
    SetBit { key: String, offset: u64, value: u8 },

    /// BITCOUNT `key` \[start end \[BYTE\|BIT\]\]. Counts set bits in the string,
    /// optionally restricted to a byte or bit range.
    BitCount {
        key: String,
        range: Option<BitRange>,
    },

    /// BITPOS `key` `bit` \[start \[end \[BYTE\|BIT\]\]\]. Returns the position of the
    /// first set (`bit=1`) or clear (`bit=0`) bit in the string.
    BitPos {
        key: String,
        bit: u8,
        range: Option<BitRange>,
    },

    /// BITOP `operation` `destkey` `key` \[key ...\]. Performs a bitwise operation
    /// across source strings and stores the result in `destkey`.
    BitOp {
        op: BitOpKind,
        dest: String,
        keys: Vec<String>,
    },

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

    /// MSETNX `key` `value` \[key value ...\]. Sets multiple keys only if none already exist.
    /// Returns 1 if all keys were set, 0 if any key already existed (atomic: all-or-nothing).
    MSetNx { pairs: Vec<(String, Bytes)> },

    /// GETSET `key` `value`. Atomically sets `key` to `value` and returns the old value.
    /// Deprecated in Redis 6.2 but widely used; kept for compatibility.
    GetSet { key: String, value: Bytes },

    /// EXPIRE `key` `seconds`. Sets a TTL on an existing key.
    Expire { key: String, seconds: u64 },

    /// EXPIREAT `key` `timestamp`. Sets expiry at an absolute Unix timestamp (seconds).
    Expireat { key: String, timestamp: u64 },

    /// TTL `key`. Returns remaining time-to-live in seconds.
    Ttl { key: String },

    /// PERSIST `key`. Removes the expiration from a key.
    Persist { key: String },

    /// PTTL `key`. Returns remaining time-to-live in milliseconds.
    Pttl { key: String },

    /// PEXPIRE `key` `milliseconds`. Sets a TTL in milliseconds on an existing key.
    Pexpire { key: String, milliseconds: u64 },

    /// PEXPIREAT `key` `timestamp-ms`. Sets expiry at an absolute Unix timestamp (milliseconds).
    Pexpireat { key: String, timestamp_ms: u64 },

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

    /// CONFIG GET `pattern`. Returns matching server configuration parameters.
    ConfigGet { pattern: String },

    /// CONFIG SET `param` `value`. Sets a server configuration parameter at runtime.
    ConfigSet { param: String, value: String },

    /// CONFIG REWRITE. Writes the current config to the file loaded at startup.
    ConfigRewrite,

    /// MULTI. Starts a transaction — subsequent commands are queued until EXEC.
    Multi,

    /// EXEC. Executes all queued commands atomically, returning an array of results.
    Exec,

    /// DISCARD. Aborts a transaction, discarding all queued commands.
    Discard,

    /// SCAN `cursor` \[MATCH pattern\] \[COUNT count\]. Iterates keys.
    Scan {
        cursor: u64,
        pattern: Option<String>,
        count: Option<usize>,
    },

    /// SSCAN `key` `cursor` \[MATCH pattern\] \[COUNT count\]. Iterates set members.
    SScan {
        key: String,
        cursor: u64,
        pattern: Option<String>,
        count: Option<usize>,
    },

    /// HSCAN `key` `cursor` \[MATCH pattern\] \[COUNT count\]. Iterates hash fields.
    HScan {
        key: String,
        cursor: u64,
        pattern: Option<String>,
        count: Option<usize>,
    },

    /// ZSCAN `key` `cursor` \[MATCH pattern\] \[COUNT count\]. Iterates sorted set members.
    ZScan {
        key: String,
        cursor: u64,
        pattern: Option<String>,
        count: Option<usize>,
    },

    /// LPUSH `key` `value` \[value ...\]. Pushes values to the head of a list.
    LPush { key: String, values: Vec<Bytes> },

    /// RPUSH `key` `value` \[value ...\]. Pushes values to the tail of a list.
    RPush { key: String, values: Vec<Bytes> },

    /// LPOP `key` \[count\]. Pops one or more values from the head of a list.
    ///
    /// Without `count`: returns a bulk string (or nil). With `count`: returns
    /// an array of up to `count` elements (Redis 6.2+ semantics).
    LPop { key: String, count: Option<usize> },

    /// RPOP `key` \[count\]. Pops one or more values from the tail of a list.
    ///
    /// Without `count`: returns a bulk string (or nil). With `count`: returns
    /// an array of up to `count` elements (Redis 6.2+ semantics).
    RPop { key: String, count: Option<usize> },

    /// LRANGE `key` `start` `stop`. Returns a range of elements by index.
    LRange { key: String, start: i64, stop: i64 },

    /// LLEN `key`. Returns the length of a list.
    LLen { key: String },

    /// BLPOP `key` \[key ...\] `timeout`. Blocks until an element is available
    /// at the head of one of the given lists, or the timeout expires.
    BLPop {
        keys: Vec<String>,
        timeout_secs: f64,
    },

    /// BRPOP `key` \[key ...\] `timeout`. Blocks until an element is available
    /// at the tail of one of the given lists, or the timeout expires.
    BRPop {
        keys: Vec<String>,
        timeout_secs: f64,
    },

    /// LINDEX `key` `index`. Returns the element at `index` in the list.
    LIndex { key: String, index: i64 },

    /// LSET `key` `index` `element`. Sets the element at `index`.
    LSet {
        key: String,
        index: i64,
        value: Bytes,
    },

    /// LTRIM `key` `start` `stop`. Trims the list to the specified range.
    LTrim { key: String, start: i64, stop: i64 },

    /// LINSERT `key` BEFORE|AFTER `pivot` `element`. Inserts before or after pivot.
    LInsert {
        key: String,
        before: bool,
        pivot: Bytes,
        value: Bytes,
    },

    /// LREM `key` `count` `element`. Removes elements equal to value.
    LRem {
        key: String,
        count: i64,
        value: Bytes,
    },

    /// LPOS `key` `element` \[RANK rank\] \[COUNT count\] \[MAXLEN maxlen\].
    LPos {
        key: String,
        element: Bytes,
        rank: i64,
        count: Option<usize>,
        maxlen: usize,
    },

    /// LMOVE `source` `destination` LEFT|RIGHT LEFT|RIGHT.
    /// Atomically pops from the source list and pushes to the destination list.
    LMove {
        source: String,
        destination: String,
        /// Pop from the left (head) if true, right (tail) if false.
        src_left: bool,
        /// Push to the left (head) if true, right (tail) if false.
        dst_left: bool,
    },

    /// GETDEL `key`. Returns the value of a key and deletes it atomically.
    GetDel { key: String },

    /// GETEX `key` \[EX seconds | PX ms | EXAT timestamp | PXAT timestamp-ms | PERSIST\].
    /// Returns the value of a key and optionally updates its expiry.
    GetEx {
        key: String,
        /// `None` — no change; `Some(None)` — remove TTL (PERSIST); `Some(Some(_))` — set TTL.
        expire: Option<Option<SetExpire>>,
    },

    /// ZDIFF `numkeys` `key` \[key ...\] \[WITHSCORES\].
    /// Returns members in the first sorted set not present in the others.
    ZDiff {
        keys: Vec<String>,
        with_scores: bool,
    },

    /// ZINTER `numkeys` `key` \[key ...\] \[WITHSCORES\].
    /// Returns members present in all of the given sorted sets.
    ZInter {
        keys: Vec<String>,
        with_scores: bool,
    },

    /// ZUNION `numkeys` `key` \[key ...\] \[WITHSCORES\].
    /// Returns the union of all given sorted sets.
    ZUnion {
        keys: Vec<String>,
        with_scores: bool,
    },

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

    /// ZREVRANGE `key` `start` `stop` \[WITHSCORES\]. Returns a range by rank in reverse order.
    ZRevRange {
        key: String,
        start: i64,
        stop: i64,
        with_scores: bool,
    },

    /// ZREVRANK `key` `member`. Returns the reverse rank of a member.
    ZRevRank { key: String, member: String },

    /// ZCOUNT `key` `min` `max`. Counts members with scores in the given range.
    ZCount {
        key: String,
        min: ScoreBound,
        max: ScoreBound,
    },

    /// ZINCRBY `key` `increment` `member`. Increments the score of a member.
    ZIncrBy {
        key: String,
        increment: f64,
        member: String,
    },

    /// ZRANGEBYSCORE `key` `min` `max` \[WITHSCORES\] \[LIMIT offset count\].
    ZRangeByScore {
        key: String,
        min: ScoreBound,
        max: ScoreBound,
        with_scores: bool,
        offset: usize,
        count: Option<usize>,
    },

    /// ZREVRANGEBYSCORE `key` `max` `min` \[WITHSCORES\] \[LIMIT offset count\].
    ZRevRangeByScore {
        key: String,
        min: ScoreBound,
        max: ScoreBound,
        with_scores: bool,
        offset: usize,
        count: Option<usize>,
    },

    /// ZPOPMIN `key` \[count\]. Removes and returns the lowest scored members.
    ZPopMin { key: String, count: usize },

    /// ZPOPMAX `key` \[count\]. Removes and returns the highest scored members.
    ZPopMax { key: String, count: usize },

    /// LMPOP `numkeys` `key` \[key ...\] LEFT|RIGHT \[COUNT n\].
    /// Tries keys left-to-right, popping up to `count` elements from the first
    /// non-empty list. Returns `[key_name, [elem, ...]]` or nil if all empty.
    Lmpop {
        keys: Vec<String>,
        left: bool,
        count: usize,
    },

    /// ZMPOP `numkeys` `key` \[key ...\] MIN|MAX \[COUNT n\].
    /// Tries keys left-to-right, popping up to `count` elements from the first
    /// non-empty sorted set. Returns `[key_name, [[member, score], ...]]` or nil.
    Zmpop {
        keys: Vec<String>,
        min: bool,
        count: usize,
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

    /// SUNION `key` \[key ...\]. Returns the union of all given sets.
    SUnion { keys: Vec<String> },

    /// SINTER `key` \[key ...\]. Returns the intersection of all given sets.
    SInter { keys: Vec<String> },

    /// SDIFF `key` \[key ...\]. Returns members of the first set not in the others.
    SDiff { keys: Vec<String> },

    /// SUNIONSTORE `destination` `key` \[key ...\]. Stores the union into `destination`.
    SUnionStore { dest: String, keys: Vec<String> },

    /// SINTERSTORE `destination` `key` \[key ...\]. Stores the intersection into `destination`.
    SInterStore { dest: String, keys: Vec<String> },

    /// SDIFFSTORE `destination` `key` \[key ...\]. Stores the difference into `destination`.
    SDiffStore { dest: String, keys: Vec<String> },

    /// SRANDMEMBER `key` \[count\]. Returns random members without removing them.
    SRandMember { key: String, count: Option<i64> },

    /// SPOP `key` \[count\]. Removes and returns random members.
    SPop { key: String, count: usize },

    /// SMISMEMBER `key` `member` \[member ...\]. Checks multiple members at once.
    SMisMember { key: String, members: Vec<String> },

    /// SMOVE `source` `destination` `member`. Atomically moves a member from one set to another.
    /// Returns 1 if moved, 0 if the member was not in the source set.
    SMove {
        source: String,
        destination: String,
        member: String,
    },

    /// SINTERCARD `numkeys` `key` \[key ...\] \[LIMIT count\]. Returns the cardinality of the set
    /// intersection. If LIMIT is given and nonzero, the result is capped at that value.
    SInterCard { keys: Vec<String>, limit: usize },

    /// EXPIRETIME `key`. Returns the absolute Unix timestamp (seconds) when the key expires.
    /// Returns -1 if the key has no expiry, -2 if the key does not exist.
    Expiretime { key: String },

    /// PEXPIRETIME `key`. Returns the absolute Unix timestamp (milliseconds) when the key expires.
    /// Returns -1 if the key has no expiry, -2 if the key does not exist.
    Pexpiretime { key: String },

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

    /// CLUSTER ADDSLOTSRANGE `start` `end` \[start end ...\]. Assign a contiguous range of hash slots.
    ClusterAddSlotsRange { ranges: Vec<(u16, u16)> },

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

    /// RESTORE `key` `ttl` `serialized-value` \[REPLACE\].
    /// Insert a key from serialized data (used by MIGRATE).
    Restore {
        key: String,
        ttl_ms: u64,
        data: Bytes,
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

    /// VADD_BATCH key DIM n element1 f32... element2 f32... [METRIC COSINE|L2|IP]
    /// [QUANT F32|F16|I8] [M n] [EF n]. Adds multiple vectors in a single command.
    VAddBatch {
        key: String,
        entries: Vec<(String, Vec<f32>)>,
        dim: usize,
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

    // --- client commands ---
    /// CLIENT ID. Returns the unique ID of the current connection.
    ClientId,

    /// CLIENT SETNAME `name`. Sets a human-readable name for the connection.
    ClientSetName { name: String },

    /// CLIENT GETNAME. Returns the name set by CLIENT SETNAME, or nil.
    ClientGetName,

    /// CLIENT LIST. Returns info about all connected clients.
    ClientList,

    /// AUTH \[username\] password. Authenticate the connection.
    Auth {
        /// Username for ACL-style auth. None for legacy AUTH.
        username: Option<String>,
        /// The password to validate.
        password: String,
    },

    // --- ACL commands ---
    /// ACL WHOAMI. Returns the username of the current connection.
    AclWhoAmI,

    /// ACL LIST. Returns all users and their ACL rules.
    AclList,

    /// ACL USERS. Returns all usernames.
    AclUsers,

    /// ACL GETUSER `username`. Returns detailed info about a user.
    AclGetUser { username: String },

    /// ACL DELUSER `username` \[username ...\]. Deletes users.
    AclDelUser { usernames: Vec<String> },

    /// ACL SETUSER `username` \[rule ...\]. Creates or modifies a user.
    AclSetUser {
        username: String,
        rules: Vec<String>,
    },

    /// ACL CAT \[category\]. Lists categories, or commands in a category.
    AclCat { category: Option<String> },

    /// WATCH `key` \[key ...\]. Marks keys for optimistic locking.
    /// If any watched key is modified before EXEC, the transaction aborts.
    Watch { keys: Vec<String> },

    /// UNWATCH. Clears all watched keys for the current connection.
    Unwatch,

    /// TIME. Returns the current server time as \[unix_seconds, microseconds\].
    Time,

    /// LASTSAVE. Returns the unix timestamp of the last successful save.
    LastSave,

    /// ROLE. Returns the replication role of the server.
    Role,

    /// WAIT numreplicas timeout-ms
    ///
    /// Blocks until `numreplicas` replicas have acknowledged all write
    /// commands processed before this WAIT, or until `timeout_ms`
    /// milliseconds elapse. Returns the count of replicas that
    /// acknowledged in time.
    Wait { numreplicas: u64, timeout_ms: u64 },

    /// OBJECT ENCODING `key`. Returns the internal encoding of the value.
    ObjectEncoding { key: String },

    /// OBJECT REFCOUNT `key`. Returns the reference count of the value (always 1).
    ObjectRefcount { key: String },

    /// COPY `source` `destination` \[DB db\] \[REPLACE\]. Copies the value at source to destination.
    Copy {
        source: String,
        destination: String,
        replace: bool,
    },

    /// QUIT. Requests the server to close the connection.
    Quit,

    /// MONITOR. Streams all commands processed by the server.
    Monitor,

    /// RANDOMKEY. Returns a random key from the database, or nil if empty.
    RandomKey,

    /// TOUCH `key` \[key ...\]. Updates last access time, returns count of existing keys.
    Touch { keys: Vec<String> },

    /// SORT `key` \[ASC|DESC\] \[ALPHA\] \[LIMIT offset count\] \[STORE dest\].
    /// Sorts a list, set, or sorted set and returns the sorted elements.
    Sort {
        key: String,
        desc: bool,
        alpha: bool,
        limit: Option<(i64, i64)>,
        store: Option<String>,
    },

    /// A command we don't recognize (yet).
    Unknown(String),
}

/// Unit for BITCOUNT and BITPOS range arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitRangeUnit {
    /// Byte-granularity range (default for Redis).
    Byte,
    /// Bit-granularity range (Redis 7.0+).
    Bit,
}

/// Range argument for BITCOUNT and BITPOS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitRange {
    pub start: i64,
    pub end: i64,
    pub unit: BitRangeUnit,
}

/// Operation kind for BITOP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitOpKind {
    And,
    Or,
    Xor,
    Not,
}

/// A score bound for sorted set range queries (ZRANGEBYSCORE, ZCOUNT, etc.).
///
/// Redis supports `-inf`, `+inf`, inclusive (default), and exclusive
/// bounds (prefixed with `(`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScoreBound {
    /// Negative infinity — matches all scores from the bottom.
    NegInf,
    /// Positive infinity — matches all scores to the top.
    PosInf,
    /// Inclusive bound: score >= value (for min) or score <= value (for max).
    Inclusive(f64),
    /// Exclusive bound: score > value (for min) or score < value (for max).
    Exclusive(f64),
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

mod attributes;
mod parse;
#[cfg(test)]
mod tests;
