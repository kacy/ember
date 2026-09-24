//! The requests a shard runs and the responses it sends back.

use super::*;

/// Generates the [`ShardRequest`] enum and its `is_write()` method from
/// read/write variant groupings. Adding a new command forces you to place
/// it in the correct group — the compiler enforces it.
macro_rules! shard_request {
    (
        read {
            $( $(#[$rmeta:meta])* $rvar:ident $({ $( $(#[$rfmeta:meta])* $rfield:ident : $rty:ty ),* $(,)? })? ),* $(,)?
        }
        write {
            $( $(#[$wmeta:meta])* $wvar:ident $({ $( $(#[$wfmeta:meta])* $wfield:ident : $wty:ty ),* $(,)? })? ),* $(,)?
        }
    ) => {
        /// A protocol-agnostic command sent to a shard.
        #[derive(Debug)]
        pub enum ShardRequest {
            $( $(#[$rmeta])* $rvar $({ $( $(#[$rfmeta])* $rfield : $rty ),* })?, )*
            $( $(#[$wmeta])* $wvar $({ $( $(#[$wfmeta])* $wfield : $wty ),* })?, )*
        }

        impl ShardRequest {
            /// Returns `true` if this request mutates the keyspace and should be
            /// rejected when the AOF disk is full. Read-only operations, admin
            /// commands, and scan operations always proceed.
            pub(super) fn is_write(&self) -> bool {
                #[allow(unreachable_patterns, unused_doc_comments)]
                match self {
                    $( $(#[$wmeta])* Self::$wvar { .. } => true, )*
                    _ => false,
                }
            }
        }
    };
}

shard_request! {
    read {
        // --- strings (read) ---
        Get { key: String },
        Strlen { key: String },
        GetRange { key: String, start: i64, end: i64 },
        /// GETBIT key offset. Returns the bit at `offset` (0 or 1). Big-endian ordering.
        GetBit { key: String, offset: u64 },
        /// BITCOUNT key [range]. Counts set bits, optionally restricted to a range.
        BitCount { key: String, range: Option<BitRange> },
        /// BITPOS key bit [range]. Finds first set or clear bit position.
        BitPos { key: String, bit: u8, range: Option<BitRange> },

        // --- keyspace (read) ---
        /// Returns all keys matching a glob pattern in this shard.
        Keys { pattern: String },
        /// Returns the internal encoding name for the value at key.
        ObjectEncoding { key: String },
        Exists { key: String },
        /// Returns a random key from the shard's keyspace.
        RandomKey,
        /// Updates last access time for a key. Returns bool (existed).
        Touch { key: String },
        /// Sorts elements from a list, set, or sorted set in this shard.
        Sort { key: String, desc: bool, alpha: bool, limit: Option<(i64, i64)> },
        Ttl { key: String },
        /// MEMORY USAGE. Returns the estimated memory footprint of a key in bytes.
        MemoryUsage { key: String },
        Pttl { key: String },
        Type { key: String },
        /// EXPIRETIME — returns the absolute expiry timestamp in seconds (-1 or -2 for missing/no-expiry).
        Expiretime { key: String },
        /// PEXPIRETIME — returns the absolute expiry timestamp in milliseconds (-1 or -2 for missing/no-expiry).
        Pexpiretime { key: String },

        // --- lists (read) ---
        LRange { key: String, start: i64, stop: i64 },
        LLen { key: String },
        LIndex { key: String, index: i64 },
        LPos { key: String, element: Bytes, rank: i64, count: usize, maxlen: usize },

        // --- sorted sets (read) ---
        ZScore { key: String, member: String },
        ZRank { key: String, member: String },
        ZRevRank { key: String, member: String },
        ZCard { key: String },
        ZRange { key: String, start: i64, stop: i64, with_scores: bool },
        ZRevRange { key: String, start: i64, stop: i64, with_scores: bool },
        ZCount { key: String, min: ScoreBound, max: ScoreBound },
        ZRangeByScore { key: String, min: ScoreBound, max: ScoreBound, offset: usize, count: Option<usize> },
        ZRevRangeByScore { key: String, min: ScoreBound, max: ScoreBound, offset: usize, count: Option<usize> },
        /// ZDIFF: returns members in the first sorted set not in the others.
        ZDiff { keys: Vec<String> },
        /// ZINTER: returns members present in all sorted sets, scores summed.
        ZInter { keys: Vec<String> },
        /// ZUNION: returns the union of all sorted sets, scores summed.
        ZUnion { keys: Vec<String> },
        /// ZRANDMEMBER — returns random member(s) from a sorted set; read-only, no AOF.
        ZRandMember { key: String, count: Option<i64>, with_scores: bool },

        // --- hashes (read) ---
        HGet { key: String, field: String },
        HGetAll { key: String },
        HExists { key: String, field: String },
        HLen { key: String },
        HKeys { key: String },
        HVals { key: String },
        HMGet { key: String, fields: Vec<String> },
        /// HRANDFIELD — returns random field(s) from a hash; read-only, no AOF.
        HRandField { key: String, count: Option<i64>, with_values: bool },

        // --- sets (read) ---
        SMembers { key: String },
        SIsMember { key: String, member: String },
        SCard { key: String },
        SUnion { keys: Vec<String> },
        SInter { keys: Vec<String> },
        SDiff { keys: Vec<String> },
        SRandMember { key: String, count: i64 },
        SMisMember { key: String, members: Vec<String> },
        /// SINTERCARD — returns cardinality of set intersection, capped at limit (0 = no limit).
        SInterCard { keys: Vec<String>, limit: usize },

        // --- admin / stats ---
        /// Returns the key count for this shard.
        DbSize,
        /// Returns keyspace stats for this shard.
        Stats,
        /// Returns the current version of a key for WATCH optimistic locking.
        /// Read-only, no AOF, no replication — cold path only.
        KeyVersion { key: String },
        /// Applies a live memory configuration update to this shard.
        ///
        /// Sent by the server when CONFIG SET maxmemory or maxmemory-policy
        /// is changed at runtime. Takes effect on the next write check.
        UpdateMemoryConfig { max_memory: Option<usize>, eviction_policy: EvictionPolicy },
        /// Triggers a snapshot write.
        Snapshot,
        /// Serializes the current shard state to bytes (in-memory snapshot).
        ///
        /// Used by the replication server to capture a consistent shard
        /// snapshot for transmission to a new replica without filesystem I/O.
        SerializeSnapshot,
        /// Triggers an AOF rewrite (snapshot + truncate AOF).
        RewriteAof,

        // --- scan operations ---
        /// Scans keys in the keyspace.
        Scan { cursor: u64, count: usize, pattern: Option<String> },
        /// Incrementally iterates set members.
        SScan { key: String, cursor: u64, count: usize, pattern: Option<String> },
        /// Incrementally iterates hash fields.
        HScan { key: String, cursor: u64, count: usize, pattern: Option<String> },
        /// Incrementally iterates sorted set members.
        ZScan { key: String, cursor: u64, count: usize, pattern: Option<String> },

        // --- cluster ---
        /// Counts keys in this shard that hash to the given cluster slot.
        CountKeysInSlot { slot: u16 },
        /// Returns up to `count` keys that hash to the given cluster slot.
        GetKeysInSlot { slot: u16, count: usize },
        /// Dumps a key's value as serialized bytes for MIGRATE.
        DumpKey { key: String },

        // --- vector (read) ---
        /// Searches for nearest neighbors in a vector set.
        #[cfg(feature = "vector")]
        VSim { key: String, query: Vec<f32>, count: usize, ef_search: usize },
        /// Gets the stored vector for an element.
        #[cfg(feature = "vector")]
        VGet { key: String, element: String },
        /// Returns the number of elements in a vector set.
        #[cfg(feature = "vector")]
        VCard { key: String },
        /// Returns the dimensionality of a vector set.
        #[cfg(feature = "vector")]
        VDim { key: String },
        /// Returns metadata about a vector set.
        #[cfg(feature = "vector")]
        VInfo { key: String },

        // --- protobuf (read) ---
        /// Retrieves a protobuf value.
        #[cfg(feature = "protobuf")]
        ProtoGet { key: String },
        /// Returns the protobuf message type name for a key.
        #[cfg(feature = "protobuf")]
        ProtoType { key: String },
        /// Cursor-based scan over proto keys, optionally filtered by type name.
        #[cfg(feature = "protobuf")]
        ProtoScan { cursor: u64, count: usize, pattern: Option<String>, type_name: Option<String> },
        /// Cursor-based scan over proto keys, returning those where the given
        /// field equals the given value.
        #[cfg(feature = "protobuf")]
        ProtoFind {
            cursor: u64,
            count: usize,
            pattern: Option<String>,
            type_name: Option<String>,
            field_path: String,
            field_value: String,
        },
    }

    write {
        // --- strings (write) ---
        Set { key: String, value: Bytes, expire: Option<Duration>, nx: bool, xx: bool },
        Incr { key: String },
        Decr { key: String },
        IncrBy { key: String, delta: i64 },
        DecrBy { key: String, delta: i64 },
        IncrByFloat { key: String, delta: f64 },
        Append { key: String, value: Bytes },
        SetRange { key: String, offset: usize, value: Bytes },
        /// SETBIT key offset value. Sets the bit at `offset` to 0 or 1. Returns old bit.
        SetBit { key: String, offset: u64, value: u8 },
        /// BITOP op destkey key [key ...]. Bitwise operation across strings.
        BitOp { op: BitOpKind, dest: String, keys: Vec<String> },

        // --- keyspace (write) ---
        /// Renames a key within this shard.
        Rename { key: String, newkey: String },
        /// Copies the value at source to destination within this shard.
        Copy { source: String, destination: String, replace: bool },
        Del { key: String },
        /// Like DEL but defers value deallocation to the background drop thread.
        Unlink { key: String },
        Expire { key: String, seconds: u64 },
        Persist { key: String },
        Pexpire { key: String, milliseconds: u64 },
        /// EXPIREAT: set expiry at an absolute Unix timestamp (seconds).
        Expireat { key: String, timestamp: u64 },
        /// PEXPIREAT: set expiry at an absolute Unix timestamp (milliseconds).
        Pexpireat { key: String, timestamp_ms: u64 },
        /// GETDEL: returns the value at key and deletes it.
        GetDel { key: String },
        /// GETSET: atomically sets key to a new value and returns the old value.
        GetSet { key: String, value: Bytes },
        /// GETEX: returns the value at key and optionally updates its TTL.
        ///
        /// `expire`: `None` = no change, `Some(None)` = persist, `Some(Some(ms))` = new TTL in ms.
        GetEx { key: String, expire: Option<Option<u64>> },
        /// MSETNX: sets multiple keys only if none already exist (atomic all-or-nothing).
        MSetNx { pairs: Vec<(String, Bytes)> },
        /// Clears all keys from the keyspace.
        FlushDb,
        /// Clears all keys, deferring deallocation to the background drop thread.
        FlushDbAsync,
        /// Restores a key from serialized bytes (received via MIGRATE).
        RestoreKey { key: String, ttl_ms: u64, data: bytes::Bytes, replace: bool },

        // --- lists (write) ---
        LPush { key: String, values: Vec<Bytes> },
        RPush { key: String, values: Vec<Bytes> },
        LPop { key: String },
        RPop { key: String },
        /// LPOP key count — pop up to `count` elements from the list head, returning an array.
        LPopCount { key: String, count: usize },
        /// RPOP key count — pop up to `count` elements from the list tail, returning an array.
        RPopCount { key: String, count: usize },
        /// Blocking left-pop. If the list has elements, pops immediately and sends
        /// the result on `waiter`. If empty, the shard registers the waiter to be
        /// woken when an element is pushed. Uses an mpsc sender so multiple shards
        /// can race to deliver the first result to a single receiver.
        BLPop { key: String, waiter: mpsc::Sender<(String, Bytes)> },
        /// Blocking right-pop. Same semantics as BLPop but pops from the tail.
        BRPop { key: String, waiter: mpsc::Sender<(String, Bytes)> },
        LSet { key: String, index: i64, value: Bytes },
        LTrim { key: String, start: i64, stop: i64 },
        LInsert { key: String, before: bool, pivot: Bytes, value: Bytes },
        LRem { key: String, count: i64, value: Bytes },
        /// LMPOP single-key sub-request: pop up to `count` items from one list.
        LmpopSingle { key: String, left: bool, count: usize },
        /// LMOVE: atomically pops from source and pushes to destination.
        LMove { source: String, destination: String, src_left: bool, dst_left: bool },

        // --- sorted sets (write) ---
        ZAdd { key: String, members: Vec<(f64, String)>, nx: bool, xx: bool, gt: bool, lt: bool, ch: bool },
        ZRem { key: String, members: Vec<String> },
        ZIncrBy { key: String, increment: f64, member: String },
        ZPopMin { key: String, count: usize },
        ZPopMax { key: String, count: usize },
        /// ZMPOP single-key sub-request: pop up to `count` items from one sorted set.
        ZmpopSingle { key: String, min: bool, count: usize },
        /// ZDIFFSTORE destkey numkeys key [key ...] — stores diff result in dest.
        ZDiffStore { dest: String, keys: Vec<String> },
        /// ZINTERSTORE destkey numkeys key [key ...] — stores intersection in dest.
        ZInterStore { dest: String, keys: Vec<String> },
        /// ZUNIONSTORE destkey numkeys key [key ...] — stores union in dest.
        ZUnionStore { dest: String, keys: Vec<String> },

        // --- hashes (write) ---
        HSet { key: String, fields: Vec<(String, Bytes)> },
        HDel { key: String, fields: Vec<String> },
        HIncrBy { key: String, field: String, delta: i64 },
        /// HINCRBYFLOAT key field increment — increments a hash field by a float.
        HIncrByFloat { key: String, field: String, delta: f64 },

        // --- sets (write) ---
        SAdd { key: String, members: Vec<String> },
        SRem { key: String, members: Vec<String> },
        SPop { key: String, count: usize },
        SUnionStore { dest: String, keys: Vec<String> },
        SInterStore { dest: String, keys: Vec<String> },
        SDiffStore { dest: String, keys: Vec<String> },
        /// SMOVE — atomically moves a member between two sets on the same shard.
        SMove { source: String, destination: String, member: String },

        // --- vector (write) ---
        /// Adds a vector to a vector set.
        #[cfg(feature = "vector")]
        VAdd { key: String, element: String, vector: Vec<f32>, metric: u8, quantization: u8, connectivity: u32, expansion_add: u32 },
        /// Adds multiple vectors to a vector set in a single command.
        #[cfg(feature = "vector")]
        VAddBatch { key: String, entries: Vec<(String, Vec<f32>)>, dim: usize, metric: u8, quantization: u8, connectivity: u32, expansion_add: u32 },
        /// Removes an element from a vector set.
        #[cfg(feature = "vector")]
        VRem { key: String, element: String },

        // --- protobuf (write) ---
        /// Stores a validated protobuf value.
        #[cfg(feature = "protobuf")]
        ProtoSet { key: String, type_name: String, data: Bytes, expire: Option<Duration>, nx: bool, xx: bool },
        /// Writes a ProtoRegister AOF record (no keyspace mutation).
        /// Broadcast to all shards after a schema registration so the
        /// schema is recovered from any shard's AOF on restart.
        #[cfg(feature = "protobuf")]
        ProtoRegisterAof { name: String, descriptor: Bytes },
        /// Atomically reads a proto value, sets a field, and writes it back.
        /// Runs entirely within the shard's single-threaded dispatch.
        #[cfg(feature = "protobuf")]
        ProtoSetField { key: String, field_path: String, value: String },
        /// Atomically reads a proto value, clears a field, and writes it back.
        /// Runs entirely within the shard's single-threaded dispatch.
        #[cfg(feature = "protobuf")]
        ProtoDelField { key: String, field_path: String },
    }
}

/// The shard's response to a request.
#[derive(Debug)]
pub enum ShardResponse {
    /// A value (or None for a cache miss).
    Value(Option<Value>),
    /// Simple acknowledgement (e.g. SET).
    Ok,
    /// Integer result (e.g. INCR, DECR).
    Integer(i64),
    /// Boolean result (e.g. DEL, EXISTS, EXPIRE).
    Bool(bool),
    /// TTL query result.
    Ttl(TtlResult),
    /// Memory limit reached and eviction policy is NoEviction.
    OutOfMemory,
    /// Key count for a shard (DBSIZE).
    KeyCount(usize),
    /// Full stats for a shard (INFO).
    Stats(KeyspaceStats),
    /// Integer length result (e.g. LPUSH, RPUSH, LLEN).
    Len(usize),
    /// Array of bulk values (e.g. LRANGE).
    Array(Vec<Bytes>),
    /// The type name of a stored value.
    TypeName(&'static str),
    /// The encoding name of a stored value, or None if the key doesn't exist.
    EncodingName(Option<&'static str>),
    /// ZADD result: count for the client + actually applied members for AOF.
    ZAddLen {
        count: usize,
        applied: Vec<(f64, String)>,
    },
    /// ZREM result: count for the client + actually removed members for AOF.
    ZRemLen { count: usize, removed: Vec<String> },
    /// Float score result (e.g. ZSCORE).
    Score(Option<f64>),
    /// Rank result (e.g. ZRANK).
    Rank(Option<usize>),
    /// Scored array of (member, score) pairs (e.g. ZRANGE).
    ScoredArray(Vec<(String, f64)>),
    /// ZINCRBY result: new score + the member/score for AOF persistence.
    ZIncrByResult { new_score: f64, member: String },
    /// ZPOPMIN/ZPOPMAX result: popped members for both response and AOF.
    ZPopResult(Vec<(String, f64)>),
    /// A bulk string result (e.g. INCRBYFLOAT).
    BulkString(String),
    /// Command used against a key holding the wrong kind of value.
    WrongType,
    /// An error message.
    Err(String),
    /// Scan result: next cursor and list of keys.
    Scan { cursor: u64, keys: Vec<String> },
    /// SSCAN/HSCAN/ZSCAN result: next cursor and pre-flattened items.
    CollectionScan { cursor: u64, items: Vec<Bytes> },
    /// HGETALL result: all field-value pairs.
    HashFields(Vec<(String, Bytes)>),
    /// HRANDFIELD result: field names with optional values.
    HRandFieldResult(Vec<(String, Option<Bytes>)>),
    /// ZRANDMEMBER result: member names with optional scores.
    ZRandMemberResult(Vec<(String, Option<f64>)>),
    /// HDEL result: removed count + field names for AOF.
    HDelLen { count: usize, removed: Vec<String> },
    /// Array of strings (e.g. HKEYS).
    StringArray(Vec<String>),
    /// Array of integer positions (e.g. LPOS).
    IntegerArray(Vec<i64>),
    /// Array of booleans (e.g. SMISMEMBER).
    BoolArray(Vec<bool>),
    /// SUNIONSTORE/SINTERSTORE/SDIFFSTORE result: count + stored members for AOF.
    SetStoreResult { count: usize, members: Vec<String> },
    /// ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE result: count + scored members for AOF.
    ZStoreResult {
        count: usize,
        /// (score, member) pairs stored in dest.
        members: Vec<(f64, String)>,
    },
    /// Serialized key dump with remaining TTL (for MIGRATE/DUMP).
    KeyDump { data: Vec<u8>, ttl_ms: i64 },
    /// In-memory snapshot of the full shard state (for replication).
    ///
    /// `offset` is the shard's replication offset when the snapshot was
    /// taken: the snapshot holds every event up to and including it.
    SnapshotData {
        shard_id: u16,
        offset: u64,
        data: Vec<u8>,
    },
    /// HMGET result: array of optional values.
    OptionalArray(Vec<Option<Bytes>>),
    /// VADD result: element, vector, and whether it was newly added.
    #[cfg(feature = "vector")]
    VAddResult {
        element: String,
        vector: Vec<f32>,
        added: bool,
    },
    /// VADD_BATCH result: count of newly added elements + applied entries for AOF.
    #[cfg(feature = "vector")]
    VAddBatchResult {
        added_count: usize,
        applied: Vec<(String, Vec<f32>)>,
    },
    /// VSIM result: nearest neighbors with distances.
    #[cfg(feature = "vector")]
    VSimResult(Vec<(String, f32)>),
    /// VGET result: stored vector or None.
    #[cfg(feature = "vector")]
    VectorData(Option<Vec<f32>>),
    /// VINFO result: vector set metadata.
    #[cfg(feature = "vector")]
    VectorInfo(Option<Vec<(String, String)>>),
    /// PROTO.GET result: (type_name, data, remaining_ttl) or None.
    #[cfg(feature = "protobuf")]
    ProtoValue(Option<(String, Bytes, Option<Duration>)>),
    /// PROTO.TYPE result: message type name or None.
    #[cfg(feature = "protobuf")]
    ProtoTypeName(Option<String>),
    /// Result of an atomic SETFIELD/DELFIELD: carries the updated value
    /// for AOF persistence.
    #[cfg(feature = "protobuf")]
    ProtoFieldUpdated {
        type_name: String,
        data: Bytes,
        expire: Option<Duration>,
    },
    /// Key version for WATCH optimistic locking. `None` means missing/expired.
    Version(Option<u64>),
}

/// A request (or batch of requests) bundled with reply channels.
///
/// The `Batch` variant reduces channel traffic during pipelining: instead
/// of N individual sends (one per pipelined command), the connection handler
/// groups commands by target shard and sends one `Batch` message per shard.
/// This cuts channel contention from O(pipeline_depth) to O(shard_count).
///
/// The `SingleReusable` variant avoids per-command `oneshot::channel()`
/// allocation on the P=1 (no pipeline) path. The connection handler keeps
/// a long-lived `mpsc::channel(1)` and reuses it across commands.
#[derive(Debug)]
pub enum ShardMessage {
    /// A single request with its reply channel.
    Single {
        request: ShardRequest,
        reply: oneshot::Sender<ShardResponse>,
    },
    /// A single request using a reusable mpsc reply channel.
    ///
    /// Avoids the heap allocation of `oneshot::channel()` on every command.
    /// Used for the P=1 fast path where the connection handler sends one
    /// command at a time and waits for the response before sending the next.
    SingleReusable {
        request: ShardRequest,
        reply: mpsc::Sender<ShardResponse>,
    },
    /// Multiple requests batched for a single channel send.
    Batch(Vec<(ShardRequest, oneshot::Sender<ShardResponse>)>),
}
