//! The command table: every command name the server accepts, with its
//! parser, the metadata the COMMAND command reports, and example calls.
//!
//! `Command::from_frame` looks names up here, so a command can't be parsed
//! without an entry. The tests check each entry's flags and key positions
//! against the `Command` methods.

use super::*;

/// Handles COMMAND [COUNT | INFO name... | DOCS name... | LIST].
///
/// Provides static command metadata for client library discovery.
/// The format matches Redis 7 conventions so clients can probe capabilities
/// without falling back to error-handling paths.
pub fn handle_command_cmd(subcommand: Option<&str>, args: &[String]) -> Frame {
    match subcommand {
        None | Some("LIST") => Frame::Array(COMMAND_TABLE.iter().map(command_entry).collect()),
        Some("COUNT") => Frame::Integer(COMMAND_TABLE.len() as i64),
        Some("INFO") => {
            if args.is_empty() {
                return Frame::Array(COMMAND_TABLE.iter().map(command_entry).collect());
            }
            let frames = args
                .iter()
                .map(|name| {
                    let upper = name.to_ascii_uppercase();
                    match COMMAND_TABLE.iter().find(|e| e.name == upper) {
                        Some(entry) => command_entry(entry),
                        None => Frame::Null,
                    }
                })
                .collect();
            Frame::Array(frames)
        }
        Some("DOCS") => {
            // return empty docs — clients use this for documentation display,
            // not capability detection. an empty map per command is valid.
            if args.is_empty() {
                return Frame::Array(vec![]);
            }
            let mut frames = Vec::with_capacity(args.len() * 2);
            for name in args {
                let upper = name.to_ascii_uppercase();
                frames.push(Frame::Bulk(Bytes::from(upper)));
                frames.push(Frame::Array(vec![]));
            }
            Frame::Array(frames)
        }
        Some("GETKEYS") => Frame::Array(vec![]),
        Some(other) => Frame::Error(format!("ERR unknown COMMAND subcommand '{other}'")),
    }
}

/// Builds a COMMAND entry array for a single command.
///
/// Format: [name, arity, [flags], first_key, last_key, step]
fn command_entry(e: &CommandEntry) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from(e.name.to_ascii_lowercase())),
        Frame::Integer(e.arity),
        Frame::Array(e.flags.iter().map(|f| Frame::Simple((*f).into())).collect()),
        Frame::Integer(e.first_key),
        Frame::Integer(e.last_key),
        Frame::Integer(e.step),
    ])
}

/// One command the server accepts: the metadata COMMAND reports for it and
/// example calls.
struct CommandEntry {
    name: &'static str,
    arity: i64,
    flags: &'static [&'static str],
    first_key: i64,
    last_key: i64,
    step: i64,
    /// Calls that parse, one per subcommand where the command has them.
    /// ACL CAT and the table tests build commands from these.
    examples: &'static [&'static str],
}

/// Returns the example call of every entry, such as `"GET k"`.
pub fn examples() -> impl Iterator<Item = &'static str> {
    COMMAND_TABLE
        .iter()
        .flat_map(|e| e.examples.iter().copied())
}

/// Parses an example call into a command.
pub fn parse_example(example: &str) -> Result<Command, ProtocolError> {
    let args = example
        .split(' ')
        .map(|word| Frame::Bulk(Bytes::copy_from_slice(word.as_bytes())))
        .collect();
    Command::from_frame(Frame::Array(args))
}

/// Builds [`COMMAND_TABLE`] and [`parse_named`] from one list, so every
/// name the parser accepts has an entry. `parse_named` is a `match` rather
/// than a table lookup because the compiled match is about twice as fast.
macro_rules! command_table {
    ($((
        $name:literal, $parse:expr, $arity:expr, [$($flag:literal),* $(,)?],
        $first:expr, $last:expr, $step:expr, [$($example:literal),+ $(,)?] $(,)?
    )),+ $(,)?) => {
        /// Every command the server accepts, sorted by name. Arity counts
        /// the command name: positive is exact, negative is a minimum.
        /// `movablekeys` marks commands whose keys aren't all at the fixed
        /// positions.
        static COMMAND_TABLE: &[CommandEntry] = &[$(CommandEntry {
            name: $name,
            arity: $arity,
            flags: &[$($flag),*],
            first_key: $first,
            last_key: $last,
            step: $step,
            examples: &[$($example),+],
        }),+];

        /// Parses a command whose uppercase name is `name`. `frames` holds
        /// the name and then the arguments. A name not in the table parses
        /// as [`Command::Unknown`].
        pub(super) fn parse_named(
            name: &str,
            frames: &[Frame],
        ) -> Result<Command, ProtocolError> {
            let args = &frames[1..];
            match name {
                $($name => ($parse)(args),)+
                _ => Ok(Command::Unknown(extract_string(&frames[0])?)),
            }
        }
    };
}

command_table! {
    ("ACL", parse_acl, -2, ["admin", "noscript", "loading"], 0, 0, 0,
        ["ACL WHOAMI", "ACL LIST", "ACL USERS", "ACL GETUSER u", "ACL DELUSER u",
         "ACL SETUSER u", "ACL CAT"]),
    ("APPEND", parse_append, 3, ["write", "denyoom", "fast"], 1, 1, 1, ["APPEND k v"]),
    ("ASKING", parse_asking, 1, ["fast"], 0, 0, 0, ["ASKING"]),
    ("AUTH", parse_auth, -2, ["noscript", "loading", "fast", "no_auth"], 0, 0, 0,
        ["AUTH pw"]),
    ("BGREWRITEAOF", parse_bgrewriteaof, 1, ["admin"], 0, 0, 0, ["BGREWRITEAOF"]),
    ("BGSAVE", parse_bgsave, -1, ["admin"], 0, 0, 0, ["BGSAVE"]),
    ("BITCOUNT", parse_bitcount, -2, ["readonly"], 1, 1, 1, ["BITCOUNT k"]),
    ("BITOP", parse_bitop, -4, ["write", "denyoom"], 2, -1, 1, ["BITOP AND d a b"]),
    ("BITPOS", parse_bitpos, -3, ["readonly"], 1, 1, 1, ["BITPOS k 1"]),
    ("BLPOP", parse_blpop, -3, ["write", "noscript"], 1, -2, 1, ["BLPOP a b 0.01"]),
    ("BRPOP", parse_brpop, -3, ["write", "noscript"], 1, -2, 1, ["BRPOP a b 0.01"]),
    ("CLIENT", parse_client, -2, ["admin", "noscript", "loading", "fast"], 0, 0, 0,
        ["CLIENT ID", "CLIENT GETNAME", "CLIENT SETNAME n", "CLIENT LIST"]),
    ("CLUSTER", parse_cluster, -2, ["admin"], 0, 0, 0,
        ["CLUSTER INFO", "CLUSTER NODES", "CLUSTER SLOTS", "CLUSTER KEYSLOT k",
         "CLUSTER MYID", "CLUSTER SETSLOT 1 IMPORTING id", "CLUSTER SETSLOT 1 MIGRATING id",
         "CLUSTER SETSLOT 1 NODE id", "CLUSTER SETSLOT 1 STABLE",
         "CLUSTER MEET 127.0.0.1 1", "CLUSTER ADDSLOTS 1", "CLUSTER ADDSLOTSRANGE 1 2",
         "CLUSTER DELSLOTS 1", "CLUSTER FORGET id", "CLUSTER REPLICATE id",
         "CLUSTER FAILOVER", "CLUSTER COUNTKEYSINSLOT 1", "CLUSTER GETKEYSINSLOT 1 10"]),
    ("COMMAND", parse_command_cmd, -1, ["loading", "fast"], 0, 0, 0,
        ["COMMAND", "COMMAND COUNT"]),
    ("CONFIG", parse_config, -2, ["admin", "loading", "noscript"], 0, 0, 0,
        ["CONFIG GET maxmemory", "CONFIG SET slowlog-log-slower-than 10000",
         "CONFIG REWRITE"]),
    ("COPY", parse_copy, -3, ["write"], 1, 2, 1, ["COPY a b"]),
    ("DBSIZE", parse_dbsize, 1, ["readonly", "fast"], 0, 0, 0, ["DBSIZE"]),
    ("DECR", parse_decr, 2, ["write", "denyoom", "fast"], 1, 1, 1, ["DECR k"]),
    ("DECRBY", parse_decrby, 3, ["write", "denyoom", "fast"], 1, 1, 1, ["DECRBY k 1"]),
    ("DEL", parse_del, -2, ["write"], 1, -1, 1, ["DEL a b"]),
    ("DISCARD", |a| parse_no_args("DISCARD", a, Command::Discard), 1,
        ["noscript", "loading", "fast"], 0, 0, 0, ["DISCARD"]),
    ("ECHO", parse_echo, 2, ["fast"], 0, 0, 0, ["ECHO hi"]),
    ("EXEC", |a| parse_no_args("EXEC", a, Command::Exec), 1, ["noscript", "loading"],
        0, 0, 0, ["EXEC"]),
    ("EXISTS", parse_exists, -2, ["readonly", "fast"], 1, -1, 1, ["EXISTS a b"]),
    ("EXPIRE", parse_expire, 3, ["write", "fast"], 1, 1, 1, ["EXPIRE k 10"]),
    ("EXPIREAT", parse_expireat, 3, ["write", "fast"], 1, 1, 1, ["EXPIREAT k 1"]),
    ("EXPIRETIME", parse_expiretime, 2, ["readonly", "fast"], 1, 1, 1, ["EXPIRETIME k"]),
    ("FLUSHALL", parse_flushall, -1, ["write"], 0, 0, 0, ["FLUSHALL"]),
    ("FLUSHDB", parse_flushdb, -1, ["write"], 0, 0, 0, ["FLUSHDB"]),
    ("GET", parse_get, 2, ["readonly", "fast"], 1, 1, 1, ["GET k"]),
    ("GETBIT", parse_getbit, 3, ["readonly", "fast"], 1, 1, 1, ["GETBIT k 0"]),
    ("GETDEL", parse_getdel, 2, ["write", "fast"], 1, 1, 1, ["GETDEL k"]),
    ("GETEX", parse_getex, -2, ["write", "fast"], 1, 1, 1, ["GETEX k"]),
    ("GETRANGE", parse_getrange, 4, ["readonly"], 1, 1, 1, ["GETRANGE k 0 1"]),
    ("GETSET", parse_getset, 3, ["write", "denyoom", "fast"], 1, 1, 1, ["GETSET k v"]),
    ("HDEL", parse_hdel, -3, ["write", "fast"], 1, 1, 1, ["HDEL h f"]),
    ("HEXISTS", parse_hexists, 3, ["readonly", "fast"], 1, 1, 1, ["HEXISTS h f"]),
    ("HGET", parse_hget, 3, ["readonly", "fast"], 1, 1, 1, ["HGET h f"]),
    ("HGETALL", parse_hgetall, 2, ["readonly", "random"], 1, 1, 1, ["HGETALL h"]),
    ("HINCRBY", parse_hincrby, 4, ["write", "denyoom", "fast"], 1, 1, 1,
        ["HINCRBY h f 1"]),
    ("HINCRBYFLOAT", parse_hincrbyfloat, 4, ["write", "denyoom", "fast"], 1, 1, 1,
        ["HINCRBYFLOAT h f 1.5"]),
    ("HKEYS", parse_hkeys, 2, ["readonly", "sort_for_script"], 1, 1, 1, ["HKEYS h"]),
    ("HLEN", parse_hlen, 2, ["readonly", "fast"], 1, 1, 1, ["HLEN h"]),
    ("HMGET", parse_hmget, -3, ["readonly", "fast"], 1, 1, 1, ["HMGET h f"]),
    ("HRANDFIELD", parse_hrandfield, -2, ["readonly", "random"], 1, 1, 1,
        ["HRANDFIELD h"]),
    ("HSCAN", |a| parse_key_scan(a, "HSCAN"), -3, ["readonly", "random"], 1, 1, 1,
        ["HSCAN h 0"]),
    ("HSET", parse_hset, -4, ["write", "denyoom", "fast"], 1, 1, 1, ["HSET h f v"]),
    ("HVALS", parse_hvals, 2, ["readonly", "sort_for_script"], 1, 1, 1, ["HVALS h"]),
    ("INCR", parse_incr, 2, ["write", "denyoom", "fast"], 1, 1, 1, ["INCR k"]),
    ("INCRBY", parse_incrby, 3, ["write", "denyoom", "fast"], 1, 1, 1, ["INCRBY k 1"]),
    ("INCRBYFLOAT", parse_incrbyfloat, 3, ["write", "denyoom", "fast"], 1, 1, 1,
        ["INCRBYFLOAT k 1.5"]),
    ("INFO", parse_info, -1, ["loading", "fast"], 0, 0, 0, ["INFO"]),
    ("KEYS", parse_keys, 2, ["readonly", "sort_for_script"], 0, 0, 0, ["KEYS *"]),
    ("LASTSAVE", |a| parse_no_args("LASTSAVE", a, Command::LastSave), 1,
        ["random", "loading", "fast"], 0, 0, 0, ["LASTSAVE"]),
    ("LINDEX", parse_lindex, 3, ["readonly"], 1, 1, 1, ["LINDEX l 0"]),
    ("LINSERT", parse_linsert, 5, ["write", "denyoom"], 1, 1, 1,
        ["LINSERT l BEFORE p v"]),
    ("LLEN", parse_llen, 2, ["readonly", "fast"], 1, 1, 1, ["LLEN l"]),
    ("LMOVE", parse_lmove, 5, ["write", "denyoom"], 1, 2, 1, ["LMOVE a b LEFT RIGHT"]),
    ("LMPOP", parse_lmpop, -4, ["write", "movablekeys"], 0, 0, 0, ["LMPOP 2 a b LEFT"]),
    ("LPOP", parse_lpop, -2, ["write", "fast"], 1, 1, 1, ["LPOP l"]),
    ("LPOS", parse_lpos, -3, ["readonly"], 1, 1, 1, ["LPOS l v"]),
    ("LPUSH", parse_lpush, -3, ["write", "denyoom", "fast"], 1, 1, 1, ["LPUSH l v"]),
    ("LRANGE", parse_lrange, 4, ["readonly"], 1, 1, 1, ["LRANGE l 0 -1"]),
    ("LREM", parse_lrem, 4, ["write"], 1, 1, 1, ["LREM l 0 v"]),
    ("LSET", parse_lset, 4, ["write", "denyoom"], 1, 1, 1, ["LSET l 0 v"]),
    ("LTRIM", parse_ltrim, 4, ["write"], 1, 1, 1, ["LTRIM l 0 1"]),
    ("MEMORY", parse_memory_cmd, -2, ["readonly"], 2, 2, 1, ["MEMORY USAGE k"]),
    ("MGET", parse_mget, -2, ["readonly", "fast"], 1, -1, 1, ["MGET a b"]),
    ("MIGRATE", parse_migrate, -6, ["write", "movablekeys"], 3, 3, 1,
        ["MIGRATE 127.0.0.1 1 k 0 100"]),
    ("MONITOR", parse_monitor, 1, ["admin", "loading"], 0, 0, 0, ["MONITOR"]),
    ("MSET", parse_mset, -3, ["write", "denyoom"], 1, -1, 2, ["MSET a 1 b 2"]),
    ("MSETNX", parse_msetnx, -3, ["write", "denyoom"], 1, -1, 2, ["MSETNX a 1 b 2"]),
    ("MULTI", |a| parse_no_args("MULTI", a, Command::Multi), 1,
        ["noscript", "loading", "fast"], 0, 0, 0, ["MULTI"]),
    ("OBJECT", parse_object, -2, ["readonly"], 2, 2, 1,
        ["OBJECT ENCODING k", "OBJECT REFCOUNT k"]),
    ("PERSIST", parse_persist, 2, ["write", "fast"], 1, 1, 1, ["PERSIST k"]),
    ("PEXPIRE", parse_pexpire, 3, ["write", "fast"], 1, 1, 1, ["PEXPIRE k 100"]),
    ("PEXPIREAT", parse_pexpireat_cmd, 3, ["write", "fast"], 1, 1, 1,
        ["PEXPIREAT k 1"]),
    ("PEXPIRETIME", parse_pexpiretime, 2, ["readonly", "fast"], 1, 1, 1,
        ["PEXPIRETIME k"]),
    ("PING", parse_ping, -1, ["fast", "loading"], 0, 0, 0, ["PING"]),
    ("PROTO.DELFIELD", parse_proto_delfield, 3, ["write"], 1, 1, 1,
        ["PROTO.DELFIELD k f"]),
    ("PROTO.DESCRIBE", parse_proto_describe, 2, ["readonly"], 0, 0, 0,
        ["PROTO.DESCRIBE n"]),
    ("PROTO.FIND", parse_proto_find, -4, ["readonly"], 0, 0, 0, ["PROTO.FIND 0 f v"]),
    ("PROTO.GET", parse_proto_get, 2, ["readonly"], 1, 1, 1, ["PROTO.GET k"]),
    ("PROTO.GETFIELD", parse_proto_getfield, 3, ["readonly"], 1, 1, 1,
        ["PROTO.GETFIELD k f"]),
    ("PROTO.REGISTER", parse_proto_register, 3, ["write", "denyoom"], 0, 0, 0,
        ["PROTO.REGISTER n d"]),
    ("PROTO.SCAN", parse_proto_scan, -2, ["readonly"], 0, 0, 0, ["PROTO.SCAN 0"]),
    ("PROTO.SCHEMAS", parse_proto_schemas, 1, ["readonly"], 0, 0, 0, ["PROTO.SCHEMAS"]),
    ("PROTO.SET", parse_proto_set, -4, ["write", "denyoom"], 1, 1, 1,
        ["PROTO.SET k T d"]),
    ("PROTO.SETFIELD", parse_proto_setfield, 4, ["write", "denyoom"], 1, 1, 1,
        ["PROTO.SETFIELD k f v"]),
    ("PROTO.TYPE", parse_proto_type, 2, ["readonly"], 1, 1, 1, ["PROTO.TYPE k"]),
    ("PSETEX", parse_psetex, 4, ["write", "denyoom"], 1, 1, 1, ["PSETEX k 100 v"]),
    ("PSUBSCRIBE", parse_psubscribe, -2, ["pubsub", "loading", "fast"], 0, 0, 0,
        ["PSUBSCRIBE p*"]),
    ("PTTL", parse_pttl, 2, ["readonly", "fast"], 1, 1, 1, ["PTTL k"]),
    ("PUBLISH", parse_publish, 3, ["pubsub", "fast"], 0, 0, 0, ["PUBLISH c m"]),
    ("PUBSUB", parse_pubsub, -2, ["pubsub", "random", "loading", "fast"], 0, 0, 0,
        ["PUBSUB CHANNELS", "PUBSUB NUMSUB c", "PUBSUB NUMPAT"]),
    ("PUNSUBSCRIBE", parse_punsubscribe, -1, ["pubsub", "loading", "fast"], 0, 0, 0,
        ["PUNSUBSCRIBE"]),
    ("QUIT", parse_quit, 1, ["fast", "loading"], 0, 0, 0, ["QUIT"]),
    ("RANDOMKEY", |a| parse_no_args("RANDOMKEY", a, Command::RandomKey), 1,
        ["readonly", "random"], 0, 0, 0, ["RANDOMKEY"]),
    ("RENAME", parse_rename, 3, ["write"], 1, 2, 1, ["RENAME a b"]),
    ("RESTORE", parse_restore, -4, ["write", "denyoom"], 1, 1, 1, ["RESTORE k 0 d"]),
    ("ROLE", |a| parse_no_args("ROLE", a, Command::Role), 1,
        ["noscript", "loading", "fast"], 0, 0, 0, ["ROLE"]),
    ("RPOP", parse_rpop, -2, ["write", "fast"], 1, 1, 1, ["RPOP l"]),
    ("RPUSH", parse_rpush, -3, ["write", "denyoom", "fast"], 1, 1, 1, ["RPUSH l v"]),
    ("SADD", parse_sadd, -3, ["write", "denyoom", "fast"], 1, 1, 1, ["SADD s m"]),
    ("SCAN", parse_scan, -2, ["readonly"], 0, 0, 0, ["SCAN 0"]),
    ("SCARD", parse_scard, 2, ["readonly", "fast"], 1, 1, 1, ["SCARD s"]),
    ("SDIFF", |a| parse_multi_key_set("SDIFF", a), -2, ["readonly", "sort_for_script"],
        1, -1, 1, ["SDIFF a b"]),
    ("SDIFFSTORE", |a| parse_store_set("SDIFFSTORE", a), -3, ["write", "denyoom"],
        1, -1, 1, ["SDIFFSTORE d a b"]),
    ("SET", parse_set, -3, ["write", "denyoom"], 1, 1, 1, ["SET k v"]),
    ("SETBIT", parse_setbit, 4, ["write", "denyoom"], 1, 1, 1, ["SETBIT k 0 1"]),
    ("SETEX", parse_setex, 4, ["write", "denyoom"], 1, 1, 1, ["SETEX k 10 v"]),
    ("SETNX", parse_setnx, 3, ["write", "denyoom", "fast"], 1, 1, 1, ["SETNX k v"]),
    ("SETRANGE", parse_setrange, 4, ["write", "denyoom"], 1, 1, 1, ["SETRANGE k 0 v"]),
    ("SINTER", |a| parse_multi_key_set("SINTER", a), -2, ["readonly", "sort_for_script"],
        1, -1, 1, ["SINTER a b"]),
    ("SINTERCARD", parse_sintercard, -3, ["readonly", "movablekeys"], 0, 0, 0,
        ["SINTERCARD 2 a b"]),
    ("SINTERSTORE", |a| parse_store_set("SINTERSTORE", a), -3, ["write", "denyoom"],
        1, -1, 1, ["SINTERSTORE d a b"]),
    ("SISMEMBER", parse_sismember, 3, ["readonly", "fast"], 1, 1, 1, ["SISMEMBER s m"]),
    ("SLOWLOG", parse_slowlog, -2, ["admin", "loading", "fast"], 0, 0, 0,
        ["SLOWLOG GET", "SLOWLOG LEN", "SLOWLOG RESET"]),
    ("SMEMBERS", parse_smembers, 2, ["readonly", "sort_for_script"], 1, 1, 1,
        ["SMEMBERS s"]),
    ("SMISMEMBER", parse_smismember, -3, ["readonly", "fast"], 1, 1, 1,
        ["SMISMEMBER s m"]),
    ("SMOVE", parse_smove, 4, ["write", "fast"], 1, 2, 1, ["SMOVE a b m"]),
    ("SORT", parse_sort, -2, ["write", "denyoom", "movablekeys"], 1, 1, 1,
        ["SORT k", "SORT k STORE d"]),
    ("SPOP", parse_spop, -2, ["write", "random", "fast"], 1, 1, 1, ["SPOP s"]),
    ("SRANDMEMBER", parse_srandmember, -2, ["readonly", "random"], 1, 1, 1,
        ["SRANDMEMBER s"]),
    ("SREM", parse_srem, -3, ["write", "fast"], 1, 1, 1, ["SREM s m"]),
    ("SSCAN", |a| parse_key_scan(a, "SSCAN"), -3, ["readonly", "random"], 1, 1, 1,
        ["SSCAN s 0"]),
    ("STRLEN", parse_strlen, 2, ["readonly", "fast"], 1, 1, 1, ["STRLEN k"]),
    ("SUBSCRIBE", parse_subscribe, -2, ["pubsub", "loading", "fast"], 0, 0, 0,
        ["SUBSCRIBE c"]),
    ("SUBSTR", parse_getrange, 4, ["readonly"], 1, 1, 1, ["SUBSTR k 0 1"]),
    ("SUNION", |a| parse_multi_key_set("SUNION", a), -2, ["readonly", "sort_for_script"],
        1, -1, 1, ["SUNION a b"]),
    ("SUNIONSTORE", |a| parse_store_set("SUNIONSTORE", a), -3, ["write", "denyoom"],
        1, -1, 1, ["SUNIONSTORE d a b"]),
    ("TIME", |a| parse_no_args("TIME", a, Command::Time), 1,
        ["random", "loading", "fast"], 0, 0, 0, ["TIME"]),
    ("TOUCH", parse_touch, -2, ["readonly", "fast"], 1, -1, 1, ["TOUCH a b"]),
    ("TTL", parse_ttl, 2, ["readonly", "fast"], 1, 1, 1, ["TTL k"]),
    ("TYPE", parse_type, 2, ["readonly", "fast"], 1, 1, 1, ["TYPE k"]),
    ("UNLINK", parse_unlink, -2, ["write", "fast"], 1, -1, 1, ["UNLINK a b"]),
    ("UNSUBSCRIBE", parse_unsubscribe, -1, ["pubsub", "loading", "fast"], 0, 0, 0,
        ["UNSUBSCRIBE"]),
    ("UNWATCH", |a| parse_no_args("UNWATCH", a, Command::Unwatch), 1,
        ["noscript", "loading", "fast"], 0, 0, 0, ["UNWATCH"]),
    ("VADD", parse_vadd, -4, ["write", "denyoom"], 1, 1, 1, ["VADD v e 1 2"]),
    ("VADD_BATCH", parse_vadd_batch, -4, ["write", "denyoom"], 1, 1, 1,
        ["VADD_BATCH v DIM 2 e 1 2"]),
    ("VCARD", parse_vcard, 2, ["readonly", "fast"], 1, 1, 1, ["VCARD v"]),
    ("VDIM", parse_vdim, 2, ["readonly", "fast"], 1, 1, 1, ["VDIM v"]),
    ("VGET", parse_vget, 3, ["readonly"], 1, 1, 1, ["VGET v e"]),
    ("VINFO", parse_vinfo, 2, ["readonly"], 1, 1, 1, ["VINFO v"]),
    ("VREM", parse_vrem, 3, ["write"], 1, 1, 1, ["VREM v e"]),
    ("VSIM", parse_vsim, -5, ["readonly"], 1, 1, 1, ["VSIM v 1 2 COUNT 1"]),
    ("WAIT", parse_wait, 3, ["noscript"], 0, 0, 0, ["WAIT 0 0"]),
    ("WATCH", parse_watch, -2, ["noscript", "loading", "fast"], 1, -1, 1, ["WATCH a b"]),
    ("ZADD", parse_zadd, -4, ["write", "denyoom", "fast"], 1, 1, 1, ["ZADD z 1 m"]),
    ("ZCARD", parse_zcard, 2, ["readonly", "fast"], 1, 1, 1, ["ZCARD z"]),
    ("ZCOUNT", parse_zcount, 4, ["readonly", "fast"], 1, 1, 1, ["ZCOUNT z 0 1"]),
    ("ZDIFF", |a| parse_zset_multi("ZDIFF", a), -3,
        ["readonly", "sort_for_script", "movablekeys"], 0, 0, 0, ["ZDIFF 2 a b"]),
    ("ZDIFFSTORE", |a| parse_zset_store("ZDIFFSTORE", a), -4,
        ["write", "denyoom", "movablekeys"], 1, 1, 1, ["ZDIFFSTORE d 2 a b"]),
    ("ZINCRBY", parse_zincrby, 4, ["write", "denyoom", "fast"], 1, 1, 1,
        ["ZINCRBY z 1 m"]),
    ("ZINTER", |a| parse_zset_multi("ZINTER", a), -3,
        ["readonly", "sort_for_script", "movablekeys"], 0, 0, 0, ["ZINTER 2 a b"]),
    ("ZINTERSTORE", |a| parse_zset_store("ZINTERSTORE", a), -4,
        ["write", "denyoom", "movablekeys"], 1, 1, 1, ["ZINTERSTORE d 2 a b"]),
    ("ZMPOP", parse_zmpop, -4, ["write", "movablekeys"], 0, 0, 0, ["ZMPOP 2 a b MIN"]),
    ("ZPOPMAX", |a| {
        let (key, count) = parse_zpop_args(a, "ZPOPMAX")?;
        Ok(Command::ZPopMax { key, count })
    }, -2, ["write", "fast"], 1, 1, 1, ["ZPOPMAX z"]),
    ("ZPOPMIN", |a| {
        let (key, count) = parse_zpop_args(a, "ZPOPMIN")?;
        Ok(Command::ZPopMin { key, count })
    }, -2, ["write", "fast"], 1, 1, 1, ["ZPOPMIN z"]),
    ("ZRANDMEMBER", parse_zrandmember, -2, ["readonly", "random"], 1, 1, 1,
        ["ZRANDMEMBER z"]),
    ("ZRANGE", parse_zrange, -4, ["readonly"], 1, 1, 1, ["ZRANGE z 0 -1"]),
    ("ZRANGEBYSCORE", parse_zrangebyscore, -4, ["readonly"], 1, 1, 1,
        ["ZRANGEBYSCORE z 0 1"]),
    ("ZRANK", parse_zrank, 3, ["readonly", "fast"], 1, 1, 1, ["ZRANK z m"]),
    ("ZREM", parse_zrem, -3, ["write", "fast"], 1, 1, 1, ["ZREM z m"]),
    ("ZREVRANGE", parse_zrevrange, -4, ["readonly"], 1, 1, 1, ["ZREVRANGE z 0 -1"]),
    ("ZREVRANGEBYSCORE", parse_zrevrangebyscore, -4, ["readonly"], 1, 1, 1,
        ["ZREVRANGEBYSCORE z 1 0"]),
    ("ZREVRANK", parse_zrevrank, 3, ["readonly", "fast"], 1, 1, 1, ["ZREVRANK z m"]),
    ("ZSCAN", |a| parse_key_scan(a, "ZSCAN"), -3, ["readonly", "random"], 1, 1, 1,
        ["ZSCAN z 0"]),
    ("ZSCORE", parse_zscore, 3, ["readonly", "fast"], 1, 1, 1, ["ZSCORE z m"]),
    ("ZUNION", |a| parse_zset_multi("ZUNION", a), -3,
        ["readonly", "sort_for_script", "movablekeys"], 0, 0, 0, ["ZUNION 2 a b"]),
    ("ZUNIONSTORE", |a| parse_zset_store("ZUNIONSTORE", a), -4,
        ["write", "denyoom", "movablekeys"], 1, 1, 1, ["ZUNIONSTORE d 2 a b"]),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::attributes::category;

    fn parsed(example: &str) -> Command {
        match parse_example(example) {
            Ok(Command::Unknown(name)) => panic!("{example}: unknown command {name}"),
            Ok(cmd) => cmd,
            Err(e) => panic!("{example}: {e}"),
        }
    }

    #[test]
    fn names_are_sorted_uppercase_and_unique() {
        for pair in COMMAND_TABLE.windows(2) {
            assert!(
                pair[0].name < pair[1].name,
                "{} before {}",
                pair[0].name,
                pair[1].name
            );
        }
        for e in COMMAND_TABLE {
            assert_eq!(e.name, e.name.to_ascii_uppercase());
        }
    }

    #[test]
    fn examples_parse_and_fit_the_arity() {
        for e in COMMAND_TABLE {
            for example in e.examples {
                parsed(example);
                let argc = example.split(' ').count() as i64;
                assert_eq!(example.split(' ').next(), Some(e.name), "{example}");
                if e.arity >= 0 {
                    assert_eq!(argc, e.arity, "{example}: arity");
                } else {
                    assert!(argc >= -e.arity, "{example}: arity");
                }
            }
        }
    }

    #[test]
    fn write_and_readonly_flags_match_the_command() {
        for e in COMMAND_TABLE {
            let cmds: Vec<Command> = e.examples.iter().map(|x| parsed(x)).collect();
            let writes = cmds
                .iter()
                .any(|c| c.acl_categories() & category::WRITE != 0);
            assert_eq!(e.flags.contains(&"write"), writes, "{}: write flag", e.name);

            let reads = cmds.iter().all(|c| {
                let cats = c.acl_categories();
                cats & category::READ != 0 && cats & category::WRITE == 0
            });
            assert_eq!(
                e.flags.contains(&"readonly"),
                reads,
                "{}: readonly flag",
                e.name
            );
        }
    }

    #[test]
    fn key_positions_match_the_command() {
        for e in COMMAND_TABLE {
            for example in e.examples {
                let args: Vec<&str> = example.split(' ').collect();
                let mut positional = Vec::new();
                if e.first_key > 0 {
                    let last = if e.last_key < 0 {
                        args.len() as i64 + e.last_key
                    } else {
                        e.last_key
                    };
                    let mut i = e.first_key;
                    while i <= last {
                        positional.push(args[i as usize]);
                        i += e.step;
                    }
                }
                let cmd = parsed(example);
                let keys: Vec<&str> = cmd.keys().iter().collect();
                if e.flags.contains(&"movablekeys") {
                    assert!(
                        positional.iter().all(|k| keys.contains(k)),
                        "{example}: {positional:?} not all in {keys:?}"
                    );
                } else {
                    assert_eq!(positional, keys, "{example}: key positions");
                }
            }
        }
    }
}
