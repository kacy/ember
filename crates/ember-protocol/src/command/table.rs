//! Static command metadata table for the COMMAND command.
//!
//! Provides arity, flags, and key positions for every command ember supports.
//! Kept here so all command-describing data lives in one crate: enum variants,
//! attribute methods, and wire-protocol metadata are all in ember-protocol.
//! Adding a new command means editing this crate and one match arm in execute.rs.

use bytes::Bytes;

use crate::Frame;

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

struct CommandEntry {
    name: &'static str,
    arity: i64,
    flags: &'static [&'static str],
    first_key: i64,
    last_key: i64,
    step: i64,
}

/// Shorthand for declaring a [`CommandEntry`] in the command table.
macro_rules! cmd {
    ($name:expr, $arity:expr, [$($flag:expr),* $(,)?], $first:expr, $last:expr, $step:expr) => {
        CommandEntry {
            name: $name,
            arity: $arity,
            flags: &[$($flag),*],
            first_key: $first,
            last_key: $last,
            step: $step,
        }
    };
}

/// Static command table. Arity: positive = exact, negative = minimum.
/// Flags: write, readonly, denyoom, admin, pubsub, noscript, fast, loading, etc.
static COMMAND_TABLE: &[CommandEntry] = &[
    cmd!("ACL", -2, ["admin", "noscript", "loading"], 0, 0, 0),
    cmd!("APPEND", 3, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!(
        "AUTH",
        -2,
        ["noscript", "loading", "fast", "no_auth"],
        0,
        0,
        0
    ),
    cmd!("BGREWRITEAOF", 1, ["admin"], 0, 0, 0),
    cmd!("BGSAVE", -1, ["admin"], 0, 0, 0),
    cmd!("BITCOUNT", -2, ["readonly"], 1, 1, 1),
    cmd!("BITOP", -4, ["write", "denyoom"], 2, -1, 1),
    cmd!("BITPOS", -3, ["readonly"], 1, 1, 1),
    cmd!("BLPOP", -3, ["write", "noscript"], 1, -2, 1),
    cmd!("BRPOP", -3, ["write", "noscript"], 1, -2, 1),
    cmd!(
        "CLIENT",
        -2,
        ["admin", "noscript", "loading", "fast"],
        0,
        0,
        0
    ),
    cmd!("CLUSTER", -2, ["admin"], 0, 0, 0),
    cmd!("COMMAND", -1, ["loading", "fast"], 0, 0, 0),
    cmd!("CONFIG", -2, ["admin", "loading", "noscript"], 0, 0, 0),
    cmd!("COPY", -3, ["write"], 1, 2, 1),
    cmd!("DBSIZE", 1, ["readonly", "fast"], 0, 0, 0),
    cmd!("DECR", 2, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("DECRBY", 3, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("DEL", -2, ["write"], 1, -1, 1),
    cmd!("DISCARD", 1, ["noscript", "loading", "fast"], 0, 0, 0),
    cmd!("ECHO", 2, ["fast"], 0, 0, 0),
    cmd!("EXEC", 1, ["noscript", "loading"], 0, 0, 0),
    cmd!("EXISTS", -2, ["readonly", "fast"], 1, -1, 1),
    cmd!("EXPIRE", 3, ["write", "fast"], 1, 1, 1),
    cmd!("EXPIREAT", 3, ["write", "fast"], 1, 1, 1),
    cmd!("EXPIRETIME", 2, ["readonly", "fast"], 1, 1, 1),
    cmd!("FLUSHALL", -1, ["write"], 0, 0, 0),
    cmd!("FLUSHDB", -1, ["write"], 0, 0, 0),
    cmd!("GET", 2, ["readonly", "fast"], 1, 1, 1),
    cmd!("GETBIT", 3, ["readonly", "fast"], 1, 1, 1),
    cmd!("GETDEL", 2, ["write", "fast"], 1, 1, 1),
    cmd!("GETEX", -2, ["write", "fast"], 1, 1, 1),
    cmd!("GETRANGE", 4, ["readonly"], 1, 1, 1),
    cmd!("GETSET", 3, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("HDEL", -3, ["write", "fast"], 1, 1, 1),
    cmd!("HEXISTS", 3, ["readonly", "fast"], 1, 1, 1),
    cmd!("HGET", 3, ["readonly", "fast"], 1, 1, 1),
    cmd!("HGETALL", 2, ["readonly", "random"], 1, 1, 1),
    cmd!("HINCRBY", 4, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("HINCRBYFLOAT", 4, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("HKEYS", 2, ["readonly", "sort_for_script"], 1, 1, 1),
    cmd!("HLEN", 2, ["readonly", "fast"], 1, 1, 1),
    cmd!("HMGET", -3, ["readonly", "fast"], 1, 1, 1),
    cmd!("HMSET", -4, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("HRANDFIELD", -2, ["readonly", "random"], 1, 1, 1),
    cmd!("HSCAN", -3, ["readonly", "random"], 1, 1, 1),
    cmd!("HSET", -4, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("HVALS", 2, ["readonly", "sort_for_script"], 1, 1, 1),
    cmd!("INCR", 2, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("INCRBY", 3, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("INCRBYFLOAT", 3, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("INFO", -1, ["loading", "fast"], 0, 0, 0),
    cmd!("KEYS", 2, ["readonly", "sort_for_script"], 0, 0, 0),
    cmd!("LASTSAVE", 1, ["random", "loading", "fast"], 0, 0, 0),
    cmd!("LINDEX", 3, ["readonly"], 1, 1, 1),
    cmd!("LINSERT", 5, ["write", "denyoom"], 1, 1, 1),
    cmd!("LLEN", 2, ["readonly", "fast"], 1, 1, 1),
    cmd!("LMOVE", 5, ["write", "denyoom"], 1, 2, 1),
    cmd!("LMPOP", -4, ["write", "fast"], 0, 0, 0),
    cmd!("LPOS", -3, ["readonly"], 1, 1, 1),
    cmd!("LPOP", -2, ["write", "fast"], 1, 1, 1),
    cmd!("LPUSH", -3, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("LPUSHX", -3, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("LRANGE", 4, ["readonly"], 1, 1, 1),
    cmd!("LREM", 4, ["write"], 1, 1, 1),
    cmd!("LSET", 4, ["write", "denyoom"], 1, 1, 1),
    cmd!("LTRIM", 4, ["write"], 1, 1, 1),
    cmd!("MEMORY", -2, ["readonly"], 0, 0, 0),
    cmd!("MGET", -2, ["readonly", "fast"], 1, -1, 1),
    cmd!("MIGRATE", -6, ["write"], 0, 0, 0),
    cmd!("MONITOR", 1, ["admin", "loading"], 0, 0, 0),
    cmd!("MSET", -3, ["write", "denyoom"], 1, -1, 2),
    cmd!("MSETNX", -3, ["write", "denyoom"], 1, -1, 2),
    cmd!("MULTI", 1, ["noscript", "loading", "fast"], 0, 0, 0),
    cmd!("OBJECT", -2, ["slow"], 2, 2, 1),
    cmd!("PERSIST", 2, ["write", "fast"], 1, 1, 1),
    cmd!("PEXPIRE", 3, ["write", "fast"], 1, 1, 1),
    cmd!("PEXPIREAT", 3, ["write", "fast"], 1, 1, 1),
    cmd!("PEXPIRETIME", 2, ["readonly", "fast"], 1, 1, 1),
    cmd!("PING", -1, ["fast", "loading"], 0, 0, 0),
    cmd!("PSETEX", 4, ["write", "denyoom"], 1, 1, 1),
    cmd!("PSUBSCRIBE", -2, ["pubsub", "loading", "fast"], 0, 0, 0),
    cmd!("PTTL", 2, ["readonly", "fast"], 1, 1, 1),
    cmd!("PUBLISH", 3, ["pubsub", "fast"], 0, 0, 0),
    cmd!(
        "PUBSUB",
        -2,
        ["pubsub", "random", "loading", "fast"],
        0,
        0,
        0
    ),
    cmd!("PUNSUBSCRIBE", -1, ["pubsub", "loading", "fast"], 0, 0, 0),
    cmd!("QUIT", 1, ["fast", "loading"], 0, 0, 0),
    cmd!("RANDOMKEY", 1, ["readonly", "random"], 0, 0, 0),
    cmd!("RENAME", 3, ["write"], 1, 2, 1),
    cmd!("ROLE", 1, ["noscript", "loading", "fast"], 0, 0, 0),
    cmd!("RPOP", -2, ["write", "fast"], 1, 1, 1),
    cmd!("RPUSH", -3, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("RPUSHX", -3, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("SADD", -3, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("SCAN", -2, ["readonly"], 0, 0, 0),
    cmd!("SCARD", 2, ["readonly", "fast"], 1, 1, 1),
    cmd!("SDIFF", -2, ["readonly", "sort_for_script"], 1, -1, 1),
    cmd!("SDIFFSTORE", -3, ["write", "denyoom"], 1, -1, 1),
    cmd!("SET", -3, ["write", "denyoom"], 1, 1, 1),
    cmd!("SETBIT", 4, ["write", "denyoom"], 1, 1, 1),
    cmd!("SETEX", 4, ["write", "denyoom"], 1, 1, 1),
    cmd!("SETNX", 3, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("SETRANGE", 4, ["write", "denyoom"], 1, 1, 1),
    cmd!("SINTER", -2, ["readonly", "sort_for_script"], 1, -1, 1),
    cmd!("SINTERCARD", -3, ["readonly"], 0, 0, 0),
    cmd!("SINTERSTORE", -3, ["write", "denyoom"], 1, -1, 1),
    cmd!("SISMEMBER", 3, ["readonly", "fast"], 1, 1, 1),
    cmd!("SLOWLOG", -2, ["admin", "loading", "fast"], 0, 0, 0),
    cmd!("SMEMBERS", 2, ["readonly", "sort_for_script"], 1, 1, 1),
    cmd!("SMISMEMBER", -3, ["readonly", "fast"], 1, 1, 1),
    cmd!("SMOVE", 4, ["write", "fast"], 1, 2, 1),
    cmd!("SORT", -2, ["write", "denyoom"], 1, 1, 1),
    cmd!("SPOP", -2, ["write", "random", "fast"], 1, 1, 1),
    cmd!("SRANDMEMBER", -2, ["readonly", "random"], 1, 1, 1),
    cmd!("SREM", -3, ["write", "fast"], 1, 1, 1),
    cmd!("SSCAN", -3, ["readonly", "random"], 1, 1, 1),
    cmd!("STRLEN", 2, ["readonly", "fast"], 1, 1, 1),
    cmd!("SUBSCRIBE", -2, ["pubsub", "loading", "fast"], 0, 0, 0),
    cmd!("SUNION", -2, ["readonly", "sort_for_script"], 1, -1, 1),
    cmd!("SUNIONSTORE", -3, ["write", "denyoom"], 1, -1, 1),
    cmd!("TIME", 1, ["random", "loading", "fast"], 0, 0, 0),
    cmd!("TOUCH", -2, ["readonly", "fast"], 1, -1, 1),
    cmd!("TTL", 2, ["readonly", "fast"], 1, 1, 1),
    cmd!("TYPE", 2, ["readonly", "fast"], 1, 1, 1),
    cmd!("UNLINK", -2, ["write", "fast"], 1, -1, 1),
    cmd!("UNSUBSCRIBE", -1, ["pubsub", "loading", "fast"], 0, 0, 0),
    cmd!("UNWATCH", 1, ["noscript", "loading", "fast"], 0, 0, 0),
    cmd!("WAIT", 3, ["noscript"], 0, 0, 0),
    cmd!("WATCH", -2, ["noscript", "loading", "fast"], 1, -1, 1),
    cmd!("ZADD", -4, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("ZCARD", 2, ["readonly", "fast"], 1, 1, 1),
    cmd!("ZCOUNT", 4, ["readonly", "fast"], 1, 1, 1),
    cmd!("ZDIFF", -3, ["readonly", "sort_for_script"], 0, 0, 0),
    cmd!("ZDIFFSTORE", -4, ["write", "denyoom"], 1, 1, 1),
    cmd!("ZINCRBY", 4, ["write", "denyoom", "fast"], 1, 1, 1),
    cmd!("ZINTER", -3, ["readonly", "sort_for_script"], 0, 0, 0),
    cmd!("ZINTERSTORE", -4, ["write", "denyoom"], 1, 1, 1),
    cmd!("ZLEXCOUNT", 4, ["readonly", "fast"], 1, 1, 1),
    cmd!("ZMPOP", -4, ["write", "fast"], 0, 0, 0),
    cmd!("ZPOPMAX", -2, ["write", "fast"], 1, 1, 1),
    cmd!("ZPOPMIN", -2, ["write", "fast"], 1, 1, 1),
    cmd!("ZRANDMEMBER", -2, ["readonly", "random"], 1, 1, 1),
    cmd!("ZRANGE", -4, ["readonly"], 1, 1, 1),
    cmd!("ZRANGEBYSCORE", -4, ["readonly"], 1, 1, 1),
    cmd!("ZRANK", 3, ["readonly", "fast"], 1, 1, 1),
    cmd!("ZREM", -3, ["write", "fast"], 1, 1, 1),
    cmd!("ZREVRANGE", -4, ["readonly"], 1, 1, 1),
    cmd!("ZREVRANGEBYSCORE", -4, ["readonly"], 1, 1, 1),
    cmd!("ZREVRANK", 3, ["readonly", "fast"], 1, 1, 1),
    cmd!("ZSCAN", -3, ["readonly", "random"], 1, 1, 1),
    cmd!("ZSCORE", 3, ["readonly", "fast"], 1, 1, 1),
    cmd!("ZUNION", -3, ["readonly", "sort_for_script"], 0, 0, 0),
    cmd!("ZUNIONSTORE", -4, ["write", "denyoom"], 1, 1, 1),
];
