//! Command parsing from RESP3 frames.
//!
//! Contains `Command::from_frame()` and all individual command parsers.
//! Helper functions for frame extraction and numeric parsing live here too.

use bytes::Bytes;

use crate::error::ProtocolError;
use crate::types::Frame;

use super::{BitOpKind, BitRange, BitRangeUnit, Command, ScoreBound, SetExpire, ZAddFlags};

/// Maximum number of dimensions in a vector. 65,536 is generous for any
/// real-world embedding model (OpenAI: 1536, Cohere: 4096) while preventing
/// memory abuse from absurdly large vectors.
const MAX_VECTOR_DIMS: usize = 65_536;

/// Maximum value for HNSW connectivity (M) and expansion parameters.
/// Values above 1024 give no practical benefit and waste memory.
const MAX_HNSW_PARAM: u64 = 1024;

/// Maximum number of results for VSIM. 10,000 is generous for any practical
/// similarity search while preventing OOM from unbounded result allocation.
const MAX_VSIM_COUNT: u64 = 10_000;

/// Maximum search beam width for VSIM. Same cap as MAX_HNSW_PARAM —
/// larger values cause worst-case O(n) graph traversal with no accuracy gain.
const MAX_VSIM_EF: u64 = MAX_HNSW_PARAM;

/// Maximum number of vectors in a single VADD_BATCH command. 10,000 keeps
/// per-command latency bounded while still being large enough to amortize
/// round-trip overhead for bulk inserts.
const MAX_VADD_BATCH_SIZE: usize = 10_000;

/// Maximum value for SCAN COUNT. Prevents clients from requesting a scan
/// hint so large it causes pre-allocation issues.
const MAX_SCAN_COUNT: u64 = 10_000_000;

impl Command {
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

        // command names are short ASCII keywords — uppercase on the stack to
        // avoid two heap allocations (extract_string + to_ascii_uppercase).
        let name_bytes = extract_raw_bytes(&frames[0])?;
        let mut upper = [0u8; MAX_KEYWORD_LEN];
        let len = name_bytes.len();
        if len > MAX_KEYWORD_LEN {
            let name = extract_string(&frames[0])?;
            return Ok(Command::Unknown(name));
        }
        upper[..len].copy_from_slice(name_bytes);
        upper[..len].make_ascii_uppercase();
        let name_upper = std::str::from_utf8(&upper[..len]).map_err(|_| {
            ProtocolError::InvalidCommandFrame("command name is not valid utf-8".into())
        })?;

        match name_upper {
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
            "SETNX" => parse_setnx(&frames[1..]),
            "SETEX" => parse_setex(&frames[1..]),
            "PSETEX" => parse_psetex(&frames[1..]),
            "GETRANGE" | "SUBSTR" => parse_getrange(&frames[1..]),
            "SETRANGE" => parse_setrange(&frames[1..]),
            "GETBIT" => parse_getbit(&frames[1..]),
            "SETBIT" => parse_setbit(&frames[1..]),
            "BITCOUNT" => parse_bitcount(&frames[1..]),
            "BITPOS" => parse_bitpos(&frames[1..]),
            "BITOP" => parse_bitop(&frames[1..]),
            "KEYS" => parse_keys(&frames[1..]),
            "RENAME" => parse_rename(&frames[1..]),
            "DEL" => parse_del(&frames[1..]),
            "UNLINK" => parse_unlink(&frames[1..]),
            "EXISTS" => parse_exists(&frames[1..]),
            "MGET" => parse_mget(&frames[1..]),
            "MSET" => parse_mset(&frames[1..]),
            "MSETNX" => parse_msetnx(&frames[1..]),
            "GETSET" => parse_getset(&frames[1..]),
            "EXPIRE" => parse_expire(&frames[1..]),
            "EXPIREAT" => parse_expireat(&frames[1..]),
            "TTL" => parse_ttl(&frames[1..]),
            "PERSIST" => parse_persist(&frames[1..]),
            "PTTL" => parse_pttl(&frames[1..]),
            "PEXPIRE" => parse_pexpire(&frames[1..]),
            "PEXPIREAT" => parse_pexpireat_cmd(&frames[1..]),
            "DBSIZE" => parse_dbsize(&frames[1..]),
            "INFO" => parse_info(&frames[1..]),
            "BGSAVE" => parse_bgsave(&frames[1..]),
            "BGREWRITEAOF" => parse_bgrewriteaof(&frames[1..]),
            "FLUSHDB" => parse_flushdb(&frames[1..]),
            "FLUSHALL" => parse_flushall(&frames[1..]),
            "MEMORY" => parse_memory_cmd(&frames[1..]),
            "SCAN" => parse_scan(&frames[1..]),
            "SSCAN" => parse_key_scan(&frames[1..], "SSCAN"),
            "HSCAN" => parse_key_scan(&frames[1..], "HSCAN"),
            "ZSCAN" => parse_key_scan(&frames[1..], "ZSCAN"),
            "LPUSH" => parse_lpush(&frames[1..]),
            "RPUSH" => parse_rpush(&frames[1..]),
            "LPOP" => parse_lpop(&frames[1..]),
            "RPOP" => parse_rpop(&frames[1..]),
            "LRANGE" => parse_lrange(&frames[1..]),
            "LLEN" => parse_llen(&frames[1..]),
            "BLPOP" => parse_blpop(&frames[1..]),
            "BRPOP" => parse_brpop(&frames[1..]),
            "LINDEX" => parse_lindex(&frames[1..]),
            "LSET" => parse_lset(&frames[1..]),
            "LTRIM" => parse_ltrim(&frames[1..]),
            "LINSERT" => parse_linsert(&frames[1..]),
            "LREM" => parse_lrem(&frames[1..]),
            "LPOS" => parse_lpos(&frames[1..]),
            "LMOVE" => parse_lmove(&frames[1..]),
            "GETDEL" => parse_getdel(&frames[1..]),
            "GETEX" => parse_getex(&frames[1..]),
            "TYPE" => parse_type(&frames[1..]),
            "ZADD" => parse_zadd(&frames[1..]),
            "ZREM" => parse_zrem(&frames[1..]),
            "ZSCORE" => parse_zscore(&frames[1..]),
            "ZRANK" => parse_zrank(&frames[1..]),
            "ZREVRANK" => parse_zrevrank(&frames[1..]),
            "ZCARD" => parse_zcard(&frames[1..]),
            "ZRANGE" => parse_zrange(&frames[1..]),
            "ZREVRANGE" => parse_zrevrange(&frames[1..]),
            "ZCOUNT" => parse_zcount(&frames[1..]),
            "ZINCRBY" => parse_zincrby(&frames[1..]),
            "ZRANGEBYSCORE" => parse_zrangebyscore(&frames[1..]),
            "ZREVRANGEBYSCORE" => parse_zrevrangebyscore(&frames[1..]),
            "ZPOPMIN" => {
                let (key, count) = parse_zpop_args(&frames[1..], "ZPOPMIN")?;
                Ok(Command::ZPopMin { key, count })
            }
            "ZPOPMAX" => {
                let (key, count) = parse_zpop_args(&frames[1..], "ZPOPMAX")?;
                Ok(Command::ZPopMax { key, count })
            }
            "LMPOP" => parse_lmpop(&frames[1..]),
            "ZMPOP" => parse_zmpop(&frames[1..]),
            "ZDIFF" => parse_zset_multi("ZDIFF", &frames[1..]),
            "ZINTER" => parse_zset_multi("ZINTER", &frames[1..]),
            "ZUNION" => parse_zset_multi("ZUNION", &frames[1..]),
            "ZDIFFSTORE" => parse_zset_store("ZDIFFSTORE", &frames[1..]),
            "ZINTERSTORE" => parse_zset_store("ZINTERSTORE", &frames[1..]),
            "ZUNIONSTORE" => parse_zset_store("ZUNIONSTORE", &frames[1..]),
            "ZRANDMEMBER" => parse_zrandmember(&frames[1..]),
            "HSET" => parse_hset(&frames[1..]),
            "HGET" => parse_hget(&frames[1..]),
            "HGETALL" => parse_hgetall(&frames[1..]),
            "HDEL" => parse_hdel(&frames[1..]),
            "HEXISTS" => parse_hexists(&frames[1..]),
            "HLEN" => parse_hlen(&frames[1..]),
            "HINCRBY" => parse_hincrby(&frames[1..]),
            "HINCRBYFLOAT" => parse_hincrbyfloat(&frames[1..]),
            "HKEYS" => parse_hkeys(&frames[1..]),
            "HVALS" => parse_hvals(&frames[1..]),
            "HMGET" => parse_hmget(&frames[1..]),
            "HRANDFIELD" => parse_hrandfield(&frames[1..]),
            "SADD" => parse_sadd(&frames[1..]),
            "SREM" => parse_srem(&frames[1..]),
            "SMEMBERS" => parse_smembers(&frames[1..]),
            "SISMEMBER" => parse_sismember(&frames[1..]),
            "SCARD" => parse_scard(&frames[1..]),
            "SUNION" => parse_multi_key_set("SUNION", &frames[1..]),
            "SINTER" => parse_multi_key_set("SINTER", &frames[1..]),
            "SDIFF" => parse_multi_key_set("SDIFF", &frames[1..]),
            "SUNIONSTORE" => parse_store_set("SUNIONSTORE", &frames[1..]),
            "SINTERSTORE" => parse_store_set("SINTERSTORE", &frames[1..]),
            "SDIFFSTORE" => parse_store_set("SDIFFSTORE", &frames[1..]),
            "SRANDMEMBER" => parse_srandmember(&frames[1..]),
            "SPOP" => parse_spop(&frames[1..]),
            "SMISMEMBER" => parse_smismember(&frames[1..]),
            "SMOVE" => parse_smove(&frames[1..]),
            "SINTERCARD" => parse_sintercard(&frames[1..]),
            "EXPIRETIME" => parse_expiretime(&frames[1..]),
            "PEXPIRETIME" => parse_pexpiretime(&frames[1..]),
            "CLUSTER" => parse_cluster(&frames[1..]),
            "ASKING" => parse_asking(&frames[1..]),
            "MIGRATE" => parse_migrate(&frames[1..]),
            "RESTORE" => parse_restore(&frames[1..]),
            "CONFIG" => parse_config(&frames[1..]),
            "COMMAND" => parse_command_cmd(&frames[1..]),
            "MULTI" => parse_no_args("MULTI", &frames[1..], Command::Multi),
            "EXEC" => parse_no_args("EXEC", &frames[1..], Command::Exec),
            "DISCARD" => parse_no_args("DISCARD", &frames[1..], Command::Discard),
            "WATCH" => parse_watch(&frames[1..]),
            "UNWATCH" => parse_no_args("UNWATCH", &frames[1..], Command::Unwatch),
            "SLOWLOG" => parse_slowlog(&frames[1..]),
            "SUBSCRIBE" => parse_subscribe(&frames[1..]),
            "UNSUBSCRIBE" => parse_unsubscribe(&frames[1..]),
            "PSUBSCRIBE" => parse_psubscribe(&frames[1..]),
            "PUNSUBSCRIBE" => parse_punsubscribe(&frames[1..]),
            "PUBLISH" => parse_publish(&frames[1..]),
            "PUBSUB" => parse_pubsub(&frames[1..]),
            "VADD" => parse_vadd(&frames[1..]),
            "VADD_BATCH" => parse_vadd_batch(&frames[1..]),
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
            "PROTO.SCAN" => parse_proto_scan(&frames[1..]),
            "PROTO.FIND" => parse_proto_find(&frames[1..]),
            "TIME" => parse_no_args("TIME", &frames[1..], Command::Time),
            "LASTSAVE" => parse_no_args("LASTSAVE", &frames[1..], Command::LastSave),
            "ROLE" => parse_no_args("ROLE", &frames[1..], Command::Role),
            "WAIT" => parse_wait(&frames[1..]),
            "OBJECT" => parse_object(&frames[1..]),
            "COPY" => parse_copy(&frames[1..]),
            "CLIENT" => parse_client(&frames[1..]),
            "ACL" => parse_acl(&frames[1..]),
            "AUTH" => parse_auth(&frames[1..]),
            "QUIT" => parse_quit(&frames[1..]),
            "MONITOR" => parse_monitor(&frames[1..]),
            "RANDOMKEY" => parse_no_args("RANDOMKEY", &frames[1..], Command::RandomKey),
            "TOUCH" => parse_touch(&frames[1..]),
            "SORT" => parse_sort(&frames[1..]),
            _ => {
                // only allocate for truly unknown commands
                let name = extract_string(&frames[0])?;
                Ok(Command::Unknown(name))
            }
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
        Frame::Simple(s) => Ok(Bytes::copy_from_slice(s.as_bytes())),
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

/// Maximum length for a command name or keyword uppercased on the stack.
/// All Redis/Ember commands and subcommands are well under this limit.
const MAX_KEYWORD_LEN: usize = 32;

/// Returns the raw bytes of a Bulk or Simple frame without allocating.
fn extract_raw_bytes(frame: &Frame) -> Result<&[u8], ProtocolError> {
    match frame {
        Frame::Bulk(data) => Ok(data.as_ref()),
        Frame::Simple(s) => Ok(s.as_bytes()),
        _ => Err(ProtocolError::InvalidCommandFrame(
            "expected bulk or simple string".into(),
        )),
    }
}

/// Uppercases a frame's bytes into a stack buffer and returns the result as `&str`.
///
/// Used for command names, subcommands, and option flags where the value is always
/// a short ASCII keyword. Avoids the two heap allocations that
/// `extract_string()?.to_ascii_uppercase()` would require.
fn uppercase_arg<'b>(
    frame: &Frame,
    buf: &'b mut [u8; MAX_KEYWORD_LEN],
) -> Result<&'b str, ProtocolError> {
    let bytes = extract_raw_bytes(frame)?;
    let len = bytes.len();
    if len > MAX_KEYWORD_LEN {
        return Err(ProtocolError::InvalidCommandFrame(
            "keyword too long".into(),
        ));
    }
    buf[..len].copy_from_slice(bytes);
    buf[..len].make_ascii_uppercase();
    std::str::from_utf8(&buf[..len])
        .map_err(|_| ProtocolError::InvalidCommandFrame("keyword is not valid utf-8".into()))
}

/// Parses a frame's bytes directly as a positive u64 without allocating a String.
fn parse_u64(frame: &Frame, cmd: &str) -> Result<u64, ProtocolError> {
    let bytes = extract_raw_bytes(frame)?;
    parse_u64_bytes(bytes).ok_or_else(|| {
        ProtocolError::InvalidCommandFrame(format!("value is not a valid integer for '{cmd}'"))
    })
}

/// Parses a frame's bytes as a `usize`. Used for optional count arguments.
fn parse_usize(frame: &Frame, cmd: &str) -> Result<usize, ProtocolError> {
    let n = parse_u64(frame, cmd)?;
    usize::try_from(n).map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!("count is out of range for '{cmd}'"))
    })
}

/// Parses an unsigned integer directly from a byte slice.
fn parse_u64_bytes(buf: &[u8]) -> Option<u64> {
    if buf.is_empty() {
        return None;
    }
    let mut n: u64 = 0;
    for &b in buf {
        if !b.is_ascii_digit() {
            return None;
        }
        n = n.checked_mul(10)?.checked_add((b - b'0') as u64)?;
    }
    Some(n)
}

/// Shorthand for the wrong-arity error returned by every parser.
fn wrong_arity(cmd: &'static str) -> ProtocolError {
    ProtocolError::WrongArity(cmd.into())
}

fn parse_ping(args: &[Frame]) -> Result<Command, ProtocolError> {
    match args.len() {
        0 => Ok(Command::Ping(None)),
        1 => {
            let msg = extract_bytes(&args[0])?;
            Ok(Command::Ping(Some(msg)))
        }
        _ => Err(wrong_arity("PING")),
    }
}

fn parse_echo(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("ECHO"));
    }
    let msg = extract_bytes(&args[0])?;
    Ok(Command::Echo(msg))
}

fn parse_get(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("GET"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Get { key })
}

/// Parses NX / XX / EX / PX options from a slice of command arguments.
///
/// Returns `(expire, nx, xx)`. `cmd` is used in error messages.
fn parse_set_options(
    args: &[Frame],
    cmd: &'static str,
) -> Result<(Option<SetExpire>, bool, bool), ProtocolError> {
    let mut expire = None;
    let mut nx = false;
    let mut xx = false;
    let mut idx = 0;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let flag = uppercase_arg(&args[idx], &mut kw)?;
        match flag {
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
                    return Err(wrong_arity(cmd));
                }
                let amount = parse_u64(&args[idx], cmd)?;
                if amount == 0 {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "invalid expire time in '{cmd}' command"
                    )));
                }
                expire = Some(SetExpire::Ex(amount));
                idx += 1;
            }
            "PX" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity(cmd));
                }
                let amount = parse_u64(&args[idx], cmd)?;
                if amount == 0 {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "invalid expire time in '{cmd}' command"
                    )));
                }
                expire = Some(SetExpire::Px(amount));
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported {cmd} option '{flag}'"
                )));
            }
        }
    }

    if nx && xx {
        return Err(ProtocolError::InvalidCommandFrame(
            "XX and NX options at the same time are not compatible".into(),
        ));
    }

    Ok((expire, nx, xx))
}

fn parse_set(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("SET"));
    }

    let key = extract_string(&args[0])?;
    let value = extract_bytes(&args[1])?;
    let (expire, nx, xx) = parse_set_options(&args[2..], "SET")?;

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
        return Err(wrong_arity("INCR"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Incr { key })
}

fn parse_decr(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("DECR"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Decr { key })
}

fn parse_incrby(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("INCRBY"));
    }
    let key = extract_string(&args[0])?;
    let delta = parse_i64(&args[1], "INCRBY")?;
    Ok(Command::IncrBy { key, delta })
}

fn parse_decrby(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("DECRBY"));
    }
    let key = extract_string(&args[0])?;
    let delta = parse_i64(&args[1], "DECRBY")?;
    Ok(Command::DecrBy { key, delta })
}

fn parse_incrbyfloat(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("INCRBYFLOAT"));
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
        return Err(wrong_arity("APPEND"));
    }
    let key = extract_string(&args[0])?;
    let value = extract_bytes(&args[1])?;
    Ok(Command::Append { key, value })
}

fn parse_strlen(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("STRLEN"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Strlen { key })
}

/// SETNX key value — set key only if it does not exist.
/// Equivalent to `SET key value NX`.
fn parse_setnx(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("SETNX"));
    }
    let key = extract_string(&args[0])?;
    let value = extract_bytes(&args[1])?;
    Ok(Command::Set {
        key,
        value,
        expire: None,
        nx: true,
        xx: false,
    })
}

/// SETEX key seconds value — set key with an expiration in seconds.
/// Equivalent to `SET key value EX seconds`.
fn parse_setex(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("SETEX"));
    }
    let key = extract_string(&args[0])?;
    let seconds = parse_u64(&args[1], "SETEX")?;
    if seconds == 0 {
        return Err(ProtocolError::InvalidCommandFrame(
            "invalid expire time in 'SETEX' command".into(),
        ));
    }
    let value = extract_bytes(&args[2])?;
    Ok(Command::Set {
        key,
        value,
        expire: Some(SetExpire::Ex(seconds)),
        nx: false,
        xx: false,
    })
}

/// PSETEX key milliseconds value — set key with an expiration in milliseconds.
/// Equivalent to `SET key value PX milliseconds`.
fn parse_psetex(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("PSETEX"));
    }
    let key = extract_string(&args[0])?;
    let ms = parse_u64(&args[1], "PSETEX")?;
    if ms == 0 {
        return Err(ProtocolError::InvalidCommandFrame(
            "invalid expire time in 'PSETEX' command".into(),
        ));
    }
    let value = extract_bytes(&args[2])?;
    Ok(Command::Set {
        key,
        value,
        expire: Some(SetExpire::Px(ms)),
        nx: false,
        xx: false,
    })
}

/// GETRANGE key start end (also aliases SUBSTR).
fn parse_getrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("GETRANGE"));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "GETRANGE")?;
    let end = parse_i64(&args[2], "GETRANGE")?;
    Ok(Command::GetRange { key, start, end })
}

/// SETRANGE key offset value.
fn parse_setrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("SETRANGE"));
    }
    let key = extract_string(&args[0])?;
    let offset = parse_u64(&args[1], "SETRANGE")? as usize;
    let value = extract_bytes(&args[2])?;
    Ok(Command::SetRange { key, offset, value })
}

/// GETBIT key offset.
fn parse_getbit(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("GETBIT"));
    }
    let key = extract_string(&args[0])?;
    let offset = parse_u64(&args[1], "GETBIT")?;
    Ok(Command::GetBit { key, offset })
}

/// SETBIT key offset value.
fn parse_setbit(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("SETBIT"));
    }
    let key = extract_string(&args[0])?;
    let offset = parse_u64(&args[1], "SETBIT")?;
    let raw = parse_u64(&args[2], "SETBIT")?;
    if raw > 1 {
        return Err(ProtocolError::InvalidCommandFrame(
            "SETBIT: bit value must be 0 or 1".into(),
        ));
    }
    Ok(Command::SetBit {
        key,
        offset,
        value: raw as u8,
    })
}

/// Parses an optional `[start end [BYTE|BIT]]` suffix for BITCOUNT / BITPOS.
///
/// Accepts 0, 2, or 3 trailing arguments. Returns `None` when there are none.
fn parse_bit_range(args: &[Frame], cmd: &str) -> Result<Option<BitRange>, ProtocolError> {
    match args.len() {
        0 => Ok(None),
        2 | 3 => {
            let start = parse_i64(&args[0], cmd)?;
            let end = parse_i64(&args[1], cmd)?;
            let unit = if args.len() == 3 {
                let mut kw = [0u8; MAX_KEYWORD_LEN];
                match uppercase_arg(&args[2], &mut kw)? {
                    "BYTE" => BitRangeUnit::Byte,
                    "BIT" => BitRangeUnit::Bit,
                    other => {
                        return Err(ProtocolError::InvalidCommandFrame(format!(
                            "{cmd}: invalid unit '{other}', expected BYTE or BIT"
                        )));
                    }
                }
            } else {
                BitRangeUnit::Byte
            };
            Ok(Some(BitRange { start, end, unit }))
        }
        _ => Err(ProtocolError::InvalidCommandFrame(format!(
            "{cmd}: wrong number of arguments"
        ))),
    }
}

/// BITCOUNT key [start end [BYTE|BIT]].
fn parse_bitcount(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("BITCOUNT"));
    }
    let key = extract_string(&args[0])?;
    let range = parse_bit_range(&args[1..], "BITCOUNT")?;
    Ok(Command::BitCount { key, range })
}

/// BITPOS key bit [start [end [BYTE|BIT]]].
///
/// Redis allows 1, 2, or 3 trailing args (start, start+end, start+end+unit).
/// No trailing args means "search the whole string".
fn parse_bitpos(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("BITPOS"));
    }
    let key = extract_string(&args[0])?;
    let raw = parse_u64(&args[1], "BITPOS")?;
    if raw > 1 {
        return Err(ProtocolError::InvalidCommandFrame(
            "BITPOS: bit value must be 0 or 1".into(),
        ));
    }
    let bit = raw as u8;
    let range = match args.len() - 2 {
        0 => None,
        1 => {
            let start = parse_i64(&args[2], "BITPOS")?;
            Some(BitRange {
                start,
                end: -1,
                unit: BitRangeUnit::Byte,
            })
        }
        2 | 3 => parse_bit_range(&args[2..], "BITPOS")?,
        _ => {
            return Err(ProtocolError::InvalidCommandFrame(
                "BITPOS: wrong number of arguments".into(),
            ))
        }
    };
    Ok(Command::BitPos { key, bit, range })
}

/// BITOP AND|OR|XOR|NOT destkey key [key ...].
fn parse_bitop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(wrong_arity("BITOP"));
    }
    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let op = match uppercase_arg(&args[0], &mut kw)? {
        "AND" => BitOpKind::And,
        "OR" => BitOpKind::Or,
        "XOR" => BitOpKind::Xor,
        "NOT" => BitOpKind::Not,
        other => {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "BITOP: unknown operation '{other}'"
            )));
        }
    };
    let dest = extract_string(&args[1])?;
    let keys = extract_strings(&args[2..])?;
    if op == BitOpKind::Not && keys.len() != 1 {
        return Err(ProtocolError::InvalidCommandFrame(
            "BITOP NOT must be called with a single source key".into(),
        ));
    }
    Ok(Command::BitOp { op, dest, keys })
}

fn parse_keys(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("KEYS"));
    }
    let pattern = extract_string(&args[0])?;
    Ok(Command::Keys { pattern })
}

fn parse_rename(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("RENAME"));
    }
    let key = extract_string(&args[0])?;
    let newkey = extract_string(&args[1])?;
    Ok(Command::Rename { key, newkey })
}

fn parse_del(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("DEL"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Del { keys })
}

fn parse_exists(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("EXISTS"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Exists { keys })
}

fn parse_mget(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("MGET"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::MGet { keys })
}

fn parse_mset(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err(wrong_arity("MSET"));
    }
    let mut pairs = Vec::with_capacity(args.len() / 2);
    for chunk in args.chunks(2) {
        let key = extract_string(&chunk[0])?;
        let value = extract_bytes(&chunk[1])?;
        pairs.push((key, value));
    }
    Ok(Command::MSet { pairs })
}

fn parse_msetnx(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err(wrong_arity("MSETNX"));
    }
    let mut pairs = Vec::with_capacity(args.len() / 2);
    for chunk in args.chunks(2) {
        let key = extract_string(&chunk[0])?;
        let value = extract_bytes(&chunk[1])?;
        pairs.push((key, value));
    }
    Ok(Command::MSetNx { pairs })
}

fn parse_getset(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("GETSET"));
    }
    let key = extract_string(&args[0])?;
    let value = extract_bytes(&args[1])?;
    Ok(Command::GetSet { key, value })
}

fn parse_expire(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("EXPIRE"));
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
        return Err(wrong_arity("TTL"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Ttl { key })
}

fn parse_persist(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("PERSIST"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Persist { key })
}

fn parse_pttl(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("PTTL"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Pttl { key })
}

fn parse_pexpire(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("PEXPIRE"));
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

fn parse_expireat(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("EXPIREAT"));
    }
    let key = extract_string(&args[0])?;
    let timestamp = parse_u64(&args[1], "EXPIREAT")?;
    Ok(Command::Expireat { key, timestamp })
}

fn parse_pexpireat_cmd(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("PEXPIREAT"));
    }
    let key = extract_string(&args[0])?;
    let timestamp_ms = parse_u64(&args[1], "PEXPIREAT")?;
    Ok(Command::Pexpireat { key, timestamp_ms })
}

fn parse_expiretime(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("EXPIRETIME"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Expiretime { key })
}

fn parse_pexpiretime(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("PEXPIRETIME"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Pexpiretime { key })
}

fn parse_smove(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("SMOVE"));
    }
    let source = extract_string(&args[0])?;
    let destination = extract_string(&args[1])?;
    let member = extract_string(&args[2])?;
    Ok(Command::SMove {
        source,
        destination,
        member,
    })
}

fn parse_sintercard(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("SINTERCARD"));
    }
    let numkeys = parse_u64(&args[0], "SINTERCARD")? as usize;
    if numkeys == 0 || args.len() < 1 + numkeys {
        return Err(ProtocolError::InvalidCommandFrame(
            "SINTERCARD numkeys must be positive and match the number of keys provided".into(),
        ));
    }
    let keys: Vec<String> = args[1..=numkeys]
        .iter()
        .map(extract_string)
        .collect::<Result<_, _>>()?;
    // optional LIMIT n
    let limit = if args.len() == numkeys + 3 {
        let tag = extract_string(&args[numkeys + 1])?.to_ascii_uppercase();
        if tag != "LIMIT" {
            return Err(ProtocolError::InvalidCommandFrame(
                "SINTERCARD: expected LIMIT keyword".into(),
            ));
        }
        parse_u64(&args[numkeys + 2], "SINTERCARD")? as usize
    } else if args.len() == numkeys + 1 {
        0
    } else {
        return Err(wrong_arity("SINTERCARD"));
    };
    Ok(Command::SInterCard { keys, limit })
}

fn parse_dbsize(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("DBSIZE"));
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
        _ => Err(wrong_arity("INFO")),
    }
}

fn parse_bgsave(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("BGSAVE"));
    }
    Ok(Command::BgSave)
}

fn parse_bgrewriteaof(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("BGREWRITEAOF"));
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
    Err(wrong_arity("FLUSHDB"))
}

fn parse_flushall(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Ok(Command::FlushAll { async_mode: false });
    }
    if args.len() == 1 {
        let arg = extract_string(&args[0])?;
        if arg.eq_ignore_ascii_case("ASYNC") {
            return Ok(Command::FlushAll { async_mode: true });
        }
    }
    Err(wrong_arity("FLUSHALL"))
}

fn parse_memory_cmd(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("memory".into()));
    }
    let subcommand = extract_string(&args[0])?;
    if subcommand.eq_ignore_ascii_case("USAGE") {
        if args.len() < 2 {
            return Err(wrong_arity("MEMORY USAGE"));
        }
        let key = extract_string(&args[1])?;
        // Accept but ignore SAMPLES count — we always use the cached value size.
        Ok(Command::MemoryUsage { key })
    } else {
        Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown subcommand '{}' for 'memory' command",
            subcommand
        )))
    }
}

fn parse_unlink(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("UNLINK"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Unlink { keys })
}

fn parse_scan(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("SCAN"));
    }

    let cursor = parse_u64(&args[0], "SCAN")?;
    let mut pattern = None;
    let mut count = None;
    let mut idx = 1;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let flag = uppercase_arg(&args[idx], &mut kw)?;
        match flag {
            "MATCH" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("SCAN"));
                }
                pattern = Some(extract_string(&args[idx])?);
                idx += 1;
            }
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("SCAN"));
                }
                let n = parse_u64(&args[idx], "SCAN")?;
                if n > MAX_SCAN_COUNT {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "SCAN COUNT {n} exceeds max {MAX_SCAN_COUNT}"
                    )));
                }
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

/// Shared parser for SSCAN, HSCAN, ZSCAN.
///
/// All three share the same shape: `key cursor [MATCH pattern] [COUNT count]`.
fn parse_key_scan(args: &[Frame], cmd: &'static str) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity(cmd));
    }

    let key = extract_string(&args[0])?;
    let cursor = parse_u64(&args[1], cmd)?;
    let mut pattern = None;
    let mut count = None;
    let mut idx = 2;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let flag = uppercase_arg(&args[idx], &mut kw)?;
        match flag {
            "MATCH" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity(cmd));
                }
                pattern = Some(extract_string(&args[idx])?);
                idx += 1;
            }
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity(cmd));
                }
                let n = parse_u64(&args[idx], cmd)?;
                if n > MAX_SCAN_COUNT {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd} COUNT {n} exceeds max {MAX_SCAN_COUNT}"
                    )));
                }
                count = Some(n as usize);
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported {cmd} option '{flag}'"
                )));
            }
        }
    }

    match cmd {
        "SSCAN" => Ok(Command::SScan {
            key,
            cursor,
            pattern,
            count,
        }),
        "HSCAN" => Ok(Command::HScan {
            key,
            cursor,
            pattern,
            count,
        }),
        "ZSCAN" => Ok(Command::ZScan {
            key,
            cursor,
            pattern,
            count,
        }),
        _ => Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown scan command '{cmd}'"
        ))),
    }
}

/// Parses a frame's bytes directly as an i64 without allocating a String.
fn parse_i64(frame: &Frame, cmd: &str) -> Result<i64, ProtocolError> {
    let bytes = extract_raw_bytes(frame)?;
    parse_i64_bytes(bytes).ok_or_else(|| {
        ProtocolError::InvalidCommandFrame(format!("value is not a valid integer for '{cmd}'"))
    })
}

/// Parses a signed integer directly from a byte slice. Accumulates in the
/// negative direction for negative numbers so that `i64::MIN` is representable.
fn parse_i64_bytes(buf: &[u8]) -> Option<i64> {
    if buf.is_empty() {
        return None;
    }
    let (negative, digits) = if buf[0] == b'-' {
        (true, &buf[1..])
    } else {
        (false, buf)
    };
    if digits.is_empty() {
        return None;
    }
    if negative {
        let mut n: i64 = 0;
        for &b in digits {
            if !b.is_ascii_digit() {
                return None;
            }
            n = n.checked_mul(10)?.checked_sub((b - b'0') as i64)?;
        }
        Some(n)
    } else {
        let mut n: i64 = 0;
        for &b in digits {
            if !b.is_ascii_digit() {
                return None;
            }
            n = n.checked_mul(10)?.checked_add((b - b'0') as i64)?;
        }
        Some(n)
    }
}

fn parse_lpush(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("LPUSH"));
    }
    let key = extract_string(&args[0])?;
    let values = extract_bytes_vec(&args[1..])?;
    Ok(Command::LPush { key, values })
}

fn parse_rpush(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("RPUSH"));
    }
    let key = extract_string(&args[0])?;
    let values = extract_bytes_vec(&args[1..])?;
    Ok(Command::RPush { key, values })
}

fn parse_lpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() || args.len() > 2 {
        return Err(wrong_arity("LPOP"));
    }
    let key = extract_string(&args[0])?;
    let count = if args.len() == 2 {
        Some(parse_usize(&args[1], "LPOP")?)
    } else {
        None
    };
    Ok(Command::LPop { key, count })
}

fn parse_rpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() || args.len() > 2 {
        return Err(wrong_arity("RPOP"));
    }
    let key = extract_string(&args[0])?;
    let count = if args.len() == 2 {
        Some(parse_usize(&args[1], "RPOP")?)
    } else {
        None
    };
    Ok(Command::RPop { key, count })
}

fn parse_lrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("LRANGE"));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "LRANGE")?;
    let stop = parse_i64(&args[2], "LRANGE")?;
    Ok(Command::LRange { key, start, stop })
}

fn parse_llen(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("LLEN"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::LLen { key })
}

/// Parses BLPOP/BRPOP: all args except the last are keys, the last is the
/// timeout in seconds (float). At least one key is required.
fn parse_blpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("BLPOP"));
    }
    let timeout_secs = parse_timeout(&args[args.len() - 1], "BLPOP")?;
    let keys = extract_strings(&args[..args.len() - 1])?;
    Ok(Command::BLPop { keys, timeout_secs })
}

fn parse_brpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("BRPOP"));
    }
    let timeout_secs = parse_timeout(&args[args.len() - 1], "BRPOP")?;
    let keys = extract_strings(&args[..args.len() - 1])?;
    Ok(Command::BRPop { keys, timeout_secs })
}

fn parse_lindex(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("LINDEX"));
    }
    let key = extract_string(&args[0])?;
    let index = parse_i64(&args[1], "LINDEX")?;
    Ok(Command::LIndex { key, index })
}

fn parse_lset(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("LSET"));
    }
    let key = extract_string(&args[0])?;
    let index = parse_i64(&args[1], "LSET")?;
    let value = extract_bytes(&args[2])?;
    Ok(Command::LSet { key, index, value })
}

fn parse_ltrim(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("LTRIM"));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "LTRIM")?;
    let stop = parse_i64(&args[2], "LTRIM")?;
    Ok(Command::LTrim { key, start, stop })
}

fn parse_linsert(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 4 {
        return Err(wrong_arity("LINSERT"));
    }
    let key = extract_string(&args[0])?;
    let direction = extract_string(&args[1])?;
    let before = match direction.to_ascii_uppercase().as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        _ => {
            return Err(ProtocolError::InvalidCommandFrame(
                "ERR syntax error".into(),
            ))
        }
    };
    let pivot = extract_bytes(&args[2])?;
    let value = extract_bytes(&args[3])?;
    Ok(Command::LInsert {
        key,
        before,
        pivot,
        value,
    })
}

fn parse_lrem(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("LREM"));
    }
    let key = extract_string(&args[0])?;
    let count = parse_i64(&args[1], "LREM")?;
    let value = extract_bytes(&args[2])?;
    Ok(Command::LRem { key, count, value })
}

fn parse_lpos(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("LPOS"));
    }
    let key = extract_string(&args[0])?;
    if args.len() < 2 {
        return Err(wrong_arity("LPOS"));
    }
    let element = extract_bytes(&args[1])?;

    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: usize = 0;

    let mut i = 2;
    while i < args.len() {
        let opt = extract_string(&args[i])?.to_ascii_uppercase();
        match opt.as_str() {
            "RANK" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "ERR syntax error".into(),
                    ));
                }
                rank = parse_i64(&args[i], "LPOS")?;
                if rank == 0 {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative values for starting from the end of the list".into(),
                    ));
                }
            }
            "COUNT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "ERR syntax error".into(),
                    ));
                }
                let n = parse_i64(&args[i], "LPOS")?;
                if n < 0 {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "ERR COUNT can't be negative".into(),
                    ));
                }
                count = Some(n as usize);
            }
            "MAXLEN" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "ERR syntax error".into(),
                    ));
                }
                let n = parse_i64(&args[i], "LPOS")?;
                if n < 0 {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "ERR MAXLEN can't be negative".into(),
                    ));
                }
                maxlen = n as usize;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(
                    "ERR syntax error".into(),
                ))
            }
        }
        i += 1;
    }

    Ok(Command::LPos {
        key,
        element,
        rank,
        count,
        maxlen,
    })
}

/// Extracts the timeout argument for BLPOP/BRPOP. Redis accepts integer or
/// float seconds; negative values are an error.
fn parse_timeout(frame: &Frame, cmd: &str) -> Result<f64, ProtocolError> {
    let bytes = extract_raw_bytes(frame)?;
    let s = std::str::from_utf8(bytes).map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!(
            "timeout is not a float or out of range for '{cmd}'"
        ))
    })?;
    let val: f64 = s.parse().map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!(
            "timeout is not a float or out of range for '{cmd}'"
        ))
    })?;
    if val < 0.0 {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "timeout is negative for '{cmd}'"
        )));
    }
    Ok(val)
}

fn parse_type(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("TYPE"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Type { key })
}

/// Parses a string argument as an f64 score.
fn parse_f64(frame: &Frame, cmd: &str) -> Result<f64, ProtocolError> {
    let bytes = extract_raw_bytes(frame)?;
    let s = std::str::from_utf8(bytes).map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!("value is not a valid float for '{cmd}'"))
    })?;
    let v = s.parse::<f64>().map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!("value is not a valid float for '{cmd}'"))
    })?;
    if v.is_nan() || v.is_infinite() {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "value is not a valid finite float for '{cmd}'"
        )));
    }
    Ok(v)
}

fn parse_zadd(args: &[Frame]) -> Result<Command, ProtocolError> {
    // ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
    if args.len() < 3 {
        return Err(wrong_arity("ZADD"));
    }

    let key = extract_string(&args[0])?;
    let mut flags = ZAddFlags::default();
    let mut idx = 1;

    // parse optional flags before score/member pairs
    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let s = uppercase_arg(&args[idx], &mut kw)?;
        match s {
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
        return Err(wrong_arity("ZADD"));
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
        return Err(wrong_arity("ZCARD"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::ZCard { key })
}

fn parse_zrem(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("ZREM"));
    }
    let key = extract_string(&args[0])?;
    let members = extract_strings(&args[1..])?;
    Ok(Command::ZRem { key, members })
}

fn parse_zscore(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("ZSCORE"));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::ZScore { key, member })
}

fn parse_zrank(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("ZRANK"));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::ZRank { key, member })
}

fn parse_zrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 || args.len() > 4 {
        return Err(wrong_arity("ZRANGE"));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "ZRANGE")?;
    let stop = parse_i64(&args[2], "ZRANGE")?;

    let with_scores = if args.len() == 4 {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let opt = uppercase_arg(&args[3], &mut kw)?;
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

fn parse_zrevrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 || args.len() > 4 {
        return Err(wrong_arity("ZREVRANGE"));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "ZREVRANGE")?;
    let stop = parse_i64(&args[2], "ZREVRANGE")?;

    let with_scores = if args.len() == 4 {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let opt = uppercase_arg(&args[3], &mut kw)?;
        if opt != "WITHSCORES" {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "unsupported ZREVRANGE option '{opt}'"
            )));
        }
        true
    } else {
        false
    };

    Ok(Command::ZRevRange {
        key,
        start,
        stop,
        with_scores,
    })
}

fn parse_zrevrank(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("ZREVRANK"));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::ZRevRank { key, member })
}

/// Parses a Redis score bound string.
///
/// Supports `-inf`, `+inf`, `inf`, exclusive `(value`, and plain inclusive values.
fn parse_score_bound(frame: &Frame, cmd: &str) -> Result<ScoreBound, ProtocolError> {
    let bytes = extract_raw_bytes(frame)?;
    let s = std::str::from_utf8(bytes).map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!("invalid score bound for '{cmd}'"))
    })?;

    match s {
        "-inf" => Ok(ScoreBound::NegInf),
        "+inf" | "inf" => Ok(ScoreBound::PosInf),
        _ if s.starts_with('(') => {
            let val = s[1..].parse::<f64>().map_err(|_| {
                ProtocolError::InvalidCommandFrame(format!("min or max is not a float for '{cmd}'"))
            })?;
            Ok(ScoreBound::Exclusive(val))
        }
        _ => {
            let val = s.parse::<f64>().map_err(|_| {
                ProtocolError::InvalidCommandFrame(format!("min or max is not a float for '{cmd}'"))
            })?;
            Ok(ScoreBound::Inclusive(val))
        }
    }
}

fn parse_zcount(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("ZCOUNT"));
    }
    let key = extract_string(&args[0])?;
    let min = parse_score_bound(&args[1], "ZCOUNT")?;
    let max = parse_score_bound(&args[2], "ZCOUNT")?;
    Ok(Command::ZCount { key, min, max })
}

fn parse_zincrby(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("ZINCRBY"));
    }
    let key = extract_string(&args[0])?;
    let increment = parse_f64(&args[1], "ZINCRBY")?;
    let member = extract_string(&args[2])?;
    Ok(Command::ZIncrBy {
        key,
        increment,
        member,
    })
}

/// Parses ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
fn parse_zrangebyscore(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(wrong_arity("ZRANGEBYSCORE"));
    }
    let key = extract_string(&args[0])?;
    let min = parse_score_bound(&args[1], "ZRANGEBYSCORE")?;
    let max = parse_score_bound(&args[2], "ZRANGEBYSCORE")?;

    let mut with_scores = false;
    let mut offset = 0usize;
    let mut count = None;
    let mut idx = 3;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let opt = uppercase_arg(&args[idx], &mut kw)?;
        match opt {
            "WITHSCORES" => {
                with_scores = true;
                idx += 1;
            }
            "LIMIT" => {
                if idx + 2 >= args.len() {
                    return Err(wrong_arity("ZRANGEBYSCORE"));
                }
                offset = parse_i64(&args[idx + 1], "ZRANGEBYSCORE")? as usize;
                count = Some(parse_i64(&args[idx + 2], "ZRANGEBYSCORE")? as usize);
                idx += 3;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported ZRANGEBYSCORE option '{opt}'"
                )));
            }
        }
    }

    Ok(Command::ZRangeByScore {
        key,
        min,
        max,
        with_scores,
        offset,
        count,
    })
}

/// Parses ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
///
/// Note: Redis reverses min/max argument order for this command.
fn parse_zrevrangebyscore(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(wrong_arity("ZREVRANGEBYSCORE"));
    }
    let key = extract_string(&args[0])?;
    // Redis: ZREVRANGEBYSCORE key max min — the order is reversed
    let max = parse_score_bound(&args[1], "ZREVRANGEBYSCORE")?;
    let min = parse_score_bound(&args[2], "ZREVRANGEBYSCORE")?;

    let mut with_scores = false;
    let mut offset = 0usize;
    let mut count = None;
    let mut idx = 3;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let opt = uppercase_arg(&args[idx], &mut kw)?;
        match opt {
            "WITHSCORES" => {
                with_scores = true;
                idx += 1;
            }
            "LIMIT" => {
                if idx + 2 >= args.len() {
                    return Err(wrong_arity("ZREVRANGEBYSCORE"));
                }
                offset = parse_i64(&args[idx + 1], "ZREVRANGEBYSCORE")? as usize;
                count = Some(parse_i64(&args[idx + 2], "ZREVRANGEBYSCORE")? as usize);
                idx += 3;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported ZREVRANGEBYSCORE option '{opt}'"
                )));
            }
        }
    }

    Ok(Command::ZRevRangeByScore {
        key,
        min,
        max,
        with_scores,
        offset,
        count,
    })
}

/// Shared argument parsing for ZPOPMIN/ZPOPMAX: key [count]
fn parse_zpop_args(args: &[Frame], cmd: &'static str) -> Result<(String, usize), ProtocolError> {
    if args.is_empty() || args.len() > 2 {
        return Err(wrong_arity(cmd));
    }
    let key = extract_string(&args[0])?;
    let count = if args.len() == 2 {
        let c = parse_i64(&args[1], cmd)?;
        if c < 0 {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "value is out of range for '{cmd}'"
            )));
        }
        c as usize
    } else {
        1
    };
    Ok((key, count))
}

// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT n]
fn parse_lmpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(wrong_arity("LMPOP"));
    }
    let numkeys = parse_u64(&args[0], "LMPOP")? as usize;
    if numkeys == 0 || args.len() < 1 + numkeys + 1 {
        return Err(ProtocolError::InvalidCommandFrame(
            "LMPOP numkeys must match key count".into(),
        ));
    }
    let keys: Vec<String> = args[1..=numkeys]
        .iter()
        .map(extract_string)
        .collect::<Result<_, _>>()?;
    let dir = extract_string(&args[numkeys + 1])?.to_ascii_uppercase();
    let left = match dir.as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => {
            return Err(ProtocolError::InvalidCommandFrame(
                "LMPOP: direction must be LEFT or RIGHT".into(),
            ))
        }
    };
    let count = if args.len() == numkeys + 4 {
        let tag = extract_string(&args[numkeys + 2])?.to_ascii_uppercase();
        if tag != "COUNT" {
            return Err(ProtocolError::InvalidCommandFrame(
                "LMPOP: expected COUNT".into(),
            ));
        }
        let n = parse_u64(&args[numkeys + 3], "LMPOP")? as usize;
        if n == 0 {
            return Err(ProtocolError::InvalidCommandFrame(
                "LMPOP: COUNT must be positive".into(),
            ));
        }
        n
    } else if args.len() == numkeys + 2 {
        1
    } else {
        return Err(wrong_arity("LMPOP"));
    };
    Ok(Command::Lmpop { keys, left, count })
}

// ZMPOP numkeys key [key ...] MIN|MAX [COUNT n]
fn parse_zmpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(wrong_arity("ZMPOP"));
    }
    let numkeys = parse_u64(&args[0], "ZMPOP")? as usize;
    if numkeys == 0 || args.len() < 1 + numkeys + 1 {
        return Err(ProtocolError::InvalidCommandFrame(
            "ZMPOP numkeys must match key count".into(),
        ));
    }
    let keys: Vec<String> = args[1..=numkeys]
        .iter()
        .map(extract_string)
        .collect::<Result<_, _>>()?;
    let order = extract_string(&args[numkeys + 1])?.to_ascii_uppercase();
    let min = match order.as_str() {
        "MIN" => true,
        "MAX" => false,
        _ => {
            return Err(ProtocolError::InvalidCommandFrame(
                "ZMPOP: order must be MIN or MAX".into(),
            ))
        }
    };
    let count = if args.len() == numkeys + 4 {
        let tag = extract_string(&args[numkeys + 2])?.to_ascii_uppercase();
        if tag != "COUNT" {
            return Err(ProtocolError::InvalidCommandFrame(
                "ZMPOP: expected COUNT".into(),
            ));
        }
        let n = parse_u64(&args[numkeys + 3], "ZMPOP")? as usize;
        if n == 0 {
            return Err(ProtocolError::InvalidCommandFrame(
                "ZMPOP: COUNT must be positive".into(),
            ));
        }
        n
    } else if args.len() == numkeys + 2 {
        1
    } else {
        return Err(wrong_arity("ZMPOP"));
    };
    Ok(Command::Zmpop { keys, min, count })
}

// --- hash commands ---

fn parse_hset(args: &[Frame]) -> Result<Command, ProtocolError> {
    // HSET key field value [field value ...]
    // args = [key, field, value, ...]
    // Need at least 3 args, and after key we need pairs (so remaining count must be even)
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err(wrong_arity("HSET"));
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
        return Err(wrong_arity("HGET"));
    }
    let key = extract_string(&args[0])?;
    let field = extract_string(&args[1])?;
    Ok(Command::HGet { key, field })
}

fn parse_hgetall(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("HGETALL"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HGetAll { key })
}

fn parse_hdel(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("HDEL"));
    }
    let key = extract_string(&args[0])?;
    let fields = extract_strings(&args[1..])?;
    Ok(Command::HDel { key, fields })
}

fn parse_hexists(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("HEXISTS"));
    }
    let key = extract_string(&args[0])?;
    let field = extract_string(&args[1])?;
    Ok(Command::HExists { key, field })
}

fn parse_hlen(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("HLEN"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HLen { key })
}

fn parse_hincrby(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("HINCRBY"));
    }
    let key = extract_string(&args[0])?;
    let field = extract_string(&args[1])?;
    let delta = parse_i64(&args[2], "HINCRBY")?;
    Ok(Command::HIncrBy { key, field, delta })
}

fn parse_hincrbyfloat(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("HINCRBYFLOAT"));
    }
    let key = extract_string(&args[0])?;
    let field = extract_string(&args[1])?;
    let delta = parse_f64(&args[2], "HINCRBYFLOAT")?;
    Ok(Command::HIncrByFloat { key, field, delta })
}

fn parse_hkeys(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("HKEYS"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HKeys { key })
}

fn parse_hvals(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("HVALS"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HVals { key })
}

fn parse_hmget(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("HMGET"));
    }
    let key = extract_string(&args[0])?;
    let fields = extract_strings(&args[1..])?;
    Ok(Command::HMGet { key, fields })
}

fn parse_hrandfield(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("HRANDFIELD"));
    }
    let key = extract_string(&args[0])?;
    let (count, with_values) = match args.len() {
        1 => (None, false),
        2 => (Some(parse_i64(&args[1], "HRANDFIELD")?), false),
        3 => {
            let count = parse_i64(&args[1], "HRANDFIELD")?;
            let flag = extract_string(&args[2])?.to_ascii_uppercase();
            if flag != "WITHVALUES" {
                return Err(ProtocolError::InvalidCommandFrame(
                    "HRANDFIELD: expected WITHVALUES".into(),
                ));
            }
            (Some(count), true)
        }
        _ => return Err(wrong_arity("HRANDFIELD")),
    };
    Ok(Command::HRandField {
        key,
        count,
        with_values,
    })
}

// --- set commands ---

fn parse_sadd(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("SADD"));
    }
    let key = extract_string(&args[0])?;
    let members = extract_strings(&args[1..])?;
    Ok(Command::SAdd { key, members })
}

fn parse_srem(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("SREM"));
    }
    let key = extract_string(&args[0])?;
    let members = extract_strings(&args[1..])?;
    Ok(Command::SRem { key, members })
}

fn parse_smembers(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("SMEMBERS"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::SMembers { key })
}

fn parse_sismember(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("SISMEMBER"));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::SIsMember { key, member })
}

fn parse_scard(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("SCARD"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::SCard { key })
}

fn parse_multi_key_set(cmd: &'static str, args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity(cmd));
    }
    let keys = extract_strings(args)?;
    match cmd {
        "SUNION" => Ok(Command::SUnion { keys }),
        "SINTER" => Ok(Command::SInter { keys }),
        "SDIFF" => Ok(Command::SDiff { keys }),
        _ => Err(wrong_arity(cmd)),
    }
}

fn parse_store_set(cmd: &'static str, args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity(cmd));
    }
    let dest = extract_string(&args[0])?;
    let keys = extract_strings(&args[1..])?;
    match cmd {
        "SUNIONSTORE" => Ok(Command::SUnionStore { dest, keys }),
        "SINTERSTORE" => Ok(Command::SInterStore { dest, keys }),
        "SDIFFSTORE" => Ok(Command::SDiffStore { dest, keys }),
        _ => Err(wrong_arity(cmd)),
    }
}

fn parse_srandmember(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() || args.len() > 2 {
        return Err(wrong_arity("SRANDMEMBER"));
    }
    let key = extract_string(&args[0])?;
    let count = if args.len() == 2 {
        let s = extract_string(&args[1])?;
        let n: i64 = s.parse().map_err(|_| {
            ProtocolError::InvalidCommandFrame("ERR value is not an integer or out of range".into())
        })?;
        Some(n)
    } else {
        None
    };
    Ok(Command::SRandMember { key, count })
}

fn parse_spop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() || args.len() > 2 {
        return Err(wrong_arity("SPOP"));
    }
    let key = extract_string(&args[0])?;
    let count = if args.len() == 2 {
        let s = extract_string(&args[1])?;
        let n: i64 = s.parse().map_err(|_| {
            ProtocolError::InvalidCommandFrame("ERR value is not an integer or out of range".into())
        })?;
        if n < 0 {
            return Err(ProtocolError::InvalidCommandFrame(
                "ERR value is not an integer or out of range".into(),
            ));
        }
        n as usize
    } else {
        1
    };
    Ok(Command::SPop { key, count })
}

fn parse_smismember(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("SMISMEMBER"));
    }
    let key = extract_string(&args[0])?;
    let members = extract_strings(&args[1..])?;
    Ok(Command::SMisMember { key, members })
}

// --- cluster commands ---

fn parse_cluster(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("CLUSTER"));
    }

    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let subcommand = uppercase_arg(&args[0], &mut kw)?;
    match subcommand {
        "INFO" => {
            if args.len() != 1 {
                return Err(wrong_arity("CLUSTER INFO"));
            }
            Ok(Command::ClusterInfo)
        }
        "NODES" => {
            if args.len() != 1 {
                return Err(wrong_arity("CLUSTER NODES"));
            }
            Ok(Command::ClusterNodes)
        }
        "SLOTS" => {
            if args.len() != 1 {
                return Err(wrong_arity("CLUSTER SLOTS"));
            }
            Ok(Command::ClusterSlots)
        }
        "KEYSLOT" => {
            if args.len() != 2 {
                return Err(wrong_arity("CLUSTER KEYSLOT"));
            }
            let key = extract_string(&args[1])?;
            Ok(Command::ClusterKeySlot { key })
        }
        "MYID" => {
            if args.len() != 1 {
                return Err(wrong_arity("CLUSTER MYID"));
            }
            Ok(Command::ClusterMyId)
        }
        "SETSLOT" => parse_cluster_setslot(&args[1..]),
        "MEET" => {
            if args.len() != 3 {
                return Err(wrong_arity("CLUSTER MEET"));
            }
            let ip = extract_string(&args[1])?;
            let p = parse_u64(&args[2], "CLUSTER MEET")?;
            let port = u16::try_from(p)
                .map_err(|_| ProtocolError::InvalidCommandFrame("invalid port number".into()))?;
            Ok(Command::ClusterMeet { ip, port })
        }
        "ADDSLOTS" => {
            if args.len() < 2 {
                return Err(wrong_arity("CLUSTER ADDSLOTS"));
            }
            let slots = parse_slot_list(&args[1..])?;
            Ok(Command::ClusterAddSlots { slots })
        }
        "ADDSLOTSRANGE" => {
            // arguments are pairs: start1 end1 [start2 end2 ...]
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                return Err(wrong_arity("CLUSTER ADDSLOTSRANGE"));
            }
            let mut ranges = Vec::new();
            for pair in args[1..].chunks(2) {
                let s = parse_u64(&pair[0], "CLUSTER ADDSLOTSRANGE")?;
                let start = u16::try_from(s)
                    .map_err(|_| ProtocolError::InvalidCommandFrame("invalid slot".into()))?;
                let e = parse_u64(&pair[1], "CLUSTER ADDSLOTSRANGE")?;
                let end = u16::try_from(e)
                    .map_err(|_| ProtocolError::InvalidCommandFrame("invalid slot".into()))?;
                if start > end || end >= 16384 {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "invalid slot range: start must be <= end and both must be 0-16383".into(),
                    ));
                }
                ranges.push((start, end));
            }
            Ok(Command::ClusterAddSlotsRange { ranges })
        }
        "DELSLOTS" => {
            if args.len() < 2 {
                return Err(wrong_arity("CLUSTER DELSLOTS"));
            }
            let slots = parse_slot_list(&args[1..])?;
            Ok(Command::ClusterDelSlots { slots })
        }
        "FORGET" => {
            if args.len() != 2 {
                return Err(wrong_arity("CLUSTER FORGET"));
            }
            let node_id = extract_string(&args[1])?;
            Ok(Command::ClusterForget { node_id })
        }
        "REPLICATE" => {
            if args.len() != 2 {
                return Err(wrong_arity("CLUSTER REPLICATE"));
            }
            let node_id = extract_string(&args[1])?;
            Ok(Command::ClusterReplicate { node_id })
        }
        "FAILOVER" => {
            let mut force = false;
            let mut takeover = false;
            for arg in &args[1..] {
                let mut kw2 = [0u8; MAX_KEYWORD_LEN];
                let opt = uppercase_arg(arg, &mut kw2)?;
                match opt {
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
                return Err(wrong_arity("CLUSTER COUNTKEYSINSLOT"));
            }
            let n = parse_u64(&args[1], "CLUSTER COUNTKEYSINSLOT")?;
            let slot = u16::try_from(n)
                .map_err(|_| ProtocolError::InvalidCommandFrame("invalid slot number".into()))?;
            Ok(Command::ClusterCountKeysInSlot { slot })
        }
        "GETKEYSINSLOT" => {
            if args.len() != 3 {
                return Err(wrong_arity("CLUSTER GETKEYSINSLOT"));
            }
            let n = parse_u64(&args[1], "CLUSTER GETKEYSINSLOT")?;
            let slot = u16::try_from(n)
                .map_err(|_| ProtocolError::InvalidCommandFrame("invalid slot number".into()))?;
            let c = parse_u64(&args[2], "CLUSTER GETKEYSINSLOT")?;
            let count = u32::try_from(c)
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
        return Err(wrong_arity("ASKING"));
    }
    Ok(Command::Asking)
}

fn parse_watch(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("WATCH"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Watch { keys })
}

fn parse_no_args(
    name: &'static str,
    args: &[Frame],
    cmd: Command,
) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity(name));
    }
    Ok(cmd)
}

fn parse_acl(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("ACL"));
    }

    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let subcmd = uppercase_arg(&args[0], &mut kw)?;
    match subcmd {
        "WHOAMI" => {
            if args.len() != 1 {
                return Err(wrong_arity("ACL|WHOAMI"));
            }
            Ok(Command::AclWhoAmI)
        }
        "LIST" => {
            if args.len() != 1 {
                return Err(wrong_arity("ACL|LIST"));
            }
            Ok(Command::AclList)
        }
        "USERS" => {
            if args.len() != 1 {
                return Err(wrong_arity("ACL|USERS"));
            }
            Ok(Command::AclUsers)
        }
        "GETUSER" => {
            if args.len() != 2 {
                return Err(wrong_arity("ACL|GETUSER"));
            }
            let username = extract_string(&args[1])?;
            Ok(Command::AclGetUser { username })
        }
        "DELUSER" => {
            if args.len() < 2 {
                return Err(wrong_arity("ACL|DELUSER"));
            }
            let usernames = extract_strings(&args[1..])?;
            Ok(Command::AclDelUser { usernames })
        }
        "SETUSER" => {
            if args.len() < 2 {
                return Err(wrong_arity("ACL|SETUSER"));
            }
            let username = extract_string(&args[1])?;
            let rules = if args.len() > 2 {
                extract_strings(&args[2..])?
            } else {
                Vec::new()
            };
            Ok(Command::AclSetUser { username, rules })
        }
        "CAT" => {
            if args.len() > 2 {
                return Err(wrong_arity("ACL|CAT"));
            }
            let category = if args.len() == 2 {
                Some(extract_string(&args[1])?)
            } else {
                None
            };
            Ok(Command::AclCat { category })
        }
        other => Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown ACL subcommand '{other}'"
        ))),
    }
}

fn parse_config(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("CONFIG"));
    }

    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let subcmd = uppercase_arg(&args[0], &mut kw)?;
    match subcmd {
        "GET" => {
            if args.len() != 2 {
                return Err(wrong_arity("CONFIG|GET"));
            }
            let pattern = extract_string(&args[1])?;
            Ok(Command::ConfigGet { pattern })
        }
        "SET" => {
            if args.len() != 3 {
                return Err(wrong_arity("CONFIG|SET"));
            }
            let param = extract_string(&args[1])?;
            let value = extract_string(&args[2])?;
            Ok(Command::ConfigSet { param, value })
        }
        "REWRITE" => {
            if args.len() != 1 {
                return Err(wrong_arity("CONFIG|REWRITE"));
            }
            Ok(Command::ConfigRewrite)
        }
        other => Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown CONFIG subcommand '{other}'"
        ))),
    }
}

fn parse_slowlog(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("SLOWLOG"));
    }

    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let subcmd = uppercase_arg(&args[0], &mut kw)?;
    match subcmd {
        "GET" => {
            let count = if args.len() > 1 {
                Some(parse_u64(&args[1], "SLOWLOG")? as usize)
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

fn parse_client(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("CLIENT"));
    }

    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let subcmd = uppercase_arg(&args[0], &mut kw)?;
    match subcmd {
        "ID" => Ok(Command::ClientId),
        "GETNAME" => Ok(Command::ClientGetName),
        "LIST" => Ok(Command::ClientList),
        "SETNAME" => {
            if args.len() < 2 {
                return Err(wrong_arity("CLIENT SETNAME"));
            }
            let name = extract_string(&args[1])?;
            Ok(Command::ClientSetName { name })
        }
        other => Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown CLIENT subcommand '{other}'"
        ))),
    }
}

fn parse_object(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("OBJECT"));
    }

    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let subcmd = uppercase_arg(&args[0], &mut kw)?;
    match subcmd {
        "ENCODING" => {
            if args.len() != 2 {
                return Err(wrong_arity("OBJECT|ENCODING"));
            }
            let key = extract_string(&args[1])?;
            Ok(Command::ObjectEncoding { key })
        }
        "REFCOUNT" => {
            if args.len() != 2 {
                return Err(wrong_arity("OBJECT|REFCOUNT"));
            }
            let key = extract_string(&args[1])?;
            Ok(Command::ObjectRefcount { key })
        }
        other => Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown OBJECT subcommand '{other}'"
        ))),
    }
}

fn parse_copy(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("COPY"));
    }
    let source = extract_string(&args[0])?;
    let destination = extract_string(&args[1])?;

    let mut replace = false;
    let mut i = 2;
    while i < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let arg = uppercase_arg(&args[i], &mut kw)?;
        match arg {
            "REPLACE" => replace = true,
            "DB" => {
                // consume and ignore the DB argument (single-db server)
                i += 1;
                if i >= args.len() {
                    return Err(wrong_arity("COPY"));
                }
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported COPY option '{arg}'"
                )));
            }
        }
        i += 1;
    }

    Ok(Command::Copy {
        source,
        destination,
        replace,
    })
}

fn parse_slot_list(args: &[Frame]) -> Result<Vec<u16>, ProtocolError> {
    let mut slots = Vec::with_capacity(args.len());
    for arg in args {
        let n = parse_u64(arg, "CLUSTER")?;
        let slot = u16::try_from(n)
            .map_err(|_| ProtocolError::InvalidCommandFrame("invalid slot number".into()))?;
        if slot >= 16384 {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "invalid slot {slot}: must be 0-16383"
            )));
        }
        slots.push(slot);
    }
    Ok(slots)
}

fn parse_cluster_setslot(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("CLUSTER SETSLOT"));
    }

    let n = parse_u64(&args[0], "CLUSTER SETSLOT")?;
    let slot = u16::try_from(n)
        .map_err(|_| ProtocolError::InvalidCommandFrame("invalid slot number".into()))?;
    if slot >= 16384 {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "invalid slot {slot}: must be 0-16383"
        )));
    }

    if args.len() < 2 {
        return Err(wrong_arity("CLUSTER SETSLOT"));
    }

    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let action = uppercase_arg(&args[1], &mut kw)?;
    match action {
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
                return Err(wrong_arity("CLUSTER SETSLOT NODE"));
            }
            let node_id = extract_string(&args[2])?;
            Ok(Command::ClusterSetSlotNode { slot, node_id })
        }
        "STABLE" => {
            if args.len() != 2 {
                return Err(wrong_arity("CLUSTER SETSLOT STABLE"));
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
        return Err(wrong_arity("MIGRATE"));
    }

    let host = extract_string(&args[0])?;
    let p = parse_u64(&args[1], "MIGRATE")?;
    let port = u16::try_from(p)
        .map_err(|_| ProtocolError::InvalidCommandFrame("invalid port number".into()))?;
    let key = extract_string(&args[2])?;
    let d = parse_u64(&args[3], "MIGRATE")?;
    let db = u32::try_from(d)
        .map_err(|_| ProtocolError::InvalidCommandFrame("invalid db number".into()))?;
    let timeout_ms = parse_u64(&args[4], "MIGRATE")?;

    let mut copy = false;
    let mut replace = false;

    for arg in &args[5..] {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let opt = uppercase_arg(arg, &mut kw)?;
        match opt {
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

fn parse_restore(args: &[Frame]) -> Result<Command, ProtocolError> {
    // RESTORE key ttl serialized-value [REPLACE]
    if args.len() < 3 {
        return Err(wrong_arity("RESTORE"));
    }

    let key = extract_string(&args[0])?;
    let ttl_ms = parse_u64(&args[1], "RESTORE")?;
    let data = extract_bytes(&args[2])?;

    let mut replace = false;
    for arg in &args[3..] {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let opt = uppercase_arg(arg, &mut kw)?;
        if opt == "REPLACE" {
            replace = true;
        } else {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "unknown RESTORE option '{opt}'"
            )));
        }
    }

    Ok(Command::Restore {
        key,
        ttl_ms,
        data,
        replace,
    })
}

fn parse_subscribe(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("SUBSCRIBE"));
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
        return Err(wrong_arity("PSUBSCRIBE"));
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
        return Err(wrong_arity("PUBLISH"));
    }
    let channel = extract_string(&args[0])?;
    let message = extract_bytes(&args[1])?;
    Ok(Command::Publish { channel, message })
}

fn parse_pubsub(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("PUBSUB"));
    }

    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let subcmd = uppercase_arg(&args[0], &mut kw)?;
    match subcmd {
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

/// Parses METRIC / QUANT / M / EF flags from a slice of command arguments.
///
/// Returns `(metric, quantization, connectivity, expansion_add)`.
/// `cmd` is used in error messages (e.g., "VADD" or "VADD_BATCH").
fn parse_vector_flags(
    args: &[Frame],
    cmd: &'static str,
) -> Result<(u8, u8, u32, u32), ProtocolError> {
    let mut metric: u8 = 0; // cosine default
    let mut quantization: u8 = 0; // f32 default
    let mut connectivity: u32 = 16;
    let mut expansion_add: u32 = 64;
    let mut idx = 0;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let flag = uppercase_arg(&args[idx], &mut kw)?;
        match flag {
            "METRIC" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: METRIC requires a value"
                    )));
                }
                let mut kw2 = [0u8; MAX_KEYWORD_LEN];
                let val = uppercase_arg(&args[idx], &mut kw2)?;
                metric = match val {
                    "COSINE" => 0,
                    "L2" => 1,
                    "IP" => 2,
                    _ => {
                        return Err(ProtocolError::InvalidCommandFrame(format!(
                            "{cmd}: unknown metric '{val}'"
                        )))
                    }
                };
                idx += 1;
            }
            "QUANT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: QUANT requires a value"
                    )));
                }
                let mut kw2 = [0u8; MAX_KEYWORD_LEN];
                let val = uppercase_arg(&args[idx], &mut kw2)?;
                quantization = match val {
                    "F32" => 0,
                    "F16" => 1,
                    "I8" | "Q8" => 2,
                    _ => {
                        return Err(ProtocolError::InvalidCommandFrame(format!(
                            "{cmd}: unknown quantization '{val}'"
                        )))
                    }
                };
                idx += 1;
            }
            "M" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: M requires a value"
                    )));
                }
                let m = parse_u64(&args[idx], cmd)?;
                if m > MAX_HNSW_PARAM {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: M value {m} exceeds max {MAX_HNSW_PARAM}"
                    )));
                }
                connectivity = m as u32;
                idx += 1;
            }
            "EF" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: EF requires a value"
                    )));
                }
                let ef = parse_u64(&args[idx], cmd)?;
                if ef > MAX_HNSW_PARAM {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "{cmd}: EF value {ef} exceeds max {MAX_HNSW_PARAM}"
                    )));
                }
                expansion_add = ef as u32;
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "{cmd}: unexpected argument '{flag}'"
                )));
            }
        }
    }

    Ok((metric, quantization, connectivity, expansion_add))
}

/// VADD key element f32 [f32 ...] [METRIC COSINE|L2|IP] [QUANT F32|F16|I8] [M n] [EF n]
fn parse_vadd(args: &[Frame]) -> Result<Command, ProtocolError> {
    // minimum: key + element + at least one float
    if args.len() < 3 {
        return Err(wrong_arity("VADD"));
    }

    let key = extract_string(&args[0])?;
    let element = extract_string(&args[1])?;

    // parse vector values until we hit a non-numeric argument, end, or dim limit
    let mut idx = 2;
    let mut vector = Vec::new();
    while idx < args.len() {
        if vector.len() >= MAX_VECTOR_DIMS {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "VADD: vector exceeds {MAX_VECTOR_DIMS} dimensions"
            )));
        }
        let s = extract_string(&args[idx])?;
        if let Ok(v) = s.parse::<f32>() {
            if v.is_nan() || v.is_infinite() {
                return Err(ProtocolError::InvalidCommandFrame(
                    "VADD: vector components must be finite (no NaN/infinity)".into(),
                ));
            }
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

    // parse optional flags
    let (metric, quantization, connectivity, expansion_add) =
        parse_vector_flags(&args[idx..], "VADD")?;

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

/// VADD_BATCH key DIM n [BINARY] element1 f32...|<blob> element2 f32...|<blob>
/// [METRIC COSINE|L2|IP] [QUANT F32|F16|I8] [M n] [EF n]
///
/// When BINARY is specified, each vector is a single bulk string of `dim * 4`
/// raw little-endian f32 bytes instead of `dim` separate text arguments.
/// This eliminates string serialization overhead on both client and server.
fn parse_vadd_batch(args: &[Frame]) -> Result<Command, ProtocolError> {
    // minimum: key + DIM + n (even an empty batch needs the DIM declaration)
    if args.len() < 3 {
        return Err(wrong_arity("VADD_BATCH"));
    }

    let key = extract_string(&args[0])?;

    // require DIM keyword
    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let dim_kw = uppercase_arg(&args[1], &mut kw)?;
    if dim_kw != "DIM" {
        return Err(ProtocolError::InvalidCommandFrame(
            "VADD_BATCH: expected DIM keyword".into(),
        ));
    }

    let dim = parse_u64(&args[2], "VADD_BATCH")? as usize;
    if dim == 0 {
        return Err(ProtocolError::InvalidCommandFrame(
            "VADD_BATCH: DIM must be at least 1".into(),
        ));
    }
    if dim > MAX_VECTOR_DIMS {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "VADD_BATCH: DIM {dim} exceeds max {MAX_VECTOR_DIMS}"
        )));
    }

    // check for optional BINARY flag after DIM
    let mut idx = 3;
    let binary_mode = if idx < args.len() {
        let mut kw2 = [0u8; MAX_KEYWORD_LEN];
        matches!(uppercase_arg(&args[idx], &mut kw2), Ok("BINARY"))
    } else {
        false
    };
    if binary_mode {
        idx += 1;
    }

    let mut entries: Vec<(String, Vec<f32>)> = Vec::new();

    if binary_mode {
        // binary mode: each entry is element_name + single blob of dim*4 bytes
        let blob_len = dim * 4;
        let entry_len = 2; // element name + blob

        while idx < args.len() {
            if idx + entry_len > args.len() {
                break;
            }

            // peek: if the second arg isn't exactly blob_len bytes, we've
            // hit the flags section (flags are short text strings)
            let blob_bytes = extract_bytes(&args[idx + 1])?;
            if blob_bytes.len() != blob_len {
                break;
            }

            let element = extract_string(&args[idx])?;
            idx += 1;

            // skip extract_bytes again — reuse what we already have
            idx += 1;

            // reinterpret raw LE bytes as f32 slice
            let vector = bytes_to_f32_vec(&blob_bytes, dim)?;

            entries.push((element, vector));

            if entries.len() >= MAX_VADD_BATCH_SIZE {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "VADD_BATCH: batch size exceeds max {MAX_VADD_BATCH_SIZE}"
                )));
            }
        }
    } else {
        // text mode: each entry is element_name followed by exactly `dim` floats.
        // we detect the end of entries by checking whether enough args remain
        // for a full entry (1 name + dim floats). this avoids misinterpreting
        // element names like "metric" as flags.
        let entry_len = 1 + dim; // element name + dim floats

        while idx < args.len() {
            // not enough remaining args for a full entry — must be flags
            if idx + entry_len > args.len() {
                break;
            }

            // peek: if the token after the element name isn't a valid float,
            // we've reached the flags section
            if dim > 0 {
                let peek = extract_string(&args[idx + 1])?;
                if peek.parse::<f32>().is_err() {
                    break;
                }
            }

            let element = extract_string(&args[idx])?;
            idx += 1;

            let mut vector = Vec::with_capacity(dim);
            for _ in 0..dim {
                let s = extract_string(&args[idx])?;
                let v = s.parse::<f32>().map_err(|_| {
                    ProtocolError::InvalidCommandFrame(format!(
                        "VADD_BATCH: expected float, got '{s}'"
                    ))
                })?;
                if v.is_nan() || v.is_infinite() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VADD_BATCH: vector components must be finite (no NaN/infinity)".into(),
                    ));
                }
                vector.push(v);
                idx += 1;
            }

            entries.push((element, vector));

            if entries.len() >= MAX_VADD_BATCH_SIZE {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "VADD_BATCH: batch size exceeds max {MAX_VADD_BATCH_SIZE}"
                )));
            }
        }
    }

    // parse optional flags (same logic as parse_vadd)
    let (metric, quantization, connectivity, expansion_add) =
        parse_vector_flags(&args[idx..], "VADD_BATCH")?;

    Ok(Command::VAddBatch {
        key,
        entries,
        dim,
        metric,
        quantization,
        connectivity,
        expansion_add,
    })
}

/// Converts a raw byte buffer of little-endian f32s into a Vec<f32>.
///
/// Validates that all values are finite (no NaN/infinity). The buffer
/// must be exactly `dim * 4` bytes.
fn bytes_to_f32_vec(data: &[u8], dim: usize) -> Result<Vec<f32>, ProtocolError> {
    // compile-time endianness check — binary protocol assumes little-endian
    #[cfg(not(target_endian = "little"))]
    compile_error!("VADD_BATCH BINARY mode requires a little-endian target");

    debug_assert_eq!(data.len(), dim * 4);

    let mut vector = Vec::with_capacity(dim);
    for chunk in data.chunks_exact(4) {
        let v = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        if !v.is_finite() {
            return Err(ProtocolError::InvalidCommandFrame(
                "VADD_BATCH BINARY: vector contains non-finite value (NaN/infinity)".into(),
            ));
        }
        vector.push(v);
    }
    Ok(vector)
}

/// VSIM key f32 [f32 ...] COUNT k [EF n] [WITHSCORES]
fn parse_vsim(args: &[Frame]) -> Result<Command, ProtocolError> {
    // minimum: key + at least one float + COUNT + k
    if args.len() < 4 {
        return Err(wrong_arity("VSIM"));
    }

    let key = extract_string(&args[0])?;

    // parse query vector until we hit a non-numeric argument, end, or dim limit
    let mut idx = 1;
    let mut query = Vec::new();
    while idx < args.len() {
        if query.len() >= MAX_VECTOR_DIMS {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "VSIM: query exceeds {MAX_VECTOR_DIMS} dimensions"
            )));
        }
        let s = extract_string(&args[idx])?;
        if let Ok(v) = s.parse::<f32>() {
            if v.is_nan() || v.is_infinite() {
                return Err(ProtocolError::InvalidCommandFrame(
                    "VSIM: query components must be finite (no NaN/infinity)".into(),
                ));
            }
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

    // COUNT k is required
    let mut count: Option<usize> = None;
    let mut ef_search: usize = 0;
    let mut with_scores = false;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let flag = uppercase_arg(&args[idx], &mut kw)?;
        match flag {
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VSIM: COUNT requires a value".into(),
                    ));
                }
                let c = parse_u64(&args[idx], "VSIM")?;
                if c > MAX_VSIM_COUNT {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "VSIM: COUNT {c} exceeds max {MAX_VSIM_COUNT}"
                    )));
                }
                count = Some(c as usize);
                idx += 1;
            }
            "EF" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "VSIM: EF requires a value".into(),
                    ));
                }
                let ef = parse_u64(&args[idx], "VSIM")?;
                if ef > MAX_VSIM_EF {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "VSIM: EF {ef} exceeds max {MAX_VSIM_EF}"
                    )));
                }
                ef_search = ef as usize;
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
        return Err(wrong_arity("VREM"));
    }
    let key = extract_string(&args[0])?;
    let element = extract_string(&args[1])?;
    Ok(Command::VRem { key, element })
}

/// VGET key element
fn parse_vget(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("VGET"));
    }
    let key = extract_string(&args[0])?;
    let element = extract_string(&args[1])?;
    Ok(Command::VGet { key, element })
}

/// VCARD key
fn parse_vcard(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("VCARD"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::VCard { key })
}

/// VDIM key
fn parse_vdim(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("VDIM"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::VDim { key })
}

/// VINFO key
fn parse_vinfo(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("VINFO"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::VInfo { key })
}

// --- proto command parsers ---

fn parse_proto_register(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("PROTO.REGISTER"));
    }
    let name = extract_string(&args[0])?;
    let descriptor = extract_bytes(&args[1])?;
    Ok(Command::ProtoRegister { name, descriptor })
}

fn parse_proto_set(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(wrong_arity("PROTO.SET"));
    }

    let key = extract_string(&args[0])?;
    let type_name = extract_string(&args[1])?;
    let data = extract_bytes(&args[2])?;
    let (expire, nx, xx) = parse_set_options(&args[3..], "PROTO.SET")?;

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
        return Err(wrong_arity("PROTO.GET"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::ProtoGet { key })
}

fn parse_proto_type(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("PROTO.TYPE"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::ProtoType { key })
}

fn parse_proto_schemas(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("PROTO.SCHEMAS"));
    }
    Ok(Command::ProtoSchemas)
}

fn parse_proto_describe(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("PROTO.DESCRIBE"));
    }
    let name = extract_string(&args[0])?;
    Ok(Command::ProtoDescribe { name })
}

fn parse_proto_getfield(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("PROTO.GETFIELD"));
    }
    let key = extract_string(&args[0])?;
    let field_path = extract_string(&args[1])?;
    Ok(Command::ProtoGetField { key, field_path })
}

fn parse_proto_setfield(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("PROTO.SETFIELD"));
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
        return Err(wrong_arity("PROTO.DELFIELD"));
    }
    let key = extract_string(&args[0])?;
    let field_path = extract_string(&args[1])?;
    Ok(Command::ProtoDelField { key, field_path })
}

fn parse_proto_scan(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("PROTO.SCAN"));
    }
    let cursor = parse_u64(&args[0], "PROTO.SCAN")?;
    let mut pattern = None;
    let mut count = None;
    let mut type_name = None;
    let mut idx = 1;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let flag = uppercase_arg(&args[idx], &mut kw)?;
        match flag {
            "MATCH" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.SCAN"));
                }
                pattern = Some(extract_string(&args[idx])?);
                idx += 1;
            }
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.SCAN"));
                }
                let n = parse_u64(&args[idx], "PROTO.SCAN")?;
                if n > MAX_SCAN_COUNT {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "PROTO.SCAN COUNT {n} exceeds max {MAX_SCAN_COUNT}"
                    )));
                }
                count = Some(n as usize);
                idx += 1;
            }
            "TYPE" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.SCAN"));
                }
                type_name = Some(extract_string(&args[idx])?);
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported PROTO.SCAN option '{flag}'"
                )));
            }
        }
    }

    Ok(Command::ProtoScan {
        cursor,
        pattern,
        count,
        type_name,
    })
}

fn parse_proto_find(args: &[Frame]) -> Result<Command, ProtocolError> {
    // minimum: cursor field_path value
    if args.len() < 3 {
        return Err(wrong_arity("PROTO.FIND"));
    }
    let cursor = parse_u64(&args[0], "PROTO.FIND")?;
    let field_path = extract_string(&args[1])?;
    let field_value = extract_string(&args[2])?;
    let mut pattern = None;
    let mut type_name = None;
    let mut count = None;
    let mut idx = 3;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let flag = uppercase_arg(&args[idx], &mut kw)?;
        match flag {
            "MATCH" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.FIND"));
                }
                pattern = Some(extract_string(&args[idx])?);
                idx += 1;
            }
            "TYPE" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.FIND"));
                }
                type_name = Some(extract_string(&args[idx])?);
                idx += 1;
            }
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.FIND"));
                }
                let n = parse_u64(&args[idx], "PROTO.FIND")?;
                if n > MAX_SCAN_COUNT {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "PROTO.FIND COUNT {n} exceeds max {MAX_SCAN_COUNT}"
                    )));
                }
                count = Some(n as usize);
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported PROTO.FIND option '{flag}'"
                )));
            }
        }
    }

    Ok(Command::ProtoFind {
        cursor,
        field_path,
        field_value,
        pattern,
        type_name,
        count,
    })
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
        _ => Err(wrong_arity("AUTH")),
    }
}

fn parse_quit(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("QUIT"));
    }
    Ok(Command::Quit)
}

fn parse_monitor(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("MONITOR"));
    }
    Ok(Command::Monitor)
}

fn parse_touch(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("TOUCH"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Touch { keys })
}

fn parse_sort(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("SORT"));
    }
    let key = extract_string(&args[0])?;
    let mut desc = false;
    let mut alpha = false;
    let mut limit = None;
    let mut store = None;
    let mut i = 1;
    while i < args.len() {
        let flag = extract_string(&args[i])?.to_uppercase();
        match flag.as_str() {
            "ASC" => {
                desc = false;
                i += 1;
            }
            "DESC" => {
                desc = true;
                i += 1;
            }
            "ALPHA" => {
                alpha = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "SORT LIMIT requires offset and count".into(),
                    ));
                }
                let offset = extract_string(&args[i + 1])?.parse::<i64>().map_err(|_| {
                    ProtocolError::InvalidCommandFrame(
                        "SORT LIMIT offset is not a valid integer".into(),
                    )
                })?;
                let count = extract_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                    ProtocolError::InvalidCommandFrame(
                        "SORT LIMIT count is not a valid integer".into(),
                    )
                })?;
                limit = Some((offset, count));
                i += 3;
            }
            "STORE" => {
                if i + 1 >= args.len() {
                    return Err(wrong_arity("SORT"));
                }
                store = Some(extract_string(&args[i + 1])?);
                i += 2;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "SORT: unsupported flag '{flag}'"
                )));
            }
        }
    }
    Ok(Command::Sort {
        key,
        desc,
        alpha,
        limit,
        store,
    })
}

// --- Redis 6.2+ commands ---

fn parse_lmove(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 4 {
        return Err(wrong_arity("LMOVE"));
    }
    let source = extract_string(&args[0])?;
    let destination = extract_string(&args[1])?;

    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let src_left = match uppercase_arg(&args[2], &mut kw)? {
        "LEFT" => true,
        "RIGHT" => false,
        other => {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "LMOVE: invalid wherefrom '{other}', expected LEFT or RIGHT"
            )));
        }
    };
    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let dst_left = match uppercase_arg(&args[3], &mut kw)? {
        "LEFT" => true,
        "RIGHT" => false,
        other => {
            return Err(ProtocolError::InvalidCommandFrame(format!(
                "LMOVE: invalid whereto '{other}', expected LEFT or RIGHT"
            )));
        }
    };

    Ok(Command::LMove {
        source,
        destination,
        src_left,
        dst_left,
    })
}

fn parse_getdel(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("GETDEL"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::GetDel { key })
}

fn parse_getex(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("GETEX"));
    }
    let key = extract_string(&args[0])?;
    let rest = &args[1..];

    let expire = if rest.is_empty() {
        // no options — TTL unchanged
        None
    } else {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        match uppercase_arg(&rest[0], &mut kw)? {
            "PERSIST" => Some(None),
            "EX" => {
                if rest.len() < 2 {
                    return Err(wrong_arity("GETEX"));
                }
                let n = parse_u64(&rest[1], "GETEX")?;
                if n == 0 {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "invalid expire time in 'GETEX' command".into(),
                    ));
                }
                Some(Some(SetExpire::Ex(n)))
            }
            "PX" => {
                if rest.len() < 2 {
                    return Err(wrong_arity("GETEX"));
                }
                let n = parse_u64(&rest[1], "GETEX")?;
                if n == 0 {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "invalid expire time in 'GETEX' command".into(),
                    ));
                }
                Some(Some(SetExpire::Px(n)))
            }
            "EXAT" => {
                if rest.len() < 2 {
                    return Err(wrong_arity("GETEX"));
                }
                let n = parse_u64(&rest[1], "GETEX")?;
                Some(Some(SetExpire::ExAt(n)))
            }
            "PXAT" => {
                if rest.len() < 2 {
                    return Err(wrong_arity("GETEX"));
                }
                let n = parse_u64(&rest[1], "GETEX")?;
                Some(Some(SetExpire::PxAt(n)))
            }
            other => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "GETEX: unsupported option '{other}'"
                )));
            }
        }
    };

    Ok(Command::GetEx { key, expire })
}

/// Parses ZDIFF/ZINTER/ZUNION: `numkeys key [key ...] [WITHSCORES]`.
fn parse_zset_multi(cmd: &'static str, args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity(cmd));
    }
    let numkeys = parse_u64(&args[0], cmd)? as usize;
    if numkeys == 0 {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "{cmd}: numkeys must be positive"
        )));
    }
    if args.len() < 1 + numkeys {
        return Err(wrong_arity(cmd));
    }
    let keys = extract_strings(&args[1..1 + numkeys])?;

    let mut with_scores = false;
    for frame in &args[1 + numkeys..] {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        if let Ok("WITHSCORES") = uppercase_arg(frame, &mut kw) {
            with_scores = true;
        }
    }

    match cmd {
        "ZDIFF" => Ok(Command::ZDiff { keys, with_scores }),
        "ZINTER" => Ok(Command::ZInter { keys, with_scores }),
        "ZUNION" => Ok(Command::ZUnion { keys, with_scores }),
        _ => Err(wrong_arity(cmd)),
    }
}

/// Parses ZDIFFSTORE/ZINTERSTORE/ZUNIONSTORE: `destkey numkeys key [key ...]`.
fn parse_zset_store(cmd: &'static str, args: &[Frame]) -> Result<Command, ProtocolError> {
    // need at least: dest numkeys key
    if args.len() < 3 {
        return Err(wrong_arity(cmd));
    }
    let dest = extract_string(&args[0])?;
    let numkeys = parse_u64(&args[1], cmd)? as usize;
    if numkeys == 0 {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "{cmd}: numkeys must be positive"
        )));
    }
    if args.len() < 2 + numkeys {
        return Err(wrong_arity(cmd));
    }
    let keys = extract_strings(&args[2..2 + numkeys])?;

    match cmd {
        "ZDIFFSTORE" => Ok(Command::ZDiffStore { dest, keys }),
        "ZINTERSTORE" => Ok(Command::ZInterStore { dest, keys }),
        "ZUNIONSTORE" => Ok(Command::ZUnionStore { dest, keys }),
        _ => Err(wrong_arity(cmd)),
    }
}

fn parse_zrandmember(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("ZRANDMEMBER"));
    }
    let key = extract_string(&args[0])?;
    let (count, with_scores) = match args.len() {
        1 => (None, false),
        2 => (Some(parse_i64(&args[1], "ZRANDMEMBER")?), false),
        3 => {
            let count = parse_i64(&args[1], "ZRANDMEMBER")?;
            let flag = extract_string(&args[2])?.to_ascii_uppercase();
            if flag != "WITHSCORES" {
                return Err(ProtocolError::InvalidCommandFrame(
                    "ZRANDMEMBER: expected WITHSCORES".into(),
                ));
            }
            (Some(count), true)
        }
        _ => return Err(wrong_arity("ZRANDMEMBER")),
    };
    Ok(Command::ZRandMember {
        key,
        count,
        with_scores,
    })
}

fn parse_wait(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("WAIT"));
    }
    let numreplicas_str = extract_string(&args[0])?;
    let timeout_ms_str = extract_string(&args[1])?;
    let numreplicas = numreplicas_str.parse::<u64>().map_err(|_| {
        ProtocolError::InvalidCommandFrame("WAIT numreplicas must be an integer".into())
    })?;
    let timeout_ms = timeout_ms_str.parse::<u64>().map_err(|_| {
        ProtocolError::InvalidCommandFrame("WAIT timeout must be an integer".into())
    })?;
    Ok(Command::Wait {
        numreplicas,
        timeout_ms,
    })
}

fn parse_command_cmd(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Ok(Command::Command {
            subcommand: None,
            args: vec![],
        });
    }
    let sub = extract_string(&args[0])?.to_ascii_uppercase();
    match sub.as_str() {
        "COUNT" => Ok(Command::Command {
            subcommand: Some("COUNT".into()),
            args: vec![],
        }),
        "INFO" => {
            let names = args[1..]
                .iter()
                .map(|f| extract_string(f).map(|s| s.to_ascii_uppercase()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::Command {
                subcommand: Some("INFO".into()),
                args: names,
            })
        }
        "DOCS" => {
            let names = args[1..]
                .iter()
                .map(|f| extract_string(f).map(|s| s.to_ascii_uppercase()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Command::Command {
                subcommand: Some("DOCS".into()),
                args: names,
            })
        }
        "GETKEYS" => Ok(Command::Command {
            subcommand: Some("GETKEYS".into()),
            args: vec![],
        }),
        "LIST" => Ok(Command::Command {
            subcommand: Some("LIST".into()),
            args: vec![],
        }),
        _ => Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown COMMAND subcommand: {sub}"
        ))),
    }
}
