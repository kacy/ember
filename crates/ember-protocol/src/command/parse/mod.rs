//! Command parsing from RESP3 frames.
//!
//! This module holds `Command::from_frame()`, which picks a parser by the
//! command name, and the helpers for pulling strings and numbers out of
//! frames. The parsers themselves live in one submodule per command family.

use bytes::Bytes;

use crate::error::ProtocolError;
use crate::types::Frame;

use super::{BitOpKind, BitRange, BitRangeUnit, Command, ScoreBound, SetExpire, ZAddFlags};

mod cluster;
mod hashes;
mod keys;
mod lists;
mod proto;
mod pubsub;
mod server;
mod sets;
mod sorted_sets;
mod strings;
mod vector;

use cluster::*;
use hashes::*;
use keys::*;
use lists::*;
use proto::*;
use pubsub::*;
use server::*;
use sets::*;
use sorted_sets::*;
use strings::*;
use vector::*;

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

/// Parses a `numkeys` argument and the key names that follow it.
///
/// `args` starts at the `numkeys` frame. Returns the keys and the remaining
/// arguments. Zero is rejected, and so is any count larger than the number of
/// arguments left, so an oversized `numkeys` cannot overflow the index math.
fn parse_numkeys<'a>(
    args: &'a [Frame],
    cmd: &'static str,
) -> Result<(Vec<String>, &'a [Frame]), ProtocolError> {
    let (numkeys, rest) = args.split_first().ok_or_else(|| wrong_arity(cmd))?;
    let numkeys = parse_u64(numkeys, cmd)?;
    if numkeys == 0 || numkeys > rest.len() as u64 {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "numkeys must be positive and match the number of keys for '{cmd}'"
        )));
    }
    let (keys, rest) = rest.split_at(numkeys as usize);
    Ok((extract_strings(keys)?, rest))
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

/// Extracts the timeout argument for BLPOP/BRPOP. Redis accepts integer or
/// float seconds; negative values are an error.
fn parse_timeout(frame: &Frame, cmd: &str) -> Result<f64, ProtocolError> {
    let val = parse_f64(frame, cmd)?;
    if val < 0.0 {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "timeout is negative for '{cmd}'"
        )));
    }
    Ok(val)
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

/// Parses a Redis score bound string.
///
/// Supports `-inf`, `+inf`, `inf`, exclusive `(value`, and plain inclusive values.
fn parse_score_bound(frame: &Frame, cmd: &str) -> Result<ScoreBound, ProtocolError> {
    let bytes = extract_raw_bytes(frame)?;
    let s = std::str::from_utf8(bytes).map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!("invalid score bound for '{cmd}'"))
    })?;

    // NaN has no place in a score range, so reject it with the parse errors
    let parse = |s: &str| {
        s.parse::<f64>()
            .ok()
            .filter(|v| !v.is_nan())
            .ok_or_else(|| {
                ProtocolError::InvalidCommandFrame(format!("min or max is not a float for '{cmd}'"))
            })
    };

    match s {
        "-inf" => Ok(ScoreBound::NegInf),
        "+inf" | "inf" => Ok(ScoreBound::PosInf),
        _ => match s.strip_prefix('(') {
            Some(rest) => parse(rest).map(ScoreBound::Exclusive),
            None => parse(s).map(ScoreBound::Inclusive),
        },
    }
}

// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT n]
// ZMPOP numkeys key [key ...] MIN|MAX [COUNT n]
