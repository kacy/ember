//! Parsers for string and bitmap commands.

use super::*;

pub(super) fn parse_get(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("GET"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Get { key })
}

/// Parses NX / XX / EX / PX options from a slice of command arguments.
///
/// Returns `(expire, nx, xx)`. `cmd` is used in error messages.
pub(super) fn parse_set_options(
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

pub(super) fn parse_set(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_incr(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("INCR"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Incr { key })
}

pub(super) fn parse_decr(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("DECR"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Decr { key })
}

pub(super) fn parse_incrby(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("INCRBY"));
    }
    let key = extract_string(&args[0])?;
    let delta = parse_i64(&args[1], "INCRBY")?;
    Ok(Command::IncrBy { key, delta })
}

pub(super) fn parse_decrby(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("DECRBY"));
    }
    let key = extract_string(&args[0])?;
    let delta = parse_i64(&args[1], "DECRBY")?;
    Ok(Command::DecrBy { key, delta })
}

pub(super) fn parse_incrbyfloat(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_append(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("APPEND"));
    }
    let key = extract_string(&args[0])?;
    let value = extract_bytes(&args[1])?;
    Ok(Command::Append { key, value })
}

pub(super) fn parse_strlen(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("STRLEN"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Strlen { key })
}

/// SETNX key value — set key only if it does not exist.
/// Equivalent to `SET key value NX`.
pub(super) fn parse_setnx(args: &[Frame]) -> Result<Command, ProtocolError> {
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
pub(super) fn parse_setex(args: &[Frame]) -> Result<Command, ProtocolError> {
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
pub(super) fn parse_psetex(args: &[Frame]) -> Result<Command, ProtocolError> {
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
pub(super) fn parse_getrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("GETRANGE"));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "GETRANGE")?;
    let end = parse_i64(&args[2], "GETRANGE")?;
    Ok(Command::GetRange { key, start, end })
}

/// SETRANGE key offset value.
pub(super) fn parse_setrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("SETRANGE"));
    }
    let key = extract_string(&args[0])?;
    let offset = parse_usize(&args[1], "SETRANGE")?;
    let value = extract_bytes(&args[2])?;
    Ok(Command::SetRange { key, offset, value })
}

/// GETBIT key offset.
pub(super) fn parse_getbit(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("GETBIT"));
    }
    let key = extract_string(&args[0])?;
    let offset = parse_u64(&args[1], "GETBIT")?;
    Ok(Command::GetBit { key, offset })
}

/// SETBIT key offset value.
pub(super) fn parse_setbit(args: &[Frame]) -> Result<Command, ProtocolError> {
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
pub(super) fn parse_bit_range(
    args: &[Frame],
    cmd: &str,
) -> Result<Option<BitRange>, ProtocolError> {
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
pub(super) fn parse_bitcount(args: &[Frame]) -> Result<Command, ProtocolError> {
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
pub(super) fn parse_bitpos(args: &[Frame]) -> Result<Command, ProtocolError> {
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
pub(super) fn parse_bitop(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_mget(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("MGET"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::MGet { keys })
}

pub(super) fn parse_mset(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_msetnx(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_getset(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("GETSET"));
    }
    let key = extract_string(&args[0])?;
    let value = extract_bytes(&args[1])?;
    Ok(Command::GetSet { key, value })
}

pub(super) fn parse_getdel(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("GETDEL"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::GetDel { key })
}

pub(super) fn parse_getex(args: &[Frame]) -> Result<Command, ProtocolError> {
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
