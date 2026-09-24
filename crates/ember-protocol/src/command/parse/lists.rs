//! Parsers for list commands.

use super::*;

pub(super) fn parse_lpush(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("LPUSH"));
    }
    let key = extract_string(&args[0])?;
    let values = extract_bytes_vec(&args[1..])?;
    Ok(Command::LPush { key, values })
}

pub(super) fn parse_rpush(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("RPUSH"));
    }
    let key = extract_string(&args[0])?;
    let values = extract_bytes_vec(&args[1..])?;
    Ok(Command::RPush { key, values })
}

pub(super) fn parse_lpop(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_rpop(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_lrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("LRANGE"));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "LRANGE")?;
    let stop = parse_i64(&args[2], "LRANGE")?;
    Ok(Command::LRange { key, start, stop })
}

pub(super) fn parse_llen(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("LLEN"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::LLen { key })
}

/// Parses BLPOP/BRPOP: all args except the last are keys, the last is the
/// timeout in seconds (float). At least one key is required.
pub(super) fn parse_blpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("BLPOP"));
    }
    let timeout_secs = parse_timeout(&args[args.len() - 1], "BLPOP")?;
    let keys = extract_strings(&args[..args.len() - 1])?;
    Ok(Command::BLPop { keys, timeout_secs })
}

pub(super) fn parse_brpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("BRPOP"));
    }
    let timeout_secs = parse_timeout(&args[args.len() - 1], "BRPOP")?;
    let keys = extract_strings(&args[..args.len() - 1])?;
    Ok(Command::BRPop { keys, timeout_secs })
}

pub(super) fn parse_lindex(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("LINDEX"));
    }
    let key = extract_string(&args[0])?;
    let index = parse_i64(&args[1], "LINDEX")?;
    Ok(Command::LIndex { key, index })
}

pub(super) fn parse_lset(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("LSET"));
    }
    let key = extract_string(&args[0])?;
    let index = parse_i64(&args[1], "LSET")?;
    let value = extract_bytes(&args[2])?;
    Ok(Command::LSet { key, index, value })
}

pub(super) fn parse_ltrim(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("LTRIM"));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "LTRIM")?;
    let stop = parse_i64(&args[2], "LTRIM")?;
    Ok(Command::LTrim { key, start, stop })
}

pub(super) fn parse_linsert(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_lrem(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("LREM"));
    }
    let key = extract_string(&args[0])?;
    let count = parse_i64(&args[1], "LREM")?;
    let value = extract_bytes(&args[2])?;
    Ok(Command::LRem { key, count, value })
}

pub(super) fn parse_lpos(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_lmpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(wrong_arity("LMPOP"));
    }
    let (keys, rest) = parse_numkeys(args, "LMPOP")?;
    let (left, count) = parse_mpop_tail(rest, "LMPOP", "LEFT", "RIGHT")?;
    Ok(Command::Lmpop { keys, left, count })
}

/// Parses the `<side> [COUNT n]` tail shared by LMPOP and ZMPOP.
///
/// Returns `true` when the side is `first` (LEFT or MIN), and the count,
/// which defaults to 1.
pub(super) fn parse_mpop_tail(
    rest: &[Frame],
    cmd: &'static str,
    first: &str,
    second: &str,
) -> Result<(bool, usize), ProtocolError> {
    let (side, count) = match rest {
        [side] => (side, 1),
        [side, tag, n] => {
            if !extract_string(tag)?.eq_ignore_ascii_case("COUNT") {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "{cmd}: expected COUNT"
                )));
            }
            let n = parse_usize(n, cmd)?;
            if n == 0 {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "{cmd}: COUNT must be positive"
                )));
            }
            (side, n)
        }
        _ => return Err(wrong_arity(cmd)),
    };
    let side = extract_string(side)?;
    if side.eq_ignore_ascii_case(first) {
        Ok((true, count))
    } else if side.eq_ignore_ascii_case(second) {
        Ok((false, count))
    } else {
        Err(ProtocolError::InvalidCommandFrame(format!(
            "{cmd}: expected {first} or {second}"
        )))
    }
}

pub(super) fn parse_lmove(args: &[Frame]) -> Result<Command, ProtocolError> {
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
