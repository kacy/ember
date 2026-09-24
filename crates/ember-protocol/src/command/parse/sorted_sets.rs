//! Parsers for sorted set commands.

use super::*;

pub(super) fn parse_zadd(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_zcard(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("ZCARD"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::ZCard { key })
}

pub(super) fn parse_zrem(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("ZREM"));
    }
    let key = extract_string(&args[0])?;
    let members = extract_strings(&args[1..])?;
    Ok(Command::ZRem { key, members })
}

pub(super) fn parse_zscore(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("ZSCORE"));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::ZScore { key, member })
}

pub(super) fn parse_zrank(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("ZRANK"));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::ZRank { key, member })
}

pub(super) fn parse_zrange(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_zrevrange(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_zrevrank(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("ZREVRANK"));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::ZRevRank { key, member })
}

pub(super) fn parse_zcount(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("ZCOUNT"));
    }
    let key = extract_string(&args[0])?;
    let min = parse_score_bound(&args[1], "ZCOUNT")?;
    let max = parse_score_bound(&args[2], "ZCOUNT")?;
    Ok(Command::ZCount { key, min, max })
}

pub(super) fn parse_zincrby(args: &[Frame]) -> Result<Command, ProtocolError> {
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
pub(super) fn parse_zrangebyscore(args: &[Frame]) -> Result<Command, ProtocolError> {
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
pub(super) fn parse_zrevrangebyscore(args: &[Frame]) -> Result<Command, ProtocolError> {
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
pub(super) fn parse_zpop_args(
    args: &[Frame],
    cmd: &'static str,
) -> Result<(String, usize), ProtocolError> {
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

pub(super) fn parse_zmpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(wrong_arity("ZMPOP"));
    }
    let (keys, rest) = parse_numkeys(args, "ZMPOP")?;
    let (min, count) = parse_mpop_tail(rest, "ZMPOP", "MIN", "MAX")?;
    Ok(Command::Zmpop { keys, min, count })
}

/// Parses ZDIFF/ZINTER/ZUNION: `numkeys key [key ...] [WITHSCORES]`.
pub(super) fn parse_zset_multi(
    cmd: &'static str,
    args: &[Frame],
) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity(cmd));
    }
    let (keys, rest) = parse_numkeys(args, cmd)?;

    let mut with_scores = false;
    for frame in rest {
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
pub(super) fn parse_zset_store(
    cmd: &'static str,
    args: &[Frame],
) -> Result<Command, ProtocolError> {
    // need at least: dest numkeys key
    if args.len() < 3 {
        return Err(wrong_arity(cmd));
    }
    let dest = extract_string(&args[0])?;
    let (keys, _) = parse_numkeys(&args[1..], cmd)?;

    match cmd {
        "ZDIFFSTORE" => Ok(Command::ZDiffStore { dest, keys }),
        "ZINTERSTORE" => Ok(Command::ZInterStore { dest, keys }),
        "ZUNIONSTORE" => Ok(Command::ZUnionStore { dest, keys }),
        _ => Err(wrong_arity(cmd)),
    }
}

pub(super) fn parse_zrandmember(args: &[Frame]) -> Result<Command, ProtocolError> {
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
