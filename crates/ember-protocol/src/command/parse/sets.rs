//! Parsers for set commands.

use super::*;

pub(super) fn parse_smove(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_sintercard(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("SINTERCARD"));
    }
    let (keys, rest) = parse_numkeys(args, "SINTERCARD")?;
    let limit = match rest {
        [] => 0,
        [tag, n] => {
            if !extract_string(tag)?.eq_ignore_ascii_case("LIMIT") {
                return Err(ProtocolError::InvalidCommandFrame(
                    "SINTERCARD: expected LIMIT keyword".into(),
                ));
            }
            parse_usize(n, "SINTERCARD")?
        }
        _ => return Err(wrong_arity("SINTERCARD")),
    };
    Ok(Command::SInterCard { keys, limit })
}

pub(super) fn parse_sadd(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("SADD"));
    }
    let key = extract_string(&args[0])?;
    let members = extract_strings(&args[1..])?;
    Ok(Command::SAdd { key, members })
}

pub(super) fn parse_srem(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("SREM"));
    }
    let key = extract_string(&args[0])?;
    let members = extract_strings(&args[1..])?;
    Ok(Command::SRem { key, members })
}

pub(super) fn parse_smembers(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("SMEMBERS"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::SMembers { key })
}

pub(super) fn parse_sismember(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("SISMEMBER"));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::SIsMember { key, member })
}

pub(super) fn parse_scard(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("SCARD"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::SCard { key })
}

pub(super) fn parse_multi_key_set(
    cmd: &'static str,
    args: &[Frame],
) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_store_set(cmd: &'static str, args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_srandmember(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() || args.len() > 2 {
        return Err(wrong_arity("SRANDMEMBER"));
    }
    let key = extract_string(&args[0])?;
    let count = match args.get(1) {
        Some(frame) => Some(parse_i64(frame, "SRANDMEMBER")?),
        None => None,
    };
    Ok(Command::SRandMember { key, count })
}

pub(super) fn parse_spop(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_smismember(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("SMISMEMBER"));
    }
    let key = extract_string(&args[0])?;
    let members = extract_strings(&args[1..])?;
    Ok(Command::SMisMember { key, members })
}
