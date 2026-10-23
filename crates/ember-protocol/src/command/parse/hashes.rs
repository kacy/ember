//! Parsers for hash commands.

use super::*;

pub(super) fn parse_hset(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_hget(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("HGET"));
    }
    let key = extract_string(&args[0])?;
    let field = extract_string(&args[1])?;
    Ok(Command::HGet { key, field })
}

pub(super) fn parse_hgetall(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("HGETALL"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HGetAll { key })
}

pub(super) fn parse_hdel(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("HDEL"));
    }
    let key = extract_string(&args[0])?;
    let fields = extract_strings(&args[1..])?;
    Ok(Command::HDel { key, fields })
}

pub(super) fn parse_hexists(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("HEXISTS"));
    }
    let key = extract_string(&args[0])?;
    let field = extract_string(&args[1])?;
    Ok(Command::HExists { key, field })
}

pub(super) fn parse_hlen(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("HLEN"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HLen { key })
}

pub(super) fn parse_hincrby(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("HINCRBY"));
    }
    let key = extract_string(&args[0])?;
    let field = extract_string(&args[1])?;
    let delta = parse_i64(&args[2], "HINCRBY")?;
    Ok(Command::HIncrBy { key, field, delta })
}

pub(super) fn parse_hincrbyfloat(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("HINCRBYFLOAT"));
    }
    let key = extract_string(&args[0])?;
    let field = extract_string(&args[1])?;
    let delta = parse_f64(&args[2], "HINCRBYFLOAT")?;
    Ok(Command::HIncrByFloat { key, field, delta })
}

pub(super) fn parse_hkeys(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("HKEYS"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HKeys { key })
}

pub(super) fn parse_hvals(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("HVALS"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::HVals { key })
}

pub(super) fn parse_hmget(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(wrong_arity("HMGET"));
    }
    let key = extract_string(&args[0])?;
    let fields = extract_strings(&args[1..])?;
    Ok(Command::HMGet { key, fields })
}

pub(super) fn parse_hrandfield(args: &[Frame]) -> Result<Command, ProtocolError> {
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
