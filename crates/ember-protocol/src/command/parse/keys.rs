//! Parsers for key and expiry commands.

use super::*;

pub(super) fn parse_keys(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("KEYS"));
    }
    let pattern = extract_string(&args[0])?;
    Ok(Command::Keys { pattern })
}

pub(super) fn parse_rename(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("RENAME"));
    }
    let key = extract_string(&args[0])?;
    let newkey = extract_string(&args[1])?;
    Ok(Command::Rename { key, newkey })
}

pub(super) fn parse_del(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("DEL"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Del { keys })
}

pub(super) fn parse_exists(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("EXISTS"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Exists { keys })
}

pub(super) fn parse_expire(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_ttl(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("TTL"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Ttl { key })
}

pub(super) fn parse_persist(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("PERSIST"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Persist { key })
}

pub(super) fn parse_pttl(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("PTTL"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Pttl { key })
}

pub(super) fn parse_pexpire(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_expireat(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("EXPIREAT"));
    }
    let key = extract_string(&args[0])?;
    let timestamp = parse_u64(&args[1], "EXPIREAT")?;
    Ok(Command::Expireat { key, timestamp })
}

pub(super) fn parse_pexpireat_cmd(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("PEXPIREAT"));
    }
    let key = extract_string(&args[0])?;
    let timestamp_ms = parse_u64(&args[1], "PEXPIREAT")?;
    Ok(Command::Pexpireat { key, timestamp_ms })
}

pub(super) fn parse_expiretime(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("EXPIRETIME"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Expiretime { key })
}

pub(super) fn parse_pexpiretime(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("PEXPIRETIME"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Pexpiretime { key })
}

pub(super) fn parse_unlink(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("UNLINK"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Unlink { keys })
}

pub(super) fn parse_type(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("TYPE"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Type { key })
}

pub(super) fn parse_object(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_copy(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_restore(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_touch(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("TOUCH"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Touch { keys })
}

pub(super) fn parse_sort(args: &[Frame]) -> Result<Command, ProtocolError> {
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
