//! Parsers for server, connection and ACL commands.

use super::*;

pub(super) fn parse_ping(args: &[Frame]) -> Result<Command, ProtocolError> {
    match args.len() {
        0 => Ok(Command::Ping(None)),
        1 => {
            let msg = extract_bytes(&args[0])?;
            Ok(Command::Ping(Some(msg)))
        }
        _ => Err(wrong_arity("PING")),
    }
}

pub(super) fn parse_echo(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("ECHO"));
    }
    let msg = extract_bytes(&args[0])?;
    Ok(Command::Echo(msg))
}

pub(super) fn parse_dbsize(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("DBSIZE"));
    }
    Ok(Command::DbSize)
}

pub(super) fn parse_info(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_bgsave(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("BGSAVE"));
    }
    Ok(Command::BgSave)
}

pub(super) fn parse_bgrewriteaof(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("BGREWRITEAOF"));
    }
    Ok(Command::BgRewriteAof)
}

pub(super) fn parse_flushdb(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_flushall(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_memory_cmd(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_watch(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("WATCH"));
    }
    let keys = extract_strings(args)?;
    Ok(Command::Watch { keys })
}

pub(super) fn parse_no_args(
    name: &'static str,
    args: &[Frame],
    cmd: Command,
) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity(name));
    }
    Ok(cmd)
}

pub(super) fn parse_acl(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_config(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_slowlog(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_client(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_auth(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_quit(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("QUIT"));
    }
    Ok(Command::Quit)
}

pub(super) fn parse_monitor(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("MONITOR"));
    }
    Ok(Command::Monitor)
}

pub(super) fn parse_wait(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_command_cmd(args: &[Frame]) -> Result<Command, ProtocolError> {
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
