//! Parsers for cluster commands.

use super::*;

pub(super) fn parse_cluster(args: &[Frame]) -> Result<Command, ProtocolError> {
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
            // the ranges expand to one entry per slot, so more slots than
            // exist means a range repeats
            let mut total = 0usize;
            for pair in args[1..].chunks(2) {
                let start = parse_slot(&pair[0])?;
                let end = parse_slot(&pair[1])?;
                if start > end {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "invalid slot range: start must be <= end".into(),
                    ));
                }
                total += usize::from(end - start) + 1;
                if total > usize::from(SLOT_COUNT) {
                    return Err(ProtocolError::InvalidCommandFrame(
                        "slot ranges overlap".into(),
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
            let slot = parse_slot(&args[1])?;
            Ok(Command::ClusterCountKeysInSlot { slot })
        }
        "GETKEYSINSLOT" => {
            if args.len() != 3 {
                return Err(wrong_arity("CLUSTER GETKEYSINSLOT"));
            }
            let slot = parse_slot(&args[1])?;
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

pub(super) fn parse_asking(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("ASKING"));
    }
    Ok(Command::Asking)
}

const SLOT_COUNT: u16 = 16384;

/// Parses a hash slot number, 0 to 16383.
fn parse_slot(arg: &Frame) -> Result<u16, ProtocolError> {
    let n = parse_u64(arg, "CLUSTER")?;
    u16::try_from(n)
        .ok()
        .filter(|&slot| slot < SLOT_COUNT)
        .ok_or_else(|| {
            ProtocolError::InvalidCommandFrame(format!("invalid slot {n}: must be 0-16383"))
        })
}

pub(super) fn parse_slot_list(args: &[Frame]) -> Result<Vec<u16>, ProtocolError> {
    args.iter().map(parse_slot).collect()
}

pub(super) fn parse_cluster_setslot(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("CLUSTER SETSLOT"));
    }

    let slot = parse_slot(&args[0])?;

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

pub(super) fn parse_migrate(args: &[Frame]) -> Result<Command, ProtocolError> {
    // MIGRATE host port key db timeout [COPY] [REPLACE]
    //         [AUTH password | AUTH2 username password]
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
    let mut auth = None;

    let mut rest = args[5..].iter();
    while let Some(arg) = rest.next() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let opt = uppercase_arg(arg, &mut kw)?;
        let mut next = || {
            rest.next()
                .ok_or_else(|| wrong_arity("MIGRATE"))
                .and_then(extract_string)
        };
        match opt {
            "COPY" => copy = true,
            "REPLACE" => replace = true,
            "AUTH" => {
                auth = Some(MigrateAuth {
                    username: None,
                    password: next()?,
                })
            }
            "AUTH2" => {
                auth = Some(MigrateAuth {
                    username: Some(next()?),
                    password: next()?,
                })
            }
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
        auth,
    })
}
