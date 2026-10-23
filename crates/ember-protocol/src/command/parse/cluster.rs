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

pub(super) fn parse_asking(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("ASKING"));
    }
    Ok(Command::Asking)
}

pub(super) fn parse_slot_list(args: &[Frame]) -> Result<Vec<u16>, ProtocolError> {
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

pub(super) fn parse_cluster_setslot(args: &[Frame]) -> Result<Command, ProtocolError> {
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

pub(super) fn parse_migrate(args: &[Frame]) -> Result<Command, ProtocolError> {
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
