//! Parsers for protobuf commands.

use super::*;

pub(super) fn parse_proto_register(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("PROTO.REGISTER"));
    }
    let name = extract_string(&args[0])?;
    let descriptor = extract_bytes(&args[1])?;
    Ok(Command::ProtoRegister { name, descriptor })
}

pub(super) fn parse_proto_set(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 {
        return Err(wrong_arity("PROTO.SET"));
    }

    let key = extract_string(&args[0])?;
    let type_name = extract_string(&args[1])?;
    let data = extract_bytes(&args[2])?;
    let (expire, nx, xx) = parse_set_options(&args[3..], "PROTO.SET")?;

    Ok(Command::ProtoSet {
        key,
        type_name,
        data,
        expire,
        nx,
        xx,
    })
}

pub(super) fn parse_proto_get(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("PROTO.GET"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::ProtoGet { key })
}

pub(super) fn parse_proto_type(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("PROTO.TYPE"));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::ProtoType { key })
}

pub(super) fn parse_proto_schemas(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(wrong_arity("PROTO.SCHEMAS"));
    }
    Ok(Command::ProtoSchemas)
}

pub(super) fn parse_proto_describe(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(wrong_arity("PROTO.DESCRIBE"));
    }
    let name = extract_string(&args[0])?;
    Ok(Command::ProtoDescribe { name })
}

pub(super) fn parse_proto_getfield(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("PROTO.GETFIELD"));
    }
    let key = extract_string(&args[0])?;
    let field_path = extract_string(&args[1])?;
    Ok(Command::ProtoGetField { key, field_path })
}

pub(super) fn parse_proto_setfield(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(wrong_arity("PROTO.SETFIELD"));
    }
    let key = extract_string(&args[0])?;
    let field_path = extract_string(&args[1])?;
    let value = extract_string(&args[2])?;
    Ok(Command::ProtoSetField {
        key,
        field_path,
        value,
    })
}

pub(super) fn parse_proto_delfield(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("PROTO.DELFIELD"));
    }
    let key = extract_string(&args[0])?;
    let field_path = extract_string(&args[1])?;
    Ok(Command::ProtoDelField { key, field_path })
}

pub(super) fn parse_proto_scan(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("PROTO.SCAN"));
    }
    let cursor = parse_u64(&args[0], "PROTO.SCAN")?;
    let mut pattern = None;
    let mut count = None;
    let mut type_name = None;
    let mut idx = 1;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let flag = uppercase_arg(&args[idx], &mut kw)?;
        match flag {
            "MATCH" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.SCAN"));
                }
                pattern = Some(extract_string(&args[idx])?);
                idx += 1;
            }
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.SCAN"));
                }
                let n = parse_u64(&args[idx], "PROTO.SCAN")?;
                if n > MAX_SCAN_COUNT {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "PROTO.SCAN COUNT {n} exceeds max {MAX_SCAN_COUNT}"
                    )));
                }
                count = Some(n as usize);
                idx += 1;
            }
            "TYPE" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.SCAN"));
                }
                type_name = Some(extract_string(&args[idx])?);
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported PROTO.SCAN option '{flag}'"
                )));
            }
        }
    }

    Ok(Command::ProtoScan {
        cursor,
        pattern,
        count,
        type_name,
    })
}

pub(super) fn parse_proto_find(args: &[Frame]) -> Result<Command, ProtocolError> {
    // minimum: cursor field_path value
    if args.len() < 3 {
        return Err(wrong_arity("PROTO.FIND"));
    }
    let cursor = parse_u64(&args[0], "PROTO.FIND")?;
    let field_path = extract_string(&args[1])?;
    let field_value = extract_string(&args[2])?;
    let mut pattern = None;
    let mut type_name = None;
    let mut count = None;
    let mut idx = 3;

    while idx < args.len() {
        let mut kw = [0u8; MAX_KEYWORD_LEN];
        let flag = uppercase_arg(&args[idx], &mut kw)?;
        match flag {
            "MATCH" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.FIND"));
                }
                pattern = Some(extract_string(&args[idx])?);
                idx += 1;
            }
            "TYPE" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.FIND"));
                }
                type_name = Some(extract_string(&args[idx])?);
                idx += 1;
            }
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(wrong_arity("PROTO.FIND"));
                }
                let n = parse_u64(&args[idx], "PROTO.FIND")?;
                if n > MAX_SCAN_COUNT {
                    return Err(ProtocolError::InvalidCommandFrame(format!(
                        "PROTO.FIND COUNT {n} exceeds max {MAX_SCAN_COUNT}"
                    )));
                }
                count = Some(n as usize);
                idx += 1;
            }
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported PROTO.FIND option '{flag}'"
                )));
            }
        }
    }

    Ok(Command::ProtoFind {
        cursor,
        field_path,
        field_value,
        pattern,
        type_name,
        count,
    })
}
