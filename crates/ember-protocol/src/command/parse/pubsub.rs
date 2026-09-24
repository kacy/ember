//! Parsers for pub/sub commands.

use super::*;

pub(super) fn parse_subscribe(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("SUBSCRIBE"));
    }
    let channels: Vec<String> = args.iter().map(extract_string).collect::<Result<_, _>>()?;
    Ok(Command::Subscribe { channels })
}

pub(super) fn parse_unsubscribe(args: &[Frame]) -> Result<Command, ProtocolError> {
    let channels: Vec<String> = args.iter().map(extract_string).collect::<Result<_, _>>()?;
    Ok(Command::Unsubscribe { channels })
}

pub(super) fn parse_psubscribe(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("PSUBSCRIBE"));
    }
    let patterns: Vec<String> = args.iter().map(extract_string).collect::<Result<_, _>>()?;
    Ok(Command::PSubscribe { patterns })
}

pub(super) fn parse_punsubscribe(args: &[Frame]) -> Result<Command, ProtocolError> {
    let patterns: Vec<String> = args.iter().map(extract_string).collect::<Result<_, _>>()?;
    Ok(Command::PUnsubscribe { patterns })
}

pub(super) fn parse_publish(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(wrong_arity("PUBLISH"));
    }
    let channel = extract_string(&args[0])?;
    let message = extract_bytes(&args[1])?;
    Ok(Command::Publish { channel, message })
}

pub(super) fn parse_pubsub(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(wrong_arity("PUBSUB"));
    }

    let mut kw = [0u8; MAX_KEYWORD_LEN];
    let subcmd = uppercase_arg(&args[0], &mut kw)?;
    match subcmd {
        "CHANNELS" => {
            let pattern = if args.len() > 1 {
                Some(extract_string(&args[1])?)
            } else {
                None
            };
            Ok(Command::PubSubChannels { pattern })
        }
        "NUMSUB" => {
            let channels: Vec<String> = args[1..]
                .iter()
                .map(extract_string)
                .collect::<Result<_, _>>()?;
            Ok(Command::PubSubNumSub { channels })
        }
        "NUMPAT" => Ok(Command::PubSubNumPat),
        other => Err(ProtocolError::InvalidCommandFrame(format!(
            "unknown PUBSUB subcommand '{other}'"
        ))),
    }
}
