//! Command parsing from RESP3 frames.
//!
//! Converts a parsed [`Frame`] (expected to be an array) into a typed
//! [`Command`] enum. This keeps protocol-level concerns separate from
//! the engine that actually executes commands.

use bytes::Bytes;

use crate::error::ProtocolError;
use crate::types::Frame;

/// Expiration option for the SET command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetExpire {
    /// EX seconds — expire after N seconds.
    Ex(u64),
    /// PX milliseconds — expire after N milliseconds.
    Px(u64),
}

/// A parsed client command, ready for execution.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// PING with an optional message. Returns PONG or echoes the message.
    Ping(Option<Bytes>),

    /// ECHO <message>. Returns the message back to the client.
    Echo(Bytes),

    /// GET <key>. Returns the value or nil.
    Get { key: String },

    /// SET <key> <value> [EX seconds | PX milliseconds].
    Set {
        key: String,
        value: Bytes,
        expire: Option<SetExpire>,
    },

    /// DEL <key> [key ...]. Returns the number of keys removed.
    Del { keys: Vec<String> },

    /// EXISTS <key> [key ...]. Returns the number of keys that exist.
    Exists { keys: Vec<String> },

    /// EXPIRE <key> <seconds>. Sets a TTL on an existing key.
    Expire { key: String, seconds: u64 },

    /// TTL <key>. Returns remaining time-to-live in seconds.
    Ttl { key: String },

    /// DBSIZE. Returns the number of keys in the database.
    DbSize,

    /// INFO [section]. Returns server info. Currently only supports "keyspace".
    Info { section: Option<String> },

    /// BGSAVE. Triggers a background snapshot.
    BgSave,

    /// BGREWRITEAOF. Triggers an AOF rewrite (snapshot + truncate).
    BgRewriteAof,

    /// LPUSH <key> <value> [value ...]. Pushes values to the head of a list.
    LPush { key: String, values: Vec<Bytes> },

    /// RPUSH <key> <value> [value ...]. Pushes values to the tail of a list.
    RPush { key: String, values: Vec<Bytes> },

    /// LPOP <key>. Pops a value from the head of a list.
    LPop { key: String },

    /// RPOP <key>. Pops a value from the tail of a list.
    RPop { key: String },

    /// LRANGE <key> <start> <stop>. Returns a range of elements by index.
    LRange { key: String, start: i64, stop: i64 },

    /// LLEN <key>. Returns the length of a list.
    LLen { key: String },

    /// TYPE <key>. Returns the type of the value stored at key.
    Type { key: String },

    /// ZADD <key> [NX|XX] [GT|LT] [CH] <score> <member> [score member ...].
    ZAdd {
        key: String,
        flags: ZAddFlags,
        members: Vec<(f64, String)>,
    },

    /// ZREM <key> <member> [member ...]. Removes members from a sorted set.
    ZRem { key: String, members: Vec<String> },

    /// ZSCORE <key> <member>. Returns the score of a member.
    ZScore { key: String, member: String },

    /// ZRANK <key> <member>. Returns the rank of a member (0-based).
    ZRank { key: String, member: String },

    /// ZRANGE <key> <start> <stop> [WITHSCORES]. Returns a range by rank.
    ZRange {
        key: String,
        start: i64,
        stop: i64,
        with_scores: bool,
    },

    /// A command we don't recognize (yet).
    Unknown(String),
}

/// Flags for the ZADD command.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ZAddFlags {
    /// Only add new members, don't update existing scores.
    pub nx: bool,
    /// Only update existing members, don't add new ones.
    pub xx: bool,
    /// Only update when new score > current score.
    pub gt: bool,
    /// Only update when new score < current score.
    pub lt: bool,
    /// Return count of changed members (added + updated) instead of just added.
    pub ch: bool,
}

impl Eq for ZAddFlags {}

impl Command {
    /// Parses a [`Frame`] into a [`Command`].
    ///
    /// Expects an array frame where the first element is the command name
    /// (as a bulk or simple string) and the rest are arguments.
    pub fn from_frame(frame: Frame) -> Result<Command, ProtocolError> {
        let frames = match frame {
            Frame::Array(frames) => frames,
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(
                    "expected array frame".into(),
                ));
            }
        };

        if frames.is_empty() {
            return Err(ProtocolError::InvalidCommandFrame(
                "empty command array".into(),
            ));
        }

        let name = extract_string(&frames[0])?;
        let name_upper = name.to_ascii_uppercase();

        match name_upper.as_str() {
            "PING" => parse_ping(&frames[1..]),
            "ECHO" => parse_echo(&frames[1..]),
            "GET" => parse_get(&frames[1..]),
            "SET" => parse_set(&frames[1..]),
            "DEL" => parse_del(&frames[1..]),
            "EXISTS" => parse_exists(&frames[1..]),
            "EXPIRE" => parse_expire(&frames[1..]),
            "TTL" => parse_ttl(&frames[1..]),
            "DBSIZE" => parse_dbsize(&frames[1..]),
            "INFO" => parse_info(&frames[1..]),
            "BGSAVE" => parse_bgsave(&frames[1..]),
            "BGREWRITEAOF" => parse_bgrewriteaof(&frames[1..]),
            "LPUSH" => parse_lpush(&frames[1..]),
            "RPUSH" => parse_rpush(&frames[1..]),
            "LPOP" => parse_lpop(&frames[1..]),
            "RPOP" => parse_rpop(&frames[1..]),
            "LRANGE" => parse_lrange(&frames[1..]),
            "LLEN" => parse_llen(&frames[1..]),
            "TYPE" => parse_type(&frames[1..]),
            "ZADD" => parse_zadd(&frames[1..]),
            "ZREM" => parse_zrem(&frames[1..]),
            "ZSCORE" => parse_zscore(&frames[1..]),
            "ZRANK" => parse_zrank(&frames[1..]),
            "ZRANGE" => parse_zrange(&frames[1..]),
            _ => Ok(Command::Unknown(name)),
        }
    }
}

/// Extracts a UTF-8 string from a Bulk or Simple frame.
fn extract_string(frame: &Frame) -> Result<String, ProtocolError> {
    match frame {
        Frame::Bulk(data) => String::from_utf8(data.to_vec()).map_err(|_| {
            ProtocolError::InvalidCommandFrame("command name is not valid utf-8".into())
        }),
        Frame::Simple(s) => Ok(s.clone()),
        _ => Err(ProtocolError::InvalidCommandFrame(
            "expected bulk or simple string for command name".into(),
        )),
    }
}

/// Extracts raw bytes from a Bulk or Simple frame.
fn extract_bytes(frame: &Frame) -> Result<Bytes, ProtocolError> {
    match frame {
        Frame::Bulk(data) => Ok(data.clone()),
        Frame::Simple(s) => Ok(Bytes::from(s.clone().into_bytes())),
        _ => Err(ProtocolError::InvalidCommandFrame(
            "expected bulk or simple string argument".into(),
        )),
    }
}

/// Parses a string argument as a positive u64.
fn parse_u64(frame: &Frame, cmd: &str) -> Result<u64, ProtocolError> {
    let s = extract_string(frame)?;
    s.parse::<u64>().map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!("value is not a valid integer for '{cmd}'"))
    })
}

fn parse_ping(args: &[Frame]) -> Result<Command, ProtocolError> {
    match args.len() {
        0 => Ok(Command::Ping(None)),
        1 => {
            let msg = extract_bytes(&args[0])?;
            Ok(Command::Ping(Some(msg)))
        }
        _ => Err(ProtocolError::WrongArity("PING".into())),
    }
}

fn parse_echo(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("ECHO".into()));
    }
    let msg = extract_bytes(&args[0])?;
    Ok(Command::Echo(msg))
}

fn parse_get(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("GET".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Get { key })
}

fn parse_set(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("SET".into()));
    }

    let key = extract_string(&args[0])?;
    let value = extract_bytes(&args[1])?;

    let expire = if args.len() > 2 {
        // parse optional EX/PX
        if args.len() != 4 {
            return Err(ProtocolError::WrongArity("SET".into()));
        }
        let flag = extract_string(&args[2])?.to_ascii_uppercase();
        let amount = parse_u64(&args[3], "SET")?;

        if amount == 0 {
            return Err(ProtocolError::InvalidCommandFrame(
                "invalid expire time in 'SET' command".into(),
            ));
        }

        match flag.as_str() {
            "EX" => Some(SetExpire::Ex(amount)),
            "PX" => Some(SetExpire::Px(amount)),
            _ => {
                return Err(ProtocolError::InvalidCommandFrame(format!(
                    "unsupported SET option '{flag}'"
                )));
            }
        }
    } else {
        None
    };

    Ok(Command::Set { key, value, expire })
}

fn parse_del(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("DEL".into()));
    }
    let keys = args
        .iter()
        .map(extract_string)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Command::Del { keys })
}

fn parse_exists(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.is_empty() {
        return Err(ProtocolError::WrongArity("EXISTS".into()));
    }
    let keys = args
        .iter()
        .map(extract_string)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Command::Exists { keys })
}

fn parse_expire(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("EXPIRE".into()));
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

fn parse_ttl(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("TTL".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Ttl { key })
}

fn parse_dbsize(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(ProtocolError::WrongArity("DBSIZE".into()));
    }
    Ok(Command::DbSize)
}

fn parse_info(args: &[Frame]) -> Result<Command, ProtocolError> {
    match args.len() {
        0 => Ok(Command::Info { section: None }),
        1 => {
            let section = extract_string(&args[0])?;
            Ok(Command::Info {
                section: Some(section),
            })
        }
        _ => Err(ProtocolError::WrongArity("INFO".into())),
    }
}

fn parse_bgsave(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(ProtocolError::WrongArity("BGSAVE".into()));
    }
    Ok(Command::BgSave)
}

fn parse_bgrewriteaof(args: &[Frame]) -> Result<Command, ProtocolError> {
    if !args.is_empty() {
        return Err(ProtocolError::WrongArity("BGREWRITEAOF".into()));
    }
    Ok(Command::BgRewriteAof)
}

/// Parses a string argument as an i64. Used by LRANGE for start/stop indices.
fn parse_i64(frame: &Frame, cmd: &str) -> Result<i64, ProtocolError> {
    let s = extract_string(frame)?;
    s.parse::<i64>().map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!("value is not a valid integer for '{cmd}'"))
    })
}

fn parse_lpush(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("LPUSH".into()));
    }
    let key = extract_string(&args[0])?;
    let values = args[1..]
        .iter()
        .map(extract_bytes)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Command::LPush { key, values })
}

fn parse_rpush(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("RPUSH".into()));
    }
    let key = extract_string(&args[0])?;
    let values = args[1..]
        .iter()
        .map(extract_bytes)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Command::RPush { key, values })
}

fn parse_lpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("LPOP".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::LPop { key })
}

fn parse_rpop(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("RPOP".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::RPop { key })
}

fn parse_lrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 3 {
        return Err(ProtocolError::WrongArity("LRANGE".into()));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "LRANGE")?;
    let stop = parse_i64(&args[2], "LRANGE")?;
    Ok(Command::LRange { key, start, stop })
}

fn parse_llen(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("LLEN".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::LLen { key })
}

fn parse_type(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 1 {
        return Err(ProtocolError::WrongArity("TYPE".into()));
    }
    let key = extract_string(&args[0])?;
    Ok(Command::Type { key })
}

/// Parses a string argument as an f64 score.
fn parse_f64(frame: &Frame, cmd: &str) -> Result<f64, ProtocolError> {
    let s = extract_string(frame)?;
    let v = s.parse::<f64>().map_err(|_| {
        ProtocolError::InvalidCommandFrame(format!("value is not a valid float for '{cmd}'"))
    })?;
    if v.is_nan() {
        return Err(ProtocolError::InvalidCommandFrame(format!(
            "NaN is not a valid score for '{cmd}'"
        )));
    }
    Ok(v)
}

fn parse_zadd(args: &[Frame]) -> Result<Command, ProtocolError> {
    // ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
    if args.len() < 3 {
        return Err(ProtocolError::WrongArity("ZADD".into()));
    }

    let key = extract_string(&args[0])?;
    let mut flags = ZAddFlags::default();
    let mut idx = 1;

    // parse optional flags before score/member pairs
    while idx < args.len() {
        let s = extract_string(&args[idx])?.to_ascii_uppercase();
        match s.as_str() {
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
        return Err(ProtocolError::WrongArity("ZADD".into()));
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

fn parse_zrem(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 2 {
        return Err(ProtocolError::WrongArity("ZREM".into()));
    }
    let key = extract_string(&args[0])?;
    let members = args[1..]
        .iter()
        .map(extract_string)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Command::ZRem { key, members })
}

fn parse_zscore(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("ZSCORE".into()));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::ZScore { key, member })
}

fn parse_zrank(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() != 2 {
        return Err(ProtocolError::WrongArity("ZRANK".into()));
    }
    let key = extract_string(&args[0])?;
    let member = extract_string(&args[1])?;
    Ok(Command::ZRank { key, member })
}

fn parse_zrange(args: &[Frame]) -> Result<Command, ProtocolError> {
    if args.len() < 3 || args.len() > 4 {
        return Err(ProtocolError::WrongArity("ZRANGE".into()));
    }
    let key = extract_string(&args[0])?;
    let start = parse_i64(&args[1], "ZRANGE")?;
    let stop = parse_i64(&args[2], "ZRANGE")?;

    let with_scores = if args.len() == 4 {
        let opt = extract_string(&args[3])?.to_ascii_uppercase();
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build an array frame from bulk strings.
    fn cmd(parts: &[&str]) -> Frame {
        Frame::Array(
            parts
                .iter()
                .map(|s| Frame::Bulk(Bytes::from(s.to_string())))
                .collect(),
        )
    }

    // --- ping ---

    #[test]
    fn ping_no_args() {
        assert_eq!(
            Command::from_frame(cmd(&["PING"])).unwrap(),
            Command::Ping(None),
        );
    }

    #[test]
    fn ping_with_message() {
        assert_eq!(
            Command::from_frame(cmd(&["PING", "hello"])).unwrap(),
            Command::Ping(Some(Bytes::from("hello"))),
        );
    }

    #[test]
    fn ping_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["ping"])).unwrap(),
            Command::Ping(None),
        );
        assert_eq!(
            Command::from_frame(cmd(&["Ping"])).unwrap(),
            Command::Ping(None),
        );
    }

    #[test]
    fn ping_too_many_args() {
        let err = Command::from_frame(cmd(&["PING", "a", "b"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- echo ---

    #[test]
    fn echo() {
        assert_eq!(
            Command::from_frame(cmd(&["ECHO", "test"])).unwrap(),
            Command::Echo(Bytes::from("test")),
        );
    }

    #[test]
    fn echo_missing_arg() {
        let err = Command::from_frame(cmd(&["ECHO"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- get ---

    #[test]
    fn get_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["GET", "mykey"])).unwrap(),
            Command::Get {
                key: "mykey".into()
            },
        );
    }

    #[test]
    fn get_no_args() {
        let err = Command::from_frame(cmd(&["GET"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn get_too_many_args() {
        let err = Command::from_frame(cmd(&["GET", "a", "b"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn get_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["get", "k"])).unwrap(),
            Command::Get { key: "k".into() },
        );
    }

    // --- set ---

    #[test]
    fn set_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["SET", "key", "value"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: Bytes::from("value"),
                expire: None,
            },
        );
    }

    #[test]
    fn set_with_ex() {
        assert_eq!(
            Command::from_frame(cmd(&["SET", "key", "val", "EX", "10"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: Bytes::from("val"),
                expire: Some(SetExpire::Ex(10)),
            },
        );
    }

    #[test]
    fn set_with_px() {
        assert_eq!(
            Command::from_frame(cmd(&["SET", "key", "val", "PX", "5000"])).unwrap(),
            Command::Set {
                key: "key".into(),
                value: Bytes::from("val"),
                expire: Some(SetExpire::Px(5000)),
            },
        );
    }

    #[test]
    fn set_ex_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["set", "k", "v", "ex", "5"])).unwrap(),
            Command::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire: Some(SetExpire::Ex(5)),
            },
        );
    }

    #[test]
    fn set_missing_value() {
        let err = Command::from_frame(cmd(&["SET", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn set_invalid_expire_value() {
        let err = Command::from_frame(cmd(&["SET", "k", "v", "EX", "notanum"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn set_zero_expire() {
        let err = Command::from_frame(cmd(&["SET", "k", "v", "EX", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn set_unknown_flag() {
        let err = Command::from_frame(cmd(&["SET", "k", "v", "ZZ", "10"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn set_incomplete_expire() {
        // EX without a value
        let err = Command::from_frame(cmd(&["SET", "k", "v", "EX"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- del ---

    #[test]
    fn del_single() {
        assert_eq!(
            Command::from_frame(cmd(&["DEL", "key"])).unwrap(),
            Command::Del {
                keys: vec!["key".into()]
            },
        );
    }

    #[test]
    fn del_multiple() {
        assert_eq!(
            Command::from_frame(cmd(&["DEL", "a", "b", "c"])).unwrap(),
            Command::Del {
                keys: vec!["a".into(), "b".into(), "c".into()]
            },
        );
    }

    #[test]
    fn del_no_args() {
        let err = Command::from_frame(cmd(&["DEL"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- exists ---

    #[test]
    fn exists_single() {
        assert_eq!(
            Command::from_frame(cmd(&["EXISTS", "key"])).unwrap(),
            Command::Exists {
                keys: vec!["key".into()]
            },
        );
    }

    #[test]
    fn exists_multiple() {
        assert_eq!(
            Command::from_frame(cmd(&["EXISTS", "a", "b"])).unwrap(),
            Command::Exists {
                keys: vec!["a".into(), "b".into()]
            },
        );
    }

    #[test]
    fn exists_no_args() {
        let err = Command::from_frame(cmd(&["EXISTS"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- expire ---

    #[test]
    fn expire_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["EXPIRE", "key", "60"])).unwrap(),
            Command::Expire {
                key: "key".into(),
                seconds: 60,
            },
        );
    }

    #[test]
    fn expire_wrong_arity() {
        let err = Command::from_frame(cmd(&["EXPIRE", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn expire_invalid_seconds() {
        let err = Command::from_frame(cmd(&["EXPIRE", "key", "abc"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn expire_zero_seconds() {
        let err = Command::from_frame(cmd(&["EXPIRE", "key", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    // --- ttl ---

    #[test]
    fn ttl_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["TTL", "key"])).unwrap(),
            Command::Ttl { key: "key".into() },
        );
    }

    #[test]
    fn ttl_wrong_arity() {
        let err = Command::from_frame(cmd(&["TTL"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- dbsize ---

    #[test]
    fn dbsize_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["DBSIZE"])).unwrap(),
            Command::DbSize,
        );
    }

    #[test]
    fn dbsize_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["dbsize"])).unwrap(),
            Command::DbSize,
        );
    }

    #[test]
    fn dbsize_extra_args() {
        let err = Command::from_frame(cmd(&["DBSIZE", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- info ---

    #[test]
    fn info_no_section() {
        assert_eq!(
            Command::from_frame(cmd(&["INFO"])).unwrap(),
            Command::Info { section: None },
        );
    }

    #[test]
    fn info_with_section() {
        assert_eq!(
            Command::from_frame(cmd(&["INFO", "keyspace"])).unwrap(),
            Command::Info {
                section: Some("keyspace".into())
            },
        );
    }

    #[test]
    fn info_too_many_args() {
        let err = Command::from_frame(cmd(&["INFO", "a", "b"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- bgsave ---

    #[test]
    fn bgsave_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["BGSAVE"])).unwrap(),
            Command::BgSave,
        );
    }

    #[test]
    fn bgsave_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["bgsave"])).unwrap(),
            Command::BgSave,
        );
    }

    #[test]
    fn bgsave_extra_args() {
        let err = Command::from_frame(cmd(&["BGSAVE", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- bgrewriteaof ---

    #[test]
    fn bgrewriteaof_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["BGREWRITEAOF"])).unwrap(),
            Command::BgRewriteAof,
        );
    }

    #[test]
    fn bgrewriteaof_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["bgrewriteaof"])).unwrap(),
            Command::BgRewriteAof,
        );
    }

    #[test]
    fn bgrewriteaof_extra_args() {
        let err = Command::from_frame(cmd(&["BGREWRITEAOF", "extra"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- lpush ---

    #[test]
    fn lpush_single() {
        assert_eq!(
            Command::from_frame(cmd(&["LPUSH", "list", "val"])).unwrap(),
            Command::LPush {
                key: "list".into(),
                values: vec![Bytes::from("val")],
            },
        );
    }

    #[test]
    fn lpush_multiple() {
        assert_eq!(
            Command::from_frame(cmd(&["LPUSH", "list", "a", "b", "c"])).unwrap(),
            Command::LPush {
                key: "list".into(),
                values: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            },
        );
    }

    #[test]
    fn lpush_no_value() {
        let err = Command::from_frame(cmd(&["LPUSH", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn lpush_case_insensitive() {
        assert!(matches!(
            Command::from_frame(cmd(&["lpush", "k", "v"])).unwrap(),
            Command::LPush { .. }
        ));
    }

    // --- rpush ---

    #[test]
    fn rpush_single() {
        assert_eq!(
            Command::from_frame(cmd(&["RPUSH", "list", "val"])).unwrap(),
            Command::RPush {
                key: "list".into(),
                values: vec![Bytes::from("val")],
            },
        );
    }

    #[test]
    fn rpush_no_value() {
        let err = Command::from_frame(cmd(&["RPUSH", "key"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- lpop ---

    #[test]
    fn lpop_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["LPOP", "list"])).unwrap(),
            Command::LPop { key: "list".into() },
        );
    }

    #[test]
    fn lpop_wrong_arity() {
        let err = Command::from_frame(cmd(&["LPOP"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- rpop ---

    #[test]
    fn rpop_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["RPOP", "list"])).unwrap(),
            Command::RPop { key: "list".into() },
        );
    }

    // --- lrange ---

    #[test]
    fn lrange_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["LRANGE", "list", "0", "-1"])).unwrap(),
            Command::LRange {
                key: "list".into(),
                start: 0,
                stop: -1,
            },
        );
    }

    #[test]
    fn lrange_wrong_arity() {
        let err = Command::from_frame(cmd(&["LRANGE", "list", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn lrange_invalid_index() {
        let err = Command::from_frame(cmd(&["LRANGE", "list", "abc", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    // --- llen ---

    #[test]
    fn llen_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["LLEN", "list"])).unwrap(),
            Command::LLen { key: "list".into() },
        );
    }

    #[test]
    fn llen_wrong_arity() {
        let err = Command::from_frame(cmd(&["LLEN"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- type ---

    #[test]
    fn type_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["TYPE", "key"])).unwrap(),
            Command::Type { key: "key".into() },
        );
    }

    #[test]
    fn type_wrong_arity() {
        let err = Command::from_frame(cmd(&["TYPE"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn type_case_insensitive() {
        assert!(matches!(
            Command::from_frame(cmd(&["type", "k"])).unwrap(),
            Command::Type { .. }
        ));
    }

    // --- general ---

    #[test]
    fn unknown_command() {
        assert_eq!(
            Command::from_frame(cmd(&["FOOBAR", "arg"])).unwrap(),
            Command::Unknown("FOOBAR".into()),
        );
    }

    #[test]
    fn non_array_frame() {
        let err = Command::from_frame(Frame::Simple("PING".into())).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn empty_array() {
        let err = Command::from_frame(Frame::Array(vec![])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    // --- zadd ---

    #[test]
    fn zadd_basic() {
        let parsed = Command::from_frame(cmd(&["ZADD", "board", "100", "alice"])).unwrap();
        match parsed {
            Command::ZAdd {
                key,
                flags,
                members,
            } => {
                assert_eq!(key, "board");
                assert_eq!(flags, ZAddFlags::default());
                assert_eq!(members, vec![(100.0, "alice".into())]);
            }
            other => panic!("expected ZAdd, got {other:?}"),
        }
    }

    #[test]
    fn zadd_multiple_members() {
        let parsed =
            Command::from_frame(cmd(&["ZADD", "board", "100", "alice", "200", "bob"])).unwrap();
        match parsed {
            Command::ZAdd { members, .. } => {
                assert_eq!(members.len(), 2);
                assert_eq!(members[0], (100.0, "alice".into()));
                assert_eq!(members[1], (200.0, "bob".into()));
            }
            other => panic!("expected ZAdd, got {other:?}"),
        }
    }

    #[test]
    fn zadd_with_flags() {
        let parsed = Command::from_frame(cmd(&["ZADD", "z", "NX", "CH", "100", "alice"])).unwrap();
        match parsed {
            Command::ZAdd { flags, .. } => {
                assert!(flags.nx);
                assert!(flags.ch);
                assert!(!flags.xx);
                assert!(!flags.gt);
                assert!(!flags.lt);
            }
            other => panic!("expected ZAdd, got {other:?}"),
        }
    }

    #[test]
    fn zadd_gt_flag() {
        let parsed = Command::from_frame(cmd(&["zadd", "z", "gt", "100", "alice"])).unwrap();
        match parsed {
            Command::ZAdd { flags, .. } => assert!(flags.gt),
            other => panic!("expected ZAdd, got {other:?}"),
        }
    }

    #[test]
    fn zadd_nx_xx_conflict() {
        let err = Command::from_frame(cmd(&["ZADD", "z", "NX", "XX", "100", "alice"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn zadd_gt_lt_conflict() {
        let err = Command::from_frame(cmd(&["ZADD", "z", "GT", "LT", "100", "alice"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn zadd_wrong_arity() {
        let err = Command::from_frame(cmd(&["ZADD", "z"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn zadd_odd_score_member_count() {
        // one score without a member
        let err = Command::from_frame(cmd(&["ZADD", "z", "100"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    #[test]
    fn zadd_invalid_score() {
        let err = Command::from_frame(cmd(&["ZADD", "z", "notanum", "alice"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn zadd_nan_score() {
        let err = Command::from_frame(cmd(&["ZADD", "z", "nan", "alice"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn zadd_negative_score() {
        let parsed = Command::from_frame(cmd(&["ZADD", "z", "-50.5", "alice"])).unwrap();
        match parsed {
            Command::ZAdd { members, .. } => {
                assert_eq!(members[0].0, -50.5);
            }
            other => panic!("expected ZAdd, got {other:?}"),
        }
    }

    // --- zrem ---

    #[test]
    fn zrem_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["ZREM", "z", "alice"])).unwrap(),
            Command::ZRem {
                key: "z".into(),
                members: vec!["alice".into()],
            },
        );
    }

    #[test]
    fn zrem_multiple() {
        let parsed = Command::from_frame(cmd(&["ZREM", "z", "a", "b", "c"])).unwrap();
        match parsed {
            Command::ZRem { members, .. } => assert_eq!(members.len(), 3),
            other => panic!("expected ZRem, got {other:?}"),
        }
    }

    #[test]
    fn zrem_wrong_arity() {
        let err = Command::from_frame(cmd(&["ZREM", "z"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- zscore ---

    #[test]
    fn zscore_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["ZSCORE", "z", "alice"])).unwrap(),
            Command::ZScore {
                key: "z".into(),
                member: "alice".into(),
            },
        );
    }

    #[test]
    fn zscore_wrong_arity() {
        let err = Command::from_frame(cmd(&["ZSCORE", "z"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- zrank ---

    #[test]
    fn zrank_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["ZRANK", "z", "alice"])).unwrap(),
            Command::ZRank {
                key: "z".into(),
                member: "alice".into(),
            },
        );
    }

    #[test]
    fn zrank_wrong_arity() {
        let err = Command::from_frame(cmd(&["ZRANK", "z"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }

    // --- zrange ---

    #[test]
    fn zrange_basic() {
        assert_eq!(
            Command::from_frame(cmd(&["ZRANGE", "z", "0", "-1"])).unwrap(),
            Command::ZRange {
                key: "z".into(),
                start: 0,
                stop: -1,
                with_scores: false,
            },
        );
    }

    #[test]
    fn zrange_with_scores() {
        assert_eq!(
            Command::from_frame(cmd(&["ZRANGE", "z", "0", "-1", "WITHSCORES"])).unwrap(),
            Command::ZRange {
                key: "z".into(),
                start: 0,
                stop: -1,
                with_scores: true,
            },
        );
    }

    #[test]
    fn zrange_withscores_case_insensitive() {
        assert_eq!(
            Command::from_frame(cmd(&["zrange", "z", "0", "-1", "withscores"])).unwrap(),
            Command::ZRange {
                key: "z".into(),
                start: 0,
                stop: -1,
                with_scores: true,
            },
        );
    }

    #[test]
    fn zrange_invalid_option() {
        let err = Command::from_frame(cmd(&["ZRANGE", "z", "0", "-1", "BADOPT"])).unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
    }

    #[test]
    fn zrange_wrong_arity() {
        let err = Command::from_frame(cmd(&["ZRANGE", "z", "0"])).unwrap_err();
        assert!(matches!(err, ProtocolError::WrongArity(_)));
    }
}
