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
#[derive(Debug, Clone, PartialEq, Eq)]
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

    /// A command we don't recognize (yet).
    Unknown(String),
}

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
}
