//! Command parsing from RESP3 frames.
//!
//! Converts a parsed [`Frame`] (expected to be an array) into a typed
//! [`Command`] enum. This keeps protocol-level concerns separate from
//! the engine that actually executes commands.

use bytes::Bytes;

use crate::error::ProtocolError;
use crate::types::Frame;

/// A parsed client command, ready for execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// PING with an optional message. Returns PONG or echoes the message.
    Ping(Option<Bytes>),

    /// ECHO <message>. Returns the message back to the client.
    Echo(Bytes),

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
