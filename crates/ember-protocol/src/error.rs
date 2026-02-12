//! Protocol error types for RESP3 parsing.

use thiserror::Error;

/// Errors that can occur when parsing the RESP3 wire format.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// The input buffer doesn't contain a complete frame yet.
    /// The caller should read more data and try again.
    #[error("incomplete frame: need more data")]
    Incomplete,

    /// Reached end of input unexpectedly while parsing a frame.
    #[error("unexpected end of input")]
    UnexpectedEof,

    /// The first byte of a frame didn't match any known RESP3 type prefix.
    #[error("invalid type prefix: {0:#04x}")]
    InvalidPrefix(u8),

    /// Failed to parse an integer value from the frame content.
    #[error("invalid integer encoding")]
    InvalidInteger,

    /// A bulk string or array declared an invalid length (e.g. negative).
    #[error("invalid frame length: {0}")]
    InvalidFrameLength(i64),

    /// Expected an array frame for a command, but got something else.
    #[error("invalid command frame: {0}")]
    InvalidCommandFrame(String),

    /// Command received with the wrong number of arguments.
    #[error("wrong number of arguments for '{0}' command")]
    WrongArity(String),

    /// The frame exceeds the maximum nesting depth.
    #[error("frame nesting depth exceeds limit of {0}")]
    NestingTooDeep(usize),

    /// An array or map declares more elements than the allowed maximum.
    #[error("array/map element count {0} exceeds limit")]
    TooManyElements(usize),

    /// A bulk string exceeds the maximum allowed length.
    #[error("bulk string length {0} exceeds limit")]
    BulkStringTooLarge(usize),
}
