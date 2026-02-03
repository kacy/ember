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
}
