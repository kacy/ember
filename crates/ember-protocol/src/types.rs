//! RESP3 frame types.
//!
//! The [`Frame`] enum represents a single parsed RESP3 value.
//! Blob strings use `Bytes` for efficient, reference-counted storage
//! that avoids unnecessary copies when moving data through the pipeline.

use bytes::Bytes;

/// A single RESP3 protocol frame.
///
/// Covers the core types needed for basic Redis-compatible commands:
/// strings, errors, integers, bulk data, arrays, null, and maps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Simple string response, e.g. `+OK\r\n`.
    /// Used for short, non-binary status replies.
    Simple(String),

    /// Error response, e.g. `-ERR unknown command\r\n`.
    Error(String),

    /// 64-bit signed integer, e.g. `:42\r\n`.
    Integer(i64),

    /// Bulk (binary-safe) string, e.g. `$5\r\nhello\r\n`.
    /// Uses `Bytes` for zero-copy-friendly handling.
    Bulk(Bytes),

    /// Ordered array of frames, e.g. `*2\r\n+hello\r\n+world\r\n`.
    Array(Vec<Frame>),

    /// Null value, e.g. `_\r\n`.
    Null,

    /// Ordered map of key-value frame pairs, e.g. `%1\r\n+key\r\n+val\r\n`.
    Map(Vec<(Frame, Frame)>),
}

impl Frame {
    /// Returns `true` if this frame is a null value.
    pub fn is_null(&self) -> bool {
        matches!(self, Frame::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_equality() {
        assert_eq!(Frame::Simple("OK".into()), Frame::Simple("OK".into()));
        assert_ne!(Frame::Simple("OK".into()), Frame::Simple("ERR".into()));
        assert_eq!(Frame::Integer(42), Frame::Integer(42));
        assert_eq!(Frame::Null, Frame::Null);
    }

    #[test]
    fn is_null() {
        assert!(Frame::Null.is_null());
        assert!(!Frame::Simple("OK".into()).is_null());
        assert!(!Frame::Integer(0).is_null());
    }

    #[test]
    fn clone_bulk() {
        let frame = Frame::Bulk(Bytes::from_static(b"hello"));
        let cloned = frame.clone();
        assert_eq!(frame, cloned);
    }
}
