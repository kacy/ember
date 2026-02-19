//! Zero-copy RESP3 parser.
//!
//! Operates on buffered byte slices. The caller is responsible for reading
//! data from the network into a buffer — this parser is purely synchronous.
//!
//! The parser uses a `Cursor<&[u8]>` to track its position through the
//! input buffer without consuming it, allowing the caller to retry once
//! more data arrives.
//!
//! # Single-pass design
//!
//! Earlier versions used a two-pass approach: `check()` to validate a
//! complete frame exists, then `parse()` to build Frame values. This
//! scanned every byte twice. The current implementation does a single
//! pass that builds Frame values directly, returning `Incomplete` if
//! the buffer doesn't contain enough data yet.
//!
//! # Zero-copy bulk strings
//!
//! When parsing from a `Bytes` buffer via [`parse_frame_bytes`], bulk
//! string data is returned as a zero-copy `Bytes::slice()` into the
//! original buffer. This avoids a heap allocation per bulk string.
//! The fallback [`parse_frame`] copies bulk data for callers that
//! only have a `&[u8]`.

use std::io::Cursor;

use bytes::Bytes;

use crate::error::ProtocolError;
use crate::types::Frame;

/// Maximum nesting depth for arrays and maps. Prevents stack overflow
/// from malicious or malformed deeply-nested frames.
const MAX_NESTING_DEPTH: usize = 64;

/// Maximum number of elements in an array or map. Prevents memory
/// amplification attacks where tiny elements (3 bytes each) create
/// disproportionately large Vec allocations.
const MAX_ARRAY_ELEMENTS: usize = 1_048_576;

/// Maximum length of a bulk string in bytes (512 MB, matching Redis).
const MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

/// Cap for Vec::with_capacity in array/map parsing. A declared count of
/// 1M elements with capacity pre-allocation costs ~72 MB upfront even
/// before any child data is parsed. This cap limits the initial allocation
/// while still letting the Vec grow organically as elements are parsed.
const PREALLOC_CAP: usize = 1024;

/// Zero-copy frame parser. Bulk string data is returned as `Bytes::slice()`
/// into the input buffer, avoiding a heap copy per bulk string.
///
/// Use this on the hot path when the caller has a `Bytes` (e.g. from
/// `BytesMut::freeze()`).
///
/// Returns `Ok(Some((frame, consumed)))` if a complete frame was parsed,
/// `Ok(None)` if the buffer doesn't contain enough data yet,
/// or `Err(...)` if the data is malformed.
#[inline]
pub fn parse_frame_bytes(buf: &Bytes) -> Result<Option<(Frame, usize)>, ProtocolError> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut cursor = Cursor::new(buf.as_ref());

    match try_parse(&mut cursor, Some(buf), 0) {
        Ok(frame) => {
            let consumed = cursor.position() as usize;
            Ok(Some((frame, consumed)))
        }
        Err(ProtocolError::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Checks whether `buf` contains a complete RESP3 frame and parses it.
///
/// Bulk string data is copied out of the buffer. Prefer [`parse_frame_bytes`]
/// on hot paths when a `Bytes` reference is available.
///
/// Returns `Ok(Some(frame))` if a complete frame was parsed,
/// `Ok(None)` if the buffer doesn't contain enough data yet,
/// or `Err(...)` if the data is malformed.
#[inline]
pub fn parse_frame(buf: &[u8]) -> Result<Option<(Frame, usize)>, ProtocolError> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut cursor = Cursor::new(buf);

    match try_parse(&mut cursor, None, 0) {
        Ok(frame) => {
            let consumed = cursor.position() as usize;
            Ok(Some((frame, consumed)))
        }
        Err(ProtocolError::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// single-pass parser: validates and builds Frame values in one traversal
// ---------------------------------------------------------------------------

/// Parses a complete RESP3 frame from the cursor position, returning
/// `Incomplete` if the buffer doesn't contain enough data.
///
/// When `src` is `Some`, bulk string bytes are sliced zero-copy from the
/// source buffer. When `None`, they are copied.
fn try_parse(
    cursor: &mut Cursor<&[u8]>,
    src: Option<&Bytes>,
    depth: usize,
) -> Result<Frame, ProtocolError> {
    let prefix = read_byte(cursor)?;

    match prefix {
        b'+' => {
            let line = read_line(cursor)?;
            let s = std::str::from_utf8(line).map_err(|_| {
                ProtocolError::InvalidCommandFrame("invalid utf-8 in simple string".into())
            })?;
            Ok(Frame::Simple(s.to_owned()))
        }
        b'-' => {
            let line = read_line(cursor)?;
            let s = std::str::from_utf8(line).map_err(|_| {
                ProtocolError::InvalidCommandFrame("invalid utf-8 in error string".into())
            })?;
            Ok(Frame::Error(s.to_owned()))
        }
        b':' => {
            let val = read_integer_line(cursor)?;
            Ok(Frame::Integer(val))
        }
        b'$' => {
            let len = read_integer_line(cursor)?;
            if len < 0 {
                return Err(ProtocolError::InvalidFrameLength(len));
            }
            if len > MAX_BULK_LEN {
                return Err(ProtocolError::BulkStringTooLarge(len as usize));
            }
            let len = len as usize;

            // need `len` bytes of data + \r\n
            let remaining = remaining(cursor);
            if remaining < len + 2 {
                return Err(ProtocolError::Incomplete);
            }

            let pos = cursor.position() as usize;

            // verify trailing \r\n (scope the borrow so we can mutate cursor after)
            {
                let buf = cursor.get_ref();
                if buf[pos + len] != b'\r' || buf[pos + len + 1] != b'\n' {
                    return Err(ProtocolError::InvalidFrameLength(len as i64));
                }
            }

            cursor.set_position((pos + len + 2) as u64);

            // zero-copy when source Bytes is available, copy otherwise
            let data = match src {
                Some(b) => b.slice(pos..pos + len),
                None => Bytes::copy_from_slice(&cursor.get_ref()[pos..pos + len]),
            };
            Ok(Frame::Bulk(data))
        }
        b'*' => {
            let next_depth = depth + 1;
            if next_depth > MAX_NESTING_DEPTH {
                return Err(ProtocolError::NestingTooDeep(MAX_NESTING_DEPTH));
            }

            let count = read_integer_line(cursor)?;
            if count < 0 {
                return Err(ProtocolError::InvalidFrameLength(count));
            }
            if count as usize > MAX_ARRAY_ELEMENTS {
                return Err(ProtocolError::TooManyElements(count as usize));
            }

            let count = count as usize;
            let mut frames = Vec::with_capacity(count.min(PREALLOC_CAP));
            for _ in 0..count {
                frames.push(try_parse(cursor, src, next_depth)?);
            }
            Ok(Frame::Array(frames))
        }
        b'_' => {
            // consume the trailing \r\n
            let _ = read_line(cursor)?;
            Ok(Frame::Null)
        }
        b'%' => {
            let next_depth = depth + 1;
            if next_depth > MAX_NESTING_DEPTH {
                return Err(ProtocolError::NestingTooDeep(MAX_NESTING_DEPTH));
            }

            let count = read_integer_line(cursor)?;
            if count < 0 {
                return Err(ProtocolError::InvalidFrameLength(count));
            }
            if count as usize > MAX_ARRAY_ELEMENTS {
                return Err(ProtocolError::TooManyElements(count as usize));
            }

            let count = count as usize;
            let mut pairs = Vec::with_capacity(count.min(PREALLOC_CAP));
            for _ in 0..count {
                let key = try_parse(cursor, src, next_depth)?;
                let val = try_parse(cursor, src, next_depth)?;
                pairs.push((key, val));
            }
            Ok(Frame::Map(pairs))
        }
        other => Err(ProtocolError::InvalidPrefix(other)),
    }
}

// ---------------------------------------------------------------------------
// low-level cursor helpers
// ---------------------------------------------------------------------------

fn read_byte(cursor: &mut Cursor<&[u8]>) -> Result<u8, ProtocolError> {
    let pos = cursor.position() as usize;
    if pos >= cursor.get_ref().len() {
        return Err(ProtocolError::Incomplete);
    }
    cursor.set_position((pos + 1) as u64);
    Ok(cursor.get_ref()[pos])
}

/// Returns the slice of bytes up to (but not including) the next `\r\n`,
/// and advances the cursor past the `\r\n`.
fn read_line<'a>(cursor: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], ProtocolError> {
    let start = cursor.position() as usize;
    let end = find_crlf(cursor)?;
    Ok(&cursor.get_ref()[start..end])
}

/// Reads a line and parses it as an i64.
fn read_integer_line(cursor: &mut Cursor<&[u8]>) -> Result<i64, ProtocolError> {
    let line = read_line(cursor)?;
    parse_i64_bytes(line)
}

/// Finds the next `\r\n` in the buffer starting from the cursor position.
/// Returns the index of `\r` and advances the cursor past the `\n`.
fn find_crlf(cursor: &mut Cursor<&[u8]>) -> Result<usize, ProtocolError> {
    let buf = cursor.get_ref();
    let start = cursor.position() as usize;

    if start >= buf.len() {
        return Err(ProtocolError::Incomplete);
    }

    // SIMD-accelerated scan for \r, then verify \n follows.
    // memchr processes 16-32 bytes per cycle vs 1 byte in a naive loop.
    let mut pos = start;
    while let Some(offset) = memchr::memchr(b'\r', &buf[pos..]) {
        let cr = pos + offset;
        if cr + 1 < buf.len() && buf[cr + 1] == b'\n' {
            cursor.set_position((cr + 2) as u64);
            return Ok(cr);
        }
        // bare \r without \n — keep scanning past it
        pos = cr + 1;
    }

    Err(ProtocolError::Incomplete)
}

fn remaining(cursor: &Cursor<&[u8]>) -> usize {
    let len = cursor.get_ref().len();
    let pos = cursor.position() as usize;
    len.saturating_sub(pos)
}

/// Parses an i64 directly from a byte slice without allocating a String.
///
/// Negative numbers are accumulated in the negative direction so that
/// `i64::MIN` (-9223372036854775808) is representable without overflow.
fn parse_i64_bytes(buf: &[u8]) -> Result<i64, ProtocolError> {
    if buf.is_empty() {
        return Err(ProtocolError::InvalidInteger);
    }

    let (negative, digits) = if buf[0] == b'-' {
        (true, &buf[1..])
    } else {
        (false, buf)
    };

    if digits.is_empty() {
        return Err(ProtocolError::InvalidInteger);
    }

    if negative {
        // accumulate in the negative direction to handle i64::MIN
        let mut n: i64 = 0;
        for &b in digits {
            if !b.is_ascii_digit() {
                return Err(ProtocolError::InvalidInteger);
            }
            n = n
                .checked_mul(10)
                .and_then(|n| n.checked_sub((b - b'0') as i64))
                .ok_or(ProtocolError::InvalidInteger)?;
        }
        Ok(n)
    } else {
        let mut n: i64 = 0;
        for &b in digits {
            if !b.is_ascii_digit() {
                return Err(ProtocolError::InvalidInteger);
            }
            n = n
                .checked_mul(10)
                .and_then(|n| n.checked_add((b - b'0') as i64))
                .ok_or(ProtocolError::InvalidInteger)?;
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn must_parse(input: &[u8]) -> Frame {
        let (frame, consumed) = parse_frame(input)
            .expect("parse should not error")
            .expect("parse should return a frame");
        assert_eq!(consumed, input.len(), "should consume entire input");
        frame
    }

    fn must_parse_zerocopy(input: &Bytes) -> Frame {
        let (frame, consumed) = parse_frame_bytes(input)
            .expect("parse should not error")
            .expect("parse should return a frame");
        assert_eq!(consumed, input.len(), "should consume entire input");
        frame
    }

    #[test]
    fn simple_string() {
        assert_eq!(must_parse(b"+OK\r\n"), Frame::Simple("OK".into()));
        assert_eq!(
            must_parse(b"+hello world\r\n"),
            Frame::Simple("hello world".into())
        );
    }

    #[test]
    fn simple_error() {
        assert_eq!(
            must_parse(b"-ERR unknown command\r\n"),
            Frame::Error("ERR unknown command".into())
        );
    }

    #[test]
    fn integer() {
        assert_eq!(must_parse(b":42\r\n"), Frame::Integer(42));
        assert_eq!(must_parse(b":0\r\n"), Frame::Integer(0));
        assert_eq!(must_parse(b":-1\r\n"), Frame::Integer(-1));
        assert_eq!(
            must_parse(b":9223372036854775807\r\n"),
            Frame::Integer(i64::MAX)
        );
        assert_eq!(
            must_parse(b":-9223372036854775808\r\n"),
            Frame::Integer(i64::MIN)
        );
    }

    #[test]
    fn bulk_string() {
        assert_eq!(
            must_parse(b"$5\r\nhello\r\n"),
            Frame::Bulk(Bytes::from_static(b"hello"))
        );
    }

    #[test]
    fn empty_bulk_string() {
        assert_eq!(
            must_parse(b"$0\r\n\r\n"),
            Frame::Bulk(Bytes::from_static(b""))
        );
    }

    #[test]
    fn bulk_string_with_binary() {
        let input = b"$4\r\n\x00\x01\x02\x03\r\n";
        assert_eq!(
            must_parse(input),
            Frame::Bulk(Bytes::copy_from_slice(&[0, 1, 2, 3]))
        );
    }

    #[test]
    fn null() {
        assert_eq!(must_parse(b"_\r\n"), Frame::Null);
    }

    #[test]
    fn array() {
        let input = b"*2\r\n+hello\r\n+world\r\n";
        assert_eq!(
            must_parse(input),
            Frame::Array(vec![
                Frame::Simple("hello".into()),
                Frame::Simple("world".into()),
            ])
        );
    }

    #[test]
    fn empty_array() {
        assert_eq!(must_parse(b"*0\r\n"), Frame::Array(vec![]));
    }

    #[test]
    fn nested_array() {
        let input = b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n";
        assert_eq!(
            must_parse(input),
            Frame::Array(vec![
                Frame::Array(vec![Frame::Integer(1), Frame::Integer(2)]),
                Frame::Array(vec![Frame::Integer(3), Frame::Integer(4)]),
            ])
        );
    }

    #[test]
    fn array_with_null() {
        let input = b"*3\r\n+OK\r\n_\r\n:1\r\n";
        assert_eq!(
            must_parse(input),
            Frame::Array(vec![
                Frame::Simple("OK".into()),
                Frame::Null,
                Frame::Integer(1),
            ])
        );
    }

    #[test]
    fn map() {
        let input = b"%2\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n";
        assert_eq!(
            must_parse(input),
            Frame::Map(vec![
                (Frame::Simple("key1".into()), Frame::Integer(1)),
                (Frame::Simple("key2".into()), Frame::Integer(2)),
            ])
        );
    }

    #[test]
    fn incomplete_returns_none() {
        assert_eq!(parse_frame(b"").unwrap(), None);
        assert_eq!(parse_frame(b"+OK").unwrap(), None);
        assert_eq!(parse_frame(b"+OK\r").unwrap(), None);
        assert_eq!(parse_frame(b"$5\r\nhel").unwrap(), None);
        assert_eq!(parse_frame(b"*2\r\n+OK\r\n").unwrap(), None);
    }

    #[test]
    fn invalid_prefix() {
        let err = parse_frame(b"~invalid\r\n").unwrap_err();
        assert_eq!(err, ProtocolError::InvalidPrefix(b'~'));
    }

    #[test]
    fn invalid_integer() {
        let err = parse_frame(b":abc\r\n").unwrap_err();
        assert_eq!(err, ProtocolError::InvalidInteger);
    }

    #[test]
    fn negative_bulk_length() {
        let err = parse_frame(b"$-1\r\n").unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidFrameLength(-1)));
    }

    #[test]
    fn parse_consumes_exact_bytes() {
        // buffer contains a full frame plus trailing garbage
        let buf = b"+OK\r\ntrailing";
        let (frame, consumed) = parse_frame(buf).unwrap().unwrap();
        assert_eq!(frame, Frame::Simple("OK".into()));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn deeply_nested_array_rejected() {
        // build a frame nested 65 levels deep (exceeds MAX_NESTING_DEPTH of 64)
        let mut buf = Vec::new();
        for _ in 0..65 {
            buf.extend_from_slice(b"*1\r\n");
        }
        buf.extend_from_slice(b":1\r\n"); // leaf value

        let err = parse_frame(&buf).unwrap_err();
        assert!(
            matches!(err, ProtocolError::NestingTooDeep(64)),
            "expected NestingTooDeep, got {err:?}"
        );
    }

    #[test]
    fn nesting_at_limit_accepted() {
        // exactly 64 levels deep — should succeed
        let mut buf = Vec::new();
        for _ in 0..64 {
            buf.extend_from_slice(b"*1\r\n");
        }
        buf.extend_from_slice(b":1\r\n");

        let result = parse_frame(&buf);
        assert!(result.is_ok(), "64 levels of nesting should be accepted");
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn zerocopy_bulk_string() {
        let input = Bytes::from_static(b"$5\r\nhello\r\n");
        assert_eq!(
            must_parse_zerocopy(&input),
            Frame::Bulk(Bytes::from_static(b"hello"))
        );
    }

    #[test]
    fn zerocopy_array() {
        let input = Bytes::from_static(b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
        let frame = must_parse_zerocopy(&input);
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"GET")),
                Frame::Bulk(Bytes::from_static(b"mykey")),
            ])
        );
    }

    #[test]
    fn parse_i64_bytes_valid() {
        assert_eq!(parse_i64_bytes(b"0").unwrap(), 0);
        assert_eq!(parse_i64_bytes(b"42").unwrap(), 42);
        assert_eq!(parse_i64_bytes(b"-1").unwrap(), -1);
        assert_eq!(
            parse_i64_bytes(b"9223372036854775807").unwrap(),
            i64::MAX
        );
        assert_eq!(
            parse_i64_bytes(b"-9223372036854775808").unwrap(),
            i64::MIN
        );
    }

    #[test]
    fn parse_i64_bytes_invalid() {
        assert!(parse_i64_bytes(b"").is_err());
        assert!(parse_i64_bytes(b"-").is_err());
        assert!(parse_i64_bytes(b"abc").is_err());
        assert!(parse_i64_bytes(b"12a").is_err());
    }
}
