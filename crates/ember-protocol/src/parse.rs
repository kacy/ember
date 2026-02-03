//! Zero-copy RESP3 parser.
//!
//! Operates on buffered byte slices. The caller is responsible for reading
//! data from the network into a buffer — this parser is purely synchronous.
//!
//! The parser uses a `Cursor<&[u8]>` to track its position through the
//! input buffer without consuming it, allowing the caller to retry once
//! more data arrives.

use std::io::Cursor;

use bytes::Bytes;

use crate::error::ProtocolError;
use crate::types::Frame;

/// Checks whether `buf` contains a complete RESP3 frame and parses it.
///
/// Returns `Ok(Some(frame))` if a complete frame was parsed,
/// `Ok(None)` if the buffer doesn't contain enough data yet,
/// or `Err(...)` if the data is malformed.
pub fn parse_frame(buf: &[u8]) -> Result<Option<(Frame, usize)>, ProtocolError> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut cursor = Cursor::new(buf);

    match check(&mut cursor) {
        Ok(()) => {
            // we know a complete frame exists — reset and parse it
            cursor.set_position(0);
            let frame = parse(&mut cursor)?;
            let consumed = cursor.position() as usize;
            Ok(Some((frame, consumed)))
        }
        Err(ProtocolError::Incomplete) => Ok(None),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// check: validates a complete frame exists without allocating
// ---------------------------------------------------------------------------

/// Peeks through the buffer to verify a complete frame is present.
/// Advances the cursor past the frame on success.
fn check(cursor: &mut Cursor<&[u8]>) -> Result<(), ProtocolError> {
    let prefix = read_byte(cursor)?;

    match prefix {
        b'+' | b'-' => check_line(cursor),
        b':' => check_line(cursor),
        b'$' => check_bulk(cursor),
        b'*' => check_array(cursor),
        b'_' => check_line(cursor),
        b'%' => check_map(cursor),
        other => Err(ProtocolError::InvalidPrefix(other)),
    }
}

fn check_line(cursor: &mut Cursor<&[u8]>) -> Result<(), ProtocolError> {
    find_crlf(cursor)?;
    Ok(())
}

fn check_bulk(cursor: &mut Cursor<&[u8]>) -> Result<(), ProtocolError> {
    let len = read_integer_line(cursor)?;
    if len < 0 {
        return Err(ProtocolError::InvalidFrameLength(len));
    }
    let len = len as usize;

    // need `len` bytes of data + \r\n
    let remaining = remaining(cursor);
    if remaining < len + 2 {
        return Err(ProtocolError::Incomplete);
    }

    let pos = cursor.position() as usize;
    // verify trailing \r\n
    let buf = cursor.get_ref();
    if buf[pos + len] != b'\r' || buf[pos + len + 1] != b'\n' {
        return Err(ProtocolError::InvalidFrameLength(len as i64));
    }

    cursor.set_position((pos + len + 2) as u64);
    Ok(())
}

fn check_array(cursor: &mut Cursor<&[u8]>) -> Result<(), ProtocolError> {
    let count = read_integer_line(cursor)?;
    if count < 0 {
        return Err(ProtocolError::InvalidFrameLength(count));
    }

    for _ in 0..count {
        check(cursor)?;
    }
    Ok(())
}

fn check_map(cursor: &mut Cursor<&[u8]>) -> Result<(), ProtocolError> {
    let count = read_integer_line(cursor)?;
    if count < 0 {
        return Err(ProtocolError::InvalidFrameLength(count));
    }

    for _ in 0..count {
        check(cursor)?; // key
        check(cursor)?; // value
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// parse: actually builds Frame values (only called after check succeeds)
// ---------------------------------------------------------------------------

fn parse(cursor: &mut Cursor<&[u8]>) -> Result<Frame, ProtocolError> {
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
            let len = read_integer_line(cursor)? as usize;
            let pos = cursor.position() as usize;
            let data = &cursor.get_ref()[pos..pos + len];
            cursor.set_position((pos + len + 2) as u64); // skip data + \r\n
            Ok(Frame::Bulk(Bytes::copy_from_slice(data)))
        }
        b'*' => {
            let count = read_integer_line(cursor)? as usize;
            let mut frames = Vec::with_capacity(count);
            for _ in 0..count {
                frames.push(parse(cursor)?);
            }
            Ok(Frame::Array(frames))
        }
        b'_' => {
            // consume the trailing \r\n
            let _ = read_line(cursor)?;
            Ok(Frame::Null)
        }
        b'%' => {
            let count = read_integer_line(cursor)? as usize;
            let mut pairs = Vec::with_capacity(count);
            for _ in 0..count {
                let key = parse(cursor)?;
                let val = parse(cursor)?;
                pairs.push((key, val));
            }
            Ok(Frame::Map(pairs))
        }
        // check() already validated the prefix, so this shouldn't happen
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
    parse_i64(line)
}

/// Finds the next `\r\n` in the buffer starting from the cursor position.
/// Returns the index of `\r` and advances the cursor past the `\n`.
fn find_crlf(cursor: &mut Cursor<&[u8]>) -> Result<usize, ProtocolError> {
    let buf = cursor.get_ref();
    let start = cursor.position() as usize;

    if start >= buf.len() {
        return Err(ProtocolError::Incomplete);
    }

    // scan for \r\n
    for i in start..buf.len().saturating_sub(1) {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            cursor.set_position((i + 2) as u64);
            return Ok(i);
        }
    }

    Err(ProtocolError::Incomplete)
}

fn remaining(cursor: &Cursor<&[u8]>) -> usize {
    let len = cursor.get_ref().len();
    let pos = cursor.position() as usize;
    len.saturating_sub(pos)
}

fn parse_i64(buf: &[u8]) -> Result<i64, ProtocolError> {
    let s = std::str::from_utf8(buf).map_err(|_| ProtocolError::InvalidInteger)?;
    s.parse::<i64>().map_err(|_| ProtocolError::InvalidInteger)
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
}
