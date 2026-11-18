//! Direct-to-buffer RESP3 serialization.
//!
//! Writes frames directly into a `BytesMut` buffer with no intermediate
//! allocations. Integer-to-string conversion uses `itoa` for fast
//! stack-based formatting.

use bytes::BufMut;
use bytes::BytesMut;

use crate::types::Frame;

/// Maximum nesting depth for serialization. Matches the parser's limit.
const MAX_SERIALIZE_DEPTH: usize = 64;

impl Frame {
    /// Serializes this frame into the provided buffer.
    ///
    /// Writes the full RESP3 wire representation, including type prefix
    /// and trailing `\r\n` delimiters. Nested structures are bounded to
    /// [`MAX_SERIALIZE_DEPTH`] levels to prevent stack overflow.
    #[inline]
    pub fn serialize(&self, dst: &mut BytesMut) {
        self.serialize_inner(dst, 0);
    }

    fn serialize_inner(&self, dst: &mut BytesMut, depth: usize) {
        use crate::types::wire;

        match self {
            Frame::Simple(s) => match s.as_str() {
                "OK" => dst.put_slice(wire::OK),
                "PONG" => dst.put_slice(wire::PONG),
                _ => put_line(b'+', s, dst),
            },
            Frame::Error(msg) => put_line(b'-', msg, dst),
            Frame::Integer(n) => match *n {
                0 => dst.put_slice(wire::ZERO),
                1 => dst.put_slice(wire::ONE),
                -1 => dst.put_slice(wire::NEG_ONE),
                _ => {
                    dst.put_u8(b':');
                    write_i64(*n, dst);
                    dst.put_slice(b"\r\n");
                }
            },
            Frame::Bulk(data) => {
                dst.put_u8(b'$');
                write_i64(data.len() as i64, dst);
                dst.put_slice(b"\r\n");
                dst.put_slice(data);
                dst.put_slice(b"\r\n");
            }
            Frame::Array(items) => {
                if depth >= MAX_SERIALIZE_DEPTH {
                    // emit an error frame instead of overflowing the stack
                    dst.put_slice(b"-ERR nesting too deep\r\n");
                    return;
                }
                dst.put_u8(b'*');
                write_i64(items.len() as i64, dst);
                dst.put_slice(b"\r\n");
                for item in items {
                    item.serialize_inner(dst, depth + 1);
                }
            }
            Frame::Null => {
                dst.put_slice(wire::NULL);
            }
            Frame::Map(pairs) => {
                if depth >= MAX_SERIALIZE_DEPTH {
                    dst.put_slice(b"-ERR nesting too deep\r\n");
                    return;
                }
                dst.put_u8(b'%');
                write_i64(pairs.len() as i64, dst);
                dst.put_slice(b"\r\n");
                for (key, val) in pairs {
                    key.serialize_inner(dst, depth + 1);
                    val.serialize_inner(dst, depth + 1);
                }
            }
        }
    }
}

/// Writes a simple string or error line. CR and LF become spaces, because
/// these frames end at the first CRLF: an error message that echoes client
/// input could otherwise add replies of its own.
fn put_line(prefix: u8, text: &str, dst: &mut BytesMut) {
    dst.put_u8(prefix);
    let bytes = text.as_bytes();
    if bytes.iter().any(|&b| b == b'\r' || b == b'\n') {
        dst.extend(bytes.iter().map(|&b| match b {
            b'\r' | b'\n' => b' ',
            b => b,
        }));
    } else {
        dst.put_slice(bytes);
    }
    dst.put_slice(b"\r\n");
}

/// Writes an i64 as its decimal ASCII representation directly into the buffer.
#[inline]
fn write_i64(val: i64, dst: &mut BytesMut) {
    let mut buf = itoa::Buffer::new();
    dst.put_slice(buf.format(val).as_bytes());
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn serialize(frame: &Frame) -> Vec<u8> {
        let mut buf = BytesMut::new();
        frame.serialize(&mut buf);
        buf.to_vec()
    }

    #[test]
    fn simple_string() {
        assert_eq!(serialize(&Frame::Simple("OK".into())), b"+OK\r\n");
    }

    #[test]
    fn error() {
        assert_eq!(serialize(&Frame::Error("ERR bad".into())), b"-ERR bad\r\n");
    }

    #[test]
    fn line_breaks_in_simple_strings_and_errors_become_spaces() {
        let injected = "ERR unknown command 'x\r\n+OK'";
        assert_eq!(
            serialize(&Frame::Error(injected.into())),
            b"-ERR unknown command 'x  +OK'\r\n"
        );
        assert_eq!(serialize(&Frame::Simple("a\nb\rc".into())), b"+a b c\r\n");
    }

    #[test]
    fn integer() {
        assert_eq!(serialize(&Frame::Integer(42)), b":42\r\n");
        assert_eq!(serialize(&Frame::Integer(-1)), b":-1\r\n");
        assert_eq!(serialize(&Frame::Integer(0)), b":0\r\n");
    }

    #[test]
    fn bulk_string() {
        assert_eq!(
            serialize(&Frame::Bulk(Bytes::from_static(b"hello"))),
            b"$5\r\nhello\r\n"
        );
    }

    #[test]
    fn empty_bulk_string() {
        assert_eq!(
            serialize(&Frame::Bulk(Bytes::from_static(b""))),
            b"$0\r\n\r\n"
        );
    }

    #[test]
    fn null() {
        assert_eq!(serialize(&Frame::Null), b"_\r\n");
    }

    #[test]
    fn array() {
        let frame = Frame::Array(vec![Frame::Simple("hello".into()), Frame::Integer(42)]);
        assert_eq!(serialize(&frame), b"*2\r\n+hello\r\n:42\r\n");
    }

    #[test]
    fn empty_array() {
        assert_eq!(serialize(&Frame::Array(vec![])), b"*0\r\n");
    }

    #[test]
    fn map() {
        let frame = Frame::Map(vec![(Frame::Simple("key".into()), Frame::Integer(1))]);
        assert_eq!(serialize(&frame), b"%1\r\n+key\r\n:1\r\n");
    }

    #[test]
    fn round_trip() {
        use crate::parse::parse_frame;

        let frames = vec![
            Frame::Simple("OK".into()),
            Frame::Error("ERR nope".into()),
            Frame::Integer(i64::MAX),
            Frame::Integer(i64::MIN),
            Frame::Bulk(Bytes::from_static(b"binary\x00data")),
            Frame::Bulk(Bytes::from_static(b"")),
            Frame::Null,
            Frame::Array(vec![
                Frame::Integer(1),
                Frame::Bulk(Bytes::from_static(b"two")),
                Frame::Null,
            ]),
            Frame::Map(vec![
                (Frame::Simple("a".into()), Frame::Integer(1)),
                (Frame::Simple("b".into()), Frame::Integer(2)),
            ]),
            Frame::Array(vec![
                Frame::Array(vec![Frame::Integer(1), Frame::Integer(2)]),
                Frame::Array(vec![Frame::Integer(3)]),
            ]),
        ];

        for original in &frames {
            let mut buf = BytesMut::new();
            original.serialize(&mut buf);

            let (parsed, consumed) = parse_frame(&buf)
                .expect("round-trip parse should not error")
                .expect("round-trip parse should return a frame");

            assert_eq!(&parsed, original, "round-trip failed for {original:?}");
            assert_eq!(consumed, buf.len(), "should consume entire buffer");
        }
    }
}
