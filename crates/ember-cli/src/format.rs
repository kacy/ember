//! Pretty-printing for RESP3 frames.
//!
//! Converts server responses into colorized, human-readable output
//! matching the style familiar to redis-cli users.

use colored::Colorize;
use ember_protocol::types::Frame;

/// Formats a RESP3 frame for terminal display.
///
/// Output style matches redis-cli conventions:
/// - simple strings: green
/// - errors: red with `(error)` prefix
/// - integers: yellow with `(integer)` prefix
/// - bulk strings: green, quoted (unless multiline)
/// - nil: dim `(nil)`
/// - arrays: numbered list
/// - maps: key => value pairs
pub fn format_response(frame: &Frame) -> String {
    format_frame(frame, 0)
}

/// Strips ANSI escape sequences and other control characters from
/// server-supplied strings to prevent terminal manipulation attacks.
/// Retains printable ASCII, tabs, and newlines (CR/LF).
fn sanitize(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            // skip the ESC and the rest of the ANSI sequence
            if let Some(next) = chars.next() {
                if next == '[' {
                    // CSI sequence — consume until a letter
                    for c in chars.by_ref() {
                        if c.is_ascii_alphabetic() {
                            break;
                        }
                    }
                }
                // else: single-char escape, already consumed
            }
        } else if ch == '\t' || ch == '\n' || ch == '\r' || !ch.is_control() {
            out.push(ch);
        }
    }
    out
}

fn format_frame(frame: &Frame, indent: usize) -> String {
    let prefix = " ".repeat(indent);

    match frame {
        Frame::Simple(s) => format!("{prefix}{}", sanitize(s).green()),

        Frame::Error(e) => format!("{prefix}{} {}", "(error)".red(), sanitize(e).red()),

        Frame::Integer(n) => format!(
            "{prefix}{} {}",
            "(integer)".yellow(),
            n.to_string().yellow()
        ),

        Frame::Bulk(data) => {
            match std::str::from_utf8(data) {
                Ok(s) if s.contains("\r\n") || s.contains('\n') => {
                    // multiline output (like INFO) — print unquoted
                    format!("{prefix}{}", sanitize(s).green())
                }
                Ok(s) => format!("{prefix}{}", format!("\"{}\"", sanitize(s)).green()),
                Err(_) => {
                    // binary data — show as hex
                    let hex: String = data.iter().map(|b| format!("{b:02x}")).collect();
                    format!("{prefix}{}", hex.green())
                }
            }
        }

        Frame::Null => format!("{prefix}{}", "(nil)".dimmed()),

        Frame::Array(items) if items.is_empty() => {
            format!("{prefix}{}", "(empty array)".dimmed())
        }

        Frame::Array(items) => {
            let mut lines = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let num = format!("{})", i + 1);
                let formatted = format_frame(item, 0);
                lines.push(format!("{prefix}{} {}", num, formatted));
            }
            lines.join("\n")
        }

        Frame::Map(pairs) if pairs.is_empty() => {
            format!("{prefix}{}", "(empty map)".dimmed())
        }

        Frame::Map(pairs) => {
            let mut lines = Vec::with_capacity(pairs.len());
            for (i, (key, val)) in pairs.iter().enumerate() {
                let num = format!("{})", i + 1);
                let key_str = format_frame(key, 0);
                let val_str = format_frame(val, 0);
                lines.push(format!("{prefix}{num} {key_str} => {val_str}"));
            }
            lines.join("\n")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // disable colors for deterministic test output
    fn no_color<F: FnOnce() -> String>(f: F) -> String {
        colored::control::set_override(false);
        let result = f();
        colored::control::unset_override();
        result
    }

    #[test]
    fn format_simple_string() {
        let out = no_color(|| format_response(&Frame::Simple("OK".into())));
        assert_eq!(out, "OK");
    }

    #[test]
    fn format_error() {
        let out = no_color(|| format_response(&Frame::Error("ERR unknown command".into())));
        assert_eq!(out, "(error) ERR unknown command");
    }

    #[test]
    fn format_integer() {
        let out = no_color(|| format_response(&Frame::Integer(42)));
        assert_eq!(out, "(integer) 42");
    }

    #[test]
    fn format_negative_integer() {
        let out = no_color(|| format_response(&Frame::Integer(-1)));
        assert_eq!(out, "(integer) -1");
    }

    #[test]
    fn format_bulk_string() {
        let out = no_color(|| format_response(&Frame::Bulk(Bytes::from_static(b"hello"))));
        assert_eq!(out, "\"hello\"");
    }

    #[test]
    fn format_bulk_multiline() {
        let out = no_color(|| format_response(&Frame::Bulk(Bytes::from_static(b"line1\r\nline2"))));
        assert_eq!(out, "line1\r\nline2");
    }

    #[test]
    fn format_bulk_binary() {
        let out = no_color(|| format_response(&Frame::Bulk(Bytes::from_static(&[0xff, 0x00]))));
        assert_eq!(out, "ff00");
    }

    #[test]
    fn format_null() {
        let out = no_color(|| format_response(&Frame::Null));
        assert_eq!(out, "(nil)");
    }

    #[test]
    fn format_empty_array() {
        let out = no_color(|| format_response(&Frame::Array(vec![])));
        assert_eq!(out, "(empty array)");
    }

    #[test]
    fn format_array() {
        let out = no_color(|| {
            format_response(&Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"foo")),
                Frame::Bulk(Bytes::from_static(b"bar")),
            ]))
        });
        assert_eq!(out, "1) \"foo\"\n2) \"bar\"");
    }

    #[test]
    fn format_array_with_nil() {
        let out = no_color(|| {
            format_response(&Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"hello")),
                Frame::Null,
            ]))
        });
        assert_eq!(out, "1) \"hello\"\n2) (nil)");
    }

    #[test]
    fn format_empty_map() {
        let out = no_color(|| format_response(&Frame::Map(vec![])));
        assert_eq!(out, "(empty map)");
    }

    #[test]
    fn format_map() {
        let out = no_color(|| {
            format_response(&Frame::Map(vec![(
                Frame::Simple("key".into()),
                Frame::Integer(1),
            )]))
        });
        assert_eq!(out, "1) key => (integer) 1");
    }

    #[test]
    fn sanitize_strips_ansi_escapes() {
        assert_eq!(sanitize("hello\x1b[31mworld\x1b[0m"), "helloworld");
    }

    #[test]
    fn sanitize_strips_control_chars() {
        assert_eq!(sanitize("hello\x07\x08world"), "helloworld");
    }

    #[test]
    fn sanitize_preserves_tabs_and_newlines() {
        assert_eq!(sanitize("line1\nline2\ttab"), "line1\nline2\ttab");
    }

    #[test]
    fn sanitize_server_response_with_escape() {
        let out = no_color(|| format_response(&Frame::Simple("\x1b[31mfake-error\x1b[0m".into())));
        assert_eq!(out, "fake-error");
    }
}
