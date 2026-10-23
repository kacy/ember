//! Redis-style glob matching.
//!
//! KEYS, SCAN, HSCAN, SSCAN, ZSCAN, PSUBSCRIBE, ACL key patterns and
//! CONFIG GET all use this matcher. It follows the same rules as Redis:
//!
//! - `*` matches any sequence of bytes, including an empty one.
//! - `?` matches exactly one byte.
//! - `[abc]` matches one byte from the set, and `[a-z]` matches a range.
//!   `[^abc]` matches one byte not in the set. A class with no closing
//!   `]` runs to the end of the pattern.
//! - `\x` matches `x` literally, both inside and outside a class.
//!
//! Matching works on bytes, not chars, so `?` matches one byte of a
//! multi-byte UTF-8 character, as in Redis.
//!
//! The matcher remembers only the most recent `*` and backtracks to it on
//! a mismatch, so the worst case is O(pattern * text) with no recursion.

/// Returns true if `text` matches `pattern`. The match is case-sensitive.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    matches(pattern.as_bytes(), text.as_bytes(), false)
}

/// Returns true if `text` matches `pattern`, ignoring ASCII case.
pub fn glob_match_nocase(pattern: &str, text: &str) -> bool {
    matches(pattern.as_bytes(), text.as_bytes(), true)
}

fn matches(pat: &[u8], txt: &[u8], nocase: bool) -> bool {
    let (mut pi, mut ti) = (0, 0);
    // pattern position just after the last `*`, and the text position
    // that star is currently assumed to extend to
    let mut star: Option<(usize, usize)> = None;

    while ti < txt.len() {
        let step = match pat.get(pi) {
            Some(b'*') => {
                pi += 1;
                star = Some((pi, ti));
                continue;
            }
            Some(b'?') => Some(1),
            Some(b'[') => match_class(&pat[pi + 1..], txt[ti], nocase).map(|len| len + 1),
            Some(b'\\') if pi + 1 < pat.len() => eq(pat[pi + 1], txt[ti], nocase).then_some(2),
            Some(&c) => eq(c, txt[ti], nocase).then_some(1),
            None => None,
        };

        match (step, star) {
            (Some(len), _) => {
                pi += len;
                ti += 1;
            }
            // let the last star absorb one more byte and retry from there
            (None, Some((star_pi, star_ti))) => {
                pi = star_pi;
                ti = star_ti + 1;
                star = Some((star_pi, ti));
            }
            (None, None) => return false,
        }
    }

    pat[pi..].iter().all(|&c| c == b'*')
}

/// Matches one byte against the class that starts just after a `[`.
///
/// Returns the class length including the closing `]` when `ch` is in
/// the class, or `None` when it isn't.
fn match_class(class: &[u8], ch: u8, nocase: bool) -> Option<usize> {
    let (negate, mut i) = match class.first() {
        Some(b'^') => (true, 1),
        _ => (false, 0),
    };
    let mut found = false;

    while i < class.len() && class[i] != b']' {
        if class[i] == b'\\' && i + 1 < class.len() {
            found |= eq(class[i + 1], ch, nocase);
            i += 2;
        } else if i + 2 < class.len() && class[i + 1] == b'-' && class[i + 2] != b']' {
            let (lo, hi) = (class[i].min(class[i + 2]), class[i].max(class[i + 2]));
            let in_range = |c: u8| (lo..=hi).contains(&c);
            found |= in_range(ch)
                || nocase
                    && (in_range(ch.to_ascii_lowercase()) || in_range(ch.to_ascii_uppercase()));
            i += 3;
        } else {
            found |= eq(class[i], ch, nocase);
            i += 1;
        }
    }

    // skip the closing `]` if there is one
    let len = (i + 1).min(class.len());
    (found != negate).then_some(len)
}

fn eq(a: u8, b: u8, nocase: bool) -> bool {
    if nocase {
        a.eq_ignore_ascii_case(&b)
    } else {
        a == b
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal() {
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "hellO"));
        assert!(!glob_match("hello", "hello!"));
        assert!(!glob_match("hello", "hell"));
        assert!(glob_match("", ""));
        assert!(!glob_match("", "a"));
    }

    #[test]
    fn star() {
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("user:*", "user:"));
        assert!(glob_match("user:*", "user:123"));
        assert!(!glob_match("user:*", "item:123"));
        assert!(glob_match("*:data", "foo:data"));
        assert!(glob_match("cache:*:data", "cache:session:data"));
        assert!(glob_match("max*len", "maxmemory-len"));
        assert!(glob_match("a*b*c", "aXbYbZc"));
        assert!(!glob_match("a*b*c", "aXbYbZ"));
        assert!(glob_match("**a", "a"));
    }

    #[test]
    fn question_mark() {
        assert!(glob_match("key?", "key1"));
        assert!(!glob_match("key?", "key"));
        assert!(!glob_match("key?", "key12"));
        assert!(glob_match("?*", "x"));
        assert!(!glob_match("?*", ""));
    }

    #[test]
    fn classes() {
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(!glob_match("h[ae]llo", "hillo"));
        assert!(glob_match("h[^ae]llo", "hillo"));
        assert!(!glob_match("h[^ae]llo", "hello"));
        assert!(glob_match("user:[0-9]*", "user:42"));
        assert!(!glob_match("user:[0-9]*", "user:x"));
        // a reversed range works like the forward one
        assert!(glob_match("[z-a]", "m"));
        // `-` at the end of a class is literal
        assert!(glob_match("[a-]", "-"));
        assert!(glob_match("[\\]]", "]"));
        assert!(!glob_match("[]", "a"));
    }

    #[test]
    fn unterminated_class_runs_to_end() {
        assert!(glob_match("[abc", "b"));
        assert!(!glob_match("[abc", "d"));
    }

    #[test]
    fn escapes() {
        assert!(glob_match("hello\\*", "hello*"));
        assert!(!glob_match("hello\\*", "helloX"));
        assert!(glob_match("a\\?", "a?"));
        assert!(!glob_match("a\\?", "ab"));
        assert!(glob_match("\\[x]", "[x]"));
        // a trailing backslash matches itself
        assert!(glob_match("a\\", "a\\"));
    }

    #[test]
    fn nocase() {
        assert!(glob_match_nocase("PORT", "port"));
        assert!(glob_match_nocase("SLOW*", "slowlog-max-len"));
        assert!(glob_match_nocase("[A-C]x", "bX"));
        assert!(!glob_match("PORT", "port"));
    }

    #[test]
    fn matches_bytes_not_chars() {
        // "é" is two bytes in UTF-8
        assert!(glob_match("??", "é"));
        assert!(!glob_match("?", "é"));
    }

    #[test]
    fn many_stars_stay_fast() {
        let text = "a".repeat(10_000);
        let pattern = format!("{}b", "*a".repeat(50));
        assert!(!glob_match(&pattern, &text));
    }
}
