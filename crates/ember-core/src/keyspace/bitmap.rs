//! Bitmap operations on string-typed keys.
//!
//! Bitmaps are not a separate data type — they use `Value::String(Bytes)`
//! with big-endian bit ordering (Redis compatible): bit 0 is the most
//! significant bit of byte 0.

use ember_protocol::command::{BitOpKind, BitRange, BitRangeUnit};

use super::*;

impl Keyspace {
    /// Returns the bit at `offset` in the string stored at `key`.
    ///
    /// Bit ordering is big-endian: byte `offset / 8`, bit position
    /// `7 - (offset % 8)`. Returns 0 for missing keys or offsets
    /// beyond the string's length.
    pub fn getbit(&mut self, key: &str, offset: u64) -> Result<u8, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }
        match self.entries.get(key) {
            None => Ok(0),
            Some(e) => match &e.value {
                Value::String(data) => {
                    let byte_idx = (offset / 8) as usize;
                    if byte_idx >= data.len() {
                        return Ok(0);
                    }
                    let bit_pos = 7 - (offset % 8) as u32;
                    Ok((data[byte_idx] >> bit_pos) & 1)
                }
                _ => Err(WrongType),
            },
        }
    }

    /// Sets the bit at `offset` to `value` (0 or 1).
    ///
    /// Extends the string with zero bytes when `offset` reaches beyond the
    /// current string length. Returns the old bit value.
    ///
    /// Follows the same memory-tracking pattern as `setrange`.
    pub fn setbit(&mut self, key: &str, offset: u64, value: u8) -> Result<u8, WriteError> {
        self.remove_if_expired(key);

        let byte_idx = (offset / 8) as usize;
        let bit_pos = 7 - (offset % 8) as u32;
        let mask = 1u8 << bit_pos;

        let (existing, expire) = match self.entries.get(key) {
            Some(entry) => match &entry.value {
                Value::String(data) => {
                    let expire = time::remaining_ms(entry.expires_at_ms).map(Duration::from_millis);
                    (data.clone(), expire)
                }
                _ => return Err(WriteError::WrongType),
            },
            None => (Bytes::new(), None),
        };

        let old_bit = if byte_idx < existing.len() {
            (existing[byte_idx] >> bit_pos) & 1
        } else {
            0
        };

        // build the new buffer: existing data + zero-padding if needed
        let new_len = existing.len().max(byte_idx + 1);
        let mut buf = existing.to_vec();
        buf.resize(new_len, 0);

        if value == 1 {
            buf[byte_idx] |= mask;
        } else {
            buf[byte_idx] &= !mask;
        }

        match self.set(key.to_owned(), Bytes::from(buf), expire, false, false) {
            SetResult::Ok | SetResult::Blocked => Ok(old_bit),
            SetResult::OutOfMemory => Err(WriteError::OutOfMemory),
        }
    }

    /// Counts set bits in the string at `key`.
    ///
    /// When `range` is `None`, counts bits across the entire string.
    /// When `range` is `Some(r)`, restricts to the given byte or bit range
    /// (negative indices count from the end of the string).
    ///
    /// Returns 0 for missing keys.
    pub fn bitcount(&mut self, key: &str, range: Option<BitRange>) -> Result<u64, WrongType> {
        if self.remove_if_expired(key) {
            return Ok(0);
        }
        let data = match self.entries.get(key) {
            None => return Ok(0),
            Some(e) => match &e.value {
                Value::String(b) => b.clone(),
                _ => return Err(WrongType),
            },
        };

        match range {
            None => Ok(data.iter().map(|b| b.count_ones() as u64).sum()),
            Some(r) if r.unit == BitRangeUnit::Bit => {
                // bit-granularity: count each individual bit in [start_bit, end_bit]
                let len_bits = data.len() as i64 * 8;
                let start = normalize_bit_index(r.start, len_bits).min(len_bits);
                let end = normalize_bit_index(r.end, len_bits).min(len_bits - 1);
                if start > end {
                    return Ok(0);
                }
                let mut count = 0u64;
                for bit_idx in start..=end {
                    let byte_idx = (bit_idx / 8) as usize;
                    let bit_pos = 7 - (bit_idx % 8) as u32;
                    count += ((data[byte_idx] >> bit_pos) & 1) as u64;
                }
                Ok(count)
            }
            Some(r) => {
                let slice = bit_range_slice(&data, r);
                Ok(slice.iter().map(|b| b.count_ones() as u64).sum())
            }
        }
    }

    /// Returns the position of the first bit equal to `bit` (0 or 1).
    ///
    /// `range` works the same as for `bitcount`. When no `end` is given for
    /// `BITPOS 1`, the search covers the whole string; for `BITPOS 0`, it
    /// also covers the virtual zero bits beyond the string's end.
    ///
    /// Returns -1 if the bit is not found (except for `BITPOS 0` on a missing
    /// key, which returns 0).
    pub fn bitpos(
        &mut self,
        key: &str,
        bit: u8,
        range: Option<BitRange>,
    ) -> Result<i64, WrongType> {
        if self.remove_if_expired(key) {
            // missing key: BITPOS 0 → 0, BITPOS 1 → -1
            return Ok(if bit == 0 { 0 } else { -1 });
        }
        let data = match self.entries.get(key) {
            None => {
                return Ok(if bit == 0 { 0 } else { -1 });
            }
            Some(e) => match &e.value {
                Value::String(b) => b.clone(),
                _ => return Err(WrongType),
            },
        };

        // determine whether the caller constrained the end boundary
        let has_explicit_end = range.map(|r| r.end != -1).unwrap_or(false);

        let (slice, bit_offset) = match range {
            None => (&data[..], 0i64),
            Some(r) if r.unit == BitRangeUnit::Bit => {
                // bit-granularity range: resolve to an inclusive bit range
                let len_bits = data.len() as i64 * 8;
                let start = normalize_bit_index(r.start, len_bits).min(len_bits);
                let end = normalize_bit_index(r.end, len_bits).min(len_bits - 1);
                if start > end {
                    return Ok(-1);
                }
                // search bit-by-bit within the resolved range
                for bit_idx in start..=end {
                    let byte_idx = (bit_idx / 8) as usize;
                    let bit_pos = 7 - (bit_idx % 8) as u32;
                    let found = (data[byte_idx] >> bit_pos) & 1;
                    if found == bit {
                        return Ok(bit_idx);
                    }
                }
                return Ok(-1);
            }
            Some(r) => {
                // byte-granularity range
                let (s, e) = resolve_byte_range(r.start, r.end, data.len());
                if s >= data.len() {
                    return Ok(-1);
                }
                let end = e.min(data.len() - 1);
                (&data[s..=end], (s as i64) * 8)
            }
        };

        // scan bytes for the first matching bit
        for (i, &byte) in slice.iter().enumerate() {
            let b = if bit == 1 { byte } else { !byte };
            if b != 0 {
                let bit_in_byte = b.leading_zeros() as i64;
                return Ok(bit_offset + (i as i64) * 8 + bit_in_byte);
            }
        }

        // not found — for BITPOS 0 without an explicit end, the answer is the
        // first virtual bit past the end of the string.
        if bit == 0 && !has_explicit_end {
            Ok((data.len() as i64) * 8)
        } else {
            Ok(-1)
        }
    }

    /// Performs a bitwise operation across `keys` and stores the result in `dest`.
    ///
    /// Returns the length of the result string (equal to the longest source).
    /// Missing keys are treated as zero-filled strings of the same length.
    /// `NOT` requires exactly one source key (enforced at parse time).
    pub fn bitop(
        &mut self,
        op: BitOpKind,
        dest: String,
        keys: &[String],
    ) -> Result<usize, WriteError> {
        // collect source bytes — type-check each before mutating anything
        let mut sources: Vec<Bytes> = Vec::with_capacity(keys.len());
        for key in keys {
            self.remove_if_expired(key);
            match self.entries.get(key.as_str()) {
                None => sources.push(Bytes::new()),
                Some(e) => match &e.value {
                    Value::String(b) => sources.push(b.clone()),
                    _ => return Err(WriteError::WrongType),
                },
            }
        }

        let result_len = sources.iter().map(|s| s.len()).max().unwrap_or(0);
        let mut result = vec![0u8; result_len];

        match op {
            BitOpKind::Not => {
                // NOT of the single source; bytes beyond the source length → 0xFF
                let src = sources.first().map(|b| b.as_ref()).unwrap_or(&[]);
                for (i, b) in result.iter_mut().enumerate() {
                    *b = if i < src.len() { !src[i] } else { 0xFF };
                }
            }
            BitOpKind::And => {
                // initialize from first source; bytes beyond any source → AND with 0
                if let Some(first) = sources.first() {
                    for (i, b) in result.iter_mut().enumerate() {
                        *b = if i < first.len() { first[i] } else { 0 };
                    }
                }
                for src in sources.iter().skip(1) {
                    for (i, b) in result.iter_mut().enumerate() {
                        let s = if i < src.len() { src[i] } else { 0 };
                        *b &= s;
                    }
                }
            }
            BitOpKind::Or => {
                for src in &sources {
                    for (i, b) in result.iter_mut().enumerate() {
                        if i < src.len() {
                            *b |= src[i];
                        }
                    }
                }
            }
            BitOpKind::Xor => {
                for src in &sources {
                    for (i, b) in result.iter_mut().enumerate() {
                        if i < src.len() {
                            *b ^= src[i];
                        }
                    }
                }
            }
        }

        // store the result — set() handles memory accounting and version bumping.
        // it also implicitly removes any prior value at dest (including wrong-type keys).
        match self.set(dest, Bytes::from(result), None, false, false) {
            SetResult::Ok | SetResult::Blocked => Ok(result_len),
            SetResult::OutOfMemory => Err(WriteError::OutOfMemory),
        }
    }
}

/// Resolves a byte-granularity range `[start, end]` (Redis semantics) into
/// a concrete `(start_byte, end_byte)` pair against a string of `len` bytes.
///
/// Negative indices count from the end (-1 = last byte). The returned range
/// is NOT yet clamped to `[0, len)` — callers must do that.
fn resolve_byte_range(start: i64, end: i64, len: usize) -> (usize, usize) {
    let len = len as i64;
    let s = if start < 0 {
        (len + start).max(0)
    } else {
        start
    } as usize;
    let e = if end < 0 { (len + end).max(0) } else { end } as usize;
    (s, e)
}

/// Resolves a signed bit index against `len_bits` (negative = from end).
fn normalize_bit_index(idx: i64, len_bits: i64) -> i64 {
    if idx < 0 {
        (len_bits + idx).max(0)
    } else {
        idx
    }
}

/// Returns the sub-slice of `data` described by `range`.
///
/// Handles both byte and bit-unit ranges. For bit-unit ranges, the slice is
/// rounded to the containing bytes (bit searches happen inside the caller).
fn bit_range_slice(data: &[u8], range: BitRange) -> &[u8] {
    match range.unit {
        BitRangeUnit::Byte => {
            let (s, e) = resolve_byte_range(range.start, range.end, data.len());
            if s >= data.len() {
                return &[];
            }
            let end = e.min(data.len() - 1);
            if s > end {
                &[]
            } else {
                &data[s..=end]
            }
        }
        BitRangeUnit::Bit => {
            // for BITCOUNT with BIT range, convert to byte boundaries (inclusive)
            let len_bits = data.len() as i64 * 8;
            let start_bit = normalize_bit_index(range.start, len_bits).min(len_bits);
            let end_bit = normalize_bit_index(range.end, len_bits).min(len_bits - 1);
            if start_bit > end_bit || data.is_empty() {
                return &[];
            }
            // return the byte slice containing all bits in [start_bit, end_bit]
            // (individual bit masking happens in the caller for bitcount)
            let start_byte = (start_bit / 8) as usize;
            let end_byte = (end_bit / 8) as usize;
            &data[start_byte..=end_byte.min(data.len() - 1)]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- getbit ---

    #[test]
    fn getbit_missing_key_returns_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.getbit("nope", 0).unwrap(), 0);
        assert_eq!(ks.getbit("nope", 100).unwrap(), 0);
    }

    #[test]
    fn getbit_reads_msb_first() {
        let mut ks = Keyspace::new();
        // byte 0xFF: all bits set
        ks.set("k".into(), Bytes::from(vec![0xFF]), None, false, false);
        for offset in 0..8 {
            assert_eq!(ks.getbit("k", offset).unwrap(), 1, "offset {offset}");
        }
        // byte 0x00: no bits set
        ks.set("k".into(), Bytes::from(vec![0x00]), None, false, false);
        for offset in 0..8 {
            assert_eq!(ks.getbit("k", offset).unwrap(), 0, "offset {offset}");
        }
    }

    #[test]
    fn getbit_big_endian_ordering() {
        let mut ks = Keyspace::new();
        // 0x80 = 0b10000000: only bit 0 (MSB) is set
        ks.set("k".into(), Bytes::from(vec![0x80]), None, false, false);
        assert_eq!(ks.getbit("k", 0).unwrap(), 1);
        assert_eq!(ks.getbit("k", 1).unwrap(), 0);
        // 0x01 = 0b00000001: only bit 7 (LSB) is set
        ks.set("k".into(), Bytes::from(vec![0x01]), None, false, false);
        assert_eq!(ks.getbit("k", 7).unwrap(), 1);
        assert_eq!(ks.getbit("k", 0).unwrap(), 0);
    }

    #[test]
    fn getbit_beyond_string_returns_zero() {
        let mut ks = Keyspace::new();
        ks.set("k".into(), Bytes::from(vec![0xFF]), None, false, false);
        // bit 8 is byte 1, which doesn't exist
        assert_eq!(ks.getbit("k", 8).unwrap(), 0);
    }

    #[test]
    fn getbit_wrong_type() {
        let mut ks = Keyspace::new();
        ks.lpush("list", &[Bytes::from("a")]).unwrap();
        assert!(ks.getbit("list", 0).is_err());
    }

    // --- setbit ---

    #[test]
    fn setbit_returns_old_bit() {
        let mut ks = Keyspace::new();
        // new key: old bit is 0
        assert_eq!(ks.setbit("k", 7, 1).unwrap(), 0);
        // now bit 7 is set, returns 1
        assert_eq!(ks.setbit("k", 7, 1).unwrap(), 1);
        // clear it: returns 1
        assert_eq!(ks.setbit("k", 7, 0).unwrap(), 1);
        // now it's 0 again
        assert_eq!(ks.setbit("k", 7, 0).unwrap(), 0);
    }

    #[test]
    fn setbit_roundtrip_with_getbit() {
        let mut ks = Keyspace::new();
        ks.setbit("k", 10, 1).unwrap();
        assert_eq!(ks.getbit("k", 10).unwrap(), 1);
        assert_eq!(ks.getbit("k", 0).unwrap(), 0);
    }

    #[test]
    fn setbit_extends_string() {
        let mut ks = Keyspace::new();
        // setting bit 15 requires 2 bytes
        ks.setbit("k", 15, 1).unwrap();
        let val = match ks.get("k").unwrap() {
            Some(Value::String(b)) => b,
            other => panic!("expected String, got {other:?}"),
        };
        assert_eq!(val.len(), 2);
        assert_eq!(ks.getbit("k", 15).unwrap(), 1);
    }

    #[test]
    fn setbit_preserves_ttl() {
        let mut ks = Keyspace::new();
        ks.set(
            "k".into(),
            Bytes::from(vec![0u8]),
            Some(Duration::from_secs(60)),
            false,
            false,
        );
        ks.setbit("k", 0, 1).unwrap();
        assert!(matches!(ks.ttl("k"), TtlResult::Seconds(_)));
    }

    #[test]
    fn setbit_wrong_type() {
        let mut ks = Keyspace::new();
        ks.lpush("list", &[Bytes::from("a")]).unwrap();
        assert!(ks.setbit("list", 0, 1).is_err());
    }

    // --- bitcount ---

    #[test]
    fn bitcount_missing_key_returns_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.bitcount("nope", None).unwrap(), 0);
    }

    #[test]
    fn bitcount_full_string() {
        let mut ks = Keyspace::new();
        // 0xFF has 8 set bits, 0x0F has 4
        ks.set(
            "k".into(),
            Bytes::from(vec![0xFF, 0x0F]),
            None,
            false,
            false,
        );
        assert_eq!(ks.bitcount("k", None).unwrap(), 12);
    }

    #[test]
    fn bitcount_byte_range() {
        let mut ks = Keyspace::new();
        ks.set(
            "k".into(),
            Bytes::from(vec![0xFF, 0x00, 0xFF]),
            None,
            false,
            false,
        );
        // only byte 0
        assert_eq!(
            ks.bitcount(
                "k",
                Some(BitRange {
                    start: 0,
                    end: 0,
                    unit: BitRangeUnit::Byte
                })
            )
            .unwrap(),
            8
        );
        // bytes 0 and 1
        assert_eq!(
            ks.bitcount(
                "k",
                Some(BitRange {
                    start: 0,
                    end: 1,
                    unit: BitRangeUnit::Byte
                })
            )
            .unwrap(),
            8
        );
    }

    #[test]
    fn bitcount_bit_range() {
        let mut ks = Keyspace::new();
        // 0xFF: bits 0-7 all set
        ks.set("k".into(), Bytes::from(vec![0xFF]), None, false, false);
        assert_eq!(
            ks.bitcount(
                "k",
                Some(BitRange {
                    start: 0,
                    end: 7,
                    unit: BitRangeUnit::Bit
                })
            )
            .unwrap(),
            8
        );
        assert_eq!(
            ks.bitcount(
                "k",
                Some(BitRange {
                    start: 0,
                    end: 3,
                    unit: BitRangeUnit::Bit
                })
            )
            .unwrap(),
            4
        );
    }

    #[test]
    fn bitcount_wrong_type() {
        let mut ks = Keyspace::new();
        ks.lpush("list", &[Bytes::from("a")]).unwrap();
        assert!(ks.bitcount("list", None).is_err());
    }

    // --- bitpos ---

    #[test]
    fn bitpos_missing_key_bit1_returns_minus_one() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.bitpos("nope", 1, None).unwrap(), -1);
    }

    #[test]
    fn bitpos_missing_key_bit0_returns_zero() {
        let mut ks = Keyspace::new();
        assert_eq!(ks.bitpos("nope", 0, None).unwrap(), 0);
    }

    #[test]
    fn bitpos_find_first_set_bit() {
        let mut ks = Keyspace::new();
        // 0x00 0x01: first set bit is at position 15 (LSB of byte 1)
        ks.set(
            "k".into(),
            Bytes::from(vec![0x00, 0x01]),
            None,
            false,
            false,
        );
        assert_eq!(ks.bitpos("k", 1, None).unwrap(), 15);
    }

    #[test]
    fn bitpos_find_first_clear_bit_in_all_ones() {
        let mut ks = Keyspace::new();
        // all bytes 0xFF: first clear bit is at position 16 (past end)
        ks.set(
            "k".into(),
            Bytes::from(vec![0xFF, 0xFF]),
            None,
            false,
            false,
        );
        assert_eq!(ks.bitpos("k", 0, None).unwrap(), 16);
    }

    #[test]
    fn bitpos_wrong_type() {
        let mut ks = Keyspace::new();
        ks.lpush("list", &[Bytes::from("a")]).unwrap();
        assert!(ks.bitpos("list", 1, None).is_err());
    }

    // --- bitop ---

    #[test]
    fn bitop_and() {
        let mut ks = Keyspace::new();
        ks.set(
            "a".into(),
            Bytes::from(vec![0xFF, 0x0F]),
            None,
            false,
            false,
        );
        ks.set(
            "b".into(),
            Bytes::from(vec![0x0F, 0xFF]),
            None,
            false,
            false,
        );
        let len = ks
            .bitop(BitOpKind::And, "dest".into(), &["a".into(), "b".into()])
            .unwrap();
        assert_eq!(len, 2);
        let val = match ks.get("dest").unwrap() {
            Some(Value::String(b)) => b,
            other => panic!("expected String, got {other:?}"),
        };
        assert_eq!(val[0], 0x0F);
        assert_eq!(val[1], 0x0F);
    }

    #[test]
    fn bitop_or() {
        let mut ks = Keyspace::new();
        ks.set("a".into(), Bytes::from(vec![0xF0]), None, false, false);
        ks.set("b".into(), Bytes::from(vec![0x0F]), None, false, false);
        ks.bitop(BitOpKind::Or, "dest".into(), &["a".into(), "b".into()])
            .unwrap();
        let val = match ks.get("dest").unwrap() {
            Some(Value::String(b)) => b,
            other => panic!("expected String, got {other:?}"),
        };
        assert_eq!(val[0], 0xFF);
    }

    #[test]
    fn bitop_xor() {
        let mut ks = Keyspace::new();
        ks.set("a".into(), Bytes::from(vec![0xFF]), None, false, false);
        ks.set("b".into(), Bytes::from(vec![0xFF]), None, false, false);
        ks.bitop(BitOpKind::Xor, "dest".into(), &["a".into(), "b".into()])
            .unwrap();
        let val = match ks.get("dest").unwrap() {
            Some(Value::String(b)) => b,
            other => panic!("expected String, got {other:?}"),
        };
        assert_eq!(val[0], 0x00);
    }

    #[test]
    fn bitop_not() {
        let mut ks = Keyspace::new();
        ks.set(
            "src".into(),
            Bytes::from(vec![0xF0, 0x0F]),
            None,
            false,
            false,
        );
        let len = ks
            .bitop(BitOpKind::Not, "dest".into(), &["src".into()])
            .unwrap();
        assert_eq!(len, 2);
        let val = match ks.get("dest").unwrap() {
            Some(Value::String(b)) => b,
            other => panic!("expected String, got {other:?}"),
        };
        assert_eq!(val[0], 0x0F);
        assert_eq!(val[1], 0xF0);
    }

    #[test]
    fn bitop_wrong_type() {
        let mut ks = Keyspace::new();
        ks.lpush("list", &[Bytes::from("a")]).unwrap();
        assert!(ks
            .bitop(BitOpKind::And, "dest".into(), &["list".into()])
            .is_err());
    }

    #[test]
    fn bitop_extends_to_longest_source() {
        let mut ks = Keyspace::new();
        ks.set(
            "a".into(),
            Bytes::from(vec![0xFF, 0xFF, 0xFF]),
            None,
            false,
            false,
        );
        ks.set("b".into(), Bytes::from(vec![0xFF]), None, false, false);
        let len = ks
            .bitop(BitOpKind::Or, "dest".into(), &["a".into(), "b".into()])
            .unwrap();
        assert_eq!(len, 3);
    }
}
