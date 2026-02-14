//! Binary format helpers shared across AOF and snapshot files.
//!
//! Provides TLV-style encoding primitives, CRC32 checksums, and magic
//! byte constants. All multi-byte integers are stored in little-endian.

use std::io::{self, Read, Write};

use crc32fast::Hasher;
use thiserror::Error;

/// Magic bytes for the AOF file header.
pub const AOF_MAGIC: &[u8; 4] = b"EAOF";

/// Magic bytes for the snapshot file header.
pub const SNAP_MAGIC: &[u8; 4] = b"ESNP";

/// Current unencrypted format version.
///
/// v1: original format (strings only)
/// v2: type-tagged entries (string, list, sorted set, hash, set)
pub const FORMAT_VERSION: u8 = 2;

/// Format version for encrypted files.
///
/// v3: per-record AES-256-GCM encryption (requires `encryption` feature)
pub const FORMAT_VERSION_ENCRYPTED: u8 = 3;

/// Errors that can occur when reading or writing persistence formats.
#[derive(Debug, Error)]
pub enum FormatError {
    #[error("unexpected end of file")]
    UnexpectedEof,

    #[error("invalid magic bytes")]
    InvalidMagic,

    #[error("unsupported format version: {0}")]
    UnsupportedVersion(u8),

    #[error("crc32 mismatch (expected {expected:#010x}, got {actual:#010x})")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("unknown record tag: {0}")]
    UnknownTag(u8),

    #[error("invalid data: {0}")]
    InvalidData(String),

    #[error("file is encrypted but no encryption key was provided")]
    EncryptionRequired,

    #[error("decryption failed (wrong key or tampered data)")]
    DecryptionFailed,

    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

/// Computes a CRC32 checksum over a byte slice.
pub fn crc32(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finalize()
}

// ---------------------------------------------------------------------------
// write helpers
// ---------------------------------------------------------------------------

/// Writes a `u8` to the writer.
pub fn write_u8(w: &mut impl Write, val: u8) -> io::Result<()> {
    w.write_all(&[val])
}

/// Writes a `u16` in little-endian.
pub fn write_u16(w: &mut impl Write, val: u16) -> io::Result<()> {
    w.write_all(&val.to_le_bytes())
}

/// Writes a `u32` in little-endian.
pub fn write_u32(w: &mut impl Write, val: u32) -> io::Result<()> {
    w.write_all(&val.to_le_bytes())
}

/// Writes an `i64` in little-endian.
pub fn write_i64(w: &mut impl Write, val: i64) -> io::Result<()> {
    w.write_all(&val.to_le_bytes())
}

/// Writes an `f32` in little-endian.
pub fn write_f32(w: &mut impl Write, val: f32) -> io::Result<()> {
    w.write_all(&val.to_le_bytes())
}

/// Writes an `f64` in little-endian.
pub fn write_f64(w: &mut impl Write, val: f64) -> io::Result<()> {
    w.write_all(&val.to_le_bytes())
}

/// Writes a collection length as u32, returning an error if it exceeds `u32::MAX`.
pub fn write_len(w: &mut impl Write, len: usize) -> io::Result<()> {
    let len = u32::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("collection length {len} exceeds u32::MAX"),
        )
    })?;
    write_u32(w, len)
}

/// Writes a length-prefixed byte slice: `[len: u32][data]`.
///
/// Returns an error if the data length exceeds `u32::MAX`.
pub fn write_bytes(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    let len = u32::try_from(data.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("data length {} exceeds u32::MAX", data.len()),
        )
    })?;
    write_u32(w, len)?;
    w.write_all(data)
}

// ---------------------------------------------------------------------------
// read helpers
// ---------------------------------------------------------------------------

/// Reads a `u8` from the reader.
pub fn read_u8(r: &mut impl Read) -> Result<u8, FormatError> {
    let mut buf = [0u8; 1];
    read_exact(r, &mut buf)?;
    Ok(buf[0])
}

/// Reads a `u16` in little-endian.
pub fn read_u16(r: &mut impl Read) -> Result<u16, FormatError> {
    let mut buf = [0u8; 2];
    read_exact(r, &mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

/// Reads a `u32` in little-endian.
pub fn read_u32(r: &mut impl Read) -> Result<u32, FormatError> {
    let mut buf = [0u8; 4];
    read_exact(r, &mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

/// Reads an `i64` in little-endian.
pub fn read_i64(r: &mut impl Read) -> Result<i64, FormatError> {
    let mut buf = [0u8; 8];
    read_exact(r, &mut buf)?;
    Ok(i64::from_le_bytes(buf))
}

/// Reads an `f32` in little-endian.
pub fn read_f32(r: &mut impl Read) -> Result<f32, FormatError> {
    let mut buf = [0u8; 4];
    read_exact(r, &mut buf)?;
    Ok(f32::from_le_bytes(buf))
}

/// Reads an `f64` in little-endian.
pub fn read_f64(r: &mut impl Read) -> Result<f64, FormatError> {
    let mut buf = [0u8; 8];
    read_exact(r, &mut buf)?;
    Ok(f64::from_le_bytes(buf))
}

/// Maximum length we'll allocate when reading a length-prefixed field.
/// 512 MB is generous for any realistic key or value — a corrupt or
/// malicious length prefix won't cause a multi-gigabyte allocation.
pub const MAX_FIELD_LEN: usize = 512 * 1024 * 1024;

/// Reads a length-prefixed byte vector: `[len: u32][data]`.
///
/// Returns an error if the declared length exceeds [`MAX_FIELD_LEN`]
/// to prevent unbounded allocations from corrupt data.
pub fn read_bytes(r: &mut impl Read) -> Result<Vec<u8>, FormatError> {
    let len = read_u32(r)? as usize;
    if len > MAX_FIELD_LEN {
        return Err(FormatError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("field length {len} exceeds maximum of {MAX_FIELD_LEN}"),
        )));
    }
    let mut buf = vec![0u8; len];
    read_exact(r, &mut buf)?;
    Ok(buf)
}

/// Reads exactly `buf.len()` bytes, returning `UnexpectedEof` on short read.
fn read_exact(r: &mut impl Read, buf: &mut [u8]) -> Result<(), FormatError> {
    r.read_exact(buf).map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            FormatError::UnexpectedEof
        } else {
            FormatError::Io(e)
        }
    })
}

/// Writes a file header: magic bytes + version byte.
pub fn write_header(w: &mut impl Write, magic: &[u8; 4]) -> io::Result<()> {
    w.write_all(magic)?;
    write_u8(w, FORMAT_VERSION)
}

/// Writes a file header with an explicit version byte.
pub fn write_header_versioned(w: &mut impl Write, magic: &[u8; 4], version: u8) -> io::Result<()> {
    w.write_all(magic)?;
    write_u8(w, version)
}

/// The maximum format version this build can read.
///
/// When the `encryption` feature is compiled in, v3 (encrypted) files
/// are supported. Without the feature, only v1 and v2 are accepted.
#[cfg(feature = "encryption")]
const MAX_READABLE_VERSION: u8 = FORMAT_VERSION_ENCRYPTED;
#[cfg(not(feature = "encryption"))]
const MAX_READABLE_VERSION: u8 = FORMAT_VERSION;

/// Reads and validates a file header. Returns an error if magic doesn't
/// match or version is unsupported. Returns the format version.
pub fn read_header(r: &mut impl Read, expected_magic: &[u8; 4]) -> Result<u8, FormatError> {
    let mut magic = [0u8; 4];
    read_exact(r, &mut magic)?;
    if &magic != expected_magic {
        return Err(FormatError::InvalidMagic);
    }
    let version = read_u8(r)?;
    if version == 0 || version > MAX_READABLE_VERSION {
        return Err(FormatError::UnsupportedVersion(version));
    }
    Ok(version)
}

/// Verifies that `data` matches the expected CRC32 checksum.
pub fn verify_crc32(data: &[u8], expected: u32) -> Result<(), FormatError> {
    let actual = crc32(data);
    verify_crc32_values(actual, expected)
}

/// Caps pre-allocation to avoid huge allocations from corrupt count fields.
/// The loop will still iterate `count` times — this just limits the
/// up-front reservation so a bogus u32 can't exhaust memory.
pub fn capped_capacity(count: u32) -> usize {
    (count as usize).min(65_536)
}

/// Maximum element count for collections (lists, sets, hashes, sorted sets)
/// in persistence formats. Prevents corrupt count fields from causing
/// unbounded iteration during deserialization. 100M is well beyond any
/// realistic collection while catching obviously corrupt u32 values.
pub const MAX_COLLECTION_COUNT: u32 = 100_000_000;

/// Validates that a deserialized collection count is within bounds.
/// Returns `InvalidData` if the count exceeds `MAX_COLLECTION_COUNT`.
pub fn validate_collection_count(count: u32, label: &str) -> Result<(), FormatError> {
    if count > MAX_COLLECTION_COUNT {
        return Err(FormatError::InvalidData(format!(
            "{label} count {count} exceeds max {MAX_COLLECTION_COUNT}"
        )));
    }
    Ok(())
}

/// Maximum vector dimensions allowed in persistence formats.
/// Matches the protocol-layer cap. Records exceeding this are rejected
/// during deserialization to prevent OOM from corrupt files.
pub const MAX_PERSISTED_VECTOR_DIMS: u32 = 65_536;

/// Maximum element count per vector set in persistence formats.
/// Prevents corrupt count fields from causing unbounded loops.
pub const MAX_PERSISTED_VECTOR_COUNT: u32 = 10_000_000;

/// Verifies that two CRC32 values match.
pub fn verify_crc32_values(computed: u32, stored: u32) -> Result<(), FormatError> {
    if computed != stored {
        return Err(FormatError::ChecksumMismatch {
            expected: stored,
            actual: computed,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn u8_round_trip() {
        let mut buf = Vec::new();
        write_u8(&mut buf, 42).unwrap();
        assert_eq!(read_u8(&mut Cursor::new(&buf)).unwrap(), 42);
    }

    #[test]
    fn u16_round_trip() {
        let mut buf = Vec::new();
        write_u16(&mut buf, 12345).unwrap();
        assert_eq!(read_u16(&mut Cursor::new(&buf)).unwrap(), 12345);
    }

    #[test]
    fn u32_round_trip() {
        let mut buf = Vec::new();
        write_u32(&mut buf, 0xDEAD_BEEF).unwrap();
        assert_eq!(read_u32(&mut Cursor::new(&buf)).unwrap(), 0xDEAD_BEEF);
    }

    #[test]
    fn i64_round_trip() {
        let mut buf = Vec::new();
        write_i64(&mut buf, -1).unwrap();
        assert_eq!(read_i64(&mut Cursor::new(&buf)).unwrap(), -1);

        let mut buf2 = Vec::new();
        write_i64(&mut buf2, i64::MAX).unwrap();
        assert_eq!(read_i64(&mut Cursor::new(&buf2)).unwrap(), i64::MAX);
    }

    #[test]
    fn bytes_round_trip() {
        let mut buf = Vec::new();
        write_bytes(&mut buf, b"hello world").unwrap();
        assert_eq!(read_bytes(&mut Cursor::new(&buf)).unwrap(), b"hello world");
    }

    #[test]
    fn empty_bytes_round_trip() {
        let mut buf = Vec::new();
        write_bytes(&mut buf, b"").unwrap();
        assert_eq!(read_bytes(&mut Cursor::new(&buf)).unwrap(), b"");
    }

    #[test]
    fn header_round_trip() {
        let mut buf = Vec::new();
        write_header(&mut buf, AOF_MAGIC).unwrap();
        read_header(&mut Cursor::new(&buf), AOF_MAGIC).unwrap();
    }

    #[test]
    fn header_wrong_magic() {
        let mut buf = Vec::new();
        write_header(&mut buf, AOF_MAGIC).unwrap();
        let err = read_header(&mut Cursor::new(&buf), SNAP_MAGIC).unwrap_err();
        assert!(matches!(err, FormatError::InvalidMagic));
    }

    #[test]
    fn header_wrong_version() {
        let buf = vec![b'E', b'A', b'O', b'F', 99];
        let err = read_header(&mut Cursor::new(&buf), AOF_MAGIC).unwrap_err();
        assert!(matches!(err, FormatError::UnsupportedVersion(99)));
    }

    #[test]
    fn crc32_deterministic() {
        let a = crc32(b"test data");
        let b = crc32(b"test data");
        assert_eq!(a, b);
        assert_ne!(a, crc32(b"different data"));
    }

    #[test]
    fn verify_crc32_pass() {
        let data = b"check me";
        let checksum = crc32(data);
        verify_crc32(data, checksum).unwrap();
    }

    #[test]
    fn verify_crc32_fail() {
        let err = verify_crc32(b"data", 0xBAD).unwrap_err();
        assert!(matches!(err, FormatError::ChecksumMismatch { .. }));
    }

    #[test]
    fn truncated_input_returns_eof() {
        let buf = [0u8; 2]; // too short for u32
        let err = read_u32(&mut Cursor::new(&buf)).unwrap_err();
        assert!(matches!(err, FormatError::UnexpectedEof));
    }

    #[test]
    fn empty_input_returns_eof() {
        let err = read_u8(&mut Cursor::new(&[])).unwrap_err();
        assert!(matches!(err, FormatError::UnexpectedEof));
    }

    #[test]
    fn read_bytes_rejects_oversized_length() {
        // encode a length that exceeds MAX_FIELD_LEN
        let bogus_len = (MAX_FIELD_LEN as u32) + 1;
        let mut buf = Vec::new();
        write_u32(&mut buf, bogus_len).unwrap();
        let err = read_bytes(&mut Cursor::new(&buf)).unwrap_err();
        assert!(matches!(err, FormatError::Io(_)));
    }
}
