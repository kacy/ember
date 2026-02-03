//! Append-only file for recording mutations.
//!
//! Each shard writes its own AOF file (`shard-{id}.aof`). Records are
//! written after successful mutations. The binary format uses a simple
//! tag + payload + CRC32 structure for each record.
//!
//! File layout:
//! ```text
//! [EAOF magic: 4B][version: 1B]
//! [record]*
//! ```
//!
//! Record layout:
//! ```text
//! [tag: 1B][payload...][crc32: 4B]
//! ```
//! The CRC32 covers the tag + payload bytes.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::format::{self, FormatError};

/// Record tags for the AOF format.
const TAG_SET: u8 = 1;
const TAG_DEL: u8 = 2;
const TAG_EXPIRE: u8 = 3;

/// A single mutation record stored in the AOF.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AofRecord {
    /// SET key value [expire_ms]. expire_ms is -1 for no expiration.
    Set {
        key: String,
        value: Bytes,
        expire_ms: i64,
    },
    /// DEL key.
    Del { key: String },
    /// EXPIRE key seconds.
    Expire { key: String, seconds: u64 },
}

impl AofRecord {
    /// Serializes this record into a byte vector (tag + payload, no CRC).
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            AofRecord::Set {
                key,
                value,
                expire_ms,
            } => {
                format::write_u8(&mut buf, TAG_SET).expect("vec write");
                format::write_bytes(&mut buf, key.as_bytes()).expect("vec write");
                format::write_bytes(&mut buf, value).expect("vec write");
                format::write_i64(&mut buf, *expire_ms).expect("vec write");
            }
            AofRecord::Del { key } => {
                format::write_u8(&mut buf, TAG_DEL).expect("vec write");
                format::write_bytes(&mut buf, key.as_bytes()).expect("vec write");
            }
            AofRecord::Expire { key, seconds } => {
                format::write_u8(&mut buf, TAG_EXPIRE).expect("vec write");
                format::write_bytes(&mut buf, key.as_bytes()).expect("vec write");
                format::write_i64(&mut buf, *seconds as i64).expect("vec write");
            }
        }
        buf
    }

    /// Deserializes a record from a byte slice (tag + payload, no CRC).
    fn from_bytes(data: &[u8]) -> Result<Self, FormatError> {
        let mut cursor = io::Cursor::new(data);
        let tag = format::read_u8(&mut cursor)?;
        match tag {
            TAG_SET => {
                let key_bytes = format::read_bytes(&mut cursor)?;
                let key = String::from_utf8(key_bytes).map_err(|_| {
                    FormatError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "key is not valid utf-8",
                    ))
                })?;
                let value = format::read_bytes(&mut cursor)?;
                let expire_ms = format::read_i64(&mut cursor)?;
                Ok(AofRecord::Set {
                    key,
                    value: Bytes::from(value),
                    expire_ms,
                })
            }
            TAG_DEL => {
                let key_bytes = format::read_bytes(&mut cursor)?;
                let key = String::from_utf8(key_bytes).map_err(|_| {
                    FormatError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "key is not valid utf-8",
                    ))
                })?;
                Ok(AofRecord::Del { key })
            }
            TAG_EXPIRE => {
                let key_bytes = format::read_bytes(&mut cursor)?;
                let key = String::from_utf8(key_bytes).map_err(|_| {
                    FormatError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "key is not valid utf-8",
                    ))
                })?;
                let seconds = format::read_i64(&mut cursor)? as u64;
                Ok(AofRecord::Expire { key, seconds })
            }
            _ => Err(FormatError::UnknownTag(tag)),
        }
    }
}

/// Configurable fsync policy for the AOF writer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync after every write. safest, slowest.
    Always,
    /// fsync once per second. the shard tick drives this.
    #[default]
    EverySec,
    /// let the OS decide when to flush. fastest, least durable.
    No,
}

/// Buffered writer for appending AOF records to a file.
pub struct AofWriter {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl AofWriter {
    /// Opens (or creates) an AOF file. If the file is new, writes the header.
    /// If the file already exists, appends to it.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, FormatError> {
        let path = path.into();
        let exists = path.exists() && fs::metadata(&path).map(|m| m.len() > 0).unwrap_or(false);

        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let mut writer = BufWriter::new(file);

        if !exists {
            format::write_header(&mut writer, format::AOF_MAGIC)?;
            writer.flush()?;
        }

        Ok(Self { writer, path })
    }

    /// Appends a record to the AOF. Writes tag+payload+crc32.
    pub fn write_record(&mut self, record: &AofRecord) -> Result<(), FormatError> {
        let payload = record.to_bytes();
        let checksum = format::crc32(&payload);
        self.writer.write_all(&payload)?;
        format::write_u32(&mut self.writer, checksum)?;
        Ok(())
    }

    /// Flushes the internal buffer to the OS.
    pub fn flush(&mut self) -> Result<(), FormatError> {
        self.writer.flush()?;
        Ok(())
    }

    /// Flushes and fsyncs the file to disk.
    pub fn sync(&mut self) -> Result<(), FormatError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Returns the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Truncates the AOF file back to just the header.
    /// Used after a successful snapshot to reset the log.
    pub fn truncate(&mut self) -> Result<(), FormatError> {
        // flush and drop the old writer
        self.writer.flush()?;

        // reopen the file with truncation, write fresh header
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        let mut writer = BufWriter::new(file);
        format::write_header(&mut writer, format::AOF_MAGIC)?;
        writer.flush()?;
        self.writer = writer;
        Ok(())
    }
}

/// Reader for iterating over AOF records.
#[derive(Debug)]
pub struct AofReader {
    reader: BufReader<File>,
}

impl AofReader {
    /// Opens an AOF file and validates the header.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FormatError> {
        let file = File::open(path.as_ref())?;
        let mut reader = BufReader::new(file);
        format::read_header(&mut reader, format::AOF_MAGIC)?;
        Ok(Self { reader })
    }

    /// Reads the next record from the AOF.
    ///
    /// Returns `Ok(None)` at end-of-file. On a truncated record (the
    /// server crashed mid-write), returns `Ok(None)` rather than an error
    /// — this is the expected recovery behavior.
    pub fn read_record(&mut self) -> Result<Option<AofRecord>, FormatError> {
        // peek for EOF — try reading the tag byte
        let tag = match format::read_u8(&mut self.reader) {
            Ok(t) => t,
            Err(FormatError::UnexpectedEof) => return Ok(None),
            Err(e) => return Err(e),
        };

        // compute the payload size from the tag, then read the raw bytes
        // in one shot so CRC verification operates on the exact bytes
        // that were written — no re-serialization needed.
        let result = self.read_raw_record(tag);
        match result {
            Ok((raw, stored_crc)) => {
                format::verify_crc32(&raw, stored_crc)?;
                AofRecord::from_bytes(&raw).map(Some)
            }
            // truncated record — treat as end of usable data
            Err(FormatError::UnexpectedEof) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Reads the complete raw record bytes (tag + payload) and trailing CRC.
    ///
    /// Reads each length-prefixed field by first reading its u32 length,
    /// then the field body, accumulating everything into a contiguous buffer.
    /// This ensures CRC verification works on the exact bytes on disk.
    fn read_raw_record(&mut self, tag: u8) -> Result<(Vec<u8>, u32), FormatError> {
        let mut raw = Vec::new();
        raw.push(tag);

        let field_count = match tag {
            TAG_SET => 2,    // key + value (length-prefixed), then i64
            TAG_DEL => 1,    // key (length-prefixed)
            TAG_EXPIRE => 1, // key (length-prefixed), then i64
            _ => return Err(FormatError::UnknownTag(tag)),
        };

        // read length-prefixed fields
        for _ in 0..field_count {
            self.read_length_prefixed_into(&mut raw)?;
        }

        // read trailing fixed-size fields
        match tag {
            TAG_SET => {
                // expire_ms: i64
                let mut buf = [0u8; 8];
                self.reader.read_exact(&mut buf).map_err(|e| {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        FormatError::UnexpectedEof
                    } else {
                        FormatError::Io(e)
                    }
                })?;
                raw.extend_from_slice(&buf);
            }
            TAG_EXPIRE => {
                // seconds: i64
                let mut buf = [0u8; 8];
                self.reader.read_exact(&mut buf).map_err(|e| {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        FormatError::UnexpectedEof
                    } else {
                        FormatError::Io(e)
                    }
                })?;
                raw.extend_from_slice(&buf);
            }
            _ => {}
        }

        let stored_crc = format::read_u32(&mut self.reader)?;
        Ok((raw, stored_crc))
    }

    /// Reads a length-prefixed field (u32 len + body) and appends the raw
    /// bytes (including the length prefix) to `out`.
    fn read_length_prefixed_into(&mut self, out: &mut Vec<u8>) -> Result<(), FormatError> {
        let mut len_buf = [0u8; 4];
        self.reader.read_exact(&mut len_buf).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                FormatError::UnexpectedEof
            } else {
                FormatError::Io(e)
            }
        })?;
        out.extend_from_slice(&len_buf);

        let len = u32::from_le_bytes(len_buf) as usize;
        if len > format::MAX_FIELD_LEN {
            return Err(FormatError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("field length {len} exceeds maximum"),
            )));
        }

        let start = out.len();
        out.resize(start + len, 0);
        self.reader.read_exact(&mut out[start..]).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                FormatError::UnexpectedEof
            } else {
                FormatError::Io(e)
            }
        })?;
        Ok(())
    }
}

/// Returns the AOF file path for a given shard in a data directory.
pub fn aof_path(data_dir: &Path, shard_id: u16) -> PathBuf {
    data_dir.join(format!("shard-{shard_id}.aof"))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("create temp dir")
    }

    #[test]
    fn record_round_trip_set() {
        let rec = AofRecord::Set {
            key: "hello".into(),
            value: Bytes::from("world"),
            expire_ms: 5000,
        };
        let bytes = rec.to_bytes();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_del() {
        let rec = AofRecord::Del { key: "gone".into() };
        let bytes = rec.to_bytes();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_expire() {
        let rec = AofRecord::Expire {
            key: "ttl".into(),
            seconds: 300,
        };
        let bytes = rec.to_bytes();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn set_with_no_expiry() {
        let rec = AofRecord::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire_ms: -1,
        };
        let bytes = rec.to_bytes();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn writer_reader_round_trip() {
        let dir = temp_dir();
        let path = dir.path().join("test.aof");

        let records = vec![
            AofRecord::Set {
                key: "a".into(),
                value: Bytes::from("1"),
                expire_ms: -1,
            },
            AofRecord::Set {
                key: "b".into(),
                value: Bytes::from("2"),
                expire_ms: 10_000,
            },
            AofRecord::Del { key: "a".into() },
            AofRecord::Expire {
                key: "b".into(),
                seconds: 60,
            },
        ];

        // write
        {
            let mut writer = AofWriter::open(&path).unwrap();
            for rec in &records {
                writer.write_record(rec).unwrap();
            }
            writer.sync().unwrap();
        }

        // read back
        let mut reader = AofReader::open(&path).unwrap();
        let mut got = Vec::new();
        while let Some(rec) = reader.read_record().unwrap() {
            got.push(rec);
        }
        assert_eq!(records, got);
    }

    #[test]
    fn empty_aof_returns_no_records() {
        let dir = temp_dir();
        let path = dir.path().join("empty.aof");

        // just write the header
        {
            let _writer = AofWriter::open(&path).unwrap();
        }

        let mut reader = AofReader::open(&path).unwrap();
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn truncated_record_treated_as_eof() {
        let dir = temp_dir();
        let path = dir.path().join("trunc.aof");

        // write one good record, then append garbage (simulating a crash)
        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "ok".into(),
                    value: Bytes::from("good"),
                    expire_ms: -1,
                })
                .unwrap();
            writer.flush().unwrap();
        }

        // append a partial tag with no payload
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[TAG_SET]).unwrap();
        }

        let mut reader = AofReader::open(&path).unwrap();
        // first record should be fine
        let rec = reader.read_record().unwrap().unwrap();
        assert!(matches!(rec, AofRecord::Set { .. }));
        // second should be None (truncated)
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn corrupt_crc_detected() {
        let dir = temp_dir();
        let path = dir.path().join("corrupt.aof");

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "k".into(),
                    value: Bytes::from("v"),
                    expire_ms: -1,
                })
                .unwrap();
            writer.flush().unwrap();
        }

        // corrupt the last byte (part of the CRC)
        let mut data = fs::read(&path).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let mut reader = AofReader::open(&path).unwrap();
        let err = reader.read_record().unwrap_err();
        assert!(matches!(err, FormatError::ChecksumMismatch { .. }));
    }

    #[test]
    fn missing_magic_is_error() {
        let dir = temp_dir();
        let path = dir.path().join("bad.aof");
        fs::write(&path, b"NOT_AOF_DATA").unwrap();

        let err = AofReader::open(&path).unwrap_err();
        assert!(matches!(err, FormatError::InvalidMagic));
    }

    #[test]
    fn truncate_resets_aof() {
        let dir = temp_dir();
        let path = dir.path().join("reset.aof");

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "old".into(),
                    value: Bytes::from("data"),
                    expire_ms: -1,
                })
                .unwrap();
            writer.truncate().unwrap();

            // write a new record after truncation
            writer
                .write_record(&AofRecord::Set {
                    key: "new".into(),
                    value: Bytes::from("fresh"),
                    expire_ms: -1,
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let mut reader = AofReader::open(&path).unwrap();
        let rec = reader.read_record().unwrap().unwrap();
        match rec {
            AofRecord::Set { key, .. } => assert_eq!(key, "new"),
            other => panic!("expected Set, got {other:?}"),
        }
        // only one record after truncation
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn aof_path_format() {
        let p = aof_path(Path::new("/data"), 3);
        assert_eq!(p, PathBuf::from("/data/shard-3.aof"));
    }
}
