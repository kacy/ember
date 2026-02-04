//! Point-in-time snapshot files.
//!
//! Each shard writes its own snapshot (`shard-{id}.snap`). The format
//! stores all live entries in a single pass. Writes go to a `.tmp`
//! file first and are atomically renamed on completion — this ensures
//! a partial/crashed snapshot never corrupts the existing `.snap` file.
//!
//! File layout:
//! ```text
//! [ESNP magic: 4B][version: 1B][shard_id: 2B][entry_count: 4B]
//! [entries...]
//! [footer_crc32: 4B]
//! ```
//!
//! Each entry:
//! ```text
//! [key_len: 4B][key][value_len: 4B][value][expire_ms: 8B]
//! ```
//! `expire_ms` is the remaining TTL in milliseconds, or -1 for no expiry.

use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::format::{self, FormatError};

/// Type tags for snapshot entries.
const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SORTED_SET: u8 = 2;

/// The value stored in a snapshot entry.
#[derive(Debug, Clone, PartialEq)]
pub enum SnapValue {
    /// A string value.
    String(Bytes),
    /// A list of values.
    List(VecDeque<Bytes>),
    /// A sorted set: vec of (score, member) pairs.
    SortedSet(Vec<(f64, String)>),
}

/// A single entry in a snapshot file.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapEntry {
    pub key: String,
    pub value: SnapValue,
    /// Remaining TTL in milliseconds, or -1 for no expiration.
    pub expire_ms: i64,
}

/// Writes a complete snapshot to disk.
///
/// Entries are written to a temporary file first, then atomically
/// renamed to the final path. The caller provides an iterator over
/// the entries to write.
pub struct SnapshotWriter {
    final_path: PathBuf,
    tmp_path: PathBuf,
    writer: BufWriter<File>,
    /// Running CRC over all entry bytes for the footer checksum.
    hasher: crc32fast::Hasher,
    count: u32,
}

impl SnapshotWriter {
    /// Creates a new snapshot writer. The file won't appear at `path`
    /// until [`finish`] is called successfully.
    pub fn create(path: impl Into<PathBuf>, shard_id: u16) -> Result<Self, FormatError> {
        let final_path = path.into();
        let tmp_path = final_path.with_extension("snap.tmp");

        let file = File::create(&tmp_path)?;
        let mut writer = BufWriter::new(file);

        // write header: magic + version + shard_id + placeholder entry count
        format::write_header(&mut writer, format::SNAP_MAGIC)?;
        format::write_u16(&mut writer, shard_id)?;
        // entry count — we'll seek back and update, or just write it now
        // and track. since we're streaming, write 0 and update after.
        format::write_u32(&mut writer, 0)?;

        Ok(Self {
            final_path,
            tmp_path,
            writer,
            hasher: crc32fast::Hasher::new(),
            count: 0,
        })
    }

    /// Writes a single entry to the snapshot.
    pub fn write_entry(&mut self, entry: &SnapEntry) -> Result<(), FormatError> {
        let mut buf = Vec::new();
        format::write_bytes(&mut buf, entry.key.as_bytes())?;
        match &entry.value {
            SnapValue::String(data) => {
                format::write_u8(&mut buf, TYPE_STRING)?;
                format::write_bytes(&mut buf, data)?;
            }
            SnapValue::List(deque) => {
                format::write_u8(&mut buf, TYPE_LIST)?;
                format::write_u32(&mut buf, deque.len() as u32)?;
                for item in deque {
                    format::write_bytes(&mut buf, item)?;
                }
            }
            SnapValue::SortedSet(members) => {
                format::write_u8(&mut buf, TYPE_SORTED_SET)?;
                format::write_u32(&mut buf, members.len() as u32)?;
                for (score, member) in members {
                    format::write_f64(&mut buf, *score)?;
                    format::write_bytes(&mut buf, member.as_bytes())?;
                }
            }
        }
        format::write_i64(&mut buf, entry.expire_ms)?;

        self.hasher.update(&buf);
        self.writer.write_all(&buf)?;
        self.count += 1;
        Ok(())
    }

    /// Finalizes the snapshot: writes the footer CRC, flushes, and
    /// atomically renames the temp file to the final path.
    pub fn finish(mut self) -> Result<(), FormatError> {
        // write footer CRC
        let checksum = self.hasher.finalize();
        format::write_u32(&mut self.writer, checksum)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        // now rewrite the header with the correct entry count.
        // reopen the tmp file for random access write.
        drop(self.writer);
        {
            use std::io::{Seek, SeekFrom};
            let mut file = fs::OpenOptions::new().write(true).open(&self.tmp_path)?;
            // header: 4 (magic) + 1 (version) + 2 (shard_id) = 7 bytes
            file.seek(SeekFrom::Start(7))?;
            format::write_u32(&mut file, self.count)?;
            file.sync_all()?;
        }

        // atomic rename
        fs::rename(&self.tmp_path, &self.final_path)?;
        Ok(())
    }
}

/// Reads entries from a snapshot file.
pub struct SnapshotReader {
    reader: BufReader<File>,
    pub shard_id: u16,
    pub entry_count: u32,
    read_so_far: u32,
    hasher: crc32fast::Hasher,
    /// Format version — v1 has no type tags, v2 has type-tagged entries.
    version: u8,
}

impl SnapshotReader {
    /// Opens a snapshot file and reads the header.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FormatError> {
        let file = File::open(path.as_ref())?;
        let mut reader = BufReader::new(file);

        let version = format::read_header(&mut reader, format::SNAP_MAGIC)?;
        let shard_id = format::read_u16(&mut reader)?;
        let entry_count = format::read_u32(&mut reader)?;

        Ok(Self {
            reader,
            shard_id,
            entry_count,
            read_so_far: 0,
            hasher: crc32fast::Hasher::new(),
            version,
        })
    }

    /// Reads the next entry. Returns `None` when all entries have been read.
    pub fn read_entry(&mut self) -> Result<Option<SnapEntry>, FormatError> {
        if self.read_so_far >= self.entry_count {
            return Ok(None);
        }

        let mut buf = Vec::new();

        let key_bytes = format::read_bytes(&mut self.reader)?;
        format::write_bytes(&mut buf, &key_bytes).expect("vec write");

        let value = if self.version == 1 {
            // v1: no type tag, value is always a string
            let value_bytes = format::read_bytes(&mut self.reader)?;
            format::write_bytes(&mut buf, &value_bytes).expect("vec write");
            SnapValue::String(Bytes::from(value_bytes))
        } else {
            // v2+: type-tagged values
            let type_tag = format::read_u8(&mut self.reader)?;
            format::write_u8(&mut buf, type_tag).expect("vec write");
            match type_tag {
                TYPE_STRING => {
                    let value_bytes = format::read_bytes(&mut self.reader)?;
                    format::write_bytes(&mut buf, &value_bytes).expect("vec write");
                    SnapValue::String(Bytes::from(value_bytes))
                }
                TYPE_LIST => {
                    let count = format::read_u32(&mut self.reader)?;
                    format::write_u32(&mut buf, count).expect("vec write");
                    let mut deque = VecDeque::with_capacity(count as usize);
                    for _ in 0..count {
                        let item = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &item).expect("vec write");
                        deque.push_back(Bytes::from(item));
                    }
                    SnapValue::List(deque)
                }
                TYPE_SORTED_SET => {
                    let count = format::read_u32(&mut self.reader)?;
                    format::write_u32(&mut buf, count).expect("vec write");
                    let mut members = Vec::with_capacity(count as usize);
                    for _ in 0..count {
                        let score = format::read_f64(&mut self.reader)?;
                        format::write_f64(&mut buf, score).expect("vec write");
                        let member_bytes = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &member_bytes).expect("vec write");
                        let member = String::from_utf8(member_bytes).map_err(|_| {
                            FormatError::Io(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "member is not valid utf-8",
                            ))
                        })?;
                        members.push((score, member));
                    }
                    SnapValue::SortedSet(members)
                }
                _ => {
                    return Err(FormatError::UnknownTag(type_tag));
                }
            }
        };

        let expire_ms = format::read_i64(&mut self.reader)?;
        format::write_i64(&mut buf, expire_ms).expect("vec write");
        self.hasher.update(&buf);

        let key = String::from_utf8(key_bytes).map_err(|_| {
            FormatError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "key is not valid utf-8",
            ))
        })?;

        self.read_so_far += 1;
        Ok(Some(SnapEntry {
            key,
            value,
            expire_ms,
        }))
    }

    /// Verifies the footer CRC32 after all entries have been read.
    /// Must be called after reading all entries.
    pub fn verify_footer(self) -> Result<(), FormatError> {
        let expected = self.hasher.finalize();
        let mut reader = self.reader;
        let stored = format::read_u32(&mut reader)?;
        format::verify_crc32_values(expected, stored)
    }
}


/// Returns the snapshot file path for a given shard in a data directory.
pub fn snapshot_path(data_dir: &Path, shard_id: u16) -> PathBuf {
    data_dir.join(format!("shard-{shard_id}.snap"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("create temp dir")
    }

    #[test]
    fn empty_snapshot_round_trip() {
        let dir = temp_dir();
        let path = dir.path().join("empty.snap");

        {
            let writer = SnapshotWriter::create(&path, 0).unwrap();
            writer.finish().unwrap();
        }

        let reader = SnapshotReader::open(&path).unwrap();
        assert_eq!(reader.shard_id, 0);
        assert_eq!(reader.entry_count, 0);
        reader.verify_footer().unwrap();
    }

    #[test]
    fn entries_round_trip() {
        let dir = temp_dir();
        let path = dir.path().join("data.snap");

        let entries = vec![
            SnapEntry {
                key: "hello".into(),
                value: SnapValue::String(Bytes::from("world")),
                expire_ms: -1,
            },
            SnapEntry {
                key: "ttl".into(),
                value: SnapValue::String(Bytes::from("expiring")),
                expire_ms: 5000,
            },
            SnapEntry {
                key: "empty".into(),
                value: SnapValue::String(Bytes::new()),
                expire_ms: -1,
            },
        ];

        {
            let mut writer = SnapshotWriter::create(&path, 7).unwrap();
            for entry in &entries {
                writer.write_entry(entry).unwrap();
            }
            writer.finish().unwrap();
        }

        let mut reader = SnapshotReader::open(&path).unwrap();
        assert_eq!(reader.shard_id, 7);
        assert_eq!(reader.entry_count, 3);

        let mut got = Vec::new();
        while let Some(entry) = reader.read_entry().unwrap() {
            got.push(entry);
        }
        assert_eq!(entries, got);
        reader.verify_footer().unwrap();
    }

    #[test]
    fn corrupt_footer_detected() {
        let dir = temp_dir();
        let path = dir.path().join("corrupt.snap");

        {
            let mut writer = SnapshotWriter::create(&path, 0).unwrap();
            writer
                .write_entry(&SnapEntry {
                    key: "k".into(),
                    value: SnapValue::String(Bytes::from("v")),
                    expire_ms: -1,
                })
                .unwrap();
            writer.finish().unwrap();
        }

        // corrupt the last byte (footer CRC)
        let mut data = fs::read(&path).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let mut reader = SnapshotReader::open(&path).unwrap();
        // reading entries should still work
        reader.read_entry().unwrap();
        // but footer verification should fail
        let err = reader.verify_footer().unwrap_err();
        assert!(matches!(err, FormatError::ChecksumMismatch { .. }));
    }

    #[test]
    fn atomic_rename_prevents_partial_snapshots() {
        let dir = temp_dir();
        let path = dir.path().join("atomic.snap");

        // write an initial snapshot
        {
            let mut writer = SnapshotWriter::create(&path, 0).unwrap();
            writer
                .write_entry(&SnapEntry {
                    key: "original".into(),
                    value: SnapValue::String(Bytes::from("data")),
                    expire_ms: -1,
                })
                .unwrap();
            writer.finish().unwrap();
        }

        // start a second snapshot but don't finish it
        {
            let mut writer = SnapshotWriter::create(&path, 0).unwrap();
            writer
                .write_entry(&SnapEntry {
                    key: "new".into(),
                    value: SnapValue::String(Bytes::from("partial")),
                    expire_ms: -1,
                })
                .unwrap();
            // drop without finish — simulates a crash
            drop(writer);
        }

        // the original snapshot should still be intact
        let mut reader = SnapshotReader::open(&path).unwrap();
        let entry = reader.read_entry().unwrap().unwrap();
        assert_eq!(entry.key, "original");
    }

    #[test]
    fn ttl_entries_preserved() {
        let dir = temp_dir();
        let path = dir.path().join("ttl.snap");

        let entry = SnapEntry {
            key: "expires".into(),
            value: SnapValue::String(Bytes::from("soon")),
            expire_ms: 42_000,
        };

        {
            let mut writer = SnapshotWriter::create(&path, 0).unwrap();
            writer.write_entry(&entry).unwrap();
            writer.finish().unwrap();
        }

        let mut reader = SnapshotReader::open(&path).unwrap();
        let got = reader.read_entry().unwrap().unwrap();
        assert_eq!(got.expire_ms, 42_000);
        reader.verify_footer().unwrap();
    }

    #[test]
    fn list_entries_round_trip() {
        let dir = temp_dir();
        let path = dir.path().join("list.snap");

        let mut deque = VecDeque::new();
        deque.push_back(Bytes::from("a"));
        deque.push_back(Bytes::from("b"));
        deque.push_back(Bytes::from("c"));

        let entries = vec![
            SnapEntry {
                key: "mylist".into(),
                value: SnapValue::List(deque),
                expire_ms: -1,
            },
            SnapEntry {
                key: "mystr".into(),
                value: SnapValue::String(Bytes::from("val")),
                expire_ms: 1000,
            },
        ];

        {
            let mut writer = SnapshotWriter::create(&path, 0).unwrap();
            for entry in &entries {
                writer.write_entry(entry).unwrap();
            }
            writer.finish().unwrap();
        }

        let mut reader = SnapshotReader::open(&path).unwrap();
        let mut got = Vec::new();
        while let Some(entry) = reader.read_entry().unwrap() {
            got.push(entry);
        }
        assert_eq!(entries, got);
        reader.verify_footer().unwrap();
    }

    #[test]
    fn sorted_set_entries_round_trip() {
        let dir = temp_dir();
        let path = dir.path().join("zset.snap");

        let entries = vec![
            SnapEntry {
                key: "board".into(),
                value: SnapValue::SortedSet(vec![
                    (100.0, "alice".into()),
                    (200.0, "bob".into()),
                    (150.0, "charlie".into()),
                ]),
                expire_ms: -1,
            },
            SnapEntry {
                key: "mystr".into(),
                value: SnapValue::String(Bytes::from("val")),
                expire_ms: 1000,
            },
        ];

        {
            let mut writer = SnapshotWriter::create(&path, 0).unwrap();
            for entry in &entries {
                writer.write_entry(entry).unwrap();
            }
            writer.finish().unwrap();
        }

        let mut reader = SnapshotReader::open(&path).unwrap();
        let mut got = Vec::new();
        while let Some(entry) = reader.read_entry().unwrap() {
            got.push(entry);
        }
        assert_eq!(entries, got);
        reader.verify_footer().unwrap();
    }

    #[test]
    fn snapshot_path_format() {
        let p = snapshot_path(Path::new("/data"), 5);
        assert_eq!(p, PathBuf::from("/data/shard-5.snap"));
    }
}
