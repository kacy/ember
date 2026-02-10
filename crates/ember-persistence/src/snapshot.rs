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
//! Each entry (v2, type-tagged):
//! ```text
//! [key_len: 4B][key][type_tag: 1B][type-specific payload][expire_ms: 8B]
//! ```
//!
//! Type tags: 0=string, 1=list, 2=sorted set.
//! `expire_ms` is the remaining TTL in milliseconds, or -1 for no expiry.
//! v1 entries (no type tag) are still readable for backward compatibility.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::format::{self, FormatError};

/// Type tags for snapshot entries.
const TYPE_STRING: u8 = 0;
const TYPE_LIST: u8 = 1;
const TYPE_SORTED_SET: u8 = 2;
const TYPE_HASH: u8 = 3;
const TYPE_SET: u8 = 4;

/// The value stored in a snapshot entry.
#[derive(Debug, Clone, PartialEq)]
pub enum SnapValue {
    /// A string value.
    String(Bytes),
    /// A list of values.
    List(VecDeque<Bytes>),
    /// A sorted set: vec of (score, member) pairs.
    SortedSet(Vec<(f64, String)>),
    /// A hash: map of field names to values.
    Hash(HashMap<String, Bytes>),
    /// An unordered set of unique string members.
    Set(HashSet<String>),
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
    /// Set to `true` after a successful `finish()`. Used by the `Drop`
    /// impl to clean up incomplete temp files.
    finished: bool,
    #[cfg(feature = "encryption")]
    encryption_key: Option<crate::encryption::EncryptionKey>,
}

impl SnapshotWriter {
    /// Creates a new snapshot writer. The file won't appear at `path`
    /// until [`Self::finish`] is called successfully.
    pub fn create(path: impl Into<PathBuf>, shard_id: u16) -> Result<Self, FormatError> {
        let final_path = path.into();
        let (tmp_path, writer) = Self::open_tmp(&final_path)?;
        let mut writer = BufWriter::new(writer);

        format::write_header(&mut writer, format::SNAP_MAGIC)?;
        format::write_u16(&mut writer, shard_id)?;
        format::write_u32(&mut writer, 0)?;

        Ok(Self {
            final_path,
            tmp_path,
            writer,
            hasher: crc32fast::Hasher::new(),
            count: 0,
            finished: false,
            #[cfg(feature = "encryption")]
            encryption_key: None,
        })
    }

    /// Creates a new encrypted snapshot writer.
    #[cfg(feature = "encryption")]
    pub fn create_encrypted(
        path: impl Into<PathBuf>,
        shard_id: u16,
        key: crate::encryption::EncryptionKey,
    ) -> Result<Self, FormatError> {
        let final_path = path.into();
        let (tmp_path, file) = Self::open_tmp(&final_path)?;
        let mut writer = BufWriter::new(file);

        format::write_header_versioned(
            &mut writer,
            format::SNAP_MAGIC,
            format::FORMAT_VERSION_ENCRYPTED,
        )?;
        format::write_u16(&mut writer, shard_id)?;
        format::write_u32(&mut writer, 0)?;

        Ok(Self {
            final_path,
            tmp_path,
            writer,
            hasher: crc32fast::Hasher::new(),
            count: 0,
            finished: false,
            encryption_key: Some(key),
        })
    }

    /// Opens the temp file for writing.
    fn open_tmp(final_path: &Path) -> Result<(PathBuf, File), FormatError> {
        let tmp_path = final_path.with_extension("snap.tmp");
        let mut opts = OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let file = opts.open(&tmp_path)?;
        Ok((tmp_path, file))
    }

    /// Writes a single entry to the snapshot.
    ///
    /// When encrypted, each entry is written as `[nonce: 12B][len: 4B][ciphertext]`.
    /// The footer CRC covers the encrypted bytes (nonce + len + ciphertext).
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
            SnapValue::Hash(map) => {
                format::write_u8(&mut buf, TYPE_HASH)?;
                format::write_u32(&mut buf, map.len() as u32)?;
                for (field, value) in map {
                    format::write_bytes(&mut buf, field.as_bytes())?;
                    format::write_bytes(&mut buf, value)?;
                }
            }
            SnapValue::Set(set) => {
                format::write_u8(&mut buf, TYPE_SET)?;
                format::write_u32(&mut buf, set.len() as u32)?;
                for member in set {
                    format::write_bytes(&mut buf, member.as_bytes())?;
                }
            }
        }
        format::write_i64(&mut buf, entry.expire_ms)?;

        #[cfg(feature = "encryption")]
        if let Some(ref key) = self.encryption_key {
            let (nonce, ciphertext) = crate::encryption::encrypt_record(key, &buf)?;
            // footer CRC covers the encrypted envelope
            self.hasher.update(&nonce);
            let ct_len_bytes = (ciphertext.len() as u32).to_le_bytes();
            self.hasher.update(&ct_len_bytes);
            self.hasher.update(&ciphertext);
            self.writer.write_all(&nonce)?;
            format::write_u32(&mut self.writer, ciphertext.len() as u32)?;
            self.writer.write_all(&ciphertext)?;
            self.count += 1;
            return Ok(());
        }

        self.hasher.update(&buf);
        self.writer.write_all(&buf)?;
        self.count += 1;
        Ok(())
    }

    /// Finalizes the snapshot: writes the footer CRC, flushes, and
    /// atomically renames the temp file to the final path.
    pub fn finish(mut self) -> Result<(), FormatError> {
        // write footer CRC — clone the hasher so we don't move out of self
        let checksum = self.hasher.clone().finalize();
        format::write_u32(&mut self.writer, checksum)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        // rewrite the header with the correct entry count.
        // open a second handle for the seek — the BufWriter is already
        // flushed and synced above.
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
        self.finished = true;
        Ok(())
    }
}

impl Drop for SnapshotWriter {
    fn drop(&mut self) {
        if !self.finished {
            // best-effort cleanup of incomplete temp file
            let _ = fs::remove_file(&self.tmp_path);
        }
    }
}

/// Reads entries from a snapshot file.
pub struct SnapshotReader {
    reader: BufReader<File>,
    pub shard_id: u16,
    pub entry_count: u32,
    read_so_far: u32,
    hasher: crc32fast::Hasher,
    /// Format version — v1 has no type tags, v2 has type-tagged entries, v3 is encrypted.
    version: u8,
    #[cfg(feature = "encryption")]
    encryption_key: Option<crate::encryption::EncryptionKey>,
}

impl SnapshotReader {
    /// Opens a snapshot file and reads the header.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FormatError> {
        let file = File::open(path.as_ref())?;
        let mut reader = BufReader::new(file);

        let version = format::read_header(&mut reader, format::SNAP_MAGIC)?;

        if version == format::FORMAT_VERSION_ENCRYPTED {
            return Err(FormatError::EncryptionRequired);
        }

        let shard_id = format::read_u16(&mut reader)?;
        let entry_count = format::read_u32(&mut reader)?;

        Ok(Self {
            reader,
            shard_id,
            entry_count,
            read_so_far: 0,
            hasher: crc32fast::Hasher::new(),
            version,
            #[cfg(feature = "encryption")]
            encryption_key: None,
        })
    }

    /// Opens a snapshot file with an encryption key for decrypting v3 entries.
    ///
    /// Also handles v1/v2 (plaintext) files — the key is simply unused.
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(
        path: impl AsRef<Path>,
        key: crate::encryption::EncryptionKey,
    ) -> Result<Self, FormatError> {
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
            encryption_key: Some(key),
        })
    }

    /// Reads the next entry. Returns `None` when all entries have been read.
    pub fn read_entry(&mut self) -> Result<Option<SnapEntry>, FormatError> {
        if self.read_so_far >= self.entry_count {
            return Ok(None);
        }

        #[cfg(feature = "encryption")]
        if self.version == format::FORMAT_VERSION_ENCRYPTED {
            return self.read_encrypted_entry();
        }

        self.read_plaintext_entry()
    }

    /// Reads a plaintext (v1/v2) entry.
    fn read_plaintext_entry(&mut self) -> Result<Option<SnapEntry>, FormatError> {
        let mut buf = Vec::new();

        let key_bytes = format::read_bytes(&mut self.reader)?;
        format::write_bytes(&mut buf, &key_bytes)?;

        let value = if self.version == 1 {
            // v1: no type tag, value is always a string
            let value_bytes = format::read_bytes(&mut self.reader)?;
            format::write_bytes(&mut buf, &value_bytes)?;
            SnapValue::String(Bytes::from(value_bytes))
        } else {
            // v2+: type-tagged values
            let type_tag = format::read_u8(&mut self.reader)?;
            format::write_u8(&mut buf, type_tag)?;
            match type_tag {
                TYPE_STRING => {
                    let value_bytes = format::read_bytes(&mut self.reader)?;
                    format::write_bytes(&mut buf, &value_bytes)?;
                    SnapValue::String(Bytes::from(value_bytes))
                }
                TYPE_LIST => {
                    let count = format::read_u32(&mut self.reader)?;
                    format::write_u32(&mut buf, count)?;
                    let mut deque = VecDeque::with_capacity(count as usize);
                    for _ in 0..count {
                        let item = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &item)?;
                        deque.push_back(Bytes::from(item));
                    }
                    SnapValue::List(deque)
                }
                TYPE_SORTED_SET => {
                    let count = format::read_u32(&mut self.reader)?;
                    format::write_u32(&mut buf, count)?;
                    let mut members = Vec::with_capacity(count as usize);
                    for _ in 0..count {
                        let score = format::read_f64(&mut self.reader)?;
                        format::write_f64(&mut buf, score)?;
                        let member_bytes = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &member_bytes)?;
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
                TYPE_HASH => {
                    let count = format::read_u32(&mut self.reader)?;
                    format::write_u32(&mut buf, count)?;
                    let mut map = HashMap::with_capacity(count as usize);
                    for _ in 0..count {
                        let field_bytes = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &field_bytes)?;
                        let field = String::from_utf8(field_bytes).map_err(|_| {
                            FormatError::Io(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "hash field is not valid utf-8",
                            ))
                        })?;
                        let value_bytes = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &value_bytes)?;
                        map.insert(field, Bytes::from(value_bytes));
                    }
                    SnapValue::Hash(map)
                }
                TYPE_SET => {
                    let count = format::read_u32(&mut self.reader)?;
                    format::write_u32(&mut buf, count)?;
                    let mut set = HashSet::with_capacity(count as usize);
                    for _ in 0..count {
                        let member_bytes = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &member_bytes)?;
                        let member = String::from_utf8(member_bytes).map_err(|_| {
                            FormatError::Io(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "set member is not valid utf-8",
                            ))
                        })?;
                        set.insert(member);
                    }
                    SnapValue::Set(set)
                }
                _ => {
                    return Err(FormatError::UnknownTag(type_tag));
                }
            }
        };

        let expire_ms = format::read_i64(&mut self.reader)?;
        format::write_i64(&mut buf, expire_ms)?;
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

    /// Reads an encrypted (v3) entry: nonce + len + ciphertext.
    /// Decrypts to get the same bytes as a plaintext entry, then parses.
    #[cfg(feature = "encryption")]
    fn read_encrypted_entry(&mut self) -> Result<Option<SnapEntry>, FormatError> {
        use std::io::Read as _;

        let key = self
            .encryption_key
            .as_ref()
            .ok_or(FormatError::EncryptionRequired)?;

        let mut nonce = [0u8; crate::encryption::NONCE_SIZE];
        self.reader
            .read_exact(&mut nonce)
            .map_err(|e| match e.kind() {
                io::ErrorKind::UnexpectedEof => FormatError::UnexpectedEof,
                _ => FormatError::Io(e),
            })?;

        let ct_len = format::read_u32(&mut self.reader)? as usize;
        if ct_len > format::MAX_FIELD_LEN {
            return Err(FormatError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("encrypted entry length {ct_len} exceeds maximum"),
            )));
        }

        let mut ciphertext = vec![0u8; ct_len];
        self.reader
            .read_exact(&mut ciphertext)
            .map_err(|e| match e.kind() {
                io::ErrorKind::UnexpectedEof => FormatError::UnexpectedEof,
                _ => FormatError::Io(e),
            })?;

        // footer CRC covers the encrypted envelope
        self.hasher.update(&nonce);
        let ct_len_bytes = (ct_len as u32).to_le_bytes();
        self.hasher.update(&ct_len_bytes);
        self.hasher.update(&ciphertext);

        let plaintext = crate::encryption::decrypt_record(key, &nonce, &ciphertext)?;

        // parse the decrypted bytes using the same logic as v2
        let mut cursor = io::Cursor::new(&plaintext);
        let key_bytes = format::read_bytes(&mut cursor)?;
        let type_tag = format::read_u8(&mut cursor)?;
        let value = match type_tag {
            TYPE_STRING => {
                let v = format::read_bytes(&mut cursor)?;
                SnapValue::String(Bytes::from(v))
            }
            TYPE_LIST => {
                let count = format::read_u32(&mut cursor)?;
                let mut deque = VecDeque::with_capacity(count as usize);
                for _ in 0..count {
                    deque.push_back(Bytes::from(format::read_bytes(&mut cursor)?));
                }
                SnapValue::List(deque)
            }
            TYPE_SORTED_SET => {
                let count = format::read_u32(&mut cursor)?;
                let mut members = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let score = format::read_f64(&mut cursor)?;
                    let member_bytes = format::read_bytes(&mut cursor)?;
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
            TYPE_HASH => {
                let count = format::read_u32(&mut cursor)?;
                let mut map = HashMap::with_capacity(count as usize);
                for _ in 0..count {
                    let field_bytes = format::read_bytes(&mut cursor)?;
                    let field = String::from_utf8(field_bytes).map_err(|_| {
                        FormatError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "hash field is not valid utf-8",
                        ))
                    })?;
                    let value_bytes = format::read_bytes(&mut cursor)?;
                    map.insert(field, Bytes::from(value_bytes));
                }
                SnapValue::Hash(map)
            }
            TYPE_SET => {
                let count = format::read_u32(&mut cursor)?;
                let mut set = HashSet::with_capacity(count as usize);
                for _ in 0..count {
                    let member_bytes = format::read_bytes(&mut cursor)?;
                    let member = String::from_utf8(member_bytes).map_err(|_| {
                        FormatError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "set member is not valid utf-8",
                        ))
                    })?;
                    set.insert(member);
                }
                SnapValue::Set(set)
            }
            _ => return Err(FormatError::UnknownTag(type_tag)),
        };
        let expire_ms = format::read_i64(&mut cursor)?;

        let entry_key = String::from_utf8(key_bytes).map_err(|_| {
            FormatError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "key is not valid utf-8",
            ))
        })?;

        self.read_so_far += 1;
        Ok(Some(SnapEntry {
            key: entry_key,
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

        // the tmp file should have been cleaned up by Drop
        let tmp = path.with_extension("snap.tmp");
        assert!(!tmp.exists(), "drop should clean up incomplete tmp file");
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

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::encryption::EncryptionKey;

        fn test_key() -> EncryptionKey {
            EncryptionKey::from_bytes([0x42; 32])
        }

        #[test]
        fn encrypted_snapshot_round_trip() {
            let dir = temp_dir();
            let path = dir.path().join("enc.snap");
            let key = test_key();

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
            ];

            {
                let mut writer = SnapshotWriter::create_encrypted(&path, 7, key.clone()).unwrap();
                for entry in &entries {
                    writer.write_entry(entry).unwrap();
                }
                writer.finish().unwrap();
            }

            let mut reader = SnapshotReader::open_encrypted(&path, key).unwrap();
            assert_eq!(reader.shard_id, 7);
            assert_eq!(reader.entry_count, 2);

            let mut got = Vec::new();
            while let Some(entry) = reader.read_entry().unwrap() {
                got.push(entry);
            }
            assert_eq!(entries, got);
            reader.verify_footer().unwrap();
        }

        #[test]
        fn encrypted_snapshot_wrong_key_fails() {
            let dir = temp_dir();
            let path = dir.path().join("enc_bad.snap");
            let key = test_key();
            let wrong_key = EncryptionKey::from_bytes([0xFF; 32]);

            {
                let mut writer = SnapshotWriter::create_encrypted(&path, 0, key).unwrap();
                writer
                    .write_entry(&SnapEntry {
                        key: "k".into(),
                        value: SnapValue::String(Bytes::from("v")),
                        expire_ms: -1,
                    })
                    .unwrap();
                writer.finish().unwrap();
            }

            let mut reader = SnapshotReader::open_encrypted(&path, wrong_key).unwrap();
            let err = reader.read_entry().unwrap_err();
            assert!(matches!(err, FormatError::DecryptionFailed));
        }

        #[test]
        fn v2_snapshot_readable_with_encryption_key() {
            let dir = temp_dir();
            let path = dir.path().join("v2.snap");
            let key = test_key();

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

            let mut reader = SnapshotReader::open_encrypted(&path, key).unwrap();
            let entry = reader.read_entry().unwrap().unwrap();
            assert_eq!(entry.key, "k");
            reader.verify_footer().unwrap();
        }

        #[test]
        fn v3_snapshot_without_key_returns_error() {
            let dir = temp_dir();
            let path = dir.path().join("v3_nokey.snap");
            let key = test_key();

            {
                let mut writer = SnapshotWriter::create_encrypted(&path, 0, key).unwrap();
                writer
                    .write_entry(&SnapEntry {
                        key: "k".into(),
                        value: SnapValue::String(Bytes::from("v")),
                        expire_ms: -1,
                    })
                    .unwrap();
                writer.finish().unwrap();
            }

            let result = SnapshotReader::open(&path);
            assert!(matches!(result, Err(FormatError::EncryptionRequired)));
        }

        #[test]
        fn encrypted_snapshot_with_all_types() {
            let dir = temp_dir();
            let path = dir.path().join("enc_types.snap");
            let key = test_key();

            let mut deque = VecDeque::new();
            deque.push_back(Bytes::from("a"));
            deque.push_back(Bytes::from("b"));

            let mut hash = HashMap::new();
            hash.insert("f1".into(), Bytes::from("v1"));

            let mut set = HashSet::new();
            set.insert("m1".into());
            set.insert("m2".into());

            let entries = vec![
                SnapEntry {
                    key: "str".into(),
                    value: SnapValue::String(Bytes::from("val")),
                    expire_ms: -1,
                },
                SnapEntry {
                    key: "list".into(),
                    value: SnapValue::List(deque),
                    expire_ms: 1000,
                },
                SnapEntry {
                    key: "zset".into(),
                    value: SnapValue::SortedSet(vec![(1.0, "a".into()), (2.0, "b".into())]),
                    expire_ms: -1,
                },
                SnapEntry {
                    key: "hash".into(),
                    value: SnapValue::Hash(hash),
                    expire_ms: -1,
                },
                SnapEntry {
                    key: "set".into(),
                    value: SnapValue::Set(set),
                    expire_ms: -1,
                },
            ];

            {
                let mut writer = SnapshotWriter::create_encrypted(&path, 0, key.clone()).unwrap();
                for entry in &entries {
                    writer.write_entry(entry).unwrap();
                }
                writer.finish().unwrap();
            }

            let mut reader = SnapshotReader::open_encrypted(&path, key).unwrap();
            let mut got = Vec::new();
            while let Some(entry) = reader.read_entry().unwrap() {
                got.push(entry);
            }
            assert_eq!(entries, got);
            reader.verify_footer().unwrap();
        }
    }
}
