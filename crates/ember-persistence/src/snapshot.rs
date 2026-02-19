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
#[cfg(feature = "vector")]
const TYPE_VECTOR: u8 = 6;
#[cfg(feature = "protobuf")]
const TYPE_PROTO: u8 = 5;

/// Converts raw bytes to a UTF-8 string, returning a descriptive error
/// on invalid data. `field` names the field for the error message
/// (e.g. "key", "member", "hash field").
fn parse_utf8(bytes: Vec<u8>, field: &str) -> Result<String, FormatError> {
    String::from_utf8(bytes).map_err(|_| {
        FormatError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} is not valid utf-8"),
        ))
    })
}

/// Reads a UTF-8 string from a length-prefixed byte field.
fn read_snap_string(r: &mut impl io::Read, field: &str) -> Result<String, FormatError> {
    let bytes = format::read_bytes(r)?;
    parse_utf8(bytes, field)
}

/// Parses a type-tagged SnapValue from a reader (v2+ format).
///
/// Used by `read_encrypted_entry` and `deserialize_snap_value` to parse
/// `[type_tag][payload]`. The plaintext snapshot path has parallel logic
/// that interleaves CRC buffer mirroring, so it stays inline.
fn parse_snap_value(r: &mut impl io::Read) -> Result<SnapValue, FormatError> {
    let type_tag = format::read_u8(r)?;
    match type_tag {
        TYPE_STRING => {
            let v = format::read_bytes(r)?;
            Ok(SnapValue::String(Bytes::from(v)))
        }
        TYPE_LIST => {
            let count = format::read_u32(r)?;
            format::validate_collection_count(count, "list")?;
            let mut deque = VecDeque::with_capacity(format::capped_capacity(count));
            for _ in 0..count {
                deque.push_back(Bytes::from(format::read_bytes(r)?));
            }
            Ok(SnapValue::List(deque))
        }
        TYPE_SORTED_SET => {
            let count = format::read_u32(r)?;
            format::validate_collection_count(count, "sorted set")?;
            let mut members = Vec::with_capacity(format::capped_capacity(count));
            for _ in 0..count {
                let score = format::read_f64(r)?;
                let member = read_snap_string(r, "member")?;
                members.push((score, member));
            }
            Ok(SnapValue::SortedSet(members))
        }
        TYPE_HASH => {
            let count = format::read_u32(r)?;
            format::validate_collection_count(count, "hash")?;
            let mut map = HashMap::with_capacity(format::capped_capacity(count));
            for _ in 0..count {
                let field = read_snap_string(r, "hash field")?;
                let value = format::read_bytes(r)?;
                map.insert(field, Bytes::from(value));
            }
            Ok(SnapValue::Hash(map))
        }
        TYPE_SET => {
            let count = format::read_u32(r)?;
            format::validate_collection_count(count, "set")?;
            let mut set = HashSet::with_capacity(format::capped_capacity(count));
            for _ in 0..count {
                let member = read_snap_string(r, "set member")?;
                set.insert(member);
            }
            Ok(SnapValue::Set(set))
        }
        #[cfg(feature = "vector")]
        TYPE_VECTOR => {
            let metric = format::read_u8(r)?;
            if metric > 2 {
                return Err(FormatError::InvalidData(format!(
                    "unknown vector metric: {metric}"
                )));
            }
            let quantization = format::read_u8(r)?;
            if quantization > 2 {
                return Err(FormatError::InvalidData(format!(
                    "unknown vector quantization: {quantization}"
                )));
            }
            let connectivity = format::read_u32(r)?;
            let expansion_add = format::read_u32(r)?;
            let dim = format::read_u32(r)?;
            if dim > format::MAX_PERSISTED_VECTOR_DIMS {
                return Err(FormatError::InvalidData(format!(
                    "vector dimension {dim} exceeds max {}",
                    format::MAX_PERSISTED_VECTOR_DIMS
                )));
            }
            let count = format::read_u32(r)?;
            if count > format::MAX_PERSISTED_VECTOR_COUNT {
                return Err(FormatError::InvalidData(format!(
                    "vector element count {count} exceeds max {}",
                    format::MAX_PERSISTED_VECTOR_COUNT
                )));
            }
            format::validate_vector_total(dim, count)?;
            let mut elements = Vec::with_capacity(format::capped_capacity(count));
            for _ in 0..count {
                let name = read_snap_string(r, "vector element name")?;
                let mut vector = Vec::with_capacity(dim as usize);
                for _ in 0..dim {
                    vector.push(format::read_f32(r)?);
                }
                elements.push((name, vector));
            }
            Ok(SnapValue::Vector {
                metric,
                quantization,
                connectivity,
                expansion_add,
                dim,
                elements,
            })
        }
        #[cfg(feature = "protobuf")]
        TYPE_PROTO => {
            let type_name = read_snap_string(r, "proto type_name")?;
            let data = format::read_bytes(r)?;
            Ok(SnapValue::Proto {
                type_name,
                data: Bytes::from(data),
            })
        }
        _ => Err(FormatError::UnknownTag(type_tag)),
    }
}

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
    /// A vector set: index config + all (element, vector) pairs.
    #[cfg(feature = "vector")]
    Vector {
        metric: u8,
        quantization: u8,
        connectivity: u32,
        expansion_add: u32,
        dim: u32,
        elements: Vec<(String, Vec<f32>)>,
    },
    /// A protobuf message: type name + serialized bytes.
    #[cfg(feature = "protobuf")]
    Proto { type_name: String, data: Bytes },
}

/// A single entry in a snapshot file.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapEntry {
    pub key: String,
    pub value: SnapValue,
    /// Remaining TTL in milliseconds, or -1 for no expiration.
    pub expire_ms: i64,
}

impl SnapEntry {
    /// Estimates the serialized byte size for buffer pre-allocation.
    fn estimated_size(&self) -> usize {
        const LEN_PREFIX: usize = 4;

        let key_size = LEN_PREFIX + self.key.len();
        let value_size = match &self.value {
            SnapValue::String(data) => 1 + LEN_PREFIX + data.len(),
            SnapValue::List(deque) => {
                let items: usize = deque.iter().map(|v| LEN_PREFIX + v.len()).sum();
                1 + 4 + items
            }
            SnapValue::SortedSet(members) => {
                let items: usize = members.iter().map(|(_, m)| 8 + LEN_PREFIX + m.len()).sum();
                1 + 4 + items
            }
            SnapValue::Hash(map) => {
                let items: usize = map
                    .iter()
                    .map(|(f, v)| LEN_PREFIX + f.len() + LEN_PREFIX + v.len())
                    .sum();
                1 + 4 + items
            }
            SnapValue::Set(set) => {
                let items: usize = set.iter().map(|m| LEN_PREFIX + m.len()).sum();
                1 + 4 + items
            }
            #[cfg(feature = "vector")]
            SnapValue::Vector { dim, elements, .. } => {
                let items: usize = elements
                    .iter()
                    .map(|(name, _)| LEN_PREFIX + name.len() + (*dim as usize) * 4)
                    .sum();
                // tag + metric + quant + connectivity + expansion + dim + count + items
                1 + 2 + 4 + 4 + 4 + 4 + items
            }
            #[cfg(feature = "protobuf")]
            SnapValue::Proto { type_name, data } => {
                1 + LEN_PREFIX + type_name.len() + LEN_PREFIX + data.len()
            }
        };
        // key + value + expire_ms (i64 = 8 bytes)
        key_size + value_size + 8
    }
}

/// Serializes a `SnapValue` to bytes: `[type_tag][payload]`.
///
/// Used by MIGRATE/DUMP to serialize values for transfer between nodes.
/// The format matches the per-entry body in snapshot files (minus the
/// key and expire_ms).
pub fn serialize_snap_value(value: &SnapValue) -> Result<Vec<u8>, FormatError> {
    let mut buf = Vec::new();
    match value {
        SnapValue::String(data) => {
            format::write_u8(&mut buf, TYPE_STRING)?;
            format::write_bytes(&mut buf, data)?;
        }
        SnapValue::List(deque) => {
            format::write_u8(&mut buf, TYPE_LIST)?;
            format::write_len(&mut buf, deque.len())?;
            for item in deque {
                format::write_bytes(&mut buf, item)?;
            }
        }
        SnapValue::SortedSet(members) => {
            format::write_u8(&mut buf, TYPE_SORTED_SET)?;
            format::write_len(&mut buf, members.len())?;
            for (score, member) in members {
                format::write_f64(&mut buf, *score)?;
                format::write_bytes(&mut buf, member.as_bytes())?;
            }
        }
        SnapValue::Hash(map) => {
            format::write_u8(&mut buf, TYPE_HASH)?;
            format::write_len(&mut buf, map.len())?;
            for (field, value) in map {
                format::write_bytes(&mut buf, field.as_bytes())?;
                format::write_bytes(&mut buf, value)?;
            }
        }
        SnapValue::Set(set) => {
            format::write_u8(&mut buf, TYPE_SET)?;
            format::write_len(&mut buf, set.len())?;
            for member in set {
                format::write_bytes(&mut buf, member.as_bytes())?;
            }
        }
        #[cfg(feature = "vector")]
        SnapValue::Vector {
            metric,
            quantization,
            connectivity,
            expansion_add,
            dim,
            elements,
        } => {
            format::write_u8(&mut buf, TYPE_VECTOR)?;
            format::write_u8(&mut buf, *metric)?;
            format::write_u8(&mut buf, *quantization)?;
            format::write_u32(&mut buf, *connectivity)?;
            format::write_u32(&mut buf, *expansion_add)?;
            format::write_u32(&mut buf, *dim)?;
            format::write_len(&mut buf, elements.len())?;
            for (name, vector) in elements {
                format::write_bytes(&mut buf, name.as_bytes())?;
                for &v in vector {
                    format::write_f32(&mut buf, v)?;
                }
            }
        }
        #[cfg(feature = "protobuf")]
        SnapValue::Proto { type_name, data } => {
            format::write_u8(&mut buf, TYPE_PROTO)?;
            format::write_bytes(&mut buf, type_name.as_bytes())?;
            format::write_bytes(&mut buf, data)?;
        }
    }
    Ok(buf)
}

/// Deserializes a `SnapValue` from bytes produced by [`serialize_snap_value`].
pub fn deserialize_snap_value(data: &[u8]) -> Result<SnapValue, FormatError> {
    let mut cursor = io::Cursor::new(data);
    parse_snap_value(&mut cursor)
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
        let mut buf = Vec::with_capacity(entry.estimated_size());
        format::write_bytes(&mut buf, entry.key.as_bytes())?;
        match &entry.value {
            SnapValue::String(data) => {
                format::write_u8(&mut buf, TYPE_STRING)?;
                format::write_bytes(&mut buf, data)?;
            }
            SnapValue::List(deque) => {
                format::write_u8(&mut buf, TYPE_LIST)?;
                format::write_len(&mut buf, deque.len())?;
                for item in deque {
                    format::write_bytes(&mut buf, item)?;
                }
            }
            SnapValue::SortedSet(members) => {
                format::write_u8(&mut buf, TYPE_SORTED_SET)?;
                format::write_len(&mut buf, members.len())?;
                for (score, member) in members {
                    format::write_f64(&mut buf, *score)?;
                    format::write_bytes(&mut buf, member.as_bytes())?;
                }
            }
            SnapValue::Hash(map) => {
                format::write_u8(&mut buf, TYPE_HASH)?;
                format::write_len(&mut buf, map.len())?;
                for (field, value) in map {
                    format::write_bytes(&mut buf, field.as_bytes())?;
                    format::write_bytes(&mut buf, value)?;
                }
            }
            SnapValue::Set(set) => {
                format::write_u8(&mut buf, TYPE_SET)?;
                format::write_len(&mut buf, set.len())?;
                for member in set {
                    format::write_bytes(&mut buf, member.as_bytes())?;
                }
            }
            #[cfg(feature = "vector")]
            SnapValue::Vector {
                metric,
                quantization,
                connectivity,
                expansion_add,
                dim,
                elements,
            } => {
                format::write_u8(&mut buf, TYPE_VECTOR)?;
                format::write_u8(&mut buf, *metric)?;
                format::write_u8(&mut buf, *quantization)?;
                format::write_u32(&mut buf, *connectivity)?;
                format::write_u32(&mut buf, *expansion_add)?;
                format::write_u32(&mut buf, *dim)?;
                format::write_len(&mut buf, elements.len())?;
                for (name, vector) in elements {
                    format::write_bytes(&mut buf, name.as_bytes())?;
                    for &v in vector {
                        format::write_f32(&mut buf, v)?;
                    }
                }
            }
            #[cfg(feature = "protobuf")]
            SnapValue::Proto { type_name, data } => {
                format::write_u8(&mut buf, TYPE_PROTO)?;
                format::write_bytes(&mut buf, type_name.as_bytes())?;
                format::write_bytes(&mut buf, data)?;
            }
        }
        format::write_i64(&mut buf, entry.expire_ms)?;

        #[cfg(feature = "encryption")]
        if let Some(ref key) = self.encryption_key {
            let (nonce, ciphertext) = crate::encryption::encrypt_record(key, &buf)?;
            let ct_len = u32::try_from(ciphertext.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "encrypted record exceeds u32::MAX bytes",
                )
            })?;
            // footer CRC covers the encrypted envelope
            self.hasher.update(&nonce);
            let ct_len_bytes = ct_len.to_le_bytes();
            self.hasher.update(&ct_len_bytes);
            self.hasher.update(&ciphertext);
            self.writer.write_all(&nonce)?;
            format::write_u32(&mut self.writer, ct_len)?;
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

        // fsync the parent directory so the rename is durable across crashes
        if let Some(parent) = self.final_path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }

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
                    format::validate_collection_count(count, "list")?;
                    format::write_u32(&mut buf, count)?;
                    let mut deque = VecDeque::with_capacity(format::capped_capacity(count));
                    for _ in 0..count {
                        let item = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &item)?;
                        deque.push_back(Bytes::from(item));
                    }
                    SnapValue::List(deque)
                }
                TYPE_SORTED_SET => {
                    let count = format::read_u32(&mut self.reader)?;
                    format::validate_collection_count(count, "sorted set")?;
                    format::write_u32(&mut buf, count)?;
                    let mut members = Vec::with_capacity(format::capped_capacity(count));
                    for _ in 0..count {
                        let score = format::read_f64(&mut self.reader)?;
                        format::write_f64(&mut buf, score)?;
                        let member_bytes = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &member_bytes)?;
                        let member = parse_utf8(member_bytes, "member")?;
                        members.push((score, member));
                    }
                    SnapValue::SortedSet(members)
                }
                TYPE_HASH => {
                    let count = format::read_u32(&mut self.reader)?;
                    format::validate_collection_count(count, "hash")?;
                    format::write_u32(&mut buf, count)?;
                    let mut map = HashMap::with_capacity(format::capped_capacity(count));
                    for _ in 0..count {
                        let field_bytes = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &field_bytes)?;
                        let field = parse_utf8(field_bytes, "hash field")?;
                        let value_bytes = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &value_bytes)?;
                        map.insert(field, Bytes::from(value_bytes));
                    }
                    SnapValue::Hash(map)
                }
                TYPE_SET => {
                    let count = format::read_u32(&mut self.reader)?;
                    format::validate_collection_count(count, "set")?;
                    format::write_u32(&mut buf, count)?;
                    let mut set = HashSet::with_capacity(format::capped_capacity(count));
                    for _ in 0..count {
                        let member_bytes = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &member_bytes)?;
                        let member = parse_utf8(member_bytes, "set member")?;
                        set.insert(member);
                    }
                    SnapValue::Set(set)
                }
                #[cfg(feature = "vector")]
                TYPE_VECTOR => {
                    let metric = format::read_u8(&mut self.reader)?;
                    if metric > 2 {
                        return Err(FormatError::InvalidData(format!(
                            "unknown vector metric: {metric}"
                        )));
                    }
                    format::write_u8(&mut buf, metric)?;
                    let quantization = format::read_u8(&mut self.reader)?;
                    if quantization > 2 {
                        return Err(FormatError::InvalidData(format!(
                            "unknown vector quantization: {quantization}"
                        )));
                    }
                    format::write_u8(&mut buf, quantization)?;
                    let connectivity = format::read_u32(&mut self.reader)?;
                    format::write_u32(&mut buf, connectivity)?;
                    let expansion_add = format::read_u32(&mut self.reader)?;
                    format::write_u32(&mut buf, expansion_add)?;
                    let dim = format::read_u32(&mut self.reader)?;
                    if dim > format::MAX_PERSISTED_VECTOR_DIMS {
                        return Err(FormatError::InvalidData(format!(
                            "vector dimension {dim} exceeds max {}",
                            format::MAX_PERSISTED_VECTOR_DIMS
                        )));
                    }
                    format::write_u32(&mut buf, dim)?;
                    let count = format::read_u32(&mut self.reader)?;
                    if count > format::MAX_PERSISTED_VECTOR_COUNT {
                        return Err(FormatError::InvalidData(format!(
                            "vector element count {count} exceeds max {}",
                            format::MAX_PERSISTED_VECTOR_COUNT
                        )));
                    }
                    format::validate_vector_total(dim, count)?;
                    format::write_u32(&mut buf, count)?;
                    let mut elements = Vec::with_capacity(format::capped_capacity(count));
                    for _ in 0..count {
                        let name_bytes = format::read_bytes(&mut self.reader)?;
                        format::write_bytes(&mut buf, &name_bytes)?;
                        let name = parse_utf8(name_bytes, "vector element name")?;
                        let mut vector = Vec::with_capacity(dim as usize);
                        for _ in 0..dim {
                            let v = format::read_f32(&mut self.reader)?;
                            format::write_f32(&mut buf, v)?;
                            vector.push(v);
                        }
                        elements.push((name, vector));
                    }
                    SnapValue::Vector {
                        metric,
                        quantization,
                        connectivity,
                        expansion_add,
                        dim,
                        elements,
                    }
                }
                #[cfg(feature = "protobuf")]
                TYPE_PROTO => {
                    let type_name_bytes = format::read_bytes(&mut self.reader)?;
                    format::write_bytes(&mut buf, &type_name_bytes)?;
                    let type_name = parse_utf8(type_name_bytes, "proto type_name")?;
                    let data = format::read_bytes(&mut self.reader)?;
                    format::write_bytes(&mut buf, &data)?;
                    SnapValue::Proto {
                        type_name,
                        data: Bytes::from(data),
                    }
                }
                _ => {
                    return Err(FormatError::UnknownTag(type_tag));
                }
            }
        };

        let expire_ms = format::read_i64(&mut self.reader)?;
        format::write_i64(&mut buf, expire_ms)?;
        self.hasher.update(&buf);

        let key = parse_utf8(key_bytes, "key")?;

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

        let mut cursor = io::Cursor::new(&plaintext);
        let entry_key = read_snap_string(&mut cursor, "key")?;
        let value = parse_snap_value(&mut cursor)?;
        let expire_ms = format::read_i64(&mut cursor)?;

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

/// Serializes snapshot entries to an in-memory byte buffer.
///
/// The format is identical to the file-based snapshot so the bytes can be
/// sent over the network and loaded with [`read_snapshot_from_bytes`].
/// Only unencrypted v2 format is produced — encryption is not used for the
/// in-memory replication path.
pub fn write_snapshot_bytes(shard_id: u16, entries: &[SnapEntry]) -> Result<Vec<u8>, FormatError> {
    use std::io::{Seek, SeekFrom, Write as _};

    let mut buf = io::Cursor::new(Vec::<u8>::new());
    let mut hasher = crc32fast::Hasher::new();

    format::write_header(&mut buf, format::SNAP_MAGIC)?;
    format::write_u16(&mut buf, shard_id)?;
    // remember where the entry count lives so we can patch it at the end
    let count_pos = buf.position();
    format::write_u32(&mut buf, 0u32)?;

    let mut count = 0u32;
    for entry in entries {
        let entry_bytes = serialize_entry(entry)?;
        hasher.update(&entry_bytes);
        buf.write_all(&entry_bytes)?;
        count += 1;
    }

    // patch entry count in the header
    let end_pos = buf.position();
    buf.seek(SeekFrom::Start(count_pos))?;
    format::write_u32(&mut buf, count)?;
    buf.seek(SeekFrom::Start(end_pos))?;

    // footer CRC
    let checksum = hasher.finalize();
    format::write_u32(&mut buf, checksum)?;

    Ok(buf.into_inner())
}

/// Deserializes snapshot entries from bytes produced by [`write_snapshot_bytes`].
///
/// Returns the shard ID from the header and all live entries. Validates the
/// footer CRC to detect transmission errors.
pub fn read_snapshot_from_bytes(data: &[u8]) -> Result<(u16, Vec<SnapEntry>), FormatError> {
    let mut r = io::Cursor::new(data);
    let mut hasher = crc32fast::Hasher::new();

    let version = format::read_header(&mut r, format::SNAP_MAGIC)?;
    if version != format::FORMAT_VERSION {
        return Err(FormatError::UnsupportedVersion(version));
    }
    let shard_id = format::read_u16(&mut r)?;
    let entry_count = format::read_u32(&mut r)?;

    let mut entries = Vec::with_capacity(entry_count.min(65536) as usize);
    for _ in 0..entry_count {
        let (entry, entry_bytes) = read_entry_with_bytes(&mut r)?;
        hasher.update(&entry_bytes);
        entries.push(entry);
    }

    // verify footer CRC
    let expected = hasher.finalize();
    let stored = format::read_u32(&mut r)?;
    format::verify_crc32_values(expected, stored)?;

    Ok((shard_id, entries))
}

/// Serializes a single snapshot entry to raw bytes (no encryption).
///
/// Used by [`write_snapshot_bytes`] for in-memory serialization.
fn serialize_entry(entry: &SnapEntry) -> Result<Vec<u8>, FormatError> {
    let mut buf = Vec::with_capacity(entry.estimated_size());
    format::write_bytes(&mut buf, entry.key.as_bytes())?;
    match &entry.value {
        SnapValue::String(data) => {
            format::write_u8(&mut buf, TYPE_STRING)?;
            format::write_bytes(&mut buf, data)?;
        }
        SnapValue::List(deque) => {
            format::write_u8(&mut buf, TYPE_LIST)?;
            format::write_len(&mut buf, deque.len())?;
            for item in deque {
                format::write_bytes(&mut buf, item)?;
            }
        }
        SnapValue::SortedSet(members) => {
            format::write_u8(&mut buf, TYPE_SORTED_SET)?;
            format::write_len(&mut buf, members.len())?;
            for (score, member) in members {
                format::write_f64(&mut buf, *score)?;
                format::write_bytes(&mut buf, member.as_bytes())?;
            }
        }
        SnapValue::Hash(map) => {
            format::write_u8(&mut buf, TYPE_HASH)?;
            format::write_len(&mut buf, map.len())?;
            for (field, value) in map {
                format::write_bytes(&mut buf, field.as_bytes())?;
                format::write_bytes(&mut buf, value)?;
            }
        }
        SnapValue::Set(set) => {
            format::write_u8(&mut buf, TYPE_SET)?;
            format::write_len(&mut buf, set.len())?;
            for member in set {
                format::write_bytes(&mut buf, member.as_bytes())?;
            }
        }
        #[cfg(feature = "vector")]
        SnapValue::Vector {
            metric,
            quantization,
            connectivity,
            expansion_add,
            dim,
            elements,
        } => {
            format::write_u8(&mut buf, TYPE_VECTOR)?;
            format::write_u8(&mut buf, *metric)?;
            format::write_u8(&mut buf, *quantization)?;
            format::write_u32(&mut buf, *connectivity)?;
            format::write_u32(&mut buf, *expansion_add)?;
            format::write_u32(&mut buf, *dim)?;
            format::write_len(&mut buf, elements.len())?;
            for (name, vector) in elements {
                format::write_bytes(&mut buf, name.as_bytes())?;
                for &v in vector {
                    format::write_f32(&mut buf, v)?;
                }
            }
        }
        #[cfg(feature = "protobuf")]
        SnapValue::Proto { type_name, data } => {
            format::write_u8(&mut buf, TYPE_PROTO)?;
            format::write_bytes(&mut buf, type_name.as_bytes())?;
            format::write_bytes(&mut buf, data)?;
        }
    }
    format::write_i64(&mut buf, entry.expire_ms)?;
    Ok(buf)
}

/// Reads a single entry from a cursor and also returns the raw bytes
/// used for CRC computation.
fn read_entry_with_bytes(r: &mut io::Cursor<&[u8]>) -> Result<(SnapEntry, Vec<u8>), FormatError> {
    let mut entry_bytes = Vec::new();

    let key_bytes = format::read_bytes(r)?;
    format::write_bytes(&mut entry_bytes, &key_bytes)?;
    let key = parse_utf8(key_bytes, "key")?;

    let type_tag = format::read_u8(r)?;
    format::write_u8(&mut entry_bytes, type_tag)?;

    let value = match type_tag {
        TYPE_STRING => {
            let v = format::read_bytes(r)?;
            format::write_bytes(&mut entry_bytes, &v)?;
            SnapValue::String(Bytes::from(v))
        }
        TYPE_LIST => {
            let count = format::read_u32(r)?;
            format::validate_collection_count(count, "list")?;
            format::write_u32(&mut entry_bytes, count)?;
            let mut deque = VecDeque::with_capacity(format::capped_capacity(count));
            for _ in 0..count {
                let item = format::read_bytes(r)?;
                format::write_bytes(&mut entry_bytes, &item)?;
                deque.push_back(Bytes::from(item));
            }
            SnapValue::List(deque)
        }
        TYPE_SORTED_SET => {
            let count = format::read_u32(r)?;
            format::validate_collection_count(count, "sorted set")?;
            format::write_u32(&mut entry_bytes, count)?;
            let mut members = Vec::with_capacity(format::capped_capacity(count));
            for _ in 0..count {
                let score = format::read_f64(r)?;
                format::write_f64(&mut entry_bytes, score)?;
                let mb = format::read_bytes(r)?;
                format::write_bytes(&mut entry_bytes, &mb)?;
                members.push((score, parse_utf8(mb, "member")?));
            }
            SnapValue::SortedSet(members)
        }
        TYPE_HASH => {
            let count = format::read_u32(r)?;
            format::validate_collection_count(count, "hash")?;
            format::write_u32(&mut entry_bytes, count)?;
            let mut map = HashMap::with_capacity(format::capped_capacity(count));
            for _ in 0..count {
                let fb = format::read_bytes(r)?;
                format::write_bytes(&mut entry_bytes, &fb)?;
                let field = parse_utf8(fb, "hash field")?;
                let vb = format::read_bytes(r)?;
                format::write_bytes(&mut entry_bytes, &vb)?;
                map.insert(field, Bytes::from(vb));
            }
            SnapValue::Hash(map)
        }
        TYPE_SET => {
            let count = format::read_u32(r)?;
            format::validate_collection_count(count, "set")?;
            format::write_u32(&mut entry_bytes, count)?;
            let mut set = HashSet::with_capacity(format::capped_capacity(count));
            for _ in 0..count {
                let mb = format::read_bytes(r)?;
                format::write_bytes(&mut entry_bytes, &mb)?;
                set.insert(parse_utf8(mb, "set member")?);
            }
            SnapValue::Set(set)
        }
        #[cfg(feature = "vector")]
        TYPE_VECTOR => {
            let metric = format::read_u8(r)?;
            format::write_u8(&mut entry_bytes, metric)?;
            let quantization = format::read_u8(r)?;
            format::write_u8(&mut entry_bytes, quantization)?;
            let connectivity = format::read_u32(r)?;
            format::write_u32(&mut entry_bytes, connectivity)?;
            let expansion_add = format::read_u32(r)?;
            format::write_u32(&mut entry_bytes, expansion_add)?;
            let dim = format::read_u32(r)?;
            format::write_u32(&mut entry_bytes, dim)?;
            let count = format::read_u32(r)?;
            format::write_u32(&mut entry_bytes, count)?;
            let mut elements = Vec::with_capacity(format::capped_capacity(count));
            for _ in 0..count {
                let nb = format::read_bytes(r)?;
                format::write_bytes(&mut entry_bytes, &nb)?;
                let name = parse_utf8(nb, "vector element name")?;
                let mut vector = Vec::with_capacity(dim as usize);
                for _ in 0..dim {
                    let v = format::read_f32(r)?;
                    format::write_f32(&mut entry_bytes, v)?;
                    vector.push(v);
                }
                elements.push((name, vector));
            }
            SnapValue::Vector {
                metric,
                quantization,
                connectivity,
                expansion_add,
                dim,
                elements,
            }
        }
        #[cfg(feature = "protobuf")]
        TYPE_PROTO => {
            let tn_bytes = format::read_bytes(r)?;
            format::write_bytes(&mut entry_bytes, &tn_bytes)?;
            let type_name = parse_utf8(tn_bytes, "proto type_name")?;
            let data = format::read_bytes(r)?;
            format::write_bytes(&mut entry_bytes, &data)?;
            SnapValue::Proto {
                type_name,
                data: Bytes::from(data),
            }
        }
        _ => return Err(FormatError::UnknownTag(type_tag)),
    };

    let expire_ms = format::read_i64(r)?;
    format::write_i64(&mut entry_bytes, expire_ms)?;

    Ok((
        SnapEntry {
            key,
            value,
            expire_ms,
        },
        entry_bytes,
    ))
}

/// Returns the snapshot file path for a given shard in a data directory.
pub fn snapshot_path(data_dir: &Path, shard_id: u16) -> PathBuf {
    data_dir.join(format!("shard-{shard_id}.snap"))
}

#[cfg(test)]
mod tests {
    use super::*;

    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("create temp dir")
    }

    #[test]
    fn empty_snapshot_round_trip() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("empty.snap");

        {
            let writer = SnapshotWriter::create(&path, 0)?;
            writer.finish()?;
        }

        let reader = SnapshotReader::open(&path)?;
        assert_eq!(reader.shard_id, 0);
        assert_eq!(reader.entry_count, 0);
        reader.verify_footer()?;
        Ok(())
    }

    #[test]
    fn entries_round_trip() -> Result {
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
            let mut writer = SnapshotWriter::create(&path, 7)?;
            for entry in &entries {
                writer.write_entry(entry)?;
            }
            writer.finish()?;
        }

        let mut reader = SnapshotReader::open(&path)?;
        assert_eq!(reader.shard_id, 7);
        assert_eq!(reader.entry_count, 3);

        let mut got = Vec::new();
        while let Some(entry) = reader.read_entry()? {
            got.push(entry);
        }
        assert_eq!(entries, got);
        reader.verify_footer()?;
        Ok(())
    }

    #[test]
    fn corrupt_footer_detected() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("corrupt.snap");

        {
            let mut writer = SnapshotWriter::create(&path, 0)?;
            writer.write_entry(&SnapEntry {
                key: "k".into(),
                value: SnapValue::String(Bytes::from("v")),
                expire_ms: -1,
            })?;
            writer.finish()?;
        }

        // corrupt the last byte (footer CRC)
        let mut data = fs::read(&path)?;
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&path, &data)?;

        let mut reader = SnapshotReader::open(&path)?;
        // reading entries should still work
        reader.read_entry()?;
        // but footer verification should fail
        let err = reader.verify_footer().unwrap_err();
        assert!(matches!(err, FormatError::ChecksumMismatch { .. }));
        Ok(())
    }

    #[test]
    fn atomic_rename_prevents_partial_snapshots() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("atomic.snap");

        // write an initial snapshot
        {
            let mut writer = SnapshotWriter::create(&path, 0)?;
            writer.write_entry(&SnapEntry {
                key: "original".into(),
                value: SnapValue::String(Bytes::from("data")),
                expire_ms: -1,
            })?;
            writer.finish()?;
        }

        // start a second snapshot but don't finish it
        {
            let mut writer = SnapshotWriter::create(&path, 0)?;
            writer.write_entry(&SnapEntry {
                key: "new".into(),
                value: SnapValue::String(Bytes::from("partial")),
                expire_ms: -1,
            })?;
            // drop without finish — simulates a crash
            drop(writer);
        }

        // the original snapshot should still be intact
        let mut reader = SnapshotReader::open(&path)?;
        let entry = reader.read_entry()?.unwrap();
        assert_eq!(entry.key, "original");

        // the tmp file should have been cleaned up by Drop
        let tmp = path.with_extension("snap.tmp");
        assert!(!tmp.exists(), "drop should clean up incomplete tmp file");
        Ok(())
    }

    #[test]
    fn ttl_entries_preserved() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("ttl.snap");

        let entry = SnapEntry {
            key: "expires".into(),
            value: SnapValue::String(Bytes::from("soon")),
            expire_ms: 42_000,
        };

        {
            let mut writer = SnapshotWriter::create(&path, 0)?;
            writer.write_entry(&entry)?;
            writer.finish()?;
        }

        let mut reader = SnapshotReader::open(&path)?;
        let got = reader.read_entry()?.unwrap();
        assert_eq!(got.expire_ms, 42_000);
        reader.verify_footer()?;
        Ok(())
    }

    #[test]
    fn list_entries_round_trip() -> Result {
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
            let mut writer = SnapshotWriter::create(&path, 0)?;
            for entry in &entries {
                writer.write_entry(entry)?;
            }
            writer.finish()?;
        }

        let mut reader = SnapshotReader::open(&path)?;
        let mut got = Vec::new();
        while let Some(entry) = reader.read_entry()? {
            got.push(entry);
        }
        assert_eq!(entries, got);
        reader.verify_footer()?;
        Ok(())
    }

    #[test]
    fn sorted_set_entries_round_trip() -> Result {
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
            let mut writer = SnapshotWriter::create(&path, 0)?;
            for entry in &entries {
                writer.write_entry(entry)?;
            }
            writer.finish()?;
        }

        let mut reader = SnapshotReader::open(&path)?;
        let mut got = Vec::new();
        while let Some(entry) = reader.read_entry()? {
            got.push(entry);
        }
        assert_eq!(entries, got);
        reader.verify_footer()?;
        Ok(())
    }

    #[test]
    fn snapshot_path_format() {
        let p = snapshot_path(Path::new("/data"), 5);
        assert_eq!(p, PathBuf::from("/data/shard-5.snap"));
    }

    #[test]
    fn truncated_snapshot_detected() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("truncated.snap");

        // write a valid 2-entry snapshot
        {
            let mut writer = SnapshotWriter::create(&path, 0)?;
            writer.write_entry(&SnapEntry {
                key: "a".into(),
                value: SnapValue::String(Bytes::from("1")),
                expire_ms: -1,
            })?;
            writer.write_entry(&SnapEntry {
                key: "b".into(),
                value: SnapValue::String(Bytes::from("2")),
                expire_ms: 5000,
            })?;
            writer.finish()?;
        }

        // truncate the file mid-way through the second entry
        let data = fs::read(&path)?;
        let truncated = &data[..data.len() - 20];
        fs::write(&path, truncated)?;

        let mut reader = SnapshotReader::open(&path)?;
        assert_eq!(reader.entry_count, 2);

        // first entry should still be readable
        let first = reader.read_entry()?;
        assert!(first.is_some());

        // second entry should fail with an EOF-related error
        let err = reader.read_entry().unwrap_err();
        assert!(
            matches!(err, FormatError::UnexpectedEof | FormatError::Io(_)),
            "expected EOF error, got {err:?}"
        );
        Ok(())
    }

    #[cfg(feature = "vector")]
    #[test]
    fn vector_entries_round_trip() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("vec.snap");

        let entries = vec![SnapEntry {
            key: "embeddings".into(),
            value: SnapValue::Vector {
                metric: 0,
                quantization: 0,
                connectivity: 16,
                expansion_add: 64,
                dim: 3,
                elements: vec![
                    ("doc1".into(), vec![0.1, 0.2, 0.3]),
                    ("doc2".into(), vec![0.4, 0.5, 0.6]),
                ],
            },
            expire_ms: -1,
        }];

        {
            let mut writer = SnapshotWriter::create(&path, 0)?;
            for entry in &entries {
                writer.write_entry(entry)?;
            }
            writer.finish()?;
        }

        let mut reader = SnapshotReader::open(&path)?;
        let mut got = Vec::new();
        while let Some(entry) = reader.read_entry()? {
            got.push(entry);
        }
        assert_eq!(entries, got);
        reader.verify_footer()?;
        Ok(())
    }

    #[cfg(feature = "vector")]
    #[test]
    fn vector_empty_set_round_trip() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("vec_empty.snap");

        let entries = vec![SnapEntry {
            key: "empty_vecs".into(),
            value: SnapValue::Vector {
                metric: 2, // inner product
                quantization: 2,
                connectivity: 8,
                expansion_add: 32,
                dim: 128,
                elements: vec![],
            },
            expire_ms: 5000,
        }];

        {
            let mut writer = SnapshotWriter::create(&path, 0)?;
            for entry in &entries {
                writer.write_entry(entry)?;
            }
            writer.finish()?;
        }

        let mut reader = SnapshotReader::open(&path)?;
        let got = reader.read_entry()?.unwrap();
        assert_eq!(entries[0], got);
        reader.verify_footer()?;
        Ok(())
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::encryption::EncryptionKey;

        type Result = std::result::Result<(), Box<dyn std::error::Error>>;

        fn test_key() -> EncryptionKey {
            EncryptionKey::from_bytes([0x42; 32])
        }

        #[test]
        fn encrypted_snapshot_round_trip() -> Result {
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
                let mut writer = SnapshotWriter::create_encrypted(&path, 7, key.clone())?;
                for entry in &entries {
                    writer.write_entry(entry)?;
                }
                writer.finish()?;
            }

            let mut reader = SnapshotReader::open_encrypted(&path, key)?;
            assert_eq!(reader.shard_id, 7);
            assert_eq!(reader.entry_count, 2);

            let mut got = Vec::new();
            while let Some(entry) = reader.read_entry()? {
                got.push(entry);
            }
            assert_eq!(entries, got);
            reader.verify_footer()?;
            Ok(())
        }

        #[test]
        fn encrypted_snapshot_wrong_key_fails() -> Result {
            let dir = temp_dir();
            let path = dir.path().join("enc_bad.snap");
            let key = test_key();
            let wrong_key = EncryptionKey::from_bytes([0xFF; 32]);

            {
                let mut writer = SnapshotWriter::create_encrypted(&path, 0, key)?;
                writer.write_entry(&SnapEntry {
                    key: "k".into(),
                    value: SnapValue::String(Bytes::from("v")),
                    expire_ms: -1,
                })?;
                writer.finish()?;
            }

            let mut reader = SnapshotReader::open_encrypted(&path, wrong_key)?;
            let err = reader.read_entry().unwrap_err();
            assert!(matches!(err, FormatError::DecryptionFailed));
            Ok(())
        }

        #[test]
        fn v2_snapshot_readable_with_encryption_key() -> Result {
            let dir = temp_dir();
            let path = dir.path().join("v2.snap");
            let key = test_key();

            {
                let mut writer = SnapshotWriter::create(&path, 0)?;
                writer.write_entry(&SnapEntry {
                    key: "k".into(),
                    value: SnapValue::String(Bytes::from("v")),
                    expire_ms: -1,
                })?;
                writer.finish()?;
            }

            let mut reader = SnapshotReader::open_encrypted(&path, key)?;
            let entry = reader.read_entry()?.unwrap();
            assert_eq!(entry.key, "k");
            reader.verify_footer()?;
            Ok(())
        }

        #[test]
        fn v3_snapshot_without_key_returns_error() -> Result {
            let dir = temp_dir();
            let path = dir.path().join("v3_nokey.snap");
            let key = test_key();

            {
                let mut writer = SnapshotWriter::create_encrypted(&path, 0, key)?;
                writer.write_entry(&SnapEntry {
                    key: "k".into(),
                    value: SnapValue::String(Bytes::from("v")),
                    expire_ms: -1,
                })?;
                writer.finish()?;
            }

            let result = SnapshotReader::open(&path);
            assert!(matches!(result, Err(FormatError::EncryptionRequired)));
            Ok(())
        }

        #[test]
        fn encrypted_snapshot_with_all_types() -> Result {
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
                let mut writer = SnapshotWriter::create_encrypted(&path, 0, key.clone())?;
                for entry in &entries {
                    writer.write_entry(entry)?;
                }
                writer.finish()?;
            }

            let mut reader = SnapshotReader::open_encrypted(&path, key)?;
            let mut got = Vec::new();
            while let Some(entry) = reader.read_entry()? {
                got.push(entry);
            }
            assert_eq!(entries, got);
            reader.verify_footer()?;
            Ok(())
        }
    }

    // -- serialize/deserialize roundtrip tests --

    #[test]
    fn snap_value_roundtrip_string() {
        let original = SnapValue::String(Bytes::from("hello world"));
        let data = serialize_snap_value(&original).unwrap();
        let decoded = deserialize_snap_value(&data).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn snap_value_roundtrip_list() {
        let original = SnapValue::List(VecDeque::from([
            Bytes::from("a"),
            Bytes::from("b"),
            Bytes::from("c"),
        ]));
        let data = serialize_snap_value(&original).unwrap();
        let decoded = deserialize_snap_value(&data).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn snap_value_roundtrip_sorted_set() {
        let original = SnapValue::SortedSet(vec![(1.5, "alice".into()), (2.7, "bob".into())]);
        let data = serialize_snap_value(&original).unwrap();
        let decoded = deserialize_snap_value(&data).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn snap_value_roundtrip_hash() {
        let mut map = HashMap::new();
        map.insert("field1".into(), Bytes::from("val1"));
        map.insert("field2".into(), Bytes::from("val2"));
        let original = SnapValue::Hash(map);
        let data = serialize_snap_value(&original).unwrap();
        let decoded = deserialize_snap_value(&data).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn snap_value_roundtrip_set() {
        let mut set = HashSet::new();
        set.insert("x".into());
        set.insert("y".into());
        set.insert("z".into());
        let original = SnapValue::Set(set);
        let data = serialize_snap_value(&original).unwrap();
        let decoded = deserialize_snap_value(&data).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn snap_value_roundtrip_empty_string() {
        let original = SnapValue::String(Bytes::new());
        let data = serialize_snap_value(&original).unwrap();
        let decoded = deserialize_snap_value(&data).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn deserialize_invalid_data() {
        assert!(deserialize_snap_value(&[]).is_err());
        assert!(deserialize_snap_value(&[255]).is_err());
    }
}
