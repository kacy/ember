//! Point-in-time snapshot files.
//!
//! Each shard writes its own snapshot (`shard-{id}.snap`). The format
//! stores all live entries in a single pass. Writes go to a `.tmp`
//! file first and are atomically renamed on completion — this ensures
//! a partial/crashed snapshot never corrupts the existing `.snap` file.
//!
//! File layout, with the header and frames described in [`crate::frame`]:
//! ```text
//! [header][shard_id: 2B]
//! [one frame per entry]
//! [an empty frame][entry_count: 4B][crc32 of the entry frame bodies: 4B]
//! ```
//!
//! Each entry:
//! ```text
//! [key_len: 4B][key][type_tag: 1B][type-specific payload][expires_at_ms: 8B]
//! ```
//!
//! Type tags: 0=string, 1=list, 2=sorted set, 3=hash, 4=set, 5=protobuf,
//! 6=vector. `expires_at_ms` is the unix time in milliseconds at which the
//! key expires, or 0 for no expiry, so a snapshot restored later doesn't
//! bring back keys that expired since.
//!
//! The trailing CRC identifies the snapshot: the AOF's checkpoint record
//! names the snapshot it follows by it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::format::{self, FormatError};
use crate::frame::{self, Codec, Frame, Key};

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

/// Parses a type-tagged SnapValue from a reader (v2+ format):
/// `[type_tag][payload]`, as written by [`write_snap_value`].
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
    /// Unix time in milliseconds at which the key expires, or 0 for none.
    pub expires_at_ms: u64,
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
        // key + value + expires_at_ms
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
    write_snap_value(&mut buf, value)?;
    Ok(buf)
}

/// Writes `[type_tag][payload]` for a value. Snapshot files, replication
/// snapshots and DUMP payloads all use this encoding.
fn write_snap_value(buf: &mut Vec<u8>, value: &SnapValue) -> Result<(), FormatError> {
    match value {
        SnapValue::String(data) => {
            format::write_u8(buf, TYPE_STRING)?;
            format::write_bytes(buf, data)?;
        }
        SnapValue::List(deque) => {
            format::write_u8(buf, TYPE_LIST)?;
            format::write_len(buf, deque.len())?;
            for item in deque {
                format::write_bytes(buf, item)?;
            }
        }
        SnapValue::SortedSet(members) => {
            format::write_u8(buf, TYPE_SORTED_SET)?;
            format::write_len(buf, members.len())?;
            for (score, member) in members {
                format::write_f64(buf, *score)?;
                format::write_bytes(buf, member.as_bytes())?;
            }
        }
        SnapValue::Hash(map) => {
            format::write_u8(buf, TYPE_HASH)?;
            format::write_len(buf, map.len())?;
            for (field, value) in map {
                format::write_bytes(buf, field.as_bytes())?;
                format::write_bytes(buf, value)?;
            }
        }
        SnapValue::Set(set) => {
            format::write_u8(buf, TYPE_SET)?;
            format::write_len(buf, set.len())?;
            for member in set {
                format::write_bytes(buf, member.as_bytes())?;
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
            format::write_u8(buf, TYPE_VECTOR)?;
            format::write_u8(buf, *metric)?;
            format::write_u8(buf, *quantization)?;
            format::write_u32(buf, *connectivity)?;
            format::write_u32(buf, *expansion_add)?;
            format::write_u32(buf, *dim)?;
            format::write_len(buf, elements.len())?;
            for (name, vector) in elements {
                format::write_bytes(buf, name.as_bytes())?;
                for &v in vector {
                    format::write_f32(buf, v)?;
                }
            }
        }
        #[cfg(feature = "protobuf")]
        SnapValue::Proto { type_name, data } => {
            format::write_u8(buf, TYPE_PROTO)?;
            format::write_bytes(buf, type_name.as_bytes())?;
            format::write_bytes(buf, data)?;
        }
    }
    Ok(())
}

/// Encodes an entry as `[key][type_tag][payload][expires_at_ms]`.
fn encode_entry(entry: &SnapEntry) -> Result<Vec<u8>, FormatError> {
    let mut buf = Vec::with_capacity(entry.estimated_size());
    format::write_bytes(&mut buf, entry.key.as_bytes())?;
    write_snap_value(&mut buf, &entry.value)?;
    buf.extend_from_slice(&entry.expires_at_ms.to_le_bytes());
    Ok(buf)
}

/// Decodes an entry written by [`encode_entry`].
fn decode_entry(data: &[u8]) -> Result<SnapEntry, FormatError> {
    let mut r = io::Cursor::new(data);
    let key = read_snap_string(&mut r, "key")?;
    let value = parse_snap_value(&mut r)?;
    let mut expires_at_ms = [0u8; 8];
    format::read_exact(&mut r, &mut expires_at_ms)?;
    if r.position() != data.len() as u64 {
        return Err(FormatError::InvalidData(
            "snapshot entry has trailing bytes".into(),
        ));
    }
    Ok(SnapEntry {
        key,
        value,
        expires_at_ms: u64::from_le_bytes(expires_at_ms),
    })
}

/// Deserializes a `SnapValue` from bytes produced by [`serialize_snap_value`].
pub fn deserialize_snap_value(data: &[u8]) -> Result<SnapValue, FormatError> {
    let mut cursor = io::Cursor::new(data);
    parse_snap_value(&mut cursor)
}

/// Writes a snapshot to any output: to a file through [`SnapshotWriter`],
/// or to memory for a replica.
struct Encoder<W: Write> {
    out: W,
    codec: Codec,
    /// Running CRC over the entry frame bodies, for the trailer.
    hasher: crc32fast::Hasher,
    count: u32,
    frame: Vec<u8>,
}

impl<W: Write> Encoder<W> {
    fn new(mut out: W, shard_id: u16, key: Option<&Key>) -> Result<Self, FormatError> {
        let mut header = Vec::new();
        let codec = frame::write_header(&mut header, format::SNAP_MAGIC, key)?;
        header.extend_from_slice(&shard_id.to_le_bytes());
        out.write_all(&header)?;
        Ok(Self {
            out,
            codec,
            hasher: crc32fast::Hasher::new(),
            count: 0,
            frame: Vec::new(),
        })
    }

    fn write_entry(&mut self, entry: &SnapEntry) -> Result<(), FormatError> {
        let plain = encode_entry(entry)?;
        let body = self.codec.seal(&plain)?;
        self.hasher.update(&body);
        self.frame.clear();
        frame::write_frame(&mut self.frame, &body)?;
        self.out.write_all(&self.frame)?;
        self.count = self.count.checked_add(1).ok_or_else(|| {
            FormatError::InvalidData("snapshot has more than u32::MAX entries".into())
        })?;
        Ok(())
    }

    /// Writes the trailer and returns the output and the snapshot's CRC.
    fn finish(mut self) -> Result<(W, u32), FormatError> {
        let crc = self.hasher.finalize();
        let mut trailer = Vec::new();
        frame::write_frame(&mut trailer, &[])?;
        trailer.extend_from_slice(&self.count.to_le_bytes());
        trailer.extend_from_slice(&crc.to_le_bytes());
        self.out.write_all(&trailer)?;
        Ok((self.out, crc))
    }
}

/// Reads a snapshot written by [`Encoder`].
struct Decoder<R: io::Read> {
    input: R,
    codec: Codec,
    shard_id: u16,
    hasher: crc32fast::Hasher,
    count: u32,
    /// The CRC, once the trailer has been read and checked.
    crc: Option<u32>,
}

impl<R: io::Read> Decoder<R> {
    fn new(mut input: R, key: Option<&Key>) -> Result<Self, FormatError> {
        let codec = frame::read_header(&mut input, format::SNAP_MAGIC, key)?;
        let shard_id = format::read_u16(&mut input)?;
        Ok(Self {
            input,
            codec,
            shard_id,
            hasher: crc32fast::Hasher::new(),
            count: 0,
            crc: None,
        })
    }

    /// Reads the next entry, or `None` after the last one, once the
    /// trailer has checked out.
    fn read_entry(&mut self) -> Result<Option<SnapEntry>, FormatError> {
        if self.crc.is_some() {
            return Ok(None);
        }
        match frame::read_frame(&mut self.input)? {
            // a snapshot is renamed into place only once it is complete
            Frame::End | Frame::Torn => Err(FormatError::UnexpectedEof),
            Frame::Body(body) if body.is_empty() => {
                let count = format::read_u32(&mut self.input)?;
                let stored = format::read_u32(&mut self.input)?;
                if count != self.count {
                    return Err(FormatError::InvalidData(format!(
                        "snapshot holds {} entries but its trailer says {count}",
                        self.count
                    )));
                }
                let crc = self.hasher.clone().finalize();
                format::verify_crc32_values(crc, stored)?;
                self.crc = Some(crc);
                Ok(None)
            }
            Frame::Body(body) => {
                self.hasher.update(&body);
                self.count += 1;
                Ok(Some(decode_entry(&self.codec.open(body)?)?))
            }
        }
    }

    /// Reads any entries left and returns the snapshot's CRC.
    fn finish(mut self) -> Result<u32, FormatError> {
        while self.read_entry()?.is_some() {}
        Ok(self.crc.expect("set when read_entry returns None"))
    }
}

/// Writes a complete snapshot to disk.
///
/// Entries are written to a temporary file first, then atomically
/// renamed to the final path by [`finish`](Self::finish).
pub struct SnapshotWriter {
    final_path: PathBuf,
    tmp_path: PathBuf,
    /// Taken by `finish()`. Until then, `Drop` removes the temp file.
    encoder: Option<Encoder<BufWriter<File>>>,
}

impl SnapshotWriter {
    /// Creates a new snapshot writer. The file won't appear at `path`
    /// until [`Self::finish`] is called successfully.
    pub fn create(path: impl Into<PathBuf>, shard_id: u16) -> Result<Self, FormatError> {
        Self::create_with(path.into(), shard_id, None)
    }

    /// Creates a new snapshot writer that encrypts with `key`.
    #[cfg(feature = "encryption")]
    pub fn create_encrypted(
        path: impl Into<PathBuf>,
        shard_id: u16,
        key: Key,
    ) -> Result<Self, FormatError> {
        Self::create_with(path.into(), shard_id, Some(&key))
    }

    fn create_with(
        final_path: PathBuf,
        shard_id: u16,
        key: Option<&Key>,
    ) -> Result<Self, FormatError> {
        let tmp_path = final_path.with_extension("snap.tmp");
        let mut opts = OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let file = opts.open(&tmp_path)?;
        let encoder = Encoder::new(BufWriter::new(file), shard_id, key)?;
        Ok(Self {
            final_path,
            tmp_path,
            encoder: Some(encoder),
        })
    }

    /// Writes a single entry to the snapshot.
    pub fn write_entry(&mut self, entry: &SnapEntry) -> Result<(), FormatError> {
        self.encoder
            .as_mut()
            .expect("taken only by finish")
            .write_entry(entry)
    }

    /// Finalizes the snapshot: writes the trailer, fsyncs, and atomically
    /// renames the temp file to the final path. Returns the snapshot's CRC,
    /// which identifies it.
    pub fn finish(mut self) -> Result<u32, FormatError> {
        let encoder = self.encoder.take().expect("taken only by finish");
        let (out, crc) = encoder.finish()?;
        let file = out.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;

        fs::rename(&self.tmp_path, &self.final_path)?;
        // fsync the parent directory so the rename is durable across crashes
        crate::aof::sync_parent_dir(&self.final_path);
        Ok(crc)
    }
}

impl Drop for SnapshotWriter {
    fn drop(&mut self) {
        if self.encoder.is_some() {
            // best-effort cleanup of incomplete temp file
            let _ = fs::remove_file(&self.tmp_path);
        }
    }
}

/// Reads entries from a snapshot file.
pub struct SnapshotReader {
    decoder: Decoder<BufReader<File>>,
    pub shard_id: u16,
}

impl SnapshotReader {
    /// Opens a plaintext snapshot file and reads the header.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FormatError> {
        Self::open_with(path.as_ref(), None)
    }

    /// Opens a snapshot file that may be encrypted with `key`. Plaintext
    /// files are read too.
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(path: impl AsRef<Path>, key: Key) -> Result<Self, FormatError> {
        Self::open_with(path.as_ref(), Some(&key))
    }

    pub(crate) fn open_with(path: &Path, key: Option<&Key>) -> Result<Self, FormatError> {
        let decoder = Decoder::new(BufReader::new(File::open(path)?), key)?;
        Ok(Self {
            shard_id: decoder.shard_id,
            decoder,
        })
    }

    /// Reads the next entry. Returns `None` after the last one.
    pub fn read_entry(&mut self) -> Result<Option<SnapEntry>, FormatError> {
        self.decoder.read_entry()
    }

    /// Checks the trailer after the last entry and returns the snapshot's
    /// CRC.
    pub fn verify_footer(self) -> Result<u32, FormatError> {
        self.decoder.finish()
    }
}

/// Serializes snapshot entries to bytes, in the file format without
/// encryption, to send a replica its initial state. Load them with
/// [`read_snapshot_from_bytes`].
pub fn write_snapshot_bytes(shard_id: u16, entries: &[SnapEntry]) -> Result<Vec<u8>, FormatError> {
    let mut encoder = Encoder::new(Vec::new(), shard_id, None)?;
    for entry in entries {
        encoder.write_entry(entry)?;
    }
    Ok(encoder.finish()?.0)
}

/// Deserializes snapshot entries from bytes produced by
/// [`write_snapshot_bytes`], checking the trailer. Returns the shard ID from
/// the header and all entries.
pub fn read_snapshot_from_bytes(data: &[u8]) -> Result<(u16, Vec<SnapEntry>), FormatError> {
    let mut decoder = Decoder::new(data, None)?;
    let mut entries = Vec::new();
    while let Some(entry) = decoder.read_entry()? {
        entries.push(entry);
    }
    Ok((decoder.shard_id, entries))
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
                expires_at_ms: 0,
            },
            SnapEntry {
                key: "ttl".into(),
                value: SnapValue::String(Bytes::from("expiring")),
                expires_at_ms: 5000,
            },
            SnapEntry {
                key: "empty".into(),
                value: SnapValue::String(Bytes::new()),
                expires_at_ms: 0,
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
                expires_at_ms: 0,
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
                expires_at_ms: 0,
            })?;
            writer.finish()?;
        }

        // start a second snapshot but don't finish it
        {
            let mut writer = SnapshotWriter::create(&path, 0)?;
            writer.write_entry(&SnapEntry {
                key: "new".into(),
                value: SnapValue::String(Bytes::from("partial")),
                expires_at_ms: 0,
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
            expires_at_ms: 42_000,
        };

        {
            let mut writer = SnapshotWriter::create(&path, 0)?;
            writer.write_entry(&entry)?;
            writer.finish()?;
        }

        let mut reader = SnapshotReader::open(&path)?;
        let got = reader.read_entry()?.unwrap();
        assert_eq!(got.expires_at_ms, 42_000);
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
                expires_at_ms: 0,
            },
            SnapEntry {
                key: "mystr".into(),
                value: SnapValue::String(Bytes::from("val")),
                expires_at_ms: 1000,
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
                expires_at_ms: 0,
            },
            SnapEntry {
                key: "mystr".into(),
                value: SnapValue::String(Bytes::from("val")),
                expires_at_ms: 1000,
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
                expires_at_ms: 0,
            })?;
            writer.write_entry(&SnapEntry {
                key: "b".into(),
                value: SnapValue::String(Bytes::from("2")),
                expires_at_ms: 5000,
            })?;
            writer.finish()?;
        }

        // truncate the file mid-way through the second entry, which ends
        // where the 20-byte trailer starts
        let data = fs::read(&path)?;
        let truncated = &data[..data.len() - 25];
        fs::write(&path, truncated)?;

        let mut reader = SnapshotReader::open(&path)?;

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

    #[test]
    fn a_snapshot_missing_its_trailer_is_refused() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("no-trailer.snap");
        let mut writer = SnapshotWriter::create(&path, 0)?;
        writer.write_entry(&SnapEntry {
            key: "a".into(),
            value: SnapValue::String(Bytes::from("1")),
            expires_at_ms: 0,
        })?;
        writer.finish()?;
        let data = fs::read(&path)?;
        fs::write(&path, &data[..data.len() - 20])?;

        let mut reader = SnapshotReader::open(&path)?;
        assert!(reader.read_entry()?.is_some());
        assert!(reader.read_entry().is_err());
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
            expires_at_ms: 0,
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
            expires_at_ms: 5000,
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
                    expires_at_ms: 0,
                },
                SnapEntry {
                    key: "ttl".into(),
                    value: SnapValue::String(Bytes::from("expiring")),
                    expires_at_ms: 5000,
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
                    expires_at_ms: 0,
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
                    expires_at_ms: 0,
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
                    expires_at_ms: 0,
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
                    expires_at_ms: 0,
                },
                SnapEntry {
                    key: "list".into(),
                    value: SnapValue::List(deque),
                    expires_at_ms: 1000,
                },
                SnapEntry {
                    key: "zset".into(),
                    value: SnapValue::SortedSet(vec![(1.0, "a".into()), (2.0, "b".into())]),
                    expires_at_ms: 0,
                },
                SnapEntry {
                    key: "hash".into(),
                    value: SnapValue::Hash(hash),
                    expires_at_ms: 0,
                },
                SnapEntry {
                    key: "set".into(),
                    value: SnapValue::Set(set),
                    expires_at_ms: 0,
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
