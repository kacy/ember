//! Persistent storage for Raft state.
//!
//! Provides disk-backed persistence for vote metadata, log entries, and
//! snapshots so that a cluster node can recover its Raft state after a
//! restart without re-bootstrapping.
//!
//! # File layout
//!
//! Three files live inside a `raft/` directory under the configured data dir:
//!
//! | file | purpose | write pattern |
//! |------|---------|---------------|
//! | `raft-meta` | vote + last_purged LogId | atomic rewrite |
//! | `raft-log` | append-only log entries | append + fsync |
//! | `raft-snapshot` | latest snapshot blob | atomic rewrite |
//!
//! Each file starts with a 5-byte header (`[magic:4B][version:1B]`) followed
//! by length-prefixed, CRC32-checksummed records encoded with postcard.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;
use openraft::{Entry, LogId, SnapshotMeta, Vote};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, warn};

use crate::raft::TypeConfig;

// ---------------------------------------------------------------------------
// constants
// ---------------------------------------------------------------------------

const META_MAGIC: &[u8; 4] = b"ERMT";
const LOG_MAGIC: &[u8; 4] = b"ERLO";
const SNAP_MAGIC: &[u8; 4] = b"ERSS";
const FORMAT_VERSION: u8 = 1;

/// Maximum record payload size (64 MB). Prevents corrupt length fields from
/// causing unbounded allocations.
const MAX_RECORD_SIZE: u32 = 64 * 1024 * 1024;

// ---------------------------------------------------------------------------
// error type
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum RaftDiskError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("invalid magic bytes in {file}")]
    InvalidMagic { file: &'static str },

    #[error("unsupported format version {version} in {file}")]
    UnsupportedVersion { file: &'static str, version: u8 },

    #[error("crc32 mismatch (expected {expected:#010x}, got {actual:#010x})")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("postcard error: {0}")]
    Postcard(String),

    #[error("record payload too large ({size} bytes, max {MAX_RECORD_SIZE})")]
    RecordTooLarge { size: u32 },
}

impl From<RaftDiskError> for openraft::StorageError<u64> {
    fn from(e: RaftDiskError) -> Self {
        openraft::StorageIOError::write(&e).into()
    }
}

// ---------------------------------------------------------------------------
// on-disk record types
// ---------------------------------------------------------------------------

/// Metadata persisted in the `raft-meta` file.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MetaRecord {
    vote: Option<Vote<u64>>,
    last_purged: Option<LogId<u64>>,
}

/// Snapshot envelope persisted in the `raft-snapshot` file.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotRecord {
    meta: SnapshotMeta<u64, openraft::BasicNode>,
    data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// recovered state (returned to Storage::open)
// ---------------------------------------------------------------------------

/// Everything recovered from disk, ready to populate the in-memory Storage.
#[derive(Debug, Default)]
pub(crate) struct RecoveredState {
    pub vote: Option<Vote<u64>>,
    pub last_purged: Option<LogId<u64>>,
    pub log: BTreeMap<u64, Entry<TypeConfig>>,
    pub snapshot: Option<(SnapshotMeta<u64, openraft::BasicNode>, Vec<u8>)>,
}

// ---------------------------------------------------------------------------
// RaftDisk
// ---------------------------------------------------------------------------

/// Disk-backed persistence for Raft state.
///
/// All writes go through `std::sync::Mutex` in the caller (Storage).
/// Methods here are intentionally synchronous — they do blocking I/O
/// but are only called on the Raft consensus path, not per client request.
pub(crate) struct RaftDisk {
    dir: PathBuf,
    log_file: File,
}

impl std::fmt::Debug for RaftDisk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftDisk").field("dir", &self.dir).finish()
    }
}

impl RaftDisk {
    /// Opens (or creates) the raft persistence directory and recovers state.
    ///
    /// On a fresh start all three files are created with just their headers.
    /// On recovery, existing files are read and any trailing corruption in
    /// the log file is silently truncated (incomplete last write).
    pub fn open(dir: &Path) -> Result<(Self, RecoveredState), RaftDiskError> {
        fs::create_dir_all(dir)?;

        let meta_path = dir.join("raft-meta");
        let log_path = dir.join("raft-log");
        let snap_path = dir.join("raft-snapshot");

        // recover meta
        let meta = if meta_path.exists() {
            read_meta_file(&meta_path)?
        } else {
            write_meta_file(&meta_path, &MetaRecord::default())?;
            MetaRecord::default()
        };

        // recover snapshot
        let snapshot = if snap_path.exists() {
            match read_snapshot_file(&snap_path) {
                Ok(rec) => Some((rec.meta, rec.data)),
                Err(e) => {
                    warn!("raft snapshot file corrupt, ignoring: {e}");
                    None
                }
            }
        } else {
            None
        };

        // recover log entries
        let (log, valid_len) = if log_path.exists() {
            recover_log_file(&log_path)?
        } else {
            (BTreeMap::new(), 0)
        };

        // open log file for appending (truncate any trailing corruption)
        let log_file = if log_path.exists() {
            let file = OpenOptions::new().write(true).open(&log_path)?;
            file.set_len(valid_len)?;
            file.sync_all()?;
            // reopen in append mode
            OpenOptions::new().append(true).open(&log_path)?
        } else {
            let mut file = File::create(&log_path)?;
            write_header(&mut file, LOG_MAGIC)?;
            file.sync_all()?;
            OpenOptions::new().append(true).open(&log_path)?
        };

        let recovered = RecoveredState {
            vote: meta.vote,
            last_purged: meta.last_purged,
            log,
            snapshot,
        };

        debug!(
            "raft disk recovered: vote={:?}, last_purged={:?}, log_entries={}, has_snapshot={}",
            recovered.vote,
            recovered.last_purged,
            recovered.log.len(),
            recovered.snapshot.is_some(),
        );

        Ok((
            Self {
                dir: dir.to_path_buf(),
                log_file,
            },
            recovered,
        ))
    }

    /// Persists the current vote and last_purged log ID.
    pub fn write_meta(
        &self,
        vote: Option<Vote<u64>>,
        last_purged: Option<LogId<u64>>,
    ) -> Result<(), RaftDiskError> {
        let record = MetaRecord { vote, last_purged };
        write_meta_file(&self.dir.join("raft-meta"), &record)
    }

    /// Appends log entries to the log file.
    pub fn append_entries(&mut self, entries: &[Entry<TypeConfig>]) -> Result<(), RaftDiskError> {
        for entry in entries {
            write_record(&mut self.log_file, entry)?;
        }
        self.log_file.flush()?;
        Ok(())
    }

    /// Rewrites the log file with only the given entries.
    ///
    /// Used after purge or truncation to compact the on-disk log.
    pub fn rewrite_log(
        &mut self,
        entries: &BTreeMap<u64, Entry<TypeConfig>>,
    ) -> Result<(), RaftDiskError> {
        let log_path = self.dir.join("raft-log");
        let tmp_path = self.dir.join("raft-log.tmp");

        let mut file = File::create(&tmp_path)?;
        write_header(&mut file, LOG_MAGIC)?;
        for entry in entries.values() {
            write_record(&mut file, entry)?;
        }
        file.flush()?;
        file.sync_all()?;

        fs::rename(&tmp_path, &log_path)?;

        // reopen for appending
        self.log_file = OpenOptions::new().append(true).open(&log_path)?;
        Ok(())
    }

    /// Atomically writes the latest snapshot to disk.
    pub fn write_snapshot(
        &self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        data: &[u8],
    ) -> Result<(), RaftDiskError> {
        let snap_path = self.dir.join("raft-snapshot");
        let tmp_path = self.dir.join("raft-snapshot.tmp");

        let record = SnapshotRecord {
            meta: meta.clone(),
            data: data.to_vec(),
        };

        let mut file = File::create(&tmp_path)?;
        write_header(&mut file, SNAP_MAGIC)?;
        write_record(&mut file, &record)?;
        file.flush()?;
        file.sync_all()?;

        fs::rename(&tmp_path, &snap_path)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// binary format helpers
// ---------------------------------------------------------------------------

fn crc32(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finalize()
}

fn write_header(w: &mut impl Write, magic: &[u8; 4]) -> Result<(), RaftDiskError> {
    w.write_all(magic)?;
    w.write_all(&[FORMAT_VERSION])?;
    Ok(())
}

fn read_header(
    r: &mut impl Read,
    expected_magic: &[u8; 4],
    file_name: &'static str,
) -> Result<(), RaftDiskError> {
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic).map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            RaftDiskError::InvalidMagic { file: file_name }
        } else {
            RaftDiskError::Io(e)
        }
    })?;
    if &magic != expected_magic {
        return Err(RaftDiskError::InvalidMagic { file: file_name });
    }
    let mut ver = [0u8; 1];
    r.read_exact(&mut ver)?;
    if ver[0] != FORMAT_VERSION {
        return Err(RaftDiskError::UnsupportedVersion {
            file: file_name,
            version: ver[0],
        });
    }
    Ok(())
}

/// Writes a single record: `[payload_len:4B LE][payload:postcard][crc32:4B LE]`
fn write_record<T: Serialize>(w: &mut impl Write, value: &T) -> Result<(), RaftDiskError> {
    let payload =
        postcard::to_allocvec(value).map_err(|e| RaftDiskError::Postcard(e.to_string()))?;
    let len = payload.len() as u32;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&payload)?;
    let checksum = crc32(&payload);
    w.write_all(&checksum.to_le_bytes())?;
    Ok(())
}

/// Reads a single record, returning the deserialized value.
/// Returns `None` on clean EOF (no bytes available).
fn read_record<T: for<'de> Deserialize<'de>>(
    r: &mut impl Read,
) -> Result<Option<T>, RaftDiskError> {
    // try to read the length prefix — EOF here is clean
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(RaftDiskError::Io(e)),
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_RECORD_SIZE {
        return Err(RaftDiskError::RecordTooLarge { size: len });
    }

    let mut payload = vec![0u8; len as usize];
    r.read_exact(&mut payload)?;

    let mut crc_buf = [0u8; 4];
    r.read_exact(&mut crc_buf)?;
    let stored_crc = u32::from_le_bytes(crc_buf);

    let computed_crc = crc32(&payload);
    if computed_crc != stored_crc {
        return Err(RaftDiskError::ChecksumMismatch {
            expected: stored_crc,
            actual: computed_crc,
        });
    }

    let value =
        postcard::from_bytes(&payload).map_err(|e| RaftDiskError::Postcard(e.to_string()))?;
    Ok(Some(value))
}

// ---------------------------------------------------------------------------
// file-level read/write
// ---------------------------------------------------------------------------

fn write_meta_file(path: &Path, record: &MetaRecord) -> Result<(), RaftDiskError> {
    let tmp_path = path.with_extension("tmp");
    let mut file = BufWriter::new(File::create(&tmp_path)?);
    write_header(&mut file, META_MAGIC)?;
    write_record(&mut file, record)?;
    file.flush()?;
    file.into_inner()
        .map_err(|e| RaftDiskError::Io(e.into_error()))?
        .sync_all()?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

fn read_meta_file(path: &Path) -> Result<MetaRecord, RaftDiskError> {
    let mut reader = BufReader::new(File::open(path)?);
    read_header(&mut reader, META_MAGIC, "raft-meta")?;
    match read_record(&mut reader)? {
        Some(record) => Ok(record),
        None => Ok(MetaRecord::default()),
    }
}

fn read_snapshot_file(path: &Path) -> Result<SnapshotRecord, RaftDiskError> {
    let mut reader = BufReader::new(File::open(path)?);
    read_header(&mut reader, SNAP_MAGIC, "raft-snapshot")?;
    match read_record(&mut reader)? {
        Some(record) => Ok(record),
        None => Err(RaftDiskError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "empty snapshot file",
        ))),
    }
}

/// Recovers log entries from the log file. Returns the entries and the byte
/// offset of the last valid record (used to truncate trailing corruption).
fn recover_log_file(path: &Path) -> Result<(BTreeMap<u64, Entry<TypeConfig>>, u64), RaftDiskError> {
    let file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);

    // read header (5 bytes)
    if file_len < 5 {
        // file too short — treat as empty
        return Ok((BTreeMap::new(), 0));
    }
    read_header(&mut reader, LOG_MAGIC, "raft-log")?;

    let mut entries = BTreeMap::new();
    let mut valid_pos: u64 = 5; // after header

    loop {
        // try reading the next record — EOF is clean
        let remaining = file_len.saturating_sub(valid_pos);
        if remaining == 0 {
            break;
        }

        match read_record::<Entry<TypeConfig>>(&mut reader) {
            Ok(Some(entry)) => {
                let payload_bytes =
                    postcard::to_allocvec(&entry)
                        .map_err(|e| RaftDiskError::Postcard(e.to_string()))?
                        .len() as u64;
                // record size: 4 (len) + payload + 4 (crc)
                valid_pos += 4 + payload_bytes + 4;
                entries.insert(entry.log_id.index, entry);
            }
            Ok(None) => break,
            Err(e) => {
                // corrupt or incomplete record — truncate here
                warn!("raft log truncated at offset {valid_pos}: {e}");
                break;
            }
        }
    }

    Ok((entries, valid_pos))
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::ClusterCommand;
    use crate::topology::NodeId;
    use openraft::{CommittedLeaderId, EntryPayload, StoredMembership};

    fn log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(term, 0), index)
    }

    fn test_entry(term: u64, index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Blank,
        }
    }

    fn test_entry_with_data(term: u64, index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Normal(ClusterCommand::AddNode {
                node_id: NodeId::new(),
                raft_id: index,
                addr: "127.0.0.1:6379".to_string(),
                is_primary: true,
            }),
        }
    }

    #[test]
    fn meta_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft-meta");

        let record = MetaRecord {
            vote: Some(Vote::new(1, 2)),
            last_purged: Some(log_id(1, 5)),
        };
        write_meta_file(&path, &record).unwrap();

        let recovered = read_meta_file(&path).unwrap();
        assert_eq!(recovered.vote, record.vote);
        assert_eq!(recovered.last_purged, record.last_purged);
    }

    #[test]
    fn meta_default_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raft-meta");

        write_meta_file(&path, &MetaRecord::default()).unwrap();
        let recovered = read_meta_file(&path).unwrap();
        assert!(recovered.vote.is_none());
        assert!(recovered.last_purged.is_none());
    }

    #[test]
    fn log_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let (mut disk, recovered) = RaftDisk::open(dir.path()).unwrap();
        assert!(recovered.log.is_empty());

        let entries = vec![
            test_entry(1, 1),
            test_entry(1, 2),
            test_entry_with_data(1, 3),
        ];
        disk.append_entries(&entries).unwrap();

        // reopen and recover
        let (_disk2, recovered2) = RaftDisk::open(dir.path()).unwrap();
        assert_eq!(recovered2.log.len(), 3);
        assert!(recovered2.log.contains_key(&1));
        assert!(recovered2.log.contains_key(&2));
        assert!(recovered2.log.contains_key(&3));
    }

    #[test]
    fn log_truncation_on_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("raft-log");

        // write two valid entries
        {
            let (mut disk, _) = RaftDisk::open(dir.path()).unwrap();
            disk.append_entries(&[test_entry(1, 1), test_entry(1, 2)])
                .unwrap();
        }

        // append garbage to the log file
        {
            let mut file = OpenOptions::new().append(true).open(&log_path).unwrap();
            file.write_all(b"GARBAGE_BYTES").unwrap();
        }

        // recover — should get the two valid entries and truncate the garbage
        let (_disk, recovered) = RaftDisk::open(dir.path()).unwrap();
        assert_eq!(recovered.log.len(), 2);
    }

    #[test]
    fn log_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let (mut disk, _) = RaftDisk::open(dir.path()).unwrap();

        disk.append_entries(&[test_entry(1, 1), test_entry(1, 2), test_entry(1, 3)])
            .unwrap();

        // rewrite with only entry 3
        let mut remaining = BTreeMap::new();
        remaining.insert(3, test_entry(1, 3));
        disk.rewrite_log(&remaining).unwrap();

        // recover
        let (_disk2, recovered) = RaftDisk::open(dir.path()).unwrap();
        assert_eq!(recovered.log.len(), 1);
        assert!(recovered.log.contains_key(&3));
    }

    #[test]
    fn snapshot_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let (disk, _) = RaftDisk::open(dir.path()).unwrap();

        let meta = SnapshotMeta {
            last_log_id: Some(log_id(1, 5)),
            last_membership: StoredMembership::default(),
            snapshot_id: "1-5".to_string(),
        };
        let data = b"snapshot-payload-here".to_vec();
        disk.write_snapshot(&meta, &data).unwrap();

        // reopen and recover
        let (_disk2, recovered) = RaftDisk::open(dir.path()).unwrap();
        let (rec_meta, rec_data) = recovered.snapshot.unwrap();
        assert_eq!(rec_meta.last_log_id, meta.last_log_id);
        assert_eq!(rec_data, data);
    }

    #[test]
    fn write_meta_persists_vote() {
        let dir = tempfile::tempdir().unwrap();
        let (disk, _) = RaftDisk::open(dir.path()).unwrap();

        let vote = Vote::new(2, 1);
        let purged = log_id(1, 10);
        disk.write_meta(Some(vote), Some(purged)).unwrap();

        // reopen
        let (_disk2, recovered) = RaftDisk::open(dir.path()).unwrap();
        assert_eq!(recovered.vote, Some(vote));
        assert_eq!(recovered.last_purged, Some(purged));
    }

    #[test]
    fn fresh_directory_creates_files() {
        let dir = tempfile::tempdir().unwrap();
        let raft_dir = dir.path().join("raft");

        let (_disk, recovered) = RaftDisk::open(&raft_dir).unwrap();
        assert!(recovered.log.is_empty());
        assert!(recovered.vote.is_none());
        assert!(recovered.snapshot.is_none());

        assert!(raft_dir.join("raft-meta").exists());
        assert!(raft_dir.join("raft-log").exists());
    }

    #[test]
    fn corrupt_meta_magic_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let meta_path = dir.path().join("raft-meta");

        // write garbage
        fs::write(&meta_path, b"JUNK").unwrap();

        let result = RaftDisk::open(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn record_crc_mismatch_detected() {
        let dir = tempfile::tempdir().unwrap();

        // write one valid entry then corrupt its CRC
        {
            let (mut disk, _) = RaftDisk::open(dir.path()).unwrap();
            disk.append_entries(&[test_entry(1, 1)]).unwrap();
        }

        // flip a byte in the log file payload
        let log_path = dir.path().join("raft-log");
        let mut data = fs::read(&log_path).unwrap();
        // payload starts at offset 5 (header) + 4 (length) = 9
        if data.len() > 10 {
            data[10] ^= 0xFF;
        }
        fs::write(&log_path, &data).unwrap();

        // recover — corrupt entry should be truncated
        let (_disk, recovered) = RaftDisk::open(dir.path()).unwrap();
        assert!(recovered.log.is_empty());
    }
}
