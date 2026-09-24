//! Recovery: loading a shard's snapshot and replaying its AOF on startup.
//!
//! [`recover_shard`] reads the files and hands their contents to a
//! [`Recover`] target. The shard implements it with its keyspace, so a
//! replayed record runs the same code as the write that logged it.
//!
//! 1. The snapshot's entries go to [`Recover::restore`], except those whose
//!    TTL ran out while the server was down. A snapshot that fails to load
//!    is skipped, and the shard starts from the AOF alone.
//! 2. A first pass over the AOF decodes every record without applying any.
//!    It cuts off a record left partial by a crash and finds where replay
//!    starts. An AOF that is corrupt in the middle is skipped entirely, so
//!    it can't leave a partial, inconsistent prefix applied.
//! 3. A second pass hands the records from that point to [`Recover::apply`].
//!
//! Replay starts after the last checkpoint that names the loaded snapshot:
//! the records before it are already in the snapshot, as when truncating
//! the AOF after a save failed. An AOF that starts with a checkpoint for a
//! different snapshot, and never reaches one for the loaded snapshot, came
//! before it: the server stopped after the snapshot was saved and before
//! the AOF was truncated. Its records are all in the snapshot, so none are
//! replayed, and the caller must rewrite the file.

use std::path::Path;
use std::time::Duration;

use tracing::{error, warn};

use crate::aof::{self, AofReader, AofRecord};
use crate::format::FormatError;
use crate::snapshot::{self, SnapValue, SnapshotReader};

/// Type alias for an optional encryption key reference. When the
/// `encryption` feature is disabled, this is always `Option<&()>` —
/// always `None` — and all encryption branches compile away.
#[cfg(feature = "encryption")]
type EncryptionKeyRef<'a> = &'a crate::encryption::EncryptionKey;
#[cfg(not(feature = "encryption"))]
type EncryptionKeyRef<'a> = &'a ();

/// Receives the state that recovery reads back.
pub trait Recover {
    /// Stores a key from the snapshot. `ttl` is the time it has left.
    fn restore(&mut self, key: String, value: SnapValue, ttl: Option<Duration>);

    /// Repeats a write from the AOF.
    fn apply(&mut self, record: AofRecord);
}

/// What [`recover_shard`] found.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryResult {
    /// Whether a snapshot was loaded.
    pub loaded_snapshot: bool,
    /// Whether any AOF records were replayed.
    pub replayed_aof: bool,
    /// The AOF was written before the loaded snapshot, so it was skipped.
    /// The caller must rewrite it before appending: otherwise the next
    /// recovery would skip the new writes too.
    pub stale_aof: bool,
}

/// Recovers a shard's state from its snapshot and AOF into `target`.
pub fn recover_shard(data_dir: &Path, shard_id: u16, target: &mut impl Recover) -> RecoveryResult {
    recover_shard_impl(data_dir, shard_id, None, target)
}

/// Like [`recover_shard`], with a key for encrypted files. Plaintext files
/// are read as well.
#[cfg(feature = "encryption")]
pub fn recover_shard_encrypted(
    data_dir: &Path,
    shard_id: u16,
    key: &crate::encryption::EncryptionKey,
    target: &mut impl Recover,
) -> RecoveryResult {
    recover_shard_impl(data_dir, shard_id, Some(key), target)
}

fn recover_shard_impl(
    data_dir: &Path,
    shard_id: u16,
    #[allow(unused_variables)] key: Option<EncryptionKeyRef<'_>>,
    target: &mut impl Recover,
) -> RecoveryResult {
    let mut result = RecoveryResult::default();

    let snap_path = snapshot::snapshot_path(data_dir, shard_id);
    let mut snapshot_crc = None;
    if snap_path.exists() {
        match load_snapshot(&snap_path, shard_id, key) {
            Ok((entries, crc)) => {
                for (key, value, ttl) in entries {
                    target.restore(key, value, ttl);
                }
                result.loaded_snapshot = true;
                snapshot_crc = Some(crc);
            }
            Err(e) => warn!(shard_id, "failed to load snapshot, starting empty: {e}"),
        }
    }

    let aof_path = aof::aof_path(data_dir, shard_id);
    if !aof_path.exists() {
        return result;
    }
    let start = match scan_aof(&aof_path, snapshot_crc, key) {
        Ok(scan) => {
            if scan.stale {
                warn!(
                    shard_id,
                    "aof was written before the snapshot; skipping its records"
                );
                result.stale_aof = true;
            }
            scan.start
        }
        Err(e) => {
            warn!(
                shard_id,
                "aof is corrupt mid-file; skipped all of it and recovered snapshot state \
                 only (writes since the last snapshot are lost): {e}"
            );
            return result;
        }
    };
    match replay_aof(&aof_path, start, key, target) {
        Ok(count) => result.replayed_aof = count > 0,
        // the first pass read the same file without trouble
        Err(e) => error!(shard_id, "aof replay failed after a clean scan: {e}"),
    }
    result
}

/// A snapshot entry as (key, value, TTL left).
type LoadedEntry = (String, SnapValue, Option<Duration>);

/// Loads the live entries from a snapshot file, leaving out keys whose TTL
/// ran out since it was written. Also returns the snapshot's footer CRC,
/// which the AOF's checkpoint record refers to.
fn load_snapshot(
    path: &Path,
    expected_shard_id: u16,
    #[allow(unused_variables)] encryption_key: Option<EncryptionKeyRef<'_>>,
) -> Result<(Vec<LoadedEntry>, u32), FormatError> {
    #[cfg(feature = "encryption")]
    let mut reader = if let Some(key) = encryption_key {
        SnapshotReader::open_encrypted(path, key.clone())?
    } else {
        SnapshotReader::open(path)?
    };
    #[cfg(not(feature = "encryption"))]
    let mut reader = SnapshotReader::open(path)?;

    if reader.shard_id != expected_shard_id {
        return Err(FormatError::InvalidData(format!(
            "snapshot shard_id {} does not match expected {}",
            reader.shard_id, expected_shard_id
        )));
    }

    // entries store the TTL left when the snapshot was written. the file's
    // modification time is that moment, so the time since then has passed
    // for every key too.
    let written_ago_ms = std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .map_or(0, |d| d.as_millis().min(i64::MAX as u128) as i64);

    let mut entries = Vec::new();
    while let Some(entry) = reader.read_entry()? {
        let ttl = match entry.expire_ms {
            ms if ms < 0 => None,
            ms if ms <= written_ago_ms => continue,
            ms => Some(Duration::from_millis((ms - written_ago_ms) as u64)),
        };
        entries.push((entry.key, entry.value, ttl));
    }

    let crc = reader.verify_footer()?;
    Ok((entries, crc))
}

/// Where replay of an AOF starts, as found by [`scan_aof`].
struct AofScan {
    /// Index of the first record to replay. Past the end for a stale AOF.
    start: usize,
    /// The AOF came before the loaded snapshot.
    stale: bool,
}

/// The first pass over the AOF: decodes every record without applying any,
/// finds where replay starts, and cuts off a partial record at the end.
fn scan_aof(
    path: &Path,
    snapshot_crc: Option<u32>,
    key: Option<EncryptionKeyRef<'_>>,
) -> Result<AofScan, FormatError> {
    let mut reader = open_aof(path, key)?;
    let (mut index, mut start, mut stale) = (0, 0, false);
    while let Some(record) = reader.read_record()? {
        if let AofRecord::Checkpoint { snapshot_crc: crc } = record {
            match snapshot_crc {
                Some(loaded) if loaded == crc => {
                    start = index + 1;
                    stale = false;
                }
                Some(_) if index == 0 => stale = true,
                _ => {}
            }
        }
        index += 1;
    }
    truncate_torn_tail(path, reader.valid_len())?;
    if stale {
        start = usize::MAX;
    }
    Ok(AofScan { start, stale })
}

/// The second pass: hands the records from index `start` on to `target`.
/// Returns how many it applied.
fn replay_aof(
    path: &Path,
    start: usize,
    key: Option<EncryptionKeyRef<'_>>,
    target: &mut impl Recover,
) -> Result<usize, FormatError> {
    let mut reader = open_aof(path, key)?;
    let mut count = 0;
    let mut index = 0;
    while let Some(record) = reader.read_record()? {
        if (index >= start || always_replayed(&record))
            && !matches!(record, AofRecord::Checkpoint { .. })
        {
            target.apply(record);
            count += 1;
        }
        index += 1;
    }
    Ok(count)
}

/// Records replayed even before the replay start. Schemas live only in the
/// AOF, never in the snapshot, and registering one again is harmless.
fn always_replayed(#[allow(unused_variables)] record: &AofRecord) -> bool {
    #[cfg(feature = "protobuf")]
    if matches!(record, AofRecord::ProtoRegister { .. }) {
        return true;
    }
    false
}

fn open_aof(
    path: &Path,
    #[allow(unused_variables)] key: Option<EncryptionKeyRef<'_>>,
) -> Result<AofReader, FormatError> {
    #[cfg(feature = "encryption")]
    if let Some(key) = key {
        return AofReader::open_encrypted(path, key.clone());
    }
    AofReader::open(path)
}

/// Cuts off a partial record left at the end of the AOF by a crash.
///
/// The writer appends at end of file. If the partial bytes stayed, the next
/// records would land after them, and the following recovery would misread
/// those records as part of the partial one.
fn truncate_torn_tail(path: &Path, valid_len: u64) -> Result<(), FormatError> {
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    let file_len = file.metadata()?.len();
    if file_len > valid_len {
        warn!(
            path = %path.display(),
            dropped_bytes = file_len - valid_len,
            "aof ends with a partial record, truncating it"
        );
        file.set_len(valid_len)?;
        file.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::AofWriter;
    use crate::snapshot::{SnapEntry, SnapshotWriter};
    use bytes::Bytes;

    /// A recovery target that records what it is given.
    #[derive(Default)]
    struct Recorded {
        restored: Vec<(String, Option<Duration>)>,
        applied: Vec<AofRecord>,
    }

    impl Recover for Recorded {
        fn restore(&mut self, key: String, _value: SnapValue, ttl: Option<Duration>) {
            self.restored.push((key, ttl));
        }

        fn apply(&mut self, record: AofRecord) {
            self.applied.push(record);
        }
    }

    fn recover(dir: &Path) -> (Recorded, RecoveryResult) {
        let mut target = Recorded::default();
        let result = recover_shard(dir, 0, &mut target);
        (target, result)
    }

    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("create temp dir")
    }

    /// Appends `records` to shard 0's AOF in `dir`, returning the offset
    /// where each record ends.
    fn write_aof(dir: &Path, records: &[AofRecord]) -> Vec<u64> {
        let path = aof::aof_path(dir, 0);
        let mut writer = AofWriter::open(&path).unwrap();
        records
            .iter()
            .map(|record| {
                writer.write_record(record).unwrap();
                writer.sync().unwrap();
                std::fs::metadata(&path).unwrap().len()
            })
            .collect()
    }

    /// Writes a snapshot of shard 0 holding `keys` and returns its CRC.
    fn write_snapshot(dir: &Path, keys: &[(&str, i64)]) -> u32 {
        let mut writer = SnapshotWriter::create(snapshot::snapshot_path(dir, 0), 0).unwrap();
        for &(key, expire_ms) in keys {
            writer
                .write_entry(&SnapEntry {
                    key: key.into(),
                    value: SnapValue::String(Bytes::from("v")),
                    expire_ms,
                })
                .unwrap();
        }
        writer.finish().unwrap()
    }

    fn del(key: &str) -> AofRecord {
        AofRecord::Del { key: key.into() }
    }

    fn checkpoint(snapshot_crc: u32) -> AofRecord {
        AofRecord::Checkpoint { snapshot_crc }
    }

    #[test]
    fn empty_dir_recovers_nothing() {
        let dir = temp_dir();
        let (target, result) = recover(dir.path());
        assert_eq!(result, RecoveryResult::default());
        assert!(target.restored.is_empty() && target.applied.is_empty());
    }

    #[test]
    fn snapshot_then_aof() {
        let dir = temp_dir();
        write_snapshot(dir.path(), &[("a", -1), ("b", 60_000)]);
        write_aof(dir.path(), &[del("a")]);

        let (target, result) = recover(dir.path());
        assert!(result.loaded_snapshot && result.replayed_aof);
        assert_eq!(target.restored.len(), 2);
        assert!(target
            .restored
            .iter()
            .any(|(k, ttl)| k == "b" && ttl.is_some()));
        assert_eq!(target.applied, [del("a")]);
    }

    #[test]
    fn snapshot_ttls_count_the_time_since_it_was_written() {
        let dir = temp_dir();
        write_snapshot(dir.path(), &[("short", 60_000), ("forever", -1)]);
        // the snapshot was taken an hour ago
        let hour_ago = std::time::SystemTime::now() - Duration::from_secs(3600);
        std::fs::File::options()
            .write(true)
            .open(snapshot::snapshot_path(dir.path(), 0))
            .unwrap()
            .set_modified(hour_ago)
            .unwrap();

        let (target, _) = recover(dir.path());
        assert_eq!(target.restored, [("forever".to_string(), None)]);
    }

    #[test]
    fn corrupt_snapshot_is_skipped_and_the_aof_still_replays() {
        let dir = temp_dir();
        std::fs::write(snapshot::snapshot_path(dir.path(), 0), b"garbage").unwrap();
        write_aof(dir.path(), &[del("a")]);

        let (target, result) = recover(dir.path());
        assert!(!result.loaded_snapshot);
        assert_eq!(target.applied, [del("a")]);
    }

    #[test]
    fn aof_corrupt_mid_file_replays_nothing() {
        // applying the good records before the damage could leave a
        // prefix that doesn't match any state the server was in
        let dir = temp_dir();
        let ends = write_aof(dir.path(), &[del("a"), del("b"), del("c")]);
        let path = aof::aof_path(dir.path(), 0);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[ends[1] as usize - 1] ^= 0xFF; // the second record's CRC
        std::fs::write(&path, bytes).unwrap();

        let (target, result) = recover(dir.path());
        assert!(!result.replayed_aof);
        assert!(target.applied.is_empty());
    }

    #[test]
    fn writes_after_a_torn_tail_survive_the_next_restart() {
        use std::io::Write;

        let dir = temp_dir();
        write_aof(dir.path(), &[del("a")]);
        // a crash halfway through writing the next record
        let mut partial = del("b").to_bytes().unwrap();
        partial.truncate(partial.len() / 2);
        std::fs::OpenOptions::new()
            .append(true)
            .open(aof::aof_path(dir.path(), 0))
            .unwrap()
            .write_all(&partial)
            .unwrap();

        assert_eq!(recover(dir.path()).0.applied, [del("a")]);
        write_aof(dir.path(), &[del("c")]);
        assert_eq!(recover(dir.path()).0.applied, [del("a"), del("c")]);
    }

    #[test]
    fn aof_after_its_snapshot_is_replayed() {
        let dir = temp_dir();
        let crc = write_snapshot(dir.path(), &[]);
        write_aof(dir.path(), &[checkpoint(crc), del("a")]);

        let (target, result) = recover(dir.path());
        assert_eq!(target.applied, [del("a")]);
        assert!(!result.stale_aof);
    }

    #[test]
    fn aof_from_before_the_snapshot_is_skipped() {
        // the snapshot was saved, then the server stopped before the AOF
        // was truncated; its records are already in the snapshot
        let dir = temp_dir();
        let crc = write_snapshot(dir.path(), &[]);
        write_aof(dir.path(), &[checkpoint(crc ^ 1), del("a")]);

        let (target, result) = recover(dir.path());
        assert!(target.applied.is_empty());
        assert!(result.stale_aof);
    }

    #[test]
    fn replay_starts_after_the_checkpoint_for_the_snapshot() {
        // truncating failed after the snapshot, so a checkpoint was
        // appended to the old AOF instead
        let dir = temp_dir();
        let crc = write_snapshot(dir.path(), &[]);
        write_aof(
            dir.path(),
            &[checkpoint(crc ^ 1), del("a"), checkpoint(crc), del("b")],
        );

        let (target, result) = recover(dir.path());
        assert_eq!(target.applied, [del("b")]);
        assert!(!result.stale_aof);
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn schemas_are_replayed_from_a_stale_aof() {
        let dir = temp_dir();
        let crc = write_snapshot(dir.path(), &[]);
        let register = AofRecord::ProtoRegister {
            name: "s".into(),
            descriptor: Bytes::from("d"),
        };
        write_aof(
            dir.path(),
            &[checkpoint(crc ^ 1), register.clone(), del("a")],
        );

        let (target, result) = recover(dir.path());
        assert!(result.stale_aof);
        assert_eq!(target.applied, [register]);
    }
}
