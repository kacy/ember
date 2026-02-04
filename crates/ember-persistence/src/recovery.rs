//! Recovery: loading snapshots and replaying AOF on shard startup.
//!
//! The recovery sequence is:
//! 1. Load snapshot if it exists (bulk restore of entries).
//! 2. Replay AOF if it exists (apply mutations on top of snapshot state).
//! 3. Skip entries whose TTL expired during downtime.
//! 4. If no files exist, start with an empty state.
//! 5. If files are corrupt, log a warning and start empty.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tracing::warn;

use crate::aof::{self, AofReader, AofRecord};
use crate::format::FormatError;
use crate::snapshot::{self, SnapValue, SnapshotReader};

/// The value of a recovered entry.
#[derive(Debug, Clone)]
pub enum RecoveredValue {
    String(Bytes),
    List(VecDeque<Bytes>),
    /// Sorted set stored as (score, member) pairs.
    SortedSet(Vec<(f64, String)>),
}

impl From<SnapValue> for RecoveredValue {
    fn from(sv: SnapValue) -> Self {
        match sv {
            SnapValue::String(data) => RecoveredValue::String(data),
            SnapValue::List(deque) => RecoveredValue::List(deque),
            SnapValue::SortedSet(members) => RecoveredValue::SortedSet(members),
        }
    }
}

/// A single recovered entry ready to be inserted into a keyspace.
#[derive(Debug, Clone)]
pub struct RecoveredEntry {
    pub key: String,
    pub value: RecoveredValue,
    /// Absolute deadline computed from the persisted remaining TTL.
    /// `None` means no expiration.
    pub expires_at: Option<Instant>,
}

/// The result of recovering a shard's persisted state.
#[derive(Debug)]
pub struct RecoveryResult {
    /// Recovered entries, keyed by name for easy insertion.
    pub entries: Vec<RecoveredEntry>,
    /// Whether a snapshot was loaded.
    pub loaded_snapshot: bool,
    /// Whether an AOF was replayed.
    pub replayed_aof: bool,
}

/// Recovers a shard's state from snapshot and/or AOF files.
///
/// Returns a list of live entries to restore into the keyspace.
/// Entries whose TTL expired during downtime are silently skipped.
pub fn recover_shard(data_dir: &Path, shard_id: u16) -> RecoveryResult {
    let now = Instant::now();
    let mut map: HashMap<String, (RecoveredValue, Option<Instant>)> = HashMap::new();
    let mut loaded_snapshot = false;
    let mut replayed_aof = false;

    // step 1: load snapshot
    let snap_path = snapshot::snapshot_path(data_dir, shard_id);
    if snap_path.exists() {
        match load_snapshot(&snap_path, now) {
            Ok(entries) => {
                for (key, value, expires_at) in entries {
                    map.insert(key, (RecoveredValue::from(value), expires_at));
                }
                loaded_snapshot = true;
            }
            Err(e) => {
                warn!(
                    shard_id,
                    "failed to load snapshot, starting empty: {e}"
                );
            }
        }
    }

    // step 2: replay AOF
    let aof_path = aof::aof_path(data_dir, shard_id);
    if aof_path.exists() {
        match replay_aof(&aof_path, &mut map, now) {
            Ok(count) => {
                if count > 0 {
                    replayed_aof = true;
                }
            }
            Err(e) => {
                warn!(
                    shard_id,
                    "failed to replay aof, using snapshot state only: {e}"
                );
            }
        }
    }

    // step 3: filter out expired entries and build result
    let entries = map
        .into_iter()
        .filter(|(_, (_, expires_at))| {
            match expires_at {
                Some(deadline) => *deadline > now,
                None => true,
            }
        })
        .map(|(key, (value, expires_at))| RecoveredEntry {
            key,
            value,
            expires_at,
        })
        .collect();

    RecoveryResult {
        entries,
        loaded_snapshot,
        replayed_aof,
    }
}

/// Loads entries from a snapshot file.
fn load_snapshot(
    path: &Path,
    now: Instant,
) -> Result<Vec<(String, SnapValue, Option<Instant>)>, FormatError> {
    let mut reader = SnapshotReader::open(path)?;
    let mut entries = Vec::new();

    while let Some(entry) = reader.read_entry()? {
        let expires_at = if entry.expire_ms >= 0 {
            Some(now + Duration::from_millis(entry.expire_ms as u64))
        } else {
            None
        };
        entries.push((entry.key, entry.value, expires_at));
    }

    reader.verify_footer()?;
    Ok(entries)
}

/// Replays AOF records into the in-memory map. Returns the number of
/// records replayed.
fn replay_aof(
    path: &Path,
    map: &mut HashMap<String, (RecoveredValue, Option<Instant>)>,
    now: Instant,
) -> Result<usize, FormatError> {
    let mut reader = AofReader::open(path)?;
    let mut count = 0;

    while let Some(record) = reader.read_record()? {
        match record {
            AofRecord::Set {
                key,
                value,
                expire_ms,
            } => {
                let expires_at = if expire_ms >= 0 {
                    Some(now + Duration::from_millis(expire_ms as u64))
                } else {
                    None
                };
                map.insert(key, (RecoveredValue::String(value), expires_at));
            }
            AofRecord::Del { key } => {
                map.remove(&key);
            }
            AofRecord::Expire { key, seconds } => {
                if let Some(entry) = map.get_mut(&key) {
                    entry.1 = Some(now + Duration::from_secs(seconds));
                }
            }
            AofRecord::LPush { key, values } => {
                let entry = map
                    .entry(key)
                    .or_insert_with(|| (RecoveredValue::List(VecDeque::new()), None));
                if let RecoveredValue::List(ref mut deque) = entry.0 {
                    for v in values {
                        deque.push_front(v);
                    }
                }
            }
            AofRecord::RPush { key, values } => {
                let entry = map
                    .entry(key)
                    .or_insert_with(|| (RecoveredValue::List(VecDeque::new()), None));
                if let RecoveredValue::List(ref mut deque) = entry.0 {
                    for v in values {
                        deque.push_back(v);
                    }
                }
            }
            AofRecord::LPop { key } => {
                if let Some(entry) = map.get_mut(&key) {
                    if let RecoveredValue::List(ref mut deque) = entry.0 {
                        deque.pop_front();
                        if deque.is_empty() {
                            map.remove(&key);
                            count += 1;
                            continue;
                        }
                    }
                }
            }
            AofRecord::RPop { key } => {
                if let Some(entry) = map.get_mut(&key) {
                    if let RecoveredValue::List(ref mut deque) = entry.0 {
                        deque.pop_back();
                        if deque.is_empty() {
                            map.remove(&key);
                            count += 1;
                            continue;
                        }
                    }
                }
            }
            AofRecord::ZAdd { key, members } => {
                let entry = map
                    .entry(key)
                    .or_insert_with(|| (RecoveredValue::SortedSet(Vec::new()), None));
                if let RecoveredValue::SortedSet(ref mut existing) = entry.0 {
                    // build a position index for O(1) member lookups
                    let mut index: HashMap<String, usize> = existing
                        .iter()
                        .enumerate()
                        .map(|(i, (_, m))| (m.clone(), i))
                        .collect();
                    for (score, member) in members {
                        if let Some(&pos) = index.get(&member) {
                            existing[pos].0 = score;
                        } else {
                            let pos = existing.len();
                            index.insert(member.clone(), pos);
                            existing.push((score, member));
                        }
                    }
                }
            }
            AofRecord::ZRem { key, members } => {
                if let Some(entry) = map.get_mut(&key) {
                    if let RecoveredValue::SortedSet(ref mut existing) = entry.0 {
                        let to_remove: HashSet<&str> =
                            members.iter().map(|m| m.as_str()).collect();
                        existing.retain(|(_, m)| !to_remove.contains(m.as_str()));
                        if existing.is_empty() {
                            map.remove(&key);
                            count += 1;
                            continue;
                        }
                    }
                }
            }
        }
        count += 1;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::AofWriter;
    use crate::snapshot::{SnapEntry, SnapValue, SnapshotWriter};

    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("create temp dir")
    }

    #[test]
    fn empty_dir_returns_empty_result() {
        let dir = temp_dir();
        let result = recover_shard(dir.path(), 0);
        assert!(result.entries.is_empty());
        assert!(!result.loaded_snapshot);
        assert!(!result.replayed_aof);
    }

    #[test]
    fn snapshot_only_recovery() {
        let dir = temp_dir();
        let path = snapshot::snapshot_path(dir.path(), 0);

        {
            let mut writer = SnapshotWriter::create(&path, 0).unwrap();
            writer
                .write_entry(&SnapEntry {
                    key: "a".into(),
                    value: SnapValue::String(Bytes::from("1")),
                    expire_ms: -1,
                })
                .unwrap();
            writer
                .write_entry(&SnapEntry {
                    key: "b".into(),
                    value: SnapValue::String(Bytes::from("2")),
                    expire_ms: 60_000,
                })
                .unwrap();
            writer.finish().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert!(result.loaded_snapshot);
        assert!(!result.replayed_aof);
        assert_eq!(result.entries.len(), 2);
    }

    #[test]
    fn aof_only_recovery() {
        let dir = temp_dir();
        let path = aof::aof_path(dir.path(), 0);

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "x".into(),
                    value: Bytes::from("10"),
                    expire_ms: -1,
                })
                .unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "y".into(),
                    value: Bytes::from("20"),
                    expire_ms: -1,
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert!(!result.loaded_snapshot);
        assert!(result.replayed_aof);
        assert_eq!(result.entries.len(), 2);
    }

    #[test]
    fn snapshot_plus_aof_overlay() {
        let dir = temp_dir();

        // snapshot with key "a" = "old"
        {
            let path = snapshot::snapshot_path(dir.path(), 0);
            let mut writer = SnapshotWriter::create(&path, 0).unwrap();
            writer
                .write_entry(&SnapEntry {
                    key: "a".into(),
                    value: SnapValue::String(Bytes::from("old")),
                    expire_ms: -1,
                })
                .unwrap();
            writer.finish().unwrap();
        }

        // AOF overwrites "a" to "new" and adds "b"
        {
            let path = aof::aof_path(dir.path(), 0);
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "a".into(),
                    value: Bytes::from("new"),
                    expire_ms: -1,
                })
                .unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "b".into(),
                    value: Bytes::from("added"),
                    expire_ms: -1,
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert!(result.loaded_snapshot);
        assert!(result.replayed_aof);

        let map: HashMap<_, _> = result
            .entries
            .iter()
            .map(|e| (e.key.as_str(), e.value.clone()))
            .collect();
        assert!(matches!(&map["a"], RecoveredValue::String(b) if b == &Bytes::from("new")));
        assert!(matches!(&map["b"], RecoveredValue::String(b) if b == &Bytes::from("added")));
    }

    #[test]
    fn del_removes_entry_during_replay() {
        let dir = temp_dir();
        let path = aof::aof_path(dir.path(), 0);

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "gone".into(),
                    value: Bytes::from("temp"),
                    expire_ms: -1,
                })
                .unwrap();
            writer
                .write_record(&AofRecord::Del {
                    key: "gone".into(),
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert!(result.entries.is_empty());
    }

    #[test]
    fn expired_entries_skipped() {
        let dir = temp_dir();
        let path = snapshot::snapshot_path(dir.path(), 0);

        {
            let mut writer = SnapshotWriter::create(&path, 0).unwrap();
            // this entry has 0ms remaining — already expired
            writer
                .write_entry(&SnapEntry {
                    key: "dead".into(),
                    value: SnapValue::String(Bytes::from("gone")),
                    expire_ms: 0,
                })
                .unwrap();
            // this one has plenty of time
            writer
                .write_entry(&SnapEntry {
                    key: "alive".into(),
                    value: SnapValue::String(Bytes::from("here")),
                    expire_ms: 60_000,
                })
                .unwrap();
            writer.finish().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].key, "alive");
    }

    #[test]
    fn corrupt_snapshot_starts_empty() {
        let dir = temp_dir();
        let path = snapshot::snapshot_path(dir.path(), 0);

        std::fs::write(&path, b"garbage data").unwrap();

        let result = recover_shard(dir.path(), 0);
        assert!(!result.loaded_snapshot);
        assert!(result.entries.is_empty());
    }

    #[test]
    fn sorted_set_snapshot_recovery() {
        let dir = temp_dir();
        let path = snapshot::snapshot_path(dir.path(), 0);

        {
            let mut writer = SnapshotWriter::create(&path, 0).unwrap();
            writer
                .write_entry(&SnapEntry {
                    key: "board".into(),
                    value: SnapValue::SortedSet(vec![
                        (100.0, "alice".into()),
                        (200.0, "bob".into()),
                    ]),
                    expire_ms: -1,
                })
                .unwrap();
            writer.finish().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert!(result.loaded_snapshot);
        assert_eq!(result.entries.len(), 1);
        match &result.entries[0].value {
            RecoveredValue::SortedSet(members) => {
                assert_eq!(members.len(), 2);
                assert!(members.contains(&(100.0, "alice".into())));
                assert!(members.contains(&(200.0, "bob".into())));
            }
            other => panic!("expected SortedSet, got {other:?}"),
        }
    }

    #[test]
    fn sorted_set_aof_replay() {
        let dir = temp_dir();
        let path = aof::aof_path(dir.path(), 0);

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::ZAdd {
                    key: "board".into(),
                    members: vec![(100.0, "alice".into()), (200.0, "bob".into())],
                })
                .unwrap();
            writer
                .write_record(&AofRecord::ZRem {
                    key: "board".into(),
                    members: vec!["alice".into()],
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert!(result.replayed_aof);
        assert_eq!(result.entries.len(), 1);
        match &result.entries[0].value {
            RecoveredValue::SortedSet(members) => {
                assert_eq!(members.len(), 1);
                assert_eq!(members[0], (200.0, "bob".into()));
            }
            other => panic!("expected SortedSet, got {other:?}"),
        }
    }

    #[test]
    fn sorted_set_zrem_auto_deletes_empty() {
        let dir = temp_dir();
        let path = aof::aof_path(dir.path(), 0);

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::ZAdd {
                    key: "board".into(),
                    members: vec![(100.0, "alice".into())],
                })
                .unwrap();
            writer
                .write_record(&AofRecord::ZRem {
                    key: "board".into(),
                    members: vec!["alice".into()],
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert!(result.entries.is_empty());
    }

    #[test]
    fn expire_record_updates_ttl() {
        let dir = temp_dir();
        let path = aof::aof_path(dir.path(), 0);

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "k".into(),
                    value: Bytes::from("v"),
                    expire_ms: -1,
                })
                .unwrap();
            writer
                .write_record(&AofRecord::Expire {
                    key: "k".into(),
                    seconds: 300,
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert_eq!(result.entries.len(), 1);
        assert!(result.entries[0].expires_at.is_some());
    }
}
