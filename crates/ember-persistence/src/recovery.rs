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
use std::time::Duration;

use bytes::Bytes;
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

/// The value of a recovered entry.
#[derive(Debug, Clone)]
pub enum RecoveredValue {
    String(Bytes),
    List(VecDeque<Bytes>),
    /// Sorted set stored as (score, member) pairs.
    SortedSet(Vec<(f64, String)>),
    /// Hash map of field names to values.
    Hash(HashMap<String, Bytes>),
    /// Unordered set of unique string members.
    Set(HashSet<String>),
    /// A vector set: index config + accumulated (element, vector) pairs.
    #[cfg(feature = "vector")]
    Vector {
        metric: u8,
        quantization: u8,
        connectivity: u32,
        expansion_add: u32,
        elements: Vec<(String, Vec<f32>)>,
    },
    /// A protobuf message: type name + serialized bytes.
    #[cfg(feature = "protobuf")]
    Proto {
        type_name: String,
        data: Bytes,
    },
}

impl From<SnapValue> for RecoveredValue {
    fn from(sv: SnapValue) -> Self {
        match sv {
            SnapValue::String(data) => RecoveredValue::String(data),
            SnapValue::List(deque) => RecoveredValue::List(deque),
            SnapValue::SortedSet(members) => RecoveredValue::SortedSet(members),
            SnapValue::Hash(map) => RecoveredValue::Hash(map),
            SnapValue::Set(set) => RecoveredValue::Set(set),
            #[cfg(feature = "vector")]
            SnapValue::Vector {
                metric,
                quantization,
                connectivity,
                expansion_add,
                elements,
                ..
            } => RecoveredValue::Vector {
                metric,
                quantization,
                connectivity,
                expansion_add,
                elements,
            },
            #[cfg(feature = "protobuf")]
            SnapValue::Proto { type_name, data } => RecoveredValue::Proto { type_name, data },
        }
    }
}

/// A single recovered entry ready to be inserted into a keyspace.
#[derive(Debug, Clone)]
pub struct RecoveredEntry {
    pub key: String,
    pub value: RecoveredValue,
    /// Remaining TTL. `None` means no expiration.
    pub ttl: Option<Duration>,
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
    /// Schemas found in the AOF, deduplicated by name (last wins).
    /// Each entry is `(schema_name, descriptor_bytes)`.
    #[cfg(feature = "protobuf")]
    pub schemas: Vec<(String, Bytes)>,
}

/// Recovers a shard's state from snapshot and/or AOF files.
///
/// Returns a list of live entries to restore into the keyspace.
/// Entries whose TTL expired during downtime are silently skipped.
pub fn recover_shard(data_dir: &Path, shard_id: u16) -> RecoveryResult {
    recover_shard_impl(data_dir, shard_id, None)
}

/// Recovers a shard's state with an encryption key for decrypting
/// v3 persistence files. Also handles plaintext v2 files transparently.
#[cfg(feature = "encryption")]
pub fn recover_shard_encrypted(
    data_dir: &Path,
    shard_id: u16,
    key: crate::encryption::EncryptionKey,
) -> RecoveryResult {
    recover_shard_impl(data_dir, shard_id, Some(&key))
}

/// Shared implementation. When encryption is not compiled in, the key
/// parameter is always `None` and all encryption branches are dead code
/// that the compiler will remove.
fn recover_shard_impl(
    data_dir: &Path,
    shard_id: u16,
    #[allow(unused_variables)] encryption_key: Option<EncryptionKeyRef<'_>>,
) -> RecoveryResult {
    // Track remaining TTL in ms (-1 = no expiry, 0+ = remaining ms)
    let mut map: HashMap<String, (RecoveredValue, i64)> = HashMap::new();
    let mut loaded_snapshot = false;
    let mut replayed_aof = false;
    #[cfg(feature = "protobuf")]
    let mut schema_map: HashMap<String, Bytes> = HashMap::new();

    // step 1: load snapshot
    let snap_path = snapshot::snapshot_path(data_dir, shard_id);
    if snap_path.exists() {
        match load_snapshot(&snap_path, shard_id, encryption_key) {
            Ok(entries) => {
                for (key, value, ttl_ms) in entries {
                    map.insert(key, (RecoveredValue::from(value), ttl_ms));
                }
                loaded_snapshot = true;
            }
            Err(e) => {
                warn!(shard_id, "failed to load snapshot, starting empty: {e}");
            }
        }
    }

    // step 2: replay AOF
    let aof_path = aof::aof_path(data_dir, shard_id);
    if aof_path.exists() {
        match replay_aof(
            &aof_path,
            &mut map,
            encryption_key,
            #[cfg(feature = "protobuf")]
            &mut schema_map,
        ) {
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

    // step 3: filter out expired entries (ttl_ms == 0) and build result.
    //
    // TTL values are preserved as-is during replay. Keys that expired while
    // the server was down (ttl_ms == 0 after decrement in replay_aof) are
    // skipped here. Any keys with positive remaining TTL will be lazily evicted
    // on first access after startup.
    let entries = map
        .into_iter()
        .filter(|(_, (_, ttl_ms))| *ttl_ms != 0) // 0 means expired, -1 means no expiry
        .map(|(key, (value, ttl_ms))| RecoveredEntry {
            key,
            value,
            ttl: if ttl_ms < 0 {
                None
            } else {
                Some(Duration::from_millis(ttl_ms as u64))
            },
        })
        .collect();

    RecoveryResult {
        entries,
        loaded_snapshot,
        replayed_aof,
        #[cfg(feature = "protobuf")]
        schemas: schema_map.into_iter().collect(),
    }
}

/// Loads entries from a snapshot file.
/// Returns (key, value, ttl_ms) where ttl_ms is -1 for no expiry.
fn load_snapshot(
    path: &Path,
    expected_shard_id: u16,
    #[allow(unused_variables)] encryption_key: Option<EncryptionKeyRef<'_>>,
) -> Result<Vec<(String, SnapValue, i64)>, FormatError> {
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

    let mut entries = Vec::new();

    while let Some(entry) = reader.read_entry()? {
        // entry.expire_ms is -1 for no expiry, or remaining ms
        entries.push((entry.key, entry.value, entry.expire_ms));
    }

    reader.verify_footer()?;
    Ok(entries)
}

/// Applies an increment/decrement to a recovered entry. If the key doesn't
/// exist, initializes it to "0" first.
///
/// Emits a warning and leaves the entry unchanged if the stored value is not
/// a valid integer. This matches the liveness policy used at runtime (an error
/// response rather than a crash), and gives operators a signal that the AOF
/// contains unexpected data.
fn apply_incr(map: &mut HashMap<String, (RecoveredValue, i64)>, key: String, delta: i64) {
    // -1 means no expiry
    let entry = map
        .entry(key.clone())
        .or_insert_with(|| (RecoveredValue::String(Bytes::from("0")), -1));
    if let RecoveredValue::String(ref mut data) = entry.0 {
        let current = std::str::from_utf8(data)
            .ok()
            .and_then(|s| s.parse::<i64>().ok());
        if let Some(n) = current {
            if let Some(new_val) = n.checked_add(delta) {
                *data = Bytes::from(new_val.to_string());
            } else {
                error!(
                    key = %key,
                    value = n,
                    delta,
                    "INCR overflow during AOF replay: value unchanged — AOF may be corrupt"
                );
            }
        } else {
            error!(
                key = %key,
                "skipping INCR replay: stored value is not an integer — AOF may be corrupt"
            );
        }
    }
}

/// Replays AOF records into the in-memory map. Returns the number of
/// records replayed. TTL is stored as remaining ms (-1 = no expiry).
fn replay_aof(
    path: &Path,
    map: &mut HashMap<String, (RecoveredValue, i64)>,
    #[allow(unused_variables)] encryption_key: Option<EncryptionKeyRef<'_>>,
    #[cfg(feature = "protobuf")] schema_map: &mut HashMap<String, Bytes>,
) -> Result<usize, FormatError> {
    #[cfg(feature = "encryption")]
    let mut reader = if let Some(key) = encryption_key {
        AofReader::open_encrypted(path, key.clone())?
    } else {
        AofReader::open(path)?
    };
    #[cfg(not(feature = "encryption"))]
    let mut reader = AofReader::open(path)?;
    let mut count = 0;

    while let Some(record) = reader.read_record()? {
        match record {
            AofRecord::Set {
                key,
                value,
                expire_ms,
            } => {
                // expire_ms is -1 for no expiry, or remaining ms
                map.insert(key, (RecoveredValue::String(value), expire_ms));
            }
            AofRecord::Del { key } => {
                map.remove(&key);
            }
            AofRecord::Expire { key, seconds } => {
                if let Some(entry) = map.get_mut(&key) {
                    entry.1 = seconds.saturating_mul(1000).min(i64::MAX as u64) as i64;
                }
            }
            AofRecord::LPush { key, values } => {
                let entry = map
                    .entry(key)
                    .or_insert_with(|| (RecoveredValue::List(VecDeque::new()), -1));
                if let RecoveredValue::List(ref mut deque) = entry.0 {
                    for v in values {
                        deque.push_front(v);
                    }
                }
            }
            AofRecord::RPush { key, values } => {
                let entry = map
                    .entry(key)
                    .or_insert_with(|| (RecoveredValue::List(VecDeque::new()), -1));
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
                    .or_insert_with(|| (RecoveredValue::SortedSet(Vec::new()), -1));
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
                        let to_remove: HashSet<&str> = members.iter().map(|m| m.as_str()).collect();
                        existing.retain(|(_, m)| !to_remove.contains(m.as_str()));
                        if existing.is_empty() {
                            map.remove(&key);
                            count += 1;
                            continue;
                        }
                    }
                }
            }
            AofRecord::Persist { key } => {
                if let Some(entry) = map.get_mut(&key) {
                    entry.1 = -1; // -1 means no expiry
                }
            }
            AofRecord::Pexpire { key, milliseconds } => {
                if let Some(entry) = map.get_mut(&key) {
                    entry.1 = milliseconds.min(i64::MAX as u64) as i64;
                }
            }
            AofRecord::Incr { key } => {
                apply_incr(map, key, 1);
            }
            AofRecord::Decr { key } => {
                apply_incr(map, key, -1);
            }
            AofRecord::IncrBy { key, delta } => {
                apply_incr(map, key, delta);
            }
            AofRecord::DecrBy { key, delta } => {
                apply_incr(map, key, delta.saturating_neg());
            }
            AofRecord::Append { key, value } => {
                let entry = map
                    .entry(key)
                    .or_insert_with(|| (RecoveredValue::String(Bytes::new()), -1));
                if let RecoveredValue::String(ref mut data) = entry.0 {
                    let mut new_data = Vec::with_capacity(data.len() + value.len());
                    new_data.extend_from_slice(data);
                    new_data.extend_from_slice(&value);
                    *data = Bytes::from(new_data);
                }
            }
            AofRecord::Rename { key, newkey } => {
                if let Some(entry) = map.remove(&key) {
                    map.insert(newkey, entry);
                }
            }
            AofRecord::HSet { key, fields } => {
                let entry = map
                    .entry(key)
                    .or_insert_with(|| (RecoveredValue::Hash(HashMap::new()), -1));
                if let RecoveredValue::Hash(ref mut hash) = entry.0 {
                    for (field, value) in fields {
                        hash.insert(field, value);
                    }
                }
            }
            AofRecord::HDel { key, fields } => {
                if let Some(entry) = map.get_mut(&key) {
                    if let RecoveredValue::Hash(ref mut hash) = entry.0 {
                        for field in fields {
                            hash.remove(&field);
                        }
                        if hash.is_empty() {
                            map.remove(&key);
                            count += 1;
                            continue;
                        }
                    }
                }
            }
            AofRecord::HIncrBy { key, field, delta } => {
                let entry = map
                    .entry(key)
                    .or_insert_with(|| (RecoveredValue::Hash(HashMap::new()), -1));
                if let RecoveredValue::Hash(ref mut hash) = entry.0 {
                    let current: i64 = hash
                        .get(&field)
                        .and_then(|v| std::str::from_utf8(v).ok())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    let new_val = current.saturating_add(delta);
                    hash.insert(field, Bytes::from(new_val.to_string()));
                }
            }
            AofRecord::SAdd { key, members } => {
                let entry = map
                    .entry(key)
                    .or_insert_with(|| (RecoveredValue::Set(HashSet::new()), -1));
                if let RecoveredValue::Set(ref mut set) = entry.0 {
                    for member in members {
                        set.insert(member);
                    }
                }
            }
            AofRecord::SRem { key, members } => {
                if let Some(entry) = map.get_mut(&key) {
                    if let RecoveredValue::Set(ref mut set) = entry.0 {
                        for member in members {
                            set.remove(&member);
                        }
                        if set.is_empty() {
                            map.remove(&key);
                            count += 1;
                            continue;
                        }
                    }
                }
            }
            #[cfg(feature = "vector")]
            AofRecord::VAdd {
                key,
                element,
                vector,
                metric,
                quantization,
                connectivity,
                expansion_add,
            } => {
                let entry = map.entry(key).or_insert_with(|| {
                    (
                        RecoveredValue::Vector {
                            metric,
                            quantization,
                            connectivity,
                            expansion_add,
                            elements: Vec::new(),
                        },
                        -1, // no expiry for vector sets
                    )
                });
                if let RecoveredValue::Vector {
                    ref mut elements, ..
                } = entry.0
                {
                    // replace existing element or add new
                    if let Some(pos) = elements.iter().position(|(e, _)| *e == element) {
                        elements[pos].1 = vector;
                    } else {
                        elements.push((element, vector));
                    }
                }
            }
            #[cfg(feature = "vector")]
            AofRecord::VRem { key, element } => {
                if let Some(entry) = map.get_mut(&key) {
                    if let RecoveredValue::Vector {
                        ref mut elements, ..
                    } = entry.0
                    {
                        elements.retain(|(e, _)| *e != element);
                        if elements.is_empty() {
                            map.remove(&key);
                        }
                    }
                }
            }
            #[cfg(feature = "protobuf")]
            AofRecord::ProtoSet {
                key,
                type_name,
                data,
                expire_ms,
            } => {
                map.insert(key, (RecoveredValue::Proto { type_name, data }, expire_ms));
            }
            #[cfg(feature = "protobuf")]
            AofRecord::ProtoRegister { name, descriptor } => {
                // last-wins: if the same schema name appears multiple times
                // in the AOF, the final registration is the one we keep.
                schema_map.insert(name, descriptor);
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
                .write_record(&AofRecord::Del { key: "gone".into() })
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
        assert!(result.entries[0].ttl.is_some());
    }

    #[test]
    fn persist_record_removes_ttl() {
        let dir = temp_dir();
        let path = aof::aof_path(dir.path(), 0);

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "k".into(),
                    value: Bytes::from("v"),
                    expire_ms: 60_000,
                })
                .unwrap();
            writer
                .write_record(&AofRecord::Persist { key: "k".into() })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert_eq!(result.entries.len(), 1);
        assert!(result.entries[0].ttl.is_none());
    }

    #[test]
    fn incr_decr_replay() {
        let dir = temp_dir();
        let path = aof::aof_path(dir.path(), 0);

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "n".into(),
                    value: Bytes::from("10"),
                    expire_ms: -1,
                })
                .unwrap();
            writer
                .write_record(&AofRecord::Incr { key: "n".into() })
                .unwrap();
            writer
                .write_record(&AofRecord::Incr { key: "n".into() })
                .unwrap();
            writer
                .write_record(&AofRecord::Decr { key: "n".into() })
                .unwrap();
            // also test INCR on a new key
            writer
                .write_record(&AofRecord::Incr {
                    key: "fresh".into(),
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        let map: HashMap<_, _> = result
            .entries
            .iter()
            .map(|e| (e.key.as_str(), e.value.clone()))
            .collect();

        // 10 + 1 + 1 - 1 = 11
        match &map["n"] {
            RecoveredValue::String(data) => assert_eq!(data, &Bytes::from("11")),
            other => panic!("expected String(\"11\"), got {other:?}"),
        }
        // 0 + 1 = 1
        match &map["fresh"] {
            RecoveredValue::String(data) => assert_eq!(data, &Bytes::from("1")),
            other => panic!("expected String(\"1\"), got {other:?}"),
        }
    }

    #[test]
    fn pexpire_record_sets_ttl() {
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
                .write_record(&AofRecord::Pexpire {
                    key: "k".into(),
                    milliseconds: 5000,
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert_eq!(result.entries.len(), 1);
        assert!(result.entries[0].ttl.is_some());
    }

    #[cfg(feature = "vector")]
    #[test]
    fn vector_snapshot_recovery() {
        let dir = temp_dir();
        let path = snapshot::snapshot_path(dir.path(), 0);

        {
            let mut writer = SnapshotWriter::create(&path, 0).unwrap();
            writer
                .write_entry(&SnapEntry {
                    key: "embeddings".into(),
                    value: SnapValue::Vector {
                        metric: 0,
                        quantization: 0,
                        connectivity: 16,
                        expansion_add: 64,
                        dim: 3,
                        elements: vec![
                            ("doc1".into(), vec![1.0, 0.0, 0.0]),
                            ("doc2".into(), vec![0.0, 1.0, 0.0]),
                        ],
                    },
                    expire_ms: -1,
                })
                .unwrap();
            writer.finish().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert!(result.loaded_snapshot);
        assert_eq!(result.entries.len(), 1);
        match &result.entries[0].value {
            RecoveredValue::Vector {
                metric,
                quantization,
                elements,
                ..
            } => {
                assert_eq!(*metric, 0);
                assert_eq!(*quantization, 0);
                assert_eq!(elements.len(), 2);
                // dim is inferred from the vector length
                assert_eq!(elements[0].1.len(), 3);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[cfg(feature = "vector")]
    #[test]
    fn vector_aof_replay() {
        let dir = temp_dir();
        let path = aof::aof_path(dir.path(), 0);

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::VAdd {
                    key: "vecs".into(),
                    element: "a".into(),
                    vector: vec![1.0, 0.0, 0.0],
                    metric: 0,
                    quantization: 0,
                    connectivity: 16,
                    expansion_add: 64,
                })
                .unwrap();
            writer
                .write_record(&AofRecord::VAdd {
                    key: "vecs".into(),
                    element: "b".into(),
                    vector: vec![0.0, 1.0, 0.0],
                    metric: 0,
                    quantization: 0,
                    connectivity: 16,
                    expansion_add: 64,
                })
                .unwrap();
            writer
                .write_record(&AofRecord::VRem {
                    key: "vecs".into(),
                    element: "a".into(),
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert!(result.replayed_aof);
        assert_eq!(result.entries.len(), 1);
        match &result.entries[0].value {
            RecoveredValue::Vector { elements, .. } => {
                assert_eq!(elements.len(), 1);
                assert_eq!(elements[0].0, "b");
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[cfg(feature = "vector")]
    #[test]
    fn vector_vrem_auto_deletes_empty() {
        let dir = temp_dir();
        let path = aof::aof_path(dir.path(), 0);

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::VAdd {
                    key: "vecs".into(),
                    element: "only".into(),
                    vector: vec![1.0, 2.0],
                    metric: 0,
                    quantization: 0,
                    connectivity: 16,
                    expansion_add: 64,
                })
                .unwrap();
            writer
                .write_record(&AofRecord::VRem {
                    key: "vecs".into(),
                    element: "only".into(),
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert!(result.entries.is_empty());
    }

    #[cfg(feature = "protobuf")]
    #[test]
    fn proto_schemas_recovered_from_aof() {
        let dir = temp_dir();
        let path = aof::aof_path(dir.path(), 0);

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::ProtoRegister {
                    name: "users".into(),
                    descriptor: Bytes::from("fake-descriptor-a"),
                })
                .unwrap();
            // a proto value that depends on the schema
            writer
                .write_record(&AofRecord::ProtoSet {
                    key: "user:1".into(),
                    type_name: "test.User".into(),
                    data: Bytes::from("some-proto-data"),
                    expire_ms: -1,
                })
                .unwrap();
            // re-registration of same schema (last wins)
            writer
                .write_record(&AofRecord::ProtoRegister {
                    name: "users".into(),
                    descriptor: Bytes::from("fake-descriptor-b"),
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let result = recover_shard(dir.path(), 0);
        assert!(result.replayed_aof);
        assert_eq!(result.entries.len(), 1);

        // schemas should be collected with last-wins dedup
        assert_eq!(result.schemas.len(), 1);
        let (name, desc) = &result.schemas[0];
        assert_eq!(name, "users");
        assert_eq!(desc, &Bytes::from("fake-descriptor-b"));
    }
}
