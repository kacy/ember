use super::*;

/// Clamps a duration's millisecond value to fit in i64.
///
/// `Duration::as_millis()` returns u128 which silently wraps when cast
/// to i64 for TTLs longer than ~292 million years. This caps at i64::MAX
/// instead, preserving "very long TTL" semantics without sign corruption.
pub(super) fn duration_to_expire_ms(d: Duration) -> i64 {
    let ms = d.as_millis();
    if ms > i64::MAX as u128 {
        i64::MAX
    } else {
        ms as i64
    }
}

/// Converts a successful mutation request+response pair into AOF records.
///
/// Takes ownership of the request to move keys and values directly into
/// the records, avoiding heap allocations from cloning. Returns SmallVec
/// since 99%+ of commands produce 0 or 1 records.
pub(super) fn to_aof_records(
    req: ShardRequest,
    resp: &mut ShardResponse,
) -> SmallVec<[AofRecord; 1]> {
    match (req, resp) {
        (
            ShardRequest::Set {
                key, value, expire, ..
            },
            ShardResponse::Ok,
        ) => {
            let expire_ms = expire.map(duration_to_expire_ms).unwrap_or(-1);
            smallvec![AofRecord::Set {
                key,
                value,
                expire_ms,
            }]
        }
        (ShardRequest::Del { key }, ShardResponse::Bool(true))
        | (ShardRequest::Unlink { key }, ShardResponse::Bool(true)) => {
            smallvec![AofRecord::Del { key }]
        }
        (ShardRequest::Expire { key, seconds }, ShardResponse::Bool(true)) => {
            smallvec![AofRecord::Expire { key, seconds }]
        }
        (ShardRequest::LPush { key, values }, ShardResponse::Len(_)) => {
            smallvec![AofRecord::LPush { key, values }]
        }
        (ShardRequest::RPush { key, values }, ShardResponse::Len(_)) => {
            smallvec![AofRecord::RPush { key, values }]
        }
        (ShardRequest::LPop { key }, ShardResponse::Value(Some(_))) => {
            smallvec![AofRecord::LPop { key }]
        }
        (ShardRequest::RPop { key }, ShardResponse::Value(Some(_))) => {
            smallvec![AofRecord::RPop { key }]
        }
        (ShardRequest::LSet { key, index, value }, ShardResponse::Ok) => {
            smallvec![AofRecord::LSet { key, index, value }]
        }
        (ShardRequest::LTrim { key, start, stop }, ShardResponse::Ok) => {
            smallvec![AofRecord::LTrim { key, start, stop }]
        }
        (
            ShardRequest::LInsert {
                key,
                before,
                pivot,
                value,
            },
            ShardResponse::Integer(n),
        ) if *n > 0 => {
            smallvec![AofRecord::LInsert {
                key,
                before,
                pivot,
                value,
            }]
        }
        (ShardRequest::LRem { key, count, value }, ShardResponse::Len(n)) if *n > 0 => {
            smallvec![AofRecord::LRem { key, count, value }]
        }
        (ShardRequest::ZAdd { key, .. }, ShardResponse::ZAddLen { applied, .. })
            if !applied.is_empty() =>
        {
            smallvec![AofRecord::ZAdd {
                key,
                members: applied.clone(),
            }]
        }
        (ShardRequest::ZRem { key, .. }, ShardResponse::ZRemLen { removed, .. })
            if !removed.is_empty() =>
        {
            smallvec![AofRecord::ZRem {
                key,
                members: removed.clone(),
            }]
        }
        // ZINCRBY: persist as a ZADD with the final score (avoids float drift on replay)
        (ShardRequest::ZIncrBy { key, .. }, ShardResponse::ZIncrByResult { new_score, member }) => {
            smallvec![AofRecord::ZAdd {
                key,
                members: vec![(*new_score, member.clone())],
            }]
        }
        // ZPOPMIN/ZPOPMAX: persist as ZREM of the removed members
        (
            ShardRequest::ZPopMin { key, .. } | ShardRequest::ZPopMax { key, .. },
            ShardResponse::ZPopResult(items),
        ) if !items.is_empty() => {
            smallvec![AofRecord::ZRem {
                key,
                members: items.iter().map(|(m, _)| m.clone()).collect(),
            }]
        }
        (ShardRequest::Incr { key }, ShardResponse::Integer(_)) => {
            smallvec![AofRecord::Incr { key }]
        }
        (ShardRequest::Decr { key }, ShardResponse::Integer(_)) => {
            smallvec![AofRecord::Decr { key }]
        }
        (ShardRequest::IncrBy { key, delta }, ShardResponse::Integer(_)) => {
            smallvec![AofRecord::IncrBy { key, delta }]
        }
        (ShardRequest::DecrBy { key, delta }, ShardResponse::Integer(_)) => {
            smallvec![AofRecord::DecrBy { key, delta }]
        }
        // INCRBYFLOAT: record as a SET with the resulting value to avoid
        // float rounding drift during replay.
        (ShardRequest::IncrByFloat { key, .. }, ShardResponse::BulkString(val)) => {
            smallvec![AofRecord::Set {
                key,
                value: Bytes::from(val.clone()),
                expire_ms: -1,
            }]
        }
        // APPEND: record the appended value for replay
        (ShardRequest::Append { key, value }, ShardResponse::Len(_)) => {
            smallvec![AofRecord::Append { key, value }]
        }
        // SETRANGE: record the offset + overlay for replay
        (ShardRequest::SetRange { key, offset, value }, ShardResponse::Len(_)) => {
            smallvec![AofRecord::SetRange { key, offset, value }]
        }
        (ShardRequest::Rename { key, newkey }, ShardResponse::Ok) => {
            smallvec![AofRecord::Rename { key, newkey }]
        }
        (
            ShardRequest::Copy {
                source,
                destination,
                replace,
            },
            ShardResponse::Bool(true),
        ) => {
            smallvec![AofRecord::Copy {
                source,
                destination,
                replace,
            }]
        }
        (ShardRequest::Persist { key }, ShardResponse::Bool(true)) => {
            smallvec![AofRecord::Persist { key }]
        }
        (ShardRequest::Pexpire { key, milliseconds }, ShardResponse::Bool(true)) => {
            smallvec![AofRecord::Pexpire { key, milliseconds }]
        }
        // Hash commands
        (ShardRequest::HSet { key, fields }, ShardResponse::Len(_)) => {
            smallvec![AofRecord::HSet { key, fields }]
        }
        (ShardRequest::HDel { key, .. }, ShardResponse::HDelLen { removed, .. })
            if !removed.is_empty() =>
        {
            smallvec![AofRecord::HDel {
                key,
                fields: removed.clone(),
            }]
        }
        (ShardRequest::HIncrBy { key, field, delta }, ShardResponse::Integer(_)) => {
            smallvec![AofRecord::HIncrBy { key, field, delta }]
        }
        // Set commands
        (ShardRequest::SAdd { key, members }, ShardResponse::Len(count)) if *count > 0 => {
            smallvec![AofRecord::SAdd { key, members }]
        }
        (ShardRequest::SRem { key, members }, ShardResponse::Len(count)) if *count > 0 => {
            smallvec![AofRecord::SRem { key, members }]
        }
        // SPOP: persist as SREM of the removed members
        (ShardRequest::SPop { key, .. }, ShardResponse::StringArray(members))
            if !members.is_empty() =>
        {
            smallvec![AofRecord::SRem {
                key,
                members: members.clone(),
            }]
        }
        // STORE commands: persist as DEL + SADD with the resulting members
        (
            ShardRequest::SUnionStore { dest, .. }
            | ShardRequest::SInterStore { dest, .. }
            | ShardRequest::SDiffStore { dest, .. },
            ShardResponse::SetStoreResult { count, members },
        ) => {
            if *count > 0 {
                smallvec![
                    AofRecord::Del { key: dest.clone() },
                    AofRecord::SAdd {
                        key: dest,
                        members: members.clone(),
                    },
                ]
            } else {
                smallvec![AofRecord::Del { key: dest }]
            }
        }
        // Proto commands
        #[cfg(feature = "protobuf")]
        (
            ShardRequest::ProtoSet {
                key,
                type_name,
                data,
                expire,
                ..
            },
            ShardResponse::Ok,
        ) => {
            let expire_ms = expire.map(duration_to_expire_ms).unwrap_or(-1);
            smallvec![AofRecord::ProtoSet {
                key,
                type_name,
                data,
                expire_ms,
            }]
        }
        #[cfg(feature = "protobuf")]
        (ShardRequest::ProtoRegisterAof { name, descriptor }, ShardResponse::Ok) => {
            smallvec![AofRecord::ProtoRegister { name, descriptor }]
        }
        // atomic field ops persist as a full ProtoSet (the whole re-encoded value)
        #[cfg(feature = "protobuf")]
        (
            ShardRequest::ProtoSetField { key, .. } | ShardRequest::ProtoDelField { key, .. },
            ShardResponse::ProtoFieldUpdated {
                type_name,
                data,
                expire,
            },
        ) => {
            let expire_ms = expire.map(duration_to_expire_ms).unwrap_or(-1);
            smallvec![AofRecord::ProtoSet {
                key,
                type_name: type_name.clone(),
                data: data.clone(),
                expire_ms,
            }]
        }
        // Vector commands
        #[cfg(feature = "vector")]
        (
            ShardRequest::VAdd {
                key,
                metric,
                quantization,
                connectivity,
                expansion_add,
                ..
            },
            ShardResponse::VAddResult {
                element, vector, ..
            },
        ) => smallvec![AofRecord::VAdd {
            key,
            element: element.clone(),
            vector: vector.clone(),
            metric,
            quantization,
            connectivity,
            expansion_add,
        }],
        // VADD_BATCH: expand each applied entry into its own AofRecord::VAdd.
        // steals applied from the response to avoid cloning vectors.
        // the connection handler only uses added_count, so the empty vec is fine.
        #[cfg(feature = "vector")]
        (
            ShardRequest::VAddBatch {
                key,
                metric,
                quantization,
                connectivity,
                expansion_add,
                ..
            },
            ShardResponse::VAddBatchResult { applied, .. },
        ) => std::mem::take(applied)
            .into_iter()
            .map(|(element, vector)| AofRecord::VAdd {
                key: key.clone(),
                element,
                vector,
                metric,
                quantization,
                connectivity,
                expansion_add,
            })
            .collect(),
        #[cfg(feature = "vector")]
        (ShardRequest::VRem { key, element }, ShardResponse::Bool(true)) => {
            smallvec![AofRecord::VRem { key, element }]
        }
        // LMOVE: persist as lpop from source + lpush to destination.
        // Only written when a value was actually moved (Value is Some).
        (
            ShardRequest::LMove {
                source,
                destination,
                src_left,
                dst_left,
            },
            ShardResponse::Value(Some(v)),
        ) => {
            let val = match v {
                Value::String(b) => b.clone(),
                _ => return SmallVec::new(),
            };
            let pop_record = if src_left {
                AofRecord::LPop { key: source }
            } else {
                AofRecord::RPop { key: source }
            };
            let push_record = if dst_left {
                AofRecord::LPush {
                    key: destination,
                    values: vec![val],
                }
            } else {
                AofRecord::RPush {
                    key: destination,
                    values: vec![val],
                }
            };
            smallvec![pop_record, push_record]
        }
        // GETDEL: persist as DEL when the key actually existed.
        (ShardRequest::GetDel { key }, ShardResponse::Value(Some(_))) => {
            smallvec![AofRecord::Del { key }]
        }
        // GETEX with a new TTL: persist as Expire (seconds) or Pexpire (ms).
        // PERSIST (expire = Some(None)) is represented as Pexpire with 0.
        // No TTL change (expire = None): nothing to persist.
        (
            ShardRequest::GetEx {
                key,
                expire: Some(new_expire),
            },
            ShardResponse::Value(Some(_)),
        ) => match new_expire {
            Some(ms) if ms > 0 => {
                smallvec![AofRecord::Pexpire { key, milliseconds: ms }]
            }
            // PERSIST — expire set to 0 in the keyspace; record as pexpire 0
            // so replay calls persist. We use a negative sentinel to signal
            // PERSIST on replay: store as Expire with seconds = 0.
            _ => smallvec![AofRecord::Persist { key }],
        },
        _ => SmallVec::new(),
    }
}

/// Writes a single AOF record, used by blocking pop operations that
/// bypass the normal dispatch → to_aof_records path.
pub(super) fn write_aof_record(
    record: &AofRecord,
    aof_writer: &mut Option<AofWriter>,
    fsync_policy: FsyncPolicy,
    shard_id: u16,
    aof_errors: &mut u32,
    disk_full: &mut bool,
) {
    if let Some(ref mut writer) = *aof_writer {
        let mut ok = true;
        if let Err(e) = writer.write_record(record) {
            if log_aof_error(shard_id, aof_errors, "write", &e) {
                *disk_full = true;
            }
            ok = false;
        }
        if fsync_policy == FsyncPolicy::Always {
            if let Err(e) = writer.sync() {
                if log_aof_error(shard_id, aof_errors, "sync", &e) {
                    *disk_full = true;
                }
                ok = false;
            }
        }
        if ok && *aof_errors > 0 {
            let missed = *aof_errors;
            *aof_errors = 0;
            *disk_full = false;
            info!(shard_id, missed_errors = missed, "aof writes recovered");
        }
    }
}

/// Returns `true` if the error indicates the disk is full (ENOSPC or EDQUOT).
fn is_disk_full_error(err: &ember_persistence::format::FormatError) -> bool {
    // ENOSPC = 28 (Linux + macOS), EDQUOT = 122 (Linux) / 69 (macOS)
    matches!(err, ember_persistence::format::FormatError::Io(ref io_err)
        if matches!(io_err.raw_os_error(), Some(28) | Some(69) | Some(122)))
}

/// Logs an AOF error with rate-limiting and severity awareness.
///
/// Disk-full (ENOSPC) errors are logged at `error!` level since they indicate
/// a condition that needs operator attention. Other I/O errors use `warn!`.
/// After the first failure, subsequent consecutive errors are suppressed —
/// only every 1000th error is logged to avoid flooding under sustained
/// disk-full conditions.
///
/// Returns `true` if this was a disk-full error, so the caller can set
/// the `disk_full` flag to reject subsequent writes.
pub(super) fn log_aof_error(
    shard_id: u16,
    consecutive: &mut u32,
    op: &str,
    err: &ember_persistence::format::FormatError,
) -> bool {
    *consecutive = consecutive.saturating_add(1);

    let disk_full = is_disk_full_error(err);

    // rate-limit: log first failure, then every 1000th
    if *consecutive != 1 && !(*consecutive).is_multiple_of(1000) {
        return disk_full;
    }

    if disk_full {
        error!(
            shard_id,
            consecutive_errors = *consecutive,
            "aof {op} failed: disk full — new writes will be rejected"
        );
    } else {
        warn!(
            shard_id,
            consecutive_errors = *consecutive,
            "aof {op} failed: {err}"
        );
    }

    disk_full
}

/// Broadcasts a single replication event, used by blocking pop operations
/// that bypass the normal process_message path.
pub(super) fn broadcast_replication(
    record: AofRecord,
    replication_tx: &Option<broadcast::Sender<ReplicationEvent>>,
    replication_offset: &mut u64,
    shard_id: u16,
) {
    if let Some(ref tx) = *replication_tx {
        *replication_offset += 1;
        let _ = tx.send(ReplicationEvent {
            shard_id,
            offset: *replication_offset,
            record,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_aof_records_for_set() {
        let req = ShardRequest::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire: Some(Duration::from_secs(60)),
            nx: false,
            xx: false,
        };
        let mut resp = ShardResponse::Ok;
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::Set { key, expire_ms, .. } => {
                assert_eq!(key, "k");
                assert_eq!(expire_ms, 60_000);
            }
            other => panic!("expected Set, got {other:?}"),
        }
    }

    #[test]
    fn to_aof_records_skips_failed_set() {
        let req = ShardRequest::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire: None,
            nx: false,
            xx: false,
        };
        let mut resp = ShardResponse::OutOfMemory;
        assert!(to_aof_records(req, &mut resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_del() {
        let req = ShardRequest::Del { key: "k".into() };
        let mut resp = ShardResponse::Bool(true);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        assert!(matches!(record, AofRecord::Del { .. }));
    }

    #[test]
    fn to_aof_records_skips_failed_del() {
        let req = ShardRequest::Del { key: "k".into() };
        let mut resp = ShardResponse::Bool(false);
        assert!(to_aof_records(req, &mut resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_append() {
        let req = ShardRequest::Append {
            key: "k".into(),
            value: Bytes::from("data"),
        };
        let mut resp = ShardResponse::Len(10);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::Append { key, value } => {
                assert_eq!(key, "k");
                assert_eq!(value, Bytes::from("data"));
            }
            other => panic!("expected Append, got {other:?}"),
        }
    }

    #[test]
    fn to_aof_records_for_incr() {
        let req = ShardRequest::Incr { key: "c".into() };
        let mut resp = ShardResponse::Integer(1);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        assert!(matches!(record, AofRecord::Incr { .. }));
    }

    #[test]
    fn to_aof_records_for_decr() {
        let req = ShardRequest::Decr { key: "c".into() };
        let mut resp = ShardResponse::Integer(-1);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        assert!(matches!(record, AofRecord::Decr { .. }));
    }

    #[test]
    fn to_aof_records_for_incrby() {
        let req = ShardRequest::IncrBy {
            key: "c".into(),
            delta: 5,
        };
        let mut resp = ShardResponse::Integer(15);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::IncrBy { key, delta } => {
                assert_eq!(key, "c");
                assert_eq!(delta, 5);
            }
            other => panic!("expected IncrBy, got {other:?}"),
        }
    }

    #[test]
    fn to_aof_records_for_decrby() {
        let req = ShardRequest::DecrBy {
            key: "c".into(),
            delta: 3,
        };
        let mut resp = ShardResponse::Integer(7);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::DecrBy { key, delta } => {
                assert_eq!(key, "c");
                assert_eq!(delta, 3);
            }
            other => panic!("expected DecrBy, got {other:?}"),
        }
    }

    #[test]
    fn to_aof_records_for_persist() {
        let req = ShardRequest::Persist { key: "k".into() };
        let mut resp = ShardResponse::Bool(true);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        assert!(matches!(record, AofRecord::Persist { .. }));
    }

    #[test]
    fn to_aof_records_skips_failed_persist() {
        let req = ShardRequest::Persist { key: "k".into() };
        let mut resp = ShardResponse::Bool(false);
        assert!(to_aof_records(req, &mut resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_pexpire() {
        let req = ShardRequest::Pexpire {
            key: "k".into(),
            milliseconds: 5000,
        };
        let mut resp = ShardResponse::Bool(true);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::Pexpire { key, milliseconds } => {
                assert_eq!(key, "k");
                assert_eq!(milliseconds, 5000);
            }
            other => panic!("expected Pexpire, got {other:?}"),
        }
    }

    #[test]
    fn to_aof_records_skips_failed_pexpire() {
        let req = ShardRequest::Pexpire {
            key: "k".into(),
            milliseconds: 5000,
        };
        let mut resp = ShardResponse::Bool(false);
        assert!(to_aof_records(req, &mut resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_hset() {
        let req = ShardRequest::HSet {
            key: "h".into(),
            fields: vec![("f1".into(), Bytes::from("v1"))],
        };
        let mut resp = ShardResponse::Len(1);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::HSet { key, fields } => {
                assert_eq!(key, "h");
                assert_eq!(fields.len(), 1);
            }
            _ => panic!("expected HSet record"),
        }
    }

    #[test]
    fn to_aof_records_for_hdel() {
        let req = ShardRequest::HDel {
            key: "h".into(),
            fields: vec!["f1".into(), "f2".into()],
        };
        let mut resp = ShardResponse::HDelLen {
            count: 2,
            removed: vec!["f1".into(), "f2".into()],
        };
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::HDel { key, fields } => {
                assert_eq!(key, "h");
                assert_eq!(fields.len(), 2);
            }
            _ => panic!("expected HDel record"),
        }
    }

    #[test]
    fn to_aof_records_skips_hdel_when_none_removed() {
        let req = ShardRequest::HDel {
            key: "h".into(),
            fields: vec!["f1".into()],
        };
        let mut resp = ShardResponse::HDelLen {
            count: 0,
            removed: vec![],
        };
        assert!(to_aof_records(req, &mut resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_hincrby() {
        let req = ShardRequest::HIncrBy {
            key: "h".into(),
            field: "counter".into(),
            delta: 5,
        };
        let mut resp = ShardResponse::Integer(10);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::HIncrBy { key, field, delta } => {
                assert_eq!(key, "h");
                assert_eq!(field, "counter");
                assert_eq!(delta, 5);
            }
            _ => panic!("expected HIncrBy record"),
        }
    }

    #[test]
    fn to_aof_records_for_sadd() {
        let req = ShardRequest::SAdd {
            key: "s".into(),
            members: vec!["m1".into(), "m2".into()],
        };
        let mut resp = ShardResponse::Len(2);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::SAdd { key, members } => {
                assert_eq!(key, "s");
                assert_eq!(members.len(), 2);
            }
            _ => panic!("expected SAdd record"),
        }
    }

    #[test]
    fn to_aof_records_skips_sadd_when_none_added() {
        let req = ShardRequest::SAdd {
            key: "s".into(),
            members: vec!["m1".into()],
        };
        let mut resp = ShardResponse::Len(0);
        assert!(to_aof_records(req, &mut resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_srem() {
        let req = ShardRequest::SRem {
            key: "s".into(),
            members: vec!["m1".into()],
        };
        let mut resp = ShardResponse::Len(1);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::SRem { key, members } => {
                assert_eq!(key, "s");
                assert_eq!(members.len(), 1);
            }
            _ => panic!("expected SRem record"),
        }
    }

    #[test]
    fn to_aof_records_skips_srem_when_none_removed() {
        let req = ShardRequest::SRem {
            key: "s".into(),
            members: vec!["m1".into()],
        };
        let mut resp = ShardResponse::Len(0);
        assert!(to_aof_records(req, &mut resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_rename() {
        let req = ShardRequest::Rename {
            key: "old".into(),
            newkey: "new".into(),
        };
        let mut resp = ShardResponse::Ok;
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::Rename { key, newkey } => {
                assert_eq!(key, "old");
                assert_eq!(newkey, "new");
            }
            other => panic!("expected Rename, got {other:?}"),
        }
    }

    #[test]
    #[cfg(feature = "vector")]
    fn to_aof_records_for_vadd_batch() {
        let req = ShardRequest::VAddBatch {
            key: "vecs".into(),
            entries: vec![
                ("a".into(), vec![1.0, 2.0]),
                ("b".into(), vec![3.0, 4.0]),
                ("c".into(), vec![5.0, 6.0]),
            ],
            dim: 2,
            metric: 0,
            quantization: 0,
            connectivity: 16,
            expansion_add: 64,
        };
        let mut resp = ShardResponse::VAddBatchResult {
            added_count: 3,
            applied: vec![
                ("a".into(), vec![1.0, 2.0]),
                ("b".into(), vec![3.0, 4.0]),
                ("c".into(), vec![5.0, 6.0]),
            ],
        };
        let records = to_aof_records(req, &mut resp);
        assert_eq!(records.len(), 3);
        for (i, record) in records.iter().enumerate() {
            match record {
                AofRecord::VAdd {
                    key,
                    element,
                    metric,
                    quantization,
                    connectivity,
                    expansion_add,
                    ..
                } => {
                    assert_eq!(key, "vecs");
                    assert_eq!(*metric, 0);
                    assert_eq!(*quantization, 0);
                    assert_eq!(*connectivity, 16);
                    assert_eq!(*expansion_add, 64);
                    match i {
                        0 => assert_eq!(element, "a"),
                        1 => assert_eq!(element, "b"),
                        2 => assert_eq!(element, "c"),
                        _ => unreachable!(),
                    }
                }
                other => panic!("expected VAdd, got {other:?}"),
            }
        }
    }

    #[test]
    fn to_aof_records_for_copy() {
        let req = ShardRequest::Copy {
            source: "src".into(),
            destination: "dst".into(),
            replace: true,
        };
        let mut resp = ShardResponse::Bool(true);
        let record = to_aof_records(req, &mut resp).into_iter().next().unwrap();
        match record {
            AofRecord::Copy {
                source,
                destination,
                replace,
            } => {
                assert_eq!(source, "src");
                assert_eq!(destination, "dst");
                assert!(replace);
            }
            other => panic!("expected Copy, got {other:?}"),
        }
    }

    #[test]
    fn to_aof_records_skips_copy_no_op() {
        let req = ShardRequest::Copy {
            source: "src".into(),
            destination: "dst".into(),
            replace: false,
        };
        let mut resp = ShardResponse::Bool(false);
        assert!(to_aof_records(req, &mut resp).is_empty());
    }

    #[test]
    fn to_aof_records_skips_nx_blocked_set() {
        let req = ShardRequest::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire: None,
            nx: true,
            xx: false,
        };
        // when NX blocks, the shard returns Value(None), not Ok
        let mut resp = ShardResponse::Value(None);
        assert!(to_aof_records(req, &mut resp).is_empty());
    }
}
