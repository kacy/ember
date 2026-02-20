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
pub(super) fn to_aof_records(req: ShardRequest, resp: &ShardResponse) -> SmallVec<[AofRecord; 1]> {
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
        (ShardRequest::Rename { key, newkey }, ShardResponse::Ok) => {
            smallvec![AofRecord::Rename { key, newkey }]
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
        // VADD_BATCH: expand each applied entry into its own AofRecord::VAdd
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
        ) => applied
            .iter()
            .map(|(element, vector)| AofRecord::VAdd {
                key: key.clone(),
                element: element.clone(),
                vector: vector.clone(),
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
) {
    if let Some(ref mut writer) = *aof_writer {
        if let Err(e) = writer.write_record(record) {
            warn!(shard_id, "aof write failed: {e}");
        }
        if fsync_policy == FsyncPolicy::Always {
            if let Err(e) = writer.sync() {
                warn!(shard_id, "aof sync failed: {e}");
            }
        }
    }
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
        let resp = ShardResponse::Ok;
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
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
        let resp = ShardResponse::OutOfMemory;
        assert!(to_aof_records(req, &resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_del() {
        let req = ShardRequest::Del { key: "k".into() };
        let resp = ShardResponse::Bool(true);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
        assert!(matches!(record, AofRecord::Del { .. }));
    }

    #[test]
    fn to_aof_records_skips_failed_del() {
        let req = ShardRequest::Del { key: "k".into() };
        let resp = ShardResponse::Bool(false);
        assert!(to_aof_records(req, &resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_append() {
        let req = ShardRequest::Append {
            key: "k".into(),
            value: Bytes::from("data"),
        };
        let resp = ShardResponse::Len(10);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
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
        let resp = ShardResponse::Integer(1);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
        assert!(matches!(record, AofRecord::Incr { .. }));
    }

    #[test]
    fn to_aof_records_for_decr() {
        let req = ShardRequest::Decr { key: "c".into() };
        let resp = ShardResponse::Integer(-1);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
        assert!(matches!(record, AofRecord::Decr { .. }));
    }

    #[test]
    fn to_aof_records_for_incrby() {
        let req = ShardRequest::IncrBy {
            key: "c".into(),
            delta: 5,
        };
        let resp = ShardResponse::Integer(15);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
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
        let resp = ShardResponse::Integer(7);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
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
        let resp = ShardResponse::Bool(true);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
        assert!(matches!(record, AofRecord::Persist { .. }));
    }

    #[test]
    fn to_aof_records_skips_failed_persist() {
        let req = ShardRequest::Persist { key: "k".into() };
        let resp = ShardResponse::Bool(false);
        assert!(to_aof_records(req, &resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_pexpire() {
        let req = ShardRequest::Pexpire {
            key: "k".into(),
            milliseconds: 5000,
        };
        let resp = ShardResponse::Bool(true);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
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
        let resp = ShardResponse::Bool(false);
        assert!(to_aof_records(req, &resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_hset() {
        let req = ShardRequest::HSet {
            key: "h".into(),
            fields: vec![("f1".into(), Bytes::from("v1"))],
        };
        let resp = ShardResponse::Len(1);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
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
        let resp = ShardResponse::HDelLen {
            count: 2,
            removed: vec!["f1".into(), "f2".into()],
        };
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
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
        let resp = ShardResponse::HDelLen {
            count: 0,
            removed: vec![],
        };
        assert!(to_aof_records(req, &resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_hincrby() {
        let req = ShardRequest::HIncrBy {
            key: "h".into(),
            field: "counter".into(),
            delta: 5,
        };
        let resp = ShardResponse::Integer(10);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
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
        let resp = ShardResponse::Len(2);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
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
        let resp = ShardResponse::Len(0);
        assert!(to_aof_records(req, &resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_srem() {
        let req = ShardRequest::SRem {
            key: "s".into(),
            members: vec!["m1".into()],
        };
        let resp = ShardResponse::Len(1);
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
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
        let resp = ShardResponse::Len(0);
        assert!(to_aof_records(req, &resp).is_empty());
    }

    #[test]
    fn to_aof_records_for_rename() {
        let req = ShardRequest::Rename {
            key: "old".into(),
            newkey: "new".into(),
        };
        let resp = ShardResponse::Ok;
        let record = to_aof_records(req, &resp).into_iter().next().unwrap();
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
        let resp = ShardResponse::VAddBatchResult {
            added_count: 3,
            applied: vec![
                ("a".into(), vec![1.0, 2.0]),
                ("b".into(), vec![3.0, 4.0]),
                ("c".into(), vec![5.0, 6.0]),
            ],
        };
        let records = to_aof_records(req, &resp);
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
    fn to_aof_records_skips_nx_blocked_set() {
        let req = ShardRequest::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire: None,
            nx: true,
            xx: false,
        };
        // when NX blocks, the shard returns Value(None), not Ok
        let resp = ShardResponse::Value(None);
        assert!(to_aof_records(req, &resp).is_empty());
    }
}
