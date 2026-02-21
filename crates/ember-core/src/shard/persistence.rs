use super::*;

/// Writes a snapshot of the current keyspace.
pub(super) fn handle_snapshot(
    keyspace: &Keyspace,
    persistence: &Option<ShardPersistenceConfig>,
    shard_id: u16,
) -> ShardResponse {
    let pcfg = match persistence {
        Some(p) => p,
        None => return ShardResponse::Err("persistence not configured".into()),
    };

    let path = snapshot::snapshot_path(&pcfg.data_dir, shard_id);
    let result = write_snapshot(
        keyspace,
        &path,
        shard_id,
        #[cfg(feature = "encryption")]
        pcfg.encryption_key.as_ref(),
    );
    match result {
        Ok(count) => {
            info!(shard_id, entries = count, "snapshot written");
            ShardResponse::Ok
        }
        Err(e) => {
            warn!(shard_id, "snapshot failed: {e}");
            ShardResponse::Err(format!("snapshot failed: {e}"))
        }
    }
}

/// Serializes the current shard state to bytes without filesystem I/O.
///
/// Used by the replication server to capture a snapshot for transmission
/// to a new replica. The format matches the file-based snapshot and can
/// be loaded via [`ember_persistence::snapshot::read_snapshot_from_bytes`].
pub(super) fn handle_serialize_snapshot(keyspace: &Keyspace, shard_id: u16) -> ShardResponse {
    let entries: Vec<SnapEntry> = keyspace
        .iter_entries()
        .map(|(key, value, expire_ms)| SnapEntry {
            key: key.to_owned(),
            value: value_to_snap(value),
            expire_ms,
        })
        .collect();

    match snapshot::write_snapshot_bytes(shard_id, &entries) {
        Ok(data) => ShardResponse::SnapshotData { shard_id, data },
        Err(e) => {
            warn!(shard_id, "snapshot serialization failed: {e}");
            ShardResponse::Err(format!("snapshot failed: {e}"))
        }
    }
}

/// Writes a snapshot and then truncates the AOF.
///
/// When protobuf is enabled, re-persists all registered schemas to the
/// AOF after truncation so they survive the next restart.
pub(super) fn handle_rewrite(
    keyspace: &Keyspace,
    persistence: &Option<ShardPersistenceConfig>,
    aof_writer: &mut Option<AofWriter>,
    shard_id: u16,
    #[cfg(feature = "protobuf")] schema_registry: &Option<crate::schema::SharedSchemaRegistry>,
) -> ShardResponse {
    let pcfg = match persistence {
        Some(p) => p,
        None => return ShardResponse::Err("persistence not configured".into()),
    };

    let path = snapshot::snapshot_path(&pcfg.data_dir, shard_id);
    let result = write_snapshot(
        keyspace,
        &path,
        shard_id,
        #[cfg(feature = "encryption")]
        pcfg.encryption_key.as_ref(),
    );
    match result {
        Ok(count) => {
            // truncate AOF after successful snapshot
            if let Some(ref mut writer) = aof_writer {
                if let Err(e) = writer.truncate() {
                    warn!(shard_id, "aof truncate after rewrite failed: {e}");
                }

                // re-persist schemas so they survive the next recovery
                #[cfg(feature = "protobuf")]
                if let Some(ref registry) = schema_registry {
                    if let Ok(reg) = registry.read() {
                        for (name, descriptor) in reg.iter_schemas() {
                            let record = AofRecord::ProtoRegister {
                                name: name.to_owned(),
                                descriptor: descriptor.clone(),
                            };
                            if let Err(e) = writer.write_record(&record) {
                                warn!(shard_id, "failed to re-persist schema after rewrite: {e}");
                            }
                        }
                    }
                }

                // flush so schemas are durable before we report success
                if let Err(e) = writer.sync() {
                    warn!(shard_id, "aof sync after rewrite failed: {e}");
                }
            }
            info!(shard_id, entries = count, "aof rewrite complete");
            ShardResponse::Ok
        }
        Err(e) => {
            warn!(shard_id, "aof rewrite failed: {e}");
            ShardResponse::Err(format!("rewrite failed: {e}"))
        }
    }
}

/// Converts a `Value` reference to a `SnapValue` for serialization.
pub(super) fn value_to_snap(value: &Value) -> SnapValue {
    match value {
        Value::String(data) => SnapValue::String(data.clone()),
        Value::List(deque) => SnapValue::List(deque.clone()),
        Value::SortedSet(ss) => {
            let members: Vec<(f64, String)> = ss
                .iter()
                .map(|(member, score)| (score, member.to_owned()))
                .collect();
            SnapValue::SortedSet(members)
        }
        Value::Hash(hash) => SnapValue::Hash(hash.to_hash_map()),
        Value::Set(set) => SnapValue::Set((**set).clone()),
        #[cfg(feature = "vector")]
        Value::Vector(ref vs) => {
            let mut elements = Vec::with_capacity(vs.len());
            for name in vs.elements() {
                if let Some(vec) = vs.get(name) {
                    elements.push((name.to_owned(), vec));
                }
            }
            SnapValue::Vector {
                metric: vs.metric().into(),
                quantization: vs.quantization().into(),
                connectivity: vs.connectivity() as u32,
                expansion_add: vs.expansion_add() as u32,
                dim: vs.dim() as u32,
                elements,
            }
        }
        #[cfg(feature = "protobuf")]
        Value::Proto { type_name, data } => SnapValue::Proto {
            type_name: type_name.clone(),
            data: data.clone(),
        },
    }
}

/// Converts a `SnapValue` into a `Value` for insertion into the keyspace.
pub(super) fn snap_to_value(snap: SnapValue) -> Value {
    match snap {
        SnapValue::String(data) => Value::String(data),
        SnapValue::List(deque) => Value::List(deque),
        SnapValue::SortedSet(members) => {
            let mut ss = crate::types::sorted_set::SortedSet::new();
            for (score, member) in members {
                ss.add(member, score);
            }
            Value::SortedSet(Box::new(ss))
        }
        SnapValue::Hash(map) => Value::Hash(Box::new(crate::types::hash::HashValue::from(map))),
        SnapValue::Set(set) => Value::Set(Box::new(set)),
        #[cfg(feature = "vector")]
        SnapValue::Vector {
            metric,
            quantization,
            connectivity,
            expansion_add,
            elements,
            ..
        } => {
            use crate::types::vector::{DistanceMetric, QuantizationType, VectorSet};
            let dim = elements.first().map(|(_, v)| v.len()).unwrap_or(0);
            let dm = DistanceMetric::from_u8(metric);
            let qt = QuantizationType::from_u8(quantization);
            match VectorSet::new(dim, dm, qt, connectivity as usize, expansion_add as usize) {
                Ok(mut vs) => {
                    for (name, vec) in elements {
                        let _ = vs.add(name, &vec);
                    }
                    Value::Vector(vs)
                }
                Err(_) => Value::String(Bytes::new()),
            }
        }
        #[cfg(feature = "protobuf")]
        SnapValue::Proto { type_name, data } => Value::Proto { type_name, data },
    }
}

/// Iterates the keyspace and writes all live entries to a snapshot file.
pub(super) fn write_snapshot(
    keyspace: &Keyspace,
    path: &std::path::Path,
    shard_id: u16,
    #[cfg(feature = "encryption")] encryption_key: Option<
        &ember_persistence::encryption::EncryptionKey,
    >,
) -> Result<u32, ember_persistence::format::FormatError> {
    #[cfg(feature = "encryption")]
    let mut writer = if let Some(key) = encryption_key {
        SnapshotWriter::create_encrypted(path, shard_id, key.clone())?
    } else {
        SnapshotWriter::create(path, shard_id)?
    };
    #[cfg(not(feature = "encryption"))]
    let mut writer = SnapshotWriter::create(path, shard_id)?;
    let mut count = 0u32;

    for (key, value, ttl_ms) in keyspace.iter_entries() {
        writer.write_entry(&SnapEntry {
            key: key.to_owned(),
            value: value_to_snap(value),
            expire_ms: ttl_ms,
        })?;
        count += 1;
    }

    writer.finish()?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shard_round_trip() {
        let handle = spawn_shard(
            16,
            ShardConfig::default(),
            None,
            None,
            None,
            #[cfg(feature = "protobuf")]
            None,
        );

        let resp = handle
            .send(ShardRequest::Set {
                key: "hello".into(),
                value: Bytes::from("world"),
                expire: None,
                nx: false,
                xx: false,
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Ok));

        let resp = handle
            .send(ShardRequest::Get {
                key: "hello".into(),
            })
            .await
            .unwrap();
        match resp {
            ShardResponse::Value(Some(Value::String(data))) => {
                assert_eq!(data, Bytes::from("world"));
            }
            other => panic!("expected Value(Some(String)), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn expired_key_through_shard() {
        let handle = spawn_shard(
            16,
            ShardConfig::default(),
            None,
            None,
            None,
            #[cfg(feature = "protobuf")]
            None,
        );

        handle
            .send(ShardRequest::Set {
                key: "temp".into(),
                value: Bytes::from("gone"),
                expire: Some(Duration::from_millis(10)),
                nx: false,
                xx: false,
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(30)).await;

        let resp = handle
            .send(ShardRequest::Get { key: "temp".into() })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Value(None)));
    }

    #[tokio::test]
    async fn active_expiration_cleans_up_without_access() {
        let handle = spawn_shard(
            16,
            ShardConfig::default(),
            None,
            None,
            None,
            #[cfg(feature = "protobuf")]
            None,
        );

        // set a key with a short TTL
        handle
            .send(ShardRequest::Set {
                key: "ephemeral".into(),
                value: Bytes::from("temp"),
                expire: Some(Duration::from_millis(10)),
                nx: false,
                xx: false,
            })
            .await
            .unwrap();

        // also set a persistent key
        handle
            .send(ShardRequest::Set {
                key: "persistent".into(),
                value: Bytes::from("stays"),
                expire: None,
                nx: false,
                xx: false,
            })
            .await
            .unwrap();

        // wait long enough for the TTL to expire AND for the background
        // tick to fire (100ms interval + some slack)
        tokio::time::sleep(Duration::from_millis(250)).await;

        // the ephemeral key should be gone even though we never accessed it
        let resp = handle
            .send(ShardRequest::Exists {
                key: "ephemeral".into(),
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Bool(false)));

        // the persistent key should still be there
        let resp = handle
            .send(ShardRequest::Exists {
                key: "persistent".into(),
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Bool(true)));
    }

    #[tokio::test]
    async fn shard_with_persistence_snapshot_and_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let pcfg = ShardPersistenceConfig {
            data_dir: dir.path().to_owned(),
            append_only: true,
            fsync_policy: FsyncPolicy::Always,
            #[cfg(feature = "encryption")]
            encryption_key: None,
        };
        let config = ShardConfig {
            shard_id: 0,
            ..ShardConfig::default()
        };

        // write some keys then trigger a snapshot
        {
            let handle = spawn_shard(
                16,
                config.clone(),
                Some(pcfg.clone()),
                None,
                None,
                #[cfg(feature = "protobuf")]
                None,
            );
            handle
                .send(ShardRequest::Set {
                    key: "a".into(),
                    value: Bytes::from("1"),
                    expire: None,
                    nx: false,
                    xx: false,
                })
                .await
                .unwrap();
            handle
                .send(ShardRequest::Set {
                    key: "b".into(),
                    value: Bytes::from("2"),
                    expire: Some(Duration::from_secs(300)),
                    nx: false,
                    xx: false,
                })
                .await
                .unwrap();
            handle.send(ShardRequest::Snapshot).await.unwrap();
            // write one more key that goes only to AOF
            handle
                .send(ShardRequest::Set {
                    key: "c".into(),
                    value: Bytes::from("3"),
                    expire: None,
                    nx: false,
                    xx: false,
                })
                .await
                .unwrap();
            // drop handle to shut down shard
        }

        // give it a moment to flush
        tokio::time::sleep(Duration::from_millis(50)).await;

        // start a new shard with the same config — should recover
        {
            let handle = spawn_shard(
                16,
                config,
                Some(pcfg),
                None,
                None,
                #[cfg(feature = "protobuf")]
                None,
            );
            // give it a moment to recover
            tokio::time::sleep(Duration::from_millis(50)).await;

            let resp = handle
                .send(ShardRequest::Get { key: "a".into() })
                .await
                .unwrap();
            match resp {
                ShardResponse::Value(Some(Value::String(data))) => {
                    assert_eq!(data, Bytes::from("1"));
                }
                other => panic!("expected a=1, got {other:?}"),
            }

            let resp = handle
                .send(ShardRequest::Get { key: "b".into() })
                .await
                .unwrap();
            assert!(matches!(resp, ShardResponse::Value(Some(_))));

            let resp = handle
                .send(ShardRequest::Get { key: "c".into() })
                .await
                .unwrap();
            match resp {
                ShardResponse::Value(Some(Value::String(data))) => {
                    assert_eq!(data, Bytes::from("3"));
                }
                other => panic!("expected c=3, got {other:?}"),
            }
        }
    }
}
