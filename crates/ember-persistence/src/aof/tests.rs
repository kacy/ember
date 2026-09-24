use bytes::Bytes;

use super::record::TAG_SET;
use super::*;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("create temp dir")
}

#[test]
fn record_round_trip_set() -> Result {
    let rec = AofRecord::Set {
        key: "hello".into(),
        value: Bytes::from("world"),
        expire_ms: 5000,
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_del() -> Result {
    let rec = AofRecord::Del { key: "gone".into() };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_expire() -> Result {
    let rec = AofRecord::Expire {
        key: "ttl".into(),
        seconds: 300,
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn set_with_no_expiry() -> Result {
    let rec = AofRecord::Set {
        key: "k".into(),
        value: Bytes::from("v"),
        expire_ms: -1,
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

/// One record of every kind the writer can produce.
fn one_of_each_record() -> Vec<AofRecord> {
    let key = || String::from("k");
    let val = || Bytes::from("v");
    vec![
        AofRecord::Set {
            key: key(),
            value: val(),
            expire_ms: 1_000,
        },
        AofRecord::Del { key: key() },
        AofRecord::Expire {
            key: key(),
            seconds: 60,
        },
        AofRecord::LPush {
            key: key(),
            values: vec![val()],
        },
        AofRecord::RPush {
            key: key(),
            values: vec![val(), val()],
        },
        AofRecord::LPop { key: key() },
        AofRecord::RPop { key: key() },
        AofRecord::LSet {
            key: key(),
            index: -1,
            value: val(),
        },
        AofRecord::LTrim {
            key: key(),
            start: 0,
            stop: -2,
        },
        AofRecord::LInsert {
            key: key(),
            before: true,
            pivot: val(),
            value: val(),
        },
        AofRecord::LRem {
            key: key(),
            count: -2,
            value: val(),
        },
        AofRecord::ZAdd {
            key: key(),
            members: vec![(1.5, "m".into())],
        },
        AofRecord::ZRem {
            key: key(),
            members: vec!["m".into()],
        },
        AofRecord::Persist { key: key() },
        AofRecord::Pexpire {
            key: key(),
            milliseconds: 500,
        },
        AofRecord::Pexpireat {
            key: key(),
            timestamp_ms: 1_700_000_000_000,
        },
        AofRecord::Incr { key: key() },
        AofRecord::Decr { key: key() },
        AofRecord::HSet {
            key: key(),
            fields: vec![("f".into(), val())],
        },
        AofRecord::HDel {
            key: key(),
            fields: vec!["f".into()],
        },
        AofRecord::HIncrBy {
            key: key(),
            field: "f".into(),
            delta: -3,
        },
        AofRecord::SAdd {
            key: key(),
            members: vec!["m".into()],
        },
        AofRecord::SRem {
            key: key(),
            members: vec!["m".into()],
        },
        AofRecord::IncrBy {
            key: key(),
            delta: 7,
        },
        AofRecord::DecrBy {
            key: key(),
            delta: 7,
        },
        AofRecord::Append {
            key: key(),
            value: val(),
        },
        AofRecord::SetRange {
            key: key(),
            offset: 3,
            value: val(),
        },
        AofRecord::SetBit {
            key: key(),
            offset: 9,
            value: 1,
        },
        AofRecord::BitOp {
            op: 1,
            dest: "d".into(),
            keys: vec![key(), key()],
        },
        AofRecord::Rename {
            key: key(),
            newkey: "n".into(),
        },
        AofRecord::Copy {
            source: key(),
            destination: "d".into(),
            replace: true,
        },
        AofRecord::SetExpireAt {
            key: key(),
            value: val(),
            timestamp_ms: 1_700_000_000_000,
        },
        AofRecord::FlushAll,
        AofRecord::Checkpoint {
            snapshot_crc: 0xDEAD_BEEF,
        },
        AofRecord::Restore {
            key: key(),
            ttl_ms: 5_000,
            data: val(),
        },
        #[cfg(feature = "vector")]
        AofRecord::VAdd {
            key: key(),
            element: "e".into(),
            vector: vec![0.5, -1.0],
            metric: 0,
            quantization: 0,
            connectivity: 16,
            expansion_add: 64,
        },
        #[cfg(feature = "vector")]
        AofRecord::VRem {
            key: key(),
            element: "e".into(),
        },
        #[cfg(feature = "protobuf")]
        AofRecord::ProtoSet {
            key: key(),
            type_name: "t.T".into(),
            data: val(),
            expire_ms: -1,
        },
        #[cfg(feature = "protobuf")]
        AofRecord::ProtoRegister {
            name: "t".into(),
            descriptor: val(),
        },
    ]
}

#[test]
fn writer_reader_round_trip_covers_every_record_kind() -> Result {
    let dir = temp_dir();
    let path = dir.path().join("all.aof");
    let records = one_of_each_record();
    {
        let mut writer = AofWriter::open(&path)?;
        for record in &records {
            writer.write_record(record)?;
        }
        writer.sync()?;
    }

    let mut reader = AofReader::open(&path)?;
    let mut got = Vec::new();
    while let Some(record) = reader.read_record()? {
        got.push(record);
    }
    assert_eq!(got, records);
    assert_eq!(reader.valid_len(), std::fs::metadata(&path)?.len());
    Ok(())
}

#[test]
fn writer_reader_round_trip() -> Result {
    let dir = temp_dir();
    let path = dir.path().join("test.aof");

    let records = vec![
        AofRecord::Set {
            key: "a".into(),
            value: Bytes::from("1"),
            expire_ms: -1,
        },
        AofRecord::Set {
            key: "b".into(),
            value: Bytes::from("2"),
            expire_ms: 10_000,
        },
        AofRecord::Del { key: "a".into() },
        AofRecord::Expire {
            key: "b".into(),
            seconds: 60,
        },
    ];

    // write
    {
        let mut writer = AofWriter::open(&path)?;
        for rec in &records {
            writer.write_record(rec)?;
        }
        writer.sync()?;
    }

    // read back
    let mut reader = AofReader::open(&path)?;
    let mut got = Vec::new();
    while let Some(rec) = reader.read_record()? {
        got.push(rec);
    }
    assert_eq!(records, got);
    Ok(())
}

#[test]
fn empty_aof_returns_no_records() -> Result {
    let dir = temp_dir();
    let path = dir.path().join("empty.aof");

    // just write the header
    {
        let _writer = AofWriter::open(&path)?;
    }

    let mut reader = AofReader::open(&path)?;
    assert!(reader.read_record()?.is_none());
    Ok(())
}

#[test]
fn truncated_record_treated_as_eof() -> Result {
    let dir = temp_dir();
    let path = dir.path().join("trunc.aof");

    // write one good record, then append garbage (simulating a crash)
    {
        let mut writer = AofWriter::open(&path)?;
        writer.write_record(&AofRecord::Set {
            key: "ok".into(),
            value: Bytes::from("good"),
            expire_ms: -1,
        })?;
        writer.flush()?;
    }

    // append a partial tag with no payload
    {
        let mut file = OpenOptions::new().append(true).open(&path)?;
        file.write_all(&[TAG_SET])?;
    }

    let mut reader = AofReader::open(&path)?;
    // first record should be fine
    let rec = reader.read_record()?.unwrap();
    assert!(matches!(rec, AofRecord::Set { .. }));
    // second should be None (truncated)
    assert!(reader.read_record()?.is_none());
    Ok(())
}

#[test]
fn corrupt_crc_detected() -> Result {
    let dir = temp_dir();
    let path = dir.path().join("corrupt.aof");

    {
        let mut writer = AofWriter::open(&path)?;
        writer.write_record(&AofRecord::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire_ms: -1,
        })?;
        writer.flush()?;
    }

    // corrupt the last byte (part of the CRC)
    let mut data = fs::read(&path)?;
    let last = data.len() - 1;
    data[last] ^= 0xFF;
    fs::write(&path, &data)?;

    let mut reader = AofReader::open(&path)?;
    let err = reader.read_record().unwrap_err();
    assert!(matches!(err, FormatError::ChecksumMismatch { .. }));
    Ok(())
}

#[test]
fn missing_magic_is_error() {
    let dir = temp_dir();
    let path = dir.path().join("bad.aof");
    fs::write(&path, b"NOT_AOF_DATA").unwrap();

    let err = AofReader::open(&path).unwrap_err();
    assert!(matches!(err, FormatError::InvalidMagic));
}

#[test]
fn truncate_resets_aof() -> Result {
    let dir = temp_dir();
    let path = dir.path().join("reset.aof");

    {
        let mut writer = AofWriter::open(&path)?;
        writer.write_record(&AofRecord::Set {
            key: "old".into(),
            value: Bytes::from("data"),
            expire_ms: -1,
        })?;
        writer.truncate(7)?;

        // write a new record after truncation
        writer.write_record(&AofRecord::Set {
            key: "new".into(),
            value: Bytes::from("fresh"),
            expire_ms: -1,
        })?;
        writer.sync()?;
    }

    let mut reader = AofReader::open(&path)?;
    assert_eq!(
        reader.read_record()?,
        Some(AofRecord::Checkpoint { snapshot_crc: 7 })
    );
    let rec = reader.read_record()?.unwrap();
    match rec {
        AofRecord::Set { key, .. } => assert_eq!(key, "new"),
        other => panic!("expected Set, got {other:?}"),
    }
    // only one record after truncation
    assert!(reader.read_record()?.is_none());
    Ok(())
}

#[test]
fn record_round_trip_lpush() -> Result {
    let rec = AofRecord::LPush {
        key: "list".into(),
        values: vec![Bytes::from("a"), Bytes::from("b")],
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_rpush() -> Result {
    let rec = AofRecord::RPush {
        key: "list".into(),
        values: vec![Bytes::from("x")],
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_lpop() -> Result {
    let rec = AofRecord::LPop { key: "list".into() };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_rpop() -> Result {
    let rec = AofRecord::RPop { key: "list".into() };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn writer_reader_round_trip_with_list_records() -> Result {
    let dir = temp_dir();
    let path = dir.path().join("list.aof");

    let records = vec![
        AofRecord::LPush {
            key: "l".into(),
            values: vec![Bytes::from("a"), Bytes::from("b")],
        },
        AofRecord::RPush {
            key: "l".into(),
            values: vec![Bytes::from("c")],
        },
        AofRecord::LPop { key: "l".into() },
        AofRecord::RPop { key: "l".into() },
    ];

    {
        let mut writer = AofWriter::open(&path)?;
        for rec in &records {
            writer.write_record(rec)?;
        }
        writer.sync()?;
    }

    let mut reader = AofReader::open(&path)?;
    let mut got = Vec::new();
    while let Some(rec) = reader.read_record()? {
        got.push(rec);
    }
    assert_eq!(records, got);
    Ok(())
}

#[test]
fn record_round_trip_zadd() -> Result {
    let rec = AofRecord::ZAdd {
        key: "board".into(),
        members: vec![(100.0, "alice".into()), (200.5, "bob".into())],
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_zrem() -> Result {
    let rec = AofRecord::ZRem {
        key: "board".into(),
        members: vec!["alice".into(), "bob".into()],
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn writer_reader_round_trip_with_sorted_set_records() -> Result {
    let dir = temp_dir();
    let path = dir.path().join("zset.aof");

    let records = vec![
        AofRecord::ZAdd {
            key: "board".into(),
            members: vec![(100.0, "alice".into()), (200.0, "bob".into())],
        },
        AofRecord::ZRem {
            key: "board".into(),
            members: vec!["alice".into()],
        },
    ];

    {
        let mut writer = AofWriter::open(&path)?;
        for rec in &records {
            writer.write_record(rec)?;
        }
        writer.sync()?;
    }

    let mut reader = AofReader::open(&path)?;
    let mut got = Vec::new();
    while let Some(rec) = reader.read_record()? {
        got.push(rec);
    }
    assert_eq!(records, got);
    Ok(())
}

#[test]
fn record_round_trip_persist() -> Result {
    let rec = AofRecord::Persist {
        key: "mykey".into(),
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_pexpire() -> Result {
    let rec = AofRecord::Pexpire {
        key: "mykey".into(),
        milliseconds: 5000,
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_incr() -> Result {
    let rec = AofRecord::Incr {
        key: "counter".into(),
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_decr() -> Result {
    let rec = AofRecord::Decr {
        key: "counter".into(),
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn writer_reader_round_trip_with_persist_pexpire() -> Result {
    let dir = temp_dir();
    let path = dir.path().join("persist_pexpire.aof");

    let records = vec![
        AofRecord::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire_ms: 5000,
        },
        AofRecord::Persist { key: "k".into() },
        AofRecord::Pexpire {
            key: "k".into(),
            milliseconds: 3000,
        },
    ];

    {
        let mut writer = AofWriter::open(&path)?;
        for rec in &records {
            writer.write_record(rec)?;
        }
        writer.sync()?;
    }

    let mut reader = AofReader::open(&path)?;
    let mut got = Vec::new();
    while let Some(rec) = reader.read_record()? {
        got.push(rec);
    }
    assert_eq!(records, got);
    Ok(())
}

#[test]
fn aof_path_format() {
    let p = aof_path(Path::new("/data"), 3);
    assert_eq!(p, PathBuf::from("/data/shard-3.aof"));
}

#[test]
fn record_round_trip_hset() -> Result {
    let rec = AofRecord::HSet {
        key: "hash".into(),
        fields: vec![
            ("f1".into(), Bytes::from("v1")),
            ("f2".into(), Bytes::from("v2")),
        ],
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_hdel() -> Result {
    let rec = AofRecord::HDel {
        key: "hash".into(),
        fields: vec!["f1".into(), "f2".into()],
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_hincrby() -> Result {
    let rec = AofRecord::HIncrBy {
        key: "hash".into(),
        field: "counter".into(),
        delta: -42,
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_sadd() -> Result {
    let rec = AofRecord::SAdd {
        key: "set".into(),
        members: vec!["m1".into(), "m2".into(), "m3".into()],
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[test]
fn record_round_trip_srem() -> Result {
    let rec = AofRecord::SRem {
        key: "set".into(),
        members: vec!["m1".into()],
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[cfg(feature = "vector")]
#[test]
fn record_round_trip_vadd() -> Result {
    let rec = AofRecord::VAdd {
        key: "embeddings".into(),
        element: "doc1".into(),
        vector: vec![0.1, 0.2, 0.3],
        metric: 0,       // cosine
        quantization: 0, // f32
        connectivity: 16,
        expansion_add: 64,
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[cfg(feature = "vector")]
#[test]
fn record_round_trip_vadd_high_dim() -> Result {
    let rec = AofRecord::VAdd {
        key: "vecs".into(),
        element: "e".into(),
        vector: vec![0.0; 1536], // typical embedding dimension
        metric: 1,               // l2
        quantization: 1,         // f16
        connectivity: 32,
        expansion_add: 128,
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
    Ok(())
}

#[cfg(feature = "vector")]
#[test]
fn record_round_trip_vrem() -> Result {
    let rec = AofRecord::VRem {
        key: "embeddings".into(),
        element: "doc1".into(),
    };
    let bytes = rec.to_bytes()?;
    let decoded = AofRecord::from_bytes(&bytes)?;
    assert_eq!(rec, decoded);
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
    fn encrypted_writer_reader_round_trip() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("enc.aof");
        let key = test_key();

        let records = vec![
            AofRecord::Set {
                key: "a".into(),
                value: Bytes::from("1"),
                expire_ms: -1,
            },
            AofRecord::Del { key: "a".into() },
            AofRecord::LPush {
                key: "list".into(),
                values: vec![Bytes::from("x"), Bytes::from("y")],
            },
            AofRecord::ZAdd {
                key: "zs".into(),
                members: vec![(1.0, "m".into())],
            },
        ];

        {
            let mut writer = AofWriter::open_encrypted(&path, key.clone())?;
            for rec in &records {
                writer.write_record(rec)?;
            }
            writer.sync()?;
        }

        let mut reader = AofReader::open_encrypted(&path, key)?;
        let mut got = Vec::new();
        while let Some(rec) = reader.read_record()? {
            got.push(rec);
        }
        assert_eq!(records, got);
        Ok(())
    }

    #[test]
    fn encrypted_aof_wrong_key_fails() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("enc_bad.aof");
        let key = test_key();
        let wrong_key = EncryptionKey::from_bytes([0xFF; 32]);

        {
            let mut writer = AofWriter::open_encrypted(&path, key)?;
            writer.write_record(&AofRecord::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire_ms: -1,
            })?;
            writer.sync()?;
        }

        let mut reader = AofReader::open_encrypted(&path, wrong_key)?;
        let err = reader.read_record().unwrap_err();
        assert!(matches!(err, FormatError::DecryptionFailed));
        Ok(())
    }

    #[test]
    fn v2_file_readable_with_encryption_key() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("v2.aof");
        let key = test_key();

        // write a plaintext v2 file
        {
            let mut writer = AofWriter::open(&path)?;
            writer.write_record(&AofRecord::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire_ms: -1,
            })?;
            writer.sync()?;
        }

        // read with encryption key — should work (v2 is plaintext)
        let mut reader = AofReader::open_encrypted(&path, key)?;
        let rec = reader.read_record()?.unwrap();
        assert!(matches!(rec, AofRecord::Set { .. }));
        Ok(())
    }

    #[test]
    fn v3_file_without_key_returns_error() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("v3_nokey.aof");
        let key = test_key();

        // write an encrypted v3 file
        {
            let mut writer = AofWriter::open_encrypted(&path, key)?;
            writer.write_record(&AofRecord::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire_ms: -1,
            })?;
            writer.sync()?;
        }

        // try to open without a key
        let err = AofReader::open(&path).unwrap_err();
        assert!(matches!(err, FormatError::EncryptionRequired));
        Ok(())
    }

    #[test]
    fn switching_encryption_on_or_off_asks_for_a_rewrite() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("switch.aof");
        {
            let mut writer = AofWriter::open(&path)?;
            writer.write_record(&AofRecord::Del { key: "k".into() })?;
            writer.sync()?;
            assert!(!writer.needs_rewrite());
        }

        let mut writer = AofWriter::open_encrypted(&path, test_key())?;
        assert!(writer.needs_rewrite());
        writer.truncate(7)?;
        assert!(!writer.needs_rewrite());
        drop(writer);

        assert!(!AofWriter::open_encrypted(&path, test_key())?.needs_rewrite());
        assert!(AofWriter::open(&path)?.needs_rewrite());
        Ok(())
    }

    #[test]
    fn encrypted_truncate_preserves_encryption() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("enc_trunc.aof");
        let key = test_key();

        {
            let mut writer = AofWriter::open_encrypted(&path, key.clone())?;
            writer.write_record(&AofRecord::Set {
                key: "old".into(),
                value: Bytes::from("data"),
                expire_ms: -1,
            })?;
            writer.truncate(7)?;

            writer.write_record(&AofRecord::Set {
                key: "new".into(),
                value: Bytes::from("fresh"),
                expire_ms: -1,
            })?;
            writer.sync()?;
        }

        let mut reader = AofReader::open_encrypted(&path, key)?;
        assert_eq!(
            reader.read_record()?,
            Some(AofRecord::Checkpoint { snapshot_crc: 7 })
        );
        let rec = reader.read_record()?.unwrap();
        match rec {
            AofRecord::Set { key, .. } => assert_eq!(key, "new"),
            other => panic!("expected Set, got {other:?}"),
        }
        assert!(reader.read_record()?.is_none());
        Ok(())
    }

    fn del(key: &str) -> AofRecord {
        AofRecord::Del { key: key.into() }
    }

    /// Writes a v3 AOF, which encrypts with the master key directly.
    fn write_v3(path: &Path, key: &EncryptionKey, records: &[AofRecord]) -> Result {
        let mut file = Vec::new();
        format::write_header_versioned(
            &mut file,
            format::AOF_MAGIC,
            format::FORMAT_VERSION_ENCRYPTED_V3,
        )?;
        let cipher = key.legacy_cipher();
        for record in records {
            let (nonce, ciphertext) = cipher.encrypt(&record.to_bytes()?)?;
            file.extend_from_slice(&nonce);
            format::write_len(&mut file, ciphertext.len())?;
            file.extend_from_slice(&ciphertext);
        }
        fs::write(path, file)?;
        Ok(())
    }

    #[test]
    fn v3_file_is_still_read_and_needs_rewrite() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("v3.aof");
        let key = test_key();
        write_v3(&path, &key, &[del("a"), del("b")])?;

        let mut reader = AofReader::open_encrypted(&path, key.clone())?;
        assert_eq!(reader.read_record()?, Some(del("a")));
        assert_eq!(reader.read_record()?, Some(del("b")));
        assert_eq!(reader.read_record()?, None);

        let mut writer = AofWriter::open_encrypted(&path, key.clone())?;
        assert!(writer.needs_rewrite());
        writer.truncate(7)?;
        writer.write_record(&del("c"))?;
        writer.sync()?;

        let mut reader = AofReader::open_encrypted(&path, key)?;
        assert_eq!(reader.version, format::FORMAT_VERSION_ENCRYPTED);
        assert_eq!(
            reader.read_record()?,
            Some(AofRecord::Checkpoint { snapshot_crc: 7 })
        );
        assert_eq!(reader.read_record()?, Some(del("c")));
        Ok(())
    }

    #[test]
    fn reopened_v4_file_keeps_its_salt() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("enc.aof");
        let key = test_key();
        for name in ["a", "b"] {
            let mut writer = AofWriter::open_encrypted(&path, key.clone())?;
            assert!(!writer.needs_rewrite());
            writer.write_record(&del(name))?;
            writer.sync()?;
        }

        let mut reader = AofReader::open_encrypted(&path, key)?;
        assert_eq!(reader.read_record()?, Some(del("a")));
        assert_eq!(reader.read_record()?, Some(del("b")));
        Ok(())
    }

    #[test]
    fn record_copied_between_files_fails() -> Result {
        let dir = temp_dir();
        let key = test_key();
        let paths = [dir.path().join("1.aof"), dir.path().join("2.aof")];
        for path in &paths {
            let mut writer = AofWriter::open_encrypted(path, key.clone())?;
            writer.write_record(&del("a"))?;
            writer.sync()?;
        }

        // same header length, so the second file's record lines up
        // after the first file's header
        let header_len = 4 + 1 + crate::encryption::SALT_SIZE;
        let first = fs::read(&paths[0])?;
        let second = fs::read(&paths[1])?;
        let spliced = [&first[..header_len], &second[header_len..]].concat();
        fs::write(&paths[0], spliced)?;

        let mut reader = AofReader::open_encrypted(&paths[0], key)?;
        assert!(matches!(
            reader.read_record(),
            Err(FormatError::DecryptionFailed)
        ));
        Ok(())
    }
}
