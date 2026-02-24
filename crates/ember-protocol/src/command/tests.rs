use super::*;

use bytes::Bytes;
use crate::error::ProtocolError;
use crate::types::Frame;

/// Helper: build an array frame from bulk strings.
fn cmd(parts: &[&str]) -> Frame {
    Frame::Array(
        parts
            .iter()
            .map(|s| Frame::Bulk(Bytes::from(s.to_string())))
            .collect(),
    )
}

// --- ping ---

#[test]
fn ping_no_args() {
    assert_eq!(
        Command::from_frame(cmd(&["PING"])).unwrap(),
        Command::Ping(None),
    );
}

#[test]
fn ping_with_message() {
    assert_eq!(
        Command::from_frame(cmd(&["PING", "hello"])).unwrap(),
        Command::Ping(Some(Bytes::from("hello"))),
    );
}

#[test]
fn ping_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["ping"])).unwrap(),
        Command::Ping(None),
    );
    assert_eq!(
        Command::from_frame(cmd(&["Ping"])).unwrap(),
        Command::Ping(None),
    );
}

#[test]
fn ping_too_many_args() {
    let err = Command::from_frame(cmd(&["PING", "a", "b"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- echo ---

#[test]
fn echo() {
    assert_eq!(
        Command::from_frame(cmd(&["ECHO", "test"])).unwrap(),
        Command::Echo(Bytes::from("test")),
    );
}

#[test]
fn echo_missing_arg() {
    let err = Command::from_frame(cmd(&["ECHO"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- get ---

#[test]
fn get_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["GET", "mykey"])).unwrap(),
        Command::Get {
            key: "mykey".into()
        },
    );
}

#[test]
fn get_no_args() {
    let err = Command::from_frame(cmd(&["GET"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn get_too_many_args() {
    let err = Command::from_frame(cmd(&["GET", "a", "b"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn get_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["get", "k"])).unwrap(),
        Command::Get { key: "k".into() },
    );
}

// --- set ---

#[test]
fn set_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["SET", "key", "value"])).unwrap(),
        Command::Set {
            key: "key".into(),
            value: Bytes::from("value"),
            expire: None,
            nx: false,
            xx: false,
        },
    );
}

#[test]
fn set_with_ex() {
    assert_eq!(
        Command::from_frame(cmd(&["SET", "key", "val", "EX", "10"])).unwrap(),
        Command::Set {
            key: "key".into(),
            value: Bytes::from("val"),
            expire: Some(SetExpire::Ex(10)),
            nx: false,
            xx: false,
        },
    );
}

#[test]
fn set_with_px() {
    assert_eq!(
        Command::from_frame(cmd(&["SET", "key", "val", "PX", "5000"])).unwrap(),
        Command::Set {
            key: "key".into(),
            value: Bytes::from("val"),
            expire: Some(SetExpire::Px(5000)),
            nx: false,
            xx: false,
        },
    );
}

#[test]
fn set_ex_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["set", "k", "v", "ex", "5"])).unwrap(),
        Command::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire: Some(SetExpire::Ex(5)),
            nx: false,
            xx: false,
        },
    );
}

#[test]
fn set_nx_flag() {
    assert_eq!(
        Command::from_frame(cmd(&["SET", "key", "val", "NX"])).unwrap(),
        Command::Set {
            key: "key".into(),
            value: Bytes::from("val"),
            expire: None,
            nx: true,
            xx: false,
        },
    );
}

#[test]
fn set_xx_flag() {
    assert_eq!(
        Command::from_frame(cmd(&["SET", "key", "val", "XX"])).unwrap(),
        Command::Set {
            key: "key".into(),
            value: Bytes::from("val"),
            expire: None,
            nx: false,
            xx: true,
        },
    );
}

#[test]
fn set_nx_with_ex() {
    assert_eq!(
        Command::from_frame(cmd(&["SET", "key", "val", "EX", "10", "NX"])).unwrap(),
        Command::Set {
            key: "key".into(),
            value: Bytes::from("val"),
            expire: Some(SetExpire::Ex(10)),
            nx: true,
            xx: false,
        },
    );
}

#[test]
fn set_nx_before_ex() {
    assert_eq!(
        Command::from_frame(cmd(&["SET", "key", "val", "NX", "PX", "5000"])).unwrap(),
        Command::Set {
            key: "key".into(),
            value: Bytes::from("val"),
            expire: Some(SetExpire::Px(5000)),
            nx: true,
            xx: false,
        },
    );
}

#[test]
fn set_nx_xx_conflict() {
    let err = Command::from_frame(cmd(&["SET", "k", "v", "NX", "XX"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn set_nx_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["set", "k", "v", "nx"])).unwrap(),
        Command::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire: None,
            nx: true,
            xx: false,
        },
    );
}

#[test]
fn set_missing_value() {
    let err = Command::from_frame(cmd(&["SET", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn set_invalid_expire_value() {
    let err = Command::from_frame(cmd(&["SET", "k", "v", "EX", "notanum"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn set_zero_expire() {
    let err = Command::from_frame(cmd(&["SET", "k", "v", "EX", "0"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn set_unknown_flag() {
    let err = Command::from_frame(cmd(&["SET", "k", "v", "ZZ", "10"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn set_incomplete_expire() {
    // EX without a value
    let err = Command::from_frame(cmd(&["SET", "k", "v", "EX"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- del ---

#[test]
fn del_single() {
    assert_eq!(
        Command::from_frame(cmd(&["DEL", "key"])).unwrap(),
        Command::Del {
            keys: vec!["key".into()]
        },
    );
}

#[test]
fn del_multiple() {
    assert_eq!(
        Command::from_frame(cmd(&["DEL", "a", "b", "c"])).unwrap(),
        Command::Del {
            keys: vec!["a".into(), "b".into(), "c".into()]
        },
    );
}

#[test]
fn del_no_args() {
    let err = Command::from_frame(cmd(&["DEL"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- exists ---

#[test]
fn exists_single() {
    assert_eq!(
        Command::from_frame(cmd(&["EXISTS", "key"])).unwrap(),
        Command::Exists {
            keys: vec!["key".into()]
        },
    );
}

#[test]
fn exists_multiple() {
    assert_eq!(
        Command::from_frame(cmd(&["EXISTS", "a", "b"])).unwrap(),
        Command::Exists {
            keys: vec!["a".into(), "b".into()]
        },
    );
}

#[test]
fn exists_no_args() {
    let err = Command::from_frame(cmd(&["EXISTS"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- mget ---

#[test]
fn mget_single() {
    assert_eq!(
        Command::from_frame(cmd(&["MGET", "key"])).unwrap(),
        Command::MGet {
            keys: vec!["key".into()]
        },
    );
}

#[test]
fn mget_multiple() {
    assert_eq!(
        Command::from_frame(cmd(&["MGET", "a", "b", "c"])).unwrap(),
        Command::MGet {
            keys: vec!["a".into(), "b".into(), "c".into()]
        },
    );
}

#[test]
fn mget_no_args() {
    let err = Command::from_frame(cmd(&["MGET"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- mset ---

#[test]
fn mset_single_pair() {
    assert_eq!(
        Command::from_frame(cmd(&["MSET", "key", "val"])).unwrap(),
        Command::MSet {
            pairs: vec![("key".into(), Bytes::from("val"))]
        },
    );
}

#[test]
fn mset_multiple_pairs() {
    assert_eq!(
        Command::from_frame(cmd(&["MSET", "a", "1", "b", "2"])).unwrap(),
        Command::MSet {
            pairs: vec![
                ("a".into(), Bytes::from("1")),
                ("b".into(), Bytes::from("2")),
            ]
        },
    );
}

#[test]
fn mset_no_args() {
    let err = Command::from_frame(cmd(&["MSET"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn mset_odd_args() {
    // missing value for second key
    let err = Command::from_frame(cmd(&["MSET", "a", "1", "b"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- expire ---

#[test]
fn expire_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["EXPIRE", "key", "60"])).unwrap(),
        Command::Expire {
            key: "key".into(),
            seconds: 60,
        },
    );
}

#[test]
fn expire_wrong_arity() {
    let err = Command::from_frame(cmd(&["EXPIRE", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn expire_invalid_seconds() {
    let err = Command::from_frame(cmd(&["EXPIRE", "key", "abc"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn expire_zero_seconds() {
    let err = Command::from_frame(cmd(&["EXPIRE", "key", "0"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

// --- ttl ---

#[test]
fn ttl_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["TTL", "key"])).unwrap(),
        Command::Ttl { key: "key".into() },
    );
}

#[test]
fn ttl_wrong_arity() {
    let err = Command::from_frame(cmd(&["TTL"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- dbsize ---

#[test]
fn dbsize_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["DBSIZE"])).unwrap(),
        Command::DbSize,
    );
}

#[test]
fn dbsize_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["dbsize"])).unwrap(),
        Command::DbSize,
    );
}

#[test]
fn dbsize_extra_args() {
    let err = Command::from_frame(cmd(&["DBSIZE", "extra"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- info ---

#[test]
fn info_no_section() {
    assert_eq!(
        Command::from_frame(cmd(&["INFO"])).unwrap(),
        Command::Info { section: None },
    );
}

#[test]
fn info_with_section() {
    assert_eq!(
        Command::from_frame(cmd(&["INFO", "keyspace"])).unwrap(),
        Command::Info {
            section: Some("keyspace".into())
        },
    );
}

#[test]
fn info_too_many_args() {
    let err = Command::from_frame(cmd(&["INFO", "a", "b"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- bgsave ---

#[test]
fn bgsave_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["BGSAVE"])).unwrap(),
        Command::BgSave,
    );
}

#[test]
fn bgsave_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["bgsave"])).unwrap(),
        Command::BgSave,
    );
}

#[test]
fn bgsave_extra_args() {
    let err = Command::from_frame(cmd(&["BGSAVE", "extra"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- bgrewriteaof ---

#[test]
fn bgrewriteaof_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["BGREWRITEAOF"])).unwrap(),
        Command::BgRewriteAof,
    );
}

#[test]
fn bgrewriteaof_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["bgrewriteaof"])).unwrap(),
        Command::BgRewriteAof,
    );
}

#[test]
fn bgrewriteaof_extra_args() {
    let err = Command::from_frame(cmd(&["BGREWRITEAOF", "extra"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- flushdb ---

#[test]
fn flushdb_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["FLUSHDB"])).unwrap(),
        Command::FlushDb { async_mode: false },
    );
}

#[test]
fn flushdb_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["flushdb"])).unwrap(),
        Command::FlushDb { async_mode: false },
    );
}

#[test]
fn flushdb_async() {
    assert_eq!(
        Command::from_frame(cmd(&["FLUSHDB", "ASYNC"])).unwrap(),
        Command::FlushDb { async_mode: true },
    );
}

#[test]
fn flushdb_async_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["flushdb", "async"])).unwrap(),
        Command::FlushDb { async_mode: true },
    );
}

#[test]
fn flushdb_extra_args() {
    let err = Command::from_frame(cmd(&["FLUSHDB", "extra"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- unlink ---

#[test]
fn unlink_single() {
    assert_eq!(
        Command::from_frame(cmd(&["UNLINK", "mykey"])).unwrap(),
        Command::Unlink {
            keys: vec!["mykey".into()]
        },
    );
}

#[test]
fn unlink_multiple() {
    assert_eq!(
        Command::from_frame(cmd(&["UNLINK", "a", "b", "c"])).unwrap(),
        Command::Unlink {
            keys: vec!["a".into(), "b".into(), "c".into()]
        },
    );
}

#[test]
fn unlink_no_args() {
    let err = Command::from_frame(cmd(&["UNLINK"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- lpush ---

#[test]
fn lpush_single() {
    assert_eq!(
        Command::from_frame(cmd(&["LPUSH", "list", "val"])).unwrap(),
        Command::LPush {
            key: "list".into(),
            values: vec![Bytes::from("val")],
        },
    );
}

#[test]
fn lpush_multiple() {
    assert_eq!(
        Command::from_frame(cmd(&["LPUSH", "list", "a", "b", "c"])).unwrap(),
        Command::LPush {
            key: "list".into(),
            values: vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
        },
    );
}

#[test]
fn lpush_no_value() {
    let err = Command::from_frame(cmd(&["LPUSH", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn lpush_case_insensitive() {
    assert!(matches!(
        Command::from_frame(cmd(&["lpush", "k", "v"])).unwrap(),
        Command::LPush { .. }
    ));
}

// --- rpush ---

#[test]
fn rpush_single() {
    assert_eq!(
        Command::from_frame(cmd(&["RPUSH", "list", "val"])).unwrap(),
        Command::RPush {
            key: "list".into(),
            values: vec![Bytes::from("val")],
        },
    );
}

#[test]
fn rpush_no_value() {
    let err = Command::from_frame(cmd(&["RPUSH", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- lpop ---

#[test]
fn lpop_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["LPOP", "list"])).unwrap(),
        Command::LPop { key: "list".into() },
    );
}

#[test]
fn lpop_wrong_arity() {
    let err = Command::from_frame(cmd(&["LPOP"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- rpop ---

#[test]
fn rpop_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["RPOP", "list"])).unwrap(),
        Command::RPop { key: "list".into() },
    );
}

// --- blpop ---

#[test]
fn blpop_single_key() {
    assert_eq!(
        Command::from_frame(cmd(&["BLPOP", "mylist", "5"])).unwrap(),
        Command::BLPop {
            keys: vec!["mylist".into()],
            timeout_secs: 5.0,
        },
    );
}

#[test]
fn blpop_multi_key() {
    assert_eq!(
        Command::from_frame(cmd(&["BLPOP", "a", "b", "c", "0"])).unwrap(),
        Command::BLPop {
            keys: vec!["a".into(), "b".into(), "c".into()],
            timeout_secs: 0.0,
        },
    );
}

#[test]
fn blpop_float_timeout() {
    assert_eq!(
        Command::from_frame(cmd(&["BLPOP", "q", "1.5"])).unwrap(),
        Command::BLPop {
            keys: vec!["q".into()],
            timeout_secs: 1.5,
        },
    );
}

#[test]
fn blpop_wrong_arity() {
    let err = Command::from_frame(cmd(&["BLPOP"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn blpop_negative_timeout() {
    let err = Command::from_frame(cmd(&["BLPOP", "k", "-1"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

// --- brpop ---

#[test]
fn brpop_single_key() {
    assert_eq!(
        Command::from_frame(cmd(&["BRPOP", "mylist", "10"])).unwrap(),
        Command::BRPop {
            keys: vec!["mylist".into()],
            timeout_secs: 10.0,
        },
    );
}

#[test]
fn brpop_wrong_arity() {
    let err = Command::from_frame(cmd(&["BRPOP"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- lrange ---

#[test]
fn lrange_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["LRANGE", "list", "0", "-1"])).unwrap(),
        Command::LRange {
            key: "list".into(),
            start: 0,
            stop: -1,
        },
    );
}

#[test]
fn lrange_wrong_arity() {
    let err = Command::from_frame(cmd(&["LRANGE", "list", "0"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn lrange_invalid_index() {
    let err = Command::from_frame(cmd(&["LRANGE", "list", "abc", "0"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

// --- llen ---

#[test]
fn llen_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["LLEN", "list"])).unwrap(),
        Command::LLen { key: "list".into() },
    );
}

#[test]
fn llen_wrong_arity() {
    let err = Command::from_frame(cmd(&["LLEN"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- type ---

#[test]
fn type_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["TYPE", "key"])).unwrap(),
        Command::Type { key: "key".into() },
    );
}

#[test]
fn type_wrong_arity() {
    let err = Command::from_frame(cmd(&["TYPE"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn type_case_insensitive() {
    assert!(matches!(
        Command::from_frame(cmd(&["type", "k"])).unwrap(),
        Command::Type { .. }
    ));
}

// --- general ---

#[test]
fn unknown_command() {
    assert_eq!(
        Command::from_frame(cmd(&["FOOBAR", "arg"])).unwrap(),
        Command::Unknown("FOOBAR".into()),
    );
}

#[test]
fn non_array_frame() {
    let err = Command::from_frame(Frame::Simple("PING".into())).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn empty_array() {
    let err = Command::from_frame(Frame::Array(vec![])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

// --- zadd ---

#[test]
fn zadd_basic() {
    let parsed = Command::from_frame(cmd(&["ZADD", "board", "100", "alice"])).unwrap();
    match parsed {
        Command::ZAdd {
            key,
            flags,
            members,
        } => {
            assert_eq!(key, "board");
            assert_eq!(flags, ZAddFlags::default());
            assert_eq!(members, vec![(100.0, "alice".into())]);
        }
        other => panic!("expected ZAdd, got {other:?}"),
    }
}

#[test]
fn zadd_multiple_members() {
    let parsed =
        Command::from_frame(cmd(&["ZADD", "board", "100", "alice", "200", "bob"])).unwrap();
    match parsed {
        Command::ZAdd { members, .. } => {
            assert_eq!(members.len(), 2);
            assert_eq!(members[0], (100.0, "alice".into()));
            assert_eq!(members[1], (200.0, "bob".into()));
        }
        other => panic!("expected ZAdd, got {other:?}"),
    }
}

#[test]
fn zadd_with_flags() {
    let parsed = Command::from_frame(cmd(&["ZADD", "z", "NX", "CH", "100", "alice"])).unwrap();
    match parsed {
        Command::ZAdd { flags, .. } => {
            assert!(flags.nx);
            assert!(flags.ch);
            assert!(!flags.xx);
            assert!(!flags.gt);
            assert!(!flags.lt);
        }
        other => panic!("expected ZAdd, got {other:?}"),
    }
}

#[test]
fn zadd_gt_flag() {
    let parsed = Command::from_frame(cmd(&["zadd", "z", "gt", "100", "alice"])).unwrap();
    match parsed {
        Command::ZAdd { flags, .. } => assert!(flags.gt),
        other => panic!("expected ZAdd, got {other:?}"),
    }
}

#[test]
fn zadd_nx_xx_conflict() {
    let err = Command::from_frame(cmd(&["ZADD", "z", "NX", "XX", "100", "alice"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn zadd_gt_lt_conflict() {
    let err = Command::from_frame(cmd(&["ZADD", "z", "GT", "LT", "100", "alice"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn zadd_wrong_arity() {
    let err = Command::from_frame(cmd(&["ZADD", "z"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn zadd_odd_score_member_count() {
    // one score without a member
    let err = Command::from_frame(cmd(&["ZADD", "z", "100"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn zadd_invalid_score() {
    let err = Command::from_frame(cmd(&["ZADD", "z", "notanum", "alice"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn zadd_nan_score() {
    let err = Command::from_frame(cmd(&["ZADD", "z", "nan", "alice"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn zadd_negative_score() {
    let parsed = Command::from_frame(cmd(&["ZADD", "z", "-50.5", "alice"])).unwrap();
    match parsed {
        Command::ZAdd { members, .. } => {
            assert_eq!(members[0].0, -50.5);
        }
        other => panic!("expected ZAdd, got {other:?}"),
    }
}

// --- zrem ---

#[test]
fn zrem_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["ZREM", "z", "alice"])).unwrap(),
        Command::ZRem {
            key: "z".into(),
            members: vec!["alice".into()],
        },
    );
}

#[test]
fn zrem_multiple() {
    let parsed = Command::from_frame(cmd(&["ZREM", "z", "a", "b", "c"])).unwrap();
    match parsed {
        Command::ZRem { members, .. } => assert_eq!(members.len(), 3),
        other => panic!("expected ZRem, got {other:?}"),
    }
}

#[test]
fn zrem_wrong_arity() {
    let err = Command::from_frame(cmd(&["ZREM", "z"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- zscore ---

#[test]
fn zscore_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["ZSCORE", "z", "alice"])).unwrap(),
        Command::ZScore {
            key: "z".into(),
            member: "alice".into(),
        },
    );
}

#[test]
fn zscore_wrong_arity() {
    let err = Command::from_frame(cmd(&["ZSCORE", "z"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- zrank ---

#[test]
fn zrank_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["ZRANK", "z", "alice"])).unwrap(),
        Command::ZRank {
            key: "z".into(),
            member: "alice".into(),
        },
    );
}

#[test]
fn zrank_wrong_arity() {
    let err = Command::from_frame(cmd(&["ZRANK", "z"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- zcard ---

#[test]
fn zcard_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["ZCARD", "z"])).unwrap(),
        Command::ZCard { key: "z".into() },
    );
}

#[test]
fn zcard_wrong_arity() {
    let err = Command::from_frame(cmd(&["ZCARD"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- zrange ---

#[test]
fn zrange_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["ZRANGE", "z", "0", "-1"])).unwrap(),
        Command::ZRange {
            key: "z".into(),
            start: 0,
            stop: -1,
            with_scores: false,
        },
    );
}

#[test]
fn zrange_with_scores() {
    assert_eq!(
        Command::from_frame(cmd(&["ZRANGE", "z", "0", "-1", "WITHSCORES"])).unwrap(),
        Command::ZRange {
            key: "z".into(),
            start: 0,
            stop: -1,
            with_scores: true,
        },
    );
}

#[test]
fn zrange_withscores_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["zrange", "z", "0", "-1", "withscores"])).unwrap(),
        Command::ZRange {
            key: "z".into(),
            start: 0,
            stop: -1,
            with_scores: true,
        },
    );
}

#[test]
fn zrange_invalid_option() {
    let err = Command::from_frame(cmd(&["ZRANGE", "z", "0", "-1", "BADOPT"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn zrange_wrong_arity() {
    let err = Command::from_frame(cmd(&["ZRANGE", "z", "0"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- incr ---

#[test]
fn incr_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["INCR", "counter"])).unwrap(),
        Command::Incr {
            key: "counter".into()
        },
    );
}

#[test]
fn incr_wrong_arity() {
    let err = Command::from_frame(cmd(&["INCR"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- decr ---

#[test]
fn decr_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["DECR", "counter"])).unwrap(),
        Command::Decr {
            key: "counter".into()
        },
    );
}

#[test]
fn decr_wrong_arity() {
    let err = Command::from_frame(cmd(&["DECR"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- persist ---

#[test]
fn persist_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PERSIST", "key"])).unwrap(),
        Command::Persist { key: "key".into() },
    );
}

#[test]
fn persist_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["persist", "key"])).unwrap(),
        Command::Persist { key: "key".into() },
    );
}

#[test]
fn persist_wrong_arity() {
    let err = Command::from_frame(cmd(&["PERSIST"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- pttl ---

#[test]
fn pttl_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PTTL", "key"])).unwrap(),
        Command::Pttl { key: "key".into() },
    );
}

#[test]
fn pttl_wrong_arity() {
    let err = Command::from_frame(cmd(&["PTTL"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- pexpire ---

#[test]
fn pexpire_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PEXPIRE", "key", "5000"])).unwrap(),
        Command::Pexpire {
            key: "key".into(),
            milliseconds: 5000,
        },
    );
}

#[test]
fn pexpire_wrong_arity() {
    let err = Command::from_frame(cmd(&["PEXPIRE", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn pexpire_zero_millis() {
    let err = Command::from_frame(cmd(&["PEXPIRE", "key", "0"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn pexpire_invalid_millis() {
    let err = Command::from_frame(cmd(&["PEXPIRE", "key", "notanum"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

// --- scan ---

#[test]
fn scan_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["SCAN", "0"])).unwrap(),
        Command::Scan {
            cursor: 0,
            pattern: None,
            count: None,
        },
    );
}

#[test]
fn scan_with_match() {
    assert_eq!(
        Command::from_frame(cmd(&["SCAN", "0", "MATCH", "user:*"])).unwrap(),
        Command::Scan {
            cursor: 0,
            pattern: Some("user:*".into()),
            count: None,
        },
    );
}

#[test]
fn scan_with_count() {
    assert_eq!(
        Command::from_frame(cmd(&["SCAN", "42", "COUNT", "100"])).unwrap(),
        Command::Scan {
            cursor: 42,
            pattern: None,
            count: Some(100),
        },
    );
}

#[test]
fn scan_with_match_and_count() {
    assert_eq!(
        Command::from_frame(cmd(&["SCAN", "0", "MATCH", "*:data", "COUNT", "50"])).unwrap(),
        Command::Scan {
            cursor: 0,
            pattern: Some("*:data".into()),
            count: Some(50),
        },
    );
}

#[test]
fn scan_count_before_match() {
    assert_eq!(
        Command::from_frame(cmd(&["SCAN", "0", "COUNT", "10", "MATCH", "foo*"])).unwrap(),
        Command::Scan {
            cursor: 0,
            pattern: Some("foo*".into()),
            count: Some(10),
        },
    );
}

#[test]
fn scan_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["scan", "0", "match", "x*", "count", "5"])).unwrap(),
        Command::Scan {
            cursor: 0,
            pattern: Some("x*".into()),
            count: Some(5),
        },
    );
}

#[test]
fn scan_wrong_arity() {
    let err = Command::from_frame(cmd(&["SCAN"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn scan_invalid_cursor() {
    let err = Command::from_frame(cmd(&["SCAN", "notanum"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn scan_invalid_count() {
    let err = Command::from_frame(cmd(&["SCAN", "0", "COUNT", "bad"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn scan_unknown_flag() {
    let err = Command::from_frame(cmd(&["SCAN", "0", "BADOPT", "val"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn scan_match_missing_pattern() {
    let err = Command::from_frame(cmd(&["SCAN", "0", "MATCH"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn scan_count_missing_value() {
    let err = Command::from_frame(cmd(&["SCAN", "0", "COUNT"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- collection scan commands ---

#[test]
fn sscan_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["SSCAN", "myset", "0"])).unwrap(),
        Command::SScan {
            key: "myset".into(),
            cursor: 0,
            pattern: None,
            count: None,
        },
    );
}

#[test]
fn sscan_with_options() {
    assert_eq!(
        Command::from_frame(cmd(&[
            "SSCAN", "myset", "0", "MATCH", "user:*", "COUNT", "100"
        ]))
        .unwrap(),
        Command::SScan {
            key: "myset".into(),
            cursor: 0,
            pattern: Some("user:*".into()),
            count: Some(100),
        },
    );
}

#[test]
fn sscan_missing_cursor() {
    let err = Command::from_frame(cmd(&["SSCAN", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn hscan_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["HSCAN", "myhash", "5"])).unwrap(),
        Command::HScan {
            key: "myhash".into(),
            cursor: 5,
            pattern: None,
            count: None,
        },
    );
}

#[test]
fn zscan_with_match() {
    assert_eq!(
        Command::from_frame(cmd(&["ZSCAN", "myzset", "0", "MATCH", "player:*"])).unwrap(),
        Command::ZScan {
            key: "myzset".into(),
            cursor: 0,
            pattern: Some("player:*".into()),
            count: None,
        },
    );
}

#[test]
fn sscan_no_args() {
    let err = Command::from_frame(cmd(&["SSCAN"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- hash commands ---

#[test]
fn hset_single_field() {
    assert_eq!(
        Command::from_frame(cmd(&["HSET", "h", "field", "value"])).unwrap(),
        Command::HSet {
            key: "h".into(),
            fields: vec![("field".into(), Bytes::from("value"))],
        },
    );
}

#[test]
fn hset_multiple_fields() {
    let parsed = Command::from_frame(cmd(&["HSET", "h", "f1", "v1", "f2", "v2"])).unwrap();
    match parsed {
        Command::HSet { key, fields } => {
            assert_eq!(key, "h");
            assert_eq!(fields.len(), 2);
        }
        other => panic!("expected HSet, got {other:?}"),
    }
}

#[test]
fn hset_wrong_arity() {
    let err = Command::from_frame(cmd(&["HSET", "h"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
    let err = Command::from_frame(cmd(&["HSET", "h", "f"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn hget_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["HGET", "h", "field"])).unwrap(),
        Command::HGet {
            key: "h".into(),
            field: "field".into(),
        },
    );
}

#[test]
fn hget_wrong_arity() {
    let err = Command::from_frame(cmd(&["HGET", "h"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn hgetall_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["HGETALL", "h"])).unwrap(),
        Command::HGetAll { key: "h".into() },
    );
}

#[test]
fn hgetall_wrong_arity() {
    let err = Command::from_frame(cmd(&["HGETALL"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn hdel_single() {
    assert_eq!(
        Command::from_frame(cmd(&["HDEL", "h", "f"])).unwrap(),
        Command::HDel {
            key: "h".into(),
            fields: vec!["f".into()],
        },
    );
}

#[test]
fn hdel_multiple() {
    let parsed = Command::from_frame(cmd(&["HDEL", "h", "f1", "f2", "f3"])).unwrap();
    match parsed {
        Command::HDel { fields, .. } => assert_eq!(fields.len(), 3),
        other => panic!("expected HDel, got {other:?}"),
    }
}

#[test]
fn hdel_wrong_arity() {
    let err = Command::from_frame(cmd(&["HDEL", "h"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn hexists_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["HEXISTS", "h", "f"])).unwrap(),
        Command::HExists {
            key: "h".into(),
            field: "f".into(),
        },
    );
}

#[test]
fn hlen_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["HLEN", "h"])).unwrap(),
        Command::HLen { key: "h".into() },
    );
}

#[test]
fn hincrby_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["HINCRBY", "h", "f", "5"])).unwrap(),
        Command::HIncrBy {
            key: "h".into(),
            field: "f".into(),
            delta: 5,
        },
    );
}

#[test]
fn hincrby_negative() {
    assert_eq!(
        Command::from_frame(cmd(&["HINCRBY", "h", "f", "-3"])).unwrap(),
        Command::HIncrBy {
            key: "h".into(),
            field: "f".into(),
            delta: -3,
        },
    );
}

#[test]
fn hincrby_wrong_arity() {
    let err = Command::from_frame(cmd(&["HINCRBY", "h", "f"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn hkeys_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["HKEYS", "h"])).unwrap(),
        Command::HKeys { key: "h".into() },
    );
}

#[test]
fn hvals_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["HVALS", "h"])).unwrap(),
        Command::HVals { key: "h".into() },
    );
}

#[test]
fn hmget_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["HMGET", "h", "f1", "f2"])).unwrap(),
        Command::HMGet {
            key: "h".into(),
            fields: vec!["f1".into(), "f2".into()],
        },
    );
}

#[test]
fn hmget_wrong_arity() {
    let err = Command::from_frame(cmd(&["HMGET", "h"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn hash_commands_case_insensitive() {
    assert!(matches!(
        Command::from_frame(cmd(&["hset", "h", "f", "v"])).unwrap(),
        Command::HSet { .. }
    ));
    assert!(matches!(
        Command::from_frame(cmd(&["hget", "h", "f"])).unwrap(),
        Command::HGet { .. }
    ));
    assert!(matches!(
        Command::from_frame(cmd(&["hgetall", "h"])).unwrap(),
        Command::HGetAll { .. }
    ));
}

// --- set commands ---

#[test]
fn sadd_single_member() {
    assert_eq!(
        Command::from_frame(cmd(&["SADD", "s", "member"])).unwrap(),
        Command::SAdd {
            key: "s".into(),
            members: vec!["member".into()],
        },
    );
}

#[test]
fn sadd_multiple_members() {
    let parsed = Command::from_frame(cmd(&["SADD", "s", "a", "b", "c"])).unwrap();
    match parsed {
        Command::SAdd { key, members } => {
            assert_eq!(key, "s");
            assert_eq!(members.len(), 3);
        }
        other => panic!("expected SAdd, got {other:?}"),
    }
}

#[test]
fn sadd_wrong_arity() {
    let err = Command::from_frame(cmd(&["SADD", "s"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn srem_single_member() {
    assert_eq!(
        Command::from_frame(cmd(&["SREM", "s", "member"])).unwrap(),
        Command::SRem {
            key: "s".into(),
            members: vec!["member".into()],
        },
    );
}

#[test]
fn srem_multiple_members() {
    let parsed = Command::from_frame(cmd(&["SREM", "s", "a", "b"])).unwrap();
    match parsed {
        Command::SRem { key, members } => {
            assert_eq!(key, "s");
            assert_eq!(members.len(), 2);
        }
        other => panic!("expected SRem, got {other:?}"),
    }
}

#[test]
fn srem_wrong_arity() {
    let err = Command::from_frame(cmd(&["SREM", "s"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn smembers_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["SMEMBERS", "s"])).unwrap(),
        Command::SMembers { key: "s".into() },
    );
}

#[test]
fn smembers_wrong_arity() {
    let err = Command::from_frame(cmd(&["SMEMBERS"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn sismember_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["SISMEMBER", "s", "member"])).unwrap(),
        Command::SIsMember {
            key: "s".into(),
            member: "member".into(),
        },
    );
}

#[test]
fn sismember_wrong_arity() {
    let err = Command::from_frame(cmd(&["SISMEMBER", "s"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn scard_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["SCARD", "s"])).unwrap(),
        Command::SCard { key: "s".into() },
    );
}

#[test]
fn scard_wrong_arity() {
    let err = Command::from_frame(cmd(&["SCARD"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn set_commands_case_insensitive() {
    assert!(matches!(
        Command::from_frame(cmd(&["sadd", "s", "m"])).unwrap(),
        Command::SAdd { .. }
    ));
    assert!(matches!(
        Command::from_frame(cmd(&["srem", "s", "m"])).unwrap(),
        Command::SRem { .. }
    ));
    assert!(matches!(
        Command::from_frame(cmd(&["smembers", "s"])).unwrap(),
        Command::SMembers { .. }
    ));
}

// --- cluster commands ---

#[test]
fn cluster_info_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "INFO"])).unwrap(),
        Command::ClusterInfo,
    );
}

#[test]
fn cluster_nodes_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "NODES"])).unwrap(),
        Command::ClusterNodes,
    );
}

#[test]
fn cluster_slots_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "SLOTS"])).unwrap(),
        Command::ClusterSlots,
    );
}

#[test]
fn cluster_keyslot_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "KEYSLOT", "mykey"])).unwrap(),
        Command::ClusterKeySlot {
            key: "mykey".into()
        },
    );
}

#[test]
fn cluster_keyslot_wrong_arity() {
    let err = Command::from_frame(cmd(&["CLUSTER", "KEYSLOT"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn cluster_myid_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "MYID"])).unwrap(),
        Command::ClusterMyId,
    );
}

#[test]
fn cluster_unknown_subcommand() {
    let err = Command::from_frame(cmd(&["CLUSTER", "BADCMD"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn cluster_no_subcommand() {
    let err = Command::from_frame(cmd(&["CLUSTER"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn cluster_case_insensitive() {
    assert!(matches!(
        Command::from_frame(cmd(&["cluster", "info"])).unwrap(),
        Command::ClusterInfo
    ));
    assert!(matches!(
        Command::from_frame(cmd(&["cluster", "keyslot", "k"])).unwrap(),
        Command::ClusterKeySlot { .. }
    ));
}

#[test]
fn asking_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["ASKING"])).unwrap(),
        Command::Asking,
    );
}

#[test]
fn asking_wrong_arity() {
    let err = Command::from_frame(cmd(&["ASKING", "extra"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn cluster_setslot_importing() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "100", "IMPORTING", "node123"]))
            .unwrap(),
        Command::ClusterSetSlotImporting {
            slot: 100,
            node_id: "node123".into()
        },
    );
}

#[test]
fn cluster_setslot_migrating() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "200", "MIGRATING", "node456"]))
            .unwrap(),
        Command::ClusterSetSlotMigrating {
            slot: 200,
            node_id: "node456".into()
        },
    );
}

#[test]
fn cluster_setslot_node() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "300", "NODE", "node789"])).unwrap(),
        Command::ClusterSetSlotNode {
            slot: 300,
            node_id: "node789".into()
        },
    );
}

#[test]
fn cluster_setslot_stable() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "400", "STABLE"])).unwrap(),
        Command::ClusterSetSlotStable { slot: 400 },
    );
}

#[test]
fn cluster_setslot_invalid_slot() {
    let err =
        Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "notanumber", "STABLE"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn cluster_setslot_wrong_arity() {
    let err = Command::from_frame(cmd(&["CLUSTER", "SETSLOT", "100"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn migrate_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["MIGRATE", "127.0.0.1", "6379", "mykey", "0", "5000"]))
            .unwrap(),
        Command::Migrate {
            host: "127.0.0.1".into(),
            port: 6379,
            key: "mykey".into(),
            db: 0,
            timeout_ms: 5000,
            copy: false,
            replace: false,
        },
    );
}

#[test]
fn migrate_with_options() {
    assert_eq!(
        Command::from_frame(cmd(&[
            "MIGRATE",
            "192.168.1.1",
            "6380",
            "testkey",
            "1",
            "10000",
            "COPY",
            "REPLACE"
        ]))
        .unwrap(),
        Command::Migrate {
            host: "192.168.1.1".into(),
            port: 6380,
            key: "testkey".into(),
            db: 1,
            timeout_ms: 10000,
            copy: true,
            replace: true,
        },
    );
}

#[test]
fn migrate_wrong_arity() {
    let err = Command::from_frame(cmd(&["MIGRATE", "host", "port", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn migrate_invalid_port() {
    let err = Command::from_frame(cmd(&["MIGRATE", "host", "notaport", "key", "0", "1000"]))
        .unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn cluster_meet_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "MEET", "192.168.1.1", "6379"])).unwrap(),
        Command::ClusterMeet {
            ip: "192.168.1.1".into(),
            port: 6379
        },
    );
}

#[test]
fn cluster_addslots_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "ADDSLOTS", "0", "1", "2"])).unwrap(),
        Command::ClusterAddSlots {
            slots: vec![0, 1, 2]
        },
    );
}

#[test]
fn cluster_addslotsrange_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "ADDSLOTSRANGE", "0", "5460"])).unwrap(),
        Command::ClusterAddSlotsRange {
            ranges: vec![(0, 5460)]
        },
    );
}

#[test]
fn cluster_addslotsrange_multiple() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "ADDSLOTSRANGE", "0", "100", "200", "300"]))
            .unwrap(),
        Command::ClusterAddSlotsRange {
            ranges: vec![(0, 100), (200, 300)]
        },
    );
}

#[test]
fn cluster_addslotsrange_invalid_range() {
    let err = Command::from_frame(cmd(&["CLUSTER", "ADDSLOTSRANGE", "100", "50"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn cluster_addslotsrange_wrong_arity() {
    // odd number of slot args
    let err = Command::from_frame(cmd(&["CLUSTER", "ADDSLOTSRANGE", "0"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn cluster_delslots_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "DELSLOTS", "100", "101"])).unwrap(),
        Command::ClusterDelSlots {
            slots: vec![100, 101]
        },
    );
}

#[test]
fn cluster_forget_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "FORGET", "abc123"])).unwrap(),
        Command::ClusterForget {
            node_id: "abc123".into()
        },
    );
}

#[test]
fn cluster_replicate_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "REPLICATE", "master-id"])).unwrap(),
        Command::ClusterReplicate {
            node_id: "master-id".into()
        },
    );
}

#[test]
fn cluster_failover_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "FAILOVER"])).unwrap(),
        Command::ClusterFailover {
            force: false,
            takeover: false
        },
    );
}

#[test]
fn cluster_failover_force() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "FAILOVER", "FORCE"])).unwrap(),
        Command::ClusterFailover {
            force: true,
            takeover: false
        },
    );
}

#[test]
fn cluster_failover_takeover() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "FAILOVER", "TAKEOVER"])).unwrap(),
        Command::ClusterFailover {
            force: false,
            takeover: true
        },
    );
}

#[test]
fn cluster_countkeysinslot_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "COUNTKEYSINSLOT", "100"])).unwrap(),
        Command::ClusterCountKeysInSlot { slot: 100 },
    );
}

#[test]
fn cluster_getkeysinslot_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["CLUSTER", "GETKEYSINSLOT", "200", "10"])).unwrap(),
        Command::ClusterGetKeysInSlot {
            slot: 200,
            count: 10
        },
    );
}

// --- pub/sub ---

#[test]
fn subscribe_single_channel() {
    assert_eq!(
        Command::from_frame(cmd(&["SUBSCRIBE", "news"])).unwrap(),
        Command::Subscribe {
            channels: vec!["news".into()]
        },
    );
}

#[test]
fn subscribe_multiple_channels() {
    assert_eq!(
        Command::from_frame(cmd(&["SUBSCRIBE", "ch1", "ch2", "ch3"])).unwrap(),
        Command::Subscribe {
            channels: vec!["ch1".into(), "ch2".into(), "ch3".into()]
        },
    );
}

#[test]
fn subscribe_no_args() {
    let err = Command::from_frame(cmd(&["SUBSCRIBE"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn unsubscribe_all() {
    assert_eq!(
        Command::from_frame(cmd(&["UNSUBSCRIBE"])).unwrap(),
        Command::Unsubscribe { channels: vec![] },
    );
}

#[test]
fn unsubscribe_specific() {
    assert_eq!(
        Command::from_frame(cmd(&["UNSUBSCRIBE", "news"])).unwrap(),
        Command::Unsubscribe {
            channels: vec!["news".into()]
        },
    );
}

#[test]
fn psubscribe_pattern() {
    assert_eq!(
        Command::from_frame(cmd(&["PSUBSCRIBE", "news.*"])).unwrap(),
        Command::PSubscribe {
            patterns: vec!["news.*".into()]
        },
    );
}

#[test]
fn psubscribe_no_args() {
    let err = Command::from_frame(cmd(&["PSUBSCRIBE"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn punsubscribe_all() {
    assert_eq!(
        Command::from_frame(cmd(&["PUNSUBSCRIBE"])).unwrap(),
        Command::PUnsubscribe { patterns: vec![] },
    );
}

#[test]
fn publish_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PUBLISH", "news", "hello world"])).unwrap(),
        Command::Publish {
            channel: "news".into(),
            message: Bytes::from("hello world"),
        },
    );
}

#[test]
fn publish_wrong_arity() {
    let err = Command::from_frame(cmd(&["PUBLISH", "news"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn subscribe_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["subscribe", "ch"])).unwrap(),
        Command::Subscribe {
            channels: vec!["ch".into()]
        },
    );
}

#[test]
fn pubsub_channels_no_pattern() {
    assert_eq!(
        Command::from_frame(cmd(&["PUBSUB", "CHANNELS"])).unwrap(),
        Command::PubSubChannels { pattern: None },
    );
}

#[test]
fn pubsub_channels_with_pattern() {
    assert_eq!(
        Command::from_frame(cmd(&["PUBSUB", "CHANNELS", "news.*"])).unwrap(),
        Command::PubSubChannels {
            pattern: Some("news.*".into())
        },
    );
}

#[test]
fn pubsub_numsub_no_args() {
    assert_eq!(
        Command::from_frame(cmd(&["PUBSUB", "NUMSUB"])).unwrap(),
        Command::PubSubNumSub { channels: vec![] },
    );
}

#[test]
fn pubsub_numsub_with_channels() {
    assert_eq!(
        Command::from_frame(cmd(&["PUBSUB", "NUMSUB", "ch1", "ch2"])).unwrap(),
        Command::PubSubNumSub {
            channels: vec!["ch1".into(), "ch2".into()]
        },
    );
}

#[test]
fn pubsub_numpat() {
    assert_eq!(
        Command::from_frame(cmd(&["PUBSUB", "NUMPAT"])).unwrap(),
        Command::PubSubNumPat,
    );
}

#[test]
fn pubsub_no_subcommand() {
    let err = Command::from_frame(cmd(&["PUBSUB"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn pubsub_unknown_subcommand() {
    let err = Command::from_frame(cmd(&["PUBSUB", "BOGUS"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

// --- INCRBY / DECRBY ---

#[test]
fn incrby_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["INCRBY", "counter", "5"])).unwrap(),
        Command::IncrBy {
            key: "counter".into(),
            delta: 5
        },
    );
}

#[test]
fn incrby_negative() {
    assert_eq!(
        Command::from_frame(cmd(&["INCRBY", "counter", "-3"])).unwrap(),
        Command::IncrBy {
            key: "counter".into(),
            delta: -3
        },
    );
}

#[test]
fn incrby_wrong_arity() {
    let err = Command::from_frame(cmd(&["INCRBY", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn incrby_not_integer() {
    let err = Command::from_frame(cmd(&["INCRBY", "key", "abc"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn decrby_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["DECRBY", "counter", "10"])).unwrap(),
        Command::DecrBy {
            key: "counter".into(),
            delta: 10
        },
    );
}

#[test]
fn decrby_wrong_arity() {
    let err = Command::from_frame(cmd(&["DECRBY"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- INCRBYFLOAT ---

#[test]
fn incrbyfloat_basic() {
    let cmd = Command::from_frame(cmd(&["INCRBYFLOAT", "key", "2.5"])).unwrap();
    match cmd {
        Command::IncrByFloat { key, delta } => {
            assert_eq!(key, "key");
            assert!((delta - 2.5).abs() < f64::EPSILON);
        }
        other => panic!("expected IncrByFloat, got {other:?}"),
    }
}

#[test]
fn incrbyfloat_negative() {
    let cmd = Command::from_frame(cmd(&["INCRBYFLOAT", "key", "-1.5"])).unwrap();
    match cmd {
        Command::IncrByFloat { key, delta } => {
            assert_eq!(key, "key");
            assert!((delta - (-1.5)).abs() < f64::EPSILON);
        }
        other => panic!("expected IncrByFloat, got {other:?}"),
    }
}

#[test]
fn incrbyfloat_wrong_arity() {
    let err = Command::from_frame(cmd(&["INCRBYFLOAT", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn incrbyfloat_not_a_float() {
    let err = Command::from_frame(cmd(&["INCRBYFLOAT", "key", "abc"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

// --- APPEND / STRLEN ---

#[test]
fn append_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["APPEND", "key", "value"])).unwrap(),
        Command::Append {
            key: "key".into(),
            value: Bytes::from("value")
        },
    );
}

#[test]
fn append_wrong_arity() {
    let err = Command::from_frame(cmd(&["APPEND", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn strlen_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["STRLEN", "key"])).unwrap(),
        Command::Strlen { key: "key".into() },
    );
}

#[test]
fn strlen_wrong_arity() {
    let err = Command::from_frame(cmd(&["STRLEN"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- KEYS ---

#[test]
fn keys_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["KEYS", "user:*"])).unwrap(),
        Command::Keys {
            pattern: "user:*".into()
        },
    );
}

#[test]
fn keys_wrong_arity() {
    let err = Command::from_frame(cmd(&["KEYS"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- RENAME ---

#[test]
fn rename_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["RENAME", "old", "new"])).unwrap(),
        Command::Rename {
            key: "old".into(),
            newkey: "new".into()
        },
    );
}

#[test]
fn rename_wrong_arity() {
    let err = Command::from_frame(cmd(&["RENAME", "only"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- AUTH ---

#[test]
fn auth_legacy() {
    assert_eq!(
        Command::from_frame(cmd(&["AUTH", "secret"])).unwrap(),
        Command::Auth {
            username: None,
            password: "secret".into()
        },
    );
}

#[test]
fn auth_with_username() {
    assert_eq!(
        Command::from_frame(cmd(&["AUTH", "default", "secret"])).unwrap(),
        Command::Auth {
            username: Some("default".into()),
            password: "secret".into()
        },
    );
}

#[test]
fn auth_no_args() {
    let err = Command::from_frame(cmd(&["AUTH"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn auth_too_many_args() {
    let err = Command::from_frame(cmd(&["AUTH", "a", "b", "c"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- QUIT ---

#[test]
fn quit_basic() {
    assert_eq!(Command::from_frame(cmd(&["QUIT"])).unwrap(), Command::Quit,);
}

#[test]
fn quit_wrong_arity() {
    let err = Command::from_frame(cmd(&["QUIT", "extra"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- PROTO.REGISTER ---

#[test]
fn proto_register_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.REGISTER", "myschema", "descriptor"])).unwrap(),
        Command::ProtoRegister {
            name: "myschema".into(),
            descriptor: Bytes::from("descriptor"),
        },
    );
}

#[test]
fn proto_register_case_insensitive() {
    assert!(Command::from_frame(cmd(&["proto.register", "s", "d"])).is_ok());
}

#[test]
fn proto_register_wrong_arity() {
    let err = Command::from_frame(cmd(&["PROTO.REGISTER", "only"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- PROTO.SET ---

#[test]
fn proto_set_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.SET", "key1", "my.Type", "data"])).unwrap(),
        Command::ProtoSet {
            key: "key1".into(),
            type_name: "my.Type".into(),
            data: Bytes::from("data"),
            expire: None,
            nx: false,
            xx: false,
        },
    );
}

#[test]
fn proto_set_with_ex() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.SET", "k", "t", "d", "EX", "60"])).unwrap(),
        Command::ProtoSet {
            key: "k".into(),
            type_name: "t".into(),
            data: Bytes::from("d"),
            expire: Some(SetExpire::Ex(60)),
            nx: false,
            xx: false,
        },
    );
}

#[test]
fn proto_set_with_px_and_nx() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.SET", "k", "t", "d", "PX", "5000", "NX"])).unwrap(),
        Command::ProtoSet {
            key: "k".into(),
            type_name: "t".into(),
            data: Bytes::from("d"),
            expire: Some(SetExpire::Px(5000)),
            nx: true,
            xx: false,
        },
    );
}

#[test]
fn proto_set_nx_xx_conflict() {
    let err = Command::from_frame(cmd(&["PROTO.SET", "k", "t", "d", "NX", "XX"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn proto_set_wrong_arity() {
    let err = Command::from_frame(cmd(&["PROTO.SET", "k", "t"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn proto_set_zero_expiry() {
    let err = Command::from_frame(cmd(&["PROTO.SET", "k", "t", "d", "EX", "0"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

// --- PROTO.GET ---

#[test]
fn proto_get_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.GET", "key1"])).unwrap(),
        Command::ProtoGet { key: "key1".into() },
    );
}

#[test]
fn proto_get_wrong_arity() {
    let err = Command::from_frame(cmd(&["PROTO.GET"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- PROTO.TYPE ---

#[test]
fn proto_type_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.TYPE", "key1"])).unwrap(),
        Command::ProtoType { key: "key1".into() },
    );
}

#[test]
fn proto_type_wrong_arity() {
    let err = Command::from_frame(cmd(&["PROTO.TYPE"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- PROTO.SCHEMAS ---

#[test]
fn proto_schemas_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.SCHEMAS"])).unwrap(),
        Command::ProtoSchemas,
    );
}

#[test]
fn proto_schemas_wrong_arity() {
    let err = Command::from_frame(cmd(&["PROTO.SCHEMAS", "extra"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- PROTO.DESCRIBE ---

#[test]
fn proto_describe_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.DESCRIBE", "myschema"])).unwrap(),
        Command::ProtoDescribe {
            name: "myschema".into()
        },
    );
}

#[test]
fn proto_describe_wrong_arity() {
    let err = Command::from_frame(cmd(&["PROTO.DESCRIBE"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- proto.getfield ---

#[test]
fn proto_getfield_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.GETFIELD", "user:1", "name"])).unwrap(),
        Command::ProtoGetField {
            key: "user:1".into(),
            field_path: "name".into(),
        },
    );
}

#[test]
fn proto_getfield_nested_path() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.GETFIELD", "key", "address.city"])).unwrap(),
        Command::ProtoGetField {
            key: "key".into(),
            field_path: "address.city".into(),
        },
    );
}

#[test]
fn proto_getfield_wrong_arity() {
    let err = Command::from_frame(cmd(&["PROTO.GETFIELD"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    let err = Command::from_frame(cmd(&["PROTO.GETFIELD", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    let err =
        Command::from_frame(cmd(&["PROTO.GETFIELD", "key", "field", "extra"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- proto.setfield ---

#[test]
fn proto_setfield_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.SETFIELD", "user:1", "name", "bob"])).unwrap(),
        Command::ProtoSetField {
            key: "user:1".into(),
            field_path: "name".into(),
            value: "bob".into(),
        },
    );
}

#[test]
fn proto_setfield_wrong_arity() {
    let err = Command::from_frame(cmd(&["PROTO.SETFIELD"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    let err = Command::from_frame(cmd(&["PROTO.SETFIELD", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    let err = Command::from_frame(cmd(&["PROTO.SETFIELD", "key", "field"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    let err = Command::from_frame(cmd(&["PROTO.SETFIELD", "key", "field", "value", "extra"]))
        .unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- proto.delfield ---

#[test]
fn proto_delfield_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["PROTO.DELFIELD", "user:1", "name"])).unwrap(),
        Command::ProtoDelField {
            key: "user:1".into(),
            field_path: "name".into(),
        },
    );
}

#[test]
fn proto_delfield_wrong_arity() {
    let err = Command::from_frame(cmd(&["PROTO.DELFIELD"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    let err = Command::from_frame(cmd(&["PROTO.DELFIELD", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    let err =
        Command::from_frame(cmd(&["PROTO.DELFIELD", "key", "field", "extra"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- vector commands ---

#[test]
fn vadd_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["VADD", "vecs", "elem1", "0.1", "0.2", "0.3"])).unwrap(),
        Command::VAdd {
            key: "vecs".into(),
            element: "elem1".into(),
            vector: vec![0.1, 0.2, 0.3],
            metric: 0,
            quantization: 0,
            connectivity: 16,
            expansion_add: 64,
        },
    );
}

#[test]
fn vadd_with_options() {
    assert_eq!(
        Command::from_frame(cmd(&[
            "VADD", "vecs", "elem1", "1.0", "2.0", "METRIC", "L2", "QUANT", "F16", "M", "32",
            "EF", "128"
        ]))
        .unwrap(),
        Command::VAdd {
            key: "vecs".into(),
            element: "elem1".into(),
            vector: vec![1.0, 2.0],
            metric: 1,
            quantization: 1,
            connectivity: 32,
            expansion_add: 128,
        },
    );
}

#[test]
fn vadd_wrong_arity() {
    // no args
    let err = Command::from_frame(cmd(&["VADD"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    // key only
    let err = Command::from_frame(cmd(&["VADD", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    // key + element but no vector — "elem" is not numeric so vector is empty
    let err = Command::from_frame(cmd(&["VADD", "key", "elem"])).unwrap_err();
    assert!(matches!(
        err,
        ProtocolError::WrongArity(_) | ProtocolError::InvalidCommandFrame(_)
    ));
}

#[test]
fn vadd_m_exceeds_max() {
    let err =
        Command::from_frame(cmd(&["VADD", "key", "elem", "1.0", "M", "9999"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn vadd_ef_exceeds_max() {
    let err =
        Command::from_frame(cmd(&["VADD", "key", "elem", "1.0", "EF", "9999"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn vadd_unknown_metric() {
    let err = Command::from_frame(cmd(&["VADD", "key", "elem", "1.0", "METRIC", "HAMMING"]))
        .unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn vadd_unknown_quantization() {
    let err =
        Command::from_frame(cmd(&["VADD", "key", "elem", "1.0", "QUANT", "F64"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

// --- vadd_batch ---

#[test]
fn vadd_batch_basic() {
    assert_eq!(
        Command::from_frame(cmd(&[
            "VADD_BATCH",
            "vecs",
            "DIM",
            "3",
            "a",
            "0.1",
            "0.2",
            "0.3",
            "b",
            "0.4",
            "0.5",
            "0.6"
        ]))
        .unwrap(),
        Command::VAddBatch {
            key: "vecs".into(),
            entries: vec![
                ("a".into(), vec![0.1, 0.2, 0.3]),
                ("b".into(), vec![0.4, 0.5, 0.6]),
            ],
            dim: 3,
            metric: 0,
            quantization: 0,
            connectivity: 16,
            expansion_add: 64,
        },
    );
}

#[test]
fn vadd_batch_with_options() {
    assert_eq!(
        Command::from_frame(cmd(&[
            "VADD_BATCH",
            "vecs",
            "DIM",
            "2",
            "a",
            "1.0",
            "2.0",
            "METRIC",
            "L2",
            "QUANT",
            "F16",
            "M",
            "32",
            "EF",
            "128"
        ]))
        .unwrap(),
        Command::VAddBatch {
            key: "vecs".into(),
            entries: vec![("a".into(), vec![1.0, 2.0])],
            dim: 2,
            metric: 1,
            quantization: 1,
            connectivity: 32,
            expansion_add: 128,
        },
    );
}

#[test]
fn vadd_batch_single_entry() {
    assert_eq!(
        Command::from_frame(cmd(&["VADD_BATCH", "vecs", "DIM", "1", "x", "1.5"])).unwrap(),
        Command::VAddBatch {
            key: "vecs".into(),
            entries: vec![("x".into(), vec![1.5])],
            dim: 1,
            metric: 0,
            quantization: 0,
            connectivity: 16,
            expansion_add: 64,
        },
    );
}

#[test]
fn vadd_batch_empty_entries() {
    // key + DIM + n but no entries — valid, returns empty batch
    assert_eq!(
        Command::from_frame(cmd(&["VADD_BATCH", "vecs", "DIM", "3"])).unwrap(),
        Command::VAddBatch {
            key: "vecs".into(),
            entries: vec![],
            dim: 3,
            metric: 0,
            quantization: 0,
            connectivity: 16,
            expansion_add: 64,
        },
    );
}

#[test]
fn vadd_batch_wrong_arity() {
    let err = Command::from_frame(cmd(&["VADD_BATCH"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    let err = Command::from_frame(cmd(&["VADD_BATCH", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    let err = Command::from_frame(cmd(&["VADD_BATCH", "key", "DIM"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn vadd_batch_missing_dim_keyword() {
    let err = Command::from_frame(cmd(&["VADD_BATCH", "key", "3", "a", "1.0"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn vadd_batch_dim_zero() {
    let err = Command::from_frame(cmd(&["VADD_BATCH", "key", "DIM", "0"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn vadd_batch_dim_exceeds_max() {
    let err = Command::from_frame(cmd(&["VADD_BATCH", "key", "DIM", "99999"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn vadd_batch_insufficient_floats() {
    // DIM=3 but only 2 floats for element "a"
    let err = Command::from_frame(cmd(&["VADD_BATCH", "key", "DIM", "3", "a", "1.0", "2.0"]))
        .unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn vadd_batch_m_exceeds_max() {
    let err = Command::from_frame(cmd(&[
        "VADD_BATCH",
        "key",
        "DIM",
        "2",
        "a",
        "1.0",
        "2.0",
        "M",
        "9999",
    ]))
    .unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn vadd_batch_ef_exceeds_max() {
    let err = Command::from_frame(cmd(&[
        "VADD_BATCH",
        "key",
        "DIM",
        "2",
        "a",
        "1.0",
        "2.0",
        "EF",
        "9999",
    ]))
    .unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn vsim_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["VSIM", "vecs", "0.1", "0.2", "0.3", "COUNT", "5"])).unwrap(),
        Command::VSim {
            key: "vecs".into(),
            query: vec![0.1, 0.2, 0.3],
            count: 5,
            ef_search: 0,
            with_scores: false,
        },
    );
}

#[test]
fn vsim_with_ef_and_scores() {
    assert_eq!(
        Command::from_frame(cmd(&[
            "VSIM",
            "vecs",
            "1.0",
            "2.0",
            "COUNT",
            "10",
            "EF",
            "128",
            "WITHSCORES"
        ]))
        .unwrap(),
        Command::VSim {
            key: "vecs".into(),
            query: vec![1.0, 2.0],
            count: 10,
            ef_search: 128,
            with_scores: true,
        },
    );
}

#[test]
fn vsim_missing_count() {
    // all args are numeric so they're consumed as query — COUNT is never found
    let err = Command::from_frame(cmd(&["VSIM", "key", "1.0", "2.0"])).unwrap_err();
    assert!(matches!(
        err,
        ProtocolError::InvalidCommandFrame(_) | ProtocolError::WrongArity(_)
    ));
}

#[test]
fn vsim_wrong_arity() {
    let err = Command::from_frame(cmd(&["VSIM"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn vsim_count_exceeds_max() {
    let err = Command::from_frame(cmd(&["VSIM", "key", "1.0", "COUNT", "99999"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn vsim_ef_exceeds_max() {
    let err = Command::from_frame(cmd(&["VSIM", "key", "1.0", "COUNT", "5", "EF", "9999"]))
        .unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn vrem_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["VREM", "key", "elem"])).unwrap(),
        Command::VRem {
            key: "key".into(),
            element: "elem".into(),
        },
    );
}

#[test]
fn vrem_wrong_arity() {
    let err = Command::from_frame(cmd(&["VREM"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));

    let err = Command::from_frame(cmd(&["VREM", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn vget_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["VGET", "key", "elem"])).unwrap(),
        Command::VGet {
            key: "key".into(),
            element: "elem".into(),
        },
    );
}

#[test]
fn vget_wrong_arity() {
    let err = Command::from_frame(cmd(&["VGET"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn vcard_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["VCARD", "key"])).unwrap(),
        Command::VCard { key: "key".into() },
    );
}

#[test]
fn vcard_wrong_arity() {
    let err = Command::from_frame(cmd(&["VCARD"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn vdim_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["VDIM", "key"])).unwrap(),
        Command::VDim { key: "key".into() },
    );
}

#[test]
fn vdim_wrong_arity() {
    let err = Command::from_frame(cmd(&["VDIM"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn vinfo_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["VINFO", "key"])).unwrap(),
        Command::VInfo { key: "key".into() },
    );
}

#[test]
fn vinfo_wrong_arity() {
    let err = Command::from_frame(cmd(&["VINFO"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn restore_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["RESTORE", "mykey", "5000", "serialized"])).unwrap(),
        Command::Restore {
            key: "mykey".into(),
            ttl_ms: 5000,
            data: Bytes::from("serialized"),
            replace: false,
        },
    );
}

#[test]
fn restore_with_replace() {
    assert_eq!(
        Command::from_frame(cmd(&["RESTORE", "mykey", "0", "data", "REPLACE"])).unwrap(),
        Command::Restore {
            key: "mykey".into(),
            ttl_ms: 0,
            data: Bytes::from("data"),
            replace: true,
        },
    );
}

#[test]
fn restore_wrong_arity() {
    let err = Command::from_frame(cmd(&["RESTORE", "key"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// -- is_write / primary_key --

#[test]
fn is_write_returns_true_for_mutations() {
    assert!(Command::Set {
        key: "k".into(),
        value: bytes::Bytes::new(),
        expire: None,
        nx: false,
        xx: false,
    }
    .is_write());
    assert!(Command::Del {
        keys: vec!["k".into()]
    }
    .is_write());
    assert!(Command::Incr { key: "k".into() }.is_write());
    assert!(Command::HSet {
        key: "k".into(),
        fields: vec![],
    }
    .is_write());
    assert!(Command::LPush {
        key: "k".into(),
        values: vec![]
    }
    .is_write());
    assert!(Command::ZAdd {
        key: "k".into(),
        flags: crate::command::ZAddFlags::default(),
        members: vec![],
    }
    .is_write());
    assert!(Command::SAdd {
        key: "k".into(),
        members: vec![]
    }
    .is_write());
    assert!(Command::FlushDb { async_mode: false }.is_write());
}

#[test]
fn is_write_returns_false_for_reads() {
    assert!(!Command::Get { key: "k".into() }.is_write());
    assert!(!Command::HGet {
        key: "k".into(),
        field: "f".into()
    }
    .is_write());
    assert!(!Command::Ping(None).is_write());
    assert!(!Command::ClusterInfo.is_write());
    assert!(!Command::DbSize.is_write());
}

#[test]
fn primary_key_returns_first_key() {
    assert_eq!(
        Command::Get {
            key: "hello".into()
        }
        .primary_key(),
        Some("hello")
    );
    assert_eq!(
        Command::Del {
            keys: vec!["a".into(), "b".into()]
        }
        .primary_key(),
        Some("a")
    );
    assert_eq!(Command::Ping(None).primary_key(), None);
    assert_eq!(Command::DbSize.primary_key(), None);
}

// --- client ---

#[test]
fn parse_client_id() {
    assert_eq!(
        Command::from_frame(cmd(&["CLIENT", "ID"])).unwrap(),
        Command::ClientId,
    );
}

#[test]
fn parse_client_id_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["client", "id"])).unwrap(),
        Command::ClientId,
    );
}

#[test]
fn parse_client_getname() {
    assert_eq!(
        Command::from_frame(cmd(&["CLIENT", "GETNAME"])).unwrap(),
        Command::ClientGetName,
    );
}

#[test]
fn parse_client_setname() {
    assert_eq!(
        Command::from_frame(cmd(&["CLIENT", "SETNAME", "myapp"])).unwrap(),
        Command::ClientSetName {
            name: "myapp".into()
        },
    );
}

#[test]
fn parse_client_setname_missing_name() {
    let err = Command::from_frame(cmd(&["CLIENT", "SETNAME"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn parse_client_list() {
    assert_eq!(
        Command::from_frame(cmd(&["CLIENT", "LIST"])).unwrap(),
        Command::ClientList,
    );
}

#[test]
fn parse_client_unknown_subcommand() {
    let err = Command::from_frame(cmd(&["CLIENT", "KILL"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn parse_client_no_subcommand() {
    let err = Command::from_frame(cmd(&["CLIENT"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- watch / unwatch ---

#[test]
fn parse_watch_single_key() {
    assert_eq!(
        Command::from_frame(cmd(&["WATCH", "mykey"])).unwrap(),
        Command::Watch {
            keys: vec!["mykey".into()]
        },
    );
}

#[test]
fn parse_watch_multiple_keys() {
    assert_eq!(
        Command::from_frame(cmd(&["WATCH", "a", "b", "c"])).unwrap(),
        Command::Watch {
            keys: vec!["a".into(), "b".into(), "c".into()]
        },
    );
}

#[test]
fn parse_watch_no_keys_error() {
    let err = Command::from_frame(cmd(&["WATCH"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn parse_unwatch() {
    assert_eq!(
        Command::from_frame(cmd(&["UNWATCH"])).unwrap(),
        Command::Unwatch,
    );
}

// --- time / lastsave / role ---

#[test]
fn parse_time() {
    assert_eq!(Command::from_frame(cmd(&["TIME"])).unwrap(), Command::Time);
}

#[test]
fn parse_lastsave() {
    assert_eq!(
        Command::from_frame(cmd(&["LASTSAVE"])).unwrap(),
        Command::LastSave,
    );
}

#[test]
fn parse_role() {
    assert_eq!(Command::from_frame(cmd(&["ROLE"])).unwrap(), Command::Role);
}

// --- object ---

#[test]
fn parse_object_encoding() {
    assert_eq!(
        Command::from_frame(cmd(&["OBJECT", "ENCODING", "mykey"])).unwrap(),
        Command::ObjectEncoding {
            key: "mykey".into()
        },
    );
}

#[test]
fn parse_object_refcount() {
    assert_eq!(
        Command::from_frame(cmd(&["OBJECT", "REFCOUNT", "mykey"])).unwrap(),
        Command::ObjectRefcount {
            key: "mykey".into()
        },
    );
}

#[test]
fn parse_object_unknown_subcommand() {
    let err = Command::from_frame(cmd(&["OBJECT", "FREQ", "mykey"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn parse_object_no_subcommand() {
    let err = Command::from_frame(cmd(&["OBJECT"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- copy ---

#[test]
fn parse_copy_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["COPY", "src", "dst"])).unwrap(),
        Command::Copy {
            source: "src".into(),
            destination: "dst".into(),
            replace: false,
        },
    );
}

#[test]
fn parse_copy_replace() {
    assert_eq!(
        Command::from_frame(cmd(&["COPY", "src", "dst", "REPLACE"])).unwrap(),
        Command::Copy {
            source: "src".into(),
            destination: "dst".into(),
            replace: true,
        },
    );
}

#[test]
fn parse_copy_db_ignored() {
    assert_eq!(
        Command::from_frame(cmd(&["COPY", "src", "dst", "DB", "0", "REPLACE"])).unwrap(),
        Command::Copy {
            source: "src".into(),
            destination: "dst".into(),
            replace: true,
        },
    );
}

#[test]
fn parse_copy_wrong_arity() {
    let err = Command::from_frame(cmd(&["COPY", "src"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- ACL ---

#[test]
fn parse_acl_whoami() {
    assert_eq!(
        Command::from_frame(cmd(&["ACL", "WHOAMI"])).unwrap(),
        Command::AclWhoAmI,
    );
}

#[test]
fn parse_acl_list() {
    assert_eq!(
        Command::from_frame(cmd(&["ACL", "LIST"])).unwrap(),
        Command::AclList,
    );
}

#[test]
fn parse_acl_users() {
    assert_eq!(
        Command::from_frame(cmd(&["ACL", "USERS"])).unwrap(),
        Command::AclUsers,
    );
}

#[test]
fn parse_acl_getuser() {
    assert_eq!(
        Command::from_frame(cmd(&["ACL", "GETUSER", "alice"])).unwrap(),
        Command::AclGetUser {
            username: "alice".into(),
        },
    );
}

#[test]
fn parse_acl_deluser() {
    assert_eq!(
        Command::from_frame(cmd(&["ACL", "DELUSER", "alice", "bob"])).unwrap(),
        Command::AclDelUser {
            usernames: vec!["alice".into(), "bob".into()],
        },
    );
}

#[test]
fn parse_acl_setuser_no_rules() {
    assert_eq!(
        Command::from_frame(cmd(&["ACL", "SETUSER", "alice"])).unwrap(),
        Command::AclSetUser {
            username: "alice".into(),
            rules: vec![],
        },
    );
}

#[test]
fn parse_acl_setuser_with_rules() {
    assert_eq!(
        Command::from_frame(cmd(&["ACL", "SETUSER", "alice", "on", ">pass", "+@read"]))
            .unwrap(),
        Command::AclSetUser {
            username: "alice".into(),
            rules: vec!["on".into(), ">pass".into(), "+@read".into()],
        },
    );
}

#[test]
fn parse_acl_cat_no_args() {
    assert_eq!(
        Command::from_frame(cmd(&["ACL", "CAT"])).unwrap(),
        Command::AclCat { category: None },
    );
}

#[test]
fn parse_acl_cat_with_category() {
    assert_eq!(
        Command::from_frame(cmd(&["ACL", "CAT", "read"])).unwrap(),
        Command::AclCat {
            category: Some("read".into()),
        },
    );
}

#[test]
fn parse_acl_unknown_subcommand() {
    let err = Command::from_frame(cmd(&["ACL", "BOGUS"])).unwrap_err();
    assert!(matches!(err, ProtocolError::InvalidCommandFrame(_)));
}

#[test]
fn parse_acl_no_subcommand() {
    let err = Command::from_frame(cmd(&["ACL"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn parse_acl_case_insensitive() {
    assert_eq!(
        Command::from_frame(cmd(&["acl", "whoami"])).unwrap(),
        Command::AclWhoAmI,
    );
}

#[test]
fn acl_categories_returns_nonzero_for_known_commands() {
    let cmd = Command::Get { key: "k".into() };
    assert_ne!(cmd.acl_categories(), 0);

    let cmd = Command::Set {
        key: "k".into(),
        value: Bytes::from_static(b"v"),
        expire: None,
        nx: false,
        xx: false,
    };
    assert_ne!(cmd.acl_categories(), 0);
}

#[test]
fn acl_categories_unknown_returns_zero() {
    let cmd = Command::Unknown("bogus".into());
    assert_eq!(cmd.acl_categories(), 0);
}

// --- randomkey ---

#[test]
fn randomkey_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["RANDOMKEY"])).unwrap(),
        Command::RandomKey,
    );
}

#[test]
fn randomkey_extra_args() {
    let err = Command::from_frame(cmd(&["RANDOMKEY", "extra"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- touch ---

#[test]
fn touch_single() {
    assert_eq!(
        Command::from_frame(cmd(&["TOUCH", "key"])).unwrap(),
        Command::Touch {
            keys: vec!["key".into()]
        },
    );
}

#[test]
fn touch_multiple() {
    assert_eq!(
        Command::from_frame(cmd(&["TOUCH", "a", "b", "c"])).unwrap(),
        Command::Touch {
            keys: vec!["a".into(), "b".into(), "c".into()]
        },
    );
}

#[test]
fn touch_no_args() {
    let err = Command::from_frame(cmd(&["TOUCH"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

// --- sort ---

#[test]
fn sort_basic() {
    assert_eq!(
        Command::from_frame(cmd(&["SORT", "mylist"])).unwrap(),
        Command::Sort {
            key: "mylist".into(),
            desc: false,
            alpha: false,
            limit: None,
            store: None,
        },
    );
}

#[test]
fn sort_desc_alpha() {
    assert_eq!(
        Command::from_frame(cmd(&["SORT", "mylist", "DESC", "ALPHA"])).unwrap(),
        Command::Sort {
            key: "mylist".into(),
            desc: true,
            alpha: true,
            limit: None,
            store: None,
        },
    );
}

#[test]
fn sort_limit() {
    assert_eq!(
        Command::from_frame(cmd(&["SORT", "mylist", "LIMIT", "0", "10"])).unwrap(),
        Command::Sort {
            key: "mylist".into(),
            desc: false,
            alpha: false,
            limit: Some((0, 10)),
            store: None,
        },
    );
}

#[test]
fn sort_store() {
    assert_eq!(
        Command::from_frame(cmd(&["SORT", "mylist", "ALPHA", "STORE", "dest"])).unwrap(),
        Command::Sort {
            key: "mylist".into(),
            desc: false,
            alpha: true,
            limit: None,
            store: Some("dest".into()),
        },
    );
}

#[test]
fn sort_no_args() {
    let err = Command::from_frame(cmd(&["SORT"])).unwrap_err();
    assert!(matches!(err, ProtocolError::WrongArity(_)));
}

#[test]
fn sort_is_write_with_store() {
    let cmd = Command::Sort {
        key: "k".into(),
        desc: false,
        alpha: false,
        limit: None,
        store: Some("dest".into()),
    };
    assert!(cmd.is_write());
}

#[test]
fn sort_is_not_write_without_store() {
    let cmd = Command::Sort {
        key: "k".into(),
        desc: false,
        alpha: false,
        limit: None,
        store: None,
    };
    assert!(!cmd.is_write());
}
