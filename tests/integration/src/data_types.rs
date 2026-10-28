//! Integration tests for lists, hashes, sets, and sorted sets.

use ember_protocol::Frame;

use crate::helpers::{ServerOptions, TestServer};

// --- lists ---

#[tokio::test]
async fn list_push_pop() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["LPUSH", "list", "a"]).await, 1);
    assert_eq!(c.get_int(&["RPUSH", "list", "b"]).await, 2);
    assert_eq!(c.get_int(&["LPUSH", "list", "c"]).await, 3);

    // order is: c, a, b
    assert_eq!(c.get_bulk(&["LPOP", "list"]).await, Some("c".into()));
    assert_eq!(c.get_bulk(&["RPOP", "list"]).await, Some("b".into()));
    assert_eq!(c.get_bulk(&["LPOP", "list"]).await, Some("a".into()));

    // empty list
    let resp = c.cmd(&["LPOP", "list"]).await;
    assert!(matches!(resp, Frame::Null));
}

#[tokio::test]
async fn list_lrange() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["RPUSH", "list", "a", "b", "c", "d"]).await;

    let resp = c.cmd(&["LRANGE", "list", "0", "-1"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 4);
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"a"[..]));
            assert!(matches!(&frames[3], Frame::Bulk(b) if b == &b"d"[..]));
        }
        other => panic!("expected Array, got {other:?}"),
    }

    // partial range
    let resp = c.cmd(&["LRANGE", "list", "1", "2"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 2);
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"b"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"c"[..]));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn list_llen() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["LLEN", "list"]).await, 0);
    c.cmd(&["RPUSH", "list", "a", "b", "c"]).await;
    assert_eq!(c.get_int(&["LLEN", "list"]).await, 3);
}

// --- hashes ---

#[tokio::test]
async fn hash_set_get() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.get_int(&["HSET", "h", "name", "ember", "version", "0.4"])
        .await;
    assert_eq!(
        c.get_bulk(&["HGET", "h", "name"]).await,
        Some("ember".into())
    );
    assert_eq!(
        c.get_bulk(&["HGET", "h", "version"]).await,
        Some("0.4".into())
    );

    // missing field
    let resp = c.cmd(&["HGET", "h", "missing"]).await;
    assert!(matches!(resp, Frame::Null));
}

#[tokio::test]
async fn hash_getall() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.get_int(&["HSET", "h", "a", "1", "b", "2"]).await;
    let resp = c.cmd(&["HGETALL", "h"]).await;
    match resp {
        Frame::Array(frames) => {
            // 2 fields × 2 (field + value) = 4 elements
            assert_eq!(frames.len(), 4);
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn hash_del_exists_len() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.get_int(&["HSET", "h", "a", "1", "b", "2", "c", "3"])
        .await;
    assert_eq!(c.get_int(&["HLEN", "h"]).await, 3);
    assert_eq!(c.get_int(&["HEXISTS", "h", "a"]).await, 1);

    assert_eq!(c.get_int(&["HDEL", "h", "a"]).await, 1);
    assert_eq!(c.get_int(&["HEXISTS", "h", "a"]).await, 0);
    assert_eq!(c.get_int(&["HLEN", "h"]).await, 2);
}

#[tokio::test]
async fn hash_incrby() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.get_int(&["HSET", "h", "counter", "10"]).await;
    assert_eq!(c.get_int(&["HINCRBY", "h", "counter", "5"]).await, 15);
    assert_eq!(c.get_int(&["HINCRBY", "h", "counter", "-3"]).await, 12);

    // new field
    assert_eq!(c.get_int(&["HINCRBY", "h", "new", "1"]).await, 1);
}

#[tokio::test]
async fn hash_keys_vals_hmget() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.get_int(&["HSET", "h", "x", "10", "y", "20"]).await;

    let resp = c.cmd(&["HKEYS", "h"]).await;
    match resp {
        Frame::Array(keys) => assert_eq!(keys.len(), 2),
        other => panic!("expected Array, got {other:?}"),
    }

    let resp = c.cmd(&["HVALS", "h"]).await;
    match resp {
        Frame::Array(vals) => assert_eq!(vals.len(), 2),
        other => panic!("expected Array, got {other:?}"),
    }

    let resp = c.cmd(&["HMGET", "h", "x", "y", "z"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 3);
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"10"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"20"[..]));
            assert!(matches!(&frames[2], Frame::Null));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

// --- sets ---

#[tokio::test]
async fn set_add_rem_members() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["SADD", "s", "a", "b", "c"]).await, 3);
    // duplicate
    assert_eq!(c.get_int(&["SADD", "s", "a"]).await, 0);
    assert_eq!(c.get_int(&["SCARD", "s"]).await, 3);

    assert_eq!(c.get_int(&["SISMEMBER", "s", "a"]).await, 1);
    assert_eq!(c.get_int(&["SISMEMBER", "s", "z"]).await, 0);

    assert_eq!(c.get_int(&["SREM", "s", "a", "z"]).await, 1);
    assert_eq!(c.get_int(&["SCARD", "s"]).await, 2);
}

#[tokio::test]
async fn set_smembers() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.get_int(&["SADD", "s", "x", "y"]).await;
    let resp = c.cmd(&["SMEMBERS", "s"]).await;
    match resp {
        Frame::Array(members) => assert_eq!(members.len(), 2),
        other => panic!("expected Array, got {other:?}"),
    }
}

// --- sorted sets ---

#[tokio::test]
async fn zset_add_score_rank() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(
        c.get_int(&["ZADD", "z", "1.0", "a", "2.0", "b", "3.0", "c"])
            .await,
        3
    );
    assert_eq!(c.get_int(&["ZCARD", "z"]).await, 3);

    let score = c.get_bulk(&["ZSCORE", "z", "b"]).await;
    assert_eq!(score, Some("2".into()));

    let rank = c.get_int(&["ZRANK", "z", "a"]).await;
    assert_eq!(rank, 0); // lowest score = rank 0
}

#[tokio::test]
async fn zset_range() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.get_int(&["ZADD", "z", "1", "a", "2", "b", "3", "c"])
        .await;

    let resp = c.cmd(&["ZRANGE", "z", "0", "-1"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 3);
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"a"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"b"[..]));
            assert!(matches!(&frames[2], Frame::Bulk(b) if b == &b"c"[..]));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn zset_rem() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.get_int(&["ZADD", "z", "1", "a", "2", "b"]).await;
    assert_eq!(c.get_int(&["ZREM", "z", "a", "missing"]).await, 1);
    assert_eq!(c.get_int(&["ZCARD", "z"]).await, 1);
}

// --- EXPIRETIME / PEXPIRETIME ---

#[tokio::test]
async fn expiretime_returns_absolute_epoch() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    c.cmd(&["SET", "k", "v"]).await;
    c.cmd(&["EXPIRE", "k", "100"]).await;

    let ts = c.get_int(&["EXPIRETIME", "k"]).await;
    // should be ≈ now + 100 seconds
    assert!(ts >= before + 99 && ts <= before + 101);
}

#[tokio::test]
async fn expiretime_no_expiry_returns_minus_one() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["SET", "k", "v"]).await;
    assert_eq!(c.get_int(&["EXPIRETIME", "k"]).await, -1);
}

#[tokio::test]
async fn expiretime_missing_key_returns_minus_two() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["EXPIRETIME", "missing"]).await, -2);
}

#[tokio::test]
async fn pexpiretime_precision() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let before_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    c.cmd(&["SET", "k", "v"]).await;
    // PEXPIRE with 10_000 ms (10 seconds)
    c.cmd(&["PEXPIRE", "k", "10000"]).await;

    let ts_ms = c.get_int(&["PEXPIRETIME", "k"]).await;
    assert!(ts_ms >= before_ms + 9_000 && ts_ms <= before_ms + 11_000);
}

#[tokio::test]
async fn pexpiretime_no_expiry_returns_minus_one() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["SET", "k", "v"]).await;
    assert_eq!(c.get_int(&["PEXPIRETIME", "k"]).await, -1);
}

#[tokio::test]
async fn pexpiretime_missing_key_returns_minus_two() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["PEXPIRETIME", "missing"]).await, -2);
}

// --- SMOVE ---

#[tokio::test]
async fn smove_basic() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["SADD", "src", "a", "b", "c"]).await;

    // move "a" from src to dst
    assert_eq!(c.get_int(&["SMOVE", "src", "dst", "a"]).await, 1);

    assert_eq!(c.get_int(&["SCARD", "src"]).await, 2);
    assert_eq!(c.get_int(&["SISMEMBER", "src", "a"]).await, 0);
    assert_eq!(c.get_int(&["SCARD", "dst"]).await, 1);
    assert_eq!(c.get_int(&["SISMEMBER", "dst", "a"]).await, 1);
}

#[tokio::test]
async fn smove_missing_member_returns_zero() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["SADD", "src", "x"]).await;

    // "y" is not in src
    assert_eq!(c.get_int(&["SMOVE", "src", "dst", "y"]).await, 0);

    // src unchanged
    assert_eq!(c.get_int(&["SCARD", "src"]).await, 1);
    // dst was not created
    assert_eq!(c.get_int(&["EXISTS", "dst"]).await, 0);
}

#[tokio::test]
async fn smove_missing_source_returns_zero() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["SMOVE", "nosrc", "dst", "m"]).await, 0);
}

// --- SINTERCARD ---

#[tokio::test]
async fn sintercard_basic() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["SADD", "s1", "a", "b", "c"]).await;
    c.cmd(&["SADD", "s2", "b", "c", "d"]).await;
    c.cmd(&["SADD", "s3", "c", "d", "e"]).await;

    // intersection of s1, s2, s3 is {"c"} → cardinality 1
    assert_eq!(c.get_int(&["SINTERCARD", "3", "s1", "s2", "s3"]).await, 1);

    // intersection of s1 and s2 is {"b", "c"} → cardinality 2
    assert_eq!(c.get_int(&["SINTERCARD", "2", "s1", "s2"]).await, 2);
}

#[tokio::test]
async fn sintercard_with_limit() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["SADD", "a", "1", "2", "3", "4", "5"]).await;
    c.cmd(&["SADD", "b", "1", "2", "3", "4", "5"]).await;

    // full intersection is 5, limit to 3
    assert_eq!(
        c.get_int(&["SINTERCARD", "2", "a", "b", "LIMIT", "3"])
            .await,
        3
    );
    // limit 0 means no cap
    assert_eq!(
        c.get_int(&["SINTERCARD", "2", "a", "b", "LIMIT", "0"])
            .await,
        5
    );
}

#[tokio::test]
async fn sintercard_missing_key_returns_zero() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["SADD", "s", "a"]).await;

    assert_eq!(c.get_int(&["SINTERCARD", "2", "s", "missing"]).await, 0);
}

// --- LMPOP ---

#[tokio::test]
async fn lmpop_returns_first_nonempty_list() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // first key is empty, second has data
    c.cmd(&["RPUSH", "b", "x", "y", "z"]).await;

    let resp = c.cmd(&["LMPOP", "2", "a", "b", "LEFT"]).await;
    match resp {
        Frame::Array(outer) => {
            assert_eq!(outer.len(), 2);
            // first element is the key name
            assert!(matches!(&outer[0], Frame::Bulk(k) if k == &b"b"[..]));
            // second is an array of popped elements (default count=1)
            match &outer[1] {
                Frame::Array(items) => {
                    assert_eq!(items.len(), 1);
                    assert!(matches!(&items[0], Frame::Bulk(v) if v == &b"x"[..]));
                }
                other => panic!("expected inner Array, got {other:?}"),
            }
        }
        other => panic!("expected outer Array, got {other:?}"),
    }
}

#[tokio::test]
async fn lmpop_count_pops_multiple() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["RPUSH", "lst", "1", "2", "3", "4"]).await;

    let resp = c.cmd(&["LMPOP", "1", "lst", "RIGHT", "COUNT", "3"]).await;
    match resp {
        Frame::Array(outer) => {
            assert_eq!(outer.len(), 2);
            match &outer[1] {
                Frame::Array(items) => {
                    // pops 3 from the right: 4, 3, 2
                    assert_eq!(items.len(), 3);
                    assert!(matches!(&items[0], Frame::Bulk(v) if v == &b"4"[..]));
                    assert!(matches!(&items[1], Frame::Bulk(v) if v == &b"3"[..]));
                    assert!(matches!(&items[2], Frame::Bulk(v) if v == &b"2"[..]));
                }
                other => panic!("expected inner Array, got {other:?}"),
            }
        }
        other => panic!("expected outer Array, got {other:?}"),
    }
}

#[tokio::test]
async fn lmpop_all_empty_returns_nil() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let resp = c.cmd(&["LMPOP", "2", "empty1", "empty2", "LEFT"]).await;
    assert!(matches!(resp, Frame::Null));
}

// --- ZMPOP ---

#[tokio::test]
async fn zmpop_min_pops_lowest_score() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // first key missing, second has data
    c.cmd(&["ZADD", "scores", "1", "alice", "2", "bob", "3", "carol"])
        .await;

    let resp = c.cmd(&["ZMPOP", "2", "missing", "scores", "MIN"]).await;
    match resp {
        Frame::Array(outer) => {
            assert_eq!(outer.len(), 2);
            assert!(matches!(&outer[0], Frame::Bulk(k) if k == &b"scores"[..]));
            match &outer[1] {
                Frame::Array(pairs) => {
                    // one (member, score) pair flattened
                    assert_eq!(pairs.len(), 2);
                    assert!(matches!(&pairs[0], Frame::Bulk(m) if m == &b"alice"[..]));
                    assert!(matches!(&pairs[1], Frame::Bulk(s) if s == &b"1"[..]));
                }
                other => panic!("expected pairs Array, got {other:?}"),
            }
        }
        other => panic!("expected outer Array, got {other:?}"),
    }
}

#[tokio::test]
async fn zmpop_count_pops_multiple() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["ZADD", "z", "10", "a", "20", "b", "30", "c"]).await;

    let resp = c.cmd(&["ZMPOP", "1", "z", "MAX", "COUNT", "2"]).await;
    match resp {
        Frame::Array(outer) => {
            match &outer[1] {
                Frame::Array(pairs) => {
                    // 2 members popped from MAX: c(30) then b(20), flattened
                    assert_eq!(pairs.len(), 4);
                    assert!(matches!(&pairs[0], Frame::Bulk(m) if m == &b"c"[..]));
                    assert!(matches!(&pairs[2], Frame::Bulk(m) if m == &b"b"[..]));
                }
                other => panic!("expected pairs Array, got {other:?}"),
            }
        }
        other => panic!("expected outer Array, got {other:?}"),
    }
}

#[tokio::test]
async fn zmpop_all_empty_returns_nil() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let resp = c.cmd(&["ZMPOP", "2", "nope1", "nope2", "MIN"]).await;
    assert!(matches!(resp, Frame::Null));
}

// --- lpop / rpop with count ---

#[tokio::test]
async fn lpop_no_count_returns_bulk_string() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["RPUSH", "list", "a", "b", "c"]).await;

    // no count — returns a bulk string, not an array
    let val = c.get_bulk(&["LPOP", "list"]).await;
    assert_eq!(val, Some("a".into()));
}

#[tokio::test]
async fn lpop_count_returns_array() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["RPUSH", "list", "a", "b", "c", "d"]).await;

    let resp = c.cmd(&["LPOP", "list", "2"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 2);
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"a"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"b"[..]));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn rpop_count_returns_array() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["RPUSH", "list", "a", "b", "c", "d"]).await;

    let resp = c.cmd(&["RPOP", "list", "2"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 2);
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"d"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"c"[..]));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn lpop_count_exceeding_list_returns_all() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["RPUSH", "list", "x", "y"]).await;

    let resp = c.cmd(&["LPOP", "list", "10"]).await;
    match resp {
        Frame::Array(frames) => assert_eq!(frames.len(), 2),
        other => panic!("expected Array, got {other:?}"),
    }
}

// --- HRANDFIELD ---

#[tokio::test]
async fn hrandfield_no_count_returns_single_field() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["HSET", "h", "a", "1", "b", "2", "c", "3"]).await;

    let resp = c.cmd(&["HRANDFIELD", "h"]).await;
    match resp {
        Frame::Bulk(b) => {
            let field = std::str::from_utf8(&b).unwrap();
            assert!(["a", "b", "c"].contains(&field));
        }
        other => panic!("expected Bulk, got {other:?}"),
    }
}

#[tokio::test]
async fn hrandfield_positive_count_distinct() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["HSET", "h", "a", "1", "b", "2", "c", "3"]).await;

    let resp = c.cmd(&["HRANDFIELD", "h", "2"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 2);
            // all should be valid fields
            for f in &frames {
                if let Frame::Bulk(b) = f {
                    let field = std::str::from_utf8(b).unwrap();
                    assert!(["a", "b", "c"].contains(&field));
                } else {
                    panic!("expected Bulk in array, got {f:?}");
                }
            }
            // distinct
            let fields: std::collections::HashSet<_> = frames
                .iter()
                .map(|f| match f {
                    Frame::Bulk(b) => std::str::from_utf8(b).unwrap().to_owned(),
                    _ => unreachable!(),
                })
                .collect();
            assert_eq!(fields.len(), 2);
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn hrandfield_negative_count_allows_duplicates() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["HSET", "h", "only", "val"]).await;

    let resp = c.cmd(&["HRANDFIELD", "h", "-5"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 5);
            for f in &frames {
                if let Frame::Bulk(b) = f {
                    assert_eq!(std::str::from_utf8(b).unwrap(), "only");
                } else {
                    panic!("expected Bulk, got {f:?}");
                }
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn hrandfield_withvalues() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["HSET", "h", "field", "value"]).await;

    let resp = c.cmd(&["HRANDFIELD", "h", "1", "WITHVALUES"]).await;
    match resp {
        Frame::Array(frames) => {
            // interleaved: [field, value]
            assert_eq!(frames.len(), 2);
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"field"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"value"[..]));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn hrandfield_missing_key_empty() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let resp = c.cmd(&["HRANDFIELD", "nosuchkey"]).await;
    assert!(matches!(resp, Frame::Null));

    let resp2 = c.cmd(&["HRANDFIELD", "nosuchkey", "3"]).await;
    match resp2 {
        Frame::Array(frames) => assert!(frames.is_empty()),
        other => panic!("expected empty Array, got {other:?}"),
    }
}

// --- ZRANDMEMBER ---

#[tokio::test]
async fn zrandmember_no_count_returns_single_member() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]).await;

    let resp = c.cmd(&["ZRANDMEMBER", "z"]).await;
    match resp {
        Frame::Bulk(b) => {
            let member = std::str::from_utf8(&b).unwrap();
            assert!(["a", "b", "c"].contains(&member));
        }
        other => panic!("expected Bulk, got {other:?}"),
    }
}

#[tokio::test]
async fn zrandmember_positive_count_distinct() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]).await;

    let resp = c.cmd(&["ZRANDMEMBER", "z", "2"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 2);
            let members: std::collections::HashSet<_> = frames
                .iter()
                .map(|f| match f {
                    Frame::Bulk(b) => std::str::from_utf8(b).unwrap().to_owned(),
                    _ => panic!("expected Bulk"),
                })
                .collect();
            assert_eq!(members.len(), 2);
            assert!(members
                .iter()
                .all(|m| ["a", "b", "c"].contains(&m.as_str())));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn zrandmember_withscores() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["ZADD", "z", "42", "member"]).await;

    let resp = c.cmd(&["ZRANDMEMBER", "z", "1", "WITHSCORES"]).await;
    match resp {
        Frame::Array(frames) => {
            // interleaved: [member, score]
            assert_eq!(frames.len(), 2);
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"member"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"42"[..]));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn zrandmember_missing_key_empty() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let resp = c.cmd(&["ZRANDMEMBER", "nosuchkey"]).await;
    assert!(matches!(resp, Frame::Null));

    let resp2 = c.cmd(&["ZRANDMEMBER", "nosuchkey", "3"]).await;
    match resp2 {
        Frame::Array(frames) => assert!(frames.is_empty()),
        other => panic!("expected empty Array, got {other:?}"),
    }
}

#[tokio::test]
async fn sort_store_replaces_dest_with_the_sorted_list() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["RPUSH", "src", "3", "1", "2"]).await;
    c.ok(&["SET", "dst", "old"]).await;
    assert_eq!(c.get_int(&["SORT", "src", "STORE", "dst"]).await, 3);
    let items = c.cmd(&["LRANGE", "dst", "0", "-1"]).await;
    let expected: Vec<Frame> = ["1", "2", "3"]
        .iter()
        .map(|s| Frame::Bulk(bytes::Bytes::from(*s)))
        .collect();
    assert_eq!(items, Frame::Array(expected));

    // sorting a missing key stores nothing and removes dest
    assert_eq!(c.get_int(&["SORT", "missing", "STORE", "dst"]).await, 0);
    assert_eq!(c.get_int(&["EXISTS", "dst"]).await, 0);
}

#[tokio::test]
async fn zrevrange_counts_ranks_from_the_highest_score() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.cmd(&["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"])
        .await;
    let bulks = |items: &[&str]| {
        Frame::Array(
            items
                .iter()
                .map(|s| Frame::Bulk(bytes::Bytes::from(s.to_string())))
                .collect(),
        )
    };
    assert_eq!(
        c.cmd(&["ZREVRANGE", "z", "0", "1"]).await,
        bulks(&["d", "c"])
    );
    assert_eq!(
        c.cmd(&["ZREVRANGE", "z", "1", "2"]).await,
        bulks(&["c", "b"])
    );
    assert_eq!(
        c.cmd(&["ZREVRANGE", "z", "-2", "-1"]).await,
        bulks(&["b", "a"])
    );
}

/// Bulk strings sorted, for comparing set replies whose order is arbitrary.
fn sorted_members(frame: Frame) -> Vec<String> {
    let Frame::Array(items) = frame else {
        panic!("expected an array, got {frame:?}");
    };
    let mut members: Vec<String> = items
        .into_iter()
        .map(|f| match f {
            Frame::Bulk(b) => String::from_utf8(b.to_vec()).unwrap(),
            other => panic!("expected a bulk string, got {other:?}"),
        })
        .collect();
    members.sort();
    members
}

#[tokio::test]
async fn set_algebra_spans_shards() {
    let server = TestServer::start_with(ServerOptions {
        shards: Some(4),
        ..Default::default()
    });
    let mut c = server.connect().await;

    c.cmd(&["SADD", "s1", "a", "b", "c"]).await;
    c.cmd(&["SADD", "s2", "b", "c", "d"]).await;
    c.cmd(&["SADD", "s3", "c", "d", "e"]).await;

    let union = c.cmd(&["SUNION", "s1", "s2", "s3"]).await;
    assert_eq!(sorted_members(union), ["a", "b", "c", "d", "e"]);
    let inter = c.cmd(&["SINTER", "s1", "s2", "s3"]).await;
    assert_eq!(sorted_members(inter), ["c"]);
    let diff = c.cmd(&["SDIFF", "s1", "s2", "s3"]).await;
    assert_eq!(sorted_members(diff), ["a"]);
    let inter = c.cmd(&["SINTER", "s1", "missing"]).await;
    assert!(sorted_members(inter).is_empty());

    assert_eq!(
        c.get_int(&["SUNIONSTORE", "dst", "s1", "s2", "s3"]).await,
        5
    );
    let stored = c.cmd(&["SMEMBERS", "dst"]).await;
    assert_eq!(sorted_members(stored), ["a", "b", "c", "d", "e"]);
    assert_eq!(
        c.get_int(&["SINTERSTORE", "dst", "s1", "s2", "s3"]).await,
        1
    );
    let stored = c.cmd(&["SMEMBERS", "dst"]).await;
    assert_eq!(sorted_members(stored), ["c"]);
    assert_eq!(c.get_int(&["SDIFFSTORE", "dst", "missing", "s1"]).await, 0);
    assert_eq!(c.get_int(&["EXISTS", "dst"]).await, 0);

    c.ok(&["SET", "str", "x"]).await;
    let err = c.err(&["SUNION", "s1", "s2", "s3", "str"]).await;
    assert!(err.starts_with("WRONGTYPE"), "{err}");
}

#[tokio::test]
async fn sorted_set_algebra_spans_shards() {
    let server = TestServer::start_with(ServerOptions {
        shards: Some(4),
        ..Default::default()
    });
    let mut c = server.connect().await;

    c.cmd(&["ZADD", "z1", "1", "a", "2", "b"]).await;
    c.cmd(&["ZADD", "z2", "3", "b", "4", "c"]).await;
    c.cmd(&["ZADD", "z3", "5", "b", "6", "c"]).await;

    let bulks = |items: &[&str]| {
        Frame::Array(
            items
                .iter()
                .map(|s| Frame::Bulk(bytes::Bytes::from(s.to_string())))
                .collect(),
        )
    };
    let union = c
        .cmd(&["ZUNION", "3", "z1", "z2", "z3", "WITHSCORES"])
        .await;
    assert_eq!(union, bulks(&["a", "1", "b", "10", "c", "10"]));
    let inter = c
        .cmd(&["ZINTER", "3", "z1", "z2", "z3", "WITHSCORES"])
        .await;
    assert_eq!(inter, bulks(&["b", "10"]));
    let diff = c.cmd(&["ZDIFF", "2", "z1", "z2", "WITHSCORES"]).await;
    assert_eq!(diff, bulks(&["a", "1"]));

    assert_eq!(
        c.get_int(&["ZUNIONSTORE", "zd", "3", "z1", "z2", "z3"])
            .await,
        3
    );
    let stored = c.cmd(&["ZRANGE", "zd", "0", "-1", "WITHSCORES"]).await;
    assert_eq!(stored, bulks(&["a", "1", "b", "10", "c", "10"]));
    assert_eq!(c.get_int(&["ZINTERSTORE", "zd", "2", "z2", "z3"]).await, 2);
    let stored = c.cmd(&["ZRANGE", "zd", "0", "-1", "WITHSCORES"]).await;
    assert_eq!(stored, bulks(&["b", "8", "c", "10"]));
}

#[tokio::test]
async fn moves_between_shards_land_on_the_destination() {
    let server = TestServer::start_with(ServerOptions {
        shards: Some(4),
        ..Default::default()
    });
    let mut c = server.connect().await;
    let dests: Vec<String> = (0..8).map(|i| format!("dest{i}")).collect();

    // LMOVE: every destination gets its element and the source empties
    let values: Vec<String> = (0..8).map(|i| format!("v{i}")).collect();
    let mut push = vec!["RPUSH", "list"];
    push.extend(values.iter().map(String::as_str));
    c.cmd(&push).await;
    for (dest, value) in dests.iter().zip(&values) {
        let moved = c.get_bulk(&["LMOVE", "list", dest, "LEFT", "RIGHT"]).await;
        assert_eq!(moved.as_deref(), Some(value.as_str()));
        assert_eq!(
            c.get_bulk(&["LINDEX", dest, "0"]).await.as_deref(),
            Some(value.as_str())
        );
    }
    assert_eq!(c.get_int(&["EXISTS", "list"]).await, 0);

    // SMOVE the same way
    let mut add = vec!["SADD", "set"];
    add.extend(values.iter().map(String::as_str));
    c.cmd(&add).await;
    for (i, value) in values.iter().enumerate() {
        let dest = format!("sdest{i}");
        assert_eq!(c.get_int(&["SMOVE", "set", &dest, value]).await, 1);
        assert_eq!(c.get_int(&["SISMEMBER", &dest, value]).await, 1);
    }
    assert_eq!(c.get_int(&["EXISTS", "set"]).await, 0);

    // a destination of the wrong type fails the move and keeps the source
    c.cmd(&["RPUSH", "list", "x"]).await;
    c.cmd(&["SADD", "set", "x"]).await;
    for i in 0..8 {
        let dest = format!("str{i}");
        c.ok(&["SET", &dest, "v"]).await;
        let err = c.err(&["LMOVE", "list", &dest, "LEFT", "LEFT"]).await;
        assert!(err.starts_with("WRONGTYPE"), "{err}");
        let err = c.err(&["SMOVE", "set", &dest, "x"]).await;
        assert!(err.starts_with("WRONGTYPE"), "{err}");
    }
    assert_eq!(c.get_int(&["LLEN", "list"]).await, 1);
    assert_eq!(c.get_int(&["SISMEMBER", "set", "x"]).await, 1);
}
