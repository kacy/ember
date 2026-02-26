//! Integration tests for lists, hashes, sets, and sorted sets.

use ember_protocol::Frame;

use crate::helpers::TestServer;

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
