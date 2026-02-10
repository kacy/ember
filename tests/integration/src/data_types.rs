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

    c.get_int(&["HSET", "h", "name", "ember", "version", "0.4"]).await;
    assert_eq!(c.get_bulk(&["HGET", "h", "name"]).await, Some("ember".into()));
    assert_eq!(c.get_bulk(&["HGET", "h", "version"]).await, Some("0.4".into()));

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

    c.get_int(&["HSET", "h", "a", "1", "b", "2", "c", "3"]).await;
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

    assert_eq!(c.get_int(&["ZADD", "z", "1.0", "a", "2.0", "b", "3.0", "c"]).await, 3);
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

    c.get_int(&["ZADD", "z", "1", "a", "2", "b", "3", "c"]).await;

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
