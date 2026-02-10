//! Integration tests for basic string/key operations.

use ember_protocol::Frame;

use crate::helpers::TestServer;

#[tokio::test]
async fn ping_pong() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let resp = c.cmd(&["PING"]).await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "PONG"));
}

#[tokio::test]
async fn ping_with_message() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let resp = c.get_bulk(&["PING", "hello"]).await;
    assert_eq!(resp, Some("hello".into()));
}

#[tokio::test]
async fn echo() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let resp = c.get_bulk(&["ECHO", "test"]).await;
    assert_eq!(resp, Some("test".into()));
}

#[tokio::test]
async fn set_get_roundtrip() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "foo", "bar"]).await;
    let val = c.get_bulk(&["GET", "foo"]).await;
    assert_eq!(val, Some("bar".into()));
}

#[tokio::test]
async fn get_missing_key() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let resp = c.cmd(&["GET", "nonexistent"]).await;
    assert!(matches!(resp, Frame::Null));
}

#[tokio::test]
async fn set_with_nx() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "key", "first", "NX"]).await;
    // second SET NX should return null (key already exists)
    let resp = c.cmd(&["SET", "key", "second", "NX"]).await;
    assert!(matches!(resp, Frame::Null));
    // original value preserved
    assert_eq!(c.get_bulk(&["GET", "key"]).await, Some("first".into()));
}

#[tokio::test]
async fn set_with_xx() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    // XX on missing key → null
    let resp = c.cmd(&["SET", "key", "val", "XX"]).await;
    assert!(matches!(resp, Frame::Null));

    c.ok(&["SET", "key", "val"]).await;
    // XX on existing key → OK
    c.ok(&["SET", "key", "updated", "XX"]).await;
    assert_eq!(c.get_bulk(&["GET", "key"]).await, Some("updated".into()));
}

#[tokio::test]
async fn set_with_ex() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "ttl", "val", "EX", "10"]).await;
    let ttl = c.get_int(&["TTL", "ttl"]).await;
    assert!(ttl > 0 && ttl <= 10);
}

#[tokio::test]
async fn set_with_px() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "ttl", "val", "PX", "10000"]).await;
    let pttl = c.get_int(&["PTTL", "ttl"]).await;
    assert!(pttl > 0 && pttl <= 10000);
}

#[tokio::test]
async fn del_existing_and_missing() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "a", "1"]).await;
    c.ok(&["SET", "b", "2"]).await;

    let count = c.get_int(&["DEL", "a", "b", "c"]).await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn exists() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "here", "yes"]).await;
    assert_eq!(c.get_int(&["EXISTS", "here"]).await, 1);
    assert_eq!(c.get_int(&["EXISTS", "gone"]).await, 0);
}

#[tokio::test]
async fn unlink() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "key", "val"]).await;
    let count = c.get_int(&["UNLINK", "key"]).await;
    assert_eq!(count, 1);
    assert!(matches!(c.cmd(&["GET", "key"]).await, Frame::Null));
}

#[tokio::test]
async fn incr_decr() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["INCR", "counter"]).await, 1);
    assert_eq!(c.get_int(&["INCR", "counter"]).await, 2);
    assert_eq!(c.get_int(&["DECR", "counter"]).await, 1);
}

#[tokio::test]
async fn incrby_decrby() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "n", "10"]).await;
    assert_eq!(c.get_int(&["INCRBY", "n", "5"]).await, 15);
    assert_eq!(c.get_int(&["DECRBY", "n", "3"]).await, 12);
}

#[tokio::test]
async fn expire_ttl_persist() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "key", "val"]).await;
    assert_eq!(c.get_int(&["TTL", "key"]).await, -1); // no expiry

    c.get_int(&["EXPIRE", "key", "100"]).await;
    let ttl = c.get_int(&["TTL", "key"]).await;
    assert!(ttl > 0 && ttl <= 100);

    c.get_int(&["PERSIST", "key"]).await;
    assert_eq!(c.get_int(&["TTL", "key"]).await, -1);
}

#[tokio::test]
async fn mget_mset() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["MSET", "a", "1", "b", "2", "c", "3"]).await;

    let resp = c.cmd(&["MGET", "a", "b", "c", "missing"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 4);
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"1"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"2"[..]));
            assert!(matches!(&frames[2], Frame::Bulk(b) if b == &b"3"[..]));
            assert!(matches!(&frames[3], Frame::Null));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn type_command() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "s", "val"]).await;
    let resp = c.cmd(&["TYPE", "s"]).await;
    assert!(matches!(resp, Frame::Simple(ref t) if t == "string"));

    c.cmd(&["LPUSH", "l", "a"]).await;
    let resp = c.cmd(&["TYPE", "l"]).await;
    assert!(matches!(resp, Frame::Simple(ref t) if t == "list"));

    let resp = c.cmd(&["TYPE", "missing"]).await;
    assert!(matches!(resp, Frame::Simple(ref t) if t == "none"));
}

#[tokio::test]
async fn rename() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "old", "value"]).await;
    c.ok(&["RENAME", "old", "new"]).await;
    assert!(matches!(c.cmd(&["GET", "old"]).await, Frame::Null));
    assert_eq!(c.get_bulk(&["GET", "new"]).await, Some("value".into()));
}

#[tokio::test]
async fn strlen_append() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "key", "hello"]).await;
    assert_eq!(c.get_int(&["STRLEN", "key"]).await, 5);
    assert_eq!(c.get_int(&["APPEND", "key", " world"]).await, 11);
    assert_eq!(c.get_bulk(&["GET", "key"]).await, Some("hello world".into()));
}

#[tokio::test]
async fn dbsize() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["DBSIZE"]).await, 0);
    c.ok(&["SET", "a", "1"]).await;
    c.ok(&["SET", "b", "2"]).await;
    assert_eq!(c.get_int(&["DBSIZE"]).await, 2);
}

#[tokio::test]
async fn flushdb() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "a", "1"]).await;
    c.ok(&["SET", "b", "2"]).await;
    c.ok(&["FLUSHDB"]).await;
    assert_eq!(c.get_int(&["DBSIZE"]).await, 0);
}

#[tokio::test]
async fn keys_pattern() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "user:1", "a"]).await;
    c.ok(&["SET", "user:2", "b"]).await;
    c.ok(&["SET", "item:1", "c"]).await;

    let resp = c.cmd(&["KEYS", "user:*"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 2);
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn scan_basic() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    for i in 0..5 {
        c.ok(&["SET", &format!("key:{i}"), "v"]).await;
    }

    // scan with a large count to get everything in one pass
    let resp = c.cmd(&["SCAN", "0", "COUNT", "100"]).await;
    match resp {
        Frame::Array(frames) => {
            assert_eq!(frames.len(), 2);
            // cursor should be "0" (complete)
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"0"[..]));
            // should have 5 keys
            if let Frame::Array(ref keys) = frames[1] {
                assert_eq!(keys.len(), 5);
            } else {
                panic!("expected array of keys");
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn info_returns_sections() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let resp = c.cmd(&["INFO"]).await;
    match resp {
        Frame::Bulk(data) => {
            let text = String::from_utf8_lossy(&data);
            assert!(text.contains("# Server"));
            assert!(text.contains("ember_version"));
        }
        other => panic!("expected Bulk, got {other:?}"),
    }
}

#[tokio::test]
async fn unknown_command() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let msg = c.err(&["NOTACOMMAND"]).await;
    assert!(msg.contains("unknown command"));
}
