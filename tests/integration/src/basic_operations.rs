//! Integration tests for basic string/key operations.

use ember_protocol::Frame;

use crate::helpers::{ServerOptions, TestServer};

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
    // use a single shard so RENAME doesn't hit cross-shard errors
    let server = TestServer::start_with(ServerOptions {
        shards: Some(1),
        ..Default::default()
    });
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
    assert_eq!(
        c.get_bulk(&["GET", "key"]).await,
        Some("hello world".into())
    );
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

// --- EXPIREAT / PEXPIREAT ---

#[tokio::test]
async fn expireat_sets_absolute_expiry() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "k", "v"]).await;

    // timestamp 100 seconds in the future
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 100;
    let future_str = future.to_string();

    assert_eq!(c.get_int(&["EXPIREAT", "k", &future_str]).await, 1);
    let ttl = c.get_int(&["TTL", "k"]).await;
    assert!(ttl > 0 && ttl <= 100, "expected TTL in (0,100], got {ttl}");
}

#[tokio::test]
async fn expireat_missing_key_returns_zero() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["EXPIREAT", "nope", "9999999999"]).await, 0);
}

#[tokio::test]
async fn pexpireat_sets_absolute_expiry_ms() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "k", "v"]).await;

    let future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    let future_str = future_ms.to_string();

    assert_eq!(c.get_int(&["PEXPIREAT", "k", &future_str]).await, 1);
    let pttl = c.get_int(&["PTTL", "k"]).await;
    assert!(
        pttl > 0 && pttl <= 60_000,
        "expected PTTL in (0,60000], got {pttl}"
    );
}

// --- GETSET ---

#[tokio::test]
async fn getset_returns_old_value() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "k", "old"]).await;

    let old = c.get_bulk(&["GETSET", "k", "new"]).await;
    assert_eq!(old, Some("old".into()));

    let current = c.get_bulk(&["GET", "k"]).await;
    assert_eq!(current, Some("new".into()));
}

#[tokio::test]
async fn getset_missing_key_returns_nil() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    let resp = c.cmd(&["GETSET", "nope", "v"]).await;
    assert!(matches!(resp, Frame::Null));

    // key should now exist
    let current = c.get_bulk(&["GET", "nope"]).await;
    assert_eq!(current, Some("v".into()));
}

// --- MSETNX ---

#[tokio::test]
async fn msetnx_all_new_returns_one() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    assert_eq!(c.get_int(&["MSETNX", "a", "1", "b", "2"]).await, 1);
    assert_eq!(c.get_bulk(&["GET", "a"]).await, Some("1".into()));
    assert_eq!(c.get_bulk(&["GET", "b"]).await, Some("2".into()));
}

#[tokio::test]
async fn msetnx_any_existing_returns_zero_and_no_changes() {
    let server = TestServer::start();
    let mut c = server.connect().await;

    c.ok(&["SET", "a", "existing"]).await;

    // should fail atomically — neither "a" nor "b" should change
    assert_eq!(c.get_int(&["MSETNX", "a", "new", "b", "2"]).await, 0);

    // "a" keeps its original value
    assert_eq!(c.get_bulk(&["GET", "a"]).await, Some("existing".into()));

    // "b" was not created
    let resp = c.cmd(&["GET", "b"]).await;
    assert!(matches!(resp, Frame::Null));
}

/// Sends each command and checks that it is rejected with an error and that
/// the server still answers PING afterwards. Release builds abort on panic,
/// so a panic here would take the whole server down.
async fn assert_rejected_without_crash(server: &TestServer, commands: &[&[&str]]) {
    let mut c = server.connect().await;
    for command in commands {
        let resp = c.cmd(command).await;
        assert!(matches!(resp, Frame::Error(_)), "{command:?} gave {resp:?}");
    }
    let resp = c.cmd(&["PING"]).await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "PONG"));
}

#[tokio::test]
async fn hostile_arguments_are_rejected_without_crashing() {
    let server = TestServer::start();
    assert_rejected_without_crash(
        &server,
        &[
            &["ZINTER", "18446744073709551615", "a"],
            &["LMPOP", "18446744073709551615", "a", "LEFT"],
            &["BLPOP", "k", "nan"],
            &["ZCOUNT", "z", "nan", "1"],
            &["SETRANGE", "s", "100000000000", "x"],
            &["SETBIT", "b", "100000000000", "1"],
            &["SRANDMEMBER", "set", "-9223372036854775808"],
        ],
    )
    .await;
}

#[tokio::test]
async fn concurrent_decrby_min_is_rejected_without_crashing() {
    let server = TestServer::start_with(ServerOptions {
        concurrent: true,
        ..Default::default()
    });
    assert_rejected_without_crash(&server, &[&["DECRBY", "n", "-9223372036854775808"]]).await;
}

#[tokio::test]
async fn pipeline_longer_than_the_depth_limit_gets_every_reply() {
    // the server parses at most 10,000 frames per batch. the rest must be
    // processed without waiting for the client to send more bytes.
    const COUNT: usize = 10_050;
    let server = TestServer::start();
    let mut c = server.connect().await;
    c.write_raw(&b"*1\r\n$4\r\nPING\r\n".repeat(COUNT)).await;

    for i in 0..COUNT {
        let resp = tokio::time::timeout(std::time::Duration::from_secs(10), c.read_frame())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for reply {i}"));
        assert!(matches!(resp, Frame::Simple(ref s) if s == "PONG"));
    }
}

/// Sends raw bytes on a fresh connection and returns everything the server
/// writes back before it closes the connection.
async fn send_and_read_to_close(server: &TestServer, request: &[u8]) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .unwrap();
    stream.write_all(request).await.unwrap();
    let mut reply = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.read_to_end(&mut reply),
    )
    .await
    .expect("server did not close the connection")
    .unwrap();
    reply
}

#[tokio::test]
async fn quit_replies_and_closes_without_running_later_commands() {
    let server = TestServer::start();
    let request =
        b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nQUIT\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
    let reply = send_and_read_to_close(&server, request).await;
    assert_eq!(reply, b"+PONG\r\n+OK\r\n");

    let mut c = server.connect().await;
    assert_eq!(c.get_bulk(&["GET", "k"]).await, None);
}

#[tokio::test]
async fn unauthenticated_client_cannot_send_large_requests() {
    let server = TestServer::start_with(ServerOptions {
        requirepass: Some("pw".into()),
        ..Default::default()
    });
    let big = vec![b'x'; 100 * 1024];
    let mut request = format!("*2\r\n$4\r\nECHO\r\n${}\r\n", big.len()).into_bytes();
    request.extend_from_slice(&big);
    request.extend_from_slice(b"\r\n");

    let reply = send_and_read_to_close(&server, &request).await;
    assert!(
        reply.starts_with(b"-ERR max buffer size exceeded"),
        "{reply:?}"
    );
}
