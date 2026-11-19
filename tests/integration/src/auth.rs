//! Integration tests for authentication.

use crate::helpers::{ServerOptions, TestServer};

#[tokio::test]
async fn auth_required() {
    let server = TestServer::start_with(ServerOptions {
        requirepass: Some("secret123".into()),
        ..Default::default()
    });
    let mut c = server.connect().await;

    // commands should be rejected before AUTH
    let msg = c.err(&["SET", "key", "val"]).await;
    assert!(msg.contains("NOAUTH"));

    // wrong password
    let msg = c.err(&["AUTH", "wrongpass"]).await;
    assert!(msg.contains("WRONGPASS"));

    // correct password
    c.ok(&["AUTH", "secret123"]).await;

    // commands work after auth
    c.ok(&["SET", "key", "val"]).await;
    assert_eq!(c.get_bulk(&["GET", "key"]).await, Some("val".into()));
}

#[tokio::test]
async fn ping_allowed_without_auth() {
    let server = TestServer::start_with(ServerOptions {
        requirepass: Some("pass".into()),
        ..Default::default()
    });
    let mut c = server.connect().await;

    // PING should work even without auth
    let resp = c.cmd(&["PING"]).await;
    assert!(matches!(resp, ember_protocol::Frame::Simple(ref s) if s == "PONG"));
}

#[tokio::test]
async fn acl_users_created_at_runtime_are_enforced() {
    let server = TestServer::start_with(ServerOptions {
        requirepass: Some("admin-pass".into()),
        ..Default::default()
    });
    let mut admin = server.connect().await;
    admin.ok(&["AUTH", "admin-pass"]).await;
    admin
        .ok(&[
            "ACL",
            "SETUSER",
            "reader",
            "on",
            ">reader-pass",
            "+get",
            "~cache:*",
        ])
        .await;

    let mut reader = server.connect().await;
    reader.ok(&["AUTH", "reader", "reader-pass"]).await;
    assert_eq!(reader.get_bulk(&["GET", "cache:1"]).await, None);

    let err = reader.err(&["GET", "secret:1"]).await;
    assert!(err.starts_with("NOPERM"), "{err}");
    let err = reader.err(&["SET", "cache:1", "v"]).await;
    assert!(err.starts_with("NOPERM"), "{err}");

    let err = server
        .connect()
        .await
        .err(&["AUTH", "reader", "wrong"])
        .await;
    assert!(err.starts_with("WRONGPASS"), "{err}");
}

/// Starts a server with an admin password and a `reader` user limited to
/// GET on `cache:*`. Returns the server and an admin connection.
async fn server_with_reader() -> (TestServer, crate::helpers::TestClient) {
    let server = TestServer::start_with(ServerOptions {
        requirepass: Some("admin-pass".into()),
        ..Default::default()
    });
    let mut admin = server.connect().await;
    admin.ok(&["AUTH", "admin-pass"]).await;
    admin
        .ok(&[
            "ACL",
            "SETUSER",
            "reader",
            "on",
            ">reader-pass",
            "+get",
            "~cache:*",
        ])
        .await;
    (server, admin)
}

#[tokio::test]
async fn auth_on_an_authenticated_connection_switches_user() {
    let (server, _admin) = server_with_reader().await;
    let mut c = server.connect().await;
    c.ok(&["AUTH", "admin-pass"]).await;
    c.ok(&["SET", "cache:1", "v"]).await;

    c.ok(&["AUTH", "reader", "reader-pass"]).await;
    assert_eq!(c.get_bulk(&["ACL", "WHOAMI"]).await, Some("reader".into()));
    let err = c.err(&["SET", "cache:1", "v"]).await;
    assert!(err.starts_with("NOPERM"), "{err}");
}

#[tokio::test]
async fn acl_changes_reach_open_connections() {
    let (server, mut admin) = server_with_reader().await;
    let mut reader = server.connect().await;
    reader.ok(&["AUTH", "reader", "reader-pass"]).await;
    assert_eq!(reader.get_bulk(&["GET", "cache:1"]).await, None);

    admin.ok(&["ACL", "SETUSER", "reader", "-get"]).await;
    let err = reader.err(&["GET", "cache:1"]).await;
    assert!(err.starts_with("NOPERM"), "{err}");

    admin.get_int(&["ACL", "DELUSER", "reader"]).await;
    let err = reader.err(&["GET", "cache:1"]).await;
    assert!(err.starts_with("NOAUTH"), "{err}");
}

#[tokio::test]
async fn monitor_needs_permission() {
    let (server, _admin) = server_with_reader().await;
    let mut reader = server.connect().await;
    reader.ok(&["AUTH", "reader", "reader-pass"]).await;

    let err = reader.err(&["MONITOR"]).await;
    assert!(err.starts_with("NOPERM"), "{err}");
    // the connection stays in normal mode
    assert_eq!(reader.get_bulk(&["GET", "cache:1"]).await, None);
}

#[tokio::test]
async fn transaction_commands_need_permission() {
    let (server, mut admin) = server_with_reader().await;
    admin.ok(&["ACL", "SETUSER", "reader", "+watch"]).await;
    let mut reader = server.connect().await;
    reader.ok(&["AUTH", "reader", "reader-pass"]).await;

    let err = reader.err(&["WATCH", "secret:1"]).await;
    assert!(err.starts_with("NOPERM"), "{err}");
    reader.ok(&["WATCH", "cache:1"]).await;
    let err = reader.err(&["MULTI"]).await;
    assert!(err.starts_with("NOPERM"), "{err}");
}

/// Writes `commands` to the connection in one write, so the server reads
/// them as one batch, and returns their replies.
async fn send_batch(
    c: &mut crate::helpers::TestClient,
    commands: &[&[&str]],
) -> Vec<ember_protocol::Frame> {
    let mut out = Vec::new();
    for args in commands {
        out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for arg in *args {
            out.extend_from_slice(format!("${}\r\n{arg}\r\n", arg.len()).as_bytes());
        }
    }
    c.write_raw(&out).await;
    let mut replies = Vec::new();
    for _ in commands {
        replies.push(c.read_frame().await);
    }
    replies
}

#[tokio::test]
async fn a_transaction_in_the_same_batch_as_auth_is_queued() {
    use ember_protocol::Frame;

    let server = TestServer::start_with(ServerOptions {
        requirepass: Some("pass".into()),
        ..Default::default()
    });
    let mut c = server.connect().await;
    let replies = send_batch(
        &mut c,
        &[&["AUTH", "pass"], &["MULTI"], &["SET", "k", "v"], &["EXEC"]],
    )
    .await;
    assert_eq!(replies[2], Frame::Simple("QUEUED".into()), "{replies:?}");
    assert!(
        matches!(&replies[3], Frame::Array(r) if r.len() == 1),
        "{replies:?}"
    );

    // a transaction already open keeps queueing when AUTH arrives
    c.ok(&["MULTI"]).await;
    let replies = send_batch(&mut c, &[&["AUTH", "pass"], &["SET", "k", "w"]]).await;
    assert_eq!(replies[1], Frame::Simple("QUEUED".into()), "{replies:?}");
    assert_eq!(c.cmd(&["GET", "k"]).await, Frame::Simple("QUEUED".into()));
    assert!(matches!(c.cmd(&["EXEC"]).await, Frame::Array(r) if r.len() == 2));
    assert_eq!(c.get_bulk(&["GET", "k"]).await, Some("w".into()));
}

#[tokio::test]
async fn deleting_a_user_disconnects_its_subscribers_and_monitors() {
    let (server, mut admin) = server_with_reader().await;
    admin
        .ok(&[
            "ACL",
            "SETUSER",
            "reader",
            "+subscribe",
            "+monitor",
            "&news",
        ])
        .await;
    let mut subscriber = server.connect().await;
    subscriber.ok(&["AUTH", "reader", "reader-pass"]).await;
    subscriber.cmd(&["SUBSCRIBE", "news"]).await;
    let mut monitor = server.connect().await;
    monitor.ok(&["AUTH", "reader", "reader-pass"]).await;
    monitor.ok(&["MONITOR"]).await;

    admin.get_int(&["ACL", "DELUSER", "reader"]).await;
    // the next message or monitored command finds the user gone
    admin.get_int(&["PUBLISH", "news", "hello"]).await;
    assert!(subscriber.closed().await, "subscriber still connected");
    assert!(monitor.closed().await, "monitor still connected");
}

#[tokio::test]
async fn a_read_only_user_from_minus_all_plus_read_can_read() {
    let (server, mut admin) = server_with_reader().await;
    admin
        .ok(&["ACL", "SETUSER", "reader", "-@all", "+@read", "allkeys"])
        .await;
    let mut reader = server.connect().await;
    reader.ok(&["AUTH", "reader", "reader-pass"]).await;
    assert_eq!(reader.get_bulk(&["GET", "k"]).await, None);
    let err = reader.err(&["SET", "k", "v"]).await;
    assert!(err.starts_with("NOPERM"), "{err}");
}

#[tokio::test]
async fn channel_rules_limit_pub_sub() {
    let (server, mut admin) = server_with_reader().await;
    admin
        .ok(&[
            "ACL",
            "SETUSER",
            "reader",
            "+publish",
            "+subscribe",
            "+psubscribe",
            "&news.*",
        ])
        .await;
    let mut reader = server.connect().await;
    reader.ok(&["AUTH", "reader", "reader-pass"]).await;

    let err = reader.err(&["PUBLISH", "secret", "x"]).await;
    assert!(err.starts_with("NOPERM"), "{err}");
    assert_eq!(reader.get_int(&["PUBLISH", "news.sport", "x"]).await, 0);
    // a pattern must be one of the user's patterns, not just overlap them
    let err = reader.err(&["PSUBSCRIBE", "*"]).await;
    assert!(err.starts_with("NOPERM"), "{err}");
    let err = reader.err(&["SUBSCRIBE", "secret"]).await;
    assert!(err.starts_with("NOPERM"), "{err}");
}

#[tokio::test]
async fn losing_a_channel_disconnects_its_subscriber() {
    let (server, mut admin) = server_with_reader().await;
    admin
        .ok(&["ACL", "SETUSER", "reader", "+subscribe", "&news"])
        .await;
    let mut subscriber = server.connect().await;
    subscriber.ok(&["AUTH", "reader", "reader-pass"]).await;
    subscriber.cmd(&["SUBSCRIBE", "news"]).await;

    admin
        .ok(&["ACL", "SETUSER", "reader", "resetchannels"])
        .await;
    admin.get_int(&["PUBLISH", "news", "hello"]).await;
    assert!(subscriber.closed().await, "subscriber still connected");
}
