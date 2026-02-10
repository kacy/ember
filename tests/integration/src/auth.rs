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
