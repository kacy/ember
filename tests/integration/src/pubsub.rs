//! Integration tests for pub/sub.

use ember_protocol::Frame;

use crate::helpers::TestServer;

#[tokio::test]
async fn subscribe_and_receive_message() {
    let server = TestServer::start();
    let mut sub = server.connect().await;
    let mut publisher = server.connect().await;

    // subscribe — confirmation frame
    let resp = sub.cmd(&["SUBSCRIBE", "events"]).await;
    match resp {
        Frame::Array(ref frames) => {
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"subscribe"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"events"[..]));
            assert!(matches!(&frames[2], Frame::Integer(1)));
        }
        other => panic!("expected subscribe confirmation, got {other:?}"),
    }

    // publish a message from another connection
    let count = publisher.get_int(&["PUBLISH", "events", "hello"]).await;
    assert_eq!(count, 1);

    // subscriber receives the message (pushed frame, no command needed)
    let msg = sub.read_frame().await;
    match msg {
        Frame::Array(ref frames) => {
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"message"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"events"[..]));
            assert!(matches!(&frames[2], Frame::Bulk(b) if b == &b"hello"[..]));
        }
        other => panic!("expected message frame, got {other:?}"),
    }
}

#[tokio::test]
async fn psubscribe_pattern_match() {
    let server = TestServer::start();
    let mut sub = server.connect().await;
    let mut publisher = server.connect().await;

    // pattern subscribe
    let resp = sub.cmd(&["PSUBSCRIBE", "user:*"]).await;
    match resp {
        Frame::Array(ref frames) => {
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"psubscribe"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"user:*"[..]));
            assert!(matches!(&frames[2], Frame::Integer(1)));
        }
        other => panic!("expected psubscribe confirmation, got {other:?}"),
    }

    // publish to a matching channel
    let count = publisher.get_int(&["PUBLISH", "user:login", "alice"]).await;
    assert_eq!(count, 1);

    // subscriber receives pmessage
    let msg = sub.read_frame().await;
    match msg {
        Frame::Array(ref frames) => {
            assert_eq!(frames.len(), 4);
            assert!(matches!(&frames[0], Frame::Bulk(b) if b == &b"pmessage"[..]));
            assert!(matches!(&frames[1], Frame::Bulk(b) if b == &b"user:*"[..]));
            assert!(matches!(&frames[2], Frame::Bulk(b) if b == &b"user:login"[..]));
            assert!(matches!(&frames[3], Frame::Bulk(b) if b == &b"alice"[..]));
        }
        other => panic!("expected pmessage frame, got {other:?}"),
    }
}

#[tokio::test]
async fn publish_returns_subscriber_count() {
    let server = TestServer::start();
    let mut sub = server.connect().await;
    let mut publisher = server.connect().await;

    // no subscribers yet
    let count = publisher.get_int(&["PUBLISH", "chan", "msg"]).await;
    assert_eq!(count, 0);

    // subscribe
    sub.cmd(&["SUBSCRIBE", "chan"]).await;

    // now there's 1 subscriber
    let count = publisher.get_int(&["PUBLISH", "chan", "msg"]).await;
    assert_eq!(count, 1);
}
