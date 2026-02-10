//! Integration tests for protobuf value storage (PROTO.* commands).

use bytes::Bytes;
use ember_protocol::Frame;
use prost_reflect::prost::Message;
use prost_reflect::prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
};
use prost_reflect::{DescriptorPool, DynamicMessage};

use crate::helpers::{ServerOptions, TestServer};

/// Builds a minimal FileDescriptorSet with a single message type.
fn make_descriptor(package: &str, message_name: &str, field_name: &str) -> Vec<u8> {
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some(format!("{package}.proto")),
            package: Some(package.to_owned()),
            message_type: vec![DescriptorProto {
                name: Some(message_name.to_owned()),
                field: vec![FieldDescriptorProto {
                    name: Some(field_name.to_owned()),
                    number: Some(1),
                    r#type: Some(9), // TYPE_STRING
                    label: Some(1),  // LABEL_OPTIONAL
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut buf = Vec::new();
    fds.encode(&mut buf).expect("encode descriptor");
    buf
}

/// Encodes a DynamicMessage with a single string field.
fn encode_message(descriptor_bytes: &[u8], type_name: &str, field: &str, value: &str) -> Vec<u8> {
    let pool = DescriptorPool::decode(descriptor_bytes).expect("decode pool");
    let msg_desc = pool.get_message_by_name(type_name).expect("find message");
    let mut msg = DynamicMessage::new(msg_desc);
    msg.set_field_by_name(field, prost_reflect::Value::String(value.into()));
    let mut buf = Vec::new();
    msg.encode(&mut buf).expect("encode message");
    buf
}

fn start_proto_server() -> TestServer {
    TestServer::start_with(ServerOptions {
        protobuf: true,
        ..Default::default()
    })
}

#[tokio::test]
async fn register_schema_and_describe() {
    let server = start_proto_server();
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");

    // register should return the message type names
    let resp = c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;
    match resp {
        Frame::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Frame::Bulk(Bytes::from("test.User")));
        }
        other => panic!("expected array, got {other:?}"),
    }

    // PROTO.SCHEMAS should list the registered schema
    let resp = c.cmd(&["PROTO.SCHEMAS"]).await;
    match resp {
        Frame::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Frame::Bulk(Bytes::from("users")));
        }
        other => panic!("expected array, got {other:?}"),
    }

    // PROTO.DESCRIBE should list message types
    let resp = c.cmd(&["PROTO.DESCRIBE", "users"]).await;
    match resp {
        Frame::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Frame::Bulk(Bytes::from("test.User")));
        }
        other => panic!("expected array, got {other:?}"),
    }
}

#[tokio::test]
async fn describe_unknown_schema() {
    let server = start_proto_server();
    let mut c = server.connect().await;

    let resp = c.cmd(&["PROTO.DESCRIBE", "nonexistent"]).await;
    assert!(matches!(resp, Frame::Error(_)));
}

#[tokio::test]
async fn set_get_and_type() {
    let server = start_proto_server();
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let data = encode_message(&desc, "test.User", "name", "alice");

    // PROTO.SET
    let resp = c
        .cmd_raw(&[b"PROTO.SET", b"user:1", b"test.User", &data])
        .await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));

    // PROTO.GET should return [type_name, data]
    let resp = c.cmd(&["PROTO.GET", "user:1"]).await;
    match resp {
        Frame::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Frame::Bulk(Bytes::from("test.User")));
            assert_eq!(items[1], Frame::Bulk(Bytes::from(data)));
        }
        other => panic!("expected array, got {other:?}"),
    }

    // PROTO.TYPE should return the type name
    let resp = c.cmd(&["PROTO.TYPE", "user:1"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("test.User")));
}

#[tokio::test]
async fn get_missing_key_returns_null() {
    let server = start_proto_server();
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let resp = c.cmd(&["PROTO.GET", "nonexistent"]).await;
    assert!(matches!(resp, Frame::Null));

    let resp = c.cmd(&["PROTO.TYPE", "nonexistent"]).await;
    assert!(matches!(resp, Frame::Null));
}

#[tokio::test]
async fn wrong_type_error() {
    let server = start_proto_server();
    let mut c = server.connect().await;

    // set a regular string key
    c.ok(&["SET", "str:key", "hello"]).await;

    // PROTO.GET on a string key should return WRONGTYPE
    let resp = c.cmd(&["PROTO.GET", "str:key"]).await;
    assert!(matches!(resp, Frame::Error(ref s) if s.starts_with("WRONGTYPE")));

    // PROTO.TYPE on a string key should also return WRONGTYPE
    let resp = c.cmd(&["PROTO.TYPE", "str:key"]).await;
    assert!(matches!(resp, Frame::Error(ref s) if s.starts_with("WRONGTYPE")));
}

#[tokio::test]
async fn invalid_proto_bytes_rejected() {
    let server = start_proto_server();
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    // send garbage bytes as proto data
    let resp = c
        .cmd_raw(&[b"PROTO.SET", b"bad:1", b"test.User", b"not valid proto"])
        .await;
    assert!(matches!(resp, Frame::Error(_)));
}

#[tokio::test]
async fn invalid_descriptor_rejected() {
    let server = start_proto_server();
    let mut c = server.connect().await;

    let resp = c
        .cmd_raw(&[b"PROTO.REGISTER", b"bad", b"not a descriptor"])
        .await;
    assert!(matches!(resp, Frame::Error(_)));
}

#[tokio::test]
async fn set_with_nx_and_xx() {
    let server = start_proto_server();
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let alice = encode_message(&desc, "test.User", "name", "alice");
    let bob = encode_message(&desc, "test.User", "name", "bob");

    // NX on a new key should succeed
    let resp = c
        .cmd_raw(&[b"PROTO.SET", b"user:nx", b"test.User", &alice, b"NX"])
        .await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));

    // NX on an existing key should return null
    let resp = c
        .cmd_raw(&[b"PROTO.SET", b"user:nx", b"test.User", &bob, b"NX"])
        .await;
    assert!(matches!(resp, Frame::Null));

    // XX on a missing key should return null
    let resp = c
        .cmd_raw(&[b"PROTO.SET", b"user:xx", b"test.User", &alice, b"XX"])
        .await;
    assert!(matches!(resp, Frame::Null));

    // XX on an existing key should succeed
    let resp = c
        .cmd_raw(&[b"PROTO.SET", b"user:nx", b"test.User", &bob, b"XX"])
        .await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));
}

#[tokio::test]
async fn set_with_ttl() {
    let server = start_proto_server();
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let data = encode_message(&desc, "test.User", "name", "alice");

    // set with 60s expiry
    let resp = c
        .cmd_raw(&[b"PROTO.SET", b"user:ttl", b"test.User", &data, b"EX", b"60"])
        .await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));

    // TTL should be set
    let ttl = c.get_int(&["TTL", "user:ttl"]).await;
    assert!(ttl > 0 && ttl <= 60);
}

#[tokio::test]
async fn persistence_recovery() {
    let data_dir = tempfile::tempdir().unwrap();
    let path = data_dir.path().to_path_buf();

    let desc = make_descriptor("test", "User", "name");
    let data = encode_message(&desc, "test.User", "name", "alice");

    // start server, register schema, set proto value
    {
        let server = TestServer::start_with(ServerOptions {
            protobuf: true,
            appendonly: true,
            data_dir_path: Some(path.clone()),
            ..Default::default()
        });
        let mut c = server.connect().await;

        c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

        let resp = c
            .cmd_raw(&[b"PROTO.SET", b"user:1", b"test.User", &data])
            .await;
        assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));

        // wait for fsync
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    // restart and verify recovery
    let server = TestServer::start_with(ServerOptions {
        protobuf: true,
        appendonly: true,
        data_dir_path: Some(path),
        ..Default::default()
    });
    let mut c = server.connect().await;

    // proto value should be recovered
    let resp = c.cmd(&["PROTO.GET", "user:1"]).await;
    match resp {
        Frame::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Frame::Bulk(Bytes::from("test.User")));
            assert_eq!(items[1], Frame::Bulk(Bytes::from(data)));
        }
        other => panic!("expected recovered proto value, got {other:?}"),
    }

    drop(data_dir);
}
