//! Integration tests for protobuf value storage (PROTO.* commands).
//!
//! Core tests run against both sharded and concurrent server modes to
//! ensure proto commands work through both code paths.

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

/// Builds a FileDescriptorSet with string, int32, and bool fields.
fn make_multi_field_descriptor() -> Vec<u8> {
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("test.proto".into()),
            package: Some("test".into()),
            message_type: vec![DescriptorProto {
                name: Some("Profile".into()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("name".into()),
                        number: Some(1),
                        r#type: Some(9), // TYPE_STRING
                        label: Some(1),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("age".into()),
                        number: Some(2),
                        r#type: Some(5), // TYPE_INT32
                        label: Some(1),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("active".into()),
                        number: Some(3),
                        r#type: Some(8), // TYPE_BOOL
                        label: Some(1),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut buf = Vec::new();
    fds.encode(&mut buf).expect("encode descriptor");
    buf
}

/// Encodes a Profile message with name, age, and active fields.
fn encode_profile(descriptor_bytes: &[u8], name: &str, age: i32, active: bool) -> Vec<u8> {
    let pool = DescriptorPool::decode(descriptor_bytes).expect("decode pool");
    let msg_desc = pool
        .get_message_by_name("test.Profile")
        .expect("find message");
    let mut msg = DynamicMessage::new(msg_desc);
    msg.set_field_by_name("name", prost_reflect::Value::String(name.into()));
    msg.set_field_by_name("age", prost_reflect::Value::I32(age));
    msg.set_field_by_name("active", prost_reflect::Value::Bool(active));
    let mut buf = Vec::new();
    msg.encode(&mut buf).expect("encode message");
    buf
}

fn start_proto_server(concurrent: bool) -> TestServer {
    TestServer::start_with(ServerOptions {
        protobuf: true,
        concurrent,
        ..Default::default()
    })
}

// ---- sharded mode tests ----

#[tokio::test]
async fn register_schema_and_describe() {
    let server = start_proto_server(false);
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
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let resp = c.cmd(&["PROTO.DESCRIBE", "nonexistent"]).await;
    assert!(matches!(resp, Frame::Error(_)));
}

#[tokio::test]
async fn set_get_and_type() {
    let server = start_proto_server(false);
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
    let server = start_proto_server(false);
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
    let server = start_proto_server(false);
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
    let server = start_proto_server(false);
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
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let resp = c
        .cmd_raw(&[b"PROTO.REGISTER", b"bad", b"not a descriptor"])
        .await;
    assert!(matches!(resp, Frame::Error(_)));
}

#[tokio::test]
async fn set_with_nx_and_xx() {
    let server = start_proto_server(false);
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
    let server = start_proto_server(false);
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

// ---- PROTO.GETFIELD sharded tests ----

#[tokio::test]
async fn getfield_string() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let data = encode_message(&desc, "test.User", "name", "alice");
    c.cmd_raw(&[b"PROTO.SET", b"user:1", b"test.User", &data])
        .await;

    let resp = c.cmd(&["PROTO.GETFIELD", "user:1", "name"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("alice")));
}

#[tokio::test]
async fn getfield_missing_key() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let resp = c.cmd(&["PROTO.GETFIELD", "nonexistent", "name"]).await;
    assert!(matches!(resp, Frame::Null));
}

#[tokio::test]
async fn getfield_wrong_type() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    c.ok(&["SET", "str:key", "hello"]).await;

    let resp = c.cmd(&["PROTO.GETFIELD", "str:key", "name"]).await;
    assert!(matches!(resp, Frame::Error(ref s) if s.starts_with("WRONGTYPE")));
}

#[tokio::test]
async fn getfield_nonexistent_field() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let data = encode_message(&desc, "test.User", "name", "alice");
    c.cmd_raw(&[b"PROTO.SET", b"user:1", b"test.User", &data])
        .await;

    let resp = c.cmd(&["PROTO.GETFIELD", "user:1", "nonexistent"]).await;
    assert!(matches!(resp, Frame::Error(_)));
}

#[tokio::test]
async fn getfield_default_value() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    // encode a message with the field at its default (empty string)
    let data = encode_message(&desc, "test.User", "name", "");
    c.cmd_raw(&[b"PROTO.SET", b"user:1", b"test.User", &data])
        .await;

    let resp = c.cmd(&["PROTO.GETFIELD", "user:1", "name"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("")));
}

// ---- PROTO.SETFIELD sharded tests ----

#[tokio::test]
async fn setfield_string() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
        .await;

    let resp = c.cmd(&["PROTO.SETFIELD", "p:1", "name", "bob"]).await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));

    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "name"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("bob")));
}

#[tokio::test]
async fn setfield_integer() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
        .await;

    let resp = c.cmd(&["PROTO.SETFIELD", "p:1", "age", "30"]).await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));

    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "age"]).await;
    assert_eq!(resp, Frame::Integer(30));
}

#[tokio::test]
async fn setfield_bool() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
        .await;

    let resp = c.cmd(&["PROTO.SETFIELD", "p:1", "active", "false"]).await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));

    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "active"]).await;
    assert_eq!(resp, Frame::Integer(0));
}

#[tokio::test]
async fn setfield_missing_key() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let resp = c
        .cmd(&["PROTO.SETFIELD", "nonexistent", "name", "bob"])
        .await;
    assert!(matches!(resp, Frame::Null));
}

#[tokio::test]
async fn setfield_wrong_type() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    c.ok(&["SET", "str:key", "hello"]).await;

    let resp = c.cmd(&["PROTO.SETFIELD", "str:key", "name", "bob"]).await;
    assert!(matches!(resp, Frame::Error(ref s) if s.starts_with("WRONGTYPE")));
}

#[tokio::test]
async fn setfield_nonexistent_field() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
        .await;

    let resp = c
        .cmd(&["PROTO.SETFIELD", "p:1", "nonexistent", "value"])
        .await;
    assert!(matches!(resp, Frame::Error(_)));
}

#[tokio::test]
async fn setfield_invalid_value() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
        .await;

    // "abc" is not a valid int32
    let resp = c.cmd(&["PROTO.SETFIELD", "p:1", "age", "abc"]).await;
    assert!(matches!(resp, Frame::Error(_)));
}

// ---- PROTO.DELFIELD sharded tests ----

#[tokio::test]
async fn delfield_clears_field() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
        .await;

    let resp = c.cmd(&["PROTO.DELFIELD", "p:1", "name"]).await;
    assert_eq!(resp, Frame::Integer(1));

    // field should be reset to default (empty string)
    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "name"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("")));

    // other fields preserved
    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "age"]).await;
    assert_eq!(resp, Frame::Integer(25));
}

#[tokio::test]
async fn delfield_missing_key() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let resp = c.cmd(&["PROTO.DELFIELD", "nonexistent", "name"]).await;
    assert!(matches!(resp, Frame::Null));
}

#[tokio::test]
async fn delfield_returns_integer() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
        .await;

    let resp = c.cmd(&["PROTO.DELFIELD", "p:1", "name"]).await;
    assert_eq!(resp, Frame::Integer(1));
}

// ---- concurrent mode tests ----
// These mirror the core sharded tests to verify the concurrent handler's
// proto command routing through the engine fallback path.

#[tokio::test]
async fn concurrent_register_schema_and_describe() {
    let server = start_proto_server(true);
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");

    let resp = c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;
    match resp {
        Frame::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Frame::Bulk(Bytes::from("test.User")));
        }
        other => panic!("expected array, got {other:?}"),
    }

    let resp = c.cmd(&["PROTO.SCHEMAS"]).await;
    match resp {
        Frame::Array(items) => {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], Frame::Bulk(Bytes::from("users")));
        }
        other => panic!("expected array, got {other:?}"),
    }

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
async fn concurrent_set_get_and_type() {
    let server = start_proto_server(true);
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let data = encode_message(&desc, "test.User", "name", "alice");

    let resp = c
        .cmd_raw(&[b"PROTO.SET", b"user:1", b"test.User", &data])
        .await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));

    let resp = c.cmd(&["PROTO.GET", "user:1"]).await;
    match resp {
        Frame::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Frame::Bulk(Bytes::from("test.User")));
            assert_eq!(items[1], Frame::Bulk(Bytes::from(data)));
        }
        other => panic!("expected array, got {other:?}"),
    }

    let resp = c.cmd(&["PROTO.TYPE", "user:1"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("test.User")));
}

#[tokio::test]
async fn concurrent_invalid_proto_bytes_rejected() {
    let server = start_proto_server(true);
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let resp = c
        .cmd_raw(&[b"PROTO.SET", b"bad:1", b"test.User", b"not valid proto"])
        .await;
    assert!(matches!(resp, Frame::Error(_)));
}

#[tokio::test]
async fn concurrent_set_with_nx_and_xx() {
    let server = start_proto_server(true);
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
async fn concurrent_get_missing_key_returns_null() {
    let server = start_proto_server(true);
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let resp = c.cmd(&["PROTO.GET", "nonexistent"]).await;
    assert!(matches!(resp, Frame::Null));

    let resp = c.cmd(&["PROTO.TYPE", "nonexistent"]).await;
    assert!(matches!(resp, Frame::Null));
}

#[tokio::test]
async fn concurrent_getfield_string() {
    let server = start_proto_server(true);
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let data = encode_message(&desc, "test.User", "name", "alice");
    c.cmd_raw(&[b"PROTO.SET", b"user:1", b"test.User", &data])
        .await;

    let resp = c.cmd(&["PROTO.GETFIELD", "user:1", "name"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("alice")));
}

#[tokio::test]
async fn concurrent_setfield_string() {
    let server = start_proto_server(true);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
        .await;

    let resp = c.cmd(&["PROTO.SETFIELD", "p:1", "name", "bob"]).await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));

    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "name"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("bob")));
}

#[tokio::test]
async fn concurrent_delfield_clears_field() {
    let server = start_proto_server(true);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
        .await;

    let resp = c.cmd(&["PROTO.DELFIELD", "p:1", "name"]).await;
    assert_eq!(resp, Frame::Integer(1));

    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "name"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("")));
}
