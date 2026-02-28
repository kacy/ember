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

// ---- TTL preservation tests ----

#[tokio::test]
async fn setfield_preserves_ttl() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[
        b"PROTO.SET",
        b"p:ttl",
        b"test.Profile",
        &data,
        b"EX",
        b"120",
    ])
    .await;

    // verify TTL is set
    let ttl_before = c.get_int(&["TTL", "p:ttl"]).await;
    assert!(ttl_before > 0 && ttl_before <= 120);

    // mutate a field
    c.cmd(&["PROTO.SETFIELD", "p:ttl", "name", "bob"]).await;

    // TTL should still be set
    let ttl_after = c.get_int(&["TTL", "p:ttl"]).await;
    assert!(ttl_after > 0 && ttl_after <= 120);
}

#[tokio::test]
async fn delfield_preserves_ttl() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[
        b"PROTO.SET",
        b"p:ttl2",
        b"test.Profile",
        &data,
        b"EX",
        b"120",
    ])
    .await;

    let ttl_before = c.get_int(&["TTL", "p:ttl2"]).await;
    assert!(ttl_before > 0 && ttl_before <= 120);

    c.cmd(&["PROTO.DELFIELD", "p:ttl2", "name"]).await;

    let ttl_after = c.get_int(&["TTL", "p:ttl2"]).await;
    assert!(ttl_after > 0 && ttl_after <= 120);
}

// ---- nested path integration tests ----

/// Builds a FileDescriptorSet with Inner { string value = 1; } and
/// Outer { Inner inner = 1; }.
fn make_nested_descriptor() -> Vec<u8> {
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("test.proto".into()),
            package: Some("test".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Inner".into()),
                    field: vec![FieldDescriptorProto {
                        name: Some("value".into()),
                        number: Some(1),
                        r#type: Some(9), // TYPE_STRING
                        label: Some(1),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Outer".into()),
                    field: vec![FieldDescriptorProto {
                        name: Some("inner".into()),
                        number: Some(1),
                        r#type: Some(11), // TYPE_MESSAGE
                        label: Some(1),
                        type_name: Some(".test.Inner".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
    };
    let mut buf = Vec::new();
    fds.encode(&mut buf).expect("encode nested descriptor");
    buf
}

/// Encodes an Outer message with the given inner value.
fn encode_outer(descriptor_bytes: &[u8], inner_value: &str) -> Vec<u8> {
    let pool = DescriptorPool::decode(descriptor_bytes).expect("decode pool");
    let inner_desc = pool.get_message_by_name("test.Inner").expect("find Inner");
    let outer_desc = pool.get_message_by_name("test.Outer").expect("find Outer");

    let mut inner = DynamicMessage::new(inner_desc);
    inner.set_field_by_name("value", prost_reflect::Value::String(inner_value.into()));

    let mut outer = DynamicMessage::new(outer_desc);
    outer.set_field_by_name("inner", prost_reflect::Value::Message(inner));

    let mut buf = Vec::new();
    outer.encode(&mut buf).expect("encode outer");
    buf
}

#[tokio::test]
async fn setfield_nested() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_nested_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"nested", &desc]).await;

    let data = encode_outer(&desc, "hello");
    c.cmd_raw(&[b"PROTO.SET", b"o:1", b"test.Outer", &data])
        .await;

    let resp = c
        .cmd(&["PROTO.SETFIELD", "o:1", "inner.value", "world"])
        .await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));

    let resp = c.cmd(&["PROTO.GETFIELD", "o:1", "inner.value"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("world")));
}

#[tokio::test]
async fn delfield_nested() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_nested_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"nested", &desc]).await;

    let data = encode_outer(&desc, "hello");
    c.cmd_raw(&[b"PROTO.SET", b"o:1", b"test.Outer", &data])
        .await;

    let resp = c.cmd(&["PROTO.DELFIELD", "o:1", "inner.value"]).await;
    assert_eq!(resp, Frame::Integer(1));

    // cleared field should return default (empty string)
    let resp = c.cmd(&["PROTO.GETFIELD", "o:1", "inner.value"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("")));
}

#[tokio::test]
async fn duplicate_registration_returns_error() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    // second registration with same name should fail
    let resp = c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;
    assert!(matches!(resp, Frame::Error(ref s) if s.contains("already registered")));
}

// ---- schema recovery tests ----

#[tokio::test]
async fn schema_recovery_after_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let path = data_dir.path().to_path_buf();

    let desc = make_multi_field_descriptor();
    let data = encode_profile(&desc, "alice", 25, true);

    // start server, register schema, set proto value
    {
        let server = TestServer::start_with(ServerOptions {
            protobuf: true,
            appendonly: true,
            data_dir_path: Some(path.clone()),
            ..Default::default()
        });
        let mut c = server.connect().await;

        c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;
        c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
            .await;

        // wait for fsync
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    // restart — schemas should be recovered from AOF
    let server = TestServer::start_with(ServerOptions {
        protobuf: true,
        appendonly: true,
        data_dir_path: Some(path),
        ..Default::default()
    });
    let mut c = server.connect().await;

    // GETFIELD requires the schema to be loaded — this proves recovery works
    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "name"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("alice")));

    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "age"]).await;
    assert_eq!(resp, Frame::Integer(25));

    drop(data_dir);
}

#[tokio::test]
async fn schema_survives_aof_rewrite() {
    let data_dir = tempfile::tempdir().unwrap();
    let path = data_dir.path().to_path_buf();

    let desc = make_multi_field_descriptor();
    let data = encode_profile(&desc, "alice", 30, true);

    // start server, register, set, then trigger AOF rewrite
    {
        let server = TestServer::start_with(ServerOptions {
            protobuf: true,
            appendonly: true,
            data_dir_path: Some(path.clone()),
            ..Default::default()
        });
        let mut c = server.connect().await;

        c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;
        c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
            .await;

        // trigger AOF rewrite — this truncates the AOF and writes a snapshot
        let resp = c.cmd(&["BGREWRITEAOF"]).await;
        assert!(matches!(resp, Frame::Simple(_)));
        // give the rewrite time to complete
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    // restart — schema must survive the rewrite
    let server = TestServer::start_with(ServerOptions {
        protobuf: true,
        appendonly: true,
        data_dir_path: Some(path),
        ..Default::default()
    });
    let mut c = server.connect().await;

    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "name"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("alice")));

    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "age"]).await;
    assert_eq!(resp, Frame::Integer(30));

    drop(data_dir);
}

#[tokio::test]
async fn del_removes_proto_key() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_descriptor("test", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &desc]).await;

    let data = encode_message(&desc, "test.User", "name", "alice");
    c.cmd_raw(&[b"PROTO.SET", b"user:del", b"test.User", &data])
        .await;

    // DEL should remove the proto key
    let resp = c.get_int(&["DEL", "user:del"]).await;
    assert_eq!(resp, 1);

    // PROTO.GET should now return null
    let resp = c.cmd(&["PROTO.GET", "user:del"]).await;
    assert!(matches!(resp, Frame::Null));
}

// ---- concurrent mode nested path and misc tests ----
// note: TTL preservation tests are sharded-only because concurrent mode
// routes proto commands through engine shards while TTL checks the
// concurrent keyspace — these are separate stores.

#[tokio::test]
async fn concurrent_setfield_nested() {
    let server = start_proto_server(true);
    let mut c = server.connect().await;

    let desc = make_nested_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"nested", &desc]).await;

    let data = encode_outer(&desc, "hello");
    c.cmd_raw(&[b"PROTO.SET", b"o:1", b"test.Outer", &data])
        .await;

    let resp = c
        .cmd(&["PROTO.SETFIELD", "o:1", "inner.value", "world"])
        .await;
    assert!(matches!(resp, Frame::Simple(ref s) if s == "OK"));

    let resp = c.cmd(&["PROTO.GETFIELD", "o:1", "inner.value"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("world")));
}

#[tokio::test]
async fn concurrent_delfield_nested() {
    let server = start_proto_server(true);
    let mut c = server.connect().await;

    let desc = make_nested_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"nested", &desc]).await;

    let data = encode_outer(&desc, "hello");
    c.cmd_raw(&[b"PROTO.SET", b"o:1", b"test.Outer", &data])
        .await;

    let resp = c.cmd(&["PROTO.DELFIELD", "o:1", "inner.value"]).await;
    assert_eq!(resp, Frame::Integer(1));

    let resp = c.cmd(&["PROTO.GETFIELD", "o:1", "inner.value"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("")));
}

// ---- concurrent mode recovery and DEL tests ----

#[tokio::test]
async fn concurrent_schema_recovery_after_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let path = data_dir.path().to_path_buf();

    let desc = make_multi_field_descriptor();
    let data = encode_profile(&desc, "bob", 42, false);

    {
        let server = TestServer::start_with(ServerOptions {
            protobuf: true,
            concurrent: true,
            appendonly: true,
            data_dir_path: Some(path.clone()),
            ..Default::default()
        });
        let mut c = server.connect().await;

        c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;
        c.cmd_raw(&[b"PROTO.SET", b"p:1", b"test.Profile", &data])
            .await;

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }

    let server = TestServer::start_with(ServerOptions {
        protobuf: true,
        concurrent: true,
        appendonly: true,
        data_dir_path: Some(path),
        ..Default::default()
    });
    let mut c = server.connect().await;

    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "name"]).await;
    assert_eq!(resp, Frame::Bulk(Bytes::from("bob")));

    let resp = c.cmd(&["PROTO.GETFIELD", "p:1", "age"]).await;
    assert_eq!(resp, Frame::Integer(42));

    drop(data_dir);
}

// ---- PROTO.SCAN tests ----

/// Helper: decode a PROTO.SCAN / PROTO.FIND response into (next_cursor, keys).
fn decode_scan_response(frame: Frame) -> (u64, Vec<String>) {
    match frame {
        Frame::Array(items) if items.len() == 2 => {
            let cursor_str = match &items[0] {
                Frame::Bulk(b) => std::str::from_utf8(b).unwrap().to_owned(),
                Frame::Simple(s) => s.clone(),
                other => panic!("expected cursor bulk, got {other:?}"),
            };
            let cursor: u64 = cursor_str.parse().expect("cursor is u64");
            let keys = match &items[1] {
                Frame::Array(ks) => ks
                    .iter()
                    .map(|k| match k {
                        Frame::Bulk(b) => std::str::from_utf8(b).unwrap().to_owned(),
                        other => panic!("expected key bulk, got {other:?}"),
                    })
                    .collect(),
                other => panic!("expected key array, got {other:?}"),
            };
            (cursor, keys)
        }
        other => panic!("expected [cursor, [keys]], got {other:?}"),
    }
}

/// PROTO.SCAN with no arguments returns all proto keys across all pages.
#[tokio::test]
async fn proto_scan_all_keys() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    // store 5 proto keys
    for i in 1..=5u32 {
        let data = encode_profile(&desc, &format!("user{i}"), i as i32, true);
        c.cmd_raw(&[
            b"PROTO.SET",
            format!("user:{i}").as_bytes(),
            b"test.Profile",
            &data,
        ])
        .await;
    }

    // also store a non-proto key to ensure it's excluded
    c.cmd(&["SET", "plain:1", "hello"]).await;

    // collect all keys via cursor iteration
    let mut all_keys = Vec::new();
    let mut cursor = 0u64;
    loop {
        let resp = c.cmd(&["PROTO.SCAN", &cursor.to_string()]).await;
        let (next, keys) = decode_scan_response(resp);
        all_keys.extend(keys);
        if next == 0 {
            break;
        }
        cursor = next;
    }

    all_keys.sort();
    assert_eq!(all_keys.len(), 5);
    for i in 1..=5u32 {
        assert!(all_keys.contains(&format!("user:{i}")));
    }
    // plain key must not appear
    assert!(!all_keys.contains(&"plain:1".to_owned()));
}

/// TYPE filter restricts PROTO.SCAN to keys of a specific message type.
#[tokio::test]
async fn proto_scan_type_filter() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    // register two different message types
    let profile_desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &profile_desc])
        .await;

    let user_desc = make_descriptor("users", "User", "username");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &user_desc]).await;

    // store one of each
    let pdata = encode_profile(&profile_desc, "alice", 30, true);
    c.cmd_raw(&[b"PROTO.SET", b"profile:1", b"test.Profile", &pdata])
        .await;

    let udata = encode_message(&user_desc, "users.User", "username", "alice");
    c.cmd_raw(&[b"PROTO.SET", b"user:1", b"users.User", &udata])
        .await;

    // scan with TYPE=test.Profile — should only return profile:1
    let resp = c.cmd(&["PROTO.SCAN", "0", "TYPE", "test.Profile"]).await;
    let (_, keys) = decode_scan_response(resp);
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], "profile:1");

    // scan with TYPE=users.User — should only return user:1
    let resp = c.cmd(&["PROTO.SCAN", "0", "TYPE", "users.User"]).await;
    let (_, keys) = decode_scan_response(resp);
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], "user:1");
}

/// MATCH pattern narrows PROTO.SCAN results to matching key names.
#[tokio::test]
async fn proto_scan_match_pattern() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    for i in 1..=3u32 {
        let data = encode_profile(&desc, "x", i as i32, false);
        c.cmd_raw(&[
            b"PROTO.SET",
            format!("profile:{i}").as_bytes(),
            b"test.Profile",
            &data,
        ])
        .await;
        let data = encode_profile(&desc, "y", i as i32, false);
        c.cmd_raw(&[
            b"PROTO.SET",
            format!("other:{i}").as_bytes(),
            b"test.Profile",
            &data,
        ])
        .await;
    }

    let mut matched = Vec::new();
    let mut cursor = 0u64;
    loop {
        let resp = c
            .cmd(&["PROTO.SCAN", &cursor.to_string(), "MATCH", "profile:*"])
            .await;
        let (next, keys) = decode_scan_response(resp);
        matched.extend(keys);
        if next == 0 {
            break;
        }
        cursor = next;
    }

    assert_eq!(matched.len(), 3);
    for k in &matched {
        assert!(k.starts_with("profile:"), "unexpected key {k}");
    }
}

/// Cursor iteration is stable — adding keys mid-scan doesn't cause duplicates or panics.
#[tokio::test]
async fn proto_scan_cursor_consistency() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    for i in 1..=10u32 {
        let data = encode_profile(&desc, "x", i as i32, true);
        c.cmd_raw(&[
            b"PROTO.SET",
            format!("p:{i}").as_bytes(),
            b"test.Profile",
            &data,
        ])
        .await;
    }

    // first page with COUNT 3
    let resp = c.cmd(&["PROTO.SCAN", "0", "COUNT", "3"]).await;
    let (cursor, first_page) = decode_scan_response(resp);
    assert!(!first_page.is_empty());

    // add more keys while iterating
    for i in 11..=15u32 {
        let data = encode_profile(&desc, "y", i as i32, false);
        c.cmd_raw(&[
            b"PROTO.SET",
            format!("p:{i}").as_bytes(),
            b"test.Profile",
            &data,
        ])
        .await;
    }

    // continue iterating — must not panic or crash
    if cursor != 0 {
        let resp = c
            .cmd(&["PROTO.SCAN", &cursor.to_string(), "COUNT", "3"])
            .await;
        let (_, _) = decode_scan_response(resp);
    }
}

// ---- PROTO.FIND tests ----

/// PROTO.FIND locates keys by scalar field value.
#[tokio::test]
async fn proto_find_scalar_match() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    // store three profiles with different active values
    let active_data = encode_profile(&desc, "alice", 25, true);
    c.cmd_raw(&[
        b"PROTO.SET",
        b"profile:alice",
        b"test.Profile",
        &active_data,
    ])
    .await;

    let inactive_data = encode_profile(&desc, "bob", 30, false);
    c.cmd_raw(&[
        b"PROTO.SET",
        b"profile:bob",
        b"test.Profile",
        &inactive_data,
    ])
    .await;

    let active2_data = encode_profile(&desc, "carol", 22, true);
    c.cmd_raw(&[
        b"PROTO.SET",
        b"profile:carol",
        b"test.Profile",
        &active2_data,
    ])
    .await;

    // find by bool field
    let mut found = Vec::new();
    let mut cursor = 0u64;
    loop {
        let resp = c
            .cmd(&["PROTO.FIND", &cursor.to_string(), "active", "true"])
            .await;
        let (next, keys) = decode_scan_response(resp);
        found.extend(keys);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    found.sort();
    assert_eq!(found.len(), 2);
    assert!(found.contains(&"profile:alice".to_owned()));
    assert!(found.contains(&"profile:carol".to_owned()));

    // find by int field
    let resp = c.cmd(&["PROTO.FIND", "0", "age", "30"]).await;
    let (_, keys) = decode_scan_response(resp);
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], "profile:bob");

    // find by string field
    let resp = c.cmd(&["PROTO.FIND", "0", "name", "alice"]).await;
    let (_, keys) = decode_scan_response(resp);
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], "profile:alice");
}

/// PROTO.FIND with a dot-separated path searches nested message fields.
#[tokio::test]
async fn proto_find_nested_path() {
    use prost_reflect::prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };

    // build a descriptor with a nested Address.city field
    let fds = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("nested.proto".into()),
            package: Some("nested".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Address".into()),
                    field: vec![FieldDescriptorProto {
                        name: Some("city".into()),
                        number: Some(1),
                        r#type: Some(9), // TYPE_STRING
                        label: Some(1),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Person".into()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("name".into()),
                            number: Some(1),
                            r#type: Some(9), // TYPE_STRING
                            label: Some(1),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("address".into()),
                            number: Some(2),
                            r#type: Some(11), // TYPE_MESSAGE
                            label: Some(1),
                            type_name: Some(".nested.Address".into()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
    };
    let mut desc_bytes = Vec::new();
    fds.encode(&mut desc_bytes).expect("encode descriptor");

    let pool = DescriptorPool::decode(desc_bytes.as_slice()).expect("decode pool");
    let person_desc = pool
        .get_message_by_name("nested.Person")
        .expect("find message");

    let encode_person = |name: &str, city: &str| {
        let addr_desc = pool
            .get_message_by_name("nested.Address")
            .expect("find address");
        let mut addr = DynamicMessage::new(addr_desc);
        addr.set_field_by_name("city", prost_reflect::Value::String(city.into()));

        let mut person = DynamicMessage::new(person_desc.clone());
        person.set_field_by_name("name", prost_reflect::Value::String(name.into()));
        person.set_field_by_name("address", prost_reflect::Value::Message(addr));
        let mut buf = Vec::new();
        person.encode(&mut buf).expect("encode person");
        buf
    };

    let server = start_proto_server(false);
    let mut c = server.connect().await;

    c.cmd_raw(&[b"PROTO.REGISTER", b"nested_schema", &desc_bytes])
        .await;

    let alice_data = encode_person("alice", "Seattle");
    c.cmd_raw(&[b"PROTO.SET", b"person:alice", b"nested.Person", &alice_data])
        .await;

    let bob_data = encode_person("bob", "Portland");
    c.cmd_raw(&[b"PROTO.SET", b"person:bob", b"nested.Person", &bob_data])
        .await;

    let carol_data = encode_person("carol", "Seattle");
    c.cmd_raw(&[b"PROTO.SET", b"person:carol", b"nested.Person", &carol_data])
        .await;

    // find by nested address.city
    let mut found = Vec::new();
    let mut cursor = 0u64;
    loop {
        let resp = c
            .cmd(&["PROTO.FIND", &cursor.to_string(), "address.city", "Seattle"])
            .await;
        let (next, keys) = decode_scan_response(resp);
        found.extend(keys);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    found.sort();
    assert_eq!(found.len(), 2);
    assert!(found.contains(&"person:alice".to_owned()));
    assert!(found.contains(&"person:carol".to_owned()));
}

/// PROTO.FIND combined with TYPE filter only inspects keys of the given type.
#[tokio::test]
async fn proto_find_type_filter() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let profile_desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &profile_desc])
        .await;

    let user_desc = make_descriptor("users", "User", "name");
    c.cmd_raw(&[b"PROTO.REGISTER", b"users", &user_desc]).await;

    // profiles with age=25
    let p1 = encode_profile(&profile_desc, "alice", 25, true);
    c.cmd_raw(&[b"PROTO.SET", b"profile:1", b"test.Profile", &p1])
        .await;

    // user key with field "name" (not "age")
    let u1 = encode_message(&user_desc, "users.User", "name", "alice");
    c.cmd_raw(&[b"PROTO.SET", b"user:1", b"users.User", &u1])
        .await;

    // PROTO.FIND age=25 TYPE=test.Profile — should find profile:1 but not crash on user:1
    let resp = c
        .cmd(&["PROTO.FIND", "0", "age", "25", "TYPE", "test.Profile"])
        .await;
    let (_, keys) = decode_scan_response(resp);
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], "profile:1");
}

/// PROTO.FIND returns cursor 0 and empty array when no keys match.
#[tokio::test]
async fn proto_find_no_match() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    let data = encode_profile(&desc, "alice", 25, false);
    c.cmd_raw(&[b"PROTO.SET", b"profile:1", b"test.Profile", &data])
        .await;

    let resp = c.cmd(&["PROTO.FIND", "0", "active", "true"]).await;
    let (cursor, keys) = decode_scan_response(resp);
    assert_eq!(cursor, 0);
    assert!(keys.is_empty());
}

/// PROTO.FIND with COUNT paginates correctly — all pages together return the full match set.
#[tokio::test]
async fn proto_find_count_pagination() {
    let server = start_proto_server(false);
    let mut c = server.connect().await;

    let desc = make_multi_field_descriptor();
    c.cmd_raw(&[b"PROTO.REGISTER", b"profiles", &desc]).await;

    // 6 active profiles
    for i in 1..=6u32 {
        let data = encode_profile(&desc, &format!("user{i}"), i as i32, true);
        c.cmd_raw(&[
            b"PROTO.SET",
            format!("profile:{i}").as_bytes(),
            b"test.Profile",
            &data,
        ])
        .await;
    }
    // 2 inactive profiles
    for i in 7..=8u32 {
        let data = encode_profile(&desc, &format!("user{i}"), i as i32, false);
        c.cmd_raw(&[
            b"PROTO.SET",
            format!("profile:{i}").as_bytes(),
            b"test.Profile",
            &data,
        ])
        .await;
    }

    let mut all_found = Vec::new();
    let mut cursor = 0u64;
    loop {
        let resp = c
            .cmd(&[
                "PROTO.FIND",
                &cursor.to_string(),
                "active",
                "true",
                "COUNT",
                "2",
            ])
            .await;
        let (next, keys) = decode_scan_response(resp);
        all_found.extend(keys);
        if next == 0 {
            break;
        }
        cursor = next;
    }

    assert_eq!(all_found.len(), 6);
    for i in 1..=6u32 {
        assert!(all_found.contains(&format!("profile:{i}")));
    }
}
