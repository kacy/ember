//! Integration tests for cluster command dispatch.

use ember_protocol::Frame;

use crate::helpers::{ServerOptions, TestServer};

/// Starts a single-node cluster server (all 16384 slots assigned).
fn cluster_server() -> TestServer {
    TestServer::start_with(ServerOptions {
        cluster_enabled: true,
        cluster_bootstrap: true,
        ..Default::default()
    })
}

/// Starts a cluster server without bootstrapping (no slots assigned).
fn cluster_server_empty() -> TestServer {
    TestServer::start_with(ServerOptions {
        cluster_enabled: true,
        ..Default::default()
    })
}

// -- query commands --

#[tokio::test]
async fn cluster_info() {
    let server = cluster_server();
    let mut c = server.connect().await;

    let resp = c.cmd(&["CLUSTER", "INFO"]).await;
    match resp {
        Frame::Bulk(data) => {
            let text = String::from_utf8_lossy(&data);
            assert!(
                text.contains("cluster_state:ok"),
                "expected cluster_state:ok, got: {text}"
            );
            assert!(
                text.contains("cluster_slots_assigned:16384"),
                "expected 16384 slots assigned, got: {text}"
            );
        }
        other => panic!("expected Bulk, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_nodes() {
    let server = cluster_server();
    let mut c = server.connect().await;

    let resp = c.cmd(&["CLUSTER", "NODES"]).await;
    match resp {
        Frame::Bulk(data) => {
            let text = String::from_utf8_lossy(&data);
            assert!(
                text.contains("myself"),
                "node entry should contain 'myself'"
            );
            assert!(
                text.contains("master"),
                "node entry should contain 'master'"
            );
        }
        other => panic!("expected Bulk, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_slots() {
    let server = cluster_server();
    let mut c = server.connect().await;

    let resp = c.cmd(&["CLUSTER", "SLOTS"]).await;
    match resp {
        Frame::Array(ref entries) => {
            assert!(
                !entries.is_empty(),
                "bootstrapped cluster should have slot ranges"
            );
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_myid() {
    let server = cluster_server();
    let mut c = server.connect().await;

    let id = c
        .get_bulk(&["CLUSTER", "MYID"])
        .await
        .expect("MYID should return a bulk string");

    // UUID v4 format: 8-4-4-4-12 hex digits
    assert_eq!(id.len(), 36, "node ID should be a 36-char UUID, got: {id}");
    assert_eq!(id.matches('-').count(), 4, "UUID should have 4 hyphens");
}

#[tokio::test]
async fn cluster_keyslot() {
    let server = cluster_server();
    let mut c = server.connect().await;

    let slot = c.get_int(&["CLUSTER", "KEYSLOT", "foo"]).await;
    assert!((0..16384).contains(&slot), "slot out of range: {slot}");

    // same key should always hash to the same slot
    let slot2 = c.get_int(&["CLUSTER", "KEYSLOT", "foo"]).await;
    assert_eq!(slot, slot2);
}

// -- state-changing commands --

#[tokio::test]
async fn cluster_addslots() {
    let server = cluster_server_empty();
    let mut c = server.connect().await;

    // empty cluster should have 0 slots
    let info = c.cmd(&["CLUSTER", "INFO"]).await;
    let text = match &info {
        Frame::Bulk(data) => String::from_utf8_lossy(data).to_string(),
        other => panic!("expected Bulk, got {other:?}"),
    };
    assert!(text.contains("cluster_slots_assigned:0"), "got: {text}");

    // add a few slots
    c.ok(&["CLUSTER", "ADDSLOTS", "0", "1", "2"]).await;

    let info = c.cmd(&["CLUSTER", "INFO"]).await;
    let text = match &info {
        Frame::Bulk(data) => String::from_utf8_lossy(data).to_string(),
        other => panic!("expected Bulk, got {other:?}"),
    };
    assert!(text.contains("cluster_slots_assigned:3"), "got: {text}");
}

#[tokio::test]
async fn cluster_delslots() {
    let server = cluster_server_empty();
    let mut c = server.connect().await;

    c.ok(&["CLUSTER", "ADDSLOTS", "100", "101", "102"]).await;
    c.ok(&["CLUSTER", "DELSLOTS", "101"]).await;

    let info = c.cmd(&["CLUSTER", "INFO"]).await;
    let text = match &info {
        Frame::Bulk(data) => String::from_utf8_lossy(data).to_string(),
        other => panic!("expected Bulk, got {other:?}"),
    };
    assert!(text.contains("cluster_slots_assigned:2"), "got: {text}");
}

#[tokio::test]
async fn cluster_forget_unknown() {
    let server = cluster_server();
    let mut c = server.connect().await;

    let err = c
        .err(&["CLUSTER", "FORGET", "00000000-0000-0000-0000-000000000000"])
        .await;
    assert!(
        err.contains("Unknown node") || err.contains("unknown"),
        "expected unknown node error, got: {err}"
    );
}

// -- data routing --

#[tokio::test]
async fn cluster_set_get_owned_slot() {
    let server = cluster_server();
    let mut c = server.connect().await;

    // bootstrapped node owns all slots, so data commands should work
    c.ok(&["SET", "hello", "world"]).await;
    let val = c.get_bulk(&["GET", "hello"]).await;
    assert_eq!(val, Some("world".into()));
}

#[tokio::test]
async fn cluster_no_slots_returns_error() {
    let server = cluster_server_empty();
    let mut c = server.connect().await;

    let err = c.err(&["SET", "key", "val"]).await;
    assert!(
        err.contains("CLUSTERDOWN"),
        "expected CLUSTERDOWN error, got: {err}"
    );
}

// -- slot queries --

#[tokio::test]
async fn cluster_countkeysinslot() {
    let server = cluster_server();
    let mut c = server.connect().await;

    // figure out which slot "mykey" hashes to
    let slot = c.get_int(&["CLUSTER", "KEYSLOT", "mykey"]).await;

    c.ok(&["SET", "mykey", "value"]).await;
    let count = c
        .get_int(&["CLUSTER", "COUNTKEYSINSLOT", &slot.to_string()])
        .await;
    assert_eq!(count, 1);
}

#[tokio::test]
async fn cluster_getkeysinslot() {
    let server = cluster_server();
    let mut c = server.connect().await;

    let slot = c.get_int(&["CLUSTER", "KEYSLOT", "slotkey"]).await;
    c.ok(&["SET", "slotkey", "val"]).await;

    let resp = c
        .cmd(&["CLUSTER", "GETKEYSINSLOT", &slot.to_string(), "10"])
        .await;
    match resp {
        Frame::Array(keys) => {
            assert_eq!(keys.len(), 1);
            assert!(matches!(&keys[0], Frame::Bulk(b) if b == &b"slotkey"[..]));
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

// -- stubs / error responses --

#[tokio::test]
async fn cluster_replicate_stub() {
    let server = cluster_server();
    let mut c = server.connect().await;

    let err = c
        .err(&[
            "CLUSTER",
            "REPLICATE",
            "00000000-0000-0000-0000-000000000000",
        ])
        .await;
    assert!(
        err.contains("not yet supported"),
        "expected stub error, got: {err}"
    );
}

#[tokio::test]
async fn cluster_failover_stub() {
    let server = cluster_server();
    let mut c = server.connect().await;

    let err = c.err(&["CLUSTER", "FAILOVER"]).await;
    assert!(
        err.contains("not yet supported"),
        "expected stub error, got: {err}"
    );
}

#[tokio::test]
async fn cluster_migrate_missing_key() {
    let server = cluster_server();
    let mut c = server.connect().await;

    let err = c
        .err(&["MIGRATE", "127.0.0.1", "6380", "key", "0", "1000"])
        .await;
    assert!(
        err.contains("no such key"),
        "expected missing key error, got: {err}"
    );
}

#[tokio::test]
async fn cluster_setslot_importing() {
    let server = cluster_server();
    let mut c = server.connect().await;

    // importing from self should fail
    let my_id = c
        .get_bulk(&["CLUSTER", "MYID"])
        .await
        .expect("MYID should return a string");

    let err = c
        .err(&["CLUSTER", "SETSLOT", "0", "IMPORTING", &my_id])
        .await;
    assert!(
        err.contains("can't import from myself") || err.contains("ERR"),
        "expected error for self-import, got: {err}"
    );
}

#[tokio::test]
async fn cluster_meet_invalid() {
    let server = cluster_server();
    let mut c = server.connect().await;

    // invalid port
    let err = c.err(&["CLUSTER", "MEET", "127.0.0.1", "99999"]).await;
    assert!(!err.is_empty(), "expected error for invalid meet address");
}
