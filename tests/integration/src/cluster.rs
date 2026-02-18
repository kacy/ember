//! Integration tests for cluster command dispatch.

use ember_protocol::Frame;

use crate::helpers::{ServerOptions, TestCluster, TestServer};

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

// -- CLUSTER ADDSLOTSRANGE --

#[tokio::test]
async fn cluster_addslotsrange() {
    let server = cluster_server_empty();
    let mut c = server.connect().await;

    c.ok(&["CLUSTER", "ADDSLOTSRANGE", "0", "5460"]).await;

    let info = c.cmd(&["CLUSTER", "INFO"]).await;
    let text = match &info {
        Frame::Bulk(data) => String::from_utf8_lossy(data).to_string(),
        other => panic!("expected Bulk, got {other:?}"),
    };
    assert!(
        text.contains("cluster_slots_assigned:5461"),
        "expected 5461 slots, got: {text}"
    );
}

#[tokio::test]
async fn cluster_addslotsrange_multiple_ranges() {
    let server = cluster_server_empty();
    let mut c = server.connect().await;

    // two non-contiguous ranges: 0–100 (101 slots) + 200–299 (100 slots) = 201
    c.ok(&["CLUSTER", "ADDSLOTSRANGE", "0", "100", "200", "299"])
        .await;

    let info = c.cmd(&["CLUSTER", "INFO"]).await;
    let text = match &info {
        Frame::Bulk(data) => String::from_utf8_lossy(data).to_string(),
        other => panic!("expected Bulk, got {other:?}"),
    };
    assert!(
        text.contains("cluster_slots_assigned:201"),
        "expected 201 slots, got: {text}"
    );
}

#[tokio::test]
async fn cluster_addslotsrange_wrong_arity() {
    let server = cluster_server_empty();
    let mut c = server.connect().await;

    // a single slot arg — odd count, should fail
    let err = c.err(&["CLUSTER", "ADDSLOTSRANGE", "0"]).await;
    assert!(!err.is_empty(), "expected error for odd arg count");
}

#[tokio::test]
async fn cluster_addslotsrange_invalid_range() {
    let server = cluster_server_empty();
    let mut c = server.connect().await;

    // start > end
    let err = c.err(&["CLUSTER", "ADDSLOTSRANGE", "100", "50"]).await;
    assert!(
        err.contains("ERR") || err.contains("invalid"),
        "expected error for start > end, got: {err}"
    );
}

#[tokio::test]
async fn cluster_addslotsrange_out_of_bounds() {
    let server = cluster_server_empty();
    let mut c = server.connect().await;

    // slot 16384 is one past the valid maximum (0–16383)
    let err = c.err(&["CLUSTER", "ADDSLOTSRANGE", "0", "16384"]).await;
    assert!(
        err.contains("ERR") || err.contains("invalid"),
        "expected error for out-of-bounds slot, got: {err}"
    );
}

// -- multi-node tests --

#[tokio::test]
async fn cluster_meet_adds_peer() {
    let a = TestServer::start_with(ServerOptions {
        cluster_enabled: true,
        ..Default::default()
    });
    let b = TestServer::start_with(ServerOptions {
        cluster_enabled: true,
        ..Default::default()
    });

    let mut ca = a.connect().await;

    // introduce b to a
    ca.ok(&[
        "CLUSTER",
        "MEET",
        "127.0.0.1",
        &b.port.to_string(),
    ])
    .await;

    // node a should immediately see b in its local state
    let resp = ca.cmd(&["CLUSTER", "NODES"]).await;
    let text = match resp {
        Frame::Bulk(data) => String::from_utf8_lossy(&data).to_string(),
        other => panic!("expected Bulk, got {other:?}"),
    };
    assert_eq!(
        text.lines().count(),
        2,
        "expected 2 nodes after MEET, got:\n{text}"
    );
}

#[tokio::test]
async fn cluster_moved_redirect() {
    let cluster = TestCluster::start();
    cluster.init().await;

    // inspect what node 0 actually knows about the cluster
    let mut c0 = cluster.connect(0).await;
    let nodes_resp = c0.cmd(&["CLUSTER", "NODES"]).await;
    eprintln!("CLUSTER NODES on node 0:\n{nodes_resp:?}");
    let slots_resp = c0.cmd(&["CLUSTER", "SLOTS"]).await;
    eprintln!("CLUSTER SLOTS on node 0:\n{slots_resp:?}");

    // "foo" hashes to slot 12352, which is in node 2's range (10923–16383).
    // Sending SET to node 0 should yield a MOVED redirect.
    let resp = c0.cmd(&["SET", "foo", "bar"]).await;
    match resp {
        Frame::Error(msg) => {
            assert!(
                msg.starts_with("MOVED"),
                "expected MOVED redirect, got: {msg}"
            );
            // the redirect port must be one of our cluster nodes
            let redirected_port: u16 = msg
                .split(':')
                .last()
                .and_then(|p| p.parse().ok())
                .expect("MOVED should include a valid port");
            assert!(
                [cluster.port(0), cluster.port(1), cluster.port(2)].contains(&redirected_port),
                "MOVED points to unknown port {redirected_port}"
            );
        }
        other => panic!("expected MOVED error, got {other:?}"),
    }
}

#[tokio::test]
async fn cluster_redirect_followthrough() {
    let cluster = TestCluster::start();
    cluster.init().await;

    // use KEYSLOT to determine which node owns "testkey", then verify
    // that a non-owner returns MOVED and the owner accepts the write.
    let mut c0 = cluster.connect(0).await;
    let slot = c0.get_int(&["CLUSTER", "KEYSLOT", "testkey"]).await;

    let (owner_idx, other_idx) = if slot <= 5460 {
        (0usize, 1usize)
    } else if slot <= 10922 {
        (1, 0)
    } else {
        (2, 0)
    };

    // non-owner should refuse with MOVED
    let mut c_other = cluster.connect(other_idx).await;
    let resp = c_other.cmd(&["SET", "testkey", "value"]).await;
    assert!(
        matches!(resp, Frame::Error(ref msg) if msg.starts_with("MOVED")),
        "expected MOVED from non-owner (node {other_idx}), got {resp:?}"
    );

    // owner should accept and serve the key
    let mut c_owner = cluster.connect(owner_idx).await;
    c_owner.ok(&["SET", "testkey", "value"]).await;
    let val = c_owner.get_bulk(&["GET", "testkey"]).await;
    assert_eq!(val, Some("value".into()));
}
