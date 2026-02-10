//! Integration tests for snapshot and AOF persistence/recovery.

use std::time::Duration;

use ember_protocol::Frame;

use crate::helpers::{ServerOptions, TestServer};

#[tokio::test]
async fn bgsave_and_snapshot_recovery() {
    let data_dir = tempfile::tempdir().unwrap();
    let path = data_dir.path().to_path_buf();

    // start server, write keys, trigger snapshot
    {
        let server = TestServer::start_with(ServerOptions {
            appendonly: true,
            data_dir_path: Some(path.clone()),
            ..Default::default()
        });
        let mut c = server.connect().await;

        c.ok(&["SET", "snap:a", "alpha"]).await;
        c.ok(&["SET", "snap:b", "beta"]).await;

        let resp = c.cmd(&["BGSAVE"]).await;
        assert!(matches!(resp, Frame::Simple(ref s) if s.contains("saving started")));

        // give the snapshot time to flush to disk
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // server killed on drop, but data_dir still alive

    // restart with same data directory
    let server = TestServer::start_with(ServerOptions {
        appendonly: true,
        data_dir_path: Some(path),
        ..Default::default()
    });
    let mut c = server.connect().await;

    assert_eq!(c.get_bulk(&["GET", "snap:a"]).await, Some("alpha".into()));
    assert_eq!(c.get_bulk(&["GET", "snap:b"]).await, Some("beta".into()));

    // keep data_dir alive until assertions complete
    drop(data_dir);
}

#[tokio::test]
async fn aof_recovery() {
    let data_dir = tempfile::tempdir().unwrap();
    let path = data_dir.path().to_path_buf();

    // start server with AOF (fsync=always), write keys
    {
        let server = TestServer::start_with(ServerOptions {
            appendonly: true,
            data_dir_path: Some(path.clone()),
            ..Default::default()
        });
        let mut c = server.connect().await;

        c.ok(&["SET", "aof:x", "100"]).await;
        c.ok(&["SET", "aof:y", "200"]).await;
        c.get_int(&["INCR", "aof:x"]).await;

        // small sleep to ensure fsync completes
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // restart — AOF replay should restore state
    let server = TestServer::start_with(ServerOptions {
        appendonly: true,
        data_dir_path: Some(path),
        ..Default::default()
    });
    let mut c = server.connect().await;

    assert_eq!(c.get_bulk(&["GET", "aof:x"]).await, Some("101".into()));
    assert_eq!(c.get_bulk(&["GET", "aof:y"]).await, Some("200".into()));

    drop(data_dir);
}
