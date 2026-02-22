//! Test helpers for spawning an ember-server and sending commands.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use ember_protocol::{parse_frame, Frame};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// An ember-server subprocess managed by the test harness.
pub struct TestServer {
    child: Child,
    pub port: u16,
    _data_dir: Option<tempfile::TempDir>,
}

/// Options for starting a test server.
#[derive(Default)]
pub struct ServerOptions {
    pub requirepass: Option<String>,
    pub appendonly: bool,
    /// Owned temp directory (cleaned up when the server drops).
    pub data_dir: Option<tempfile::TempDir>,
    /// Use an existing path without taking ownership.
    /// If both `data_dir` and `data_dir_path` are set, `data_dir_path` wins.
    pub data_dir_path: Option<PathBuf>,
    /// Start with cluster support enabled.
    pub cluster_enabled: bool,
    /// Bootstrap as a single-node cluster owning all 16384 slots.
    pub cluster_bootstrap: bool,
    /// Enable protobuf value storage.
    pub protobuf: bool,
    /// Number of shards (defaults to 2 for test coverage).
    pub shards: Option<usize>,
    /// Use concurrent (DashMap) mode instead of sharded channels.
    pub concurrent: bool,
}

impl TestServer {
    /// Starts a new ember-server on a random port.
    ///
    /// Blocks until the server is accepting connections (up to 5 seconds).
    pub fn start() -> Self {
        Self::start_with(ServerOptions::default())
    }

    /// Starts a new ember-server with custom options.
    pub fn start_with(opts: ServerOptions) -> Self {
        let binary = server_binary();

        let port = find_free_port();

        let mut cmd = Command::new(&binary);
        cmd.arg("--port").arg(port.to_string());
        cmd.arg("--host").arg("127.0.0.1");
        cmd.arg("--shards")
            .arg(opts.shards.unwrap_or(2).to_string());
        // use info for cluster tests so we can see gossip startup
        cmd.env("RUST_LOG", "error");

        if let Some(ref pass) = opts.requirepass {
            cmd.arg("--requirepass").arg(pass);
        }

        if opts.protobuf {
            cmd.arg("--protobuf");
        }

        if opts.concurrent {
            cmd.arg("--concurrent");
        }

        if opts.cluster_enabled {
            cmd.arg("--cluster-enabled");
            // use small offsets so gossip and raft ports stay in valid u16 range.
            // random test ports are often >55000, and the defaults (+10000/+10001)
            // would overflow past 65535.
            cmd.arg("--cluster-port-offset").arg("1");
            cmd.arg("--cluster-raft-port-offset").arg("2");
        }
        if opts.cluster_bootstrap {
            cmd.arg("--cluster-bootstrap");
        }

        let data_dir = if opts.appendonly || opts.cluster_enabled {
            if opts.appendonly {
                cmd.arg("--appendonly");
                cmd.arg("--appendfsync").arg("always");
            }

            if let Some(ref path) = opts.data_dir_path {
                cmd.arg("--data-dir").arg(path);
                None // caller manages the directory lifetime
            } else {
                let dir = opts
                    .data_dir
                    .unwrap_or_else(|| tempfile::tempdir().unwrap());
                cmd.arg("--data-dir").arg(dir.path());
                Some(dir)
            }
        } else {
            None
        };

        let child = cmd
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap_or_else(|e| {
                panic!("failed to spawn ember-server at {}: {e}", binary.display())
            });

        // wait for the server to accept TCP connections
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if std::time::Instant::now() > deadline {
                panic!("ember-server failed to start within 5 seconds on port {port}");
            }
            if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        let server = Self {
            child,
            port,
            _data_dir: data_dir,
        };

        // for bootstrapped cluster nodes, wait until raft reconciliation
        // has applied and the cluster reports all 16384 slots assigned
        if opts.cluster_bootstrap {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                if std::time::Instant::now() > deadline {
                    panic!("bootstrapped cluster failed to become ready on port {port}");
                }
                if Self::check_cluster_ready_sync(port) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        server
    }

    /// Checks if a bootstrapped cluster node is ready by sending CLUSTER INFO
    /// over a blocking TCP connection and looking for `cluster_state:ok`.
    fn check_cluster_ready_sync(port: u16) -> bool {
        use std::io::{Read, Write};

        let Ok(mut stream) = std::net::TcpStream::connect(format!("127.0.0.1:{port}")) else {
            return false;
        };
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .ok();

        let cmd = b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n";
        if stream.write_all(cmd).is_err() {
            return false;
        }

        let mut buf = vec![0u8; 4096];
        match stream.read(&mut buf) {
            Ok(n) if n > 0 => {
                let response = String::from_utf8_lossy(&buf[..n]);
                response.contains("cluster_state:ok")
            }
            _ => false,
        }
    }

    /// Connects a test client to this server.
    pub async fn connect(&self) -> TestClient {
        TestClient::connect(self.port).await
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A minimal RESP3 client for integration testing.
pub struct TestClient {
    stream: TcpStream,
    buf: BytesMut,
}

impl TestClient {
    async fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap_or_else(|e| panic!("failed to connect to 127.0.0.1:{port}: {e}"));
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
        }
    }

    /// Sends a command with raw byte arguments and returns the parsed response.
    /// Useful for binary data like protobuf descriptors.
    #[allow(dead_code)]
    pub async fn cmd_raw(&mut self, args: &[&[u8]]) -> Frame {
        let parts: Vec<Frame> = args
            .iter()
            .map(|a| Frame::Bulk(Bytes::copy_from_slice(a)))
            .collect();
        let frame = Frame::Array(parts);

        let mut out = BytesMut::new();
        frame.serialize(&mut out);
        self.stream.write_all(&out).await.unwrap();

        loop {
            match parse_frame(&self.buf) {
                Ok(Some((frame, consumed))) => {
                    let _ = self.buf.split_to(consumed);
                    return frame;
                }
                Ok(None) => {
                    let n = self.stream.read_buf(&mut self.buf).await.unwrap();
                    if n == 0 {
                        panic!("server closed connection while waiting for response");
                    }
                }
                Err(e) => panic!("protocol error: {e}"),
            }
        }
    }

    /// Sends a command and returns the parsed response frame.
    pub async fn cmd(&mut self, args: &[&str]) -> Frame {
        // build RESP3 array
        let parts: Vec<Frame> = args
            .iter()
            .map(|a| Frame::Bulk(Bytes::copy_from_slice(a.as_bytes())))
            .collect();
        let frame = Frame::Array(parts);

        let mut out = BytesMut::new();
        frame.serialize(&mut out);
        self.stream.write_all(&out).await.unwrap();

        // read response
        loop {
            match parse_frame(&self.buf) {
                Ok(Some((frame, consumed))) => {
                    let _ = self.buf.split_to(consumed);
                    return frame;
                }
                Ok(None) => {
                    let n = self.stream.read_buf(&mut self.buf).await.unwrap();
                    if n == 0 {
                        panic!("server closed connection while waiting for response");
                    }
                }
                Err(e) => panic!("protocol error: {e}"),
            }
        }
    }

    /// Sends a command and extracts the bulk string value.
    pub async fn get_bulk(&mut self, args: &[&str]) -> Option<String> {
        match self.cmd(args).await {
            Frame::Bulk(data) => Some(String::from_utf8_lossy(&data).to_string()),
            Frame::Null => None,
            other => panic!("expected Bulk or Null, got {other:?}"),
        }
    }

    /// Sends a command and extracts the integer value.
    pub async fn get_int(&mut self, args: &[&str]) -> i64 {
        match self.cmd(args).await {
            Frame::Integer(n) => n,
            other => panic!("expected Integer, got {other:?}"),
        }
    }

    /// Sends a command and expects a Simple "OK" response.
    pub async fn ok(&mut self, args: &[&str]) {
        match self.cmd(args).await {
            Frame::Simple(s) if s == "OK" => {}
            other => panic!("expected OK, got {other:?}"),
        }
    }

    /// Sends a command and expects an error response. Returns the error message.
    pub async fn err(&mut self, args: &[&str]) -> String {
        match self.cmd(args).await {
            Frame::Error(msg) => msg,
            other => panic!("expected Error, got {other:?}"),
        }
    }

    /// Reads the next frame from the connection without sending a command.
    /// Useful for pub/sub where the server pushes messages asynchronously.
    pub async fn read_frame(&mut self) -> Frame {
        loop {
            match parse_frame(&self.buf) {
                Ok(Some((frame, consumed))) => {
                    let _ = self.buf.split_to(consumed);
                    return frame;
                }
                Ok(None) => {
                    let n = self.stream.read_buf(&mut self.buf).await.unwrap();
                    if n == 0 {
                        panic!("server closed connection while waiting for frame");
                    }
                }
                Err(e) => panic!("protocol error: {e}"),
            }
        }
    }
}

/// Finds a free TCP port by binding to port 0.
fn find_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Locates a binary in the cargo target directory.
fn find_binary(name: &str) -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    // test binary is in target/debug/deps/ — go up to target/debug/
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    path.push(name);
    if !path.exists() {
        panic!(
            "{name} binary not found. run `cargo build` first.\nlooked at: {}",
            path.display()
        );
    }
    path
}

/// Locates the ember-server binary in the cargo target directory.
fn server_binary() -> PathBuf {
    find_binary("ember-server")
}

/// Locates the ember-cli binary in the cargo target directory.
pub fn cli_binary() -> PathBuf {
    find_binary("ember-cli")
}

/// A 3-node local cluster for multi-node integration tests.
///
/// Slot distribution after `init()`:
/// - node 0: slots 0–5460
/// - node 1: slots 5461–10922
/// - node 2: slots 10923–16383
pub struct TestCluster {
    pub nodes: [TestServer; 3],
}

impl TestCluster {
    /// Starts 3 cluster-enabled nodes. Call `init().await` next to form
    /// the cluster and assign slots.
    pub fn start() -> Self {
        let make = || {
            TestServer::start_with(ServerOptions {
                cluster_enabled: true,
                ..Default::default()
            })
        };
        Self {
            nodes: [make(), make(), make()],
        }
    }

    /// Issues CLUSTER MEET and CLUSTER ADDSLOTSRANGE to form a working
    /// 3-node cluster, then sleeps long enough for gossip to propagate
    /// slot assignments to all nodes.
    pub async fn init(&self) {
        let mut c0 = self.connect(0).await;
        let mut c1 = self.connect(1).await;
        let mut c2 = self.connect(2).await;

        // introduce node 1 and 2 to node 0
        c0.ok(&[
            "CLUSTER",
            "MEET",
            "127.0.0.1",
            &self.nodes[1].port.to_string(),
        ])
        .await;
        c0.ok(&[
            "CLUSTER",
            "MEET",
            "127.0.0.1",
            &self.nodes[2].port.to_string(),
        ])
        .await;

        // assign slots via ADDSLOTSRANGE
        c0.ok(&["CLUSTER", "ADDSLOTSRANGE", "0", "5460"]).await;
        c1.ok(&["CLUSTER", "ADDSLOTSRANGE", "5461", "10922"]).await;
        c2.ok(&["CLUSTER", "ADDSLOTSRANGE", "10923", "16383"]).await;

        // wait for gossip to deliver slot assignments across all nodes;
        // the protocol period is 1 second so we allow two full periods
        tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    }

    /// Connects a test client to the node at `idx` (0–2).
    pub async fn connect(&self, idx: usize) -> TestClient {
        self.nodes[idx].connect().await
    }

    /// Returns the data port for node `idx`.
    pub fn port(&self, idx: usize) -> u16 {
        self.nodes[idx].port
    }
}

/// Runs the CLI binary with the given args and captures output.
pub fn run_cli(port: u16, args: &[&str]) -> std::process::Output {
    Command::new(cli_binary())
        .arg("--port")
        .arg(port.to_string())
        .arg("--host")
        .arg("127.0.0.1")
        .args(args)
        .output()
        .expect("failed to run emberkv-cli")
}
