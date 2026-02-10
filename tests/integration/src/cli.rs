//! Integration tests for the emberkv-cli binary.

use crate::helpers::{run_cli, ServerOptions, TestServer};

/// Helper — start a plain (non-cluster) server.
fn plain_server() -> TestServer {
    TestServer::start()
}

/// Helper — start a cluster-enabled server.
fn cluster_server() -> TestServer {
    TestServer::start_with(ServerOptions {
        cluster_enabled: true,
        cluster_bootstrap: true,
        ..Default::default()
    })
}

// -- one-shot mode --

#[tokio::test]
async fn cli_oneshot_ping() {
    let server = plain_server();
    let output = run_cli(server.port, &["PING"]);

    assert!(output.status.success(), "exit code: {:?}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("PONG"),
        "expected PONG in stdout, got: {stdout}"
    );
}

#[tokio::test]
async fn cli_oneshot_set_get() {
    let server = plain_server();

    let set_out = run_cli(server.port, &["SET", "clikey", "clival"]);
    assert!(set_out.status.success());
    let stdout = String::from_utf8_lossy(&set_out.stdout);
    assert!(stdout.contains("OK"), "expected OK, got: {stdout}");

    let get_out = run_cli(server.port, &["GET", "clikey"]);
    assert!(get_out.status.success());
    let stdout = String::from_utf8_lossy(&get_out.stdout);
    assert!(
        stdout.contains("clival"),
        "expected clival in stdout, got: {stdout}"
    );
}

#[tokio::test]
async fn cli_oneshot_error() {
    let server = plain_server();
    let output = run_cli(server.port, &["NOTAREALCOMMAND"]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("unknown command")
            || combined.contains("error")
            || combined.contains("ERR"),
        "expected error output, got stdout={stdout} stderr={stderr}"
    );
}

// -- cluster subcommands --

#[tokio::test]
async fn cli_cluster_info() {
    let server = cluster_server();
    let output = run_cli(server.port, &["cluster", "info"]);

    assert!(output.status.success(), "exit code: {:?}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("cluster_state") || stdout.contains("cluster_slots"),
        "expected cluster info in stdout, got: {stdout}"
    );
}

#[tokio::test]
async fn cli_cluster_keyslot() {
    let server = cluster_server();
    let output = run_cli(server.port, &["cluster", "keyslot", "foo"]);

    assert!(output.status.success(), "exit code: {:?}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    // output should contain the integer slot number
    let trimmed = stdout.trim();
    assert!(
        trimmed.contains("integer") || trimmed.parse::<u64>().is_ok(),
        "expected integer slot output, got: {trimmed}"
    );
}

// -- benchmark --

#[tokio::test]
async fn cli_benchmark_smoke() {
    let server = plain_server();
    let output = run_cli(server.port, &["benchmark", "-n", "100", "-c", "2", "-q"]);

    assert!(
        output.status.success(),
        "benchmark failed: {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("rps"),
        "expected 'rps' in benchmark output, got: {stdout}"
    );
}

// -- error handling --

#[tokio::test]
async fn cli_connection_refused() {
    // connect to a port that nothing is listening on
    let output = run_cli(1, &["PING"]);

    assert!(
        !output.status.success(),
        "expected non-zero exit code for refused connection"
    );
}
