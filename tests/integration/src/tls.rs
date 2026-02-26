//! TLS integration tests.
//!
//! Verifies that the server accepts TLS connections and processes commands
//! correctly over an encrypted transport. Uses a self-signed certificate
//! generated at test time via `rcgen`.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use ember_protocol::{parse_frame, Frame};
use rustls::pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsConnector;

use crate::helpers::{ServerOptions, TestServer};

/// Generates a self-signed cert/key pair for `localhost` and writes PEM files
/// into the given directory. Returns the cert path, key path, and the raw DER
/// bytes needed to build a client-side trust store.
fn generate_test_cert(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf, Vec<u8>) {
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("rcgen cert generation failed");

    let cert_der = cert.der().to_vec();
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    std::fs::write(&cert_path, cert_pem).expect("failed to write cert");
    std::fs::write(&key_path, key_pem).expect("failed to write key");

    (cert_path, key_path, cert_der)
}

/// Builds a rustls ClientConfig that trusts only the given DER certificate.
///
/// This simulates a client that has been explicitly configured to trust the
/// server's self-signed cert — the same trust model as pinned certificates.
fn client_config(cert_der: Vec<u8>) -> rustls::ClientConfig {
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(CertificateDer::from(cert_der))
        .expect("failed to add cert to root store");

    rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

/// Helper: send a command over a TLS stream and return the parsed response.
async fn tls_cmd(
    stream: &mut tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
    args: &[&str],
) -> Frame {
    let parts: Vec<Frame> = args
        .iter()
        .map(|a| Frame::Bulk(Bytes::copy_from_slice(a.as_bytes())))
        .collect();
    let mut out = BytesMut::new();
    Frame::Array(parts).serialize(&mut out);
    stream.write_all(&out).await.expect("write failed");

    let mut buf = BytesMut::with_capacity(1024);
    loop {
        let n = stream.read_buf(&mut buf).await.expect("read failed");
        assert!(
            n > 0 || !buf.is_empty(),
            "server closed connection unexpectedly"
        );
        match parse_frame(&buf) {
            Ok(Some((frame, consumed))) => {
                let _ = buf.split_to(consumed);
                return frame;
            }
            Ok(None) => continue,
            Err(e) => panic!("protocol error: {e}"),
        }
    }
}

/// Verifies that PING, SET, and GET work correctly over a TLS connection.
#[tokio::test]
async fn tls_basic_commands() {
    let tmp = tempfile::tempdir().unwrap();
    let (cert_path, key_path, cert_der) = generate_test_cert(tmp.path());

    let server = TestServer::start_with(ServerOptions {
        tls_cert_file: Some(cert_path),
        tls_key_file: Some(key_path),
        ..Default::default()
    });

    let tls_port = server
        .tls_port
        .expect("server should have a TLS port after start_with with TLS options");

    let config = Arc::new(client_config(cert_der));
    let connector = TlsConnector::from(config);
    let server_name = ServerName::try_from("localhost").expect("invalid server name");

    let tcp = tokio::net::TcpStream::connect(format!("127.0.0.1:{tls_port}"))
        .await
        .unwrap_or_else(|e| panic!("failed to connect to TLS port {tls_port}: {e}"));

    let mut stream = connector
        .connect(server_name, tcp)
        .await
        .expect("TLS handshake failed");

    // basic roundtrip: PING
    let pong = tls_cmd(&mut stream, &["PING"]).await;
    assert!(
        matches!(pong, Frame::Simple(ref s) if s == "PONG"),
        "expected PONG, got {pong:?}"
    );

    // write and read back a value
    let ok = tls_cmd(&mut stream, &["SET", "tls:key", "hello"]).await;
    assert!(
        matches!(ok, Frame::Simple(ref s) if s == "OK"),
        "expected OK from SET, got {ok:?}"
    );

    let val = tls_cmd(&mut stream, &["GET", "tls:key"]).await;
    assert!(
        matches!(val, Frame::Bulk(ref b) if b.as_ref() == b"hello"),
        "expected 'hello' from GET, got {val:?}"
    );
}

/// Verifies that a plain TCP connection to the TLS port is rejected cleanly
/// (the TLS handshake never completes, connection is closed).
#[tokio::test]
async fn plain_tcp_rejected_on_tls_port() {
    let tmp = tempfile::tempdir().unwrap();
    let (cert_path, key_path, _cert_der) = generate_test_cert(tmp.path());

    let server = TestServer::start_with(ServerOptions {
        tls_cert_file: Some(cert_path),
        tls_key_file: Some(key_path),
        ..Default::default()
    });

    let tls_port = server.tls_port.unwrap();

    // connect without TLS and attempt a plain PING — the server should
    // close the connection when it sees non-TLS bytes, or return garbage.
    let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{tls_port}"))
        .await
        .unwrap();

    let mut out = BytesMut::new();
    Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))]).serialize(&mut out);
    let _ = stream.write_all(&out).await;

    // read whatever comes back — the server either closes the connection
    // outright (0 bytes) or sends a TLS alert. either way it must NOT
    // respond with a valid RESP3 PONG.
    let mut buf = vec![0u8; 128];
    match stream.read(&mut buf).await {
        Ok(0) | Err(_) => {} // closed — expected
        Ok(n) => {
            // got bytes back (likely a TLS alert record); it must not be PONG
            assert!(
                !String::from_utf8_lossy(&buf[..n]).contains("PONG"),
                "server sent PONG on TLS port for a plain TCP connection"
            );
        }
    }
}
