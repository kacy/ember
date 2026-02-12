//! TLS client support for ember-cli.
//!
//! Provides a `MaybeTlsStream` wrapper that implements `AsyncRead` and
//! `AsyncWrite`, allowing the rest of the codebase to work with either
//! plain TCP or TLS connections transparently.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

/// TLS configuration for client connections.
#[derive(Clone, Debug)]
pub struct TlsClientConfig {
    /// Optional path to a CA certificate (PEM) for verifying the server.
    /// When `None`, the system trust store is used.
    pub ca_cert: Option<String>,

    /// Skip server certificate verification entirely. Prints a warning
    /// to stderr when enabled — useful for development with self-signed certs.
    pub insecure: bool,
}

/// A TCP stream that may or may not be wrapped in TLS.
pub enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTlsStream::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Connects to `host:port`, optionally upgrading to TLS.
///
/// When `tls` is `None`, returns a plain TCP stream. When `Some`, builds a
/// rustls `ClientConfig` and performs the TLS handshake before returning.
pub async fn connect(
    host: &str,
    port: u16,
    tls: Option<&TlsClientConfig>,
) -> io::Result<MaybeTlsStream> {
    let tcp = TcpStream::connect((host, port)).await?;

    let Some(tls_config) = tls else {
        return Ok(MaybeTlsStream::Plain(tcp));
    };

    let client_config = build_client_config(tls_config)?;
    let connector = TlsConnector::from(Arc::new(client_config));

    let server_name = ServerName::try_from(host.to_string()).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid server name '{host}': {e}"),
        )
    })?;

    let tls_stream = connector.connect(server_name, tcp).await?;
    Ok(MaybeTlsStream::Tls(Box::new(tls_stream)))
}

/// Builds a rustls `ClientConfig` from the CLI's TLS options.
fn build_client_config(config: &TlsClientConfig) -> io::Result<rustls::ClientConfig> {
    if config.insecure {
        eprintln!("warning: TLS certificate verification is disabled");
        return build_insecure_config();
    }

    let roots = load_root_certs(config.ca_cert.as_deref())?;

    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
        .pipe_ok()
}

/// Loads root certificates from a custom CA file or the system trust store.
fn load_root_certs(ca_cert_path: Option<&str>) -> io::Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();

    if let Some(path) = ca_cert_path {
        let pem = std::fs::read(path).map_err(|e| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("failed to read CA cert '{path}': {e}"),
            )
        })?;
        let certs = rustls::pki_types::CertificateDer::pem_slice_iter(&pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        if certs.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("no certificates found in '{path}'"),
            ));
        }

        for cert in certs {
            roots.add(cert).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid CA certificate: {e}"),
                )
            })?;
        }
    } else {
        // load the platform's native root certificates
        let native_certs = rustls_native_certs::load_native_certs();
        for cert in native_certs.certs {
            roots.add(cert).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid native CA certificate: {e}"),
                )
            })?;
        }
    }

    Ok(roots)
}

/// Builds a client config that accepts any server certificate.
///
/// This is intentionally insecure and only for development/testing.
fn build_insecure_config() -> io::Result<rustls::ClientConfig> {
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();
    Ok(config)
}

/// A certificate verifier that accepts everything. Used with `--tls-insecure`.
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Helper trait to make `Ok(config)` less noisy.
trait PipeOk: Sized {
    fn pipe_ok(self) -> io::Result<Self> {
        Ok(self)
    }
}

impl PipeOk for rustls::ClientConfig {}
