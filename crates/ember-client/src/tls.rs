//! TLS configuration and connection helpers for ember-client.
//!
//! All items in this module are only compiled when the `tls` feature is
//! enabled (which is the default). Downstream code that disables the feature
//! can use `Client::connect` for plain TCP without pulling in rustls.

use std::io;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::connection::Transport;

/// TLS configuration for client connections.
///
/// Pass this to `Client::connect_tls` to establish a TLS-encrypted connection
/// to the server.
#[derive(Clone, Debug, Default)]
pub struct TlsClientConfig {
    /// Path to a custom CA certificate (PEM format) for verifying the server.
    ///
    /// When `None`, the platform's native root certificate store is used.
    pub ca_cert: Option<String>,

    /// Skip server certificate verification entirely.
    ///
    /// Emits a warning to stderr when set. Only use in development with
    /// self-signed certificates — never in production.
    pub insecure: bool,
}

/// Establishes a TCP connection and upgrades it to TLS.
///
/// Used by `Client::connect_tls`; not typically called directly.
pub(crate) async fn connect(
    host: &str,
    port: u16,
    config: &TlsClientConfig,
) -> io::Result<Transport> {
    let tcp = TcpStream::connect((host, port)).await?;
    let client_config = build_client_config(config)?;
    let connector = TlsConnector::from(Arc::new(client_config));

    let server_name = ServerName::try_from(host.to_string()).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid server name '{host}': {e}"),
        )
    })?;

    let tls_stream = connector.connect(server_name, tcp).await?;
    Ok(Transport::Tls(Box::new(tls_stream)))
}

/// Builds a `rustls::ClientConfig` from the provided options.
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

/// Builds a client config that skips all certificate verification.
///
/// Only safe for development/testing with self-signed certificates.
fn build_insecure_config() -> io::Result<rustls::ClientConfig> {
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerifier))
        .with_no_client_auth();
    Ok(config)
}

/// A certificate verifier that accepts anything. Used with `insecure: true`.
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

/// Convenience helper to wrap a value in `Ok`.
trait PipeOk: Sized {
    fn pipe_ok(self) -> io::Result<Self> {
        Ok(self)
    }
}

impl PipeOk for rustls::ClientConfig {}
