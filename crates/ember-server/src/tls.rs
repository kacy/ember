//! TLS configuration and certificate loading.
//!
//! Provides utilities for setting up TLS with rustls, including loading
//! certificates and private keys from PEM files, and optionally configuring
//! client certificate verification (mTLS).

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::RootCertStore;
use rustls_pki_types::pem::PemObject;
use thiserror::Error;
use tokio_rustls::TlsAcceptor;

/// TLS configuration loaded from command-line arguments.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the server certificate (PEM format).
    pub cert_file: String,
    /// Path to the server private key (PEM format).
    pub key_file: String,
    /// Optional path to CA certificate for client verification.
    pub ca_cert_file: Option<String>,
    /// Whether to require client certificates (mTLS).
    pub auth_clients: bool,
}

/// Errors that can occur when loading TLS configuration.
#[derive(Debug, Error)]
pub enum TlsError {
    #[error("certificate file not found: {0}")]
    CertFileNotFound(String),

    #[error("private key file not found: {0}")]
    KeyFileNotFound(String),

    #[error("CA certificate file not found: {0}")]
    CaCertFileNotFound(String),

    #[error("failed to read certificate file: {0}")]
    CertReadError(#[source] std::io::Error),

    #[error("failed to parse PEM certificate: {0}")]
    PemError(String),

    #[error("failed to read private key file: {0}")]
    KeyReadError(#[source] std::io::Error),

    #[error("failed to read CA certificate file: {0}")]
    CaCertReadError(#[source] std::io::Error),

    #[error("no certificates found in file: {0}")]
    NoCertsFound(String),

    #[error("failed to build TLS config: {0}")]
    ConfigError(#[from] rustls::Error),

    #[error("failed to build client verifier: {0}")]
    VerifierError(String),
}

/// Loads TLS configuration and creates a `TlsAcceptor`.
///
/// Reads certificates and private key from PEM files. If `ca_cert_file` is
/// specified, sets up client certificate verification. When `auth_clients`
/// is true, clients must present valid certificates signed by the CA.
pub fn load_tls_acceptor(config: &TlsConfig) -> Result<TlsAcceptor, TlsError> {
    // load server certificates
    let cert_path = Path::new(&config.cert_file);
    if !cert_path.exists() {
        return Err(TlsError::CertFileNotFound(config.cert_file.clone()));
    }

    let cert_file = File::open(cert_path).map_err(TlsError::CertReadError)?;
    let cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_reader_iter(cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::PemError(e.to_string()))?;

    if certs.is_empty() {
        return Err(TlsError::NoCertsFound(config.cert_file.clone()));
    }

    // load private key
    let key_path = Path::new(&config.key_file);
    if !key_path.exists() {
        return Err(TlsError::KeyFileNotFound(config.key_file.clone()));
    }

    let key_file = File::open(key_path).map_err(TlsError::KeyReadError)?;
    let key_reader = BufReader::new(key_file);
    let key: PrivateKeyDer<'static> = PrivateKeyDer::from_pem_reader(key_reader)
        .map_err(|e| TlsError::PemError(e.to_string()))?;

    // build server config
    let server_config = if let Some(ref ca_path) = config.ca_cert_file {
        // load CA cert for client verification
        let ca_cert_path = Path::new(ca_path);
        if !ca_cert_path.exists() {
            return Err(TlsError::CaCertFileNotFound(ca_path.clone()));
        }

        let ca_file = File::open(ca_cert_path).map_err(TlsError::CaCertReadError)?;
        let ca_reader = BufReader::new(ca_file);
        let ca_certs: Vec<CertificateDer<'static>> = CertificateDer::pem_reader_iter(ca_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TlsError::PemError(e.to_string()))?;

        let mut root_store = RootCertStore::empty();
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| TlsError::VerifierError(e.to_string()))?;
        }

        let verifier = if config.auth_clients {
            // require client certs
            WebPkiClientVerifier::builder(Arc::new(root_store))
                .build()
                .map_err(|e| TlsError::VerifierError(e.to_string()))?
        } else {
            // allow but don't require client certs
            WebPkiClientVerifier::builder(Arc::new(root_store))
                .allow_unauthenticated()
                .build()
                .map_err(|e| TlsError::VerifierError(e.to_string()))?
        };

        rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)?
    } else {
        // no client verification
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?
    };

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_missing_cert_file() {
        let config = TlsConfig {
            cert_file: "/nonexistent/cert.pem".into(),
            key_file: "/nonexistent/key.pem".into(),
            ca_cert_file: None,
            auth_clients: false,
        };
        match load_tls_acceptor(&config) {
            Err(TlsError::CertFileNotFound(_)) => {}
            Err(e) => panic!("expected CertFileNotFound, got: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn test_missing_key_file() {
        // create temp cert file
        let tmp = std::env::temp_dir().join("test_cert.pem");
        std::fs::write(
            &tmp,
            "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n",
        )
        .unwrap();

        let config = TlsConfig {
            cert_file: tmp.to_string_lossy().into(),
            key_file: "/nonexistent/key.pem".into(),
            ca_cert_file: None,
            auth_clients: false,
        };
        match load_tls_acceptor(&config) {
            Err(TlsError::KeyFileNotFound(_)) => {}
            Err(e) => panic!("expected KeyFileNotFound, got: {e}"),
            Ok(_) => panic!("expected error, got Ok"),
        }

        std::fs::remove_file(tmp).ok();
    }
}
