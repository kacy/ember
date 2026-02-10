//! Encryption at rest using AES-256-GCM.
//!
//! Each record (AOF or snapshot entry) is encrypted independently with a
//! random 12-byte nonce. AES-GCM provides authenticated encryption — a
//! tampered ciphertext is detected immediately rather than producing garbage.
//!
//! This module is only compiled when the `encryption` feature is enabled.

use std::fmt;
use std::io;
use std::path::Path;

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Nonce};

use crate::format::FormatError;

/// AES-256-GCM nonce size in bytes.
pub const NONCE_SIZE: usize = 12;

/// AES-256-GCM authentication tag size in bytes.
pub const TAG_SIZE: usize = 16;

/// A 256-bit encryption key for AES-256-GCM.
///
/// The key is stored inline — no heap allocation. Implements `Clone`
/// but not `Debug` to avoid accidentally logging key material.
#[derive(Clone)]
pub struct EncryptionKey {
    bytes: [u8; 32],
}

impl fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptionKey")
            .field("bytes", &"[redacted]")
            .finish()
    }
}

impl EncryptionKey {
    /// Creates a key from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self { bytes }
    }

    /// Reads an encryption key from a file.
    ///
    /// The file may contain either:
    /// - exactly 32 raw bytes, or
    /// - 64 hex characters (with optional trailing whitespace/newline)
    pub fn from_file(path: &Path) -> Result<Self, FormatError> {
        let data = std::fs::read(path).map_err(|e| {
            FormatError::Io(io::Error::new(
                e.kind(),
                format!("failed to read encryption key file '{}': {e}", path.display()),
            ))
        })?;

        // try raw 32 bytes first
        if data.len() == 32 {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&data);
            return Ok(Self { bytes });
        }

        // try hex-encoded (64 chars + optional trailing whitespace)
        let trimmed = std::str::from_utf8(&data)
            .map_err(|_| {
                FormatError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "encryption key file is not valid UTF-8 or raw 32 bytes",
                ))
            })?
            .trim();

        if trimmed.len() != 64 {
            return Err(FormatError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "encryption key file must be 32 raw bytes or 64 hex characters, got {} bytes",
                    data.len()
                ),
            )));
        }

        let mut bytes = [0u8; 32];
        for i in 0..32 {
            bytes[i] = u8::from_str_radix(&trimmed[i * 2..i * 2 + 2], 16).map_err(|_| {
                FormatError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "encryption key file contains invalid hex characters",
                ))
            })?;
        }

        Ok(Self { bytes })
    }

    /// Returns the raw key bytes.
    fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

/// Encrypts a plaintext record using AES-256-GCM.
///
/// Returns `(nonce, ciphertext)` where ciphertext includes the 16-byte
/// auth tag appended by AES-GCM.
pub fn encrypt_record(key: &EncryptionKey, plaintext: &[u8]) -> Result<([u8; NONCE_SIZE], Vec<u8>), FormatError> {
    let cipher = Aes256Gcm::new(key.as_bytes().into());
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

    let ciphertext = cipher.encrypt(&nonce, plaintext).map_err(|e| {
        FormatError::Io(io::Error::new(
            io::ErrorKind::Other,
            format!("encryption failed: {e}"),
        ))
    })?;

    let mut nonce_bytes = [0u8; NONCE_SIZE];
    nonce_bytes.copy_from_slice(&nonce);
    Ok((nonce_bytes, ciphertext))
}

/// Decrypts a ciphertext record using AES-256-GCM.
///
/// The ciphertext must include the 16-byte auth tag (as produced by
/// [`encrypt_record`]). Returns `DecryptionFailed` if the key is wrong
/// or the data has been tampered with.
pub fn decrypt_record(key: &EncryptionKey, nonce: &[u8; NONCE_SIZE], ciphertext: &[u8]) -> Result<Vec<u8>, FormatError> {
    let cipher = Aes256Gcm::new(key.as_bytes().into());
    let nonce = Nonce::from_slice(nonce);

    cipher.decrypt(nonce, ciphertext).map_err(|_| FormatError::DecryptionFailed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> EncryptionKey {
        EncryptionKey::from_bytes([0x42; 32])
    }

    #[test]
    fn round_trip() {
        let key = test_key();
        let plaintext = b"hello, encrypted world";

        let (nonce, ciphertext) = encrypt_record(&key, plaintext).unwrap();
        let decrypted = decrypt_record(&key, &nonce, &ciphertext).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn wrong_key_fails() {
        let key = test_key();
        let wrong_key = EncryptionKey::from_bytes([0xFF; 32]);
        let plaintext = b"secret data";

        let (nonce, ciphertext) = encrypt_record(&key, plaintext).unwrap();
        let err = decrypt_record(&wrong_key, &nonce, &ciphertext).unwrap_err();

        assert!(matches!(err, FormatError::DecryptionFailed));
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let key = test_key();
        let plaintext = b"integrity check";

        let (nonce, mut ciphertext) = encrypt_record(&key, plaintext).unwrap();
        // flip a byte in the ciphertext
        ciphertext[0] ^= 0xFF;

        let err = decrypt_record(&key, &nonce, &ciphertext).unwrap_err();
        assert!(matches!(err, FormatError::DecryptionFailed));
    }

    #[test]
    fn empty_plaintext() {
        let key = test_key();
        let plaintext = b"";

        let (nonce, ciphertext) = encrypt_record(&key, plaintext).unwrap();
        // ciphertext should be exactly the auth tag size
        assert_eq!(ciphertext.len(), TAG_SIZE);

        let decrypted = decrypt_record(&key, &nonce, &ciphertext).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn different_nonces_per_call() {
        let key = test_key();
        let plaintext = b"same data";

        let (nonce1, ct1) = encrypt_record(&key, plaintext).unwrap();
        let (nonce2, ct2) = encrypt_record(&key, plaintext).unwrap();

        // nonces should differ (probabilistically guaranteed with random nonces)
        assert_ne!(nonce1, nonce2);
        // ciphertexts should differ too
        assert_ne!(ct1, ct2);
    }

    #[test]
    fn key_from_raw_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.bin");
        let raw = [0xAB; 32];
        std::fs::write(&path, raw).unwrap();

        let key = EncryptionKey::from_file(&path).unwrap();
        assert_eq!(*key.as_bytes(), raw);
    }

    #[test]
    fn key_from_hex_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.hex");
        let hex = "ab".repeat(32);
        std::fs::write(&path, format!("{hex}\n")).unwrap();

        let key = EncryptionKey::from_file(&path).unwrap();
        assert_eq!(*key.as_bytes(), [0xAB; 32]);
    }

    #[test]
    fn key_from_bad_file_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.bad");
        std::fs::write(&path, "too short").unwrap();

        let err = EncryptionKey::from_file(&path).unwrap_err();
        assert!(matches!(err, FormatError::Io(_)));
    }

    #[test]
    fn key_from_missing_file_fails() {
        let path = std::path::Path::new("/nonexistent/key.bin");
        let err = EncryptionKey::from_file(path).unwrap_err();
        assert!(matches!(err, FormatError::Io(_)));
    }

    #[test]
    fn debug_redacts_key() {
        let key = test_key();
        let debug = format!("{key:?}");
        assert!(debug.contains("redacted"));
        assert!(!debug.contains("42"));
    }
}
