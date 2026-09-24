//! Encryption at rest using AES-256-GCM.
//!
//! Each record (AOF or snapshot entry) is encrypted independently with a
//! random 12-byte nonce. AES-GCM provides authenticated encryption — a
//! tampered ciphertext is detected immediately rather than producing garbage.
//!
//! Each v4 file starts with a random salt, and its records are encrypted
//! with a key derived from the master key and that salt. Random nonces
//! stay far from GCM's collision limits because no key encrypts more than
//! one file, and a record copied from one file into another fails to
//! decrypt. v3 files, which use the master key directly, are still read.
//!
//! This module is only compiled when the `encryption` feature is enabled.

use std::fmt;
use std::io::{self, Read, Write};
use std::path::Path;

use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Nonce};
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::format::{self, FormatError};

/// AES-256-GCM nonce size in bytes.
pub const NONCE_SIZE: usize = 12;

/// AES-256-GCM authentication tag size in bytes.
pub const TAG_SIZE: usize = 16;

/// Size of the random salt at the start of each v4 file.
pub const SALT_SIZE: usize = 16;

/// A 256-bit master key for AES-256-GCM.
///
/// The key is stored inline — no heap allocation. `Debug` redacts it, and
/// the bytes are zeroed on drop so the key doesn't linger in freed memory.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
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
        let data = Zeroizing::new(std::fs::read(path).map_err(|e| {
            FormatError::Io(io::Error::new(
                e.kind(),
                format!(
                    "failed to read encryption key file '{}': {e}",
                    path.display()
                ),
            ))
        })?);

        // try raw 32 bytes first
        let mut key = Self { bytes: [0; 32] };
        if data.len() == 32 {
            key.bytes.copy_from_slice(&data);
            return Ok(key);
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

        for (i, byte) in key.bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&trimmed[i * 2..i * 2 + 2], 16).map_err(|_| {
                FormatError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "encryption key file contains invalid hex characters",
                ))
            })?;
        }

        Ok(key)
    }

    /// Returns the raw key bytes.
    #[cfg(test)]
    fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }

    /// Returns the cipher for a v4 file with this salt. Its key is derived
    /// from the master key and the salt with HKDF-SHA256.
    pub fn file_cipher(&self, salt: &[u8; SALT_SIZE]) -> FileCipher {
        let mut subkey = Zeroizing::new([0u8; 32]);
        Hkdf::<Sha256>::new(Some(salt), &self.bytes)
            .expand(b"ember file key", subkey.as_mut())
            .expect("32 bytes is a valid HKDF-SHA256 output length");
        FileCipher::new(&subkey)
    }

    /// Returns the cipher for a v3 file, which used the master key directly.
    pub fn legacy_cipher(&self) -> FileCipher {
        FileCipher::new(&self.bytes)
    }

    /// Picks a salt for a new file, writes it to `w` and returns the file's
    /// cipher.
    pub fn write_new_file_salt(&self, w: &mut impl Write) -> io::Result<FileCipher> {
        let mut salt = [0u8; SALT_SIZE];
        OsRng.fill_bytes(&mut salt);
        w.write_all(&salt)?;
        Ok(self.file_cipher(&salt))
    }

    /// Returns the cipher for an existing file with header `version`. For a
    /// v4 file, reads the salt from `r`, which must be positioned at it.
    pub fn read_file_cipher(
        &self,
        version: u8,
        r: &mut impl Read,
    ) -> Result<FileCipher, FormatError> {
        if version == format::FORMAT_VERSION_ENCRYPTED_V3 {
            return Ok(self.legacy_cipher());
        }
        let mut salt = [0u8; SALT_SIZE];
        format::read_exact(r, &mut salt)?;
        Ok(self.file_cipher(&salt))
    }
}

/// AES-256-GCM set up with the key for one file.
pub struct FileCipher {
    cipher: Aes256Gcm,
}

impl FileCipher {
    fn new(key: &[u8; 32]) -> Self {
        Self {
            cipher: Aes256Gcm::new(key.into()),
        }
    }

    /// Encrypts a record. Returns `(nonce, ciphertext)`, where the
    /// ciphertext ends with the 16-byte auth tag.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<([u8; NONCE_SIZE], Vec<u8>), FormatError> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| FormatError::Io(io::Error::other(format!("encryption failed: {e}"))))?;
        Ok((nonce.into(), ciphertext))
    }

    /// Decrypts a record produced by [`encrypt`](Self::encrypt). Returns
    /// `DecryptionFailed` if the key is wrong or the data was changed.
    pub fn decrypt(
        &self,
        nonce: &[u8; NONCE_SIZE],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, FormatError> {
        self.cipher
            .decrypt(Nonce::from_slice(nonce), ciphertext)
            .map_err(|_| FormatError::DecryptionFailed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> EncryptionKey {
        EncryptionKey::from_bytes([0x42; 32])
    }

    const SALT: [u8; SALT_SIZE] = [7; SALT_SIZE];

    fn cipher(key: &EncryptionKey) -> FileCipher {
        key.file_cipher(&SALT)
    }

    #[test]
    fn each_salt_gets_its_own_key() {
        let key = test_key();
        let (nonce, ciphertext) = key.file_cipher(&SALT).encrypt(b"data").unwrap();
        let other = key.file_cipher(&[8; SALT_SIZE]);
        assert!(matches!(
            other.decrypt(&nonce, &ciphertext),
            Err(FormatError::DecryptionFailed)
        ));
        assert!(key.legacy_cipher().decrypt(&nonce, &ciphertext).is_err());
    }

    #[test]
    fn salt_round_trips_through_a_file() {
        let key = test_key();
        let mut file = Vec::new();
        let (nonce, ciphertext) = key
            .write_new_file_salt(&mut file)
            .unwrap()
            .encrypt(b"data")
            .unwrap();
        let cipher = key
            .read_file_cipher(format::FORMAT_VERSION_ENCRYPTED, &mut file.as_slice())
            .unwrap();
        assert_eq!(cipher.decrypt(&nonce, &ciphertext).unwrap(), b"data");
    }

    #[test]
    fn round_trip() {
        let key = test_key();
        let plaintext = b"hello, encrypted world";

        let (nonce, ciphertext) = cipher(&key).encrypt(plaintext).unwrap();
        let decrypted = cipher(&key).decrypt(&nonce, &ciphertext).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn wrong_key_fails() {
        let key = test_key();
        let wrong_key = EncryptionKey::from_bytes([0xFF; 32]);
        let plaintext = b"secret data";

        let (nonce, ciphertext) = cipher(&key).encrypt(plaintext).unwrap();
        let err = cipher(&wrong_key).decrypt(&nonce, &ciphertext).unwrap_err();

        assert!(matches!(err, FormatError::DecryptionFailed));
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let key = test_key();
        let plaintext = b"integrity check";

        let (nonce, mut ciphertext) = cipher(&key).encrypt(plaintext).unwrap();
        // flip a byte in the ciphertext
        ciphertext[0] ^= 0xFF;

        let err = cipher(&key).decrypt(&nonce, &ciphertext).unwrap_err();
        assert!(matches!(err, FormatError::DecryptionFailed));
    }

    #[test]
    fn empty_plaintext() {
        let key = test_key();
        let plaintext = b"";

        let (nonce, ciphertext) = cipher(&key).encrypt(plaintext).unwrap();
        // ciphertext should be exactly the auth tag size
        assert_eq!(ciphertext.len(), TAG_SIZE);

        let decrypted = cipher(&key).decrypt(&nonce, &ciphertext).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn different_nonces_per_call() {
        let key = test_key();
        let plaintext = b"same data";

        let (nonce1, ct1) = cipher(&key).encrypt(plaintext).unwrap();
        let (nonce2, ct2) = cipher(&key).encrypt(plaintext).unwrap();

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
