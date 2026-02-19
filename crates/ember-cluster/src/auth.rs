//! Shared-secret authentication for cluster transport.
//!
//! When a `ClusterSecret` is configured, every gossip UDP message and Raft TCP
//! frame is authenticated with HMAC-SHA256. Unauthenticated messages are silently
//! dropped to prevent oracle attacks.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// HMAC tag length (SHA-256 output).
pub const TAG_LEN: usize = 32;

/// A shared secret used to authenticate cluster transport messages.
///
/// Wraps the raw password bytes and provides sign/verify operations.
/// The `Debug` impl redacts the secret to prevent accidental logging.
pub struct ClusterSecret {
    key: Vec<u8>,
}

impl ClusterSecret {
    /// Creates a secret from a password string.
    pub fn from_password(password: &str) -> Self {
        Self {
            key: password.as_bytes().to_vec(),
        }
    }

    /// Creates a secret by reading and trimming a file.
    pub fn from_file(path: &std::path::Path) -> Result<Self, std::io::Error> {
        let contents = std::fs::read_to_string(path)?;
        let password = contents.trim_end();
        if password.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cluster auth password file is empty",
            ));
        }
        Ok(Self::from_password(password))
    }

    /// Computes an HMAC-SHA256 tag over `payload`.
    pub fn sign(&self, payload: &[u8]) -> [u8; TAG_LEN] {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(payload);
        mac.finalize().into_bytes().into()
    }

    /// Verifies an HMAC-SHA256 tag in constant time.
    pub fn verify(&self, payload: &[u8], tag: &[u8]) -> bool {
        if tag.len() != TAG_LEN {
            return false;
        }
        let expected = self.sign(payload);
        bool::from(expected.ct_eq(tag))
    }
}

impl std::fmt::Debug for ClusterSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterSecret")
            .field("key", &"[redacted]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_roundtrip() {
        let secret = ClusterSecret::from_password("test-secret");
        let payload = b"hello cluster";
        let tag = secret.sign(payload);
        assert!(secret.verify(payload, &tag));
    }

    #[test]
    fn wrong_secret_rejects() {
        let s1 = ClusterSecret::from_password("secret-a");
        let s2 = ClusterSecret::from_password("secret-b");
        let tag = s1.sign(b"data");
        assert!(!s2.verify(b"data", &tag));
    }

    #[test]
    fn tampered_payload_rejects() {
        let secret = ClusterSecret::from_password("test");
        let tag = secret.sign(b"original");
        assert!(!secret.verify(b"tampered", &tag));
    }

    #[test]
    fn wrong_tag_length_rejects() {
        let secret = ClusterSecret::from_password("test");
        assert!(!secret.verify(b"data", &[0u8; 16]));
    }

    #[test]
    fn debug_redacts_secret() {
        let secret = ClusterSecret::from_password("super-secret");
        let debug = format!("{:?}", secret);
        assert!(!debug.contains("super-secret"));
        assert!(debug.contains("redacted"));
    }
}
