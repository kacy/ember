//! Active expiration via random sampling.
//!
//! Instead of maintaining a time wheel or sorted expiry index, we
//! periodically sample random keys and evict any that have expired.
//! This is the same algorithm Redis uses — simple, memory-free, and
//! effective across all TTL ranges.

use crate::keyspace::Keyspace;

/// Maximum keys to sample per round.
const SAMPLE_SIZE: usize = 20;

/// If more than this fraction of the sample was expired, go again.
const EXPIRED_THRESHOLD: f64 = 0.25;

/// Maximum rounds per tick to avoid starving the command loop.
const MAX_ROUNDS: usize = 3;

/// Runs one active expiration cycle on the keyspace.
///
/// Samples up to `SAMPLE_SIZE` random keys per round, removes expired
/// ones, and repeats if more than 25% of the sample was expired (up to
/// `MAX_ROUNDS` total). Returns the total number of keys removed.
pub fn run_expiration_cycle(ks: &mut Keyspace) -> usize {
    let mut total_removed = 0;

    for _ in 0..MAX_ROUNDS {
        let removed = ks.expire_sample(SAMPLE_SIZE);
        total_removed += removed;

        // if we removed fewer than 25% of the sample, the keyspace
        // is reasonably clean — stop early
        if (removed as f64) < (SAMPLE_SIZE as f64) * EXPIRED_THRESHOLD {
            break;
        }
    }

    total_removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn no_expired_keys_removes_nothing() {
        let mut ks = Keyspace::new();
        for i in 0..10 {
            ks.set(format!("key:{i}"), Bytes::from("val"), None);
        }
        let removed = run_expiration_cycle(&mut ks);
        assert_eq!(removed, 0);
        assert_eq!(ks.len(), 10);
    }

    #[test]
    fn removes_expired_keys() {
        let mut ks = Keyspace::new();
        // insert some keys with very short TTLs
        for i in 0..10 {
            ks.set(
                format!("temp:{i}"),
                Bytes::from("gone"),
                Some(Duration::from_millis(5)),
            );
        }
        // and some persistent keys
        for i in 0..5 {
            ks.set(format!("keep:{i}"), Bytes::from("stay"), None);
        }

        thread::sleep(Duration::from_millis(20));

        let removed = run_expiration_cycle(&mut ks);
        assert_eq!(removed, 10);
        assert_eq!(ks.len(), 5);
    }

    #[test]
    fn leaves_unexpired_keys_alone() {
        let mut ks = Keyspace::new();
        for i in 0..10 {
            ks.set(
                format!("key:{i}"),
                Bytes::from("val"),
                Some(Duration::from_secs(3600)),
            );
        }
        let removed = run_expiration_cycle(&mut ks);
        assert_eq!(removed, 0);
        assert_eq!(ks.len(), 10);
    }

    #[test]
    fn empty_keyspace_is_fine() {
        let mut ks = Keyspace::new();
        let removed = run_expiration_cycle(&mut ks);
        assert_eq!(removed, 0);
    }
}
