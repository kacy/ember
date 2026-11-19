//! Active expiration via random sampling.
//!
//! Each shard periodically samples random keys that have a TTL and removes
//! the ones that have expired, as Redis does. The keyspace keeps the set of
//! keys with a TTL, so keys without one never take up sample slots. There is
//! no time wheel or sorted index of deadlines.

use crate::keyspace::Keyspace;

/// Maximum keys to sample per round.
const SAMPLE_SIZE: usize = 20;

/// If more than this fraction of the sample was expired, go again.
const EXPIRED_THRESHOLD: f64 = 0.25;

/// Maximum rounds per tick to avoid starving the command loop.
const MAX_ROUNDS: usize = 3;

/// Runs one active expiration cycle on the keyspace.
///
/// The keys it removes are kept by the keyspace, for the caller to take
/// with `take_expired` and send to keyspace notifications.
pub fn run_expiration_cycle(ks: &mut Keyspace) {
    for _ in 0..MAX_ROUNDS {
        let removed = ks.expire_sample(SAMPLE_SIZE);

        // if we removed fewer than 25% of the sample, the keyspace
        // is reasonably clean — stop early
        if (removed as f64) < (SAMPLE_SIZE as f64) * EXPIRED_THRESHOLD {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn finds_expired_keys_among_many_without_ttl() {
        let mut ks = Keyspace::new();
        for i in 0..10_000 {
            ks.set(format!("plain:{i}"), Bytes::from("v"), None, false, false);
        }
        for i in 0..10 {
            let ttl = Some(Duration::from_millis(10));
            ks.set(format!("temp:{i}"), Bytes::from("v"), ttl, false, false);
        }
        thread::sleep(Duration::from_millis(30));

        run_expiration_cycle(&mut ks);
        let removed = ks.take_expired();
        assert_eq!(removed.len(), 10);
        assert_eq!(ks.len(), 10_000);
        assert_eq!(ks.stats().keys_with_expiry, 0);
    }

    #[test]
    fn no_expired_keys_removes_nothing() {
        let mut ks = Keyspace::new();
        for i in 0..10 {
            ks.set(format!("key:{i}"), Bytes::from("val"), None, false, false);
        }
        run_expiration_cycle(&mut ks);
        let removed = ks.take_expired();
        assert!(removed.is_empty());
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
                false,
                false,
            );
        }
        // and some persistent keys
        for i in 0..5 {
            ks.set(format!("keep:{i}"), Bytes::from("stay"), None, false, false);
        }

        thread::sleep(Duration::from_millis(20));

        run_expiration_cycle(&mut ks);
        let removed = ks.take_expired();
        assert_eq!(removed.len(), 10);
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
                false,
                false,
            );
        }
        run_expiration_cycle(&mut ks);
        let removed = ks.take_expired();
        assert!(removed.is_empty());
        assert_eq!(ks.len(), 10);
    }

    #[test]
    fn empty_keyspace_is_fine() {
        let mut ks = Keyspace::new();
        run_expiration_cycle(&mut ks);
        let removed = ks.take_expired();
        assert!(removed.is_empty());
    }
}
