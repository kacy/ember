use super::*;
use std::thread;

#[test]
fn del_existing() {
    let mut ks = Keyspace::new();
    ks.set("key".into(), Bytes::from("val"), None, false, false);
    assert!(ks.del("key"));
    assert_eq!(ks.get("key").unwrap(), None);
}

#[test]
fn del_missing() {
    let mut ks = Keyspace::new();
    assert!(!ks.del("nope"));
}

#[test]
fn exists_present_and_absent() {
    let mut ks = Keyspace::new();
    ks.set("yes".into(), Bytes::from("here"), None, false, false);
    assert!(ks.exists("yes"));
    assert!(!ks.exists("no"));
}

#[test]
fn ttl_no_expiry() {
    let mut ks = Keyspace::new();
    ks.set("key".into(), Bytes::from("val"), None, false, false);
    assert_eq!(ks.ttl("key"), TtlResult::NoExpiry);
}

#[test]
fn ttl_not_found() {
    let mut ks = Keyspace::new();
    assert_eq!(ks.ttl("missing"), TtlResult::NotFound);
}

#[test]
fn ttl_with_expiry() {
    let mut ks = Keyspace::new();
    ks.set(
        "key".into(),
        Bytes::from("val"),
        Some(Duration::from_secs(100)),
        false,
        false,
    );
    match ks.ttl("key") {
        TtlResult::Seconds(s) => assert!((98..=100).contains(&s)),
        other => panic!("expected Seconds, got {other:?}"),
    }
}

#[test]
fn ttl_expired_key() {
    let mut ks = Keyspace::new();
    ks.set(
        "temp".into(),
        Bytes::from("val"),
        Some(Duration::from_millis(10)),
        false,
        false,
    );
    thread::sleep(Duration::from_millis(30));
    assert_eq!(ks.ttl("temp"), TtlResult::NotFound);
}

#[test]
fn expire_existing_key() {
    let mut ks = Keyspace::new();
    ks.set("key".into(), Bytes::from("val"), None, false, false);
    assert!(ks.expire("key", 60));
    match ks.ttl("key") {
        TtlResult::Seconds(s) => assert!((58..=60).contains(&s)),
        other => panic!("expected Seconds, got {other:?}"),
    }
}

#[test]
fn expire_missing_key() {
    let mut ks = Keyspace::new();
    assert!(!ks.expire("nope", 60));
}

#[test]
fn del_expired_key_returns_false() {
    let mut ks = Keyspace::new();
    ks.set(
        "temp".into(),
        Bytes::from("val"),
        Some(Duration::from_millis(10)),
        false,
        false,
    );
    thread::sleep(Duration::from_millis(30));
    // key is expired, del should return false (not found)
    assert!(!ks.del("temp"));
}

// -- memory tracking tests --

#[test]
fn memory_increases_on_set() {
    let mut ks = Keyspace::new();
    assert_eq!(ks.stats().used_bytes, 0);
    ks.set("key".into(), Bytes::from("value"), None, false, false);
    assert!(ks.stats().used_bytes > 0);
    assert_eq!(ks.stats().key_count, 1);
}

#[test]
fn memory_decreases_on_del() {
    let mut ks = Keyspace::new();
    ks.set("key".into(), Bytes::from("value"), None, false, false);
    let after_set = ks.stats().used_bytes;
    ks.del("key");
    assert_eq!(ks.stats().used_bytes, 0);
    assert!(after_set > 0);
}

#[test]
fn memory_adjusts_on_overwrite() {
    let mut ks = Keyspace::new();
    ks.set("key".into(), Bytes::from("short"), None, false, false);
    let small = ks.stats().used_bytes;

    ks.set(
        "key".into(),
        Bytes::from("a much longer value"),
        None,
        false,
        false,
    );
    let large = ks.stats().used_bytes;

    assert!(large > small);
    assert_eq!(ks.stats().key_count, 1);
}

#[test]
fn memory_decreases_on_expired_removal() {
    let mut ks = Keyspace::new();
    ks.set(
        "temp".into(),
        Bytes::from("data"),
        Some(Duration::from_millis(10)),
        false,
        false,
    );
    assert!(ks.stats().used_bytes > 0);
    thread::sleep(Duration::from_millis(30));
    // trigger lazy expiration
    let _ = ks.get("temp");
    assert_eq!(ks.stats().used_bytes, 0);
    assert_eq!(ks.stats().key_count, 0);
}

#[test]
fn stats_tracks_expiry_count() {
    let mut ks = Keyspace::new();
    ks.set("a".into(), Bytes::from("1"), None, false, false);
    ks.set(
        "b".into(),
        Bytes::from("2"),
        Some(Duration::from_secs(100)),
        false,
        false,
    );
    ks.set(
        "c".into(),
        Bytes::from("3"),
        Some(Duration::from_secs(200)),
        false,
        false,
    );

    let stats = ks.stats();
    assert_eq!(stats.key_count, 3);
    assert_eq!(stats.keys_with_expiry, 2);
}

// -- eviction tests --

#[test]
fn scan_finishes_in_len_over_count_calls() {
    let mut ks = Keyspace::new();
    for i in 0..100 {
        ks.set(format!("k{i}"), Bytes::from("v"), None, false, false);
    }
    let (mut cursor, mut calls, mut seen) = (0, 0, 0);
    loop {
        let (next, keys) = ks.scan_keys(cursor, 10, None);
        calls += 1;
        seen += keys.len();
        if next == 0 {
            break;
        }
        cursor = next;
    }
    assert_eq!((calls, seen), (10, 100));
}

#[test]
fn scan_returns_every_key_present_for_the_whole_scan() {
    let mut ks = Keyspace::new();
    for i in 0..200 {
        ks.set(format!("k{i}"), Bytes::from("v"), None, false, false);
    }
    // even-numbered keys stay; odd ones are deleted while scanning
    let mut found = std::collections::HashSet::new();
    let (mut cursor, mut next_odd) = (0, 1);
    loop {
        let (next, keys) = ks.scan_keys(cursor, 7, None);
        found.extend(keys);
        if next == 0 {
            break;
        }
        cursor = next;
        for _ in 0..3 {
            if next_odd < 200 {
                ks.del(&format!("k{next_odd}"));
                next_odd += 2;
            }
        }
    }
    for i in (0..200).step_by(2) {
        assert!(found.contains(&format!("k{i}")), "k{i} was skipped");
    }
}

/// Size of the small test entry `"a" = "val"`.
const SMALL_ENTRY: usize = 1 + 3 + memory::ENTRY_OVERHEAD;

/// A memory limit that fits one small entry but not two, after the
/// safety margin `memory::effective_limit` takes off.
fn one_entry_limit() -> usize {
    (SMALL_ENTRY + 10) * 100 / memory::MEMORY_SAFETY_MARGIN_PERCENT
}

#[test]
fn noeviction_returns_oom_when_full() {
    let config = ShardConfig {
        max_memory: Some(one_entry_limit()),
        eviction_policy: EvictionPolicy::NoEviction,
        ..ShardConfig::default()
    };
    let mut ks = Keyspace::with_config(config);

    // first key should fit
    assert_eq!(
        ks.set("a".into(), Bytes::from("val"), None, false, false),
        SetResult::Ok
    );

    // second key should push us over the limit
    let result = ks.set("b".into(), Bytes::from("val"), None, false, false);
    assert_eq!(result, SetResult::OutOfMemory);

    // original key should still be there
    assert!(ks.exists("a"));
}

#[test]
fn value_sizes_past_u32_fall_back_to_the_value() {
    let mut entry = Entry::new(Value::String(Bytes::from("abc")), None);
    let real = entry.value_size();

    entry.set_value_size(u32::MAX as usize + 10);
    // the cache cannot hold that size, so it is read from the value
    assert_eq!(entry.value_size(), real);
    entry.grow_value_size(5);
    assert_eq!(entry.value_size(), real);

    entry.set_value_size(100);
    entry.grow_value_size(5);
    entry.shrink_value_size(200);
    assert_eq!(entry.value_size(), 0);
}

#[test]
fn lru_eviction_makes_room() {
    let config = ShardConfig {
        max_memory: Some(one_entry_limit()),
        eviction_policy: EvictionPolicy::AllKeysLru,
        ..ShardConfig::default()
    };
    let mut ks = Keyspace::with_config(config);

    assert_eq!(
        ks.set("a".into(), Bytes::from("val"), None, false, false),
        SetResult::Ok
    );

    // this should trigger eviction of "a" to make room
    assert_eq!(
        ks.set("b".into(), Bytes::from("val"), None, false, false),
        SetResult::Ok
    );

    // "a" should have been evicted
    assert!(!ks.exists("a"));
    assert!(ks.exists("b"));
}

#[test]
fn safety_margin_rejects_near_raw_limit() {
    // the effective limit equals one small entry, so a second entry is
    // rejected even though the raw limit has headroom left
    let limit = (SMALL_ENTRY * 100).div_ceil(memory::MEMORY_SAFETY_MARGIN_PERCENT);
    assert_eq!(memory::effective_limit(limit), SMALL_ENTRY);
    assert!(limit > SMALL_ENTRY);
    let config = ShardConfig {
        max_memory: Some(limit),
        eviction_policy: EvictionPolicy::NoEviction,
        ..ShardConfig::default()
    };
    let mut ks = Keyspace::with_config(config);

    assert_eq!(
        ks.set("a".into(), Bytes::from("val"), None, false, false),
        SetResult::Ok
    );

    let result = ks.set("b".into(), Bytes::from("val"), None, false, false);
    assert_eq!(result, SetResult::OutOfMemory);
}

#[test]
fn overwrite_same_size_succeeds_at_limit() {
    let config = ShardConfig {
        max_memory: Some(one_entry_limit()),
        eviction_policy: EvictionPolicy::NoEviction,
        ..ShardConfig::default()
    };
    let mut ks = Keyspace::with_config(config);

    assert_eq!(
        ks.set("a".into(), Bytes::from("val"), None, false, false),
        SetResult::Ok
    );

    // overwriting with same-size value should succeed — no net increase
    assert_eq!(
        ks.set("a".into(), Bytes::from("new"), None, false, false),
        SetResult::Ok
    );
    assert_eq!(
        ks.get("a").unwrap(),
        Some(Value::String(Bytes::from("new")))
    );
}

#[test]
fn overwrite_larger_value_respects_limit() {
    let config = ShardConfig {
        max_memory: Some(one_entry_limit()),
        eviction_policy: EvictionPolicy::NoEviction,
        ..ShardConfig::default()
    };
    let mut ks = Keyspace::with_config(config);

    assert_eq!(
        ks.set("a".into(), Bytes::from("val"), None, false, false),
        SetResult::Ok
    );

    // overwriting with a much larger value should fail if it exceeds limit
    let big_value = "x".repeat(200);
    let result = ks.set("a".into(), Bytes::from(big_value), None, false, false);
    assert_eq!(result, SetResult::OutOfMemory);

    // original value should still be intact
    assert_eq!(
        ks.get("a").unwrap(),
        Some(Value::String(Bytes::from("val")))
    );
}

// -- iter_entries tests --

#[test]
fn iter_entries_returns_live_entries() {
    let mut ks = Keyspace::new();
    ks.set("a".into(), Bytes::from("1"), None, false, false);
    ks.set(
        "b".into(),
        Bytes::from("2"),
        Some(Duration::from_secs(100)),
        false,
        false,
    );

    let entries: Vec<_> = ks.iter_entries().collect();
    assert_eq!(entries.len(), 2);
}

#[test]
fn iter_entries_skips_expired() {
    let mut ks = Keyspace::new();
    ks.set(
        "dead".into(),
        Bytes::from("gone"),
        Some(Duration::from_millis(1)),
        false,
        false,
    );
    ks.set("alive".into(), Bytes::from("here"), None, false, false);
    thread::sleep(Duration::from_millis(10));

    let entries: Vec<_> = ks.iter_entries().collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, "alive");
}

#[test]
fn iter_entries_ttl_for_no_expiry() {
    let mut ks = Keyspace::new();
    ks.set("permanent".into(), Bytes::from("val"), None, false, false);

    let entries: Vec<_> = ks.iter_entries().collect();
    assert_eq!(entries[0].2, -1);
}

// -- restore tests --

#[test]
fn restore_adds_entry() {
    let mut ks = Keyspace::new();
    ks.restore("restored".into(), Value::String(Bytes::from("data")), None);
    assert_eq!(
        ks.get("restored").unwrap(),
        Some(Value::String(Bytes::from("data")))
    );
    assert_eq!(ks.stats().key_count, 1);
}

#[test]
fn restore_with_zero_ttl_expires_immediately() {
    let mut ks = Keyspace::new();
    // TTL of 0 should create entry that expires immediately
    ks.restore(
        "short-lived".into(),
        Value::String(Bytes::from("data")),
        Some(Duration::from_millis(1)),
    );
    // Entry exists but will be expired on access
    std::thread::sleep(Duration::from_millis(5));
    assert!(ks.get("short-lived").is_err() || ks.get("short-lived").unwrap().is_none());
}

#[test]
fn restore_overwrites_existing() {
    let mut ks = Keyspace::new();
    ks.set("key".into(), Bytes::from("old"), None, false, false);
    ks.restore("key".into(), Value::String(Bytes::from("new")), None);
    assert_eq!(
        ks.get("key").unwrap(),
        Some(Value::String(Bytes::from("new")))
    );
    assert_eq!(ks.stats().key_count, 1);
}

#[test]
fn restore_bypasses_memory_limit() {
    let config = ShardConfig {
        max_memory: Some(50), // very small
        eviction_policy: EvictionPolicy::NoEviction,
        ..ShardConfig::default()
    };
    let mut ks = Keyspace::with_config(config);

    // normal set would fail due to memory limit
    ks.restore(
        "big".into(),
        Value::String(Bytes::from("x".repeat(200))),
        None,
    );
    assert_eq!(ks.stats().key_count, 1);
}

#[test]
fn no_limit_never_rejects() {
    // default config has no memory limit
    let mut ks = Keyspace::new();
    for i in 0..100 {
        assert_eq!(
            ks.set(format!("key:{i}"), Bytes::from("value"), None, false, false),
            SetResult::Ok
        );
    }
    assert_eq!(ks.len(), 100);
}

#[test]
fn clear_removes_all_keys() {
    let mut ks = Keyspace::new();
    ks.set("a".into(), Bytes::from("1"), None, false, false);
    ks.set(
        "b".into(),
        Bytes::from("2"),
        Some(Duration::from_secs(60)),
        false,
        false,
    );
    ks.lpush("list", &[Bytes::from("x")]).unwrap();

    assert_eq!(ks.len(), 3);
    assert!(ks.stats().used_bytes > 0);
    assert_eq!(ks.stats().keys_with_expiry, 1);

    ks.clear();

    assert_eq!(ks.len(), 0);
    assert!(ks.is_empty());
    assert_eq!(ks.stats().used_bytes, 0);
    assert_eq!(ks.stats().keys_with_expiry, 0);
}

// --- scan ---

#[test]
fn scan_returns_keys() {
    let mut ks = Keyspace::new();
    ks.set("key1".into(), Bytes::from("a"), None, false, false);
    ks.set("key2".into(), Bytes::from("b"), None, false, false);
    ks.set("key3".into(), Bytes::from("c"), None, false, false);

    let (cursor, keys) = ks.scan_keys(0, 10, None);
    assert_eq!(cursor, 0); // complete in one pass
    assert_eq!(keys.len(), 3);
}

#[test]
fn scan_empty_keyspace() {
    let ks = Keyspace::new();
    let (cursor, keys) = ks.scan_keys(0, 10, None);
    assert_eq!(cursor, 0);
    assert!(keys.is_empty());
}

#[test]
fn scan_with_pattern() {
    let mut ks = Keyspace::new();
    ks.set("user:1".into(), Bytes::from("a"), None, false, false);
    ks.set("user:2".into(), Bytes::from("b"), None, false, false);
    ks.set("item:1".into(), Bytes::from("c"), None, false, false);

    let (cursor, keys) = ks.scan_keys(0, 10, Some("user:*"));
    assert_eq!(cursor, 0);
    assert_eq!(keys.len(), 2);
    for k in &keys {
        assert!(k.starts_with("user:"));
    }
}

#[test]
fn scan_with_count_limit() {
    let mut ks = Keyspace::new();
    for i in 0..10 {
        ks.set(format!("k{i}"), Bytes::from("v"), None, false, false);
    }

    // first batch
    let (cursor, keys) = ks.scan_keys(0, 3, None);
    assert!(!keys.is_empty());
    assert!(keys.len() <= 3);

    // if there are more keys, cursor should be non-zero
    if cursor != 0 {
        let (cursor2, keys2) = ks.scan_keys(cursor, 3, None);
        assert!(!keys2.is_empty());
        // continue until complete
        let _ = (cursor2, keys2);
    }
}

#[test]
fn scan_skips_expired_keys() {
    let mut ks = Keyspace::new();
    ks.set("live".into(), Bytes::from("a"), None, false, false);
    ks.set(
        "expired".into(),
        Bytes::from("b"),
        Some(Duration::from_millis(1)),
        false,
        false,
    );

    std::thread::sleep(Duration::from_millis(5));

    let (_, keys) = ks.scan_keys(0, 10, None);
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], "live");
}

// --- persist/pttl/pexpire ---

#[test]
fn persist_removes_expiry() {
    let mut ks = Keyspace::new();
    ks.set(
        "key".into(),
        Bytes::from("val"),
        Some(Duration::from_secs(60)),
        false,
        false,
    );
    assert!(matches!(ks.ttl("key"), TtlResult::Seconds(_)));

    assert!(ks.persist("key"));
    assert_eq!(ks.ttl("key"), TtlResult::NoExpiry);
    assert_eq!(ks.stats().keys_with_expiry, 0);
}

#[test]
fn persist_returns_false_without_expiry() {
    let mut ks = Keyspace::new();
    ks.set("key".into(), Bytes::from("val"), None, false, false);
    assert!(!ks.persist("key"));
}

#[test]
fn persist_returns_false_for_missing_key() {
    let mut ks = Keyspace::new();
    assert!(!ks.persist("missing"));
}

#[test]
fn pttl_returns_milliseconds() {
    let mut ks = Keyspace::new();
    ks.set(
        "key".into(),
        Bytes::from("val"),
        Some(Duration::from_secs(60)),
        false,
        false,
    );
    match ks.pttl("key") {
        TtlResult::Milliseconds(ms) => assert!(ms > 59_000 && ms <= 60_000),
        other => panic!("expected Milliseconds, got {other:?}"),
    }
}

#[test]
fn pttl_no_expiry() {
    let mut ks = Keyspace::new();
    ks.set("key".into(), Bytes::from("val"), None, false, false);
    assert_eq!(ks.pttl("key"), TtlResult::NoExpiry);
}

#[test]
fn pttl_not_found() {
    let mut ks = Keyspace::new();
    assert_eq!(ks.pttl("missing"), TtlResult::NotFound);
}

#[test]
fn pexpire_sets_ttl_in_millis() {
    let mut ks = Keyspace::new();
    ks.set("key".into(), Bytes::from("val"), None, false, false);
    assert!(ks.pexpire("key", 5000));
    match ks.pttl("key") {
        TtlResult::Milliseconds(ms) => assert!(ms > 4000 && ms <= 5000),
        other => panic!("expected Milliseconds, got {other:?}"),
    }
    assert_eq!(ks.stats().keys_with_expiry, 1);
}

#[test]
fn pexpire_missing_key_returns_false() {
    let mut ks = Keyspace::new();
    assert!(!ks.pexpire("missing", 5000));
}

#[test]
fn pexpire_overwrites_existing_ttl() {
    let mut ks = Keyspace::new();
    ks.set(
        "key".into(),
        Bytes::from("val"),
        Some(Duration::from_secs(60)),
        false,
        false,
    );
    assert!(ks.pexpire("key", 500));
    match ks.pttl("key") {
        TtlResult::Milliseconds(ms) => assert!(ms <= 500),
        other => panic!("expected Milliseconds, got {other:?}"),
    }
    // expiry count shouldn't double-count
    assert_eq!(ks.stats().keys_with_expiry, 1);
}

// --- expireat / pexpireat ---

#[test]
fn expireat_sets_expiry_on_existing_key() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut ks = Keyspace::new();
    ks.set("k".into(), Bytes::from("v"), None, false, false);
    let future_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60;
    assert!(ks.expireat("k", future_secs));
    assert!(matches!(ks.ttl("k"), TtlResult::Seconds(_)));
    assert_eq!(ks.stats().keys_with_expiry, 1);
}

#[test]
fn expireat_missing_key_returns_false() {
    let mut ks = Keyspace::new();
    assert!(!ks.expireat("missing", 9_999_999_999));
}

#[test]
fn expireat_does_not_double_count_expiry() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut ks = Keyspace::new();
    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    ks.set(
        "k".into(),
        Bytes::from("v"),
        Some(Duration::from_secs(30)),
        false,
        false,
    );
    assert_eq!(ks.stats().keys_with_expiry, 1);
    assert!(ks.expireat("k", base + 120));
    assert_eq!(ks.stats().keys_with_expiry, 1);
}

#[test]
fn pexpireat_sets_expiry_in_ms() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut ks = Keyspace::new();
    ks.set("k".into(), Bytes::from("v"), None, false, false);
    let future_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    assert!(ks.pexpireat("k", future_ms));
    assert!(matches!(ks.pttl("k"), TtlResult::Milliseconds(_)));
    assert_eq!(ks.stats().keys_with_expiry, 1);
}

#[test]
fn pexpireat_missing_key_returns_false() {
    let mut ks = Keyspace::new();
    assert!(!ks.pexpireat("missing", 9_999_999_999_000));
}

// --- keys tests ---

#[test]
fn keys_match_all() {
    let mut ks = Keyspace::new();
    ks.set("a".into(), Bytes::from("1"), None, false, false);
    ks.set("b".into(), Bytes::from("2"), None, false, false);
    ks.set("c".into(), Bytes::from("3"), None, false, false);
    let mut result = ks.keys("*");
    result.sort();
    assert_eq!(result, vec!["a", "b", "c"]);
}

#[test]
fn keys_with_pattern() {
    let mut ks = Keyspace::new();
    ks.set("user:1".into(), Bytes::from("a"), None, false, false);
    ks.set("user:2".into(), Bytes::from("b"), None, false, false);
    ks.set("item:1".into(), Bytes::from("c"), None, false, false);
    let mut result = ks.keys("user:*");
    result.sort();
    assert_eq!(result, vec!["user:1", "user:2"]);
}

#[test]
fn keys_skips_expired() {
    let mut ks = Keyspace::new();
    ks.set("live".into(), Bytes::from("a"), None, false, false);
    ks.set(
        "dead".into(),
        Bytes::from("b"),
        Some(Duration::from_millis(1)),
        false,
        false,
    );
    thread::sleep(Duration::from_millis(5));
    let result = ks.keys("*");
    assert_eq!(result, vec!["live"]);
}

#[test]
fn keys_empty_keyspace() {
    let ks = Keyspace::new();
    assert!(ks.keys("*").is_empty());
}

// --- rename tests ---

#[test]
fn rename_basic() {
    let mut ks = Keyspace::new();
    ks.set("old".into(), Bytes::from("value"), None, false, false);
    ks.rename("old", "new").unwrap();
    assert!(!ks.exists("old"));
    assert_eq!(
        ks.get("new").unwrap(),
        Some(Value::String(Bytes::from("value")))
    );
}

#[test]
fn rename_preserves_expiry() {
    let mut ks = Keyspace::new();
    ks.set(
        "old".into(),
        Bytes::from("val"),
        Some(Duration::from_secs(60)),
        false,
        false,
    );
    ks.rename("old", "new").unwrap();
    match ks.ttl("new") {
        TtlResult::Seconds(s) => assert!((58..=60).contains(&s)),
        other => panic!("expected TTL preserved, got {other:?}"),
    }
}

#[test]
fn rename_overwrites_destination() {
    let mut ks = Keyspace::new();
    ks.set("src".into(), Bytes::from("new_val"), None, false, false);
    ks.set("dst".into(), Bytes::from("old_val"), None, false, false);
    ks.rename("src", "dst").unwrap();
    assert!(!ks.exists("src"));
    assert_eq!(
        ks.get("dst").unwrap(),
        Some(Value::String(Bytes::from("new_val")))
    );
    assert_eq!(ks.len(), 1);
}

#[test]
fn rename_missing_key_returns_error() {
    let mut ks = Keyspace::new();
    let err = ks.rename("missing", "new").unwrap_err();
    assert_eq!(err, RenameError::NoSuchKey);
}

#[test]
fn rename_same_key() {
    let mut ks = Keyspace::new();
    ks.set("key".into(), Bytes::from("val"), None, false, false);
    // renaming to itself should succeed (Redis behavior)
    ks.rename("key", "key").unwrap();
    assert_eq!(
        ks.get("key").unwrap(),
        Some(Value::String(Bytes::from("val")))
    );
}

#[test]
fn rename_tracks_memory() {
    let mut ks = Keyspace::new();
    ks.set("old".into(), Bytes::from("value"), None, false, false);
    let before = ks.stats().used_bytes;
    ks.rename("old", "new").unwrap();
    let after = ks.stats().used_bytes;
    // same key length, so memory should be the same
    assert_eq!(before, after);
    assert_eq!(ks.stats().key_count, 1);
}

#[test]
fn zero_ttl_expires_immediately() {
    let mut ks = Keyspace::new();
    ks.set(
        "key".into(),
        Bytes::from("val"),
        Some(Duration::ZERO),
        false,
        false,
    );

    // key should be expired immediately
    std::thread::sleep(Duration::from_millis(1));
    assert!(ks.get("key").unwrap().is_none());
}

#[test]
fn very_small_ttl_expires_quickly() {
    let mut ks = Keyspace::new();
    ks.set(
        "key".into(),
        Bytes::from("val"),
        Some(Duration::from_millis(1)),
        false,
        false,
    );

    std::thread::sleep(Duration::from_millis(5));
    assert!(ks.get("key").unwrap().is_none());
}

#[test]
fn count_keys_in_slot_empty() {
    let ks = Keyspace::new();
    assert_eq!(ks.count_keys_in_slot(0), 0);
}

#[test]
fn count_keys_in_slot_matches() {
    let mut ks = Keyspace::new();
    // insert a few keys and count those in a specific slot
    ks.set("a".into(), Bytes::from("1"), None, false, false);
    ks.set("b".into(), Bytes::from("2"), None, false, false);
    ks.set("c".into(), Bytes::from("3"), None, false, false);

    let slot_a = ember_protocol::slots::key_slot(b"a");
    let count = ks.count_keys_in_slot(slot_a);
    // at minimum, "a" should be in its own slot
    assert!(count >= 1);
}

#[test]
fn count_keys_in_slot_skips_expired() {
    let mut ks = Keyspace::new();
    let slot = ember_protocol::slots::key_slot(b"temp");
    ks.set(
        "temp".into(),
        Bytes::from("gone"),
        Some(Duration::from_millis(0)),
        false,
        false,
    );
    // key is expired — should not be counted
    thread::sleep(Duration::from_millis(5));
    assert_eq!(ks.count_keys_in_slot(slot), 0);
}

#[test]
fn get_keys_in_slot_returns_matching() {
    let mut ks = Keyspace::new();
    ks.set("x".into(), Bytes::from("1"), None, false, false);
    ks.set("y".into(), Bytes::from("2"), None, false, false);

    let slot_x = ember_protocol::slots::key_slot(b"x");
    let keys = ks.get_keys_in_slot(slot_x, 100);
    assert!(keys.contains(&"x".to_string()));
}

#[test]
fn get_keys_in_slot_respects_count_limit() {
    let mut ks = Keyspace::new();
    // insert several keys — some might share a slot
    for i in 0..100 {
        ks.set(format!("key:{i}"), Bytes::from("v"), None, false, false);
    }
    // ask for at most 3 keys from slot 0
    let keys = ks.get_keys_in_slot(0, 3);
    assert!(keys.len() <= 3);
}

// --- key_version (WATCH support) ---
//
// Version tracking uses a lazily-populated side table. `key_version()`
// inserts the key into the table on first call (simulating WATCH).
// Subsequent mutations only bump the version if the key is tracked.

#[test]
fn key_version_returns_none_for_missing() {
    let mut ks = Keyspace::new();
    assert_eq!(ks.key_version("nope"), None);
}

#[test]
fn key_version_changes_on_set() {
    let mut ks = Keyspace::new();
    ks.set("k".into(), Bytes::from("v1"), None, false, false);
    // first call registers the key in the version table (like WATCH)
    let v1 = ks.key_version("k").expect("key should exist");
    ks.set("k".into(), Bytes::from("v2"), None, false, false);
    let v2 = ks.key_version("k").expect("key should exist");
    assert!(v2 > v1, "version should increase on overwrite");
}

#[test]
fn key_version_none_after_del() {
    let mut ks = Keyspace::new();
    ks.set("k".into(), Bytes::from("v"), None, false, false);
    assert!(ks.key_version("k").is_some());
    ks.del("k");
    assert_eq!(ks.key_version("k"), None);
}

#[test]
fn key_version_changes_on_list_push() {
    let mut ks = Keyspace::new();
    ks.lpush("list", &[Bytes::from("a")]).unwrap();
    let v1 = ks.key_version("list").expect("list should exist");
    ks.rpush("list", &[Bytes::from("b")]).unwrap();
    let v2 = ks.key_version("list").expect("list should exist");
    assert!(v2 > v1, "version should increase on rpush");
}

#[test]
fn key_version_changes_on_hash_set() {
    let mut ks = Keyspace::new();
    ks.hset("h", &[("f1".into(), Bytes::from("v1"))]).unwrap();
    let v1 = ks.key_version("h").expect("hash should exist");
    ks.hset("h", &[("f2".into(), Bytes::from("v2"))]).unwrap();
    let v2 = ks.key_version("h").expect("hash should exist");
    assert!(v2 > v1, "version should increase on hset");
}

#[test]
fn key_version_changes_on_expire() {
    let mut ks = Keyspace::new();
    ks.set("k".into(), Bytes::from("v"), None, false, false);
    let v1 = ks.key_version("k").expect("key should exist");
    ks.expire("k", 100);
    let v2 = ks.key_version("k").expect("key should exist");
    assert!(v2 > v1, "version should increase on expire");
}

#[test]
fn key_version_stable_without_watch() {
    // if key_version is never called, mutations don't create
    // version entries — the side table stays empty
    let mut ks = Keyspace::new();
    ks.set("a".into(), Bytes::from("1"), None, false, false);
    ks.set("a".into(), Bytes::from("2"), None, false, false);
    // first call to key_version returns a snapshot
    let v1 = ks.key_version("a").unwrap();
    // no mutation between calls — version is stable
    let v2 = ks.key_version("a").unwrap();
    assert_eq!(v1, v2, "version should be stable without mutations");
}

// --- copy tests ---

#[test]
fn copy_basic() {
    let mut ks = Keyspace::new();
    ks.set("src".into(), Bytes::from("hello"), None, false, false);
    assert_eq!(ks.copy("src", "dst", false), Ok(true));
    assert_eq!(
        ks.get("dst").unwrap(),
        Some(Value::String(Bytes::from("hello")))
    );
    // source should still exist
    assert!(ks.exists("src"));
}

#[test]
fn copy_preserves_expiry() {
    let mut ks = Keyspace::new();
    ks.set(
        "src".into(),
        Bytes::from("val"),
        Some(Duration::from_secs(60)),
        false,
        false,
    );
    assert_eq!(ks.copy("src", "dst", false), Ok(true));
    match ks.ttl("dst") {
        TtlResult::Seconds(s) => assert!((58..=60).contains(&s)),
        other => panic!("expected TTL preserved, got {other:?}"),
    }
}

#[test]
fn copy_no_replace_returns_false() {
    let mut ks = Keyspace::new();
    ks.set("src".into(), Bytes::from("a"), None, false, false);
    ks.set("dst".into(), Bytes::from("b"), None, false, false);
    assert_eq!(ks.copy("src", "dst", false), Ok(false));
    // destination should be unchanged
    assert_eq!(
        ks.get("dst").unwrap(),
        Some(Value::String(Bytes::from("b")))
    );
}

#[test]
fn copy_replace_overwrites() {
    let mut ks = Keyspace::new();
    ks.set("src".into(), Bytes::from("new"), None, false, false);
    ks.set("dst".into(), Bytes::from("old"), None, false, false);
    assert_eq!(ks.copy("src", "dst", true), Ok(true));
    assert_eq!(
        ks.get("dst").unwrap(),
        Some(Value::String(Bytes::from("new")))
    );
}

#[test]
fn copy_missing_source() {
    let mut ks = Keyspace::new();
    assert_eq!(ks.copy("missing", "dst", false), Err(CopyError::NoSuchKey));
}

#[test]
fn copy_tracks_memory() {
    let mut ks = Keyspace::new();
    ks.set("src".into(), Bytes::from("value"), None, false, false);
    let before = ks.stats().used_bytes;
    ks.copy("src", "dst", false).unwrap();
    let after = ks.stats().used_bytes;
    // memory should roughly double (two entries with same value)
    assert!(after > before);
    assert_eq!(ks.stats().key_count, 2);
}

// --- random_key ---

#[test]
fn random_key_empty() {
    let mut ks = Keyspace::new();
    assert_eq!(ks.random_key(), None);
}

#[test]
fn random_key_returns_existing() {
    let mut ks = Keyspace::new();
    ks.set("only".into(), Bytes::from("val"), None, false, false);
    assert_eq!(ks.random_key(), Some("only".into()));
}

// --- touch ---

#[test]
fn touch_existing_key() {
    let mut ks = Keyspace::new();
    ks.set("k".into(), Bytes::from("v"), None, false, false);
    assert!(ks.touch("k"));
}

#[test]
fn touch_missing_key() {
    let mut ks = Keyspace::new();
    assert!(!ks.touch("missing"));
}

// --- sort ---

#[test]
fn sort_list_numeric() {
    let mut ks = Keyspace::new();
    let _ = ks.lpush(
        "nums",
        &[Bytes::from("3"), Bytes::from("1"), Bytes::from("2")],
    );
    let result = ks.sort("nums", false, false, None).unwrap();
    assert_eq!(
        result,
        vec![Bytes::from("1"), Bytes::from("2"), Bytes::from("3")]
    );
}

#[test]
fn sort_list_alpha_desc() {
    let mut ks = Keyspace::new();
    let _ = ks.lpush(
        "words",
        &[
            Bytes::from("banana"),
            Bytes::from("apple"),
            Bytes::from("cherry"),
        ],
    );
    let result = ks.sort("words", true, true, None).unwrap();
    assert_eq!(
        result,
        vec![
            Bytes::from("cherry"),
            Bytes::from("banana"),
            Bytes::from("apple")
        ]
    );
}

#[test]
fn sort_with_limit() {
    let mut ks = Keyspace::new();
    let _ = ks.lpush(
        "nums",
        &[
            Bytes::from("4"),
            Bytes::from("3"),
            Bytes::from("2"),
            Bytes::from("1"),
        ],
    );
    let result = ks.sort("nums", false, false, Some((1, 2))).unwrap();
    assert_eq!(result, vec![Bytes::from("2"), Bytes::from("3")]);
}

#[test]
fn sort_set_alpha() {
    let mut ks = Keyspace::new();
    let members: Vec<String> = vec!["c".into(), "a".into(), "b".into()];
    let _ = ks.sadd("myset", &members);
    let result = ks.sort("myset", false, true, None).unwrap();
    assert_eq!(
        result,
        vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]
    );
}

#[test]
fn sort_missing_key() {
    let mut ks = Keyspace::new();
    let result = ks.sort("nope", false, false, None).unwrap();
    assert!(result.is_empty());
}

#[test]
fn sort_wrong_type() {
    let mut ks = Keyspace::new();
    ks.set("str".into(), Bytes::from("hello"), None, false, false);
    let result = ks.sort("str", false, false, None);
    assert!(result.is_err());
}
