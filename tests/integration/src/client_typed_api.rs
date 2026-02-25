//! Integration tests for the typed ember-client API.
//!
//! Each test spins up a real server subprocess via `TestServer::start()` and
//! connects with the typed `Client`. These tests exercise the full stack —
//! TCP, RESP3 framing, command dispatch, and response decoding.

use ember_client::{Client, ClientError, Pipeline};

use crate::helpers::TestServer;

// Helper: connect a typed client to a freshly started server.
async fn connect() -> (TestServer, Client) {
    let server = TestServer::start();
    let client = Client::connect("127.0.0.1", server.port)
        .await
        .expect("connect failed");
    (server, client)
}

// --- string commands ---

#[tokio::test]
async fn get_set_roundtrip() {
    let (_server, mut client) = connect().await;
    client.set("greeting", "hello").await.unwrap();
    let val = client.get("greeting").await.unwrap();
    assert_eq!(val.as_deref(), Some(b"hello" as &[u8]));
}

#[tokio::test]
async fn get_missing_key_returns_none() {
    let (_server, mut client) = connect().await;
    let val = client.get("no_such_key").await.unwrap();
    assert!(val.is_none());
}

#[tokio::test]
async fn del_removes_keys_and_returns_count() {
    let (_server, mut client) = connect().await;
    client.set("k1", "a").await.unwrap();
    client.set("k2", "b").await.unwrap();
    let removed = client.del(&["k1", "k2", "k3"]).await.unwrap();
    assert_eq!(removed, 2); // k3 did not exist
    assert!(client.get("k1").await.unwrap().is_none());
}

#[tokio::test]
async fn incr_decr_sequencing() {
    let (_server, mut client) = connect().await;
    client.set("counter", "10").await.unwrap();
    assert_eq!(client.incr("counter").await.unwrap(), 11);
    assert_eq!(client.incrby("counter", 4).await.unwrap(), 15);
    assert_eq!(client.decr("counter").await.unwrap(), 14);
    assert_eq!(client.decrby("counter", 4).await.unwrap(), 10);
}

#[tokio::test]
async fn mget_with_missing_key() {
    let (_server, mut client) = connect().await;
    client.set("present", "yes").await.unwrap();
    let vals = client.mget(&["present", "absent"]).await.unwrap();
    assert_eq!(vals.len(), 2);
    assert_eq!(vals[0].as_deref(), Some(b"yes" as &[u8]));
    assert!(vals[1].is_none());
}

#[tokio::test]
async fn mset_then_mget() {
    let (_server, mut client) = connect().await;
    client
        .mset(&[("x", "1"), ("y", "2"), ("z", "3")])
        .await
        .unwrap();
    let vals = client.mget(&["x", "y", "z"]).await.unwrap();
    assert_eq!(vals[0].as_deref(), Some(b"1" as &[u8]));
    assert_eq!(vals[1].as_deref(), Some(b"2" as &[u8]));
    assert_eq!(vals[2].as_deref(), Some(b"3" as &[u8]));
}

#[tokio::test]
async fn getdel_removes_key() {
    let (_server, mut client) = connect().await;
    client.set("temp", "value").await.unwrap();
    let got = client.getdel("temp").await.unwrap();
    assert_eq!(got.as_deref(), Some(b"value" as &[u8]));
    assert!(client.get("temp").await.unwrap().is_none());
}

#[tokio::test]
async fn expire_and_ttl() {
    let (_server, mut client) = connect().await;
    client.set("expiring", "soon").await.unwrap();
    let set = client.expire("expiring", 60).await.unwrap();
    assert!(set);
    let ttl = client.ttl("expiring").await.unwrap();
    assert!(ttl > 0 && ttl <= 60);
}

// --- list commands ---

#[tokio::test]
async fn lpush_rpush_lrange() {
    let (_server, mut client) = connect().await;
    client.rpush("mylist", &["a", "b", "c"]).await.unwrap();
    let items = client.lrange("mylist", 0, -1).await.unwrap();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].as_ref(), b"a");
    assert_eq!(items[2].as_ref(), b"c");
}

#[tokio::test]
async fn lpop_rpop() {
    let (_server, mut client) = connect().await;
    client.rpush("q", &["first", "second"]).await.unwrap();
    let head = client.lpop("q").await.unwrap();
    let tail = client.rpop("q").await.unwrap();
    assert_eq!(head.as_deref(), Some(b"first" as &[u8]));
    assert_eq!(tail.as_deref(), Some(b"second" as &[u8]));
    assert!(client.lpop("q").await.unwrap().is_none());
}

// --- hash commands ---

#[tokio::test]
async fn hset_hgetall_roundtrip() {
    let (_server, mut client) = connect().await;
    let added = client
        .hset("user:1", &[("name", "alice"), ("age", "30")])
        .await
        .unwrap();
    assert_eq!(added, 2);

    let mut all = client.hgetall("user:1").await.unwrap();
    // order is not guaranteed — sort by field name for comparison
    all.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].0.as_ref(), b"age");
    assert_eq!(all[0].1.as_ref(), b"30");
    assert_eq!(all[1].0.as_ref(), b"name");
    assert_eq!(all[1].1.as_ref(), b"alice");
}

#[tokio::test]
async fn hget_missing_field() {
    let (_server, mut client) = connect().await;
    client.hset("h", &[("exists", "yes")]).await.unwrap();
    assert!(client.hget("h", "missing").await.unwrap().is_none());
}

// --- hincrby ---

#[tokio::test]
async fn hincrby_increments_and_returns_new_value() {
    let (_server, mut client) = connect().await;
    client.hset("counter", &[("hits", "10")]).await.unwrap();
    assert_eq!(client.hincrby("counter", "hits", 5).await.unwrap(), 15);
    assert_eq!(client.hincrby("counter", "hits", -3).await.unwrap(), 12);
    // field created on first call if missing
    assert_eq!(client.hincrby("counter", "new_field", 1).await.unwrap(), 1);
}

// --- sorted set commands ---

#[tokio::test]
async fn zadd_zrange_zscore() {
    let (_server, mut client) = connect().await;
    client
        .zadd("scores", &[(1.0, "alice"), (2.0, "bob"), (3.0, "carol")])
        .await
        .unwrap();

    let members = client.zrange("scores", 0, -1).await.unwrap();
    assert_eq!(members.len(), 3);
    assert_eq!(members[0].as_ref(), b"alice");

    let score = client.zscore("scores", "bob").await.unwrap();
    assert_eq!(score, Some(2.0));
}

#[tokio::test]
async fn zscore_missing_member_returns_none() {
    let (_server, mut client) = connect().await;
    client.zadd("zs", &[(1.0, "only")]).await.unwrap();
    let score = client.zscore("zs", "nobody").await.unwrap();
    assert!(score.is_none());
}

#[tokio::test]
async fn zrank_present_and_absent() {
    let (_server, mut client) = connect().await;
    client
        .zadd("ranking", &[(10.0, "a"), (20.0, "b")])
        .await
        .unwrap();
    assert_eq!(client.zrank("ranking", "a").await.unwrap(), Some(0));
    assert_eq!(client.zrank("ranking", "b").await.unwrap(), Some(1));
    assert!(client.zrank("ranking", "nobody").await.unwrap().is_none());
}

// --- pipeline ---

#[tokio::test]
async fn pipeline_multiple_gets() {
    let (_server, mut client) = connect().await;
    client.set("p1", "v1").await.unwrap();
    client.set("p2", "v2").await.unwrap();
    client.set("p3", "v3").await.unwrap();

    let frames = client
        .execute_pipeline(Pipeline::new().get("p1").get("p2").get("p3"))
        .await
        .unwrap();

    assert_eq!(frames.len(), 3);
}

#[tokio::test]
async fn pipeline_empty_returns_empty_vec() {
    let (_server, mut client) = connect().await;
    let frames = client.execute_pipeline(Pipeline::new()).await.unwrap();
    assert!(frames.is_empty());
}

#[tokio::test]
async fn pipeline_mixed_commands() {
    let (_server, mut client) = connect().await;

    let frames = client
        .execute_pipeline(
            Pipeline::new()
                .set("batch_key", "batch_value")
                .get("batch_key")
                .incr("batch_counter"),
        )
        .await
        .unwrap();

    assert_eq!(frames.len(), 3);
}

// --- strlen ---

#[tokio::test]
async fn strlen_existing_and_missing() {
    let (_server, mut client) = connect().await;
    client.set("slen", "hello").await.unwrap();
    assert_eq!(client.strlen("slen").await.unwrap(), 5);
    assert_eq!(client.strlen("nosuchkey").await.unwrap(), 0);
}

// --- incr_by_float ---

#[tokio::test]
async fn incr_by_float_positive_and_negative() {
    let (_server, mut client) = connect().await;
    client.set("flt", "10.5").await.unwrap();
    let v = client.incr_by_float("flt", 1.5).await.unwrap();
    assert!((v - 12.0).abs() < 1e-9);
    let v2 = client.incr_by_float("flt", -2.0).await.unwrap();
    assert!((v2 - 10.0).abs() < 1e-9);
}

// --- pexpire ---

#[tokio::test]
async fn pexpire_sets_ttl_in_millis() {
    let (_server, mut client) = connect().await;
    client.set("px_key", "v").await.unwrap();
    let set = client.pexpire("px_key", 60_000).await.unwrap();
    assert!(set);
    let pttl = client.pttl("px_key").await.unwrap();
    assert!(pttl > 0 && pttl <= 60_000);
}

// --- hmget ---

#[tokio::test]
async fn hmget_subset_and_missing() {
    let (_server, mut client) = connect().await;
    client
        .hset("hm", &[("a", "1"), ("b", "2"), ("c", "3")])
        .await
        .unwrap();
    let vals = client.hmget("hm", &["a", "c", "missing"]).await.unwrap();
    assert_eq!(vals.len(), 3);
    assert_eq!(vals[0].as_deref(), Some(b"1" as &[u8]));
    assert_eq!(vals[1].as_deref(), Some(b"3" as &[u8]));
    assert!(vals[2].is_none());
}

// --- key_type ---

#[tokio::test]
async fn key_type_returns_correct_types() {
    let (_server, mut client) = connect().await;
    client.set("str_key", "v").await.unwrap();
    assert_eq!(client.key_type("str_key").await.unwrap(), "string");

    client.rpush("lst_key", &["item"]).await.unwrap();
    assert_eq!(client.key_type("lst_key").await.unwrap(), "list");

    assert_eq!(client.key_type("no_such_key").await.unwrap(), "none");
}

// --- keys ---

#[tokio::test]
async fn keys_glob_returns_matching_keys() {
    let (_server, mut client) = connect().await;
    client.set("pfx:a", "1").await.unwrap();
    client.set("pfx:b", "2").await.unwrap();
    client.set("other", "3").await.unwrap();
    let mut matched = client.keys("pfx:*").await.unwrap();
    matched.sort();
    assert_eq!(matched.len(), 2);
    assert_eq!(matched[0].as_ref(), b"pfx:a");
    assert_eq!(matched[1].as_ref(), b"pfx:b");
}

// --- rename ---

#[tokio::test]
async fn rename_then_old_key_gone() {
    let (_server, mut client) = connect().await;
    client.set("old_name", "value").await.unwrap();
    client.rename("old_name", "new_name").await.unwrap();
    assert!(client.get("old_name").await.unwrap().is_none());
    assert_eq!(
        client.get("new_name").await.unwrap().as_deref(),
        Some(b"value" as &[u8])
    );
}

// --- scan ---

#[tokio::test]
async fn scan_full_iteration_collects_all_keys() {
    let (_server, mut client) = connect().await;
    let expected_keys = ["scan_a", "scan_b", "scan_c"];
    for k in &expected_keys {
        client.set(k, "v").await.unwrap();
    }

    let mut all_keys = Vec::new();
    let mut cursor = 0u64;
    loop {
        let page = client.scan(cursor, None, Some("scan_*")).await.unwrap();
        all_keys.extend(page.keys);
        cursor = page.cursor;
        if cursor == 0 {
            break;
        }
    }

    all_keys.sort();
    assert_eq!(all_keys.len(), 3);
    assert_eq!(all_keys[0].as_ref(), b"scan_a");
    assert_eq!(all_keys[2].as_ref(), b"scan_c");
}

// --- echo ---

#[tokio::test]
async fn echo_returns_same_bytes() {
    let (_server, mut client) = connect().await;
    let reply = client.echo("hello world").await.unwrap();
    assert_eq!(reply.as_ref(), b"hello world");
}

// --- unlink ---

#[tokio::test]
async fn unlink_returns_count_like_del() {
    let (_server, mut client) = connect().await;
    client.set("ul1", "v").await.unwrap();
    client.set("ul2", "v").await.unwrap();
    let removed = client.unlink(&["ul1", "ul2", "ul_missing"]).await.unwrap();
    assert_eq!(removed, 2);
    assert!(client.get("ul1").await.unwrap().is_none());
}

// --- info ---

#[tokio::test]
async fn info_returns_non_empty_string() {
    let (_server, mut client) = connect().await;
    let output = client.info(None).await.unwrap();
    assert!(!output.is_empty());
}

// --- bgsave ---

#[tokio::test]
async fn bgsave_returns_status_string() {
    let (_server, mut client) = connect().await;
    let status = client.bgsave().await.unwrap();
    assert!(!status.is_empty());
}

// --- slowlog ---

#[tokio::test]
async fn slowlog_len_is_non_negative() {
    let (_server, mut client) = connect().await;
    let len = client.slowlog_len().await.unwrap();
    assert!(len >= 0);
}

#[tokio::test]
async fn slowlog_reset_clears_log() {
    let (_server, mut client) = connect().await;
    client.slowlog_reset().await.unwrap();
    assert_eq!(client.slowlog_len().await.unwrap(), 0);
}

// --- pub/sub (request-response) ---

#[tokio::test]
async fn publish_to_channel_with_no_subscribers_returns_zero() {
    let (_server, mut client) = connect().await;
    let count = client.publish("unsubscribed_channel", "msg").await.unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn pubsub_channels_with_pattern_and_none() {
    let (_server, mut client) = connect().await;
    // no subscribers active — both should return empty
    let with_pat = client.pubsub_channels(Some("*")).await.unwrap();
    let all = client.pubsub_channels(None).await.unwrap();
    assert!(with_pat.is_empty());
    assert!(all.is_empty());
}

#[tokio::test]
async fn pubsub_numsub_empty_channels() {
    let (_server, mut client) = connect().await;
    let pairs = client.pubsub_numsub(&["no_one_here"]).await.unwrap();
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].0.as_ref(), b"no_one_here");
    assert_eq!(pairs[0].1, 0);
}

#[tokio::test]
async fn pubsub_numpat_is_zero_before_subscriptions() {
    let (_server, mut client) = connect().await;
    let n = client.pubsub_numpat().await.unwrap();
    assert_eq!(n, 0);
}

// --- subscriber ---

#[tokio::test]
async fn subscriber_recv_message() {
    let server = crate::helpers::TestServer::start();
    let sub_conn = Client::connect("127.0.0.1", server.port)
        .await
        .expect("connect failed");
    let mut publisher = Client::connect("127.0.0.1", server.port)
        .await
        .expect("connect failed");

    let mut sub = sub_conn.subscribe(&["typed_events"]).await.unwrap();
    publisher.publish("typed_events", "hello").await.unwrap();

    let msg = sub.recv().await.unwrap();
    assert_eq!(msg.channel.as_ref(), b"typed_events");
    assert_eq!(msg.data.as_ref(), b"hello");
    assert!(msg.pattern.is_none());
}

#[tokio::test]
async fn subscriber_multiple_channels_correct_message() {
    let server = crate::helpers::TestServer::start();
    let sub_conn = Client::connect("127.0.0.1", server.port)
        .await
        .expect("connect failed");
    let mut publisher = Client::connect("127.0.0.1", server.port)
        .await
        .expect("connect failed");

    let mut sub = sub_conn.subscribe(&["ch_a", "ch_b"]).await.unwrap();
    publisher.publish("ch_b", "payload").await.unwrap();

    let msg = sub.recv().await.unwrap();
    assert_eq!(msg.channel.as_ref(), b"ch_b");
    assert_eq!(msg.data.as_ref(), b"payload");
}

#[tokio::test]
async fn psubscriber_pattern_match() {
    let server = crate::helpers::TestServer::start();
    let sub_conn = Client::connect("127.0.0.1", server.port)
        .await
        .expect("connect failed");
    let mut publisher = Client::connect("127.0.0.1", server.port)
        .await
        .expect("connect failed");

    let mut sub = sub_conn.psubscribe(&["typed:*"]).await.unwrap();
    publisher.publish("typed:update", "data").await.unwrap();

    let msg = sub.recv().await.unwrap();
    assert_eq!(msg.channel.as_ref(), b"typed:update");
    assert_eq!(msg.data.as_ref(), b"data");
    assert_eq!(msg.pattern.as_deref(), Some(b"typed:*" as &[u8]));
}

// --- error surfacing ---

#[tokio::test]
async fn wrongtype_error_surfaces_as_server_error() {
    let (_server, mut client) = connect().await;

    // Store a list then try to GET it — server returns WRONGTYPE
    client.rpush("mylist", &["item"]).await.unwrap();

    let err = client.get("mylist").await.unwrap_err();
    assert!(
        matches!(err, ClientError::Server(_)),
        "expected Server error, got {err:?}"
    );
}
