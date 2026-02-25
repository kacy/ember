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
