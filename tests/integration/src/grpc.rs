//! Integration tests for the gRPC API.
//!
//! These pin down the responses and status codes each RPC returns today,
//! so changes to the handlers can be checked against them.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};

use crate::helpers::{ServerOptions, TestServer};

mod proto {
    tonic::include_proto!("ember.v1");
}

use proto::ember_cache_client::EmberCacheClient;
use proto::pipeline_request::Command as PipeCmd;
use proto::pipeline_response::Result as PipeResult;
use proto::*;

type Client = EmberCacheClient<Channel>;

/// Starts a single-shard server with gRPC enabled and connects a client.
/// One shard keeps multi-key commands (stores, moves, BITOP) on one shard.
async fn start() -> (TestServer, Client) {
    start_with(ServerOptions {
        shards: Some(1),
        ..Default::default()
    })
    .await
}

async fn start_with(opts: ServerOptions) -> (TestServer, Client) {
    let server = TestServer::start_with(ServerOptions { grpc: true, ..opts });
    let client = connect(&server).await;
    (server, client)
}

async fn connect(server: &TestServer) -> Client {
    let port = server.grpc_port.expect("server started without gRPC");
    EmberCacheClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .expect("failed to connect gRPC client")
}

/// Returns the status code of a failed call, panicking on success.
fn code<T: std::fmt::Debug>(result: Result<Response<T>, Status>) -> Code {
    match result {
        Ok(resp) => panic!("expected an error, got {:?}", resp.into_inner()),
        Err(status) => status.code(),
    }
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

async fn set(c: &mut Client, key: &str, value: &str) {
    let resp = c
        .set(SetRequest {
            key: key.into(),
            value: b(value),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(resp.into_inner().ok);
}

async fn get(c: &mut Client, key: &str) -> Option<Vec<u8>> {
    c.get(GetRequest { key: key.into() })
        .await
        .unwrap()
        .into_inner()
        .value
}

async fn rpush(c: &mut Client, key: &str, values: &[&str]) -> i64 {
    c.r_push(RPushRequest {
        key: key.into(),
        values: values.iter().map(|v| b(v)).collect(),
    })
    .await
    .unwrap()
    .into_inner()
    .value
}

async fn sadd(c: &mut Client, key: &str, members: &[&str]) -> i64 {
    c.s_add(SAddRequest {
        key: key.into(),
        members: strs(members),
    })
    .await
    .unwrap()
    .into_inner()
    .value
}

async fn smembers(c: &mut Client, key: &str) -> Vec<String> {
    let keys = c
        .s_members(SMembersRequest { key: key.into() })
        .await
        .unwrap()
        .into_inner()
        .keys;
    sorted(keys)
}

async fn zadd(c: &mut Client, key: &str, members: &[(f64, &str)]) -> i64 {
    c.z_add(ZAddRequest {
        key: key.into(),
        members: members
            .iter()
            .map(|(score, member)| ScoreMember {
                score: *score,
                member: member.to_string(),
            })
            .collect(),
        ..Default::default()
    })
    .await
    .unwrap()
    .into_inner()
    .value
}

async fn hset(c: &mut Client, key: &str, fields: &[(&str, &str)]) -> i64 {
    c.h_set(HSetRequest {
        key: key.into(),
        fields: fields
            .iter()
            .map(|(f, v)| FieldValue {
                field: f.to_string(),
                value: b(v),
            })
            .collect(),
    })
    .await
    .unwrap()
    .into_inner()
    .value
}

async fn ttl(c: &mut Client, key: &str) -> i64 {
    c.ttl(TtlRequest { key: key.into() })
        .await
        .unwrap()
        .into_inner()
        .value
}

fn members(resp: ZRangeResponse) -> Vec<(String, f64)> {
    resp.members
        .into_iter()
        .map(|m| (m.member, m.score))
        .collect()
}

fn names(resp: ZRangeResponse) -> Vec<String> {
    resp.members.into_iter().map(|m| m.member).collect()
}

// ---------------------------------------------------------------------------
// strings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grpc_set_get_del() {
    let (_s, mut c) = start().await;

    set(&mut c, "k", "v").await;
    assert_eq!(get(&mut c, "k").await, Some(b("v")));
    assert_eq!(get(&mut c, "missing").await, None);

    let resp = c
        .del(DelRequest {
            keys: strs(&["k", "missing"]),
        })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().deleted, 1);
    assert_eq!(get(&mut c, "k").await, None);
}

#[tokio::test]
async fn grpc_set_nx_xx_and_expiry() {
    let (_s, mut c) = start().await;

    let mut req = SetRequest {
        key: "k".into(),
        value: b("1"),
        xx: true,
        ..Default::default()
    };
    // XX on a missing key does nothing
    assert!(!c.set(req.clone()).await.unwrap().into_inner().ok);
    assert_eq!(get(&mut c, "k").await, None);

    req.xx = false;
    req.nx = true;
    assert!(c.set(req.clone()).await.unwrap().into_inner().ok);
    // NX on an existing key does nothing
    req.value = b("2");
    assert!(!c.set(req).await.unwrap().into_inner().ok);
    assert_eq!(get(&mut c, "k").await, Some(b("1")));

    let req = SetRequest {
        key: "e".into(),
        value: b("v"),
        expire_seconds: 100,
        ..Default::default()
    };
    assert!(c.set(req).await.unwrap().into_inner().ok);
    assert!((99..=100).contains(&ttl(&mut c, "e").await));

    let req = SetRequest {
        key: "pe".into(),
        value: b("v"),
        expire_millis: 50_000,
        ..Default::default()
    };
    assert!(c.set(req).await.unwrap().into_inner().ok);
    assert!((49..=50).contains(&ttl(&mut c, "pe").await));
}

#[tokio::test]
async fn grpc_empty_key_is_invalid_argument() {
    let (_s, mut c) = start().await;
    let result = c.get(GetRequest { key: String::new() }).await;
    assert_eq!(code(result), Code::InvalidArgument);
}

#[tokio::test]
async fn grpc_mget_mset() {
    let (_s, mut c) = start().await;

    c.m_set(MSetRequest {
        pairs: vec![
            KeyValue {
                key: "a".into(),
                value: b("1"),
            },
            KeyValue {
                key: "b".into(),
                value: b("2"),
            },
        ],
    })
    .await
    .unwrap();

    let values: Vec<Option<Vec<u8>>> = c
        .m_get(MGetRequest {
            keys: strs(&["a", "missing", "b"]),
        })
        .await
        .unwrap()
        .into_inner()
        .values
        .into_iter()
        .map(|v| v.value)
        .collect();
    assert_eq!(values, vec![Some(b("1")), None, Some(b("2"))]);
}

#[tokio::test]
async fn grpc_msetnx() {
    let (_s, mut c) = start().await;
    let pair = |k: &str| KeyValue {
        key: k.into(),
        value: b("v"),
    };

    let resp = c
        .msetnx(MsetnxRequest {
            pairs: vec![pair("a"), pair("b")],
        })
        .await
        .unwrap();
    assert!(resp.into_inner().value);

    // one key already exists, so nothing is set
    let resp = c
        .msetnx(MsetnxRequest {
            pairs: vec![pair("b"), pair("c")],
        })
        .await
        .unwrap();
    assert!(!resp.into_inner().value);
    assert_eq!(get(&mut c, "c").await, None);

    let result = c.msetnx(MsetnxRequest { pairs: vec![] }).await;
    assert_eq!(code(result), Code::InvalidArgument);
}

#[tokio::test]
async fn grpc_incr_decr_family() {
    let (_s, mut c) = start().await;
    let key = || "n".to_string();

    let v = c.incr(IncrRequest { key: key() }).await.unwrap();
    assert_eq!(v.into_inner().value, 1);
    let v = c
        .incr_by(IncrByRequest {
            key: key(),
            delta: 5,
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, 6);
    let v = c
        .decr_by(DecrByRequest {
            key: key(),
            delta: 2,
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, 4);
    let v = c.decr(DecrRequest { key: key() }).await.unwrap();
    assert_eq!(v.into_inner().value, 3);

    let v = c
        .incr_by_float(IncrByFloatRequest {
            key: key(),
            delta: 1.5,
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, "4.5");
    assert_eq!(get(&mut c, "n").await, Some(b("4.5")));
}

#[tokio::test]
async fn grpc_append_strlen() {
    let (_s, mut c) = start().await;

    let n = c
        .append(AppendRequest {
            key: "k".into(),
            value: b("Hello"),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 5);
    let n = c
        .append(AppendRequest {
            key: "k".into(),
            value: b(" World"),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 11);

    let n = c.strlen(StrlenRequest { key: "k".into() }).await.unwrap();
    assert_eq!(n.into_inner().value, 11);
    let n = c
        .strlen(StrlenRequest {
            key: "missing".into(),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 0);
}

#[tokio::test]
async fn grpc_getdel_getset() {
    let (_s, mut c) = start().await;
    set(&mut c, "k", "v").await;

    let v = c.get_del(GetDelRequest { key: "k".into() }).await.unwrap();
    assert_eq!(v.into_inner().value, Some(b("v")));
    assert_eq!(get(&mut c, "k").await, None);
    let v = c.get_del(GetDelRequest { key: "k".into() }).await.unwrap();
    assert_eq!(v.into_inner().value, None);

    let v = c
        .getset(GetsetRequest {
            key: "g".into(),
            value: b("1"),
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, None);
    let v = c
        .getset(GetsetRequest {
            key: "g".into(),
            value: b("2"),
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, Some(b("1")));
    assert_eq!(get(&mut c, "g").await, Some(b("2")));
}

#[tokio::test]
async fn grpc_getex() {
    let (_s, mut c) = start().await;
    set(&mut c, "k", "v").await;

    let v = c
        .get_ex(GetExRequest {
            key: "k".into(),
            expire_seconds: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, Some(b("v")));
    assert!((99..=100).contains(&ttl(&mut c, "k").await));

    let v = c
        .get_ex(GetExRequest {
            key: "k".into(),
            persist: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, Some(b("v")));
    assert_eq!(ttl(&mut c, "k").await, -1);

    let v = c
        .get_ex(GetExRequest {
            key: "missing".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, None);
}

#[tokio::test]
async fn grpc_getrange_setrange() {
    let (_s, mut c) = start().await;
    set(&mut c, "k", "Hello World").await;

    let v = c
        .get_range(GetRangeRequest {
            key: "k".into(),
            start: 0,
            end: 4,
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, Some(b("Hello")));

    let n = c
        .set_range(SetRangeRequest {
            key: "k".into(),
            offset: 6,
            value: b("Ember"),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 11);
    assert_eq!(get(&mut c, "k").await, Some(b("Hello Ember")));

    let result = c
        .set_range(SetRangeRequest {
            key: "k".into(),
            offset: -1,
            value: b("x"),
        })
        .await;
    assert_eq!(code(result), Code::InvalidArgument);
}

#[tokio::test]
async fn grpc_bitmaps() {
    let (_s, mut c) = start().await;

    let setbit = |offset, value| SetbitRequest {
        key: "bits".into(),
        offset,
        value,
    };
    assert_eq!(c.setbit(setbit(7, 1)).await.unwrap().into_inner().value, 0);
    assert_eq!(c.setbit(setbit(0, 1)).await.unwrap().into_inner().value, 0);
    // returns the previous bit
    assert_eq!(c.setbit(setbit(0, 1)).await.unwrap().into_inner().value, 1);
    assert_eq!(get(&mut c, "bits").await, Some(vec![0x81]));
    assert_eq!(code(c.setbit(setbit(1, 2)).await), Code::InvalidArgument);

    let getbit = |offset| GetbitRequest {
        key: "bits".into(),
        offset,
    };
    assert_eq!(c.getbit(getbit(7)).await.unwrap().into_inner().value, 1);
    assert_eq!(c.getbit(getbit(3)).await.unwrap().into_inner().value, 0);

    let count = c
        .bitcount(BitcountRequest {
            key: "bits".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(count.into_inner().value, 2);
    let count = c
        .bitcount(BitcountRequest {
            key: "bits".into(),
            has_range: true,
            start: 1,
            end: 6,
            unit: "BIT".into(),
        })
        .await
        .unwrap();
    assert_eq!(count.into_inner().value, 0);

    let pos = |bit| BitposRequest {
        key: "bits".into(),
        bit,
        ..Default::default()
    };
    assert_eq!(c.bitpos(pos(1)).await.unwrap().into_inner().value, 0);
    assert_eq!(c.bitpos(pos(0)).await.unwrap().into_inner().value, 1);
}

#[tokio::test]
async fn grpc_bitop() {
    let (_s, mut c) = start().await;
    c.m_set(MSetRequest {
        pairs: vec![
            KeyValue {
                key: "a".into(),
                value: vec![0b1100],
            },
            KeyValue {
                key: "b".into(),
                value: vec![0b1010],
            },
        ],
    })
    .await
    .unwrap();

    let op = |op: &str| BitopRequest {
        op: op.into(),
        dest: "d".into(),
        keys: strs(&["a", "b"]),
    };
    assert_eq!(c.bitop(op("AND")).await.unwrap().into_inner().value, 1);
    assert_eq!(get(&mut c, "d").await, Some(vec![0b1000]));
    assert_eq!(c.bitop(op("or")).await.unwrap().into_inner().value, 1);
    assert_eq!(get(&mut c, "d").await, Some(vec![0b1110]));
    assert_eq!(c.bitop(op("XOR")).await.unwrap().into_inner().value, 1);
    assert_eq!(get(&mut c, "d").await, Some(vec![0b0110]));
    assert_eq!(code(c.bitop(op("NAND")).await), Code::InvalidArgument);
}

// ---------------------------------------------------------------------------
// keyspace
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grpc_exists_touch_unlink() {
    let (_s, mut c) = start().await;
    set(&mut c, "a", "1").await;
    set(&mut c, "b", "2").await;

    let keys = || strs(&["a", "b", "missing"]);
    let n = c.exists(ExistsRequest { keys: keys() }).await.unwrap();
    assert_eq!(n.into_inner().value, 2);
    let n = c.touch(TouchRequest { keys: keys() }).await.unwrap();
    assert_eq!(n.into_inner().value, 2);
    let n = c.unlink(UnlinkRequest { keys: keys() }).await.unwrap();
    assert_eq!(n.into_inner().deleted, 2);
    let n = c.exists(ExistsRequest { keys: keys() }).await.unwrap();
    assert_eq!(n.into_inner().value, 0);
}

#[tokio::test]
async fn grpc_expire_ttl_persist() {
    let (_s, mut c) = start().await;
    set(&mut c, "k", "v").await;

    assert_eq!(ttl(&mut c, "k").await, -1);
    assert_eq!(ttl(&mut c, "missing").await, -2);

    let ok = c
        .expire(ExpireRequest {
            key: "k".into(),
            seconds: 100,
        })
        .await
        .unwrap();
    assert!(ok.into_inner().value);
    assert!((99..=100).contains(&ttl(&mut c, "k").await));

    let ok = c
        .p_expire(PExpireRequest {
            key: "k".into(),
            milliseconds: 50_000,
        })
        .await
        .unwrap();
    assert!(ok.into_inner().value);
    let pttl = c.p_ttl(PTtlRequest { key: "k".into() }).await.unwrap();
    assert!((49_000..=50_000).contains(&pttl.into_inner().value));

    let ok = c.persist(PersistRequest { key: "k".into() }).await.unwrap();
    assert!(ok.into_inner().value);
    assert_eq!(ttl(&mut c, "k").await, -1);
    let ok = c.persist(PersistRequest { key: "k".into() }).await.unwrap();
    assert!(!ok.into_inner().value);

    let ok = c
        .expire(ExpireRequest {
            key: "missing".into(),
            seconds: 10,
        })
        .await
        .unwrap();
    assert!(!ok.into_inner().value);
    let pttl = c
        .p_ttl(PTtlRequest {
            key: "missing".into(),
        })
        .await
        .unwrap();
    assert_eq!(pttl.into_inner().value, -2);
}

#[tokio::test]
async fn grpc_short_expiry_removes_key() {
    let (_s, mut c) = start().await;
    set(&mut c, "k", "v").await;
    c.p_expire(PExpireRequest {
        key: "k".into(),
        milliseconds: 50,
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(get(&mut c, "k").await, None);
}

async fn expiretime(c: &mut Client, key: &str) -> i64 {
    c.expiretime(ExpiretimeRequest { key: key.into() })
        .await
        .unwrap()
        .into_inner()
        .value
}

#[tokio::test]
async fn grpc_expireat_expiretime() {
    let (_s, mut c) = start().await;
    set(&mut c, "k", "v").await;

    assert_eq!(expiretime(&mut c, "k").await, -1);
    assert_eq!(expiretime(&mut c, "missing").await, -2);

    let ts = now_secs() + 1000;
    let ok = c
        .expireat(ExpireatRequest {
            key: "k".into(),
            timestamp: ts,
        })
        .await
        .unwrap();
    assert!(ok.into_inner().value);
    assert_eq!(expiretime(&mut c, "k").await, ts as i64);

    let ts_ms = ts * 1000 + 500;
    let ok = c
        .pexpireat(PexpireatRequest {
            key: "k".into(),
            timestamp_ms: ts_ms,
        })
        .await
        .unwrap();
    assert!(ok.into_inner().value);
    let pt = c
        .pexpiretime(PexpiretimeRequest { key: "k".into() })
        .await
        .unwrap();
    assert_eq!(pt.into_inner().value, ts_ms as i64);

    let ok = c
        .expireat(ExpireatRequest {
            key: "missing".into(),
            timestamp: ts,
        })
        .await
        .unwrap();
    assert!(!ok.into_inner().value);

    // a timestamp in the past expires the key right away
    let ok = c
        .expireat(ExpireatRequest {
            key: "k".into(),
            timestamp: 1,
        })
        .await
        .unwrap();
    assert!(ok.into_inner().value);
    assert_eq!(get(&mut c, "k").await, None);
}

#[tokio::test]
async fn grpc_type() {
    let (_s, mut c) = start().await;
    set(&mut c, "s", "v").await;
    rpush(&mut c, "l", &["a"]).await;
    hset(&mut c, "h", &[("f", "v")]).await;
    sadd(&mut c, "set", &["m"]).await;
    zadd(&mut c, "z", &[(1.0, "m")]).await;

    for (key, want) in [
        ("s", "string"),
        ("l", "list"),
        ("h", "hash"),
        ("set", "set"),
        ("z", "zset"),
        ("missing", "none"),
    ] {
        let t = c.r#type(TypeRequest { key: key.into() }).await.unwrap();
        assert_eq!(t.into_inner().type_name, want, "type of {key}");
    }
}

#[tokio::test]
async fn grpc_keys_and_scan() {
    // two shards so SCAN has to walk across shard boundaries
    let (_s, mut c) = start_with(ServerOptions::default()).await;
    let mut all = Vec::new();
    for i in 0..30 {
        let key = format!("key:{i}");
        set(&mut c, &key, "v").await;
        all.push(key);
    }
    set(&mut c, "other", "v").await;
    all.sort();

    let keys = c
        .keys(KeysRequest {
            pattern: "key:*".into(),
        })
        .await
        .unwrap();
    assert_eq!(sorted(keys.into_inner().keys), all);

    let mut cursor = 0;
    let mut seen = Vec::new();
    loop {
        let resp = c
            .scan(ScanRequest {
                cursor,
                count: 5,
                pattern: Some("key:*".into()),
            })
            .await
            .unwrap()
            .into_inner();
        seen.extend(resp.keys);
        cursor = resp.cursor;
        if cursor == 0 {
            break;
        }
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen, all);
}

#[tokio::test]
async fn grpc_rename() {
    let (_s, mut c) = start().await;
    set(&mut c, "a", "v").await;

    let resp = c
        .rename(RenameRequest {
            key: "a".into(),
            new_key: "b".into(),
        })
        .await
        .unwrap();
    assert_eq!(resp.into_inner().status, "OK");
    assert_eq!(get(&mut c, "a").await, None);
    assert_eq!(get(&mut c, "b").await, Some(b("v")));

    let result = c
        .rename(RenameRequest {
            key: "missing".into(),
            new_key: "x".into(),
        })
        .await;
    assert_eq!(code(result), Code::NotFound);
}

#[tokio::test]
async fn grpc_copy() {
    let (_s, mut c) = start().await;
    set(&mut c, "src", "1").await;

    let copy = |replace| CopyRequest {
        source: "src".into(),
        destination: "dst".into(),
        replace,
    };
    assert!(c.copy(copy(false)).await.unwrap().into_inner().value);
    assert_eq!(get(&mut c, "dst").await, Some(b("1")));

    set(&mut c, "src", "2").await;
    // destination exists and replace is off
    assert!(!c.copy(copy(false)).await.unwrap().into_inner().value);
    assert_eq!(get(&mut c, "dst").await, Some(b("1")));
    assert!(c.copy(copy(true)).await.unwrap().into_inner().value);
    assert_eq!(get(&mut c, "dst").await, Some(b("2")));

    let result = c
        .copy(CopyRequest {
            source: "missing".into(),
            destination: "x".into(),
            replace: false,
        })
        .await;
    assert_eq!(code(result), Code::NotFound);
}

#[tokio::test]
async fn grpc_randomkey() {
    let (_s, mut c) = start().await;

    let k = c.random_key(RandomKeyRequest {}).await.unwrap();
    assert_eq!(k.into_inner().value, None);

    set(&mut c, "only", "v").await;
    let k = c.random_key(RandomKeyRequest {}).await.unwrap();
    assert_eq!(k.into_inner().value, Some(b("only")));
}

// ---------------------------------------------------------------------------
// lists
// ---------------------------------------------------------------------------

async fn lrange(c: &mut Client, key: &str) -> Vec<Vec<u8>> {
    c.l_range(LRangeRequest {
        key: key.into(),
        start: 0,
        stop: -1,
    })
    .await
    .unwrap()
    .into_inner()
    .values
}

fn bs(v: &[&str]) -> Vec<Vec<u8>> {
    v.iter().map(|s| b(s)).collect()
}

#[tokio::test]
async fn grpc_list_push_pop_range() {
    let (_s, mut c) = start().await;

    assert_eq!(rpush(&mut c, "l", &["a", "b", "c"]).await, 3);
    let n = c
        .l_push(LPushRequest {
            key: "l".into(),
            values: bs(&["z"]),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 4);
    assert_eq!(lrange(&mut c, "l").await, bs(&["z", "a", "b", "c"]));

    let r = c
        .l_range(LRangeRequest {
            key: "l".into(),
            start: 1,
            stop: 2,
        })
        .await
        .unwrap();
    assert_eq!(r.into_inner().values, bs(&["a", "b"]));

    let n = c.l_len(LLenRequest { key: "l".into() }).await.unwrap();
    assert_eq!(n.into_inner().value, 4);

    let v = c.l_pop(LPopRequest { key: "l".into() }).await.unwrap();
    assert_eq!(v.into_inner().value, Some(b("z")));
    let v = c.r_pop(RPopRequest { key: "l".into() }).await.unwrap();
    assert_eq!(v.into_inner().value, Some(b("c")));

    let v = c
        .l_pop(LPopRequest {
            key: "missing".into(),
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, None);
    let v = c
        .r_pop(RPopRequest {
            key: "missing".into(),
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, None);
    assert!(lrange(&mut c, "missing").await.is_empty());
}

#[tokio::test]
async fn grpc_list_index_set_insert() {
    let (_s, mut c) = start().await;
    rpush(&mut c, "l", &["a", "b", "c"]).await;

    let index = |index| LIndexRequest {
        key: "l".into(),
        index,
    };
    let v = c.l_index(index(1)).await.unwrap();
    assert_eq!(v.into_inner().value, Some(b("b")));
    let v = c.l_index(index(-1)).await.unwrap();
    assert_eq!(v.into_inner().value, Some(b("c")));
    let v = c.l_index(index(99)).await.unwrap();
    assert_eq!(v.into_inner().value, None);

    let s = c
        .l_set(LSetRequest {
            key: "l".into(),
            index: 0,
            value: b("A"),
        })
        .await
        .unwrap();
    assert_eq!(s.into_inner().status, "OK");

    let n = c
        .l_insert(LInsertRequest {
            key: "l".into(),
            before: true,
            pivot: b("c"),
            value: b("x"),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 4);
    let n = c
        .l_insert(LInsertRequest {
            key: "l".into(),
            before: false,
            pivot: b("c"),
            value: b("y"),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 5);
    assert_eq!(lrange(&mut c, "l").await, bs(&["A", "b", "x", "c", "y"]));

    // pivot not found
    let n = c
        .l_insert(LInsertRequest {
            key: "l".into(),
            before: true,
            pivot: b("nope"),
            value: b("v"),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, -1);
}

#[tokio::test]
async fn grpc_lset_errors() {
    let (_s, mut c) = start().await;
    rpush(&mut c, "l", &["a"]).await;

    let lset = |key: &str, index| LSetRequest {
        key: key.into(),
        index,
        value: b("v"),
    };
    assert_eq!(code(c.l_set(lset("l", 5)).await), Code::FailedPrecondition);
    assert_eq!(
        code(c.l_set(lset("missing", 0)).await),
        Code::FailedPrecondition
    );
}

#[tokio::test]
async fn grpc_list_trim_rem_pos() {
    let (_s, mut c) = start().await;
    rpush(&mut c, "l", &["a", "b", "a", "c", "a"]).await;

    let pos = c
        .l_pos(LPosRequest {
            key: "l".into(),
            value: b("c"),
            count: None,
        })
        .await
        .unwrap();
    assert_eq!(pos.into_inner().value, Some(3));
    let pos = c
        .l_pos(LPosRequest {
            key: "l".into(),
            value: b("nope"),
            count: None,
        })
        .await
        .unwrap();
    assert_eq!(pos.into_inner().value, None);

    let n = c
        .l_rem(LRemRequest {
            key: "l".into(),
            count: 2,
            value: b("a"),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 2);
    assert_eq!(lrange(&mut c, "l").await, bs(&["b", "c", "a"]));

    let s = c
        .l_trim(LTrimRequest {
            key: "l".into(),
            start: 1,
            stop: -1,
        })
        .await
        .unwrap();
    assert_eq!(s.into_inner().status, "OK");
    assert_eq!(lrange(&mut c, "l").await, bs(&["c", "a"]));
}

#[tokio::test]
async fn grpc_lmove() {
    let (_s, mut c) = start().await;
    rpush(&mut c, "src", &["a", "b", "c"]).await;
    rpush(&mut c, "dst", &["x"]).await;

    let v = c
        .l_move(LMoveRequest {
            source: "src".into(),
            destination: "dst".into(),
            src_left: true,
            dst_left: false,
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, Some(b("a")));
    let v = c
        .l_move(LMoveRequest {
            source: "src".into(),
            destination: "dst".into(),
            src_left: false,
            dst_left: true,
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, Some(b("c")));
    assert_eq!(lrange(&mut c, "src").await, bs(&["b"]));
    assert_eq!(lrange(&mut c, "dst").await, bs(&["c", "x", "a"]));

    let v = c
        .l_move(LMoveRequest {
            source: "missing".into(),
            destination: "dst".into(),
            src_left: true,
            dst_left: true,
        })
        .await
        .unwrap();
    assert_eq!(v.into_inner().value, None);
}

#[tokio::test]
async fn grpc_lmpop() {
    let (_s, mut c) = start().await;
    rpush(&mut c, "l2", &["a", "b", "c"]).await;

    let resp = c
        .lmpop(LmpopRequest {
            keys: strs(&["l1", "l2"]),
            left: true,
            count: 2,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.found);
    assert_eq!(resp.key, "l2");
    assert_eq!(resp.elements, bs(&["a", "b"]));

    let resp = c
        .lmpop(LmpopRequest {
            keys: strs(&["l2"]),
            left: false,
            count: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.elements, bs(&["c"]));

    let resp = c
        .lmpop(LmpopRequest {
            keys: strs(&["l1", "l2"]),
            left: true,
            count: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.found);
    assert!(resp.key.is_empty());
    assert!(resp.elements.is_empty());

    let result = c
        .lmpop(LmpopRequest {
            keys: vec![],
            left: true,
            count: 1,
        })
        .await;
    assert_eq!(code(result), Code::InvalidArgument);
}

// ---------------------------------------------------------------------------
// hashes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grpc_hash_basics() {
    let (_s, mut c) = start().await;

    assert_eq!(hset(&mut c, "h", &[("a", "1"), ("b", "2")]).await, 2);
    // updating an existing field adds nothing
    assert_eq!(hset(&mut c, "h", &[("a", "10"), ("c", "3")]).await, 1);

    let hget = |field: &str| HGetRequest {
        key: "h".into(),
        field: field.into(),
    };
    let v = c.h_get(hget("a")).await.unwrap();
    assert_eq!(v.into_inner().value, Some(b("10")));
    let v = c.h_get(hget("nope")).await.unwrap();
    assert_eq!(v.into_inner().value, None);

    let mut all: Vec<(String, Vec<u8>)> = c
        .h_get_all(HGetAllRequest { key: "h".into() })
        .await
        .unwrap()
        .into_inner()
        .fields
        .into_iter()
        .map(|f| (f.field, f.value))
        .collect();
    all.sort();
    assert_eq!(
        all,
        vec![
            ("a".to_string(), b("10")),
            ("b".to_string(), b("2")),
            ("c".to_string(), b("3")),
        ]
    );

    let n = c.h_len(HLenRequest { key: "h".into() }).await.unwrap();
    assert_eq!(n.into_inner().value, 3);

    let keys = c.h_keys(HKeysRequest { key: "h".into() }).await.unwrap();
    assert_eq!(sorted(keys.into_inner().keys), strs(&["a", "b", "c"]));
    let mut vals = c
        .h_vals(HValsRequest { key: "h".into() })
        .await
        .unwrap()
        .into_inner()
        .values;
    vals.sort();
    assert_eq!(vals, bs(&["10", "2", "3"]));

    let n = c
        .h_del(HDelRequest {
            key: "h".into(),
            fields: strs(&["a", "nope"]),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 1);

    let exists = |field: &str| HExistsRequest {
        key: "h".into(),
        field: field.into(),
    };
    assert!(!c.h_exists(exists("a")).await.unwrap().into_inner().value);
    assert!(c.h_exists(exists("b")).await.unwrap().into_inner().value);

    let all = c
        .h_get_all(HGetAllRequest {
            key: "missing".into(),
        })
        .await
        .unwrap();
    assert!(all.into_inner().fields.is_empty());
}

#[tokio::test]
async fn grpc_hincrby_hmget() {
    let (_s, mut c) = start().await;
    hset(&mut c, "h", &[("n", "5"), ("s", "x")]).await;

    let incr = |field: &str, delta| HIncrByRequest {
        key: "h".into(),
        field: field.into(),
        delta,
    };
    let n = c.h_incr_by(incr("n", 3)).await.unwrap();
    assert_eq!(n.into_inner().value, 8);
    let n = c.h_incr_by(incr("new", -2)).await.unwrap();
    assert_eq!(n.into_inner().value, -2);

    let values: Vec<Option<Vec<u8>>> = c
        .hm_get(HmGetRequest {
            key: "h".into(),
            fields: strs(&["n", "nope", "s"]),
        })
        .await
        .unwrap()
        .into_inner()
        .values
        .into_iter()
        .map(|v| v.value)
        .collect();
    assert_eq!(values, vec![Some(b("8")), None, Some(b("x"))]);
}

#[tokio::test]
async fn grpc_hscan_hrandfield() {
    let (_s, mut c) = start().await;
    hset(&mut c, "h", &[("a", "1"), ("b", "2"), ("c", "3")]).await;

    let resp = c
        .h_scan(HScanRequest {
            key: "h".into(),
            cursor: 0,
            pattern: None,
            count: 100,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.cursor, 0);
    let mut fields: Vec<(String, Vec<u8>)> = resp
        .fields
        .into_iter()
        .map(|f| (f.field, f.value))
        .collect();
    fields.sort();
    assert_eq!(
        fields,
        vec![
            ("a".to_string(), b("1")),
            ("b".to_string(), b("2")),
            ("c".to_string(), b("3")),
        ]
    );

    let one = c
        .hrandfield(HrandfieldRequest {
            key: "h".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .values;
    assert_eq!(one.len(), 1);
    assert!(bs(&["a", "b", "c"]).contains(&one[0]));

    // with values the reply alternates field, value
    let pairs = c
        .hrandfield(HrandfieldRequest {
            key: "h".into(),
            has_count: true,
            count: 3,
            with_values: true,
        })
        .await
        .unwrap()
        .into_inner()
        .values;
    let mut got: Vec<(Vec<u8>, Vec<u8>)> = pairs
        .chunks(2)
        .map(|p| (p[0].clone(), p[1].clone()))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![(b("a"), b("1")), (b("b"), b("2")), (b("c"), b("3"))]
    );
}

// ---------------------------------------------------------------------------
// sets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grpc_set_basics() {
    let (_s, mut c) = start().await;

    assert_eq!(sadd(&mut c, "s", &["a", "b", "c"]).await, 3);
    assert_eq!(sadd(&mut c, "s", &["a", "d"]).await, 1);
    assert_eq!(smembers(&mut c, "s").await, strs(&["a", "b", "c", "d"]));

    let n = c
        .s_rem(SRemRequest {
            key: "s".into(),
            members: strs(&["d", "nope"]),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 1);

    let n = c.s_card(SCardRequest { key: "s".into() }).await.unwrap();
    assert_eq!(n.into_inner().value, 3);

    let is = |member: &str| SIsMemberRequest {
        key: "s".into(),
        member: member.into(),
    };
    assert!(c.s_is_member(is("a")).await.unwrap().into_inner().value);
    assert!(!c.s_is_member(is("d")).await.unwrap().into_inner().value);

    let mis = c
        .s_mis_member(SMisMemberRequest {
            key: "s".into(),
            members: strs(&["a", "nope", "c"]),
        })
        .await
        .unwrap();
    assert_eq!(mis.into_inner().values, vec![true, false, true]);

    assert!(smembers(&mut c, "missing").await.is_empty());
}

#[tokio::test]
async fn grpc_set_algebra() {
    let (_s, mut c) = start().await;
    sadd(&mut c, "a", &["1", "2", "3"]).await;
    sadd(&mut c, "b", &["2", "3", "4"]).await;
    let keys = || strs(&["a", "b"]);

    let r = c.s_union(SUnionRequest { keys: keys() }).await.unwrap();
    assert_eq!(sorted(r.into_inner().keys), strs(&["1", "2", "3", "4"]));
    let r = c.s_inter(SInterRequest { keys: keys() }).await.unwrap();
    assert_eq!(sorted(r.into_inner().keys), strs(&["2", "3"]));
    let r = c.s_diff(SDiffRequest { keys: keys() }).await.unwrap();
    assert_eq!(sorted(r.into_inner().keys), strs(&["1"]));

    let result = c.s_union(SUnionRequest { keys: vec![] }).await;
    assert_eq!(code(result), Code::InvalidArgument);

    let n = c
        .sintercard(SintercardRequest {
            keys: keys(),
            limit: 0,
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 2);
    let n = c
        .sintercard(SintercardRequest {
            keys: keys(),
            limit: 1,
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 1);
}

#[tokio::test]
async fn grpc_set_store_variants() {
    let (_s, mut c) = start().await;
    sadd(&mut c, "a", &["1", "2", "3"]).await;
    sadd(&mut c, "b", &["2", "3", "4"]).await;
    let keys = || strs(&["a", "b"]);
    let dest = || "d".to_string();

    let n = c
        .s_union_store(SUnionStoreRequest {
            destination: dest(),
            keys: keys(),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 4);
    assert_eq!(smembers(&mut c, "d").await, strs(&["1", "2", "3", "4"]));

    let n = c
        .s_inter_store(SInterStoreRequest {
            destination: dest(),
            keys: keys(),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 2);
    assert_eq!(smembers(&mut c, "d").await, strs(&["2", "3"]));

    let n = c
        .s_diff_store(SDiffStoreRequest {
            destination: dest(),
            keys: keys(),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 1);
    assert_eq!(smembers(&mut c, "d").await, strs(&["1"]));
}

#[tokio::test]
async fn grpc_srandmember_spop() {
    let (_s, mut c) = start().await;
    sadd(&mut c, "s", &["a", "b", "c"]).await;
    let all = bs(&["a", "b", "c"]);

    let r = c
        .s_rand_member(SRandMemberRequest {
            key: "s".into(),
            count: 2,
        })
        .await
        .unwrap()
        .into_inner()
        .values;
    assert_eq!(r.len(), 2);
    assert_ne!(r[0], r[1]);
    assert!(r.iter().all(|m| all.contains(m)));

    // a negative count may repeat members
    let r = c
        .s_rand_member(SRandMemberRequest {
            key: "s".into(),
            count: -5,
        })
        .await
        .unwrap()
        .into_inner()
        .values;
    assert_eq!(r.len(), 5);
    assert!(r.iter().all(|m| all.contains(m)));

    let popped = c
        .s_pop(SPopRequest {
            key: "s".into(),
            count: 2,
        })
        .await
        .unwrap()
        .into_inner()
        .values;
    assert_eq!(popped.len(), 2);
    let n = c.s_card(SCardRequest { key: "s".into() }).await.unwrap();
    assert_eq!(n.into_inner().value, 1);
}

#[tokio::test]
async fn grpc_sscan_smove() {
    let (_s, mut c) = start().await;
    sadd(&mut c, "src", &["a", "b", "c"]).await;

    let resp = c
        .s_scan(SScanRequest {
            key: "src".into(),
            cursor: 0,
            pattern: None,
            count: 100,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.cursor, 0);
    assert_eq!(sorted(resp.members), strs(&["a", "b", "c"]));

    let mv = |member: &str| SmoveRequest {
        source: "src".into(),
        destination: "dst".into(),
        member: member.into(),
    };
    assert!(c.smove(mv("a")).await.unwrap().into_inner().value);
    assert!(!c.smove(mv("nope")).await.unwrap().into_inner().value);
    assert_eq!(smembers(&mut c, "src").await, strs(&["b", "c"]));
    assert_eq!(smembers(&mut c, "dst").await, strs(&["a"]));
}

// ---------------------------------------------------------------------------
// sorted sets
// ---------------------------------------------------------------------------

async fn zrange(c: &mut Client, key: &str) -> Vec<(String, f64)> {
    let resp = c
        .z_range(ZRangeRequest {
            key: key.into(),
            start: 0,
            stop: -1,
            with_scores: true,
        })
        .await
        .unwrap();
    members(resp.into_inner())
}

fn scored(v: &[(&str, f64)]) -> Vec<(String, f64)> {
    v.iter().map(|(m, s)| (m.to_string(), *s)).collect()
}

#[tokio::test]
async fn grpc_zset_basics() {
    let (_s, mut c) = start().await;

    assert_eq!(
        zadd(&mut c, "z", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]).await,
        3
    );
    assert_eq!(
        zrange(&mut c, "z").await,
        scored(&[("a", 1.0), ("b", 2.0), ("c", 3.0)])
    );

    // CH counts updated members too
    let n = c
        .z_add(ZAddRequest {
            key: "z".into(),
            members: vec![
                ScoreMember {
                    score: 5.0,
                    member: "a".into(),
                },
                ScoreMember {
                    score: 4.0,
                    member: "d".into(),
                },
            ],
            ch: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 2);

    // NX skips existing members
    let n = c
        .z_add(ZAddRequest {
            key: "z".into(),
            members: vec![ScoreMember {
                score: 0.0,
                member: "a".into(),
            }],
            nx: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 0);

    let score = |member: &str| ZScoreRequest {
        key: "z".into(),
        member: member.into(),
    };
    let s = c.z_score(score("a")).await.unwrap();
    assert_eq!(s.into_inner().value, Some(5.0));
    let s = c.z_score(score("nope")).await.unwrap();
    assert_eq!(s.into_inner().value, None);

    let n = c.z_card(ZCardRequest { key: "z".into() }).await.unwrap();
    assert_eq!(n.into_inner().value, 4);

    let n = c
        .z_rem(ZRemRequest {
            key: "z".into(),
            members: strs(&["d", "nope"]),
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 1);
    assert_eq!(
        zrange(&mut c, "z").await,
        scored(&[("b", 2.0), ("c", 3.0), ("a", 5.0)])
    );
}

#[tokio::test]
async fn grpc_zrank_zrevrank() {
    let (_s, mut c) = start().await;
    zadd(&mut c, "z", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]).await;

    let rank = |member: &str| ZRankRequest {
        key: "z".into(),
        member: member.into(),
    };
    let r = c.z_rank(rank("a")).await.unwrap();
    assert_eq!(r.into_inner().value, Some(0));
    let r = c.z_rank(rank("nope")).await.unwrap();
    assert_eq!(r.into_inner().value, None);

    let rev = |member: &str| ZRevRankRequest {
        key: "z".into(),
        member: member.into(),
    };
    let r = c.z_rev_rank(rev("a")).await.unwrap();
    assert_eq!(r.into_inner().value, Some(2));
    let r = c.z_rev_rank(rev("nope")).await.unwrap();
    assert_eq!(r.into_inner().value, None);
}

#[tokio::test]
async fn grpc_zrange_variants() {
    let (_s, mut c) = start().await;
    zadd(
        &mut c,
        "z",
        &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")],
    )
    .await;

    let r = c
        .z_range(ZRangeRequest {
            key: "z".into(),
            start: 1,
            stop: 2,
            with_scores: false,
        })
        .await
        .unwrap();
    assert_eq!(names(r.into_inner()), strs(&["b", "c"]));

    // only the full range: a partial ZREVRANGE takes ranks from the low end
    let r = c
        .z_rev_range(ZRevRangeRequest {
            key: "z".into(),
            start: 0,
            stop: -1,
            with_scores: true,
        })
        .await
        .unwrap();
    assert_eq!(
        members(r.into_inner()),
        scored(&[("d", 4.0), ("c", 3.0), ("b", 2.0), ("a", 1.0)])
    );

    let r = c
        .z_range_by_score(ZRangeByScoreRequest {
            key: "z".into(),
            min: "(1".into(),
            max: "+inf".into(),
            offset: Some(1),
            count: Some(2),
            with_scores: true,
        })
        .await
        .unwrap();
    assert_eq!(members(r.into_inner()), scored(&[("c", 3.0), ("d", 4.0)]));

    let r = c
        .z_rev_range_by_score(ZRevRangeByScoreRequest {
            key: "z".into(),
            max: "3".into(),
            min: "-inf".into(),
            offset: None,
            count: None,
            with_scores: false,
        })
        .await
        .unwrap();
    assert_eq!(names(r.into_inner()), strs(&["c", "b", "a"]));
}

#[tokio::test]
async fn grpc_zcount_zincrby() {
    let (_s, mut c) = start().await;
    zadd(&mut c, "z", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]).await;

    let count = |min: &str, max: &str| ZCountRequest {
        key: "z".into(),
        min: min.into(),
        max: max.into(),
    };
    let n = c.z_count(count("(1", "+inf")).await.unwrap();
    assert_eq!(n.into_inner().value, 2);
    let n = c.z_count(count("-inf", "2")).await.unwrap();
    assert_eq!(n.into_inner().value, 2);
    assert_eq!(
        code(c.z_count(count("abc", "2")).await),
        Code::InvalidArgument
    );

    let s = c
        .z_incr_by(ZIncrByRequest {
            key: "z".into(),
            delta: 2.5,
            member: "a".into(),
        })
        .await
        .unwrap();
    assert_eq!(s.into_inner().value, "3.5");
    let s = c
        .z_incr_by(ZIncrByRequest {
            key: "z".into(),
            delta: 1.5,
            member: "new".into(),
        })
        .await
        .unwrap();
    assert_eq!(s.into_inner().value, "1.5");
}

#[tokio::test]
async fn grpc_zpop() {
    let (_s, mut c) = start().await;
    zadd(
        &mut c,
        "z",
        &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")],
    )
    .await;

    let r = c
        .z_pop_min(ZPopMinRequest {
            key: "z".into(),
            count: 2,
        })
        .await
        .unwrap();
    assert_eq!(members(r.into_inner()), scored(&[("a", 1.0), ("b", 2.0)]));
    let r = c
        .z_pop_max(ZPopMaxRequest {
            key: "z".into(),
            count: 1,
        })
        .await
        .unwrap();
    assert_eq!(members(r.into_inner()), scored(&[("d", 4.0)]));
    assert_eq!(zrange(&mut c, "z").await, scored(&[("c", 3.0)]));

    let r = c
        .z_pop_min(ZPopMinRequest {
            key: "missing".into(),
            count: 1,
        })
        .await
        .unwrap();
    assert!(r.into_inner().members.is_empty());
}

#[tokio::test]
async fn grpc_zset_algebra() {
    let (_s, mut c) = start().await;
    zadd(&mut c, "a", &[(1.0, "x"), (2.0, "y")]).await;
    zadd(&mut c, "b", &[(10.0, "y"), (20.0, "z")]).await;
    let keys = || strs(&["a", "b"]);

    let r = c
        .z_diff(ZDiffRequest {
            keys: keys(),
            with_scores: true,
        })
        .await
        .unwrap();
    assert_eq!(members(r.into_inner()), scored(&[("x", 1.0)]));

    // scores are summed by default
    let r = c
        .z_inter(ZInterRequest {
            keys: keys(),
            with_scores: true,
        })
        .await
        .unwrap();
    assert_eq!(members(r.into_inner()), scored(&[("y", 12.0)]));

    let r = c
        .z_union(ZUnionRequest {
            keys: keys(),
            with_scores: true,
        })
        .await
        .unwrap();
    assert_eq!(
        members(r.into_inner()),
        scored(&[("x", 1.0), ("y", 12.0), ("z", 20.0)])
    );

    let r = c
        .z_union(ZUnionRequest {
            keys: keys(),
            with_scores: false,
        })
        .await
        .unwrap();
    assert_eq!(names(r.into_inner()), strs(&["x", "y", "z"]));

    let result = c
        .z_inter(ZInterRequest {
            keys: vec![],
            with_scores: false,
        })
        .await;
    assert_eq!(code(result), Code::InvalidArgument);
}

#[tokio::test]
async fn grpc_zscan_zmpop_zrandmember() {
    let (_s, mut c) = start().await;
    zadd(&mut c, "z", &[(1.0, "a"), (2.5, "b"), (3.0, "c")]).await;

    let resp = c
        .z_scan(ZScanRequest {
            key: "z".into(),
            cursor: 0,
            pattern: None,
            count: 100,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.cursor, 0);
    let mut got: Vec<(String, f64)> = resp
        .members
        .into_iter()
        .map(|m| (m.member, m.score))
        .collect();
    got.sort_by(|x, y| x.0.cmp(&y.0));
    assert_eq!(got, scored(&[("a", 1.0), ("b", 2.5), ("c", 3.0)]));

    let one = c
        .zrandmember(ZrandmemberRequest {
            key: "z".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .values;
    assert_eq!(one.len(), 1);
    assert!(bs(&["a", "b", "c"]).contains(&one[0]));

    // with scores the reply alternates member, score
    let pairs = c
        .zrandmember(ZrandmemberRequest {
            key: "z".into(),
            has_count: true,
            count: 3,
            with_scores: true,
        })
        .await
        .unwrap()
        .into_inner()
        .values;
    let mut got: Vec<(Vec<u8>, Vec<u8>)> = pairs
        .chunks(2)
        .map(|p| (p[0].clone(), p[1].clone()))
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![(b("a"), b("1")), (b("b"), b("2.5")), (b("c"), b("3"))]
    );

    let resp = c
        .zmpop(ZmpopRequest {
            keys: strs(&["missing", "z"]),
            min: true,
            count: 2,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.found);
    assert_eq!(resp.key, "z");
    let got: Vec<(String, f64)> = resp
        .members
        .into_iter()
        .map(|m| (m.member, m.score))
        .collect();
    assert_eq!(got, scored(&[("a", 1.0), ("b", 2.5)]));

    let resp = c
        .zmpop(ZmpopRequest {
            keys: strs(&["z"]),
            min: false,
            count: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.members.len(), 1);
    assert_eq!(resp.members[0].member, "c");

    let resp = c
        .zmpop(ZmpopRequest {
            keys: strs(&["z"]),
            min: true,
            count: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.found);
    assert!(resp.members.is_empty());
}

// ---------------------------------------------------------------------------
// wrong type errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grpc_wrongtype_is_failed_precondition() {
    let (_s, mut c) = start().await;
    set(&mut c, "str", "v").await;
    rpush(&mut c, "list", &["a"]).await;

    let result = c.get(GetRequest { key: "list".into() }).await;
    let status = result.unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert!(status.message().starts_with("WRONGTYPE"), "{status:?}");

    let result = c
        .l_push(LPushRequest {
            key: "str".into(),
            values: bs(&["x"]),
        })
        .await;
    assert_eq!(code(result), Code::FailedPrecondition);

    let result = c
        .h_get(HGetRequest {
            key: "str".into(),
            field: "f".into(),
        })
        .await;
    assert_eq!(code(result), Code::FailedPrecondition);

    let result = c
        .s_add(SAddRequest {
            key: "str".into(),
            members: strs(&["m"]),
        })
        .await;
    assert_eq!(code(result), Code::FailedPrecondition);

    let result = c
        .z_add(ZAddRequest {
            key: "list".into(),
            members: vec![ScoreMember {
                score: 1.0,
                member: "m".into(),
            }],
            ..Default::default()
        })
        .await;
    assert_eq!(code(result), Code::FailedPrecondition);

    let result = c.incr(IncrRequest { key: "list".into() }).await;
    assert_eq!(code(result), Code::FailedPrecondition);
}

// ---------------------------------------------------------------------------
// vectors
// ---------------------------------------------------------------------------

/// Returns true when the server binary has the vector feature. Without it,
/// every vector RPC answers Unimplemented.
async fn has_vector(c: &mut Client) -> bool {
    match c
        .v_info(VInfoRequest {
            key: "probe".into(),
        })
        .await
    {
        Ok(_) => true,
        Err(status) if status.code() == Code::Unimplemented => false,
        Err(status) => panic!("unexpected VInfo error: {status:?}"),
    }
}

#[tokio::test]
async fn grpc_vectors() {
    let (_s, mut c) = start().await;
    if !has_vector(&mut c).await {
        for result in [
            c.v_card(VCardRequest { key: "v".into() }).await.map(|_| ()),
            c.v_dim(VDimRequest { key: "v".into() }).await.map(|_| ()),
        ] {
            assert_eq!(result.unwrap_err().code(), Code::Unimplemented);
        }
        eprintln!("skipping vector RPCs: server built without the vector feature");
        return;
    }

    let added = c
        .v_add(VAddRequest {
            key: "v".into(),
            element: "a".into(),
            vector: vec![1.0, 0.0, 0.0],
            metric: VectorMetric::Euclidean as i32,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(added.into_inner().value);

    let entry = |element: &str, vector: Vec<f32>| VAddBatchEntry {
        element: element.into(),
        vector,
    };
    let n = c
        .v_add_batch(VAddBatchRequest {
            key: "v".into(),
            entries: vec![
                entry("b", vec![0.0, 1.0, 0.0]),
                entry("c", vec![0.9, 0.1, 0.0]),
            ],
            metric: VectorMetric::Euclidean as i32,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(n.into_inner().value, 2);

    let sim = c
        .v_sim(VSimRequest {
            key: "v".into(),
            query: vec![1.0, 0.0, 0.0],
            count: 2,
            ef_search: None,
        })
        .await
        .unwrap()
        .into_inner()
        .results;
    let elements: Vec<&str> = sim.iter().map(|r| r.element.as_str()).collect();
    assert_eq!(elements, vec!["a", "c"]);

    let got = c
        .v_get(VGetRequest {
            key: "v".into(),
            element: "b".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.exists, Some(true));
    assert_eq!(got.vector, vec![0.0, 1.0, 0.0]);

    let info = c
        .v_info(VInfoRequest { key: "v".into() })
        .await
        .unwrap()
        .into_inner();
    assert!(info.exists);
    assert!(!info.info.is_empty());

    let removed = c
        .v_rem(VRemRequest {
            key: "v".into(),
            element: "b".into(),
        })
        .await
        .unwrap();
    assert!(removed.into_inner().value);
    let got = c
        .v_get(VGetRequest {
            key: "v".into(),
            element: "b".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.exists, Some(false));

    let info = c
        .v_info(VInfoRequest {
            key: "missing".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!info.exists);
}

// ---------------------------------------------------------------------------
// server
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grpc_ping_echo() {
    let (_s, mut c) = start().await;

    let p = c.ping(PingRequest { message: None }).await.unwrap();
    assert_eq!(p.into_inner().message, "PONG");
    let p = c
        .ping(PingRequest {
            message: Some("hi".into()),
        })
        .await
        .unwrap();
    assert_eq!(p.into_inner().message, "hi");

    let e = c
        .echo(EchoRequest {
            message: "hello".into(),
        })
        .await
        .unwrap();
    assert_eq!(e.into_inner().message, "hello");
}

#[tokio::test]
async fn grpc_dbsize_flushdb() {
    let (_s, mut c) = start_with(ServerOptions::default()).await;
    for i in 0..10 {
        set(&mut c, &format!("k{i}"), "v").await;
    }

    let n = c.db_size(DbSizeRequest {}).await.unwrap();
    assert_eq!(n.into_inner().value, 10);

    let s = c.flush_db(FlushDbRequest { r#async: false }).await.unwrap();
    assert_eq!(s.into_inner().status, "OK");
    let n = c.db_size(DbSizeRequest {}).await.unwrap();
    assert_eq!(n.into_inner().value, 0);

    set(&mut c, "k", "v").await;
    let s = c.flush_db(FlushDbRequest { r#async: true }).await.unwrap();
    assert_eq!(s.into_inner().status, "OK");
    assert_eq!(get(&mut c, "k").await, None);
}

#[tokio::test]
async fn grpc_info() {
    let (_s, mut c) = start().await;
    let info = c
        .info(InfoRequest { section: None })
        .await
        .unwrap()
        .into_inner()
        .info;
    for field in ["uptime_in_seconds:", "connected_clients:", "used_memory:"] {
        assert!(info.contains(field), "INFO is missing {field}: {info}");
    }
}

#[tokio::test]
async fn grpc_time_lastsave() {
    let (_s, mut c) = start().await;

    let t = c.time(TimeRequest {}).await.unwrap().into_inner();
    assert!((t.seconds - now_secs() as i64).abs() <= 2);
    assert!((0..1_000_000).contains(&t.microseconds));

    let ts = c.last_save(LastSaveRequest {}).await.unwrap();
    assert!(ts.into_inner().value >= 0);
}

#[tokio::test]
async fn grpc_bgsave_bgrewriteaof() {
    let dir = tempfile::tempdir().unwrap();
    let (_s, mut c) = start_with(ServerOptions {
        shards: Some(1),
        appendonly: true,
        data_dir: Some(dir),
        ..Default::default()
    })
    .await;
    set(&mut c, "k", "v").await;

    let s = c.bg_save(BgSaveRequest {}).await.unwrap();
    assert_eq!(s.into_inner().status, "Background saving started");
    let s = c.bg_rewrite_aof(BgRewriteAofRequest {}).await.unwrap();
    assert_eq!(
        s.into_inner().status,
        "Background append only file rewriting started"
    );
}

#[tokio::test]
async fn grpc_slowlog() {
    let (server, mut c) = start().await;
    // log every command
    let mut resp = server.connect().await;
    resp.ok(&["CONFIG", "SET", "slowlog-log-slower-than", "0"])
        .await;

    let s = c.slow_log_reset(SlowLogResetRequest {}).await.unwrap();
    assert_eq!(s.into_inner().status, "OK");
    let n = c.slow_log_len(SlowLogLenRequest {}).await.unwrap();
    assert_eq!(n.into_inner().value, 0);

    set(&mut c, "k", "v").await;
    get(&mut c, "k").await;

    let n = c.slow_log_len(SlowLogLenRequest {}).await.unwrap();
    assert_eq!(n.into_inner().value, 2);

    let entries = c
        .slow_log_get(SlowLogGetRequest { count: Some(1) })
        .await
        .unwrap()
        .into_inner()
        .entries;
    assert_eq!(entries.len(), 1);
    // newest first
    assert!(entries[0].command.to_uppercase().starts_with("GET"));
    assert!(entries[0].timestamp_unix > 0);

    let entries = c
        .slow_log_get(SlowLogGetRequest { count: None })
        .await
        .unwrap()
        .into_inner()
        .entries;
    assert_eq!(entries.len(), 2);
    assert!(entries[1].command.to_uppercase().starts_with("SET"));
    assert!(entries[0].id > entries[1].id);
}

// ---------------------------------------------------------------------------
// auth
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grpc_requirepass() {
    let (_s, mut c) = start_with(ServerOptions {
        requirepass: Some("secret".into()),
        ..Default::default()
    })
    .await;

    let ping = |token: Option<&str>| {
        let mut req = Request::new(PingRequest { message: None });
        if let Some(t) = token {
            req.metadata_mut()
                .insert("authorization", t.parse().unwrap());
        }
        req
    };

    assert_eq!(code(c.ping(ping(None)).await), Code::Unauthenticated);
    assert_eq!(
        code(c.ping(ping(Some("wrong"))).await),
        Code::Unauthenticated
    );
    let p = c.ping(ping(Some("secret"))).await.unwrap();
    assert_eq!(p.into_inner().message, "PONG");

    let mut req = Request::new(GetRequest { key: "k".into() });
    req.metadata_mut()
        .insert("authorization", "secret".parse().unwrap());
    assert_eq!(c.get(req).await.unwrap().into_inner().value, None);
}

// ---------------------------------------------------------------------------
// pub/sub
// ---------------------------------------------------------------------------

async fn next_event(stream: &mut tonic::Streaming<SubscribeEvent>) -> SubscribeEvent {
    tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("timed out waiting for a pub/sub event")
        .unwrap()
        .expect("subscription stream ended")
}

async fn publish(c: &mut Client, channel: &str, message: &str) -> i64 {
    c.publish(PublishRequest {
        channel: channel.into(),
        message: b(message),
    })
    .await
    .unwrap()
    .into_inner()
    .value
}

#[tokio::test]
async fn grpc_publish_subscribe() {
    let (server, mut c) = start().await;
    let mut sub_client = connect(&server).await;

    assert_eq!(publish(&mut c, "news", "nobody").await, 0);

    let mut stream = sub_client
        .subscribe(SubscribeRequest {
            channels: strs(&["news"]),
            patterns: strs(&["sport.*"]),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(publish(&mut c, "news", "hello").await, 1);
    let evt = next_event(&mut stream).await;
    assert_eq!(evt.kind, "message");
    assert_eq!(evt.channel, "news");
    assert_eq!(evt.data, b("hello"));
    assert_eq!(evt.pattern, None);

    assert_eq!(publish(&mut c, "sport.golf", "fore").await, 1);
    let evt = next_event(&mut stream).await;
    assert_eq!(evt.kind, "pmessage");
    assert_eq!(evt.channel, "sport.golf");
    assert_eq!(evt.data, b("fore"));
    assert_eq!(evt.pattern.as_deref(), Some("sport.*"));

    // RESP publishers reach gRPC subscribers too
    let mut resp = server.connect().await;
    assert_eq!(resp.get_int(&["PUBLISH", "news", "resp"]).await, 1);
    let evt = next_event(&mut stream).await;
    assert_eq!(evt.data, b("resp"));
}

#[tokio::test]
async fn grpc_subscribe_requires_a_target() {
    let (_s, mut c) = start().await;
    let result = c
        .subscribe(SubscribeRequest {
            channels: vec![],
            patterns: vec![],
        })
        .await;
    assert_eq!(code(result), Code::InvalidArgument);
}

#[tokio::test]
async fn grpc_pubsub_introspection() {
    let (server, mut c) = start().await;
    let mut sub_client = connect(&server).await;

    let _stream = sub_client
        .subscribe(SubscribeRequest {
            channels: strs(&["news", "weather"]),
            patterns: strs(&["sport.*"]),
        })
        .await
        .unwrap()
        .into_inner();

    let all = c
        .pub_sub_channels(PubSubChannelsRequest { pattern: None })
        .await
        .unwrap();
    assert_eq!(sorted(all.into_inner().keys), strs(&["news", "weather"]));
    let some = c
        .pub_sub_channels(PubSubChannelsRequest {
            pattern: Some("n*".into()),
        })
        .await
        .unwrap();
    assert_eq!(some.into_inner().keys, strs(&["news"]));

    let counts: Vec<(String, i64)> = c
        .pub_sub_num_sub(PubSubNumSubRequest {
            channels: strs(&["news", "nobody"]),
        })
        .await
        .unwrap()
        .into_inner()
        .counts
        .into_iter()
        .map(|cc| (cc.channel, cc.count))
        .collect();
    assert_eq!(
        counts,
        vec![("news".to_string(), 1), ("nobody".to_string(), 0)]
    );

    let n = c.pub_sub_num_pat(PubSubNumPatRequest {}).await.unwrap();
    assert_eq!(n.into_inner().value, 1);
}

// ---------------------------------------------------------------------------
// pipeline
// ---------------------------------------------------------------------------

async fn pipeline(c: &mut Client, cmds: Vec<PipeCmd>) -> Vec<PipelineResponse> {
    let reqs: Vec<PipelineRequest> = cmds
        .into_iter()
        .enumerate()
        .map(|(i, cmd)| PipelineRequest {
            id: i as u64 + 1,
            command: Some(cmd),
        })
        .collect();
    let mut stream = c
        .pipeline(tokio_stream::iter(reqs))
        .await
        .unwrap()
        .into_inner();
    let mut out = Vec::new();
    while let Some(resp) = stream.message().await.unwrap() {
        out.push(resp);
    }
    out
}

#[tokio::test]
async fn grpc_pipeline() {
    let (_s, mut c) = start().await;

    let responses = pipeline(
        &mut c,
        vec![
            PipeCmd::Set(SetRequest {
                key: "k".into(),
                value: b("1"),
                ..Default::default()
            }),
            PipeCmd::Incr(IncrRequest { key: "k".into() }),
            PipeCmd::Get(GetRequest { key: "k".into() }),
            PipeCmd::Rpush(RPushRequest {
                key: "l".into(),
                values: bs(&["a", "b"]),
            }),
            PipeCmd::Lrange(LRangeRequest {
                key: "l".into(),
                start: 0,
                stop: -1,
            }),
            PipeCmd::Ttl(TtlRequest { key: "k".into() }),
            PipeCmd::Type(TypeRequest { key: "l".into() }),
            PipeCmd::Ping(PingRequest { message: None }),
            PipeCmd::Dbsize(DbSizeRequest {}),
        ],
    )
    .await;

    let ids: Vec<u64> = responses.iter().map(|r| r.id).collect();
    assert_eq!(ids, (1..=9).collect::<Vec<_>>());
    let results: Vec<PipeResult> = responses.into_iter().map(|r| r.result.unwrap()).collect();
    assert_eq!(
        results,
        vec![
            PipeResult::Set(SetResponse { ok: true }),
            PipeResult::IntVal(IntResponse { value: 2 }),
            PipeResult::Get(GetResponse {
                value: Some(b("2"))
            }),
            PipeResult::IntVal(IntResponse { value: 2 }),
            PipeResult::Array(ArrayResponse {
                values: bs(&["a", "b"])
            }),
            PipeResult::Ttl(TtlResponse { value: -1 }),
            PipeResult::Type(TypeResponse {
                type_name: "list".into()
            }),
            PipeResult::Ping(PingResponse {
                message: "PONG".into()
            }),
            PipeResult::IntVal(IntResponse { value: 2 }),
        ]
    );
}

#[tokio::test]
async fn grpc_pipeline_errors_do_not_abort() {
    let (_s, mut c) = start().await;
    rpush(&mut c, "l", &["a"]).await;

    let responses = pipeline(
        &mut c,
        vec![
            PipeCmd::Get(GetRequest { key: "l".into() }),
            PipeCmd::Llen(LLenRequest { key: "l".into() }),
        ],
    )
    .await;
    assert_eq!(responses.len(), 2);

    match &responses[0].result {
        Some(PipeResult::Error(e)) => assert!(e.message.starts_with("WRONGTYPE"), "{e:?}"),
        other => panic!("expected an error result, got {other:?}"),
    }
    assert_eq!(
        responses[1].result,
        Some(PipeResult::IntVal(IntResponse { value: 1 }))
    );

    // a request without a command yields an error entry, not a failed stream
    let mut stream = c
        .pipeline(tokio_stream::iter(vec![PipelineRequest {
            id: 7,
            command: None,
        }]))
        .await
        .unwrap()
        .into_inner();
    let resp = stream.message().await.unwrap().unwrap();
    assert_eq!(resp.id, 7);
    assert!(matches!(resp.result, Some(PipeResult::Error(_))));
}
