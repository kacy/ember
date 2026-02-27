//! String and bitmap command handlers.

use std::time::Duration;

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse, Value};
use ember_protocol::{command::BitOpKind, Frame, SetExpire};

use super::ExecCtx;

pub(in crate::connection) async fn get(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Get { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn set(
    key: String,
    value: Bytes,
    expire: Option<SetExpire>,
    nx: bool,
    xx: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    let duration = expire.map(super::set_expire_to_duration);
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Set {
        key: key.clone(),
        value,
        expire: duration,
        nx,
        xx,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Ok) => {
            cx.notify_write(crate::keyspace_notifications::FLAG_DOLLAR, "set", &key);
            Frame::Simple("OK".into())
        }
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn incr(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Incr { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn decr(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Decr { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn incrby(key: String, delta: i64, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::IncrBy { key, delta };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn decrby(key: String, delta: i64, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::DecrBy { key, delta };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn incrbyfloat(key: String, delta: f64, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::IncrByFloat { key, delta };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::BulkString(val)) => Frame::Bulk(Bytes::from(val)),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn append(key: String, value: Bytes, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Append { key, value };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn strlen(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Strlen { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn getrange(
    key: String,
    start: i64,
    end: i64,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::GetRange { key, start, end };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn setrange(
    key: String,
    offset: usize,
    value: Bytes,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::SetRange { key, offset, value };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn getbit(key: String, offset: u64, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::GetBit { key, offset };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(bit)) => Frame::Integer(bit),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn setbit(
    key: String,
    offset: u64,
    value: u8,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::SetBit { key, offset, value };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(old_bit)) => Frame::Integer(old_bit),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn bitcount(
    key: String,
    range: Option<ember_protocol::command::BitRange>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::BitCount { key, range };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn bitpos(
    key: String,
    bit: u8,
    range: Option<ember_protocol::command::BitRange>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::BitPos { key, bit, range };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(pos)) => Frame::Integer(pos),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn bitop(
    op: BitOpKind,
    dest: String,
    keys: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    // Read source keys from their respective shards, then compute the
    // bitwise result here and write it to dest's shard. This is
    // necessary because source keys may live on different shards.
    let responses = match cx
        .engine
        .route_multi(&keys, |k| ShardRequest::Get { key: k })
        .await
    {
        Ok(r) => r,
        Err(e) => return Frame::Error(format!("ERR {e}")),
    };

    let mut sources: Vec<Bytes> = Vec::with_capacity(responses.len());
    for r in responses {
        match r {
            ShardResponse::Value(Some(Value::String(b))) => sources.push(b),
            ShardResponse::Value(None) => sources.push(Bytes::new()),
            ShardResponse::WrongType => return super::wrongtype_error(),
            _ => sources.push(Bytes::new()),
        }
    }

    let result_len = sources.iter().map(|s| s.len()).max().unwrap_or(0);
    let mut result = vec![0u8; result_len];
    match op {
        BitOpKind::Not => {
            let src = sources.first().map(|b| b.as_ref()).unwrap_or(&[]);
            for (i, b) in result.iter_mut().enumerate() {
                *b = if i < src.len() { !src[i] } else { 0xFF };
            }
        }
        BitOpKind::And => {
            if let Some(first) = sources.first() {
                for (i, b) in result.iter_mut().enumerate() {
                    *b = if i < first.len() { first[i] } else { 0 };
                }
            }
            for src in sources.iter().skip(1) {
                for (i, b) in result.iter_mut().enumerate() {
                    *b &= if i < src.len() { src[i] } else { 0 };
                }
            }
        }
        BitOpKind::Or => {
            for src in &sources {
                for (i, b) in result.iter_mut().enumerate() {
                    if i < src.len() {
                        *b |= src[i];
                    }
                }
            }
        }
        BitOpKind::Xor => {
            for src in &sources {
                for (i, b) in result.iter_mut().enumerate() {
                    if i < src.len() {
                        *b ^= src[i];
                    }
                }
            }
        }
    }

    let dest_idx = cx.engine.shard_for_key(&dest);
    let req = ShardRequest::Set {
        key: dest,
        value: Bytes::from(result),
        expire: None,
        nx: false,
        xx: false,
    };
    match cx.engine.send_to_shard(dest_idx, req).await {
        Ok(ShardResponse::Ok) | Ok(ShardResponse::Value(_)) => Frame::Integer(result_len as i64),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn getdel(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::GetDel { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn getex(
    key: String,
    expire: Option<Option<SetExpire>>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    // convert SetExpire into an Option<Option<u64>> (milliseconds from now)
    let expire_ms: Option<Option<u64>> = expire.map(|opt| {
        opt.map(|se| match se {
            SetExpire::Ex(s) => Duration::from_secs(s).as_millis() as u64,
            SetExpire::Px(ms) => ms,
            SetExpire::ExAt(ts) => {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now_s = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                Duration::from_secs(ts.saturating_sub(now_s)).as_millis() as u64
            }
            SetExpire::PxAt(ts_ms) => {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                ts_ms.saturating_sub(now_ms)
            }
        })
    });
    let req = ShardRequest::GetEx {
        key,
        expire: expire_ms,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn getset(key: String, value: Bytes, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::GetSet { key, value };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn mget(keys: Vec<String>, cx: &ExecCtx<'_>) -> Frame {
    match cx
        .engine
        .route_multi(&keys, |k| ShardRequest::Get { key: k })
        .await
    {
        Ok(responses) => {
            let frames: Vec<Frame> = responses
                .into_iter()
                .map(|r| match r {
                    ShardResponse::Value(Some(Value::String(data))) => Frame::Bulk(data),
                    ShardResponse::Value(None) => Frame::Null,
                    // MGET on wrong type returns null per Redis behavior
                    ShardResponse::WrongType => Frame::Null,
                    _ => Frame::Null,
                })
                .collect();
            Frame::Array(frames)
        }
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn mset(pairs: Vec<(String, Bytes)>, cx: &ExecCtx<'_>) -> Frame {
    // Fan out individual SET requests — MSET always succeeds (or OOMs).
    // We build a HashMap for O(1) value lookups during routing. If there
    // are duplicate keys in pairs, the HashMap keeps the last value, which
    // matches Redis semantics (last write wins).
    let keys: Vec<String> = pairs.iter().map(|(k, _)| k.clone()).collect();
    let values: std::collections::HashMap<String, Bytes> = pairs.into_iter().collect();

    match cx
        .engine
        .route_multi(&keys, |k| {
            // Safe: k comes from keys, which came from pairs, so it exists in values.
            let value = values.get(&k).cloned().unwrap_or_default();
            ShardRequest::Set {
                key: k,
                value,
                expire: None,
                nx: false,
                xx: false,
            }
        })
        .await
    {
        Ok(responses) => {
            // check if any SET failed due to OOM
            for r in &responses {
                if matches!(r, ShardResponse::OutOfMemory) {
                    return super::oom_error();
                }
            }
            Frame::Simple("OK".into())
        }
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn msetnx(pairs: Vec<(String, Bytes)>, cx: &ExecCtx<'_>) -> Frame {
    // MSETNX is all-or-nothing: set all keys only if none exist.
    //
    // We implement this with two fan-out passes:
    //   1. Check existence of every key across all shards.
    //   2. If all are absent, write all pairs.
    //
    // This is not atomic across shards (no distributed transaction),
    // but matches Redis cluster semantics where MSETNX pairs must
    // share a hash slot. For single-node mode it is correct.
    if pairs.is_empty() {
        return Frame::Error("ERR wrong number of arguments for 'MSETNX'".into());
    }

    let keys: Vec<String> = pairs.iter().map(|(k, _)| k.clone()).collect();

    // phase 1: check existence
    let exists_responses = match cx
        .engine
        .route_multi(&keys, |k| ShardRequest::Exists { key: k })
        .await
    {
        Ok(r) => r,
        Err(e) => return Frame::Error(format!("ERR {e}")),
    };

    let any_exists = exists_responses
        .iter()
        .any(|r| matches!(r, ShardResponse::Bool(true)));
    if any_exists {
        return Frame::Integer(0);
    }

    // phase 2: write all pairs
    let values: std::collections::HashMap<String, Bytes> = pairs.into_iter().collect();
    match cx
        .engine
        .route_multi(&keys, |k| {
            let value = values.get(&k).cloned().unwrap_or_default();
            ShardRequest::Set {
                key: k,
                value,
                expire: None,
                nx: false,
                xx: false,
            }
        })
        .await
    {
        Ok(responses) => {
            for r in &responses {
                if matches!(r, ShardResponse::OutOfMemory) {
                    return super::oom_error();
                }
            }
            Frame::Integer(1)
        }
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

// Suppress unused import warning — BytesMut is not needed in this module but
// was present in the original. Keep it removed since we don't use it here.
