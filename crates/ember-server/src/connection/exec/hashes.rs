//! Hash command handlers.

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse, Value};
use ember_protocol::Frame;

use super::ExecCtx;

pub(in crate::connection) async fn hset(
    key: String,
    fields: Vec<(String, Bytes)>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HSet {
        key: key.clone(),
        fields,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Len(n)) => {
            cx.notify_write(crate::keyspace_notifications::FLAG_H, "hset", &key);
            Frame::Integer(n as i64)
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hget(key: String, field: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HGet { key, field };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hgetall(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HGetAll { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::HashFields(fields)) => {
            let mut frames = Vec::with_capacity(fields.len() * 2);
            for (field, value) in fields {
                frames.push(Frame::Bulk(Bytes::from(field)));
                frames.push(Frame::Bulk(value));
            }
            Frame::Array(frames)
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hdel(
    key: String,
    fields: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HDel { key, fields };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::HDelLen { count, .. }) => Frame::Integer(count as i64),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hexists(key: String, field: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HExists { key, field };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Bool(b)) => Frame::Integer(if b { 1 } else { 0 }),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hlen(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HLen { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hincrby(
    key: String,
    field: String,
    delta: i64,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HIncrBy { key, field, delta };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hincrbyfloat(
    key: String,
    field: String,
    delta: f64,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HIncrByFloat { key, field, delta };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::BulkString(val)) => Frame::Bulk(Bytes::from(val)),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hkeys(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HKeys { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::StringArray(keys)) => Frame::Array(
            keys.into_iter()
                .map(|k| Frame::Bulk(Bytes::from(k)))
                .collect(),
        ),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hvals(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HVals { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Array(vals)) => Frame::Array(vals.into_iter().map(Frame::Bulk).collect()),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hmget(
    key: String,
    fields: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HMGet { key, fields };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::OptionalArray(vals)) => Frame::Array(
            vals.into_iter()
                .map(|v| match v {
                    Some(data) => Frame::Bulk(data),
                    None => Frame::Null,
                })
                .collect(),
        ),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hrandfield(
    key: String,
    count: Option<i64>,
    with_values: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HRandField {
        key,
        count,
        with_values,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::HRandFieldResult(pairs)) => {
            if count.is_none() {
                // no count: return a single bulk string (or nil if empty)
                match pairs.into_iter().next() {
                    Some((field, _)) => Frame::Bulk(Bytes::from(field)),
                    None => Frame::Null,
                }
            } else {
                // with count: return array, interleaved with values if requested
                let frames: Vec<Frame> = pairs
                    .into_iter()
                    .flat_map(|(f, v)| {
                        let mut items = vec![Frame::Bulk(Bytes::from(f))];
                        if let Some(val) = v {
                            items.push(Frame::Bulk(val));
                        }
                        items
                    })
                    .collect();
                Frame::Array(frames)
            }
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn hscan(
    key: String,
    cursor: u64,
    pattern: Option<String>,
    count: Option<usize>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let count = count.unwrap_or(10);
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HScan {
        key,
        cursor,
        count,
        pattern,
    };
    super::resolve_collection_scan(cx.engine.send_to_shard(idx, req).await)
}
