//! Hash command handlers.

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse};
use ember_protocol::Frame;

use super::ExecCtx;

pub(in crate::connection) async fn hset(
    key: String,
    fields: Vec<(String, Bytes)>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let notify_key = key.clone();
    let req = ShardRequest::HSet { key, fields };
    super::route_to_shard(cx, idx, req, |resp| {
        let frame = super::resp_len(resp);
        cx.notify_write(crate::keyspace_notifications::FLAG_H, "hset", &notify_key);
        frame
    })
    .await
}

pub(in crate::connection) async fn hget(key: String, field: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HGet { key, field };
    super::route_to_shard(cx, idx, req, super::resp_string_value).await
}

pub(in crate::connection) async fn hgetall(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HGetAll { key };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::HashFields(fields) => {
            let mut frames = Vec::with_capacity(fields.len() * 2);
            for (field, value) in fields {
                frames.push(Frame::Bulk(Bytes::from(field)));
                frames.push(Frame::Bulk(value));
            }
            Frame::Array(frames)
        }
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
}

pub(in crate::connection) async fn hdel(
    key: String,
    fields: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HDel { key, fields };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::HDelLen { count, .. } => Frame::Integer(count as i64),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
}

pub(in crate::connection) async fn hexists(key: String, field: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HExists { key, field };
    super::route_to_shard(cx, idx, req, super::resp_bool_int).await
}

pub(in crate::connection) async fn hlen(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HLen { key };
    super::route_to_shard(cx, idx, req, super::resp_len).await
}

pub(in crate::connection) async fn hincrby(
    key: String,
    field: String,
    delta: i64,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HIncrBy { key, field, delta };
    super::route_to_shard(cx, idx, req, super::resp_integer).await
}

pub(in crate::connection) async fn hincrbyfloat(
    key: String,
    field: String,
    delta: f64,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HIncrByFloat { key, field, delta };
    super::route_to_shard(cx, idx, req, super::resp_bulk_string).await
}

pub(in crate::connection) async fn hkeys(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HKeys { key };
    super::route_to_shard(cx, idx, req, super::resp_string_array).await
}

pub(in crate::connection) async fn hvals(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HVals { key };
    super::route_to_shard(cx, idx, req, super::resp_bulk_array).await
}

pub(in crate::connection) async fn hmget(
    key: String,
    fields: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::HMGet { key, fields };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::OptionalArray(vals) => Frame::Array(
            vals.into_iter()
                .map(|v| match v {
                    Some(data) => Frame::Bulk(data),
                    None => Frame::Null,
                })
                .collect(),
        ),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
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
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::HRandFieldResult(pairs) => {
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
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
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
