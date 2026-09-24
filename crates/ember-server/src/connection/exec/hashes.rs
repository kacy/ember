//! Hash command handlers.

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse};
use ember_protocol::Frame;

use super::ExecCtx;

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
