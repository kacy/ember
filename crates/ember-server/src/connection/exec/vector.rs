//! Vector command handlers.
//!
//! All items in this module are gated behind `#[cfg(feature = "vector")]`.

use ember_protocol::Frame;

#[cfg(feature = "vector")]
use super::ExecCtx;
#[cfg(feature = "vector")]
use bytes::Bytes;
#[cfg(feature = "vector")]
use ember_core::{ShardRequest, ShardResponse};

#[cfg(feature = "vector")]
pub(in crate::connection) async fn vadd(
    key: String,
    element: String,
    vector: Vec<f32>,
    metric: ember_core::VectorMetric,
    quantization: ember_core::VectorQuantization,
    connectivity: Option<usize>,
    expansion_add: Option<usize>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::VAdd {
        key,
        element,
        vector,
        metric,
        quantization,
        connectivity,
        expansion_add,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::VAddResult { added, .. }) => Frame::Integer(if added { 1 } else { 0 }),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "vector")]
pub(in crate::connection) async fn vaddbatch(
    key: String,
    entries: Vec<(String, Vec<f32>)>,
    dim: usize,
    metric: ember_core::VectorMetric,
    quantization: ember_core::VectorQuantization,
    connectivity: Option<usize>,
    expansion_add: Option<usize>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::VAddBatch {
        key,
        entries,
        dim,
        metric,
        quantization,
        connectivity,
        expansion_add,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::VAddBatchResult { added_count, .. }) => {
            Frame::Integer(added_count as i64)
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "vector")]
pub(in crate::connection) async fn vsim(
    key: String,
    query: ember_core::VectorQuery,
    count: usize,
    ef_search: Option<usize>,
    with_scores: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::VSim {
        key,
        query,
        count,
        ef_search,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::VSimResult(results)) => {
            let mut frames = Vec::new();
            for (element, distance) in results {
                frames.push(Frame::Bulk(Bytes::from(element)));
                if with_scores {
                    frames.push(Frame::Bulk(Bytes::from(distance.to_string())));
                }
            }
            Frame::Array(frames)
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "vector")]
pub(in crate::connection) async fn vrem(key: String, element: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::VRem { key, element };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Bool(removed)) => Frame::Integer(if removed { 1 } else { 0 }),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "vector")]
pub(in crate::connection) async fn vget(key: String, element: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::VGet { key, element };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::VectorData(Some(vector))) => Frame::Array(
            vector
                .into_iter()
                .map(|v| Frame::Bulk(Bytes::from(v.to_string())))
                .collect(),
        ),
        Ok(ShardResponse::VectorData(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "vector")]
pub(in crate::connection) async fn vcard(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::VCard { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(count)) => Frame::Integer(count),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "vector")]
pub(in crate::connection) async fn vdim(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::VDim { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(dim)) => Frame::Integer(dim),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "vector")]
pub(in crate::connection) async fn vinfo(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::VInfo { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::VectorInfo(Some(fields))) => {
            let mut frames = Vec::with_capacity(fields.len() * 2);
            for (k, v) in fields {
                frames.push(Frame::Bulk(Bytes::from(k)));
                frames.push(Frame::Bulk(Bytes::from(v)));
            }
            Frame::Array(frames)
        }
        Ok(ShardResponse::VectorInfo(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

/// Returns an error when vector support is not compiled in.
#[cfg(not(feature = "vector"))]
pub(in crate::connection) fn not_compiled() -> Frame {
    Frame::Error("ERR unknown command (vector support not compiled)".into())
}
