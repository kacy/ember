//! List command handlers.

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse};
use ember_protocol::Frame;

use super::ExecCtx;

pub(in crate::connection) async fn lmove(
    source: String,
    destination: String,
    src_left: bool,
    dst_left: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    let src_idx = cx.engine.shard_for_key(&source);
    let dst_idx = cx.engine.shard_for_key(&destination);
    if src_idx == dst_idx {
        // one shard moves the element atomically
        let req = ShardRequest::LMove {
            source,
            destination,
            src_left,
            dst_left,
        };
        return super::route_to_shard(cx, src_idx, req, super::resp_string_value).await;
    }

    // across shards: check the destination, pop, then push, and put the
    // element back if the push fails so a failed move changes nothing
    if let Err(frame) = super::check_type(cx, dst_idx, &destination, "list").await {
        return frame;
    }
    let pop = if src_left {
        ShardRequest::LPop {
            key: source.clone(),
        }
    } else {
        ShardRequest::RPop {
            key: source.clone(),
        }
    };
    let value = match super::route_to_shard(cx, src_idx, pop, super::resp_string_value).await {
        Frame::Bulk(value) => value,
        other => return other,
    };
    let push = |key: String, left: bool| {
        let values = vec![value.clone()];
        if left {
            ShardRequest::LPush { key, values }
        } else {
            ShardRequest::RPush { key, values }
        }
    };
    let pushed = super::route_to_shard(cx, dst_idx, push(destination, dst_left), |_| {
        Frame::Bulk(value.clone())
    })
    .await;
    if matches!(pushed, Frame::Error(_)) {
        let _ = cx
            .engine
            .send_to_shard(src_idx, push(source, src_left))
            .await;
    }
    pushed
}

pub(in crate::connection) async fn lmpop(
    keys: Vec<String>,
    left: bool,
    count: usize,
    cx: &ExecCtx<'_>,
) -> Frame {
    for key in &keys {
        let idx = cx.engine.shard_for_key(key);
        let req = ShardRequest::LmpopSingle {
            key: key.clone(),
            left,
            count,
        };
        match cx.engine.send_to_shard(idx, req).await {
            Ok(ShardResponse::Array(items)) if !items.is_empty() => {
                let elems = Frame::Array(items.into_iter().map(Frame::Bulk).collect());
                return Frame::Array(vec![Frame::Bulk(Bytes::from(key.clone())), elems]);
            }
            Ok(ShardResponse::Array(_)) | Ok(ShardResponse::Value(None)) => continue,
            Ok(ShardResponse::WrongType) => return super::wrongtype_error(),
            Ok(other) => return Frame::Error(format!("ERR unexpected shard response: {other:?}")),
            Err(e) => return Frame::Error(format!("ERR {e}")),
        }
    }
    Frame::Null
}

/// blocking list ops are handled by handle_blocking_pop_cmd in the
/// main loop; reaching here means they're inside a transaction.
pub(in crate::connection) fn blpop_in_tx() -> Frame {
    Frame::Error("ERR blocking commands are not allowed inside transactions".into())
}

pub(in crate::connection) fn brpop_in_tx() -> Frame {
    Frame::Error("ERR blocking commands are not allowed inside transactions".into())
}
