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
    // route to the source key's shard
    let idx = cx.engine.shard_for_key(&source);
    let req = ShardRequest::LMove {
        source,
        destination,
        src_left,
        dst_left,
    };
    super::route_to_shard(cx, idx, req, super::resp_string_value).await
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
