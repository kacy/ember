//! List command handlers.

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse, Value};
use ember_protocol::Frame;

use super::ExecCtx;

pub(in crate::connection) async fn lpush(
    key: String,
    values: Vec<Bytes>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::LPush {
        key: key.clone(),
        values,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Len(n)) => {
            cx.notify_write(crate::keyspace_notifications::FLAG_L, "lpush", &key);
            Frame::Integer(n as i64)
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn rpush(
    key: String,
    values: Vec<Bytes>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::RPush {
        key: key.clone(),
        values,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Len(n)) => {
            cx.notify_write(crate::keyspace_notifications::FLAG_L, "rpush", &key);
            Frame::Integer(n as i64)
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn lpop(
    key: String,
    count: Option<usize>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    match count {
        None => {
            let req = ShardRequest::LPop { key };
            match cx.engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => super::wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }
        Some(count) => {
            let req = ShardRequest::LPopCount { key, count };
            match cx.engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Array(items)) => {
                    Frame::Array(items.into_iter().map(Frame::Bulk).collect())
                }
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => super::wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }
    }
}

pub(in crate::connection) async fn rpop(
    key: String,
    count: Option<usize>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    match count {
        None => {
            let req = ShardRequest::RPop { key };
            match cx.engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => super::wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }
        Some(count) => {
            let req = ShardRequest::RPopCount { key, count };
            match cx.engine.send_to_shard(idx, req).await {
                Ok(ShardResponse::Array(items)) => {
                    Frame::Array(items.into_iter().map(Frame::Bulk).collect())
                }
                Ok(ShardResponse::Value(None)) => Frame::Null,
                Ok(ShardResponse::WrongType) => super::wrongtype_error(),
                Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }
    }
}

pub(in crate::connection) async fn lrange(
    key: String,
    start: i64,
    stop: i64,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::LRange { key, start, stop };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Array(items)) => {
            Frame::Array(items.into_iter().map(Frame::Bulk).collect())
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn llen(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::LLen { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn lindex(key: String, index: i64, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::LIndex { key, index };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn lset(
    key: String,
    index: i64,
    value: Bytes,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::LSet { key, index, value };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn ltrim(
    key: String,
    start: i64,
    stop: i64,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::LTrim { key, start, stop };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn linsert(
    key: String,
    before: bool,
    pivot: Bytes,
    value: Bytes,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::LInsert {
        key,
        before,
        pivot,
        value,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn lrem(
    key: String,
    count: i64,
    value: Bytes,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::LRem { key, count, value };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Len(n)) => Frame::Integer(n as i64),
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn lpos(
    key: String,
    element: Bytes,
    rank: i64,
    count: Option<usize>,
    maxlen: usize,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let shard_count = count.unwrap_or(1);
    let req = ShardRequest::LPos {
        key,
        element,
        rank,
        count: shard_count,
        maxlen,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::IntegerArray(positions)) => {
            if count.is_some() {
                Frame::Array(positions.into_iter().map(Frame::Integer).collect())
            } else if let Some(&pos) = positions.first() {
                Frame::Integer(pos)
            } else {
                Frame::Null
            }
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

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
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Value(Some(Value::String(data)))) => Frame::Bulk(data),
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
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
