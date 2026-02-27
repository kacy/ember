//! Protobuf command handlers.
//!
//! All items in this module are gated behind `#[cfg(feature = "protobuf")]`.

use ember_protocol::Frame;

#[cfg(feature = "protobuf")]
use super::ExecCtx;
#[cfg(feature = "protobuf")]
use bytes::Bytes;
#[cfg(feature = "protobuf")]
use ember_core::{ShardRequest, ShardResponse};

#[cfg(feature = "protobuf")]
pub(in crate::connection) async fn proto_register(
    name: String,
    descriptor: Bytes,
    cx: &ExecCtx<'_>,
) -> Frame {
    use std::time::Duration;

    let registry = match cx.engine.schema_registry() {
        Some(r) => r,
        None => return Frame::Error("ERR protobuf support is not enabled".into()),
    };
    let result = {
        let mut reg = match registry.write() {
            Ok(r) => r,
            Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
        };
        reg.register(name.clone(), descriptor.clone())
    };
    match result {
        Ok(types) => {
            // persist the registration to all shards' AOF
            if let Err(e) = cx
                .engine
                .broadcast(|| ShardRequest::ProtoRegisterAof {
                    name: name.clone(),
                    descriptor: descriptor.clone(),
                })
                .await
            {
                tracing::warn!("failed to persist proto registration to AOF: {e}");
            }
            Frame::Array(
                types
                    .into_iter()
                    .map(|t| Frame::Bulk(Bytes::from(t)))
                    .collect(),
            )
        }
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "protobuf")]
pub(in crate::connection) async fn proto_set(
    key: String,
    type_name: String,
    data: Bytes,
    expire: Option<ember_protocol::SetExpire>,
    nx: bool,
    xx: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    use std::time::Duration;

    let registry = match cx.engine.schema_registry() {
        Some(r) => r,
        None => return Frame::Error("ERR protobuf support is not enabled".into()),
    };
    // validate the bytes against the schema before storing
    {
        let reg = match registry.read() {
            Ok(r) => r,
            Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
        };
        if let Err(e) = reg.validate(&type_name, &data) {
            return Frame::Error(format!("ERR {e}"));
        }
    }
    let duration = expire.map(|e| {
        use std::time::{SystemTime, UNIX_EPOCH};
        match e {
            ember_protocol::SetExpire::Ex(secs) => Duration::from_secs(secs),
            ember_protocol::SetExpire::Px(millis) => Duration::from_millis(millis),
            ember_protocol::SetExpire::ExAt(ts) => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                Duration::from_secs(ts.saturating_sub(now))
            }
            ember_protocol::SetExpire::PxAt(ts_ms) => {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                Duration::from_millis(ts_ms.saturating_sub(now_ms))
            }
        }
    });
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ProtoSet {
        key,
        type_name,
        data,
        expire: duration,
        nx,
        xx,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "protobuf")]
pub(in crate::connection) async fn proto_get(key: String, cx: &ExecCtx<'_>) -> Frame {
    if cx.engine.schema_registry().is_none() {
        return Frame::Error("ERR protobuf support is not enabled".into());
    }
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ProtoGet { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::ProtoValue(Some((type_name, data, _ttl)))) => {
            Frame::Array(vec![Frame::Bulk(Bytes::from(type_name)), Frame::Bulk(data)])
        }
        Ok(ShardResponse::ProtoValue(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "protobuf")]
pub(in crate::connection) async fn proto_type(key: String, cx: &ExecCtx<'_>) -> Frame {
    if cx.engine.schema_registry().is_none() {
        return Frame::Error("ERR protobuf support is not enabled".into());
    }
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ProtoType { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::ProtoTypeName(Some(name))) => Frame::Bulk(Bytes::from(name)),
        Ok(ShardResponse::ProtoTypeName(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "protobuf")]
pub(in crate::connection) async fn proto_schemas(cx: &ExecCtx<'_>) -> Frame {
    let registry = match cx.engine.schema_registry() {
        Some(r) => r,
        None => return Frame::Error("ERR protobuf support is not enabled".into()),
    };
    let reg = match registry.read() {
        Ok(r) => r,
        Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
    };
    let names = reg.schema_names();
    Frame::Array(
        names
            .into_iter()
            .map(|n| Frame::Bulk(Bytes::from(n)))
            .collect(),
    )
}

#[cfg(feature = "protobuf")]
pub(in crate::connection) async fn proto_describe(name: String, cx: &ExecCtx<'_>) -> Frame {
    let registry = match cx.engine.schema_registry() {
        Some(r) => r,
        None => return Frame::Error("ERR protobuf support is not enabled".into()),
    };
    let reg = match registry.read() {
        Ok(r) => r,
        Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
    };
    match reg.describe(&name) {
        Some(types) => Frame::Array(
            types
                .into_iter()
                .map(|t| Frame::Bulk(Bytes::from(t)))
                .collect(),
        ),
        None => Frame::Error(format!("ERR unknown schema '{name}'")),
    }
}

#[cfg(feature = "protobuf")]
pub(in crate::connection) async fn proto_get_field(
    key: String,
    field_path: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    let registry = match cx.engine.schema_registry() {
        Some(r) => r,
        None => return Frame::Error("ERR protobuf support is not enabled".into()),
    };
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ProtoGet { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::ProtoValue(Some((type_name, data, _ttl)))) => {
            let reg = match registry.read() {
                Ok(r) => r,
                Err(_) => return Frame::Error("ERR schema registry lock poisoned".into()),
            };
            match reg.get_field(&type_name, &data, &field_path) {
                Ok(frame) => frame,
                Err(e) => Frame::Error(format!("ERR {e}")),
            }
        }
        Ok(ShardResponse::ProtoValue(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "protobuf")]
pub(in crate::connection) async fn proto_set_field(
    key: String,
    field_path: String,
    value: bytes::Bytes,
    cx: &ExecCtx<'_>,
) -> Frame {
    if cx.engine.schema_registry().is_none() {
        return Frame::Error("ERR protobuf support is not enabled".into());
    }
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ProtoSetField {
        key,
        field_path,
        value,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::ProtoFieldUpdated { .. }) => Frame::Simple("OK".into()),
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

#[cfg(feature = "protobuf")]
pub(in crate::connection) async fn proto_del_field(
    key: String,
    field_path: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    if cx.engine.schema_registry().is_none() {
        return Frame::Error("ERR protobuf support is not enabled".into());
    }
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ProtoDelField { key, field_path };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::ProtoFieldUpdated { .. }) => Frame::Integer(1),
        Ok(ShardResponse::Value(None)) => Frame::Null,
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(ShardResponse::Err(msg)) => Frame::Error(format!("ERR {msg}")),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

/// Returns an error when protobuf support is not compiled in.
#[cfg(not(feature = "protobuf"))]
pub(in crate::connection) fn not_compiled() -> Frame {
    Frame::Error("ERR unknown command (protobuf support not compiled)".into())
}
