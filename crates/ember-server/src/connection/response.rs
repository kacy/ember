//! Response conversion — transforms ShardResponse into RESP3 frames.

use bytes::Bytes;
use ember_core::{ShardResponse, TtlResult, Value};
use ember_protocol::Frame;

use crate::server::ServerContext;
use crate::slowlog::SlowLog;

use super::{PendingResponse, ResponseTag};

/// Resolves a `PendingResponse` into a `Frame`, recording timing if applicable.
pub(super) async fn resolve_response(
    pending: PendingResponse,
    ctx: &ServerContext,
    slow_log: &SlowLog,
) -> Frame {
    match pending {
        PendingResponse::Immediate(frame) => frame,
        PendingResponse::Pending {
            rx,
            tag,
            start,
            cmd_name,
        } => {
            let frame = match rx.await {
                Ok(resp) => resolve_shard_response(resp, tag),
                Err(_) => Frame::Error("ERR shard unavailable".into()),
            };
            if let Some(start) = start {
                let elapsed = start.elapsed();
                slow_log.maybe_record(elapsed, cmd_name);
                if ctx.metrics_enabled {
                    let is_error = matches!(&frame, Frame::Error(_));
                    crate::metrics::record_command(cmd_name, elapsed, is_error);
                }
            }
            frame
        }
    }
}

/// Converts a `ShardResponse` to a `Frame` based on the response tag.
pub(super) fn resolve_shard_response(resp: ShardResponse, tag: ResponseTag) -> Frame {
    match tag {
        // Value(Some(String)) → Bulk, Value(None) → Null
        ResponseTag::Get | ResponseTag::PopResult | ResponseTag::HGetResult => match resp {
            ShardResponse::Value(Some(Value::String(data))) => Frame::Bulk(data),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // Ok → Simple("OK"), with optional Null/OOM
        ResponseTag::Set => match resp {
            ShardResponse::Ok => Frame::Simple("OK".into()),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // Bool → Integer(0/1), no type check
        ResponseTag::BoolToInt => match resp {
            ShardResponse::Bool(b) => Frame::Integer(i64::from(b)),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // Bool → Integer(0/1), with WrongType
        ResponseTag::HExistsResult | ResponseTag::SIsMemberResult => match resp {
            ShardResponse::Bool(b) => Frame::Integer(i64::from(b)),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // BoolArray → Array of Integer(0/1)
        ResponseTag::SMisMemberResult => match resp {
            ShardResponse::BoolArray(arr) => Frame::Array(
                arr.into_iter()
                    .map(|b| Frame::Integer(i64::from(b)))
                    .collect(),
            ),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // SetStoreResult → Integer (count), with WrongType/OOM
        ResponseTag::SetStoreResult => match resp {
            ShardResponse::SetStoreResult { count, .. } => Frame::Integer(count as i64),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::Ttl => match resp {
            ShardResponse::Ttl(TtlResult::Seconds(s)) => Frame::Integer(s as i64),
            ShardResponse::Ttl(TtlResult::NoExpiry) => Frame::Integer(-1),
            ShardResponse::Ttl(TtlResult::NotFound) => Frame::Integer(-2),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::Pttl => match resp {
            ShardResponse::Ttl(TtlResult::Milliseconds(ms)) => Frame::Integer(ms as i64),
            ShardResponse::Ttl(TtlResult::NoExpiry) => Frame::Integer(-1),
            ShardResponse::Ttl(TtlResult::NotFound) => Frame::Integer(-2),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // Integer → Integer, with WrongType/OOM/Err
        ResponseTag::IntResult => match resp {
            ShardResponse::Integer(n) => Frame::Integer(n),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(msg),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // Len → Integer, with WrongType only
        ResponseTag::LenResult => match resp {
            ShardResponse::Len(n) => Frame::Integer(n as i64),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // Len → Integer, with WrongType + OOM
        ResponseTag::LenResultOom | ResponseTag::HSetResult => match resp {
            ShardResponse::Len(n) => Frame::Integer(n as i64),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::FloatResult => match resp {
            ShardResponse::BulkString(val) => Frame::Bulk(Bytes::from(val)),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(msg),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // Array of Bytes → Array of Bulk
        ResponseTag::ArrayResult | ResponseTag::HValsResult | ResponseTag::SortResult => match resp
        {
            ShardResponse::Array(items) => {
                Frame::Array(items.into_iter().map(Frame::Bulk).collect())
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::TypeResult => match resp {
            ShardResponse::TypeName(name) => Frame::Simple(name.into()),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::ZAddResult => match resp {
            ShardResponse::ZAddLen { count, .. } => Frame::Integer(count as i64),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::ZRemResult => match resp {
            ShardResponse::ZRemLen { count, .. } => Frame::Integer(count as i64),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::ZScoreResult => match resp {
            ShardResponse::Score(Some(s)) => Frame::Bulk(Bytes::from(format!("{s}"))),
            ShardResponse::Score(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::ZRankResult => match resp {
            ShardResponse::Rank(Some(r)) => Frame::Integer(r as i64),
            ShardResponse::Rank(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::ZRangeResult { with_scores } => match resp {
            ShardResponse::ScoredArray(items) => {
                let mut frames = Vec::new();
                for (member, score) in items {
                    frames.push(Frame::Bulk(Bytes::from(member)));
                    if with_scores {
                        frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                    }
                }
                Frame::Array(frames)
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::ZIncrByResult => match resp {
            ShardResponse::ZIncrByResult { new_score, .. } => {
                Frame::Bulk(Bytes::from(format!("{new_score}")))
            }
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::ZPopResult => match resp {
            ShardResponse::ZPopResult(items) => {
                let mut frames = Vec::with_capacity(items.len() * 2);
                for (member, score) in items {
                    frames.push(Frame::Bulk(Bytes::from(member)));
                    frames.push(Frame::Bulk(Bytes::from(format!("{score}"))));
                }
                Frame::Array(frames)
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::HGetAllResult => match resp {
            ShardResponse::HashFields(fields) => {
                let mut frames = Vec::with_capacity(fields.len() * 2);
                for (field, value) in fields {
                    frames.push(Frame::Bulk(Bytes::from(field)));
                    frames.push(Frame::Bulk(value));
                }
                Frame::Array(frames)
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::HDelResult => match resp {
            ShardResponse::HDelLen { count, .. } => Frame::Integer(count as i64),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // Integer → Integer, with WrongType/OOM and prefixed Err
        ResponseTag::HIncrByResult => match resp {
            ShardResponse::Integer(n) => Frame::Integer(n),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(format!("ERR {msg}")),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // StringArray → Array of Bulk
        ResponseTag::StringArrayResult => match resp {
            ShardResponse::StringArray(items) => Frame::Array(
                items
                    .into_iter()
                    .map(|s| Frame::Bulk(Bytes::from(s)))
                    .collect(),
            ),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::HMGetResult => match resp {
            ShardResponse::OptionalArray(vals) => Frame::Array(
                vals.into_iter()
                    .map(|v| match v {
                        Some(data) => Frame::Bulk(data),
                        None => Frame::Null,
                    })
                    .collect(),
            ),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::CollectionScanResult => match resp {
            ShardResponse::CollectionScan { cursor, items } => {
                let cursor_frame = Frame::Bulk(Bytes::from(cursor.to_string()));
                let item_frames = items.into_iter().map(Frame::Bulk).collect();
                Frame::Array(vec![cursor_frame, Frame::Array(item_frames)])
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        ResponseTag::RenameResult => match resp {
            ShardResponse::Ok => Frame::Simple("OK".into()),
            ShardResponse::Err(msg) => Frame::Error(msg),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::CopyResult => match resp {
            ShardResponse::Bool(b) => Frame::Integer(i64::from(b)),
            ShardResponse::Err(msg) => Frame::Error(msg),
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::EncodingResult => match resp {
            ShardResponse::EncodingName(Some(name)) => Frame::Bulk(Bytes::from(name)),
            ShardResponse::EncodingName(None) => Frame::Null,
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // -- additional list commands --
        ResponseTag::LIndexResult => match resp {
            ShardResponse::Value(Some(Value::String(data))) => Frame::Bulk(data),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::LSetResult => match resp {
            ShardResponse::Ok => Frame::Simple("OK".into()),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::Err(msg) => Frame::Error(msg),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::LTrimResult => match resp {
            ShardResponse::Ok => Frame::Simple("OK".into()),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::LInsertResult => match resp {
            ShardResponse::Integer(n) => Frame::Integer(n),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::LRemResult => match resp {
            ShardResponse::Len(n) => Frame::Integer(n as i64),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        ResponseTag::LPosResult { count } => match resp {
            ShardResponse::IntegerArray(positions) => {
                if count.is_some() {
                    // COUNT was specified: always return array
                    Frame::Array(
                        positions
                            .into_iter()
                            .map(Frame::Integer)
                            .collect(),
                    )
                } else {
                    // no COUNT: return single integer or null
                    if let Some(&pos) = positions.first() {
                        Frame::Integer(pos)
                    } else {
                        Frame::Null
                    }
                }
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // -- vector commands --
        #[cfg(feature = "vector")]
        ResponseTag::VAddResult => match resp {
            ShardResponse::VAddResult { added, .. } => Frame::Integer(i64::from(added)),
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(format!("ERR {msg}")),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VAddBatchResult => match resp {
            ShardResponse::VAddBatchResult { added_count, .. } => {
                Frame::Integer(added_count as i64)
            }
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(format!("ERR {msg}")),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VSimResult { with_scores } => match resp {
            ShardResponse::VSimResult(results) => {
                let mut frames = Vec::new();
                for (element, distance) in results {
                    frames.push(Frame::Bulk(Bytes::from(element)));
                    if with_scores {
                        frames.push(Frame::Bulk(Bytes::from(distance.to_string())));
                    }
                }
                Frame::Array(frames)
            }
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VRemResult => match resp {
            ShardResponse::Bool(removed) => Frame::Integer(i64::from(removed)),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VGetResult => match resp {
            ShardResponse::VectorData(Some(vector)) => Frame::Array(
                vector
                    .into_iter()
                    .map(|v| Frame::Bulk(Bytes::from(v.to_string())))
                    .collect(),
            ),
            ShardResponse::VectorData(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VIntResult => match resp {
            ShardResponse::Integer(n) => Frame::Integer(n),
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "vector")]
        ResponseTag::VInfoResult => match resp {
            ShardResponse::VectorInfo(Some(fields)) => {
                let mut frames = Vec::with_capacity(fields.len() * 2);
                for (k, v) in fields {
                    frames.push(Frame::Bulk(Bytes::from(k)));
                    frames.push(Frame::Bulk(Bytes::from(v)));
                }
                Frame::Array(frames)
            }
            ShardResponse::VectorInfo(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },

        // -- protobuf commands --
        #[cfg(feature = "protobuf")]
        ResponseTag::ProtoSetResult => match resp {
            ShardResponse::Ok => Frame::Simple("OK".into()),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::OutOfMemory => oom_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "protobuf")]
        ResponseTag::ProtoGetResult => match resp {
            ShardResponse::ProtoValue(Some((type_name, data, _ttl))) => {
                Frame::Array(vec![Frame::Bulk(Bytes::from(type_name)), Frame::Bulk(data)])
            }
            ShardResponse::ProtoValue(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "protobuf")]
        ResponseTag::ProtoTypeResult => match resp {
            ShardResponse::ProtoTypeName(Some(name)) => Frame::Bulk(Bytes::from(name)),
            ShardResponse::ProtoTypeName(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "protobuf")]
        ResponseTag::ProtoSetFieldResult => match resp {
            ShardResponse::ProtoFieldUpdated { .. } => Frame::Simple("OK".into()),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(format!("ERR {msg}")),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
        #[cfg(feature = "protobuf")]
        ResponseTag::ProtoDelFieldResult => match resp {
            ShardResponse::ProtoFieldUpdated { .. } => Frame::Integer(1),
            ShardResponse::Value(None) => Frame::Null,
            ShardResponse::WrongType => wrongtype_error(),
            ShardResponse::OutOfMemory => oom_error(),
            ShardResponse::Err(msg) => Frame::Error(format!("ERR {msg}")),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        },
    }
}

/// Returns the standard WRONGTYPE error frame.
fn wrongtype_error() -> Frame {
    Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}

/// Returns the standard OOM error frame.
fn oom_error() -> Frame {
    Frame::Error("OOM command not allowed when used memory > 'maxmemory'".into())
}
