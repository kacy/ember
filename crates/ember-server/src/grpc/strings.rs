//! String and bitmap command handlers for the gRPC service.

use std::time::Instant;

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse, Value};
use ember_protocol::command::{BitOpKind, BitRange, BitRangeUnit};
use tonic::{Request, Response, Status};

use super::proto::*;
use super::{
    parse_expire, unexpected_response, validate_key, validate_value, value_to_bytes, EmberService,
};

pub(super) async fn get(
    svc: &EmberService,
    request: Request<GetRequest>,
) -> Result<Response<GetResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let key = req.key;
    validate_key(&key, &svc.ctx.limits)?;

    let resp = svc
        .route(&key, ShardRequest::Get { key: key.clone() })
        .await?;
    svc.record_command(start, "GET");

    match resp {
        ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
            value: Some(value_to_bytes(v)),
        })),
        ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn set(
    svc: &EmberService,
    request: Request<SetRequest>,
) -> Result<Response<SetResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    validate_value(&req.value, &svc.ctx.limits)?;

    let expire = parse_expire(req.expire_seconds, req.expire_millis);
    let resp = svc
        .route(
            &req.key,
            ShardRequest::Set {
                key: req.key.clone(),
                value: Bytes::from(req.value),
                expire,
                nx: req.nx,
                xx: req.xx,
            },
        )
        .await?;
    svc.record_command(start, "SET");

    match resp {
        ShardResponse::Ok => Ok(Response::new(SetResponse { ok: true })),
        ShardResponse::Value(None) => Ok(Response::new(SetResponse { ok: false })),
        ShardResponse::OutOfMemory => Err(Status::resource_exhausted("OOM")),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn m_get(
    svc: &EmberService,
    request: Request<MGetRequest>,
) -> Result<Response<MGetResponse>, Status> {
    let start = Instant::now();
    let keys = request.into_inner().keys;
    for k in &keys {
        validate_key(k, &svc.ctx.limits)?;
    }

    let responses = svc
        .engine
        .route_multi(&keys, |k| ShardRequest::Get { key: k })
        .await
        .map_err(|_| Status::unavailable("shard unavailable"))?;

    let values: Vec<OptionalValue> = responses
        .into_iter()
        .map(|resp| match resp {
            ShardResponse::Value(Some(v)) => OptionalValue {
                value: Some(value_to_bytes(v)),
            },
            _ => OptionalValue { value: None },
        })
        .collect();

    svc.record_command(start, "MGET");
    Ok(Response::new(MGetResponse { values }))
}

pub(super) async fn m_set(
    svc: &EmberService,
    request: Request<MSetRequest>,
) -> Result<Response<MSetResponse>, Status> {
    let start = Instant::now();
    let pairs = request.into_inner().pairs;
    for p in &pairs {
        validate_key(&p.key, &svc.ctx.limits)?;
        validate_value(&p.value, &svc.ctx.limits)?;
    }

    let keys: Vec<String> = pairs.iter().map(|p| p.key.clone()).collect();
    let values: Vec<Bytes> = pairs.into_iter().map(|p| Bytes::from(p.value)).collect();

    // dispatch all SETs concurrently
    let mut receivers = Vec::with_capacity(keys.len());
    for (key, value) in keys.iter().zip(values) {
        let idx = svc.engine.shard_for_key(key);
        let rx = svc
            .engine
            .dispatch_to_shard(
                idx,
                ShardRequest::Set {
                    key: key.clone(),
                    value,
                    expire: None,
                    nx: false,
                    xx: false,
                },
            )
            .await
            .map_err(|_| Status::unavailable("shard unavailable"))?;
        receivers.push(rx);
    }

    // collect all responses
    for rx in receivers {
        let resp = rx
            .await
            .map_err(|_| Status::unavailable("shard unavailable"))?;
        if let ShardResponse::OutOfMemory = resp {
            return Err(Status::resource_exhausted("OOM"));
        }
    }

    svc.record_command(start, "MSET");
    Ok(Response::new(MSetResponse {}))
}

pub(super) async fn incr(
    svc: &EmberService,
    request: Request<IncrRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::Incr { key: key.clone() })
        .await?;
    svc.record_command(start, "INCR");

    match resp {
        ShardResponse::Integer(v) => Ok(Response::new(IntResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn incr_by(
    svc: &EmberService,
    request: Request<IncrByRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::IncrBy {
                key: req.key.clone(),
                delta: req.delta,
            },
        )
        .await?;
    svc.record_command(start, "INCRBY");

    match resp {
        ShardResponse::Integer(v) => Ok(Response::new(IntResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn decr_by(
    svc: &EmberService,
    request: Request<DecrByRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::DecrBy {
                key: req.key.clone(),
                delta: req.delta,
            },
        )
        .await?;
    svc.record_command(start, "DECRBY");

    match resp {
        ShardResponse::Integer(v) => Ok(Response::new(IntResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn incr_by_float(
    svc: &EmberService,
    request: Request<IncrByFloatRequest>,
) -> Result<Response<FloatResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::IncrByFloat {
                key: req.key.clone(),
                delta: req.delta,
            },
        )
        .await?;
    svc.record_command(start, "INCRBYFLOAT");

    match resp {
        ShardResponse::BulkString(s) => Ok(Response::new(FloatResponse { value: s })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn append(
    svc: &EmberService,
    request: Request<AppendRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::Append {
                key: req.key.clone(),
                value: Bytes::from(req.value),
            },
        )
        .await?;
    svc.record_command(start, "APPEND");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn strlen(
    svc: &EmberService,
    request: Request<StrlenRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::Strlen { key: key.clone() })
        .await?;
    svc.record_command(start, "STRLEN");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn decr(
    svc: &EmberService,
    request: Request<DecrRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    validate_key(&key, &svc.ctx.limits)?;
    let resp = svc
        .route(&key, ShardRequest::Decr { key: key.clone() })
        .await?;
    svc.record_command(start, "DECR");

    match resp {
        ShardResponse::Integer(v) => Ok(Response::new(IntResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn get_del(
    svc: &EmberService,
    request: Request<GetDelRequest>,
) -> Result<Response<GetResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    validate_key(&key, &svc.ctx.limits)?;
    let resp = svc
        .route(&key, ShardRequest::GetDel { key: key.clone() })
        .await?;
    svc.record_command(start, "GETDEL");

    match resp {
        ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
            value: Some(value_to_bytes(v)),
        })),
        ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn get_ex(
    svc: &EmberService,
    request: Request<GetExRequest>,
) -> Result<Response<GetResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;

    // map proto expiry fields to the engine's Option<Option<u64>> convention:
    //   None        = leave TTL unchanged
    //   Some(None)  = persist (remove TTL)
    //   Some(Some(ms)) = set TTL to this many milliseconds
    let expire = if req.persist {
        Some(None)
    } else if req.expire_millis > 0 {
        Some(Some(req.expire_millis))
    } else if req.expire_seconds > 0 {
        Some(Some(req.expire_seconds * 1_000))
    } else {
        None
    };

    let resp = svc
        .route(
            &req.key,
            ShardRequest::GetEx {
                key: req.key.clone(),
                expire,
            },
        )
        .await?;
    svc.record_command(start, "GETEX");

    match resp {
        ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
            value: Some(value_to_bytes(v)),
        })),
        ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn get_range(
    svc: &EmberService,
    request: Request<GetRangeRequest>,
) -> Result<Response<GetResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::GetRange {
                key: req.key.clone(),
                start: req.start,
                end: req.end,
            },
        )
        .await?;
    svc.record_command(start, "GETRANGE");

    match resp {
        ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
            value: Some(value_to_bytes(v)),
        })),
        ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn set_range(
    svc: &EmberService,
    request: Request<SetRangeRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    validate_value(&req.value, &svc.ctx.limits)?;
    if req.offset < 0 {
        return Err(Status::invalid_argument("offset must not be negative"));
    }
    let resp = svc
        .route(
            &req.key,
            ShardRequest::SetRange {
                key: req.key.clone(),
                offset: req.offset as usize,
                value: bytes::Bytes::from(req.value),
            },
        )
        .await?;
    svc.record_command(start, "SETRANGE");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn getset(
    svc: &EmberService,
    request: Request<GetsetRequest>,
) -> Result<Response<GetResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::GetSet {
                key: req.key.clone(),
                value: req.value.into(),
            },
        )
        .await?;
    svc.record_command(start, "GETSET");

    match resp {
        ShardResponse::Value(opt) => Ok(Response::new(GetResponse {
            value: opt.and_then(|v| match v {
                Value::String(b) => Some(b.to_vec()),
                _ => None,
            }),
        })),
        ShardResponse::WrongType => Err(Status::invalid_argument(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn msetnx(
    svc: &EmberService,
    request: Request<MsetnxRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    if req.pairs.is_empty() {
        return Err(Status::invalid_argument("at least one pair required"));
    }
    for p in &req.pairs {
        validate_key(&p.key, &svc.ctx.limits)?;
    }
    let pairs: Vec<(String, Bytes)> = req
        .pairs
        .into_iter()
        .map(|kv| (kv.key, kv.value.into()))
        .collect();
    let first_key = pairs[0].0.clone();
    let resp = svc
        .route(&first_key, ShardRequest::MSetNx { pairs })
        .await?;
    svc.record_command(start, "MSETNX");

    match resp {
        ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn getbit(
    svc: &EmberService,
    request: Request<GetbitRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::GetBit {
                key: req.key.clone(),
                offset: req.offset,
            },
        )
        .await?;
    svc.record_command(start, "GETBIT");

    match resp {
        ShardResponse::Integer(n) => Ok(Response::new(IntResponse { value: n })),
        ShardResponse::WrongType => Err(Status::invalid_argument(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn setbit(
    svc: &EmberService,
    request: Request<SetbitRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    if req.value > 1 {
        return Err(Status::invalid_argument("bit value must be 0 or 1"));
    }
    let resp = svc
        .route(
            &req.key,
            ShardRequest::SetBit {
                key: req.key.clone(),
                offset: req.offset,
                value: req.value as u8,
            },
        )
        .await?;
    svc.record_command(start, "SETBIT");

    match resp {
        ShardResponse::Integer(n) => Ok(Response::new(IntResponse { value: n })),
        ShardResponse::WrongType => Err(Status::invalid_argument(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn bitcount(
    svc: &EmberService,
    request: Request<BitcountRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let range = if req.has_range {
        let unit = if req.unit == "BIT" {
            BitRangeUnit::Bit
        } else {
            BitRangeUnit::Byte
        };
        Some(BitRange {
            start: req.start,
            end: req.end,
            unit,
        })
    } else {
        None
    };
    let resp = svc
        .route(
            &req.key,
            ShardRequest::BitCount {
                key: req.key.clone(),
                range,
            },
        )
        .await?;
    svc.record_command(start, "BITCOUNT");

    match resp {
        ShardResponse::Integer(n) => Ok(Response::new(IntResponse { value: n })),
        ShardResponse::WrongType => Err(Status::invalid_argument(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn bitpos(
    svc: &EmberService,
    request: Request<BitposRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    if req.bit > 1 {
        return Err(Status::invalid_argument("bit argument must be 0 or 1"));
    }
    let range = if req.has_range {
        let unit = if req.unit == "BIT" {
            BitRangeUnit::Bit
        } else {
            BitRangeUnit::Byte
        };
        Some(BitRange {
            start: req.start,
            end: req.end,
            unit,
        })
    } else {
        None
    };
    let resp = svc
        .route(
            &req.key,
            ShardRequest::BitPos {
                key: req.key.clone(),
                bit: req.bit as u8,
                range,
            },
        )
        .await?;
    svc.record_command(start, "BITPOS");

    match resp {
        ShardResponse::Integer(n) => Ok(Response::new(IntResponse { value: n })),
        ShardResponse::WrongType => Err(Status::invalid_argument(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn bitop(
    svc: &EmberService,
    request: Request<BitopRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    if req.keys.is_empty() {
        return Err(Status::invalid_argument("at least one source key required"));
    }
    validate_key(&req.dest, &svc.ctx.limits)?;
    for k in &req.keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let op = match req.op.to_uppercase().as_str() {
        "AND" => BitOpKind::And,
        "OR" => BitOpKind::Or,
        "XOR" => BitOpKind::Xor,
        "NOT" => BitOpKind::Not,
        other => {
            return Err(Status::invalid_argument(format!(
                "unsupported BITOP operation: {other}"
            )));
        }
    };
    let resp = svc
        .route(
            &req.dest,
            ShardRequest::BitOp {
                op,
                dest: req.dest.clone(),
                keys: req.keys,
            },
        )
        .await?;
    svc.record_command(start, "BITOP");

    match resp {
        ShardResponse::Integer(n) => Ok(Response::new(IntResponse { value: n })),
        ShardResponse::WrongType => Err(Status::invalid_argument(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        other => Err(unexpected_response(&other)),
    }
}
