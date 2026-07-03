//! List command handlers for the gRPC service.

use std::time::Instant;

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse};
use tonic::{Request, Response, Status};

use super::proto::*;
use super::{unexpected_response, validate_key, validate_value, value_to_bytes, EmberService};

pub(super) async fn l_push(
    svc: &EmberService,
    request: Request<LPushRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let values: Vec<Bytes> = req.values.into_iter().map(Bytes::from).collect();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::LPush {
                key: req.key.clone(),
                values,
            },
        )
        .await?;
    svc.record_command(start, "LPUSH");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn r_push(
    svc: &EmberService,
    request: Request<RPushRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let values: Vec<Bytes> = req.values.into_iter().map(Bytes::from).collect();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::RPush {
                key: req.key.clone(),
                values,
            },
        )
        .await?;
    svc.record_command(start, "RPUSH");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn l_pop(
    svc: &EmberService,
    request: Request<LPopRequest>,
) -> Result<Response<GetResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::LPop { key: key.clone() })
        .await?;
    svc.record_command(start, "LPOP");

    match resp {
        ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
            value: Some(value_to_bytes(v)),
        })),
        ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn r_pop(
    svc: &EmberService,
    request: Request<RPopRequest>,
) -> Result<Response<GetResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::RPop { key: key.clone() })
        .await?;
    svc.record_command(start, "RPOP");

    match resp {
        ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
            value: Some(value_to_bytes(v)),
        })),
        ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn l_range(
    svc: &EmberService,
    request: Request<LRangeRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::LRange {
                key: req.key.clone(),
                start: req.start,
                stop: req.stop,
            },
        )
        .await?;
    svc.record_command(start, "LRANGE");

    match resp {
        ShardResponse::Array(arr) => Ok(Response::new(ArrayResponse {
            values: arr.into_iter().map(|b| b.to_vec()).collect(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn l_len(
    svc: &EmberService,
    request: Request<LLenRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::LLen { key: key.clone() })
        .await?;
    svc.record_command(start, "LLEN");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn l_index(
    svc: &EmberService,
    request: Request<LIndexRequest>,
) -> Result<Response<GetResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::LIndex {
                key: req.key.clone(),
                index: req.index,
            },
        )
        .await?;
    svc.record_command(start, "LINDEX");

    match resp {
        ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
            value: Some(value_to_bytes(v)),
        })),
        ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn l_set(
    svc: &EmberService,
    request: Request<LSetRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    validate_value(&req.value, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::LSet {
                key: req.key.clone(),
                index: req.index,
                value: bytes::Bytes::from(req.value),
            },
        )
        .await?;
    svc.record_command(start, "LSET");

    match resp {
        ShardResponse::Ok => Ok(Response::new(StatusResponse {
            status: "OK".to_string(),
        })),
        ShardResponse::Err(msg) => Err(Status::failed_precondition(msg)),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn l_trim(
    svc: &EmberService,
    request: Request<LTrimRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::LTrim {
                key: req.key.clone(),
                start: req.start,
                stop: req.stop,
            },
        )
        .await?;
    svc.record_command(start, "LTRIM");

    match resp {
        ShardResponse::Ok => Ok(Response::new(StatusResponse {
            status: "OK".to_string(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn l_insert(
    svc: &EmberService,
    request: Request<LInsertRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::LInsert {
                key: req.key.clone(),
                before: req.before,
                pivot: bytes::Bytes::from(req.pivot),
                value: bytes::Bytes::from(req.value),
            },
        )
        .await?;
    svc.record_command(start, "LINSERT");

    match resp {
        ShardResponse::Integer(n) => Ok(Response::new(IntResponse { value: n })),
        ShardResponse::OutOfMemory => Err(Status::resource_exhausted("OOM")),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn l_rem(
    svc: &EmberService,
    request: Request<LRemRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::LRem {
                key: req.key.clone(),
                count: req.count,
                value: bytes::Bytes::from(req.value),
            },
        )
        .await?;
    svc.record_command(start, "LREM");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn l_pos(
    svc: &EmberService,
    request: Request<LPosRequest>,
) -> Result<Response<OptionalIntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    // if count is absent or 0, find first occurrence (count=0 means "all" in
    // the engine, but we only return one result via OptionalIntResponse).
    let count = req.count.unwrap_or(0) as usize;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::LPos {
                key: req.key.clone(),
                element: bytes::Bytes::from(req.value),
                rank: 0,
                count,
                maxlen: 0,
            },
        )
        .await?;
    svc.record_command(start, "LPOS");

    match resp {
        ShardResponse::IntegerArray(positions) => Ok(Response::new(OptionalIntResponse {
            value: positions.into_iter().next(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn l_move(
    svc: &EmberService,
    request: Request<LMoveRequest>,
) -> Result<Response<GetResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.source, &svc.ctx.limits)?;
    validate_key(&req.destination, &svc.ctx.limits)?;

    if !svc.engine.same_shard(&req.source, &req.destination) {
        return Err(Status::failed_precondition(
            "ERR source and destination keys must hash to the same shard",
        ));
    }

    let resp = svc
        .route(
            &req.source,
            ShardRequest::LMove {
                source: req.source.clone(),
                destination: req.destination,
                src_left: req.src_left,
                dst_left: req.dst_left,
            },
        )
        .await?;
    svc.record_command(start, "LMOVE");

    match resp {
        ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
            value: Some(value_to_bytes(v)),
        })),
        ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
        ShardResponse::OutOfMemory => Err(Status::resource_exhausted("OOM")),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn lmpop(
    svc: &EmberService,
    request: Request<LmpopRequest>,
) -> Result<Response<LmpopResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    if req.keys.is_empty() {
        return Err(Status::invalid_argument("at least one key required"));
    }
    for k in &req.keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let count = req.count.max(1) as usize;

    for key in &req.keys {
        let resp = svc
            .route(
                key,
                ShardRequest::LmpopSingle {
                    key: key.clone(),
                    left: req.left,
                    count,
                },
            )
            .await?;
        match resp {
            ShardResponse::Array(items) if !items.is_empty() => {
                svc.record_command(start, "LMPOP");
                return Ok(Response::new(LmpopResponse {
                    found: true,
                    key: key.clone(),
                    elements: items.into_iter().map(|b| b.to_vec()).collect(),
                }));
            }
            ShardResponse::Array(_) | ShardResponse::Value(None) => continue,
            ShardResponse::WrongType => {
                return Err(Status::invalid_argument(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
            other => return Err(unexpected_response(&other)),
        }
    }

    svc.record_command(start, "LMPOP");
    Ok(Response::new(LmpopResponse {
        found: false,
        key: String::new(),
        elements: vec![],
    }))
}
