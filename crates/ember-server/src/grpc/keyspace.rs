//! Key management command handlers for the gRPC service.

use std::time::Instant;

use ember_core::{ShardRequest, ShardResponse, TtlResult};
use tonic::{Request, Response, Status};

use super::proto::*;
use super::{unexpected_response, validate_key, EmberService};

pub(super) async fn del(
    svc: &EmberService,
    request: Request<DelRequest>,
) -> Result<Response<DelResponse>, Status> {
    let start = Instant::now();
    let keys = request.into_inner().keys;
    for k in &keys {
        validate_key(k, &svc.ctx.limits)?;
    }

    let responses = svc
        .engine
        .route_multi(&keys, |k| ShardRequest::Del { key: k })
        .await
        .map_err(|_| Status::unavailable("shard unavailable"))?;

    let mut deleted = 0i64;
    for resp in responses {
        if let ShardResponse::Bool(true) = resp {
            deleted += 1;
        }
    }
    svc.record_command(start, "DEL");

    Ok(Response::new(DelResponse { deleted }))
}

pub(super) async fn exists(
    svc: &EmberService,
    request: Request<ExistsRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let keys = request.into_inner().keys;

    let responses = svc
        .engine
        .route_multi(&keys, |k| ShardRequest::Exists { key: k })
        .await
        .map_err(|_| Status::unavailable("shard unavailable"))?;

    let mut count = 0i64;
    for resp in responses {
        if let ShardResponse::Bool(true) = resp {
            count += 1;
        }
    }
    svc.record_command(start, "EXISTS");
    Ok(Response::new(IntResponse { value: count }))
}

pub(super) async fn expire(
    svc: &EmberService,
    request: Request<ExpireRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::Expire {
                key: req.key.clone(),
                seconds: req.seconds,
            },
        )
        .await?;
    svc.record_command(start, "EXPIRE");

    match resp {
        ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn p_expire(
    svc: &EmberService,
    request: Request<PExpireRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::Pexpire {
                key: req.key.clone(),
                milliseconds: req.milliseconds,
            },
        )
        .await?;
    svc.record_command(start, "PEXPIRE");

    match resp {
        ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn persist(
    svc: &EmberService,
    request: Request<PersistRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::Persist { key: key.clone() })
        .await?;
    svc.record_command(start, "PERSIST");

    match resp {
        ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn ttl(
    svc: &EmberService,
    request: Request<TtlRequest>,
) -> Result<Response<TtlResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::Ttl { key: key.clone() })
        .await?;
    svc.record_command(start, "TTL");

    match resp {
        ShardResponse::Ttl(TtlResult::Seconds(s)) => {
            Ok(Response::new(TtlResponse { value: s as i64 }))
        }
        ShardResponse::Ttl(TtlResult::Milliseconds(ms)) => Ok(Response::new(TtlResponse {
            value: (ms / 1000) as i64,
        })),
        ShardResponse::Ttl(TtlResult::NoExpiry) => Ok(Response::new(TtlResponse { value: -1 })),
        ShardResponse::Ttl(TtlResult::NotFound) => Ok(Response::new(TtlResponse { value: -2 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn p_ttl(
    svc: &EmberService,
    request: Request<PTtlRequest>,
) -> Result<Response<TtlResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::Pttl { key: key.clone() })
        .await?;
    svc.record_command(start, "PTTL");

    match resp {
        ShardResponse::Ttl(TtlResult::Milliseconds(ms)) => {
            Ok(Response::new(TtlResponse { value: ms as i64 }))
        }
        ShardResponse::Ttl(TtlResult::Seconds(s)) => Ok(Response::new(TtlResponse {
            value: s.saturating_mul(1000) as i64,
        })),
        ShardResponse::Ttl(TtlResult::NoExpiry) => Ok(Response::new(TtlResponse { value: -1 })),
        ShardResponse::Ttl(TtlResult::NotFound) => Ok(Response::new(TtlResponse { value: -2 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn r#type(
    svc: &EmberService,
    request: Request<TypeRequest>,
) -> Result<Response<TypeResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::Type { key: key.clone() })
        .await?;
    svc.record_command(start, "TYPE");

    match resp {
        ShardResponse::TypeName(name) => Ok(Response::new(TypeResponse {
            type_name: name.to_string(),
        })),
        _ => Ok(Response::new(TypeResponse {
            type_name: "none".to_string(),
        })),
    }
}

pub(super) async fn keys(
    svc: &EmberService,
    request: Request<KeysRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let start = Instant::now();
    let pattern = request.into_inner().pattern;

    let responses = svc
        .broadcast(|| ShardRequest::Keys {
            pattern: pattern.clone(),
        })
        .await?;

    let mut keys = Vec::new();
    for resp in responses {
        if let ShardResponse::StringArray(shard_keys) = resp {
            keys.extend(shard_keys);
        }
    }
    svc.record_command(start, "KEYS");
    Ok(Response::new(KeysResponse { keys }))
}

pub(super) async fn rename(
    svc: &EmberService,
    request: Request<RenameRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();

    if !svc.engine.same_shard(&req.key, &req.new_key) {
        return Err(Status::failed_precondition(
            "ERR source and destination keys must hash to the same shard",
        ));
    }

    let resp = svc
        .route(
            &req.key,
            ShardRequest::Rename {
                key: req.key.clone(),
                newkey: req.new_key,
            },
        )
        .await?;
    svc.record_command(start, "RENAME");

    match resp {
        ShardResponse::Ok => Ok(Response::new(StatusResponse {
            status: "OK".to_string(),
        })),
        ShardResponse::Err(msg) => Err(Status::not_found(msg)),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn scan(
    svc: &EmberService,
    request: Request<ScanRequest>,
) -> Result<Response<ScanResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let count = if req.count == 0 {
        10
    } else {
        (req.count as usize).min(10_000)
    };

    // the global cursor encodes both which shard we're scanning and where
    // we are within that shard. `cursor % shard_count` gives the shard index,
    // `cursor / shard_count` gives the per-shard cursor. when a shard finishes
    // (returns cursor=0), we advance to the next shard. global cursor 0 means
    // "scan complete" — same convention as redis.
    // arithmetic stays in u64 to avoid truncation on 32-bit platforms.
    let shard_count = svc.engine.shard_count() as u64;
    let shard_idx = (req.cursor % shard_count) as usize;
    let shard_cursor = req.cursor / shard_count;

    let resp = svc
        .engine
        .send_to_shard(
            shard_idx,
            ShardRequest::Scan {
                cursor: shard_cursor,
                count,
                pattern: req.pattern,
            },
        )
        .await
        .map_err(|_| Status::unavailable("shard unavailable"))?;

    svc.record_command(start, "SCAN");

    match resp {
        ShardResponse::Scan {
            cursor: next_cursor,
            keys,
        } => {
            let global_cursor = if next_cursor == 0 {
                // this shard is done, move to next
                let next_shard = (shard_idx as u64) + 1;
                if next_shard < shard_count {
                    next_shard
                } else {
                    0 // all shards done
                }
            } else {
                next_cursor * shard_count + (shard_idx as u64)
            };
            Ok(Response::new(ScanResponse {
                cursor: global_cursor,
                keys,
            }))
        }
        _ => Err(Status::internal("unexpected response")),
    }
}

pub(super) async fn unlink(
    svc: &EmberService,
    request: Request<UnlinkRequest>,
) -> Result<Response<DelResponse>, Status> {
    let start = Instant::now();
    let keys = request.into_inner().keys;
    for k in &keys {
        validate_key(k, &svc.ctx.limits)?;
    }

    let responses = svc
        .engine
        .route_multi(&keys, |k| ShardRequest::Unlink { key: k })
        .await
        .map_err(|_| Status::unavailable("shard unavailable"))?;

    let mut deleted = 0i64;
    for resp in responses {
        if let ShardResponse::Bool(true) = resp {
            deleted += 1;
        }
    }
    svc.record_command(start, "UNLINK");
    Ok(Response::new(DelResponse { deleted }))
}

pub(super) async fn copy(
    svc: &EmberService,
    request: Request<CopyRequest>,
) -> Result<Response<BoolResponse>, Status> {
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
            ShardRequest::Copy {
                source: req.source.clone(),
                destination: req.destination,
                replace: req.replace,
            },
        )
        .await?;
    svc.record_command(start, "COPY");

    match resp {
        ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
        ShardResponse::Err(msg) => Err(Status::not_found(msg)),
        ShardResponse::OutOfMemory => Err(Status::resource_exhausted("OOM")),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn random_key(
    svc: &EmberService,
    _request: Request<RandomKeyRequest>,
) -> Result<Response<GetResponse>, Status> {
    let start = Instant::now();
    let responses = svc.broadcast(|| ShardRequest::RandomKey).await?;

    // pick the first non-empty result from any shard
    let key = responses.into_iter().find_map(|resp| match resp {
        ShardResponse::StringArray(mut v) if !v.is_empty() => Some(v.remove(0)),
        _ => None,
    });

    svc.record_command(start, "RANDOMKEY");
    Ok(Response::new(GetResponse {
        value: key.map(|k| k.into_bytes()),
    }))
}

pub(super) async fn touch(
    svc: &EmberService,
    request: Request<TouchRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let keys = request.into_inner().keys;
    for k in &keys {
        validate_key(k, &svc.ctx.limits)?;
    }

    let responses = svc
        .engine
        .route_multi(&keys, |k| ShardRequest::Touch { key: k })
        .await
        .map_err(|_| Status::unavailable("shard unavailable"))?;

    let mut count = 0i64;
    for resp in responses {
        if let ShardResponse::Bool(true) = resp {
            count += 1;
        }
    }
    svc.record_command(start, "TOUCH");
    Ok(Response::new(IntResponse { value: count }))
}

pub(super) async fn expiretime(
    svc: &EmberService,
    request: Request<ExpiretimeRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::Expiretime {
                key: req.key.clone(),
            },
        )
        .await?;
    svc.record_command(start, "EXPIRETIME");

    match resp {
        ShardResponse::Integer(n) => Ok(Response::new(IntResponse { value: n })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn pexpiretime(
    svc: &EmberService,
    request: Request<PexpiretimeRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::Pexpiretime {
                key: req.key.clone(),
            },
        )
        .await?;
    svc.record_command(start, "PEXPIRETIME");

    match resp {
        ShardResponse::Integer(n) => Ok(Response::new(IntResponse { value: n })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn expireat(
    svc: &EmberService,
    request: Request<ExpireatRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::Expireat {
                key: req.key.clone(),
                timestamp: req.timestamp,
            },
        )
        .await?;
    svc.record_command(start, "EXPIREAT");

    match resp {
        ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn pexpireat(
    svc: &EmberService,
    request: Request<PexpireatRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::Pexpireat {
                key: req.key.clone(),
                timestamp_ms: req.timestamp_ms,
            },
        )
        .await?;
    svc.record_command(start, "PEXPIREAT");

    match resp {
        ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}
