//! Set command handlers for the gRPC service.

use std::time::Instant;

use ember_core::{ShardRequest, ShardResponse};
use tonic::{Request, Response, Status};

use super::proto::*;
use super::{unexpected_response, validate_key, EmberService};

pub(super) async fn s_add(
    svc: &EmberService,
    request: Request<SAddRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::SAdd {
                key: req.key.clone(),
                members: req.members,
            },
        )
        .await?;
    svc.record_command(start, "SADD");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_rem(
    svc: &EmberService,
    request: Request<SRemRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::SRem {
                key: req.key.clone(),
                members: req.members,
            },
        )
        .await?;
    svc.record_command(start, "SREM");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_members(
    svc: &EmberService,
    request: Request<SMembersRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::SMembers { key: key.clone() })
        .await?;
    svc.record_command(start, "SMEMBERS");

    match resp {
        ShardResponse::StringArray(members) => Ok(Response::new(KeysResponse { keys: members })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_is_member(
    svc: &EmberService,
    request: Request<SIsMemberRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::SIsMember {
                key: req.key.clone(),
                member: req.member,
            },
        )
        .await?;
    svc.record_command(start, "SISMEMBER");

    match resp {
        ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_card(
    svc: &EmberService,
    request: Request<SCardRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::SCard { key: key.clone() })
        .await?;
    svc.record_command(start, "SCARD");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_union(
    svc: &EmberService,
    request: Request<SUnionRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let start = Instant::now();
    let keys = request.into_inner().keys;
    if keys.is_empty() {
        return Err(Status::invalid_argument("at least one key required"));
    }
    for k in &keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let resp = svc
        .route(&keys[0].clone(), ShardRequest::SUnion { keys })
        .await?;
    svc.record_command(start, "SUNION");

    match resp {
        ShardResponse::StringArray(members) => Ok(Response::new(KeysResponse { keys: members })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_inter(
    svc: &EmberService,
    request: Request<SInterRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let start = Instant::now();
    let keys = request.into_inner().keys;
    if keys.is_empty() {
        return Err(Status::invalid_argument("at least one key required"));
    }
    for k in &keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let resp = svc
        .route(&keys[0].clone(), ShardRequest::SInter { keys })
        .await?;
    svc.record_command(start, "SINTER");

    match resp {
        ShardResponse::StringArray(members) => Ok(Response::new(KeysResponse { keys: members })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_diff(
    svc: &EmberService,
    request: Request<SDiffRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let start = Instant::now();
    let keys = request.into_inner().keys;
    if keys.is_empty() {
        return Err(Status::invalid_argument("at least one key required"));
    }
    for k in &keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let resp = svc
        .route(&keys[0].clone(), ShardRequest::SDiff { keys })
        .await?;
    svc.record_command(start, "SDIFF");

    match resp {
        ShardResponse::StringArray(members) => Ok(Response::new(KeysResponse { keys: members })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_union_store(
    svc: &EmberService,
    request: Request<SUnionStoreRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.destination, &svc.ctx.limits)?;
    for k in &req.keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let dest = req.destination.clone();
    let resp = svc
        .route(
            &dest,
            ShardRequest::SUnionStore {
                dest: req.destination,
                keys: req.keys,
            },
        )
        .await?;
    svc.record_command(start, "SUNIONSTORE");

    match resp {
        ShardResponse::SetStoreResult { count, .. } => Ok(Response::new(IntResponse {
            value: count as i64,
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_inter_store(
    svc: &EmberService,
    request: Request<SInterStoreRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.destination, &svc.ctx.limits)?;
    for k in &req.keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let dest = req.destination.clone();
    let resp = svc
        .route(
            &dest,
            ShardRequest::SInterStore {
                dest: req.destination,
                keys: req.keys,
            },
        )
        .await?;
    svc.record_command(start, "SINTERSTORE");

    match resp {
        ShardResponse::SetStoreResult { count, .. } => Ok(Response::new(IntResponse {
            value: count as i64,
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_diff_store(
    svc: &EmberService,
    request: Request<SDiffStoreRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.destination, &svc.ctx.limits)?;
    for k in &req.keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let dest = req.destination.clone();
    let resp = svc
        .route(
            &dest,
            ShardRequest::SDiffStore {
                dest: req.destination,
                keys: req.keys,
            },
        )
        .await?;
    svc.record_command(start, "SDIFFSTORE");

    match resp {
        ShardResponse::SetStoreResult { count, .. } => Ok(Response::new(IntResponse {
            value: count as i64,
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_rand_member(
    svc: &EmberService,
    request: Request<SRandMemberRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::SRandMember {
                key: req.key.clone(),
                count: req.count as i64,
            },
        )
        .await?;
    svc.record_command(start, "SRANDMEMBER");

    match resp {
        ShardResponse::StringArray(members) => Ok(Response::new(ArrayResponse {
            values: members.into_iter().map(|s| s.into_bytes()).collect(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_pop(
    svc: &EmberService,
    request: Request<SPopRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::SPop {
                key: req.key.clone(),
                count: req.count as usize,
            },
        )
        .await?;
    svc.record_command(start, "SPOP");

    match resp {
        ShardResponse::StringArray(members) => Ok(Response::new(ArrayResponse {
            values: members.into_iter().map(|s| s.into_bytes()).collect(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_mis_member(
    svc: &EmberService,
    request: Request<SMisMemberRequest>,
) -> Result<Response<BoolArrayResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::SMisMember {
                key: req.key.clone(),
                members: req.members,
            },
        )
        .await?;
    svc.record_command(start, "SMISMEMBER");

    match resp {
        ShardResponse::BoolArray(results) => {
            Ok(Response::new(BoolArrayResponse { values: results }))
        }
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn s_scan(
    svc: &EmberService,
    request: Request<SScanRequest>,
) -> Result<Response<SScanResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let count = if req.count == 0 {
        10
    } else {
        req.count as usize
    };
    let resp = svc
        .route(
            &req.key,
            ShardRequest::SScan {
                key: req.key.clone(),
                cursor: req.cursor,
                count,
                pattern: req.pattern,
            },
        )
        .await?;
    svc.record_command(start, "SSCAN");

    match resp {
        ShardResponse::CollectionScan { cursor, items } => {
            let members = items
                .into_iter()
                .map(|b| String::from_utf8_lossy(&b).into_owned())
                .collect();
            Ok(Response::new(SScanResponse { cursor, members }))
        }
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn smove(
    svc: &EmberService,
    request: Request<SmoveRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.source, &svc.ctx.limits)?;
    validate_key(&req.destination, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.source,
            ShardRequest::SMove {
                source: req.source.clone(),
                destination: req.destination,
                member: req.member,
            },
        )
        .await?;
    svc.record_command(start, "SMOVE");

    match resp {
        ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
        ShardResponse::WrongType => Err(Status::invalid_argument(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn sintercard(
    svc: &EmberService,
    request: Request<SintercardRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let keys = req.keys;
    if keys.is_empty() {
        return Err(Status::invalid_argument("at least one key required"));
    }
    for k in &keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let limit = req.limit as usize;
    let first_key = keys[0].clone();
    let resp = svc
        .route(&first_key, ShardRequest::SInterCard { keys, limit })
        .await?;
    svc.record_command(start, "SINTERCARD");

    match resp {
        ShardResponse::Integer(n) => Ok(Response::new(IntResponse { value: n })),
        ShardResponse::WrongType => Err(Status::invalid_argument(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        other => Err(unexpected_response(&other)),
    }
}
