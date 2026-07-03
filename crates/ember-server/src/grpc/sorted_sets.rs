//! Sorted set command handlers for the gRPC service.

use std::time::Instant;

use ember_core::{ShardRequest, ShardResponse};
use tonic::{Request, Response, Status};

use super::proto::*;
use super::{
    parse_score_bound, scored_array_to_zrange, unexpected_response, validate_key, EmberService,
};

pub(super) async fn z_add(
    svc: &EmberService,
    request: Request<ZAddRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let members: Vec<(f64, String)> = req
        .members
        .into_iter()
        .map(|m| (m.score, m.member))
        .collect();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZAdd {
                key: req.key.clone(),
                members,
                nx: req.nx,
                xx: req.xx,
                gt: req.gt,
                lt: req.lt,
                ch: req.ch,
            },
        )
        .await?;
    svc.record_command(start, "ZADD");

    match resp {
        ShardResponse::ZAddLen { count, .. } => Ok(Response::new(IntResponse {
            value: count as i64,
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_rem(
    svc: &EmberService,
    request: Request<ZRemRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZRem {
                key: req.key.clone(),
                members: req.members,
            },
        )
        .await?;
    svc.record_command(start, "ZREM");

    match resp {
        ShardResponse::ZRemLen { count, .. } => Ok(Response::new(IntResponse {
            value: count as i64,
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_score(
    svc: &EmberService,
    request: Request<ZScoreRequest>,
) -> Result<Response<OptionalFloatResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZScore {
                key: req.key.clone(),
                member: req.member,
            },
        )
        .await?;
    svc.record_command(start, "ZSCORE");

    match resp {
        ShardResponse::Score(s) => Ok(Response::new(OptionalFloatResponse { value: s })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_rank(
    svc: &EmberService,
    request: Request<ZRankRequest>,
) -> Result<Response<OptionalIntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZRank {
                key: req.key.clone(),
                member: req.member,
            },
        )
        .await?;
    svc.record_command(start, "ZRANK");

    match resp {
        ShardResponse::Rank(r) => Ok(Response::new(OptionalIntResponse {
            value: r.map(|n| n as i64),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_card(
    svc: &EmberService,
    request: Request<ZCardRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::ZCard { key: key.clone() })
        .await?;
    svc.record_command(start, "ZCARD");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_range(
    svc: &EmberService,
    request: Request<ZRangeRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZRange {
                key: req.key.clone(),
                start: req.start,
                stop: req.stop,
                with_scores: req.with_scores,
            },
        )
        .await?;
    svc.record_command(start, "ZRANGE");

    match resp {
        ShardResponse::ScoredArray(arr) => Ok(Response::new(ZRangeResponse {
            members: arr
                .into_iter()
                .map(|(member, score)| ScoreMember { score, member })
                .collect(),
        })),
        ShardResponse::Array(arr) => Ok(Response::new(ZRangeResponse {
            members: arr
                .into_iter()
                .map(|b| ScoreMember {
                    member: String::from_utf8_lossy(&b).to_string(),
                    score: 0.0,
                })
                .collect(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_rev_rank(
    svc: &EmberService,
    request: Request<ZRevRankRequest>,
) -> Result<Response<OptionalIntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZRevRank {
                key: req.key.clone(),
                member: req.member,
            },
        )
        .await?;
    svc.record_command(start, "ZREVRANK");

    match resp {
        ShardResponse::Rank(r) => Ok(Response::new(OptionalIntResponse {
            value: r.map(|n| n as i64),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_rev_range(
    svc: &EmberService,
    request: Request<ZRevRangeRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let with_scores = req.with_scores;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZRevRange {
                key: req.key.clone(),
                start: req.start,
                stop: req.stop,
                with_scores,
            },
        )
        .await?;
    svc.record_command(start, "ZREVRANGE");

    match resp {
        ShardResponse::ScoredArray(arr) => {
            Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
        }
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_count(
    svc: &EmberService,
    request: Request<ZCountRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let min = parse_score_bound(&req.min)?;
    let max = parse_score_bound(&req.max)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZCount {
                key: req.key.clone(),
                min,
                max,
            },
        )
        .await?;
    svc.record_command(start, "ZCOUNT");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_incr_by(
    svc: &EmberService,
    request: Request<ZIncrByRequest>,
) -> Result<Response<FloatResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZIncrBy {
                key: req.key.clone(),
                increment: req.delta,
                member: req.member,
            },
        )
        .await?;
    svc.record_command(start, "ZINCRBY");

    match resp {
        ShardResponse::ZIncrByResult { new_score, .. } => Ok(Response::new(FloatResponse {
            value: new_score.to_string(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_range_by_score(
    svc: &EmberService,
    request: Request<ZRangeByScoreRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let min = parse_score_bound(&req.min)?;
    let max = parse_score_bound(&req.max)?;
    let with_scores = req.with_scores;
    let offset = req.offset.unwrap_or(0).max(0) as usize;
    let count = req.count.map(|c| c.max(0) as usize);
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZRangeByScore {
                key: req.key.clone(),
                min,
                max,
                offset,
                count,
            },
        )
        .await?;
    svc.record_command(start, "ZRANGEBYSCORE");

    match resp {
        ShardResponse::ScoredArray(arr) => {
            Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
        }
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_rev_range_by_score(
    svc: &EmberService,
    request: Request<ZRevRangeByScoreRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    // note: for ZREVRANGEBYSCORE, max and min are swapped in the proto
    let max = parse_score_bound(&req.max)?;
    let min = parse_score_bound(&req.min)?;
    let with_scores = req.with_scores;
    let offset = req.offset.unwrap_or(0).max(0) as usize;
    let count = req.count.map(|c| c.max(0) as usize);
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZRevRangeByScore {
                key: req.key.clone(),
                min,
                max,
                offset,
                count,
            },
        )
        .await?;
    svc.record_command(start, "ZREVRANGEBYSCORE");

    match resp {
        ShardResponse::ScoredArray(arr) => {
            Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
        }
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_pop_min(
    svc: &EmberService,
    request: Request<ZPopMinRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZPopMin {
                key: req.key.clone(),
                count: req.count as usize,
            },
        )
        .await?;
    svc.record_command(start, "ZPOPMIN");

    match resp {
        ShardResponse::ZPopResult(pairs) => Ok(Response::new(ZRangeResponse {
            members: pairs
                .into_iter()
                .map(|(member, score)| ScoreMember { member, score })
                .collect(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_pop_max(
    svc: &EmberService,
    request: Request<ZPopMaxRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZPopMax {
                key: req.key.clone(),
                count: req.count as usize,
            },
        )
        .await?;
    svc.record_command(start, "ZPOPMAX");

    match resp {
        ShardResponse::ZPopResult(pairs) => Ok(Response::new(ZRangeResponse {
            members: pairs
                .into_iter()
                .map(|(member, score)| ScoreMember { member, score })
                .collect(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_diff(
    svc: &EmberService,
    request: Request<ZDiffRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    if req.keys.is_empty() {
        return Err(Status::invalid_argument("at least one key required"));
    }
    for k in &req.keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let with_scores = req.with_scores;
    let first_key = req.keys[0].clone();
    let resp = svc
        .route(&first_key, ShardRequest::ZDiff { keys: req.keys })
        .await?;
    svc.record_command(start, "ZDIFF");

    match resp {
        ShardResponse::ScoredArray(arr) => {
            Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
        }
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_inter(
    svc: &EmberService,
    request: Request<ZInterRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    if req.keys.is_empty() {
        return Err(Status::invalid_argument("at least one key required"));
    }
    for k in &req.keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let with_scores = req.with_scores;
    let first_key = req.keys[0].clone();
    let resp = svc
        .route(&first_key, ShardRequest::ZInter { keys: req.keys })
        .await?;
    svc.record_command(start, "ZINTER");

    match resp {
        ShardResponse::ScoredArray(arr) => {
            Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
        }
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_union(
    svc: &EmberService,
    request: Request<ZUnionRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    if req.keys.is_empty() {
        return Err(Status::invalid_argument("at least one key required"));
    }
    for k in &req.keys {
        validate_key(k, &svc.ctx.limits)?;
    }
    let with_scores = req.with_scores;
    let first_key = req.keys[0].clone();
    let resp = svc
        .route(&first_key, ShardRequest::ZUnion { keys: req.keys })
        .await?;
    svc.record_command(start, "ZUNION");

    match resp {
        ShardResponse::ScoredArray(arr) => {
            Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
        }
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn z_scan(
    svc: &EmberService,
    request: Request<ZScanRequest>,
) -> Result<Response<ZScanResponse>, Status> {
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
            ShardRequest::ZScan {
                key: req.key.clone(),
                cursor: req.cursor,
                count,
                pattern: req.pattern,
            },
        )
        .await?;
    svc.record_command(start, "ZSCAN");

    match resp {
        ShardResponse::CollectionScan { cursor, items } => {
            // items are interleaved: [member, score_str, member, score_str, ...]
            let members = items
                .chunks(2)
                .filter_map(|pair| {
                    if pair.len() == 2 {
                        let member = String::from_utf8_lossy(&pair[0]).into_owned();
                        let score = String::from_utf8_lossy(&pair[1])
                            .parse::<f64>()
                            .unwrap_or(0.0);
                        Some(ScoreMember { member, score })
                    } else {
                        None
                    }
                })
                .collect();
            Ok(Response::new(ZScanResponse { cursor, members }))
        }
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn zmpop(
    svc: &EmberService,
    request: Request<ZmpopRequest>,
) -> Result<Response<ZmpopResponse>, Status> {
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
                ShardRequest::ZmpopSingle {
                    key: key.clone(),
                    min: req.min,
                    count,
                },
            )
            .await?;
        match resp {
            ShardResponse::ZPopResult(members) if !members.is_empty() => {
                svc.record_command(start, "ZMPOP");
                return Ok(Response::new(ZmpopResponse {
                    found: true,
                    key: key.clone(),
                    members: members
                        .into_iter()
                        .map(|(m, s)| ScoreMember {
                            score: s,
                            member: m,
                        })
                        .collect(),
                }));
            }
            ShardResponse::ZPopResult(_) | ShardResponse::Value(None) => continue,
            ShardResponse::WrongType => {
                return Err(Status::invalid_argument(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
            other => return Err(unexpected_response(&other)),
        }
    }

    svc.record_command(start, "ZMPOP");
    Ok(Response::new(ZmpopResponse {
        found: false,
        key: String::new(),
        members: vec![],
    }))
}

pub(super) async fn zrandmember(
    svc: &EmberService,
    request: Request<ZrandmemberRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let count = if req.has_count {
        Some(req.count as i64)
    } else {
        None
    };
    let with_scores = req.with_scores;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::ZRandMember {
                key: req.key.clone(),
                count,
                with_scores,
            },
        )
        .await?;
    svc.record_command(start, "ZRANDMEMBER");

    match resp {
        ShardResponse::ZRandMemberResult(pairs) => {
            let values = if with_scores {
                let mut v = Vec::with_capacity(pairs.len() * 2);
                for (member, score) in pairs {
                    v.push(member.into_bytes());
                    if let Some(s) = score {
                        v.push(format!("{s}").into_bytes());
                    }
                }
                v
            } else {
                pairs.into_iter().map(|(m, _)| m.into_bytes()).collect()
            };
            Ok(Response::new(ArrayResponse { values }))
        }
        ShardResponse::WrongType => Err(Status::invalid_argument(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        other => Err(unexpected_response(&other)),
    }
}
