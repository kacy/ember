//! Sorted set command handlers for the gRPC service.

use ember_protocol::command::ZAddFlags;
use ember_protocol::{Command, Frame};
use tonic::{Request, Response, Status};

use super::keyspace::scan_count;
use super::proto::*;
use super::strings::int_response;
use super::{non_empty, parse_score_bound, reply, EmberService};

pub(super) async fn z_add(
    svc: &EmberService,
    request: Request<ZAddRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::ZAdd {
            key: r.key,
            flags: ZAddFlags {
                nx: r.nx,
                xx: r.xx,
                gt: r.gt,
                lt: r.lt,
                ch: r.ch,
            },
            members: r.members.into_iter().map(|m| (m.score, m.member)).collect(),
        })
        .await?;
    int_response(frame)
}

pub(super) async fn z_rem(
    svc: &EmberService,
    request: Request<ZRemRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::ZRem {
            key: r.key,
            members: r.members,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn z_score(
    svc: &EmberService,
    request: Request<ZScoreRequest>,
) -> Result<Response<OptionalFloatResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::ZScore {
            key: r.key,
            member: r.member,
        })
        .await?;
    Ok(Response::new(OptionalFloatResponse {
        value: reply::opt_float(frame)?,
    }))
}

pub(super) async fn z_rank(
    svc: &EmberService,
    request: Request<ZRankRequest>,
) -> Result<Response<OptionalIntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::ZRank {
            key: r.key,
            member: r.member,
        })
        .await?;
    opt_int_response(frame)
}

pub(super) async fn z_rev_rank(
    svc: &EmberService,
    request: Request<ZRevRankRequest>,
) -> Result<Response<OptionalIntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::ZRevRank {
            key: r.key,
            member: r.member,
        })
        .await?;
    opt_int_response(frame)
}

pub(super) async fn z_card(
    svc: &EmberService,
    request: Request<ZCardRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |r| Command::ZCard { key: r.key }).await?;
    int_response(frame)
}

pub(super) async fn z_range(
    svc: &EmberService,
    request: Request<ZRangeRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let with_scores = request.get_ref().with_scores;
    let frame = svc
        .call(request, |r| Command::ZRange {
            key: r.key,
            start: r.start,
            stop: r.stop,
            with_scores,
        })
        .await?;
    zrange_response(frame, with_scores)
}

pub(super) async fn z_rev_range(
    svc: &EmberService,
    request: Request<ZRevRangeRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let with_scores = request.get_ref().with_scores;
    let frame = svc
        .call(request, |r| Command::ZRevRange {
            key: r.key,
            start: r.start,
            stop: r.stop,
            with_scores,
        })
        .await?;
    zrange_response(frame, with_scores)
}

pub(super) async fn z_count(
    svc: &EmberService,
    request: Request<ZCountRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .try_call(request, |r| {
            Ok(Command::ZCount {
                min: parse_score_bound(&r.min)?,
                max: parse_score_bound(&r.max)?,
                key: r.key,
            })
        })
        .await?;
    int_response(frame)
}

pub(super) async fn z_incr_by(
    svc: &EmberService,
    request: Request<ZIncrByRequest>,
) -> Result<Response<FloatResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::ZIncrBy {
            key: r.key,
            increment: r.delta,
            member: r.member,
        })
        .await?;
    Ok(Response::new(FloatResponse {
        value: reply::string(frame)?,
    }))
}

pub(super) async fn z_range_by_score(
    svc: &EmberService,
    request: Request<ZRangeByScoreRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let with_scores = request.get_ref().with_scores;
    let frame = svc
        .try_call(request, |r| {
            Ok(Command::ZRangeByScore {
                min: parse_score_bound(&r.min)?,
                max: parse_score_bound(&r.max)?,
                key: r.key,
                with_scores,
                offset: r.offset.unwrap_or(0).max(0) as usize,
                count: r.count.map(|c| c.max(0) as usize),
            })
        })
        .await?;
    zrange_response(frame, with_scores)
}

pub(super) async fn z_rev_range_by_score(
    svc: &EmberService,
    request: Request<ZRevRangeByScoreRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let with_scores = request.get_ref().with_scores;
    let frame = svc
        .try_call(request, |r| {
            Ok(Command::ZRevRangeByScore {
                min: parse_score_bound(&r.min)?,
                max: parse_score_bound(&r.max)?,
                key: r.key,
                with_scores,
                offset: r.offset.unwrap_or(0).max(0) as usize,
                count: r.count.map(|c| c.max(0) as usize),
            })
        })
        .await?;
    zrange_response(frame, with_scores)
}

pub(super) async fn z_pop_min(
    svc: &EmberService,
    request: Request<ZPopMinRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::ZPopMin {
            key: r.key,
            count: r.count as usize,
        })
        .await?;
    zrange_response(frame, true)
}

pub(super) async fn z_pop_max(
    svc: &EmberService,
    request: Request<ZPopMaxRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::ZPopMax {
            key: r.key,
            count: r.count as usize,
        })
        .await?;
    zrange_response(frame, true)
}

pub(super) async fn z_diff(
    svc: &EmberService,
    request: Request<ZDiffRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let with_scores = request.get_ref().with_scores;
    let frame = svc
        .try_call(request, |r| {
            Ok(Command::ZDiff {
                keys: non_empty(r.keys)?,
                with_scores,
            })
        })
        .await?;
    zrange_response(frame, with_scores)
}

pub(super) async fn z_inter(
    svc: &EmberService,
    request: Request<ZInterRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let with_scores = request.get_ref().with_scores;
    let frame = svc
        .try_call(request, |r| {
            Ok(Command::ZInter {
                keys: non_empty(r.keys)?,
                with_scores,
            })
        })
        .await?;
    zrange_response(frame, with_scores)
}

pub(super) async fn z_union(
    svc: &EmberService,
    request: Request<ZUnionRequest>,
) -> Result<Response<ZRangeResponse>, Status> {
    let with_scores = request.get_ref().with_scores;
    let frame = svc
        .try_call(request, |r| {
            Ok(Command::ZUnion {
                keys: non_empty(r.keys)?,
                with_scores,
            })
        })
        .await?;
    zrange_response(frame, with_scores)
}

pub(super) async fn z_scan(
    svc: &EmberService,
    request: Request<ZScanRequest>,
) -> Result<Response<ZScanResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::ZScan {
            key: r.key,
            cursor: r.cursor,
            pattern: r.pattern,
            count: scan_count(r.count),
        })
        .await?;
    let (cursor, page) = reply::scan(frame)?;
    Ok(Response::new(ZScanResponse {
        cursor,
        members: reply::score_members(page, true)?,
    }))
}

pub(super) async fn zmpop(
    svc: &EmberService,
    request: Request<ZmpopRequest>,
) -> Result<Response<ZmpopResponse>, Status> {
    let frame = svc
        .try_call(request, |r| {
            Ok(Command::Zmpop {
                keys: non_empty(r.keys)?,
                min: r.min,
                count: r.count.max(1) as usize,
            })
        })
        .await?;
    let response = match reply::keyed(frame)? {
        Some((key, members)) => ZmpopResponse {
            found: true,
            key,
            members: reply::score_members(members, true)?,
        },
        None => ZmpopResponse::default(),
    };
    Ok(Response::new(response))
}

/// With scores, the array alternates member and score.
pub(super) async fn zrandmember(
    svc: &EmberService,
    request: Request<ZrandmemberRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::ZRandMember {
            key: r.key,
            count: r.has_count.then_some(i64::from(r.count)),
            with_scores: r.with_scores,
        })
        .await?;
    Ok(Response::new(ArrayResponse {
        values: reply::one_or_many(frame)?,
    }))
}

fn zrange_response(frame: Frame, with_scores: bool) -> Result<Response<ZRangeResponse>, Status> {
    Ok(Response::new(ZRangeResponse {
        members: reply::score_members(frame, with_scores)?,
    }))
}

fn opt_int_response(frame: Frame) -> Result<Response<OptionalIntResponse>, Status> {
    Ok(Response::new(OptionalIntResponse {
        value: reply::opt_int(frame)?,
    }))
}
