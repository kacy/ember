//! Set command handlers for the gRPC service.

use ember_protocol::Command;
use tonic::{Request, Response, Status};

use super::keyspace::{bool_response, scan_count};
use super::proto::*;
use super::strings::int_response;
use super::{reply, EmberService};

pub(super) async fn s_add(
    svc: &EmberService,
    request: Request<SAddRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SAdd {
            key: r.key,
            members: r.members,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn s_rem(
    svc: &EmberService,
    request: Request<SRemRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SRem {
            key: r.key,
            members: r.members,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn s_members(
    svc: &EmberService,
    request: Request<SMembersRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SMembers { key: r.key })
        .await?;
    keys_response(frame)
}

pub(super) async fn s_is_member(
    svc: &EmberService,
    request: Request<SIsMemberRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SIsMember {
            key: r.key,
            member: r.member,
        })
        .await?;
    bool_response(frame)
}

pub(super) async fn s_card(
    svc: &EmberService,
    request: Request<SCardRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |r| Command::SCard { key: r.key }).await?;
    int_response(frame)
}

pub(super) async fn s_union(
    svc: &EmberService,
    request: Request<SUnionRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SUnion { keys: r.keys })
        .await?;
    keys_response(frame)
}

pub(super) async fn s_inter(
    svc: &EmberService,
    request: Request<SInterRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SInter { keys: r.keys })
        .await?;
    keys_response(frame)
}

pub(super) async fn s_diff(
    svc: &EmberService,
    request: Request<SDiffRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SDiff { keys: r.keys })
        .await?;
    keys_response(frame)
}

pub(super) async fn s_union_store(
    svc: &EmberService,
    request: Request<SUnionStoreRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SUnionStore {
            dest: r.destination,
            keys: r.keys,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn s_inter_store(
    svc: &EmberService,
    request: Request<SInterStoreRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SInterStore {
            dest: r.destination,
            keys: r.keys,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn s_diff_store(
    svc: &EmberService,
    request: Request<SDiffStoreRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SDiffStore {
            dest: r.destination,
            keys: r.keys,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn s_rand_member(
    svc: &EmberService,
    request: Request<SRandMemberRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SRandMember {
            key: r.key,
            count: Some(i64::from(r.count)),
        })
        .await?;
    Ok(Response::new(ArrayResponse {
        values: reply::one_or_many(frame)?,
    }))
}

pub(super) async fn s_pop(
    svc: &EmberService,
    request: Request<SPopRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SPop {
            key: r.key,
            count: r.count as usize,
        })
        .await?;
    Ok(Response::new(ArrayResponse {
        values: reply::one_or_many(frame)?,
    }))
}

pub(super) async fn s_mis_member(
    svc: &EmberService,
    request: Request<SMisMemberRequest>,
) -> Result<Response<BoolArrayResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SMisMember {
            key: r.key,
            members: r.members,
        })
        .await?;
    let values = reply::items(frame)?
        .into_iter()
        .map(reply::flag)
        .collect::<Result<_, _>>()?;
    Ok(Response::new(BoolArrayResponse { values }))
}

pub(super) async fn s_scan(
    svc: &EmberService,
    request: Request<SScanRequest>,
) -> Result<Response<SScanResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SScan {
            key: r.key,
            cursor: r.cursor,
            pattern: r.pattern,
            count: scan_count(r.count),
        })
        .await?;
    let (cursor, page) = reply::scan(frame)?;
    Ok(Response::new(SScanResponse {
        cursor,
        members: reply::string_list(page)?,
    }))
}

pub(super) async fn smove(
    svc: &EmberService,
    request: Request<SmoveRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::SMove {
            source: r.source,
            destination: r.destination,
            member: r.member,
        })
        .await?;
    bool_response(frame)
}

pub(super) async fn sintercard(
    svc: &EmberService,
    request: Request<SintercardRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .try_call(request, |r| {
            if r.keys.is_empty() {
                return Err(Status::invalid_argument("at least one key required"));
            }
            Ok(Command::SInterCard {
                keys: r.keys,
                limit: r.limit as usize,
            })
        })
        .await?;
    int_response(frame)
}

fn keys_response(frame: ember_protocol::Frame) -> Result<Response<KeysResponse>, Status> {
    Ok(Response::new(KeysResponse {
        keys: reply::string_list(frame)?,
    }))
}
