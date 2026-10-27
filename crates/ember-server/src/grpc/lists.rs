//! List command handlers for the gRPC service.

use bytes::Bytes;
use ember_protocol::Command;
use tonic::{Request, Response, Status};

use super::proto::*;
use super::strings::{get_response, int_response};
use super::{non_empty, reply, EmberService};

pub(super) async fn l_push(
    svc: &EmberService,
    request: Request<LPushRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::LPush {
            key: r.key,
            values: r.values.into_iter().map(Bytes::from).collect(),
        })
        .await?;
    int_response(frame)
}

pub(super) async fn r_push(
    svc: &EmberService,
    request: Request<RPushRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::RPush {
            key: r.key,
            values: r.values.into_iter().map(Bytes::from).collect(),
        })
        .await?;
    int_response(frame)
}

pub(super) async fn l_pop(
    svc: &EmberService,
    request: Request<LPopRequest>,
) -> Result<Response<GetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::LPop {
            key: r.key,
            count: None,
        })
        .await?;
    get_response(frame)
}

pub(super) async fn r_pop(
    svc: &EmberService,
    request: Request<RPopRequest>,
) -> Result<Response<GetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::RPop {
            key: r.key,
            count: None,
        })
        .await?;
    get_response(frame)
}

pub(super) async fn l_range(
    svc: &EmberService,
    request: Request<LRangeRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::LRange {
            key: r.key,
            start: r.start,
            stop: r.stop,
        })
        .await?;
    array_response(frame)
}

pub(super) async fn l_len(
    svc: &EmberService,
    request: Request<LLenRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |r| Command::LLen { key: r.key }).await?;
    int_response(frame)
}

pub(super) async fn l_index(
    svc: &EmberService,
    request: Request<LIndexRequest>,
) -> Result<Response<GetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::LIndex {
            key: r.key,
            index: r.index,
        })
        .await?;
    get_response(frame)
}

pub(super) async fn l_set(
    svc: &EmberService,
    request: Request<LSetRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::LSet {
            key: r.key,
            index: r.index,
            value: Bytes::from(r.value),
        })
        .await?;
    status_response(frame)
}

pub(super) async fn l_trim(
    svc: &EmberService,
    request: Request<LTrimRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::LTrim {
            key: r.key,
            start: r.start,
            stop: r.stop,
        })
        .await?;
    status_response(frame)
}

pub(super) async fn l_insert(
    svc: &EmberService,
    request: Request<LInsertRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::LInsert {
            key: r.key,
            before: r.before,
            pivot: Bytes::from(r.pivot),
            value: Bytes::from(r.value),
        })
        .await?;
    int_response(frame)
}

pub(super) async fn l_rem(
    svc: &EmberService,
    request: Request<LRemRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::LRem {
            key: r.key,
            count: r.count,
            value: Bytes::from(r.value),
        })
        .await?;
    int_response(frame)
}

/// Returns the position of the first match. The response holds a single
/// position, so the request's `count` does not change the result.
pub(super) async fn l_pos(
    svc: &EmberService,
    request: Request<LPosRequest>,
) -> Result<Response<OptionalIntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::LPos {
            key: r.key,
            element: Bytes::from(r.value),
            rank: 1,
            count: None,
            maxlen: 0,
        })
        .await?;
    Ok(Response::new(OptionalIntResponse {
        value: reply::opt_int(frame)?,
    }))
}

pub(super) async fn l_move(
    svc: &EmberService,
    request: Request<LMoveRequest>,
) -> Result<Response<GetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::LMove {
            source: r.source,
            destination: r.destination,
            src_left: r.src_left,
            dst_left: r.dst_left,
        })
        .await?;
    get_response(frame)
}

pub(super) async fn lmpop(
    svc: &EmberService,
    request: Request<LmpopRequest>,
) -> Result<Response<LmpopResponse>, Status> {
    let frame = svc
        .try_call(request, |r| {
            Ok(Command::Lmpop {
                keys: non_empty(r.keys)?,
                left: r.left,
                count: r.count.max(1) as usize,
            })
        })
        .await?;
    let response = match reply::keyed(frame)? {
        Some((key, elements)) => LmpopResponse {
            found: true,
            key,
            elements: reply::byte_list(elements)?,
        },
        None => LmpopResponse::default(),
    };
    Ok(Response::new(response))
}

pub(super) fn array_response(
    frame: ember_protocol::Frame,
) -> Result<Response<ArrayResponse>, Status> {
    Ok(Response::new(ArrayResponse {
        values: reply::byte_list(frame)?,
    }))
}

pub(super) fn status_response(
    frame: ember_protocol::Frame,
) -> Result<Response<StatusResponse>, Status> {
    Ok(Response::new(StatusResponse {
        status: reply::string(frame)?,
    }))
}
