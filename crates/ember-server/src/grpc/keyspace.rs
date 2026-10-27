//! Key-level command handlers for the gRPC service: DEL, EXISTS, EXPIRE,
//! TTL, TYPE, KEYS, RENAME, SCAN and friends.

use ember_protocol::Command;
use tonic::{Request, Response, Status};

use super::proto::*;
use super::strings::{get_response, int_response};
use super::{reply, EmberService};

pub(super) async fn del(
    svc: &EmberService,
    request: Request<DelRequest>,
) -> Result<Response<DelResponse>, Status> {
    let frame = svc.call(request, |r| Command::Del { keys: r.keys }).await?;
    Ok(Response::new(DelResponse {
        deleted: reply::int(frame)?,
    }))
}

pub(super) async fn unlink(
    svc: &EmberService,
    request: Request<UnlinkRequest>,
) -> Result<Response<DelResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Unlink { keys: r.keys })
        .await?;
    Ok(Response::new(DelResponse {
        deleted: reply::int(frame)?,
    }))
}

pub(super) async fn exists(
    svc: &EmberService,
    request: Request<ExistsRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Exists { keys: r.keys })
        .await?;
    int_response(frame)
}

pub(super) async fn touch(
    svc: &EmberService,
    request: Request<TouchRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Touch { keys: r.keys })
        .await?;
    int_response(frame)
}

pub(super) async fn expire(
    svc: &EmberService,
    request: Request<ExpireRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Expire {
            key: r.key,
            seconds: r.seconds,
        })
        .await?;
    bool_response(frame)
}

pub(super) async fn p_expire(
    svc: &EmberService,
    request: Request<PExpireRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Pexpire {
            key: r.key,
            milliseconds: r.milliseconds,
        })
        .await?;
    bool_response(frame)
}

pub(super) async fn expireat(
    svc: &EmberService,
    request: Request<ExpireatRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Expireat {
            key: r.key,
            timestamp: r.timestamp,
        })
        .await?;
    bool_response(frame)
}

pub(super) async fn pexpireat(
    svc: &EmberService,
    request: Request<PexpireatRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Pexpireat {
            key: r.key,
            timestamp_ms: r.timestamp_ms,
        })
        .await?;
    bool_response(frame)
}

pub(super) async fn persist(
    svc: &EmberService,
    request: Request<PersistRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Persist { key: r.key })
        .await?;
    bool_response(frame)
}

pub(super) async fn ttl(
    svc: &EmberService,
    request: Request<TtlRequest>,
) -> Result<Response<TtlResponse>, Status> {
    let frame = svc.call(request, |r| Command::Ttl { key: r.key }).await?;
    Ok(Response::new(TtlResponse {
        value: reply::int(frame)?,
    }))
}

pub(super) async fn p_ttl(
    svc: &EmberService,
    request: Request<PTtlRequest>,
) -> Result<Response<TtlResponse>, Status> {
    let frame = svc.call(request, |r| Command::Pttl { key: r.key }).await?;
    Ok(Response::new(TtlResponse {
        value: reply::int(frame)?,
    }))
}

pub(super) async fn expiretime(
    svc: &EmberService,
    request: Request<ExpiretimeRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Expiretime { key: r.key })
        .await?;
    int_response(frame)
}

pub(super) async fn pexpiretime(
    svc: &EmberService,
    request: Request<PexpiretimeRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Pexpiretime { key: r.key })
        .await?;
    int_response(frame)
}

pub(super) async fn r#type(
    svc: &EmberService,
    request: Request<TypeRequest>,
) -> Result<Response<TypeResponse>, Status> {
    let frame = svc.call(request, |r| Command::Type { key: r.key }).await?;
    Ok(Response::new(TypeResponse {
        type_name: reply::string(frame)?,
    }))
}

pub(super) async fn keys(
    svc: &EmberService,
    request: Request<KeysRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Keys { pattern: r.pattern })
        .await?;
    Ok(Response::new(KeysResponse {
        keys: reply::string_list(frame)?,
    }))
}

pub(super) async fn rename(
    svc: &EmberService,
    request: Request<RenameRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Rename {
            key: r.key,
            newkey: r.new_key,
        })
        .await?;
    Ok(Response::new(StatusResponse {
        status: reply::string(frame)?,
    }))
}

pub(super) async fn copy(
    svc: &EmberService,
    request: Request<CopyRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Copy {
            source: r.source,
            destination: r.destination,
            replace: r.replace,
        })
        .await?;
    bool_response(frame)
}

pub(super) async fn scan(
    svc: &EmberService,
    request: Request<ScanRequest>,
) -> Result<Response<ScanResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Scan {
            cursor: r.cursor,
            pattern: r.pattern,
            count: scan_count(r.count),
        })
        .await?;
    let (cursor, page) = reply::scan(frame)?;
    Ok(Response::new(ScanResponse {
        cursor,
        keys: reply::string_list(page)?,
    }))
}

pub(super) async fn random_key(
    svc: &EmberService,
    request: Request<RandomKeyRequest>,
) -> Result<Response<GetResponse>, Status> {
    let frame = svc.call(request, |_| Command::RandomKey).await?;
    get_response(frame)
}

/// The COUNT for a scan request, where 0 means the server default.
pub(super) fn scan_count(count: u32) -> Option<usize> {
    (count > 0).then_some(count as usize)
}

pub(super) fn bool_response(
    frame: ember_protocol::Frame,
) -> Result<Response<BoolResponse>, Status> {
    Ok(Response::new(BoolResponse {
        value: reply::flag(frame)?,
    }))
}
