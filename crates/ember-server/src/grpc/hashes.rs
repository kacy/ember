//! Hash command handlers for the gRPC service.

use bytes::Bytes;
use ember_protocol::Command;
use tonic::{Request, Response, Status};

use super::keyspace::{bool_response, scan_count};
use super::lists::array_response;
use super::proto::*;
use super::strings::{get_response, int_response};
use super::{reply, EmberService};

pub(super) async fn h_set(
    svc: &EmberService,
    request: Request<HSetRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::HSet {
            key: r.key,
            fields: r
                .fields
                .into_iter()
                .map(|f| (f.field, Bytes::from(f.value)))
                .collect(),
        })
        .await?;
    int_response(frame)
}

pub(super) async fn h_get(
    svc: &EmberService,
    request: Request<HGetRequest>,
) -> Result<Response<GetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::HGet {
            key: r.key,
            field: r.field,
        })
        .await?;
    get_response(frame)
}

pub(super) async fn h_get_all(
    svc: &EmberService,
    request: Request<HGetAllRequest>,
) -> Result<Response<HashResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::HGetAll { key: r.key })
        .await?;
    Ok(Response::new(HashResponse {
        fields: reply::field_values(frame)?,
    }))
}

pub(super) async fn h_del(
    svc: &EmberService,
    request: Request<HDelRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::HDel {
            key: r.key,
            fields: r.fields,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn h_exists(
    svc: &EmberService,
    request: Request<HExistsRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::HExists {
            key: r.key,
            field: r.field,
        })
        .await?;
    bool_response(frame)
}

pub(super) async fn h_len(
    svc: &EmberService,
    request: Request<HLenRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |r| Command::HLen { key: r.key }).await?;
    int_response(frame)
}

pub(super) async fn h_incr_by(
    svc: &EmberService,
    request: Request<HIncrByRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::HIncrBy {
            key: r.key,
            field: r.field,
            delta: r.delta,
        })
        .await?;
    int_response(frame)
}

pub(super) async fn h_keys(
    svc: &EmberService,
    request: Request<HKeysRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let frame = svc.call(request, |r| Command::HKeys { key: r.key }).await?;
    Ok(Response::new(KeysResponse {
        keys: reply::string_list(frame)?,
    }))
}

pub(super) async fn h_vals(
    svc: &EmberService,
    request: Request<HValsRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let frame = svc.call(request, |r| Command::HVals { key: r.key }).await?;
    array_response(frame)
}

pub(super) async fn hm_get(
    svc: &EmberService,
    request: Request<HmGetRequest>,
) -> Result<Response<OptionalArrayResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::HMGet {
            key: r.key,
            fields: r.fields,
        })
        .await?;
    let values = reply::opt_byte_list(frame)?
        .into_iter()
        .map(|value| OptionalValue { value })
        .collect();
    Ok(Response::new(OptionalArrayResponse { values }))
}

pub(super) async fn h_scan(
    svc: &EmberService,
    request: Request<HScanRequest>,
) -> Result<Response<HScanResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::HScan {
            key: r.key,
            cursor: r.cursor,
            pattern: r.pattern,
            count: scan_count(r.count),
        })
        .await?;
    let (cursor, page) = reply::scan(frame)?;
    Ok(Response::new(HScanResponse {
        cursor,
        fields: reply::field_values(page)?,
    }))
}

/// With values, the array alternates field and value.
pub(super) async fn hrandfield(
    svc: &EmberService,
    request: Request<HrandfieldRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::HRandField {
            key: r.key,
            count: r.has_count.then_some(i64::from(r.count)),
            with_values: r.with_values,
        })
        .await?;
    Ok(Response::new(ArrayResponse {
        values: reply::one_or_many(frame)?,
    }))
}
