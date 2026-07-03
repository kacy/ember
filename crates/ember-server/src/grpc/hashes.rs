//! Hash command handlers for the gRPC service.

use std::time::Instant;

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse};
use tonic::{Request, Response, Status};

use super::proto::*;
use super::{unexpected_response, validate_key, value_to_bytes, EmberService};

pub(super) async fn h_set(
    svc: &EmberService,
    request: Request<HSetRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let fields: Vec<(String, Bytes)> = req
        .fields
        .into_iter()
        .map(|f| (f.field, Bytes::from(f.value)))
        .collect();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::HSet {
                key: req.key.clone(),
                fields,
            },
        )
        .await?;
    svc.record_command(start, "HSET");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn h_get(
    svc: &EmberService,
    request: Request<HGetRequest>,
) -> Result<Response<GetResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::HGet {
                key: req.key.clone(),
                field: req.field,
            },
        )
        .await?;
    svc.record_command(start, "HGET");

    match resp {
        ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
            value: Some(value_to_bytes(v)),
        })),
        ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn h_get_all(
    svc: &EmberService,
    request: Request<HGetAllRequest>,
) -> Result<Response<HashResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::HGetAll { key: key.clone() })
        .await?;
    svc.record_command(start, "HGETALL");

    match resp {
        ShardResponse::HashFields(fields) => Ok(Response::new(HashResponse {
            fields: fields
                .into_iter()
                .map(|(f, v)| FieldValue {
                    field: f,
                    value: v.to_vec(),
                })
                .collect(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn h_del(
    svc: &EmberService,
    request: Request<HDelRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::HDel {
                key: req.key.clone(),
                fields: req.fields,
            },
        )
        .await?;
    svc.record_command(start, "HDEL");

    match resp {
        ShardResponse::HDelLen { count, .. } => Ok(Response::new(IntResponse {
            value: count as i64,
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn h_exists(
    svc: &EmberService,
    request: Request<HExistsRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::HExists {
                key: req.key.clone(),
                field: req.field,
            },
        )
        .await?;
    svc.record_command(start, "HEXISTS");

    match resp {
        ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn h_len(
    svc: &EmberService,
    request: Request<HLenRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::HLen { key: key.clone() })
        .await?;
    svc.record_command(start, "HLEN");

    match resp {
        ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn h_incr_by(
    svc: &EmberService,
    request: Request<HIncrByRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::HIncrBy {
                key: req.key.clone(),
                field: req.field,
                delta: req.delta,
            },
        )
        .await?;
    svc.record_command(start, "HINCRBY");

    match resp {
        ShardResponse::Integer(v) => Ok(Response::new(IntResponse { value: v })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn h_keys(
    svc: &EmberService,
    request: Request<HKeysRequest>,
) -> Result<Response<KeysResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::HKeys { key: key.clone() })
        .await?;
    svc.record_command(start, "HKEYS");

    match resp {
        ShardResponse::StringArray(keys) => Ok(Response::new(KeysResponse { keys })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn h_vals(
    svc: &EmberService,
    request: Request<HValsRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let start = Instant::now();
    let key = request.into_inner().key;
    let resp = svc
        .route(&key, ShardRequest::HVals { key: key.clone() })
        .await?;
    svc.record_command(start, "HVALS");

    match resp {
        ShardResponse::Array(arr) => Ok(Response::new(ArrayResponse {
            values: arr.into_iter().map(|b| b.to_vec()).collect(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn hm_get(
    svc: &EmberService,
    request: Request<HmGetRequest>,
) -> Result<Response<OptionalArrayResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    let resp = svc
        .route(
            &req.key,
            ShardRequest::HMGet {
                key: req.key.clone(),
                fields: req.fields,
            },
        )
        .await?;
    svc.record_command(start, "HMGET");

    match resp {
        ShardResponse::OptionalArray(arr) => Ok(Response::new(OptionalArrayResponse {
            values: arr
                .into_iter()
                .map(|opt| OptionalValue {
                    value: opt.map(|b| b.to_vec()),
                })
                .collect(),
        })),
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn h_scan(
    svc: &EmberService,
    request: Request<HScanRequest>,
) -> Result<Response<HScanResponse>, Status> {
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
            ShardRequest::HScan {
                key: req.key.clone(),
                cursor: req.cursor,
                count,
                pattern: req.pattern,
            },
        )
        .await?;
    svc.record_command(start, "HSCAN");

    match resp {
        ShardResponse::CollectionScan { cursor, items } => {
            // items are interleaved: [field, value, field, value, ...]
            let fields = items
                .chunks(2)
                .filter_map(|pair| {
                    if pair.len() == 2 {
                        Some(FieldValue {
                            field: String::from_utf8_lossy(&pair[0]).into_owned(),
                            value: pair[1].to_vec(),
                        })
                    } else {
                        None
                    }
                })
                .collect();
            Ok(Response::new(HScanResponse { cursor, fields }))
        }
        other => Err(unexpected_response(&other)),
    }
}

pub(super) async fn hrandfield(
    svc: &EmberService,
    request: Request<HrandfieldRequest>,
) -> Result<Response<ArrayResponse>, Status> {
    let start = Instant::now();
    let req = request.into_inner();
    validate_key(&req.key, &svc.ctx.limits)?;
    let count = if req.has_count {
        Some(req.count as i64)
    } else {
        None
    };
    let with_values = req.with_values;
    let resp = svc
        .route(
            &req.key,
            ShardRequest::HRandField {
                key: req.key.clone(),
                count,
                with_values,
            },
        )
        .await?;
    svc.record_command(start, "HRANDFIELD");

    match resp {
        ShardResponse::HRandFieldResult(pairs) => {
            let values = if with_values {
                let mut v = Vec::with_capacity(pairs.len() * 2);
                for (field, val) in pairs {
                    v.push(field.into_bytes());
                    v.push(val.map(|b| b.to_vec()).unwrap_or_default());
                }
                v
            } else {
                pairs.into_iter().map(|(f, _)| f.into_bytes()).collect()
            };
            Ok(Response::new(ArrayResponse { values }))
        }
        ShardResponse::WrongType => Err(Status::invalid_argument(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
        other => Err(unexpected_response(&other)),
    }
}
