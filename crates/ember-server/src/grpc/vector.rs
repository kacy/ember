//! Vector similarity search command handlers for the gRPC service.

#[cfg(feature = "vector")]
use std::time::Instant;

#[cfg(feature = "vector")]
use ember_core::{ShardRequest, ShardResponse};
use tonic::{Request, Response, Status};

use super::proto::*;
use super::EmberService;
#[cfg(feature = "vector")]
use super::{unexpected_response, validate_key};

// input validation limits — defaults used as fallback documentation
#[cfg(feature = "vector")]
const MAX_VECTOR_DIMS: usize = 65_536;
#[cfg(feature = "vector")]
const MAX_VSIM_COUNT: usize = 10_000;
#[cfg(feature = "vector")]
const MAX_HNSW_M: u32 = 1_024;
#[cfg(feature = "vector")]
const MAX_HNSW_EF: u32 = 1_024;

pub(super) async fn v_add(
    svc: &EmberService,
    request: Request<VAddRequest>,
) -> Result<Response<BoolResponse>, Status> {
    #[cfg(not(feature = "vector"))]
    {
        let _ = request;
        let _ = svc;
        Err(Status::unimplemented(
            "vector commands require the 'vector' feature",
        ))
    }

    #[cfg(feature = "vector")]
    {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &svc.ctx.limits)?;
        if req.vector.len() > MAX_VECTOR_DIMS {
            return Err(Status::invalid_argument(format!(
                "vector dimensions {} exceeds max {MAX_VECTOR_DIMS}",
                req.vector.len()
            )));
        }
        if let Some(m) = req.connectivity {
            if m > MAX_HNSW_M {
                return Err(Status::invalid_argument(format!(
                    "connectivity {m} exceeds max {MAX_HNSW_M}"
                )));
            }
        }
        if let Some(ef) = req.ef_construction {
            if ef > MAX_HNSW_EF {
                return Err(Status::invalid_argument(format!(
                    "ef_construction {ef} exceeds max {MAX_HNSW_EF}"
                )));
            }
        }

        let metric = match req.metric() {
            VectorMetric::Cosine => 0,
            VectorMetric::Euclidean => 1,
            VectorMetric::InnerProduct => 2,
        };
        let quantization = match req.quantization() {
            VectorQuantization::None => 0,
            VectorQuantization::F16 => 1,
            VectorQuantization::I8 => 2,
        };

        let resp = svc
            .route(
                &req.key,
                ShardRequest::VAdd {
                    key: req.key.clone(),
                    element: req.element,
                    vector: req.vector,
                    metric,
                    quantization,
                    connectivity: req.connectivity.unwrap_or(16),
                    expansion_add: req.ef_construction.unwrap_or(64),
                },
            )
            .await?;
        svc.record_command(start, "VADD");

        match resp {
            ShardResponse::VAddResult { added, .. } => {
                Ok(Response::new(BoolResponse { value: added }))
            }
            other => Err(unexpected_response(&other)),
        }
    }
}

pub(super) async fn v_add_batch(
    svc: &EmberService,
    request: Request<VAddBatchRequest>,
) -> Result<Response<IntResponse>, Status> {
    #[cfg(not(feature = "vector"))]
    {
        let _ = request;
        let _ = svc;
        Err(Status::unimplemented(
            "vector commands require the 'vector' feature",
        ))
    }

    #[cfg(feature = "vector")]
    {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &svc.ctx.limits)?;

        if req.entries.is_empty() {
            return Ok(Response::new(IntResponse { value: 0 }));
        }
        if req.entries.len() > 10_000 {
            return Err(Status::invalid_argument(format!(
                "batch size {} exceeds max 10000",
                req.entries.len()
            )));
        }

        // validate all vectors have same dimensionality
        let dim = req.entries[0].vector.len();
        if dim == 0 || dim > MAX_VECTOR_DIMS {
            return Err(Status::invalid_argument(format!(
                "vector dimensions {dim} out of range (1..{MAX_VECTOR_DIMS})"
            )));
        }
        for entry in &req.entries {
            if entry.vector.len() != dim {
                return Err(Status::invalid_argument(format!(
                    "dimension mismatch: expected {dim}, element '{}' has {}",
                    entry.element,
                    entry.vector.len()
                )));
            }
        }

        if let Some(m) = req.connectivity {
            if m > MAX_HNSW_M {
                return Err(Status::invalid_argument(format!(
                    "connectivity {m} exceeds max {MAX_HNSW_M}"
                )));
            }
        }
        if let Some(ef) = req.ef_construction {
            if ef > MAX_HNSW_EF {
                return Err(Status::invalid_argument(format!(
                    "ef_construction {ef} exceeds max {MAX_HNSW_EF}"
                )));
            }
        }

        let metric = match req.metric() {
            VectorMetric::Cosine => 0,
            VectorMetric::Euclidean => 1,
            VectorMetric::InnerProduct => 2,
        };
        let quantization = match req.quantization() {
            VectorQuantization::None => 0,
            VectorQuantization::F16 => 1,
            VectorQuantization::I8 => 2,
        };

        let entries: Vec<(String, Vec<f32>)> = req
            .entries
            .into_iter()
            .map(|e| (e.element, e.vector))
            .collect();

        let resp = svc
            .route(
                &req.key,
                ShardRequest::VAddBatch {
                    key: req.key.clone(),
                    entries,
                    dim,
                    metric,
                    quantization,
                    connectivity: req.connectivity.unwrap_or(16),
                    expansion_add: req.ef_construction.unwrap_or(64),
                },
            )
            .await?;
        svc.record_command(start, "VADD_BATCH");

        match resp {
            ShardResponse::VAddBatchResult { added_count, .. } => Ok(Response::new(IntResponse {
                value: added_count as i64,
            })),
            other => Err(unexpected_response(&other)),
        }
    }
}

pub(super) async fn v_sim(
    svc: &EmberService,
    request: Request<VSimRequest>,
) -> Result<Response<VSimResponse>, Status> {
    #[cfg(not(feature = "vector"))]
    {
        let _ = request;
        let _ = svc;
        Err(Status::unimplemented(
            "vector commands require the 'vector' feature",
        ))
    }

    #[cfg(feature = "vector")]
    {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &svc.ctx.limits)?;
        if (req.count as usize) > MAX_VSIM_COUNT {
            return Err(Status::invalid_argument(format!(
                "vsim count {} exceeds max {MAX_VSIM_COUNT}",
                req.count
            )));
        }
        let resp = svc
            .route(
                &req.key,
                ShardRequest::VSim {
                    key: req.key.clone(),
                    query: req.query,
                    count: req.count as usize,
                    ef_search: req.ef_search.unwrap_or(0) as usize,
                },
            )
            .await?;
        svc.record_command(start, "VSIM");

        match resp {
            ShardResponse::VSimResult(results) => Ok(Response::new(VSimResponse {
                results: results
                    .into_iter()
                    .map(|(element, distance)| VSimResult { element, distance })
                    .collect(),
            })),
            other => Err(unexpected_response(&other)),
        }
    }
}

pub(super) async fn v_rem(
    svc: &EmberService,
    request: Request<VRemRequest>,
) -> Result<Response<BoolResponse>, Status> {
    #[cfg(not(feature = "vector"))]
    {
        let _ = request;
        let _ = svc;
        Err(Status::unimplemented(
            "vector commands require the 'vector' feature",
        ))
    }

    #[cfg(feature = "vector")]
    {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = svc
            .route(
                &req.key,
                ShardRequest::VRem {
                    key: req.key.clone(),
                    element: req.element,
                },
            )
            .await?;
        svc.record_command(start, "VREM");

        match resp {
            ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
            other => Err(unexpected_response(&other)),
        }
    }
}

pub(super) async fn v_get(
    svc: &EmberService,
    request: Request<VGetRequest>,
) -> Result<Response<VGetResponse>, Status> {
    #[cfg(not(feature = "vector"))]
    {
        let _ = request;
        let _ = svc;
        Err(Status::unimplemented(
            "vector commands require the 'vector' feature",
        ))
    }

    #[cfg(feature = "vector")]
    {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = svc
            .route(
                &req.key,
                ShardRequest::VGet {
                    key: req.key.clone(),
                    element: req.element,
                },
            )
            .await?;
        svc.record_command(start, "VGET");

        match resp {
            ShardResponse::VectorData(Some(v)) => Ok(Response::new(VGetResponse {
                exists: Some(true),
                vector: v,
            })),
            ShardResponse::VectorData(None) => Ok(Response::new(VGetResponse {
                exists: Some(false),
                vector: vec![],
            })),
            other => Err(unexpected_response(&other)),
        }
    }
}

pub(super) async fn v_card(
    svc: &EmberService,
    request: Request<VCardRequest>,
) -> Result<Response<IntResponse>, Status> {
    #[cfg(not(feature = "vector"))]
    {
        let _ = request;
        let _ = svc;
        Err(Status::unimplemented(
            "vector commands require the 'vector' feature",
        ))
    }

    #[cfg(feature = "vector")]
    {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = svc
            .route(&key, ShardRequest::VCard { key: key.clone() })
            .await?;
        svc.record_command(start, "VCARD");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }
}

pub(super) async fn v_dim(
    svc: &EmberService,
    request: Request<VDimRequest>,
) -> Result<Response<IntResponse>, Status> {
    #[cfg(not(feature = "vector"))]
    {
        let _ = request;
        let _ = svc;
        Err(Status::unimplemented(
            "vector commands require the 'vector' feature",
        ))
    }

    #[cfg(feature = "vector")]
    {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = svc
            .route(&key, ShardRequest::VDim { key: key.clone() })
            .await?;
        svc.record_command(start, "VDIM");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }
}

pub(super) async fn v_info(
    svc: &EmberService,
    request: Request<VInfoRequest>,
) -> Result<Response<VInfoResponse>, Status> {
    #[cfg(not(feature = "vector"))]
    {
        let _ = request;
        let _ = svc;
        Err(Status::unimplemented(
            "vector commands require the 'vector' feature",
        ))
    }

    #[cfg(feature = "vector")]
    {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = svc
            .route(&key, ShardRequest::VInfo { key: key.clone() })
            .await?;
        svc.record_command(start, "VINFO");

        match resp {
            ShardResponse::VectorInfo(Some(info)) => Ok(Response::new(VInfoResponse {
                exists: true,
                info: info
                    .into_iter()
                    .map(|(f, v)| FieldValue {
                        field: f,
                        value: v.into_bytes(),
                    })
                    .collect(),
            })),
            ShardResponse::VectorInfo(None) => Ok(Response::new(VInfoResponse {
                exists: false,
                info: vec![],
            })),
            other => Err(unexpected_response(&other)),
        }
    }
}
