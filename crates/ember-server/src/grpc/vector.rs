//! Vector similarity search command handlers for the gRPC service.
//!
//! The RESP parser checks these limits for RESP clients. gRPC requests
//! build their commands directly, so the checks are repeated here. A server
//! built without the `vector` feature answers every command with an error.

use ember_protocol::{Command, Frame};
use tonic::{Request, Response, Status};

use super::keyspace::bool_response;
use super::proto::*;
use super::strings::int_response;
use super::{reply, EmberService};

const MAX_VECTOR_DIMS: usize = 65_536;
const MAX_VSIM_COUNT: usize = 10_000;
const MAX_BATCH: usize = 10_000;
const MAX_HNSW_M: u32 = 1_024;
const MAX_HNSW_EF: u32 = 1_024;

/// Index options shared by VADD and VADD_BATCH, as the command fields
/// `(metric, quantization, connectivity, expansion_add)`.
fn index_options(
    metric: VectorMetric,
    quantization: VectorQuantization,
    connectivity: Option<u32>,
    ef_construction: Option<u32>,
) -> Result<(u8, u8, u32, u32), Status> {
    if let Some(m) = connectivity.filter(|&m| m > MAX_HNSW_M) {
        return Err(Status::invalid_argument(format!(
            "connectivity {m} exceeds max {MAX_HNSW_M}"
        )));
    }
    if let Some(ef) = ef_construction.filter(|&ef| ef > MAX_HNSW_EF) {
        return Err(Status::invalid_argument(format!(
            "ef_construction {ef} exceeds max {MAX_HNSW_EF}"
        )));
    }
    let metric = match metric {
        VectorMetric::Cosine => 0,
        VectorMetric::Euclidean => 1,
        VectorMetric::InnerProduct => 2,
    };
    let quantization = match quantization {
        VectorQuantization::None => 0,
        VectorQuantization::F16 => 1,
        VectorQuantization::I8 => 2,
    };
    Ok((
        metric,
        quantization,
        connectivity.unwrap_or(16),
        ef_construction.unwrap_or(64),
    ))
}

pub(super) async fn v_add(
    svc: &EmberService,
    request: Request<VAddRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .try_call(request, |r| {
            if r.vector.len() > MAX_VECTOR_DIMS {
                return Err(Status::invalid_argument(format!(
                    "vector dimensions {} exceeds max {MAX_VECTOR_DIMS}",
                    r.vector.len()
                )));
            }
            let (metric, quantization, connectivity, expansion_add) = index_options(
                r.metric(),
                r.quantization(),
                r.connectivity,
                r.ef_construction,
            )?;
            Ok(Command::VAdd {
                key: r.key,
                element: r.element,
                vector: r.vector,
                metric,
                quantization,
                connectivity,
                expansion_add,
            })
        })
        .await?;
    bool_response(frame)
}

pub(super) async fn v_add_batch(
    svc: &EmberService,
    request: Request<VAddBatchRequest>,
) -> Result<Response<IntResponse>, Status> {
    if request.get_ref().entries.is_empty() {
        return Ok(Response::new(IntResponse { value: 0 }));
    }
    let frame = svc
        .try_call(request, |r| {
            if r.entries.len() > MAX_BATCH {
                return Err(Status::invalid_argument(format!(
                    "batch size {} exceeds max {MAX_BATCH}",
                    r.entries.len()
                )));
            }
            // every vector must have the first one's dimensions
            let dim = r.entries[0].vector.len();
            if dim == 0 || dim > MAX_VECTOR_DIMS {
                return Err(Status::invalid_argument(format!(
                    "vector dimensions {dim} out of range (1..{MAX_VECTOR_DIMS})"
                )));
            }
            if let Some(entry) = r.entries.iter().find(|e| e.vector.len() != dim) {
                return Err(Status::invalid_argument(format!(
                    "dimension mismatch: expected {dim}, element '{}' has {}",
                    entry.element,
                    entry.vector.len()
                )));
            }
            let (metric, quantization, connectivity, expansion_add) = index_options(
                r.metric(),
                r.quantization(),
                r.connectivity,
                r.ef_construction,
            )?;
            Ok(Command::VAddBatch {
                key: r.key,
                entries: r
                    .entries
                    .into_iter()
                    .map(|e| (e.element, e.vector))
                    .collect(),
                dim,
                metric,
                quantization,
                connectivity,
                expansion_add,
            })
        })
        .await?;
    int_response(frame)
}

pub(super) async fn v_sim(
    svc: &EmberService,
    request: Request<VSimRequest>,
) -> Result<Response<VSimResponse>, Status> {
    let frame = svc
        .try_call(request, |r| {
            if r.count as usize > MAX_VSIM_COUNT {
                return Err(Status::invalid_argument(format!(
                    "vsim count {} exceeds max {MAX_VSIM_COUNT}",
                    r.count
                )));
            }
            Ok(Command::VSim {
                key: r.key,
                query: r.query,
                count: r.count as usize,
                ef_search: r.ef_search.unwrap_or(0) as usize,
                with_scores: true,
            })
        })
        .await?;
    let results = reply::pairs(frame)?
        .into_iter()
        .map(|(element, distance)| {
            Ok(VSimResult {
                element: reply::string(element)?,
                distance: f32_of(distance)?,
            })
        })
        .collect::<Result<_, Status>>()?;
    Ok(Response::new(VSimResponse { results }))
}

pub(super) async fn v_rem(
    svc: &EmberService,
    request: Request<VRemRequest>,
) -> Result<Response<BoolResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::VRem {
            key: r.key,
            element: r.element,
        })
        .await?;
    bool_response(frame)
}

pub(super) async fn v_get(
    svc: &EmberService,
    request: Request<VGetRequest>,
) -> Result<Response<VGetResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::VGet {
            key: r.key,
            element: r.element,
        })
        .await?;
    let exists = !matches!(frame, Frame::Null);
    let vector = reply::items(frame)?
        .into_iter()
        .map(f32_of)
        .collect::<Result<_, _>>()?;
    Ok(Response::new(VGetResponse {
        exists: Some(exists),
        vector,
    }))
}

pub(super) async fn v_card(
    svc: &EmberService,
    request: Request<VCardRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |r| Command::VCard { key: r.key }).await?;
    int_response(frame)
}

pub(super) async fn v_dim(
    svc: &EmberService,
    request: Request<VDimRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |r| Command::VDim { key: r.key }).await?;
    int_response(frame)
}

pub(super) async fn v_info(
    svc: &EmberService,
    request: Request<VInfoRequest>,
) -> Result<Response<VInfoResponse>, Status> {
    let frame = svc.call(request, |r| Command::VInfo { key: r.key }).await?;
    let exists = !matches!(frame, Frame::Null);
    Ok(Response::new(VInfoResponse {
        exists,
        info: reply::field_values(frame)?,
    }))
}

fn f32_of(frame: Frame) -> Result<f32, Status> {
    let text = reply::string(frame)?;
    text.parse()
        .map_err(|_| Status::internal(format!("not a number: {text}")))
}
