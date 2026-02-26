//! gRPC service implementation for ember.
//!
//! Implements the `EmberCache` tonic service by translating proto requests
//! into `ShardRequest`s, routing through the engine, and mapping responses
//! back to proto types. Shares the same engine, semaphore, and slowlog as
//! the RESP3 listener.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use ember_core::{Engine, ShardRequest, ShardResponse, TtlResult, Value};
use ember_protocol::command::ScoreBound;
use subtle::ConstantTimeEq;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;
use tonic::{Request, Response, Status, Streaming};

use crate::pubsub::PubSubManager;
use crate::server::ServerContext;
use crate::slowlog::SlowLog;

pub mod proto {
    tonic::include_proto!("ember.v1");
}

use proto::ember_cache_server::EmberCache;
use proto::*;

/// The gRPC service backed by the sharded engine.
pub struct EmberService {
    engine: Engine,
    ctx: Arc<ServerContext>,
    slow_log: Arc<SlowLog>,
    pubsub: Arc<PubSubManager>,
}

impl EmberService {
    pub fn new(
        engine: Engine,
        ctx: Arc<ServerContext>,
        slow_log: Arc<SlowLog>,
        pubsub: Arc<PubSubManager>,
    ) -> Self {
        Self {
            engine,
            ctx,
            slow_log,
            pubsub,
        }
    }

    /// Build this service into a tonic router with optional authentication.
    ///
    /// When `requirepass` is configured on the server, every gRPC request
    /// must carry a matching `authorization` metadata header. Comparison
    /// uses constant-time equality to prevent timing side-channels.
    pub fn into_service(
        self,
    ) -> InterceptedService<proto::ember_cache_server::EmberCacheServer<Self>, AuthInterceptor>
    {
        let interceptor = AuthInterceptor {
            requirepass: self.ctx.requirepass.clone(),
        };
        let svc = proto::ember_cache_server::EmberCacheServer::new(self)
            .max_decoding_message_size(4 * 1024 * 1024) // 4 MB
            .max_encoding_message_size(4 * 1024 * 1024);
        InterceptedService::new(svc, interceptor)
    }

    /// Routes a single-key request through the engine.
    async fn route(&self, key: &str, req: ShardRequest) -> Result<ShardResponse, Status> {
        self.engine
            .route(key, req)
            .await
            .map_err(|_| Status::unavailable("shard unavailable"))
    }

    /// Broadcasts a request to all shards.
    async fn broadcast<F>(&self, make_req: F) -> Result<Vec<ShardResponse>, Status>
    where
        F: Fn() -> ShardRequest,
    {
        self.engine
            .broadcast(make_req)
            .await
            .map_err(|_| Status::unavailable("shard unavailable"))
    }

    /// Records command metrics and slowlog.
    fn record_command(&self, start: Instant, cmd: &str) {
        self.ctx.commands_processed.fetch_add(1, Ordering::Relaxed);
        let elapsed = start.elapsed();
        self.slow_log.maybe_record(elapsed, cmd);
    }
}

/// gRPC authentication interceptor.
///
/// When `requirepass` is `Some`, every request must include an
/// `authorization` metadata header whose value matches the password.
/// Uses constant-time comparison to prevent timing side-channels.
/// When `requirepass` is `None`, all requests pass through.
#[derive(Clone)]
pub struct AuthInterceptor {
    requirepass: Option<String>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, req: Request<()>) -> Result<Request<()>, Status> {
        let password = match &self.requirepass {
            Some(pw) => pw,
            None => return Ok(req),
        };
        let token = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());
        match token {
            Some(t) if bool::from(t.as_bytes().ct_eq(password.as_bytes())) => Ok(req),
            _ => Err(Status::unauthenticated("authentication required")),
        }
    }
}

/// Extracts the bytes from a Value::String, or returns an empty vec for
/// non-string types. This is intentionally lenient — callers that need
/// strict type checking should match on Value::String directly.
fn value_to_bytes(v: Value) -> Vec<u8> {
    match v {
        Value::String(b) => b.to_vec(),
        _ => Vec::new(),
    }
}

/// Maps a ShardResponse to a gRPC error status. Handles the common error
/// variants (WrongType, OutOfMemory, Err) and falls back to "unexpected
/// response" for anything else. Used as the catch-all in response matching.
fn unexpected_response(resp: &ShardResponse) -> Status {
    match resp {
        ShardResponse::WrongType => Status::failed_precondition(
            "WRONGTYPE operation against a key holding the wrong kind of value",
        ),
        ShardResponse::OutOfMemory => {
            Status::resource_exhausted("OOM command not allowed when used memory > maxmemory")
        }
        ShardResponse::Err(msg) => Status::internal(msg.clone()),
        _ => Status::internal("unexpected response"),
    }
}

// input validation limits — defaults used as fallback documentation
use crate::config::ConnectionLimits;
#[cfg(feature = "vector")]
const MAX_VECTOR_DIMS: usize = 65_536;
#[cfg(feature = "vector")]
const MAX_VSIM_COUNT: usize = 10_000;
#[cfg(feature = "vector")]
const MAX_HNSW_M: u32 = 1_024;
#[cfg(feature = "vector")]
const MAX_HNSW_EF: u32 = 1_024;

#[allow(clippy::result_large_err)] // Status is tonic's idiomatic error type
fn validate_key(key: &str, limits: &ConnectionLimits) -> Result<(), Status> {
    if key.is_empty() {
        return Err(Status::invalid_argument("key must not be empty"));
    }
    if key.len() > limits.max_key_len {
        return Err(Status::invalid_argument(format!(
            "key length {} exceeds max {}",
            key.len(),
            limits.max_key_len
        )));
    }
    Ok(())
}

#[allow(clippy::result_large_err)] // Status is tonic's idiomatic error type
fn validate_value(value: &[u8], limits: &ConnectionLimits) -> Result<(), Status> {
    if value.len() > limits.max_value_len {
        return Err(Status::invalid_argument(format!(
            "value length {} exceeds max {}",
            value.len(),
            limits.max_value_len
        )));
    }
    Ok(())
}

fn parse_expire(seconds: u64, millis: u64) -> Option<Duration> {
    if millis > 0 {
        Some(Duration::from_millis(millis))
    } else if seconds > 0 {
        Some(Duration::from_secs(seconds))
    } else {
        None
    }
}

/// Parses a Redis-style score bound string into a `ScoreBound`.
///
/// Supports "-inf", "+inf", exclusive bounds like "(5.0", and inclusive
/// bounds like "5.0". Returns an error status for invalid strings.
#[allow(clippy::result_large_err)]
fn parse_score_bound(s: &str) -> Result<ScoreBound, Status> {
    match s {
        "-inf" | "-INF" => Ok(ScoreBound::NegInf),
        "+inf" | "+INF" | "inf" | "INF" => Ok(ScoreBound::PosInf),
        s if s.starts_with('(') => s[1..]
            .parse::<f64>()
            .map(ScoreBound::Exclusive)
            .map_err(|_| Status::invalid_argument(format!("invalid score bound: {s}"))),
        s => s
            .parse::<f64>()
            .map(ScoreBound::Inclusive)
            .map_err(|_| Status::invalid_argument(format!("invalid score bound: {s}"))),
    }
}

/// Converts a ScoredArray response into a ZRangeResponse.
///
/// When `with_scores` is false, scores are set to 0.0 — clients should
/// ignore them. This avoids a second round-trip to the engine.
fn scored_array_to_zrange(arr: Vec<(String, f64)>, with_scores: bool) -> ZRangeResponse {
    ZRangeResponse {
        members: arr
            .into_iter()
            .map(|(member, score)| ScoreMember {
                member,
                score: if with_scores { score } else { 0.0 },
            })
            .collect(),
    }
}

#[tonic::async_trait]
impl EmberCache for EmberService {
    // -----------------------------------------------------------------------
    // strings
    // -----------------------------------------------------------------------

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let key = req.key;
        validate_key(&key, &self.ctx.limits)?;

        let resp = self
            .route(&key, ShardRequest::Get { key: key.clone() })
            .await?;
        self.record_command(start, "GET");

        match resp {
            ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
                value: Some(value_to_bytes(v)),
            })),
            ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        validate_value(&req.value, &self.ctx.limits)?;

        let expire = parse_expire(req.expire_seconds, req.expire_millis);
        let resp = self
            .route(
                &req.key,
                ShardRequest::Set {
                    key: req.key.clone(),
                    value: Bytes::from(req.value),
                    expire,
                    nx: req.nx,
                    xx: req.xx,
                },
            )
            .await?;
        self.record_command(start, "SET");

        match resp {
            ShardResponse::Ok => Ok(Response::new(SetResponse { ok: true })),
            ShardResponse::Value(None) => Ok(Response::new(SetResponse { ok: false })),
            ShardResponse::OutOfMemory => Err(Status::resource_exhausted("OOM")),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn del(&self, request: Request<DelRequest>) -> Result<Response<DelResponse>, Status> {
        let start = Instant::now();
        let keys = request.into_inner().keys;
        for k in &keys {
            validate_key(k, &self.ctx.limits)?;
        }

        let responses = self
            .engine
            .route_multi(&keys, |k| ShardRequest::Del { key: k })
            .await
            .map_err(|_| Status::unavailable("shard unavailable"))?;

        let mut deleted = 0i64;
        for resp in responses {
            if let ShardResponse::Bool(true) = resp {
                deleted += 1;
            }
        }
        self.record_command(start, "DEL");

        Ok(Response::new(DelResponse { deleted }))
    }

    async fn m_get(&self, request: Request<MGetRequest>) -> Result<Response<MGetResponse>, Status> {
        let start = Instant::now();
        let keys = request.into_inner().keys;
        for k in &keys {
            validate_key(k, &self.ctx.limits)?;
        }

        let responses = self
            .engine
            .route_multi(&keys, |k| ShardRequest::Get { key: k })
            .await
            .map_err(|_| Status::unavailable("shard unavailable"))?;

        let values: Vec<OptionalValue> = responses
            .into_iter()
            .map(|resp| match resp {
                ShardResponse::Value(Some(v)) => OptionalValue {
                    value: Some(value_to_bytes(v)),
                },
                _ => OptionalValue { value: None },
            })
            .collect();

        self.record_command(start, "MGET");
        Ok(Response::new(MGetResponse { values }))
    }

    async fn m_set(&self, request: Request<MSetRequest>) -> Result<Response<MSetResponse>, Status> {
        let start = Instant::now();
        let pairs = request.into_inner().pairs;
        for p in &pairs {
            validate_key(&p.key, &self.ctx.limits)?;
            validate_value(&p.value, &self.ctx.limits)?;
        }

        let keys: Vec<String> = pairs.iter().map(|p| p.key.clone()).collect();
        let values: Vec<Bytes> = pairs.into_iter().map(|p| Bytes::from(p.value)).collect();

        // dispatch all SETs concurrently
        let mut receivers = Vec::with_capacity(keys.len());
        for (key, value) in keys.iter().zip(values) {
            let idx = self.engine.shard_for_key(key);
            let rx = self
                .engine
                .dispatch_to_shard(
                    idx,
                    ShardRequest::Set {
                        key: key.clone(),
                        value,
                        expire: None,
                        nx: false,
                        xx: false,
                    },
                )
                .await
                .map_err(|_| Status::unavailable("shard unavailable"))?;
            receivers.push(rx);
        }

        // collect all responses
        for rx in receivers {
            let resp = rx
                .await
                .map_err(|_| Status::unavailable("shard unavailable"))?;
            if let ShardResponse::OutOfMemory = resp {
                return Err(Status::resource_exhausted("OOM"));
            }
        }

        self.record_command(start, "MSET");
        Ok(Response::new(MSetResponse {}))
    }

    async fn incr(&self, request: Request<IncrRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::Incr { key: key.clone() })
            .await?;
        self.record_command(start, "INCR");

        match resp {
            ShardResponse::Integer(v) => Ok(Response::new(IntResponse { value: v })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn incr_by(
        &self,
        request: Request<IncrByRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::IncrBy {
                    key: req.key.clone(),
                    delta: req.delta,
                },
            )
            .await?;
        self.record_command(start, "INCRBY");

        match resp {
            ShardResponse::Integer(v) => Ok(Response::new(IntResponse { value: v })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn decr_by(
        &self,
        request: Request<DecrByRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::DecrBy {
                    key: req.key.clone(),
                    delta: req.delta,
                },
            )
            .await?;
        self.record_command(start, "DECRBY");

        match resp {
            ShardResponse::Integer(v) => Ok(Response::new(IntResponse { value: v })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn incr_by_float(
        &self,
        request: Request<IncrByFloatRequest>,
    ) -> Result<Response<FloatResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::IncrByFloat {
                    key: req.key.clone(),
                    delta: req.delta,
                },
            )
            .await?;
        self.record_command(start, "INCRBYFLOAT");

        match resp {
            ShardResponse::BulkString(s) => Ok(Response::new(FloatResponse { value: s })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::Append {
                    key: req.key.clone(),
                    value: Bytes::from(req.value),
                },
            )
            .await?;
        self.record_command(start, "APPEND");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn strlen(
        &self,
        request: Request<StrlenRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::Strlen { key: key.clone() })
            .await?;
        self.record_command(start, "STRLEN");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    // -----------------------------------------------------------------------
    // keys
    // -----------------------------------------------------------------------

    async fn exists(
        &self,
        request: Request<ExistsRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let keys = request.into_inner().keys;

        let responses = self
            .engine
            .route_multi(&keys, |k| ShardRequest::Exists { key: k })
            .await
            .map_err(|_| Status::unavailable("shard unavailable"))?;

        let mut count = 0i64;
        for resp in responses {
            if let ShardResponse::Bool(true) = resp {
                count += 1;
            }
        }
        self.record_command(start, "EXISTS");
        Ok(Response::new(IntResponse { value: count }))
    }

    async fn expire(
        &self,
        request: Request<ExpireRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::Expire {
                    key: req.key.clone(),
                    seconds: req.seconds,
                },
            )
            .await?;
        self.record_command(start, "EXPIRE");

        match resp {
            ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn p_expire(
        &self,
        request: Request<PExpireRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::Pexpire {
                    key: req.key.clone(),
                    milliseconds: req.milliseconds,
                },
            )
            .await?;
        self.record_command(start, "PEXPIRE");

        match resp {
            ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn persist(
        &self,
        request: Request<PersistRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::Persist { key: key.clone() })
            .await?;
        self.record_command(start, "PERSIST");

        match resp {
            ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn ttl(&self, request: Request<TtlRequest>) -> Result<Response<TtlResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::Ttl { key: key.clone() })
            .await?;
        self.record_command(start, "TTL");

        match resp {
            ShardResponse::Ttl(TtlResult::Seconds(s)) => {
                Ok(Response::new(TtlResponse { value: s as i64 }))
            }
            ShardResponse::Ttl(TtlResult::Milliseconds(ms)) => Ok(Response::new(TtlResponse {
                value: (ms / 1000) as i64,
            })),
            ShardResponse::Ttl(TtlResult::NoExpiry) => Ok(Response::new(TtlResponse { value: -1 })),
            ShardResponse::Ttl(TtlResult::NotFound) => Ok(Response::new(TtlResponse { value: -2 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn p_ttl(&self, request: Request<PTtlRequest>) -> Result<Response<TtlResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::Pttl { key: key.clone() })
            .await?;
        self.record_command(start, "PTTL");

        match resp {
            ShardResponse::Ttl(TtlResult::Milliseconds(ms)) => {
                Ok(Response::new(TtlResponse { value: ms as i64 }))
            }
            ShardResponse::Ttl(TtlResult::Seconds(s)) => Ok(Response::new(TtlResponse {
                value: s.saturating_mul(1000) as i64,
            })),
            ShardResponse::Ttl(TtlResult::NoExpiry) => Ok(Response::new(TtlResponse { value: -1 })),
            ShardResponse::Ttl(TtlResult::NotFound) => Ok(Response::new(TtlResponse { value: -2 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn r#type(
        &self,
        request: Request<TypeRequest>,
    ) -> Result<Response<TypeResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::Type { key: key.clone() })
            .await?;
        self.record_command(start, "TYPE");

        match resp {
            ShardResponse::TypeName(name) => Ok(Response::new(TypeResponse {
                type_name: name.to_string(),
            })),
            _ => Ok(Response::new(TypeResponse {
                type_name: "none".to_string(),
            })),
        }
    }

    async fn keys(&self, request: Request<KeysRequest>) -> Result<Response<KeysResponse>, Status> {
        let start = Instant::now();
        let pattern = request.into_inner().pattern;

        let responses = self
            .broadcast(|| ShardRequest::Keys {
                pattern: pattern.clone(),
            })
            .await?;

        let mut keys = Vec::new();
        for resp in responses {
            if let ShardResponse::StringArray(shard_keys) = resp {
                keys.extend(shard_keys);
            }
        }
        self.record_command(start, "KEYS");
        Ok(Response::new(KeysResponse { keys }))
    }

    async fn rename(
        &self,
        request: Request<RenameRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();

        if !self.engine.same_shard(&req.key, &req.new_key) {
            return Err(Status::failed_precondition(
                "ERR source and destination keys must hash to the same shard",
            ));
        }

        let resp = self
            .route(
                &req.key,
                ShardRequest::Rename {
                    key: req.key.clone(),
                    newkey: req.new_key,
                },
            )
            .await?;
        self.record_command(start, "RENAME");

        match resp {
            ShardResponse::Ok => Ok(Response::new(StatusResponse {
                status: "OK".to_string(),
            })),
            ShardResponse::Err(msg) => Err(Status::not_found(msg)),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let count = if req.count == 0 {
            10
        } else {
            (req.count as usize).min(10_000)
        };

        // the global cursor encodes both which shard we're scanning and where
        // we are within that shard. `cursor % shard_count` gives the shard index,
        // `cursor / shard_count` gives the per-shard cursor. when a shard finishes
        // (returns cursor=0), we advance to the next shard. global cursor 0 means
        // "scan complete" — same convention as redis.
        // arithmetic stays in u64 to avoid truncation on 32-bit platforms.
        let shard_count = self.engine.shard_count() as u64;
        let shard_idx = (req.cursor % shard_count) as usize;
        let shard_cursor = req.cursor / shard_count;

        let resp = self
            .engine
            .send_to_shard(
                shard_idx,
                ShardRequest::Scan {
                    cursor: shard_cursor,
                    count,
                    pattern: req.pattern,
                },
            )
            .await
            .map_err(|_| Status::unavailable("shard unavailable"))?;

        self.record_command(start, "SCAN");

        match resp {
            ShardResponse::Scan {
                cursor: next_cursor,
                keys,
            } => {
                let global_cursor = if next_cursor == 0 {
                    // this shard is done, move to next
                    let next_shard = (shard_idx as u64) + 1;
                    if next_shard < shard_count {
                        next_shard
                    } else {
                        0 // all shards done
                    }
                } else {
                    next_cursor * shard_count + (shard_idx as u64)
                };
                Ok(Response::new(ScanResponse {
                    cursor: global_cursor,
                    keys,
                }))
            }
            _ => Err(Status::internal("unexpected response")),
        }
    }

    // -----------------------------------------------------------------------
    // lists
    // -----------------------------------------------------------------------

    async fn l_push(
        &self,
        request: Request<LPushRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let values: Vec<Bytes> = req.values.into_iter().map(Bytes::from).collect();
        let resp = self
            .route(
                &req.key,
                ShardRequest::LPush {
                    key: req.key.clone(),
                    values,
                },
            )
            .await?;
        self.record_command(start, "LPUSH");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn r_push(
        &self,
        request: Request<RPushRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let values: Vec<Bytes> = req.values.into_iter().map(Bytes::from).collect();
        let resp = self
            .route(
                &req.key,
                ShardRequest::RPush {
                    key: req.key.clone(),
                    values,
                },
            )
            .await?;
        self.record_command(start, "RPUSH");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn l_pop(&self, request: Request<LPopRequest>) -> Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::LPop { key: key.clone() })
            .await?;
        self.record_command(start, "LPOP");

        match resp {
            ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
                value: Some(value_to_bytes(v)),
            })),
            ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn r_pop(&self, request: Request<RPopRequest>) -> Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::RPop { key: key.clone() })
            .await?;
        self.record_command(start, "RPOP");

        match resp {
            ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
                value: Some(value_to_bytes(v)),
            })),
            ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn l_range(
        &self,
        request: Request<LRangeRequest>,
    ) -> Result<Response<ArrayResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::LRange {
                    key: req.key.clone(),
                    start: req.start,
                    stop: req.stop,
                },
            )
            .await?;
        self.record_command(start, "LRANGE");

        match resp {
            ShardResponse::Array(arr) => Ok(Response::new(ArrayResponse {
                values: arr.into_iter().map(|b| b.to_vec()).collect(),
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn l_len(&self, request: Request<LLenRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::LLen { key: key.clone() })
            .await?;
        self.record_command(start, "LLEN");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    // -----------------------------------------------------------------------
    // hashes
    // -----------------------------------------------------------------------

    async fn h_set(&self, request: Request<HSetRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let fields: Vec<(String, Bytes)> = req
            .fields
            .into_iter()
            .map(|f| (f.field, Bytes::from(f.value)))
            .collect();
        let resp = self
            .route(
                &req.key,
                ShardRequest::HSet {
                    key: req.key.clone(),
                    fields,
                },
            )
            .await?;
        self.record_command(start, "HSET");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn h_get(&self, request: Request<HGetRequest>) -> Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::HGet {
                    key: req.key.clone(),
                    field: req.field,
                },
            )
            .await?;
        self.record_command(start, "HGET");

        match resp {
            ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
                value: Some(value_to_bytes(v)),
            })),
            ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn h_get_all(
        &self,
        request: Request<HGetAllRequest>,
    ) -> Result<Response<HashResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::HGetAll { key: key.clone() })
            .await?;
        self.record_command(start, "HGETALL");

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

    async fn h_del(&self, request: Request<HDelRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::HDel {
                    key: req.key.clone(),
                    fields: req.fields,
                },
            )
            .await?;
        self.record_command(start, "HDEL");

        match resp {
            ShardResponse::HDelLen { count, .. } => Ok(Response::new(IntResponse {
                value: count as i64,
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn h_exists(
        &self,
        request: Request<HExistsRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::HExists {
                    key: req.key.clone(),
                    field: req.field,
                },
            )
            .await?;
        self.record_command(start, "HEXISTS");

        match resp {
            ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn h_len(&self, request: Request<HLenRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::HLen { key: key.clone() })
            .await?;
        self.record_command(start, "HLEN");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn h_incr_by(
        &self,
        request: Request<HIncrByRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::HIncrBy {
                    key: req.key.clone(),
                    field: req.field,
                    delta: req.delta,
                },
            )
            .await?;
        self.record_command(start, "HINCRBY");

        match resp {
            ShardResponse::Integer(v) => Ok(Response::new(IntResponse { value: v })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn h_keys(
        &self,
        request: Request<HKeysRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::HKeys { key: key.clone() })
            .await?;
        self.record_command(start, "HKEYS");

        match resp {
            ShardResponse::StringArray(keys) => Ok(Response::new(KeysResponse { keys })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn h_vals(
        &self,
        request: Request<HValsRequest>,
    ) -> Result<Response<ArrayResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::HVals { key: key.clone() })
            .await?;
        self.record_command(start, "HVALS");

        match resp {
            ShardResponse::Array(arr) => Ok(Response::new(ArrayResponse {
                values: arr.into_iter().map(|b| b.to_vec()).collect(),
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn hm_get(
        &self,
        request: Request<HmGetRequest>,
    ) -> Result<Response<OptionalArrayResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::HMGet {
                    key: req.key.clone(),
                    fields: req.fields,
                },
            )
            .await?;
        self.record_command(start, "HMGET");

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

    // -----------------------------------------------------------------------
    // sets
    // -----------------------------------------------------------------------

    async fn s_add(&self, request: Request<SAddRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::SAdd {
                    key: req.key.clone(),
                    members: req.members,
                },
            )
            .await?;
        self.record_command(start, "SADD");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_rem(&self, request: Request<SRemRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::SRem {
                    key: req.key.clone(),
                    members: req.members,
                },
            )
            .await?;
        self.record_command(start, "SREM");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_members(
        &self,
        request: Request<SMembersRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::SMembers { key: key.clone() })
            .await?;
        self.record_command(start, "SMEMBERS");

        match resp {
            ShardResponse::StringArray(members) => {
                Ok(Response::new(KeysResponse { keys: members }))
            }
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_is_member(
        &self,
        request: Request<SIsMemberRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::SIsMember {
                    key: req.key.clone(),
                    member: req.member,
                },
            )
            .await?;
        self.record_command(start, "SISMEMBER");

        match resp {
            ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_card(
        &self,
        request: Request<SCardRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::SCard { key: key.clone() })
            .await?;
        self.record_command(start, "SCARD");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    // -----------------------------------------------------------------------
    // sorted sets
    // -----------------------------------------------------------------------

    async fn z_add(&self, request: Request<ZAddRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let members: Vec<(f64, String)> = req
            .members
            .into_iter()
            .map(|m| (m.score, m.member))
            .collect();
        let resp = self
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
        self.record_command(start, "ZADD");

        match resp {
            ShardResponse::ZAddLen { count, .. } => Ok(Response::new(IntResponse {
                value: count as i64,
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_rem(&self, request: Request<ZRemRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::ZRem {
                    key: req.key.clone(),
                    members: req.members,
                },
            )
            .await?;
        self.record_command(start, "ZREM");

        match resp {
            ShardResponse::ZRemLen { count, .. } => Ok(Response::new(IntResponse {
                value: count as i64,
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_score(
        &self,
        request: Request<ZScoreRequest>,
    ) -> Result<Response<OptionalFloatResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::ZScore {
                    key: req.key.clone(),
                    member: req.member,
                },
            )
            .await?;
        self.record_command(start, "ZSCORE");

        match resp {
            ShardResponse::Score(s) => Ok(Response::new(OptionalFloatResponse { value: s })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_rank(
        &self,
        request: Request<ZRankRequest>,
    ) -> Result<Response<OptionalIntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
            .route(
                &req.key,
                ShardRequest::ZRank {
                    key: req.key.clone(),
                    member: req.member,
                },
            )
            .await?;
        self.record_command(start, "ZRANK");

        match resp {
            ShardResponse::Rank(r) => Ok(Response::new(OptionalIntResponse {
                value: r.map(|n| n as i64),
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_card(
        &self,
        request: Request<ZCardRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        let resp = self
            .route(&key, ShardRequest::ZCard { key: key.clone() })
            .await?;
        self.record_command(start, "ZCARD");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_range(
        &self,
        request: Request<ZRangeRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let resp = self
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
        self.record_command(start, "ZRANGE");

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

    // -----------------------------------------------------------------------
    // vectors
    // -----------------------------------------------------------------------

    async fn v_add(&self, request: Request<VAddRequest>) -> Result<Response<BoolResponse>, Status> {
        #[cfg(not(feature = "vector"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "vector commands require the 'vector' feature",
            ));
        }

        #[cfg(feature = "vector")]
        {
            let start = Instant::now();
            let req = request.into_inner();
            validate_key(&req.key, &self.ctx.limits)?;
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

            let resp = self
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
            self.record_command(start, "VADD");

            match resp {
                ShardResponse::VAddResult { added, .. } => {
                    Ok(Response::new(BoolResponse { value: added }))
                }
                other => Err(unexpected_response(&other)),
            }
        }
    }

    async fn v_add_batch(
        &self,
        request: Request<VAddBatchRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        #[cfg(not(feature = "vector"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "vector commands require the 'vector' feature",
            ));
        }

        #[cfg(feature = "vector")]
        {
            let start = Instant::now();
            let req = request.into_inner();
            validate_key(&req.key, &self.ctx.limits)?;

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

            let resp = self
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
            self.record_command(start, "VADD_BATCH");

            match resp {
                ShardResponse::VAddBatchResult { added_count, .. } => {
                    Ok(Response::new(IntResponse {
                        value: added_count as i64,
                    }))
                }
                other => Err(unexpected_response(&other)),
            }
        }
    }

    async fn v_sim(&self, request: Request<VSimRequest>) -> Result<Response<VSimResponse>, Status> {
        #[cfg(not(feature = "vector"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "vector commands require the 'vector' feature",
            ));
        }

        #[cfg(feature = "vector")]
        {
            let start = Instant::now();
            let req = request.into_inner();
            validate_key(&req.key, &self.ctx.limits)?;
            if (req.count as usize) > MAX_VSIM_COUNT {
                return Err(Status::invalid_argument(format!(
                    "vsim count {} exceeds max {MAX_VSIM_COUNT}",
                    req.count
                )));
            }
            let resp = self
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
            self.record_command(start, "VSIM");

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

    async fn v_rem(&self, request: Request<VRemRequest>) -> Result<Response<BoolResponse>, Status> {
        #[cfg(not(feature = "vector"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "vector commands require the 'vector' feature",
            ));
        }

        #[cfg(feature = "vector")]
        {
            let start = Instant::now();
            let req = request.into_inner();
            let resp = self
                .route(
                    &req.key,
                    ShardRequest::VRem {
                        key: req.key.clone(),
                        element: req.element,
                    },
                )
                .await?;
            self.record_command(start, "VREM");

            match resp {
                ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
                other => Err(unexpected_response(&other)),
            }
        }
    }

    async fn v_get(&self, request: Request<VGetRequest>) -> Result<Response<VGetResponse>, Status> {
        #[cfg(not(feature = "vector"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "vector commands require the 'vector' feature",
            ));
        }

        #[cfg(feature = "vector")]
        {
            let start = Instant::now();
            let req = request.into_inner();
            let resp = self
                .route(
                    &req.key,
                    ShardRequest::VGet {
                        key: req.key.clone(),
                        element: req.element,
                    },
                )
                .await?;
            self.record_command(start, "VGET");

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

    async fn v_card(
        &self,
        request: Request<VCardRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        #[cfg(not(feature = "vector"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "vector commands require the 'vector' feature",
            ));
        }

        #[cfg(feature = "vector")]
        {
            let start = Instant::now();
            let key = request.into_inner().key;
            let resp = self
                .route(&key, ShardRequest::VCard { key: key.clone() })
                .await?;
            self.record_command(start, "VCARD");

            match resp {
                ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
                other => Err(unexpected_response(&other)),
            }
        }
    }

    async fn v_dim(&self, request: Request<VDimRequest>) -> Result<Response<IntResponse>, Status> {
        #[cfg(not(feature = "vector"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "vector commands require the 'vector' feature",
            ));
        }

        #[cfg(feature = "vector")]
        {
            let start = Instant::now();
            let key = request.into_inner().key;
            let resp = self
                .route(&key, ShardRequest::VDim { key: key.clone() })
                .await?;
            self.record_command(start, "VDIM");

            match resp {
                ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
                other => Err(unexpected_response(&other)),
            }
        }
    }

    async fn v_info(
        &self,
        request: Request<VInfoRequest>,
    ) -> Result<Response<VInfoResponse>, Status> {
        #[cfg(not(feature = "vector"))]
        {
            let _ = request;
            return Err(Status::unimplemented(
                "vector commands require the 'vector' feature",
            ));
        }

        #[cfg(feature = "vector")]
        {
            let start = Instant::now();
            let key = request.into_inner().key;
            let resp = self
                .route(&key, ShardRequest::VInfo { key: key.clone() })
                .await?;
            self.record_command(start, "VINFO");

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

    // -----------------------------------------------------------------------
    // server
    // -----------------------------------------------------------------------

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let msg = request.into_inner().message;
        Ok(Response::new(PingResponse {
            message: msg.unwrap_or_else(|| "PONG".to_string()),
        }))
    }

    async fn flush_db(
        &self,
        request: Request<FlushDbRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let start = Instant::now();
        let async_mode = request.into_inner().r#async;

        if async_mode {
            self.broadcast(|| ShardRequest::FlushDbAsync).await?;
        } else {
            self.broadcast(|| ShardRequest::FlushDb).await?;
        }

        self.record_command(start, "FLUSHDB");
        Ok(Response::new(StatusResponse {
            status: "OK".to_string(),
        }))
    }

    async fn db_size(
        &self,
        _request: Request<DbSizeRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();

        let responses = self.broadcast(|| ShardRequest::DbSize).await?;
        let mut total = 0i64;
        for resp in responses {
            if let ShardResponse::KeyCount(n) = resp {
                total += n as i64;
            }
        }
        self.record_command(start, "DBSIZE");
        Ok(Response::new(IntResponse { value: total }))
    }

    async fn info(&self, request: Request<InfoRequest>) -> Result<Response<InfoResponse>, Status> {
        let start = Instant::now();
        let _section = request.into_inner().section;

        let responses = self.broadcast(|| ShardRequest::Stats).await?;
        let mut total_keys = 0usize;
        let mut total_memory = 0usize;
        for resp in &responses {
            if let ShardResponse::Stats(stats) = resp {
                total_keys += stats.key_count;
                total_memory += stats.used_bytes;
            }
        }

        let uptime = self.ctx.start_time.elapsed().as_secs();
        let info = format!(
            "# server\r\n\
             version:{}\r\n\
             uptime_in_seconds:{}\r\n\
             shard_count:{}\r\n\
             \r\n\
             # clients\r\n\
             connected_clients:{}\r\n\
             \r\n\
             # memory\r\n\
             used_memory:{}\r\n\
             \r\n\
             # keyspace\r\n\
             total_keys:{}\r\n\
             \r\n\
             # stats\r\n\
             total_commands_processed:{}\r\n\
             total_connections_received:{}\r\n",
            self.ctx.version,
            uptime,
            self.ctx.shard_count,
            self.ctx.connections_active.load(Ordering::Relaxed),
            total_memory,
            total_keys,
            self.ctx.commands_processed.load(Ordering::Relaxed),
            self.ctx.connections_accepted.load(Ordering::Relaxed),
        );

        self.record_command(start, "INFO");
        Ok(Response::new(InfoResponse { info }))
    }

    // -----------------------------------------------------------------------
    // additional commands
    // -----------------------------------------------------------------------

    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        Ok(Response::new(EchoResponse {
            message: request.into_inner().message,
        }))
    }

    async fn decr(&self, request: Request<DecrRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        validate_key(&key, &self.ctx.limits)?;
        let resp = self
            .route(&key, ShardRequest::Decr { key: key.clone() })
            .await?;
        self.record_command(start, "DECR");

        match resp {
            ShardResponse::Integer(v) => Ok(Response::new(IntResponse { value: v })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn unlink(
        &self,
        request: Request<UnlinkRequest>,
    ) -> Result<Response<DelResponse>, Status> {
        let start = Instant::now();
        let keys = request.into_inner().keys;
        for k in &keys {
            validate_key(k, &self.ctx.limits)?;
        }

        let responses = self
            .engine
            .route_multi(&keys, |k| ShardRequest::Unlink { key: k })
            .await
            .map_err(|_| Status::unavailable("shard unavailable"))?;

        let mut deleted = 0i64;
        for resp in responses {
            if let ShardResponse::Bool(true) = resp {
                deleted += 1;
            }
        }
        self.record_command(start, "UNLINK");
        Ok(Response::new(DelResponse { deleted }))
    }

    async fn bg_save(
        &self,
        _request: Request<BgSaveRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let start = Instant::now();
        self.broadcast(|| ShardRequest::Snapshot).await?;
        self.record_command(start, "BGSAVE");
        Ok(Response::new(StatusResponse {
            status: "Background saving started".to_string(),
        }))
    }

    async fn bg_rewrite_aof(
        &self,
        _request: Request<BgRewriteAofRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let start = Instant::now();
        self.broadcast(|| ShardRequest::RewriteAof).await?;
        self.record_command(start, "BGREWRITEAOF");
        Ok(Response::new(StatusResponse {
            status: "Background append only file rewriting started".to_string(),
        }))
    }

    // -----------------------------------------------------------------------
    // slowlog
    // -----------------------------------------------------------------------

    async fn slow_log_get(
        &self,
        request: Request<SlowLogGetRequest>,
    ) -> Result<Response<SlowLogGetResponse>, Status> {
        let count = request.into_inner().count.map(|c| c as usize);
        let entries = self.slow_log.get(count);
        Ok(Response::new(SlowLogGetResponse {
            entries: entries
                .into_iter()
                .map(|e| {
                    let ts = e
                        .timestamp
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    SlowLogEntry {
                        id: e.id,
                        timestamp_unix: ts,
                        duration_micros: e.duration.as_micros() as u64,
                        command: e.command,
                    }
                })
                .collect(),
        }))
    }

    async fn slow_log_len(
        &self,
        _request: Request<SlowLogLenRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        Ok(Response::new(IntResponse {
            value: self.slow_log.len() as i64,
        }))
    }

    async fn slow_log_reset(
        &self,
        _request: Request<SlowLogResetRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        self.slow_log.reset();
        Ok(Response::new(StatusResponse {
            status: "OK".to_string(),
        }))
    }

    // -----------------------------------------------------------------------
    // pub/sub
    // -----------------------------------------------------------------------

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let req = request.into_inner();
        let count = self.pubsub.publish(&req.channel, Bytes::from(req.message));
        Ok(Response::new(IntResponse {
            value: count as i64,
        }))
    }

    type SubscribeStream = ReceiverStream<Result<SubscribeEvent, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        if req.channels.is_empty() && req.patterns.is_empty() {
            return Err(Status::invalid_argument(
                "at least one channel or pattern required",
            ));
        }

        let total_subs = req.channels.len() + req.patterns.len();
        if total_subs > self.ctx.limits.max_subscriptions_per_conn {
            return Err(Status::invalid_argument(format!(
                "too many subscriptions ({total_subs}), max {}",
                self.ctx.limits.max_subscriptions_per_conn
            )));
        }

        for pat in &req.patterns {
            if pat.len() > self.ctx.limits.max_pattern_len {
                return Err(Status::invalid_argument(format!(
                    "pattern too long ({} bytes), max {}",
                    pat.len(),
                    self.ctx.limits.max_pattern_len
                )));
            }
        }

        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let pubsub = Arc::clone(&self.pubsub);

        // collect all broadcast receivers
        let mut channel_rxs: Vec<(
            String,
            tokio::sync::broadcast::Receiver<crate::pubsub::PubMessage>,
        )> = Vec::new();
        let mut pattern_rxs: Vec<(
            String,
            tokio::sync::broadcast::Receiver<crate::pubsub::PubMessage>,
        )> = Vec::new();

        for ch in &req.channels {
            channel_rxs.push((ch.clone(), pubsub.subscribe(ch)));
        }
        for pat in &req.patterns {
            if let Some(rx) = pubsub.psubscribe(pat) {
                pattern_rxs.push((pat.clone(), rx));
            }
        }

        tokio::spawn(async move {
            loop {
                // build a future that races all receivers
                let event = tokio::select! {
                    biased;
                    result = recv_any_channel(&mut channel_rxs) => result,
                    result = recv_any_pattern(&mut pattern_rxs) => result,
                };

                match event {
                    Some(evt) => {
                        if tx.send(Ok(evt)).await.is_err() {
                            break; // client disconnected
                        }
                    }
                    None => {
                        // all receivers closed
                        break;
                    }
                }
            }

            // cleanup subscriptions
            for (ch, _) in &channel_rxs {
                pubsub.unsubscribe(ch);
            }
            for (pat, _) in &pattern_rxs {
                pubsub.punsubscribe(pat);
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn pub_sub_channels(
        &self,
        request: Request<PubSubChannelsRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        let pattern = request.into_inner().pattern;
        let names = self.pubsub.channel_names(pattern.as_deref());
        Ok(Response::new(KeysResponse { keys: names }))
    }

    async fn pub_sub_num_sub(
        &self,
        request: Request<PubSubNumSubRequest>,
    ) -> Result<Response<PubSubNumSubResponse>, Status> {
        let channels = request.into_inner().channels;
        let pairs = self.pubsub.numsub(&channels);
        Ok(Response::new(PubSubNumSubResponse {
            counts: pairs
                .into_iter()
                .map(|(channel, count)| ChannelCount {
                    channel,
                    count: count as i64,
                })
                .collect(),
        }))
    }

    async fn pub_sub_num_pat(
        &self,
        _request: Request<PubSubNumPatRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        Ok(Response::new(IntResponse {
            value: self.pubsub.active_patterns() as i64,
        }))
    }

    // -----------------------------------------------------------------------
    // strings (extended)
    // -----------------------------------------------------------------------

    async fn get_del(
        &self,
        request: Request<GetDelRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let key = request.into_inner().key;
        validate_key(&key, &self.ctx.limits)?;
        let resp = self
            .route(&key, ShardRequest::GetDel { key: key.clone() })
            .await?;
        self.record_command(start, "GETDEL");

        match resp {
            ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
                value: Some(value_to_bytes(v)),
            })),
            ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn get_ex(
        &self,
        request: Request<GetExRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;

        // map proto expiry fields to the engine's Option<Option<u64>> convention:
        //   None        = leave TTL unchanged
        //   Some(None)  = persist (remove TTL)
        //   Some(Some(ms)) = set TTL to this many milliseconds
        let expire = if req.persist {
            Some(None)
        } else if req.expire_millis > 0 {
            Some(Some(req.expire_millis))
        } else if req.expire_seconds > 0 {
            Some(Some(req.expire_seconds * 1_000))
        } else {
            None
        };

        let resp = self
            .route(
                &req.key,
                ShardRequest::GetEx {
                    key: req.key.clone(),
                    expire,
                },
            )
            .await?;
        self.record_command(start, "GETEX");

        match resp {
            ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
                value: Some(value_to_bytes(v)),
            })),
            ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn get_range(
        &self,
        request: Request<GetRangeRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::GetRange {
                    key: req.key.clone(),
                    start: req.start,
                    end: req.end,
                },
            )
            .await?;
        self.record_command(start, "GETRANGE");

        match resp {
            ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
                value: Some(value_to_bytes(v)),
            })),
            ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn set_range(
        &self,
        request: Request<SetRangeRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        validate_value(&req.value, &self.ctx.limits)?;
        if req.offset < 0 {
            return Err(Status::invalid_argument("offset must not be negative"));
        }
        let resp = self
            .route(
                &req.key,
                ShardRequest::SetRange {
                    key: req.key.clone(),
                    offset: req.offset as usize,
                    value: bytes::Bytes::from(req.value),
                },
            )
            .await?;
        self.record_command(start, "SETRANGE");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    // -----------------------------------------------------------------------
    // keys (extended)
    // -----------------------------------------------------------------------

    async fn copy(&self, request: Request<CopyRequest>) -> Result<Response<BoolResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.source, &self.ctx.limits)?;
        validate_key(&req.destination, &self.ctx.limits)?;

        if !self.engine.same_shard(&req.source, &req.destination) {
            return Err(Status::failed_precondition(
                "ERR source and destination keys must hash to the same shard",
            ));
        }

        let resp = self
            .route(
                &req.source,
                ShardRequest::Copy {
                    source: req.source.clone(),
                    destination: req.destination,
                    replace: req.replace,
                },
            )
            .await?;
        self.record_command(start, "COPY");

        match resp {
            ShardResponse::Bool(v) => Ok(Response::new(BoolResponse { value: v })),
            ShardResponse::Err(msg) => Err(Status::not_found(msg)),
            ShardResponse::OutOfMemory => Err(Status::resource_exhausted("OOM")),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn random_key(
        &self,
        _request: Request<RandomKeyRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let responses = self.broadcast(|| ShardRequest::RandomKey).await?;

        // pick the first non-empty result from any shard
        let key = responses.into_iter().find_map(|resp| match resp {
            ShardResponse::StringArray(mut v) if !v.is_empty() => Some(v.remove(0)),
            _ => None,
        });

        self.record_command(start, "RANDOMKEY");
        Ok(Response::new(GetResponse {
            value: key.map(|k| k.into_bytes()),
        }))
    }

    async fn touch(&self, request: Request<TouchRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let keys = request.into_inner().keys;
        for k in &keys {
            validate_key(k, &self.ctx.limits)?;
        }

        let responses = self
            .engine
            .route_multi(&keys, |k| ShardRequest::Touch { key: k })
            .await
            .map_err(|_| Status::unavailable("shard unavailable"))?;

        let mut count = 0i64;
        for resp in responses {
            if let ShardResponse::Bool(true) = resp {
                count += 1;
            }
        }
        self.record_command(start, "TOUCH");
        Ok(Response::new(IntResponse { value: count }))
    }

    // -----------------------------------------------------------------------
    // lists (extended)
    // -----------------------------------------------------------------------

    async fn l_index(
        &self,
        request: Request<LIndexRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::LIndex {
                    key: req.key.clone(),
                    index: req.index,
                },
            )
            .await?;
        self.record_command(start, "LINDEX");

        match resp {
            ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
                value: Some(value_to_bytes(v)),
            })),
            ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn l_set(
        &self,
        request: Request<LSetRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        validate_value(&req.value, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::LSet {
                    key: req.key.clone(),
                    index: req.index,
                    value: bytes::Bytes::from(req.value),
                },
            )
            .await?;
        self.record_command(start, "LSET");

        match resp {
            ShardResponse::Ok => Ok(Response::new(StatusResponse {
                status: "OK".to_string(),
            })),
            ShardResponse::Err(msg) => Err(Status::failed_precondition(msg)),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn l_trim(
        &self,
        request: Request<LTrimRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::LTrim {
                    key: req.key.clone(),
                    start: req.start,
                    stop: req.stop,
                },
            )
            .await?;
        self.record_command(start, "LTRIM");

        match resp {
            ShardResponse::Ok => Ok(Response::new(StatusResponse {
                status: "OK".to_string(),
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn l_insert(
        &self,
        request: Request<LInsertRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::LInsert {
                    key: req.key.clone(),
                    before: req.before,
                    pivot: bytes::Bytes::from(req.pivot),
                    value: bytes::Bytes::from(req.value),
                },
            )
            .await?;
        self.record_command(start, "LINSERT");

        match resp {
            ShardResponse::Integer(n) => Ok(Response::new(IntResponse { value: n })),
            ShardResponse::OutOfMemory => Err(Status::resource_exhausted("OOM")),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn l_rem(&self, request: Request<LRemRequest>) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::LRem {
                    key: req.key.clone(),
                    count: req.count,
                    value: bytes::Bytes::from(req.value),
                },
            )
            .await?;
        self.record_command(start, "LREM");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn l_pos(
        &self,
        request: Request<LPosRequest>,
    ) -> Result<Response<OptionalIntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        // if count is absent or 0, find first occurrence (count=0 means "all" in
        // the engine, but we only return one result via OptionalIntResponse).
        let count = req.count.unwrap_or(0) as usize;
        let resp = self
            .route(
                &req.key,
                ShardRequest::LPos {
                    key: req.key.clone(),
                    element: bytes::Bytes::from(req.value),
                    rank: 0,
                    count,
                    maxlen: 0,
                },
            )
            .await?;
        self.record_command(start, "LPOS");

        match resp {
            ShardResponse::IntegerArray(positions) => Ok(Response::new(OptionalIntResponse {
                value: positions.into_iter().next(),
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn l_move(
        &self,
        request: Request<LMoveRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.source, &self.ctx.limits)?;
        validate_key(&req.destination, &self.ctx.limits)?;

        if !self.engine.same_shard(&req.source, &req.destination) {
            return Err(Status::failed_precondition(
                "ERR source and destination keys must hash to the same shard",
            ));
        }

        let resp = self
            .route(
                &req.source,
                ShardRequest::LMove {
                    source: req.source.clone(),
                    destination: req.destination,
                    src_left: req.src_left,
                    dst_left: req.dst_left,
                },
            )
            .await?;
        self.record_command(start, "LMOVE");

        match resp {
            ShardResponse::Value(Some(v)) => Ok(Response::new(GetResponse {
                value: Some(value_to_bytes(v)),
            })),
            ShardResponse::Value(None) => Ok(Response::new(GetResponse { value: None })),
            ShardResponse::OutOfMemory => Err(Status::resource_exhausted("OOM")),
            other => Err(unexpected_response(&other)),
        }
    }

    // -----------------------------------------------------------------------
    // sets (extended)
    // -----------------------------------------------------------------------

    async fn s_union(
        &self,
        request: Request<SUnionRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        let start = Instant::now();
        let keys = request.into_inner().keys;
        if keys.is_empty() {
            return Err(Status::invalid_argument("at least one key required"));
        }
        for k in &keys {
            validate_key(k, &self.ctx.limits)?;
        }
        let resp = self
            .route(&keys[0].clone(), ShardRequest::SUnion { keys })
            .await?;
        self.record_command(start, "SUNION");

        match resp {
            ShardResponse::StringArray(members) => {
                Ok(Response::new(KeysResponse { keys: members }))
            }
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_inter(
        &self,
        request: Request<SInterRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        let start = Instant::now();
        let keys = request.into_inner().keys;
        if keys.is_empty() {
            return Err(Status::invalid_argument("at least one key required"));
        }
        for k in &keys {
            validate_key(k, &self.ctx.limits)?;
        }
        let resp = self
            .route(&keys[0].clone(), ShardRequest::SInter { keys })
            .await?;
        self.record_command(start, "SINTER");

        match resp {
            ShardResponse::StringArray(members) => {
                Ok(Response::new(KeysResponse { keys: members }))
            }
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_diff(
        &self,
        request: Request<SDiffRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        let start = Instant::now();
        let keys = request.into_inner().keys;
        if keys.is_empty() {
            return Err(Status::invalid_argument("at least one key required"));
        }
        for k in &keys {
            validate_key(k, &self.ctx.limits)?;
        }
        let resp = self
            .route(&keys[0].clone(), ShardRequest::SDiff { keys })
            .await?;
        self.record_command(start, "SDIFF");

        match resp {
            ShardResponse::StringArray(members) => {
                Ok(Response::new(KeysResponse { keys: members }))
            }
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_union_store(
        &self,
        request: Request<SUnionStoreRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.destination, &self.ctx.limits)?;
        for k in &req.keys {
            validate_key(k, &self.ctx.limits)?;
        }
        let dest = req.destination.clone();
        let resp = self
            .route(
                &dest,
                ShardRequest::SUnionStore {
                    dest: req.destination,
                    keys: req.keys,
                },
            )
            .await?;
        self.record_command(start, "SUNIONSTORE");

        match resp {
            ShardResponse::SetStoreResult { count, .. } => Ok(Response::new(IntResponse {
                value: count as i64,
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_inter_store(
        &self,
        request: Request<SInterStoreRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.destination, &self.ctx.limits)?;
        for k in &req.keys {
            validate_key(k, &self.ctx.limits)?;
        }
        let dest = req.destination.clone();
        let resp = self
            .route(
                &dest,
                ShardRequest::SInterStore {
                    dest: req.destination,
                    keys: req.keys,
                },
            )
            .await?;
        self.record_command(start, "SINTERSTORE");

        match resp {
            ShardResponse::SetStoreResult { count, .. } => Ok(Response::new(IntResponse {
                value: count as i64,
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_diff_store(
        &self,
        request: Request<SDiffStoreRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.destination, &self.ctx.limits)?;
        for k in &req.keys {
            validate_key(k, &self.ctx.limits)?;
        }
        let dest = req.destination.clone();
        let resp = self
            .route(
                &dest,
                ShardRequest::SDiffStore {
                    dest: req.destination,
                    keys: req.keys,
                },
            )
            .await?;
        self.record_command(start, "SDIFFSTORE");

        match resp {
            ShardResponse::SetStoreResult { count, .. } => Ok(Response::new(IntResponse {
                value: count as i64,
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_rand_member(
        &self,
        request: Request<SRandMemberRequest>,
    ) -> Result<Response<ArrayResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::SRandMember {
                    key: req.key.clone(),
                    count: req.count as i64,
                },
            )
            .await?;
        self.record_command(start, "SRANDMEMBER");

        match resp {
            ShardResponse::StringArray(members) => Ok(Response::new(ArrayResponse {
                values: members.into_iter().map(|s| s.into_bytes()).collect(),
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_pop(
        &self,
        request: Request<SPopRequest>,
    ) -> Result<Response<ArrayResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::SPop {
                    key: req.key.clone(),
                    count: req.count as usize,
                },
            )
            .await?;
        self.record_command(start, "SPOP");

        match resp {
            ShardResponse::StringArray(members) => Ok(Response::new(ArrayResponse {
                values: members.into_iter().map(|s| s.into_bytes()).collect(),
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn s_mis_member(
        &self,
        request: Request<SMisMemberRequest>,
    ) -> Result<Response<BoolArrayResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::SMisMember {
                    key: req.key.clone(),
                    members: req.members,
                },
            )
            .await?;
        self.record_command(start, "SMISMEMBER");

        match resp {
            ShardResponse::BoolArray(results) => {
                Ok(Response::new(BoolArrayResponse { values: results }))
            }
            other => Err(unexpected_response(&other)),
        }
    }

    // -----------------------------------------------------------------------
    // hashes (extended)
    // -----------------------------------------------------------------------

    async fn h_scan(
        &self,
        request: Request<HScanRequest>,
    ) -> Result<Response<HScanResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let count = if req.count == 0 {
            10
        } else {
            req.count as usize
        };
        let resp = self
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
        self.record_command(start, "HSCAN");

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

    // -----------------------------------------------------------------------
    // sorted sets (extended)
    // -----------------------------------------------------------------------

    async fn z_rev_rank(
        &self,
        request: Request<ZRevRankRequest>,
    ) -> Result<Response<OptionalIntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::ZRevRank {
                    key: req.key.clone(),
                    member: req.member,
                },
            )
            .await?;
        self.record_command(start, "ZREVRANK");

        match resp {
            ShardResponse::Rank(r) => Ok(Response::new(OptionalIntResponse {
                value: r.map(|n| n as i64),
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_rev_range(
        &self,
        request: Request<ZRevRangeRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let with_scores = req.with_scores;
        let resp = self
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
        self.record_command(start, "ZREVRANGE");

        match resp {
            ShardResponse::ScoredArray(arr) => {
                Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
            }
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_count(
        &self,
        request: Request<ZCountRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let min = parse_score_bound(&req.min)?;
        let max = parse_score_bound(&req.max)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::ZCount {
                    key: req.key.clone(),
                    min,
                    max,
                },
            )
            .await?;
        self.record_command(start, "ZCOUNT");

        match resp {
            ShardResponse::Len(n) => Ok(Response::new(IntResponse { value: n as i64 })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_incr_by(
        &self,
        request: Request<ZIncrByRequest>,
    ) -> Result<Response<FloatResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::ZIncrBy {
                    key: req.key.clone(),
                    increment: req.delta,
                    member: req.member,
                },
            )
            .await?;
        self.record_command(start, "ZINCRBY");

        match resp {
            ShardResponse::ZIncrByResult { new_score, .. } => Ok(Response::new(FloatResponse {
                value: new_score.to_string(),
            })),
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_range_by_score(
        &self,
        request: Request<ZRangeByScoreRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let min = parse_score_bound(&req.min)?;
        let max = parse_score_bound(&req.max)?;
        let with_scores = req.with_scores;
        let offset = req.offset.unwrap_or(0).max(0) as usize;
        let count = req.count.map(|c| c.max(0) as usize);
        let resp = self
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
        self.record_command(start, "ZRANGEBYSCORE");

        match resp {
            ShardResponse::ScoredArray(arr) => {
                Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
            }
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_rev_range_by_score(
        &self,
        request: Request<ZRevRangeByScoreRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        // note: for ZREVRANGEBYSCORE, max and min are swapped in the proto
        let max = parse_score_bound(&req.max)?;
        let min = parse_score_bound(&req.min)?;
        let with_scores = req.with_scores;
        let offset = req.offset.unwrap_or(0).max(0) as usize;
        let count = req.count.map(|c| c.max(0) as usize);
        let resp = self
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
        self.record_command(start, "ZREVRANGEBYSCORE");

        match resp {
            ShardResponse::ScoredArray(arr) => {
                Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
            }
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_pop_min(
        &self,
        request: Request<ZPopMinRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::ZPopMin {
                    key: req.key.clone(),
                    count: req.count as usize,
                },
            )
            .await?;
        self.record_command(start, "ZPOPMIN");

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

    async fn z_pop_max(
        &self,
        request: Request<ZPopMaxRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let resp = self
            .route(
                &req.key,
                ShardRequest::ZPopMax {
                    key: req.key.clone(),
                    count: req.count as usize,
                },
            )
            .await?;
        self.record_command(start, "ZPOPMAX");

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

    async fn z_diff(
        &self,
        request: Request<ZDiffRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        if req.keys.is_empty() {
            return Err(Status::invalid_argument("at least one key required"));
        }
        for k in &req.keys {
            validate_key(k, &self.ctx.limits)?;
        }
        let with_scores = req.with_scores;
        let first_key = req.keys[0].clone();
        let resp = self
            .route(&first_key, ShardRequest::ZDiff { keys: req.keys })
            .await?;
        self.record_command(start, "ZDIFF");

        match resp {
            ShardResponse::ScoredArray(arr) => {
                Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
            }
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_inter(
        &self,
        request: Request<ZInterRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        if req.keys.is_empty() {
            return Err(Status::invalid_argument("at least one key required"));
        }
        for k in &req.keys {
            validate_key(k, &self.ctx.limits)?;
        }
        let with_scores = req.with_scores;
        let first_key = req.keys[0].clone();
        let resp = self
            .route(&first_key, ShardRequest::ZInter { keys: req.keys })
            .await?;
        self.record_command(start, "ZINTER");

        match resp {
            ShardResponse::ScoredArray(arr) => {
                Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
            }
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_union(
        &self,
        request: Request<ZUnionRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        if req.keys.is_empty() {
            return Err(Status::invalid_argument("at least one key required"));
        }
        for k in &req.keys {
            validate_key(k, &self.ctx.limits)?;
        }
        let with_scores = req.with_scores;
        let first_key = req.keys[0].clone();
        let resp = self
            .route(&first_key, ShardRequest::ZUnion { keys: req.keys })
            .await?;
        self.record_command(start, "ZUNION");

        match resp {
            ShardResponse::ScoredArray(arr) => {
                Ok(Response::new(scored_array_to_zrange(arr, with_scores)))
            }
            other => Err(unexpected_response(&other)),
        }
    }

    async fn z_scan(
        &self,
        request: Request<ZScanRequest>,
    ) -> Result<Response<ZScanResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let count = if req.count == 0 {
            10
        } else {
            req.count as usize
        };
        let resp = self
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
        self.record_command(start, "ZSCAN");

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

    // -----------------------------------------------------------------------
    // scans
    // -----------------------------------------------------------------------

    async fn s_scan(
        &self,
        request: Request<SScanRequest>,
    ) -> Result<Response<SScanResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        validate_key(&req.key, &self.ctx.limits)?;
        let count = if req.count == 0 {
            10
        } else {
            req.count as usize
        };
        let resp = self
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
        self.record_command(start, "SSCAN");

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

    // -----------------------------------------------------------------------
    // server (extended)
    // -----------------------------------------------------------------------

    async fn time(&self, _request: Request<TimeRequest>) -> Result<Response<TimeResponse>, Status> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Ok(Response::new(TimeResponse {
            seconds: now.as_secs() as i64,
            microseconds: now.subsec_micros() as i64,
        }))
    }

    async fn last_save(
        &self,
        _request: Request<LastSaveRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        use std::sync::atomic::Ordering;
        Ok(Response::new(IntResponse {
            value: self.ctx.last_save_timestamp.load(Ordering::Relaxed) as i64,
        }))
    }

    // -----------------------------------------------------------------------
    // pipeline (bidirectional streaming)
    // -----------------------------------------------------------------------

    type PipelineStream = ReceiverStream<Result<PipelineResponse, Status>>;

    async fn pipeline(
        &self,
        request: Request<Streaming<PipelineRequest>>,
    ) -> Result<Response<Self::PipelineStream>, Status> {
        let mut stream = request.into_inner();
        let engine = self.engine.clone();
        let ctx = Arc::clone(&self.ctx);
        let slow_log = Arc::clone(&self.slow_log);
        let pubsub = Arc::clone(&self.pubsub);

        let (tx, rx) = tokio::sync::mpsc::channel(256);

        tokio::spawn(async move {
            let svc = EmberService::new(engine, ctx, slow_log, pubsub);
            while let Ok(Some(req)) = stream.message().await {
                let id = req.id;
                let result = handle_pipeline_command(&svc, req).await;
                let resp = match result {
                    Ok(pr) => pr,
                    Err(status) => PipelineResponse {
                        id,
                        result: Some(pipeline_response::Result::Error(ErrorResponse {
                            message: status.message().to_string(),
                            kind: ErrorKind::Internal as i32,
                        })),
                    },
                };
                if tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// Dispatches a single pipeline command to the appropriate RPC handler.
/// Dispatches a single pipeline command to the service method and wraps the
/// response in a PipelineResponse. Each arm follows the same pattern: call
/// the service method, extract the inner response, wrap it in the correct
/// result variant. The macro eliminates the boilerplate of ~57 identical arms.
macro_rules! pipeline_dispatch {
    ($svc:expr, $id:expr, $cmd:expr, {
        $( $Variant:ident => $method:ident => $Result:ident ),* $(,)?
    }) => {
        match $cmd {
            $(
                pipeline_request::Command::$Variant(r) => {
                    let resp = $svc.$method(Request::new(r)).await?;
                    Ok(PipelineResponse {
                        id: $id,
                        result: Some(pipeline_response::Result::$Result(resp.into_inner())),
                    })
                }
            )*
        }
    };
}

async fn handle_pipeline_command(
    svc: &EmberService,
    req: PipelineRequest,
) -> Result<PipelineResponse, Status> {
    let id = req.id;
    let cmd = req
        .command
        .ok_or_else(|| Status::invalid_argument("missing command"))?;

    pipeline_dispatch!(svc, id, cmd, {
        // string commands
        Get       => get           => Get,
        Set       => set           => Set,
        Del       => del           => Del,
        Exists    => exists         => IntVal,
        Incr      => incr           => IntVal,
        IncrBy    => incr_by        => IntVal,
        DecrBy    => decr_by        => IntVal,
        IncrByFloat => incr_by_float => FloatVal,
        Append    => append         => IntVal,
        Strlen    => strlen         => IntVal,

        // ttl / expiry
        Expire    => expire         => BoolVal,
        Pexpire   => p_expire       => BoolVal,
        Persist   => persist        => BoolVal,
        Ttl       => ttl            => Ttl,
        Pttl      => p_ttl          => Ttl,
        Type      => r#type         => Type,

        // list commands
        Lpush     => l_push         => IntVal,
        Rpush     => r_push         => IntVal,
        Lpop      => l_pop          => Get,
        Rpop      => r_pop          => Get,
        Lrange    => l_range        => Array,
        Llen      => l_len          => IntVal,

        // hash commands
        Hset      => h_set          => IntVal,
        Hget      => h_get          => Get,
        Hgetall   => h_get_all      => Hash,
        Hdel      => h_del          => IntVal,
        Hexists   => h_exists       => BoolVal,
        Hlen      => h_len          => IntVal,
        HincrBy   => h_incr_by      => IntVal,
        Hkeys     => h_keys         => Keys,
        Hvals     => h_vals         => Array,
        Hmget     => hm_get         => OptionalArray,

        // set commands
        Sadd      => s_add          => IntVal,
        Srem      => s_rem          => IntVal,
        Smembers  => s_members      => Keys,
        Sismember => s_is_member    => BoolVal,
        Scard     => s_card         => IntVal,

        // sorted set commands
        Zadd      => z_add          => IntVal,
        Zrem      => z_rem          => IntVal,
        Zscore    => z_score        => OptionalFloat,
        Zrank     => z_rank         => OptionalInt,
        Zcard     => z_card         => IntVal,
        Zrange    => z_range        => Zrange,

        // vector commands
        Vadd      => v_add          => BoolVal,
        VaddBatch => v_add_batch    => IntVal,
        Vsim      => v_sim          => Vsim,
        Vrem      => v_rem          => BoolVal,
        Vget      => v_get          => Vget,
        Vcard     => v_card         => IntVal,
        Vdim      => v_dim          => IntVal,
        Vinfo     => v_info         => Vinfo,

        // server commands
        Ping      => ping           => Ping,
        Echo      => echo           => Echo,
        Decr      => decr           => IntVal,
        Unlink    => unlink         => Del,
        Flushdb   => flush_db       => Status,
        Dbsize    => db_size        => IntVal,
        Bgsave    => bg_save        => Status,
        Bgrewriteaof => bg_rewrite_aof => Status,
        Mget      => m_get          => Mget,
        Mset      => m_set          => Mset,
        Keys      => keys           => Keys,
        Rename    => rename         => Status,
        Scan      => scan           => Scan,

        // slowlog
        SlowlogGet   => slow_log_get   => SlowlogGet,
        SlowlogLen   => slow_log_len   => IntVal,
        SlowlogReset => slow_log_reset => Status,

        // pub/sub (unary only — Subscribe is streaming)
        Publish        => publish          => IntVal,
        PubsubChannels => pub_sub_channels => Keys,
        PubsubNumsub   => pub_sub_num_sub  => PubsubNumsub,
        PubsubNumpat   => pub_sub_num_pat  => IntVal,

        // extended strings
        GetDel   => get_del   => Get,
        GetEx    => get_ex    => Get,
        GetRange => get_range => Get,
        SetRange => set_range => IntVal,

        // extended keys
        Copy      => copy       => BoolVal,
        RandomKey => random_key => Get,
        Touch     => touch      => IntVal,

        // extended lists
        Lindex  => l_index  => Get,
        Lset    => l_set    => Status,
        Ltrim   => l_trim   => Status,
        Linsert => l_insert => IntVal,
        Lrem    => l_rem    => IntVal,
        Lpos    => l_pos    => OptionalInt,
        Lmove   => l_move   => Get,

        // extended sets
        Sunion      => s_union       => Keys,
        Sinter      => s_inter       => Keys,
        Sdiff       => s_diff        => Keys,
        SunionStore => s_union_store => IntVal,
        SinterStore => s_inter_store => IntVal,
        SdiffStore  => s_diff_store  => IntVal,
        SrandMember => s_rand_member => Array,
        Spop        => s_pop         => Array,
        Smismember  => s_mis_member  => BoolArray,

        // extended hashes
        Hscan => h_scan => Hscan,

        // extended sorted sets
        ZrevRank         => z_rev_rank          => OptionalInt,
        ZrevRange        => z_rev_range         => Zrange,
        Zcount           => z_count             => IntVal,
        Zincrby          => z_incr_by           => FloatVal,
        ZrangeByScore    => z_range_by_score    => Zrange,
        ZrevRangeByScore => z_rev_range_by_score => Zrange,
        Zpopmin          => z_pop_min           => Zrange,
        Zpopmax          => z_pop_max           => Zrange,
        Zdiff            => z_diff              => Zrange,
        Zinter           => z_inter             => Zrange,
        Zunion           => z_union             => Zrange,
        Zscan            => z_scan              => Zscan,

        // scans
        Sscan => s_scan => Sscan,

        // extended server
        Time     => time      => TimeResp,
        LastSave => last_save => IntVal,
    })
}

/// Waits for the next message on any channel subscription receiver.
/// Returns None when all receivers are closed.
async fn recv_any_channel(
    rxs: &mut [(
        String,
        tokio::sync::broadcast::Receiver<crate::pubsub::PubMessage>,
    )],
) -> Option<SubscribeEvent> {
    if rxs.is_empty() {
        // no channel subscriptions — park forever so pattern branch can drive
        std::future::pending::<()>().await;
        return None;
    }

    loop {
        // poll each receiver in round-robin (tokio::select! on a slice
        // requires a loop because we can't use select! with dynamic count).
        for (_, rx) in rxs.iter_mut() {
            match rx.try_recv() {
                Ok(msg) => {
                    return Some(SubscribeEvent {
                        kind: "message".to_string(),
                        channel: msg.channel.to_string(),
                        data: msg.data.to_vec(),
                        pattern: None,
                    });
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => return None,
            }
        }
        // yield to avoid busy-spinning
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Waits for the next message on any pattern subscription receiver.
/// Returns None when all receivers are closed.
async fn recv_any_pattern(
    rxs: &mut [(
        String,
        tokio::sync::broadcast::Receiver<crate::pubsub::PubMessage>,
    )],
) -> Option<SubscribeEvent> {
    if rxs.is_empty() {
        std::future::pending::<()>().await;
        return None;
    }

    loop {
        for (pat, rx) in rxs.iter_mut() {
            match rx.try_recv() {
                Ok(msg) => {
                    return Some(SubscribeEvent {
                        kind: "pmessage".to_string(),
                        channel: msg.channel.to_string(),
                        data: msg.data.to_vec(),
                        pattern: Some(pat.clone()),
                    });
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => return None,
            }
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
