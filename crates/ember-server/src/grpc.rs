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

// input validation limits — reject obviously malformed requests early
const MAX_KEY_LEN: usize = 512 * 1024; // 512 KB
const MAX_VALUE_LEN: usize = 512 * 1024 * 1024; // 512 MB
#[cfg(feature = "vector")]
const MAX_VECTOR_DIMS: usize = 65_536;
#[cfg(feature = "vector")]
const MAX_VSIM_COUNT: usize = 10_000;
#[cfg(feature = "vector")]
const MAX_HNSW_M: u32 = 1_024;
#[cfg(feature = "vector")]
const MAX_HNSW_EF: u32 = 1_024;

#[allow(clippy::result_large_err)] // Status is tonic's idiomatic error type
fn validate_key(key: &str) -> Result<(), Status> {
    if key.is_empty() {
        return Err(Status::invalid_argument("key must not be empty"));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(Status::invalid_argument(format!(
            "key length {} exceeds max {MAX_KEY_LEN}",
            key.len()
        )));
    }
    Ok(())
}

#[allow(clippy::result_large_err)] // Status is tonic's idiomatic error type
fn validate_value(value: &[u8]) -> Result<(), Status> {
    if value.len() > MAX_VALUE_LEN {
        return Err(Status::invalid_argument(format!(
            "value length {} exceeds max {MAX_VALUE_LEN}",
            value.len()
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

#[tonic::async_trait]
impl EmberCache for EmberService {
    // -----------------------------------------------------------------------
    // strings
    // -----------------------------------------------------------------------

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let start = Instant::now();
        let req = request.into_inner();
        let key = req.key;
        validate_key(&key)?;

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
        validate_key(&req.key)?;
        validate_value(&req.value)?;

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
            validate_key(k)?;
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
            validate_key(k)?;
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
            validate_key(&p.key)?;
            validate_value(&p.value)?;
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
            validate_key(&req.key)?;
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
            validate_key(&req.key)?;

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
            validate_key(&req.key)?;
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
        validate_key(&key)?;
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
            validate_key(k)?;
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
        if total_subs > crate::connection_common::MAX_SUBSCRIPTIONS_PER_CONN {
            return Err(Status::invalid_argument(format!(
                "too many subscriptions ({total_subs}), max {}",
                crate::connection_common::MAX_SUBSCRIPTIONS_PER_CONN
            )));
        }

        for pat in &req.patterns {
            if pat.len() > crate::connection_common::MAX_PATTERN_LEN {
                return Err(Status::invalid_argument(format!(
                    "pattern too long ({} bytes), max {}",
                    pat.len(),
                    crate::connection_common::MAX_PATTERN_LEN
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
                        channel: msg.channel,
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
                        channel: msg.channel,
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
