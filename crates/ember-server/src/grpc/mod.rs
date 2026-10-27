//! gRPC service implementation for ember.
//!
//! Implements the `EmberCache` tonic service. Each handler turns its proto
//! request into a [`Command`] and runs it through the same path as a RESP
//! command, so ACLs, cluster checks, persistence, replication and keyspace
//! notifications behave the same for both protocols. [`reply`] maps the
//! RESP reply back to the proto response.

// tonic's Status is large, and it is the error type every handler returns
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use ember_core::Engine;
use ember_protocol::command::ScoreBound;
use ember_protocol::{Command, Frame};
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;
use tonic::{Request, Response, Status, Streaming};

use crate::connection_common::Session;
use crate::pubsub::PubSubManager;
use crate::server::ServerContext;
use crate::slowlog::SlowLog;

pub mod proto {
    tonic::include_proto!("ember.v1");
}

mod hashes;
mod keyspace;
mod lists;
mod pubsub;
mod reply;
mod server;
mod sets;
mod sorted_sets;
mod strings;
mod vector;

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

    /// Builds this service into a tonic router behind [`AuthInterceptor`].
    pub fn into_service(
        self,
    ) -> InterceptedService<proto::ember_cache_server::EmberCacheServer<Self>, AuthInterceptor>
    {
        let interceptor = AuthInterceptor {
            ctx: Arc::clone(&self.ctx),
        };
        let svc = proto::ember_cache_server::EmberCacheServer::new(self)
            .max_decoding_message_size(4 * 1024 * 1024) // 4 MB
            .max_encoding_message_size(4 * 1024 * 1024);
        InterceptedService::new(svc, interceptor)
    }

    /// Runs `cmd` as the caller in `session`, through the same checks and
    /// execution path as a RESP command: size limits, ACL, cluster slot and
    /// replica checks, keyspace notifications, metrics and the slowlog. An
    /// error reply becomes a gRPC status.
    pub(super) async fn run(&self, session: &Session, cmd: Command) -> Result<Frame, Status> {
        let frame = crate::connection::dispatch::run_command(
            cmd,
            &self.engine,
            &self.ctx,
            &self.slow_log,
            &self.pubsub,
            &mut false,
            0,
            session,
        )
        .await;
        match frame {
            Frame::Error(msg) => Err(error_status(msg)),
            frame => Ok(frame),
        }
    }

    /// Runs the command `to_cmd` builds from the request, as the caller
    /// the interceptor logged in.
    pub(super) async fn call<T>(
        &self,
        request: Request<T>,
        to_cmd: impl FnOnce(T) -> Command,
    ) -> Result<Frame, Status> {
        self.try_call(request, |req| Ok(to_cmd(req))).await
    }

    /// Like [`call`](Self::call), for requests that can fail validation.
    pub(super) async fn try_call<T>(
        &self,
        request: Request<T>,
        to_cmd: impl FnOnce(T) -> Result<Command, Status>,
    ) -> Result<Frame, Status> {
        let session = session_of(&request);
        let cmd = to_cmd(request.into_inner())?;
        self.run(&session, cmd).await
    }

    /// Checks that the caller may run `cmd`, for handlers that answer from
    /// server state instead of running the command.
    pub(super) fn authorize<T>(&self, request: &Request<T>, cmd: &Command) -> Result<(), Status> {
        match session_of(request).check(cmd) {
            Some(Frame::Error(msg)) => Err(error_status(msg)),
            Some(other) => Err(Status::internal(format!("unexpected reply: {other:?}"))),
            None => Ok(()),
        }
    }
}

/// gRPC authentication interceptor.
///
/// Logs each request in as an ACL user, the way AUTH does on a RESP
/// connection, and stores the [`Session`] in the request's extensions for
/// [`EmberService::run`]. The `authorization` metadata header carries the
/// password, and the optional `username` header names the user (`default`
/// when absent). When `default` needs no password, a request without a
/// `username` header runs as `default`.
#[derive(Clone)]
pub struct AuthInterceptor {
    ctx: Arc<ServerContext>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        // the same protected mode the RESP listeners apply
        let peer = req.remote_addr().map(|addr| addr.ip());
        if crate::server::is_protected_mode_violation(&self.ctx, peer) {
            return Err(Status::permission_denied(
                "protected mode: no password is set, so only loopback clients may connect",
            ));
        }
        let header = |name| req.metadata().get(name).and_then(|v| v.to_str().ok());
        let open = Session::new(&self.ctx);
        let session = match (header("username"), header("authorization")) {
            // no password needed: a password sent anyway is ignored, as
            // before ACL users applied to gRPC
            (None, _) if open.is_authenticated() => open,
            (username, Some(password)) => {
                Session::login(&self.ctx, username.unwrap_or("default"), password)
                    .map_err(Status::unauthenticated)?
            }
            _ => return Err(Status::unauthenticated("authentication required")),
        };
        req.extensions_mut().insert(session);
        Ok(req)
    }
}

/// Returns the session the interceptor stored on `req`. A request that
/// did not pass through the interceptor gets an unauthenticated session,
/// whose commands fail with NOAUTH.
pub(super) fn session_of<T>(req: &Request<T>) -> Session {
    req.extensions()
        .get::<Session>()
        .cloned()
        .unwrap_or_else(Session::anonymous)
}

/// Maps an error reply from command execution to a gRPC status. The
/// message keeps the full Redis error text.
pub(super) fn error_status(msg: String) -> Status {
    let code = msg.split_whitespace().next().unwrap_or_default();
    match code {
        "NOAUTH" | "WRONGPASS" => Status::unauthenticated(msg),
        "NOPERM" => Status::permission_denied(msg),
        "OOM" => Status::resource_exhausted(msg),
        "CLUSTERDOWN" | "TRYAGAIN" | "LOADING" => Status::unavailable(msg),
        "WRONGTYPE" | "MOVED" | "ASK" | "READONLY" | "CROSSSLOT" => {
            Status::failed_precondition(msg)
        }
        _ if msg.starts_with("ERR no such key") => Status::not_found(msg),
        _ if msg.starts_with("ERR unknown command") => Status::unimplemented(msg),
        _ => Status::invalid_argument(msg),
    }
}

/// Rejects an empty key list, which the RESP parser would refuse for
/// having too few arguments.
pub(super) fn non_empty(keys: Vec<String>) -> Result<Vec<String>, Status> {
    if keys.is_empty() {
        return Err(Status::invalid_argument("at least one key required"));
    }
    Ok(keys)
}

/// Parses a Redis-style score bound string into a `ScoreBound`.
///
/// Supports "-inf", "+inf", exclusive bounds like "(5.0", and inclusive
/// bounds like "5.0". Returns an error status for invalid strings.
pub(super) fn parse_score_bound(s: &str) -> Result<ScoreBound, Status> {
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

#[tonic::async_trait]
impl EmberCache for EmberService {
    // -----------------------------------------------------------------------
    // strings
    // -----------------------------------------------------------------------

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        strings::get(self, request).await
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        strings::set(self, request).await
    }

    async fn del(&self, request: Request<DelRequest>) -> Result<Response<DelResponse>, Status> {
        keyspace::del(self, request).await
    }

    async fn m_get(&self, request: Request<MGetRequest>) -> Result<Response<MGetResponse>, Status> {
        strings::m_get(self, request).await
    }

    async fn m_set(&self, request: Request<MSetRequest>) -> Result<Response<MSetResponse>, Status> {
        strings::m_set(self, request).await
    }

    async fn incr(&self, request: Request<IncrRequest>) -> Result<Response<IntResponse>, Status> {
        strings::incr(self, request).await
    }

    async fn incr_by(
        &self,
        request: Request<IncrByRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        strings::incr_by(self, request).await
    }

    async fn decr_by(
        &self,
        request: Request<DecrByRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        strings::decr_by(self, request).await
    }

    async fn incr_by_float(
        &self,
        request: Request<IncrByFloatRequest>,
    ) -> Result<Response<FloatResponse>, Status> {
        strings::incr_by_float(self, request).await
    }

    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        strings::append(self, request).await
    }

    async fn strlen(
        &self,
        request: Request<StrlenRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        strings::strlen(self, request).await
    }

    // -----------------------------------------------------------------------
    // keys
    // -----------------------------------------------------------------------

    async fn exists(
        &self,
        request: Request<ExistsRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        keyspace::exists(self, request).await
    }

    async fn expire(
        &self,
        request: Request<ExpireRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        keyspace::expire(self, request).await
    }

    async fn p_expire(
        &self,
        request: Request<PExpireRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        keyspace::p_expire(self, request).await
    }

    async fn persist(
        &self,
        request: Request<PersistRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        keyspace::persist(self, request).await
    }

    async fn ttl(&self, request: Request<TtlRequest>) -> Result<Response<TtlResponse>, Status> {
        keyspace::ttl(self, request).await
    }

    async fn p_ttl(&self, request: Request<PTtlRequest>) -> Result<Response<TtlResponse>, Status> {
        keyspace::p_ttl(self, request).await
    }

    async fn r#type(
        &self,
        request: Request<TypeRequest>,
    ) -> Result<Response<TypeResponse>, Status> {
        keyspace::r#type(self, request).await
    }

    async fn keys(&self, request: Request<KeysRequest>) -> Result<Response<KeysResponse>, Status> {
        keyspace::keys(self, request).await
    }

    async fn rename(
        &self,
        request: Request<RenameRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        keyspace::rename(self, request).await
    }

    async fn scan(&self, request: Request<ScanRequest>) -> Result<Response<ScanResponse>, Status> {
        keyspace::scan(self, request).await
    }

    // -----------------------------------------------------------------------
    // lists
    // -----------------------------------------------------------------------

    async fn l_push(
        &self,
        request: Request<LPushRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        lists::l_push(self, request).await
    }

    async fn r_push(
        &self,
        request: Request<RPushRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        lists::r_push(self, request).await
    }

    async fn l_pop(&self, request: Request<LPopRequest>) -> Result<Response<GetResponse>, Status> {
        lists::l_pop(self, request).await
    }

    async fn r_pop(&self, request: Request<RPopRequest>) -> Result<Response<GetResponse>, Status> {
        lists::r_pop(self, request).await
    }

    async fn l_range(
        &self,
        request: Request<LRangeRequest>,
    ) -> Result<Response<ArrayResponse>, Status> {
        lists::l_range(self, request).await
    }

    async fn l_len(&self, request: Request<LLenRequest>) -> Result<Response<IntResponse>, Status> {
        lists::l_len(self, request).await
    }

    // -----------------------------------------------------------------------
    // hashes
    // -----------------------------------------------------------------------

    async fn h_set(&self, request: Request<HSetRequest>) -> Result<Response<IntResponse>, Status> {
        hashes::h_set(self, request).await
    }

    async fn h_get(&self, request: Request<HGetRequest>) -> Result<Response<GetResponse>, Status> {
        hashes::h_get(self, request).await
    }

    async fn h_get_all(
        &self,
        request: Request<HGetAllRequest>,
    ) -> Result<Response<HashResponse>, Status> {
        hashes::h_get_all(self, request).await
    }

    async fn h_del(&self, request: Request<HDelRequest>) -> Result<Response<IntResponse>, Status> {
        hashes::h_del(self, request).await
    }

    async fn h_exists(
        &self,
        request: Request<HExistsRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        hashes::h_exists(self, request).await
    }

    async fn h_len(&self, request: Request<HLenRequest>) -> Result<Response<IntResponse>, Status> {
        hashes::h_len(self, request).await
    }

    async fn h_incr_by(
        &self,
        request: Request<HIncrByRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        hashes::h_incr_by(self, request).await
    }

    async fn h_keys(
        &self,
        request: Request<HKeysRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        hashes::h_keys(self, request).await
    }

    async fn h_vals(
        &self,
        request: Request<HValsRequest>,
    ) -> Result<Response<ArrayResponse>, Status> {
        hashes::h_vals(self, request).await
    }

    async fn hm_get(
        &self,
        request: Request<HmGetRequest>,
    ) -> Result<Response<OptionalArrayResponse>, Status> {
        hashes::hm_get(self, request).await
    }

    // -----------------------------------------------------------------------
    // sets
    // -----------------------------------------------------------------------

    async fn s_add(&self, request: Request<SAddRequest>) -> Result<Response<IntResponse>, Status> {
        sets::s_add(self, request).await
    }

    async fn s_rem(&self, request: Request<SRemRequest>) -> Result<Response<IntResponse>, Status> {
        sets::s_rem(self, request).await
    }

    async fn s_members(
        &self,
        request: Request<SMembersRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        sets::s_members(self, request).await
    }

    async fn s_is_member(
        &self,
        request: Request<SIsMemberRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        sets::s_is_member(self, request).await
    }

    async fn s_card(
        &self,
        request: Request<SCardRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        sets::s_card(self, request).await
    }

    // -----------------------------------------------------------------------
    // sorted sets
    // -----------------------------------------------------------------------

    async fn z_add(&self, request: Request<ZAddRequest>) -> Result<Response<IntResponse>, Status> {
        sorted_sets::z_add(self, request).await
    }

    async fn z_rem(&self, request: Request<ZRemRequest>) -> Result<Response<IntResponse>, Status> {
        sorted_sets::z_rem(self, request).await
    }

    async fn z_score(
        &self,
        request: Request<ZScoreRequest>,
    ) -> Result<Response<OptionalFloatResponse>, Status> {
        sorted_sets::z_score(self, request).await
    }

    async fn z_rank(
        &self,
        request: Request<ZRankRequest>,
    ) -> Result<Response<OptionalIntResponse>, Status> {
        sorted_sets::z_rank(self, request).await
    }

    async fn z_card(
        &self,
        request: Request<ZCardRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        sorted_sets::z_card(self, request).await
    }

    async fn z_range(
        &self,
        request: Request<ZRangeRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        sorted_sets::z_range(self, request).await
    }

    // -----------------------------------------------------------------------
    // vectors
    // -----------------------------------------------------------------------

    async fn v_add(&self, request: Request<VAddRequest>) -> Result<Response<BoolResponse>, Status> {
        vector::v_add(self, request).await
    }

    async fn v_add_batch(
        &self,
        request: Request<VAddBatchRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        vector::v_add_batch(self, request).await
    }

    async fn v_sim(&self, request: Request<VSimRequest>) -> Result<Response<VSimResponse>, Status> {
        vector::v_sim(self, request).await
    }

    async fn v_rem(&self, request: Request<VRemRequest>) -> Result<Response<BoolResponse>, Status> {
        vector::v_rem(self, request).await
    }

    async fn v_get(&self, request: Request<VGetRequest>) -> Result<Response<VGetResponse>, Status> {
        vector::v_get(self, request).await
    }

    async fn v_card(
        &self,
        request: Request<VCardRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        vector::v_card(self, request).await
    }

    async fn v_dim(&self, request: Request<VDimRequest>) -> Result<Response<IntResponse>, Status> {
        vector::v_dim(self, request).await
    }

    async fn v_info(
        &self,
        request: Request<VInfoRequest>,
    ) -> Result<Response<VInfoResponse>, Status> {
        vector::v_info(self, request).await
    }

    // -----------------------------------------------------------------------
    // server
    // -----------------------------------------------------------------------

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        server::ping(self, request).await
    }

    async fn flush_db(
        &self,
        request: Request<FlushDbRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        server::flush_db(self, request).await
    }

    async fn db_size(
        &self,
        _request: Request<DbSizeRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        server::db_size(self, _request).await
    }

    async fn info(&self, request: Request<InfoRequest>) -> Result<Response<InfoResponse>, Status> {
        server::info(self, request).await
    }

    // -----------------------------------------------------------------------
    // additional commands
    // -----------------------------------------------------------------------

    async fn echo(&self, request: Request<EchoRequest>) -> Result<Response<EchoResponse>, Status> {
        server::echo(self, request).await
    }

    async fn decr(&self, request: Request<DecrRequest>) -> Result<Response<IntResponse>, Status> {
        strings::decr(self, request).await
    }

    async fn unlink(
        &self,
        request: Request<UnlinkRequest>,
    ) -> Result<Response<DelResponse>, Status> {
        keyspace::unlink(self, request).await
    }

    async fn bg_save(
        &self,
        _request: Request<BgSaveRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        server::bg_save(self, _request).await
    }

    async fn bg_rewrite_aof(
        &self,
        _request: Request<BgRewriteAofRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        server::bg_rewrite_aof(self, _request).await
    }

    // -----------------------------------------------------------------------
    // slowlog
    // -----------------------------------------------------------------------

    async fn slow_log_get(
        &self,
        request: Request<SlowLogGetRequest>,
    ) -> Result<Response<SlowLogGetResponse>, Status> {
        server::slow_log_get(self, request).await
    }

    async fn slow_log_len(
        &self,
        _request: Request<SlowLogLenRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        server::slow_log_len(self, _request).await
    }

    async fn slow_log_reset(
        &self,
        _request: Request<SlowLogResetRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        server::slow_log_reset(self, _request).await
    }

    // -----------------------------------------------------------------------
    // pub/sub
    // -----------------------------------------------------------------------

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        pubsub::publish(self, request).await
    }

    type SubscribeStream = ReceiverStream<Result<SubscribeEvent, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        pubsub::subscribe(self, request).await
    }

    async fn pub_sub_channels(
        &self,
        request: Request<PubSubChannelsRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        pubsub::pub_sub_channels(self, request).await
    }

    async fn pub_sub_num_sub(
        &self,
        request: Request<PubSubNumSubRequest>,
    ) -> Result<Response<PubSubNumSubResponse>, Status> {
        pubsub::pub_sub_num_sub(self, request).await
    }

    async fn pub_sub_num_pat(
        &self,
        _request: Request<PubSubNumPatRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        pubsub::pub_sub_num_pat(self, _request).await
    }

    // -----------------------------------------------------------------------
    // strings (extended)
    // -----------------------------------------------------------------------

    async fn get_del(
        &self,
        request: Request<GetDelRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        strings::get_del(self, request).await
    }

    async fn get_ex(
        &self,
        request: Request<GetExRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        strings::get_ex(self, request).await
    }

    async fn get_range(
        &self,
        request: Request<GetRangeRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        strings::get_range(self, request).await
    }

    async fn set_range(
        &self,
        request: Request<SetRangeRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        strings::set_range(self, request).await
    }

    // -----------------------------------------------------------------------
    // keys (extended)
    // -----------------------------------------------------------------------

    async fn copy(&self, request: Request<CopyRequest>) -> Result<Response<BoolResponse>, Status> {
        keyspace::copy(self, request).await
    }

    async fn random_key(
        &self,
        _request: Request<RandomKeyRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        keyspace::random_key(self, _request).await
    }

    async fn touch(&self, request: Request<TouchRequest>) -> Result<Response<IntResponse>, Status> {
        keyspace::touch(self, request).await
    }

    // -----------------------------------------------------------------------
    // lists (extended)
    // -----------------------------------------------------------------------

    async fn l_index(
        &self,
        request: Request<LIndexRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        lists::l_index(self, request).await
    }

    async fn l_set(
        &self,
        request: Request<LSetRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        lists::l_set(self, request).await
    }

    async fn l_trim(
        &self,
        request: Request<LTrimRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        lists::l_trim(self, request).await
    }

    async fn l_insert(
        &self,
        request: Request<LInsertRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        lists::l_insert(self, request).await
    }

    async fn l_rem(&self, request: Request<LRemRequest>) -> Result<Response<IntResponse>, Status> {
        lists::l_rem(self, request).await
    }

    async fn l_pos(
        &self,
        request: Request<LPosRequest>,
    ) -> Result<Response<OptionalIntResponse>, Status> {
        lists::l_pos(self, request).await
    }

    async fn l_move(
        &self,
        request: Request<LMoveRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        lists::l_move(self, request).await
    }

    // -----------------------------------------------------------------------
    // sets (extended)
    // -----------------------------------------------------------------------

    async fn s_union(
        &self,
        request: Request<SUnionRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        sets::s_union(self, request).await
    }

    async fn s_inter(
        &self,
        request: Request<SInterRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        sets::s_inter(self, request).await
    }

    async fn s_diff(
        &self,
        request: Request<SDiffRequest>,
    ) -> Result<Response<KeysResponse>, Status> {
        sets::s_diff(self, request).await
    }

    async fn s_union_store(
        &self,
        request: Request<SUnionStoreRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        sets::s_union_store(self, request).await
    }

    async fn s_inter_store(
        &self,
        request: Request<SInterStoreRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        sets::s_inter_store(self, request).await
    }

    async fn s_diff_store(
        &self,
        request: Request<SDiffStoreRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        sets::s_diff_store(self, request).await
    }

    async fn s_rand_member(
        &self,
        request: Request<SRandMemberRequest>,
    ) -> Result<Response<ArrayResponse>, Status> {
        sets::s_rand_member(self, request).await
    }

    async fn s_pop(
        &self,
        request: Request<SPopRequest>,
    ) -> Result<Response<ArrayResponse>, Status> {
        sets::s_pop(self, request).await
    }

    async fn s_mis_member(
        &self,
        request: Request<SMisMemberRequest>,
    ) -> Result<Response<BoolArrayResponse>, Status> {
        sets::s_mis_member(self, request).await
    }

    // -----------------------------------------------------------------------
    // hashes (extended)
    // -----------------------------------------------------------------------

    async fn h_scan(
        &self,
        request: Request<HScanRequest>,
    ) -> Result<Response<HScanResponse>, Status> {
        hashes::h_scan(self, request).await
    }

    // -----------------------------------------------------------------------
    // sorted sets (extended)
    // -----------------------------------------------------------------------

    async fn z_rev_rank(
        &self,
        request: Request<ZRevRankRequest>,
    ) -> Result<Response<OptionalIntResponse>, Status> {
        sorted_sets::z_rev_rank(self, request).await
    }

    async fn z_rev_range(
        &self,
        request: Request<ZRevRangeRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        sorted_sets::z_rev_range(self, request).await
    }

    async fn z_count(
        &self,
        request: Request<ZCountRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        sorted_sets::z_count(self, request).await
    }

    async fn z_incr_by(
        &self,
        request: Request<ZIncrByRequest>,
    ) -> Result<Response<FloatResponse>, Status> {
        sorted_sets::z_incr_by(self, request).await
    }

    async fn z_range_by_score(
        &self,
        request: Request<ZRangeByScoreRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        sorted_sets::z_range_by_score(self, request).await
    }

    async fn z_rev_range_by_score(
        &self,
        request: Request<ZRevRangeByScoreRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        sorted_sets::z_rev_range_by_score(self, request).await
    }

    async fn z_pop_min(
        &self,
        request: Request<ZPopMinRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        sorted_sets::z_pop_min(self, request).await
    }

    async fn z_pop_max(
        &self,
        request: Request<ZPopMaxRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        sorted_sets::z_pop_max(self, request).await
    }

    async fn z_diff(
        &self,
        request: Request<ZDiffRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        sorted_sets::z_diff(self, request).await
    }

    async fn z_inter(
        &self,
        request: Request<ZInterRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        sorted_sets::z_inter(self, request).await
    }

    async fn z_union(
        &self,
        request: Request<ZUnionRequest>,
    ) -> Result<Response<ZRangeResponse>, Status> {
        sorted_sets::z_union(self, request).await
    }

    async fn z_scan(
        &self,
        request: Request<ZScanRequest>,
    ) -> Result<Response<ZScanResponse>, Status> {
        sorted_sets::z_scan(self, request).await
    }

    // -----------------------------------------------------------------------
    // scans
    // -----------------------------------------------------------------------

    async fn s_scan(
        &self,
        request: Request<SScanRequest>,
    ) -> Result<Response<SScanResponse>, Status> {
        sets::s_scan(self, request).await
    }

    // -----------------------------------------------------------------------
    // server (extended)
    // -----------------------------------------------------------------------

    async fn time(&self, _request: Request<TimeRequest>) -> Result<Response<TimeResponse>, Status> {
        server::time(self, _request).await
    }

    async fn last_save(
        &self,
        _request: Request<LastSaveRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        server::last_save(self, _request).await
    }

    // -----------------------------------------------------------------------
    // keys (new)
    // -----------------------------------------------------------------------

    async fn expiretime(
        &self,
        request: Request<ExpiretimeRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        keyspace::expiretime(self, request).await
    }

    async fn pexpiretime(
        &self,
        request: Request<PexpiretimeRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        keyspace::pexpiretime(self, request).await
    }

    async fn expireat(
        &self,
        request: Request<ExpireatRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        keyspace::expireat(self, request).await
    }

    async fn pexpireat(
        &self,
        request: Request<PexpireatRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        keyspace::pexpireat(self, request).await
    }

    // -----------------------------------------------------------------------
    // strings (new)
    // -----------------------------------------------------------------------

    async fn getset(
        &self,
        request: Request<GetsetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        strings::getset(self, request).await
    }

    async fn msetnx(
        &self,
        request: Request<MsetnxRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        strings::msetnx(self, request).await
    }

    // -----------------------------------------------------------------------
    // bitmaps
    // -----------------------------------------------------------------------

    async fn getbit(
        &self,
        request: Request<GetbitRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        strings::getbit(self, request).await
    }

    async fn setbit(
        &self,
        request: Request<SetbitRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        strings::setbit(self, request).await
    }

    async fn bitcount(
        &self,
        request: Request<BitcountRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        strings::bitcount(self, request).await
    }

    async fn bitpos(
        &self,
        request: Request<BitposRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        strings::bitpos(self, request).await
    }

    async fn bitop(&self, request: Request<BitopRequest>) -> Result<Response<IntResponse>, Status> {
        strings::bitop(self, request).await
    }

    // -----------------------------------------------------------------------
    // sets (new)
    // -----------------------------------------------------------------------

    async fn smove(
        &self,
        request: Request<SmoveRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        sets::smove(self, request).await
    }

    async fn sintercard(
        &self,
        request: Request<SintercardRequest>,
    ) -> Result<Response<IntResponse>, Status> {
        sets::sintercard(self, request).await
    }

    // -----------------------------------------------------------------------
    // lists (new)
    // -----------------------------------------------------------------------

    async fn lmpop(
        &self,
        request: Request<LmpopRequest>,
    ) -> Result<Response<LmpopResponse>, Status> {
        lists::lmpop(self, request).await
    }

    // -----------------------------------------------------------------------
    // sorted sets (new)
    // -----------------------------------------------------------------------

    async fn zmpop(
        &self,
        request: Request<ZmpopRequest>,
    ) -> Result<Response<ZmpopResponse>, Status> {
        sorted_sets::zmpop(self, request).await
    }

    // -----------------------------------------------------------------------
    // hashes (new)
    // -----------------------------------------------------------------------

    async fn hrandfield(
        &self,
        request: Request<HrandfieldRequest>,
    ) -> Result<Response<ArrayResponse>, Status> {
        hashes::hrandfield(self, request).await
    }

    async fn zrandmember(
        &self,
        request: Request<ZrandmemberRequest>,
    ) -> Result<Response<ArrayResponse>, Status> {
        sorted_sets::zrandmember(self, request).await
    }

    // -----------------------------------------------------------------------
    // pipeline (bidirectional streaming)
    // -----------------------------------------------------------------------

    type PipelineStream = ReceiverStream<Result<PipelineResponse, Status>>;

    async fn pipeline(
        &self,
        request: Request<Streaming<PipelineRequest>>,
    ) -> Result<Response<Self::PipelineStream>, Status> {
        server::pipeline(self, request).await
    }
}
