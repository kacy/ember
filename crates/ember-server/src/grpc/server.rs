//! Server administration, slowlog, and pipeline handlers for the gRPC service.

use std::sync::Arc;

use bytes::Bytes;
use ember_protocol::Command;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::lists::status_response;
use super::proto::ember_cache_server::EmberCache;
use super::proto::*;
use super::strings::int_response;
use super::{reply, session_of, EmberService};
use crate::connection_common::Session;

pub(super) async fn ping(
    svc: &EmberService,
    request: Request<PingRequest>,
) -> Result<Response<PingResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Ping(r.message.map(Bytes::from)))
        .await?;
    Ok(Response::new(PingResponse {
        message: reply::string(frame)?,
    }))
}

pub(super) async fn echo(
    svc: &EmberService,
    request: Request<EchoRequest>,
) -> Result<Response<EchoResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Echo(Bytes::from(r.message)))
        .await?;
    Ok(Response::new(EchoResponse {
        message: reply::string(frame)?,
    }))
}

pub(super) async fn flush_db(
    svc: &EmberService,
    request: Request<FlushDbRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::FlushDb {
            async_mode: r.r#async,
        })
        .await?;
    status_response(frame)
}

pub(super) async fn db_size(
    svc: &EmberService,
    request: Request<DbSizeRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |_| Command::DbSize).await?;
    int_response(frame)
}

pub(super) async fn info(
    svc: &EmberService,
    request: Request<InfoRequest>,
) -> Result<Response<InfoResponse>, Status> {
    let frame = svc
        .call(request, |r| Command::Info { section: r.section })
        .await?;
    Ok(Response::new(InfoResponse {
        info: reply::string(frame)?,
    }))
}

pub(super) async fn bg_save(
    svc: &EmberService,
    request: Request<BgSaveRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let frame = svc.call(request, |_| Command::BgSave).await?;
    status_response(frame)
}

pub(super) async fn bg_rewrite_aof(
    svc: &EmberService,
    request: Request<BgRewriteAofRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let frame = svc.call(request, |_| Command::BgRewriteAof).await?;
    status_response(frame)
}

/// Checks the caller may run SLOWLOG GET, then builds the entries from the
/// slowlog directly, since the RESP reply drops their types.
pub(super) async fn slow_log_get(
    svc: &EmberService,
    request: Request<SlowLogGetRequest>,
) -> Result<Response<SlowLogGetResponse>, Status> {
    let count = request.get_ref().count.map(|c| c as usize);
    svc.authorize(&request, &Command::SlowLogGet { count })?;
    let entries = svc.slow_log.get(count);
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

pub(super) async fn slow_log_len(
    svc: &EmberService,
    request: Request<SlowLogLenRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |_| Command::SlowLogLen).await?;
    int_response(frame)
}

pub(super) async fn slow_log_reset(
    svc: &EmberService,
    request: Request<SlowLogResetRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let frame = svc.call(request, |_| Command::SlowLogReset).await?;
    status_response(frame)
}

pub(super) async fn time(
    svc: &EmberService,
    request: Request<TimeRequest>,
) -> Result<Response<TimeResponse>, Status> {
    let frame = svc.call(request, |_| Command::Time).await?;
    let mut parts = reply::items(frame)?.into_iter().map(reply::string);
    let mut next = || -> Result<i64, Status> {
        let text = parts.next().transpose()?.unwrap_or_default();
        text.parse()
            .map_err(|_| Status::internal(format!("bad TIME reply: {text}")))
    };
    Ok(Response::new(TimeResponse {
        seconds: next()?,
        microseconds: next()?,
    }))
}

pub(super) async fn last_save(
    svc: &EmberService,
    request: Request<LastSaveRequest>,
) -> Result<Response<IntResponse>, Status> {
    let frame = svc.call(request, |_| Command::LastSave).await?;
    int_response(frame)
}

pub(super) async fn pipeline(
    svc: &EmberService,
    request: Request<Streaming<PipelineRequest>>,
) -> Result<Response<ReceiverStream<Result<PipelineResponse, Status>>>, Status> {
    let session = session_of(&request);
    let mut stream = request.into_inner();
    let engine = svc.engine.clone();
    let ctx = Arc::clone(&svc.ctx);
    let slow_log = Arc::clone(&svc.slow_log);
    let pubsub = Arc::clone(&svc.pubsub);

    let (tx, rx) = tokio::sync::mpsc::channel(256);

    tokio::spawn(async move {
        let svc = EmberService::new(engine, ctx, slow_log, pubsub);
        while let Ok(Some(req)) = stream.message().await {
            let id = req.id;
            let result = handle_pipeline_command(&svc, &session, req).await;
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

/// Dispatches a single pipeline command to the appropriate RPC handler.
/// Dispatches a single pipeline command to the service method and wraps the
/// response in a PipelineResponse. Each arm follows the same pattern: call
/// the service method, extract the inner response, wrap it in the correct
/// result variant. The macro eliminates the boilerplate of ~57 identical arms.
macro_rules! pipeline_dispatch {
    ($svc:expr, $session:expr, $id:expr, $cmd:expr, {
        $( $Variant:ident => $method:ident => $Result:ident ),* $(,)?
    }) => {
        match $cmd {
            $(
                pipeline_request::Command::$Variant(r) => {
                    // each command runs as the stream's caller
                    let mut req = Request::new(r);
                    req.extensions_mut().insert($session.clone());
                    let resp = $svc.$method(req).await?;
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
    session: &Session,
    req: PipelineRequest,
) -> Result<PipelineResponse, Status> {
    let id = req.id;
    let cmd = req
        .command
        .ok_or_else(|| Status::invalid_argument("missing command"))?;

    pipeline_dispatch!(svc, session, id, cmd, {
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

        // new keys
        Expiretime  => expiretime  => IntVal,
        Pexpiretime => pexpiretime => IntVal,
        Expireat    => expireat    => BoolVal,
        Pexpireat   => pexpireat   => BoolVal,

        // new strings
        Getset => getset => Get,
        Msetnx => msetnx => BoolVal,

        // bitmaps
        Getbit   => getbit   => IntVal,
        Setbit   => setbit   => IntVal,
        Bitcount => bitcount => IntVal,
        Bitpos   => bitpos   => IntVal,
        Bitop    => bitop    => IntVal,

        // new sets
        Smove     => smove     => BoolVal,
        Sintercard => sintercard => IntVal,

        // new lists
        Lmpop => lmpop => Lmpop,

        // new sorted sets
        Zmpop       => zmpop       => Zmpop,
        Hrandfield  => hrandfield  => Array,
        Zrandmember => zrandmember => Array,
    })
}
