//! Server administration, slowlog, and pipeline handlers for the gRPC service.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use ember_core::{ShardRequest, ShardResponse};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::proto::ember_cache_server::EmberCache;
use super::proto::*;
use super::EmberService;

pub(super) async fn ping(
    _svc: &EmberService,
    request: Request<PingRequest>,
) -> Result<Response<PingResponse>, Status> {
    let msg = request.into_inner().message;
    Ok(Response::new(PingResponse {
        message: msg.unwrap_or_else(|| "PONG".to_string()),
    }))
}

pub(super) async fn flush_db(
    svc: &EmberService,
    request: Request<FlushDbRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let start = Instant::now();
    let async_mode = request.into_inner().r#async;

    if async_mode {
        svc.broadcast(|| ShardRequest::FlushDbAsync).await?;
    } else {
        svc.broadcast(|| ShardRequest::FlushDb).await?;
    }

    svc.record_command(start, "FLUSHDB");
    Ok(Response::new(StatusResponse {
        status: "OK".to_string(),
    }))
}

pub(super) async fn db_size(
    svc: &EmberService,
    _request: Request<DbSizeRequest>,
) -> Result<Response<IntResponse>, Status> {
    let start = Instant::now();

    let responses = svc.broadcast(|| ShardRequest::DbSize).await?;
    let mut total = 0i64;
    for resp in responses {
        if let ShardResponse::KeyCount(n) = resp {
            total += n as i64;
        }
    }
    svc.record_command(start, "DBSIZE");
    Ok(Response::new(IntResponse { value: total }))
}

pub(super) async fn info(
    svc: &EmberService,
    request: Request<InfoRequest>,
) -> Result<Response<InfoResponse>, Status> {
    let start = Instant::now();
    let _section = request.into_inner().section;

    let responses = svc.broadcast(|| ShardRequest::Stats).await?;
    let mut total_keys = 0usize;
    let mut total_memory = 0usize;
    for resp in &responses {
        if let ShardResponse::Stats(stats) = resp {
            total_keys += stats.key_count;
            total_memory += stats.used_bytes;
        }
    }

    let uptime = svc.ctx.start_time.elapsed().as_secs();
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
        svc.ctx.version,
        uptime,
        svc.ctx.shard_count,
        svc.ctx.connections_active.load(Ordering::Relaxed),
        total_memory,
        total_keys,
        svc.ctx.commands_processed.load(Ordering::Relaxed),
        svc.ctx.connections_accepted.load(Ordering::Relaxed),
    );

    svc.record_command(start, "INFO");
    Ok(Response::new(InfoResponse { info }))
}

pub(super) async fn echo(
    _svc: &EmberService,
    request: Request<EchoRequest>,
) -> Result<Response<EchoResponse>, Status> {
    Ok(Response::new(EchoResponse {
        message: request.into_inner().message,
    }))
}

pub(super) async fn bg_save(
    svc: &EmberService,
    _request: Request<BgSaveRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let start = Instant::now();
    svc.broadcast(|| ShardRequest::Snapshot).await?;
    svc.record_command(start, "BGSAVE");
    Ok(Response::new(StatusResponse {
        status: "Background saving started".to_string(),
    }))
}

pub(super) async fn bg_rewrite_aof(
    svc: &EmberService,
    _request: Request<BgRewriteAofRequest>,
) -> Result<Response<StatusResponse>, Status> {
    let start = Instant::now();
    svc.broadcast(|| ShardRequest::RewriteAof).await?;
    svc.record_command(start, "BGREWRITEAOF");
    Ok(Response::new(StatusResponse {
        status: "Background append only file rewriting started".to_string(),
    }))
}

pub(super) async fn slow_log_get(
    svc: &EmberService,
    request: Request<SlowLogGetRequest>,
) -> Result<Response<SlowLogGetResponse>, Status> {
    let count = request.into_inner().count.map(|c| c as usize);
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
    _request: Request<SlowLogLenRequest>,
) -> Result<Response<IntResponse>, Status> {
    Ok(Response::new(IntResponse {
        value: svc.slow_log.len() as i64,
    }))
}

pub(super) async fn slow_log_reset(
    svc: &EmberService,
    _request: Request<SlowLogResetRequest>,
) -> Result<Response<StatusResponse>, Status> {
    svc.slow_log.reset();
    Ok(Response::new(StatusResponse {
        status: "OK".to_string(),
    }))
}

pub(super) async fn time(
    _svc: &EmberService,
    _request: Request<TimeRequest>,
) -> Result<Response<TimeResponse>, Status> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Ok(Response::new(TimeResponse {
        seconds: now.as_secs() as i64,
        microseconds: now.subsec_micros() as i64,
    }))
}

pub(super) async fn last_save(
    svc: &EmberService,
    _request: Request<LastSaveRequest>,
) -> Result<Response<IntResponse>, Status> {
    use std::sync::atomic::Ordering;
    Ok(Response::new(IntResponse {
        value: svc.ctx.last_save_timestamp.load(Ordering::Relaxed) as i64,
    }))
}

pub(super) async fn pipeline(
    svc: &EmberService,
    request: Request<Streaming<PipelineRequest>>,
) -> Result<Response<ReceiverStream<Result<PipelineResponse, Status>>>, Status> {
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
