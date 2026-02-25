//! Command preparation and dispatch for pipelined execution.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::acl;
use crate::connection_common::{frame_to_monitor_args, validate_command_sizes, MonitorEvent};
use crate::pubsub::PubSubManager;
use crate::server::ServerContext;
use crate::slowlog::SlowLog;
use bytes::Bytes;
use ember_core::{Engine, ShardRequest};
use ember_protocol::{Command, Frame, SetExpire};

use super::{PendingResponse, PreparedDispatch, ResponseTag};

/// Converts a raw frame into a command and executes it.
///
/// When metrics or slowlog are enabled, brackets the command with
/// `Instant::now()` to measure latency. Skips timing entirely when
/// neither feature needs it.
#[allow(clippy::too_many_arguments)]
pub(super) async fn process(
    frame: Frame,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
    asking: &mut bool,
    peer_addr: &str,
    client_id: u64,
    acl_user: &Option<Arc<acl::AclUser>>,
    current_username: &str,
) -> Frame {
    // broadcast to MONITOR subscribers
    if ctx.monitor_tx.receiver_count() > 0 {
        let args = frame_to_monitor_args(&frame);
        if !args.is_empty() {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();
            let _ = ctx.monitor_tx.send(MonitorEvent {
                timestamp: ts,
                client_addr: peer_addr.to_owned(),
                args,
            });
        }
    }

    match Command::from_frame(frame) {
        Ok(cmd) => {
            // ACL WHOAMI needs per-connection state
            if matches!(cmd, Command::AclWhoAmI) {
                return Frame::Bulk(Bytes::from(current_username.to_string()));
            }

            // permission check — fast path: skip when unrestricted
            if let Some(ref user) = acl_user {
                if !user.allcommands || !user.allkeys {
                    if let Some(err) =
                        acl::check_permission(user, &cmd, cmd.command_name(), cmd.acl_categories())
                    {
                        return err;
                    }
                }
            }

            // handle ASKING: set the flag and return OK immediately
            if matches!(cmd, Command::Asking) {
                *asking = true;
                return Frame::Simple("OK".into());
            }

            // consume the asking flag for this command
            let was_asking = std::mem::take(asking);

            let cmd_name = cmd.command_name();
            let needs_timing = ctx.metrics_enabled || slow_log.is_enabled();
            let start = if needs_timing {
                Some(Instant::now())
            } else {
                None
            };

            let response =
                super::execute::execute(cmd, engine, ctx, slow_log, pubsub, was_asking, client_id)
                    .await;
            ctx.commands_processed.fetch_add(1, Ordering::Relaxed);

            if let Some(start) = start {
                let elapsed = start.elapsed();
                slow_log.maybe_record(elapsed, cmd_name);
                if ctx.metrics_enabled {
                    let is_error = matches!(&response, Frame::Error(_));
                    crate::metrics::record_command(cmd_name, elapsed, is_error);
                }
            }

            response
        }
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

/// Prepares a single frame for batch dispatch without sending to a shard.
///
/// Does all the work of `dispatch_command` — parsing, validation, cluster
/// checks, MONITOR broadcast — but instead of sending to the shard channel,
/// returns a `PreparedDispatch::Routed` with the shard index and request.
/// The caller groups routed commands by shard for batch dispatch.
///
/// Commands that don't target a single shard (broadcast, multi-key, cluster,
/// pub/sub, errors) are resolved immediately and returned as `Immediate`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn prepare_command(
    frame: Frame,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
    asking: &mut bool,
    peer_addr: &str,
    client_id: u64,
    acl_user: &Option<Arc<acl::AclUser>>,
    current_username: &str,
) -> PreparedDispatch {
    // broadcast to MONITOR subscribers (one atomic load when nobody's listening)
    if ctx.monitor_tx.receiver_count() > 0 {
        let args = frame_to_monitor_args(&frame);
        if !args.is_empty() {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();
            let _ = ctx.monitor_tx.send(MonitorEvent {
                timestamp: ts,
                client_addr: peer_addr.to_owned(),
                args,
            });
        }
    }

    let cmd = match Command::from_frame(frame) {
        Ok(cmd) => cmd,
        Err(e) => {
            return PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Error(format!(
                "ERR {e}"
            ))))
        }
    };

    // reject oversized keys/values before any further processing
    if let Some(err) =
        validate_command_sizes(&cmd, ctx.limits.max_key_len, ctx.limits.max_value_len)
    {
        return PreparedDispatch::Immediate(PendingResponse::Immediate(err));
    }

    // ACL WHOAMI needs per-connection state
    if matches!(cmd, Command::AclWhoAmI) {
        return PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Bulk(Bytes::from(
            current_username.to_string(),
        ))));
    }

    // permission check — fast path: skip when unrestricted
    if let Some(ref user) = acl_user {
        if !user.allcommands || !user.allkeys {
            if let Some(err) =
                acl::check_permission(user, &cmd, cmd.command_name(), cmd.acl_categories())
            {
                return PreparedDispatch::Immediate(PendingResponse::Immediate(err));
            }
        }
    }

    // handle ASKING: set the flag and return OK immediately
    if matches!(cmd, Command::Asking) {
        *asking = true;
        return PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Simple("OK".into())));
    }

    // consume the asking flag for this command
    let was_asking = std::mem::take(asking);

    let cmd_name = cmd.command_name();
    let needs_timing = ctx.metrics_enabled || slow_log.is_enabled();
    let start = if needs_timing {
        Some(Instant::now())
    } else {
        None
    };

    // cluster slot validation (migration-aware when cluster is enabled)
    if let Some(redirect) = cluster_slot_check(ctx, &cmd, was_asking).await {
        return PreparedDispatch::Immediate(PendingResponse::Immediate(redirect));
    }

    // macro that returns Routed instead of sending to the channel
    macro_rules! route {
        ($key:expr, $req:expr, $tag:expr) => {{
            let idx = engine.shard_for_key(&$key);
            PreparedDispatch::Routed {
                shard_idx: idx,
                request: $req,
                tag: $tag,
                start,
                cmd_name,
            }
        }};
    }

    match cmd {
        // -- no shard needed --
        Command::Ping(None) => {
            PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Simple("PONG".into())))
        }
        Command::Ping(Some(msg)) => {
            PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Bulk(msg)))
        }
        Command::Echo(msg) => {
            PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Bulk(msg)))
        }

        // -- client commands (connection-scoped, no shard needed) --
        Command::ClientId => PreparedDispatch::Immediate(PendingResponse::Immediate(
            Frame::Integer(client_id as i64),
        )),
        Command::ClientGetName => {
            let name = ctx
                .clients
                .lock()
                .ok()
                .and_then(|map| map.get(&client_id).and_then(|c| c.name.clone()));
            let frame = match name {
                Some(n) => Frame::Bulk(Bytes::from(n)),
                None => Frame::Null,
            };
            PreparedDispatch::Immediate(PendingResponse::Immediate(frame))
        }
        Command::ClientSetName { name } => {
            if let Ok(mut map) = ctx.clients.lock() {
                if let Some(info) = map.get_mut(&client_id) {
                    info.name = if name.is_empty() { None } else { Some(name) };
                }
            }
            PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Simple("OK".into())))
        }
        Command::ClientList => {
            let output = crate::server::format_client_list(ctx);
            PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Bulk(Bytes::from(
                output,
            ))))
        }

        // -- single-key string commands --
        Command::Get { key } => {
            route!(key, ShardRequest::Get { key }, ResponseTag::Get)
        }
        Command::Set {
            key,
            value,
            expire,
            nx,
            xx,
        } => {
            let duration = expire.map(|e| {
                use std::time::{SystemTime, UNIX_EPOCH};
                match e {
                    SetExpire::Ex(secs) => Duration::from_secs(secs),
                    SetExpire::Px(millis) => Duration::from_millis(millis),
                    SetExpire::ExAt(ts) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        Duration::from_secs(ts.saturating_sub(now))
                    }
                    SetExpire::PxAt(ts_ms) => {
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        Duration::from_millis(ts_ms.saturating_sub(now_ms))
                    }
                }
            });
            route!(
                key,
                ShardRequest::Set {
                    key,
                    value,
                    expire: duration,
                    nx,
                    xx
                },
                ResponseTag::Set
            )
        }
        Command::Incr { key } => {
            route!(key, ShardRequest::Incr { key }, ResponseTag::IntResult)
        }
        Command::Decr { key } => {
            route!(key, ShardRequest::Decr { key }, ResponseTag::IntResult)
        }
        Command::IncrBy { key, delta } => {
            route!(
                key,
                ShardRequest::IncrBy { key, delta },
                ResponseTag::IntResult
            )
        }
        Command::DecrBy { key, delta } => {
            route!(
                key,
                ShardRequest::DecrBy { key, delta },
                ResponseTag::IntResult
            )
        }
        Command::IncrByFloat { key, delta } => {
            route!(
                key,
                ShardRequest::IncrByFloat { key, delta },
                ResponseTag::FloatResult
            )
        }
        Command::Append { key, value } => {
            route!(
                key,
                ShardRequest::Append { key, value },
                ResponseTag::LenResultOom
            )
        }
        Command::Strlen { key } => {
            route!(key, ShardRequest::Strlen { key }, ResponseTag::LenResult)
        }
        Command::GetRange { key, start, end } => {
            route!(
                key,
                ShardRequest::GetRange { key, start, end },
                ResponseTag::Get
            )
        }
        Command::SetRange { key, offset, value } => {
            route!(
                key,
                ShardRequest::SetRange { key, offset, value },
                ResponseTag::LenResultOom
            )
        }
        Command::Expire { key, seconds } => {
            route!(
                key,
                ShardRequest::Expire { key, seconds },
                ResponseTag::BoolToInt
            )
        }
        Command::Ttl { key } => {
            route!(key, ShardRequest::Ttl { key }, ResponseTag::Ttl)
        }
        Command::Persist { key } => {
            route!(key, ShardRequest::Persist { key }, ResponseTag::BoolToInt)
        }
        Command::Pttl { key } => {
            route!(key, ShardRequest::Pttl { key }, ResponseTag::Pttl)
        }
        Command::Pexpire { key, milliseconds } => {
            route!(
                key,
                ShardRequest::Pexpire { key, milliseconds },
                ResponseTag::BoolToInt
            )
        }
        Command::Type { key } => {
            route!(key, ShardRequest::Type { key }, ResponseTag::TypeResult)
        }
        Command::ObjectEncoding { key } => {
            route!(
                key,
                ShardRequest::ObjectEncoding { key },
                ResponseTag::EncodingResult
            )
        }
        Command::ObjectRefcount { key } => {
            route!(key, ShardRequest::Exists { key }, ResponseTag::BoolToInt)
        }
        Command::Sort {
            key,
            desc,
            alpha,
            limit,
            store: None,
        } => {
            route!(
                key,
                ShardRequest::Sort {
                    key,
                    desc,
                    alpha,
                    limit,
                },
                ResponseTag::SortResult
            )
        }

        // -- list commands --
        Command::LPush { key, values } => {
            route!(
                key,
                ShardRequest::LPush { key, values },
                ResponseTag::LenResultOom
            )
        }
        Command::RPush { key, values } => {
            route!(
                key,
                ShardRequest::RPush { key, values },
                ResponseTag::LenResultOom
            )
        }
        Command::LPop { key } => {
            route!(key, ShardRequest::LPop { key }, ResponseTag::PopResult)
        }
        Command::RPop { key } => {
            route!(key, ShardRequest::RPop { key }, ResponseTag::PopResult)
        }
        Command::LRange { key, start, stop } => {
            route!(
                key,
                ShardRequest::LRange { key, start, stop },
                ResponseTag::ArrayResult
            )
        }
        Command::LLen { key } => {
            route!(key, ShardRequest::LLen { key }, ResponseTag::LenResult)
        }
        Command::LIndex { key, index } => {
            route!(
                key,
                ShardRequest::LIndex { key, index },
                ResponseTag::LIndexResult
            )
        }
        Command::LSet { key, index, value } => {
            route!(
                key,
                ShardRequest::LSet { key, index, value },
                ResponseTag::LSetResult
            )
        }
        Command::LTrim { key, start, stop } => {
            route!(
                key,
                ShardRequest::LTrim { key, start, stop },
                ResponseTag::LTrimResult
            )
        }
        Command::LInsert {
            key,
            before,
            pivot,
            value,
        } => {
            route!(
                key,
                ShardRequest::LInsert {
                    key,
                    before,
                    pivot,
                    value
                },
                ResponseTag::LInsertResult
            )
        }
        Command::LRem { key, count, value } => {
            route!(
                key,
                ShardRequest::LRem { key, count, value },
                ResponseTag::LRemResult
            )
        }
        Command::LPos {
            key,
            element,
            rank,
            count,
            maxlen,
        } => {
            // When COUNT is not specified, fetch at most 1 match from the shard
            // and return as a single value (not array). When COUNT is specified,
            // pass the raw value (0 = all) and return an array.
            let shard_count = count.unwrap_or(1);
            route!(
                key,
                ShardRequest::LPos {
                    key,
                    element,
                    rank,
                    count: shard_count,
                    maxlen
                },
                ResponseTag::LPosResult { count }
            )
        }

        // blocking list ops are handled in the main loop before dispatch;
        // if they reach here (e.g. inside MULTI), return an error.
        Command::BLPop { .. } | Command::BRPop { .. } => {
            PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Error(
                "ERR blocking commands are not allowed inside transactions".into(),
            )))
        }

        // -- sorted set commands --
        Command::ZAdd {
            key,
            flags,
            members,
        } => {
            route!(
                key,
                ShardRequest::ZAdd {
                    key,
                    members,
                    nx: flags.nx,
                    xx: flags.xx,
                    gt: flags.gt,
                    lt: flags.lt,
                    ch: flags.ch
                },
                ResponseTag::ZAddResult
            )
        }
        Command::ZRem { key, members } => {
            route!(
                key,
                ShardRequest::ZRem { key, members },
                ResponseTag::ZRemResult
            )
        }
        Command::ZScore { key, member } => {
            route!(
                key,
                ShardRequest::ZScore { key, member },
                ResponseTag::ZScoreResult
            )
        }
        Command::ZRank { key, member } => {
            route!(
                key,
                ShardRequest::ZRank { key, member },
                ResponseTag::ZRankResult
            )
        }
        Command::ZRange {
            key,
            start,
            stop,
            with_scores,
        } => {
            route!(
                key,
                ShardRequest::ZRange {
                    key,
                    start,
                    stop,
                    with_scores
                },
                ResponseTag::ZRangeResult { with_scores }
            )
        }
        Command::ZRevRank { key, member } => {
            route!(
                key,
                ShardRequest::ZRevRank { key, member },
                ResponseTag::ZRankResult
            )
        }
        Command::ZCard { key } => {
            route!(key, ShardRequest::ZCard { key }, ResponseTag::LenResult)
        }
        Command::ZRevRange {
            key,
            start,
            stop,
            with_scores,
        } => {
            route!(
                key,
                ShardRequest::ZRevRange {
                    key,
                    start,
                    stop,
                    with_scores
                },
                ResponseTag::ZRangeResult { with_scores }
            )
        }
        Command::ZCount { key, min, max } => {
            route!(
                key,
                ShardRequest::ZCount { key, min, max },
                ResponseTag::LenResult
            )
        }
        Command::ZIncrBy {
            key,
            increment,
            member,
        } => {
            route!(
                key,
                ShardRequest::ZIncrBy {
                    key,
                    increment,
                    member
                },
                ResponseTag::ZIncrByResult
            )
        }
        Command::ZRangeByScore {
            key,
            min,
            max,
            with_scores,
            offset,
            count,
        } => {
            route!(
                key,
                ShardRequest::ZRangeByScore {
                    key,
                    min,
                    max,
                    offset,
                    count
                },
                ResponseTag::ZRangeResult { with_scores }
            )
        }
        Command::ZRevRangeByScore {
            key,
            min,
            max,
            with_scores,
            offset,
            count,
        } => {
            route!(
                key,
                ShardRequest::ZRevRangeByScore {
                    key,
                    min,
                    max,
                    offset,
                    count
                },
                ResponseTag::ZRangeResult { with_scores }
            )
        }
        Command::ZPopMin { key, count } => {
            route!(
                key,
                ShardRequest::ZPopMin { key, count },
                ResponseTag::ZPopResult
            )
        }
        Command::ZPopMax { key, count } => {
            route!(
                key,
                ShardRequest::ZPopMax { key, count },
                ResponseTag::ZPopResult
            )
        }

        // -- hash commands --
        Command::HSet { key, fields } => {
            route!(
                key,
                ShardRequest::HSet { key, fields },
                ResponseTag::HSetResult
            )
        }
        Command::HGet { key, field } => {
            route!(
                key,
                ShardRequest::HGet { key, field },
                ResponseTag::HGetResult
            )
        }
        Command::HGetAll { key } => {
            route!(
                key,
                ShardRequest::HGetAll { key },
                ResponseTag::HGetAllResult
            )
        }
        Command::HDel { key, fields } => {
            route!(
                key,
                ShardRequest::HDel { key, fields },
                ResponseTag::HDelResult
            )
        }
        Command::HExists { key, field } => {
            route!(
                key,
                ShardRequest::HExists { key, field },
                ResponseTag::HExistsResult
            )
        }
        Command::HLen { key } => {
            route!(key, ShardRequest::HLen { key }, ResponseTag::LenResult)
        }
        Command::HIncrBy { key, field, delta } => {
            route!(
                key,
                ShardRequest::HIncrBy { key, field, delta },
                ResponseTag::HIncrByResult
            )
        }
        Command::HKeys { key } => {
            route!(
                key,
                ShardRequest::HKeys { key },
                ResponseTag::StringArrayResult
            )
        }
        Command::HVals { key } => {
            route!(key, ShardRequest::HVals { key }, ResponseTag::HValsResult)
        }
        Command::HMGet { key, fields } => {
            route!(
                key,
                ShardRequest::HMGet { key, fields },
                ResponseTag::HMGetResult
            )
        }

        // -- set commands --
        Command::SAdd { key, members } => {
            route!(
                key,
                ShardRequest::SAdd { key, members },
                ResponseTag::LenResultOom
            )
        }
        Command::SRem { key, members } => {
            route!(
                key,
                ShardRequest::SRem { key, members },
                ResponseTag::LenResult
            )
        }
        Command::SMembers { key } => {
            route!(
                key,
                ShardRequest::SMembers { key },
                ResponseTag::StringArrayResult
            )
        }
        Command::SIsMember { key, member } => {
            route!(
                key,
                ShardRequest::SIsMember { key, member },
                ResponseTag::SIsMemberResult
            )
        }
        Command::SCard { key } => {
            route!(key, ShardRequest::SCard { key }, ResponseTag::LenResult)
        }
        Command::SUnion { keys } => {
            let key = keys.first().cloned().unwrap_or_default();
            route!(
                key,
                ShardRequest::SUnion { keys },
                ResponseTag::StringArrayResult
            )
        }
        Command::SInter { keys } => {
            let key = keys.first().cloned().unwrap_or_default();
            route!(
                key,
                ShardRequest::SInter { keys },
                ResponseTag::StringArrayResult
            )
        }
        Command::SDiff { keys } => {
            let key = keys.first().cloned().unwrap_or_default();
            route!(
                key,
                ShardRequest::SDiff { keys },
                ResponseTag::StringArrayResult
            )
        }
        Command::SUnionStore { dest, keys } => {
            route!(
                dest,
                ShardRequest::SUnionStore { dest, keys },
                ResponseTag::SetStoreResult
            )
        }
        Command::SInterStore { dest, keys } => {
            route!(
                dest,
                ShardRequest::SInterStore { dest, keys },
                ResponseTag::SetStoreResult
            )
        }
        Command::SDiffStore { dest, keys } => {
            route!(
                dest,
                ShardRequest::SDiffStore { dest, keys },
                ResponseTag::SetStoreResult
            )
        }
        Command::SRandMember { key, count } => {
            let count = count.unwrap_or(1);
            route!(
                key,
                ShardRequest::SRandMember { key, count },
                ResponseTag::StringArrayResult
            )
        }
        Command::SPop { key, count } => {
            route!(
                key,
                ShardRequest::SPop { key, count },
                ResponseTag::StringArrayResult
            )
        }
        Command::SMisMember { key, members } => {
            route!(
                key,
                ShardRequest::SMisMember { key, members },
                ResponseTag::SMisMemberResult
            )
        }
        Command::SScan {
            key,
            cursor,
            pattern,
            count,
        } => {
            let count = count.unwrap_or(10);
            route!(
                key,
                ShardRequest::SScan {
                    key,
                    cursor,
                    count,
                    pattern
                },
                ResponseTag::CollectionScanResult
            )
        }
        Command::HScan {
            key,
            cursor,
            pattern,
            count,
        } => {
            let count = count.unwrap_or(10);
            route!(
                key,
                ShardRequest::HScan {
                    key,
                    cursor,
                    count,
                    pattern
                },
                ResponseTag::CollectionScanResult
            )
        }
        Command::ZScan {
            key,
            cursor,
            pattern,
            count,
        } => {
            let count = count.unwrap_or(10);
            route!(
                key,
                ShardRequest::ZScan {
                    key,
                    cursor,
                    count,
                    pattern
                },
                ResponseTag::CollectionScanResult
            )
        }

        // -- vector commands --
        #[cfg(feature = "vector")]
        Command::VAdd {
            key,
            element,
            vector,
            metric,
            quantization,
            connectivity,
            expansion_add,
        } => {
            route!(
                key,
                ShardRequest::VAdd {
                    key,
                    element,
                    vector,
                    metric,
                    quantization,
                    connectivity,
                    expansion_add
                },
                ResponseTag::VAddResult
            )
        }
        #[cfg(feature = "vector")]
        Command::VAddBatch {
            key,
            entries,
            dim,
            metric,
            quantization,
            connectivity,
            expansion_add,
        } => {
            route!(
                key,
                ShardRequest::VAddBatch {
                    key,
                    entries,
                    dim,
                    metric,
                    quantization,
                    connectivity,
                    expansion_add
                },
                ResponseTag::VAddBatchResult
            )
        }
        #[cfg(feature = "vector")]
        Command::VSim {
            key,
            query,
            count,
            ef_search,
            with_scores,
        } => {
            route!(
                key,
                ShardRequest::VSim {
                    key,
                    query,
                    count,
                    ef_search
                },
                ResponseTag::VSimResult { with_scores }
            )
        }
        #[cfg(feature = "vector")]
        Command::VRem { key, element } => {
            route!(
                key,
                ShardRequest::VRem { key, element },
                ResponseTag::VRemResult
            )
        }
        #[cfg(feature = "vector")]
        Command::VGet { key, element } => {
            route!(
                key,
                ShardRequest::VGet { key, element },
                ResponseTag::VGetResult
            )
        }
        #[cfg(feature = "vector")]
        Command::VCard { key } => {
            route!(key, ShardRequest::VCard { key }, ResponseTag::VIntResult)
        }
        #[cfg(feature = "vector")]
        Command::VDim { key } => {
            route!(key, ShardRequest::VDim { key }, ResponseTag::VIntResult)
        }
        #[cfg(feature = "vector")]
        Command::VInfo { key } => {
            route!(key, ShardRequest::VInfo { key }, ResponseTag::VInfoResult)
        }

        // -- rename (needs same-shard validation) --
        Command::Rename { key, newkey } => {
            if !engine.same_shard(&key, &newkey) {
                PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Error(
                    "ERR source and destination keys must hash to the same shard".into(),
                )))
            } else {
                route!(
                    key,
                    ShardRequest::Rename { key, newkey },
                    ResponseTag::RenameResult
                )
            }
        }
        Command::Copy {
            source,
            destination,
            replace,
        } => {
            if !engine.same_shard(&source, &destination) {
                PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Error(
                    "ERR source and destination keys must hash to the same shard".into(),
                )))
            } else {
                route!(
                    source,
                    ShardRequest::Copy {
                        source,
                        destination,
                        replace,
                    },
                    ResponseTag::CopyResult
                )
            }
        }

        // -- proto commands that are single-key dispatches --
        #[cfg(feature = "protobuf")]
        Command::ProtoSet {
            key,
            type_name,
            data,
            expire,
            nx,
            xx,
        } => {
            let Some(registry) = engine.schema_registry() else {
                return PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Error(
                    "ERR protobuf support is not enabled".into(),
                )));
            };
            {
                let reg = match registry.read() {
                    Ok(r) => r,
                    Err(_) => {
                        return PreparedDispatch::Immediate(PendingResponse::Immediate(
                            Frame::Error("ERR schema registry lock poisoned".into()),
                        ))
                    }
                };
                if let Err(e) = reg.validate(&type_name, &data) {
                    return PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Error(
                        format!("ERR {e}"),
                    )));
                }
            }
            let duration = expire.map(|e| {
                use std::time::{SystemTime, UNIX_EPOCH};
                match e {
                    SetExpire::Ex(secs) => Duration::from_secs(secs),
                    SetExpire::Px(millis) => Duration::from_millis(millis),
                    SetExpire::ExAt(ts) => {
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        Duration::from_secs(ts.saturating_sub(now))
                    }
                    SetExpire::PxAt(ts_ms) => {
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        Duration::from_millis(ts_ms.saturating_sub(now_ms))
                    }
                }
            });
            route!(
                key,
                ShardRequest::ProtoSet {
                    key,
                    type_name,
                    data,
                    expire: duration,
                    nx,
                    xx
                },
                ResponseTag::ProtoSetResult
            )
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoGet { key } => {
            if engine.schema_registry().is_none() {
                return PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Error(
                    "ERR protobuf support is not enabled".into(),
                )));
            }
            route!(
                key,
                ShardRequest::ProtoGet { key },
                ResponseTag::ProtoGetResult
            )
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoType { key } => {
            if engine.schema_registry().is_none() {
                return PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Error(
                    "ERR protobuf support is not enabled".into(),
                )));
            }
            route!(
                key,
                ShardRequest::ProtoType { key },
                ResponseTag::ProtoTypeResult
            )
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoSetField {
            key,
            field_path,
            value,
        } => {
            if engine.schema_registry().is_none() {
                return PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Error(
                    "ERR protobuf support is not enabled".into(),
                )));
            }
            route!(
                key,
                ShardRequest::ProtoSetField {
                    key,
                    field_path,
                    value
                },
                ResponseTag::ProtoSetFieldResult
            )
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoDelField { key, field_path } => {
            if engine.schema_registry().is_none() {
                return PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Error(
                    "ERR protobuf support is not enabled".into(),
                )));
            }
            route!(
                key,
                ShardRequest::ProtoDelField { key, field_path },
                ResponseTag::ProtoDelFieldResult
            )
        }

        // -- everything else falls back to the full execute() path --
        cmd => {
            let response =
                super::execute::execute(cmd, engine, ctx, slow_log, pubsub, was_asking, client_id)
                    .await;
            PreparedDispatch::Immediate(PendingResponse::Immediate(response))
        }
    }
}

/// Validates cluster slot ownership for the given command.
///
/// Returns `None` if the command should proceed (not in cluster mode,
/// or the local node owns the slot). Returns `Some(Frame)` with a MOVED,
/// ASK, CLUSTERDOWN, or CROSSSLOT error if execution should be rejected.
///
/// The `asking` flag is set when the client sent ASKING before this command,
/// allowing access to importing slots during migration.
pub(super) async fn cluster_slot_check(
    ctx: &ServerContext,
    cmd: &Command,
    asking: bool,
) -> Option<Frame> {
    let cluster = ctx.cluster.as_ref()?;

    // Replicas serve all reads locally — skip slot routing.
    // Write rejection is handled separately in execute() before this call.
    if cluster.is_replica().await {
        return None;
    }

    match cmd {
        // single-key commands — check slot ownership for the key
        Command::Get { ref key }
        | Command::Set { ref key, .. }
        | Command::Expire { ref key, .. }
        | Command::Ttl { ref key }
        | Command::Incr { ref key }
        | Command::Decr { ref key }
        | Command::IncrBy { ref key, .. }
        | Command::DecrBy { ref key, .. }
        | Command::Append { ref key, .. }
        | Command::Strlen { ref key }
        | Command::GetRange { ref key, .. }
        | Command::SetRange { ref key, .. }
        | Command::IncrByFloat { ref key, .. }
        | Command::Persist { ref key }
        | Command::Pttl { ref key }
        | Command::Pexpire { ref key, .. }
        | Command::Type { ref key }
        | Command::LPush { ref key, .. }
        | Command::RPush { ref key, .. }
        | Command::LPop { ref key }
        | Command::RPop { ref key }
        | Command::LRange { ref key, .. }
        | Command::LLen { ref key }
        | Command::LIndex { ref key, .. }
        | Command::LSet { ref key, .. }
        | Command::LTrim { ref key, .. }
        | Command::LInsert { ref key, .. }
        | Command::LRem { ref key, .. }
        | Command::LPos { ref key, .. }
        | Command::ZAdd { ref key, .. }
        | Command::ZRem { ref key, .. }
        | Command::ZScore { ref key, .. }
        | Command::ZRank { ref key, .. }
        | Command::ZRevRank { ref key, .. }
        | Command::ZRange { ref key, .. }
        | Command::ZRevRange { ref key, .. }
        | Command::ZCard { ref key }
        | Command::ZCount { ref key, .. }
        | Command::ZIncrBy { ref key, .. }
        | Command::ZRangeByScore { ref key, .. }
        | Command::ZRevRangeByScore { ref key, .. }
        | Command::ZPopMin { ref key, .. }
        | Command::ZPopMax { ref key, .. }
        | Command::HSet { ref key, .. }
        | Command::HGet { ref key, .. }
        | Command::HGetAll { ref key }
        | Command::HDel { ref key, .. }
        | Command::HExists { ref key, .. }
        | Command::HLen { ref key }
        | Command::HIncrBy { ref key, .. }
        | Command::HKeys { ref key }
        | Command::HVals { ref key }
        | Command::HMGet { ref key, .. }
        | Command::SAdd { ref key, .. }
        | Command::SRem { ref key, .. }
        | Command::SMembers { ref key }
        | Command::SIsMember { ref key, .. }
        | Command::SCard { ref key }
        | Command::SScan { ref key, .. }
        | Command::SRandMember { ref key, .. }
        | Command::SPop { ref key, .. }
        | Command::SMisMember { ref key, .. }
        | Command::HScan { ref key, .. }
        | Command::ZScan { ref key, .. }
        | Command::ProtoSet { ref key, .. }
        | Command::ProtoGet { ref key }
        | Command::ProtoType { ref key }
        | Command::ProtoGetField { ref key, .. }
        | Command::ProtoSetField { ref key, .. }
        | Command::ProtoDelField { ref key, .. }
        | Command::VAdd { ref key, .. }
        | Command::VAddBatch { ref key, .. }
        | Command::VSim { ref key, .. }
        | Command::VRem { ref key, .. }
        | Command::VGet { ref key, .. }
        | Command::VCard { ref key }
        | Command::VDim { ref key }
        | Command::VInfo { ref key }
        | Command::Sort {
            ref key,
            store: None,
            ..
        } => {
            cluster
                .check_slot_with_migration(key.as_bytes(), asking)
                .await
        }
        // SORT with STORE: crossslot check on source + destination
        Command::Sort {
            ref key,
            store: Some(ref dest),
            ..
        } => {
            let pair = [key.as_str(), dest.as_str()];
            if let Err(err) = cluster.check_crossslot(&pair) {
                return Some(err);
            }
            cluster
                .check_slot_with_migration(key.as_bytes(), asking)
                .await
        }

        // multi-key commands — crossslot validation + slot ownership
        Command::Del { ref keys }
        | Command::Unlink { ref keys }
        | Command::Exists { ref keys }
        | Command::Touch { ref keys }
        | Command::MGet { ref keys }
        | Command::BLPop { ref keys, .. }
        | Command::BRPop { ref keys, .. }
        | Command::SUnion { ref keys }
        | Command::SInter { ref keys }
        | Command::SDiff { ref keys } => {
            if let Err(err) = cluster.check_crossslot(keys) {
                return Some(err);
            }
            if let Some(first) = keys.first() {
                return cluster
                    .check_slot_with_migration(first.as_bytes(), asking)
                    .await;
            }
            None
        }

        // set store: crossslot check on dest + all source keys
        Command::SUnionStore { ref dest, ref keys }
        | Command::SInterStore { ref dest, ref keys }
        | Command::SDiffStore { ref dest, ref keys } => {
            let mut all_keys: Vec<&str> = vec![dest];
            all_keys.extend(keys.iter().map(|s| s.as_str()));
            if let Err(err) = cluster.check_crossslot(&all_keys) {
                return Some(err);
            }
            cluster
                .check_slot_with_migration(dest.as_bytes(), asking)
                .await
        }

        // rename: crossslot check on both keys
        Command::Rename {
            ref key,
            ref newkey,
        } => {
            let pair = [key.as_str(), newkey.as_str()];
            if let Err(err) = cluster.check_crossslot(&pair) {
                return Some(err);
            }
            cluster
                .check_slot_with_migration(key.as_bytes(), asking)
                .await
        }

        // mset: extract keys from pairs for crossslot check
        Command::MSet { ref pairs } => {
            let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
            if let Err(err) = cluster.check_crossslot(&keys) {
                return Some(err);
            }
            if let Some(first) = keys.first() {
                return cluster
                    .check_slot_with_migration(first.as_bytes(), asking)
                    .await;
            }
            None
        }

        // everything else (PING, ECHO, INFO, DBSIZE, cluster commands,
        // pubsub, AUTH, etc.) doesn't need slot routing
        _ => None,
    }
}
