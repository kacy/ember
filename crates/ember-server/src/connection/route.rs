//! Maps single-key commands to the shard request that runs them.
//!
//! The pipelined path batches these requests by shard, and the serial path
//! (transactions and the special connection modes) sends them one at a
//! time. Both go through [`route`], so a command behaves the same either
//! way. Commands that aren't one request to one shard come back as
//! [`Routing::Other`] and run through `execute`.

use std::sync::atomic::Ordering;
use std::time::Duration;

use ember_core::{Engine, ShardRequest};
use ember_protocol::{Command, Frame, SetExpire};

use crate::keyspace_notifications::{
    notify_keyspace_event, FLAG_DOLLAR, FLAG_G, FLAG_H, FLAG_L, FLAG_S, FLAG_Z,
};
use crate::pubsub::PubSubManager;
use crate::server::ServerContext;

use super::ResponseTag;

/// How a command runs.
pub(crate) enum Routing {
    /// One request to one shard.
    Shard(Route),
    /// Answered without a shard, such as an error found while routing.
    Reply(Frame),
    /// Anything else, which runs through `execute`.
    Other(Command),
}

/// A request for one shard, with how to turn its response into a reply.
pub(crate) struct Route {
    pub(crate) shard_idx: usize,
    pub(crate) request: ShardRequest,
    pub(crate) tag: ResponseTag,
    /// The keyspace event to publish when the command succeeds.
    pub(crate) notify: Option<Notify>,
}

/// A keyspace event for a write, published once the reply shows the write
/// happened.
pub(crate) struct Notify {
    flag: u32,
    event: &'static str,
    key: String,
    /// Publish only when the reply counts at least one change, as for SADD.
    if_changed: bool,
}

impl Notify {
    /// Publishes the event if `reply` shows the write happened.
    pub(crate) fn send(self, reply: &Frame, ctx: &ServerContext, pubsub: &PubSubManager) {
        let happened = match reply {
            Frame::Error(_) | Frame::Null => false,
            Frame::Integer(n) => !self.if_changed || *n > 0,
            _ => true,
        };
        let flags = ctx.keyspace_event_flags.load(Ordering::Relaxed);
        if happened && flags != 0 {
            notify_keyspace_event(flags, self.flag, self.event, &self.key, pubsub);
        }
    }
}

/// Routes `cmd`. `notify` says whether keyspace events are on, so the key
/// is only copied for an event when one could be sent.
pub(crate) fn route(cmd: Command, engine: &Engine, notify: bool) -> Routing {
    macro_rules! shard {
        ($key:expr, $req:expr, $tag:expr) => {{
            let shard_idx = engine.shard_for_key(&$key);
            Routing::Shard(Route {
                shard_idx,
                request: $req,
                tag: $tag,
                notify: None,
            })
        }};
        ($key:expr, $req:expr, $tag:expr, notify($flag:expr, $event:expr, $if_changed:expr)) => {{
            let shard_idx = engine.shard_for_key(&$key);
            let notify = notify.then(|| Notify {
                flag: $flag,
                event: $event,
                key: $key.clone(),
                if_changed: $if_changed,
            });
            Routing::Shard(Route {
                shard_idx,
                request: $req,
                tag: $tag,
                notify,
            })
        }};
    }

    match cmd {
        Command::Get { key } => {
            shard!(key, ShardRequest::Get { key }, ResponseTag::Get)
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
            shard!(
                key,
                ShardRequest::Set {
                    key,
                    value,
                    expire: duration,
                    nx,
                    xx
                },
                ResponseTag::Set,
                notify(FLAG_DOLLAR, "set", false)
            )
        }
        Command::Incr { key } => {
            shard!(key, ShardRequest::Incr { key }, ResponseTag::IntResult)
        }
        Command::Decr { key } => {
            shard!(key, ShardRequest::Decr { key }, ResponseTag::IntResult)
        }
        Command::IncrBy { key, delta } => {
            shard!(
                key,
                ShardRequest::IncrBy { key, delta },
                ResponseTag::IntResult
            )
        }
        Command::DecrBy { key, delta } => {
            shard!(
                key,
                ShardRequest::DecrBy { key, delta },
                ResponseTag::IntResult
            )
        }
        Command::IncrByFloat { key, delta } => {
            shard!(
                key,
                ShardRequest::IncrByFloat { key, delta },
                ResponseTag::FloatResult
            )
        }
        Command::Append { key, value } => {
            shard!(
                key,
                ShardRequest::Append { key, value },
                ResponseTag::LenResultOom
            )
        }
        Command::Strlen { key } => {
            shard!(key, ShardRequest::Strlen { key }, ResponseTag::LenResult)
        }
        Command::GetRange { key, start, end } => {
            shard!(
                key,
                ShardRequest::GetRange { key, start, end },
                ResponseTag::Get
            )
        }
        Command::SetRange { key, offset, value } => {
            shard!(
                key,
                ShardRequest::SetRange { key, offset, value },
                ResponseTag::LenResultOom
            )
        }
        Command::Expire { key, seconds } => {
            shard!(
                key,
                ShardRequest::Expire { key, seconds },
                ResponseTag::BoolToInt,
                notify(FLAG_G, "expire", true)
            )
        }
        Command::Ttl { key } => {
            shard!(key, ShardRequest::Ttl { key }, ResponseTag::Ttl)
        }
        Command::Persist { key } => {
            shard!(key, ShardRequest::Persist { key }, ResponseTag::BoolToInt)
        }
        Command::Pttl { key } => {
            shard!(key, ShardRequest::Pttl { key }, ResponseTag::Pttl)
        }
        Command::Pexpire { key, milliseconds } => {
            shard!(
                key,
                ShardRequest::Pexpire { key, milliseconds },
                ResponseTag::BoolToInt,
                notify(FLAG_G, "expire", true)
            )
        }
        Command::Type { key } => {
            shard!(key, ShardRequest::Type { key }, ResponseTag::TypeResult)
        }
        Command::ObjectEncoding { key } => {
            shard!(
                key,
                ShardRequest::ObjectEncoding { key },
                ResponseTag::EncodingResult
            )
        }
        Command::ObjectRefcount { key } => {
            shard!(key, ShardRequest::Exists { key }, ResponseTag::BoolToInt)
        }
        Command::Sort {
            key,
            desc,
            alpha,
            limit,
            store: None,
        } => {
            shard!(
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

        Command::LPush { key, values } => {
            shard!(
                key,
                ShardRequest::LPush { key, values },
                ResponseTag::LenResultOom,
                notify(FLAG_L, "lpush", false)
            )
        }
        Command::RPush { key, values } => {
            shard!(
                key,
                ShardRequest::RPush { key, values },
                ResponseTag::LenResultOom,
                notify(FLAG_L, "rpush", false)
            )
        }
        Command::LPop { key, count: None } => {
            shard!(key, ShardRequest::LPop { key }, ResponseTag::PopResult)
        }
        Command::LPop {
            key,
            count: Some(count),
        } => {
            shard!(
                key,
                ShardRequest::LPopCount { key, count },
                ResponseTag::ArrayResult
            )
        }
        Command::RPop { key, count: None } => {
            shard!(key, ShardRequest::RPop { key }, ResponseTag::PopResult)
        }
        Command::RPop {
            key,
            count: Some(count),
        } => {
            shard!(
                key,
                ShardRequest::RPopCount { key, count },
                ResponseTag::ArrayResult
            )
        }
        Command::LRange { key, start, stop } => {
            shard!(
                key,
                ShardRequest::LRange { key, start, stop },
                ResponseTag::ArrayResult
            )
        }
        Command::LLen { key } => {
            shard!(key, ShardRequest::LLen { key }, ResponseTag::LenResult)
        }
        Command::LIndex { key, index } => {
            shard!(
                key,
                ShardRequest::LIndex { key, index },
                ResponseTag::LIndexResult
            )
        }
        Command::LSet { key, index, value } => {
            shard!(
                key,
                ShardRequest::LSet { key, index, value },
                ResponseTag::LSetResult
            )
        }
        Command::LTrim { key, start, stop } => {
            shard!(
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
            shard!(
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
            shard!(
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
            shard!(
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

        Command::ZAdd {
            key,
            flags,
            members,
        } => {
            shard!(
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
                ResponseTag::ZAddResult,
                notify(FLAG_Z, "zadd", true)
            )
        }
        Command::ZRem { key, members } => {
            shard!(
                key,
                ShardRequest::ZRem { key, members },
                ResponseTag::ZRemResult
            )
        }
        Command::ZScore { key, member } => {
            shard!(
                key,
                ShardRequest::ZScore { key, member },
                ResponseTag::ZScoreResult
            )
        }
        Command::ZRank { key, member } => {
            shard!(
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
            shard!(
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
            shard!(
                key,
                ShardRequest::ZRevRank { key, member },
                ResponseTag::ZRankResult
            )
        }
        Command::ZCard { key } => {
            shard!(key, ShardRequest::ZCard { key }, ResponseTag::LenResult)
        }
        Command::ZRevRange {
            key,
            start,
            stop,
            with_scores,
        } => {
            shard!(
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
            shard!(
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
            shard!(
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
            shard!(
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
            shard!(
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
            shard!(
                key,
                ShardRequest::ZPopMin { key, count },
                ResponseTag::ZPopResult
            )
        }
        Command::ZPopMax { key, count } => {
            shard!(
                key,
                ShardRequest::ZPopMax { key, count },
                ResponseTag::ZPopResult
            )
        }

        Command::HSet { key, fields } => {
            shard!(
                key,
                ShardRequest::HSet { key, fields },
                ResponseTag::HSetResult,
                notify(FLAG_H, "hset", false)
            )
        }
        Command::HGet { key, field } => {
            shard!(
                key,
                ShardRequest::HGet { key, field },
                ResponseTag::HGetResult
            )
        }
        Command::HGetAll { key } => {
            shard!(
                key,
                ShardRequest::HGetAll { key },
                ResponseTag::HGetAllResult
            )
        }
        Command::HDel { key, fields } => {
            shard!(
                key,
                ShardRequest::HDel { key, fields },
                ResponseTag::HDelResult
            )
        }
        Command::HExists { key, field } => {
            shard!(
                key,
                ShardRequest::HExists { key, field },
                ResponseTag::HExistsResult
            )
        }
        Command::HLen { key } => {
            shard!(key, ShardRequest::HLen { key }, ResponseTag::LenResult)
        }
        Command::HIncrBy { key, field, delta } => {
            shard!(
                key,
                ShardRequest::HIncrBy { key, field, delta },
                ResponseTag::HIncrByResult
            )
        }
        Command::HKeys { key } => {
            shard!(
                key,
                ShardRequest::HKeys { key },
                ResponseTag::StringArrayResult
            )
        }
        Command::HVals { key } => {
            shard!(key, ShardRequest::HVals { key }, ResponseTag::HValsResult)
        }
        Command::HMGet { key, fields } => {
            shard!(
                key,
                ShardRequest::HMGet { key, fields },
                ResponseTag::HMGetResult
            )
        }

        Command::SAdd { key, members } => {
            shard!(
                key,
                ShardRequest::SAdd { key, members },
                ResponseTag::LenResultOom,
                notify(FLAG_S, "sadd", true)
            )
        }
        Command::SRem { key, members } => {
            shard!(
                key,
                ShardRequest::SRem { key, members },
                ResponseTag::LenResult
            )
        }
        Command::SMembers { key } => {
            shard!(
                key,
                ShardRequest::SMembers { key },
                ResponseTag::StringArrayResult
            )
        }
        Command::SIsMember { key, member } => {
            shard!(
                key,
                ShardRequest::SIsMember { key, member },
                ResponseTag::SIsMemberResult
            )
        }
        Command::SCard { key } => {
            shard!(key, ShardRequest::SCard { key }, ResponseTag::LenResult)
        }
        Command::SUnion { keys } => {
            let key = keys.first().cloned().unwrap_or_default();
            shard!(
                key,
                ShardRequest::SUnion { keys },
                ResponseTag::StringArrayResult
            )
        }
        Command::SInter { keys } => {
            let key = keys.first().cloned().unwrap_or_default();
            shard!(
                key,
                ShardRequest::SInter { keys },
                ResponseTag::StringArrayResult
            )
        }
        Command::SDiff { keys } => {
            let key = keys.first().cloned().unwrap_or_default();
            shard!(
                key,
                ShardRequest::SDiff { keys },
                ResponseTag::StringArrayResult
            )
        }
        Command::SUnionStore { dest, keys } => {
            shard!(
                dest,
                ShardRequest::SUnionStore { dest, keys },
                ResponseTag::SetStoreResult
            )
        }
        Command::SInterStore { dest, keys } => {
            shard!(
                dest,
                ShardRequest::SInterStore { dest, keys },
                ResponseTag::SetStoreResult
            )
        }
        Command::SDiffStore { dest, keys } => {
            shard!(
                dest,
                ShardRequest::SDiffStore { dest, keys },
                ResponseTag::SetStoreResult
            )
        }
        Command::SRandMember { key, count } => {
            let count = count.unwrap_or(1);
            shard!(
                key,
                ShardRequest::SRandMember { key, count },
                ResponseTag::StringArrayResult
            )
        }
        Command::SPop { key, count } => {
            shard!(
                key,
                ShardRequest::SPop { key, count },
                ResponseTag::StringArrayResult
            )
        }
        Command::SMisMember { key, members } => {
            shard!(
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
            shard!(
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
            shard!(
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
            shard!(
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
            shard!(
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
            shard!(
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
            shard!(
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
            shard!(
                key,
                ShardRequest::VRem { key, element },
                ResponseTag::VRemResult
            )
        }
        #[cfg(feature = "vector")]
        Command::VGet { key, element } => {
            shard!(
                key,
                ShardRequest::VGet { key, element },
                ResponseTag::VGetResult
            )
        }
        #[cfg(feature = "vector")]
        Command::VCard { key } => {
            shard!(key, ShardRequest::VCard { key }, ResponseTag::VIntResult)
        }
        #[cfg(feature = "vector")]
        Command::VDim { key } => {
            shard!(key, ShardRequest::VDim { key }, ResponseTag::VIntResult)
        }
        #[cfg(feature = "vector")]
        Command::VInfo { key } => {
            shard!(key, ShardRequest::VInfo { key }, ResponseTag::VInfoResult)
        }

        Command::Rename { key, newkey } => {
            if !engine.same_shard(&key, &newkey) {
                Routing::Reply(Frame::Error(
                    "ERR source and destination keys must hash to the same shard".into(),
                ))
            } else {
                shard!(
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
                Routing::Reply(Frame::Error(
                    "ERR source and destination keys must hash to the same shard".into(),
                ))
            } else {
                shard!(
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
                return Routing::Reply(Frame::Error("ERR protobuf support is not enabled".into()));
            };
            {
                let reg = match registry.read() {
                    Ok(r) => r,
                    Err(_) => {
                        return Routing::Reply(Frame::Error(
                            "ERR schema registry lock poisoned".into(),
                        ))
                    }
                };
                if let Err(e) = reg.validate(&type_name, &data) {
                    return Routing::Reply(Frame::Error(format!("ERR {e}")));
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
            shard!(
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
                return Routing::Reply(Frame::Error("ERR protobuf support is not enabled".into()));
            }
            shard!(
                key,
                ShardRequest::ProtoGet { key },
                ResponseTag::ProtoGetResult
            )
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoType { key } => {
            if engine.schema_registry().is_none() {
                return Routing::Reply(Frame::Error("ERR protobuf support is not enabled".into()));
            }
            shard!(
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
                return Routing::Reply(Frame::Error("ERR protobuf support is not enabled".into()));
            }
            shard!(
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
                return Routing::Reply(Frame::Error("ERR protobuf support is not enabled".into()));
            }
            shard!(
                key,
                ShardRequest::ProtoDelField { key, field_path },
                ResponseTag::ProtoDelFieldResult
            )
        }

        cmd => Routing::Other(cmd),
    }
}
