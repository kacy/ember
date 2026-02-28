//! Sorted set command handlers.

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse};
use ember_protocol::command::ScoreBound;
use ember_protocol::Frame;

use super::ExecCtx;

pub(in crate::connection) async fn zadd(
    key: String,
    flags: ember_protocol::command::ZAddFlags,
    members: Vec<(f64, String)>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZAdd {
        key: key.clone(),
        members,
        nx: flags.nx,
        xx: flags.xx,
        gt: flags.gt,
        lt: flags.lt,
        ch: flags.ch,
    };
    let frame = super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ZAddLen { count, .. } => Frame::Integer(count as i64),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await;
    if let Frame::Integer(n) = &frame {
        if *n > 0 {
            cx.notify_write(crate::keyspace_notifications::FLAG_Z, "zadd", &key);
        }
    }
    frame
}

pub(in crate::connection) async fn zrem(
    key: String,
    members: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZRem { key, members };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ZRemLen { count, .. } => Frame::Integer(count as i64),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
}

pub(in crate::connection) async fn zscore(key: String, member: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZScore { key, member };
    super::route_to_shard(cx, idx, req, super::resp_score).await
}

pub(in crate::connection) async fn zrank(key: String, member: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZRank { key, member };
    super::route_to_shard(cx, idx, req, super::resp_rank).await
}

pub(in crate::connection) async fn zrevrank(
    key: String,
    member: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZRevRank { key, member };
    super::route_to_shard(cx, idx, req, super::resp_rank).await
}

pub(in crate::connection) async fn zrange(
    key: String,
    start: i64,
    stop: i64,
    with_scores: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZRange {
        key,
        start,
        stop,
        with_scores,
    };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ScoredArray(items) => super::scored_to_frame(items, with_scores),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
}

pub(in crate::connection) async fn zrevrange(
    key: String,
    start: i64,
    stop: i64,
    with_scores: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZRevRange {
        key,
        start,
        stop,
        with_scores,
    };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ScoredArray(items) => super::scored_to_frame(items, with_scores),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
}

pub(in crate::connection) async fn zcard(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZCard { key };
    super::route_to_shard(cx, idx, req, super::resp_len).await
}

pub(in crate::connection) async fn zcount(
    key: String,
    min: ScoreBound,
    max: ScoreBound,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZCount { key, min, max };
    super::route_to_shard(cx, idx, req, super::resp_len).await
}

pub(in crate::connection) async fn zincrby(
    key: String,
    increment: f64,
    member: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZIncrBy {
        key,
        increment,
        member,
    };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ZIncrByResult { new_score, .. } => {
            Frame::Bulk(Bytes::from(format!("{new_score}")))
        }
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
}

pub(in crate::connection) async fn zrangebyscore(
    key: String,
    min: ScoreBound,
    max: ScoreBound,
    with_scores: bool,
    offset: usize,
    count: Option<usize>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZRangeByScore {
        key,
        min,
        max,
        offset,
        count,
    };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ScoredArray(items) => super::scored_to_frame(items, with_scores),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
}

pub(in crate::connection) async fn zrevrangebyscore(
    key: String,
    min: ScoreBound,
    max: ScoreBound,
    with_scores: bool,
    offset: usize,
    count: Option<usize>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZRevRangeByScore {
        key,
        min,
        max,
        offset,
        count,
    };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ScoredArray(items) => super::scored_to_frame(items, with_scores),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
}

pub(in crate::connection) async fn zpopmin(key: String, count: usize, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZPopMin { key, count };
    super::route_to_shard(cx, idx, req, super::resp_zpop).await
}

pub(in crate::connection) async fn zpopmax(key: String, count: usize, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZPopMax { key, count };
    super::route_to_shard(cx, idx, req, super::resp_zpop).await
}

pub(in crate::connection) async fn zmpop(
    keys: Vec<String>,
    min: bool,
    count: usize,
    cx: &ExecCtx<'_>,
) -> Frame {
    for key in &keys {
        let idx = cx.engine.shard_for_key(key);
        let req = ShardRequest::ZmpopSingle {
            key: key.clone(),
            min,
            count,
        };
        match cx.engine.send_to_shard(idx, req).await {
            Ok(ShardResponse::ZPopResult(members)) if !members.is_empty() => {
                let pairs: Vec<Frame> = members
                    .into_iter()
                    .flat_map(|(m, s)| {
                        vec![
                            Frame::Bulk(Bytes::from(m)),
                            Frame::Bulk(Bytes::from(format!("{s}"))),
                        ]
                    })
                    .collect();
                return Frame::Array(vec![
                    Frame::Bulk(Bytes::from(key.clone())),
                    Frame::Array(pairs),
                ]);
            }
            Ok(ShardResponse::ZPopResult(_)) | Ok(ShardResponse::Value(None)) => continue,
            Ok(ShardResponse::WrongType) => return super::wrongtype_error(),
            Ok(other) => return Frame::Error(format!("ERR unexpected shard response: {other:?}")),
            Err(e) => return Frame::Error(format!("ERR {e}")),
        }
    }
    Frame::Null
}

pub(in crate::connection) async fn zdiff(
    keys: Vec<String>,
    with_scores: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    let key = keys.first().cloned().unwrap_or_default();
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZDiff { keys };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ScoredArray(items) => super::scored_to_frame(items, with_scores),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
}

pub(in crate::connection) async fn zinter(
    keys: Vec<String>,
    with_scores: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    let key = keys.first().cloned().unwrap_or_default();
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZInter { keys };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ScoredArray(items) => super::scored_to_frame(items, with_scores),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
}

pub(in crate::connection) async fn zunion(
    keys: Vec<String>,
    with_scores: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    let key = keys.first().cloned().unwrap_or_default();
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZUnion { keys };
    super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ScoredArray(items) => super::scored_to_frame(items, with_scores),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await
}

pub(in crate::connection) async fn zdiffstore(
    dest: String,
    keys: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&dest);
    let req = ShardRequest::ZDiffStore {
        dest: dest.clone(),
        keys,
    };
    let frame = super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ZStoreResult { count, .. } => Frame::Integer(count as i64),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await;
    if matches!(frame, Frame::Integer(_)) {
        cx.notify_write(crate::keyspace_notifications::FLAG_Z, "zdiffstore", &dest);
    }
    frame
}

pub(in crate::connection) async fn zinterstore(
    dest: String,
    keys: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&dest);
    let req = ShardRequest::ZInterStore {
        dest: dest.clone(),
        keys,
    };
    let frame = super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ZStoreResult { count, .. } => Frame::Integer(count as i64),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await;
    if matches!(frame, Frame::Integer(_)) {
        cx.notify_write(crate::keyspace_notifications::FLAG_Z, "zinterstore", &dest);
    }
    frame
}

pub(in crate::connection) async fn zunionstore(
    dest: String,
    keys: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&dest);
    let req = ShardRequest::ZUnionStore {
        dest: dest.clone(),
        keys,
    };
    let frame = super::route_to_shard(cx, idx, req, |resp| match resp {
        ShardResponse::ZStoreResult { count, .. } => Frame::Integer(count as i64),
        other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
    })
    .await;
    if matches!(frame, Frame::Integer(_)) {
        cx.notify_write(crate::keyspace_notifications::FLAG_Z, "zunionstore", &dest);
    }
    frame
}

pub(in crate::connection) async fn zrandmember(
    key: String,
    count: Option<i64>,
    with_scores: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZRandMember {
        key,
        count,
        with_scores,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::ZRandMemberResult(pairs)) => {
            if count.is_none() {
                // no count: return a single bulk string (or nil if empty)
                match pairs.into_iter().next() {
                    Some((member, _)) => Frame::Bulk(Bytes::from(member)),
                    None => Frame::Null,
                }
            } else {
                // with count: return array, interleaved with scores if requested
                let frames: Vec<Frame> = pairs
                    .into_iter()
                    .flat_map(|(m, s)| {
                        let mut items = vec![Frame::Bulk(Bytes::from(m))];
                        if let Some(score) = s {
                            items.push(Frame::Bulk(Bytes::from(score.to_string())));
                        }
                        items
                    })
                    .collect();
                Frame::Array(frames)
            }
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn zscan(
    key: String,
    cursor: u64,
    pattern: Option<String>,
    count: Option<usize>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let count = count.unwrap_or(10);
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ZScan {
        key,
        cursor,
        count,
        pattern,
    };
    super::resolve_collection_scan(cx.engine.send_to_shard(idx, req).await)
}
