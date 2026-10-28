//! Sorted set command handlers.

use std::collections::HashMap;

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse};
use ember_protocol::Frame;

use super::{ExecCtx, SetOp};

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

/// ZUNION, ZINTER and ZDIFF. The keys may live on different shards.
pub(in crate::connection) async fn zset_op(
    op: SetOp,
    keys: Vec<String>,
    with_scores: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    match combine(op, keys, cx).await {
        Ok(items) => super::scored_to_frame(items, with_scores),
        Err(frame) => frame,
    }
}

/// ZUNIONSTORE, ZINTERSTORE and ZDIFFSTORE. When `dest` and every key
/// share a shard, the shard computes and stores the result in one step.
pub(in crate::connection) async fn zset_op_store(
    op: SetOp,
    dest: String,
    keys: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let dest_idx = cx.engine.shard_for_key(&dest);
    let frame = if keys.iter().all(|k| cx.engine.shard_for_key(k) == dest_idx) {
        let dest = dest.clone();
        let req = match op {
            SetOp::Union => ShardRequest::ZUnionStore { dest, keys },
            SetOp::Inter => ShardRequest::ZInterStore { dest, keys },
            SetOp::Diff => ShardRequest::ZDiffStore { dest, keys },
        };
        super::route_to_shard(cx, dest_idx, req, |resp| match resp {
            ShardResponse::ZStoreResult { count, .. } => Frame::Integer(count as i64),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        })
        .await
    } else {
        match combine(op, keys, cx).await {
            Ok(items) => {
                let count = items.len();
                let add = (!items.is_empty()).then(|| ShardRequest::ZAdd {
                    key: dest.clone(),
                    members: items.into_iter().map(|(m, s)| (s, m)).collect(),
                    nx: false,
                    xx: false,
                    gt: false,
                    lt: false,
                    ch: false,
                });
                super::store_result(cx, &dest, add, count).await
            }
            Err(frame) => frame,
        }
    };
    if matches!(frame, Frame::Integer(_)) {
        let event = match op {
            SetOp::Union => "zunionstore",
            SetOp::Inter => "zinterstore",
            SetOp::Diff => "zdiffstore",
        };
        cx.notify_write(crate::keyspace_notifications::FLAG_Z, event, &dest);
    }
    frame
}

/// Computes `op` over sorted sets that may live on different shards. Each
/// shard combines its own keys (summing scores for union and intersection),
/// then the per-shard results are merged here the same way. Results are
/// ordered by score, then member.
async fn combine(
    op: SetOp,
    keys: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Result<Vec<(String, f64)>, Frame> {
    let replies = super::per_shard(cx, op, keys, |op, keys| match op {
        SetOp::Union => ShardRequest::ZUnion { keys },
        SetOp::Inter => ShardRequest::ZInter { keys },
        SetOp::Diff => ShardRequest::ZDiff { keys },
    })
    .await?;
    let single = replies.len() == 1;
    let mut parts = replies.into_iter().map(|resp| match resp {
        ShardResponse::ScoredArray(items) => Ok(items),
        other => Err(Frame::Error(format!(
            "ERR unexpected shard response: {other:?}"
        ))),
    });
    let first = parts.next().transpose()?.unwrap_or_default();
    if single {
        return Ok(first);
    }
    let mut acc: HashMap<String, f64> = first.into_iter().collect();
    for part in parts {
        let part = part?;
        match op {
            SetOp::Union => {
                for (member, score) in part {
                    *acc.entry(member).or_insert(0.0) += score;
                }
            }
            SetOp::Inter => {
                let part: HashMap<String, f64> = part.into_iter().collect();
                acc.retain(|member, score| match part.get(member) {
                    Some(s) => {
                        *score += s;
                        true
                    }
                    None => false,
                });
            }
            SetOp::Diff => {
                for (member, _) in &part {
                    acc.remove(member);
                }
            }
        }
    }
    let mut items: Vec<(String, f64)> = acc.into_iter().collect();
    items.sort_by(|(am, a), (bm, b)| a.total_cmp(b).then_with(|| am.cmp(bm)));
    Ok(items)
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
