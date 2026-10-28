//! Set command handlers.

use std::collections::HashSet;

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse};
use ember_protocol::Frame;

use super::{ExecCtx, SetOp};
use crate::keyspace_notifications::FLAG_S;

/// SUNION, SINTER and SDIFF. The keys may live on different shards.
pub(in crate::connection) async fn set_op(op: SetOp, keys: Vec<String>, cx: &ExecCtx<'_>) -> Frame {
    match combine(op, keys, cx).await {
        Ok(members) => Frame::Array(
            members
                .into_iter()
                .map(|m| Frame::Bulk(Bytes::from(m)))
                .collect(),
        ),
        Err(frame) => frame,
    }
}

/// SUNIONSTORE, SINTERSTORE and SDIFFSTORE. When `dest` and every key
/// share a shard, the shard computes and stores the result in one step.
pub(in crate::connection) async fn set_op_store(
    op: SetOp,
    dest: String,
    keys: Vec<String>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let dest_idx = cx.engine.shard_for_key(&dest);
    let frame = if keys.iter().all(|k| cx.engine.shard_for_key(k) == dest_idx) {
        let dest = dest.clone();
        let req = match op {
            SetOp::Union => ShardRequest::SUnionStore { dest, keys },
            SetOp::Inter => ShardRequest::SInterStore { dest, keys },
            SetOp::Diff => ShardRequest::SDiffStore { dest, keys },
        };
        super::route_to_shard(cx, dest_idx, req, |resp| match resp {
            ShardResponse::SetStoreResult { count, .. } => Frame::Integer(count as i64),
            other => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        })
        .await
    } else {
        match combine(op, keys, cx).await {
            Ok(members) => {
                let count = members.len();
                let add = (!members.is_empty()).then(|| ShardRequest::SAdd {
                    key: dest.clone(),
                    members,
                });
                super::store_result(cx, &dest, add, count).await
            }
            Err(frame) => frame,
        }
    };
    if matches!(frame, Frame::Integer(_)) {
        let event = match op {
            SetOp::Union => "sunionstore",
            SetOp::Inter => "sinterstore",
            SetOp::Diff => "sdiffstore",
        };
        cx.notify_write(FLAG_S, event, &dest);
    }
    frame
}

/// Computes `op` over sets that may live on different shards. Each shard
/// combines its own keys, then the per-shard results are merged here.
async fn combine(op: SetOp, keys: Vec<String>, cx: &ExecCtx<'_>) -> Result<Vec<String>, Frame> {
    let replies = super::per_shard(cx, op, keys, |op, keys| match op {
        SetOp::Union => ShardRequest::SUnion { keys },
        SetOp::Inter => ShardRequest::SInter { keys },
        SetOp::Diff => ShardRequest::SDiff { keys },
    })
    .await?;
    let mut parts = replies.into_iter().map(|resp| match resp {
        ShardResponse::StringArray(members) => Ok(members),
        other => Err(Frame::Error(format!(
            "ERR unexpected shard response: {other:?}"
        ))),
    });
    let first = parts.next().transpose()?.unwrap_or_default();
    let mut acc: HashSet<String> = first.into_iter().collect();
    for part in parts {
        let part = part?;
        match op {
            SetOp::Union => acc.extend(part),
            SetOp::Inter => {
                let part: HashSet<String> = part.into_iter().collect();
                acc.retain(|m| part.contains(m));
            }
            SetOp::Diff => {
                for m in &part {
                    acc.remove(m);
                }
            }
        }
    }
    Ok(acc.into_iter().collect())
}

pub(in crate::connection) async fn smove(
    source: String,
    destination: String,
    member: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    let src_idx = cx.engine.shard_for_key(&source);
    let dst_idx = cx.engine.shard_for_key(&destination);

    if src_idx == dst_idx {
        // same shard — single atomic operation
        let req = ShardRequest::SMove {
            source,
            destination,
            member,
        };
        match cx.engine.send_to_shard(src_idx, req).await {
            Ok(ShardResponse::Bool(moved)) => Frame::Integer(if moved { 1 } else { 0 }),
            Ok(ShardResponse::WrongType) => super::wrongtype_error(),
            Ok(ShardResponse::OutOfMemory) => super::oom_error(),
            Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
            Err(e) => Frame::Error(format!("ERR {e}")),
        }
    } else {
        // cross-shard: remove from source, then add to destination
        let rem_req = ShardRequest::SRem {
            key: source,
            members: vec![member.clone()],
        };
        let removed = match cx.engine.send_to_shard(src_idx, rem_req).await {
            Ok(ShardResponse::Len(n)) => n,
            Ok(ShardResponse::WrongType) => return super::wrongtype_error(),
            Ok(other) => return Frame::Error(format!("ERR unexpected shard response: {other:?}")),
            Err(e) => return Frame::Error(format!("ERR {e}")),
        };

        if removed == 0 {
            return Frame::Integer(0);
        }

        let add_req = ShardRequest::SAdd {
            key: destination,
            members: vec![member],
        };
        match cx.engine.send_to_shard(dst_idx, add_req).await {
            Ok(ShardResponse::Len(_)) => Frame::Integer(1),
            Ok(ShardResponse::WrongType) => super::wrongtype_error(),
            Ok(ShardResponse::OutOfMemory) => super::oom_error(),
            Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
            Err(e) => Frame::Error(format!("ERR {e}")),
        }
    }
}

pub(in crate::connection) async fn sintercard(
    keys: Vec<String>,
    limit: usize,
    cx: &ExecCtx<'_>,
) -> Frame {
    // Fetch members for each key from its owning shard, then intersect.
    // This handles keys spread across shards without cross-shard calls
    // inside the keyspace layer.
    let mut sets: Vec<std::collections::HashSet<String>> = Vec::with_capacity(keys.len());
    for key in &keys {
        let idx = cx.engine.shard_for_key(key);
        let req = ShardRequest::SMembers { key: key.clone() };
        match cx.engine.send_to_shard(idx, req).await {
            Ok(ShardResponse::StringArray(members)) => {
                // an empty set (including missing key) short-circuits to 0
                if members.is_empty() {
                    return Frame::Integer(0);
                }
                sets.push(members.into_iter().collect());
            }
            Ok(ShardResponse::WrongType) => return super::wrongtype_error(),
            Ok(other) => return Frame::Error(format!("ERR unexpected shard response: {other:?}")),
            Err(e) => return Frame::Error(format!("ERR {e}")),
        }
    }

    if sets.is_empty() {
        return Frame::Integer(0);
    }

    // Start with the smallest set to minimise comparisons.
    sets.sort_unstable_by_key(|s| s.len());
    let Some((first, rest)) = sets.split_first() else {
        return Frame::Integer(0);
    };
    let mut count = 0usize;
    'outer: for member in first {
        for other in rest {
            if !other.contains(member.as_str()) {
                continue 'outer;
            }
        }
        count += 1;
        if limit > 0 && count >= limit {
            break;
        }
    }

    Frame::Integer(count as i64)
}
