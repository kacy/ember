//! Set command handlers.

use ember_core::{ShardRequest, ShardResponse};
use ember_protocol::Frame;

use super::ExecCtx;

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
