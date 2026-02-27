//! Keyspace management command handlers (TTL, expiry, scan, type, etc.).

use bytes::Bytes;
use ember_core::{ShardRequest, ShardResponse, TtlResult};
use ember_protocol::Frame;

use super::ExecCtx;

pub(in crate::connection) async fn expire(key: String, seconds: u64, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Expire {
        key: key.clone(),
        seconds,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Bool(true)) => {
            cx.notify_write(crate::keyspace_notifications::FLAG_G, "expire", &key);
            Frame::Integer(1)
        }
        Ok(ShardResponse::Bool(false)) => Frame::Integer(0),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn expireat(
    key: String,
    timestamp: u64,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Expireat {
        key: key.clone(),
        timestamp,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Bool(true)) => {
            cx.notify_write(crate::keyspace_notifications::FLAG_G, "expireat", &key);
            Frame::Integer(1)
        }
        Ok(ShardResponse::Bool(false)) => Frame::Integer(0),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn pexpire(
    key: String,
    milliseconds: u64,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Pexpire { key, milliseconds };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Bool(b)) => Frame::Integer(i64::from(b)),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn pexpireat(
    key: String,
    timestamp_ms: u64,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Pexpireat {
        key: key.clone(),
        timestamp_ms,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Bool(true)) => {
            cx.notify_write(crate::keyspace_notifications::FLAG_G, "pexpireat", &key);
            Frame::Integer(1)
        }
        Ok(ShardResponse::Bool(false)) => Frame::Integer(0),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn ttl(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Ttl { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Ttl(TtlResult::Seconds(s))) => Frame::Integer(s as i64),
        Ok(ShardResponse::Ttl(TtlResult::NoExpiry)) => Frame::Integer(-1),
        Ok(ShardResponse::Ttl(TtlResult::NotFound)) => Frame::Integer(-2),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn pttl(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Pttl { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Ttl(TtlResult::Milliseconds(ms))) => Frame::Integer(ms as i64),
        Ok(ShardResponse::Ttl(TtlResult::NoExpiry)) => Frame::Integer(-1),
        Ok(ShardResponse::Ttl(TtlResult::NotFound)) => Frame::Integer(-2),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn persist(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Persist { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Bool(b)) => Frame::Integer(i64::from(b)),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn expiretime(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Expiretime { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn pexpiretime(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Pexpiretime { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn del(keys: Vec<String>, cx: &ExecCtx<'_>) -> Frame {
    super::multi_key_bool(cx.engine, &keys, |k| ShardRequest::Del { key: k }).await
}

pub(in crate::connection) async fn unlink(keys: Vec<String>, cx: &ExecCtx<'_>) -> Frame {
    super::multi_key_bool(cx.engine, &keys, |k| ShardRequest::Unlink { key: k }).await
}

pub(in crate::connection) async fn exists(keys: Vec<String>, cx: &ExecCtx<'_>) -> Frame {
    super::multi_key_bool(cx.engine, &keys, |k| ShardRequest::Exists { key: k }).await
}

pub(in crate::connection) async fn touch(keys: Vec<String>, cx: &ExecCtx<'_>) -> Frame {
    super::multi_key_bool(cx.engine, &keys, |k| ShardRequest::Touch { key: k }).await
}

pub(in crate::connection) async fn keys(pattern: String, cx: &ExecCtx<'_>) -> Frame {
    match cx
        .engine
        .broadcast(|| ShardRequest::Keys {
            pattern: pattern.clone(),
        })
        .await
    {
        Ok(responses) => {
            let mut all_keys = Vec::new();
            for r in responses {
                if let ShardResponse::StringArray(ks) = r {
                    all_keys.extend(ks);
                }
            }
            Frame::Array(
                all_keys
                    .into_iter()
                    .map(|k| Frame::Bulk(Bytes::from(k)))
                    .collect(),
            )
        }
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn scan(
    cursor: u64,
    pattern: Option<String>,
    count: Option<usize>,
    cx: &ExecCtx<'_>,
) -> Frame {
    // cursor encoding: (shard_id << 48) | position_within_shard
    //
    // this gives us 16 bits for shard_id (up to 65536 shards) and 48 bits
    // for position within each shard. cursor 0 always means "start fresh".
    //
    // the cursor is opaque to clients — they just pass back whatever we
    // returned last time. this lets us iterate across the sharded keyspace
    // without clients needing to know the topology.
    let shard_count = cx.engine.shard_count();
    let count = count.unwrap_or(10);

    let (shard_id, position) = if cursor == 0 {
        (0usize, 0u64)
    } else {
        let shard_id = (cursor >> 48) as usize;
        let position = cursor & 0xFFFF_FFFF_FFFF;

        // guard against invalid cursor (shard_id out of range)
        if shard_id >= shard_count {
            return Frame::Array(vec![Frame::Bulk(Bytes::from("0")), Frame::Array(vec![])]);
        }

        (shard_id, position)
    };

    // collect keys from current shard and possibly subsequent shards
    let mut all_keys = Vec::new();
    let mut current_shard = shard_id;
    let mut current_pos = position;

    while all_keys.len() < count && current_shard < shard_count {
        let req = ShardRequest::Scan {
            cursor: current_pos,
            count: count.saturating_sub(all_keys.len()),
            pattern: pattern.clone(),
        };
        match cx.engine.send_to_shard(current_shard, req).await {
            Ok(ShardResponse::Scan {
                cursor: next_pos,
                keys,
            }) => {
                all_keys.extend(keys);
                if next_pos == 0 {
                    // shard exhausted, move to next
                    current_shard += 1;
                    current_pos = 0;
                } else {
                    current_pos = next_pos;
                    break; // have more in this shard, stop here
                }
            }
            Ok(other) => {
                return Frame::Error(format!("ERR unexpected shard response: {other:?}"));
            }
            Err(e) => {
                return Frame::Error(format!("ERR {e}"));
            }
        }
    }

    // compute next cursor
    let next_cursor = if current_shard >= shard_count {
        0 // scan complete
    } else {
        ((current_shard as u64) << 48) | current_pos
    };

    // return [cursor, [keys...]]
    let cursor_str = next_cursor.to_string();
    let keys_frames: Vec<Frame> = all_keys
        .into_iter()
        .map(|k| Frame::Bulk(Bytes::from(k)))
        .collect();
    Frame::Array(vec![
        Frame::Bulk(Bytes::from(cursor_str)),
        Frame::Array(keys_frames),
    ])
}

pub(in crate::connection) async fn rename(key: String, newkey: String, cx: &ExecCtx<'_>) -> Frame {
    if !cx.engine.same_shard(&key, &newkey) {
        return Frame::Error("ERR source and destination keys must hash to the same shard".into());
    }
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Rename { key, newkey };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
        Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn copy(
    source: String,
    destination: String,
    replace: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    if !cx.engine.same_shard(&source, &destination) {
        return Frame::Error("ERR source and destination keys must hash to the same shard".into());
    }
    let idx = cx.engine.shard_for_key(&source);
    let req = ShardRequest::Copy {
        source,
        destination,
        replace,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Bool(b)) => Frame::Integer(i64::from(b)),
        Ok(ShardResponse::Err(msg)) => Frame::Error(msg),
        Ok(ShardResponse::OutOfMemory) => super::oom_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn randomkey(cx: &ExecCtx<'_>) -> Frame {
    match cx.engine.broadcast(|| ShardRequest::RandomKey).await {
        Ok(responses) => {
            let ks: Vec<String> = responses
                .into_iter()
                .flat_map(|r| match r {
                    ShardResponse::StringArray(v) => v,
                    _ => vec![],
                })
                .collect();
            if ks.is_empty() {
                Frame::Null
            } else {
                use rand::seq::IndexedRandom;
                let mut rng = rand::rng();
                match ks.choose(&mut rng) {
                    Some(k) => Frame::Bulk(Bytes::from(k.to_owned())),
                    None => Frame::Null,
                }
            }
        }
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn sort_with_store(
    key: String,
    desc: bool,
    alpha: bool,
    limit: Option<(i64, i64)>,
    dest: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    // phase 1: sort on the source shard
    let src_idx = cx.engine.shard_for_key(&key);
    let sort_req = ShardRequest::Sort {
        key,
        desc,
        alpha,
        limit,
    };
    match cx.engine.send_to_shard(src_idx, sort_req).await {
        Ok(ShardResponse::Array(items)) => {
            let count = items.len() as i64;
            // phase 2: delete dest + rpush sorted items
            let dst_idx = cx.engine.shard_for_key(&dest);
            let del_req = ShardRequest::Del { key: dest.clone() };
            let _ = cx.engine.send_to_shard(dst_idx, del_req).await;
            if !items.is_empty() {
                let rpush_req = ShardRequest::RPush {
                    key: dest,
                    values: items,
                };
                let _ = cx.engine.send_to_shard(dst_idx, rpush_req).await;
            }
            Frame::Integer(count)
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn sort_no_store(
    key: String,
    desc: bool,
    alpha: bool,
    limit: Option<(i64, i64)>,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Sort {
        key,
        desc,
        alpha,
        limit,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Array(items)) => {
            Frame::Array(items.into_iter().map(Frame::Bulk).collect())
        }
        Ok(ShardResponse::WrongType) => super::wrongtype_error(),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn type_cmd(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Type { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::TypeName(name)) => Frame::Simple(name.into()),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn object_encoding(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::ObjectEncoding { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::EncodingName(Some(name))) => Frame::Bulk(Bytes::from(name)),
        Ok(ShardResponse::EncodingName(None)) => Frame::Null,
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn object_refcount(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::Exists { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Bool(true)) => Frame::Integer(1),
        Ok(ShardResponse::Bool(false)) => Frame::Null,
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn memory_usage(key: String, cx: &ExecCtx<'_>) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::MemoryUsage { key };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Integer(-1)) => Frame::Null,
        Ok(ShardResponse::Integer(n)) => Frame::Integer(n),
        Ok(other) => Frame::Error(format!("ERR unexpected shard response: {other:?}")),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}
