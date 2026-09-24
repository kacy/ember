//! Cluster command handlers.

use std::io;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use ember_core::{ShardRequest, ShardResponse};
use ember_protocol::{parse_frame, Frame};

use super::ExecCtx;

pub(in crate::connection) fn cluster_keyslot(key: String) -> Frame {
    let slot = ember_cluster::key_slot(key.as_bytes());
    Frame::Integer(slot as i64)
}

pub(in crate::connection) async fn cluster_info(cx: &ExecCtx<'_>) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_info().await,
        None => Frame::Bulk(Bytes::from("cluster_enabled:0\r\n")),
    }
}

pub(in crate::connection) async fn cluster_nodes(cx: &ExecCtx<'_>) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_nodes().await,
        None => Frame::Bulk(Bytes::from("")),
    }
}

pub(in crate::connection) async fn cluster_slots(cx: &ExecCtx<'_>) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_slots().await,
        None => Frame::Array(vec![]),
    }
}

pub(in crate::connection) fn cluster_myid(cx: &ExecCtx<'_>) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_myid(),
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn cluster_meet(ip: String, port: u16, cx: &ExecCtx<'_>) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_meet(&ip, port).await,
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn cluster_addslots(slots: Vec<u16>, cx: &ExecCtx<'_>) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_addslots(&slots).await,
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn cluster_addslots_range(
    ranges: Vec<(u16, u16)>,
    cx: &ExecCtx<'_>,
) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => {
            let slots: Vec<u16> = ranges.iter().flat_map(|&(s, e)| s..=e).collect();
            c.cluster_addslots(&slots).await
        }
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn cluster_delslots(slots: Vec<u16>, cx: &ExecCtx<'_>) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_delslots(&slots).await,
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn cluster_forget(node_id: String, cx: &ExecCtx<'_>) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_forget(&node_id).await,
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn cluster_setslot_importing(
    slot: u16,
    node_id: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_setslot_importing(slot, &node_id).await,
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn cluster_setslot_migrating(
    slot: u16,
    node_id: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_setslot_migrating(slot, &node_id).await,
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn cluster_setslot_node(
    slot: u16,
    node_id: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_setslot_node(slot, &node_id).await,
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn cluster_setslot_stable(slot: u16, cx: &ExecCtx<'_>) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_setslot_stable(slot).await,
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn cluster_count_keys_in_slot(
    slot: u16,
    cx: &ExecCtx<'_>,
) -> Frame {
    match cx
        .engine
        .broadcast(|| ShardRequest::CountKeysInSlot { slot })
        .await
    {
        Ok(responses) => {
            let total: usize = responses
                .iter()
                .map(|r| match r {
                    ShardResponse::KeyCount(n) => *n,
                    _ => 0,
                })
                .sum();
            Frame::Integer(total as i64)
        }
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn cluster_get_keys_in_slot(
    slot: u16,
    count: u32,
    cx: &ExecCtx<'_>,
) -> Frame {
    let count = count as usize;
    match cx
        .engine
        .broadcast(|| ShardRequest::GetKeysInSlot { slot, count })
        .await
    {
        Ok(responses) => {
            let mut all_keys = Vec::new();
            for r in responses {
                if let ShardResponse::StringArray(keys) = r {
                    all_keys.extend(keys);
                }
            }
            all_keys.truncate(count);
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

pub(in crate::connection) async fn cluster_replicate(node_id: String, cx: &ExecCtx<'_>) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_replicate(&node_id).await,
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn cluster_failover(
    force: bool,
    takeover: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    match &cx.ctx.cluster {
        Some(c) => c.cluster_failover(force, takeover).await,
        None => Frame::Error("ERR This instance has cluster support disabled".into()),
    }
}

pub(in crate::connection) async fn migrate(
    host: String,
    port: u16,
    key: String,
    timeout_ms: u64,
    replace: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    // other clients must not change the key between the dump and the local
    // delete, or their write is lost. hold it until the move is complete.
    let cluster = cx.ctx.cluster.as_ref();
    if let Some(c) = cluster {
        if !c.begin_key_transfer(key.as_bytes()).await {
            return Frame::Error("ERR the key is already being migrated".into());
        }
    }
    let reply = transfer_key(host, port, &key, timeout_ms, replace, cx).await;
    if let Some(c) = cluster {
        c.end_key_transfer(key.as_bytes()).await;
    }
    reply
}

/// Copies `key` to the target with RESTORE, then deletes it locally and
/// marks it migrated so later commands for it are sent there with ASK.
async fn transfer_key(
    host: String,
    port: u16,
    key: &str,
    timeout_ms: u64,
    replace: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // dump the key from the local shard
    let idx = cx.engine.shard_for_key(key);
    let dump_req = ShardRequest::DumpKey {
        key: key.to_owned(),
    };
    let dump_resp = match cx.engine.send_to_shard(idx, dump_req).await {
        Ok(r) => r,
        Err(e) => return Frame::Error(format!("ERR {e}")),
    };

    let (data, ttl_ms) = match dump_resp {
        ShardResponse::KeyDump { data, ttl_ms } => (data, ttl_ms),
        ShardResponse::Value(None) => {
            return Frame::Error("ERR no such key".into());
        }
        _ => return Frame::Error("ERR internal error".into()),
    };

    // send RESTORE to the target node
    let ttl_arg = if ttl_ms < 0 { 0u64 } else { ttl_ms as u64 };
    let timeout = Duration::from_millis(timeout_ms.max(1000));
    let addr = format!("{host}:{port}");

    let result = tokio::time::timeout(timeout, async {
        let mut stream = tokio::net::TcpStream::connect(&addr).await?;

        // build RESTORE command as RESP3 array
        let mut parts = vec![
            Frame::Bulk(Bytes::from("RESTORE")),
            Frame::Bulk(Bytes::from(key.to_owned())),
            Frame::Bulk(Bytes::from(ttl_arg.to_string())),
            Frame::Bulk(Bytes::from(data)),
        ];
        if replace {
            parts.push(Frame::Bulk(Bytes::from("REPLACE")));
        }
        let cmd_frame = Frame::Array(parts);

        let mut buf = BytesMut::new();
        cmd_frame.serialize(&mut buf);
        stream.write_all(&buf).await?;

        // read response
        let mut read_buf = BytesMut::with_capacity(256);
        loop {
            let n = stream.read_buf(&mut read_buf).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed by target",
                ));
            }
            match parse_frame(&read_buf) {
                Ok(Some((frame, _))) => return Ok(frame),
                Ok(None) => {} // need more data
                Err(e) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
                }
            }
        }
    })
    .await;

    match result {
        Ok(Ok(Frame::Simple(_))) => {
            // the target has it: send later commands there, then drop the
            // local copy. the key is still marked in flight, so no write can
            // land in between.
            if let Some(c) = &cx.ctx.cluster {
                let slot = ember_cluster::key_slot(key.as_bytes());
                c.mark_key_migrated(slot, key.as_bytes()).await;
            }
            let del_req = ShardRequest::Del {
                key: key.to_owned(),
            };
            let _ = cx.engine.send_to_shard(idx, del_req).await;
            Frame::Simple("OK".into())
        }
        Ok(Ok(Frame::Error(e))) => Frame::Error(format!("ERR target error: {e}")),
        Ok(Ok(_)) => Frame::Error("ERR unexpected response from target".into()),
        Ok(Err(e)) => Frame::Error(format!("ERR {e}")),
        Err(_) => Frame::Error("ERR timeout connecting to target".into()),
    }
}

pub(in crate::connection) async fn restore(
    key: String,
    ttl_ms: u64,
    data: Bytes,
    replace: bool,
    cx: &ExecCtx<'_>,
) -> Frame {
    let idx = cx.engine.shard_for_key(&key);
    let req = ShardRequest::RestoreKey {
        key,
        ttl_ms,
        data,
        replace,
    };
    match cx.engine.send_to_shard(idx, req).await {
        Ok(ShardResponse::Ok) => Frame::Simple("OK".into()),
        Ok(ShardResponse::Err(e)) => Frame::Error(e),
        Ok(_) => Frame::Error("ERR internal error".into()),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}
