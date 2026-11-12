//! Cluster command handlers.

use std::io;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use ember_core::{ShardRequest, ShardResponse};
use ember_protocol::{parse_frame, Frame, MigrateAuth};

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

/// What MIGRATE was asked to do with one key.
pub(in crate::connection) struct MigrateRequest {
    pub host: String,
    pub port: u16,
    pub key: String,
    pub timeout_ms: u64,
    /// Keep the local key.
    pub copy: bool,
    /// Overwrite the key on the target.
    pub replace: bool,
    pub auth: Option<MigrateAuth>,
}

pub(in crate::connection) async fn migrate(req: MigrateRequest, cx: &ExecCtx<'_>) -> Frame {
    // other clients must not change the key between the dump and the local
    // delete, or their write is lost. hold it until the move is complete.
    let cluster = cx.ctx.cluster.as_ref();
    if let Some(c) = cluster {
        if !c.begin_key_transfer(req.key.as_bytes()).await {
            return Frame::Error("ERR the key is already being migrated".into());
        }
    }
    let reply = transfer_key(&req, cx).await;
    if let Some(c) = cluster {
        c.end_key_transfer(req.key.as_bytes()).await;
    }
    reply
}

/// Copies the key to the target with RESTORE, then, unless COPY was given,
/// deletes it locally, so later commands for it are sent there with ASK.
async fn transfer_key(req: &MigrateRequest, cx: &ExecCtx<'_>) -> Frame {
    let key = &req.key;
    let idx = cx.engine.shard_for_key(key);
    let dump_req = ShardRequest::DumpKey { key: key.clone() };
    let (data, ttl_ms) = match cx.engine.send_to_shard(idx, dump_req).await {
        Ok(ShardResponse::KeyDump { data, ttl_ms }) => (data, ttl_ms),
        Ok(ShardResponse::Value(None)) => return Frame::Simple("NOKEY".into()),
        Ok(_) => return Frame::Error("ERR internal error".into()),
        Err(e) => return Frame::Error(format!("ERR {e}")),
    };

    // the target is importing the slot, so it serves the key only after
    // ASKING. a target with a password needs AUTH first, which MIGRATE
    // takes as an option, as in Redis.
    let bulk = |s: &str| Frame::Bulk(Bytes::copy_from_slice(s.as_bytes()));
    let mut commands = Vec::new();
    if let Some(auth) = &req.auth {
        let mut parts = vec![bulk("AUTH")];
        parts.extend(auth.username.as_deref().map(bulk));
        parts.push(bulk(&auth.password));
        commands.push(Frame::Array(parts));
    }
    commands.push(Frame::Array(vec![bulk("ASKING")]));
    let mut restore = vec![
        bulk("RESTORE"),
        bulk(key),
        bulk(&ttl_ms.max(0).to_string()),
        Frame::Bulk(Bytes::from(data)),
    ];
    if req.replace {
        restore.push(bulk("REPLACE"));
    }
    commands.push(Frame::Array(restore));

    let addr = format!("{}:{}", req.host, req.port);
    let timeout = Duration::from_millis(req.timeout_ms.max(1000));
    let replies = match tokio::time::timeout(timeout, send_commands(&addr, &commands)).await {
        Ok(Ok(replies)) => replies,
        Ok(Err(e)) => return Frame::Error(format!("IOERR error talking to the target: {e}")),
        Err(_) => return Frame::Error("IOERR timeout talking to the target".into()),
    };
    if let Some(Frame::Error(e)) = replies.iter().find(|r| matches!(r, Frame::Error(_))) {
        return Frame::Error(format!("ERR target error: {e}"));
    }

    if !req.copy {
        // the key is still marked in flight, so no write lands between the
        // RESTORE and this delete
        let _ = cx
            .engine
            .send_to_shard(idx, ShardRequest::Del { key: key.clone() })
            .await;
        if let Some(c) = &cx.ctx.cluster {
            c.mark_key_migrated(ember_cluster::key_slot(key.as_bytes()))
                .await;
        }
    }
    Frame::Simple("OK".into())
}

/// Sends `commands` to `addr` in one write and reads one reply for each.
async fn send_commands(addr: &str, commands: &[Frame]) -> io::Result<Vec<Frame>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    let mut buf = BytesMut::new();
    for command in commands {
        command.serialize(&mut buf);
    }
    stream.write_all(&buf).await?;

    let mut replies = Vec::with_capacity(commands.len());
    let mut read_buf = BytesMut::with_capacity(256);
    while replies.len() < commands.len() {
        match parse_frame(&read_buf) {
            Ok(Some((frame, used))) => {
                let _ = read_buf.split_to(used);
                replies.push(frame);
            }
            Ok(None) => {
                if stream.read_buf(&mut read_buf).await? == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed by target",
                    ));
                }
            }
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        }
    }
    Ok(replies)
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
