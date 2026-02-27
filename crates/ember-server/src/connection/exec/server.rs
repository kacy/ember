//! Server management command handlers (INFO, CONFIG, BGSAVE, FLUSHDB, etc.).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use ember_core::{KeyspaceStats, ShardRequest, ShardResponse};
use ember_protocol::Frame;

use crate::connection_common::{get_rss_bytes, human_bytes};
use crate::server::ServerContext;

use super::ExecCtx;

pub(in crate::connection) fn dbsize_from_responses(responses: Vec<ShardResponse>) -> Frame {
    let total: usize = responses
        .iter()
        .map(|r| match r {
            ShardResponse::KeyCount(n) => *n,
            _ => 0,
        })
        .sum();
    Frame::Integer(total as i64)
}

pub(in crate::connection) async fn dbsize(cx: &ExecCtx<'_>) -> Frame {
    match cx.engine.broadcast(|| ShardRequest::DbSize).await {
        Ok(responses) => dbsize_from_responses(responses),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn info(cx: &ExecCtx<'_>, section: Option<&str>) -> Frame {
    render_info(cx.engine, cx.ctx, section).await
}

pub(in crate::connection) async fn config_get(pattern: String, cx: &ExecCtx<'_>) -> Frame {
    let pairs = cx.ctx.config.get_matching(&pattern);
    let mut frames = Vec::with_capacity(pairs.len() * 2);
    for (key, value) in pairs {
        frames.push(Frame::Bulk(Bytes::from(key)));
        frames.push(Frame::Bulk(Bytes::from(value)));
    }
    Frame::Array(frames)
}

pub(in crate::connection) async fn config_set(
    param: String,
    value: String,
    cx: &ExecCtx<'_>,
) -> Frame {
    if let Err(e) = cx.ctx.config.set(&param, &value) {
        return Frame::Error(e);
    }
    // apply dynamic updates for known parameters
    let key = param.to_ascii_lowercase();
    if key == "slowlog-log-slower-than" {
        if let Ok(us) = value.parse::<i64>() {
            cx.slow_log.update_threshold(us);
        }
    } else if key == "slowlog-max-len" {
        if let Ok(len) = value.parse::<usize>() {
            cx.slow_log.update_max_len(len);
        }
    } else if key == "maxmemory" || key == "maxmemory-policy" {
        let limit = cx.ctx.config.memory_limit();
        let policy = cx.ctx.config.eviction_policy();
        // broadcast is fallible but config is already stored — log and continue
        let _ = cx
            .engine
            .broadcast(move || ShardRequest::UpdateMemoryConfig {
                max_memory: limit,
                eviction_policy: policy,
            })
            .await;
        // keep the INFO-visible limit in sync
        cx.ctx.max_memory_limit.store(
            limit.unwrap_or(0) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    } else if key == "notify-keyspace-events" {
        let flags = crate::keyspace_notifications::parse_keyspace_event_flags(&value);
        cx.ctx
            .keyspace_event_flags
            .store(flags, std::sync::atomic::Ordering::Relaxed);
    }
    Frame::Simple("OK".into())
}

pub(in crate::connection) async fn config_rewrite(cx: &ExecCtx<'_>) -> Frame {
    match &cx.ctx.config_path {
        Some(path) => match cx.ctx.config.rewrite(path) {
            Ok(()) => Frame::Simple("OK".into()),
            Err(e) => Frame::Error(e),
        },
        None => Frame::Error("ERR The server is running without a config file".into()),
    }
}

pub(in crate::connection) async fn bgsave(cx: &ExecCtx<'_>) -> Frame {
    match cx.engine.broadcast(|| ShardRequest::Snapshot).await {
        Ok(_) => {
            use std::time::{SystemTime, UNIX_EPOCH};
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            cx.ctx
                .last_save_timestamp
                .store(ts, std::sync::atomic::Ordering::Relaxed);
            Frame::Simple("Background saving started".into())
        }
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn bgrewriteaof(cx: &ExecCtx<'_>) -> Frame {
    match cx.engine.broadcast(|| ShardRequest::RewriteAof).await {
        Ok(_) => Frame::Simple("Background append only file rewriting started".into()),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) fn time() -> Frame {
    use std::time::{SystemTime, UNIX_EPOCH};
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Frame::Array(vec![
        Frame::Bulk(Bytes::from(dur.as_secs().to_string())),
        Frame::Bulk(Bytes::from(dur.subsec_micros().to_string())),
    ])
}

pub(in crate::connection) fn lastsave(cx: &ExecCtx<'_>) -> Frame {
    let ts = cx
        .ctx
        .last_save_timestamp
        .load(std::sync::atomic::Ordering::Relaxed);
    Frame::Integer(ts as i64)
}

pub(in crate::connection) async fn role(cx: &ExecCtx<'_>) -> Frame {
    if let Some(ref cluster) = cx.ctx.cluster {
        use ember_cluster::NodeRole;
        let info = cluster.replication_info().await;
        match info.role {
            NodeRole::Primary => Frame::Array(vec![
                Frame::Bulk(Bytes::from("master")),
                Frame::Integer(0),
                Frame::Array(vec![]),
            ]),
            NodeRole::Replica => {
                let (host, port) = match info.primary_addr {
                    Some(addr) => (addr.ip().to_string(), addr.port() as i64),
                    None => (String::new(), 0),
                };
                Frame::Array(vec![
                    Frame::Bulk(Bytes::from("slave")),
                    Frame::Bulk(Bytes::from(host)),
                    Frame::Integer(port),
                    Frame::Bulk(Bytes::from("connected")),
                    Frame::Integer(0),
                ])
            }
        }
    } else {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from("master")),
            Frame::Integer(0),
            Frame::Array(vec![]),
        ])
    }
}

pub(in crate::connection) async fn flushdb(async_mode: bool, cx: &ExecCtx<'_>) -> Frame {
    let req = if async_mode {
        || ShardRequest::FlushDbAsync
    } else {
        || ShardRequest::FlushDb
    };
    match cx.engine.broadcast(req).await {
        Ok(_) => Frame::Simple("OK".into()),
        Err(e) => Frame::Error(format!("ERR {e}")),
    }
}

pub(in crate::connection) async fn flushall(async_mode: bool, cx: &ExecCtx<'_>) -> Frame {
    // Ember is single-database, so FLUSHALL is identical to FLUSHDB.
    flushdb(async_mode, cx).await
}

pub(in crate::connection) fn slowlog_get(count: Option<usize>, cx: &ExecCtx<'_>) -> Frame {
    let entries = cx.slow_log.get(count);
    let frames: Vec<Frame> = entries
        .into_iter()
        .map(|e| {
            let ts = e
                .timestamp
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Frame::Array(vec![
                Frame::Integer(e.id.min(i64::MAX as u64) as i64),
                Frame::Integer(ts.min(i64::MAX as u64) as i64),
                Frame::Integer(e.duration.as_micros().min(i64::MAX as u128) as i64),
                Frame::Array(vec![Frame::Bulk(Bytes::from(e.command))]),
            ])
        })
        .collect();
    Frame::Array(frames)
}

pub(in crate::connection) fn slowlog_len(cx: &ExecCtx<'_>) -> Frame {
    Frame::Integer(cx.slow_log.len() as i64)
}

pub(in crate::connection) fn slowlog_reset(cx: &ExecCtx<'_>) -> Frame {
    cx.slow_log.reset();
    Frame::Simple("OK".into())
}

/// Implements the WAIT command: blocks until `needed` replicas have
/// acknowledged all writes at or before the current primary offset,
/// or until `timeout_ms` milliseconds elapse.
///
/// Returns the count of replicas that acknowledged in time as a
/// RESP integer. When there are no replicas or no writes, returns
/// immediately without sleeping.
pub(in crate::connection) async fn handle_wait(
    ctx: &Arc<ServerContext>,
    numreplicas: u64,
    timeout_ms: u64,
) -> Frame {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    let needed = numreplicas as usize;
    let tracker = &ctx.replica_tracker;

    // fast path: no replicas connected
    if tracker.connected_count() == 0 {
        return Frame::Integer(0);
    }

    let target = tracker.write_offset.load(Ordering::Relaxed);

    // fast path: already satisfied or no timeout needed
    let count = tracker.count_at_or_above(target);
    if count >= needed || timeout_ms == 0 {
        return Frame::Integer(count as i64);
    }

    // poll until enough replicas have caught up or the deadline passes
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let c = tracker.count_at_or_above(target);
        if c >= needed || tokio::time::Instant::now() >= deadline {
            return Frame::Integer(c as i64);
        }
    }
}

/// Renders the INFO response with multiple sections.
///
/// With no argument, returns all sections. With a section name,
/// returns only that section. Matches Redis convention of `#` headers
/// followed by `key:value` pairs separated by `\r\n`.
async fn render_info(
    engine: &ember_core::Engine,
    ctx: &Arc<ServerContext>,
    section: Option<&str>,
) -> Frame {
    let section_upper = section.map(|s| s.to_ascii_uppercase());
    let want_all = section_upper.is_none();
    let want = |name: &str| want_all || section_upper.as_deref() == Some(name);

    // only broadcast to shards if we need keyspace/memory/persistence sections
    let stats = if want("KEYSPACE") || want("MEMORY") || want("PERSISTENCE") || want("STATS") {
        match engine.broadcast(|| ShardRequest::Stats).await {
            Ok(responses) => {
                let mut total = KeyspaceStats {
                    key_count: 0,
                    used_bytes: 0,
                    keys_with_expiry: 0,
                    keys_expired: 0,
                    keys_evicted: 0,
                    oom_rejections: 0,
                    keyspace_hits: 0,
                    keyspace_misses: 0,
                };
                for r in &responses {
                    if let ShardResponse::Stats(s) = r {
                        total.key_count += s.key_count;
                        total.used_bytes += s.used_bytes;
                        total.keys_with_expiry += s.keys_with_expiry;
                        total.keys_expired += s.keys_expired;
                        total.keys_evicted += s.keys_evicted;
                        total.oom_rejections += s.oom_rejections;
                        total.keyspace_hits += s.keyspace_hits;
                        total.keyspace_misses += s.keyspace_misses;
                    }
                }
                Some(total)
            }
            Err(e) => return Frame::Error(format!("ERR {e}")),
        }
    } else {
        None
    };

    let mut out = String::with_capacity(512);

    if want("SERVER") {
        let uptime = ctx.start_time.elapsed().as_secs();
        out.push_str("# Server\r\n");
        out.push_str(&format!("ember_version:{}\r\n", ctx.version));
        out.push_str(&format!("process_id:{}\r\n", std::process::id()));
        out.push_str(&format!("uptime_in_seconds:{uptime}\r\n"));
        out.push_str(&format!("shard_count:{}\r\n", ctx.shard_count));
        out.push_str(&format!("tcp_port:{}\r\n", ctx.bind_addr.port()));
        out.push_str("hz:10\r\n");
        if let Some(ref path) = ctx.config_path {
            out.push_str(&format!("config_file:{}\r\n", path.display()));
        } else {
            out.push_str("config_file:\r\n");
        }
        out.push_str("\r\n");
    }

    if want("CLIENTS") {
        let connected = ctx.connections_active.load(Ordering::Relaxed);
        out.push_str("# Clients\r\n");
        out.push_str(&format!("connected_clients:{connected}\r\n"));
        out.push_str(&format!("max_clients:{}\r\n", ctx.max_connections));
        out.push_str("\r\n");
    }

    if want("MEMORY") {
        if let Some(ref stats) = stats {
            out.push_str("# Memory\r\n");
            out.push_str(&format!("used_memory:{}\r\n", stats.used_bytes));
            out.push_str(&format!(
                "used_memory_human:{}\r\n",
                human_bytes(stats.used_bytes)
            ));
            if let Some(rss) = get_rss_bytes() {
                out.push_str(&format!("used_memory_rss:{rss}\r\n"));
                out.push_str(&format!("used_memory_rss_human:{}\r\n", human_bytes(rss)));
            }
            let max_bytes = ctx
                .max_memory_limit
                .load(std::sync::atomic::Ordering::Relaxed) as usize;
            if max_bytes > 0 {
                let effective = ember_core::memory::effective_limit(max_bytes);
                out.push_str(&format!("max_memory:{max_bytes}\r\n"));
                out.push_str(&format!("max_memory_human:{}\r\n", human_bytes(max_bytes)));
                out.push_str(&format!("max_memory_effective:{effective}\r\n"));
                out.push_str(&format!(
                    "max_memory_effective_human:{}\r\n",
                    human_bytes(effective)
                ));
            } else {
                out.push_str("max_memory:0\r\n");
                out.push_str("max_memory_human:unlimited\r\n");
            }
            out.push_str("\r\n");
        }
    }

    if want("PERSISTENCE") {
        let last_save = ctx
            .last_save_timestamp
            .load(std::sync::atomic::Ordering::Relaxed);
        out.push_str("# Persistence\r\n");
        out.push_str(&format!(
            "aof_enabled:{}\r\n",
            if ctx.aof_enabled { 1 } else { 0 }
        ));
        out.push_str("aof_last_bgrewrite_status:ok\r\n");
        out.push_str(&format!("rdb_last_save_time:{last_save}\r\n"));
        out.push_str("\r\n");
    }

    if want("STATS") {
        let total_conns = ctx.connections_accepted.load(Ordering::Relaxed);
        let total_cmds = ctx.commands_processed.load(Ordering::Relaxed);
        out.push_str("# Stats\r\n");
        out.push_str(&format!("total_connections_received:{total_conns}\r\n"));
        out.push_str(&format!("total_commands_processed:{total_cmds}\r\n"));
        if let Some(ref stats) = stats {
            out.push_str(&format!("expired_keys:{}\r\n", stats.keys_expired));
            out.push_str(&format!("evicted_keys:{}\r\n", stats.keys_evicted));
            out.push_str(&format!("oom_rejections:{}\r\n", stats.oom_rejections));
            out.push_str(&format!("keyspace_hits:{}\r\n", stats.keyspace_hits));
            out.push_str(&format!("keyspace_misses:{}\r\n", stats.keyspace_misses));
        }
        out.push_str("\r\n");
    }

    if want("KEYSPACE") {
        if let Some(ref stats) = stats {
            out.push_str("# Keyspace\r\n");
            if stats.key_count > 0 {
                out.push_str(&format!(
                    "db0:keys={},expires={},used_bytes={}\r\n",
                    stats.key_count, stats.keys_with_expiry, stats.used_bytes
                ));
            }
            out.push_str("\r\n");
        }
    }

    if want("REPLICATION") {
        out.push_str("# Replication\r\n");
        if let Some(ref cluster) = ctx.cluster {
            let info = cluster.replication_info().await;
            use ember_cluster::NodeRole;
            match info.role {
                NodeRole::Primary => {
                    out.push_str("role:primary\r\n");
                    out.push_str(&format!("connected_replicas:{}\r\n", info.replica_count));
                }
                NodeRole::Replica => {
                    out.push_str("role:replica\r\n");
                    if let Some(addr) = info.primary_addr {
                        out.push_str(&format!("master_host:{}\r\n", addr.ip()));
                        out.push_str(&format!("master_port:{}\r\n", addr.port()));
                        out.push_str("master_link_status:up\r\n");
                    } else {
                        out.push_str("master_link_status:down\r\n");
                    }
                }
            }
        } else {
            out.push_str("role:primary\r\n");
            out.push_str("connected_replicas:0\r\n");
        }
        out.push_str("\r\n");
    }

    // trim trailing blank line
    if out.ends_with("\r\n\r\n") {
        out.truncate(out.len() - 2);
    }

    Frame::Bulk(Bytes::from(out))
}
