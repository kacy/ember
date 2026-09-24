//! Command preparation and dispatch for pipelined execution.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::connection_common::{
    frame_to_monitor_args, validate_command_sizes, MonitorEvent, Session,
};
use crate::pubsub::PubSubManager;
use crate::server::ServerContext;
use crate::slowlog::SlowLog;
use bytes::Bytes;
use ember_core::Engine;
use ember_protocol::{Command, Frame};

use super::route::Routing;
use super::{PendingResponse, PreparedDispatch};

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
    session: &Session,
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
            // reject oversized keys/values, as the pipelined path does
            if let Some(err) = validate_command_sizes(
                &cmd,
                ctx.limits.max_key_len,
                ctx.limits.max_value_len,
                ctx.limits.max_command_memory,
            ) {
                return err;
            }

            // ACL WHOAMI needs per-connection state
            if matches!(cmd, Command::AclWhoAmI) {
                return Frame::Bulk(Bytes::from(session.username().to_owned()));
            }

            // permission check — fast path: skip when unrestricted
            if let Some(err) = session.check(&cmd) {
                return err;
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
/// Does the same checks as [`process`] (parsing, sizes, ACL, cluster, MONITOR
/// broadcast), then asks [`route`](super::route::route) for the command's
/// shard request. That comes back as `PreparedDispatch::Routed`, and the
/// caller groups routed commands by shard for batch dispatch.
///
/// Every other command (broadcast, multi-key, cluster, pub/sub, errors) runs
/// through `execute` right away and comes back as `Immediate`.
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
    session: &Session,
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
    if let Some(err) = validate_command_sizes(
        &cmd,
        ctx.limits.max_key_len,
        ctx.limits.max_value_len,
        ctx.limits.max_command_memory,
    ) {
        return PreparedDispatch::Immediate(PendingResponse::Immediate(err));
    }

    // ACL WHOAMI needs per-connection state
    if matches!(cmd, Command::AclWhoAmI) {
        return PreparedDispatch::Immediate(PendingResponse::Immediate(Frame::Bulk(Bytes::from(
            session.username().to_owned(),
        ))));
    }

    // permission check — fast path: skip when unrestricted
    if let Some(err) = session.check(&cmd) {
        return PreparedDispatch::Immediate(PendingResponse::Immediate(err));
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

    let notify = ctx.keyspace_event_flags.load(Ordering::Relaxed) != 0;
    match super::route::route(cmd, engine, notify) {
        Routing::Shard(route) => PreparedDispatch::Routed {
            route,
            start,
            cmd_name,
        },
        Routing::Reply(frame) => PreparedDispatch::Immediate(PendingResponse::Immediate(frame)),
        // everything else runs through the full execute() path
        Routing::Other(cmd) => {
            let response =
                super::execute::execute(cmd, engine, ctx, slow_log, pubsub, was_asking, client_id)
                    .await;
            PreparedDispatch::Immediate(PendingResponse::Immediate(response))
        }
    }
}

/// Checks whether this cluster node may run the command.
///
/// Returns `None` if the command should proceed. Otherwise returns the
/// error to send:
/// - READONLY for a write while failover has paused writes, or MOVED to the
///   primary (READONLY if it is unknown) for a write on a replica
/// - CROSSSLOT when the command's keys hash to different slots
/// - MOVED, ASK or CLUSTERDOWN when this node does not serve the key's slot
///
/// Every command path calls this, so the write checks cover pipelined
/// commands as well as those that go through `execute`. The `asking` flag
/// is set when the client sent ASKING before this command, allowing access
/// to importing slots during migration.
pub(super) async fn cluster_slot_check(
    ctx: &ServerContext,
    cmd: &Command,
    asking: bool,
) -> Option<Frame> {
    let cluster = ctx.cluster.as_ref()?;
    let is_replica = cluster.is_replica().await;

    if cmd.is_write() {
        if cluster.is_writes_paused() {
            return Some(Frame::Error(
                "READONLY Failover in progress; writes are temporarily paused.".into(),
            ));
        }
        if is_replica {
            if let Some(key) = cmd.primary_key() {
                let slot = ember_cluster::key_slot(key.as_bytes());
                if let Some(addr) = cluster.primary_addr_for_slot(slot).await {
                    return Some(Frame::Error(format!("MOVED {slot} {addr}")));
                }
            }
            return Some(Frame::Error(
                "READONLY You can't write against a read only replica.".into(),
            ));
        }
    }

    // replicas serve every read locally
    if is_replica {
        return None;
    }

    let keys = cmd.keys();
    let first = keys.iter().next()?;
    if keys.iter().nth(1).is_some() {
        let all: Vec<&str> = keys.iter().collect();
        if let Err(err) = cluster.check_crossslot(&all) {
            return Some(err);
        }
    }
    cluster
        .check_slot_with_migration(first.as_bytes(), asking)
        .await
}
