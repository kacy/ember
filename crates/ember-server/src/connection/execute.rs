//! Command execution — routes parsed commands to engine shards.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use ember_core::Engine;
use ember_protocol::{Command, Frame};

use crate::pubsub::PubSubManager;
use crate::server::{format_client_list, ServerContext};
use crate::slowlog::SlowLog;

use super::exec;
use super::route::Routing;

/// Executes a parsed command and returns the response frame.
///
/// Single-key commands go to their shard through
/// [`route`](super::route::route), the same routing the pipelined path uses.
/// The rest are handled here: Ping and Echo inline, multi-key commands
/// (DEL, EXISTS) by fanning out across shards, and so on.
#[allow(clippy::too_many_arguments)]
pub(super) async fn execute(
    cmd: Command,
    engine: &Engine,
    ctx: &Arc<ServerContext>,
    slow_log: &Arc<SlowLog>,
    pubsub: &Arc<PubSubManager>,
    asking: bool,
    client_id: u64,
) -> Frame {
    // cluster checks: replica writes, slot ownership, cross-slot keys.
    // when `asking` is true, importing slots are allowed through.
    if let Some(redirect) = super::dispatch::cluster_slot_check(ctx, &cmd, asking).await {
        return redirect;
    }

    let notify = ctx.keyspace_event_flags.load(Ordering::Relaxed) != 0;
    let cmd = match super::route::route(cmd, engine, notify) {
        Routing::Shard(route) => {
            let frame = match engine.send_to_shard(route.shard_idx, route.request).await {
                Ok(resp) => super::response::resolve_shard_response(resp, route.tag),
                Err(e) => Frame::Error(format!("ERR {e}")),
            };
            if let Some(notify) = route.notify {
                notify.send(&frame, ctx, pubsub);
            }
            return frame;
        }
        Routing::Reply(frame) => return frame,
        Routing::Other(cmd) => cmd,
    };

    let cx = exec::ExecCtx {
        engine,
        ctx,
        pubsub,
        slow_log,
        client_id,
    };

    match cmd {
        // -- no shard needed --
        Command::Ping(None) => Frame::Simple("PONG".into()),
        Command::Ping(Some(msg)) => Frame::Bulk(msg),
        Command::Echo(msg) => Frame::Bulk(msg),
        Command::Command { subcommand, args } => {
            ember_protocol::command::table::handle_command_cmd(subcommand.as_deref(), &args)
        }

        // -- client commands (connection-scoped, no shard needed) --
        Command::ClientId => Frame::Integer(client_id as i64),
        Command::ClientGetName => {
            let name = ctx
                .clients
                .lock()
                .ok()
                .and_then(|map| map.get(&client_id).and_then(|c| c.name.clone()));
            match name {
                Some(n) => Frame::Bulk(Bytes::from(n)),
                None => Frame::Null,
            }
        }
        Command::ClientSetName { name } => {
            if let Ok(mut map) = ctx.clients.lock() {
                if let Some(info) = map.get_mut(&client_id) {
                    info.name = if name.is_empty() { None } else { Some(name) };
                }
            }
            Frame::Simple("OK".into())
        }
        Command::ClientList => {
            let output = format_client_list(ctx);
            Frame::Bulk(Bytes::from(output))
        }

        // -- string commands --
        Command::GetBit { key, offset } => exec::strings::getbit(key, offset, &cx).await,
        Command::SetBit { key, offset, value } => {
            exec::strings::setbit(key, offset, value, &cx).await
        }
        Command::BitCount { key, range } => exec::strings::bitcount(key, range, &cx).await,
        Command::BitPos { key, bit, range } => exec::strings::bitpos(key, bit, range, &cx).await,
        Command::BitOp { op, dest, keys } => exec::strings::bitop(op, dest, keys, &cx).await,
        Command::GetDel { key } => exec::strings::getdel(key, &cx).await,
        Command::GetEx { key, expire } => exec::strings::getex(key, expire, &cx).await,
        Command::GetSet { key, value } => exec::strings::getset(key, value, &cx).await,
        Command::MGet { keys } => exec::strings::mget(keys, &cx).await,
        Command::MSet { pairs } => exec::strings::mset(pairs, &cx).await,
        Command::MSetNx { pairs } => exec::strings::msetnx(pairs, &cx).await,

        // -- keyspace commands --
        Command::Del { keys } => exec::keyspace::del(keys, &cx).await,
        Command::Unlink { keys } => exec::keyspace::unlink(keys, &cx).await,
        Command::Exists { keys } => exec::keyspace::exists(keys, &cx).await,
        Command::Touch { keys } => exec::keyspace::touch(keys, &cx).await,
        Command::Expireat { key, timestamp } => exec::keyspace::expireat(key, timestamp, &cx).await,
        Command::Pexpireat { key, timestamp_ms } => {
            exec::keyspace::pexpireat(key, timestamp_ms, &cx).await
        }
        Command::Expiretime { key } => exec::keyspace::expiretime(key, &cx).await,
        Command::Pexpiretime { key } => exec::keyspace::pexpiretime(key, &cx).await,
        Command::Keys { pattern } => exec::keyspace::keys(pattern, &cx).await,
        Command::Scan {
            cursor,
            pattern,
            count,
        } => exec::keyspace::scan(cursor, pattern, count, &cx).await,
        Command::RandomKey => exec::keyspace::randomkey(&cx).await,
        Command::MemoryUsage { key } => exec::keyspace::memory_usage(key, &cx).await,

        // -- server commands --
        Command::DbSize => exec::server::dbsize(&cx).await,
        Command::Info { section } => exec::server::info(&cx, section.as_deref()).await,
        Command::ConfigGet { pattern } => exec::server::config_get(pattern, &cx).await,
        Command::ConfigSet { param, value } => {
            crate::connection_common::config_set(&param, &value, ctx, engine, slow_log).await
        }
        Command::ConfigRewrite => exec::server::config_rewrite(&cx).await,
        Command::BgSave => exec::server::bgsave(&cx).await,
        Command::BgRewriteAof => exec::server::bgrewriteaof(&cx).await,
        Command::Time => exec::server::time(),
        Command::LastSave => exec::server::lastsave(&cx),
        Command::Role => exec::server::role(&cx).await,
        Command::Wait {
            numreplicas,
            timeout_ms,
        } => exec::server::handle_wait(ctx, engine, numreplicas, timeout_ms).await,
        Command::FlushDb { async_mode } => exec::server::flushdb(async_mode, &cx).await,
        Command::FlushAll { async_mode } => exec::server::flushall(async_mode, &cx).await,
        Command::SlowLogGet { count } => exec::server::slowlog_get(count, &cx),
        Command::SlowLogLen => exec::server::slowlog_len(&cx),
        Command::SlowLogReset => exec::server::slowlog_reset(&cx),

        // -- list commands --
        Command::LMove {
            source,
            destination,
            src_left,
            dst_left,
        } => exec::lists::lmove(source, destination, src_left, dst_left, &cx).await,
        Command::Lmpop { keys, left, count } => exec::lists::lmpop(keys, left, count, &cx).await,
        // blocking list ops are handled by handle_blocking_pop_cmd in the
        // main loop; reaching here means they're inside a transaction.
        Command::BLPop { .. } => exec::lists::blpop_in_tx(),
        Command::BRPop { .. } => exec::lists::brpop_in_tx(),

        // -- sorted set commands --
        Command::Zmpop { keys, min, count } => {
            exec::sorted_sets::zmpop(keys, min, count, &cx).await
        }
        Command::ZDiff { keys, with_scores } => {
            exec::sorted_sets::zdiff(keys, with_scores, &cx).await
        }
        Command::ZInter { keys, with_scores } => {
            exec::sorted_sets::zinter(keys, with_scores, &cx).await
        }
        Command::ZUnion { keys, with_scores } => {
            exec::sorted_sets::zunion(keys, with_scores, &cx).await
        }
        Command::ZDiffStore { dest, keys } => exec::sorted_sets::zdiffstore(dest, keys, &cx).await,
        Command::ZInterStore { dest, keys } => {
            exec::sorted_sets::zinterstore(dest, keys, &cx).await
        }
        Command::ZUnionStore { dest, keys } => {
            exec::sorted_sets::zunionstore(dest, keys, &cx).await
        }
        Command::ZRandMember {
            key,
            count,
            with_scores,
        } => exec::sorted_sets::zrandmember(key, count, with_scores, &cx).await,

        // -- hash commands --
        Command::HIncrByFloat { key, field, delta } => {
            exec::hashes::hincrbyfloat(key, field, delta, &cx).await
        }
        Command::HRandField {
            key,
            count,
            with_values,
        } => exec::hashes::hrandfield(key, count, with_values, &cx).await,

        // -- set commands --
        Command::SMove {
            source,
            destination,
            member,
        } => exec::sets::smove(source, destination, member, &cx).await,
        Command::SInterCard { keys, limit } => exec::sets::sintercard(keys, limit, &cx).await,

        // -- cluster commands --
        Command::ClusterKeySlot { key } => exec::cluster::cluster_keyslot(key),
        Command::ClusterInfo => exec::cluster::cluster_info(&cx).await,
        Command::ClusterNodes => exec::cluster::cluster_nodes(&cx).await,
        Command::ClusterSlots => exec::cluster::cluster_slots(&cx).await,
        Command::ClusterMyId => exec::cluster::cluster_myid(&cx),
        Command::ClusterMeet { ip, port } => exec::cluster::cluster_meet(ip, port, &cx).await,
        Command::ClusterAddSlots { slots } => exec::cluster::cluster_addslots(slots, &cx).await,
        Command::ClusterAddSlotsRange { ranges } => {
            exec::cluster::cluster_addslots_range(ranges, &cx).await
        }
        Command::ClusterDelSlots { slots } => exec::cluster::cluster_delslots(slots, &cx).await,
        Command::ClusterForget { node_id } => exec::cluster::cluster_forget(node_id, &cx).await,
        Command::ClusterSetSlotImporting { slot, node_id } => {
            exec::cluster::cluster_setslot_importing(slot, node_id, &cx).await
        }
        Command::ClusterSetSlotMigrating { slot, node_id } => {
            exec::cluster::cluster_setslot_migrating(slot, node_id, &cx).await
        }
        Command::ClusterSetSlotNode { slot, node_id } => {
            exec::cluster::cluster_setslot_node(slot, node_id, &cx).await
        }
        Command::ClusterSetSlotStable { slot } => {
            exec::cluster::cluster_setslot_stable(slot, &cx).await
        }
        Command::ClusterCountKeysInSlot { slot } => {
            exec::cluster::cluster_count_keys_in_slot(slot, &cx).await
        }
        Command::ClusterGetKeysInSlot { slot, count } => {
            exec::cluster::cluster_get_keys_in_slot(slot, count, &cx).await
        }
        Command::ClusterReplicate { node_id } => {
            exec::cluster::cluster_replicate(node_id, &cx).await
        }
        Command::ClusterFailover { force, takeover } => {
            exec::cluster::cluster_failover(force, takeover, &cx).await
        }
        Command::Migrate {
            host,
            port,
            key,
            timeout_ms,
            replace,
            ..
        } => exec::cluster::migrate(host, port, key, timeout_ms, replace, &cx).await,
        Command::Restore {
            key,
            ttl_ms,
            data,
            replace,
        } => exec::cluster::restore(key, ttl_ms, data, replace, &cx).await,

        // -- pub/sub commands --
        Command::Publish { channel, message } => exec::pubsub::publish(channel, message, &cx),
        Command::PubSubChannels { pattern } => exec::pubsub::pubsub_channels(pattern, &cx),
        Command::PubSubNumSub { channels } => exec::pubsub::pubsub_numsub(channels, &cx),
        Command::PubSubNumPat => exec::pubsub::pubsub_numpat(&cx),
        // subscribe commands are handled in the connection loop, not here.
        Command::Subscribe { .. }
        | Command::Unsubscribe { .. }
        | Command::PSubscribe { .. }
        | Command::PUnsubscribe { .. } => exec::pubsub::subscribe_error(),

        // -- ACL / AUTH commands --
        Command::Auth { username, password } => exec::acl::auth(username, password, &cx),
        Command::AclWhoAmI => exec::acl::acl_whoami(),
        cmd @ (Command::AclList
        | Command::AclUsers
        | Command::AclGetUser { .. }
        | Command::AclDelUser { .. }
        | Command::AclSetUser { .. }
        | Command::AclCat { .. }) => exec::acl::acl_admin(cmd, &cx),

        // -- vector commands --

        // -- protobuf commands --
        #[cfg(feature = "protobuf")]
        Command::ProtoRegister { name, descriptor } => {
            exec::protobuf::proto_register(name, descriptor, &cx).await
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoSchemas => exec::protobuf::proto_schemas(&cx).await,
        #[cfg(feature = "protobuf")]
        Command::ProtoDescribe { name } => exec::protobuf::proto_describe(name, &cx).await,
        #[cfg(feature = "protobuf")]
        Command::ProtoGetField { key, field_path } => {
            exec::protobuf::proto_get_field(key, field_path, &cx).await
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoScan {
            cursor,
            pattern,
            count,
            type_name,
        } => exec::protobuf::proto_scan(cursor, pattern, count, type_name, &cx).await,
        #[cfg(feature = "protobuf")]
        Command::ProtoFind {
            cursor,
            field_path,
            field_value,
            pattern,
            type_name,
            count,
        } => {
            exec::protobuf::proto_find(
                cursor,
                field_path,
                field_value,
                pattern,
                type_name,
                count,
                &cx,
            )
            .await
        }
        #[cfg(not(feature = "protobuf"))]
        Command::ProtoRegister { .. }
        | Command::ProtoSet { .. }
        | Command::ProtoGet { .. }
        | Command::ProtoType { .. }
        | Command::ProtoSchemas
        | Command::ProtoDescribe { .. }
        | Command::ProtoGetField { .. }
        | Command::ProtoSetField { .. }
        | Command::ProtoDelField { .. }
        | Command::ProtoScan { .. }
        | Command::ProtoFind { .. } => exec::protobuf::not_compiled(),

        Command::Quit => Frame::Simple("OK".into()),
        Command::Asking => Frame::Simple("OK".into()),
        Command::Multi => Frame::Error("ERR MULTI calls can not be nested".into()),
        Command::Exec => Frame::Error("ERR EXEC without MULTI".into()),
        Command::Discard => Frame::Error("ERR DISCARD without MULTI".into()),
        Command::Monitor => Frame::Simple("OK".into()),
        Command::Watch { .. } | Command::Unwatch => Frame::Simple("OK".into()),

        Command::Unknown(name) => Frame::Error(format!("ERR unknown command '{name}'")),

        // single-key commands, sent to their shard at the top of this function
        Command::Append { .. }
        | Command::Copy { .. }
        | Command::Decr { .. }
        | Command::DecrBy { .. }
        | Command::Expire { .. }
        | Command::Get { .. }
        | Command::GetRange { .. }
        | Command::HDel { .. }
        | Command::HExists { .. }
        | Command::HGet { .. }
        | Command::HGetAll { .. }
        | Command::HIncrBy { .. }
        | Command::HKeys { .. }
        | Command::HLen { .. }
        | Command::HMGet { .. }
        | Command::HScan { .. }
        | Command::HSet { .. }
        | Command::HVals { .. }
        | Command::Incr { .. }
        | Command::IncrBy { .. }
        | Command::IncrByFloat { .. }
        | Command::LIndex { .. }
        | Command::LInsert { .. }
        | Command::LLen { .. }
        | Command::LPop { .. }
        | Command::LPos { .. }
        | Command::LPush { .. }
        | Command::LRange { .. }
        | Command::LRem { .. }
        | Command::LSet { .. }
        | Command::LTrim { .. }
        | Command::ObjectEncoding { .. }
        | Command::ObjectRefcount { .. }
        | Command::Persist { .. }
        | Command::Pexpire { .. }
        | Command::Pttl { .. }
        | Command::RPop { .. }
        | Command::RPush { .. }
        | Command::Rename { .. }
        | Command::SAdd { .. }
        | Command::SCard { .. }
        | Command::SDiff { .. }
        | Command::SDiffStore { .. }
        | Command::SInter { .. }
        | Command::SInterStore { .. }
        | Command::SIsMember { .. }
        | Command::SMembers { .. }
        | Command::SMisMember { .. }
        | Command::SPop { .. }
        | Command::SRandMember { .. }
        | Command::SRem { .. }
        | Command::SScan { .. }
        | Command::SUnion { .. }
        | Command::SUnionStore { .. }
        | Command::Set { .. }
        | Command::SetRange { .. }
        | Command::Sort { .. }
        | Command::Strlen { .. }
        | Command::Ttl { .. }
        | Command::Type { .. }
        | Command::ZAdd { .. }
        | Command::ZCard { .. }
        | Command::ZCount { .. }
        | Command::ZIncrBy { .. }
        | Command::ZPopMax { .. }
        | Command::ZPopMin { .. }
        | Command::ZRange { .. }
        | Command::ZRangeByScore { .. }
        | Command::ZRank { .. }
        | Command::ZRem { .. }
        | Command::ZRevRange { .. }
        | Command::ZRevRangeByScore { .. }
        | Command::ZRevRank { .. }
        | Command::ZScan { .. }
        | Command::ZScore { .. } => not_routed(),
        #[cfg(feature = "vector")]
        Command::VAdd { .. }
        | Command::VAddBatch { .. }
        | Command::VCard { .. }
        | Command::VDim { .. }
        | Command::VGet { .. }
        | Command::VInfo { .. }
        | Command::VRem { .. }
        | Command::VSim { .. } => not_routed(),
        #[cfg(feature = "protobuf")]
        Command::ProtoDelField { .. }
        | Command::ProtoGet { .. }
        | Command::ProtoSet { .. }
        | Command::ProtoSetField { .. }
        | Command::ProtoType { .. } => not_routed(),
        #[cfg(not(feature = "vector"))]
        Command::VAdd { .. }
        | Command::VAddBatch { .. }
        | Command::VCard { .. }
        | Command::VDim { .. }
        | Command::VGet { .. }
        | Command::VInfo { .. }
        | Command::VRem { .. }
        | Command::VSim { .. } => exec::vector::not_compiled(),
    }
}

/// The reply for a command `route` handles, should one ever get here.
fn not_routed() -> Frame {
    Frame::Error("ERR internal error: command was not routed to a shard".into())
}
