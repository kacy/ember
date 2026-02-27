//! Command execution — routes parsed commands to engine shards.

use std::sync::Arc;

use bytes::Bytes;
use ember_core::Engine;
use ember_protocol::{Command, Frame};

use crate::pubsub::PubSubManager;
use crate::server::{format_client_list, ServerContext};
use crate::slowlog::SlowLog;

use super::exec;

/// Executes a parsed command and returns the response frame.
///
/// Ping and Echo are handled inline (no shard routing needed).
/// Single-key commands route to the owning shard. Multi-key commands
/// (DEL, EXISTS) fan out across shards and aggregate results.
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
    // Write gating: reject mutations when the node is a replica or when
    // writes are temporarily paused (e.g. during failover coordination).
    if let Some(ref cluster) = ctx.cluster {
        if cluster.is_writes_paused() && cmd.is_write() {
            return Frame::Error(
                "READONLY Failover in progress; writes are temporarily paused.".into(),
            );
        }
        if cluster.is_replica().await && cmd.is_write() {
            if let Some(key) = cmd.primary_key() {
                use ember_cluster::key_slot;
                let slot = key_slot(key.as_bytes());
                if let Some(addr) = cluster.primary_addr_for_slot(slot).await {
                    return Frame::Error(format!("MOVED {slot} {addr}"));
                }
            }
            return Frame::Error("READONLY You can't write against a read only replica.".into());
        }
    }

    // cluster slot validation — check whether we own the slot for this key.
    // when `asking` is true, importing slots are allowed through.
    if let Some(redirect) = super::dispatch::cluster_slot_check(ctx, &cmd, asking).await {
        return redirect;
    }

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
        Command::Command { subcommand, args } => handle_command_cmd(subcommand.as_deref(), &args),

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
        Command::Get { key } => exec::strings::get(key, &cx).await,
        Command::Set {
            key,
            value,
            expire,
            nx,
            xx,
        } => exec::strings::set(key, value, expire, nx, xx, &cx).await,
        Command::Incr { key } => exec::strings::incr(key, &cx).await,
        Command::Decr { key } => exec::strings::decr(key, &cx).await,
        Command::IncrBy { key, delta } => exec::strings::incrby(key, delta, &cx).await,
        Command::DecrBy { key, delta } => exec::strings::decrby(key, delta, &cx).await,
        Command::IncrByFloat { key, delta } => exec::strings::incrbyfloat(key, delta, &cx).await,
        Command::Append { key, value } => exec::strings::append(key, value, &cx).await,
        Command::Strlen { key } => exec::strings::strlen(key, &cx).await,
        Command::GetRange { key, start, end } => {
            exec::strings::getrange(key, start, end, &cx).await
        }
        Command::SetRange { key, offset, value } => {
            exec::strings::setrange(key, offset, value, &cx).await
        }
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
        Command::Expire { key, seconds } => exec::keyspace::expire(key, seconds, &cx).await,
        Command::Expireat { key, timestamp } => exec::keyspace::expireat(key, timestamp, &cx).await,
        Command::Pexpire { key, milliseconds } => {
            exec::keyspace::pexpire(key, milliseconds, &cx).await
        }
        Command::Pexpireat { key, timestamp_ms } => {
            exec::keyspace::pexpireat(key, timestamp_ms, &cx).await
        }
        Command::Ttl { key } => exec::keyspace::ttl(key, &cx).await,
        Command::Pttl { key } => exec::keyspace::pttl(key, &cx).await,
        Command::Persist { key } => exec::keyspace::persist(key, &cx).await,
        Command::Expiretime { key } => exec::keyspace::expiretime(key, &cx).await,
        Command::Pexpiretime { key } => exec::keyspace::pexpiretime(key, &cx).await,
        Command::Keys { pattern } => exec::keyspace::keys(pattern, &cx).await,
        Command::Scan {
            cursor,
            pattern,
            count,
        } => exec::keyspace::scan(cursor, pattern, count, &cx).await,
        Command::Rename { key, newkey } => exec::keyspace::rename(key, newkey, &cx).await,
        Command::Copy {
            source,
            destination,
            replace,
        } => exec::keyspace::copy(source, destination, replace, &cx).await,
        Command::RandomKey => exec::keyspace::randomkey(&cx).await,
        Command::Sort {
            key,
            desc,
            alpha,
            limit,
            store: Some(dest),
        } => exec::keyspace::sort_with_store(key, desc, alpha, limit, dest, &cx).await,
        Command::Sort {
            key,
            desc,
            alpha,
            limit,
            store: None,
        } => exec::keyspace::sort_no_store(key, desc, alpha, limit, &cx).await,
        Command::Type { key } => exec::keyspace::type_cmd(key, &cx).await,
        Command::ObjectEncoding { key } => exec::keyspace::object_encoding(key, &cx).await,
        Command::ObjectRefcount { key } => exec::keyspace::object_refcount(key, &cx).await,
        Command::MemoryUsage { key } => exec::keyspace::memory_usage(key, &cx).await,

        // -- server commands --
        Command::DbSize => exec::server::dbsize(&cx).await,
        Command::Info { section } => exec::server::info(&cx, section.as_deref()).await,
        Command::ConfigGet { pattern } => exec::server::config_get(pattern, &cx).await,
        Command::ConfigSet { param, value } => exec::server::config_set(param, value, &cx).await,
        Command::ConfigRewrite => exec::server::config_rewrite(&cx).await,
        Command::BgSave => exec::server::bgsave(&cx).await,
        Command::BgRewriteAof => exec::server::bgrewriteaof(&cx).await,
        Command::Time => exec::server::time(),
        Command::LastSave => exec::server::lastsave(&cx),
        Command::Role => exec::server::role(&cx).await,
        Command::Wait {
            numreplicas,
            timeout_ms,
        } => exec::server::handle_wait(ctx, numreplicas, timeout_ms).await,
        Command::FlushDb { async_mode } => exec::server::flushdb(async_mode, &cx).await,
        Command::FlushAll { async_mode } => exec::server::flushall(async_mode, &cx).await,
        Command::SlowLogGet { count } => exec::server::slowlog_get(count, &cx),
        Command::SlowLogLen => exec::server::slowlog_len(&cx),
        Command::SlowLogReset => exec::server::slowlog_reset(&cx),

        // -- list commands --
        Command::LPush { key, values } => exec::lists::lpush(key, values, &cx).await,
        Command::RPush { key, values } => exec::lists::rpush(key, values, &cx).await,
        Command::LPop { key, count } => exec::lists::lpop(key, count, &cx).await,
        Command::RPop { key, count } => exec::lists::rpop(key, count, &cx).await,
        Command::LRange { key, start, stop } => exec::lists::lrange(key, start, stop, &cx).await,
        Command::LLen { key } => exec::lists::llen(key, &cx).await,
        Command::LIndex { key, index } => exec::lists::lindex(key, index, &cx).await,
        Command::LSet { key, index, value } => exec::lists::lset(key, index, value, &cx).await,
        Command::LTrim { key, start, stop } => exec::lists::ltrim(key, start, stop, &cx).await,
        Command::LInsert {
            key,
            before,
            pivot,
            value,
        } => exec::lists::linsert(key, before, pivot, value, &cx).await,
        Command::LRem { key, count, value } => exec::lists::lrem(key, count, value, &cx).await,
        Command::LPos {
            key,
            element,
            rank,
            count,
            maxlen,
        } => exec::lists::lpos(key, element, rank, count, maxlen, &cx).await,
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
        Command::ZAdd {
            key,
            flags,
            members,
        } => exec::sorted_sets::zadd(key, flags, members, &cx).await,
        Command::ZRem { key, members } => exec::sorted_sets::zrem(key, members, &cx).await,
        Command::ZScore { key, member } => exec::sorted_sets::zscore(key, member, &cx).await,
        Command::ZRank { key, member } => exec::sorted_sets::zrank(key, member, &cx).await,
        Command::ZRevRank { key, member } => exec::sorted_sets::zrevrank(key, member, &cx).await,
        Command::ZRange {
            key,
            start,
            stop,
            with_scores,
        } => exec::sorted_sets::zrange(key, start, stop, with_scores, &cx).await,
        Command::ZRevRange {
            key,
            start,
            stop,
            with_scores,
        } => exec::sorted_sets::zrevrange(key, start, stop, with_scores, &cx).await,
        Command::ZCard { key } => exec::sorted_sets::zcard(key, &cx).await,
        Command::ZCount { key, min, max } => exec::sorted_sets::zcount(key, min, max, &cx).await,
        Command::ZIncrBy {
            key,
            increment,
            member,
        } => exec::sorted_sets::zincrby(key, increment, member, &cx).await,
        Command::ZRangeByScore {
            key,
            min,
            max,
            with_scores,
            offset,
            count,
        } => exec::sorted_sets::zrangebyscore(key, min, max, with_scores, offset, count, &cx).await,
        Command::ZRevRangeByScore {
            key,
            min,
            max,
            with_scores,
            offset,
            count,
        } => {
            exec::sorted_sets::zrevrangebyscore(key, min, max, with_scores, offset, count, &cx)
                .await
        }
        Command::ZPopMin { key, count } => exec::sorted_sets::zpopmin(key, count, &cx).await,
        Command::ZPopMax { key, count } => exec::sorted_sets::zpopmax(key, count, &cx).await,
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
        Command::ZScan {
            key,
            cursor,
            pattern,
            count,
        } => exec::sorted_sets::zscan(key, cursor, pattern, count, &cx).await,

        // -- hash commands --
        Command::HSet { key, fields } => exec::hashes::hset(key, fields, &cx).await,
        Command::HGet { key, field } => exec::hashes::hget(key, field, &cx).await,
        Command::HGetAll { key } => exec::hashes::hgetall(key, &cx).await,
        Command::HDel { key, fields } => exec::hashes::hdel(key, fields, &cx).await,
        Command::HExists { key, field } => exec::hashes::hexists(key, field, &cx).await,
        Command::HLen { key } => exec::hashes::hlen(key, &cx).await,
        Command::HIncrBy { key, field, delta } => {
            exec::hashes::hincrby(key, field, delta, &cx).await
        }
        Command::HIncrByFloat { key, field, delta } => {
            exec::hashes::hincrbyfloat(key, field, delta, &cx).await
        }
        Command::HKeys { key } => exec::hashes::hkeys(key, &cx).await,
        Command::HVals { key } => exec::hashes::hvals(key, &cx).await,
        Command::HMGet { key, fields } => exec::hashes::hmget(key, fields, &cx).await,
        Command::HRandField {
            key,
            count,
            with_values,
        } => exec::hashes::hrandfield(key, count, with_values, &cx).await,
        Command::HScan {
            key,
            cursor,
            pattern,
            count,
        } => exec::hashes::hscan(key, cursor, pattern, count, &cx).await,

        // -- set commands --
        Command::SAdd { key, members } => exec::sets::sadd(key, members, &cx).await,
        Command::SRem { key, members } => exec::sets::srem(key, members, &cx).await,
        Command::SMembers { key } => exec::sets::smembers(key, &cx).await,
        Command::SIsMember { key, member } => exec::sets::sismember(key, member, &cx).await,
        Command::SCard { key } => exec::sets::scard(key, &cx).await,
        Command::SUnion { keys } => exec::sets::sunion(keys, &cx).await,
        Command::SInter { keys } => exec::sets::sinter(keys, &cx).await,
        Command::SDiff { keys } => exec::sets::sdiff(keys, &cx).await,
        Command::SUnionStore { dest, keys } => exec::sets::sunionstore(dest, keys, &cx).await,
        Command::SInterStore { dest, keys } => exec::sets::sinterstore(dest, keys, &cx).await,
        Command::SDiffStore { dest, keys } => exec::sets::sdiffstore(dest, keys, &cx).await,
        Command::SRandMember { key, count } => exec::sets::srandmember(key, count, &cx).await,
        Command::SPop { key, count } => exec::sets::spop(key, count, &cx).await,
        Command::SMisMember { key, members } => exec::sets::smismember(key, members, &cx).await,
        Command::SMove {
            source,
            destination,
            member,
        } => exec::sets::smove(source, destination, member, &cx).await,
        Command::SInterCard { keys, limit } => exec::sets::sintercard(keys, limit, &cx).await,
        Command::SScan {
            key,
            cursor,
            pattern,
            count,
        } => exec::sets::sscan(key, cursor, pattern, count, &cx).await,

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
        Command::AclList => exec::acl::acl_list(&cx),
        Command::AclUsers => exec::acl::acl_users(&cx),
        Command::AclGetUser { username } => exec::acl::acl_getuser(username, &cx),
        Command::AclDelUser { usernames } => exec::acl::acl_deluser(usernames, &cx),
        Command::AclSetUser { username, rules } => exec::acl::acl_setuser(username, rules, &cx),
        Command::AclCat { category } => exec::acl::acl_cat(category),

        // -- vector commands --
        #[cfg(feature = "vector")]
        Command::VAdd {
            key,
            element,
            vector,
            metric,
            quantization,
            connectivity,
            expansion_add,
        } => {
            exec::vector::vadd(
                key,
                element,
                vector,
                metric,
                quantization,
                connectivity,
                expansion_add,
                &cx,
            )
            .await
        }
        #[cfg(feature = "vector")]
        Command::VAddBatch {
            key,
            entries,
            dim,
            metric,
            quantization,
            connectivity,
            expansion_add,
        } => {
            exec::vector::vaddbatch(
                key,
                entries,
                dim,
                metric,
                quantization,
                connectivity,
                expansion_add,
                &cx,
            )
            .await
        }
        #[cfg(feature = "vector")]
        Command::VSim {
            key,
            query,
            count,
            ef_search,
            with_scores,
        } => exec::vector::vsim(key, query, count, ef_search, with_scores, &cx).await,
        #[cfg(feature = "vector")]
        Command::VRem { key, element } => exec::vector::vrem(key, element, &cx).await,
        #[cfg(feature = "vector")]
        Command::VGet { key, element } => exec::vector::vget(key, element, &cx).await,
        #[cfg(feature = "vector")]
        Command::VCard { key } => exec::vector::vcard(key, &cx).await,
        #[cfg(feature = "vector")]
        Command::VDim { key } => exec::vector::vdim(key, &cx).await,
        #[cfg(feature = "vector")]
        Command::VInfo { key } => exec::vector::vinfo(key, &cx).await,
        #[cfg(not(feature = "vector"))]
        Command::VAdd { .. }
        | Command::VAddBatch { .. }
        | Command::VSim { .. }
        | Command::VRem { .. }
        | Command::VGet { .. }
        | Command::VCard { .. }
        | Command::VDim { .. }
        | Command::VInfo { .. } => exec::vector::not_compiled(),

        // -- protobuf commands --
        #[cfg(feature = "protobuf")]
        Command::ProtoRegister { name, descriptor } => {
            exec::protobuf::proto_register(name, descriptor, &cx).await
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoSet {
            key,
            type_name,
            data,
            expire,
            nx,
            xx,
        } => exec::protobuf::proto_set(key, type_name, data, expire, nx, xx, &cx).await,
        #[cfg(feature = "protobuf")]
        Command::ProtoGet { key } => exec::protobuf::proto_get(key, &cx).await,
        #[cfg(feature = "protobuf")]
        Command::ProtoType { key } => exec::protobuf::proto_type(key, &cx).await,
        #[cfg(feature = "protobuf")]
        Command::ProtoSchemas => exec::protobuf::proto_schemas(&cx).await,
        #[cfg(feature = "protobuf")]
        Command::ProtoDescribe { name } => exec::protobuf::proto_describe(name, &cx).await,
        #[cfg(feature = "protobuf")]
        Command::ProtoGetField { key, field_path } => {
            exec::protobuf::proto_get_field(key, field_path, &cx).await
        }
        #[cfg(feature = "protobuf")]
        Command::ProtoSetField {
            key,
            field_path,
            value,
        } => exec::protobuf::proto_set_field(key, field_path, value, &cx).await,
        #[cfg(feature = "protobuf")]
        Command::ProtoDelField { key, field_path } => {
            exec::protobuf::proto_del_field(key, field_path, &cx).await
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
        | Command::ProtoDelField { .. } => exec::protobuf::not_compiled(),

        Command::Quit => Frame::Simple("OK".into()),
        Command::Asking => Frame::Simple("OK".into()),
        Command::Multi => Frame::Error("ERR MULTI calls can not be nested".into()),
        Command::Exec => Frame::Error("ERR EXEC without MULTI".into()),
        Command::Discard => Frame::Error("ERR DISCARD without MULTI".into()),
        Command::Monitor => Frame::Simple("OK".into()),
        Command::Watch { .. } | Command::Unwatch => Frame::Simple("OK".into()),

        Command::Unknown(name) => Frame::Error(format!("ERR unknown command '{name}'")),
    }
}

/// Handles COMMAND [COUNT | INFO name... | DOCS name... | LIST].
///
/// Provides static command metadata for client library discovery.
/// The format matches Redis 7 conventions so clients can probe capabilities
/// without falling back to error-handling paths.
fn handle_command_cmd(subcommand: Option<&str>, args: &[String]) -> Frame {
    match subcommand {
        None | Some("LIST") => Frame::Array(COMMAND_TABLE.iter().map(command_entry).collect()),
        Some("COUNT") => Frame::Integer(COMMAND_TABLE.len() as i64),
        Some("INFO") => {
            if args.is_empty() {
                return Frame::Array(COMMAND_TABLE.iter().map(command_entry).collect());
            }
            let frames = args
                .iter()
                .map(|name| {
                    let upper = name.to_ascii_uppercase();
                    match COMMAND_TABLE.iter().find(|e| e.name == upper) {
                        Some(entry) => command_entry(entry),
                        None => Frame::Null,
                    }
                })
                .collect();
            Frame::Array(frames)
        }
        Some("DOCS") => {
            // return empty docs — clients use this for documentation display,
            // not capability detection. an empty map per command is valid.
            if args.is_empty() {
                return Frame::Array(vec![]);
            }
            let mut frames = Vec::with_capacity(args.len() * 2);
            for name in args {
                let upper = name.to_ascii_uppercase();
                frames.push(Frame::Bulk(Bytes::from(upper)));
                frames.push(Frame::Array(vec![]));
            }
            Frame::Array(frames)
        }
        Some("GETKEYS") => Frame::Array(vec![]),
        Some(other) => Frame::Error(format!("ERR unknown COMMAND subcommand '{other}'")),
    }
}

/// Builds a COMMAND entry array for a single command.
///
/// Format: [name, arity, [flags], first_key, last_key, step]
fn command_entry(e: &CommandEntry) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from(e.name.to_ascii_lowercase())),
        Frame::Integer(e.arity),
        Frame::Array(e.flags.iter().map(|f| Frame::Simple((*f).into())).collect()),
        Frame::Integer(e.first_key),
        Frame::Integer(e.last_key),
        Frame::Integer(e.step),
    ])
}

struct CommandEntry {
    name: &'static str,
    arity: i64,
    flags: &'static [&'static str],
    first_key: i64,
    last_key: i64,
    step: i64,
}

/// Static command table. Arity: positive = exact, negative = minimum.
/// Flags: write, readonly, denyoom, admin, pubsub, noscript, fast, loading, etc.
static COMMAND_TABLE: &[CommandEntry] = &[
    CommandEntry {
        name: "APPEND",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "AUTH",
        arity: -2,
        flags: &["noscript", "loading", "fast", "no_auth"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "BGREWRITEAOF",
        arity: 1,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "BGSAVE",
        arity: -1,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "BITCOUNT",
        arity: -2,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "BITOP",
        arity: -4,
        flags: &["write", "denyoom"],
        first_key: 2,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "BITPOS",
        arity: -3,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "BLPOP",
        arity: -3,
        flags: &["write", "noscript"],
        first_key: 1,
        last_key: -2,
        step: 1,
    },
    CommandEntry {
        name: "BRPOP",
        arity: -3,
        flags: &["write", "noscript"],
        first_key: 1,
        last_key: -2,
        step: 1,
    },
    CommandEntry {
        name: "CLIENT",
        arity: -2,
        flags: &["admin", "noscript", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "CLUSTER",
        arity: -2,
        flags: &["admin"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "COMMAND",
        arity: -1,
        flags: &["loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "CONFIG",
        arity: -2,
        flags: &["admin", "loading", "noscript"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "COPY",
        arity: -3,
        flags: &["write"],
        first_key: 1,
        last_key: 2,
        step: 1,
    },
    CommandEntry {
        name: "DBSIZE",
        arity: 1,
        flags: &["readonly", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "DECR",
        arity: 2,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "DECRBY",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "DEL",
        arity: -2,
        flags: &["write"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "DISCARD",
        arity: 1,
        flags: &["noscript", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "ECHO",
        arity: 2,
        flags: &["fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "EXEC",
        arity: 1,
        flags: &["noscript", "loading"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "EXISTS",
        arity: -2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "EXPIRE",
        arity: 3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "EXPIREAT",
        arity: 3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "EXPIRETIME",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "FLUSHALL",
        arity: -1,
        flags: &["write"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "FLUSHDB",
        arity: -1,
        flags: &["write"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "GET",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "GETBIT",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "GETDEL",
        arity: 2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "GETEX",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "GETRANGE",
        arity: 4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "GETSET",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HDEL",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HEXISTS",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HGET",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HGETALL",
        arity: 2,
        flags: &["readonly", "random"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HINCRBY",
        arity: 4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HINCRBYFLOAT",
        arity: 4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HKEYS",
        arity: 2,
        flags: &["readonly", "sort_for_script"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HLEN",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HMGET",
        arity: -3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HMSET",
        arity: -4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HRANDFIELD",
        arity: -2,
        flags: &["readonly", "random"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HSCAN",
        arity: -3,
        flags: &["readonly", "random"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HSET",
        arity: -4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "HVALS",
        arity: 2,
        flags: &["readonly", "sort_for_script"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "INCR",
        arity: 2,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "INCRBY",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "INCRBYFLOAT",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "INFO",
        arity: -1,
        flags: &["loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "KEYS",
        arity: 2,
        flags: &["readonly", "sort_for_script"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "LASTSAVE",
        arity: 1,
        flags: &["random", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "LINDEX",
        arity: 3,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "LINSERT",
        arity: 5,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "LLEN",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "LMOVE",
        arity: 5,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 2,
        step: 1,
    },
    CommandEntry {
        name: "LMPOP",
        arity: -4,
        flags: &["write", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "LPOS",
        arity: -3,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "LPOP",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "LPUSH",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "LPUSHX",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "LRANGE",
        arity: 4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "LREM",
        arity: 4,
        flags: &["write"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "LSET",
        arity: 4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "LTRIM",
        arity: 4,
        flags: &["write"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "MEMORY",
        arity: -2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "MGET",
        arity: -2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "MIGRATE",
        arity: -6,
        flags: &["write"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "MONITOR",
        arity: 1,
        flags: &["admin", "loading"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "MSET",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        step: 2,
    },
    CommandEntry {
        name: "MSETNX",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        step: 2,
    },
    CommandEntry {
        name: "MULTI",
        arity: 1,
        flags: &["noscript", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "OBJECT",
        arity: -2,
        flags: &["slow"],
        first_key: 2,
        last_key: 2,
        step: 1,
    },
    CommandEntry {
        name: "PERSIST",
        arity: 2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "PEXPIRE",
        arity: 3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "PEXPIREAT",
        arity: 3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "PEXPIRETIME",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "PING",
        arity: -1,
        flags: &["fast", "loading"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "PSETEX",
        arity: 4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "PSUBSCRIBE",
        arity: -2,
        flags: &["pubsub", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "PTTL",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "PUBLISH",
        arity: 3,
        flags: &["pubsub", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "PUBSUB",
        arity: -2,
        flags: &["pubsub", "random", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "PUNSUBSCRIBE",
        arity: -1,
        flags: &["pubsub", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "QUIT",
        arity: 1,
        flags: &["fast", "loading"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "RANDOMKEY",
        arity: 1,
        flags: &["readonly", "random"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "RENAME",
        arity: 3,
        flags: &["write"],
        first_key: 1,
        last_key: 2,
        step: 1,
    },
    CommandEntry {
        name: "ROLE",
        arity: 1,
        flags: &["noscript", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "RPOP",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "RPUSH",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "RPUSHX",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SADD",
        arity: -3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SCAN",
        arity: -2,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "SCARD",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SDIFF",
        arity: -2,
        flags: &["readonly", "sort_for_script"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "SDIFFSTORE",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "SET",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SETBIT",
        arity: 4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SETEX",
        arity: 4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SETNX",
        arity: 3,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SETRANGE",
        arity: 4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SINTER",
        arity: -2,
        flags: &["readonly", "sort_for_script"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "SINTERCARD",
        arity: -3,
        flags: &["readonly"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "SINTERSTORE",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "SISMEMBER",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SLOWLOG",
        arity: -2,
        flags: &["admin", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "SMEMBERS",
        arity: 2,
        flags: &["readonly", "sort_for_script"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SMISMEMBER",
        arity: -3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SMOVE",
        arity: 4,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 2,
        step: 1,
    },
    CommandEntry {
        name: "SORT",
        arity: -2,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SPOP",
        arity: -2,
        flags: &["write", "random", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SRANDMEMBER",
        arity: -2,
        flags: &["readonly", "random"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SREM",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SSCAN",
        arity: -3,
        flags: &["readonly", "random"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "STRLEN",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "SUBSCRIBE",
        arity: -2,
        flags: &["pubsub", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "SUNION",
        arity: -2,
        flags: &["readonly", "sort_for_script"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "SUNIONSTORE",
        arity: -3,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "TIME",
        arity: 1,
        flags: &["random", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "TOUCH",
        arity: -2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "TTL",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "TYPE",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "UNLINK",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "UNSUBSCRIBE",
        arity: -1,
        flags: &["pubsub", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "UNWATCH",
        arity: 1,
        flags: &["noscript", "loading", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "WAIT",
        arity: 3,
        flags: &["noscript"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "WATCH",
        arity: -2,
        flags: &["noscript", "loading", "fast"],
        first_key: 1,
        last_key: -1,
        step: 1,
    },
    CommandEntry {
        name: "ZADD",
        arity: -4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZCARD",
        arity: 2,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZCOUNT",
        arity: 4,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZDIFF",
        arity: -3,
        flags: &["readonly", "sort_for_script"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "ZDIFFSTORE",
        arity: -4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZINCRBY",
        arity: 4,
        flags: &["write", "denyoom", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZINTER",
        arity: -3,
        flags: &["readonly", "sort_for_script"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "ZINTERSTORE",
        arity: -4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZLEXCOUNT",
        arity: 4,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZMPOP",
        arity: -4,
        flags: &["write", "fast"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "ZPOPMAX",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZPOPMIN",
        arity: -2,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZRANDMEMBER",
        arity: -2,
        flags: &["readonly", "random"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZRANGE",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZRANGEBYSCORE",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZRANK",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZREM",
        arity: -3,
        flags: &["write", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZREVRANGE",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZREVRANGEBYSCORE",
        arity: -4,
        flags: &["readonly"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZREVRANK",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZSCAN",
        arity: -3,
        flags: &["readonly", "random"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZSCORE",
        arity: 3,
        flags: &["readonly", "fast"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
    CommandEntry {
        name: "ZUNION",
        arity: -3,
        flags: &["readonly", "sort_for_script"],
        first_key: 0,
        last_key: 0,
        step: 0,
    },
    CommandEntry {
        name: "ZUNIONSTORE",
        arity: -4,
        flags: &["write", "denyoom"],
        first_key: 1,
        last_key: 1,
        step: 1,
    },
];
