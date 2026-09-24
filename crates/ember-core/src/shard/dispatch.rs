//! Runs a request against the keyspace: the one place each shard request
//! maps to a keyspace operation.

use super::*;

/// Converts an `IncrError` result into a `ShardResponse::Integer`.
fn incr_result(result: Result<i64, IncrError>) -> ShardResponse {
    match result {
        Ok(val) => ShardResponse::Integer(val),
        Err(IncrError::WrongType) => ShardResponse::WrongType,
        Err(IncrError::OutOfMemory) => ShardResponse::OutOfMemory,
        Err(e) => ShardResponse::Err(e.to_string()),
    }
}

/// Converts a `WriteError` result into a `ShardResponse::Len`.
fn write_result_len(result: Result<usize, WriteError>) -> ShardResponse {
    match result {
        Ok(len) => ShardResponse::Len(len),
        Err(WriteError::WrongType) => ShardResponse::WrongType,
        Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
    }
}

fn store_set_response(result: Result<(usize, Vec<String>), WriteError>) -> ShardResponse {
    match result {
        Ok((count, members)) => ShardResponse::SetStoreResult { count, members },
        Err(WriteError::WrongType) => ShardResponse::WrongType,
        Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
    }
}

/// Largest string SETRANGE and SETBIT may build: 512 MB, the same as the
/// largest bulk string a client can send.
const MAX_STRING_LEN: usize = 512 * 1024 * 1024;

/// Largest reply SRANDMEMBER, ZRANDMEMBER and HRANDFIELD may return. A
/// negative count allows repeats, so the collection size does not bound it.
const MAX_RANDOM_COUNT: u64 = 10_000_000;

/// Returns an error message for requests whose arguments would make the shard
/// allocate without bound. Both RESP and gRPC build shard requests, so the
/// limits live here.
fn limit_error(req: &ShardRequest) -> Option<&'static str> {
    let msg = match req {
        ShardRequest::SetRange { offset, value, .. }
            if offset.saturating_add(value.len()) > MAX_STRING_LEN =>
        {
            "ERR string exceeds maximum allowed size (512MB)"
        }
        ShardRequest::SetBit { offset, .. } if *offset >= MAX_STRING_LEN as u64 * 8 => {
            "ERR bit offset is not an integer or out of range"
        }
        ShardRequest::SRandMember { count, .. }
        | ShardRequest::ZRandMember {
            count: Some(count), ..
        }
        | ShardRequest::HRandField {
            count: Some(count), ..
        } if count.unsigned_abs() > MAX_RANDOM_COUNT => "ERR value is out of range",
        _ => return None,
    };
    Some(msg)
}

/// Routes a request to the appropriate keyspace operation and returns a response.
///
/// This is the hot path — every read and write goes through here.
pub(super) fn dispatch(
    ks: &mut Keyspace,
    req: &mut ShardRequest,
    #[cfg(feature = "protobuf")] schema_registry: &Option<crate::schema::SharedSchemaRegistry>,
) -> ShardResponse {
    if let Some(msg) = limit_error(req) {
        return ShardResponse::Err(msg.into());
    }
    match req {
        ShardRequest::Get { key } => match ks.get_string(key) {
            Ok(val) => ShardResponse::Value(val.map(Value::String)),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::Set {
            key,
            value,
            expire,
            nx,
            xx,
        } => match ks.set(key.clone(), value.clone(), *expire, *nx, *xx) {
            SetResult::Ok => ShardResponse::Ok,
            SetResult::Blocked => ShardResponse::Value(None),
            SetResult::OutOfMemory => ShardResponse::OutOfMemory,
        },
        ShardRequest::Incr { key } => incr_result(ks.incr(key)),
        ShardRequest::Decr { key } => incr_result(ks.decr(key)),
        ShardRequest::IncrBy { key, delta } => incr_result(ks.incr_by(key, *delta)),
        ShardRequest::DecrBy { key, delta } => match delta.checked_neg() {
            Some(neg) => incr_result(ks.incr_by(key, neg)),
            None => ShardResponse::Err("ERR increment or decrement would overflow".into()),
        },
        ShardRequest::IncrByFloat { key, delta } => match ks.incr_by_float(key, *delta) {
            Ok(val) => ShardResponse::BulkString(val),
            Err(IncrFloatError::WrongType) => ShardResponse::WrongType,
            Err(IncrFloatError::OutOfMemory) => ShardResponse::OutOfMemory,
            Err(e) => ShardResponse::Err(e.to_string()),
        },
        ShardRequest::Append { key, value } => write_result_len(ks.append(key, value)),
        ShardRequest::Strlen { key } => match ks.strlen(key) {
            Ok(len) => ShardResponse::Len(len),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::GetRange { key, start, end } => match ks.getrange(key, *start, *end) {
            Ok(data) => ShardResponse::Value(Some(Value::String(data))),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SetRange { key, offset, value } => {
            write_result_len(ks.setrange(key, *offset, value))
        }
        ShardRequest::GetBit { key, offset } => match ks.getbit(key, *offset) {
            Ok(bit) => ShardResponse::Integer(bit as i64),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SetBit { key, offset, value } => match ks.setbit(key, *offset, *value) {
            Ok(old_bit) => ShardResponse::Integer(old_bit as i64),
            Err(WriteError::WrongType) => ShardResponse::WrongType,
            Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
        },
        ShardRequest::BitCount { key, range } => match ks.bitcount(key, *range) {
            Ok(count) => ShardResponse::Integer(count as i64),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::BitPos { key, bit, range } => match ks.bitpos(key, *bit, *range) {
            Ok(pos) => ShardResponse::Integer(pos),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::BitOp { op, dest, keys } => match ks.bitop(*op, dest.clone(), keys) {
            Ok(len) => ShardResponse::Integer(len as i64),
            Err(WriteError::WrongType) => ShardResponse::WrongType,
            Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
        },
        ShardRequest::Keys { pattern } => {
            let keys = ks.keys(pattern);
            ShardResponse::StringArray(keys)
        }
        ShardRequest::Rename { key, newkey } => {
            use crate::keyspace::RenameError;
            match ks.rename(key, newkey) {
                Ok(()) => ShardResponse::Ok,
                Err(RenameError::NoSuchKey) => ShardResponse::Err("ERR no such key".into()),
            }
        }
        ShardRequest::Copy {
            source,
            destination,
            replace,
        } => {
            use crate::keyspace::CopyError;
            match ks.copy(source, destination, *replace) {
                Ok(copied) => ShardResponse::Bool(copied),
                Err(CopyError::NoSuchKey) => ShardResponse::Err("ERR no such key".into()),
                Err(CopyError::OutOfMemory) => ShardResponse::OutOfMemory,
            }
        }
        ShardRequest::ObjectEncoding { key } => ShardResponse::EncodingName(ks.encoding(key)),
        ShardRequest::Del { key } => ShardResponse::Bool(ks.del(key)),
        ShardRequest::Unlink { key } => ShardResponse::Bool(ks.unlink(key)),
        ShardRequest::Exists { key } => ShardResponse::Bool(ks.exists(key)),
        ShardRequest::RandomKey => match ks.random_key() {
            Some(k) => ShardResponse::StringArray(vec![k]),
            None => ShardResponse::StringArray(vec![]),
        },
        ShardRequest::Touch { key } => ShardResponse::Bool(ks.touch(key)),
        ShardRequest::Sort {
            key,
            desc,
            alpha,
            limit,
        } => match ks.sort(key, *desc, *alpha, *limit) {
            Ok(items) => ShardResponse::Array(items),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::Expire { key, seconds } => ShardResponse::Bool(ks.expire(key, *seconds)),
        ShardRequest::Expireat { key, timestamp } => {
            ShardResponse::Bool(ks.expireat(key, *timestamp))
        }
        ShardRequest::Ttl { key } => ShardResponse::Ttl(ks.ttl(key)),
        ShardRequest::MemoryUsage { key } => {
            ShardResponse::Integer(ks.memory_usage(key).map(|n| n as i64).unwrap_or(-1))
        }
        ShardRequest::Persist { key } => ShardResponse::Bool(ks.persist(key)),
        ShardRequest::Pttl { key } => ShardResponse::Ttl(ks.pttl(key)),
        ShardRequest::Pexpire { key, milliseconds } => {
            ShardResponse::Bool(ks.pexpire(key, *milliseconds))
        }
        ShardRequest::Pexpireat { key, timestamp_ms } => {
            ShardResponse::Bool(ks.pexpireat(key, *timestamp_ms))
        }
        ShardRequest::LPush { key, values } => write_result_len(ks.lpush(key, values)),
        ShardRequest::RPush { key, values } => write_result_len(ks.rpush(key, values)),
        ShardRequest::LPop { key } => match ks.lpop(key) {
            Ok(val) => ShardResponse::Value(val.map(Value::String)),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::RPop { key } => match ks.rpop(key) {
            Ok(val) => ShardResponse::Value(val.map(Value::String)),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::LPopCount { key, count } => match ks.lpop_count(key, *count) {
            Ok(Some(items)) => ShardResponse::Array(items),
            Ok(None) => ShardResponse::Value(None),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::RPopCount { key, count } => match ks.rpop_count(key, *count) {
            Ok(Some(items)) => ShardResponse::Array(items),
            Ok(None) => ShardResponse::Value(None),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::LRange { key, start, stop } => match ks.lrange(key, *start, *stop) {
            Ok(items) => ShardResponse::Array(items),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::LLen { key } => match ks.llen(key) {
            Ok(len) => ShardResponse::Len(len),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::LIndex { key, index } => match ks.lindex(key, *index) {
            Ok(val) => ShardResponse::Value(val.map(Value::String)),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::LSet { key, index, value } => match ks.lset(key, *index, value.clone()) {
            Ok(()) => ShardResponse::Ok,
            Err(e) => match e {
                LsetError::WrongType => ShardResponse::WrongType,
                LsetError::NoSuchKey => ShardResponse::Err("ERR no such key".into()),
                LsetError::IndexOutOfRange => ShardResponse::Err("ERR index out of range".into()),
            },
        },
        ShardRequest::LTrim { key, start, stop } => match ks.ltrim(key, *start, *stop) {
            Ok(()) => ShardResponse::Ok,
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::LInsert {
            key,
            before,
            pivot,
            value,
        } => match ks.linsert(key, *before, pivot, value.clone()) {
            Ok(n) => ShardResponse::Integer(n),
            Err(WriteError::WrongType) => ShardResponse::WrongType,
            Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
        },
        ShardRequest::LRem { key, count, value } => match ks.lrem(key, *count, value) {
            Ok(n) => ShardResponse::Len(n),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::LPos {
            key,
            element,
            rank,
            count,
            maxlen,
        } => match ks.lpos(key, element, *rank, *count, *maxlen) {
            Ok(positions) => ShardResponse::IntegerArray(positions),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::Type { key } => ShardResponse::TypeName(ks.value_type(key)),
        ShardRequest::ZAdd {
            key,
            members,
            nx,
            xx,
            gt,
            lt,
            ch,
        } => {
            let flags = ZAddFlags {
                nx: *nx,
                xx: *xx,
                gt: *gt,
                lt: *lt,
                ch: *ch,
            };
            match ks.zadd(key, members, &flags) {
                Ok(result) => ShardResponse::ZAddLen {
                    count: result.count,
                    applied: result.applied,
                },
                Err(WriteError::WrongType) => ShardResponse::WrongType,
                Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
            }
        }
        ShardRequest::ZRem { key, members } => match ks.zrem(key, members) {
            Ok(removed) => ShardResponse::ZRemLen {
                count: removed.len(),
                removed,
            },
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZScore { key, member } => match ks.zscore(key, member) {
            Ok(score) => ShardResponse::Score(score),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZRank { key, member } => match ks.zrank(key, member) {
            Ok(rank) => ShardResponse::Rank(rank),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZCard { key } => match ks.zcard(key) {
            Ok(len) => ShardResponse::Len(len),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZRevRank { key, member } => match ks.zrevrank(key, member) {
            Ok(rank) => ShardResponse::Rank(rank),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZRange {
            key, start, stop, ..
        } => match ks.zrange(key, *start, *stop) {
            Ok(items) => ShardResponse::ScoredArray(items),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZRevRange {
            key, start, stop, ..
        } => match ks.zrevrange(key, *start, *stop) {
            Ok(items) => ShardResponse::ScoredArray(items),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZCount { key, min, max } => match ks.zcount(key, *min, *max) {
            Ok(count) => ShardResponse::Len(count),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZIncrBy {
            key,
            increment,
            member,
        } => match ks.zincrby(key, *increment, member) {
            Ok(new_score) => ShardResponse::ZIncrByResult {
                new_score,
                member: member.clone(),
            },
            Err(WriteError::WrongType) => ShardResponse::WrongType,
            Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
        },
        ShardRequest::ZRangeByScore {
            key,
            min,
            max,
            offset,
            count,
        } => match ks.zrangebyscore(key, *min, *max, *offset, *count) {
            Ok(items) => ShardResponse::ScoredArray(items),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZRevRangeByScore {
            key,
            min,
            max,
            offset,
            count,
        } => match ks.zrevrangebyscore(key, *min, *max, *offset, *count) {
            Ok(items) => ShardResponse::ScoredArray(items),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZPopMin { key, count } => match ks.zpopmin(key, *count) {
            Ok(items) => ShardResponse::ZPopResult(items),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZPopMax { key, count } => match ks.zpopmax(key, *count) {
            Ok(items) => ShardResponse::ZPopResult(items),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::LmpopSingle { key, left, count } => {
            let result = if *left {
                ks.lpop_count(key, *count)
            } else {
                ks.rpop_count(key, *count)
            };
            match result {
                Ok(Some(items)) => ShardResponse::Array(items),
                Ok(None) => ShardResponse::Value(None),
                Err(_) => ShardResponse::WrongType,
            }
        }
        ShardRequest::ZmpopSingle { key, min, count } => {
            let result = if *min {
                ks.zpopmin(key, *count)
            } else {
                ks.zpopmax(key, *count)
            };
            match result {
                Ok(items) if !items.is_empty() => ShardResponse::ZPopResult(items),
                Ok(_) => ShardResponse::Value(None),
                Err(_) => ShardResponse::WrongType,
            }
        }
        ShardRequest::DbSize => ShardResponse::KeyCount(ks.len()),
        ShardRequest::Stats => ShardResponse::Stats(ks.stats()),
        ShardRequest::KeyVersion { ref key } => ShardResponse::Version(ks.key_version(key)),
        ShardRequest::FlushDb => {
            ks.clear();
            ShardResponse::Ok
        }
        ShardRequest::Scan {
            cursor,
            count,
            pattern,
        } => {
            let (next_cursor, keys) = ks.scan_keys(*cursor, *count, pattern.as_deref());
            ShardResponse::Scan {
                cursor: next_cursor,
                keys,
            }
        }
        ShardRequest::HSet { key, fields } => write_result_len(ks.hset(key, fields)),
        ShardRequest::HGet { key, field } => match ks.hget(key, field) {
            Ok(val) => ShardResponse::Value(val.map(Value::String)),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HGetAll { key } => match ks.hgetall(key) {
            Ok(fields) => ShardResponse::HashFields(fields),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HDel { key, fields } => match ks.hdel(key, fields) {
            Ok(removed) => ShardResponse::HDelLen {
                count: removed.len(),
                removed,
            },
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HExists { key, field } => match ks.hexists(key, field) {
            Ok(exists) => ShardResponse::Bool(exists),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HLen { key } => match ks.hlen(key) {
            Ok(len) => ShardResponse::Len(len),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HIncrBy { key, field, delta } => incr_result(ks.hincrby(key, field, *delta)),
        ShardRequest::HIncrByFloat { key, field, delta } => {
            match ks.hincrbyfloat(key, field, *delta) {
                Ok(val) => ShardResponse::BulkString(val),
                Err(IncrFloatError::WrongType) => ShardResponse::WrongType,
                Err(IncrFloatError::OutOfMemory) => ShardResponse::OutOfMemory,
                Err(e) => ShardResponse::Err(e.to_string()),
            }
        }
        ShardRequest::HKeys { key } => match ks.hkeys(key) {
            Ok(keys) => ShardResponse::StringArray(keys),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HVals { key } => match ks.hvals(key) {
            Ok(vals) => ShardResponse::Array(vals),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HMGet { key, fields } => match ks.hmget(key, fields) {
            Ok(vals) => ShardResponse::OptionalArray(vals),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HRandField {
            key,
            count,
            with_values,
        } => match ks.hrandfield(key, *count, *with_values) {
            Ok(pairs) => ShardResponse::HRandFieldResult(pairs),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SAdd { key, members } => write_result_len(ks.sadd(key, members)),
        ShardRequest::SRem { key, members } => match ks.srem(key, members) {
            Ok(count) => ShardResponse::Len(count),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SMembers { key } => match ks.smembers(key) {
            Ok(members) => ShardResponse::StringArray(members),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SIsMember { key, member } => match ks.sismember(key, member) {
            Ok(exists) => ShardResponse::Bool(exists),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SCard { key } => match ks.scard(key) {
            Ok(count) => ShardResponse::Len(count),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SUnion { keys } => match ks.sunion(keys) {
            Ok(members) => ShardResponse::StringArray(members),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SInter { keys } => match ks.sinter(keys) {
            Ok(members) => ShardResponse::StringArray(members),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SDiff { keys } => match ks.sdiff(keys) {
            Ok(members) => ShardResponse::StringArray(members),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SUnionStore { dest, keys } => store_set_response(ks.sunionstore(dest, keys)),
        ShardRequest::SInterStore { dest, keys } => store_set_response(ks.sinterstore(dest, keys)),
        ShardRequest::SDiffStore { dest, keys } => store_set_response(ks.sdiffstore(dest, keys)),
        ShardRequest::SRandMember { key, count } => match ks.srandmember(key, *count) {
            Ok(members) => ShardResponse::StringArray(members),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SPop { key, count } => match ks.spop(key, *count) {
            Ok(members) => ShardResponse::StringArray(members),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SMisMember { key, members } => match ks.smismember(key, members) {
            Ok(results) => ShardResponse::BoolArray(results),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SMove {
            source,
            destination,
            member,
        } => match ks.smove(source, destination, member) {
            Ok(moved) => ShardResponse::Bool(moved),
            Err(WriteError::WrongType) => ShardResponse::WrongType,
            Err(WriteError::OutOfMemory) => ShardResponse::OutOfMemory,
        },
        ShardRequest::SInterCard { keys, limit } => match ks.sintercard(keys, *limit) {
            Ok(n) => ShardResponse::Integer(n as i64),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::Expiretime { key } => ShardResponse::Integer(ks.expiretime(key)),
        ShardRequest::Pexpiretime { key } => ShardResponse::Integer(ks.pexpiretime(key)),
        ShardRequest::LMove {
            source,
            destination,
            src_left,
            dst_left,
        } => match ks.lmove(source, destination, *src_left, *dst_left) {
            Ok(Some(v)) => ShardResponse::Value(Some(Value::String(v))),
            Ok(None) => ShardResponse::Value(None),
            Err(e) => match e {
                WriteError::WrongType => ShardResponse::WrongType,
                WriteError::OutOfMemory => ShardResponse::OutOfMemory,
            },
        },
        ShardRequest::GetDel { key } => match ks.getdel(key) {
            Ok(Some(v)) => ShardResponse::Value(Some(Value::String(v))),
            Ok(None) => ShardResponse::Value(None),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::GetEx { key, expire } => {
            let dur = expire.map(|opt| opt.map(Duration::from_millis));
            match ks.getex(key, dur) {
                Ok(Some(v)) => ShardResponse::Value(Some(Value::String(v))),
                Ok(None) => ShardResponse::Value(None),
                Err(_) => ShardResponse::WrongType,
            }
        }
        ShardRequest::GetSet { key, value } => match ks.getset(key, value.clone()) {
            Ok(old) => ShardResponse::Value(old.map(Value::String)),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::MSetNx { pairs } => {
            let result = ks.msetnx(pairs);
            ShardResponse::Bool(result)
        }
        ShardRequest::ZDiff { keys } => match ks.zdiff(keys) {
            Ok(pairs) => ShardResponse::ScoredArray(pairs),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZInter { keys } => match ks.zinter(keys) {
            Ok(pairs) => ShardResponse::ScoredArray(pairs),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZUnion { keys } => match ks.zunion(keys) {
            Ok(pairs) => ShardResponse::ScoredArray(pairs),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZDiffStore { dest, keys } => match ks.zdiffstore(dest, keys) {
            Ok((count, members)) => ShardResponse::ZStoreResult { count, members },
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZInterStore { dest, keys } => match ks.zinterstore(dest, keys) {
            Ok((count, members)) => ShardResponse::ZStoreResult { count, members },
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZUnionStore { dest, keys } => match ks.zunionstore(dest, keys) {
            Ok((count, members)) => ShardResponse::ZStoreResult { count, members },
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZRandMember {
            key,
            count,
            with_scores,
        } => match ks.zrandmember(key, *count, *with_scores) {
            Ok(pairs) => ShardResponse::ZRandMemberResult(pairs),
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::SScan {
            key,
            cursor,
            count,
            pattern,
        } => match ks.scan_set(key, *cursor, *count, pattern.as_deref()) {
            Ok((next, members)) => {
                let items = members.into_iter().map(Bytes::from).collect();
                ShardResponse::CollectionScan {
                    cursor: next,
                    items,
                }
            }
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::HScan {
            key,
            cursor,
            count,
            pattern,
        } => match ks.scan_hash(key, *cursor, *count, pattern.as_deref()) {
            Ok((next, fields)) => {
                let mut items = Vec::with_capacity(fields.len() * 2);
                for (field, value) in fields {
                    items.push(Bytes::from(field));
                    items.push(value);
                }
                ShardResponse::CollectionScan {
                    cursor: next,
                    items,
                }
            }
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::ZScan {
            key,
            cursor,
            count,
            pattern,
        } => match ks.scan_sorted_set(key, *cursor, *count, pattern.as_deref()) {
            Ok((next, members)) => {
                let mut items = Vec::with_capacity(members.len() * 2);
                for (member, score) in members {
                    items.push(Bytes::from(member));
                    items.push(Bytes::from(score.to_string()));
                }
                ShardResponse::CollectionScan {
                    cursor: next,
                    items,
                }
            }
            Err(_) => ShardResponse::WrongType,
        },
        ShardRequest::CountKeysInSlot { slot } => {
            ShardResponse::KeyCount(ks.count_keys_in_slot(*slot))
        }
        ShardRequest::GetKeysInSlot { slot, count } => {
            ShardResponse::StringArray(ks.get_keys_in_slot(*slot, *count))
        }
        ShardRequest::DumpKey { key } => match ks.dump(key) {
            Some((value, ttl_ms)) => {
                let snap = persistence::value_to_snap(value);
                match snapshot::serialize_snap_value(&snap) {
                    Ok(data) => ShardResponse::KeyDump { data, ttl_ms },
                    Err(e) => ShardResponse::Err(format!("ERR snapshot serialization failed: {e}")),
                }
            }
            None => ShardResponse::Value(None),
        },
        ShardRequest::RestoreKey {
            key,
            ttl_ms,
            data,
            replace,
        } => match snapshot::deserialize_snap_value(data) {
            Ok(snap) => {
                let exists = ks.exists(key);
                if exists && !*replace {
                    ShardResponse::Err("ERR Target key name already exists".into())
                } else {
                    let value = persistence::snap_to_value(snap);
                    let ttl = if *ttl_ms == 0 {
                        None
                    } else {
                        Some(Duration::from_millis(*ttl_ms))
                    };
                    ks.restore(key.clone(), value, ttl);
                    ShardResponse::Ok
                }
            }
            Err(e) => ShardResponse::Err(format!("ERR DUMP payload corrupted: {e}")),
        },
        #[cfg(feature = "vector")]
        ShardRequest::VAdd {
            key,
            element,
            vector,
            metric,
            quantization,
            connectivity,
            expansion_add,
        } => {
            use crate::types::vector::{DistanceMetric, QuantizationType};
            match ks.vadd(
                key,
                element.clone(),
                vector.clone(),
                DistanceMetric::from_u8(*metric),
                QuantizationType::from_u8(*quantization),
                *connectivity as usize,
                *expansion_add as usize,
            ) {
                Ok(result) => ShardResponse::VAddResult {
                    element: result.element,
                    vector: result.vector,
                    added: result.added,
                },
                Err(crate::keyspace::VectorWriteError::WrongType) => ShardResponse::WrongType,
                Err(crate::keyspace::VectorWriteError::OutOfMemory) => ShardResponse::OutOfMemory,
                Err(crate::keyspace::VectorWriteError::IndexError(e))
                | Err(crate::keyspace::VectorWriteError::PartialBatch { message: e, .. }) => {
                    ShardResponse::Err(format!("ERR vector index: {e}"))
                }
            }
        }
        #[cfg(feature = "vector")]
        ShardRequest::VAddBatch {
            key,
            entries,
            metric,
            quantization,
            connectivity,
            expansion_add,
            ..
        } => {
            use crate::types::vector::{DistanceMetric, QuantizationType};
            // take ownership of entries to avoid cloning vectors during
            // batch insertion. the entries vec in the request becomes empty,
            // which is fine because to_aof_records uses response.applied
            // instead of request.entries.
            let owned_entries = std::mem::take(entries);
            match ks.vadd_batch(
                key,
                owned_entries,
                DistanceMetric::from_u8(*metric),
                QuantizationType::from_u8(*quantization),
                *connectivity as usize,
                *expansion_add as usize,
            ) {
                Ok(result) => ShardResponse::VAddBatchResult {
                    added_count: result.added_count,
                    applied: result.applied,
                },
                Err(crate::keyspace::VectorWriteError::WrongType) => ShardResponse::WrongType,
                Err(crate::keyspace::VectorWriteError::OutOfMemory) => ShardResponse::OutOfMemory,
                Err(crate::keyspace::VectorWriteError::IndexError(e)) => {
                    ShardResponse::Err(format!("ERR vector index: {e}"))
                }
                Err(crate::keyspace::VectorWriteError::PartialBatch { applied, .. }) => {
                    // partial success: return applied vectors for AOF persistence
                    ShardResponse::VAddBatchResult {
                        added_count: applied.len(),
                        applied,
                    }
                }
            }
        }
        #[cfg(feature = "vector")]
        ShardRequest::VSim {
            key,
            query,
            count,
            ef_search,
        } => match ks.vsim(key, query, *count, *ef_search) {
            Ok(results) => ShardResponse::VSimResult(
                results
                    .into_iter()
                    .map(|r| (r.element, r.distance))
                    .collect(),
            ),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "vector")]
        ShardRequest::VRem { key, element } => match ks.vrem(key, element) {
            Ok(removed) => ShardResponse::Bool(removed),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "vector")]
        ShardRequest::VGet { key, element } => match ks.vget(key, element) {
            Ok(data) => ShardResponse::VectorData(data),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "vector")]
        ShardRequest::VCard { key } => match ks.vcard(key) {
            Ok(count) => ShardResponse::Integer(count as i64),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "vector")]
        ShardRequest::VDim { key } => match ks.vdim(key) {
            Ok(dim) => ShardResponse::Integer(dim as i64),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "vector")]
        ShardRequest::VInfo { key } => match ks.vinfo(key) {
            Ok(Some(info)) => {
                let fields = vec![
                    ("dim".to_owned(), info.dim.to_string()),
                    ("count".to_owned(), info.count.to_string()),
                    ("metric".to_owned(), info.metric.to_string()),
                    ("quantization".to_owned(), info.quantization.to_string()),
                    ("connectivity".to_owned(), info.connectivity.to_string()),
                    ("expansion_add".to_owned(), info.expansion_add.to_string()),
                ];
                ShardResponse::VectorInfo(Some(fields))
            }
            Ok(None) => ShardResponse::VectorInfo(None),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoSet {
            key,
            type_name,
            data,
            expire,
            nx,
            xx,
        } => {
            if *nx && ks.exists(key) {
                return ShardResponse::Value(None);
            }
            if *xx && !ks.exists(key) {
                return ShardResponse::Value(None);
            }
            match ks.proto_set(key.clone(), type_name.clone(), data.clone(), *expire) {
                SetResult::Ok | SetResult::Blocked => ShardResponse::Ok,
                SetResult::OutOfMemory => ShardResponse::OutOfMemory,
            }
        }
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoGet { key } => match ks.proto_get(key) {
            Ok(val) => ShardResponse::ProtoValue(val),
            Err(_) => ShardResponse::WrongType,
        },
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoType { key } => match ks.proto_type(key) {
            Ok(name) => ShardResponse::ProtoTypeName(name),
            Err(_) => ShardResponse::WrongType,
        },
        // ProtoRegisterAof is a no-op for the keyspace — the AOF record
        // is written by the to_aof_record path after dispatch returns Ok.
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoRegisterAof { .. } => ShardResponse::Ok,
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoSetField {
            key,
            field_path,
            value,
        } => dispatch_proto_field_op(ks, schema_registry, key, |reg, type_name, data, ttl| {
            let new_data = reg.set_field(type_name, data, field_path, value)?;
            Ok(ShardResponse::ProtoFieldUpdated {
                type_name: type_name.to_owned(),
                data: new_data,
                expire: ttl,
            })
        }),
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoDelField { key, field_path } => {
            dispatch_proto_field_op(ks, schema_registry, key, |reg, type_name, data, ttl| {
                let new_data = reg.clear_field(type_name, data, field_path)?;
                Ok(ShardResponse::ProtoFieldUpdated {
                    type_name: type_name.to_owned(),
                    data: new_data,
                    expire: ttl,
                })
            })
        }
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoScan {
            cursor,
            count,
            pattern,
            type_name,
        } => {
            let (next_cursor, keys) =
                ks.scan_proto_keys(*cursor, *count, pattern.as_deref(), type_name.as_deref());
            ShardResponse::Scan {
                cursor: next_cursor,
                keys,
            }
        }
        #[cfg(feature = "protobuf")]
        ShardRequest::ProtoFind {
            cursor,
            count,
            pattern,
            type_name,
            field_path,
            field_value,
        } => {
            let registry = match schema_registry {
                Some(r) => r,
                None => return ShardResponse::Err("protobuf support is not enabled".into()),
            };
            let reg = match registry.read() {
                Ok(r) => r,
                Err(_) => return ShardResponse::Err("schema registry lock poisoned".into()),
            };
            let (next_cursor, keys) = ks.scan_proto_find(
                crate::keyspace::ProtoFindOpts {
                    cursor: *cursor,
                    count: *count,
                    pattern: pattern.as_deref(),
                    type_name: type_name.as_deref(),
                    field_path,
                    field_value,
                },
                &reg,
            );
            ShardResponse::Scan {
                cursor: next_cursor,
                keys,
            }
        }
        // these requests are intercepted in process_message, not handled here
        ShardRequest::Snapshot
        | ShardRequest::SerializeSnapshot
        | ShardRequest::RewriteAof
        | ShardRequest::FlushDbAsync
        | ShardRequest::UpdateMemoryConfig { .. }
        | ShardRequest::BLPop { .. }
        | ShardRequest::BRPop { .. } => ShardResponse::Ok,
    }
}

/// Shared logic for atomic proto field operations (SETFIELD/DELFIELD).
///
/// Reads the proto value, acquires the schema registry, calls the
/// provided mutation closure, then writes the result back to the keyspace
/// — all within the single-threaded shard dispatch.
#[cfg(feature = "protobuf")]
fn dispatch_proto_field_op<F>(
    ks: &mut Keyspace,
    schema_registry: &Option<crate::schema::SharedSchemaRegistry>,
    key: &str,
    mutate: F,
) -> ShardResponse
where
    F: FnOnce(
        &crate::schema::SchemaRegistry,
        &str,
        &[u8],
        Option<Duration>,
    ) -> Result<ShardResponse, crate::schema::SchemaError>,
{
    let registry = match schema_registry {
        Some(r) => r,
        None => return ShardResponse::Err("protobuf support is not enabled".into()),
    };

    let (type_name, data, remaining_ttl) = match ks.proto_get(key) {
        Ok(Some(tuple)) => tuple,
        Ok(None) => return ShardResponse::Value(None),
        Err(_) => return ShardResponse::WrongType,
    };

    let reg = match registry.read() {
        Ok(r) => r,
        Err(_) => return ShardResponse::Err("schema registry lock poisoned".into()),
    };

    let resp = match mutate(&reg, &type_name, &data, remaining_ttl) {
        Ok(r) => r,
        Err(e) => return ShardResponse::Err(e.to_string()),
    };

    // write the updated value back, preserving the original TTL
    if let ShardResponse::ProtoFieldUpdated {
        ref type_name,
        ref data,
        expire,
    } = resp
    {
        ks.proto_set(key.to_owned(), type_name.clone(), data.clone(), expire);
    }

    resp
}
