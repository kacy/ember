//! The AOF record type and its binary encoding.

use std::io;

use bytes::Bytes;

use crate::format::{self, FormatError};

/// Reads a length-prefixed field and decodes it as UTF-8.
fn read_string(r: &mut impl io::Read, field: &str) -> Result<String, FormatError> {
    let bytes = format::read_bytes(r)?;
    String::from_utf8(bytes).map_err(|_| {
        FormatError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} is not valid utf-8"),
        ))
    })
}

/// Reads a count-prefixed list of strings: `[count: u32][string]*`.
/// Used by SADD, SREM, HDEL, and ZREM deserialization.
fn read_string_list(r: &mut impl io::Read, field: &str) -> Result<Vec<String>, FormatError> {
    let count = format::read_u32(r)?;
    format::validate_collection_count(count, field)?;
    let mut items = Vec::with_capacity(format::capped_capacity(count));
    for _ in 0..count {
        items.push(read_string(r, field)?);
    }
    Ok(items)
}

/// Reads a count-prefixed list of raw byte blobs: `[count: u32][bytes]*`.
/// Used by LPUSH and RPUSH deserialization.
fn read_bytes_list(r: &mut impl io::Read, label: &str) -> Result<Vec<Bytes>, FormatError> {
    let count = format::read_u32(r)?;
    format::validate_collection_count(count, label)?;
    let mut items = Vec::with_capacity(format::capped_capacity(count));
    for _ in 0..count {
        items.push(Bytes::from(format::read_bytes(r)?));
    }
    Ok(items)
}

// -- record tags --
// values are stable and must not change (on-disk format).

// string
pub(super) const TAG_SET: u8 = 1;
const TAG_INCR: u8 = 12;
const TAG_DECR: u8 = 13;
const TAG_INCRBY: u8 = 19;
const TAG_DECRBY: u8 = 20;
const TAG_APPEND: u8 = 21;

// list
const TAG_LPUSH: u8 = 4;
const TAG_RPUSH: u8 = 5;
const TAG_LPOP: u8 = 6;
const TAG_RPOP: u8 = 7;

// sorted set
const TAG_ZADD: u8 = 8;
const TAG_ZREM: u8 = 9;

// hash
const TAG_HSET: u8 = 14;
const TAG_HDEL: u8 = 15;
const TAG_HINCRBY: u8 = 16;

// set
const TAG_SADD: u8 = 17;
const TAG_SREM: u8 = 18;

// key lifecycle
const TAG_DEL: u8 = 2;
const TAG_EXPIRE: u8 = 3;
const TAG_PERSIST: u8 = 10;
const TAG_PEXPIRE: u8 = 11;
const TAG_PEXPIREAT: u8 = 35;
const TAG_RENAME: u8 = 22;
const TAG_COPY: u8 = 27;
const TAG_LSET: u8 = 28;
const TAG_LTRIM: u8 = 29;
const TAG_LINSERT: u8 = 30;
const TAG_LREM: u8 = 31;
const TAG_SETRANGE: u8 = 32;

// bitmap
const TAG_SETBIT: u8 = 33;
const TAG_BITOP: u8 = 34;

// whole keyspace and serialized values
const TAG_FLUSH_ALL: u8 = 36;
const TAG_RESTORE: u8 = 37;
const TAG_SET_EXPIRE_AT: u8 = 38;
const TAG_CHECKPOINT: u8 = 39;

// vector
#[cfg(feature = "vector")]
const TAG_VADD: u8 = 25;
#[cfg(feature = "vector")]
const TAG_VREM: u8 = 26;

// protobuf
#[cfg(feature = "protobuf")]
const TAG_PROTO_SET: u8 = 23;
#[cfg(feature = "protobuf")]
const TAG_PROTO_REGISTER: u8 = 24;

/// A single mutation record stored in the AOF.
#[derive(Debug, Clone, PartialEq)]
pub enum AofRecord {
    /// SET key value \[expire_ms\]. expire_ms is -1 for no expiration.
    Set {
        key: String,
        value: Bytes,
        expire_ms: i64,
    },
    /// DEL key.
    Del { key: String },
    /// EXPIRE key seconds.
    Expire { key: String, seconds: u64 },
    /// LPUSH key value [value ...].
    LPush { key: String, values: Vec<Bytes> },
    /// RPUSH key value [value ...].
    RPush { key: String, values: Vec<Bytes> },
    /// LPOP key.
    LPop { key: String },
    /// RPOP key.
    RPop { key: String },
    /// LSET key index element.
    LSet {
        key: String,
        index: i64,
        value: Bytes,
    },
    /// LTRIM key start stop.
    LTrim { key: String, start: i64, stop: i64 },
    /// LINSERT key BEFORE|AFTER pivot element.
    LInsert {
        key: String,
        before: bool,
        pivot: Bytes,
        value: Bytes,
    },
    /// LREM key count element.
    LRem {
        key: String,
        count: i64,
        value: Bytes,
    },
    /// ZADD key score member [score member ...].
    ZAdd {
        key: String,
        members: Vec<(f64, String)>,
    },
    /// ZREM key member [member ...].
    ZRem { key: String, members: Vec<String> },
    /// PERSIST key — remove expiration.
    Persist { key: String },
    /// PEXPIRE key milliseconds.
    Pexpire { key: String, milliseconds: u64 },
    /// PEXPIREAT key timestamp-ms — set expiry at an absolute Unix timestamp (ms).
    ///
    /// Used to persist EXPIREAT and PEXPIREAT commands so that after recovery
    /// the expiry deadline is the same absolute point in time rather than
    /// being re-anchored to the moment of replay.
    Pexpireat { key: String, timestamp_ms: u64 },
    /// INCR key.
    Incr { key: String },
    /// DECR key.
    Decr { key: String },
    /// HSET key field value [field value ...].
    HSet {
        key: String,
        fields: Vec<(String, Bytes)>,
    },
    /// HDEL key field [field ...].
    HDel { key: String, fields: Vec<String> },
    /// HINCRBY key field delta.
    HIncrBy {
        key: String,
        field: String,
        delta: i64,
    },
    /// SADD key member [member ...].
    SAdd { key: String, members: Vec<String> },
    /// SREM key member [member ...].
    SRem { key: String, members: Vec<String> },
    /// INCRBY key delta.
    IncrBy { key: String, delta: i64 },
    /// DECRBY key delta.
    DecrBy { key: String, delta: i64 },
    /// APPEND key value.
    Append { key: String, value: Bytes },
    /// SETRANGE key offset value.
    SetRange {
        key: String,
        offset: usize,
        value: Bytes,
    },
    /// SETBIT key offset value. Replays the bit mutation verbatim.
    SetBit { key: String, offset: u64, value: u8 },
    /// BITOP op destkey key [key ...]. Replays the bitwise operation.
    ///
    /// `op` is stored as a raw byte: 0=AND, 1=OR, 2=XOR, 3=NOT.
    BitOp {
        op: u8,
        dest: String,
        keys: Vec<String>,
    },
    /// RENAME key newkey.
    Rename { key: String, newkey: String },
    /// SET with an expiry, stored as an absolute unix time in ms. The plain
    /// `Set` record stores the TTL left at write time, which replay counts
    /// from the restart instead of from the write.
    SetExpireAt {
        key: String,
        value: Bytes,
        timestamp_ms: u64,
    },
    /// FLUSHDB or FLUSHALL. Removes every key in the shard.
    FlushAll,
    /// The first record after a snapshot truncates the AOF. Names that
    /// snapshot by its footer CRC, so recovery can tell whether this AOF
    /// continues the snapshot on disk or came before it.
    Checkpoint { snapshot_crc: u32 },
    /// RESTORE key ttl payload. `data` is the value in the snapshot value
    /// encoding, as carried by the RESTORE request. `ttl_ms` is 0 for no
    /// expiry.
    Restore {
        key: String,
        ttl_ms: u64,
        data: Bytes,
    },
    /// COPY source destination [REPLACE].
    Copy {
        source: String,
        destination: String,
        replace: bool,
    },
    /// VADD key element vector [metric quant connectivity expansion_add].
    /// Stores the full index config so recovery can recreate the set.
    #[cfg(feature = "vector")]
    VAdd {
        key: String,
        element: String,
        vector: Vec<f32>,
        /// 0 = cosine, 1 = l2, 2 = inner product
        metric: u8,
        /// 0 = f32, 1 = f16, 2 = i8
        quantization: u8,
        connectivity: u32,
        expansion_add: u32,
    },
    /// VREM key element.
    #[cfg(feature = "vector")]
    VRem { key: String, element: String },
    /// PROTO.SET key type_name data [expire_ms].
    #[cfg(feature = "protobuf")]
    ProtoSet {
        key: String,
        type_name: String,
        data: Bytes,
        expire_ms: i64,
    },
    /// PROTO.REGISTER name descriptor_bytes (for schema persistence).
    #[cfg(feature = "protobuf")]
    ProtoRegister { name: String, descriptor: Bytes },
}

impl AofRecord {
    // IMPORTANT: each variant has three match arms that must stay in sync:
    //   - `tag()`: the one-byte discriminant written to disk
    //   - `estimated_size()`: the capacity hint for the serialization buffer
    //   - `to_bytes()`: the actual serialized payload
    //
    // When adding a new variant, update all three in that order.
    // The binary format is stable — tag byte values must never be reused.

    /// Returns the on-disk tag byte for this record variant.
    fn tag(&self) -> u8 {
        match self {
            AofRecord::Set { .. } => TAG_SET,
            AofRecord::Del { .. } => TAG_DEL,
            AofRecord::Expire { .. } => TAG_EXPIRE,
            AofRecord::LPush { .. } => TAG_LPUSH,
            AofRecord::RPush { .. } => TAG_RPUSH,
            AofRecord::LPop { .. } => TAG_LPOP,
            AofRecord::RPop { .. } => TAG_RPOP,
            AofRecord::LSet { .. } => TAG_LSET,
            AofRecord::LTrim { .. } => TAG_LTRIM,
            AofRecord::LInsert { .. } => TAG_LINSERT,
            AofRecord::LRem { .. } => TAG_LREM,
            AofRecord::ZAdd { .. } => TAG_ZADD,
            AofRecord::ZRem { .. } => TAG_ZREM,
            AofRecord::Persist { .. } => TAG_PERSIST,
            AofRecord::Pexpire { .. } => TAG_PEXPIRE,
            AofRecord::Pexpireat { .. } => TAG_PEXPIREAT,
            AofRecord::Incr { .. } => TAG_INCR,
            AofRecord::Decr { .. } => TAG_DECR,
            AofRecord::HSet { .. } => TAG_HSET,
            AofRecord::HDel { .. } => TAG_HDEL,
            AofRecord::HIncrBy { .. } => TAG_HINCRBY,
            AofRecord::SAdd { .. } => TAG_SADD,
            AofRecord::SRem { .. } => TAG_SREM,
            AofRecord::IncrBy { .. } => TAG_INCRBY,
            AofRecord::DecrBy { .. } => TAG_DECRBY,
            AofRecord::Append { .. } => TAG_APPEND,
            AofRecord::SetRange { .. } => TAG_SETRANGE,
            AofRecord::SetBit { .. } => TAG_SETBIT,
            AofRecord::BitOp { .. } => TAG_BITOP,
            AofRecord::Rename { .. } => TAG_RENAME,
            AofRecord::Copy { .. } => TAG_COPY,
            AofRecord::SetExpireAt { .. } => TAG_SET_EXPIRE_AT,
            AofRecord::FlushAll => TAG_FLUSH_ALL,
            AofRecord::Checkpoint { .. } => TAG_CHECKPOINT,
            AofRecord::Restore { .. } => TAG_RESTORE,
            #[cfg(feature = "vector")]
            AofRecord::VAdd { .. } => TAG_VADD,
            #[cfg(feature = "vector")]
            AofRecord::VRem { .. } => TAG_VREM,
            #[cfg(feature = "protobuf")]
            AofRecord::ProtoSet { .. } => TAG_PROTO_SET,
            #[cfg(feature = "protobuf")]
            AofRecord::ProtoRegister { .. } => TAG_PROTO_REGISTER,
        }
    }

    /// Estimates the serialized size of this record in bytes.
    ///
    /// Used as a capacity hint for `to_bytes()` to avoid intermediate
    /// reallocations. The estimate includes the tag byte plus all
    /// length-prefixed fields, erring slightly high to avoid growing.
    fn estimated_size(&self) -> usize {
        // overhead per length-prefixed field: 4 bytes for the u32 length
        const LEN_PREFIX: usize = 4;

        match self {
            // 1 tag + 4 key-len + key + 4 value-len + value + 8 expire_ms
            AofRecord::Set {
                key,
                value,
                expire_ms: _,
            } => 1 + LEN_PREFIX + key.len() + LEN_PREFIX + value.len() + 8,
            // 1 tag + 4 key-len + key
            AofRecord::Del { key }
            | AofRecord::LPop { key }
            | AofRecord::RPop { key }
            | AofRecord::Persist { key }
            | AofRecord::Incr { key }
            | AofRecord::Decr { key } => 1 + LEN_PREFIX + key.len(),
            // 1 tag + 4 key-len + key + 8 seconds/millis/timestamp
            AofRecord::Expire { key, .. }
            | AofRecord::Pexpire { key, .. }
            | AofRecord::Pexpireat { key, .. } => 1 + LEN_PREFIX + key.len() + 8,
            // 1 tag + 4 key-len + key + 4 count + (4 value-len + value) * n
            AofRecord::LPush { key, values } | AofRecord::RPush { key, values } => {
                let values_size: usize = values.iter().map(|v| LEN_PREFIX + v.len()).sum();
                1 + LEN_PREFIX + key.len() + 4 + values_size
            }
            // 1 tag + 4 key-len + key + 8 index + 4 value-len + value
            AofRecord::LSet { key, value, .. } => {
                1 + LEN_PREFIX + key.len() + 8 + LEN_PREFIX + value.len()
            }
            // 1 tag + 4 key-len + key + 8 start + 8 stop
            AofRecord::LTrim { key, .. } => 1 + LEN_PREFIX + key.len() + 8 + 8,
            // 1 tag + 4 key-len + key + 1 before + 4 pivot-len + pivot + 4 value-len + value
            AofRecord::LInsert {
                key, pivot, value, ..
            } => {
                1 + LEN_PREFIX + key.len() + 1 + LEN_PREFIX + pivot.len() + LEN_PREFIX + value.len()
            }
            // 1 tag + 4 key-len + key + 8 count + 4 value-len + value
            AofRecord::LRem { key, value, .. } => {
                1 + LEN_PREFIX + key.len() + 8 + LEN_PREFIX + value.len()
            }
            AofRecord::ZAdd { key, members } => {
                let members_size: usize =
                    members.iter().map(|(_, m)| 8 + LEN_PREFIX + m.len()).sum();
                1 + LEN_PREFIX + key.len() + 4 + members_size
            }
            AofRecord::ZRem { key, members }
            | AofRecord::SAdd { key, members }
            | AofRecord::SRem { key, members } => {
                let members_size: usize = members.iter().map(|m| LEN_PREFIX + m.len()).sum();
                1 + LEN_PREFIX + key.len() + 4 + members_size
            }
            AofRecord::HSet { key, fields } => {
                let fields_size: usize = fields
                    .iter()
                    .map(|(f, v)| LEN_PREFIX + f.len() + LEN_PREFIX + v.len())
                    .sum();
                1 + LEN_PREFIX + key.len() + 4 + fields_size
            }
            AofRecord::HDel { key, fields } => {
                let fields_size: usize = fields.iter().map(|f| LEN_PREFIX + f.len()).sum();
                1 + LEN_PREFIX + key.len() + 4 + fields_size
            }
            AofRecord::HIncrBy { key, field, .. } => {
                1 + LEN_PREFIX + key.len() + LEN_PREFIX + field.len() + 8
            }
            AofRecord::IncrBy { key, .. } | AofRecord::DecrBy { key, .. } => {
                1 + LEN_PREFIX + key.len() + 8
            }
            AofRecord::Append { key, value } => {
                1 + LEN_PREFIX + key.len() + LEN_PREFIX + value.len()
            }
            AofRecord::SetRange { key, value, .. } => {
                1 + LEN_PREFIX + key.len() + 8 + LEN_PREFIX + value.len()
            }
            // 1 tag + 4 key-len + key + 8 offset + 1 value
            AofRecord::SetBit { key, .. } => 1 + LEN_PREFIX + key.len() + 8 + 1,
            // 1 tag + 1 op + 4 dest-len + dest + 4 count + (4 key-len + key) * n
            AofRecord::BitOp { dest, keys, .. } => {
                let keys_size: usize = keys.iter().map(|k| LEN_PREFIX + k.len()).sum();
                1 + 1 + LEN_PREFIX + dest.len() + 4 + keys_size
            }
            AofRecord::Rename { key, newkey } => {
                1 + LEN_PREFIX + key.len() + LEN_PREFIX + newkey.len()
            }
            AofRecord::Copy {
                source,
                destination,
                ..
            } => 1 + LEN_PREFIX + source.len() + LEN_PREFIX + destination.len() + 1,
            AofRecord::SetExpireAt { key, value, .. } => {
                1 + LEN_PREFIX + key.len() + LEN_PREFIX + value.len() + 8
            }
            AofRecord::FlushAll => 1,
            AofRecord::Checkpoint { .. } => 1 + 4,
            AofRecord::Restore { key, data, .. } => {
                1 + LEN_PREFIX + key.len() + 8 + LEN_PREFIX + data.len()
            }
            #[cfg(feature = "vector")]
            AofRecord::VAdd {
                key,
                element,
                vector,
                ..
            } => {
                1 + LEN_PREFIX + key.len() + LEN_PREFIX + element.len() + 4 + vector.len() * 4 + 10
            }
            #[cfg(feature = "vector")]
            AofRecord::VRem { key, element } => {
                1 + LEN_PREFIX + key.len() + LEN_PREFIX + element.len()
            }
            #[cfg(feature = "protobuf")]
            AofRecord::ProtoSet {
                key,
                type_name,
                data,
                ..
            } => {
                1 + LEN_PREFIX
                    + key.len()
                    + LEN_PREFIX
                    + type_name.len()
                    + LEN_PREFIX
                    + data.len()
                    + 8
            }
            #[cfg(feature = "protobuf")]
            AofRecord::ProtoRegister { name, descriptor } => {
                1 + LEN_PREFIX + name.len() + LEN_PREFIX + descriptor.len()
            }
        }
    }

    /// Serializes this record into a byte vector (tag + payload, no CRC).
    pub fn to_bytes(&self) -> Result<Vec<u8>, FormatError> {
        let mut buf = Vec::with_capacity(self.estimated_size());
        format::write_u8(&mut buf, self.tag())?;

        match self {
            // key-only: tag + key
            AofRecord::Del { key }
            | AofRecord::LPop { key }
            | AofRecord::RPop { key }
            | AofRecord::Persist { key }
            | AofRecord::Incr { key }
            | AofRecord::Decr { key } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
            }

            // key + bytes value + expire
            AofRecord::Set {
                key,
                value,
                expire_ms,
            } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, value)?;
                format::write_i64(&mut buf, *expire_ms)?;
            }

            // key + i64 (seconds/milliseconds are capped at i64::MAX on write
            // so that deserialization can safely cast back to u64)
            AofRecord::Expire { key, seconds } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, (*seconds).min(i64::MAX as u64) as i64)?;
            }
            AofRecord::Pexpire { key, milliseconds } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, (*milliseconds).min(i64::MAX as u64) as i64)?;
            }
            AofRecord::Pexpireat { key, timestamp_ms } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, (*timestamp_ms).min(i64::MAX as u64) as i64)?;
            }
            AofRecord::IncrBy { key, delta } | AofRecord::DecrBy { key, delta } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, *delta)?;
            }

            // key + byte list
            AofRecord::LPush { key, values } | AofRecord::RPush { key, values } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_len(&mut buf, values.len())?;
                for v in values {
                    format::write_bytes(&mut buf, v)?;
                }
            }

            // key + index + value
            AofRecord::LSet { key, index, value } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, *index)?;
                format::write_bytes(&mut buf, value)?;
            }

            // key + start + stop
            AofRecord::LTrim { key, start, stop } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, *start)?;
                format::write_i64(&mut buf, *stop)?;
            }

            // key + before(u8) + pivot + value
            AofRecord::LInsert {
                key,
                before,
                pivot,
                value,
            } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_u8(&mut buf, if *before { 1 } else { 0 })?;
                format::write_bytes(&mut buf, pivot)?;
                format::write_bytes(&mut buf, value)?;
            }

            // key + count + value
            AofRecord::LRem { key, count, value } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, *count)?;
                format::write_bytes(&mut buf, value)?;
            }

            // key + string list
            AofRecord::ZRem { key, members }
            | AofRecord::SAdd { key, members }
            | AofRecord::SRem { key, members } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_len(&mut buf, members.len())?;
                for member in members {
                    format::write_bytes(&mut buf, member.as_bytes())?;
                }
            }
            AofRecord::HDel { key, fields } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_len(&mut buf, fields.len())?;
                for field in fields {
                    format::write_bytes(&mut buf, field.as_bytes())?;
                }
            }

            // key + scored members
            AofRecord::ZAdd { key, members } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_len(&mut buf, members.len())?;
                for (score, member) in members {
                    format::write_f64(&mut buf, *score)?;
                    format::write_bytes(&mut buf, member.as_bytes())?;
                }
            }

            // key + field-value pairs
            AofRecord::HSet { key, fields } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_len(&mut buf, fields.len())?;
                for (field, value) in fields {
                    format::write_bytes(&mut buf, field.as_bytes())?;
                    format::write_bytes(&mut buf, value)?;
                }
            }

            // key + field + delta
            AofRecord::HIncrBy { key, field, delta } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, field.as_bytes())?;
                format::write_i64(&mut buf, *delta)?;
            }

            // key + bytes value (no expire)
            AofRecord::Append { key, value } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, value)?;
            }

            // key + offset (as i64) + bytes value
            AofRecord::SetRange { key, offset, value } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, *offset as i64)?;
                format::write_bytes(&mut buf, value)?;
            }

            // key + offset (as i64) + bit value (u8)
            AofRecord::SetBit { key, offset, value } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                // offset is u64 but fits in i64 in practice (max bit offset < 2^32)
                format::write_i64(&mut buf, *offset as i64)?;
                format::write_u8(&mut buf, *value)?;
            }

            // op byte + dest + key list
            AofRecord::BitOp { op, dest, keys } => {
                format::write_u8(&mut buf, *op)?;
                format::write_bytes(&mut buf, dest.as_bytes())?;
                format::write_len(&mut buf, keys.len())?;
                for key in keys {
                    format::write_bytes(&mut buf, key.as_bytes())?;
                }
            }

            // key + newkey
            AofRecord::Rename { key, newkey } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, newkey.as_bytes())?;
            }

            // source + destination + replace flag
            AofRecord::Copy {
                source,
                destination,
                replace,
            } => {
                format::write_bytes(&mut buf, source.as_bytes())?;
                format::write_bytes(&mut buf, destination.as_bytes())?;
                buf.push(u8::from(*replace));
            }
            AofRecord::SetExpireAt {
                key,
                value,
                timestamp_ms,
            } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, value)?;
                format::write_i64(&mut buf, (*timestamp_ms).min(i64::MAX as u64) as i64)?;
            }
            AofRecord::FlushAll => {}
            AofRecord::Checkpoint { snapshot_crc } => format::write_u32(&mut buf, *snapshot_crc)?,
            AofRecord::Restore { key, ttl_ms, data } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, (*ttl_ms).min(i64::MAX as u64) as i64)?;
                format::write_bytes(&mut buf, data)?;
            }

            #[cfg(feature = "vector")]
            AofRecord::VAdd {
                key,
                element,
                vector,
                metric,
                quantization,
                connectivity,
                expansion_add,
            } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, element.as_bytes())?;
                format::write_len(&mut buf, vector.len())?;
                for &v in vector {
                    format::write_f32(&mut buf, v)?;
                }
                format::write_u8(&mut buf, *metric)?;
                format::write_u8(&mut buf, *quantization)?;
                format::write_u32(&mut buf, *connectivity)?;
                format::write_u32(&mut buf, *expansion_add)?;
            }
            #[cfg(feature = "vector")]
            AofRecord::VRem { key, element } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, element.as_bytes())?;
            }

            #[cfg(feature = "protobuf")]
            AofRecord::ProtoSet {
                key,
                type_name,
                data,
                expire_ms,
            } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, type_name.as_bytes())?;
                format::write_bytes(&mut buf, data)?;
                format::write_i64(&mut buf, *expire_ms)?;
            }
            #[cfg(feature = "protobuf")]
            AofRecord::ProtoRegister { name, descriptor } => {
                format::write_bytes(&mut buf, name.as_bytes())?;
                format::write_bytes(&mut buf, descriptor)?;
            }
        }
        Ok(buf)
    }

    /// Deserializes a record from its binary payload (tag byte + fields, no CRC).
    ///
    /// The format is the same as `to_bytes()`. CRC validation is the caller's
    /// responsibility.
    pub fn from_bytes(data: &[u8]) -> Result<Self, FormatError> {
        Self::decode(&mut io::Cursor::new(data))
    }

    /// Decodes one record (tag byte + fields, no CRC) from a stream.
    ///
    /// This is the only record decoder. The AOF reader runs it over a
    /// [`format::CrcReader`] so the checksum covers exactly the bytes decoded.
    pub(super) fn decode(cursor: &mut impl io::Read) -> Result<Self, FormatError> {
        let tag = format::read_u8(cursor)?;
        match tag {
            TAG_SET => {
                let key = read_string(cursor, "key")?;
                let value = format::read_bytes(cursor)?;
                let expire_ms = format::read_i64(cursor)?;
                Ok(AofRecord::Set {
                    key,
                    value: Bytes::from(value),
                    expire_ms,
                })
            }
            TAG_DEL => {
                let key = read_string(cursor, "key")?;
                Ok(AofRecord::Del { key })
            }
            TAG_EXPIRE => {
                let key = read_string(cursor, "key")?;
                let raw = format::read_i64(cursor)?;
                let seconds = u64::try_from(raw).map_err(|_| {
                    FormatError::InvalidData(format!(
                        "EXPIRE seconds is negative ({raw}) in AOF record"
                    ))
                })?;
                Ok(AofRecord::Expire { key, seconds })
            }
            TAG_LPUSH | TAG_RPUSH => {
                let key = read_string(cursor, "key")?;
                let values = read_bytes_list(cursor, "list")?;
                if tag == TAG_LPUSH {
                    Ok(AofRecord::LPush { key, values })
                } else {
                    Ok(AofRecord::RPush { key, values })
                }
            }
            TAG_LPOP => {
                let key = read_string(cursor, "key")?;
                Ok(AofRecord::LPop { key })
            }
            TAG_RPOP => {
                let key = read_string(cursor, "key")?;
                Ok(AofRecord::RPop { key })
            }
            TAG_LSET => {
                let key = read_string(cursor, "key")?;
                let index = format::read_i64(cursor)?;
                let value = Bytes::from(format::read_bytes(cursor)?);
                Ok(AofRecord::LSet { key, index, value })
            }
            TAG_LTRIM => {
                let key = read_string(cursor, "key")?;
                let start = format::read_i64(cursor)?;
                let stop = format::read_i64(cursor)?;
                Ok(AofRecord::LTrim { key, start, stop })
            }
            TAG_LINSERT => {
                let key = read_string(cursor, "key")?;
                let before_byte = format::read_u8(cursor)?;
                let before = before_byte != 0;
                let pivot = Bytes::from(format::read_bytes(cursor)?);
                let value = Bytes::from(format::read_bytes(cursor)?);
                Ok(AofRecord::LInsert {
                    key,
                    before,
                    pivot,
                    value,
                })
            }
            TAG_LREM => {
                let key = read_string(cursor, "key")?;
                let count = format::read_i64(cursor)?;
                let value = Bytes::from(format::read_bytes(cursor)?);
                Ok(AofRecord::LRem { key, count, value })
            }
            TAG_ZADD => {
                let key = read_string(cursor, "key")?;
                let count = format::read_u32(cursor)?;
                format::validate_collection_count(count, "sorted set")?;
                let mut members = Vec::with_capacity(format::capped_capacity(count));
                for _ in 0..count {
                    let score = format::read_f64(cursor)?;
                    let member = read_string(cursor, "member")?;
                    members.push((score, member));
                }
                Ok(AofRecord::ZAdd { key, members })
            }
            TAG_ZREM => {
                let key = read_string(cursor, "key")?;
                let members = read_string_list(cursor, "member")?;
                Ok(AofRecord::ZRem { key, members })
            }
            TAG_PERSIST => {
                let key = read_string(cursor, "key")?;
                Ok(AofRecord::Persist { key })
            }
            TAG_PEXPIRE => {
                let key = read_string(cursor, "key")?;
                let raw = format::read_i64(cursor)?;
                let milliseconds = u64::try_from(raw).map_err(|_| {
                    FormatError::InvalidData(format!(
                        "PEXPIRE milliseconds is negative ({raw}) in AOF record"
                    ))
                })?;
                Ok(AofRecord::Pexpire { key, milliseconds })
            }
            TAG_PEXPIREAT => {
                let key = read_string(cursor, "key")?;
                let raw = format::read_i64(cursor)?;
                let timestamp_ms = u64::try_from(raw).map_err(|_| {
                    FormatError::InvalidData(format!(
                        "PEXPIREAT timestamp_ms is negative ({raw}) in AOF record"
                    ))
                })?;
                Ok(AofRecord::Pexpireat { key, timestamp_ms })
            }
            TAG_INCR => {
                let key = read_string(cursor, "key")?;
                Ok(AofRecord::Incr { key })
            }
            TAG_DECR => {
                let key = read_string(cursor, "key")?;
                Ok(AofRecord::Decr { key })
            }
            TAG_HSET => {
                let key = read_string(cursor, "key")?;
                let count = format::read_u32(cursor)?;
                format::validate_collection_count(count, "hash")?;
                let mut fields = Vec::with_capacity(format::capped_capacity(count));
                for _ in 0..count {
                    let field = read_string(cursor, "field")?;
                    let value = Bytes::from(format::read_bytes(cursor)?);
                    fields.push((field, value));
                }
                Ok(AofRecord::HSet { key, fields })
            }
            TAG_HDEL => {
                let key = read_string(cursor, "key")?;
                let fields = read_string_list(cursor, "field")?;
                Ok(AofRecord::HDel { key, fields })
            }
            TAG_HINCRBY => {
                let key = read_string(cursor, "key")?;
                let field = read_string(cursor, "field")?;
                let delta = format::read_i64(cursor)?;
                Ok(AofRecord::HIncrBy { key, field, delta })
            }
            TAG_SADD => {
                let key = read_string(cursor, "key")?;
                let members = read_string_list(cursor, "member")?;
                Ok(AofRecord::SAdd { key, members })
            }
            TAG_SREM => {
                let key = read_string(cursor, "key")?;
                let members = read_string_list(cursor, "member")?;
                Ok(AofRecord::SRem { key, members })
            }
            TAG_INCRBY => {
                let key = read_string(cursor, "key")?;
                let delta = format::read_i64(cursor)?;
                Ok(AofRecord::IncrBy { key, delta })
            }
            TAG_DECRBY => {
                let key = read_string(cursor, "key")?;
                let delta = format::read_i64(cursor)?;
                Ok(AofRecord::DecrBy { key, delta })
            }
            TAG_APPEND => {
                let key = read_string(cursor, "key")?;
                let value = Bytes::from(format::read_bytes(cursor)?);
                Ok(AofRecord::Append { key, value })
            }
            TAG_SETRANGE => {
                let key = read_string(cursor, "key")?;
                let offset = format::read_i64(cursor)? as usize;
                let value = Bytes::from(format::read_bytes(cursor)?);
                Ok(AofRecord::SetRange { key, offset, value })
            }
            TAG_RENAME => {
                let key = read_string(cursor, "key")?;
                let newkey = read_string(cursor, "newkey")?;
                Ok(AofRecord::Rename { key, newkey })
            }
            TAG_COPY => {
                let source = read_string(cursor, "source")?;
                let destination = read_string(cursor, "destination")?;
                let replace = format::read_u8(cursor)? != 0;
                Ok(AofRecord::Copy {
                    source,
                    destination,
                    replace,
                })
            }
            TAG_SET_EXPIRE_AT => {
                let key = read_string(cursor, "key")?;
                let value = Bytes::from(format::read_bytes(cursor)?);
                let raw = format::read_i64(cursor)?;
                let timestamp_ms = u64::try_from(raw).map_err(|_| {
                    FormatError::InvalidData(format!(
                        "SET deadline is negative ({raw}) in AOF record"
                    ))
                })?;
                Ok(AofRecord::SetExpireAt {
                    key,
                    value,
                    timestamp_ms,
                })
            }
            TAG_FLUSH_ALL => Ok(AofRecord::FlushAll),
            TAG_CHECKPOINT => Ok(AofRecord::Checkpoint {
                snapshot_crc: format::read_u32(cursor)?,
            }),
            TAG_RESTORE => {
                let key = read_string(cursor, "key")?;
                let raw = format::read_i64(cursor)?;
                let ttl_ms = u64::try_from(raw).map_err(|_| {
                    FormatError::InvalidData(format!(
                        "RESTORE ttl is negative ({raw}) in AOF record"
                    ))
                })?;
                let data = Bytes::from(format::read_bytes(cursor)?);
                Ok(AofRecord::Restore { key, ttl_ms, data })
            }
            TAG_SETBIT => {
                let key = read_string(cursor, "key")?;
                let offset = format::read_i64(cursor)? as u64;
                let value = format::read_u8(cursor)?;
                Ok(AofRecord::SetBit { key, offset, value })
            }
            TAG_BITOP => {
                let op = format::read_u8(cursor)?;
                if op > 3 {
                    return Err(FormatError::InvalidData(format!(
                        "BITOP: unknown op byte {op} in AOF record"
                    )));
                }
                let dest = read_string(cursor, "dest")?;
                let keys = read_string_list(cursor, "key")?;
                Ok(AofRecord::BitOp { op, dest, keys })
            }
            #[cfg(feature = "vector")]
            TAG_VADD => {
                let key = read_string(cursor, "key")?;
                let element = read_string(cursor, "element")?;
                let dim = format::read_u32(cursor)?;
                if dim > format::MAX_PERSISTED_VECTOR_DIMS {
                    return Err(FormatError::InvalidData(format!(
                        "AOF VADD dimension {dim} exceeds max {}",
                        format::MAX_PERSISTED_VECTOR_DIMS
                    )));
                }
                let mut vector = Vec::with_capacity(dim as usize);
                for _ in 0..dim {
                    vector.push(format::read_f32(cursor)?);
                }
                let metric = format::read_u8(cursor)?;
                let quantization = format::read_u8(cursor)?;
                let connectivity = format::read_u32(cursor)?;
                let expansion_add = format::read_u32(cursor)?;
                Ok(AofRecord::VAdd {
                    key,
                    element,
                    vector,
                    metric,
                    quantization,
                    connectivity,
                    expansion_add,
                })
            }
            #[cfg(feature = "vector")]
            TAG_VREM => {
                let key = read_string(cursor, "key")?;
                let element = read_string(cursor, "element")?;
                Ok(AofRecord::VRem { key, element })
            }
            #[cfg(feature = "protobuf")]
            TAG_PROTO_SET => {
                let key = read_string(cursor, "key")?;
                let type_name = read_string(cursor, "type_name")?;
                let data = format::read_bytes(cursor)?;
                let expire_ms = format::read_i64(cursor)?;
                Ok(AofRecord::ProtoSet {
                    key,
                    type_name,
                    data: Bytes::from(data),
                    expire_ms,
                })
            }
            #[cfg(feature = "protobuf")]
            TAG_PROTO_REGISTER => {
                let name = read_string(cursor, "name")?;
                let descriptor = format::read_bytes(cursor)?;
                Ok(AofRecord::ProtoRegister {
                    name,
                    descriptor: Bytes::from(descriptor),
                })
            }
            _ => Err(FormatError::UnknownTag(tag)),
        }
    }
}
