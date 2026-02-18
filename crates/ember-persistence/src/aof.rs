//! Append-only file for recording mutations.
//!
//! Each shard writes its own AOF file (`shard-{id}.aof`). Records are
//! written after successful mutations. The binary format uses a simple
//! tag + payload + CRC32 structure for each record.
//!
//! File layout:
//! ```text
//! [EAOF magic: 4B][version: 1B]
//! [record]*
//! ```
//!
//! Record layout:
//! ```text
//! [tag: 1B][payload...][crc32: 4B]
//! ```
//! The CRC32 covers the tag + payload bytes.

use std::fmt;
use std::fs::{self, File, OpenOptions};
#[cfg(feature = "encryption")]
use std::io::Read as _;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

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

// -- record tags --
// values are stable and must not change (on-disk format).

// string
const TAG_SET: u8 = 1;
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
const TAG_RENAME: u8 = 22;

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
    /// RENAME key newkey.
    Rename { key: String, newkey: String },
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
            AofRecord::ZAdd { .. } => TAG_ZADD,
            AofRecord::ZRem { .. } => TAG_ZREM,
            AofRecord::Persist { .. } => TAG_PERSIST,
            AofRecord::Pexpire { .. } => TAG_PEXPIRE,
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
            AofRecord::Rename { .. } => TAG_RENAME,
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
            AofRecord::Set {
                key,
                value,
                expire_ms: _,
            } => 1 + LEN_PREFIX + key.len() + LEN_PREFIX + value.len() + 8,
            AofRecord::Del { key }
            | AofRecord::LPop { key }
            | AofRecord::RPop { key }
            | AofRecord::Persist { key }
            | AofRecord::Incr { key }
            | AofRecord::Decr { key } => 1 + LEN_PREFIX + key.len(),
            AofRecord::Expire { key, .. } | AofRecord::Pexpire { key, .. } => {
                1 + LEN_PREFIX + key.len() + 8
            }
            AofRecord::LPush { key, values } | AofRecord::RPush { key, values } => {
                let values_size: usize = values.iter().map(|v| LEN_PREFIX + v.len()).sum();
                1 + LEN_PREFIX + key.len() + 4 + values_size
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
            AofRecord::Rename { key, newkey } => {
                1 + LEN_PREFIX + key.len() + LEN_PREFIX + newkey.len()
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
    fn to_bytes(&self) -> Result<Vec<u8>, FormatError> {
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

            // key + newkey
            AofRecord::Rename { key, newkey } => {
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, newkey.as_bytes())?;
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

    /// Deserializes a record from a byte slice (tag + payload, no CRC).
    fn from_bytes(data: &[u8]) -> Result<Self, FormatError> {
        let mut cursor = io::Cursor::new(data);
        let tag = format::read_u8(&mut cursor)?;
        match tag {
            TAG_SET => {
                let key = read_string(&mut cursor, "key")?;
                let value = format::read_bytes(&mut cursor)?;
                let expire_ms = format::read_i64(&mut cursor)?;
                Ok(AofRecord::Set {
                    key,
                    value: Bytes::from(value),
                    expire_ms,
                })
            }
            TAG_DEL => {
                let key = read_string(&mut cursor, "key")?;
                Ok(AofRecord::Del { key })
            }
            TAG_EXPIRE => {
                let key = read_string(&mut cursor, "key")?;
                let raw = format::read_i64(&mut cursor)?;
                let seconds = u64::try_from(raw).map_err(|_| {
                    FormatError::InvalidData(format!(
                        "EXPIRE seconds is negative ({raw}) in AOF record"
                    ))
                })?;
                Ok(AofRecord::Expire { key, seconds })
            }
            TAG_LPUSH | TAG_RPUSH => {
                let key = read_string(&mut cursor, "key")?;
                let count = format::read_u32(&mut cursor)?;
                format::validate_collection_count(count, "list")?;
                let mut values = Vec::with_capacity(format::capped_capacity(count));
                for _ in 0..count {
                    values.push(Bytes::from(format::read_bytes(&mut cursor)?));
                }
                if tag == TAG_LPUSH {
                    Ok(AofRecord::LPush { key, values })
                } else {
                    Ok(AofRecord::RPush { key, values })
                }
            }
            TAG_LPOP => {
                let key = read_string(&mut cursor, "key")?;
                Ok(AofRecord::LPop { key })
            }
            TAG_RPOP => {
                let key = read_string(&mut cursor, "key")?;
                Ok(AofRecord::RPop { key })
            }
            TAG_ZADD => {
                let key = read_string(&mut cursor, "key")?;
                let count = format::read_u32(&mut cursor)?;
                format::validate_collection_count(count, "sorted set")?;
                let mut members = Vec::with_capacity(format::capped_capacity(count));
                for _ in 0..count {
                    let score = format::read_f64(&mut cursor)?;
                    let member = read_string(&mut cursor, "member")?;
                    members.push((score, member));
                }
                Ok(AofRecord::ZAdd { key, members })
            }
            TAG_ZREM => {
                let key = read_string(&mut cursor, "key")?;
                let members = read_string_list(&mut cursor, "member")?;
                Ok(AofRecord::ZRem { key, members })
            }
            TAG_PERSIST => {
                let key = read_string(&mut cursor, "key")?;
                Ok(AofRecord::Persist { key })
            }
            TAG_PEXPIRE => {
                let key = read_string(&mut cursor, "key")?;
                let raw = format::read_i64(&mut cursor)?;
                let milliseconds = u64::try_from(raw).map_err(|_| {
                    FormatError::InvalidData(format!(
                        "PEXPIRE milliseconds is negative ({raw}) in AOF record"
                    ))
                })?;
                Ok(AofRecord::Pexpire { key, milliseconds })
            }
            TAG_INCR => {
                let key = read_string(&mut cursor, "key")?;
                Ok(AofRecord::Incr { key })
            }
            TAG_DECR => {
                let key = read_string(&mut cursor, "key")?;
                Ok(AofRecord::Decr { key })
            }
            TAG_HSET => {
                let key = read_string(&mut cursor, "key")?;
                let count = format::read_u32(&mut cursor)?;
                format::validate_collection_count(count, "hash")?;
                let mut fields = Vec::with_capacity(format::capped_capacity(count));
                for _ in 0..count {
                    let field = read_string(&mut cursor, "field")?;
                    let value = Bytes::from(format::read_bytes(&mut cursor)?);
                    fields.push((field, value));
                }
                Ok(AofRecord::HSet { key, fields })
            }
            TAG_HDEL => {
                let key = read_string(&mut cursor, "key")?;
                let fields = read_string_list(&mut cursor, "field")?;
                Ok(AofRecord::HDel { key, fields })
            }
            TAG_HINCRBY => {
                let key = read_string(&mut cursor, "key")?;
                let field = read_string(&mut cursor, "field")?;
                let delta = format::read_i64(&mut cursor)?;
                Ok(AofRecord::HIncrBy { key, field, delta })
            }
            TAG_SADD => {
                let key = read_string(&mut cursor, "key")?;
                let members = read_string_list(&mut cursor, "member")?;
                Ok(AofRecord::SAdd { key, members })
            }
            TAG_SREM => {
                let key = read_string(&mut cursor, "key")?;
                let members = read_string_list(&mut cursor, "member")?;
                Ok(AofRecord::SRem { key, members })
            }
            TAG_INCRBY => {
                let key = read_string(&mut cursor, "key")?;
                let delta = format::read_i64(&mut cursor)?;
                Ok(AofRecord::IncrBy { key, delta })
            }
            TAG_DECRBY => {
                let key = read_string(&mut cursor, "key")?;
                let delta = format::read_i64(&mut cursor)?;
                Ok(AofRecord::DecrBy { key, delta })
            }
            TAG_APPEND => {
                let key = read_string(&mut cursor, "key")?;
                let value = Bytes::from(format::read_bytes(&mut cursor)?);
                Ok(AofRecord::Append { key, value })
            }
            TAG_RENAME => {
                let key = read_string(&mut cursor, "key")?;
                let newkey = read_string(&mut cursor, "newkey")?;
                Ok(AofRecord::Rename { key, newkey })
            }
            #[cfg(feature = "vector")]
            TAG_VADD => {
                let key = read_string(&mut cursor, "key")?;
                let element = read_string(&mut cursor, "element")?;
                let dim = format::read_u32(&mut cursor)?;
                if dim > format::MAX_PERSISTED_VECTOR_DIMS {
                    return Err(FormatError::InvalidData(format!(
                        "AOF VADD dimension {dim} exceeds max {}",
                        format::MAX_PERSISTED_VECTOR_DIMS
                    )));
                }
                let mut vector = Vec::with_capacity(dim as usize);
                for _ in 0..dim {
                    vector.push(format::read_f32(&mut cursor)?);
                }
                let metric = format::read_u8(&mut cursor)?;
                let quantization = format::read_u8(&mut cursor)?;
                let connectivity = format::read_u32(&mut cursor)?;
                let expansion_add = format::read_u32(&mut cursor)?;
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
                let key = read_string(&mut cursor, "key")?;
                let element = read_string(&mut cursor, "element")?;
                Ok(AofRecord::VRem { key, element })
            }
            #[cfg(feature = "protobuf")]
            TAG_PROTO_SET => {
                let key = read_string(&mut cursor, "key")?;
                let type_name = read_string(&mut cursor, "type_name")?;
                let data = format::read_bytes(&mut cursor)?;
                let expire_ms = format::read_i64(&mut cursor)?;
                Ok(AofRecord::ProtoSet {
                    key,
                    type_name,
                    data: Bytes::from(data),
                    expire_ms,
                })
            }
            #[cfg(feature = "protobuf")]
            TAG_PROTO_REGISTER => {
                let name = read_string(&mut cursor, "name")?;
                let descriptor = format::read_bytes(&mut cursor)?;
                Ok(AofRecord::ProtoRegister {
                    name,
                    descriptor: Bytes::from(descriptor),
                })
            }
            _ => Err(FormatError::UnknownTag(tag)),
        }
    }
}

/// Configurable fsync policy for the AOF writer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync after every write. safest, slowest.
    Always,
    /// fsync once per second. the shard tick drives this.
    #[default]
    EverySec,
    /// let the OS decide when to flush. fastest, least durable.
    No,
}

/// Buffered writer for appending AOF records to a file.
pub struct AofWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    #[cfg(feature = "encryption")]
    encryption_key: Option<crate::encryption::EncryptionKey>,
}

impl AofWriter {
    /// Opens (or creates) an AOF file. If the file is new, writes the header.
    /// If the file already exists, appends to it.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, FormatError> {
        let path = path.into();
        let exists = path.exists() && fs::metadata(&path).map(|m| m.len() > 0).unwrap_or(false);

        let file = open_persistence_file(&path)?;
        let mut writer = BufWriter::new(file);

        if !exists {
            format::write_header(&mut writer, format::AOF_MAGIC)?;
            writer.flush()?;
        }

        Ok(Self {
            writer,
            path,
            #[cfg(feature = "encryption")]
            encryption_key: None,
        })
    }

    /// Opens (or creates) an encrypted AOF file using AES-256-GCM.
    ///
    /// New files get a v3 header. Existing v2 files can be appended to —
    /// new records will be written unencrypted (use `BGREWRITEAOF` to
    /// migrate the full file to v3).
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(
        path: impl Into<PathBuf>,
        key: crate::encryption::EncryptionKey,
    ) -> Result<Self, FormatError> {
        let path = path.into();
        let exists = path.exists() && fs::metadata(&path).map(|m| m.len() > 0).unwrap_or(false);

        let file = open_persistence_file(&path)?;
        let mut writer = BufWriter::new(file);

        if !exists {
            format::write_header_versioned(
                &mut writer,
                format::AOF_MAGIC,
                format::FORMAT_VERSION_ENCRYPTED,
            )?;
            writer.flush()?;
        }

        Ok(Self {
            writer,
            path,
            encryption_key: Some(key),
        })
    }

    /// Appends a record to the AOF.
    ///
    /// When an encryption key is set, writes: `[nonce: 12B][len: 4B][ciphertext]`.
    /// Otherwise writes the v2 format: `[tag+payload][crc32: 4B]`.
    pub fn write_record(&mut self, record: &AofRecord) -> Result<(), FormatError> {
        let payload = record.to_bytes()?;

        #[cfg(feature = "encryption")]
        if let Some(ref key) = self.encryption_key {
            let (nonce, ciphertext) = crate::encryption::encrypt_record(key, &payload)?;
            self.writer.write_all(&nonce)?;
            format::write_len(&mut self.writer, ciphertext.len())?;
            self.writer.write_all(&ciphertext)?;
            return Ok(());
        }

        let checksum = format::crc32(&payload);
        self.writer.write_all(&payload)?;
        format::write_u32(&mut self.writer, checksum)?;
        Ok(())
    }

    /// Flushes the internal buffer to the OS.
    pub fn flush(&mut self) -> Result<(), FormatError> {
        self.writer.flush()?;
        Ok(())
    }

    /// Flushes and fsyncs the file to disk.
    pub fn sync(&mut self) -> Result<(), FormatError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Returns the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Truncates the AOF file back to just the header.
    ///
    /// Uses write-to-temp-then-rename for crash safety: the old AOF
    /// remains intact until the new file (with only a header) is fully
    /// synced and atomically renamed into place.
    pub fn truncate(&mut self) -> Result<(), FormatError> {
        // flush the old writer so no data is in the BufWriter
        self.writer.flush()?;

        // write a fresh header to a temp file next to the real AOF
        let tmp_path = self.path.with_extension("aof.tmp");
        let mut opts = OpenOptions::new();
        opts.create(true).write(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let tmp_file = opts.open(&tmp_path)?;
        let mut tmp_writer = BufWriter::new(tmp_file);

        #[cfg(feature = "encryption")]
        if self.encryption_key.is_some() {
            format::write_header_versioned(
                &mut tmp_writer,
                format::AOF_MAGIC,
                format::FORMAT_VERSION_ENCRYPTED,
            )?;
        } else {
            format::write_header(&mut tmp_writer, format::AOF_MAGIC)?;
        }
        #[cfg(not(feature = "encryption"))]
        format::write_header(&mut tmp_writer, format::AOF_MAGIC)?;

        tmp_writer.flush()?;
        tmp_writer.get_ref().sync_all()?;

        // atomic rename: old AOF is replaced only after new file is durable
        std::fs::rename(&tmp_path, &self.path)?;

        // reopen for appending
        let file = OpenOptions::new().append(true).open(&self.path)?;
        self.writer = BufWriter::new(file);
        Ok(())
    }
}

/// Reader for iterating over AOF records.
pub struct AofReader {
    reader: BufReader<File>,
    /// Format version from the file header. v2 = plaintext, v3 = encrypted.
    version: u8,
    #[cfg(feature = "encryption")]
    encryption_key: Option<crate::encryption::EncryptionKey>,
}

impl fmt::Debug for AofReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AofReader")
            .field("version", &self.version)
            .finish()
    }
}

impl AofReader {
    /// Opens an AOF file and validates the header.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, FormatError> {
        let file = File::open(path.as_ref())?;
        let mut reader = BufReader::new(file);
        let version = format::read_header(&mut reader, format::AOF_MAGIC)?;

        if version == format::FORMAT_VERSION_ENCRYPTED {
            return Err(FormatError::EncryptionRequired);
        }

        Ok(Self {
            reader,
            version,
            #[cfg(feature = "encryption")]
            encryption_key: None,
        })
    }

    /// Opens an AOF file with an encryption key for decrypting v3 records.
    ///
    /// Also handles v2 (plaintext) files — the key is simply unused,
    /// allowing transparent migration.
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(
        path: impl AsRef<Path>,
        key: crate::encryption::EncryptionKey,
    ) -> Result<Self, FormatError> {
        let file = File::open(path.as_ref())?;
        let mut reader = BufReader::new(file);
        let version = format::read_header(&mut reader, format::AOF_MAGIC)?;

        Ok(Self {
            reader,
            version,
            encryption_key: Some(key),
        })
    }

    /// Reads the next record from the AOF.
    ///
    /// Returns `Ok(None)` at end-of-file. On a truncated record (the
    /// server crashed mid-write), returns `Ok(None)` rather than an error
    /// — this is the expected recovery behavior.
    pub fn read_record(&mut self) -> Result<Option<AofRecord>, FormatError> {
        #[cfg(feature = "encryption")]
        if self.version == format::FORMAT_VERSION_ENCRYPTED {
            return self.read_encrypted_record();
        }

        self.read_v2_record()
    }

    /// Reads a v2 (plaintext) record: tag + payload + crc32.
    fn read_v2_record(&mut self) -> Result<Option<AofRecord>, FormatError> {
        // peek for EOF — try reading the tag byte
        let tag = match format::read_u8(&mut self.reader) {
            Ok(t) => t,
            Err(FormatError::UnexpectedEof) => return Ok(None),
            Err(e) => return Err(e),
        };

        // read the rest of the payload based on tag, building the full
        // record bytes for CRC verification
        let record_result = self.read_payload_for_tag(tag);
        match record_result {
            Ok((payload, stored_crc)) => {
                // prepend the tag to the payload for CRC check
                let mut full = Vec::with_capacity(1 + payload.len());
                full.push(tag);
                full.extend_from_slice(&payload);
                format::verify_crc32(&full, stored_crc)?;
                AofRecord::from_bytes(&full).map(Some)
            }
            // truncated record — treat as end of usable data
            Err(FormatError::UnexpectedEof) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Reads a v3 (encrypted) record: nonce + len + ciphertext.
    #[cfg(feature = "encryption")]
    fn read_encrypted_record(&mut self) -> Result<Option<AofRecord>, FormatError> {
        let key = self
            .encryption_key
            .as_ref()
            .ok_or(FormatError::EncryptionRequired)?;

        // read the 12-byte nonce
        let mut nonce = [0u8; crate::encryption::NONCE_SIZE];
        if let Err(e) = self.reader.read_exact(&mut nonce) {
            return if e.kind() == io::ErrorKind::UnexpectedEof {
                Ok(None)
            } else {
                Err(FormatError::Io(e))
            };
        }

        // read ciphertext length and ciphertext
        let ct_len = match format::read_u32(&mut self.reader) {
            Ok(n) => n as usize,
            Err(FormatError::UnexpectedEof) => return Ok(None),
            Err(e) => return Err(e),
        };

        if ct_len > format::MAX_FIELD_LEN {
            return Err(FormatError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("encrypted record length {ct_len} exceeds maximum"),
            )));
        }

        let mut ciphertext = vec![0u8; ct_len];
        if let Err(e) = self.reader.read_exact(&mut ciphertext) {
            return if e.kind() == io::ErrorKind::UnexpectedEof {
                Ok(None)
            } else {
                Err(FormatError::Io(e))
            };
        }

        let plaintext = crate::encryption::decrypt_record(key, &nonce, &ciphertext)?;
        AofRecord::from_bytes(&plaintext).map(Some)
    }

    /// Reads the remaining payload bytes (after the tag) and the trailing CRC.
    fn read_payload_for_tag(&mut self, tag: u8) -> Result<(Vec<u8>, u32), FormatError> {
        let mut payload = Vec::new();
        match tag {
            TAG_SET => {
                // key_len + key + value_len + value + expire_ms
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let value = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &value)?;
                let expire_ms = format::read_i64(&mut self.reader)?;
                format::write_i64(&mut payload, expire_ms)?;
            }
            TAG_DEL => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
            }
            TAG_EXPIRE => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let seconds = format::read_i64(&mut self.reader)?;
                format::write_i64(&mut payload, seconds)?;
            }
            TAG_LPUSH | TAG_RPUSH => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let count = format::read_u32(&mut self.reader)?;
                format::validate_collection_count(count, "list")?;
                format::write_u32(&mut payload, count)?;
                for _ in 0..count {
                    let val = format::read_bytes(&mut self.reader)?;
                    format::write_bytes(&mut payload, &val)?;
                }
            }
            TAG_LPOP | TAG_RPOP => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
            }
            TAG_ZADD => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let count = format::read_u32(&mut self.reader)?;
                format::validate_collection_count(count, "sorted set")?;
                format::write_u32(&mut payload, count)?;
                for _ in 0..count {
                    let score = format::read_f64(&mut self.reader)?;
                    format::write_f64(&mut payload, score)?;
                    let member = format::read_bytes(&mut self.reader)?;
                    format::write_bytes(&mut payload, &member)?;
                }
            }
            TAG_ZREM => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let count = format::read_u32(&mut self.reader)?;
                format::validate_collection_count(count, "sorted set")?;
                format::write_u32(&mut payload, count)?;
                for _ in 0..count {
                    let member = format::read_bytes(&mut self.reader)?;
                    format::write_bytes(&mut payload, &member)?;
                }
            }
            TAG_PERSIST => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
            }
            TAG_PEXPIRE => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let millis = format::read_i64(&mut self.reader)?;
                format::write_i64(&mut payload, millis)?;
            }
            TAG_INCR | TAG_DECR => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
            }
            TAG_HSET => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let count = format::read_u32(&mut self.reader)?;
                format::validate_collection_count(count, "hash")?;
                format::write_u32(&mut payload, count)?;
                for _ in 0..count {
                    let field = format::read_bytes(&mut self.reader)?;
                    format::write_bytes(&mut payload, &field)?;
                    let value = format::read_bytes(&mut self.reader)?;
                    format::write_bytes(&mut payload, &value)?;
                }
            }
            TAG_HDEL | TAG_SADD | TAG_SREM => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let count = format::read_u32(&mut self.reader)?;
                format::validate_collection_count(count, "set")?;
                format::write_u32(&mut payload, count)?;
                for _ in 0..count {
                    let item = format::read_bytes(&mut self.reader)?;
                    format::write_bytes(&mut payload, &item)?;
                }
            }
            TAG_HINCRBY => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let field = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &field)?;
                let delta = format::read_i64(&mut self.reader)?;
                format::write_i64(&mut payload, delta)?;
            }
            TAG_INCRBY | TAG_DECRBY => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let delta = format::read_i64(&mut self.reader)?;
                format::write_i64(&mut payload, delta)?;
            }
            TAG_APPEND => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let value = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &value)?;
            }
            TAG_RENAME => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let newkey = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &newkey)?;
            }
            #[cfg(feature = "vector")]
            TAG_VADD => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let element = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &element)?;
                let dim = format::read_u32(&mut self.reader)?;
                if dim > format::MAX_PERSISTED_VECTOR_DIMS {
                    return Err(FormatError::InvalidData(format!(
                        "AOF VADD dimension {dim} exceeds max {}",
                        format::MAX_PERSISTED_VECTOR_DIMS
                    )));
                }
                format::write_u32(&mut payload, dim)?;
                for _ in 0..dim {
                    let v = format::read_f32(&mut self.reader)?;
                    format::write_f32(&mut payload, v)?;
                }
                let metric = format::read_u8(&mut self.reader)?;
                format::write_u8(&mut payload, metric)?;
                let quantization = format::read_u8(&mut self.reader)?;
                format::write_u8(&mut payload, quantization)?;
                let connectivity = format::read_u32(&mut self.reader)?;
                format::write_u32(&mut payload, connectivity)?;
                let expansion_add = format::read_u32(&mut self.reader)?;
                format::write_u32(&mut payload, expansion_add)?;
            }
            #[cfg(feature = "vector")]
            TAG_VREM => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let element = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &element)?;
            }
            #[cfg(feature = "protobuf")]
            TAG_PROTO_SET => {
                let key = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &key)?;
                let type_name = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &type_name)?;
                let data = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &data)?;
                let expire_ms = format::read_i64(&mut self.reader)?;
                format::write_i64(&mut payload, expire_ms)?;
            }
            #[cfg(feature = "protobuf")]
            TAG_PROTO_REGISTER => {
                let name = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &name)?;
                let descriptor = format::read_bytes(&mut self.reader)?;
                format::write_bytes(&mut payload, &descriptor)?;
            }
            _ => return Err(FormatError::UnknownTag(tag)),
        }
        let stored_crc = format::read_u32(&mut self.reader)?;
        Ok((payload, stored_crc))
    }
}

/// Opens a persistence file with create+append and restrictive permissions.
fn open_persistence_file(path: &Path) -> Result<File, FormatError> {
    let mut opts = OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    Ok(opts.open(path)?)
}

/// Returns the AOF file path for a given shard in a data directory.
pub fn aof_path(data_dir: &Path, shard_id: u16) -> PathBuf {
    data_dir.join(format!("shard-{shard_id}.aof"))
}

#[cfg(test)]
mod tests {
    use super::*;

    type Result = std::result::Result<(), Box<dyn std::error::Error>>;

    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("create temp dir")
    }

    #[test]
    fn record_round_trip_set() -> Result {
        let rec = AofRecord::Set {
            key: "hello".into(),
            value: Bytes::from("world"),
            expire_ms: 5000,
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_del() -> Result {
        let rec = AofRecord::Del { key: "gone".into() };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_expire() -> Result {
        let rec = AofRecord::Expire {
            key: "ttl".into(),
            seconds: 300,
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn set_with_no_expiry() -> Result {
        let rec = AofRecord::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire_ms: -1,
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn writer_reader_round_trip() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("test.aof");

        let records = vec![
            AofRecord::Set {
                key: "a".into(),
                value: Bytes::from("1"),
                expire_ms: -1,
            },
            AofRecord::Set {
                key: "b".into(),
                value: Bytes::from("2"),
                expire_ms: 10_000,
            },
            AofRecord::Del { key: "a".into() },
            AofRecord::Expire {
                key: "b".into(),
                seconds: 60,
            },
        ];

        // write
        {
            let mut writer = AofWriter::open(&path)?;
            for rec in &records {
                writer.write_record(rec)?;
            }
            writer.sync()?;
        }

        // read back
        let mut reader = AofReader::open(&path)?;
        let mut got = Vec::new();
        while let Some(rec) = reader.read_record()? {
            got.push(rec);
        }
        assert_eq!(records, got);
        Ok(())
    }

    #[test]
    fn empty_aof_returns_no_records() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("empty.aof");

        // just write the header
        {
            let _writer = AofWriter::open(&path)?;
        }

        let mut reader = AofReader::open(&path)?;
        assert!(reader.read_record()?.is_none());
        Ok(())
    }

    #[test]
    fn truncated_record_treated_as_eof() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("trunc.aof");

        // write one good record, then append garbage (simulating a crash)
        {
            let mut writer = AofWriter::open(&path)?;
            writer.write_record(&AofRecord::Set {
                key: "ok".into(),
                value: Bytes::from("good"),
                expire_ms: -1,
            })?;
            writer.flush()?;
        }

        // append a partial tag with no payload
        {
            let mut file = OpenOptions::new().append(true).open(&path)?;
            file.write_all(&[TAG_SET])?;
        }

        let mut reader = AofReader::open(&path)?;
        // first record should be fine
        let rec = reader.read_record()?.unwrap();
        assert!(matches!(rec, AofRecord::Set { .. }));
        // second should be None (truncated)
        assert!(reader.read_record()?.is_none());
        Ok(())
    }

    #[test]
    fn corrupt_crc_detected() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("corrupt.aof");

        {
            let mut writer = AofWriter::open(&path)?;
            writer.write_record(&AofRecord::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire_ms: -1,
            })?;
            writer.flush()?;
        }

        // corrupt the last byte (part of the CRC)
        let mut data = fs::read(&path)?;
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&path, &data)?;

        let mut reader = AofReader::open(&path)?;
        let err = reader.read_record().unwrap_err();
        assert!(matches!(err, FormatError::ChecksumMismatch { .. }));
        Ok(())
    }

    #[test]
    fn missing_magic_is_error() {
        let dir = temp_dir();
        let path = dir.path().join("bad.aof");
        fs::write(&path, b"NOT_AOF_DATA").unwrap();

        let err = AofReader::open(&path).unwrap_err();
        assert!(matches!(err, FormatError::InvalidMagic));
    }

    #[test]
    fn truncate_resets_aof() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("reset.aof");

        {
            let mut writer = AofWriter::open(&path)?;
            writer.write_record(&AofRecord::Set {
                key: "old".into(),
                value: Bytes::from("data"),
                expire_ms: -1,
            })?;
            writer.truncate()?;

            // write a new record after truncation
            writer.write_record(&AofRecord::Set {
                key: "new".into(),
                value: Bytes::from("fresh"),
                expire_ms: -1,
            })?;
            writer.sync()?;
        }

        let mut reader = AofReader::open(&path)?;
        let rec = reader.read_record()?.unwrap();
        match rec {
            AofRecord::Set { key, .. } => assert_eq!(key, "new"),
            other => panic!("expected Set, got {other:?}"),
        }
        // only one record after truncation
        assert!(reader.read_record()?.is_none());
        Ok(())
    }

    #[test]
    fn record_round_trip_lpush() -> Result {
        let rec = AofRecord::LPush {
            key: "list".into(),
            values: vec![Bytes::from("a"), Bytes::from("b")],
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_rpush() -> Result {
        let rec = AofRecord::RPush {
            key: "list".into(),
            values: vec![Bytes::from("x")],
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_lpop() -> Result {
        let rec = AofRecord::LPop { key: "list".into() };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_rpop() -> Result {
        let rec = AofRecord::RPop { key: "list".into() };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn writer_reader_round_trip_with_list_records() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("list.aof");

        let records = vec![
            AofRecord::LPush {
                key: "l".into(),
                values: vec![Bytes::from("a"), Bytes::from("b")],
            },
            AofRecord::RPush {
                key: "l".into(),
                values: vec![Bytes::from("c")],
            },
            AofRecord::LPop { key: "l".into() },
            AofRecord::RPop { key: "l".into() },
        ];

        {
            let mut writer = AofWriter::open(&path)?;
            for rec in &records {
                writer.write_record(rec)?;
            }
            writer.sync()?;
        }

        let mut reader = AofReader::open(&path)?;
        let mut got = Vec::new();
        while let Some(rec) = reader.read_record()? {
            got.push(rec);
        }
        assert_eq!(records, got);
        Ok(())
    }

    #[test]
    fn record_round_trip_zadd() -> Result {
        let rec = AofRecord::ZAdd {
            key: "board".into(),
            members: vec![(100.0, "alice".into()), (200.5, "bob".into())],
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_zrem() -> Result {
        let rec = AofRecord::ZRem {
            key: "board".into(),
            members: vec!["alice".into(), "bob".into()],
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn writer_reader_round_trip_with_sorted_set_records() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("zset.aof");

        let records = vec![
            AofRecord::ZAdd {
                key: "board".into(),
                members: vec![(100.0, "alice".into()), (200.0, "bob".into())],
            },
            AofRecord::ZRem {
                key: "board".into(),
                members: vec!["alice".into()],
            },
        ];

        {
            let mut writer = AofWriter::open(&path)?;
            for rec in &records {
                writer.write_record(rec)?;
            }
            writer.sync()?;
        }

        let mut reader = AofReader::open(&path)?;
        let mut got = Vec::new();
        while let Some(rec) = reader.read_record()? {
            got.push(rec);
        }
        assert_eq!(records, got);
        Ok(())
    }

    #[test]
    fn record_round_trip_persist() -> Result {
        let rec = AofRecord::Persist {
            key: "mykey".into(),
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_pexpire() -> Result {
        let rec = AofRecord::Pexpire {
            key: "mykey".into(),
            milliseconds: 5000,
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_incr() -> Result {
        let rec = AofRecord::Incr {
            key: "counter".into(),
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_decr() -> Result {
        let rec = AofRecord::Decr {
            key: "counter".into(),
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn writer_reader_round_trip_with_persist_pexpire() -> Result {
        let dir = temp_dir();
        let path = dir.path().join("persist_pexpire.aof");

        let records = vec![
            AofRecord::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire_ms: 5000,
            },
            AofRecord::Persist { key: "k".into() },
            AofRecord::Pexpire {
                key: "k".into(),
                milliseconds: 3000,
            },
        ];

        {
            let mut writer = AofWriter::open(&path)?;
            for rec in &records {
                writer.write_record(rec)?;
            }
            writer.sync()?;
        }

        let mut reader = AofReader::open(&path)?;
        let mut got = Vec::new();
        while let Some(rec) = reader.read_record()? {
            got.push(rec);
        }
        assert_eq!(records, got);
        Ok(())
    }

    #[test]
    fn aof_path_format() {
        let p = aof_path(Path::new("/data"), 3);
        assert_eq!(p, PathBuf::from("/data/shard-3.aof"));
    }

    #[test]
    fn record_round_trip_hset() -> Result {
        let rec = AofRecord::HSet {
            key: "hash".into(),
            fields: vec![
                ("f1".into(), Bytes::from("v1")),
                ("f2".into(), Bytes::from("v2")),
            ],
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_hdel() -> Result {
        let rec = AofRecord::HDel {
            key: "hash".into(),
            fields: vec!["f1".into(), "f2".into()],
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_hincrby() -> Result {
        let rec = AofRecord::HIncrBy {
            key: "hash".into(),
            field: "counter".into(),
            delta: -42,
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_sadd() -> Result {
        let rec = AofRecord::SAdd {
            key: "set".into(),
            members: vec!["m1".into(), "m2".into(), "m3".into()],
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[test]
    fn record_round_trip_srem() -> Result {
        let rec = AofRecord::SRem {
            key: "set".into(),
            members: vec!["m1".into()],
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[cfg(feature = "vector")]
    #[test]
    fn record_round_trip_vadd() -> Result {
        let rec = AofRecord::VAdd {
            key: "embeddings".into(),
            element: "doc1".into(),
            vector: vec![0.1, 0.2, 0.3],
            metric: 0,       // cosine
            quantization: 0, // f32
            connectivity: 16,
            expansion_add: 64,
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[cfg(feature = "vector")]
    #[test]
    fn record_round_trip_vadd_high_dim() -> Result {
        let rec = AofRecord::VAdd {
            key: "vecs".into(),
            element: "e".into(),
            vector: vec![0.0; 1536], // typical embedding dimension
            metric: 1,               // l2
            quantization: 1,         // f16
            connectivity: 32,
            expansion_add: 128,
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[cfg(feature = "vector")]
    #[test]
    fn record_round_trip_vrem() -> Result {
        let rec = AofRecord::VRem {
            key: "embeddings".into(),
            element: "doc1".into(),
        };
        let bytes = rec.to_bytes()?;
        let decoded = AofRecord::from_bytes(&bytes)?;
        assert_eq!(rec, decoded);
        Ok(())
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::encryption::EncryptionKey;

        type Result = std::result::Result<(), Box<dyn std::error::Error>>;

        fn test_key() -> EncryptionKey {
            EncryptionKey::from_bytes([0x42; 32])
        }

        #[test]
        fn encrypted_writer_reader_round_trip() -> Result {
            let dir = temp_dir();
            let path = dir.path().join("enc.aof");
            let key = test_key();

            let records = vec![
                AofRecord::Set {
                    key: "a".into(),
                    value: Bytes::from("1"),
                    expire_ms: -1,
                },
                AofRecord::Del { key: "a".into() },
                AofRecord::LPush {
                    key: "list".into(),
                    values: vec![Bytes::from("x"), Bytes::from("y")],
                },
                AofRecord::ZAdd {
                    key: "zs".into(),
                    members: vec![(1.0, "m".into())],
                },
            ];

            {
                let mut writer = AofWriter::open_encrypted(&path, key.clone())?;
                for rec in &records {
                    writer.write_record(rec)?;
                }
                writer.sync()?;
            }

            let mut reader = AofReader::open_encrypted(&path, key)?;
            let mut got = Vec::new();
            while let Some(rec) = reader.read_record()? {
                got.push(rec);
            }
            assert_eq!(records, got);
            Ok(())
        }

        #[test]
        fn encrypted_aof_wrong_key_fails() -> Result {
            let dir = temp_dir();
            let path = dir.path().join("enc_bad.aof");
            let key = test_key();
            let wrong_key = EncryptionKey::from_bytes([0xFF; 32]);

            {
                let mut writer = AofWriter::open_encrypted(&path, key)?;
                writer.write_record(&AofRecord::Set {
                    key: "k".into(),
                    value: Bytes::from("v"),
                    expire_ms: -1,
                })?;
                writer.sync()?;
            }

            let mut reader = AofReader::open_encrypted(&path, wrong_key)?;
            let err = reader.read_record().unwrap_err();
            assert!(matches!(err, FormatError::DecryptionFailed));
            Ok(())
        }

        #[test]
        fn v2_file_readable_with_encryption_key() -> Result {
            let dir = temp_dir();
            let path = dir.path().join("v2.aof");
            let key = test_key();

            // write a plaintext v2 file
            {
                let mut writer = AofWriter::open(&path)?;
                writer.write_record(&AofRecord::Set {
                    key: "k".into(),
                    value: Bytes::from("v"),
                    expire_ms: -1,
                })?;
                writer.sync()?;
            }

            // read with encryption key — should work (v2 is plaintext)
            let mut reader = AofReader::open_encrypted(&path, key)?;
            let rec = reader.read_record()?.unwrap();
            assert!(matches!(rec, AofRecord::Set { .. }));
            Ok(())
        }

        #[test]
        fn v3_file_without_key_returns_error() -> Result {
            let dir = temp_dir();
            let path = dir.path().join("v3_nokey.aof");
            let key = test_key();

            // write an encrypted v3 file
            {
                let mut writer = AofWriter::open_encrypted(&path, key)?;
                writer.write_record(&AofRecord::Set {
                    key: "k".into(),
                    value: Bytes::from("v"),
                    expire_ms: -1,
                })?;
                writer.sync()?;
            }

            // try to open without a key
            let err = AofReader::open(&path).unwrap_err();
            assert!(matches!(err, FormatError::EncryptionRequired));
            Ok(())
        }

        #[test]
        fn encrypted_truncate_preserves_encryption() -> Result {
            let dir = temp_dir();
            let path = dir.path().join("enc_trunc.aof");
            let key = test_key();

            {
                let mut writer = AofWriter::open_encrypted(&path, key.clone())?;
                writer.write_record(&AofRecord::Set {
                    key: "old".into(),
                    value: Bytes::from("data"),
                    expire_ms: -1,
                })?;
                writer.truncate()?;

                writer.write_record(&AofRecord::Set {
                    key: "new".into(),
                    value: Bytes::from("fresh"),
                    expire_ms: -1,
                })?;
                writer.sync()?;
            }

            let mut reader = AofReader::open_encrypted(&path, key)?;
            let rec = reader.read_record()?.unwrap();
            match rec {
                AofRecord::Set { key, .. } => assert_eq!(key, "new"),
                other => panic!("expected Set, got {other:?}"),
            }
            assert!(reader.read_record()?.is_none());
            Ok(())
        }
    }
}
