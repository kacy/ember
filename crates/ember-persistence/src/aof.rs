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
    /// Serializes this record into a byte vector (tag + payload, no CRC).
    fn to_bytes(&self) -> Result<Vec<u8>, FormatError> {
        let mut buf = Vec::new();
        match self {
            AofRecord::Set {
                key,
                value,
                expire_ms,
            } => {
                format::write_u8(&mut buf, TAG_SET)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, value)?;
                format::write_i64(&mut buf, *expire_ms)?;
            }
            AofRecord::Del { key } => {
                format::write_u8(&mut buf, TAG_DEL)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
            }
            AofRecord::Expire { key, seconds } => {
                format::write_u8(&mut buf, TAG_EXPIRE)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, *seconds as i64)?;
            }
            AofRecord::LPush { key, values } => {
                format::write_u8(&mut buf, TAG_LPUSH)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_u32(&mut buf, values.len() as u32)?;
                for v in values {
                    format::write_bytes(&mut buf, v)?;
                }
            }
            AofRecord::RPush { key, values } => {
                format::write_u8(&mut buf, TAG_RPUSH)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_u32(&mut buf, values.len() as u32)?;
                for v in values {
                    format::write_bytes(&mut buf, v)?;
                }
            }
            AofRecord::LPop { key } => {
                format::write_u8(&mut buf, TAG_LPOP)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
            }
            AofRecord::RPop { key } => {
                format::write_u8(&mut buf, TAG_RPOP)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
            }
            AofRecord::ZAdd { key, members } => {
                format::write_u8(&mut buf, TAG_ZADD)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_u32(&mut buf, members.len() as u32)?;
                for (score, member) in members {
                    format::write_f64(&mut buf, *score)?;
                    format::write_bytes(&mut buf, member.as_bytes())?;
                }
            }
            AofRecord::ZRem { key, members } => {
                format::write_u8(&mut buf, TAG_ZREM)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_u32(&mut buf, members.len() as u32)?;
                for member in members {
                    format::write_bytes(&mut buf, member.as_bytes())?;
                }
            }
            AofRecord::Persist { key } => {
                format::write_u8(&mut buf, TAG_PERSIST)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
            }
            AofRecord::Pexpire { key, milliseconds } => {
                format::write_u8(&mut buf, TAG_PEXPIRE)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, *milliseconds as i64)?;
            }
            AofRecord::Incr { key } => {
                format::write_u8(&mut buf, TAG_INCR)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
            }
            AofRecord::Decr { key } => {
                format::write_u8(&mut buf, TAG_DECR)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
            }
            AofRecord::HSet { key, fields } => {
                format::write_u8(&mut buf, TAG_HSET)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_u32(&mut buf, fields.len() as u32)?;
                for (field, value) in fields {
                    format::write_bytes(&mut buf, field.as_bytes())?;
                    format::write_bytes(&mut buf, value)?;
                }
            }
            AofRecord::HDel { key, fields } => {
                format::write_u8(&mut buf, TAG_HDEL)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_u32(&mut buf, fields.len() as u32)?;
                for field in fields {
                    format::write_bytes(&mut buf, field.as_bytes())?;
                }
            }
            AofRecord::HIncrBy { key, field, delta } => {
                format::write_u8(&mut buf, TAG_HINCRBY)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, field.as_bytes())?;
                format::write_i64(&mut buf, *delta)?;
            }
            AofRecord::SAdd { key, members } => {
                format::write_u8(&mut buf, TAG_SADD)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_u32(&mut buf, members.len() as u32)?;
                for member in members {
                    format::write_bytes(&mut buf, member.as_bytes())?;
                }
            }
            AofRecord::SRem { key, members } => {
                format::write_u8(&mut buf, TAG_SREM)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_u32(&mut buf, members.len() as u32)?;
                for member in members {
                    format::write_bytes(&mut buf, member.as_bytes())?;
                }
            }
            AofRecord::IncrBy { key, delta } => {
                format::write_u8(&mut buf, TAG_INCRBY)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, *delta)?;
            }
            AofRecord::DecrBy { key, delta } => {
                format::write_u8(&mut buf, TAG_DECRBY)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_i64(&mut buf, *delta)?;
            }
            AofRecord::Append { key, value } => {
                format::write_u8(&mut buf, TAG_APPEND)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, value)?;
            }
            AofRecord::Rename { key, newkey } => {
                format::write_u8(&mut buf, TAG_RENAME)?;
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
                format::write_u8(&mut buf, TAG_VADD)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, element.as_bytes())?;
                format::write_u32(&mut buf, vector.len() as u32)?;
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
                format::write_u8(&mut buf, TAG_VREM)?;
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
                format::write_u8(&mut buf, TAG_PROTO_SET)?;
                format::write_bytes(&mut buf, key.as_bytes())?;
                format::write_bytes(&mut buf, type_name.as_bytes())?;
                format::write_bytes(&mut buf, data)?;
                format::write_i64(&mut buf, *expire_ms)?;
            }
            #[cfg(feature = "protobuf")]
            AofRecord::ProtoRegister { name, descriptor } => {
                format::write_u8(&mut buf, TAG_PROTO_REGISTER)?;
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
                let seconds = format::read_i64(&mut cursor)? as u64;
                Ok(AofRecord::Expire { key, seconds })
            }
            TAG_LPUSH | TAG_RPUSH => {
                let key = read_string(&mut cursor, "key")?;
                let count = format::read_u32(&mut cursor)?;
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
                let milliseconds = format::read_i64(&mut cursor)? as u64;
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
                let mut vector = Vec::with_capacity(format::capped_capacity(dim));
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
            format::write_u32(&mut self.writer, ciphertext.len() as u32)?;
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
    /// Used after a successful snapshot to reset the log.
    pub fn truncate(&mut self) -> Result<(), FormatError> {
        // flush and drop the old writer
        self.writer.flush()?;

        // reopen the file with truncation, write fresh header
        let mut opts = OpenOptions::new();
        opts.create(true).write(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let file = opts.open(&self.path)?;
        let mut writer = BufWriter::new(file);

        #[cfg(feature = "encryption")]
        if self.encryption_key.is_some() {
            format::write_header_versioned(
                &mut writer,
                format::AOF_MAGIC,
                format::FORMAT_VERSION_ENCRYPTED,
            )?;
        } else {
            format::write_header(&mut writer, format::AOF_MAGIC)?;
        }
        #[cfg(not(feature = "encryption"))]
        format::write_header(&mut writer, format::AOF_MAGIC)?;

        writer.flush()?;
        // ensure the fresh header is durable before we start appending
        writer.get_ref().sync_all()?;
        self.writer = writer;
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
    fn temp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("create temp dir")
    }

    #[test]
    fn record_round_trip_set() {
        let rec = AofRecord::Set {
            key: "hello".into(),
            value: Bytes::from("world"),
            expire_ms: 5000,
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_del() {
        let rec = AofRecord::Del { key: "gone".into() };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_expire() {
        let rec = AofRecord::Expire {
            key: "ttl".into(),
            seconds: 300,
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn set_with_no_expiry() {
        let rec = AofRecord::Set {
            key: "k".into(),
            value: Bytes::from("v"),
            expire_ms: -1,
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn writer_reader_round_trip() {
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
            let mut writer = AofWriter::open(&path).unwrap();
            for rec in &records {
                writer.write_record(rec).unwrap();
            }
            writer.sync().unwrap();
        }

        // read back
        let mut reader = AofReader::open(&path).unwrap();
        let mut got = Vec::new();
        while let Some(rec) = reader.read_record().unwrap() {
            got.push(rec);
        }
        assert_eq!(records, got);
    }

    #[test]
    fn empty_aof_returns_no_records() {
        let dir = temp_dir();
        let path = dir.path().join("empty.aof");

        // just write the header
        {
            let _writer = AofWriter::open(&path).unwrap();
        }

        let mut reader = AofReader::open(&path).unwrap();
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn truncated_record_treated_as_eof() {
        let dir = temp_dir();
        let path = dir.path().join("trunc.aof");

        // write one good record, then append garbage (simulating a crash)
        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "ok".into(),
                    value: Bytes::from("good"),
                    expire_ms: -1,
                })
                .unwrap();
            writer.flush().unwrap();
        }

        // append a partial tag with no payload
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&[TAG_SET]).unwrap();
        }

        let mut reader = AofReader::open(&path).unwrap();
        // first record should be fine
        let rec = reader.read_record().unwrap().unwrap();
        assert!(matches!(rec, AofRecord::Set { .. }));
        // second should be None (truncated)
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn corrupt_crc_detected() {
        let dir = temp_dir();
        let path = dir.path().join("corrupt.aof");

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "k".into(),
                    value: Bytes::from("v"),
                    expire_ms: -1,
                })
                .unwrap();
            writer.flush().unwrap();
        }

        // corrupt the last byte (part of the CRC)
        let mut data = fs::read(&path).unwrap();
        let last = data.len() - 1;
        data[last] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let mut reader = AofReader::open(&path).unwrap();
        let err = reader.read_record().unwrap_err();
        assert!(matches!(err, FormatError::ChecksumMismatch { .. }));
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
    fn truncate_resets_aof() {
        let dir = temp_dir();
        let path = dir.path().join("reset.aof");

        {
            let mut writer = AofWriter::open(&path).unwrap();
            writer
                .write_record(&AofRecord::Set {
                    key: "old".into(),
                    value: Bytes::from("data"),
                    expire_ms: -1,
                })
                .unwrap();
            writer.truncate().unwrap();

            // write a new record after truncation
            writer
                .write_record(&AofRecord::Set {
                    key: "new".into(),
                    value: Bytes::from("fresh"),
                    expire_ms: -1,
                })
                .unwrap();
            writer.sync().unwrap();
        }

        let mut reader = AofReader::open(&path).unwrap();
        let rec = reader.read_record().unwrap().unwrap();
        match rec {
            AofRecord::Set { key, .. } => assert_eq!(key, "new"),
            other => panic!("expected Set, got {other:?}"),
        }
        // only one record after truncation
        assert!(reader.read_record().unwrap().is_none());
    }

    #[test]
    fn record_round_trip_lpush() {
        let rec = AofRecord::LPush {
            key: "list".into(),
            values: vec![Bytes::from("a"), Bytes::from("b")],
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_rpush() {
        let rec = AofRecord::RPush {
            key: "list".into(),
            values: vec![Bytes::from("x")],
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_lpop() {
        let rec = AofRecord::LPop { key: "list".into() };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_rpop() {
        let rec = AofRecord::RPop { key: "list".into() };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn writer_reader_round_trip_with_list_records() {
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
            let mut writer = AofWriter::open(&path).unwrap();
            for rec in &records {
                writer.write_record(rec).unwrap();
            }
            writer.sync().unwrap();
        }

        let mut reader = AofReader::open(&path).unwrap();
        let mut got = Vec::new();
        while let Some(rec) = reader.read_record().unwrap() {
            got.push(rec);
        }
        assert_eq!(records, got);
    }

    #[test]
    fn record_round_trip_zadd() {
        let rec = AofRecord::ZAdd {
            key: "board".into(),
            members: vec![(100.0, "alice".into()), (200.5, "bob".into())],
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_zrem() {
        let rec = AofRecord::ZRem {
            key: "board".into(),
            members: vec!["alice".into(), "bob".into()],
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn writer_reader_round_trip_with_sorted_set_records() {
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
            let mut writer = AofWriter::open(&path).unwrap();
            for rec in &records {
                writer.write_record(rec).unwrap();
            }
            writer.sync().unwrap();
        }

        let mut reader = AofReader::open(&path).unwrap();
        let mut got = Vec::new();
        while let Some(rec) = reader.read_record().unwrap() {
            got.push(rec);
        }
        assert_eq!(records, got);
    }

    #[test]
    fn record_round_trip_persist() {
        let rec = AofRecord::Persist {
            key: "mykey".into(),
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_pexpire() {
        let rec = AofRecord::Pexpire {
            key: "mykey".into(),
            milliseconds: 5000,
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_incr() {
        let rec = AofRecord::Incr {
            key: "counter".into(),
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_decr() {
        let rec = AofRecord::Decr {
            key: "counter".into(),
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn writer_reader_round_trip_with_persist_pexpire() {
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
            let mut writer = AofWriter::open(&path).unwrap();
            for rec in &records {
                writer.write_record(rec).unwrap();
            }
            writer.sync().unwrap();
        }

        let mut reader = AofReader::open(&path).unwrap();
        let mut got = Vec::new();
        while let Some(rec) = reader.read_record().unwrap() {
            got.push(rec);
        }
        assert_eq!(records, got);
    }

    #[test]
    fn aof_path_format() {
        let p = aof_path(Path::new("/data"), 3);
        assert_eq!(p, PathBuf::from("/data/shard-3.aof"));
    }

    #[test]
    fn record_round_trip_hset() {
        let rec = AofRecord::HSet {
            key: "hash".into(),
            fields: vec![
                ("f1".into(), Bytes::from("v1")),
                ("f2".into(), Bytes::from("v2")),
            ],
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_hdel() {
        let rec = AofRecord::HDel {
            key: "hash".into(),
            fields: vec!["f1".into(), "f2".into()],
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_hincrby() {
        let rec = AofRecord::HIncrBy {
            key: "hash".into(),
            field: "counter".into(),
            delta: -42,
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_sadd() {
        let rec = AofRecord::SAdd {
            key: "set".into(),
            members: vec!["m1".into(), "m2".into(), "m3".into()],
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[test]
    fn record_round_trip_srem() {
        let rec = AofRecord::SRem {
            key: "set".into(),
            members: vec!["m1".into()],
        };
        let bytes = rec.to_bytes().unwrap();
        let decoded = AofRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec, decoded);
    }

    #[cfg(feature = "encryption")]
    mod encrypted {
        use super::*;
        use crate::encryption::EncryptionKey;

        fn test_key() -> EncryptionKey {
            EncryptionKey::from_bytes([0x42; 32])
        }

        #[test]
        fn encrypted_writer_reader_round_trip() {
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
                let mut writer = AofWriter::open_encrypted(&path, key.clone()).unwrap();
                for rec in &records {
                    writer.write_record(rec).unwrap();
                }
                writer.sync().unwrap();
            }

            let mut reader = AofReader::open_encrypted(&path, key).unwrap();
            let mut got = Vec::new();
            while let Some(rec) = reader.read_record().unwrap() {
                got.push(rec);
            }
            assert_eq!(records, got);
        }

        #[test]
        fn encrypted_aof_wrong_key_fails() {
            let dir = temp_dir();
            let path = dir.path().join("enc_bad.aof");
            let key = test_key();
            let wrong_key = EncryptionKey::from_bytes([0xFF; 32]);

            {
                let mut writer = AofWriter::open_encrypted(&path, key).unwrap();
                writer
                    .write_record(&AofRecord::Set {
                        key: "k".into(),
                        value: Bytes::from("v"),
                        expire_ms: -1,
                    })
                    .unwrap();
                writer.sync().unwrap();
            }

            let mut reader = AofReader::open_encrypted(&path, wrong_key).unwrap();
            let err = reader.read_record().unwrap_err();
            assert!(matches!(err, FormatError::DecryptionFailed));
        }

        #[test]
        fn v2_file_readable_with_encryption_key() {
            let dir = temp_dir();
            let path = dir.path().join("v2.aof");
            let key = test_key();

            // write a plaintext v2 file
            {
                let mut writer = AofWriter::open(&path).unwrap();
                writer
                    .write_record(&AofRecord::Set {
                        key: "k".into(),
                        value: Bytes::from("v"),
                        expire_ms: -1,
                    })
                    .unwrap();
                writer.sync().unwrap();
            }

            // read with encryption key — should work (v2 is plaintext)
            let mut reader = AofReader::open_encrypted(&path, key).unwrap();
            let rec = reader.read_record().unwrap().unwrap();
            assert!(matches!(rec, AofRecord::Set { .. }));
        }

        #[test]
        fn v3_file_without_key_returns_error() {
            let dir = temp_dir();
            let path = dir.path().join("v3_nokey.aof");
            let key = test_key();

            // write an encrypted v3 file
            {
                let mut writer = AofWriter::open_encrypted(&path, key).unwrap();
                writer
                    .write_record(&AofRecord::Set {
                        key: "k".into(),
                        value: Bytes::from("v"),
                        expire_ms: -1,
                    })
                    .unwrap();
                writer.sync().unwrap();
            }

            // try to open without a key
            let err = AofReader::open(&path).unwrap_err();
            assert!(matches!(err, FormatError::EncryptionRequired));
        }

        #[test]
        fn encrypted_truncate_preserves_encryption() {
            let dir = temp_dir();
            let path = dir.path().join("enc_trunc.aof");
            let key = test_key();

            {
                let mut writer = AofWriter::open_encrypted(&path, key.clone()).unwrap();
                writer
                    .write_record(&AofRecord::Set {
                        key: "old".into(),
                        value: Bytes::from("data"),
                        expire_ms: -1,
                    })
                    .unwrap();
                writer.truncate().unwrap();

                writer
                    .write_record(&AofRecord::Set {
                        key: "new".into(),
                        value: Bytes::from("fresh"),
                        expire_ms: -1,
                    })
                    .unwrap();
                writer.sync().unwrap();
            }

            let mut reader = AofReader::open_encrypted(&path, key).unwrap();
            let rec = reader.read_record().unwrap().unwrap();
            match rec {
                AofRecord::Set { key, .. } => assert_eq!(key, "new"),
                other => panic!("expected Set, got {other:?}"),
            }
            assert!(reader.read_record().unwrap().is_none());
        }
    }
}
