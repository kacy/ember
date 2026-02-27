//! Typed command API for the ember client.
//!
//! Provides ergonomic methods on [`Client`] for all common commands. Each
//! method takes strongly-typed inputs and returns decoded Rust values — no
//! manual frame inspection needed.
//!
//! Value inputs use `impl AsRef<[u8]>`, so `&str`, `String`, `Vec<u8>`, and
//! `bytes::Bytes` all work without explicit conversion.
//!
//! Value outputs use `Bytes` (binary-safe, reference-counted). Convert to a
//! `String` with `String::from_utf8(bytes.to_vec())` when you know the value
//! is UTF-8.

use bytes::Bytes;
use ember_protocol::types::Frame;

use crate::connection::{Client, ClientError};
use crate::pipeline::Pipeline;
use crate::subscriber::Subscriber;

// --- public types ---

/// A page of keys returned by [`Client::scan`].
///
/// Iterate until `cursor` is `0` to walk the full keyspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPage {
    /// Cursor for the next call. `0` means iteration is complete.
    pub cursor: u64,
    /// Keys returned in this page.
    pub keys: Vec<Bytes>,
}

/// A single entry from [`Client::slowlog_get`].
#[derive(Debug, Clone)]
pub struct SlowlogEntry {
    /// Monotonically increasing log entry ID.
    pub id: i64,
    /// Unix timestamp (seconds) when the command was logged.
    pub timestamp: i64,
    /// Execution time in microseconds.
    pub duration_us: i64,
    /// The command and its arguments as raw bytes.
    pub command: Vec<Bytes>,
}

// --- frame construction helpers ---

fn cmd2(name: &'static [u8], a: &[u8]) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(name)),
        Frame::Bulk(Bytes::copy_from_slice(a)),
    ])
}

fn cmd3(name: &'static [u8], a: &[u8], b: &[u8]) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(name)),
        Frame::Bulk(Bytes::copy_from_slice(a)),
        Frame::Bulk(Bytes::copy_from_slice(b)),
    ])
}

fn cmd4(name: &'static [u8], a: &[u8], b: &[u8], c: &[u8]) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(name)),
        Frame::Bulk(Bytes::copy_from_slice(a)),
        Frame::Bulk(Bytes::copy_from_slice(b)),
        Frame::Bulk(Bytes::copy_from_slice(c)),
    ])
}

// --- frame decoding helpers ---
//
// Each decoder maps a raw Frame to a typed result.
// `Frame::Error` → `ClientError::Server`
// Unexpected variants → `ClientError::Protocol`

/// Decodes a bulk string response, returning `None` for null.
///
/// Used by: GET, LPOP, RPOP, HGET, GETDEL.
fn optional_bytes(frame: Frame) -> Result<Option<Bytes>, ClientError> {
    match frame {
        Frame::Bulk(b) => Ok(Some(b)),
        Frame::Null => Ok(None),
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected bulk or null, got {other:?}"
        ))),
    }
}

/// Decodes an integer response.
///
/// Used by: DEL, EXISTS, INCR, DECR, INCRBY, DECRBY, APPEND, LPUSH, RPUSH,
/// LLEN, HSET, HDEL, HLEN, SADD, SREM, SCARD, ZREM, ZCARD, ZADD, DBSIZE.
fn integer(frame: Frame) -> Result<i64, ClientError> {
    match frame {
        Frame::Integer(n) => Ok(n),
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected integer, got {other:?}"
        ))),
    }
}

/// Decodes an integer response, returning `None` for null.
///
/// Used by: ZRANK (returns null when member is absent).
fn optional_integer(frame: Frame) -> Result<Option<i64>, ClientError> {
    match frame {
        Frame::Integer(n) => Ok(Some(n)),
        Frame::Null => Ok(None),
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected integer or null, got {other:?}"
        ))),
    }
}

/// Decodes an integer 0/1 as a boolean.
///
/// Used by: EXPIRE, PERSIST, HEXISTS, SISMEMBER.
fn bool_flag(frame: Frame) -> Result<bool, ClientError> {
    match frame {
        Frame::Integer(1) => Ok(true),
        Frame::Integer(0) => Ok(false),
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected 0 or 1, got {other:?}"
        ))),
    }
}

/// Decodes an OK simple string response.
///
/// Used by: SET, MSET, FLUSHDB.
fn ok(frame: Frame) -> Result<(), ClientError> {
    match frame {
        Frame::Simple(s) if s == "OK" => Ok(()),
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected +OK, got {other:?}"
        ))),
    }
}

/// Decodes an array of bulk strings.
///
/// Used by: LRANGE, SMEMBERS, HKEYS, HVALS, ZRANGE.
fn bytes_vec(frame: Frame) -> Result<Vec<Bytes>, ClientError> {
    match frame {
        Frame::Array(elems) => elems
            .into_iter()
            .map(|e| match e {
                Frame::Bulk(b) => Ok(b),
                Frame::Error(e) => Err(ClientError::Server(e)),
                other => Err(ClientError::Protocol(format!(
                    "expected bulk in array, got {other:?}"
                ))),
            })
            .collect(),
        Frame::Null => Ok(Vec::new()),
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected array, got {other:?}"
        ))),
    }
}

/// Decodes an array where individual elements may be null.
///
/// Used by: MGET (missing keys come back as null within the array).
fn optional_bytes_vec(frame: Frame) -> Result<Vec<Option<Bytes>>, ClientError> {
    match frame {
        Frame::Array(elems) => elems
            .into_iter()
            .map(|e| match e {
                Frame::Bulk(b) => Ok(Some(b)),
                Frame::Null => Ok(None),
                Frame::Error(e) => Err(ClientError::Server(e)),
                other => Err(ClientError::Protocol(format!(
                    "unexpected element in array: {other:?}"
                ))),
            })
            .collect(),
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected array, got {other:?}"
        ))),
    }
}

/// Decodes a flat array of alternating keys and values into pairs.
///
/// Used by: HGETALL (field, value, field, value, ...).
fn pairs(frame: Frame) -> Result<Vec<(Bytes, Bytes)>, ClientError> {
    let elems = match frame {
        Frame::Array(e) => e,
        Frame::Null => return Ok(Vec::new()),
        Frame::Error(e) => return Err(ClientError::Server(e)),
        other => {
            return Err(ClientError::Protocol(format!(
                "expected array for pairs, got {other:?}"
            )))
        }
    };

    if elems.len() % 2 != 0 {
        return Err(ClientError::Protocol(format!(
            "pairs array has odd length ({})",
            elems.len()
        )));
    }

    let mut result = Vec::with_capacity(elems.len() / 2);
    let mut iter = elems.into_iter();
    while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
        let key = match k {
            Frame::Bulk(b) => b,
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected bulk key in pairs, got {other:?}"
                )))
            }
        };
        let val = match v {
            Frame::Bulk(b) => b,
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected bulk value in pairs, got {other:?}"
                )))
            }
        };
        result.push((key, val));
    }
    Ok(result)
}

/// Decodes a score returned as a bulk string float, returning `None` for null.
///
/// Used by: ZSCORE.
fn optional_score(frame: Frame) -> Result<Option<f64>, ClientError> {
    match frame {
        Frame::Bulk(b) => {
            let s = std::str::from_utf8(&b)
                .map_err(|_| ClientError::Protocol("score is not valid UTF-8".into()))?;
            s.parse::<f64>()
                .map(Some)
                .map_err(|_| ClientError::Protocol(format!("score is not a valid float: {s:?}")))
        }
        Frame::Null => Ok(None),
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected bulk or null for score, got {other:?}"
        ))),
    }
}

/// Decodes `ZRANGE WITHSCORES` — alternating member / score pairs.
///
/// Used by: zrange_withscores.
fn scored_members(frame: Frame) -> Result<Vec<(Bytes, f64)>, ClientError> {
    let elems = match frame {
        Frame::Array(e) => e,
        Frame::Null => return Ok(Vec::new()),
        Frame::Error(e) => return Err(ClientError::Server(e)),
        other => {
            return Err(ClientError::Protocol(format!(
                "expected array for scored members, got {other:?}"
            )))
        }
    };

    if elems.len() % 2 != 0 {
        return Err(ClientError::Protocol(format!(
            "WITHSCORES array has odd length ({})",
            elems.len()
        )));
    }

    let mut result = Vec::with_capacity(elems.len() / 2);
    let mut iter = elems.into_iter();
    while let (Some(member_frame), Some(score_frame)) = (iter.next(), iter.next()) {
        let member = match member_frame {
            Frame::Bulk(b) => b,
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected bulk member, got {other:?}"
                )))
            }
        };
        let score = match score_frame {
            Frame::Bulk(b) => {
                let s = std::str::from_utf8(&b)
                    .map_err(|_| ClientError::Protocol("score is not valid UTF-8".into()))?;
                s.parse::<f64>().map_err(|_| {
                    ClientError::Protocol(format!("score is not a valid float: {s:?}"))
                })?
            }
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected bulk score, got {other:?}"
                )))
            }
        };
        result.push((member, score));
    }
    Ok(result)
}

/// Decodes a simple string or bulk string response into a `String`.
///
/// Used by: TYPE, INFO, ECHO, BGSAVE, BGREWRITEAOF.
fn string_value(frame: Frame) -> Result<String, ClientError> {
    match frame {
        Frame::Simple(s) => Ok(s),
        Frame::Bulk(b) => String::from_utf8(b.to_vec())
            .map_err(|_| ClientError::Protocol("response is not valid UTF-8".into())),
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected simple or bulk string, got {other:?}"
        ))),
    }
}

/// Decodes a float returned as a bulk string.
///
/// Used by: INCRBYFLOAT.
fn float_value(frame: Frame) -> Result<f64, ClientError> {
    match frame {
        Frame::Bulk(b) => {
            let s = std::str::from_utf8(&b)
                .map_err(|_| ClientError::Protocol("float response is not valid UTF-8".into()))?;
            s.parse::<f64>()
                .map_err(|_| ClientError::Protocol(format!("not a valid float: {s:?}")))
        }
        Frame::Error(e) => Err(ClientError::Server(e)),
        other => Err(ClientError::Protocol(format!(
            "expected bulk float, got {other:?}"
        ))),
    }
}

/// Decodes a SCAN response into a `ScanPage`.
///
/// RESP3 layout: `Array([Bulk(cursor), Array([Bulk(key), ...])])`.
fn scan_page(frame: Frame) -> Result<ScanPage, ClientError> {
    let elems = match frame {
        Frame::Array(e) => e,
        Frame::Error(e) => return Err(ClientError::Server(e)),
        other => {
            return Err(ClientError::Protocol(format!(
                "expected array for SCAN, got {other:?}"
            )))
        }
    };

    if elems.len() != 2 {
        return Err(ClientError::Protocol(format!(
            "SCAN response must have 2 elements, got {}",
            elems.len()
        )));
    }

    let mut iter = elems.into_iter();
    let cursor_frame = iter.next().unwrap();
    let keys_frame = iter.next().unwrap();

    let cursor = match cursor_frame {
        Frame::Bulk(b) => {
            let s = std::str::from_utf8(&b)
                .map_err(|_| ClientError::Protocol("SCAN cursor is not valid UTF-8".into()))?;
            s.parse::<u64>()
                .map_err(|_| ClientError::Protocol(format!("SCAN cursor is not a u64: {s:?}")))?
        }
        other => {
            return Err(ClientError::Protocol(format!(
                "expected bulk cursor in SCAN, got {other:?}"
            )))
        }
    };

    let keys = bytes_vec(keys_frame)?;
    Ok(ScanPage { cursor, keys })
}

/// Decodes a SLOWLOG GET response into a list of entries.
///
/// Each entry: `Array([Integer(id), Integer(ts), Integer(us), Array([Bulk(cmd), ...])])`.
fn slowlog_entries(frame: Frame) -> Result<Vec<SlowlogEntry>, ClientError> {
    let outer = match frame {
        Frame::Array(e) => e,
        Frame::Null => return Ok(Vec::new()),
        Frame::Error(e) => return Err(ClientError::Server(e)),
        other => {
            return Err(ClientError::Protocol(format!(
                "expected array for SLOWLOG, got {other:?}"
            )))
        }
    };

    outer
        .into_iter()
        .map(|entry_frame| {
            let entry = match entry_frame {
                Frame::Array(e) => e,
                other => {
                    return Err(ClientError::Protocol(format!(
                        "expected array for slowlog entry, got {other:?}"
                    )))
                }
            };

            if entry.len() < 4 {
                return Err(ClientError::Protocol(format!(
                    "slowlog entry too short: {} elements",
                    entry.len()
                )));
            }

            let id = match &entry[0] {
                Frame::Integer(n) => *n,
                other => {
                    return Err(ClientError::Protocol(format!(
                        "expected integer id in slowlog, got {other:?}"
                    )))
                }
            };
            let timestamp = match &entry[1] {
                Frame::Integer(n) => *n,
                other => {
                    return Err(ClientError::Protocol(format!(
                        "expected integer timestamp in slowlog, got {other:?}"
                    )))
                }
            };
            let duration_us = match &entry[2] {
                Frame::Integer(n) => *n,
                other => {
                    return Err(ClientError::Protocol(format!(
                        "expected integer duration in slowlog, got {other:?}"
                    )))
                }
            };
            let command = match entry.into_iter().nth(3).unwrap() {
                Frame::Array(parts) => parts
                    .into_iter()
                    .map(|p| match p {
                        Frame::Bulk(b) => Ok(b),
                        other => Err(ClientError::Protocol(format!(
                            "expected bulk in slowlog command, got {other:?}"
                        ))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                other => {
                    return Err(ClientError::Protocol(format!(
                        "expected array for slowlog command, got {other:?}"
                    )))
                }
            };

            Ok(SlowlogEntry {
                id,
                timestamp,
                duration_us,
                command,
            })
        })
        .collect()
}

/// Decodes a PUBSUB NUMSUB response into channel/count pairs.
///
/// Layout: `Array([Bulk(channel), Integer(count), ...])` — alternating.
fn numsub_pairs(frame: Frame) -> Result<Vec<(Bytes, i64)>, ClientError> {
    let elems = match frame {
        Frame::Array(e) => e,
        Frame::Null => return Ok(Vec::new()),
        Frame::Error(e) => return Err(ClientError::Server(e)),
        other => {
            return Err(ClientError::Protocol(format!(
                "expected array for PUBSUB NUMSUB, got {other:?}"
            )))
        }
    };

    if elems.len() % 2 != 0 {
        return Err(ClientError::Protocol(format!(
            "PUBSUB NUMSUB array has odd length ({})",
            elems.len()
        )));
    }

    let mut result = Vec::with_capacity(elems.len() / 2);
    let mut iter = elems.into_iter();
    while let (Some(ch_frame), Some(cnt_frame)) = (iter.next(), iter.next()) {
        let channel = match ch_frame {
            Frame::Bulk(b) => b,
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected bulk channel in PUBSUB NUMSUB, got {other:?}"
                )))
            }
        };
        let count = match cnt_frame {
            Frame::Integer(n) => n,
            other => {
                return Err(ClientError::Protocol(format!(
                    "expected integer count in PUBSUB NUMSUB, got {other:?}"
                )))
            }
        };
        result.push((channel, count));
    }
    Ok(result)
}

// --- typed command API ---

impl Client {
    // --- string commands ---

    /// Returns the value for `key`, or `None` if it does not exist.
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>, ClientError> {
        let frame = self.send_frame(cmd2(b"GET", key.as_bytes())).await?;
        optional_bytes(frame)
    }

    /// Sets `key` to `value` with no expiry.
    pub async fn set(&mut self, key: &str, value: impl AsRef<[u8]>) -> Result<(), ClientError> {
        let frame = self
            .send_frame(cmd3(b"SET", key.as_bytes(), value.as_ref()))
            .await?;
        ok(frame)
    }

    /// Sets `key` to `value` with an expiry of `seconds`.
    pub async fn set_ex(
        &mut self,
        key: &str,
        value: impl AsRef<[u8]>,
        seconds: u64,
    ) -> Result<(), ClientError> {
        let secs = seconds.to_string();
        let frame = self
            .send_frame(cmd4(
                b"SETEX",
                key.as_bytes(),
                secs.as_bytes(),
                value.as_ref(),
            ))
            .await?;
        ok(frame)
    }

    /// Deletes one or more keys. Returns the number of keys that were removed.
    pub async fn del(&mut self, keys: &[&str]) -> Result<i64, ClientError> {
        let mut parts = Vec::with_capacity(1 + keys.len());
        parts.push(Frame::Bulk(Bytes::from_static(b"DEL")));
        for k in keys {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        integer(frame)
    }

    /// Returns the number of supplied keys that exist.
    pub async fn exists(&mut self, keys: &[&str]) -> Result<i64, ClientError> {
        let mut parts = Vec::with_capacity(1 + keys.len());
        parts.push(Frame::Bulk(Bytes::from_static(b"EXISTS")));
        for k in keys {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        integer(frame)
    }

    /// Sets a timeout of `seconds` on `key`. Returns `true` if the timeout
    /// was set, `false` if the key does not exist.
    pub async fn expire(&mut self, key: &str, seconds: u64) -> Result<bool, ClientError> {
        let secs = seconds.to_string();
        let frame = self
            .send_frame(cmd3(b"EXPIRE", key.as_bytes(), secs.as_bytes()))
            .await?;
        bool_flag(frame)
    }

    /// Removes any existing timeout on `key`. Returns `true` if the timeout
    /// was removed, `false` if the key has no timeout or does not exist.
    pub async fn persist(&mut self, key: &str) -> Result<bool, ClientError> {
        let frame = self.send_frame(cmd2(b"PERSIST", key.as_bytes())).await?;
        bool_flag(frame)
    }

    /// Returns the remaining TTL in seconds. Returns `-2` if the key does not
    /// exist, `-1` if it has no expiry.
    pub async fn ttl(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd2(b"TTL", key.as_bytes())).await?;
        integer(frame)
    }

    /// Returns the remaining TTL in milliseconds.
    pub async fn pttl(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd2(b"PTTL", key.as_bytes())).await?;
        integer(frame)
    }

    /// Increments the integer stored at `key` by 1. Returns the new value.
    pub async fn incr(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd2(b"INCR", key.as_bytes())).await?;
        integer(frame)
    }

    /// Decrements the integer stored at `key` by 1. Returns the new value.
    pub async fn decr(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd2(b"DECR", key.as_bytes())).await?;
        integer(frame)
    }

    /// Increments the integer stored at `key` by `delta`. Returns the new value.
    pub async fn incrby(&mut self, key: &str, delta: i64) -> Result<i64, ClientError> {
        let d = delta.to_string();
        let frame = self
            .send_frame(cmd3(b"INCRBY", key.as_bytes(), d.as_bytes()))
            .await?;
        integer(frame)
    }

    /// Decrements the integer stored at `key` by `delta`. Returns the new value.
    pub async fn decrby(&mut self, key: &str, delta: i64) -> Result<i64, ClientError> {
        let d = delta.to_string();
        let frame = self
            .send_frame(cmd3(b"DECRBY", key.as_bytes(), d.as_bytes()))
            .await?;
        integer(frame)
    }

    /// Appends `value` to the string at `key`. Returns the new length.
    pub async fn append(&mut self, key: &str, value: impl AsRef<[u8]>) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(cmd3(b"APPEND", key.as_bytes(), value.as_ref()))
            .await?;
        integer(frame)
    }

    /// Returns the values for multiple keys. Missing keys are `None`.
    pub async fn mget(&mut self, keys: &[&str]) -> Result<Vec<Option<Bytes>>, ClientError> {
        let mut parts = Vec::with_capacity(1 + keys.len());
        parts.push(Frame::Bulk(Bytes::from_static(b"MGET")));
        for k in keys {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        optional_bytes_vec(frame)
    }

    /// Sets multiple key-value pairs atomically.
    pub async fn mset<V: AsRef<[u8]>>(&mut self, pairs: &[(&str, V)]) -> Result<(), ClientError> {
        let mut parts = Vec::with_capacity(1 + pairs.len() * 2);
        parts.push(Frame::Bulk(Bytes::from_static(b"MSET")));
        for (k, v) in pairs {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(v.as_ref())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        ok(frame)
    }

    /// Returns the value of `key` and deletes it atomically. Returns `None`
    /// if the key does not exist.
    pub async fn getdel(&mut self, key: &str) -> Result<Option<Bytes>, ClientError> {
        let frame = self.send_frame(cmd2(b"GETDEL", key.as_bytes())).await?;
        optional_bytes(frame)
    }

    // --- list commands ---

    /// Prepends `values` to the list at `key`. Returns the new list length.
    pub async fn lpush<V: AsRef<[u8]>>(
        &mut self,
        key: &str,
        values: &[V],
    ) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(cmd_key_values(b"LPUSH", key, values))
            .await?;
        integer(frame)
    }

    /// Appends `values` to the list at `key`. Returns the new list length.
    pub async fn rpush<V: AsRef<[u8]>>(
        &mut self,
        key: &str,
        values: &[V],
    ) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(cmd_key_values(b"RPUSH", key, values))
            .await?;
        integer(frame)
    }

    /// Removes and returns the first element of the list at `key`.
    pub async fn lpop(&mut self, key: &str) -> Result<Option<Bytes>, ClientError> {
        let frame = self.send_frame(cmd2(b"LPOP", key.as_bytes())).await?;
        optional_bytes(frame)
    }

    /// Removes and returns the last element of the list at `key`.
    pub async fn rpop(&mut self, key: &str) -> Result<Option<Bytes>, ClientError> {
        let frame = self.send_frame(cmd2(b"RPOP", key.as_bytes())).await?;
        optional_bytes(frame)
    }

    /// Returns the elements of the list between `start` and `stop` (inclusive).
    pub async fn lrange(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Bytes>, ClientError> {
        let s = start.to_string();
        let e = stop.to_string();
        let frame = self
            .send_frame(cmd4(b"LRANGE", key.as_bytes(), s.as_bytes(), e.as_bytes()))
            .await?;
        bytes_vec(frame)
    }

    /// Returns the length of the list at `key`. Returns 0 for missing keys.
    pub async fn llen(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd2(b"LLEN", key.as_bytes())).await?;
        integer(frame)
    }

    // --- hash commands ---

    /// Sets `field`/`value` pairs on the hash at `key`. Returns the number of
    /// new fields added.
    pub async fn hset<V: AsRef<[u8]>>(
        &mut self,
        key: &str,
        pairs: &[(&str, V)],
    ) -> Result<i64, ClientError> {
        let mut parts = Vec::with_capacity(2 + pairs.len() * 2);
        parts.push(Frame::Bulk(Bytes::from_static(b"HSET")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
        for (field, val) in pairs {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(field.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(val.as_ref())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        integer(frame)
    }

    /// Returns the value at `field` in the hash at `key`, or `None` if either
    /// does not exist.
    pub async fn hget(&mut self, key: &str, field: &str) -> Result<Option<Bytes>, ClientError> {
        let frame = self
            .send_frame(cmd3(b"HGET", key.as_bytes(), field.as_bytes()))
            .await?;
        optional_bytes(frame)
    }

    /// Returns all field-value pairs in the hash at `key`.
    pub async fn hgetall(&mut self, key: &str) -> Result<Vec<(Bytes, Bytes)>, ClientError> {
        let frame = self.send_frame(cmd2(b"HGETALL", key.as_bytes())).await?;
        pairs(frame)
    }

    /// Deletes `fields` from the hash at `key`. Returns the number removed.
    pub async fn hdel(&mut self, key: &str, fields: &[&str]) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(cmd_key_and_keys(b"HDEL", key, fields))
            .await?;
        integer(frame)
    }

    /// Returns `true` if `field` exists in the hash at `key`.
    pub async fn hexists(&mut self, key: &str, field: &str) -> Result<bool, ClientError> {
        let frame = self
            .send_frame(cmd3(b"HEXISTS", key.as_bytes(), field.as_bytes()))
            .await?;
        bool_flag(frame)
    }

    /// Returns the number of fields in the hash at `key`.
    pub async fn hlen(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd2(b"HLEN", key.as_bytes())).await?;
        integer(frame)
    }

    /// Increments the integer stored at `field` in the hash at `key` by
    /// `delta`. Returns the new value.
    pub async fn hincrby(
        &mut self,
        key: &str,
        field: &str,
        delta: i64,
    ) -> Result<i64, ClientError> {
        let d = delta.to_string();
        let frame = self
            .send_frame(cmd4(
                b"HINCRBY",
                key.as_bytes(),
                field.as_bytes(),
                d.as_bytes(),
            ))
            .await?;
        integer(frame)
    }

    /// Returns all field names in the hash at `key`.
    pub async fn hkeys(&mut self, key: &str) -> Result<Vec<Bytes>, ClientError> {
        let frame = self.send_frame(cmd2(b"HKEYS", key.as_bytes())).await?;
        bytes_vec(frame)
    }

    /// Returns all values in the hash at `key`.
    pub async fn hvals(&mut self, key: &str) -> Result<Vec<Bytes>, ClientError> {
        let frame = self.send_frame(cmd2(b"HVALS", key.as_bytes())).await?;
        bytes_vec(frame)
    }

    // --- set commands ---

    /// Adds `members` to the set at `key`. Returns the number added.
    pub async fn sadd(&mut self, key: &str, members: &[&str]) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(cmd_key_and_keys(b"SADD", key, members))
            .await?;
        integer(frame)
    }

    /// Removes `members` from the set at `key`. Returns the number removed.
    pub async fn srem(&mut self, key: &str, members: &[&str]) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(cmd_key_and_keys(b"SREM", key, members))
            .await?;
        integer(frame)
    }

    /// Returns all members of the set at `key`.
    pub async fn smembers(&mut self, key: &str) -> Result<Vec<Bytes>, ClientError> {
        let frame = self.send_frame(cmd2(b"SMEMBERS", key.as_bytes())).await?;
        bytes_vec(frame)
    }

    /// Returns `true` if `member` belongs to the set at `key`.
    pub async fn sismember(&mut self, key: &str, member: &str) -> Result<bool, ClientError> {
        let frame = self
            .send_frame(cmd3(b"SISMEMBER", key.as_bytes(), member.as_bytes()))
            .await?;
        bool_flag(frame)
    }

    /// Returns the cardinality (number of members) of the set at `key`.
    pub async fn scard(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd2(b"SCARD", key.as_bytes())).await?;
        integer(frame)
    }

    // --- sorted set commands ---

    /// Adds `members` to the sorted set at `key`. Each member is a
    /// `(score, name)` pair. Returns the number of new members added.
    pub async fn zadd(&mut self, key: &str, members: &[(f64, &str)]) -> Result<i64, ClientError> {
        let mut parts = Vec::with_capacity(2 + members.len() * 2);
        parts.push(Frame::Bulk(Bytes::from_static(b"ZADD")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
        for (score, member) in members {
            let s = score.to_string();
            parts.push(Frame::Bulk(Bytes::copy_from_slice(s.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(member.as_bytes())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        integer(frame)
    }

    /// Returns members of the sorted set between rank `start` and `stop`
    /// (0-based, inclusive).
    pub async fn zrange(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<Bytes>, ClientError> {
        let s = start.to_string();
        let e = stop.to_string();
        let frame = self
            .send_frame(cmd4(b"ZRANGE", key.as_bytes(), s.as_bytes(), e.as_bytes()))
            .await?;
        bytes_vec(frame)
    }

    /// Like `zrange`, but also returns scores as `(member, score)` pairs.
    pub async fn zrange_withscores(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<(Bytes, f64)>, ClientError> {
        let s = start.to_string();
        let e = stop.to_string();
        let frame = self
            .send_frame(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"ZRANGE")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                Frame::Bulk(Bytes::copy_from_slice(s.as_bytes())),
                Frame::Bulk(Bytes::copy_from_slice(e.as_bytes())),
                Frame::Bulk(Bytes::from_static(b"WITHSCORES")),
            ]))
            .await?;
        scored_members(frame)
    }

    /// Returns the score of `member` in the sorted set, or `None` if absent.
    pub async fn zscore(&mut self, key: &str, member: &str) -> Result<Option<f64>, ClientError> {
        let frame = self
            .send_frame(cmd3(b"ZSCORE", key.as_bytes(), member.as_bytes()))
            .await?;
        optional_score(frame)
    }

    /// Returns the rank of `member` in the sorted set (0-based, ascending
    /// score order), or `None` if absent.
    pub async fn zrank(&mut self, key: &str, member: &str) -> Result<Option<i64>, ClientError> {
        let frame = self
            .send_frame(cmd3(b"ZRANK", key.as_bytes(), member.as_bytes()))
            .await?;
        optional_integer(frame)
    }

    /// Removes `members` from the sorted set at `key`. Returns the number
    /// removed.
    pub async fn zrem(&mut self, key: &str, members: &[&str]) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(cmd_key_and_keys(b"ZREM", key, members))
            .await?;
        integer(frame)
    }

    /// Returns the number of members in the sorted set at `key`.
    pub async fn zcard(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd2(b"ZCARD", key.as_bytes())).await?;
        integer(frame)
    }

    // --- server commands ---

    /// Sends a `PING` and expects a `PONG` response.
    pub async fn ping(&mut self) -> Result<(), ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))]))
            .await?;
        match frame {
            Frame::Simple(s) if s == "PONG" => Ok(()),
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "expected PONG, got {other:?}"
            ))),
        }
    }

    /// Returns the number of keys in the current database.
    pub async fn dbsize(&mut self) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![Frame::Bulk(Bytes::from_static(
                b"DBSIZE",
            ))]))
            .await?;
        integer(frame)
    }

    /// Removes all keys from the current database.
    pub async fn flushdb(&mut self) -> Result<(), ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![Frame::Bulk(Bytes::from_static(
                b"FLUSHDB",
            ))]))
            .await?;
        ok(frame)
    }

    // --- more string commands ---

    /// Returns the length of the string at `key`. Returns 0 for missing keys.
    pub async fn strlen(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd2(b"STRLEN", key.as_bytes())).await?;
        integer(frame)
    }

    /// Increments the float stored at `key` by `delta`. Returns the new value.
    pub async fn incr_by_float(&mut self, key: &str, delta: f64) -> Result<f64, ClientError> {
        let d = delta.to_string();
        let frame = self
            .send_frame(cmd3(b"INCRBYFLOAT", key.as_bytes(), d.as_bytes()))
            .await?;
        float_value(frame)
    }

    // --- key commands ---

    /// Returns the type of the value stored at `key` as a string:
    /// `"string"`, `"list"`, `"set"`, `"zset"`, `"hash"`, or `"none"`.
    pub async fn key_type(&mut self, key: &str) -> Result<String, ClientError> {
        let frame = self.send_frame(cmd2(b"TYPE", key.as_bytes())).await?;
        string_value(frame)
    }

    /// Returns all keys matching `pattern`.
    ///
    /// Use `"*"` to return every key. This is a blocking O(N) scan — prefer
    /// [`Client::scan`] in production.
    pub async fn keys(&mut self, pattern: &str) -> Result<Vec<Bytes>, ClientError> {
        let frame = self.send_frame(cmd2(b"KEYS", pattern.as_bytes())).await?;
        bytes_vec(frame)
    }

    /// Renames `key` to `newkey`. Returns an error if `key` does not exist.
    pub async fn rename(&mut self, key: &str, newkey: &str) -> Result<(), ClientError> {
        let frame = self
            .send_frame(cmd3(b"RENAME", key.as_bytes(), newkey.as_bytes()))
            .await?;
        ok(frame)
    }

    /// Incrementally iterates keys in the keyspace.
    ///
    /// Pass `cursor: 0` to start a new iteration. Continue calling with the
    /// returned cursor until the cursor is `0` again. An optional `pattern`
    /// filters by glob and `count` hints at the page size (server may return
    /// more or fewer).
    pub async fn scan(
        &mut self,
        cursor: u64,
        count: Option<u32>,
        pattern: Option<&str>,
    ) -> Result<ScanPage, ClientError> {
        let cur = cursor.to_string();
        let mut parts = Vec::with_capacity(6);
        parts.push(Frame::Bulk(Bytes::from_static(b"SCAN")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(cur.as_bytes())));
        if let Some(pat) = pattern {
            parts.push(Frame::Bulk(Bytes::from_static(b"MATCH")));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(pat.as_bytes())));
        }
        if let Some(n) = count {
            let ns = n.to_string();
            parts.push(Frame::Bulk(Bytes::from_static(b"COUNT")));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(ns.as_bytes())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        scan_page(frame)
    }

    /// Sets a timeout of `millis` milliseconds on `key`. Returns `true` if
    /// the timeout was set, `false` if the key does not exist.
    pub async fn pexpire(&mut self, key: &str, millis: u64) -> Result<bool, ClientError> {
        let ms = millis.to_string();
        let frame = self
            .send_frame(cmd3(b"PEXPIRE", key.as_bytes(), ms.as_bytes()))
            .await?;
        bool_flag(frame)
    }

    // --- more hash commands ---

    /// Returns values for multiple `fields` in the hash at `key`. Missing
    /// fields are `None`.
    pub async fn hmget(
        &mut self,
        key: &str,
        fields: &[&str],
    ) -> Result<Vec<Option<Bytes>>, ClientError> {
        let frame = self
            .send_frame(cmd_key_and_keys(b"HMGET", key, fields))
            .await?;
        optional_bytes_vec(frame)
    }

    // --- more server commands ---

    /// Echoes `message` back from the server. Useful for round-trip testing.
    pub async fn echo(&mut self, message: &str) -> Result<Bytes, ClientError> {
        let frame = self.send_frame(cmd2(b"ECHO", message.as_bytes())).await?;
        match frame {
            Frame::Bulk(b) => Ok(b),
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "expected bulk for ECHO, got {other:?}"
            ))),
        }
    }

    /// Deletes keys asynchronously. Behaves like `del` but frees memory in
    /// the background for large values. Returns the number of keys removed.
    pub async fn unlink(&mut self, keys: &[&str]) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd_keys_only(b"UNLINK", keys)).await?;
        integer(frame)
    }

    /// Returns server information. Pass `Some("keyspace")` for a specific
    /// section, or `None` for all sections.
    pub async fn info(&mut self, section: Option<&str>) -> Result<String, ClientError> {
        let frame = match section {
            Some(s) => self.send_frame(cmd2(b"INFO", s.as_bytes())).await?,
            None => {
                self.send_frame(Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"INFO"))]))
                    .await?
            }
        };
        match frame {
            Frame::Bulk(b) => String::from_utf8(b.to_vec())
                .map_err(|_| ClientError::Protocol("INFO response is not valid UTF-8".into())),
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "expected bulk for INFO, got {other:?}"
            ))),
        }
    }

    /// Triggers a background snapshot (`BGSAVE`). Returns the server status
    /// message.
    pub async fn bgsave(&mut self) -> Result<String, ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![Frame::Bulk(Bytes::from_static(
                b"BGSAVE",
            ))]))
            .await?;
        string_value(frame)
    }

    /// Triggers an AOF rewrite in the background (`BGREWRITEAOF`). Returns
    /// the server status message.
    pub async fn bgrewriteaof(&mut self) -> Result<String, ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![Frame::Bulk(Bytes::from_static(
                b"BGREWRITEAOF",
            ))]))
            .await?;
        string_value(frame)
    }

    // --- slowlog commands ---

    /// Returns up to `count` recent slow-log entries, or all entries if
    /// `count` is `None`.
    pub async fn slowlog_get(
        &mut self,
        count: Option<u32>,
    ) -> Result<Vec<SlowlogEntry>, ClientError> {
        let frame = match count {
            Some(n) => {
                let ns = n.to_string();
                self.send_frame(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"SLOWLOG")),
                    Frame::Bulk(Bytes::from_static(b"GET")),
                    Frame::Bulk(Bytes::copy_from_slice(ns.as_bytes())),
                ]))
                .await?
            }
            None => {
                self.send_frame(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"SLOWLOG")),
                    Frame::Bulk(Bytes::from_static(b"GET")),
                ]))
                .await?
            }
        };
        slowlog_entries(frame)
    }

    /// Returns the number of entries currently in the slow log.
    pub async fn slowlog_len(&mut self) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"SLOWLOG")),
                Frame::Bulk(Bytes::from_static(b"LEN")),
            ]))
            .await?;
        integer(frame)
    }

    /// Clears all entries from the slow log.
    pub async fn slowlog_reset(&mut self) -> Result<(), ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"SLOWLOG")),
                Frame::Bulk(Bytes::from_static(b"RESET")),
            ]))
            .await?;
        ok(frame)
    }

    // --- pub/sub commands (request-response only) ---

    /// Publishes `message` to `channel`. Returns the number of subscribers
    /// that received the message.
    pub async fn publish(
        &mut self,
        channel: &str,
        message: impl AsRef<[u8]>,
    ) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(cmd3(b"PUBLISH", channel.as_bytes(), message.as_ref()))
            .await?;
        integer(frame)
    }

    /// Returns the names of active pub/sub channels. Pass `Some(pattern)` to
    /// filter by glob, or `None` for all channels.
    pub async fn pubsub_channels(
        &mut self,
        pattern: Option<&str>,
    ) -> Result<Vec<Bytes>, ClientError> {
        let frame = match pattern {
            Some(p) => {
                self.send_frame(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"PUBSUB")),
                    Frame::Bulk(Bytes::from_static(b"CHANNELS")),
                    Frame::Bulk(Bytes::copy_from_slice(p.as_bytes())),
                ]))
                .await?
            }
            None => {
                self.send_frame(Frame::Array(vec![
                    Frame::Bulk(Bytes::from_static(b"PUBSUB")),
                    Frame::Bulk(Bytes::from_static(b"CHANNELS")),
                ]))
                .await?
            }
        };
        bytes_vec(frame)
    }

    /// Returns the subscriber counts for the given channels as
    /// `(channel, count)` pairs.
    pub async fn pubsub_numsub(
        &mut self,
        channels: &[&str],
    ) -> Result<Vec<(Bytes, i64)>, ClientError> {
        let mut parts = Vec::with_capacity(2 + channels.len());
        parts.push(Frame::Bulk(Bytes::from_static(b"PUBSUB")));
        parts.push(Frame::Bulk(Bytes::from_static(b"NUMSUB")));
        for ch in channels {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(ch.as_bytes())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        numsub_pairs(frame)
    }

    /// Returns the number of active pattern subscriptions across all clients.
    pub async fn pubsub_numpat(&mut self) -> Result<i64, ClientError> {
        let frame = self
            .send_frame(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"PUBSUB")),
                Frame::Bulk(Bytes::from_static(b"NUMPAT")),
            ]))
            .await?;
        integer(frame)
    }

    // --- pub/sub subscriber mode ---

    /// Puts the connection into subscriber mode on `channels`.
    ///
    /// The connection is consumed and a [`Subscriber`] is returned. Use a
    /// separate [`Client`] for regular commands while subscribed.
    pub async fn subscribe(mut self, channels: &[&str]) -> Result<Subscriber, ClientError> {
        let mut parts = Vec::with_capacity(1 + channels.len());
        parts.push(Frame::Bulk(Bytes::from_static(b"SUBSCRIBE")));
        for ch in channels {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(ch.as_bytes())));
        }
        self.write_frame(Frame::Array(parts)).await?;
        // drain confirmation frames (one per channel)
        for _ in 0..channels.len() {
            self.read_response().await?;
        }
        Ok(Subscriber::new(self))
    }

    /// Same as [`subscribe`](Client::subscribe) but subscribes to glob
    /// patterns with `PSUBSCRIBE`.
    pub async fn psubscribe(mut self, patterns: &[&str]) -> Result<Subscriber, ClientError> {
        let mut parts = Vec::with_capacity(1 + patterns.len());
        parts.push(Frame::Bulk(Bytes::from_static(b"PSUBSCRIBE")));
        for p in patterns {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(p.as_bytes())));
        }
        self.write_frame(Frame::Array(parts)).await?;
        // drain confirmation frames (one per pattern)
        for _ in 0..patterns.len() {
            self.read_response().await?;
        }
        Ok(Subscriber::new(self))
    }

    // --- expiry commands ---

    /// Returns the absolute unix expiry timestamp in seconds, or -1 if no expiry, -2 if key missing.
    pub async fn expiretime(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd2(b"EXPIRETIME", key.as_bytes())).await?;
        integer(frame)
    }

    /// Returns the absolute unix expiry timestamp in milliseconds, or -1 if no expiry, -2 if key missing.
    pub async fn pexpiretime(&mut self, key: &str) -> Result<i64, ClientError> {
        let frame = self.send_frame(cmd2(b"PEXPIRETIME", key.as_bytes())).await?;
        integer(frame)
    }

    /// Sets expiry at an absolute unix timestamp (seconds). Returns `true` if the timeout was set.
    pub async fn expireat(&mut self, key: &str, timestamp: u64) -> Result<bool, ClientError> {
        let ts = timestamp.to_string();
        let frame = self
            .send_frame(cmd3(b"EXPIREAT", key.as_bytes(), ts.as_bytes()))
            .await?;
        bool_flag(frame)
    }

    /// Sets expiry at an absolute unix timestamp (milliseconds). Returns `true` if the timeout was set.
    pub async fn pexpireat(&mut self, key: &str, timestamp_ms: u64) -> Result<bool, ClientError> {
        let ts = timestamp_ms.to_string();
        let frame = self
            .send_frame(cmd3(b"PEXPIREAT", key.as_bytes(), ts.as_bytes()))
            .await?;
        bool_flag(frame)
    }

    // --- string commands (extended) ---

    /// Sets `key` to `value`, returning the old value. Returns `None` if the key didn't exist.
    pub async fn getset(
        &mut self,
        key: &str,
        value: impl AsRef<[u8]>,
    ) -> Result<Option<Bytes>, ClientError> {
        let frame = self
            .send_frame(cmd3(b"GETSET", key.as_bytes(), value.as_ref()))
            .await?;
        optional_bytes(frame)
    }

    /// Sets multiple key-value pairs only if none of the keys already exist.
    /// Returns `true` if all keys were set, `false` if any key existed.
    pub async fn msetnx<V: AsRef<[u8]>>(
        &mut self,
        pairs: &[(&str, V)],
    ) -> Result<bool, ClientError> {
        let mut parts = Vec::with_capacity(1 + pairs.len() * 2);
        parts.push(Frame::Bulk(Bytes::from_static(b"MSETNX")));
        for (k, v) in pairs {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(v.as_ref())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        bool_flag(frame)
    }

    // --- bitmap commands ---

    /// Returns the bit value at `offset` in the string stored at `key`.
    pub async fn getbit(&mut self, key: &str, offset: u64) -> Result<i64, ClientError> {
        let off = offset.to_string();
        let frame = self
            .send_frame(cmd3(b"GETBIT", key.as_bytes(), off.as_bytes()))
            .await?;
        integer(frame)
    }

    /// Sets or clears the bit at `offset`. Returns the original bit value.
    pub async fn setbit(&mut self, key: &str, offset: u64, value: u8) -> Result<i64, ClientError> {
        let off = offset.to_string();
        let val = value.to_string();
        let frame = self
            .send_frame(cmd4(b"SETBIT", key.as_bytes(), off.as_bytes(), val.as_bytes()))
            .await?;
        integer(frame)
    }

    /// Counts set bits in the string at `key`.
    ///
    /// `range`: optional `(start, end, unit)` where unit is `"BYTE"` or `"BIT"`.
    pub async fn bitcount(
        &mut self,
        key: &str,
        range: Option<(i64, i64, &str)>,
    ) -> Result<i64, ClientError> {
        let frame = if let Some((start, end, unit)) = range {
            let s = start.to_string();
            let e = end.to_string();
            let mut parts = Vec::with_capacity(5);
            parts.push(Frame::Bulk(Bytes::from_static(b"BITCOUNT")));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(s.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(e.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(unit.as_bytes())));
            self.send_frame(Frame::Array(parts)).await?
        } else {
            self.send_frame(cmd2(b"BITCOUNT", key.as_bytes())).await?
        };
        integer(frame)
    }

    /// Finds the first set (`bit=1`) or clear (`bit=0`) bit in the string at `key`.
    ///
    /// `range`: optional `(start, end, unit)` where unit is `"BYTE"` or `"BIT"`.
    pub async fn bitpos(
        &mut self,
        key: &str,
        bit: u8,
        range: Option<(i64, i64, &str)>,
    ) -> Result<i64, ClientError> {
        let b = bit.to_string();
        let frame = if let Some((start, end, unit)) = range {
            let s = start.to_string();
            let e = end.to_string();
            let mut parts = Vec::with_capacity(6);
            parts.push(Frame::Bulk(Bytes::from_static(b"BITPOS")));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(b.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(s.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(e.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(unit.as_bytes())));
            self.send_frame(Frame::Array(parts)).await?
        } else {
            self.send_frame(cmd3(b"BITPOS", key.as_bytes(), b.as_bytes()))
                .await?
        };
        integer(frame)
    }

    /// Performs a bitwise operation between strings.
    ///
    /// `op` is `"AND"`, `"OR"`, `"XOR"`, or `"NOT"`. Result is stored at `dest`.
    /// Returns the length of the resulting string.
    pub async fn bitop(
        &mut self,
        op: &str,
        dest: &str,
        keys: &[&str],
    ) -> Result<i64, ClientError> {
        let mut parts = Vec::with_capacity(3 + keys.len());
        parts.push(Frame::Bulk(Bytes::from_static(b"BITOP")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(op.as_bytes())));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(dest.as_bytes())));
        for k in keys {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        integer(frame)
    }

    // --- set commands (extended) ---

    /// Atomically moves `member` from `src` to `dst`. Returns `true` if the move succeeded.
    pub async fn smove(
        &mut self,
        src: &str,
        dst: &str,
        member: &str,
    ) -> Result<bool, ClientError> {
        let frame = self
            .send_frame(cmd4(b"SMOVE", src.as_bytes(), dst.as_bytes(), member.as_bytes()))
            .await?;
        bool_flag(frame)
    }

    /// Returns the cardinality of the intersection of multiple sets. `limit=0` means no limit.
    pub async fn sintercard(&mut self, keys: &[&str], limit: usize) -> Result<i64, ClientError> {
        let n = keys.len().to_string();
        let lim = limit.to_string();
        let mut parts = Vec::with_capacity(3 + keys.len() + 2);
        parts.push(Frame::Bulk(Bytes::from_static(b"SINTERCARD")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(n.as_bytes())));
        for k in keys {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())));
        }
        if limit > 0 {
            parts.push(Frame::Bulk(Bytes::from_static(b"LIMIT")));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(lim.as_bytes())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        integer(frame)
    }

    // --- list commands (extended) ---

    /// Pops up to `count` elements from the first non-empty list in `keys`.
    ///
    /// `left=true` pops from the head, `left=false` from the tail.
    /// Returns `None` if all lists are empty, or `Some((key, elements))`.
    pub async fn lmpop(
        &mut self,
        keys: &[&str],
        left: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<Bytes>)>, ClientError> {
        let n = keys.len().to_string();
        let dir = if left { b"LEFT" as &[u8] } else { b"RIGHT" };
        let cnt = count.to_string();
        let mut parts = Vec::with_capacity(4 + keys.len() + 2);
        parts.push(Frame::Bulk(Bytes::from_static(b"LMPOP")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(n.as_bytes())));
        for k in keys {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())));
        }
        parts.push(Frame::Bulk(Bytes::copy_from_slice(dir)));
        if count > 0 {
            parts.push(Frame::Bulk(Bytes::from_static(b"COUNT")));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(cnt.as_bytes())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        match frame {
            Frame::Null => Ok(None),
            Frame::Array(mut elems) if elems.len() == 2 => {
                let key = match elems.remove(0) {
                    Frame::Bulk(b) => String::from_utf8(b.to_vec())
                        .map_err(|_| ClientError::Protocol("key is not valid UTF-8".into()))?,
                    other => {
                        return Err(ClientError::Protocol(format!(
                            "expected bulk key in LMPOP response, got {other:?}"
                        )))
                    }
                };
                let elements = bytes_vec(elems.remove(0))?;
                Ok(Some((key, elements)))
            }
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "unexpected LMPOP response: {other:?}"
            ))),
        }
    }

    // --- hash commands (extended) ---

    /// Returns random field(s) from the hash at `key`.
    ///
    /// `count=None` returns a single field name as a one-element Vec.
    /// Positive count returns that many distinct fields; negative allows repeats.
    pub async fn hrandfield(
        &mut self,
        key: &str,
        count: Option<i64>,
    ) -> Result<Vec<Bytes>, ClientError> {
        let frame = if let Some(n) = count {
            let s = n.to_string();
            self.send_frame(cmd3(b"HRANDFIELD", key.as_bytes(), s.as_bytes()))
                .await?
        } else {
            self.send_frame(cmd2(b"HRANDFIELD", key.as_bytes())).await?
        };
        match frame {
            Frame::Bulk(b) => Ok(vec![b]),
            Frame::Null => Ok(vec![]),
            Frame::Array(_) => bytes_vec(frame),
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "unexpected HRANDFIELD response: {other:?}"
            ))),
        }
    }

    /// Returns random field-value pairs from the hash at `key`.
    ///
    /// `count` controls how many pairs to return (positive = distinct, negative = allow repeats).
    pub async fn hrandfield_withvalues(
        &mut self,
        key: &str,
        count: i64,
    ) -> Result<Vec<(Bytes, Bytes)>, ClientError> {
        let s = count.to_string();
        let frame = self
            .send_frame(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"HRANDFIELD")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                Frame::Bulk(Bytes::copy_from_slice(s.as_bytes())),
                Frame::Bulk(Bytes::from_static(b"WITHVALUES")),
            ]))
            .await?;
        pairs(frame)
    }

    // --- sorted set commands (extended) ---

    /// Pops up to `count` elements from the first non-empty sorted set in `keys`.
    ///
    /// `min=true` pops minimum-score members, `min=false` pops maximum-score members.
    /// Returns `None` if all sorted sets are empty, or `Some((key, members))`.
    pub async fn zmpop(
        &mut self,
        keys: &[&str],
        min: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<(Bytes, f64)>)>, ClientError> {
        let n = keys.len().to_string();
        let dir = if min { b"MIN" as &[u8] } else { b"MAX" };
        let cnt = count.to_string();
        let mut parts = Vec::with_capacity(4 + keys.len() + 2);
        parts.push(Frame::Bulk(Bytes::from_static(b"ZMPOP")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(n.as_bytes())));
        for k in keys {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())));
        }
        parts.push(Frame::Bulk(Bytes::copy_from_slice(dir)));
        if count > 0 {
            parts.push(Frame::Bulk(Bytes::from_static(b"COUNT")));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(cnt.as_bytes())));
        }
        let frame = self.send_frame(Frame::Array(parts)).await?;
        match frame {
            Frame::Null => Ok(None),
            Frame::Array(mut elems) if elems.len() == 2 => {
                let key = match elems.remove(0) {
                    Frame::Bulk(b) => String::from_utf8(b.to_vec())
                        .map_err(|_| ClientError::Protocol("key is not valid UTF-8".into()))?,
                    other => {
                        return Err(ClientError::Protocol(format!(
                            "expected bulk key in ZMPOP response, got {other:?}"
                        )))
                    }
                };
                let members = scored_members(elems.remove(0))?;
                Ok(Some((key, members)))
            }
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "unexpected ZMPOP response: {other:?}"
            ))),
        }
    }

    /// Returns random member(s) from the sorted set at `key`.
    ///
    /// `count=None` returns a single member name. Positive count returns distinct members;
    /// negative allows repeats.
    pub async fn zrandmember(
        &mut self,
        key: &str,
        count: Option<i64>,
    ) -> Result<Vec<Bytes>, ClientError> {
        let frame = if let Some(n) = count {
            let s = n.to_string();
            self.send_frame(cmd3(b"ZRANDMEMBER", key.as_bytes(), s.as_bytes()))
                .await?
        } else {
            self.send_frame(cmd2(b"ZRANDMEMBER", key.as_bytes())).await?
        };
        match frame {
            Frame::Bulk(b) => Ok(vec![b]),
            Frame::Null => Ok(vec![]),
            Frame::Array(_) => bytes_vec(frame),
            Frame::Error(e) => Err(ClientError::Server(e)),
            other => Err(ClientError::Protocol(format!(
                "unexpected ZRANDMEMBER response: {other:?}"
            ))),
        }
    }

    /// Returns random member-score pairs from the sorted set at `key`.
    ///
    /// `count` controls how many pairs to return (positive = distinct, negative = allow repeats).
    pub async fn zrandmember_withscores(
        &mut self,
        key: &str,
        count: i64,
    ) -> Result<Vec<(Bytes, f64)>, ClientError> {
        let s = count.to_string();
        let frame = self
            .send_frame(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"ZRANDMEMBER")),
                Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
                Frame::Bulk(Bytes::copy_from_slice(s.as_bytes())),
                Frame::Bulk(Bytes::from_static(b"WITHSCORES")),
            ]))
            .await?;
        scored_members(frame)
    }

    // --- pipeline ---

    /// Executes all commands queued in `pipeline` as a single batch.
    ///
    /// Returns one raw [`Frame`] per command in the same order they were
    /// queued. An empty pipeline returns an empty `Vec` without touching the
    /// network.
    ///
    /// Use this when you need full control over decoding the responses (e.g.
    /// mixed command types in one batch). For homogeneous batches the typed
    /// builder methods on [`Pipeline`] pair well with a manual decode loop.
    pub async fn execute_pipeline(
        &mut self,
        pipeline: Pipeline,
    ) -> Result<Vec<Frame>, ClientError> {
        if pipeline.is_empty() {
            return Ok(Vec::new());
        }
        self.send_batch(&pipeline.cmds).await
    }
}

// --- shared frame construction helpers used by multiple command groups ---

fn cmd_key_and_keys(cmd: &'static [u8], key: &str, rest: &[&str]) -> Frame {
    let mut parts = Vec::with_capacity(2 + rest.len());
    parts.push(Frame::Bulk(Bytes::from_static(cmd)));
    parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
    for s in rest {
        parts.push(Frame::Bulk(Bytes::copy_from_slice(s.as_bytes())));
    }
    Frame::Array(parts)
}

fn cmd_key_values<V: AsRef<[u8]>>(cmd: &'static [u8], key: &str, values: &[V]) -> Frame {
    let mut parts = Vec::with_capacity(2 + values.len());
    parts.push(Frame::Bulk(Bytes::from_static(cmd)));
    parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
    for v in values {
        parts.push(Frame::Bulk(Bytes::copy_from_slice(v.as_ref())));
    }
    Frame::Array(parts)
}

/// `CMD key1 key2 ...` (no separate leading key argument)
fn cmd_keys_only(cmd: &'static [u8], keys: &[&str]) -> Frame {
    let mut parts = Vec::with_capacity(1 + keys.len());
    parts.push(Frame::Bulk(Bytes::from_static(cmd)));
    for k in keys {
        parts.push(Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())));
    }
    Frame::Array(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(b: &[u8]) -> Frame {
        Frame::Bulk(Bytes::copy_from_slice(b))
    }

    // --- optional_bytes ---

    #[test]
    fn optional_bytes_bulk() {
        let f = Frame::Bulk(Bytes::from_static(b"hello"));
        assert_eq!(
            optional_bytes(f).unwrap(),
            Some(Bytes::from_static(b"hello"))
        );
    }

    #[test]
    fn optional_bytes_null() {
        assert_eq!(optional_bytes(Frame::Null).unwrap(), None);
    }

    #[test]
    fn optional_bytes_server_error() {
        let f = Frame::Error("WRONGTYPE".into());
        assert!(matches!(optional_bytes(f), Err(ClientError::Server(_))));
    }

    #[test]
    fn optional_bytes_unexpected() {
        let f = Frame::Integer(1);
        assert!(matches!(optional_bytes(f), Err(ClientError::Protocol(_))));
    }

    // --- bool_flag ---

    #[test]
    fn bool_flag_one() {
        assert_eq!(bool_flag(Frame::Integer(1)).unwrap(), true);
    }

    #[test]
    fn bool_flag_zero() {
        assert_eq!(bool_flag(Frame::Integer(0)).unwrap(), false);
    }

    #[test]
    fn bool_flag_unexpected_value() {
        let f = Frame::Integer(42);
        assert!(matches!(bool_flag(f), Err(ClientError::Protocol(_))));
    }

    // --- pairs ---

    #[test]
    fn pairs_even_array() {
        let f = Frame::Array(vec![
            bulk(b"field1"),
            bulk(b"val1"),
            bulk(b"field2"),
            bulk(b"val2"),
        ]);
        let result = pairs(f).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, Bytes::from_static(b"field1"));
        assert_eq!(result[0].1, Bytes::from_static(b"val1"));
    }

    #[test]
    fn pairs_odd_array_is_error() {
        let f = Frame::Array(vec![bulk(b"orphan")]);
        assert!(matches!(pairs(f), Err(ClientError::Protocol(_))));
    }

    #[test]
    fn pairs_empty_array() {
        assert_eq!(pairs(Frame::Array(vec![])).unwrap(), vec![]);
    }

    #[test]
    fn pairs_null() {
        assert_eq!(pairs(Frame::Null).unwrap(), vec![]);
    }

    // --- optional_score ---

    #[test]
    fn optional_score_valid_float() {
        let f = Frame::Bulk(Bytes::from_static(b"3.14"));
        let s = optional_score(f).unwrap();
        assert!((s.unwrap() - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn optional_score_null() {
        assert_eq!(optional_score(Frame::Null).unwrap(), None);
    }

    #[test]
    fn optional_score_malformed() {
        let f = Frame::Bulk(Bytes::from_static(b"notanumber"));
        assert!(matches!(optional_score(f), Err(ClientError::Protocol(_))));
    }

    // --- optional_bytes_vec (MGET) ---

    #[test]
    fn optional_bytes_vec_null_elements() {
        let f = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"value")),
            Frame::Null,
            Frame::Bulk(Bytes::from_static(b"another")),
        ]);
        let result = optional_bytes_vec(f).unwrap();
        assert_eq!(result.len(), 3);
        assert!(result[0].is_some());
        assert!(result[1].is_none());
        assert!(result[2].is_some());
    }
}
