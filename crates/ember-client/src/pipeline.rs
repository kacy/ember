//! Command pipeline builder.
//!
//! A [`Pipeline`] queues multiple commands and sends them in a single network
//! round-trip. This dramatically reduces latency when you need to issue many
//! independent commands in sequence.
//!
//! # Example
//!
//! ```no_run
//! use ember_client::{Client, Pipeline};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), ember_client::ClientError> {
//!     let mut client = Client::connect("127.0.0.1", 6379).await?;
//!
//!     let results = client.execute_pipeline(
//!         Pipeline::new()
//!             .set("greeting", "hello")
//!             .get("greeting")
//!             .incr("visits"),
//!     ).await?;
//!
//!     println!("{} responses", results.len());
//!     Ok(())
//! }
//! ```

use bytes::Bytes;
use ember_protocol::types::Frame;

/// A batch of commands to be sent in a single network round-trip.
///
/// Build the pipeline with the typed builder methods, then execute it
/// with [`Client::execute_pipeline`].
pub struct Pipeline {
    pub(crate) cmds: Vec<Frame>,
}

impl Pipeline {
    /// Creates an empty pipeline.
    pub fn new() -> Self {
        Self { cmds: Vec::new() }
    }

    /// Returns the number of queued commands.
    pub fn len(&self) -> usize {
        self.cmds.len()
    }

    /// Returns `true` if no commands have been queued.
    pub fn is_empty(&self) -> bool {
        self.cmds.is_empty()
    }

    /// Queues a raw command from string slices.
    ///
    /// Useful for commands not covered by the typed builders.
    pub fn send(self, args: &[&str]) -> Self {
        let parts = args
            .iter()
            .map(|s| Frame::Bulk(Bytes::copy_from_slice(s.as_bytes())))
            .collect();
        self.push(Frame::Array(parts))
    }

    // --- string commands ---

    /// Queues a `GET key` command.
    pub fn get(self, key: &str) -> Self {
        self.push(array2(b"GET", key.as_bytes()))
    }

    /// Queues a `SET key value` command.
    pub fn set(self, key: &str, value: impl AsRef<[u8]>) -> Self {
        self.push(array3(b"SET", key.as_bytes(), value.as_ref()))
    }

    /// Queues a `DEL key [key ...]` command.
    pub fn del(self, keys: &[&str]) -> Self {
        self.push(array_with_keys(b"DEL", keys))
    }

    /// Queues an `EXPIRE key seconds` command.
    pub fn expire(self, key: &str, seconds: u64) -> Self {
        let secs = seconds.to_string();
        self.push(array3(b"EXPIRE", key.as_bytes(), secs.as_bytes()))
    }

    /// Queues an `INCR key` command.
    pub fn incr(self, key: &str) -> Self {
        self.push(array2(b"INCR", key.as_bytes()))
    }

    /// Queues a `DECR key` command.
    pub fn decr(self, key: &str) -> Self {
        self.push(array2(b"DECR", key.as_bytes()))
    }

    /// Queues an `INCRBY key delta` command.
    pub fn incrby(self, key: &str, delta: i64) -> Self {
        let d = delta.to_string();
        self.push(array3(b"INCRBY", key.as_bytes(), d.as_bytes()))
    }

    /// Queues a `PING` command.
    pub fn ping(self) -> Self {
        self.push(Frame::Array(vec![Frame::Bulk(Bytes::from_static(b"PING"))]))
    }

    /// Queues an `EXISTS key [key ...]` command.
    pub fn exists(self, keys: &[&str]) -> Self {
        self.push(array_with_keys(b"EXISTS", keys))
    }

    /// Queues a `TTL key` command.
    pub fn ttl(self, key: &str) -> Self {
        self.push(array2(b"TTL", key.as_bytes()))
    }

    // --- list commands ---

    /// Queues an `LPUSH key value [value ...]` command.
    pub fn lpush<V: AsRef<[u8]>>(self, key: &str, values: &[V]) -> Self {
        self.push(array_with_cmd_key_values(b"LPUSH", key, values))
    }

    /// Queues an `RPUSH key value [value ...]` command.
    pub fn rpush<V: AsRef<[u8]>>(self, key: &str, values: &[V]) -> Self {
        self.push(array_with_cmd_key_values(b"RPUSH", key, values))
    }

    /// Queues an `LPOP key` command.
    pub fn lpop(self, key: &str) -> Self {
        self.push(array2(b"LPOP", key.as_bytes()))
    }

    /// Queues an `RPOP key` command.
    pub fn rpop(self, key: &str) -> Self {
        self.push(array2(b"RPOP", key.as_bytes()))
    }

    /// Queues an `LLEN key` command.
    pub fn llen(self, key: &str) -> Self {
        self.push(array2(b"LLEN", key.as_bytes()))
    }

    // --- hash commands ---

    /// Queues an `HGET key field` command.
    pub fn hget(self, key: &str, field: &str) -> Self {
        self.push(array3(b"HGET", key.as_bytes(), field.as_bytes()))
    }

    /// Queues an `HSET key field value [field value ...]` command.
    pub fn hset<V: AsRef<[u8]>>(self, key: &str, pairs: &[(&str, V)]) -> Self {
        let mut parts = Vec::with_capacity(2 + pairs.len() * 2);
        parts.push(Frame::Bulk(Bytes::from_static(b"HSET")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
        for (field, val) in pairs {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(field.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(val.as_ref())));
        }
        self.push(Frame::Array(parts))
    }

    /// Queues an `HDEL key field [field ...]` command.
    pub fn hdel(self, key: &str, fields: &[&str]) -> Self {
        self.push(array_with_key_and_keys(b"HDEL", key, fields))
    }

    /// Queues an `HINCRBY key field delta` command.
    pub fn hincrby(self, key: &str, field: &str, delta: i64) -> Self {
        let d = delta.to_string();
        self.push(Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"HINCRBY")),
            Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())),
            Frame::Bulk(Bytes::copy_from_slice(field.as_bytes())),
            Frame::Bulk(Bytes::copy_from_slice(d.as_bytes())),
        ]))
    }

    // --- set commands ---

    /// Queues an `SADD key member [member ...]` command.
    pub fn sadd(self, key: &str, members: &[&str]) -> Self {
        self.push(array_with_key_and_keys(b"SADD", key, members))
    }

    /// Queues an `SREM key member [member ...]` command.
    pub fn srem(self, key: &str, members: &[&str]) -> Self {
        self.push(array_with_key_and_keys(b"SREM", key, members))
    }

    /// Queues an `SCARD key` command.
    pub fn scard(self, key: &str) -> Self {
        self.push(array2(b"SCARD", key.as_bytes()))
    }

    // --- sorted set commands ---

    /// Queues a `ZADD key score member [score member ...]` command.
    pub fn zadd(self, key: &str, members: &[(f64, &str)]) -> Self {
        let mut parts = Vec::with_capacity(2 + members.len() * 2);
        parts.push(Frame::Bulk(Bytes::from_static(b"ZADD")));
        parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
        for (score, member) in members {
            let s = score.to_string();
            parts.push(Frame::Bulk(Bytes::copy_from_slice(s.as_bytes())));
            parts.push(Frame::Bulk(Bytes::copy_from_slice(member.as_bytes())));
        }
        self.push(Frame::Array(parts))
    }

    /// Queues a `ZCARD key` command.
    pub fn zcard(self, key: &str) -> Self {
        self.push(array2(b"ZCARD", key.as_bytes()))
    }

    /// Queues a `ZSCORE key member` command.
    pub fn zscore(self, key: &str, member: &str) -> Self {
        self.push(array3(b"ZSCORE", key.as_bytes(), member.as_bytes()))
    }

    // --- more string commands ---

    /// Queues a `STRLEN key` command.
    pub fn strlen(self, key: &str) -> Self {
        self.push(array2(b"STRLEN", key.as_bytes()))
    }

    /// Queues an `INCRBYFLOAT key delta` command.
    pub fn incr_by_float(self, key: &str, delta: f64) -> Self {
        let d = delta.to_string();
        self.push(array3(b"INCRBYFLOAT", key.as_bytes(), d.as_bytes()))
    }

    // --- key commands ---

    /// Queues a `TYPE key` command.
    pub fn key_type(self, key: &str) -> Self {
        self.push(array2(b"TYPE", key.as_bytes()))
    }

    /// Queues a `KEYS pattern` command.
    pub fn keys(self, pattern: &str) -> Self {
        self.push(array2(b"KEYS", pattern.as_bytes()))
    }

    /// Queues a `RENAME key newkey` command.
    pub fn rename(self, key: &str, newkey: &str) -> Self {
        self.push(array3(b"RENAME", key.as_bytes(), newkey.as_bytes()))
    }

    /// Queues a `PEXPIRE key millis` command.
    pub fn pexpire(self, key: &str, millis: u64) -> Self {
        let ms = millis.to_string();
        self.push(array3(b"PEXPIRE", key.as_bytes(), ms.as_bytes()))
    }

    /// Queues an `UNLINK key [key ...]` command.
    pub fn unlink(self, keys: &[&str]) -> Self {
        self.push(array_with_keys(b"UNLINK", keys))
    }

    // --- more hash commands ---

    /// Queues an `HMGET key field [field ...]` command.
    pub fn hmget(self, key: &str, fields: &[&str]) -> Self {
        self.push(array_with_key_and_keys(b"HMGET", key, fields))
    }

    // --- server commands ---

    /// Queues an `ECHO message` command.
    pub fn echo(self, message: &str) -> Self {
        self.push(array2(b"ECHO", message.as_bytes()))
    }

    /// Queues a `PUBLISH channel message` command.
    pub fn publish(self, channel: &str, message: impl AsRef<[u8]>) -> Self {
        self.push(array3(b"PUBLISH", channel.as_bytes(), message.as_ref()))
    }

    // --- internal ---

    fn push(mut self, frame: Frame) -> Self {
        self.cmds.push(frame);
        self
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

// --- frame construction helpers ---
// These exist so each builder method stays a one-liner.

fn array2(cmd: &'static [u8], a: &[u8]) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(cmd)),
        Frame::Bulk(Bytes::copy_from_slice(a)),
    ])
}

fn array3(cmd: &'static [u8], a: &[u8], b: &[u8]) -> Frame {
    Frame::Array(vec![
        Frame::Bulk(Bytes::from_static(cmd)),
        Frame::Bulk(Bytes::copy_from_slice(a)),
        Frame::Bulk(Bytes::copy_from_slice(b)),
    ])
}

/// `CMD key1 key2 ...`
fn array_with_keys(cmd: &'static [u8], keys: &[&str]) -> Frame {
    let mut parts = Vec::with_capacity(1 + keys.len());
    parts.push(Frame::Bulk(Bytes::from_static(cmd)));
    for k in keys {
        parts.push(Frame::Bulk(Bytes::copy_from_slice(k.as_bytes())));
    }
    Frame::Array(parts)
}

/// `CMD key field1 field2 ...`
fn array_with_key_and_keys(cmd: &'static [u8], key: &str, rest: &[&str]) -> Frame {
    let mut parts = Vec::with_capacity(2 + rest.len());
    parts.push(Frame::Bulk(Bytes::from_static(cmd)));
    parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
    for s in rest {
        parts.push(Frame::Bulk(Bytes::copy_from_slice(s.as_bytes())));
    }
    Frame::Array(parts)
}

/// `CMD key value1 value2 ...`
fn array_with_cmd_key_values<V: AsRef<[u8]>>(cmd: &'static [u8], key: &str, values: &[V]) -> Frame {
    let mut parts = Vec::with_capacity(2 + values.len());
    parts.push(Frame::Bulk(Bytes::from_static(cmd)));
    parts.push(Frame::Bulk(Bytes::copy_from_slice(key.as_bytes())));
    for v in values {
        parts.push(Frame::Bulk(Bytes::copy_from_slice(v.as_ref())));
    }
    Frame::Array(parts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(b: &[u8]) -> Frame {
        Frame::Bulk(Bytes::copy_from_slice(b))
    }

    #[test]
    fn get_produces_correct_frame() {
        let pipe = Pipeline::new().get("mykey");
        assert_eq!(pipe.len(), 1);
        assert_eq!(
            pipe.cmds[0],
            Frame::Array(vec![bulk(b"GET"), bulk(b"mykey")])
        );
    }

    #[test]
    fn del_multiple_keys() {
        let pipe = Pipeline::new().del(&["a", "b"]);
        assert_eq!(pipe.len(), 1);
        match &pipe.cmds[0] {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3); // DEL + 2 keys
                assert_eq!(parts[0], bulk(b"DEL"));
                assert_eq!(parts[1], bulk(b"a"));
                assert_eq!(parts[2], bulk(b"b"));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn chaining_accumulates_commands() {
        let pipe = Pipeline::new()
            .ping()
            .get("k1")
            .set("k2", "v2")
            .incr("counter");
        assert_eq!(pipe.len(), 4);
    }

    #[test]
    fn empty_pipeline() {
        let pipe = Pipeline::new();
        assert!(pipe.is_empty());
        assert_eq!(pipe.len(), 0);
    }

    #[test]
    fn hset_pairs_layout() {
        let pipe = Pipeline::new().hset("myhash", &[("field1", "val1"), ("field2", "val2")]);
        match &pipe.cmds[0] {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 6); // HSET + key + 2*(field+val)
                assert_eq!(parts[0], bulk(b"HSET"));
                assert_eq!(parts[1], bulk(b"myhash"));
                assert_eq!(parts[2], bulk(b"field1"));
                assert_eq!(parts[3], bulk(b"val1"));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn zadd_score_member_layout() {
        let pipe = Pipeline::new().zadd("leaderboard", &[(1.5, "alice"), (2.0, "bob")]);
        match &pipe.cmds[0] {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 6); // ZADD + key + 2*(score+member)
                assert_eq!(parts[0], bulk(b"ZADD"));
                assert_eq!(parts[2], bulk(b"1.5"));
                assert_eq!(parts[3], bulk(b"alice"));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }
}
