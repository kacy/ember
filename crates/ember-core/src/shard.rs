//! Shard: an independent partition of the keyspace.
//!
//! Each shard runs as its own tokio task, owning a `Keyspace` with no
//! internal locking. Commands arrive over an mpsc channel and responses
//! go back on a per-request oneshot. This is the shared-nothing core of
//! Ember's thread-per-core design.

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::error::ShardError;
use crate::keyspace::{Keyspace, TtlResult};
use crate::types::Value;

/// A protocol-agnostic command sent to a shard.
#[derive(Debug)]
pub enum ShardRequest {
    Get { key: String },
    Set { key: String, value: Bytes, expire: Option<Duration> },
    Del { key: String },
    Exists { key: String },
    Expire { key: String, seconds: u64 },
    Ttl { key: String },
}

/// The shard's response to a request.
#[derive(Debug)]
pub enum ShardResponse {
    /// A value (or None for a cache miss).
    Value(Option<Value>),
    /// Simple acknowledgement (e.g. SET).
    Ok,
    /// Boolean result (e.g. DEL, EXISTS, EXPIRE).
    Bool(bool),
    /// TTL query result.
    Ttl(TtlResult),
}

/// A request bundled with its reply channel.
#[derive(Debug)]
pub struct ShardMessage {
    pub request: ShardRequest,
    pub reply: oneshot::Sender<ShardResponse>,
}

/// A cloneable handle for sending commands to a shard task.
///
/// Wraps the mpsc sender so callers don't need to manage oneshot
/// channels directly.
#[derive(Debug, Clone)]
pub struct ShardHandle {
    tx: mpsc::Sender<ShardMessage>,
}

impl ShardHandle {
    /// Sends a request and waits for the response.
    ///
    /// Returns `ShardError::Unavailable` if the shard task has stopped.
    pub async fn send(&self, request: ShardRequest) -> Result<ShardResponse, ShardError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let msg = ShardMessage {
            request,
            reply: reply_tx,
        };
        self.tx.send(msg).await.map_err(|_| ShardError::Unavailable)?;
        reply_rx.await.map_err(|_| ShardError::Unavailable)
    }
}

/// Spawns a shard task and returns the handle for communicating with it.
///
/// `buffer` controls the mpsc channel capacity — higher values absorb
/// burst traffic at the cost of memory.
pub fn spawn_shard(buffer: usize) -> ShardHandle {
    let (tx, rx) = mpsc::channel(buffer);
    tokio::spawn(run_shard(rx));
    ShardHandle { tx }
}

/// The shard's main loop. Processes messages until the channel closes.
async fn run_shard(mut rx: mpsc::Receiver<ShardMessage>) {
    let mut keyspace = Keyspace::new();

    while let Some(msg) = rx.recv().await {
        let response = dispatch(&mut keyspace, msg.request);
        // if the caller dropped the receiver, that's fine — just discard
        let _ = msg.reply.send(response);
    }
}

/// Executes a single request against the keyspace.
fn dispatch(ks: &mut Keyspace, req: ShardRequest) -> ShardResponse {
    match req {
        ShardRequest::Get { key } => ShardResponse::Value(ks.get(&key)),
        ShardRequest::Set { key, value, expire } => {
            ks.set(key, value, expire);
            ShardResponse::Ok
        }
        ShardRequest::Del { key } => ShardResponse::Bool(ks.del(&key)),
        ShardRequest::Exists { key } => ShardResponse::Bool(ks.exists(&key)),
        ShardRequest::Expire { key, seconds } => ShardResponse::Bool(ks.expire(&key, seconds)),
        ShardRequest::Ttl { key } => ShardResponse::Ttl(ks.ttl(&key)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_set_and_get() {
        let mut ks = Keyspace::new();

        let resp = dispatch(
            &mut ks,
            ShardRequest::Set {
                key: "k".into(),
                value: Bytes::from("v"),
                expire: None,
            },
        );
        assert!(matches!(resp, ShardResponse::Ok));

        let resp = dispatch(&mut ks, ShardRequest::Get { key: "k".into() });
        match resp {
            ShardResponse::Value(Some(Value::String(data))) => {
                assert_eq!(data, Bytes::from("v"));
            }
            other => panic!("expected Value(Some(String)), got {other:?}"),
        }
    }

    #[test]
    fn dispatch_get_missing() {
        let mut ks = Keyspace::new();
        let resp = dispatch(&mut ks, ShardRequest::Get { key: "nope".into() });
        assert!(matches!(resp, ShardResponse::Value(None)));
    }

    #[test]
    fn dispatch_del() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None);

        let resp = dispatch(&mut ks, ShardRequest::Del { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = dispatch(&mut ks, ShardRequest::Del { key: "key".into() });
        assert!(matches!(resp, ShardResponse::Bool(false)));
    }

    #[test]
    fn dispatch_exists() {
        let mut ks = Keyspace::new();
        ks.set("yes".into(), Bytes::from("here"), None);

        let resp = dispatch(&mut ks, ShardRequest::Exists { key: "yes".into() });
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = dispatch(&mut ks, ShardRequest::Exists { key: "no".into() });
        assert!(matches!(resp, ShardResponse::Bool(false)));
    }

    #[test]
    fn dispatch_expire_and_ttl() {
        let mut ks = Keyspace::new();
        ks.set("key".into(), Bytes::from("val"), None);

        let resp = dispatch(
            &mut ks,
            ShardRequest::Expire {
                key: "key".into(),
                seconds: 60,
            },
        );
        assert!(matches!(resp, ShardResponse::Bool(true)));

        let resp = dispatch(&mut ks, ShardRequest::Ttl { key: "key".into() });
        match resp {
            ShardResponse::Ttl(TtlResult::Seconds(s)) => assert!(s >= 58 && s <= 60),
            other => panic!("expected Ttl(Seconds), got {other:?}"),
        }
    }

    #[test]
    fn dispatch_ttl_missing() {
        let mut ks = Keyspace::new();
        let resp = dispatch(&mut ks, ShardRequest::Ttl { key: "gone".into() });
        assert!(matches!(resp, ShardResponse::Ttl(TtlResult::NotFound)));
    }

    #[tokio::test]
    async fn shard_round_trip() {
        let handle = spawn_shard(16);

        let resp = handle
            .send(ShardRequest::Set {
                key: "hello".into(),
                value: Bytes::from("world"),
                expire: None,
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Ok));

        let resp = handle
            .send(ShardRequest::Get {
                key: "hello".into(),
            })
            .await
            .unwrap();
        match resp {
            ShardResponse::Value(Some(Value::String(data))) => {
                assert_eq!(data, Bytes::from("world"));
            }
            other => panic!("expected Value(Some(String)), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn expired_key_through_shard() {
        let handle = spawn_shard(16);

        handle
            .send(ShardRequest::Set {
                key: "temp".into(),
                value: Bytes::from("gone"),
                expire: Some(Duration::from_millis(10)),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(30)).await;

        let resp = handle
            .send(ShardRequest::Get {
                key: "temp".into(),
            })
            .await
            .unwrap();
        assert!(matches!(resp, ShardResponse::Value(None)));
    }
}
