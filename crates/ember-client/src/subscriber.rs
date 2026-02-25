//! Pub/sub subscriber mode.
//!
//! When a connection issues [`Client::subscribe`] or [`Client::psubscribe`] it
//! enters pub/sub mode: the server will push [`Message`] frames whenever a
//! matching publish event occurs. Normal request-response commands cannot be
//! issued on the same connection while it is in sub mode.
//!
//! Use a separate [`Client`] for regular commands while subscribed.

use bytes::Bytes;
use ember_protocol::types::Frame;

use crate::connection::{Client, ClientError};

/// A message pushed by the server to a subscribed connection.
#[derive(Debug, Clone)]
pub struct Message {
    /// The channel the message was published to.
    pub channel: Bytes,
    /// The message payload.
    pub data: Bytes,
    /// Set only for pattern-matched messages (`PSUBSCRIBE`). Contains the
    /// pattern that matched the channel.
    pub pattern: Option<Bytes>,
}

/// A connection locked into pub/sub mode.
///
/// Obtained by calling [`Client::subscribe`] or [`Client::psubscribe`].
/// The underlying transport is consumed — create a separate [`Client`]
/// for regular commands while this subscriber is active.
///
/// # Example
///
/// ```no_run
/// use ember_client::Client;
///
/// #[tokio::main]
/// async fn main() -> Result<(), ember_client::ClientError> {
///     let mut publisher = Client::connect("127.0.0.1", 6379).await?;
///     let subscriber_conn = Client::connect("127.0.0.1", 6379).await?;
///
///     let mut sub = subscriber_conn.subscribe(&["news"]).await?;
///
///     publisher.publish("news", "breaking: hello world").await?;
///
///     let msg = sub.recv().await?;
///     println!("got: {:?}", msg.data);
///     Ok(())
/// }
/// ```
pub struct Subscriber {
    inner: Client,
}

impl Subscriber {
    pub(crate) fn new(inner: Client) -> Self {
        Self { inner }
    }

    /// Blocks until the next message arrives on any subscribed channel.
    ///
    /// Subscription confirmation frames (`subscribe`/`psubscribe`) are
    /// skipped silently — only actual message frames are returned.
    pub async fn recv(&mut self) -> Result<Message, ClientError> {
        loop {
            let frame = self.inner.read_response().await?;
            if let Some(msg) = try_parse_message(frame)? {
                return Ok(msg);
            }
            // confirmation frame (subscribe/unsubscribe/psubscribe/punsubscribe)
            // — loop back and wait for the next frame
        }
    }

    /// Subscribes to additional channels without leaving sub mode.
    pub async fn subscribe(&mut self, channels: &[&str]) -> Result<(), ClientError> {
        let mut parts = Vec::with_capacity(1 + channels.len());
        parts.push(Frame::Bulk(Bytes::from_static(b"SUBSCRIBE")));
        for ch in channels {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(ch.as_bytes())));
        }
        self.inner.write_frame(Frame::Array(parts)).await?;
        // drain the confirmation frames (one per channel)
        for _ in 0..channels.len() {
            self.inner.read_response().await?;
        }
        Ok(())
    }

    /// Unsubscribes from the given channels.
    ///
    /// When all subscriptions have been removed the inner [`Client`] is
    /// returned so the connection can be reused for regular commands.
    pub async fn unsubscribe(mut self, channels: &[&str]) -> Result<Client, ClientError> {
        let mut parts = Vec::with_capacity(1 + channels.len());
        parts.push(Frame::Bulk(Bytes::from_static(b"UNSUBSCRIBE")));
        for ch in channels {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(ch.as_bytes())));
        }
        self.inner.write_frame(Frame::Array(parts)).await?;
        // drain the unsubscribe confirmation frames
        for _ in 0..channels.len() {
            self.inner.read_response().await?;
        }
        Ok(self.inner)
    }
}

/// Tries to parse a push frame into a [`Message`].
///
/// Returns `Ok(Some(_))` for `message` and `pmessage` frames.
/// Returns `Ok(None)` for subscription management frames.
/// Returns `Err` for unexpected or malformed frames.
fn try_parse_message(frame: Frame) -> Result<Option<Message>, ClientError> {
    let elems = match frame {
        Frame::Array(e) => e,
        Frame::Error(e) => return Err(ClientError::Server(e)),
        other => {
            return Err(ClientError::Protocol(format!(
                "expected array push frame, got {other:?}"
            )))
        }
    };

    if elems.len() < 3 {
        return Err(ClientError::Protocol(format!(
            "push frame too short: {} elements",
            elems.len()
        )));
    }

    let kind = match &elems[0] {
        Frame::Bulk(b) => b.clone(),
        Frame::Simple(s) => Bytes::copy_from_slice(s.as_bytes()),
        other => {
            return Err(ClientError::Protocol(format!(
                "expected bulk/simple frame kind, got {other:?}"
            )))
        }
    };

    match kind.as_ref() {
        b"message" => {
            if elems.len() < 3 {
                return Err(ClientError::Protocol(
                    "message frame has fewer than 3 elements".into(),
                ));
            }
            let channel = bulk_bytes(elems[1].clone())?;
            let data = bulk_bytes(elems[2].clone())?;
            Ok(Some(Message {
                channel,
                data,
                pattern: None,
            }))
        }
        b"pmessage" => {
            if elems.len() < 4 {
                return Err(ClientError::Protocol(
                    "pmessage frame has fewer than 4 elements".into(),
                ));
            }
            let pattern = bulk_bytes(elems[1].clone())?;
            let channel = bulk_bytes(elems[2].clone())?;
            let data = bulk_bytes(elems[3].clone())?;
            Ok(Some(Message {
                channel,
                data,
                pattern: Some(pattern),
            }))
        }
        // subscribe / unsubscribe / psubscribe / punsubscribe confirmations
        _ => Ok(None),
    }
}

fn bulk_bytes(frame: Frame) -> Result<Bytes, ClientError> {
    match frame {
        Frame::Bulk(b) => Ok(b),
        other => Err(ClientError::Protocol(format!(
            "expected bulk in push frame, got {other:?}"
        ))),
    }
}
