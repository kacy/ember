//! Pub/sub subscriber mode.
//!
//! When a connection issues [`Client::subscribe`] or [`Client::psubscribe`] it
//! enters pub/sub mode: the server will push [`Message`] frames whenever a
//! matching publish event occurs. Normal request-response commands cannot be
//! issued on the same connection while it is in sub mode.
//!
//! Use a separate [`Client`] for regular commands while subscribed.

use std::collections::{HashSet, VecDeque};

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
    /// Messages that arrived while waiting for a confirmation, returned by
    /// [`recv`](Self::recv) before anything else.
    pending: VecDeque<Message>,
    /// The channels subscribed to, so UNSUBSCRIBE without names knows how
    /// many confirmations to expect.
    channels: HashSet<Bytes>,
}

impl Subscriber {
    pub(crate) fn new(inner: Client) -> Self {
        Self {
            inner,
            pending: VecDeque::new(),
            channels: HashSet::new(),
        }
    }

    /// Blocks until the next message arrives on any subscribed channel.
    ///
    /// Subscription confirmation frames (`subscribe`/`psubscribe`) are
    /// skipped silently — only actual message frames are returned.
    pub async fn recv(&mut self) -> Result<Message, ClientError> {
        if let Some(msg) = self.pending.pop_front() {
            return Ok(msg);
        }
        loop {
            match parse_push(self.inner.read_message().await?)? {
                Push::Message(msg) => return Ok(msg),
                Push::Confirmation { .. } => {}
            }
        }
    }

    /// Subscribes to additional channels without leaving sub mode.
    pub async fn subscribe(&mut self, channels: &[&str]) -> Result<(), ClientError> {
        self.send_and_confirm("SUBSCRIBE", channels, channels.len())
            .await
    }

    /// Subscribes to additional glob patterns without leaving sub mode.
    pub async fn psubscribe(&mut self, patterns: &[&str]) -> Result<(), ClientError> {
        self.send_and_confirm("PSUBSCRIBE", patterns, patterns.len())
            .await
    }

    /// Unsubscribes from the given channels, or from every channel when
    /// `channels` is empty.
    ///
    /// When all subscriptions have been removed the inner [`Client`] is
    /// returned so the connection can be reused for regular commands.
    pub async fn unsubscribe(mut self, channels: &[&str]) -> Result<Client, ClientError> {
        // without names the server confirms each channel, or sends one
        // confirmation when there are none
        let expected = if channels.is_empty() {
            self.channels.len().max(1)
        } else {
            channels.len()
        };
        self.send_and_confirm("UNSUBSCRIBE", channels, expected)
            .await?;
        Ok(self.inner)
    }

    /// Sends `command` with `names` and reads replies until `expected`
    /// confirmations have arrived. Messages that arrive in between are kept
    /// for [`recv`](Self::recv), so none is lost or taken for a
    /// confirmation.
    async fn send_and_confirm(
        &mut self,
        command: &'static str,
        names: &[&str],
        expected: usize,
    ) -> Result<(), ClientError> {
        let mut parts = Vec::with_capacity(1 + names.len());
        parts.push(Frame::Bulk(Bytes::from_static(command.as_bytes())));
        for name in names {
            parts.push(Frame::Bulk(Bytes::copy_from_slice(name.as_bytes())));
        }
        self.inner.write_frame(Frame::Array(parts)).await?;

        let mut confirmed = 0;
        while confirmed < expected {
            match parse_push(self.inner.read_message().await?)? {
                Push::Message(msg) => self.pending.push_back(msg),
                Push::Confirmation { kind, name } => {
                    match (kind.as_ref(), name) {
                        (b"subscribe", Some(name)) => {
                            self.channels.insert(name);
                        }
                        (b"unsubscribe", Some(name)) => {
                            self.channels.remove(&name);
                        }
                        _ => {}
                    }
                    confirmed += 1;
                }
            }
        }
        Ok(())
    }
}

/// A frame pushed to a subscribed connection.
enum Push {
    Message(Message),
    /// A reply to SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE or PUNSUBSCRIBE: its
    /// kind and the channel or pattern it is about, if any.
    Confirmation {
        kind: Bytes,
        name: Option<Bytes>,
    },
}

/// Sorts a pushed frame into a message (`message` or `pmessage`) or a
/// subscription confirmation. Returns `Err` for unexpected or malformed
/// frames.
fn parse_push(frame: Frame) -> Result<Push, ClientError> {
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
            Ok(Push::Message(Message {
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
            Ok(Push::Message(Message {
                channel,
                data,
                pattern: Some(pattern),
            }))
        }
        // subscribe / unsubscribe / psubscribe / punsubscribe confirmations
        _ => {
            let name = match &elems[1] {
                Frame::Bulk(b) => Some(b.clone()),
                _ => None,
            };
            Ok(Push::Confirmation { kind, name })
        }
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
