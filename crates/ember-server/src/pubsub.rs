//! Pub/sub message broker for channel-based messaging.
//!
//! Manages subscriptions and broadcasts messages to all matching
//! subscribers. Supports both exact channel names and glob patterns.
//! Thread-safe via DashMap for lock-free concurrent access.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use ember_core::glob_match;
use tokio::sync::broadcast;

/// Maximum allowed byte length for a pub/sub pattern.
///
/// Longer patterns provide no real-world value and allow clients to
/// force repeated glob-match work on every PUBLISH call.
const MAX_PATTERN_LEN: usize = 512;

/// Maximum number of buffered messages per subscription before
/// slow consumers start missing messages. This is per-channel,
/// not global — a subscriber that falls behind on one busy channel
/// won't affect its other subscriptions.
const CHANNEL_CAPACITY: usize = 256;

/// A message published to a channel.
///
/// `channel` and `pattern` are stored as `Arc<str>` so multiple subscribers
/// receiving the same publish share a single allocation rather than each
/// getting their own `String` copy.
#[derive(Debug, Clone)]
pub struct PubMessage {
    /// The channel the message was published to.
    pub channel: Arc<str>,
    /// The raw message data.
    pub data: Bytes,
    /// For pattern subscriptions, the pattern that matched.
    /// None for exact channel subscriptions.
    pub pattern: Option<Arc<str>>,
}

/// Manages pub/sub state: channel subscriptions, pattern subscriptions,
/// and message broadcasting.
///
/// Shared across all connection handlers via `Arc<PubSubManager>`.
/// Uses DashMap internally so all operations are lock-free.
pub struct PubSubManager {
    /// Exact channel subscriptions: channel name → broadcast sender.
    channels: DashMap<String, broadcast::Sender<PubMessage>>,
    /// Pattern subscriptions: pattern string → broadcast sender.
    patterns: DashMap<String, broadcast::Sender<PubMessage>>,
    /// Total number of active subscriptions (channels + patterns).
    subscription_count: AtomicUsize,
}

impl PubSubManager {
    pub fn new() -> Self {
        Self {
            channels: DashMap::new(),
            patterns: DashMap::new(),
            subscription_count: AtomicUsize::new(0),
        }
    }

    /// Subscribe to an exact channel. Returns a receiver for messages
    /// on that channel.
    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<PubMessage> {
        self.subscribe_to(&self.channels, channel)
    }

    /// Unsubscribe from an exact channel by handing back the receiver that
    /// [`subscribe`](Self::subscribe) returned. The channel is removed once
    /// no subscriber holds a receiver for it.
    pub fn unsubscribe(&self, channel: &str, rx: broadcast::Receiver<PubMessage>) {
        self.unsubscribe_from(&self.channels, channel, rx)
    }

    /// Subscribe to a glob pattern. Returns a receiver for messages
    /// matching the pattern.
    ///
    /// Returns `None` if the pattern exceeds `MAX_PATTERN_LEN` bytes.
    pub fn psubscribe(&self, pattern: &str) -> Option<broadcast::Receiver<PubMessage>> {
        if pattern.len() > MAX_PATTERN_LEN {
            return None;
        }
        Some(self.subscribe_to(&self.patterns, pattern))
    }

    /// Unsubscribe from a pattern by handing back the receiver that
    /// [`psubscribe`](Self::psubscribe) returned.
    pub fn punsubscribe(&self, pattern: &str, rx: broadcast::Receiver<PubMessage>) {
        self.unsubscribe_from(&self.patterns, pattern, rx)
    }

    /// Subscribes to a key in the given map (channels or patterns).
    fn subscribe_to(
        &self,
        map: &DashMap<String, broadcast::Sender<PubMessage>>,
        key: &str,
    ) -> broadcast::Receiver<PubMessage> {
        let entry = map.entry(key.to_string()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            tx
        });
        self.subscription_count.fetch_add(1, Ordering::Relaxed);
        entry.subscribe()
    }

    /// Drops `rx` and removes the key's sender once no receivers remain.
    ///
    /// Taking the receiver means only a real subscriber can unsubscribe, so
    /// the count cannot drift. It also means the receiver is gone before the
    /// check: counting it as "ours" instead would remove a channel that
    /// other clients still listen on. `remove_if` checks and removes under
    /// the map's lock, so a concurrent subscribe cannot slip in between.
    fn unsubscribe_from(
        &self,
        map: &DashMap<String, broadcast::Sender<PubMessage>>,
        key: &str,
        rx: broadcast::Receiver<PubMessage>,
    ) {
        drop(rx);
        self.subscription_count.fetch_sub(1, Ordering::Relaxed);
        map.remove_if(key, |_, tx| tx.receiver_count() == 0);
    }

    /// Publish a message to a channel. Returns the total number of
    /// subscribers that received the message (exact + pattern).
    pub fn publish(&self, channel: &str, data: Bytes) -> usize {
        let mut count = 0;

        // allocate the channel name once; all PubMessage clones share the Arc
        let channel_arc: Arc<str> = Arc::from(channel);

        // send to exact channel subscribers
        if let Some(tx) = self.channels.get(channel) {
            let msg = PubMessage {
                channel: Arc::clone(&channel_arc),
                data: data.clone(),
                pattern: None,
            };
            // send returns the number of receivers that got the message
            count += tx.send(msg).unwrap_or(0);
        }

        // send to pattern subscribers
        for entry in self.patterns.iter() {
            let pattern = entry.key();
            if glob_match(pattern, channel) {
                // allocate the pattern string once per matching entry
                let pattern_arc: Arc<str> = Arc::from(pattern.as_str());
                let msg = PubMessage {
                    channel: Arc::clone(&channel_arc),
                    data: data.clone(),
                    pattern: Some(pattern_arc),
                };
                count += entry.value().send(msg).unwrap_or(0);
            }
        }

        count
    }

    /// Returns the total number of active subscriptions.
    #[allow(dead_code)] // used in tests
    pub fn total_subscriptions(&self) -> usize {
        self.subscription_count.load(Ordering::Relaxed)
    }

    /// Returns active channel names, optionally filtered by a glob pattern.
    /// Used by PUBSUB CHANNELS [pattern].
    pub fn channel_names(&self, pattern: Option<&str>) -> Vec<String> {
        self.channels
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|name| match pattern {
                Some(pat) => glob_match(pat, name),
                None => true,
            })
            .collect()
    }

    /// Returns (channel, subscriber_count) pairs for the given channels.
    /// Used by PUBSUB NUMSUB [channel ...].
    pub fn numsub(&self, channels: &[String]) -> Vec<(String, usize)> {
        channels
            .iter()
            .map(|ch| {
                let count = self
                    .channels
                    .get(ch)
                    .map(|tx| tx.receiver_count())
                    .unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }

    /// Returns the number of active pattern subscriptions.
    /// Used by PUBSUB NUMPAT.
    pub fn active_patterns(&self) -> usize {
        self.patterns.len()
    }
}

/// Waits for the next message on any of the subscriptions.
///
/// A receiver that lagged skips the messages it missed, and the wait starts
/// over, so every subscription is listened to again. A channel whose sender
/// is gone is dropped from the subscriptions.
pub async fn recv_any(
    channel_rxs: &mut HashMap<String, broadcast::Receiver<PubMessage>>,
    pattern_rxs: &mut HashMap<String, broadcast::Receiver<PubMessage>>,
) -> Option<PubMessage> {
    use futures::stream::{FuturesUnordered, StreamExt};

    loop {
        if channel_rxs.is_empty() && pattern_rxs.is_empty() {
            // no subscriptions — sleep forever (will be cancelled by select)
            return std::future::pending::<Option<PubMessage>>().await;
        }

        // the first receiver to resolve, with the subscription it belongs
        // to: (is a pattern, name)
        let (from, result) = {
            let subscriptions = channel_rxs
                .iter_mut()
                .map(|(name, rx)| (false, name, rx))
                .chain(pattern_rxs.iter_mut().map(|(name, rx)| (true, name, rx)));
            let mut pending: FuturesUnordered<_> = subscriptions
                .map(|(is_pattern, name, rx)| async move {
                    ((is_pattern, name.clone()), rx.recv().await)
                })
                .collect();
            pending.next().await?
        };

        match result {
            Ok(msg) => return Some(msg),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                // the receiver moved past the gap; listen again
                tracing::warn!("subscriber lagged, missed {n} messages");
            }
            Err(broadcast::error::RecvError::Closed) => {
                let (is_pattern, name) = from;
                if is_pattern {
                    pattern_rxs.remove(&name);
                } else {
                    channel_rxs.remove(&name);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(channel: &str) -> PubMessage {
        PubMessage {
            channel: channel.into(),
            data: Bytes::from_static(b"m"),
            pattern: None,
        }
    }

    #[tokio::test]
    async fn recv_any_keeps_listening_after_a_lag() {
        // one subscription lags while the other stays idle; the next
        // message must still arrive
        let (busy_tx, busy_rx) = broadcast::channel(1);
        let (_idle_tx, idle_rx) = broadcast::channel::<PubMessage>(1);
        let mut channels =
            HashMap::from([("busy".to_owned(), busy_rx), ("idle".to_owned(), idle_rx)]);
        let mut patterns = HashMap::new();
        for _ in 0..3 {
            busy_tx.send(message("busy")).unwrap();
        }
        let got = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            recv_any(&mut channels, &mut patterns),
        )
        .await
        .expect("recv_any hung after a lag");
        assert_eq!(got.unwrap().channel.as_ref(), "busy");
    }

    #[tokio::test]
    async fn recv_any_drops_a_closed_channel() {
        let (closed_tx, closed_rx) = broadcast::channel::<PubMessage>(1);
        let (open_tx, open_rx) = broadcast::channel(1);
        drop(closed_tx);
        let mut channels = HashMap::from([
            ("closed".to_owned(), closed_rx),
            ("open".to_owned(), open_rx),
        ]);
        let mut patterns = HashMap::new();
        open_tx.send(message("open")).unwrap();
        let got = recv_any(&mut channels, &mut patterns).await.unwrap();
        assert_eq!(got.channel.as_ref(), "open");
        let _ = futures::FutureExt::now_or_never(recv_any(&mut channels, &mut patterns));
        assert!(!channels.contains_key("closed"));
    }

    #[test]
    fn subscribe_and_publish() {
        let mgr = PubSubManager::new();
        let mut rx = mgr.subscribe("test");
        let count = mgr.publish("test", Bytes::from("hello"));
        assert_eq!(count, 1);

        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.channel.as_ref(), "test");
        assert_eq!(msg.data, Bytes::from("hello"));
        assert!(msg.pattern.is_none());
    }

    #[test]
    fn publish_to_empty_channel() {
        let mgr = PubSubManager::new();
        let count = mgr.publish("nobody", Bytes::from("hello"));
        assert_eq!(count, 0);
    }

    #[test]
    fn multiple_subscribers() {
        let mgr = PubSubManager::new();
        let mut rx1 = mgr.subscribe("ch");
        let mut rx2 = mgr.subscribe("ch");

        let count = mgr.publish("ch", Bytes::from("msg"));
        assert_eq!(count, 2);

        assert_eq!(rx1.try_recv().unwrap().data, Bytes::from("msg"));
        assert_eq!(rx2.try_recv().unwrap().data, Bytes::from("msg"));
    }

    #[test]
    fn pattern_subscribe_and_publish() {
        let mgr = PubSubManager::new();
        let mut rx = mgr.psubscribe("news.*").unwrap();

        let count = mgr.publish("news.sports", Bytes::from("goal!"));
        assert_eq!(count, 1);

        let msg = rx.try_recv().unwrap();
        assert_eq!(msg.channel.as_ref(), "news.sports");
        assert_eq!(msg.pattern.as_deref(), Some("news.*"));

        // shouldn't match
        let count = mgr.publish("old.news", Bytes::from("nope"));
        assert_eq!(count, 0);
    }

    #[test]
    fn exact_and_pattern_both_receive() {
        let mgr = PubSubManager::new();
        let mut rx_exact = mgr.subscribe("news.sports");
        let mut rx_pattern = mgr.psubscribe("news.*").unwrap();

        let count = mgr.publish("news.sports", Bytes::from("goal!"));
        assert_eq!(count, 2);

        assert!(rx_exact.try_recv().is_ok());
        assert!(rx_pattern.try_recv().is_ok());
    }

    #[test]
    fn unsubscribe_stops_delivery() {
        let mgr = PubSubManager::new();
        let rx = mgr.subscribe("ch");
        mgr.unsubscribe("ch", rx);

        let count = mgr.publish("ch", Bytes::from("msg"));
        assert_eq!(count, 0);
    }

    #[test]
    fn unsubscribe_keeps_the_channel_for_other_subscribers() {
        let mgr = PubSubManager::new();
        let rx_a = mgr.subscribe("ch");
        let mut rx_b = mgr.subscribe("ch");
        mgr.unsubscribe("ch", rx_a);

        assert_eq!(mgr.publish("ch", Bytes::from("msg")), 1);
        assert!(rx_b.try_recv().is_ok());
    }

    #[test]
    fn subscription_counts() {
        let mgr = PubSubManager::new();
        assert_eq!(mgr.total_subscriptions(), 0);

        let _rx1 = mgr.subscribe("a");
        let _rx2 = mgr.subscribe("b");
        let _rx3 = mgr.psubscribe("c.*").unwrap();
        assert_eq!(mgr.total_subscriptions(), 3);
        assert_eq!(mgr.channel_names(None).len(), 2);
        assert_eq!(mgr.active_patterns(), 1);
    }

    #[test]
    fn psubscribe_rejects_oversized_pattern() {
        let mgr = PubSubManager::new();
        let long_pattern = "*".repeat(MAX_PATTERN_LEN + 1);
        assert!(
            mgr.psubscribe(&long_pattern).is_none(),
            "oversized pattern should be rejected"
        );
        // a pattern right at the limit is allowed
        let ok_pattern = "*".repeat(MAX_PATTERN_LEN);
        assert!(mgr.psubscribe(&ok_pattern).is_some());
    }
}
