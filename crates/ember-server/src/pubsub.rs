//! Pub/sub message broker for channel-based messaging.
//!
//! Each subscribing connection has one inbox. The manager maps every
//! channel and pattern to the inboxes subscribed to it, and PUBLISH puts
//! the message in each matching inbox, as Redis does. A connection waits on
//! its inbox alone, however many channels it listens to.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use ember_core::glob_match;
use tokio::sync::mpsc;

/// Maximum allowed byte length for a pub/sub pattern.
///
/// Longer patterns provide no real-world value and allow clients to
/// force repeated glob-match work on every PUBLISH call.
const MAX_PATTERN_LEN: usize = 512;

/// Messages one subscriber's inbox holds. A subscriber that falls this far
/// behind misses the messages published until it catches up.
const INBOX_CAPACITY: usize = 1024;

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

/// Where PUBLISH puts a subscriber's messages.
struct Inbox {
    tx: mpsc::Sender<PubMessage>,
    /// Messages dropped because the inbox was full, since the subscriber
    /// last checked.
    dropped: AtomicU64,
}

impl Inbox {
    /// Puts `msg` in the inbox. Returns whether it went in.
    fn deliver(&self, msg: PubMessage) -> bool {
        match self.tx.try_send(msg) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }
}

/// The inboxes subscribed to each channel or pattern, keyed by subscriber.
type Subscribers = DashMap<String, HashMap<u64, Arc<Inbox>>>;

/// Manages pub/sub state: channel subscriptions, pattern subscriptions,
/// and message broadcasting.
///
/// Shared across all connection handlers via `Arc<PubSubManager>`.
pub struct PubSubManager {
    channels: Subscribers,
    patterns: Subscribers,
    next_id: AtomicU64,
}

impl PubSubManager {
    pub fn new() -> Self {
        Self {
            channels: DashMap::new(),
            patterns: DashMap::new(),
            next_id: AtomicU64::new(0),
        }
    }

    /// Publish a message to a channel. Returns the number of subscribers it
    /// was delivered to, exact and pattern subscriptions both counted.
    pub fn publish(&self, channel: &str, data: Bytes) -> usize {
        let mut count = 0;

        // allocate the channel name once; all PubMessage clones share the Arc
        let channel_arc: Arc<str> = Arc::from(channel);

        if let Some(inboxes) = self.channels.get(channel) {
            let msg = PubMessage {
                channel: Arc::clone(&channel_arc),
                data: data.clone(),
                pattern: None,
            };
            count += inboxes
                .values()
                .filter(|inbox| inbox.deliver(msg.clone()))
                .count();
        }

        for entry in self.patterns.iter() {
            if glob_match(entry.key(), channel) {
                let msg = PubMessage {
                    channel: Arc::clone(&channel_arc),
                    data: data.clone(),
                    pattern: Some(Arc::from(entry.key().as_str())),
                };
                count += entry
                    .value()
                    .values()
                    .filter(|inbox| inbox.deliver(msg.clone()))
                    .count();
            }
        }

        count
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
                let count = self.channels.get(ch).map_or(0, |inboxes| inboxes.len());
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

fn add(map: &Subscribers, key: &str, id: u64, inbox: &Arc<Inbox>) {
    map.entry(key.to_string())
        .or_default()
        .insert(id, Arc::clone(inbox));
}

/// Removes subscriber `id` from `key`, and `key` once nobody is left on it.
fn remove(map: &Subscribers, key: &str, id: u64) {
    if let Some(mut inboxes) = map.get_mut(key) {
        inboxes.remove(&id);
    }
    // checks and removes under the map's lock, so a concurrent subscribe
    // cannot slip in between
    map.remove_if(key, |_, inboxes| inboxes.is_empty());
}

/// One subscriber's channels and patterns, and the inbox their messages
/// arrive in. Dropping it unsubscribes from everything, so the
/// subscriptions are released on every exit, including early returns on
/// I/O errors.
pub struct Subscriber {
    pubsub: Arc<PubSubManager>,
    id: u64,
    inbox: Arc<Inbox>,
    rx: mpsc::Receiver<PubMessage>,
    channels: HashSet<String>,
    patterns: HashSet<String>,
}

impl Subscriber {
    pub fn new(pubsub: Arc<PubSubManager>) -> Self {
        let (tx, rx) = mpsc::channel(INBOX_CAPACITY);
        let id = pubsub.next_id.fetch_add(1, Ordering::Relaxed);
        Self {
            pubsub,
            id,
            inbox: Arc::new(Inbox {
                tx,
                dropped: AtomicU64::new(0),
            }),
            rx,
            channels: HashSet::new(),
            patterns: HashSet::new(),
        }
    }

    /// Subscribes to a channel. Subscribing twice is a no-op, as in Redis.
    pub fn subscribe(&mut self, channel: &str) {
        if self.channels.insert(channel.to_string()) {
            add(&self.pubsub.channels, channel, self.id, &self.inbox);
        }
    }

    /// Unsubscribes from a channel, if subscribed.
    pub fn unsubscribe(&mut self, channel: &str) {
        if self.channels.remove(channel) {
            remove(&self.pubsub.channels, channel, self.id);
        }
    }

    /// Subscribes to a glob pattern. Subscribing twice is a no-op. Returns
    /// `false` if the pattern exceeds `MAX_PATTERN_LEN` bytes.
    pub fn psubscribe(&mut self, pattern: &str) -> bool {
        if pattern.len() > MAX_PATTERN_LEN {
            return false;
        }
        if self.patterns.insert(pattern.to_string()) {
            add(&self.pubsub.patterns, pattern, self.id, &self.inbox);
        }
        true
    }

    /// Unsubscribes from a pattern, if subscribed.
    pub fn punsubscribe(&mut self, pattern: &str) {
        if self.patterns.remove(pattern) {
            remove(&self.pubsub.patterns, pattern, self.id);
        }
    }

    pub fn channels(&self) -> &HashSet<String> {
        &self.channels
    }

    pub fn patterns(&self) -> &HashSet<String> {
        &self.patterns
    }

    /// Channels and patterns subscribed to.
    pub fn count(&self) -> usize {
        self.channels.len() + self.patterns.len()
    }

    /// Waits for the next message. A message that arrived for a channel or
    /// pattern unsubscribed from since is skipped. Cancel safe.
    pub async fn recv(&mut self) -> PubMessage {
        loop {
            // the subscriber holds a sender, so the inbox never closes
            let Some(msg) = self.rx.recv().await else {
                return std::future::pending().await;
            };
            let dropped = self.inbox.dropped.swap(0, Ordering::Relaxed);
            if dropped > 0 {
                tracing::warn!("subscriber fell behind and missed {dropped} messages");
            }
            let subscribed = match &msg.pattern {
                Some(pattern) => self.patterns.contains(&**pattern),
                None => self.channels.contains(&*msg.channel),
            };
            if subscribed {
                return msg;
            }
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        for channel in &self.channels {
            remove(&self.pubsub.channels, channel, self.id);
        }
        for pattern in &self.patterns {
            remove(&self.pubsub.patterns, pattern, self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager() -> Arc<PubSubManager> {
        Arc::new(PubSubManager::new())
    }

    fn data(msg: &PubMessage) -> &[u8] {
        &msg.data
    }

    #[tokio::test]
    async fn subscribe_and_publish() {
        let mgr = manager();
        let mut sub = Subscriber::new(Arc::clone(&mgr));
        sub.subscribe("test");

        assert_eq!(mgr.publish("test", Bytes::from("hello")), 1);
        let msg = sub.recv().await;
        assert_eq!(&*msg.channel, "test");
        assert_eq!(data(&msg), b"hello");
        assert!(msg.pattern.is_none());
    }

    #[test]
    fn publish_to_empty_channel() {
        assert_eq!(manager().publish("nobody", Bytes::from("hello")), 0);
    }

    #[tokio::test]
    async fn multiple_subscribers() {
        let mgr = manager();
        let mut a = Subscriber::new(Arc::clone(&mgr));
        let mut b = Subscriber::new(Arc::clone(&mgr));
        a.subscribe("ch");
        b.subscribe("ch");

        assert_eq!(mgr.publish("ch", Bytes::from("msg")), 2);
        assert_eq!(data(&a.recv().await), b"msg");
        assert_eq!(data(&b.recv().await), b"msg");
        assert_eq!(mgr.numsub(&["ch".into()]), [("ch".into(), 2)]);
    }

    #[tokio::test]
    async fn exact_and_pattern_both_receive() {
        let mgr = manager();
        let mut sub = Subscriber::new(Arc::clone(&mgr));
        sub.subscribe("news.sports");
        assert!(sub.psubscribe("news.*"));

        // one subscriber, two subscriptions: one message each
        assert_eq!(mgr.publish("news.sports", Bytes::from("goal")), 2);
        let first = sub.recv().await;
        let second = sub.recv().await;
        assert!(first.pattern.is_none());
        assert_eq!(second.pattern.as_deref(), Some("news.*"));
        assert_eq!(&*second.channel, "news.sports");
    }

    #[test]
    fn subscribing_twice_counts_once() {
        let mgr = manager();
        let mut sub = Subscriber::new(Arc::clone(&mgr));
        sub.subscribe("ch");
        sub.subscribe("ch");
        assert!(sub.psubscribe("p*"));
        assert!(sub.psubscribe("p*"));
        assert_eq!(sub.count(), 2);
        assert_eq!(mgr.publish("ch", Bytes::new()), 1);
    }

    #[test]
    fn unsubscribe_releases_the_channel() {
        let mgr = manager();
        let mut a = Subscriber::new(Arc::clone(&mgr));
        let mut b = Subscriber::new(Arc::clone(&mgr));
        a.subscribe("ch");
        b.subscribe("ch");

        a.unsubscribe("ch");
        assert_eq!(mgr.publish("ch", Bytes::new()), 1);
        b.unsubscribe("ch");
        assert_eq!(mgr.publish("ch", Bytes::new()), 0);
        assert!(mgr.channel_names(None).is_empty());
    }

    #[tokio::test]
    async fn a_message_queued_before_unsubscribing_is_skipped() {
        let mgr = manager();
        let mut sub = Subscriber::new(Arc::clone(&mgr));
        sub.subscribe("old");
        sub.subscribe("new");
        mgr.publish("old", Bytes::from("stale"));
        sub.unsubscribe("old");
        mgr.publish("new", Bytes::from("fresh"));
        assert_eq!(data(&sub.recv().await), b"fresh");
    }

    #[test]
    fn dropping_a_subscriber_releases_everything() {
        let mgr = manager();
        let mut sub = Subscriber::new(Arc::clone(&mgr));
        sub.subscribe("a");
        sub.subscribe("b");
        assert!(sub.psubscribe("c.*"));
        assert_eq!(mgr.channel_names(None).len(), 2);
        assert_eq!(mgr.active_patterns(), 1);

        drop(sub);
        assert!(mgr.channel_names(None).is_empty());
        assert_eq!(mgr.active_patterns(), 0);
    }

    #[tokio::test]
    async fn a_full_inbox_drops_messages_and_keeps_going() {
        let mgr = manager();
        let mut sub = Subscriber::new(Arc::clone(&mgr));
        sub.subscribe("busy");
        for _ in 0..INBOX_CAPACITY {
            assert_eq!(mgr.publish("busy", Bytes::from("old")), 1);
        }
        // the inbox is full, so this one is dropped
        assert_eq!(mgr.publish("busy", Bytes::from("lost")), 0);
        for _ in 0..INBOX_CAPACITY {
            assert_eq!(data(&sub.recv().await), b"old");
        }
        mgr.publish("busy", Bytes::from("new"));
        assert_eq!(data(&sub.recv().await), b"new");
    }

    #[test]
    fn psubscribe_rejects_oversized_pattern() {
        let mut sub = Subscriber::new(manager());
        let long_pattern = "*".repeat(MAX_PATTERN_LEN + 1);
        assert!(!sub.psubscribe(&long_pattern));
        assert_eq!(sub.count(), 0);
        // a pattern right at the limit is allowed
        assert!(sub.psubscribe(&"*".repeat(MAX_PATTERN_LEN)));
    }
}
