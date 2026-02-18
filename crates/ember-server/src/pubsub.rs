//! Pub/sub message broker for channel-based messaging.
//!
//! Manages subscriptions and broadcasts messages to all matching
//! subscribers. Supports both exact channel names and glob patterns.
//! Thread-safe via DashMap for lock-free concurrent access.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
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

    /// Unsubscribe from an exact channel. Returns true if the channel
    /// existed in the registry.
    ///
    /// Note: the actual receiver is dropped by the caller. This just
    /// cleans up empty channels and adjusts the subscription count.
    pub fn unsubscribe(&self, channel: &str) -> bool {
        self.unsubscribe_from(&self.channels, channel)
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

    /// Unsubscribe from a pattern. Returns true if the pattern existed
    /// in the registry.
    pub fn punsubscribe(&self, pattern: &str) -> bool {
        self.unsubscribe_from(&self.patterns, pattern)
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

    /// Unsubscribes from a key in the given map. Returns true if the
    /// key existed. Removes the entry when no receivers remain.
    fn unsubscribe_from(
        &self,
        map: &DashMap<String, broadcast::Sender<PubMessage>>,
        key: &str,
    ) -> bool {
        if let Some(entry) = map.get(key) {
            self.subscription_count.fetch_sub(1, Ordering::Relaxed);
            if entry.receiver_count() <= 1 {
                drop(entry);
                map.remove(key);
            }
            true
        } else {
            false
        }
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

/// Simple glob matching for pub/sub patterns.
///
/// Supports:
/// - `*` matches any sequence of characters
/// - `?` matches any single character
/// - `[abc]` matches any character in the set
/// - `\x` escapes the next character
///
/// Matching is byte-wise, which is correct for Redis-compatible pub/sub:
/// Redis treats patterns as raw byte sequences, and all metacharacters
/// (`*`, `?`, `[`, `\`) are ASCII so byte comparison is unambiguous.
/// This avoids allocating `Vec<char>` on every match call.
fn glob_match(pattern: &str, input: &str) -> bool {
    glob_match_inner(pattern.as_bytes(), input.as_bytes())
}

/// Inner backtracking glob matcher operating on byte slices.
///
/// The algorithm tracks the last `*` position in both the pattern and
/// input. On a mismatch it rewinds to that checkpoint and advances the
/// input by one byte — standard linear-time glob matching.
fn glob_match_inner(pat: &[u8], inp: &[u8]) -> bool {
    let (mut pi, mut ii) = (0, 0);
    let (mut star_pi, mut star_ii) = (usize::MAX, usize::MAX);

    while ii < inp.len() {
        if pi < pat.len() && pat[pi] == b'\\' && pi + 1 < pat.len() {
            // escaped character — must match literally
            pi += 1;
            if inp[ii] == pat[pi] {
                pi += 1;
                ii += 1;
                continue;
            }
        } else if pi < pat.len() && pat[pi] == b'?' {
            pi += 1;
            ii += 1;
            continue;
        } else if pi < pat.len() && pat[pi] == b'*' {
            star_pi = pi;
            star_ii = ii;
            pi += 1;
            continue;
        } else if pi < pat.len() && pat[pi] == b'[' {
            // character class
            if let Some((matched, end)) = match_char_class(&pat[pi..], inp[ii]) {
                if matched {
                    pi += end;
                    ii += 1;
                    continue;
                }
            }
        } else if pi < pat.len() && pat[pi] == inp[ii] {
            pi += 1;
            ii += 1;
            continue;
        }

        // no match — backtrack to last star if possible
        if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ii += 1;
            ii = star_ii;
            continue;
        }

        return false;
    }

    // consume trailing stars
    while pi < pat.len() && pat[pi] == b'*' {
        pi += 1;
    }

    pi == pat.len()
}

/// Matches a `[...]` character class against a single byte.
/// Returns `(matched, bytes_consumed_from_pat)` when the bracket is valid.
fn match_char_class(pat: &[u8], ch: u8) -> Option<(bool, usize)> {
    if pat.is_empty() || pat[0] != b'[' {
        return None;
    }

    let mut i = 1;
    let negate = if i < pat.len() && pat[i] == b'^' {
        i += 1;
        true
    } else {
        false
    };

    let mut matched = false;
    while i < pat.len() && pat[i] != b']' {
        if i + 2 < pat.len() && pat[i + 1] == b'-' {
            // range: [a-z]
            if ch >= pat[i] && ch <= pat[i + 2] {
                matched = true;
            }
            i += 3;
        } else {
            if ch == pat[i] {
                matched = true;
            }
            i += 1;
        }
    }

    if i < pat.len() && pat[i] == b']' {
        Some((matched ^ negate, i + 1))
    } else {
        None // unterminated bracket
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_exact_match() {
        assert!(glob_match("hello", "hello"));
        assert!(!glob_match("hello", "world"));
    }

    #[test]
    fn glob_star_match() {
        assert!(glob_match("news.*", "news.sports"));
        assert!(glob_match("news.*", "news.weather.today"));
        assert!(!glob_match("news.*", "old.news"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("h*o", "hello"));
        assert!(glob_match("h*o", "ho"));
    }

    #[test]
    fn glob_question_mark() {
        assert!(glob_match("h?llo", "hello"));
        assert!(glob_match("h?llo", "hallo"));
        assert!(!glob_match("h?llo", "hllo"));
    }

    #[test]
    fn glob_char_class() {
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(!glob_match("h[ae]llo", "hillo"));
    }

    #[test]
    fn glob_negated_class() {
        assert!(glob_match("h[^ae]llo", "hillo"));
        assert!(!glob_match("h[^ae]llo", "hello"));
    }

    #[test]
    fn glob_escaped_char() {
        assert!(glob_match("hello\\*", "hello*"));
        assert!(!glob_match("hello\\*", "helloX"));
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
        mgr.unsubscribe("ch");
        drop(rx);

        let count = mgr.publish("ch", Bytes::from("msg"));
        assert_eq!(count, 0);
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
