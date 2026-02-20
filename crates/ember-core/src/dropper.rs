//! Background value dropper for lazy free.
//!
//! Expensive destructor work (dropping large lists, hashes, sorted sets)
//! is offloaded to a dedicated OS thread so shard loops stay responsive.
//! This is the same strategy Redis uses with its `lazyfree` threads.
//!
//! The dropper runs as a plain `std::thread` rather than a tokio task
//! because dropping data structures is CPU-bound work that would starve
//! the async executor.

use std::sync::mpsc::{self, SyncSender, TrySendError};

use ahash::AHashMap;

use crate::keyspace::Entry;
use crate::memory::is_large_value;
use crate::types::Value;

/// Bounded channel capacity. Large enough to absorb bursts without
/// meaningful memory overhead (~4096 pointers).
const DROP_CHANNEL_CAPACITY: usize = 4096;

/// Items that can be sent to the background drop thread.
///
/// The fields are never explicitly read — the whole point is that the
/// drop thread receives them and lets their destructors run.
#[allow(dead_code)]
enum Droppable {
    /// A single value removed from the keyspace (e.g. DEL, UNLINK, eviction).
    Value(Value),
    /// All entries from a FLUSHDB ASYNC — dropped in bulk.
    Entries(AHashMap<Box<str>, Entry>),
}

/// A cloneable handle for deferring expensive drops to the background thread.
///
/// When all handles are dropped, the background thread's channel closes
/// and it exits cleanly.
#[derive(Debug, Clone)]
pub struct DropHandle {
    tx: SyncSender<Droppable>,
}

impl DropHandle {
    /// Spawns the background drop thread and returns a handle.
    ///
    /// If the thread fails to spawn (resource exhaustion), logs a warning
    /// and returns a handle that drops everything inline. The channel
    /// disconnects immediately since the receiver is never started, and
    /// `try_send` gracefully falls back to inline dropping.
    pub fn spawn() -> Self {
        let (tx, rx) = mpsc::sync_channel::<Droppable>(DROP_CHANNEL_CAPACITY);

        if let Err(e) = std::thread::Builder::new()
            .name("ember-drop".into())
            .spawn(move || {
                // just drain the channel — dropping each item frees the memory
                while rx.recv().is_ok() {}
            })
        {
            tracing::warn!("failed to spawn drop thread, large values will be freed inline: {e}");
        }

        Self { tx }
    }

    /// Defers dropping a value to the background thread if it's large enough
    /// to be worth the channel overhead. Small values are dropped inline.
    ///
    /// If the channel is full, falls back to inline drop — never blocks.
    pub fn defer_value(&self, value: Value) {
        if !is_large_value(&value) {
            return; // small value — inline drop is fine
        }
        // try_send: never block the shard even if the drop thread is behind
        match self.tx.try_send(Droppable::Value(value)) {
            Ok(()) => {}
            Err(TrySendError::Full(item)) => {
                // channel full — drop inline as fallback
                drop(item);
            }
            Err(TrySendError::Disconnected(_)) => {
                // drop thread gone — nothing we can do, value drops here
            }
        }
    }

    /// Defers dropping all entries from a flush operation. Always deferred
    /// since a full keyspace is always worth offloading.
    pub(crate) fn defer_entries(&self, entries: AHashMap<Box<str>, Entry>) {
        if entries.is_empty() {
            return;
        }
        match self.tx.try_send(Droppable::Entries(entries)) {
            Ok(()) => {}
            Err(TrySendError::Full(item)) => {
                drop(item);
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::VecDeque;

    #[test]
    fn defer_small_value_drops_inline() {
        let handle = DropHandle::spawn();
        // small string — should not be sent to channel
        handle.defer_value(Value::String(Bytes::from("hello")));
    }

    #[test]
    fn defer_large_list() {
        let handle = DropHandle::spawn();
        let mut list = VecDeque::new();
        for i in 0..100 {
            list.push_back(Bytes::from(format!("item-{i}")));
        }
        handle.defer_value(Value::List(list));
        // give the drop thread a moment to process
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    #[test]
    fn defer_entries_from_flush() {
        let handle = DropHandle::spawn();
        let mut entries = AHashMap::new();
        for i in 0..10 {
            entries.insert(
                Box::from(format!("key-{i}").as_str()),
                Entry {
                    value: Value::String(Bytes::from(format!("val-{i}"))),
                    expires_at_ms: 0,
                    cached_value_size: 0,
                    last_access_secs: 0,
                },
            );
        }
        handle.defer_entries(entries);
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    #[test]
    fn empty_entries_skipped() {
        let handle = DropHandle::spawn();
        handle.defer_entries(AHashMap::new());
    }
}
