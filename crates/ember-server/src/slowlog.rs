//! Slow command log.
//!
//! Records commands that exceed a configurable latency threshold into
//! a fixed-size ring buffer. The buffer is protected by a `Mutex`,
//! but contention is negligible since only slow commands (rare by
//! definition) ever acquire it.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// A single slow log entry.
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
    /// Monotonically increasing entry id.
    pub id: u64,
    /// Wall clock time when the command was executed.
    pub timestamp: SystemTime,
    /// How long the command took.
    pub duration: Duration,
    /// Truncated command summary (e.g. "SET key value").
    pub command: String,
}

/// Configuration for the slow log.
#[derive(Debug, Clone, Copy)]
pub struct SlowLogConfig {
    /// Commands slower than this are logged. 0 means log everything.
    /// Negative means disabled (matching Redis semantics where -1 disables).
    pub slower_than: Duration,
    /// Maximum number of entries to keep. Oldest are evicted when full.
    pub max_len: usize,
    /// Whether the slow log is enabled at all.
    pub enabled: bool,
}

impl Default for SlowLogConfig {
    fn default() -> Self {
        Self {
            slower_than: Duration::from_micros(10_000), // 10ms, matches Redis
            max_len: 128,
            enabled: true,
        }
    }
}

/// Thread-safe slow command log backed by a ring buffer.
pub struct SlowLog {
    config: SlowLogConfig,
    inner: Mutex<SlowLogInner>,
}

struct SlowLogInner {
    entries: VecDeque<SlowLogEntry>,
    next_id: u64,
}

impl SlowLog {
    /// Creates a new slow log with the given configuration.
    pub fn new(config: SlowLogConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(SlowLogInner {
                entries: VecDeque::with_capacity(config.max_len),
                next_id: 0,
            }),
        }
    }

    /// Returns whether the slow log is enabled.
    ///
    /// Used by the connection handler to skip timing overhead entirely
    /// when both metrics and slowlog are disabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Records a command if it exceeded the threshold.
    ///
    /// Called from the connection handler after each command completes.
    /// The mutex is effectively uncontended since slow commands are rare.
    /// If the lock is poisoned (another thread panicked while holding it),
    /// we recover by clearing the entries rather than propagating the panic.
    pub fn maybe_record(&self, duration: Duration, command: &str) {
        if !self.config.enabled || duration < self.config.slower_than {
            return;
        }

        let mut inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                guard.entries.clear();
                guard
            }
        };

        let id = inner.next_id;
        inner.next_id += 1;

        if inner.entries.len() >= self.config.max_len {
            inner.entries.pop_front();
        }

        inner.entries.push_back(SlowLogEntry {
            id,
            timestamp: SystemTime::now(),
            duration,
            command: command.to_owned(),
        });
    }

    /// Returns the most recent entries, newest first.
    ///
    /// If `count` is `None`, returns all entries.
    pub fn get(&self, count: Option<usize>) -> Vec<SlowLogEntry> {
        let inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let n = count.unwrap_or(inner.entries.len()).min(inner.entries.len());
        inner.entries.iter().rev().take(n).cloned().collect()
    }

    /// Returns the number of entries currently in the log.
    pub fn len(&self) -> usize {
        match self.inner.lock() {
            Ok(guard) => guard.entries.len(),
            Err(poisoned) => poisoned.into_inner().entries.len(),
        }
    }

    /// Clears all entries from the log.
    pub fn reset(&self) {
        let mut inner = match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        inner.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_slow_command() {
        let log = SlowLog::new(SlowLogConfig {
            slower_than: Duration::from_millis(1),
            max_len: 10,
            enabled: true,
        });

        // fast command — should not be recorded
        log.maybe_record(Duration::from_micros(500), "GET fast");
        assert_eq!(log.len(), 0);

        // slow command — should be recorded
        log.maybe_record(Duration::from_millis(5), "SET slow value");
        assert_eq!(log.len(), 1);

        let entries = log.get(None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].command, "SET slow value");
        assert_eq!(entries[0].id, 0);
    }

    #[test]
    fn ring_buffer_evicts_oldest() {
        let log = SlowLog::new(SlowLogConfig {
            slower_than: Duration::ZERO,
            max_len: 3,
            enabled: true,
        });

        for i in 0..5 {
            log.maybe_record(Duration::from_millis(1), &format!("CMD {i}"));
        }

        assert_eq!(log.len(), 3);
        let entries = log.get(None);
        // newest first
        assert_eq!(entries[0].command, "CMD 4");
        assert_eq!(entries[1].command, "CMD 3");
        assert_eq!(entries[2].command, "CMD 2");
    }

    #[test]
    fn get_with_count() {
        let log = SlowLog::new(SlowLogConfig {
            slower_than: Duration::ZERO,
            max_len: 10,
            enabled: true,
        });

        for i in 0..5 {
            log.maybe_record(Duration::from_millis(1), &format!("CMD {i}"));
        }

        let entries = log.get(Some(2));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].command, "CMD 4");
        assert_eq!(entries[1].command, "CMD 3");
    }

    #[test]
    fn reset_clears_entries() {
        let log = SlowLog::new(SlowLogConfig {
            slower_than: Duration::ZERO,
            max_len: 10,
            enabled: true,
        });

        log.maybe_record(Duration::from_millis(1), "CMD 1");
        assert_eq!(log.len(), 1);

        log.reset();
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn disabled_log_records_nothing() {
        let log = SlowLog::new(SlowLogConfig {
            slower_than: Duration::ZERO,
            max_len: 10,
            enabled: false,
        });

        log.maybe_record(Duration::from_millis(100), "SLOW CMD");
        assert_eq!(log.len(), 0);
    }
}
