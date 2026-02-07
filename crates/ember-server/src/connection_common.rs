//! Shared constants and utilities for connection handlers.
//!
//! Both the sharded connection handler (`connection.rs`) and concurrent
//! handler (`concurrent_handler.rs`) use these constants to ensure
//! consistent behavior across execution modes.

use std::time::Duration;

/// Initial read buffer capacity. 4KB covers most commands comfortably
/// without over-allocating for simple PING/SET/GET workloads.
pub const BUF_CAPACITY: usize = 4096;

/// Maximum read buffer size before we disconnect the client. Prevents
/// a single slow or malicious client from consuming unbounded memory
/// with incomplete frames. Set to 64MB to allow very large pipelined
/// batches while still protecting against runaway growth.
pub const MAX_BUF_SIZE: usize = 64 * 1024 * 1024;

/// How long a connection can be idle (no data received) before we
/// close it. Prevents abandoned connections from leaking resources.
/// 5 minutes matches Redis default behavior.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(300);
