//! Vector command handlers.
//!
//! The vector commands are routed to their shard in `route`. This module
//! only holds the reply for builds without the `vector` feature.

/// Returns an error when vector support is not compiled in.
#[cfg(not(feature = "vector"))]
pub(in crate::connection) fn not_compiled() -> ember_protocol::Frame {
    ember_protocol::Frame::Error("ERR unknown command (vector support not compiled)".into())
}
