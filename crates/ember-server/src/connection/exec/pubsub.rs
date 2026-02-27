//! Pub/sub command handlers.

use ember_protocol::Frame;

use super::ExecCtx;

pub(in crate::connection) fn publish(
    channel: String,
    message: bytes::Bytes,
    cx: &ExecCtx<'_>,
) -> Frame {
    let count = cx.pubsub.publish(&channel, message);
    Frame::Integer(count as i64)
}

pub(in crate::connection) fn pubsub_channels(pattern: Option<String>, cx: &ExecCtx<'_>) -> Frame {
    let names = cx.pubsub.channel_names(pattern.as_deref());
    Frame::Array(names.into_iter().map(|n| Frame::Bulk(n.into())).collect())
}

pub(in crate::connection) fn pubsub_numsub(channels: Vec<String>, cx: &ExecCtx<'_>) -> Frame {
    let pairs = cx.pubsub.numsub(&channels);
    let mut frames = Vec::with_capacity(pairs.len() * 2);
    for (ch, count) in pairs {
        frames.push(Frame::Bulk(ch.into()));
        frames.push(Frame::Integer(count as i64));
    }
    Frame::Array(frames)
}

pub(in crate::connection) fn pubsub_numpat(cx: &ExecCtx<'_>) -> Frame {
    Frame::Integer(cx.pubsub.active_patterns() as i64)
}

/// subscribe commands are handled in the connection loop, not here.
/// if we reach this point, something went wrong.
pub(in crate::connection) fn subscribe_error() -> Frame {
    Frame::Error("ERR subscribe commands should not reach execute".into())
}
