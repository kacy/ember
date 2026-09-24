//! TCP framing for Raft RPC messages.
//!
//! Length-prefixed framing: a 4-byte big-endian u32 length field followed by
//! a postcard payload. Postcard is compact, no-std friendly, and purpose-built
//! for serialization — well suited to the numeric-heavy Raft message types.
//! Used by `RaftNetworkClient` to send RPCs and by `spawn_raft_listener` to
//! receive them.
//!
//! When a [`ClusterSecret`] is configured, an HMAC-SHA256 tag is appended after
//! the payload (inside the length-delimited frame). The receiver verifies the
//! tag before deserializing.

use std::io;

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::auth::{ClusterSecret, TAG_LEN};
use crate::raft::TypeConfig;

/// Maximum Raft frame size (10 MB). Raft snapshots can be large, but anything
/// beyond this is almost certainly a bug or an attack. The previous 64 MB limit
/// allowed a single unauthenticated frame to force a 64 MB heap allocation;
/// 10 MB is generous for JSON-encoded Raft RPCs while limiting the blast radius.
pub(crate) const MAX_RAFT_FRAME_SIZE: usize = 10 * 1024 * 1024;

/// An inbound Raft RPC message.
#[derive(Serialize, Deserialize)]
pub(crate) enum RaftRpc {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<u64>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

/// A Raft RPC response.
#[derive(Serialize, Deserialize)]
pub(crate) enum RaftRpcResponse {
    AppendEntries(AppendEntriesResponse<u64>),
    Vote(VoteResponse<u64>),
    InstallSnapshot(InstallSnapshotResponse<u64>),
}

/// Writes a length-prefixed postcard frame to `w`.
pub(crate) async fn write_frame<W, T>(w: &mut W, msg: &T) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let data =
        postcard::to_allocvec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len = data.len() as u32;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(&data).await?;
    Ok(())
}

/// Reads a length-prefixed postcard frame from `r`, rejecting oversized frames.
pub(crate) async fn read_frame<R, T>(r: &mut R) -> io::Result<T>
where
    R: AsyncReadExt + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let data = read_body(r).await?;
    postcard::from_bytes(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Reads a frame's length prefix and body. The buffer grows as bytes
/// arrive instead of being sized from the prefix, so a peer that claims a
/// large frame and sends nothing costs no memory.
async fn read_body<R>(r: &mut R) -> io::Result<Vec<u8>>
where
    R: AsyncReadExt + Unpin,
{
    let len = r.read_u32().await? as usize;
    if len > MAX_RAFT_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("raft frame size {len} exceeds limit {MAX_RAFT_FRAME_SIZE}"),
        ));
    }
    let mut body = Vec::new();
    r.take(len as u64).read_to_end(&mut body).await?;
    if body.len() < len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "raft frame ended early",
        ));
    }
    Ok(body)
}

/// Writes a length-prefixed frame with an appended HMAC-SHA256 tag.
///
/// Wire format: `[4-byte len][postcard payload][32-byte HMAC tag]`
/// where `len = postcard_len + 32`.
pub(crate) async fn write_frame_authenticated<W, T>(
    w: &mut W,
    msg: &T,
    secret: &ClusterSecret,
) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let data =
        postcard::to_allocvec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let tag = secret.sign(&data);
    let total_len = (data.len() + TAG_LEN) as u32;
    w.write_all(&total_len.to_be_bytes()).await?;
    w.write_all(&data).await?;
    w.write_all(&tag).await?;
    Ok(())
}

/// Reads a length-prefixed frame, verifying the trailing HMAC-SHA256 tag.
pub(crate) async fn read_frame_authenticated<R, T>(
    r: &mut R,
    secret: &ClusterSecret,
) -> io::Result<T>
where
    R: AsyncReadExt + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let buf = read_body(r).await?;
    let Some(payload_len) = buf.len().checked_sub(TAG_LEN) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raft frame too short for auth tag",
        ));
    };
    let (payload, tag) = buf.split_at(payload_len);
    if !secret.verify(payload, tag) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "raft auth failed",
        ));
    }
    postcard::from_bytes(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frame_round_trips() {
        let mut buf = Vec::new();
        write_frame(&mut buf, &(7u64, "vote".to_string()))
            .await
            .unwrap();
        let got: (u64, String) = read_frame(&mut buf.as_slice()).await.unwrap();
        assert_eq!(got, (7, "vote".to_string()));
    }

    #[tokio::test]
    async fn frame_shorter_than_its_length_fails() {
        // claims the largest allowed frame, then sends three bytes
        let mut buf = (MAX_RAFT_FRAME_SIZE as u32).to_be_bytes().to_vec();
        buf.extend_from_slice(b"abc");
        let err = read_frame::<_, u64>(&mut buf.as_slice()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn oversized_frame_is_rejected() {
        let buf = (MAX_RAFT_FRAME_SIZE as u32 + 1).to_be_bytes();
        let err = read_frame::<_, u64>(&mut buf.as_slice()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
