//! TCP framing for Raft RPC messages.
//!
//! Length-prefixed framing: a 4-byte big-endian u32 length field followed by
//! a bincode payload. Bincode is 2-5× smaller and 5-10× faster to encode than
//! JSON for the numeric-heavy Raft message types. Used by `RaftNetworkClient`
//! to send RPCs and by `spawn_raft_listener` to receive them.
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

/// Writes a length-prefixed bincode frame to `w`.
pub(crate) async fn write_frame<W, T>(w: &mut W, msg: &T) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let data =
        bincode::serialize(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len = data.len() as u32;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(&data).await?;
    Ok(())
}

/// Reads a length-prefixed bincode frame from `r`, rejecting oversized frames.
pub(crate) async fn read_frame<R, T>(r: &mut R) -> io::Result<T>
where
    R: AsyncReadExt + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_RAFT_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("raft frame size {len} exceeds limit {MAX_RAFT_FRAME_SIZE}"),
        ));
    }
    let mut data = vec![0u8; len];
    r.read_exact(&mut data).await?;
    bincode::deserialize(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Writes a length-prefixed frame with an appended HMAC-SHA256 tag.
///
/// Wire format: `[4-byte len][bincode payload][32-byte HMAC tag]`
/// where `len = bincode_len + 32`.
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
        bincode::serialize(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
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
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_RAFT_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("raft frame size {len} exceeds limit {MAX_RAFT_FRAME_SIZE}"),
        ));
    }
    if len < TAG_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raft frame too short for auth tag",
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;

    let (payload, tag) = buf.split_at(len - TAG_LEN);
    if !secret.verify(payload, tag) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "raft auth failed",
        ));
    }
    bincode::deserialize(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
