//! TCP framing for Raft RPC messages.
//!
//! Length-prefixed framing: a 4-byte big-endian u32 length field followed by
//! a serde_json payload. Used by `RaftNetworkClient` to send RPCs and by
//! `spawn_raft_listener` to receive them.

use std::io;

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::raft::TypeConfig;

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

/// Writes a length-prefixed JSON frame to `w`.
pub(crate) async fn write_frame<W, T>(w: &mut W, msg: &T) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
    T: Serialize,
{
    let data =
        serde_json::to_vec(msg).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len = data.len() as u32;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(&data).await?;
    Ok(())
}

/// Reads a length-prefixed JSON frame from `r`.
pub(crate) async fn read_frame<R, T>(r: &mut R) -> io::Result<T>
where
    R: AsyncReadExt + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut data = vec![0u8; len];
    r.read_exact(&mut data).await?;
    serde_json::from_slice(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
