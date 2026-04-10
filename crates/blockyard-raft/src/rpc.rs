//! Raft RPC message types for TCP transport.
//!
//! Wraps all openraft RPC types into a single [`RaftRpc`] enum for
//! length-prefixed framing over TCP. Each variant carries a serde-serializable
//! request; the corresponding [`RaftRpcResponse`] carries the response.

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

use crate::typ::TypeConfig;

type NodeId = u64;

/// A Raft RPC request sent over the wire.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRpc {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

/// A Raft RPC response sent over the wire.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRpcResponse {
    AppendEntries(AppendEntriesResponse<NodeId>),
    Vote(VoteResponse<NodeId>),
    InstallSnapshot(InstallSnapshotResponse<NodeId>),
}

/// Write a length-prefixed JSON frame to an async writer.
pub async fn write_frame<W, T>(writer: &mut W, msg: &T) -> std::io::Result<()>
where
    W: tokio::io::AsyncWriteExt + Unpin,
    T: Serialize,
{
    let payload = serde_json::to_vec(msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let len = payload.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

/// Read a length-prefixed JSON frame from an async reader.
pub async fn read_frame<R, T>(reader: &mut R) -> std::io::Result<T>
where
    R: tokio::io::AsyncReadExt + Unpin,
    T: serde::de::DeserializeOwned,
{
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    if len > 64 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame too large: {len} bytes"),
        ));
    }

    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_rpc_vote_roundtrip_serde() {
        let vote = openraft::Vote::new(1, 2);
        let req = VoteRequest::<u64> {
            vote,
            last_log_id: None,
        };
        let rpc = RaftRpc::Vote(req);
        let json = serde_json::to_vec(&rpc).unwrap();
        let decoded: RaftRpc = serde_json::from_slice(&json).unwrap();
        match decoded {
            RaftRpc::Vote(v) => {
                assert_eq!(v.vote, vote);
                assert!(v.last_log_id.is_none());
            }
            _ => panic!("expected Vote variant"),
        }
    }

    #[test]
    fn test_rpc_response_vote_roundtrip_serde() {
        let vote = openraft::Vote::new(1, 2);
        let resp = VoteResponse::<u64> {
            vote,
            vote_granted: true,
            last_log_id: None,
        };
        let rpc_resp = RaftRpcResponse::Vote(resp);
        let json = serde_json::to_vec(&rpc_resp).unwrap();
        let decoded: RaftRpcResponse = serde_json::from_slice(&json).unwrap();
        match decoded {
            RaftRpcResponse::Vote(v) => {
                assert!(v.vote_granted);
            }
            _ => panic!("expected Vote variant"),
        }
    }

    #[test]
    fn test_rpc_append_entries_roundtrip_serde() {
        let vote = openraft::Vote::new(1, 1);
        let req = AppendEntriesRequest::<TypeConfig> {
            vote,
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        };
        let rpc = RaftRpc::AppendEntries(req);
        let json = serde_json::to_vec(&rpc).unwrap();
        let decoded: RaftRpc = serde_json::from_slice(&json).unwrap();
        match decoded {
            RaftRpc::AppendEntries(a) => {
                assert_eq!(a.vote, vote);
                assert!(a.entries.is_empty());
            }
            _ => panic!("expected AppendEntries variant"),
        }
    }

    #[test]
    fn test_rpc_install_snapshot_roundtrip_serde() {
        let vote = openraft::Vote::new(1, 1);
        let meta = openraft::SnapshotMeta {
            last_log_id: None,
            last_membership: openraft::StoredMembership::new(
                None,
                openraft::Membership::new(
                    vec![],
                    std::collections::BTreeMap::<u64, openraft::BasicNode>::new(),
                ),
            ),
            snapshot_id: "snap-1".into(),
        };
        let req = InstallSnapshotRequest::<TypeConfig> {
            vote,
            meta,
            offset: 0,
            data: vec![1, 2, 3],
            done: true,
        };
        let rpc = RaftRpc::InstallSnapshot(req);
        let json = serde_json::to_vec(&rpc).unwrap();
        let decoded: RaftRpc = serde_json::from_slice(&json).unwrap();
        match decoded {
            RaftRpc::InstallSnapshot(s) => {
                assert_eq!(s.data, vec![1, 2, 3]);
                assert!(s.done);
            }
            _ => panic!("expected InstallSnapshot variant"),
        }
    }

    #[tokio::test]
    async fn test_write_read_frame_roundtrip() {
        let vote = openraft::Vote::new(1, 2);
        let req = VoteRequest::<u64> {
            vote,
            last_log_id: None,
        };
        let rpc = RaftRpc::Vote(req);

        let mut buf = Vec::new();
        write_frame(&mut buf, &rpc).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded: RaftRpc = read_frame(&mut cursor).await.unwrap();
        match decoded {
            RaftRpc::Vote(v) => {
                assert_eq!(v.vote, vote);
            }
            _ => panic!("expected Vote variant"),
        }
    }

    #[tokio::test]
    async fn test_read_frame_too_large() {
        let len: u32 = 128 * 1024 * 1024;
        let mut data = len.to_be_bytes().to_vec();
        data.extend_from_slice(&[0u8; 10]);
        let mut cursor = Cursor::new(data);
        let result: std::io::Result<RaftRpc> = read_frame(&mut cursor).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_read_frame_truncated_length() {
        let mut cursor = Cursor::new(vec![0u8; 2]);
        let result: std::io::Result<RaftRpc> = read_frame(&mut cursor).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_frame_truncated_payload() {
        let mut data = 100u32.to_be_bytes().to_vec();
        data.extend_from_slice(&[0u8; 10]);
        let mut cursor = Cursor::new(data);
        let result: std::io::Result<RaftRpc> = read_frame(&mut cursor).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_frame_invalid_json() {
        let garbage = b"not valid json at all";
        let len = garbage.len() as u32;
        let mut data = len.to_be_bytes().to_vec();
        data.extend_from_slice(garbage);
        let mut cursor = Cursor::new(data);
        let result: std::io::Result<RaftRpc> = read_frame(&mut cursor).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_write_frame_multiple() {
        let vote1 = openraft::Vote::new(1, 1);
        let vote2 = openraft::Vote::new(2, 2);
        let rpc1 = RaftRpc::Vote(VoteRequest {
            vote: vote1,
            last_log_id: None,
        });
        let rpc2 = RaftRpc::Vote(VoteRequest {
            vote: vote2,
            last_log_id: None,
        });

        let mut buf = Vec::new();
        write_frame(&mut buf, &rpc1).await.unwrap();
        write_frame(&mut buf, &rpc2).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let d1: RaftRpc = read_frame(&mut cursor).await.unwrap();
        let d2: RaftRpc = read_frame(&mut cursor).await.unwrap();
        match (d1, d2) {
            (RaftRpc::Vote(v1), RaftRpc::Vote(v2)) => {
                assert_eq!(v1.vote, vote1);
                assert_eq!(v2.vote, vote2);
            }
            _ => panic!("expected Vote variants"),
        }
    }

    #[test]
    fn test_rpc_response_append_entries_serde() {
        let resp = RaftRpcResponse::AppendEntries(AppendEntriesResponse::Success);
        let json = serde_json::to_vec(&resp).unwrap();
        let decoded: RaftRpcResponse = serde_json::from_slice(&json).unwrap();
        match decoded {
            RaftRpcResponse::AppendEntries(AppendEntriesResponse::Success) => {}
            _ => panic!("expected AppendEntries Success"),
        }
    }

    #[test]
    fn test_rpc_response_install_snapshot_serde() {
        let vote = openraft::Vote::new(1, 1);
        let resp = RaftRpcResponse::InstallSnapshot(InstallSnapshotResponse { vote });
        let json = serde_json::to_vec(&resp).unwrap();
        let decoded: RaftRpcResponse = serde_json::from_slice(&json).unwrap();
        match decoded {
            RaftRpcResponse::InstallSnapshot(s) => {
                assert_eq!(s.vote, vote);
            }
            _ => panic!("expected InstallSnapshot variant"),
        }
    }
}
