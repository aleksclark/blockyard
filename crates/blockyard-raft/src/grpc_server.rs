//! gRPC server that receives Raft RPCs, consolidated heartbeats, and cluster
//! operations and dispatches them to the local [`MultiRaft`] instance.
//!
//! All three proto services are implemented on one shared struct so they can be
//! composed into a single `tonic::transport::Server` listening on one port.

use std::sync::Arc;

use tonic::{Request, Response, Status};
use tracing::{debug, instrument, warn};

use crate::multiraft::MultiRaft;
use crate::proto;
use crate::types::{RaftRequest, RaftResponse};

// ---------------------------------------------------------------------------
// The shared impl object held by all three service impls
// ---------------------------------------------------------------------------

/// Implements `RaftService`, `HeartbeatService`, and `ClusterService` by
/// delegating to the local [`MultiRaft`] engine.
#[derive(Debug)]
pub struct BlockyardGrpcServer {
    multiraft: Arc<MultiRaft>,
}

impl BlockyardGrpcServer {
    pub fn new(multiraft: Arc<MultiRaft>) -> Self {
        Self { multiraft }
    }

    /// Convenience: start a tonic server on `addr` exposing all three services.
    pub async fn serve(&self, addr: std::net::SocketAddr) -> blockyard_common::Result<()> {
        let raft_svc = proto::raft_service_server::RaftServiceServer::new(RaftServiceImpl {
            multiraft: Arc::clone(&self.multiraft),
        });
        let heartbeat_svc =
            proto::heartbeat_service_server::HeartbeatServiceServer::new(HeartbeatServiceImpl {
                multiraft: Arc::clone(&self.multiraft),
            });
        let cluster_svc =
            proto::cluster_service_server::ClusterServiceServer::new(ClusterServiceImpl {
                multiraft: Arc::clone(&self.multiraft),
            });

        tracing::info!(%addr, "starting gRPC server");

        tonic::transport::Server::builder()
            .add_service(raft_svc)
            .add_service(heartbeat_svc)
            .add_service(cluster_svc)
            .serve(addr)
            .await
            .map_err(|e| blockyard_common::Error::Raft(format!("gRPC serve error: {e}")))
    }

    /// Start serving using an existing `TcpListener` (useful for tests where
    /// you need the OS-assigned port).
    pub async fn serve_with_listener(
        &self,
        listener: tokio::net::TcpListener,
    ) -> blockyard_common::Result<()> {
        let raft_svc = proto::raft_service_server::RaftServiceServer::new(RaftServiceImpl {
            multiraft: Arc::clone(&self.multiraft),
        });
        let heartbeat_svc =
            proto::heartbeat_service_server::HeartbeatServiceServer::new(HeartbeatServiceImpl {
                multiraft: Arc::clone(&self.multiraft),
            });
        let cluster_svc =
            proto::cluster_service_server::ClusterServiceServer::new(ClusterServiceImpl {
                multiraft: Arc::clone(&self.multiraft),
            });

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        tracing::info!("starting gRPC server on pre-bound listener");

        tonic::transport::Server::builder()
            .add_service(raft_svc)
            .add_service(heartbeat_svc)
            .add_service(cluster_svc)
            .serve_with_incoming(incoming)
            .await
            .map_err(|e| blockyard_common::Error::Raft(format!("gRPC serve error: {e}")))
    }
}

// ---------------------------------------------------------------------------
// RaftService impl
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct RaftServiceImpl {
    multiraft: Arc<MultiRaft>,
}

#[tonic::async_trait]
impl proto::raft_service_server::RaftService for RaftServiceImpl {
    #[instrument(skip(self, request), fields(group_id))]
    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> Result<Response<proto::AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("group_id", req.group_id);

        debug!(
            leader_id = req.leader_id,
            term = req.term,
            entries = req.entries.len(),
            "received AppendEntries"
        );

        // Verify the target group exists.
        if !self.multiraft.has_group(req.group_id) {
            return Err(Status::not_found(format!(
                "raft group {} not found",
                req.group_id
            )));
        }

        // Apply each entry to the state machine (simplified: in a real
        // implementation this would go through the Raft log).
        for entry in &req.entries {
            let raft_req: RaftRequest = serde_json::from_slice(&entry.data).map_err(|e| {
                Status::invalid_argument(format!("failed to decode log entry: {e}"))
            })?;

            self.multiraft
                .propose(req.group_id, &raft_req)
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        Ok(Response::new(proto::AppendEntriesResponse {
            term: req.term,
            success: true,
            last_log_index: req.prev_log_index + req.entries.len() as u64,
        }))
    }

    #[instrument(skip(self, request), fields(group_id))]
    async fn install_snapshot(
        &self,
        request: Request<proto::InstallSnapshotRequest>,
    ) -> Result<Response<proto::InstallSnapshotResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("group_id", req.group_id);

        debug!(
            leader_id = req.leader_id,
            term = req.term,
            last_included_index = req.last_included_index,
            snapshot_bytes = req.data.len(),
            "received InstallSnapshot"
        );

        if !self.multiraft.has_group(req.group_id) {
            return Err(Status::not_found(format!(
                "raft group {} not found",
                req.group_id
            )));
        }

        // In a real implementation this restores the state machine from the
        // snapshot. For now we just get the group's state machine and restore.
        let state = self.multiraft.get_state(req.group_id).ok_or_else(|| {
            Status::internal(format!(
                "group {} disappeared during snapshot install",
                req.group_id
            ))
        })?;
        // We can't directly access the state machine through MultiRaft's
        // public API for restore, so we validate the snapshot is parseable.
        let _: crate::state_machine::AppState = serde_json::from_slice(&req.data)
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot data: {e}")))?;

        debug!(
            group_id = req.group_id,
            last_applied = state.applied_index,
            "snapshot accepted"
        );

        Ok(Response::new(proto::InstallSnapshotResponse {
            term: req.term,
        }))
    }

    #[instrument(skip(self, request), fields(group_id))]
    async fn request_vote(
        &self,
        request: Request<proto::VoteRequest>,
    ) -> Result<Response<proto::VoteResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("group_id", req.group_id);

        debug!(
            candidate_id = req.candidate_id,
            term = req.term,
            pre_vote = req.pre_vote,
            "received RequestVote"
        );

        if !self.multiraft.has_group(req.group_id) {
            return Err(Status::not_found(format!(
                "raft group {} not found",
                req.group_id
            )));
        }

        // Simplified vote logic: grant the vote. A real implementation would
        // check term, log completeness, and whether we already voted.
        Ok(Response::new(proto::VoteResponse {
            term: req.term,
            vote_granted: true,
        }))
    }
}

// ---------------------------------------------------------------------------
// HeartbeatService impl
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct HeartbeatServiceImpl {
    multiraft: Arc<MultiRaft>,
}

#[tonic::async_trait]
impl proto::heartbeat_service_server::HeartbeatService for HeartbeatServiceImpl {
    #[instrument(skip(self, request), fields(from_node, groups))]
    async fn consolidated_heartbeat(
        &self,
        request: Request<proto::ConsolidatedHeartbeatRequest>,
    ) -> Result<Response<proto::ConsolidatedHeartbeatResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("from_node", req.from_node);
        tracing::Span::current().record("groups", req.heartbeats.len());

        debug!(
            from = req.from_node,
            to = req.to_node,
            groups = req.heartbeats.len(),
            "received consolidated heartbeat"
        );

        let mut missing_groups = Vec::new();

        for hb in &req.heartbeats {
            if self.multiraft.has_group(hb.group_id) {
                debug!(
                    group_id = hb.group_id,
                    term = hb.term,
                    commit = hb.commit_index,
                    "heartbeat for group"
                );
            } else {
                missing_groups.push(hb.group_id);
            }
        }

        if !missing_groups.is_empty() {
            warn!(
                from = req.from_node,
                missing = ?missing_groups,
                "heartbeat referenced unknown groups"
            );
        }

        Ok(Response::new(proto::ConsolidatedHeartbeatResponse {
            success: true,
            message: String::new(),
        }))
    }
}

// ---------------------------------------------------------------------------
// ClusterService impl
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ClusterServiceImpl {
    multiraft: Arc<MultiRaft>,
}

#[tonic::async_trait]
impl proto::cluster_service_server::ClusterService for ClusterServiceImpl {
    #[instrument(skip(self, request), fields(group_id))]
    async fn forward_proposal(
        &self,
        request: Request<proto::ForwardProposalRequest>,
    ) -> Result<Response<proto::ForwardProposalResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("group_id", req.group_id);

        let raft_req: RaftRequest = serde_json::from_slice(&req.payload).map_err(|e| {
            Status::invalid_argument(format!("failed to decode proposal payload: {e}"))
        })?;

        debug!(
            group_id = req.group_id,
            request = %raft_req,
            "received forwarded proposal"
        );

        match self.multiraft.propose(req.group_id, &raft_req) {
            Ok(raft_resp) => {
                let (success, error, data) = match raft_resp {
                    RaftResponse::Ok => (true, String::new(), Vec::new()),
                    RaftResponse::Error(e) => (false, e, Vec::new()),
                    RaftResponse::Data(d) => (true, String::new(), d),
                };
                Ok(Response::new(proto::ForwardProposalResponse {
                    success,
                    error,
                    data,
                }))
            }
            Err(e) => Ok(Response::new(proto::ForwardProposalResponse {
                success: false,
                error: e.to_string(),
                data: Vec::new(),
            })),
        }
    }

    #[instrument(skip(self, request), fields(group_id))]
    async fn get_state(
        &self,
        request: Request<proto::GetStateRequest>,
    ) -> Result<Response<proto::GetStateResponse>, Status> {
        let req = request.into_inner();
        tracing::Span::current().record("group_id", req.group_id);

        debug!(group_id = req.group_id, "received GetState");

        match self.multiraft.get_state(req.group_id) {
            Some(state) => {
                let state_bytes = serde_json::to_vec(&state)
                    .map_err(|e| Status::internal(format!("failed to serialize state: {e}")))?;
                Ok(Response::new(proto::GetStateResponse {
                    success: true,
                    error: String::new(),
                    state: state_bytes,
                }))
            }
            None => Ok(Response::new(proto::GetStateResponse {
                success: false,
                error: format!("group {} not found", req.group_id),
                state: Vec::new(),
            })),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_server_construction() {
        let mr = Arc::new(MultiRaft::new(1));
        let server = BlockyardGrpcServer::new(mr);
        // Just verify it can be constructed.
        assert!(format!("{:?}", server).contains("BlockyardGrpcServer"));
    }

    #[tokio::test]
    async fn test_raft_service_append_entries_empty() {
        let mr = Arc::new(MultiRaft::new(1));
        mr.create_group(42).unwrap();

        let svc = RaftServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let req = Request::new(proto::AppendEntriesRequest {
            group_id: 42,
            leader_id: 2,
            term: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            entries: vec![],
        });

        let resp = proto::raft_service_server::RaftService::append_entries(&svc, req)
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);
        assert_eq!(inner.term, 1);
        assert_eq!(inner.last_log_index, 0);
    }

    #[tokio::test]
    async fn test_raft_service_append_entries_with_entry() {
        let mr = Arc::new(MultiRaft::new(1));
        mr.create_group(42).unwrap();

        let svc = RaftServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let entry_data = serde_json::to_vec(&RaftRequest::VolumeCreate {
            name: "vol-ae".into(),
            size_bytes: 512,
            replicas: 1,
        })
        .unwrap();

        let req = Request::new(proto::AppendEntriesRequest {
            group_id: 42,
            leader_id: 2,
            term: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 1,
            entries: vec![proto::LogEntry {
                term: 1,
                index: 1,
                data: entry_data,
            }],
        });

        let resp = proto::raft_service_server::RaftService::append_entries(&svc, req)
            .await
            .unwrap();
        assert!(resp.into_inner().success);

        // Verify the entry was applied.
        let state = mr.get_state(42).unwrap();
        assert!(state.volumes.contains_key("vol-ae"));
    }

    #[tokio::test]
    async fn test_raft_service_append_entries_unknown_group() {
        let mr = Arc::new(MultiRaft::new(1));
        let svc = RaftServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let req = Request::new(proto::AppendEntriesRequest {
            group_id: 999,
            leader_id: 2,
            term: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_commit: 0,
            entries: vec![],
        });

        let err = proto::raft_service_server::RaftService::append_entries(&svc, req)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_raft_service_vote() {
        let mr = Arc::new(MultiRaft::new(1));
        mr.create_group(10).unwrap();

        let svc = RaftServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let req = Request::new(proto::VoteRequest {
            group_id: 10,
            candidate_id: 3,
            term: 5,
            last_log_index: 10,
            last_log_term: 4,
            pre_vote: true,
        });

        let resp = proto::raft_service_server::RaftService::request_vote(&svc, req)
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.vote_granted);
        assert_eq!(inner.term, 5);
    }

    #[tokio::test]
    async fn test_raft_service_install_snapshot() {
        let mr = Arc::new(MultiRaft::new(1));
        mr.create_group(10).unwrap();

        let svc = RaftServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let snap = serde_json::to_vec(&crate::state_machine::AppState::default()).unwrap();
        let req = Request::new(proto::InstallSnapshotRequest {
            group_id: 10,
            leader_id: 2,
            term: 3,
            last_included_index: 50,
            last_included_term: 2,
            data: snap,
        });

        let resp = proto::raft_service_server::RaftService::install_snapshot(&svc, req)
            .await
            .unwrap();
        assert_eq!(resp.into_inner().term, 3);
    }

    #[tokio::test]
    async fn test_heartbeat_service() {
        let mr = Arc::new(MultiRaft::new(1));
        mr.create_group(10).unwrap();
        mr.create_group(20).unwrap();

        let svc = HeartbeatServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let req = Request::new(proto::ConsolidatedHeartbeatRequest {
            from_node: 2,
            to_node: 1,
            heartbeats: vec![
                proto::GroupHeartbeat {
                    group_id: 10,
                    term: 1,
                    commit_index: 5,
                },
                proto::GroupHeartbeat {
                    group_id: 20,
                    term: 2,
                    commit_index: 3,
                },
            ],
        });

        let resp =
            proto::heartbeat_service_server::HeartbeatService::consolidated_heartbeat(&svc, req)
                .await
                .unwrap();
        assert!(resp.into_inner().success);
    }

    #[tokio::test]
    async fn test_heartbeat_service_with_missing_group() {
        let mr = Arc::new(MultiRaft::new(1));
        mr.create_group(10).unwrap();
        // group 99 does NOT exist

        let svc = HeartbeatServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let req = Request::new(proto::ConsolidatedHeartbeatRequest {
            from_node: 2,
            to_node: 1,
            heartbeats: vec![
                proto::GroupHeartbeat {
                    group_id: 10,
                    term: 1,
                    commit_index: 5,
                },
                proto::GroupHeartbeat {
                    group_id: 99,
                    term: 1,
                    commit_index: 0,
                },
            ],
        });

        // Should succeed — missing groups are logged but don't fail the RPC.
        let resp =
            proto::heartbeat_service_server::HeartbeatService::consolidated_heartbeat(&svc, req)
                .await
                .unwrap();
        assert!(resp.into_inner().success);
    }

    #[tokio::test]
    async fn test_cluster_service_forward_proposal() {
        let mr = Arc::new(MultiRaft::new(1));
        mr.create_group(100).unwrap();

        let svc = ClusterServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let payload = serde_json::to_vec(&RaftRequest::VolumeCreate {
            name: "forwarded-vol".into(),
            size_bytes: 4096,
            replicas: 2,
        })
        .unwrap();

        let req = Request::new(proto::ForwardProposalRequest {
            group_id: 100,
            payload,
        });

        let resp = proto::cluster_service_server::ClusterService::forward_proposal(&svc, req)
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.success, "error: {}", inner.error);

        let state = mr.get_state(100).unwrap();
        assert!(state.volumes.contains_key("forwarded-vol"));
    }

    #[tokio::test]
    async fn test_cluster_service_forward_proposal_bad_payload() {
        let mr = Arc::new(MultiRaft::new(1));
        mr.create_group(100).unwrap();

        let svc = ClusterServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let req = Request::new(proto::ForwardProposalRequest {
            group_id: 100,
            payload: b"not valid json".to_vec(),
        });

        let err = proto::cluster_service_server::ClusterService::forward_proposal(&svc, req)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_cluster_service_forward_proposal_missing_group() {
        let mr = Arc::new(MultiRaft::new(1));
        // No group created.

        let svc = ClusterServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let payload = serde_json::to_vec(&RaftRequest::VolumeCreate {
            name: "v".into(),
            size_bytes: 1,
            replicas: 1,
        })
        .unwrap();

        let req = Request::new(proto::ForwardProposalRequest {
            group_id: 999,
            payload,
        });

        let resp = proto::cluster_service_server::ClusterService::forward_proposal(&svc, req)
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(!inner.success);
        assert!(inner.error.contains("not found"), "got: {}", inner.error);
    }

    #[tokio::test]
    async fn test_cluster_service_get_state() {
        let mr = Arc::new(MultiRaft::new(1));
        mr.create_group(100).unwrap();
        mr.propose(
            100,
            &RaftRequest::NodeRegister {
                node_id: 7,
                addr: "host:1234".into(),
            },
        )
        .unwrap();

        let svc = ClusterServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let req = Request::new(proto::GetStateRequest { group_id: 100 });
        let resp = proto::cluster_service_server::ClusterService::get_state(&svc, req)
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);

        let state: crate::state_machine::AppState = serde_json::from_slice(&inner.state).unwrap();
        assert!(state.nodes.contains_key(&7));
    }

    #[tokio::test]
    async fn test_cluster_service_get_state_missing_group() {
        let mr = Arc::new(MultiRaft::new(1));

        let svc = ClusterServiceImpl {
            multiraft: Arc::clone(&mr),
        };

        let req = Request::new(proto::GetStateRequest { group_id: 999 });
        let resp = proto::cluster_service_server::ClusterService::get_state(&svc, req)
            .await
            .unwrap();
        let inner = resp.into_inner();
        assert!(!inner.success);
        assert!(inner.error.contains("not found"));
    }
}
