//! Management REST API — axum-based HTTP server for cluster operations.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use blockyard_common::{NodeId, ProtectionPolicy, VolumeId};
use blockyard_raft::{MetadataService, PeerRegistry};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::node::{JoinRequest, JoinResponse};

#[derive(Clone)]
struct AppState {
    metadata: MetadataService,
    peer_registry: PeerRegistry,
    #[allow(dead_code)]
    node_id: NodeId,
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateVolumeRequest {
    name: String,
    size_bytes: u64,
    #[serde(default = "default_protection")]
    protection: ProtectionPolicy,
}

fn default_protection() -> ProtectionPolicy {
    ProtectionPolicy::Replicated { replicas: 3 }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum VolumeState {
    Healthy,
    Degraded,
    Rebuilding,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VolumeInfo {
    id: VolumeId,
    name: String,
    size_bytes: u64,
    protection: ProtectionPolicy,
    state: VolumeState,
    replica_nodes: Vec<NodeId>,
    created_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeInfo {
    id: NodeId,
    address: String,
    state: String,
    disk_count: u32,
    volume_count: u32,
    uptime_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClusterStatus {
    node_count: u32,
    nodes_online: u32,
    volume_count: u32,
    disk_count: u32,
    placement_epoch: u64,
    quorum_health: String,
    total_capacity_bytes: u64,
    used_capacity_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiskInfo {
    id: String,
    node_id: NodeId,
    path: String,
    state: String,
    total_bytes: u64,
    used_bytes: u64,
    extent_count: u64,
    error_count: u64,
}

#[derive(Debug, Serialize)]
struct ApiError {
    error: String,
}

pub async fn start_management_api(
    addr: SocketAddr,
    metadata: MetadataService,
    peer_registry: PeerRegistry,
    node_id: NodeId,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let state = AppState {
        metadata,
        peer_registry,
        node_id,
    };

    let app = Router::new()
        .route("/api/v1/volumes", post(create_volume))
        .route("/api/v1/volumes", get(list_volumes))
        .route("/api/v1/volumes/{id}", get(inspect_volume))
        .route("/api/v1/volumes/{id}", delete(delete_volume))
        .route("/api/v1/nodes", get(list_nodes))
        .route("/api/v1/nodes/{id}", get(inspect_node))
        .route("/api/v1/cluster/status", get(cluster_status))
        .route("/api/v1/cluster/join", post(cluster_join))
        .route("/api/v1/disks", get(list_disks))
        .route("/api/v1/extent-mappings", post(commit_extent_mapping))
        .route("/api/v1/operations/{id}", get(lookup_operation))
        .route("/api/v1/leases/acquire", post(acquire_lease))
        .route("/api/v1/leases/renew", post(renew_lease))
        .route("/api/v1/leases/release", post(release_lease))
        .with_state(Arc::new(state));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "management API listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await?;

    Ok(())
}

async fn create_volume(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateVolumeRequest>,
) -> impl IntoResponse {
    let volume_id = VolumeId::generate();
    match state
        .metadata
        .create_volume(volume_id, req.size_bytes, req.protection)
        .await
    {
        Ok(()) => {
            let info = VolumeInfo {
                id: volume_id,
                name: req.name,
                size_bytes: req.size_bytes,
                protection: req.protection,
                state: VolumeState::Healthy,
                replica_nodes: vec![],
                created_at: Utc::now(),
            };
            (
                StatusCode::CREATED,
                Json(serde_json::to_value(&info).unwrap()),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(
                serde_json::to_value(&ApiError {
                    error: e.to_string(),
                })
                .unwrap(),
            ),
        )
            .into_response(),
    }
}

async fn delete_volume(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let volume_id: VolumeId = match id.parse() {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid volume ID"})),
            )
                .into_response();
        }
    };

    match state.metadata.delete_volume(volume_id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"deleted": volume_id.to_string()})),
        )
            .into_response(),
        Err(e) => {
            let status = if e.to_string().contains("not found") {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (status, Json(serde_json::json!({"error": e.to_string()}))).into_response()
        }
    }
}

async fn list_volumes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let volumes = state.metadata.list_volumes();
    let infos: Vec<VolumeInfo> = volumes
        .into_iter()
        .map(|v| VolumeInfo {
            id: v.volume_id,
            name: String::new(),
            size_bytes: v.size_bytes,
            protection: v.protection,
            state: VolumeState::Healthy,
            replica_nodes: vec![],
            created_at: Utc::now(),
        })
        .collect();
    Json(serde_json::to_value(&infos).unwrap())
}

async fn inspect_volume(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let volume_id: VolumeId = match id.parse() {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid volume ID"})),
            )
                .into_response();
        }
    };

    match state.metadata.get_volume(&volume_id) {
        Some(v) => {
            let info = VolumeInfo {
                id: v.volume_id,
                name: String::new(),
                size_bytes: v.size_bytes,
                protection: v.protection,
                state: VolumeState::Healthy,
                replica_nodes: vec![],
                created_at: Utc::now(),
            };
            Json(serde_json::to_value(&info).unwrap()).into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("volume {} not found", volume_id)})),
        )
            .into_response(),
    }
}

async fn list_nodes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let nodes = state.metadata.list_nodes();
    let infos: Vec<NodeInfo> = nodes
        .into_iter()
        .map(|n| NodeInfo {
            id: n.node_id,
            address: n.addr.clone(),
            state: "online".into(),
            disk_count: 0,
            volume_count: 0,
            uptime_seconds: 0,
        })
        .collect();
    Json(serde_json::to_value(&infos).unwrap())
}

async fn inspect_node(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let node_id: NodeId = match id.parse() {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid node ID"})),
            )
                .into_response();
        }
    };

    match state.metadata.get_node(&node_id) {
        Some(n) => {
            let info = NodeInfo {
                id: n.node_id,
                address: n.addr.clone(),
                state: "online".into(),
                disk_count: 0,
                volume_count: 0,
                uptime_seconds: 0,
            };
            Json(serde_json::to_value(&info).unwrap()).into_response()
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": format!("node {} not found", node_id)})),
        )
            .into_response(),
    }
}

async fn cluster_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let nodes = state.metadata.list_nodes();
    let volumes = state.metadata.list_volumes();
    let epoch = state.metadata.current_epoch();

    let status = ClusterStatus {
        node_count: nodes.len() as u32,
        nodes_online: nodes.len() as u32,
        volume_count: volumes.len() as u32,
        disk_count: 0,
        placement_epoch: epoch.as_u64(),
        quorum_health: "healthy".into(),
        total_capacity_bytes: 0,
        used_capacity_bytes: 0,
    };
    Json(serde_json::to_value(&status).unwrap())
}

async fn list_disks(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    let disks: Vec<DiskInfo> = vec![];
    Json(serde_json::to_value(&disks).unwrap())
}

/// POST /api/v1/cluster/join
///
/// Allows a new node to join the cluster. The leader registers the node
/// in the state machine (assigning a raft u64 ID), then adds it to
/// the Raft membership, registers it in the PeerRegistry for immediate
/// replication, and returns the assigned raft_id plus existing peers.
async fn cluster_join(
    State(state): State<Arc<AppState>>,
    Json(req): Json<JoinRequest>,
) -> impl IntoResponse {
    use openraft::BasicNode;
    use std::collections::BTreeSet;

    // Step 1: Register node in state machine (get raft_id)
    let raft_id = match state
        .metadata
        .register_node(req.node_id, req.raft_addr.clone())
        .await
    {
        Ok(id) => id,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("failed to register node: {e}")})),
            )
                .into_response();
        }
    };

    // Step 2: Register the new node in PeerRegistry immediately so the
    // leader can send AppendEntries without waiting for gossip.
    if let Ok(addr) = req.raft_addr.parse::<std::net::SocketAddr>() {
        state.peer_registry.register(raft_id, addr);
        tracing::info!(raft_id, raft_addr = %addr, "registered new peer in PeerRegistry via join");
    }

    // Step 3: Add node to raft membership
    let raft = state.metadata.raft();
    let metrics = raft.metrics().borrow().clone();

    let mut new_members = BTreeSet::new();
    if let Some(membership) = &metrics
        .membership_config
        .membership()
        .get_joint_config()
        .first()
    {
        for &node_id in *membership {
            new_members.insert(node_id);
        }
    }
    new_members.insert(raft_id);

    // First, add the node as a learner
    if let Err(e) = raft.add_learner(raft_id, BasicNode::default(), true).await {
        tracing::warn!(error = %e, raft_id, "failed to add learner (may already be a member)");
    }

    // Then try to change membership to include the new node
    if let Err(e) = raft.change_membership(new_members, false).await {
        tracing::warn!(error = %e, raft_id, "failed to change membership (may already be a member)");
        // Not fatal — node might already be in membership
    }

    // Step 4: Build the peer map for the response so the joining node
    // can pre-populate its own PeerRegistry immediately.
    let mut peers = std::collections::HashMap::new();
    {
        let sm_data = state.metadata.sm_data();
        let data = sm_data.read();
        for node_entry in data.nodes.values() {
            if let Some(&peer_raft_id) = data.node_raft_map.get(&node_entry.node_id) {
                peers.insert(peer_raft_id, node_entry.addr.clone());
            }
        }
    }

    let resp = JoinResponse { raft_id, peers };
    (StatusCode::OK, Json(serde_json::to_value(&resp).unwrap())).into_response()
}

// --- Data plane wiring API endpoints ---

/// Request body for committing extent mappings.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExtentMappingRequest {
    volume_id: VolumeId,
    block_range_start: u64,
    block_range_end: u64,
    extent_id: blockyard_common::ExtentId,
    extent_version: u64,
    epoch: u64,
    replica_locations: Vec<NodeId>,
    checksums: Vec<Vec<u8>>,
    operation_id: Option<blockyard_common::OperationId>,
    previous_version: Option<u64>,
}

/// Response from extent mapping commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExtentMappingResponse {
    epoch: u64,
}

/// Response from operation lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperationLookupResponse {
    found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    extent_id: Option<blockyard_common::ExtentId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    extent_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_range_start: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_range_end: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    replica_locations: Option<Vec<NodeId>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksums: Option<Vec<Vec<u8>>>,
}

/// Request body for lease acquire/renew.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseActionRequest {
    volume_id: VolumeId,
    session_id: blockyard_common::SessionId,
    now_ms: u64,
    ttl_ms: u64,
}

/// Request body for lease release.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseReleaseRequest {
    volume_id: VolumeId,
    session_id: blockyard_common::SessionId,
}

/// POST /api/v1/extent-mappings
///
/// Commit an extent mapping through Raft consensus.
async fn commit_extent_mapping(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ExtentMappingRequest>,
) -> impl IntoResponse {
    match state
        .metadata
        .commit_extent_mapping(
            req.volume_id,
            req.block_range_start..req.block_range_end,
            req.extent_id,
            req.extent_version,
            blockyard_common::EpochId::new(req.epoch),
            req.replica_locations,
            req.checksums,
            req.operation_id,
            req.previous_version,
        )
        .await
    {
        Ok(epoch) => {
            let resp = ExtentMappingResponse {
                epoch: epoch.as_u64(),
            };
            (StatusCode::OK, Json(serde_json::to_value(&resp).unwrap())).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// GET /api/v1/operations/{id}
///
/// Look up a committed operation by its OperationId.
async fn lookup_operation(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let operation_id: blockyard_common::OperationId = match id.parse() {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "invalid operation ID"})),
            )
                .into_response();
        }
    };

    match state.metadata.lookup_by_operation_id(&operation_id) {
        Some(mapping) => {
            let resp = OperationLookupResponse {
                found: true,
                extent_id: Some(mapping.extent_id),
                extent_version: Some(mapping.extent_version),
                epoch: Some(mapping.epoch.as_u64()),
                block_range_start: Some(mapping.block_range.start),
                block_range_end: Some(mapping.block_range.end),
                replica_locations: Some(mapping.replica_locations.clone()),
                checksums: Some(mapping.checksums.clone()),
            };
            Json(serde_json::to_value(&resp).unwrap()).into_response()
        }
        None => {
            let resp = OperationLookupResponse {
                found: false,
                extent_id: None,
                extent_version: None,
                epoch: None,
                block_range_start: None,
                block_range_end: None,
                replica_locations: None,
                checksums: None,
            };
            Json(serde_json::to_value(&resp).unwrap()).into_response()
        }
    }
}

/// POST /api/v1/leases/acquire
///
/// Acquire a volume write lease.
async fn acquire_lease(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LeaseActionRequest>,
) -> impl IntoResponse {
    match state
        .metadata
        .acquire_lease(req.volume_id, req.session_id, req.now_ms, req.ttl_ms)
        .await
    {
        Ok(resp) => Json(serde_json::to_value(&resp).unwrap()).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /api/v1/leases/renew
///
/// Renew a volume write lease.
async fn renew_lease(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LeaseActionRequest>,
) -> impl IntoResponse {
    match state
        .metadata
        .renew_lease(req.volume_id, req.session_id, req.now_ms, req.ttl_ms)
        .await
    {
        Ok(resp) => Json(serde_json::to_value(&resp).unwrap()).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /api/v1/leases/release
///
/// Release a volume write lease.
async fn release_lease(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LeaseReleaseRequest>,
) -> impl IntoResponse {
    match state
        .metadata
        .release_lease(req.volume_id, req.session_id)
        .await
    {
        Ok(resp) => Json(serde_json::to_value(&resp).unwrap()).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_protection() {
        let p = default_protection();
        assert_eq!(p, ProtectionPolicy::Replicated { replicas: 3 });
    }

    #[test]
    fn test_create_volume_request_serde() {
        let req = CreateVolumeRequest {
            name: "test".into(),
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: CreateVolumeRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "test");
        assert_eq!(parsed.size_bytes, 1024);
    }

    #[test]
    fn test_create_volume_request_default_protection() {
        let json = r#"{"name":"test","size_bytes":1024}"#;
        let parsed: CreateVolumeRequest = serde_json::from_str(json).unwrap();
        assert_eq!(
            parsed.protection,
            ProtectionPolicy::Replicated { replicas: 3 }
        );
    }

    #[test]
    fn test_volume_info_serde() {
        let info = VolumeInfo {
            id: VolumeId::generate(),
            name: "vol".into(),
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            state: VolumeState::Healthy,
            replica_nodes: vec![],
            created_at: Utc::now(),
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("vol"));
    }

    #[test]
    fn test_node_info_serde() {
        let info = NodeInfo {
            id: NodeId::generate(),
            address: "10.0.0.1:9800".into(),
            state: "online".into(),
            disk_count: 4,
            volume_count: 10,
            uptime_seconds: 86400,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("10.0.0.1:9800"));
    }

    #[test]
    fn test_cluster_status_serde() {
        let status = ClusterStatus {
            node_count: 3,
            nodes_online: 3,
            volume_count: 1,
            disk_count: 6,
            placement_epoch: 1,
            quorum_health: "healthy".into(),
            total_capacity_bytes: 1000,
            used_capacity_bytes: 500,
        };
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("healthy"));
    }

    #[test]
    fn test_api_error_serde() {
        let err = ApiError {
            error: "test error".into(),
        };
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("test error"));
    }

    #[test]
    fn test_disk_info_serde() {
        let info = DiskInfo {
            id: "disk-1".into(),
            node_id: NodeId::generate(),
            path: "/dev/sda".into(),
            state: "healthy".into(),
            total_bytes: 1000,
            used_bytes: 500,
            extent_count: 42,
            error_count: 0,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("/dev/sda"));
    }

    #[test]
    fn test_join_request_serde() {
        let req = JoinRequest {
            node_id: NodeId::generate(),
            raft_addr: "10.0.0.1:9810".into(),
            data_addr: "10.0.0.1:9800".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: JoinRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.raft_addr, "10.0.0.1:9810");
        assert_eq!(parsed.data_addr, "10.0.0.1:9800");
    }

    #[test]
    fn test_join_response_serde() {
        let mut peers = std::collections::HashMap::new();
        peers.insert(1u64, "10.0.0.1:9810".to_string());
        let resp = JoinResponse {
            raft_id: 5,
            peers,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: JoinResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.raft_id, 5);
        assert_eq!(parsed.peers.len(), 1);
    }

    #[test]
    fn test_extent_mapping_request_serde() {
        let req = ExtentMappingRequest {
            volume_id: VolumeId::generate(),
            block_range_start: 0,
            block_range_end: 64,
            extent_id: blockyard_common::ExtentId::generate(),
            extent_version: 1,
            epoch: 10,
            replica_locations: vec![NodeId::generate()],
            checksums: vec![vec![0xFF]],
            operation_id: Some(blockyard_common::OperationId::generate()),
            previous_version: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ExtentMappingRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.extent_version, 1);
        assert_eq!(parsed.epoch, 10);
    }

    #[test]
    fn test_extent_mapping_response_serde() {
        let resp = ExtentMappingResponse { epoch: 42 };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ExtentMappingResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.epoch, 42);
    }

    #[test]
    fn test_operation_lookup_response_found_serde() {
        let resp = OperationLookupResponse {
            found: true,
            extent_id: Some(blockyard_common::ExtentId::generate()),
            extent_version: Some(3),
            epoch: Some(5),
            block_range_start: Some(0),
            block_range_end: Some(64),
            replica_locations: Some(vec![NodeId::generate()]),
            checksums: Some(vec![vec![1, 2]]),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: OperationLookupResponse = serde_json::from_str(&json).unwrap();
        assert!(parsed.found);
        assert_eq!(parsed.extent_version, Some(3));
    }

    #[test]
    fn test_operation_lookup_response_not_found_serde() {
        let resp = OperationLookupResponse {
            found: false,
            extent_id: None,
            extent_version: None,
            epoch: None,
            block_range_start: None,
            block_range_end: None,
            replica_locations: None,
            checksums: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: OperationLookupResponse = serde_json::from_str(&json).unwrap();
        assert!(!parsed.found);
    }

    #[test]
    fn test_lease_action_request_serde() {
        let req = LeaseActionRequest {
            volume_id: VolumeId::generate(),
            session_id: blockyard_common::SessionId::generate(),
            now_ms: 1000,
            ttl_ms: 5000,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: LeaseActionRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.now_ms, 1000);
        assert_eq!(parsed.ttl_ms, 5000);
    }

    #[test]
    fn test_lease_release_request_serde() {
        let req = LeaseReleaseRequest {
            volume_id: VolumeId::generate(),
            session_id: blockyard_common::SessionId::generate(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: LeaseReleaseRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.volume_id, req.volume_id);
    }
}
