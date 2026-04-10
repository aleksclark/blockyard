//! HTTP-based [`MetadataClient`] implementation.
//!
//! Connects to the Blockyard management REST API to perform metadata
//! operations: epoch queries, extent mapping commits, operation lookups,
//! and volume lease management.

use blockyard_common::error::Error;
use blockyard_common::{EpochId, LeaseResponse, OperationId, SessionId, VolumeId};
use serde::{Deserialize, Serialize};

use crate::metadata_cache::MetadataCache;
use crate::traits::{CommitRequest, CommittedMapping, MetadataClient};

/// HTTP-based metadata client that communicates with the management API.
#[derive(Debug, Clone)]
pub struct HttpMetadataClient {
    /// Base URL of the management API (e.g., "http://127.0.0.1:9801").
    endpoint: String,
    /// HTTP client for making requests.
    client: reqwest::Client,
}

/// Response from the cluster status endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClusterStatusResponse {
    #[serde(default)]
    node_count: u32,
    #[serde(default)]
    nodes_online: u32,
    #[serde(default)]
    volume_count: u32,
    #[serde(default)]
    disk_count: u32,
    placement_epoch: u64,
    #[serde(default)]
    quorum_health: String,
    #[serde(default)]
    total_capacity_bytes: u64,
    #[serde(default)]
    used_capacity_bytes: u64,
}

/// Request body for committing extent mappings via the API.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExtentMappingRequest {
    volume_id: VolumeId,
    block_range_start: u64,
    block_range_end: u64,
    extent_id: blockyard_common::ExtentId,
    extent_version: u64,
    epoch: u64,
    replica_locations: Vec<blockyard_common::NodeId>,
    checksums: Vec<Vec<u8>>,
    operation_id: Option<OperationId>,
    previous_version: Option<u64>,
}

/// Response from the extent mapping commit endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExtentMappingResponse {
    epoch: u64,
}

/// Response from the operation lookup endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperationLookupResponse {
    found: bool,
    #[serde(default)]
    extent_id: Option<blockyard_common::ExtentId>,
    #[serde(default)]
    extent_version: Option<u64>,
    #[serde(default)]
    epoch: Option<u64>,
    #[serde(default)]
    block_range_start: Option<u64>,
    #[serde(default)]
    block_range_end: Option<u64>,
    #[serde(default)]
    replica_locations: Option<Vec<blockyard_common::NodeId>>,
    #[serde(default)]
    checksums: Option<Vec<Vec<u8>>>,
}

/// Request for lease acquire/renew.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseRequest {
    volume_id: VolumeId,
    session_id: SessionId,
    now_ms: u64,
    ttl_ms: u64,
}

/// Request for lease release.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseReleaseRequest {
    volume_id: VolumeId,
    session_id: SessionId,
}

/// Node info from list nodes endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeInfo {
    id: blockyard_common::NodeId,
    address: String,
    #[serde(default)]
    state: String,
    #[serde(default)]
    disk_count: u32,
    #[serde(default)]
    volume_count: u32,
    #[serde(default)]
    uptime_seconds: u64,
}

/// Volume info from list volumes endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VolumeInfo {
    id: VolumeId,
    #[serde(default)]
    name: String,
    size_bytes: u64,
    protection: blockyard_common::ProtectionPolicy,
    #[serde(default)]
    state: String,
    #[serde(default)]
    replica_nodes: Vec<blockyard_common::NodeId>,
}

impl HttpMetadataClient {
    /// Create a new HTTP metadata client with the given management API endpoint.
    ///
    /// `endpoint` should be a base URL like "http://127.0.0.1:9801".
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            client: reqwest::Client::new(),
        }
    }

    /// Create a new HTTP metadata client with a custom reqwest client.
    pub fn with_client(endpoint: String, client: reqwest::Client) -> Self {
        Self { endpoint, client }
    }

    /// Get the base endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.endpoint, path)
    }
}

impl MetadataClient for HttpMetadataClient {
    async fn refresh_metadata(&self, cache: &MetadataCache) -> Result<EpochId, Error> {
        // Fetch cluster status to get current epoch
        let status_resp = self
            .client
            .get(self.url("/api/v1/cluster/status"))
            .send()
            .await
            .map_err(|e| Error::Network(format!("failed to fetch cluster status: {e}")))?;

        if !status_resp.status().is_success() {
            return Err(Error::Network(format!(
                "cluster status returned {}",
                status_resp.status()
            )));
        }

        let status: ClusterStatusResponse = status_resp
            .json()
            .await
            .map_err(|e| Error::Network(format!("failed to parse cluster status: {e}")))?;

        let new_epoch = EpochId::new(status.placement_epoch);

        // Fetch node list
        let nodes_resp = self
            .client
            .get(self.url("/api/v1/nodes"))
            .send()
            .await
            .map_err(|e| Error::Network(format!("failed to fetch nodes: {e}")))?;

        if nodes_resp.status().is_success() {
            let nodes: Vec<NodeInfo> = nodes_resp
                .json()
                .await
                .map_err(|e| Error::Network(format!("failed to parse nodes: {e}")))?;

            let mut cache_nodes = Vec::new();
            for n in &nodes {
                if let Ok(addr) = n.address.parse() {
                    cache_nodes.push(crate::metadata_cache::NodeAddress {
                        node_id: n.id,
                        addr,
                    });
                }
            }

            // Fetch volume list
            let volumes_resp = self
                .client
                .get(self.url("/api/v1/volumes"))
                .send()
                .await
                .map_err(|e| Error::Network(format!("failed to fetch volumes: {e}")))?;

            let mut cache_volumes = Vec::new();
            if volumes_resp.status().is_success() {
                let volumes: Vec<VolumeInfo> = volumes_resp
                    .json()
                    .await
                    .map_err(|e| Error::Network(format!("failed to parse volumes: {e}")))?;

                for v in volumes {
                    cache_volumes.push(crate::metadata_cache::CachedVolumeInfo {
                        volume_id: v.id,
                        size_bytes: v.size_bytes,
                        protection: v.protection,
                        extent_mappings: std::collections::BTreeMap::new(),
                    });
                }
            }

            cache.refresh(new_epoch, cache_nodes, cache_volumes);
        } else {
            // Just update the epoch if we can't get full data
            cache.set_epoch(new_epoch);
        }

        Ok(new_epoch)
    }

    async fn commit_extent_mapping(&self, request: CommitRequest) -> Result<EpochId, Error> {
        let api_req = ExtentMappingRequest {
            volume_id: request.volume_id,
            block_range_start: request.block_range.start,
            block_range_end: request.block_range.end,
            extent_id: request.extent_id,
            extent_version: request.extent_version,
            epoch: request.epoch.as_u64(),
            replica_locations: request.replica_locations,
            checksums: request.checksums,
            operation_id: request.operation_id,
            previous_version: request.previous_version,
        };

        let resp = self
            .client
            .post(self.url("/api/v1/extent-mappings"))
            .json(&api_req)
            .send()
            .await
            .map_err(|e| Error::Network(format!("failed to commit extent mapping: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Raft(format!(
                "extent mapping commit failed (status {status}): {body}"
            )));
        }

        let mapping_resp: ExtentMappingResponse = resp
            .json()
            .await
            .map_err(|e| Error::Network(format!("failed to parse commit response: {e}")))?;

        Ok(EpochId::new(mapping_resp.epoch))
    }

    async fn lookup_operation(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<CommittedMapping>, Error> {
        let resp = self
            .client
            .get(self.url(&format!("/api/v1/operations/{operation_id}")))
            .send()
            .await
            .map_err(|e| Error::Network(format!("failed to lookup operation: {e}")))?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !resp.status().is_success() {
            return Err(Error::Network(format!(
                "operation lookup failed: {}",
                resp.status()
            )));
        }

        let lookup: OperationLookupResponse = resp
            .json()
            .await
            .map_err(|e| Error::Network(format!("failed to parse operation lookup: {e}")))?;

        if !lookup.found {
            return Ok(None);
        }

        // All fields should be present when found=true
        Ok(Some(CommittedMapping {
            extent_id: lookup
                .extent_id
                .ok_or_else(|| Error::Network("operation lookup missing extent_id".into()))?,
            extent_version: lookup
                .extent_version
                .ok_or_else(|| Error::Network("operation lookup missing extent_version".into()))?,
            epoch: EpochId::new(
                lookup
                    .epoch
                    .ok_or_else(|| Error::Network("operation lookup missing epoch".into()))?,
            ),
            block_range: lookup.block_range_start.unwrap_or(0)..lookup.block_range_end.unwrap_or(0),
            replica_locations: lookup.replica_locations.unwrap_or_default(),
            checksums: lookup.checksums.unwrap_or_default(),
        }))
    }

    async fn current_epoch(&self) -> Result<EpochId, Error> {
        let resp = self
            .client
            .get(self.url("/api/v1/cluster/status"))
            .send()
            .await
            .map_err(|e| Error::Network(format!("failed to fetch cluster status: {e}")))?;

        if !resp.status().is_success() {
            return Err(Error::Network(format!(
                "cluster status returned {}",
                resp.status()
            )));
        }

        let status: ClusterStatusResponse = resp
            .json()
            .await
            .map_err(|e| Error::Network(format!("failed to parse cluster status: {e}")))?;

        Ok(EpochId::new(status.placement_epoch))
    }

    async fn acquire_lease(
        &self,
        volume_id: VolumeId,
        session_id: SessionId,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<LeaseResponse, Error> {
        let req = LeaseRequest {
            volume_id,
            session_id,
            now_ms,
            ttl_ms,
        };

        let resp = self
            .client
            .post(self.url("/api/v1/leases/acquire"))
            .json(&req)
            .send()
            .await
            .map_err(|e| Error::Network(format!("failed to acquire lease: {e}")))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Network(format!("lease acquire failed: {body}")));
        }

        resp.json()
            .await
            .map_err(|e| Error::Network(format!("failed to parse lease response: {e}")))
    }

    async fn renew_lease(
        &self,
        volume_id: VolumeId,
        session_id: SessionId,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Result<LeaseResponse, Error> {
        let req = LeaseRequest {
            volume_id,
            session_id,
            now_ms,
            ttl_ms,
        };

        let resp = self
            .client
            .post(self.url("/api/v1/leases/renew"))
            .json(&req)
            .send()
            .await
            .map_err(|e| Error::Network(format!("failed to renew lease: {e}")))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Network(format!("lease renew failed: {body}")));
        }

        resp.json()
            .await
            .map_err(|e| Error::Network(format!("failed to parse lease response: {e}")))
    }

    async fn release_lease(
        &self,
        volume_id: VolumeId,
        session_id: SessionId,
    ) -> Result<LeaseResponse, Error> {
        let req = LeaseReleaseRequest {
            volume_id,
            session_id,
        };

        let resp = self
            .client
            .post(self.url("/api/v1/leases/release"))
            .json(&req)
            .send()
            .await
            .map_err(|e| Error::Network(format!("failed to release lease: {e}")))?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(Error::Network(format!("lease release failed: {body}")));
        }

        resp.json()
            .await
            .map_err(|e| Error::Network(format!("failed to parse lease response: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use std::sync::Arc;

    /// Shared test state for mock API server.
    #[derive(Debug, Clone)]
    struct MockState {
        epoch: u64,
    }

    async fn mock_cluster_status(State(state): State<Arc<MockState>>) -> impl IntoResponse {
        Json(serde_json::json!({
            "node_count": 1,
            "nodes_online": 1,
            "volume_count": 0,
            "disk_count": 0,
            "placement_epoch": state.epoch,
            "quorum_health": "healthy",
            "total_capacity_bytes": 0,
            "used_capacity_bytes": 0
        }))
    }

    async fn mock_list_nodes() -> impl IntoResponse {
        Json(serde_json::json!([]))
    }

    async fn mock_list_volumes() -> impl IntoResponse {
        Json(serde_json::json!([]))
    }

    async fn mock_commit_mapping(
        State(state): State<Arc<MockState>>,
        Json(_req): Json<ExtentMappingRequest>,
    ) -> impl IntoResponse {
        Json(serde_json::json!({
            "epoch": state.epoch
        }))
    }

    async fn mock_lookup_operation_not_found() -> impl IntoResponse {
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "found": false
            })),
        )
    }

    async fn mock_acquire_lease(Json(_req): Json<LeaseRequest>) -> impl IntoResponse {
        Json(serde_json::json!({
            "Denied": { "reason": "mock server" }
        }))
    }

    async fn mock_renew_lease(Json(_req): Json<LeaseRequest>) -> impl IntoResponse {
        Json(serde_json::json!({
            "Denied": { "reason": "mock server" }
        }))
    }

    async fn mock_release_lease(Json(_req): Json<LeaseReleaseRequest>) -> impl IntoResponse {
        Json(serde_json::json!("Released"))
    }

    async fn start_mock_server(epoch: u64) -> (String, tokio::task::JoinHandle<()>) {
        let state = Arc::new(MockState { epoch });

        let app = Router::new()
            .route("/api/v1/cluster/status", get(mock_cluster_status))
            .route("/api/v1/nodes", get(mock_list_nodes))
            .route("/api/v1/volumes", get(mock_list_volumes))
            .route("/api/v1/extent-mappings", post(mock_commit_mapping))
            .route(
                "/api/v1/operations/{id}",
                get(mock_lookup_operation_not_found),
            )
            .route("/api/v1/leases/acquire", post(mock_acquire_lease))
            .route("/api/v1/leases/renew", post(mock_renew_lease))
            .route("/api/v1/leases/release", post(mock_release_lease))
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let endpoint = format!("http://{addr}");

        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        (endpoint, handle)
    }

    #[test]
    fn test_http_metadata_client_new() {
        let client = HttpMetadataClient::new("http://localhost:9801".into());
        assert_eq!(client.endpoint(), "http://localhost:9801");
    }

    #[test]
    fn test_http_metadata_client_with_client() {
        let reqwest_client = reqwest::Client::new();
        let client =
            HttpMetadataClient::with_client("http://localhost:9801".into(), reqwest_client);
        assert_eq!(client.endpoint(), "http://localhost:9801");
    }

    #[test]
    fn test_http_metadata_client_url() {
        let client = HttpMetadataClient::new("http://127.0.0.1:9801".into());
        assert_eq!(
            client.url("/api/v1/cluster/status"),
            "http://127.0.0.1:9801/api/v1/cluster/status"
        );
    }

    #[test]
    fn test_http_metadata_client_debug() {
        let client = HttpMetadataClient::new("http://localhost:9801".into());
        let debug = format!("{:?}", client);
        assert!(debug.contains("HttpMetadataClient"));
        assert!(debug.contains("localhost:9801"));
    }

    #[test]
    fn test_http_metadata_client_clone() {
        let client = HttpMetadataClient::new("http://localhost:9801".into());
        let cloned = client.clone();
        assert_eq!(client.endpoint(), cloned.endpoint());
    }

    #[tokio::test]
    async fn test_http_metadata_client_current_epoch() {
        let (endpoint, _handle) = start_mock_server(42).await;
        let client = HttpMetadataClient::new(endpoint);

        let epoch = client.current_epoch().await.unwrap();
        assert_eq!(epoch, EpochId::new(42));
    }

    #[tokio::test]
    async fn test_http_metadata_client_refresh_metadata() {
        let (endpoint, _handle) = start_mock_server(7).await;
        let client = HttpMetadataClient::new(endpoint);

        let cache = MetadataCache::new();
        let epoch = client.refresh_metadata(&cache).await.unwrap();
        assert_eq!(epoch, EpochId::new(7));
        assert_eq!(cache.current_epoch(), EpochId::new(7));
    }

    #[tokio::test]
    async fn test_http_metadata_client_commit_extent_mapping() {
        let (endpoint, _handle) = start_mock_server(10).await;
        let client = HttpMetadataClient::new(endpoint);

        let req = CommitRequest {
            volume_id: VolumeId::generate(),
            block_range: 0..64,
            extent_id: blockyard_common::ExtentId::generate(),
            extent_version: 1,
            epoch: EpochId::new(10),
            replica_locations: vec![blockyard_common::NodeId::generate()],
            checksums: vec![vec![0xFF]],
            operation_id: Some(OperationId::generate()),
            previous_version: None,
        };

        let epoch = client.commit_extent_mapping(req).await.unwrap();
        assert_eq!(epoch, EpochId::new(10));
    }

    #[tokio::test]
    async fn test_http_metadata_client_lookup_operation_not_found() {
        let (endpoint, _handle) = start_mock_server(1).await;
        let client = HttpMetadataClient::new(endpoint);

        let result = client
            .lookup_operation(&OperationId::generate())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_http_metadata_client_acquire_lease() {
        let (endpoint, _handle) = start_mock_server(1).await;
        let client = HttpMetadataClient::new(endpoint);

        let resp = client
            .acquire_lease(VolumeId::generate(), SessionId::generate(), 1000, 5000)
            .await
            .unwrap();

        // Our mock returns Denied
        assert!(!resp.is_success());
    }

    #[tokio::test]
    async fn test_http_metadata_client_renew_lease() {
        let (endpoint, _handle) = start_mock_server(1).await;
        let client = HttpMetadataClient::new(endpoint);

        let resp = client
            .renew_lease(VolumeId::generate(), SessionId::generate(), 1000, 5000)
            .await
            .unwrap();

        assert!(!resp.is_success());
    }

    #[tokio::test]
    async fn test_http_metadata_client_release_lease() {
        let (endpoint, _handle) = start_mock_server(1).await;
        let client = HttpMetadataClient::new(endpoint);

        let resp = client
            .release_lease(VolumeId::generate(), SessionId::generate())
            .await
            .unwrap();

        assert!(matches!(resp, LeaseResponse::Released));
    }

    #[tokio::test]
    async fn test_http_metadata_client_connection_refused() {
        let client = HttpMetadataClient::new("http://127.0.0.1:1".into());

        let result = client.current_epoch().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_http_metadata_client_commit_mapping_with_all_fields() {
        let (endpoint, _handle) = start_mock_server(15).await;
        let client = HttpMetadataClient::new(endpoint);

        let req = CommitRequest {
            volume_id: VolumeId::generate(),
            block_range: 64..128,
            extent_id: blockyard_common::ExtentId::generate(),
            extent_version: 5,
            epoch: EpochId::new(15),
            replica_locations: vec![
                blockyard_common::NodeId::generate(),
                blockyard_common::NodeId::generate(),
            ],
            checksums: vec![vec![0xAA, 0xBB], vec![0xCC, 0xDD]],
            operation_id: Some(OperationId::generate()),
            previous_version: Some(4),
        };

        let epoch = client.commit_extent_mapping(req).await.unwrap();
        assert_eq!(epoch, EpochId::new(15));
    }

    #[test]
    fn test_cluster_status_response_serde() {
        let resp = ClusterStatusResponse {
            node_count: 3,
            nodes_online: 3,
            volume_count: 1,
            disk_count: 6,
            placement_epoch: 42,
            quorum_health: "healthy".into(),
            total_capacity_bytes: 1000,
            used_capacity_bytes: 500,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ClusterStatusResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.placement_epoch, 42);
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
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ExtentMappingRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.extent_version, 1);
    }

    #[test]
    fn test_extent_mapping_response_serde() {
        let resp = ExtentMappingResponse { epoch: 5 };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ExtentMappingResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.epoch, 5);
    }

    #[test]
    fn test_operation_lookup_response_serde_found() {
        let resp = OperationLookupResponse {
            found: true,
            extent_id: Some(blockyard_common::ExtentId::generate()),
            extent_version: Some(1),
            epoch: Some(5),
            block_range_start: Some(0),
            block_range_end: Some(64),
            replica_locations: Some(vec![]),
            checksums: Some(vec![]),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: OperationLookupResponse = serde_json::from_str(&json).unwrap();
        assert!(parsed.found);
    }

    #[test]
    fn test_operation_lookup_response_serde_not_found() {
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
    fn test_lease_request_serde() {
        let req = LeaseRequest {
            volume_id: VolumeId::generate(),
            session_id: SessionId::generate(),
            now_ms: 1000,
            ttl_ms: 5000,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: LeaseRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.now_ms, 1000);
        assert_eq!(parsed.ttl_ms, 5000);
    }

    #[test]
    fn test_lease_release_request_serde() {
        let req = LeaseReleaseRequest {
            volume_id: VolumeId::generate(),
            session_id: SessionId::generate(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: LeaseReleaseRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.volume_id, req.volume_id);
    }
}
