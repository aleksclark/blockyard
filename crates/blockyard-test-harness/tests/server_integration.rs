//! Comprehensive integration tests for the Blockyard data plane server.
//!
//! These tests start a REAL `DataPlaneServer` in-process, listening on localhost,
//! then connect via raw TCP as a client. The server uses a `FakeHandler` that
//! stores extents in memory, allowing us to test the full wire protocol without
//! requiring XFS disks.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use blockyard_common::checksum::compute_checksum as blake3_checksum;
use blockyard_common::{DiskId, EpochId, ExtentId, NodeId, OperationId, SessionId, VolumeId};
use blockyard_protocol::messages::{
    CURRENT_PROTOCOL_VERSION, HandshakeRequest, HandshakeResponse, ProtocolMessage,
    ReadExtentRequest, ReadExtentResponse, WriteExtentRequest, WriteExtentResponse,
};
use blockyard_protocol::server::{DataPlaneHandler, DataPlaneServer};
use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// FakeHandler — in-memory extent storage for testing
// ---------------------------------------------------------------------------

/// In-memory fake handler implementing `DataPlaneHandler`.
///
/// Stores extent data keyed by `(ExtentId, extent_version)`. Validates checksums,
/// detects duplicate operations, and simulates stale epoch rejection.
#[derive(Debug)]
struct FakeHandler {
    extents: RwLock<HashMap<(ExtentId, u64), (Vec<u8>, String)>>,
    operations: RwLock<HashMap<OperationId, WriteExtentResponse>>,
    current_epoch: RwLock<EpochId>,
}

impl FakeHandler {
    fn new() -> Self {
        Self {
            extents: RwLock::new(HashMap::new()),
            operations: RwLock::new(HashMap::new()),
            current_epoch: RwLock::new(EpochId::new(1)),
        }
    }

    fn with_epoch(epoch: u64) -> Self {
        Self {
            extents: RwLock::new(HashMap::new()),
            operations: RwLock::new(HashMap::new()),
            current_epoch: RwLock::new(EpochId::new(epoch)),
        }
    }
}

impl DataPlaneHandler for FakeHandler {
    fn handle_write(&self, request: &WriteExtentRequest, payload: &[u8]) -> WriteExtentResponse {
        let op_id = request.operation_id;

        // Duplicate operation check
        if let Some(prev) = self.operations.read().get(&op_id) {
            return prev.clone();
        }

        // Stale epoch check
        let current = *self.current_epoch.read();
        if request.epoch < current {
            let resp = WriteExtentResponse {
                operation_id: op_id,
                extent_id: request.extent_id,
                extent_version: request.extent_version,
                disk_id: DiskId::generate(),
                success: false,
                checksum: String::new(),
                error: Some(format!(
                    "stale epoch: request={}, current={}",
                    request.epoch, current
                )),
            };
            self.operations.write().insert(op_id, resp.clone());
            return resp;
        }

        // Checksum validation
        let computed = compute_sha256(payload);
        if request.checksum != computed {
            let resp = WriteExtentResponse {
                operation_id: op_id,
                extent_id: request.extent_id,
                extent_version: request.extent_version,
                disk_id: DiskId::generate(),
                success: false,
                checksum: String::new(),
                error: Some(format!(
                    "payload checksum mismatch: expected {}, got {}",
                    request.checksum, computed
                )),
            };
            self.operations.write().insert(op_id, resp.clone());
            return resp;
        }

        let disk_id = request.target_disk_id.unwrap_or_else(DiskId::generate);
        self.extents.write().insert(
            (request.extent_id, request.extent_version),
            (payload.to_vec(), computed.clone()),
        );

        let resp = WriteExtentResponse {
            operation_id: op_id,
            extent_id: request.extent_id,
            extent_version: request.extent_version,
            disk_id,
            success: true,
            checksum: computed,
            error: None,
        };
        self.operations.write().insert(op_id, resp.clone());
        resp
    }

    fn handle_read(&self, request: &ReadExtentRequest) -> (ReadExtentResponse, Option<Vec<u8>>) {
        let extents = self.extents.read();
        let key = (request.extent_id, request.extent_version);

        match extents.get(&key) {
            Some((data, checksum)) => {
                let payload_data = if request.offset == 0 && request.length == 0 {
                    data.clone()
                } else {
                    let start = request.offset as usize;
                    let end = (request.offset + request.length) as usize;
                    if end > data.len() {
                        return (
                            ReadExtentResponse {
                                operation_id: request.operation_id,
                                extent_id: request.extent_id,
                                extent_version: request.extent_version,
                                success: false,
                                checksum: String::new(),
                                payload_size: 0,
                                error: Some("read range exceeds extent size".into()),
                            },
                            None,
                        );
                    }
                    data[start..end].to_vec()
                };

                (
                    ReadExtentResponse {
                        operation_id: request.operation_id,
                        extent_id: request.extent_id,
                        extent_version: request.extent_version,
                        success: true,
                        checksum: checksum.clone(),
                        payload_size: payload_data.len() as u64,
                        error: None,
                    },
                    Some(payload_data),
                )
            }
            None => (
                ReadExtentResponse {
                    operation_id: request.operation_id,
                    extent_id: request.extent_id,
                    extent_version: request.extent_version,
                    success: false,
                    checksum: String::new(),
                    payload_size: 0,
                    error: Some(format!("extent {} not found", request.extent_id)),
                },
                None,
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// TestServer — starts a DataPlaneServer on a random port
// ---------------------------------------------------------------------------

struct TestServer {
    server_handle: JoinHandle<()>,
    addr: SocketAddr,
    shutdown: CancellationToken,
    #[allow(dead_code)]
    handler: Arc<FakeHandler>,
}

async fn start_test_server() -> TestServer {
    start_test_server_with_handler(Arc::new(FakeHandler::new())).await
}

async fn start_test_server_with_handler(handler: Arc<FakeHandler>) -> TestServer {
    let node_id = NodeId::generate();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = DataPlaneServer::bind(addr, Arc::clone(&handler), node_id)
        .await
        .unwrap();
    let local_addr = server.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let shutdown2 = shutdown.clone();

    let server_handle = tokio::spawn(async move {
        server.run(shutdown2).await;
    });

    TestServer {
        server_handle,
        addr: local_addr,
        shutdown,
        handler,
    }
}

impl TestServer {
    async fn stop(self) {
        self.shutdown.cancel();
        self.server_handle.await.unwrap();
    }
}

// ---------------------------------------------------------------------------
// TestClient — raw TCP client for the data plane protocol
// ---------------------------------------------------------------------------

struct TestClient {
    stream: TcpStream,
}

impl TestClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.unwrap();
        Self { stream }
    }

    async fn handshake(&mut self) -> HandshakeResponse {
        let req = HandshakeRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            node_id: None,
            session_id: None,
            features: vec![],
            auth_token: None,
        };
        let req_bytes = serde_json::to_vec(&req).unwrap();
        self.send_frame(&req_bytes).await;
        let resp_bytes = self.read_frame().await;
        serde_json::from_slice(&resp_bytes).unwrap()
    }

    async fn handshake_with_version(&mut self, version: u32) -> HandshakeResponse {
        let req = HandshakeRequest {
            protocol_version: version,
            node_id: None,
            session_id: None,
            features: vec![],
            auth_token: None,
        };
        let req_bytes = serde_json::to_vec(&req).unwrap();
        self.send_frame(&req_bytes).await;
        let resp_bytes = self.read_frame().await;
        serde_json::from_slice(&resp_bytes).unwrap()
    }

    async fn write_extent(
        &mut self,
        extent_id: ExtentId,
        data: &[u8],
        epoch: EpochId,
    ) -> WriteExtentResponse {
        self.write_extent_versioned(extent_id, data, epoch, 1).await
    }

    async fn write_extent_versioned(
        &mut self,
        extent_id: ExtentId,
        data: &[u8],
        epoch: EpochId,
        version: u64,
    ) -> WriteExtentResponse {
        self.write_extent_full(
            OperationId::generate(),
            extent_id,
            data,
            epoch,
            version,
            &compute_sha256(data),
        )
        .await
    }

    async fn write_extent_full(
        &mut self,
        operation_id: OperationId,
        extent_id: ExtentId,
        data: &[u8],
        epoch: EpochId,
        version: u64,
        checksum: &str,
    ) -> WriteExtentResponse {
        let write_req = WriteExtentRequest {
            operation_id,
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id,
            extent_version: version,
            epoch,
            target_disk_id: None,
            checksum: checksum.to_string(),
            payload_size: data.len() as u64,
            lease_version: None,
        };
        let msg = ProtocolMessage::WriteReq(write_req);
        let msg_bytes = serde_json::to_vec(&msg).unwrap();
        self.send_frame(&msg_bytes).await;

        // Send raw payload
        self.stream.write_all(data).await.unwrap();
        self.stream.flush().await.unwrap();

        // Read response
        let resp_bytes = self.read_frame().await;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes).unwrap();
        match resp_msg {
            ProtocolMessage::WriteResp(wr) => wr,
            other => panic!("expected WriteResp, got {other:?}"),
        }
    }

    async fn read_extent(
        &mut self,
        extent_id: ExtentId,
        version: u64,
        epoch: EpochId,
    ) -> (ReadExtentResponse, Option<Vec<u8>>) {
        let read_req = ReadExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id,
            extent_version: version,
            epoch,
            offset: 0,
            length: 0, // read entire extent
        };
        let msg = ProtocolMessage::ReadReq(read_req);
        let msg_bytes = serde_json::to_vec(&msg).unwrap();
        self.send_frame(&msg_bytes).await;

        let resp_bytes = self.read_frame().await;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes).unwrap();
        match resp_msg {
            ProtocolMessage::ReadResp(rr) => {
                let data = if rr.success && rr.payload_size > 0 {
                    let mut buf = vec![0u8; rr.payload_size as usize];
                    self.stream.read_exact(&mut buf).await.unwrap();
                    Some(buf)
                } else {
                    None
                };
                (rr, data)
            }
            other => panic!("expected ReadResp, got {other:?}"),
        }
    }

    async fn send_frame(&mut self, data: &[u8]) {
        self.stream.write_u32(data.len() as u32).await.unwrap();
        self.stream.write_all(data).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    async fn read_frame(&mut self) -> Vec<u8> {
        let len = self.stream.read_u32().await.unwrap();
        let mut buf = vec![0u8; len as usize];
        self.stream.read_exact(&mut buf).await.unwrap();
        buf
    }
}

fn compute_sha256(data: &[u8]) -> String {
    // Use canonical blake3 checksum (was SHA-256, now blake3 via blockyard_common)
    blake3_checksum(data)
}

// ===========================================================================
// Connection & Handshake Tests
// ===========================================================================

#[tokio::test]
async fn test_server_accepts_connection() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;

    let resp = client.handshake().await;
    assert!(resp.accepted);
    assert_eq!(resp.protocol_version, CURRENT_PROTOCOL_VERSION);

    server.stop().await;
}

#[tokio::test]
async fn test_server_rejects_bad_version() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;

    let resp = client.handshake_with_version(0).await;
    assert!(!resp.accepted);
    assert!(resp.message.is_some());
    assert!(
        resp.message.unwrap().contains("below minimum"),
        "expected rejection message about minimum version"
    );

    server.stop().await;
}

#[tokio::test]
async fn test_multiple_concurrent_connections() {
    let server = start_test_server().await;

    let mut handles = vec![];
    for _ in 0..5 {
        let addr = server.addr;
        handles.push(tokio::spawn(async move {
            let mut client = TestClient::connect(addr).await;
            let resp = client.handshake().await;
            assert!(resp.accepted);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    server.stop().await;
}

#[tokio::test]
async fn test_handshake_with_future_version() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;

    let resp = client.handshake_with_version(999).await;
    assert!(resp.accepted);
    // Negotiated version should be server's current version
    assert_eq!(resp.protocol_version, CURRENT_PROTOCOL_VERSION);

    server.stop().await;
}

// ===========================================================================
// Write Path Tests
// ===========================================================================

#[tokio::test]
async fn test_write_single_extent() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = vec![0xABu8; 4096]; // 4KB
    let extent_id = ExtentId::generate();
    let resp = client.write_extent(extent_id, &data, EpochId::new(1)).await;

    assert!(resp.success, "write failed: {:?}", resp.error);
    assert_eq!(resp.extent_id, extent_id);
    assert_eq!(resp.checksum, compute_sha256(&data));

    server.stop().await;
}

#[tokio::test]
async fn test_write_multiple_extents() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    for i in 0..10 {
        let data = format!("extent-data-{i}").into_bytes();
        let extent_id = ExtentId::generate();
        let resp = client.write_extent(extent_id, &data, EpochId::new(1)).await;
        assert!(resp.success, "write {i} failed: {:?}", resp.error);
        assert_eq!(resp.extent_id, extent_id);
    }

    server.stop().await;
}

#[tokio::test]
async fn test_write_large_payload() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = vec![0x42u8; 1024 * 1024]; // 1MB
    let extent_id = ExtentId::generate();
    let resp = client.write_extent(extent_id, &data, EpochId::new(1)).await;

    assert!(resp.success, "write failed: {:?}", resp.error);
    assert_eq!(resp.checksum, compute_sha256(&data));

    server.stop().await;
}

#[tokio::test]
async fn test_write_zero_payload() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"";
    let extent_id = ExtentId::generate();
    let resp = client.write_extent(extent_id, data, EpochId::new(1)).await;

    assert!(resp.success, "write failed: {:?}", resp.error);

    server.stop().await;
}

#[tokio::test]
async fn test_write_duplicate_operation() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"dedup-test-data";
    let extent_id = ExtentId::generate();
    let op_id = OperationId::generate();
    let checksum = compute_sha256(data);

    // First write
    let resp1 = client
        .write_extent_full(op_id, extent_id, data, EpochId::new(1), 1, &checksum)
        .await;
    assert!(resp1.success);

    // Second write with same operation_id — should be idempotent
    let resp2 = client
        .write_extent_full(op_id, extent_id, data, EpochId::new(1), 1, &checksum)
        .await;
    assert!(resp2.success);
    assert_eq!(resp1.checksum, resp2.checksum);
    assert_eq!(resp1.extent_id, resp2.extent_id);

    server.stop().await;
}

// ===========================================================================
// Read Path Tests
// ===========================================================================

#[tokio::test]
async fn test_read_after_write() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"hello blockyard integration test";
    let extent_id = ExtentId::generate();

    // Write
    let write_resp = client.write_extent(extent_id, data, EpochId::new(1)).await;
    assert!(write_resp.success);

    // Read back
    let (read_resp, payload) = client.read_extent(extent_id, 1, EpochId::new(1)).await;
    assert!(read_resp.success, "read failed: {:?}", read_resp.error);
    assert_eq!(read_resp.payload_size, data.len() as u64);
    assert_eq!(payload.unwrap(), data);

    server.stop().await;
}

#[tokio::test]
async fn test_read_nonexistent_extent() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let (resp, payload) = client
        .read_extent(ExtentId::generate(), 1, EpochId::new(1))
        .await;
    assert!(!resp.success);
    assert!(resp.error.is_some());
    assert_eq!(resp.payload_size, 0);
    assert!(payload.is_none());

    server.stop().await;
}

#[tokio::test]
async fn test_read_write_roundtrip_multiple() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let mut extents = Vec::new();
    let n = 20;

    // Write N extents
    for i in 0..n {
        let data = format!("roundtrip-data-{i:04}").into_bytes();
        let extent_id = ExtentId::generate();
        let resp = client.write_extent(extent_id, &data, EpochId::new(1)).await;
        assert!(resp.success, "write {i} failed: {:?}", resp.error);
        extents.push((extent_id, data));
    }

    // Read all back and verify
    for (i, (extent_id, expected_data)) in extents.iter().enumerate() {
        let (resp, payload) = client.read_extent(*extent_id, 1, EpochId::new(1)).await;
        assert!(resp.success, "read {i} failed: {:?}", resp.error);
        assert_eq!(
            payload.as_deref(),
            Some(expected_data.as_slice()),
            "data mismatch for extent {i}"
        );
    }

    server.stop().await;
}

// ===========================================================================
// Error Case Tests
// ===========================================================================

#[tokio::test]
async fn test_write_stale_epoch() {
    let handler = Arc::new(FakeHandler::with_epoch(5));
    let server = start_test_server_with_handler(handler).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"stale-epoch-data";
    let extent_id = ExtentId::generate();
    // Write with epoch 1 when server is at epoch 5
    let resp = client.write_extent(extent_id, data, EpochId::new(1)).await;

    assert!(!resp.success);
    assert!(resp.error.as_deref().unwrap().contains("stale epoch"));

    server.stop().await;
}

#[tokio::test]
async fn test_write_bad_checksum() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"data with wrong checksum";
    let extent_id = ExtentId::generate();
    let resp = client
        .write_extent_full(
            OperationId::generate(),
            extent_id,
            data,
            EpochId::new(1),
            1,
            "definitely_wrong_checksum",
        )
        .await;

    assert!(!resp.success);
    assert!(resp.error.as_deref().unwrap().contains("checksum"));

    server.stop().await;
}

#[tokio::test]
async fn test_read_wrong_version() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"version-test-data";
    let extent_id = ExtentId::generate();

    // Write as version 1
    let write_resp = client.write_extent(extent_id, data, EpochId::new(1)).await;
    assert!(write_resp.success);

    // Try to read version 2 (doesn't exist)
    let (resp, payload) = client.read_extent(extent_id, 2, EpochId::new(1)).await;
    assert!(!resp.success);
    assert!(resp.error.is_some());
    assert!(payload.is_none());

    server.stop().await;
}

// ===========================================================================
// Concurrent Operation Tests
// ===========================================================================

#[tokio::test]
async fn test_concurrent_writes() {
    let server = start_test_server().await;
    let mut handles = vec![];

    for i in 0..10 {
        let addr = server.addr;
        handles.push(tokio::spawn(async move {
            let mut client = TestClient::connect(addr).await;
            client.handshake().await;

            let data = format!("concurrent-write-{i}").into_bytes();
            let extent_id = ExtentId::generate();
            let resp = client.write_extent(extent_id, &data, EpochId::new(1)).await;
            assert!(
                resp.success,
                "concurrent write {i} failed: {:?}",
                resp.error
            );
            (extent_id, data)
        }));
    }

    let mut results = Vec::new();
    for h in handles {
        results.push(h.await.unwrap());
    }

    // Verify we can read back all extents
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    for (extent_id, expected) in &results {
        let (resp, payload) = client.read_extent(*extent_id, 1, EpochId::new(1)).await;
        assert!(resp.success);
        assert_eq!(payload.unwrap(), *expected);
    }

    server.stop().await;
}

#[tokio::test]
async fn test_concurrent_read_write() {
    let server = start_test_server().await;

    // Pre-write some extents
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let mut written = Vec::new();
    for i in 0..5 {
        let data = format!("pre-written-{i}").into_bytes();
        let extent_id = ExtentId::generate();
        let resp = client.write_extent(extent_id, &data, EpochId::new(1)).await;
        assert!(resp.success);
        written.push((extent_id, data));
    }
    drop(client);

    // Spawn concurrent readers and writers
    let mut handles = vec![];

    // Writers
    for i in 0..5 {
        let addr = server.addr;
        handles.push(tokio::spawn(async move {
            let mut c = TestClient::connect(addr).await;
            c.handshake().await;
            let data = format!("concurrent-new-{i}").into_bytes();
            let eid = ExtentId::generate();
            let resp = c.write_extent(eid, &data, EpochId::new(1)).await;
            assert!(resp.success);
        }));
    }

    // Readers
    for (eid, expected) in written.clone() {
        let addr = server.addr;
        handles.push(tokio::spawn(async move {
            let mut c = TestClient::connect(addr).await;
            c.handshake().await;
            let (resp, payload) = c.read_extent(eid, 1, EpochId::new(1)).await;
            assert!(resp.success, "concurrent read failed: {:?}", resp.error);
            assert_eq!(payload.unwrap(), expected);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    server.stop().await;
}

// ===========================================================================
// Server Lifecycle Tests
// ===========================================================================

#[tokio::test]
async fn test_graceful_shutdown() {
    let server = start_test_server().await;

    // Write some data
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;
    let data = b"pre-shutdown-data";
    let extent_id = ExtentId::generate();
    let resp = client.write_extent(extent_id, data, EpochId::new(1)).await;
    assert!(resp.success);

    // Drop the client first so the server isn't waiting on it
    drop(client);

    // Shutdown should complete cleanly within timeout
    let shutdown_token = server.shutdown.clone();
    let handle = server.server_handle;
    shutdown_token.cancel();

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
    assert!(result.is_ok(), "server didn't shutdown within 5 seconds");
    result.unwrap().unwrap();
}

#[tokio::test]
async fn test_server_handles_client_disconnect() {
    let server = start_test_server().await;

    {
        let mut client = TestClient::connect(server.addr).await;
        client.handshake().await;

        let data = b"before-disconnect";
        let eid = ExtentId::generate();
        let resp = client.write_extent(eid, data, EpochId::new(1)).await;
        assert!(resp.success);
        // client drops here
    }

    // Give server time to handle disconnect
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // New connections should still work
    let mut client2 = TestClient::connect(server.addr).await;
    let resp = client2.handshake().await;
    assert!(resp.accepted);

    server.stop().await;
}

// ===========================================================================
// Multi-operation Sequence Tests
// ===========================================================================

#[tokio::test]
async fn test_write_read_overwrite_read() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let extent_id = ExtentId::generate();

    // Write v1
    let data_v1 = b"version-1-data";
    let resp = client
        .write_extent_versioned(extent_id, data_v1, EpochId::new(1), 1)
        .await;
    assert!(resp.success);

    // Read v1
    let (resp, payload) = client.read_extent(extent_id, 1, EpochId::new(1)).await;
    assert!(resp.success);
    assert_eq!(payload.unwrap(), data_v1);

    // Write v2 (new version)
    let data_v2 = b"version-2-data-updated";
    let resp = client
        .write_extent_versioned(extent_id, data_v2, EpochId::new(1), 2)
        .await;
    assert!(resp.success);

    // Read v2
    let (resp, payload) = client.read_extent(extent_id, 2, EpochId::new(1)).await;
    assert!(resp.success);
    assert_eq!(payload.unwrap(), data_v2);

    // v1 should still be readable
    let (resp, payload) = client.read_extent(extent_id, 1, EpochId::new(1)).await;
    assert!(resp.success);
    assert_eq!(payload.unwrap(), data_v1);

    server.stop().await;
}

#[tokio::test]
async fn test_session_multiple_operations() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let mut written_extents = Vec::new();

    // Perform 50 write+read pairs on a single connection
    for i in 0..50 {
        let data = format!("session-op-{i:04}-payload").into_bytes();
        let extent_id = ExtentId::generate();

        // Write
        let write_resp = client.write_extent(extent_id, &data, EpochId::new(1)).await;
        assert!(
            write_resp.success,
            "write {i} failed: {:?}",
            write_resp.error
        );

        // Read back immediately
        let (read_resp, payload) = client.read_extent(extent_id, 1, EpochId::new(1)).await;
        assert!(read_resp.success, "read {i} failed: {:?}", read_resp.error);
        assert_eq!(payload.unwrap(), data, "data mismatch at operation {i}");

        written_extents.push((extent_id, data));
    }

    // Final verification: read all extents back one more time
    for (i, (extent_id, expected)) in written_extents.iter().enumerate() {
        let (resp, payload) = client.read_extent(*extent_id, 1, EpochId::new(1)).await;
        assert!(resp.success, "final read {i} failed: {:?}", resp.error);
        assert_eq!(payload.as_deref(), Some(expected.as_slice()));
    }

    server.stop().await;
}

// ===========================================================================
// Additional Edge Case Tests
// ===========================================================================

#[tokio::test]
async fn test_write_various_sizes() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    // Test various payload sizes: 1 byte, 512 bytes, 4KB, 64KB, 256KB
    let sizes = [1, 512, 4096, 65536, 262144];
    for &size in &sizes {
        let data = vec![0xFFu8; size];
        let extent_id = ExtentId::generate();
        let resp = client.write_extent(extent_id, &data, EpochId::new(1)).await;
        assert!(
            resp.success,
            "write of {size} bytes failed: {:?}",
            resp.error
        );

        // Read back to verify
        let (read_resp, payload) = client.read_extent(extent_id, 1, EpochId::new(1)).await;
        assert!(read_resp.success);
        assert_eq!(payload.unwrap().len(), size);
    }

    server.stop().await;
}

#[tokio::test]
async fn test_write_epoch_boundary() {
    // Server at epoch 1: epoch 1 should succeed, epoch 0 should fail
    let handler = Arc::new(FakeHandler::with_epoch(1));
    let server = start_test_server_with_handler(handler).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    // Epoch 1 should succeed
    let data = b"epoch-boundary-data";
    let eid = ExtentId::generate();
    let resp = client.write_extent(eid, data, EpochId::new(1)).await;
    assert!(resp.success);

    // Epoch 0 should fail (stale)
    let eid2 = ExtentId::generate();
    let resp = client.write_extent(eid2, data, EpochId::new(0)).await;
    assert!(!resp.success);
    assert!(resp.error.as_deref().unwrap().contains("stale epoch"));

    server.stop().await;
}

#[tokio::test]
async fn test_multiple_clients_same_extent() {
    let server = start_test_server().await;

    // Client 1 writes an extent
    let extent_id = ExtentId::generate();
    let data = b"shared-extent-data";

    let mut client1 = TestClient::connect(server.addr).await;
    client1.handshake().await;
    let resp = client1.write_extent(extent_id, data, EpochId::new(1)).await;
    assert!(resp.success);

    // Client 2 reads the same extent
    let mut client2 = TestClient::connect(server.addr).await;
    client2.handshake().await;
    let (resp, payload) = client2.read_extent(extent_id, 1, EpochId::new(1)).await;
    assert!(resp.success);
    assert_eq!(payload.unwrap(), data);

    server.stop().await;
}

#[tokio::test]
async fn test_rapid_connect_disconnect() {
    let server = start_test_server().await;

    // Rapidly connect and disconnect 20 times
    for _ in 0..20 {
        let mut client = TestClient::connect(server.addr).await;
        let resp = client.handshake().await;
        assert!(resp.accepted);
        // Drop immediately
    }

    // Server should still be healthy
    let mut client = TestClient::connect(server.addr).await;
    let resp = client.handshake().await;
    assert!(resp.accepted);

    server.stop().await;
}

#[tokio::test]
async fn test_write_then_read_different_clients() {
    let server = start_test_server().await;

    // Write with one client
    let mut writer = TestClient::connect(server.addr).await;
    writer.handshake().await;

    let extent_ids: Vec<ExtentId> = (0..5).map(|_| ExtentId::generate()).collect();
    let mut expected_data = Vec::new();

    for (i, &eid) in extent_ids.iter().enumerate() {
        let data = format!("cross-client-{i}").into_bytes();
        let resp = writer.write_extent(eid, &data, EpochId::new(1)).await;
        assert!(resp.success);
        expected_data.push(data);
    }
    drop(writer);

    // Read with a different client
    let mut reader = TestClient::connect(server.addr).await;
    reader.handshake().await;

    for (i, (&eid, expected)) in extent_ids.iter().zip(expected_data.iter()).enumerate() {
        let (resp, payload) = reader.read_extent(eid, 1, EpochId::new(1)).await;
        assert!(
            resp.success,
            "cross-client read {i} failed: {:?}",
            resp.error
        );
        assert_eq!(payload.unwrap(), *expected);
    }

    server.stop().await;
}

#[tokio::test]
async fn test_checksum_integrity() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"checksum-integrity-test-data";
    let extent_id = ExtentId::generate();
    let expected_checksum = compute_sha256(data);

    let write_resp = client.write_extent(extent_id, data, EpochId::new(1)).await;
    assert!(write_resp.success);
    assert_eq!(write_resp.checksum, expected_checksum);

    let (read_resp, _) = client.read_extent(extent_id, 1, EpochId::new(1)).await;
    assert!(read_resp.success);
    assert_eq!(read_resp.checksum, expected_checksum);

    server.stop().await;
}

#[tokio::test]
async fn test_interleaved_reads_and_writes() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let eid1 = ExtentId::generate();
    let eid2 = ExtentId::generate();

    // Write eid1
    let data1 = b"first-extent";
    let resp = client.write_extent(eid1, data1, EpochId::new(1)).await;
    assert!(resp.success);

    // Read eid1
    let (resp, payload) = client.read_extent(eid1, 1, EpochId::new(1)).await;
    assert!(resp.success);
    assert_eq!(payload.unwrap(), data1);

    // Write eid2
    let data2 = b"second-extent";
    let resp = client.write_extent(eid2, data2, EpochId::new(1)).await;
    assert!(resp.success);

    // Read nonexistent eid
    let (resp, _) = client
        .read_extent(ExtentId::generate(), 1, EpochId::new(1))
        .await;
    assert!(!resp.success);

    // Read eid2
    let (resp, payload) = client.read_extent(eid2, 1, EpochId::new(1)).await;
    assert!(resp.success);
    assert_eq!(payload.unwrap(), data2);

    // Read eid1 again (still there)
    let (resp, payload) = client.read_extent(eid1, 1, EpochId::new(1)).await;
    assert!(resp.success);
    assert_eq!(payload.unwrap(), data1);

    server.stop().await;
}

#[tokio::test]
async fn test_binary_payload_roundtrip() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    // Test with all byte values 0x00..0xFF
    let data: Vec<u8> = (0..=255).collect();
    let extent_id = ExtentId::generate();

    let resp = client.write_extent(extent_id, &data, EpochId::new(1)).await;
    assert!(resp.success);

    let (resp, payload) = client.read_extent(extent_id, 1, EpochId::new(1)).await;
    assert!(resp.success);
    assert_eq!(payload.unwrap(), data);

    server.stop().await;
}

#[tokio::test]
async fn test_concurrent_connections_write_and_verify() {
    let server = start_test_server().await;
    let num_clients = 10;
    let ops_per_client = 5;

    let handles: Vec<_> = (0..num_clients)
        .map(|client_idx| {
            let addr = server.addr;
            tokio::spawn(async move {
                let mut client = TestClient::connect(addr).await;
                client.handshake().await;

                let mut pairs = Vec::new();
                for op_idx in 0..ops_per_client {
                    let data = format!("client-{client_idx}-op-{op_idx}").into_bytes();
                    let eid = ExtentId::generate();
                    let resp = client.write_extent(eid, &data, EpochId::new(1)).await;
                    assert!(resp.success);
                    pairs.push((eid, data));
                }

                // Verify all writes from this client
                for (eid, expected) in &pairs {
                    let (resp, payload) = client.read_extent(*eid, 1, EpochId::new(1)).await;
                    assert!(resp.success);
                    assert_eq!(payload.unwrap(), *expected);
                }
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap();
    }

    server.stop().await;
}
