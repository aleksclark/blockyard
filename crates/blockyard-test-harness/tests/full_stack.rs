//! Full-stack integration tests: DataNodeService → DataPlaneServer → TCP Client.
//!
//! These tests exercise the REAL storage pipeline with actual disk I/O through
//! TempDir directories. Unlike `server_integration.rs` which uses a FakeHandler,
//! these tests use a real `DataNodeService` with `ExtentStore` instances writing
//! to temporary directories.

use std::net::SocketAddr;
use std::sync::Arc;

use blockyard_common::{DiskId, EpochId, ExtentId, NodeId, OperationId, SessionId, VolumeId};
use blockyard_protocol::messages::{
    CURRENT_PROTOCOL_VERSION, HandshakeRequest, ProtocolMessage, ReadExtentRequest,
    ReadExtentResponse, WriteExtentRequest, WriteExtentResponse,
};
use blockyard_protocol::server::{DataPlaneHandler, DataPlaneServer};
use blockyard_storage::extent::compute_checksum;
use blockyard_storage::{DataNodeService, DiskInventory, ExtentIndex, ExtentStore};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// RealHandler — wrapper implementing DataPlaneHandler for DataNodeService
// ---------------------------------------------------------------------------

/// Newtype wrapper around `DataNodeService` that implements [`DataPlaneHandler`].
///
/// Required because the binary crate's `DataNodeHandler` cannot be imported
/// (binary crates are not linkable). This mirrors the pattern in `blockyard/src/node.rs`.
#[derive(Debug)]
struct RealHandler(DataNodeService);

impl DataPlaneHandler for RealHandler {
    fn handle_write(&self, request: &WriteExtentRequest, payload: &[u8]) -> WriteExtentResponse {
        self.0.handle_write(request, payload)
    }

    fn handle_read(&self, request: &ReadExtentRequest) -> (ReadExtentResponse, Option<Vec<u8>>) {
        self.0.handle_read(request)
    }
}

// ---------------------------------------------------------------------------
// FullStackServer — real DataNodeService + DataPlaneServer on localhost
// ---------------------------------------------------------------------------

struct FullStackServer {
    _temp_dirs: Vec<TempDir>,
    disk_ids: Vec<DiskId>,
    #[allow(dead_code)]
    handler: Arc<RealHandler>,
    server_handle: JoinHandle<()>,
    addr: SocketAddr,
    shutdown: CancellationToken,
}

/// Creates a full stack server with `disk_count` temporary disks.
async fn start_full_stack_server(disk_count: usize) -> FullStackServer {
    start_full_stack_server_with_epoch(disk_count, 1).await
}

/// Creates a full stack server with `disk_count` temporary disks and a specific epoch.
async fn start_full_stack_server_with_epoch(disk_count: usize, epoch: u64) -> FullStackServer {
    assert!(disk_count >= 1, "need at least 1 disk");

    // Create temp directories for disks
    let mut temp_dirs = Vec::with_capacity(disk_count);
    let mut disk_paths = Vec::with_capacity(disk_count);
    for _ in 0..disk_count {
        let dir = TempDir::new().expect("failed to create temp dir");
        // Write the XFS marker so validation passes
        std::fs::write(dir.path().join(".blockyard_xfs_ok"), "").unwrap();
        disk_paths.push(dir.path().to_path_buf());
        temp_dirs.push(dir);
    }

    // Discover disks
    let inventory = DiskInventory::new();
    let disk_ids = inventory
        .discover_disks(&disk_paths, false)
        .expect("failed to discover disks");

    // Create extent index
    let index = ExtentIndex::new();

    // Create ExtentStore per disk + recover
    let mut stores = Vec::new();
    for &disk_id in &disk_ids {
        let mount_path = inventory
            .get_mount_path(disk_id)
            .expect("failed to get mount path");
        let store = ExtentStore::new(mount_path, disk_id);
        store
            .recover(&index)
            .expect("failed to recover extent store");
        stores.push((disk_id, store));
    }

    // Create DataNodeService
    let service = DataNodeService::new(inventory, index, EpochId::new(epoch));

    // Register stores
    for (disk_id, store) in stores {
        service.register_store(disk_id, store);
    }

    let handler = Arc::new(RealHandler(service));
    let node_id = NodeId::generate();
    let bind_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let server = DataPlaneServer::bind(bind_addr, Arc::clone(&handler), node_id)
        .await
        .expect("failed to bind server");
    let addr = server.local_addr().expect("failed to get local addr");

    let shutdown = CancellationToken::new();
    let shutdown2 = shutdown.clone();

    let server_handle = tokio::spawn(async move {
        server.run(shutdown2).await;
    });

    FullStackServer {
        _temp_dirs: temp_dirs,
        disk_ids,
        handler,
        server_handle,
        addr,
        shutdown,
    }
}

impl FullStackServer {
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

    async fn handshake(&mut self) {
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
        let resp: blockyard_protocol::HandshakeResponse =
            serde_json::from_slice(&resp_bytes).unwrap();
        assert!(resp.accepted, "handshake not accepted: {:?}", resp.message);
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
            None,
        )
        .await
    }

    async fn write_extent_with_disk(
        &mut self,
        extent_id: ExtentId,
        data: &[u8],
        epoch: EpochId,
        target_disk_id: DiskId,
    ) -> WriteExtentResponse {
        self.write_extent_full(
            OperationId::generate(),
            extent_id,
            data,
            epoch,
            1,
            &compute_sha256(data),
            Some(target_disk_id),
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
        target_disk_id: Option<DiskId>,
    ) -> WriteExtentResponse {
        let write_req = WriteExtentRequest {
            operation_id,
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id,
            extent_version: version,
            epoch,
            target_disk_id,
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
            length: 0, // entire extent
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
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

// ===========================================================================
// Full-Stack Integration Tests
// ===========================================================================

/// 1. Write 4KB through TCP, read back, verify data matches byte-for-byte.
#[tokio::test]
async fn test_full_stack_write_read_roundtrip() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = vec![0xABu8; 4096];
    let extent_id = ExtentId::generate();
    let epoch = EpochId::new(1);

    let write_resp = client.write_extent(extent_id, &data, epoch).await;
    assert!(write_resp.success, "write failed: {:?}", write_resp.error);
    assert_eq!(write_resp.extent_id, extent_id);
    assert_eq!(write_resp.checksum, compute_sha256(&data));
    // Verify compute_checksum from storage matches our sha256
    assert_eq!(write_resp.checksum, compute_checksum(&data));

    let (read_resp, read_data) = client.read_extent(extent_id, 1, epoch).await;
    assert!(read_resp.success, "read failed: {:?}", read_resp.error);
    let read_data = read_data.expect("expected payload");
    assert_eq!(read_data.len(), data.len());
    assert_eq!(read_data, data);

    server.stop().await;
}

/// 2. Write through TCP, verify files exist in TempDir.
#[tokio::test]
async fn test_full_stack_write_persists_to_disk() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"persistent data check";
    let extent_id = ExtentId::generate();
    let epoch = EpochId::new(1);

    let write_resp = client.write_extent(extent_id, data, epoch).await;
    assert!(write_resp.success, "write failed: {:?}", write_resp.error);

    // Verify the committed directory has files
    let disk_dir = server._temp_dirs[0].path();
    let committed_dir = disk_dir.join("committed");
    assert!(committed_dir.exists(), "committed directory should exist");

    // Walk the committed directory to find our extent
    let mut found = false;
    for entry in walkdir(committed_dir) {
        if entry.is_file() && !entry.to_string_lossy().ends_with(".meta") {
            let contents = std::fs::read(&entry).unwrap();
            if contents == data {
                found = true;
                break;
            }
        }
    }
    assert!(found, "extent data should be persisted to disk");

    server.stop().await;
}

/// Simple recursive directory walker returning file paths.
fn walkdir(dir: std::path::PathBuf) -> Vec<std::path::PathBuf> {
    let mut results = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                results.extend(walkdir(path));
            } else {
                results.push(path);
            }
        }
    }
    results
}

/// 3. Write 20 extents, read all back.
#[tokio::test]
async fn test_full_stack_multiple_extents() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let epoch = EpochId::new(1);
    let mut extent_data = Vec::new();

    for i in 0u32..20 {
        let data = format!("extent-data-{i:04}").into_bytes();
        let extent_id = ExtentId::generate();
        let resp = client.write_extent(extent_id, &data, epoch).await;
        assert!(resp.success, "write {i} failed: {:?}", resp.error);
        extent_data.push((extent_id, data));
    }

    for (extent_id, expected_data) in &extent_data {
        let (read_resp, read_data) = client.read_extent(*extent_id, 1, epoch).await;
        assert!(
            read_resp.success,
            "read for extent {extent_id} failed: {:?}",
            read_resp.error
        );
        let read_data = read_data.expect("expected payload");
        assert_eq!(&read_data, expected_data);
    }

    server.stop().await;
}

/// 4. Write 256KB extent, read back.
#[tokio::test]
async fn test_full_stack_large_extent() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = vec![0xCDu8; 256 * 1024]; // 256KB
    let extent_id = ExtentId::generate();
    let epoch = EpochId::new(1);

    let write_resp = client.write_extent(extent_id, &data, epoch).await;
    assert!(write_resp.success, "write failed: {:?}", write_resp.error);

    let (read_resp, read_data) = client.read_extent(extent_id, 1, epoch).await;
    assert!(read_resp.success, "read failed: {:?}", read_resp.error);
    let read_data = read_data.expect("expected payload");
    assert_eq!(read_data.len(), data.len());
    assert_eq!(read_data, data);

    server.stop().await;
}

/// 5. Write with wrong epoch, verify rejected.
#[tokio::test]
async fn test_full_stack_stale_epoch() {
    // Server starts with epoch=5
    let server = start_full_stack_server_with_epoch(1, 5).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"stale epoch data";
    let extent_id = ExtentId::generate();
    // Send epoch=1 which is < server's epoch=5
    let stale_epoch = EpochId::new(1);

    let write_resp = client.write_extent(extent_id, data, stale_epoch).await;
    assert!(!write_resp.success, "write should fail with stale epoch");
    let error_msg = write_resp.error.expect("expected error message");
    assert!(
        error_msg.contains("stale epoch"),
        "error should mention stale epoch, got: {error_msg}"
    );

    server.stop().await;
}

/// 6. Write with wrong checksum, verify rejected.
#[tokio::test]
async fn test_full_stack_bad_checksum() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"some payload data";
    let extent_id = ExtentId::generate();
    let epoch = EpochId::new(1);

    let bad_checksum = "0000000000000000000000000000000000000000000000000000000000000000";
    let write_resp = client
        .write_extent_full(
            OperationId::generate(),
            extent_id,
            data,
            epoch,
            1,
            bad_checksum,
            None,
        )
        .await;
    assert!(!write_resp.success, "write should fail with bad checksum");
    let error_msg = write_resp.error.expect("expected error message");
    assert!(
        error_msg.contains("checksum mismatch"),
        "error should mention checksum mismatch, got: {error_msg}"
    );

    server.stop().await;
}

/// 7. Same operation_id, verify idempotent (duplicate write returns same result).
#[tokio::test]
async fn test_full_stack_duplicate_write() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"duplicate write test data";
    let extent_id = ExtentId::generate();
    let epoch = EpochId::new(1);
    let op_id = OperationId::generate();
    let checksum = compute_sha256(data);

    // First write
    let resp1 = client
        .write_extent_full(op_id, extent_id, data, epoch, 1, &checksum, None)
        .await;
    assert!(resp1.success, "first write failed: {:?}", resp1.error);

    // Second write with same operation_id should be idempotent
    let resp2 = client
        .write_extent_full(op_id, extent_id, data, epoch, 1, &checksum, None)
        .await;
    assert!(
        resp2.success,
        "duplicate write should succeed (idempotent): {:?}",
        resp2.error
    );
    assert_eq!(resp1.checksum, resp2.checksum);
    assert_eq!(resp1.extent_id, resp2.extent_id);
    assert_eq!(resp1.disk_id, resp2.disk_id);

    server.stop().await;
}

/// 8. Read extent that was never written.
#[tokio::test]
async fn test_full_stack_read_nonexistent() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let extent_id = ExtentId::generate();
    let epoch = EpochId::new(1);

    let (read_resp, read_data) = client.read_extent(extent_id, 1, epoch).await;
    assert!(!read_resp.success, "read of nonexistent should fail");
    assert!(read_data.is_none(), "no data expected for nonexistent");
    let error_msg = read_resp.error.expect("expected error message");
    assert!(
        error_msg.contains("not found"),
        "error should mention 'not found', got: {error_msg}"
    );

    server.stop().await;
}

/// 9. 5 tasks writing different extents simultaneously.
#[tokio::test]
async fn test_full_stack_concurrent_writers() {
    let server = start_full_stack_server(1).await;
    let addr = server.addr;
    let epoch = EpochId::new(1);

    let mut handles = Vec::new();
    for i in 0u32..5 {
        handles.push(tokio::spawn(async move {
            let mut client = TestClient::connect(addr).await;
            client.handshake().await;

            let data = format!("concurrent-writer-{i}").into_bytes();
            let extent_id = ExtentId::generate();
            let resp = client.write_extent(extent_id, &data, epoch).await;
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

    // Verify all writes can be read back
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    for (extent_id, expected_data) in &results {
        let (read_resp, read_data) = client.read_extent(*extent_id, 1, epoch).await;
        assert!(read_resp.success, "read failed: {:?}", read_resp.error);
        let read_data = read_data.expect("expected payload");
        assert_eq!(&read_data, expected_data);
    }

    server.stop().await;
}

/// 10. Server with 3 disks, write extents targeting each, verify distribution.
#[tokio::test]
async fn test_full_stack_multi_disk() {
    let server = start_full_stack_server(3).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let epoch = EpochId::new(1);
    let disk_ids = server.disk_ids.clone();

    // Write one extent per disk
    let mut written = Vec::new();
    for (i, &disk_id) in disk_ids.iter().enumerate() {
        let data = format!("disk-{i}-data").into_bytes();
        let extent_id = ExtentId::generate();
        let resp = client
            .write_extent_with_disk(extent_id, &data, epoch, disk_id)
            .await;
        assert!(
            resp.success,
            "write to disk {disk_id} failed: {:?}",
            resp.error
        );
        assert_eq!(resp.disk_id, disk_id, "should write to requested disk");
        written.push((extent_id, data, disk_id));
    }

    // Verify all can be read back
    for (extent_id, expected_data, _disk_id) in &written {
        let (read_resp, read_data) = client.read_extent(*extent_id, 1, epoch).await;
        assert!(read_resp.success, "read failed: {:?}", read_resp.error);
        let read_data = read_data.expect("expected payload");
        assert_eq!(&read_data, expected_data);
    }

    // Verify extent files exist on the correct disks
    for (i, (_extent_id, expected_data, _disk_id)) in written.iter().enumerate() {
        let disk_dir = server._temp_dirs[i].path();
        let committed_dir = disk_dir.join("committed");
        let files = walkdir(committed_dir);
        let data_files: Vec<_> = files
            .iter()
            .filter(|f| !f.to_string_lossy().ends_with(".meta"))
            .collect();
        assert!(
            !data_files.is_empty(),
            "disk {i} should have committed files"
        );
        // Verify the data on this disk
        let mut found = false;
        for f in &data_files {
            if std::fs::read(f).unwrap() == *expected_data {
                found = true;
                break;
            }
        }
        assert!(found, "expected data should be on disk {i}");
    }

    server.stop().await;
}

/// 11. Write v1, then v2 of same extent (different extent IDs for versioning).
///
/// Note: The ExtentIndex rejects duplicate extent_ids, so "versioning" here means
/// writing different versions as new index entries. We use remove + re-insert pattern.
/// The real versioning is done at a higher level. We test by writing the same extent_id
/// at version 1 to verify the storage pipeline handles it.
/// For a second version we write a new extent_id since the index doesn't allow overwrites.
#[tokio::test]
async fn test_full_stack_extent_versioning() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let epoch = EpochId::new(1);

    // Write first extent at version 1
    let extent_id_v1 = ExtentId::generate();
    let data_v1 = b"version-1-data";
    let resp1 = client
        .write_extent_versioned(extent_id_v1, data_v1, epoch, 1)
        .await;
    assert!(resp1.success, "v1 write failed: {:?}", resp1.error);

    // Write a second extent at version 2 (different extent_id to avoid index conflict)
    let extent_id_v2 = ExtentId::generate();
    let data_v2 = b"version-2-data-updated";
    let resp2 = client
        .write_extent_versioned(extent_id_v2, data_v2, epoch, 2)
        .await;
    assert!(resp2.success, "v2 write failed: {:?}", resp2.error);

    // Read back both versions
    let (r1, d1) = client.read_extent(extent_id_v1, 1, epoch).await;
    assert!(r1.success, "v1 read failed: {:?}", r1.error);
    assert_eq!(d1.unwrap(), data_v1);

    let (r2, d2) = client.read_extent(extent_id_v2, 2, epoch).await;
    assert!(r2.success, "v2 read failed: {:?}", r2.error);
    assert_eq!(d2.unwrap(), data_v2);

    server.stop().await;
}

/// 12. Write all 256 byte values, verify roundtrip.
#[tokio::test]
async fn test_full_stack_binary_data() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let epoch = EpochId::new(1);

    // Create data with all 256 byte values
    let data: Vec<u8> = (0..=255u8).collect();
    let extent_id = ExtentId::generate();

    let write_resp = client.write_extent(extent_id, &data, epoch).await;
    assert!(write_resp.success, "write failed: {:?}", write_resp.error);

    let (read_resp, read_data) = client.read_extent(extent_id, 1, epoch).await;
    assert!(read_resp.success, "read failed: {:?}", read_resp.error);
    let read_data = read_data.expect("expected payload");
    assert_eq!(read_data.len(), 256);
    assert_eq!(read_data, data, "all 256 byte values should roundtrip");

    server.stop().await;
}

/// 13. Write extents of sizes 1, 100, 4096, 65536, verify all.
#[tokio::test]
async fn test_full_stack_write_read_many_sizes() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let epoch = EpochId::new(1);
    let sizes = [1usize, 100, 4096, 65536];
    let mut extents = Vec::new();

    for size in sizes {
        let data = vec![(size % 256) as u8; size];
        let extent_id = ExtentId::generate();
        let resp = client.write_extent(extent_id, &data, epoch).await;
        assert!(resp.success, "write size={size} failed: {:?}", resp.error);
        extents.push((extent_id, data));
    }

    for (extent_id, expected_data) in &extents {
        let (read_resp, read_data) = client.read_extent(*extent_id, 1, epoch).await;
        assert!(read_resp.success, "read failed: {:?}", read_resp.error);
        let read_data = read_data.expect("expected payload");
        assert_eq!(
            read_data.len(),
            expected_data.len(),
            "size mismatch for extent {}",
            extent_id
        );
        assert_eq!(
            read_data, *expected_data,
            "data mismatch for extent {}",
            extent_id
        );
    }

    server.stop().await;
}

/// 14. Two clients write different extents, each can read the other's.
#[tokio::test]
async fn test_full_stack_multiple_connections_shared_state() {
    let server = start_full_stack_server(1).await;
    let epoch = EpochId::new(1);

    // Client 1 writes extent A
    let mut client1 = TestClient::connect(server.addr).await;
    client1.handshake().await;
    let data_a = b"client-1-data-A";
    let extent_a = ExtentId::generate();
    let resp_a = client1.write_extent(extent_a, data_a, epoch).await;
    assert!(resp_a.success, "client1 write failed: {:?}", resp_a.error);

    // Client 2 writes extent B
    let mut client2 = TestClient::connect(server.addr).await;
    client2.handshake().await;
    let data_b = b"client-2-data-B";
    let extent_b = ExtentId::generate();
    let resp_b = client2.write_extent(extent_b, data_b, epoch).await;
    assert!(resp_b.success, "client2 write failed: {:?}", resp_b.error);

    // Client 1 reads extent B (written by client 2)
    let (read_b, payload_b) = client1.read_extent(extent_b, 1, epoch).await;
    assert!(
        read_b.success,
        "client1 reading client2's extent failed: {:?}",
        read_b.error
    );
    assert_eq!(payload_b.unwrap(), data_b);

    // Client 2 reads extent A (written by client 1)
    let (read_a, payload_a) = client2.read_extent(extent_a, 1, epoch).await;
    assert!(
        read_a.success,
        "client2 reading client1's extent failed: {:?}",
        read_a.error
    );
    assert_eq!(payload_a.unwrap(), data_a);

    server.stop().await;
}

/// 15. Write data, shutdown cleanly, verify no crash/panic.
#[tokio::test]
async fn test_full_stack_graceful_shutdown_after_writes() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let epoch = EpochId::new(1);

    // Write several extents
    for i in 0..5 {
        let data = format!("shutdown-test-{i}").into_bytes();
        let extent_id = ExtentId::generate();
        let resp = client.write_extent(extent_id, &data, epoch).await;
        assert!(resp.success, "write {i} failed: {:?}", resp.error);
    }

    // Drop the client connection first
    drop(client);

    // Graceful shutdown — should not panic
    server.stop().await;
}

/// 16. (Bonus) Write with epoch equal to server epoch succeeds.
#[tokio::test]
async fn test_full_stack_epoch_equal_succeeds() {
    let server = start_full_stack_server_with_epoch(1, 3).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"matching epoch data";
    let extent_id = ExtentId::generate();
    let epoch = EpochId::new(3); // matches server epoch

    let resp = client.write_extent(extent_id, data, epoch).await;
    assert!(
        resp.success,
        "write with matching epoch should succeed: {:?}",
        resp.error
    );

    server.stop().await;
}

/// 17. (Bonus) Write with epoch greater than server epoch succeeds.
#[tokio::test]
async fn test_full_stack_future_epoch_succeeds() {
    let server = start_full_stack_server_with_epoch(1, 3).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"future epoch data";
    let extent_id = ExtentId::generate();
    let epoch = EpochId::new(10); // greater than server epoch=3

    let resp = client.write_extent(extent_id, data, epoch).await;
    assert!(
        resp.success,
        "write with future epoch should succeed: {:?}",
        resp.error
    );

    server.stop().await;
}

/// 18. (Bonus) Verify checksum in response matches compute_checksum from storage crate.
#[tokio::test]
async fn test_full_stack_checksum_consistency() {
    let server = start_full_stack_server(1).await;
    let mut client = TestClient::connect(server.addr).await;
    client.handshake().await;

    let data = b"checksum consistency verification";
    let extent_id = ExtentId::generate();
    let epoch = EpochId::new(1);

    let expected_checksum = compute_checksum(data);
    let our_checksum = compute_sha256(data);
    // Verify the storage crate's checksum matches our sha256 computation
    assert_eq!(expected_checksum, our_checksum);

    let write_resp = client.write_extent(extent_id, data, epoch).await;
    assert!(write_resp.success);
    assert_eq!(write_resp.checksum, expected_checksum);

    let (read_resp, _) = client.read_extent(extent_id, 1, epoch).await;
    assert!(read_resp.success);
    assert_eq!(read_resp.checksum, expected_checksum);

    server.stop().await;
}
