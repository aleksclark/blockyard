//! TCP data plane server for extent read/write operations.
//!
//! Implements a length-prefixed JSON framing protocol with raw payload
//! transfer for write operations. Uses a [`DataPlaneHandler`] trait
//! to decouple from the storage layer.

use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use blockyard_common::{AuthProvider, NodeId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::messages::{
    ProtocolMessage, ReadExtentRequest, ReadExtentResponse, WriteExtentRequest, WriteExtentResponse,
};
use crate::version::negotiate_version_with_auth;

/// Error type for data plane server operations.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// I/O error from TCP operations.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Protocol-level error (bad framing, unexpected message, etc).
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// Maximum frame size: 64 MiB (matches default protocol.max_message_size).
const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;

/// Trait for handling data plane requests.
///
/// Implemented by the binary crate's wrapper around `DataNodeService`.
pub trait DataPlaneHandler: Send + Sync + Debug + 'static {
    /// Handle a write extent request with the given payload.
    fn handle_write(&self, request: &WriteExtentRequest, payload: &[u8]) -> WriteExtentResponse;

    /// Handle a read extent request. Returns the response and optional data payload.
    fn handle_read(&self, request: &ReadExtentRequest) -> (ReadExtentResponse, Option<Vec<u8>>);
}

/// TCP server for the data plane (extent read/write).
#[derive(Debug)]
pub struct DataPlaneServer<H: DataPlaneHandler> {
    listener: TcpListener,
    handler: Arc<H>,
    node_id: NodeId,
    auth_provider: Option<Arc<dyn AuthProvider>>,
}

impl<H: DataPlaneHandler> DataPlaneServer<H> {
    /// Bind the data plane server to the given address.
    pub async fn bind(
        addr: SocketAddr,
        handler: Arc<H>,
        node_id: NodeId,
    ) -> Result<Self, ServerError> {
        let listener = TcpListener::bind(addr).await?;
        info!(%addr, "data plane server bound");
        Ok(Self {
            listener,
            handler,
            node_id,
            auth_provider: None,
        })
    }

    /// Bind with an auth provider that enforces authentication on handshake (§8).
    pub async fn bind_with_auth(
        addr: SocketAddr,
        handler: Arc<H>,
        node_id: NodeId,
        auth_provider: Arc<dyn AuthProvider>,
    ) -> Result<Self, ServerError> {
        let listener = TcpListener::bind(addr).await?;
        info!(%addr, "data plane server bound (with auth)");
        Ok(Self {
            listener,
            handler,
            node_id,
            auth_provider: Some(auth_provider),
        })
    }

    /// Return the local address the server is listening on.
    pub fn local_addr(&self) -> Result<SocketAddr, ServerError> {
        Ok(self.listener.local_addr()?)
    }

    /// Run the server until the cancellation token is cancelled.
    pub async fn run(self, shutdown: CancellationToken) {
        info!(
            addr = %self.listener.local_addr().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0))),
            "data plane server running"
        );

        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    info!("data plane server shutting down");
                    break;
                }
                result = self.listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            debug!(%peer_addr, "accepted connection");
                            let handler = Arc::clone(&self.handler);
                            let node_id = self.node_id;
                            let auth = self.auth_provider.clone();
                            let token = shutdown.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, handler, node_id, auth, token).await {
                                    warn!(%peer_addr, error = %e, "connection error");
                                }
                            });
                        }
                        Err(e) => {
                            error!(error = %e, "failed to accept connection");
                        }
                    }
                }
            }
        }
    }
}

/// Read a length-prefixed frame from the stream.
///
/// Frame format: 4-byte big-endian length prefix, then `length` bytes of payload.
async fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, ServerError> {
    let len = stream.read_u32().await?;
    if len > MAX_FRAME_SIZE {
        return Err(ServerError::Protocol(format!(
            "frame too large: {len} > {MAX_FRAME_SIZE}"
        )));
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Write a length-prefixed frame to the stream.
async fn write_frame(stream: &mut TcpStream, data: &[u8]) -> Result<(), ServerError> {
    let len = data.len() as u32;
    stream.write_u32(len).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}

/// Handle a single client connection.
async fn handle_connection<H: DataPlaneHandler>(
    mut stream: TcpStream,
    handler: Arc<H>,
    node_id: NodeId,
    auth_provider: Option<Arc<dyn AuthProvider>>,
    shutdown: CancellationToken,
) -> Result<(), ServerError> {
    // Step 1: Read handshake request
    let frame = read_frame(&mut stream).await?;
    let handshake_req = serde_json::from_slice(&frame)?;

    // Step 2: Negotiate version with auth
    let auth_ref: Option<&dyn AuthProvider> = auth_provider.as_deref();
    let (handshake_resp, _peer_identity) =
        negotiate_version_with_auth(&handshake_req, node_id, auth_ref);
    let accepted = handshake_resp.accepted;

    // Step 3: Send handshake response
    let resp_bytes = serde_json::to_vec(&handshake_resp)?;
    write_frame(&mut stream, &resp_bytes).await?;

    if !accepted {
        debug!("handshake rejected, closing connection");
        return Ok(());
    }

    // Step 4: Request loop
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                debug!("connection shutting down");
                break;
            }
            result = read_frame(&mut stream) => {
                let frame = match result {
                    Ok(f) => f,
                    Err(ServerError::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        debug!("client disconnected");
                        break;
                    }
                    Err(e) => return Err(e),
                };

                let msg: ProtocolMessage = serde_json::from_slice(&frame)?;
                match msg {
                    ProtocolMessage::WriteReq(req) => {
                        // Read raw payload bytes
                        let payload_size = req.payload_size as usize;
                        let mut payload = vec![0u8; payload_size];
                        stream.read_exact(&mut payload).await?;

                        let resp = handler.handle_write(&req, &payload);
                        let resp_msg = ProtocolMessage::WriteResp(resp);
                        let resp_bytes = serde_json::to_vec(&resp_msg)?;
                        write_frame(&mut stream, &resp_bytes).await?;
                    }
                    ProtocolMessage::ReadReq(req) => {
                        let (resp, data) = handler.handle_read(&req);
                        let resp_msg = ProtocolMessage::ReadResp(resp);
                        let resp_bytes = serde_json::to_vec(&resp_msg)?;
                        write_frame(&mut stream, &resp_bytes).await?;

                        // Send raw data payload if present
                        if let Some(payload) = data {
                            stream.write_all(&payload).await?;
                            stream.flush().await?;
                        }
                    }
                    other => {
                        return Err(ServerError::Protocol(format!(
                            "unexpected message in request loop: {other:?}"
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockyard_common::{DiskId, ExtentId, NodeId, OperationId, SessionId, VolumeId};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::net::SocketAddr;

    use crate::messages::{
        CURRENT_PROTOCOL_VERSION, HandshakeRequest, HandshakeResponse, ReadExtentRequest,
        ReadExtentResponse, WriteExtentRequest, WriteExtentResponse,
    };

    /// A simple in-memory fake handler for testing the server protocol layer.
    #[derive(Debug)]
    struct FakeHandler {
        /// Store extent data in memory: (extent_id, version) -> (data, checksum)
        extents: RwLock<HashMap<(ExtentId, u64), (Vec<u8>, String)>>,
    }

    #[allow(clippy::type_complexity)]
    impl FakeHandler {
        fn new() -> Self {
            Self {
                extents: RwLock::new(HashMap::new()),
            }
        }
    }

    impl DataPlaneHandler for FakeHandler {
        fn handle_write(
            &self,
            request: &WriteExtentRequest,
            payload: &[u8],
        ) -> WriteExtentResponse {
            // Compute checksum
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(payload);
            let computed = format!("{:x}", hasher.finalize());

            if request.checksum != computed {
                return WriteExtentResponse {
                    operation_id: request.operation_id,
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
            }

            if request.epoch.as_u64() < 2 {
                // Simulate stale epoch when epoch < 2
                // (use epoch >= 2 threshold to test stale epoch)
            }

            let disk_id = request.target_disk_id.unwrap_or_else(DiskId::generate);
            self.extents.write().insert(
                (request.extent_id, request.extent_version),
                (payload.to_vec(), computed.clone()),
            );

            WriteExtentResponse {
                operation_id: request.operation_id,
                extent_id: request.extent_id,
                extent_version: request.extent_version,
                disk_id,
                success: true,
                checksum: computed,
                error: None,
            }
        }

        fn handle_read(
            &self,
            request: &ReadExtentRequest,
        ) -> (ReadExtentResponse, Option<Vec<u8>>) {
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

    fn make_test_handler() -> Arc<FakeHandler> {
        Arc::new(FakeHandler::new())
    }

    fn make_handshake_request() -> HandshakeRequest {
        HandshakeRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            node_id: None,
            session_id: None,
            features: vec![],
            auth_token: None,
        }
    }

    fn compute_sha256(data: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    async fn client_write_frame(stream: &mut TcpStream, data: &[u8]) {
        stream.write_u32(data.len() as u32).await.unwrap();
        stream.write_all(data).await.unwrap();
        stream.flush().await.unwrap();
    }

    async fn client_read_frame(stream: &mut TcpStream) -> Vec<u8> {
        let len = stream.read_u32().await.unwrap();
        let mut buf = vec![0u8; len as usize];
        stream.read_exact(&mut buf).await.unwrap();
        buf
    }

    async fn do_handshake(stream: &mut TcpStream) -> HandshakeResponse {
        let req = make_handshake_request();
        let req_bytes = serde_json::to_vec(&req).unwrap();
        client_write_frame(stream, &req_bytes).await;
        let resp_bytes = client_read_frame(stream).await;
        serde_json::from_slice(&resp_bytes).unwrap()
    }

    #[tokio::test]
    async fn test_bind_and_local_addr() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let server = DataPlaneServer::bind(addr, handler, node_id).await.unwrap();
        let local = server.local_addr().unwrap();
        assert_ne!(local.port(), 0);
    }

    #[tokio::test]
    async fn test_handshake_accepted() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, handler, node_id).await.unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        let resp = do_handshake(&mut stream).await;
        assert!(resp.accepted);
        assert_eq!(resp.protocol_version, CURRENT_PROTOCOL_VERSION);
        assert_eq!(resp.node_id, node_id);

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_handshake_rejected_bad_version() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, handler, node_id).await.unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        let req = HandshakeRequest {
            protocol_version: 0, // below MIN_PROTOCOL_VERSION
            node_id: None,
            session_id: None,
            features: vec![],
            auth_token: None,
        };
        let req_bytes = serde_json::to_vec(&req).unwrap();
        client_write_frame(&mut stream, &req_bytes).await;
        let resp_bytes = client_read_frame(&mut stream).await;
        let resp: HandshakeResponse = serde_json::from_slice(&resp_bytes).unwrap();
        assert!(!resp.accepted);
        assert!(resp.message.is_some());

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_write_and_read_extent() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, Arc::clone(&handler), node_id)
            .await
            .unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        let resp = do_handshake(&mut stream).await;
        assert!(resp.accepted);

        // Write an extent
        let payload = b"hello blockyard data plane";
        let checksum = compute_sha256(payload);

        let extent_id = ExtentId::generate();
        let write_req = WriteExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id,
            extent_version: 1,
            epoch: blockyard_common::EpochId::new(1),
            target_disk_id: None,
            checksum: checksum.clone(),
            payload_size: payload.len() as u64,
            lease_version: None,
        };
        let msg = ProtocolMessage::WriteReq(write_req);
        let msg_bytes = serde_json::to_vec(&msg).unwrap();
        client_write_frame(&mut stream, &msg_bytes).await;

        // Send raw payload
        stream.write_all(payload).await.unwrap();
        stream.flush().await.unwrap();

        // Read write response
        let resp_bytes = client_read_frame(&mut stream).await;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes).unwrap();
        match resp_msg {
            ProtocolMessage::WriteResp(wr) => {
                assert!(wr.success, "write failed: {:?}", wr.error);
                assert_eq!(wr.checksum, checksum);
            }
            other => panic!("expected WriteResp, got {other:?}"),
        }

        // Now read the extent back
        let read_req = ReadExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id,
            extent_version: 1,
            epoch: blockyard_common::EpochId::new(1),
            offset: 0,
            length: 0, // read entire extent
        };
        let msg = ProtocolMessage::ReadReq(read_req);
        let msg_bytes = serde_json::to_vec(&msg).unwrap();
        client_write_frame(&mut stream, &msg_bytes).await;

        // Read response
        let resp_bytes = client_read_frame(&mut stream).await;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes).unwrap();
        match resp_msg {
            ProtocolMessage::ReadResp(rr) => {
                assert!(rr.success, "read failed: {:?}", rr.error);
                assert_eq!(rr.payload_size, payload.len() as u64);

                // Read raw data payload
                let mut data = vec![0u8; rr.payload_size as usize];
                stream.read_exact(&mut data).await.unwrap();
                assert_eq!(&data, payload);
            }
            other => panic!("expected ReadResp, got {other:?}"),
        }

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_read_nonexistent_extent() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, handler, node_id).await.unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        do_handshake(&mut stream).await;

        let read_req = ReadExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            epoch: blockyard_common::EpochId::new(1),
            offset: 0,
            length: 0,
        };
        let msg = ProtocolMessage::ReadReq(read_req);
        let msg_bytes = serde_json::to_vec(&msg).unwrap();
        client_write_frame(&mut stream, &msg_bytes).await;

        let resp_bytes = client_read_frame(&mut stream).await;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes).unwrap();
        match resp_msg {
            ProtocolMessage::ReadResp(rr) => {
                assert!(!rr.success);
                assert!(rr.error.is_some());
                assert_eq!(rr.payload_size, 0);
            }
            other => panic!("expected ReadResp, got {other:?}"),
        }

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_client_disconnect_graceful() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, handler, node_id).await.unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        do_handshake(&mut stream).await;

        // Just drop the stream (client disconnects)
        drop(stream);

        // Give the server a moment to handle the disconnect
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_unexpected_message_type() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, handler, node_id).await.unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        do_handshake(&mut stream).await;

        // Send a HandshakeReq in the request loop (unexpected)
        let bad_msg = ProtocolMessage::HandshakeReq(make_handshake_request());
        let msg_bytes = serde_json::to_vec(&bad_msg).unwrap();
        client_write_frame(&mut stream, &msg_bytes).await;

        // The server should close the connection
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Trying to read should fail or get EOF
        let result = stream.read_u32().await;
        assert!(result.is_err());

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_write_with_bad_checksum() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, handler, node_id).await.unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        do_handshake(&mut stream).await;

        let payload = b"data with wrong checksum";
        let write_req = WriteExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            epoch: blockyard_common::EpochId::new(1),
            target_disk_id: None,
            checksum: "definitely_wrong_checksum".into(),
            payload_size: payload.len() as u64,
            lease_version: None,
        };
        let msg = ProtocolMessage::WriteReq(write_req);
        let msg_bytes = serde_json::to_vec(&msg).unwrap();
        client_write_frame(&mut stream, &msg_bytes).await;
        stream.write_all(payload).await.unwrap();
        stream.flush().await.unwrap();

        let resp_bytes = client_read_frame(&mut stream).await;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes).unwrap();
        match resp_msg {
            ProtocolMessage::WriteResp(wr) => {
                assert!(!wr.success);
                assert!(wr.error.as_deref().unwrap().contains("checksum"));
            }
            other => panic!("expected WriteResp, got {other:?}"),
        }

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_shutdown_stops_server() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, handler, node_id).await.unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        shutdown.cancel();
        // Server should exit promptly
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("server didn't shut down in time")
            .unwrap();
    }

    #[tokio::test]
    async fn test_multiple_operations_per_connection() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, Arc::clone(&handler), node_id)
            .await
            .unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        do_handshake(&mut stream).await;

        // Perform multiple write operations
        for i in 0..3 {
            let payload = format!("payload {i}");
            let payload_bytes = payload.as_bytes();
            let checksum = compute_sha256(payload_bytes);

            let write_req = WriteExtentRequest {
                operation_id: OperationId::generate(),
                session_id: SessionId::generate(),
                volume_id: VolumeId::generate(),
                extent_id: ExtentId::generate(),
                extent_version: 1,
                epoch: blockyard_common::EpochId::new(1),
                target_disk_id: None,
                checksum,
                payload_size: payload_bytes.len() as u64,
                lease_version: None,
            };
            let msg = ProtocolMessage::WriteReq(write_req);
            let msg_bytes = serde_json::to_vec(&msg).unwrap();
            client_write_frame(&mut stream, &msg_bytes).await;
            stream.write_all(payload_bytes).await.unwrap();
            stream.flush().await.unwrap();

            let resp_bytes = client_read_frame(&mut stream).await;
            let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes).unwrap();
            match resp_msg {
                ProtocolMessage::WriteResp(wr) => {
                    assert!(wr.success, "write {i} failed: {:?}", wr.error);
                }
                other => panic!("expected WriteResp, got {other:?}"),
            }
        }

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_read_frame_too_large() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, handler, node_id).await.unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();

        // Send an oversized frame length (bigger than MAX_FRAME_SIZE)
        stream.write_u32(MAX_FRAME_SIZE + 1).await.unwrap();
        stream.flush().await.unwrap();

        // Server should drop the connection
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let result = stream.read_u32().await;
        assert!(result.is_err());

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_server_error_display() {
        let io_err = ServerError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert!(io_err.to_string().contains("reset"));

        let json_err =
            ServerError::Json(serde_json::from_str::<HandshakeRequest>("bad json").unwrap_err());
        assert!(json_err.to_string().contains("json"));

        let proto_err = ServerError::Protocol("bad frame".into());
        assert!(proto_err.to_string().contains("bad frame"));
    }

    #[tokio::test]
    async fn test_server_error_debug() {
        let err = ServerError::Protocol("test".into());
        let debug = format!("{err:?}");
        assert!(debug.contains("Protocol"));
    }

    #[tokio::test]
    async fn test_write_frame_and_read_frame_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let frame = read_frame(&mut stream).await.unwrap();
            write_frame(&mut stream, &frame).await.unwrap();
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let test_data = b"roundtrip test data";
        client_write_frame(&mut stream, test_data).await;
        let echoed = client_read_frame(&mut stream).await;
        assert_eq!(&echoed, test_data);

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_read_partial_extent_via_server() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, Arc::clone(&handler), node_id)
            .await
            .unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        do_handshake(&mut stream).await;

        // Write an extent first
        let payload = b"ABCDEFGHIJ";
        let checksum = compute_sha256(payload);

        let extent_id = ExtentId::generate();
        let write_req = WriteExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id,
            extent_version: 1,
            epoch: blockyard_common::EpochId::new(1),
            target_disk_id: None,
            checksum,
            payload_size: payload.len() as u64,
            lease_version: None,
        };
        let msg = ProtocolMessage::WriteReq(write_req);
        let msg_bytes = serde_json::to_vec(&msg).unwrap();
        client_write_frame(&mut stream, &msg_bytes).await;
        stream.write_all(payload).await.unwrap();
        stream.flush().await.unwrap();

        let resp_bytes = client_read_frame(&mut stream).await;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes).unwrap();
        assert!(matches!(
            resp_msg,
            ProtocolMessage::WriteResp(ref w) if w.success
        ));

        // Read partial extent (offset=2, length=5 => "CDEFG")
        let read_req = ReadExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id,
            extent_version: 1,
            epoch: blockyard_common::EpochId::new(1),
            offset: 2,
            length: 5,
        };
        let msg = ProtocolMessage::ReadReq(read_req);
        let msg_bytes = serde_json::to_vec(&msg).unwrap();
        client_write_frame(&mut stream, &msg_bytes).await;

        let resp_bytes = client_read_frame(&mut stream).await;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes).unwrap();
        match resp_msg {
            ProtocolMessage::ReadResp(rr) => {
                assert!(rr.success);
                assert_eq!(rr.payload_size, 5);
                let mut data = vec![0u8; 5];
                stream.read_exact(&mut data).await.unwrap();
                assert_eq!(&data, b"CDEFG");
            }
            other => panic!("expected ReadResp, got {other:?}"),
        }

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_concurrent_connections() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, handler, node_id).await.unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        // Open 3 concurrent connections and handshake each
        let mut handles = vec![];
        for _ in 0..3 {
            let local_copy = local;
            handles.push(tokio::spawn(async move {
                let mut stream = TcpStream::connect(local_copy).await.unwrap();
                let resp = do_handshake(&mut stream).await;
                assert!(resp.accepted);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_zero_payload_write() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, Arc::clone(&handler), node_id)
            .await
            .unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        do_handshake(&mut stream).await;

        let payload = b"";
        let checksum = compute_sha256(payload);

        let write_req = WriteExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            epoch: blockyard_common::EpochId::new(1),
            target_disk_id: None,
            checksum,
            payload_size: 0,
            lease_version: None,
        };
        let msg = ProtocolMessage::WriteReq(write_req);
        let msg_bytes = serde_json::to_vec(&msg).unwrap();
        client_write_frame(&mut stream, &msg_bytes).await;
        // No payload bytes to send (payload_size == 0)

        let resp_bytes = client_read_frame(&mut stream).await;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes).unwrap();
        match resp_msg {
            ProtocolMessage::WriteResp(wr) => {
                assert!(wr.success, "write failed: {:?}", wr.error);
            }
            other => panic!("expected WriteResp, got {other:?}"),
        }

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_handshake_response_has_features() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, handler, node_id).await.unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        let req = HandshakeRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            node_id: None,
            session_id: None,
            features: vec!["compression".into()],
            auth_token: None,
        };
        let req_bytes = serde_json::to_vec(&req).unwrap();
        client_write_frame(&mut stream, &req_bytes).await;
        let resp_bytes = client_read_frame(&mut stream).await;
        let resp: HandshakeResponse = serde_json::from_slice(&resp_bytes).unwrap();
        assert!(resp.accepted);
        // Features list is returned empty for now
        assert!(resp.supported_features.is_empty());

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_read_out_of_range() {
        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = DataPlaneServer::bind(addr, Arc::clone(&handler), node_id)
            .await
            .unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        do_handshake(&mut stream).await;

        // Write a small extent
        let payload = b"tiny";
        let checksum = compute_sha256(payload);
        let extent_id = ExtentId::generate();

        let write_req = WriteExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id,
            extent_version: 1,
            epoch: blockyard_common::EpochId::new(1),
            target_disk_id: None,
            checksum,
            payload_size: payload.len() as u64,
            lease_version: None,
        };
        let msg = ProtocolMessage::WriteReq(write_req);
        let msg_bytes = serde_json::to_vec(&msg).unwrap();
        client_write_frame(&mut stream, &msg_bytes).await;
        stream.write_all(payload).await.unwrap();
        stream.flush().await.unwrap();
        let _ = client_read_frame(&mut stream).await;

        // Read out of range
        let read_req = ReadExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id,
            extent_version: 1,
            epoch: blockyard_common::EpochId::new(1),
            offset: 0,
            length: 100, // way past end
        };
        let msg = ProtocolMessage::ReadReq(read_req);
        let msg_bytes = serde_json::to_vec(&msg).unwrap();
        client_write_frame(&mut stream, &msg_bytes).await;

        let resp_bytes = client_read_frame(&mut stream).await;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes).unwrap();
        match resp_msg {
            ProtocolMessage::ReadResp(rr) => {
                assert!(!rr.success);
                assert!(rr.error.is_some());
            }
            other => panic!("expected ReadResp, got {other:?}"),
        }

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_handshake_rejected_without_auth_token_when_auth_required() {
        use blockyard_common::SharedSecretAuth;

        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let auth: Arc<dyn AuthProvider> =
            Arc::new(SharedSecretAuth::new("test-server-secret").unwrap());
        let server = DataPlaneServer::bind_with_auth(addr, handler, node_id, auth)
            .await
            .unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        let req = HandshakeRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            node_id: None,
            session_id: None,
            features: vec![],
            auth_token: None,
        };
        let req_bytes = serde_json::to_vec(&req).unwrap();
        client_write_frame(&mut stream, &req_bytes).await;
        let resp_bytes = client_read_frame(&mut stream).await;
        let resp: HandshakeResponse = serde_json::from_slice(&resp_bytes).unwrap();
        assert!(
            !resp.accepted,
            "handshake should be rejected without auth token"
        );
        assert!(!resp.authenticated);
        assert!(resp.message.unwrap().contains("no token"));

        shutdown.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_handshake_accepted_with_valid_auth_token() {
        use blockyard_common::{PeerIdentity, SharedSecretAuth};

        let handler = make_test_handler();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let shared_auth = SharedSecretAuth::new("test-server-secret-2").unwrap();
        let sid = SessionId::generate();
        let peer = PeerIdentity::Client(sid);
        let token = shared_auth.create_token(&peer, 300_000).unwrap();
        let auth: Arc<dyn AuthProvider> = Arc::new(shared_auth);
        let server = DataPlaneServer::bind_with_auth(addr, handler, node_id, Arc::clone(&auth))
            .await
            .unwrap();
        let local = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();

        let handle = tokio::spawn(async move {
            server.run(shutdown2).await;
        });

        let mut stream = TcpStream::connect(local).await.unwrap();
        let req = HandshakeRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            node_id: None,
            session_id: Some(sid),
            features: vec![],
            auth_token: Some(token),
        };
        let req_bytes = serde_json::to_vec(&req).unwrap();
        client_write_frame(&mut stream, &req_bytes).await;
        let resp_bytes = client_read_frame(&mut stream).await;
        let resp: HandshakeResponse = serde_json::from_slice(&resp_bytes).unwrap();
        assert!(
            resp.accepted,
            "handshake should be accepted with valid token"
        );
        assert!(resp.authenticated);

        shutdown.cancel();
        handle.await.unwrap();
    }
}
