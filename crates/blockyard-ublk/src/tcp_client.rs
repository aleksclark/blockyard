//! TCP-based [`DataNodeClient`] implementation.
//!
//! Connects to data node TCP servers using the same length-prefixed JSON
//! framing as [`DataPlaneServer`](blockyard_protocol::DataPlaneServer):
//! 4-byte big-endian length + JSON envelope, with raw payload following
//! write request frames.
//!
//! Maintains a connection pool with one connection per node, automatically
//! reconnecting on failure.

use std::collections::HashMap;
use std::net::SocketAddr;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex as TokioMutex;

use blockyard_common::error::Error;
use blockyard_common::{EpochId, ExtentId, NodeId, OperationId, SessionId, VolumeId};
use blockyard_client::error::ReadError;
use blockyard_client::traits::DataNodeReader;
use blockyard_client::types::DataNodeReadResult;
use blockyard_protocol::messages::{
    CURRENT_PROTOCOL_VERSION, HandshakeRequest, HandshakeResponse, ProtocolMessage,
    ReadExtentRequest, WriteExtentRequest, WriteExtentResponse,
};

use crate::traits::{DataNodeClient, WriteAck, WriteAckError};

/// Maximum frame size: 64 MiB (matches server's MAX_FRAME_SIZE).
const MAX_FRAME_SIZE: u32 = 64 * 1024 * 1024;

/// A pooled TCP connection to a data node.
struct TcpConnection {
    addr: SocketAddr,
    stream: TokioMutex<TcpStream>,
    handshake_done: bool,
}

impl std::fmt::Debug for TcpConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpConnection")
            .field("addr", &self.addr)
            .field("handshake_done", &self.handshake_done)
            .finish()
    }
}

/// TCP-based data node client with connection pooling.
///
/// Implements [`DataNodeClient`] by connecting to data node TCP servers
/// and using the standard length-prefixed JSON framing protocol.
#[derive(Debug)]
pub struct TcpDataNodeClient {
    /// Map from NodeId -> SocketAddr for resolving node addresses.
    node_addresses: RwLock<HashMap<String, SocketAddr>>,
    /// Pooled connections: one per node, reconnect on failure.
    connections: RwLock<HashMap<String, std::sync::Arc<TcpConnection>>>,
}

impl TcpDataNodeClient {
    /// Create a new TCP data node client with no known node addresses.
    pub fn new() -> Self {
        Self {
            node_addresses: RwLock::new(HashMap::new()),
            connections: RwLock::new(HashMap::new()),
        }
    }

    /// Register a node's data plane address.
    pub fn register_node(&self, node_id: NodeId, addr: SocketAddr) {
        self.node_addresses
            .write()
            .insert(node_id.to_string(), addr);
    }

    /// Remove a node's registered address and drop its connection.
    pub fn unregister_node(&self, node_id: &NodeId) {
        let key = node_id.to_string();
        self.node_addresses.write().remove(&key);
        self.connections.write().remove(&key);
    }

    /// Get or create a connection to the specified node.
    async fn get_connection(
        &self,
        node_id: NodeId,
    ) -> Result<std::sync::Arc<TcpConnection>, Error> {
        let key = node_id.to_string();

        // Check for existing connection
        {
            let conns = self.connections.read();
            if let Some(conn) = conns.get(&key) {
                if conn.handshake_done {
                    return Ok(std::sync::Arc::clone(conn));
                }
            }
        }

        // Need to create a new connection
        let addr = {
            let addrs = self.node_addresses.read();
            addrs.get(&key).copied().ok_or_else(|| {
                Error::Network(format!("no address registered for node {node_id}"))
            })?
        };

        let conn = self.connect_and_handshake(addr).await?;
        let conn = std::sync::Arc::new(conn);
        self.connections
            .write()
            .insert(key, std::sync::Arc::clone(&conn));
        Ok(conn)
    }

    /// Establish a TCP connection and perform the protocol handshake.
    async fn connect_and_handshake(&self, addr: SocketAddr) -> Result<TcpConnection, Error> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| Error::Network(format!("failed to connect to {addr}: {e}")))?;

        let mut conn = TcpConnection {
            addr,
            stream: TokioMutex::new(stream),
            handshake_done: false,
        };

        // Perform handshake
        {
            let mut stream = conn.stream.lock().await;
            let handshake_req = HandshakeRequest {
                protocol_version: CURRENT_PROTOCOL_VERSION,
                node_id: None,
                session_id: None,
                features: vec![],
                auth_token: None,
            };
            let req_bytes = serde_json::to_vec(&handshake_req).map_err(|e| {
                Error::Network(format!("failed to serialize handshake request: {e}"))
            })?;
            write_frame(&mut stream, &req_bytes).await?;

            let resp_bytes = read_frame(&mut stream).await?;
            let resp: HandshakeResponse = serde_json::from_slice(&resp_bytes).map_err(|e| {
                Error::Network(format!("failed to deserialize handshake response: {e}"))
            })?;

            if !resp.accepted {
                return Err(Error::Network(format!(
                    "handshake rejected by {addr}: {}",
                    resp.message.unwrap_or_default()
                )));
            }
        }

        conn.handshake_done = true;
        Ok(conn)
    }

    /// Remove a connection from the pool (e.g., on failure).
    fn drop_connection(&self, node_id: &NodeId) {
        self.connections.write().remove(&node_id.to_string());
    }

    /// Read extent data from a data node via TCP.
    pub async fn read_extent(
        &self,
        node_id: NodeId,
        volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, Error> {
        let result = self
            .try_read_extent(node_id, volume_id, extent_id, extent_version, offset, length)
            .await;

        match result {
            Ok(data) => Ok(data),
            Err(_) => {
                self.drop_connection(&node_id);
                self.try_read_extent(node_id, volume_id, extent_id, extent_version, offset, length)
                    .await
            }
        }
    }

    async fn try_read_extent(
        &self,
        node_id: NodeId,
        volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, Error> {
        let conn = self.get_connection(node_id).await?;
        let mut stream = conn.stream.lock().await;

        let read_req = ReadExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id,
            extent_id,
            extent_version,
            epoch: EpochId::new(0),
            offset,
            length,
        };

        let msg = ProtocolMessage::ReadReq(read_req);
        let msg_bytes = serde_json::to_vec(&msg)
            .map_err(|e| Error::Network(format!("failed to serialize read request: {e}")))?;

        write_frame(&mut stream, &msg_bytes).await?;

        let resp_bytes = read_frame(&mut stream).await?;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes)
            .map_err(|e| Error::Network(format!("failed to deserialize read response: {e}")))?;

        match resp_msg {
            ProtocolMessage::ReadResp(resp) => {
                if !resp.success {
                    return Err(Error::Storage(format!(
                        "read failed on node {}: {}",
                        node_id,
                        resp.error.unwrap_or_default()
                    )));
                }

                if resp.payload_size > 0 {
                    let mut payload = vec![0u8; resp.payload_size as usize];
                    stream.read_exact(&mut payload).await.map_err(|e| {
                        Error::Network(format!("failed to read payload: {e}"))
                    })?;
                    Ok(Bytes::from(payload))
                } else {
                    Ok(Bytes::new())
                }
            }
            other => Err(Error::Network(format!(
                "unexpected response type: {other:?}"
            ))),
        }
    }
}

impl Default for TcpDataNodeClient {
    fn default() -> Self {
        Self::new()
    }
}

impl DataNodeReader for TcpDataNodeClient {
    async fn read_extent(
        &self,
        node_id: NodeId,
        volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        offset: u64,
        length: u64,
    ) -> Result<DataNodeReadResult, ReadError> {
        let data = TcpDataNodeClient::read_extent(
            self, node_id, volume_id, extent_id, extent_version, offset, length,
        )
        .await
        .map_err(|e| ReadError::DataNodeReadFailed {
            node_id,
            reason: e.to_string(),
        })?;

        let checksum = blockyard_common::checksum::compute_checksum(&data);

        Ok(DataNodeReadResult {
            extent_id,
            extent_version,
            checksum,
            data,
        })
    }
}

impl DataNodeClient for TcpDataNodeClient {
    #[allow(clippy::too_many_arguments)]
    async fn write_extent(
        &self,
        node_id: NodeId,
        operation_id: OperationId,
        session_id: SessionId,
        volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        epoch: EpochId,
        data: Bytes,
        checksum: String,
    ) -> Result<WriteAck, Error> {
        // Try with existing connection first, reconnect on failure
        let result = self
            .try_write_extent(
                node_id,
                operation_id,
                session_id,
                volume_id,
                extent_id,
                extent_version,
                epoch,
                &data,
                &checksum,
            )
            .await;

        match result {
            Ok(ack) => Ok(ack),
            Err(_) => {
                // Drop the connection and retry once
                self.drop_connection(&node_id);
                self.try_write_extent(
                    node_id,
                    operation_id,
                    session_id,
                    volume_id,
                    extent_id,
                    extent_version,
                    epoch,
                    &data,
                    &checksum,
                )
                .await
            }
        }
    }
}

impl TcpDataNodeClient {
    #[allow(clippy::too_many_arguments)]
    async fn try_write_extent(
        &self,
        node_id: NodeId,
        operation_id: OperationId,
        session_id: SessionId,
        volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        epoch: EpochId,
        data: &Bytes,
        checksum: &str,
    ) -> Result<WriteAck, Error> {
        let conn = self.get_connection(node_id).await?;
        let mut stream = conn.stream.lock().await;

        // Build the write request
        let write_req = WriteExtentRequest {
            operation_id,
            session_id,
            volume_id,
            extent_id,
            extent_version,
            epoch,
            target_disk_id: None,
            checksum: checksum.to_string(),
            payload_size: data.len() as u64,
            lease_version: None,
        };

        let msg = ProtocolMessage::WriteReq(write_req);
        let msg_bytes = serde_json::to_vec(&msg)
            .map_err(|e| Error::Network(format!("failed to serialize write request: {e}")))?;

        // Send frame (4-byte BE length + JSON)
        write_frame(&mut stream, &msg_bytes).await?;

        // Send raw payload bytes (no framing, server reads payload_size bytes)
        stream
            .write_all(data)
            .await
            .map_err(|e| Error::Network(format!("failed to send payload: {e}")))?;
        stream
            .flush()
            .await
            .map_err(|e| Error::Network(format!("failed to flush: {e}")))?;

        // Read response frame
        let resp_bytes = read_frame(&mut stream).await?;
        let resp_msg: ProtocolMessage = serde_json::from_slice(&resp_bytes)
            .map_err(|e| Error::Network(format!("failed to deserialize write response: {e}")))?;

        match resp_msg {
            ProtocolMessage::WriteResp(resp) => Ok(convert_write_response(node_id, resp)),
            other => Err(Error::Network(format!(
                "unexpected response type: {other:?}"
            ))),
        }
    }
}

/// Convert a protocol WriteExtentResponse to our trait's WriteAck.
fn convert_write_response(node_id: NodeId, resp: WriteExtentResponse) -> WriteAck {
    let error = if resp.success {
        None
    } else {
        let err_msg = resp.error.unwrap_or_default();
        if err_msg.contains("stale") || err_msg.contains("epoch") {
            Some(WriteAckError::StaleEpoch)
        } else if err_msg.contains("disk") || err_msg.contains("unavailable") {
            Some(WriteAckError::DiskUnavailable)
        } else if err_msg.contains("duplicate") {
            Some(WriteAckError::DuplicateOperation)
        } else {
            Some(WriteAckError::InternalError(err_msg))
        }
    };

    WriteAck {
        node_id,
        success: resp.success,
        checksum: resp.checksum,
        error,
    }
}

/// Read a length-prefixed frame from a TCP stream.
///
/// Frame format: 4-byte big-endian length prefix, then `length` bytes.
async fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, Error> {
    let len = stream
        .read_u32()
        .await
        .map_err(|e| Error::Network(format!("failed to read frame length: {e}")))?;
    if len > MAX_FRAME_SIZE {
        return Err(Error::Network(format!(
            "frame too large: {len} > {MAX_FRAME_SIZE}"
        )));
    }
    let mut buf = vec![0u8; len as usize];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| Error::Network(format!("failed to read frame data: {e}")))?;
    Ok(buf)
}

/// Write a length-prefixed frame to a TCP stream.
async fn write_frame(stream: &mut TcpStream, data: &[u8]) -> Result<(), Error> {
    let len = data.len() as u32;
    stream
        .write_u32(len)
        .await
        .map_err(|e| Error::Network(format!("failed to write frame length: {e}")))?;
    stream
        .write_all(data)
        .await
        .map_err(|e| Error::Network(format!("failed to write frame data: {e}")))?;
    stream
        .flush()
        .await
        .map_err(|e| Error::Network(format!("failed to flush frame: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockyard_common::{DiskId, EpochId};
    use blockyard_protocol::messages::{ReadExtentRequest, ReadExtentResponse};
    use blockyard_protocol::server::DataPlaneHandler;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    /// Test handler that accepts writes and reads.
    #[derive(Debug)]
    struct TestHandler;

    impl DataPlaneHandler for TestHandler {
        fn handle_write(
            &self,
            request: &WriteExtentRequest,
            _payload: &[u8],
        ) -> WriteExtentResponse {
            WriteExtentResponse {
                operation_id: request.operation_id,
                extent_id: request.extent_id,
                extent_version: request.extent_version,
                disk_id: DiskId::generate(),
                success: true,
                checksum: request.checksum.clone(),
                error: None,
            }
        }

        fn handle_read(
            &self,
            request: &ReadExtentRequest,
        ) -> (ReadExtentResponse, Option<Vec<u8>>) {
            let payload: Vec<u8> = (0..request.length).map(|i| (i % 256) as u8).collect();
            let checksum = blockyard_common::checksum::compute_checksum(&payload);
            (
                ReadExtentResponse {
                    operation_id: request.operation_id,
                    extent_id: request.extent_id,
                    extent_version: request.extent_version,
                    success: true,
                    checksum,
                    payload_size: payload.len() as u64,
                    error: None,
                },
                Some(payload),
            )
        }
    }

    /// Test handler that returns write failures.
    #[derive(Debug)]
    struct FailingHandler {
        error_msg: String,
    }

    impl DataPlaneHandler for FailingHandler {
        fn handle_write(
            &self,
            request: &WriteExtentRequest,
            _payload: &[u8],
        ) -> WriteExtentResponse {
            WriteExtentResponse {
                operation_id: request.operation_id,
                extent_id: request.extent_id,
                extent_version: request.extent_version,
                disk_id: DiskId::generate(),
                success: false,
                checksum: String::new(),
                error: Some(self.error_msg.clone()),
            }
        }

        fn handle_read(
            &self,
            request: &ReadExtentRequest,
        ) -> (ReadExtentResponse, Option<Vec<u8>>) {
            (
                ReadExtentResponse {
                    operation_id: request.operation_id,
                    extent_id: request.extent_id,
                    extent_version: request.extent_version,
                    success: false,
                    checksum: String::new(),
                    payload_size: 0,
                    error: Some("not implemented".into()),
                },
                None,
            )
        }
    }

    async fn start_test_server<H: DataPlaneHandler>(
        handler: Arc<H>,
    ) -> (SocketAddr, CancellationToken) {
        let node_id = NodeId::generate();
        let server = blockyard_protocol::DataPlaneServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            handler,
            node_id,
        )
        .await
        .unwrap();
        let addr = server.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let shutdown2 = shutdown.clone();
        tokio::spawn(async move {
            server.run(shutdown2).await;
        });
        (addr, shutdown)
    }

    #[tokio::test]
    async fn test_tcp_client_new() {
        let client = TcpDataNodeClient::new();
        assert!(client.node_addresses.read().is_empty());
        assert!(client.connections.read().is_empty());
    }

    #[tokio::test]
    async fn test_tcp_client_default() {
        let client = TcpDataNodeClient::default();
        assert!(client.node_addresses.read().is_empty());
    }

    #[tokio::test]
    async fn test_tcp_client_register_unregister_node() {
        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        let addr: SocketAddr = "10.0.0.1:9800".parse().unwrap();

        client.register_node(node_id, addr);
        assert_eq!(
            client.node_addresses.read().get(&node_id.to_string()),
            Some(&addr)
        );

        client.unregister_node(&node_id);
        assert!(
            client
                .node_addresses
                .read()
                .get(&node_id.to_string())
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_tcp_client_write_extent_success() {
        let handler = Arc::new(TestHandler);
        let (addr, shutdown) = start_test_server(handler).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let data = Bytes::from_static(b"test data for write");
        let checksum = blockyard_common::checksum::compute_checksum(&data);

        let ack = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                data,
                checksum.clone(),
            )
            .await
            .unwrap();

        assert!(ack.success);
        assert_eq!(ack.node_id, node_id);
        assert_eq!(ack.checksum, checksum);
        assert!(ack.error.is_none());

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_tcp_client_write_extent_stale_epoch_error() {
        let handler = Arc::new(FailingHandler {
            error_msg: "stale epoch detected".into(),
        });
        let (addr, shutdown) = start_test_server(handler).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let ack = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                Bytes::from_static(b"data"),
                "checksum".into(),
            )
            .await
            .unwrap();

        assert!(!ack.success);
        assert_eq!(ack.error, Some(WriteAckError::StaleEpoch));

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_tcp_client_write_extent_disk_unavailable_error() {
        let handler = Arc::new(FailingHandler {
            error_msg: "disk unavailable".into(),
        });
        let (addr, shutdown) = start_test_server(handler).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let ack = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                Bytes::from_static(b"data"),
                "checksum".into(),
            )
            .await
            .unwrap();

        assert!(!ack.success);
        assert_eq!(ack.error, Some(WriteAckError::DiskUnavailable));

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_tcp_client_write_extent_duplicate_error() {
        let handler = Arc::new(FailingHandler {
            error_msg: "duplicate operation".into(),
        });
        let (addr, shutdown) = start_test_server(handler).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let ack = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                Bytes::from_static(b"data"),
                "checksum".into(),
            )
            .await
            .unwrap();

        assert!(!ack.success);
        assert_eq!(ack.error, Some(WriteAckError::DuplicateOperation));

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_tcp_client_write_extent_internal_error() {
        let handler = Arc::new(FailingHandler {
            error_msg: "something went wrong".into(),
        });
        let (addr, shutdown) = start_test_server(handler).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let ack = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                Bytes::from_static(b"data"),
                "checksum".into(),
            )
            .await
            .unwrap();

        assert!(!ack.success);
        assert_eq!(
            ack.error,
            Some(WriteAckError::InternalError("something went wrong".into()))
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_tcp_client_no_address_registered() {
        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();

        let result = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                Bytes::from_static(b"data"),
                "checksum".into(),
            )
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("no address registered"));
    }

    #[tokio::test]
    async fn test_tcp_client_connection_refused() {
        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        // Use a port that nothing is listening on
        client.register_node(node_id, "127.0.0.1:1".parse().unwrap());

        let result = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                Bytes::from_static(b"data"),
                "checksum".into(),
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tcp_client_connection_reuse() {
        let handler = Arc::new(TestHandler);
        let (addr, shutdown) = start_test_server(handler).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let data = Bytes::from_static(b"first write");
        let checksum = blockyard_common::checksum::compute_checksum(&data);

        // First write
        let ack1 = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                data.clone(),
                checksum.clone(),
            )
            .await
            .unwrap();
        assert!(ack1.success);

        // Second write (should reuse connection)
        let ack2 = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                2,
                EpochId::new(1),
                data,
                checksum,
            )
            .await
            .unwrap();
        assert!(ack2.success);

        // Verify only one connection exists
        assert_eq!(client.connections.read().len(), 1);

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_tcp_client_drop_connection() {
        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();

        // drop_connection on empty map is a no-op
        client.drop_connection(&node_id);
        assert!(client.connections.read().is_empty());

        // drop_connection on non-existent key is also a no-op
        let nid = NodeId::generate();
        client.drop_connection(&nid);
        assert!(client.connections.read().is_empty());
    }

    #[tokio::test]
    async fn test_tcp_client_reconnect_on_server_restart() {
        let handler = Arc::new(TestHandler);
        let (addr, shutdown1) = start_test_server(Arc::clone(&handler)).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let data = Bytes::from_static(b"before restart");
        let checksum = blockyard_common::checksum::compute_checksum(&data);

        // First write succeeds
        let ack = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                data,
                checksum,
            )
            .await
            .unwrap();
        assert!(ack.success);

        // Shut down server
        shutdown1.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Start new server on same addr
        let listener = TcpListener::bind(addr).await.unwrap();
        let new_addr = listener.local_addr().unwrap();
        let shutdown2 = CancellationToken::new();
        let handler2 = Arc::clone(&handler);
        let server_node_id = NodeId::generate();
        // We need to use the server directly instead of re-binding
        drop(listener);

        let server = blockyard_protocol::DataPlaneServer::bind(new_addr, handler2, server_node_id)
            .await
            .unwrap();
        let shutdown2_run = shutdown2.clone();
        tokio::spawn(async move {
            server.run(shutdown2_run).await;
        });

        // Update address
        client.register_node(node_id, new_addr);
        // Drop stale connection
        client.drop_connection(&node_id);

        let data2 = Bytes::from_static(b"after restart");
        let checksum2 = blockyard_common::checksum::compute_checksum(&data2);

        let ack2 = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                2,
                EpochId::new(1),
                data2,
                checksum2,
            )
            .await
            .unwrap();
        assert!(ack2.success);

        shutdown2.cancel();
    }

    #[test]
    fn test_convert_write_response_success() {
        let node_id = NodeId::generate();
        let resp = WriteExtentResponse {
            operation_id: OperationId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            disk_id: DiskId::generate(),
            success: true,
            checksum: "abc".into(),
            error: None,
        };
        let ack = convert_write_response(node_id, resp);
        assert!(ack.success);
        assert_eq!(ack.checksum, "abc");
        assert!(ack.error.is_none());
    }

    #[test]
    fn test_convert_write_response_stale_epoch() {
        let node_id = NodeId::generate();
        let resp = WriteExtentResponse {
            operation_id: OperationId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            disk_id: DiskId::generate(),
            success: false,
            checksum: String::new(),
            error: Some("stale epoch".into()),
        };
        let ack = convert_write_response(node_id, resp);
        assert!(!ack.success);
        assert_eq!(ack.error, Some(WriteAckError::StaleEpoch));
    }

    #[test]
    fn test_convert_write_response_disk_unavailable() {
        let node_id = NodeId::generate();
        let resp = WriteExtentResponse {
            operation_id: OperationId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            disk_id: DiskId::generate(),
            success: false,
            checksum: String::new(),
            error: Some("disk unavailable".into()),
        };
        let ack = convert_write_response(node_id, resp);
        assert_eq!(ack.error, Some(WriteAckError::DiskUnavailable));
    }

    #[test]
    fn test_convert_write_response_duplicate() {
        let node_id = NodeId::generate();
        let resp = WriteExtentResponse {
            operation_id: OperationId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            disk_id: DiskId::generate(),
            success: false,
            checksum: String::new(),
            error: Some("duplicate operation".into()),
        };
        let ack = convert_write_response(node_id, resp);
        assert_eq!(ack.error, Some(WriteAckError::DuplicateOperation));
    }

    #[test]
    fn test_convert_write_response_internal_error() {
        let node_id = NodeId::generate();
        let resp = WriteExtentResponse {
            operation_id: OperationId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            disk_id: DiskId::generate(),
            success: false,
            checksum: String::new(),
            error: Some("random failure".into()),
        };
        let ack = convert_write_response(node_id, resp);
        assert_eq!(
            ack.error,
            Some(WriteAckError::InternalError("random failure".into()))
        );
    }

    #[test]
    fn test_convert_write_response_no_error_message() {
        let node_id = NodeId::generate();
        let resp = WriteExtentResponse {
            operation_id: OperationId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            disk_id: DiskId::generate(),
            success: false,
            checksum: String::new(),
            error: None,
        };
        let ack = convert_write_response(node_id, resp);
        assert!(!ack.success);
        assert_eq!(ack.error, Some(WriteAckError::InternalError(String::new())));
    }

    #[test]
    fn test_tcp_connection_debug() {
        // Just verify the Debug impl doesn't panic
        let debug_str = format!("{:?}", TcpDataNodeClient::new());
        assert!(debug_str.contains("TcpDataNodeClient"));
    }

    #[tokio::test]
    async fn test_tcp_client_multiple_nodes() {
        let handler1 = Arc::new(TestHandler);
        let handler2 = Arc::new(TestHandler);
        let (addr1, shutdown1) = start_test_server(handler1).await;
        let (addr2, shutdown2) = start_test_server(handler2).await;

        let client = TcpDataNodeClient::new();
        let node1 = NodeId::generate();
        let node2 = NodeId::generate();
        client.register_node(node1, addr1);
        client.register_node(node2, addr2);

        let data = Bytes::from_static(b"multi node test");
        let checksum = blockyard_common::checksum::compute_checksum(&data);

        let ack1 = client
            .write_extent(
                node1,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                data.clone(),
                checksum.clone(),
            )
            .await
            .unwrap();
        assert!(ack1.success);
        assert_eq!(ack1.node_id, node1);

        let ack2 = client
            .write_extent(
                node2,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                data,
                checksum,
            )
            .await
            .unwrap();
        assert!(ack2.success);
        assert_eq!(ack2.node_id, node2);

        assert_eq!(client.connections.read().len(), 2);

        shutdown1.cancel();
        shutdown2.cancel();
    }

    #[tokio::test]
    async fn test_tcp_client_read_extent_success() {
        let handler = Arc::new(TestHandler);
        let (addr, shutdown) = start_test_server(handler).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let data = client
            .read_extent(
                node_id,
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                0,
                256,
            )
            .await
            .unwrap();

        assert_eq!(data.len(), 256);
        let expected: Vec<u8> = (0..256u64).map(|i| (i % 256) as u8).collect();
        assert_eq!(&data[..], &expected[..]);

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_tcp_client_read_extent_failure() {
        let handler = Arc::new(FailingHandler {
            error_msg: "extent not found".into(),
        });
        let (addr, shutdown) = start_test_server(handler).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let result = client
            .read_extent(
                node_id,
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                0,
                256,
            )
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not implemented"));

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_tcp_client_read_no_address() {
        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();

        let result = client
            .read_extent(
                node_id,
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                0,
                256,
            )
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no address registered"));
    }

    #[tokio::test]
    async fn test_tcp_client_read_connection_reuse() {
        let handler = Arc::new(TestHandler);
        let (addr, shutdown) = start_test_server(handler).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let data1 = client
            .read_extent(
                node_id,
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                0,
                128,
            )
            .await
            .unwrap();
        assert_eq!(data1.len(), 128);

        let data2 = client
            .read_extent(
                node_id,
                VolumeId::generate(),
                ExtentId::generate(),
                2,
                0,
                64,
            )
            .await
            .unwrap();
        assert_eq!(data2.len(), 64);

        assert_eq!(client.connections.read().len(), 1);

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_tcp_client_write_then_read() {
        let handler = Arc::new(TestHandler);
        let (addr, shutdown) = start_test_server(handler).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let write_data = Bytes::from_static(b"write before read");
        let checksum = blockyard_common::checksum::compute_checksum(&write_data);
        let ack = client
            .write_extent(
                node_id,
                OperationId::generate(),
                SessionId::generate(),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                EpochId::new(1),
                write_data,
                checksum,
            )
            .await
            .unwrap();
        assert!(ack.success);

        let read_data = client
            .read_extent(
                node_id,
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                0,
                512,
            )
            .await
            .unwrap();
        assert_eq!(read_data.len(), 512);

        shutdown.cancel();
    }

    #[tokio::test]
    async fn test_tcp_client_data_node_reader_trait() {
        let handler = Arc::new(TestHandler);
        let (addr, shutdown) = start_test_server(handler).await;

        let client = TcpDataNodeClient::new();
        let node_id = NodeId::generate();
        client.register_node(node_id, addr);

        let result = <TcpDataNodeClient as DataNodeReader>::read_extent(
            &client,
            node_id,
            VolumeId::generate(),
            ExtentId::generate(),
            1,
            0,
            256,
        )
        .await
        .unwrap();

        assert_eq!(result.data.len(), 256);
        assert_eq!(result.extent_version, 1);
        assert!(!result.checksum.is_empty());

        shutdown.cancel();
    }
}
