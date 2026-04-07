//! Wire protocol message definitions for data read/write (§7, P2.1).
//!
//! Defines request and response types for client-to-node and node-to-node
//! data plane operations. Uses JSON serialization initially; a binary format
//! (protobuf/flatbuffers) is a future optimization.

use blockyard_common::{
    AuthToken, DiskId, EpochId, ExtentId, LeaseVersion, NodeId, OperationId, SessionId, VolumeId,
};
use serde::{Deserialize, Serialize};

/// Protocol version identifier.
pub type ProtocolVersion = u32;

/// Current protocol version.
pub const CURRENT_PROTOCOL_VERSION: ProtocolVersion = 1;

/// Minimum supported protocol version.
pub const MIN_PROTOCOL_VERSION: ProtocolVersion = 1;

/// Connection handshake request (P2.2, P6.3, P6.4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeRequest {
    pub protocol_version: ProtocolVersion,
    pub node_id: Option<NodeId>,
    pub session_id: Option<SessionId>,
    pub features: Vec<String>,
    #[serde(default)]
    pub auth_token: Option<AuthToken>,
}

/// Connection handshake response (P2.2, P6.3, P6.4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeResponse {
    pub protocol_version: ProtocolVersion,
    pub node_id: NodeId,
    pub accepted: bool,
    pub message: Option<String>,
    pub supported_features: Vec<String>,
    #[serde(default)]
    pub authenticated: bool,
}

/// Write extent request from client to data node (§5.5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteExtentRequest {
    pub operation_id: OperationId,
    pub session_id: SessionId,
    pub volume_id: VolumeId,
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub epoch: EpochId,
    pub target_disk_id: Option<DiskId>,
    pub checksum: String,
    pub payload_size: u64,
    pub lease_version: Option<LeaseVersion>,
}

/// Write extent response from data node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteExtentResponse {
    pub operation_id: OperationId,
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub disk_id: DiskId,
    pub success: bool,
    pub checksum: String,
    pub error: Option<String>,
}

/// Read extent request from client to data node (§5.6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadExtentRequest {
    pub operation_id: OperationId,
    pub session_id: SessionId,
    pub volume_id: VolumeId,
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub epoch: EpochId,
    pub offset: u64,
    pub length: u64,
}

/// Read extent response from data node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadExtentResponse {
    pub operation_id: OperationId,
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub success: bool,
    pub checksum: String,
    pub payload_size: u64,
    pub error: Option<String>,
}

/// Error codes for protocol-level errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    Ok,
    StaleEpoch,
    ExtentNotFound,
    ChecksumMismatch,
    DiskUnavailable,
    AllocationDenied,
    DuplicateOperation,
    UnsupportedVersion,
    InternalError,
    Unauthorized,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::Ok => write!(f, "ok"),
            ErrorCode::StaleEpoch => write!(f, "stale_epoch"),
            ErrorCode::ExtentNotFound => write!(f, "extent_not_found"),
            ErrorCode::ChecksumMismatch => write!(f, "checksum_mismatch"),
            ErrorCode::DiskUnavailable => write!(f, "disk_unavailable"),
            ErrorCode::AllocationDenied => write!(f, "allocation_denied"),
            ErrorCode::DuplicateOperation => write!(f, "duplicate_operation"),
            ErrorCode::UnsupportedVersion => write!(f, "unsupported_version"),
            ErrorCode::InternalError => write!(f, "internal_error"),
            ErrorCode::Unauthorized => write!(f, "unauthorized"),
        }
    }
}

/// Top-level protocol envelope for all messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtocolMessage {
    HandshakeReq(HandshakeRequest),
    HandshakeResp(HandshakeResponse),
    WriteReq(WriteExtentRequest),
    WriteResp(WriteExtentResponse),
    ReadReq(ReadExtentRequest),
    ReadResp(ReadExtentResponse),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_request_serde() {
        let req = HandshakeRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            node_id: None,
            session_id: Some(SessionId::generate()),
            features: vec!["compression".into()],
            auth_token: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: HandshakeRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.protocol_version, CURRENT_PROTOCOL_VERSION);
        assert!(parsed.session_id.is_some());
    }

    #[test]
    fn test_handshake_response_serde() {
        let resp = HandshakeResponse {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            node_id: NodeId::generate(),
            accepted: true,
            message: None,
            supported_features: vec![],
            authenticated: false,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: HandshakeResponse = serde_json::from_str(&json).unwrap();
        assert!(parsed.accepted);
    }

    #[test]
    fn test_write_request_serde() {
        let req = WriteExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            epoch: EpochId::new(5),
            target_disk_id: None,
            checksum: "abc123".into(),
            payload_size: 4096,
            lease_version: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: WriteExtentRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.extent_version, 1);
        assert_eq!(parsed.payload_size, 4096);
    }

    #[test]
    fn test_write_response_serde() {
        let resp = WriteExtentResponse {
            operation_id: OperationId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            disk_id: DiskId::generate(),
            success: true,
            checksum: "abc".into(),
            error: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: WriteExtentResponse = serde_json::from_str(&json).unwrap();
        assert!(parsed.success);
    }

    #[test]
    fn test_read_request_serde() {
        let req = ReadExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 2,
            epoch: EpochId::new(3),
            offset: 0,
            length: 512,
        };
        let json = serde_json::to_string(&req).unwrap();
        let parsed: ReadExtentRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.extent_version, 2);
        assert_eq!(parsed.length, 512);
    }

    #[test]
    fn test_read_response_serde() {
        let resp = ReadExtentResponse {
            operation_id: OperationId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 2,
            success: false,
            checksum: String::new(),
            payload_size: 0,
            error: Some("extent not found".into()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let parsed: ReadExtentResponse = serde_json::from_str(&json).unwrap();
        assert!(!parsed.success);
        assert!(parsed.error.is_some());
    }

    #[test]
    fn test_error_code_display() {
        assert_eq!(ErrorCode::Ok.to_string(), "ok");
        assert_eq!(ErrorCode::StaleEpoch.to_string(), "stale_epoch");
        assert_eq!(ErrorCode::ExtentNotFound.to_string(), "extent_not_found");
        assert_eq!(ErrorCode::ChecksumMismatch.to_string(), "checksum_mismatch");
        assert_eq!(ErrorCode::DiskUnavailable.to_string(), "disk_unavailable");
        assert_eq!(ErrorCode::AllocationDenied.to_string(), "allocation_denied");
        assert_eq!(
            ErrorCode::DuplicateOperation.to_string(),
            "duplicate_operation"
        );
        assert_eq!(
            ErrorCode::UnsupportedVersion.to_string(),
            "unsupported_version"
        );
        assert_eq!(ErrorCode::InternalError.to_string(), "internal_error");
        assert_eq!(ErrorCode::Unauthorized.to_string(), "unauthorized");
    }

    #[test]
    fn test_error_code_serde() {
        for code in [
            ErrorCode::Ok,
            ErrorCode::StaleEpoch,
            ErrorCode::ExtentNotFound,
            ErrorCode::ChecksumMismatch,
            ErrorCode::DiskUnavailable,
            ErrorCode::AllocationDenied,
            ErrorCode::DuplicateOperation,
            ErrorCode::UnsupportedVersion,
            ErrorCode::InternalError,
            ErrorCode::Unauthorized,
        ] {
            let json = serde_json::to_string(&code).unwrap();
            let parsed: ErrorCode = serde_json::from_str(&json).unwrap();
            assert_eq!(code, parsed);
        }
    }

    #[test]
    fn test_protocol_message_envelope_write() {
        let req = WriteExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            epoch: EpochId::new(1),
            target_disk_id: None,
            checksum: "x".into(),
            payload_size: 100,
            lease_version: None,
        };
        let msg = ProtocolMessage::WriteReq(req);
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: ProtocolMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ProtocolMessage::WriteReq(_)));
    }

    #[test]
    fn test_protocol_message_envelope_read() {
        let req = ReadExtentRequest {
            operation_id: OperationId::generate(),
            session_id: SessionId::generate(),
            volume_id: VolumeId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            epoch: EpochId::new(1),
            offset: 0,
            length: 100,
        };
        let msg = ProtocolMessage::ReadReq(req);
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: ProtocolMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ProtocolMessage::ReadReq(_)));
    }

    #[test]
    fn test_protocol_message_envelope_handshake() {
        let req = HandshakeRequest {
            protocol_version: 1,
            node_id: None,
            session_id: None,
            features: vec![],
            auth_token: None,
        };
        let msg = ProtocolMessage::HandshakeReq(req);
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: ProtocolMessage = serde_json::from_str(&json).unwrap();
        assert!(matches!(parsed, ProtocolMessage::HandshakeReq(_)));
    }

    #[test]
    fn test_current_version_constants() {
        assert!(CURRENT_PROTOCOL_VERSION >= MIN_PROTOCOL_VERSION);
    }

    #[test]
    fn test_write_response_error() {
        let resp = WriteExtentResponse {
            operation_id: OperationId::generate(),
            extent_id: ExtentId::generate(),
            extent_version: 1,
            disk_id: DiskId::generate(),
            success: false,
            checksum: String::new(),
            error: Some("stale epoch".into()),
        };
        assert!(!resp.success);
        assert_eq!(resp.error.as_deref(), Some("stale epoch"));
    }
}
