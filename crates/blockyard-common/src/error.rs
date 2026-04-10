//! Shared error types for Blockyard library crates.
//!
//! Binary crates should wrap these with `anyhow` for context.

/// Shared error type for all Blockyard library crates.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// I/O errors from filesystem, network, or device operations.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Configuration parsing or validation errors.
    #[error("config error: {0}")]
    Config(String),

    /// Raft consensus errors.
    #[error("raft error: {0}")]
    Raft(String),

    /// Storage engine errors (extents, disks, placement).
    #[error("storage error: {0}")]
    Storage(String),

    /// Network communication errors (connection, timeout, framing).
    #[error("network error: {0}")]
    Network(String),

    /// Wire protocol errors (serialization, version mismatch).
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Authentication or authorization errors.
    #[error("auth error: {0}")]
    Auth(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = Error::Io(io_err);
        assert!(err.to_string().contains("file missing"));
    }

    #[test]
    fn test_io_error_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let err: Error = io_err.into();
        assert!(err.to_string().contains("denied"));
    }

    #[test]
    fn test_config_error_display() {
        let err = Error::Config("invalid port".into());
        assert_eq!(err.to_string(), "config error: invalid port");
    }

    #[test]
    fn test_raft_error_display() {
        let err = Error::Raft("no quorum".into());
        assert_eq!(err.to_string(), "raft error: no quorum");
    }

    #[test]
    fn test_storage_error_display() {
        let err = Error::Storage("disk full".into());
        assert_eq!(err.to_string(), "storage error: disk full");
    }

    #[test]
    fn test_protocol_error_display() {
        let err = Error::Protocol("version mismatch".into());
        assert_eq!(err.to_string(), "protocol error: version mismatch");
    }

    #[test]
    fn test_network_error_display() {
        let err = Error::Network("connection refused".into());
        assert_eq!(err.to_string(), "network error: connection refused");
    }

    #[test]
    fn test_network_error_debug() {
        let err = Error::Network("timeout".into());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Network"));
        assert!(debug.contains("timeout"));
    }

    #[test]
    fn test_network_error_source() {
        use std::error::Error as StdError;
        let err = Error::Network("conn reset".into());
        assert!(err.source().is_none());
    }

    #[test]
    fn test_auth_error_display() {
        let err = Error::Auth("token expired".into());
        assert_eq!(err.to_string(), "auth error: token expired");
    }

    #[test]
    fn test_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Error>();
    }

    #[test]
    fn test_error_debug() {
        let err = Error::Config("bad value".into());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Config"));
        assert!(debug.contains("bad value"));
    }

    #[test]
    fn test_error_source_io() {
        use std::error::Error as StdError;
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        let err = Error::Io(io_err);
        assert!(err.source().is_some());
    }

    #[test]
    fn test_error_source_config() {
        use std::error::Error as StdError;
        let err = Error::Config("bad".into());
        assert!(err.source().is_none());
    }
}
