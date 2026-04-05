use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("raft error: {0}")]
    Raft(String),

    #[error("gossip error: {0}")]
    Gossip(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("volume not found: {0}")]
    VolumeNotFound(String),

    #[error("node not found: {0}")]
    NodeNotFound(String),

    #[error("no quorum available")]
    NoQuorum,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_config() {
        let e = Error::Config("bad value".to_string());
        assert_eq!(e.to_string(), "configuration error: bad value");
    }

    #[test]
    fn test_error_display_storage() {
        let e = Error::Storage("disk full".to_string());
        assert_eq!(e.to_string(), "storage error: disk full");
    }

    #[test]
    fn test_error_display_raft() {
        let e = Error::Raft("no leader".to_string());
        assert_eq!(e.to_string(), "raft error: no leader");
    }

    #[test]
    fn test_error_display_gossip() {
        let e = Error::Gossip("timeout".to_string());
        assert_eq!(e.to_string(), "gossip error: timeout");
    }

    #[test]
    fn test_error_display_protocol() {
        let e = Error::Protocol("invalid frame".to_string());
        assert_eq!(e.to_string(), "protocol error: invalid frame");
    }

    #[test]
    fn test_error_display_volume_not_found() {
        let e = Error::VolumeNotFound("vol-1".to_string());
        assert_eq!(e.to_string(), "volume not found: vol-1");
    }

    #[test]
    fn test_error_display_node_not_found() {
        let e = Error::NodeNotFound("node-a".to_string());
        assert_eq!(e.to_string(), "node not found: node-a");
    }

    #[test]
    fn test_error_display_no_quorum() {
        let e = Error::NoQuorum;
        assert_eq!(e.to_string(), "no quorum available");
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let e: Error = io_err.into();
        assert!(e.to_string().contains("file missing"));
    }

    #[test]
    fn test_result_type_alias() {
        let ok: Result<i32> = Ok(42);
        assert_eq!(ok.unwrap(), 42);
        let err: Result<i32> = Err(Error::NoQuorum);
        assert!(err.is_err());
    }
}
