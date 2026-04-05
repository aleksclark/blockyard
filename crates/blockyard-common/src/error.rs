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
