//! Read-policy routing for the block protocol.
//!
//! [`ReadRouter`] selects which replica to read from based on the configured
//! [`ReadPolicy`].

use crate::server::RequestHandler;
use crate::wire::Status;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tracing::debug;

/// Supported read-routing policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPolicy {
    /// Always read from the Raft leader.
    Leader,
    /// Round-robin across all replicas.
    Any,
    /// Prefer the local replica if available, fall back to any.
    Local,
}

impl std::fmt::Display for ReadPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Leader => write!(f, "leader"),
            Self::Any => write!(f, "any"),
            Self::Local => write!(f, "local"),
        }
    }
}

impl std::str::FromStr for ReadPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "leader" => Ok(Self::Leader),
            "any" => Ok(Self::Any),
            "local" => Ok(Self::Local),
            _ => Err(format!("unknown read policy: {s}")),
        }
    }
}

/// Identifies a replica that can serve reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaId(pub u64);

/// A [`ReadRouter`] wraps a [`RequestHandler`] and selects which replica
/// should serve each read request.
///
/// For simplicity in the current in-process implementation the actual I/O is
/// always delegated to the wrapped handler; the router's job is to track
/// **which** node would be selected and expose that decision for observability.
#[derive(Debug)]
pub struct ReadRouter<H> {
    inner: Arc<H>,
    policy: ReadPolicy,
    /// IDs of all replicas that can serve reads.
    replicas: Vec<ReplicaId>,
    /// Which replica is the leader (index into `replicas`).
    leader_idx: usize,
    /// Which replica is local (index into `replicas`), if any.
    local_idx: Option<usize>,
    /// Round-robin counter for [`ReadPolicy::Any`].
    rr_counter: AtomicUsize,
    /// Total reads routed.
    reads_routed: AtomicU64,
}

impl<H: RequestHandler> ReadRouter<H> {
    /// Create a new `ReadRouter`.
    ///
    /// * `replicas` – ordered list of replica node IDs.
    /// * `leader_idx` – index of the leader in `replicas`.
    /// * `local_idx` – index of the local node in `replicas`, if present.
    pub fn new(
        inner: H,
        policy: ReadPolicy,
        replicas: Vec<ReplicaId>,
        leader_idx: usize,
        local_idx: Option<usize>,
    ) -> Self {
        Self {
            inner: Arc::new(inner),
            policy,
            replicas,
            leader_idx,
            local_idx,
            rr_counter: AtomicUsize::new(0),
            reads_routed: AtomicU64::new(0),
        }
    }

    /// Returns the configured read policy.
    pub fn policy(&self) -> ReadPolicy {
        self.policy
    }

    /// Returns the number of reads routed so far.
    pub fn reads_routed(&self) -> u64 {
        self.reads_routed.load(Ordering::Relaxed)
    }

    /// Select which replica should serve the next read according to the policy.
    pub fn select_replica(&self) -> ReplicaId {
        match self.policy {
            ReadPolicy::Leader => self.replicas[self.leader_idx],
            ReadPolicy::Any => {
                let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed) % self.replicas.len();
                self.replicas[idx]
            }
            ReadPolicy::Local => {
                if let Some(li) = self.local_idx {
                    self.replicas[li]
                } else {
                    // Fallback to round-robin when no local replica.
                    let idx = self.rr_counter.fetch_add(1, Ordering::Relaxed) % self.replicas.len();
                    self.replicas[idx]
                }
            }
        }
    }
}

impl<H: RequestHandler> RequestHandler for ReadRouter<H> {
    async fn handle_read(&self, volume_id: u64, offset: u64, length: u32) -> Result<Bytes, Status> {
        let target = self.select_replica();
        debug!(
            policy = %self.policy,
            target = target.0,
            "routing read"
        );
        self.reads_routed.fetch_add(1, Ordering::Relaxed);
        self.inner.handle_read(volume_id, offset, length).await
    }

    async fn handle_write(&self, volume_id: u64, offset: u64, data: Bytes) -> Result<(), Status> {
        self.inner.handle_write(volume_id, offset, data).await
    }

    async fn handle_flush(&self, volume_id: u64) -> Result<(), Status> {
        self.inner.handle_flush(volume_id).await
    }

    async fn handle_trim(&self, volume_id: u64, offset: u64, length: u32) -> Result<(), Status> {
        self.inner.handle_trim(volume_id, offset, length).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    struct MemHandler {
        data: Arc<Mutex<HashMap<(u64, u64), Vec<u8>>>>,
    }

    impl MemHandler {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    impl RequestHandler for MemHandler {
        async fn handle_read(
            &self,
            volume_id: u64,
            offset: u64,
            length: u32,
        ) -> Result<Bytes, Status> {
            let map = self.data.lock();
            match map.get(&(volume_id, offset)) {
                Some(d) => Ok(Bytes::copy_from_slice(&d[..length as usize])),
                None => Ok(Bytes::from(vec![0u8; length as usize])),
            }
        }

        async fn handle_write(
            &self,
            volume_id: u64,
            offset: u64,
            data: Bytes,
        ) -> Result<(), Status> {
            self.data.lock().insert((volume_id, offset), data.to_vec());
            Ok(())
        }

        async fn handle_flush(&self, _volume_id: u64) -> Result<(), Status> {
            Ok(())
        }

        async fn handle_trim(
            &self,
            volume_id: u64,
            offset: u64,
            _length: u32,
        ) -> Result<(), Status> {
            self.data.lock().remove(&(volume_id, offset));
            Ok(())
        }
    }

    fn make_replicas(ids: &[u64]) -> Vec<ReplicaId> {
        ids.iter().map(|&id| ReplicaId(id)).collect()
    }

    // ── ReadPolicy Display / FromStr ─────────────────────────────────────

    #[test]
    fn test_read_policy_display() {
        assert_eq!(ReadPolicy::Leader.to_string(), "leader");
        assert_eq!(ReadPolicy::Any.to_string(), "any");
        assert_eq!(ReadPolicy::Local.to_string(), "local");
    }

    #[test]
    fn test_read_policy_from_str() {
        assert_eq!("leader".parse::<ReadPolicy>().unwrap(), ReadPolicy::Leader);
        assert_eq!("ANY".parse::<ReadPolicy>().unwrap(), ReadPolicy::Any);
        assert_eq!("Local".parse::<ReadPolicy>().unwrap(), ReadPolicy::Local);
        assert!("nope".parse::<ReadPolicy>().is_err());
    }

    // ── select_replica ───────────────────────────────────────────────────

    #[test]
    fn test_select_replica_leader() {
        let router = ReadRouter::new(
            MemHandler::new(),
            ReadPolicy::Leader,
            make_replicas(&[10, 20, 30]),
            0, // leader is replica 10
            Some(1),
        );
        // Must always return the leader.
        for _ in 0..10 {
            assert_eq!(router.select_replica(), ReplicaId(10));
        }
    }

    #[test]
    fn test_select_replica_any_round_robin() {
        let router = ReadRouter::new(
            MemHandler::new(),
            ReadPolicy::Any,
            make_replicas(&[10, 20, 30]),
            0,
            None,
        );
        let first = router.select_replica();
        let second = router.select_replica();
        let third = router.select_replica();
        // Should cycle through all replicas.
        assert_eq!(first, ReplicaId(10));
        assert_eq!(second, ReplicaId(20));
        assert_eq!(third, ReplicaId(30));
        // Wraps around.
        assert_eq!(router.select_replica(), ReplicaId(10));
    }

    #[test]
    fn test_select_replica_local_prefers_local() {
        let router = ReadRouter::new(
            MemHandler::new(),
            ReadPolicy::Local,
            make_replicas(&[10, 20, 30]),
            0,
            Some(2), // local is replica 30
        );
        for _ in 0..5 {
            assert_eq!(router.select_replica(), ReplicaId(30));
        }
    }

    #[test]
    fn test_select_replica_local_fallback_to_round_robin() {
        let router = ReadRouter::new(
            MemHandler::new(),
            ReadPolicy::Local,
            make_replicas(&[10, 20, 30]),
            0,
            None, // no local replica
        );
        let first = router.select_replica();
        let second = router.select_replica();
        assert_eq!(first, ReplicaId(10));
        assert_eq!(second, ReplicaId(20));
    }

    // ── ReadRouter as RequestHandler ─────────────────────────────────────

    #[tokio::test]
    async fn test_router_handle_read() {
        let handler = MemHandler::new();
        handler
            .data
            .lock()
            .insert((1, 0), vec![0xAA, 0xBB, 0xCC, 0xDD]);
        let router = ReadRouter::new(handler, ReadPolicy::Leader, make_replicas(&[1]), 0, Some(0));
        let data = router.handle_read(1, 0, 4).await.unwrap();
        assert_eq!(data, Bytes::from(vec![0xAA, 0xBB, 0xCC, 0xDD]));
        assert_eq!(router.reads_routed(), 1);
    }

    #[tokio::test]
    async fn test_router_handle_write_passthrough() {
        let router = ReadRouter::new(
            MemHandler::new(),
            ReadPolicy::Any,
            make_replicas(&[1, 2, 3]),
            0,
            None,
        );
        let res = router.handle_write(1, 0, Bytes::from(vec![0x01; 4])).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_router_handle_flush_passthrough() {
        let router = ReadRouter::new(
            MemHandler::new(),
            ReadPolicy::Leader,
            make_replicas(&[1]),
            0,
            Some(0),
        );
        assert!(router.handle_flush(1).await.is_ok());
    }

    #[tokio::test]
    async fn test_router_handle_trim_passthrough() {
        let handler = MemHandler::new();
        handler.data.lock().insert((1, 0), vec![1, 2, 3, 4]);
        let router = ReadRouter::new(handler, ReadPolicy::Any, make_replicas(&[1, 2]), 0, None);
        assert!(router.handle_trim(1, 0, 4).await.is_ok());
    }

    #[tokio::test]
    async fn test_router_reads_routed_counter() {
        let router = ReadRouter::new(
            MemHandler::new(),
            ReadPolicy::Any,
            make_replicas(&[1, 2, 3]),
            0,
            None,
        );
        for _ in 0..7 {
            router.handle_read(1, 0, 4).await.unwrap();
        }
        assert_eq!(router.reads_routed(), 7);
    }

    #[test]
    fn test_router_policy_accessor() {
        let router = ReadRouter::new(
            MemHandler::new(),
            ReadPolicy::Local,
            make_replicas(&[1]),
            0,
            Some(0),
        );
        assert_eq!(router.policy(), ReadPolicy::Local);
    }
}
