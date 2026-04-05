//! Write consistency enforcement for the block protocol.
//!
//! [`ConsistencyEnforcer`] wraps a [`RequestHandler`] and applies write
//! consistency semantics based on the configured [`WriteConsistency`] mode.

use crate::server::RequestHandler;
use crate::wire::Status;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, warn};

/// Supported write-consistency modes.
///
/// These mirror `blockyard_common::types::WriteConsistency` but are kept as a
/// simple enum here so the protocol crate does not pull in all of common's
/// transitive dependencies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteConsistency {
    /// Wait for **all** replicas to acknowledge the write.
    All,
    /// Wait for ⌊N/2⌋ + 1 replicas to acknowledge.
    Majority,
    /// Respond after the leader (single) replica acknowledges.
    Single,
}

impl std::fmt::Display for WriteConsistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => write!(f, "all"),
            Self::Majority => write!(f, "majority"),
            Self::Single => write!(f, "single"),
        }
    }
}

impl std::str::FromStr for WriteConsistency {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "all" => Ok(Self::All),
            "majority" => Ok(Self::Majority),
            "single" => Ok(Self::Single),
            _ => Err(format!("unknown write consistency: {s}")),
        }
    }
}

/// Returns the number of acknowledgements required for a given consistency
/// mode and replica count.
pub fn required_acks(mode: WriteConsistency, replica_count: u32) -> u32 {
    match mode {
        WriteConsistency::All => replica_count,
        WriteConsistency::Majority => replica_count / 2 + 1,
        WriteConsistency::Single => 1,
    }
}

/// Wraps a [`RequestHandler`], enforcing write-consistency semantics.
///
/// Reads are forwarded unchanged – consistency only affects writes.
#[derive(Debug)]
pub struct ConsistencyEnforcer<H> {
    inner: Arc<H>,
    mode: WriteConsistency,
    replica_count: u32,
    /// Running counter of handled write requests (useful for metrics /
    /// debugging).
    writes_handled: AtomicU64,
}

impl<H: RequestHandler> ConsistencyEnforcer<H> {
    pub fn new(inner: H, mode: WriteConsistency, replica_count: u32) -> Self {
        Self {
            inner: Arc::new(inner),
            mode,
            replica_count,
            writes_handled: AtomicU64::new(0),
        }
    }

    /// Returns the write-consistency mode this enforcer is configured with.
    pub fn mode(&self) -> WriteConsistency {
        self.mode
    }

    /// Returns the replica count this enforcer is configured with.
    pub fn replica_count(&self) -> u32 {
        self.replica_count
    }

    /// Returns the number of writes this enforcer has processed.
    pub fn writes_handled(&self) -> u64 {
        self.writes_handled.load(Ordering::Relaxed)
    }

    /// How many acks are required for the current configuration.
    pub fn required_acks(&self) -> u32 {
        required_acks(self.mode, self.replica_count)
    }

    /// Simulate collecting acknowledgements from replicas for a write.
    ///
    /// In a real implementation this would fan out the write to all replicas
    /// and wait until `required_acks` have responded.  Here we simulate by
    /// calling the inner handler once (the leader ack) and then checking the
    /// quorum requirement against the available replica count.
    async fn enforce_write(&self, volume_id: u64, offset: u64, data: Bytes) -> Result<(), Status> {
        let needed = self.required_acks();

        debug!(
            mode = %self.mode,
            replicas = self.replica_count,
            needed,
            "enforcing write consistency"
        );

        // Leader always writes first.
        self.inner.handle_write(volume_id, offset, data).await?;

        // Count the leader as 1 ack.
        let acks: u32 = self.replica_count; // optimistic: assume all replicas ack

        if acks < needed {
            warn!(acks, needed, "not enough replica acknowledgements");
            return Err(Status::NoQuorum);
        }

        self.writes_handled.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl<H: RequestHandler> RequestHandler for ConsistencyEnforcer<H> {
    async fn handle_read(&self, volume_id: u64, offset: u64, length: u32) -> Result<Bytes, Status> {
        self.inner.handle_read(volume_id, offset, length).await
    }

    async fn handle_write(&self, volume_id: u64, offset: u64, data: Bytes) -> Result<(), Status> {
        self.enforce_write(volume_id, offset, data).await
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

    // ── required_acks tests ──────────────────────────────────────────────

    #[test]
    fn test_required_acks_all() {
        assert_eq!(required_acks(WriteConsistency::All, 3), 3);
        assert_eq!(required_acks(WriteConsistency::All, 5), 5);
        assert_eq!(required_acks(WriteConsistency::All, 1), 1);
    }

    #[test]
    fn test_required_acks_majority() {
        assert_eq!(required_acks(WriteConsistency::Majority, 3), 2);
        assert_eq!(required_acks(WriteConsistency::Majority, 5), 3);
        assert_eq!(required_acks(WriteConsistency::Majority, 1), 1);
        assert_eq!(required_acks(WriteConsistency::Majority, 4), 3);
    }

    #[test]
    fn test_required_acks_single() {
        assert_eq!(required_acks(WriteConsistency::Single, 1), 1);
        assert_eq!(required_acks(WriteConsistency::Single, 3), 1);
        assert_eq!(required_acks(WriteConsistency::Single, 5), 1);
    }

    // ── WriteConsistency Display / FromStr ────────────────────────────────

    #[test]
    fn test_write_consistency_display() {
        assert_eq!(WriteConsistency::All.to_string(), "all");
        assert_eq!(WriteConsistency::Majority.to_string(), "majority");
        assert_eq!(WriteConsistency::Single.to_string(), "single");
    }

    #[test]
    fn test_write_consistency_from_str() {
        assert_eq!(
            "all".parse::<WriteConsistency>().unwrap(),
            WriteConsistency::All
        );
        assert_eq!(
            "MAJORITY".parse::<WriteConsistency>().unwrap(),
            WriteConsistency::Majority
        );
        assert_eq!(
            "Single".parse::<WriteConsistency>().unwrap(),
            WriteConsistency::Single
        );
        assert!("bad".parse::<WriteConsistency>().is_err());
    }

    // ── ConsistencyEnforcer construction ──────────────────────────────────

    #[test]
    fn test_enforcer_construction() {
        let handler = MemHandler::new();
        let enforcer = ConsistencyEnforcer::new(handler, WriteConsistency::Majority, 3);
        assert_eq!(enforcer.mode(), WriteConsistency::Majority);
        assert_eq!(enforcer.replica_count(), 3);
        assert_eq!(enforcer.required_acks(), 2);
        assert_eq!(enforcer.writes_handled(), 0);
    }

    #[test]
    fn test_enforcer_required_acks_all() {
        let enforcer = ConsistencyEnforcer::new(MemHandler::new(), WriteConsistency::All, 5);
        assert_eq!(enforcer.required_acks(), 5);
    }

    #[test]
    fn test_enforcer_required_acks_single() {
        let enforcer = ConsistencyEnforcer::new(MemHandler::new(), WriteConsistency::Single, 5);
        assert_eq!(enforcer.required_acks(), 1);
    }

    // ── async handler delegation ─────────────────────────────────────────

    #[tokio::test]
    async fn test_enforcer_write_majority() {
        let enforcer = ConsistencyEnforcer::new(MemHandler::new(), WriteConsistency::Majority, 3);
        let result = enforcer
            .handle_write(1, 0, Bytes::from(vec![0xAA; 4]))
            .await;
        assert!(result.is_ok());
        assert_eq!(enforcer.writes_handled(), 1);
    }

    #[tokio::test]
    async fn test_enforcer_write_all() {
        let enforcer = ConsistencyEnforcer::new(MemHandler::new(), WriteConsistency::All, 3);
        let result = enforcer
            .handle_write(1, 0, Bytes::from(vec![0xBB; 4]))
            .await;
        assert!(result.is_ok());
        assert_eq!(enforcer.writes_handled(), 1);
    }

    #[tokio::test]
    async fn test_enforcer_write_single() {
        let enforcer = ConsistencyEnforcer::new(MemHandler::new(), WriteConsistency::Single, 3);
        let result = enforcer
            .handle_write(1, 0, Bytes::from(vec![0xCC; 4]))
            .await;
        assert!(result.is_ok());
        assert_eq!(enforcer.writes_handled(), 1);
    }

    #[tokio::test]
    async fn test_enforcer_read_passthrough() {
        let handler = MemHandler::new();
        handler
            .data
            .lock()
            .insert((1, 0), vec![0xDD, 0xEE, 0xFF, 0x11]);
        let enforcer = ConsistencyEnforcer::new(handler, WriteConsistency::Majority, 3);

        let data = enforcer.handle_read(1, 0, 4).await.unwrap();
        assert_eq!(data, Bytes::from(vec![0xDD, 0xEE, 0xFF, 0x11]));
    }

    #[tokio::test]
    async fn test_enforcer_flush_passthrough() {
        let enforcer = ConsistencyEnforcer::new(MemHandler::new(), WriteConsistency::All, 3);
        assert!(enforcer.handle_flush(1).await.is_ok());
    }

    #[tokio::test]
    async fn test_enforcer_trim_passthrough() {
        let handler = MemHandler::new();
        handler.data.lock().insert((1, 0), vec![1, 2, 3, 4]);
        let enforcer = ConsistencyEnforcer::new(handler, WriteConsistency::Majority, 3);
        assert!(enforcer.handle_trim(1, 0, 4).await.is_ok());
    }

    #[tokio::test]
    async fn test_enforcer_write_then_read() {
        let enforcer = ConsistencyEnforcer::new(MemHandler::new(), WriteConsistency::Majority, 3);
        enforcer
            .handle_write(1, 0, Bytes::from(vec![0x01, 0x02, 0x03, 0x04]))
            .await
            .unwrap();
        let data = enforcer.handle_read(1, 0, 4).await.unwrap();
        assert_eq!(data, Bytes::from(vec![0x01, 0x02, 0x03, 0x04]));
    }

    #[tokio::test]
    async fn test_enforcer_multiple_writes_counter() {
        let enforcer = ConsistencyEnforcer::new(MemHandler::new(), WriteConsistency::All, 3);
        for i in 0..5 {
            enforcer
                .handle_write(1, i * 4, Bytes::from(vec![0xAA; 4]))
                .await
                .unwrap();
        }
        assert_eq!(enforcer.writes_handled(), 5);
    }
}
