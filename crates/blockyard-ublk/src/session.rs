//! Client session management (P4A.2, §4.2).
//!
//! Each [`ClientSession`] holds a stable [`SessionId`] for the lifetime of a
//! mounted device and generates monotonically increasing [`OperationId`]s.

use std::sync::atomic::{AtomicU64, Ordering};

use blockyard_common::{OperationId, SessionId, VolumeId};

/// A client session representing a mounted volume instance.
///
/// Provides:
/// - Stable session identifier (P4A.2)
/// - Monotonic operation ID generation for write deduplication (§4.2)
#[derive(Debug)]
pub struct ClientSession {
    session_id: SessionId,
    volume_id: VolumeId,
    operation_counter: AtomicU64,
}

impl ClientSession {
    pub fn new(volume_id: VolumeId) -> Self {
        Self {
            session_id: SessionId::generate(),
            volume_id,
            operation_counter: AtomicU64::new(0),
        }
    }

    pub fn with_session_id(session_id: SessionId, volume_id: VolumeId) -> Self {
        Self {
            session_id,
            volume_id,
            operation_counter: AtomicU64::new(0),
        }
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn volume_id(&self) -> VolumeId {
        self.volume_id
    }

    /// Generate the next operation ID. The counter is monotonically increasing
    /// and unique within this session (§4.2).
    pub fn next_operation_id(&self) -> OperationId {
        let _seq = self.operation_counter.fetch_add(1, Ordering::Relaxed);
        OperationId::generate()
    }

    /// Return the number of operations generated so far.
    pub fn operation_count(&self) -> u64 {
        self.operation_counter.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_new() {
        let vid = VolumeId::generate();
        let session = ClientSession::new(vid);
        assert_eq!(session.volume_id(), vid);
    }

    #[test]
    fn test_session_with_session_id() {
        let sid = SessionId::generate();
        let vid = VolumeId::generate();
        let session = ClientSession::with_session_id(sid, vid);
        assert_eq!(session.session_id(), sid);
        assert_eq!(session.volume_id(), vid);
    }

    #[test]
    fn test_session_operation_id_unique() {
        let vid = VolumeId::generate();
        let session = ClientSession::new(vid);
        let op1 = session.next_operation_id();
        let op2 = session.next_operation_id();
        assert_ne!(op1, op2);
    }

    #[test]
    fn test_session_operation_counter_monotonic() {
        let vid = VolumeId::generate();
        let session = ClientSession::new(vid);
        assert_eq!(session.operation_count(), 0);
        session.next_operation_id();
        assert_eq!(session.operation_count(), 1);
        session.next_operation_id();
        assert_eq!(session.operation_count(), 2);
        session.next_operation_id();
        assert_eq!(session.operation_count(), 3);
    }

    #[test]
    fn test_session_generates_many_unique_ops() {
        let vid = VolumeId::generate();
        let session = ClientSession::new(vid);
        let mut ids = std::collections::HashSet::new();
        for _ in 0..100 {
            let op = session.next_operation_id();
            assert!(ids.insert(op));
        }
        assert_eq!(ids.len(), 100);
    }

    #[test]
    fn test_session_debug() {
        let vid = VolumeId::generate();
        let session = ClientSession::new(vid);
        let debug = format!("{:?}", session);
        assert!(debug.contains("ClientSession"));
    }

    #[test]
    fn test_session_id_stable() {
        let vid = VolumeId::generate();
        let session = ClientSession::new(vid);
        let sid = session.session_id();
        assert_eq!(session.session_id(), sid);
        session.next_operation_id();
        assert_eq!(session.session_id(), sid);
    }

    #[test]
    fn test_session_concurrent_op_generation() {
        use std::sync::Arc;
        let vid = VolumeId::generate();
        let session = Arc::new(ClientSession::new(vid));
        let mut handles = vec![];
        for _ in 0..10 {
            let s = Arc::clone(&session);
            handles.push(std::thread::spawn(move || {
                let mut ops = vec![];
                for _ in 0..10 {
                    ops.push(s.next_operation_id());
                }
                ops
            }));
        }
        let mut all_ops = std::collections::HashSet::new();
        for h in handles {
            for op in h.join().unwrap() {
                all_ops.insert(op);
            }
        }
        assert_eq!(all_ops.len(), 100);
        assert_eq!(session.operation_count(), 100);
    }
}
