//! Session write watermark (P4A.4, §4.4).
//!
//! Monotonically non-decreasing tracker of the latest committed metadata version.
//! Advanced on successful commit; subsequent reads must see at least this version.

use std::sync::atomic::{AtomicU64, Ordering};

use blockyard_common::EpochId;

/// A monotonically non-decreasing watermark tracking the latest committed version.
///
/// The watermark is used to enforce read-your-own-writes semantics (§4.4):
/// before serving a read, the client must verify that the metadata view is
/// at least as new as this watermark.
#[derive(Debug)]
pub struct WriteWatermark {
    value: AtomicU64,
}

impl WriteWatermark {
    pub fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    pub fn with_initial(epoch: EpochId) -> Self {
        Self {
            value: AtomicU64::new(epoch.as_u64()),
        }
    }

    /// Get the current watermark value as a u64 commit version.
    pub fn current_version(&self) -> u64 {
        self.value.load(Ordering::Acquire)
    }

    /// Get the current watermark value.
    pub fn current(&self) -> EpochId {
        EpochId::new(self.value.load(Ordering::Acquire))
    }

    /// Advance the watermark to `epoch` if it is greater than the current value.
    /// Returns `true` if the watermark was actually advanced.
    pub fn advance(&self, epoch: EpochId) -> bool {
        self.advance_to(epoch.as_u64())
    }

    /// Advance the watermark to the given commit version if it is greater.
    /// Returns `true` if the watermark was actually advanced.
    pub fn advance_to(&self, version: u64) -> bool {
        loop {
            let current = self.value.load(Ordering::Acquire);
            if version <= current {
                return false;
            }
            match self.value.compare_exchange_weak(
                current,
                version,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    /// Check if a given epoch meets the watermark requirement.
    pub fn is_fresh(&self, epoch: EpochId) -> bool {
        epoch.as_u64() >= self.value.load(Ordering::Acquire)
    }
}

impl Default for WriteWatermark {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_new() {
        let wm = WriteWatermark::new();
        assert_eq!(wm.current(), EpochId::new(0));
    }

    #[test]
    fn test_watermark_default() {
        let wm = WriteWatermark::default();
        assert_eq!(wm.current(), EpochId::new(0));
    }

    #[test]
    fn test_watermark_with_initial() {
        let wm = WriteWatermark::with_initial(EpochId::new(10));
        assert_eq!(wm.current(), EpochId::new(10));
    }

    #[test]
    fn test_watermark_advance() {
        let wm = WriteWatermark::new();
        assert!(wm.advance(EpochId::new(5)));
        assert_eq!(wm.current(), EpochId::new(5));
    }

    #[test]
    fn test_watermark_advance_monotonic() {
        let wm = WriteWatermark::new();
        assert!(wm.advance(EpochId::new(5)));
        assert!(!wm.advance(EpochId::new(3)));
        assert_eq!(wm.current(), EpochId::new(5));
    }

    #[test]
    fn test_watermark_advance_same_value() {
        let wm = WriteWatermark::new();
        assert!(wm.advance(EpochId::new(5)));
        assert!(!wm.advance(EpochId::new(5)));
        assert_eq!(wm.current(), EpochId::new(5));
    }

    #[test]
    fn test_watermark_advance_increasing() {
        let wm = WriteWatermark::new();
        for i in 1..=10 {
            assert!(wm.advance(EpochId::new(i)));
            assert_eq!(wm.current(), EpochId::new(i));
        }
    }

    #[test]
    fn test_watermark_is_fresh() {
        let wm = WriteWatermark::new();
        wm.advance(EpochId::new(5));
        assert!(wm.is_fresh(EpochId::new(5)));
        assert!(wm.is_fresh(EpochId::new(6)));
        assert!(wm.is_fresh(EpochId::new(100)));
        assert!(!wm.is_fresh(EpochId::new(4)));
        assert!(!wm.is_fresh(EpochId::new(0)));
    }

    #[test]
    fn test_watermark_is_fresh_zero() {
        let wm = WriteWatermark::new();
        assert!(wm.is_fresh(EpochId::new(0)));
        assert!(wm.is_fresh(EpochId::new(1)));
    }

    #[test]
    fn test_watermark_debug() {
        let wm = WriteWatermark::new();
        let debug = format!("{:?}", wm);
        assert!(debug.contains("WriteWatermark"));
    }

    #[test]
    fn test_watermark_concurrent_advance() {
        use std::sync::Arc;
        let wm = Arc::new(WriteWatermark::new());
        let mut handles = vec![];
        for i in 0..10 {
            let w = Arc::clone(&wm);
            handles.push(std::thread::spawn(move || {
                for j in 0..10 {
                    w.advance(EpochId::new(i * 10 + j));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert!(wm.current().as_u64() >= 90);
    }

    #[test]
    fn test_watermark_advance_zero_from_zero() {
        let wm = WriteWatermark::new();
        assert!(!wm.advance(EpochId::new(0)));
        assert_eq!(wm.current(), EpochId::new(0));
    }

    #[test]
    fn test_watermark_with_initial_advance() {
        let wm = WriteWatermark::with_initial(EpochId::new(10));
        assert!(!wm.advance(EpochId::new(5)));
        assert!(wm.advance(EpochId::new(15)));
        assert_eq!(wm.current(), EpochId::new(15));
    }
}
