//! Metadata freshness checks (P4A.5, §4.3, §4.4).
//!
//! Before serving reads, the client must verify that cached metadata is at
//! least as new as the session write watermark. If stale, a refresh is triggered.

use blockyard_common::EpochId;

use crate::metadata_cache::MetadataCache;
use crate::watermark::WriteWatermark;

/// Checks whether cached metadata is fresh enough relative to the session
/// write watermark, and triggers refresh when stale.
#[derive(Debug)]
pub struct FreshnessChecker<'a> {
    cache: &'a MetadataCache,
    watermark: &'a WriteWatermark,
}

/// Result of a freshness check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreshnessStatus {
    Fresh,
    Stale {
        cached_epoch: EpochId,
        required_epoch: EpochId,
    },
}

impl<'a> FreshnessChecker<'a> {
    pub fn new(cache: &'a MetadataCache, watermark: &'a WriteWatermark) -> Self {
        Self { cache, watermark }
    }

    /// Check whether the cached metadata epoch meets the watermark requirement.
    pub fn check(&self) -> FreshnessStatus {
        let cached_epoch = self.cache.current_epoch();
        let required = self.watermark.current();
        if cached_epoch.as_u64() >= required.as_u64() {
            FreshnessStatus::Fresh
        } else {
            FreshnessStatus::Stale {
                cached_epoch,
                required_epoch: required,
            }
        }
    }

    /// Check and return true if fresh, false if stale.
    pub fn is_fresh(&self) -> bool {
        self.check() == FreshnessStatus::Fresh
    }

    /// Check freshness against a specific epoch requirement (e.g., for a
    /// particular read that needs a minimum version).
    pub fn check_against(&self, required: EpochId) -> FreshnessStatus {
        let cached_epoch = self.cache.current_epoch();
        if cached_epoch.as_u64() >= required.as_u64() {
            FreshnessStatus::Fresh
        } else {
            FreshnessStatus::Stale {
                cached_epoch,
                required_epoch: required,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_freshness_fresh_at_zero() {
        let cache = MetadataCache::new();
        let wm = WriteWatermark::new();
        let checker = FreshnessChecker::new(&cache, &wm);
        assert_eq!(checker.check(), FreshnessStatus::Fresh);
        assert!(checker.is_fresh());
    }

    #[test]
    fn test_freshness_fresh_equal_epoch() {
        let cache = MetadataCache::new();
        let wm = WriteWatermark::new();
        cache.set_epoch(EpochId::new(5));
        wm.advance(EpochId::new(5));
        let checker = FreshnessChecker::new(&cache, &wm);
        assert_eq!(checker.check(), FreshnessStatus::Fresh);
    }

    #[test]
    fn test_freshness_fresh_ahead() {
        let cache = MetadataCache::new();
        let wm = WriteWatermark::new();
        cache.set_epoch(EpochId::new(10));
        wm.advance(EpochId::new(5));
        let checker = FreshnessChecker::new(&cache, &wm);
        assert!(checker.is_fresh());
    }

    #[test]
    fn test_freshness_stale() {
        let cache = MetadataCache::new();
        let wm = WriteWatermark::new();
        cache.set_epoch(EpochId::new(3));
        wm.advance(EpochId::new(5));
        let checker = FreshnessChecker::new(&cache, &wm);
        assert!(!checker.is_fresh());
        match checker.check() {
            FreshnessStatus::Stale {
                cached_epoch,
                required_epoch,
            } => {
                assert_eq!(cached_epoch, EpochId::new(3));
                assert_eq!(required_epoch, EpochId::new(5));
            }
            FreshnessStatus::Fresh => panic!("expected stale"),
        }
    }

    #[test]
    fn test_freshness_becomes_fresh_after_refresh() {
        let cache = MetadataCache::new();
        let wm = WriteWatermark::new();
        wm.advance(EpochId::new(5));
        let checker = FreshnessChecker::new(&cache, &wm);
        assert!(!checker.is_fresh());

        cache.set_epoch(EpochId::new(5));
        assert!(checker.is_fresh());
    }

    #[test]
    fn test_freshness_check_against_specific_epoch() {
        let cache = MetadataCache::new();
        let wm = WriteWatermark::new();
        cache.set_epoch(EpochId::new(3));
        let checker = FreshnessChecker::new(&cache, &wm);

        assert_eq!(
            checker.check_against(EpochId::new(3)),
            FreshnessStatus::Fresh
        );
        assert_eq!(
            checker.check_against(EpochId::new(2)),
            FreshnessStatus::Fresh
        );
        match checker.check_against(EpochId::new(5)) {
            FreshnessStatus::Stale {
                cached_epoch,
                required_epoch,
            } => {
                assert_eq!(cached_epoch, EpochId::new(3));
                assert_eq!(required_epoch, EpochId::new(5));
            }
            FreshnessStatus::Fresh => panic!("expected stale"),
        }
    }

    #[test]
    fn test_freshness_debug() {
        let cache = MetadataCache::new();
        let wm = WriteWatermark::new();
        let checker = FreshnessChecker::new(&cache, &wm);
        let debug = format!("{:?}", checker);
        assert!(debug.contains("FreshnessChecker"));
    }

    #[test]
    fn test_freshness_status_debug() {
        let status = FreshnessStatus::Fresh;
        let debug = format!("{:?}", status);
        assert_eq!(debug, "Fresh");
    }

    #[test]
    fn test_freshness_status_stale_debug() {
        let status = FreshnessStatus::Stale {
            cached_epoch: EpochId::new(1),
            required_epoch: EpochId::new(5),
        };
        let debug = format!("{:?}", status);
        assert!(debug.contains("Stale"));
    }

    #[test]
    fn test_freshness_status_clone() {
        let status = FreshnessStatus::Fresh;
        let cloned = status.clone();
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_freshness_status_eq() {
        assert_eq!(FreshnessStatus::Fresh, FreshnessStatus::Fresh);
        assert_ne!(
            FreshnessStatus::Fresh,
            FreshnessStatus::Stale {
                cached_epoch: EpochId::new(0),
                required_epoch: EpochId::new(1),
            }
        );
    }

    #[test]
    fn test_freshness_watermark_at_zero_always_fresh() {
        let cache = MetadataCache::new();
        let wm = WriteWatermark::new();
        cache.set_epoch(EpochId::new(0));
        let checker = FreshnessChecker::new(&cache, &wm);
        assert!(checker.is_fresh());
    }

    #[test]
    fn test_freshness_check_against_zero() {
        let cache = MetadataCache::new();
        let wm = WriteWatermark::new();
        let checker = FreshnessChecker::new(&cache, &wm);
        assert_eq!(
            checker.check_against(EpochId::new(0)),
            FreshnessStatus::Fresh
        );
    }

    #[test]
    fn test_bounded_staleness_freshness_checker() {
        let cache = MetadataCache::new();
        let watermark = WriteWatermark::new();

        cache.set_epoch(EpochId::new(5));
        watermark.advance(EpochId::new(5));

        let checker = FreshnessChecker::new(&cache, &watermark);
        assert!(checker.is_fresh());
        assert_eq!(checker.check(), FreshnessStatus::Fresh);

        watermark.advance(EpochId::new(10));

        let checker2 = FreshnessChecker::new(&cache, &watermark);
        assert!(!checker2.is_fresh());
        match checker2.check() {
            FreshnessStatus::Stale {
                cached_epoch,
                required_epoch,
            } => {
                assert_eq!(cached_epoch, EpochId::new(5));
                assert_eq!(required_epoch, EpochId::new(10));
            }
            FreshnessStatus::Fresh => panic!("expected Stale status"),
        }

        let status = checker2.check_against(EpochId::new(7));
        match status {
            FreshnessStatus::Stale {
                cached_epoch,
                required_epoch,
            } => {
                assert_eq!(cached_epoch, EpochId::new(5));
                assert_eq!(required_epoch, EpochId::new(7));
            }
            FreshnessStatus::Fresh => panic!("expected Stale for epoch 7"),
        }

        cache.set_epoch(EpochId::new(10));
        let checker3 = FreshnessChecker::new(&cache, &watermark);
        assert!(checker3.is_fresh());
        assert_eq!(checker3.check(), FreshnessStatus::Fresh);
    }
}
