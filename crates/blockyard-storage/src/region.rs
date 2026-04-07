//! Bad region tracking for disks (§5.9).
//!
//! Maintains a map of quarantined byte ranges on a disk. New extents must
//! not be placed in quarantined regions, and existing extents overlapping
//! bad regions should be reported for repair.

use blockyard_common::{DiskId, ExtentId};

/// A contiguous bad region on a disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadRegion {
    pub offset: u64,
    pub length: u64,
}

impl BadRegion {
    pub fn end(&self) -> u64 {
        self.offset.saturating_add(self.length)
    }

    pub fn overlaps(&self, offset: u64, length: u64) -> bool {
        let end = offset.saturating_add(length);
        self.offset < end && offset < self.end()
    }
}

/// Per-disk bad region map.
#[derive(Debug)]
pub struct BadRegionMap {
    pub disk_id: DiskId,
    regions: Vec<BadRegion>,
}

impl BadRegionMap {
    pub fn new(disk_id: DiskId) -> Self {
        Self {
            disk_id,
            regions: Vec::new(),
        }
    }

    /// Add a bad region to the map.
    pub fn add_region(&mut self, offset: u64, length: u64) {
        if length == 0 {
            return;
        }
        self.regions.push(BadRegion { offset, length });
    }

    /// Check whether a given range overlaps any bad region.
    pub fn overlaps(&self, offset: u64, length: u64) -> bool {
        self.regions.iter().any(|r| r.overlaps(offset, length))
    }

    /// Return all bad regions.
    pub fn regions(&self) -> &[BadRegion] {
        &self.regions
    }

    /// Return the total number of quarantined regions.
    pub fn count(&self) -> usize {
        self.regions.len()
    }

    /// Find extent IDs that overlap with bad regions.
    /// Caller provides a mapping of (ExtentId, offset, length) for extents on this disk.
    pub fn affected_extents(&self, extents: &[(ExtentId, u64, u64)]) -> Vec<ExtentId> {
        extents
            .iter()
            .filter(|(_, offset, length)| self.overlaps(*offset, *length))
            .map(|(id, _, _)| *id)
            .collect()
    }

    /// Clear all regions.
    pub fn clear(&mut self) {
        self.regions.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bad_region_end() {
        let r = BadRegion {
            offset: 100,
            length: 50,
        };
        assert_eq!(r.end(), 150);
    }

    #[test]
    fn test_bad_region_end_overflow() {
        let r = BadRegion {
            offset: u64::MAX - 10,
            length: 100,
        };
        assert_eq!(r.end(), u64::MAX);
    }

    #[test]
    fn test_overlaps_exact() {
        let r = BadRegion {
            offset: 100,
            length: 50,
        };
        assert!(r.overlaps(100, 50));
    }

    #[test]
    fn test_overlaps_partial_start() {
        let r = BadRegion {
            offset: 100,
            length: 50,
        };
        assert!(r.overlaps(90, 20));
    }

    #[test]
    fn test_overlaps_partial_end() {
        let r = BadRegion {
            offset: 100,
            length: 50,
        };
        assert!(r.overlaps(140, 20));
    }

    #[test]
    fn test_overlaps_contained() {
        let r = BadRegion {
            offset: 100,
            length: 50,
        };
        assert!(r.overlaps(110, 10));
    }

    #[test]
    fn test_overlaps_containing() {
        let r = BadRegion {
            offset: 100,
            length: 50,
        };
        assert!(r.overlaps(80, 100));
    }

    #[test]
    fn test_no_overlap_before() {
        let r = BadRegion {
            offset: 100,
            length: 50,
        };
        assert!(!r.overlaps(50, 50));
    }

    #[test]
    fn test_no_overlap_after() {
        let r = BadRegion {
            offset: 100,
            length: 50,
        };
        assert!(!r.overlaps(150, 50));
    }

    #[test]
    fn test_no_overlap_adjacent_before() {
        let r = BadRegion {
            offset: 100,
            length: 50,
        };
        assert!(!r.overlaps(50, 50));
    }

    #[test]
    fn test_map_add_and_check() {
        let mut map = BadRegionMap::new(DiskId::generate());
        map.add_region(100, 50);
        assert!(map.overlaps(120, 10));
        assert!(!map.overlaps(200, 10));
    }

    #[test]
    fn test_map_zero_length_ignored() {
        let mut map = BadRegionMap::new(DiskId::generate());
        map.add_region(100, 0);
        assert_eq!(map.count(), 0);
    }

    #[test]
    fn test_map_multiple_regions() {
        let mut map = BadRegionMap::new(DiskId::generate());
        map.add_region(100, 50);
        map.add_region(300, 50);
        assert!(map.overlaps(120, 10));
        assert!(map.overlaps(320, 10));
        assert!(!map.overlaps(200, 50));
    }

    #[test]
    fn test_map_regions() {
        let mut map = BadRegionMap::new(DiskId::generate());
        map.add_region(100, 50);
        map.add_region(300, 50);
        assert_eq!(map.regions().len(), 2);
    }

    #[test]
    fn test_map_count() {
        let mut map = BadRegionMap::new(DiskId::generate());
        assert_eq!(map.count(), 0);
        map.add_region(100, 50);
        assert_eq!(map.count(), 1);
    }

    #[test]
    fn test_map_clear() {
        let mut map = BadRegionMap::new(DiskId::generate());
        map.add_region(100, 50);
        assert_eq!(map.count(), 1);
        map.clear();
        assert_eq!(map.count(), 0);
    }

    #[test]
    fn test_affected_extents() {
        let mut map = BadRegionMap::new(DiskId::generate());
        map.add_region(100, 50);

        let e1 = ExtentId::generate();
        let e2 = ExtentId::generate();
        let e3 = ExtentId::generate();

        let extents = vec![(e1, 120, 10), (e2, 200, 10), (e3, 90, 20)];

        let affected = map.affected_extents(&extents);
        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&e1));
        assert!(affected.contains(&e3));
        assert!(!affected.contains(&e2));
    }

    #[test]
    fn test_affected_extents_none() {
        let map = BadRegionMap::new(DiskId::generate());
        let e1 = ExtentId::generate();
        let extents = vec![(e1, 0, 100)];
        assert!(map.affected_extents(&extents).is_empty());
    }

    #[test]
    fn test_empty_map_no_overlaps() {
        let map = BadRegionMap::new(DiskId::generate());
        assert!(!map.overlaps(0, 1000));
    }
}
