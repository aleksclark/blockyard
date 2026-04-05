use blockyard_common::types::ExtentId;
use serde::{Deserialize, Serialize};
use std::ops::Range;

pub const DEFAULT_EXTENT_SIZE: u64 = 4 * 1024 * 1024; // 4MB

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Extent {
    pub id: ExtentId,
    pub offset: u64,
    pub size: u64,
}

impl Extent {
    pub fn new(id: ExtentId, offset: u64, size: u64) -> Self {
        Self { id, offset, size }
    }

    pub fn byte_range(&self) -> Range<u64> {
        self.offset..self.offset + self.size
    }

    pub fn contains_offset(&self, byte_offset: u64) -> bool {
        byte_offset >= self.offset && byte_offset < self.offset + self.size
    }

    pub fn end(&self) -> u64 {
        self.offset + self.size
    }
}

#[derive(Debug, Clone)]
pub struct ExtentMap {
    extent_size: u64,
    volume_size: u64,
    count: u64,
}

impl ExtentMap {
    pub fn new(volume_size: u64, extent_size: u64) -> Self {
        let count = volume_size.div_ceil(extent_size);
        Self {
            extent_size,
            volume_size,
            count,
        }
    }

    pub fn with_default_size(volume_size: u64) -> Self {
        Self::new(volume_size, DEFAULT_EXTENT_SIZE)
    }

    pub fn extent_size(&self) -> u64 {
        self.extent_size
    }

    pub fn volume_size(&self) -> u64 {
        self.volume_size
    }

    pub fn extent_count(&self) -> u64 {
        self.count
    }

    pub fn extent_for_offset(&self, byte_offset: u64) -> Option<Extent> {
        if byte_offset >= self.volume_size {
            return None;
        }
        let id = byte_offset / self.extent_size;
        let offset = id * self.extent_size;
        let remaining = self.volume_size - offset;
        let size = remaining.min(self.extent_size);
        Some(Extent::new(id, offset, size))
    }

    pub fn extent_by_id(&self, id: ExtentId) -> Option<Extent> {
        if id >= self.count {
            return None;
        }
        let offset = id * self.extent_size;
        let remaining = self.volume_size - offset;
        let size = remaining.min(self.extent_size);
        Some(Extent::new(id, offset, size))
    }

    pub fn extents_for_range(&self, start: u64, length: u64) -> Vec<Extent> {
        if start >= self.volume_size || length == 0 {
            return Vec::new();
        }
        let end = (start + length).min(self.volume_size);
        let first_id = start / self.extent_size;
        let last_id = (end - 1) / self.extent_size;
        (first_id..=last_id)
            .filter_map(|id| self.extent_by_id(id))
            .collect()
    }

    pub fn all_extents(&self) -> Vec<Extent> {
        (0..self.count)
            .map(|id| self.extent_by_id(id).unwrap())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1024 * 1024;
    const GB: u64 = 1024 * MB;

    #[test]
    fn test_extent_new() {
        let e = Extent::new(0, 0, 4 * MB);
        assert_eq!(e.id, 0);
        assert_eq!(e.offset, 0);
        assert_eq!(e.size, 4 * MB);
    }

    #[test]
    fn test_extent_byte_range() {
        let e = Extent::new(1, 4 * MB, 4 * MB);
        let range = e.byte_range();
        assert_eq!(range.start, 4 * MB);
        assert_eq!(range.end, 8 * MB);
    }

    #[test]
    fn test_extent_contains_offset() {
        let e = Extent::new(0, 0, 4 * MB);
        assert!(e.contains_offset(0));
        assert!(e.contains_offset(2 * MB));
        assert!(!e.contains_offset(4 * MB));
        assert!(!e.contains_offset(10 * MB));
    }

    #[test]
    fn test_extent_end() {
        let e = Extent::new(0, 0, 4 * MB);
        assert_eq!(e.end(), 4 * MB);
    }

    #[test]
    fn test_extent_map_basic() {
        let map = ExtentMap::new(100 * GB, 4 * MB);
        assert_eq!(map.extent_size(), 4 * MB);
        assert_eq!(map.volume_size(), 100 * GB);
        assert_eq!(map.extent_count(), 100 * GB / (4 * MB));
    }

    #[test]
    fn test_extent_map_default_size() {
        let map = ExtentMap::with_default_size(16 * MB);
        assert_eq!(map.extent_size(), DEFAULT_EXTENT_SIZE);
        assert_eq!(map.extent_count(), 4);
    }

    #[test]
    fn test_extent_map_non_aligned() {
        let map = ExtentMap::new(10 * MB, 4 * MB);
        assert_eq!(map.extent_count(), 3);

        let last = map.extent_by_id(2).unwrap();
        assert_eq!(last.size, 2 * MB);
    }

    #[test]
    fn test_extent_for_offset() {
        let map = ExtentMap::new(16 * MB, 4 * MB);

        let e0 = map.extent_for_offset(0).unwrap();
        assert_eq!(e0.id, 0);
        assert_eq!(e0.offset, 0);

        let e1 = map.extent_for_offset(5 * MB).unwrap();
        assert_eq!(e1.id, 1);
        assert_eq!(e1.offset, 4 * MB);

        let e3 = map.extent_for_offset(15 * MB).unwrap();
        assert_eq!(e3.id, 3);
    }

    #[test]
    fn test_extent_for_offset_out_of_bounds() {
        let map = ExtentMap::new(16 * MB, 4 * MB);
        assert!(map.extent_for_offset(16 * MB).is_none());
        assert!(map.extent_for_offset(100 * MB).is_none());
    }

    #[test]
    fn test_extent_by_id() {
        let map = ExtentMap::new(16 * MB, 4 * MB);

        let e = map.extent_by_id(2).unwrap();
        assert_eq!(e.offset, 8 * MB);
        assert_eq!(e.size, 4 * MB);

        assert!(map.extent_by_id(4).is_none());
    }

    #[test]
    fn test_extents_for_range_single() {
        let map = ExtentMap::new(16 * MB, 4 * MB);
        let extents = map.extents_for_range(1 * MB, 2 * MB);
        assert_eq!(extents.len(), 1);
        assert_eq!(extents[0].id, 0);
    }

    #[test]
    fn test_extents_for_range_spanning() {
        let map = ExtentMap::new(16 * MB, 4 * MB);
        let extents = map.extents_for_range(3 * MB, 6 * MB);
        assert_eq!(extents.len(), 3);
        assert_eq!(extents[0].id, 0);
        assert_eq!(extents[1].id, 1);
        assert_eq!(extents[2].id, 2);
    }

    #[test]
    fn test_extents_for_range_empty() {
        let map = ExtentMap::new(16 * MB, 4 * MB);
        assert!(map.extents_for_range(20 * MB, 1 * MB).is_empty());
        assert!(map.extents_for_range(0, 0).is_empty());
    }

    #[test]
    fn test_extents_for_range_clamps_to_volume() {
        let map = ExtentMap::new(16 * MB, 4 * MB);
        let extents = map.extents_for_range(14 * MB, 100 * MB);
        assert_eq!(extents.len(), 1);
        assert_eq!(extents[0].id, 3);
    }

    #[test]
    fn test_all_extents() {
        let map = ExtentMap::new(12 * MB, 4 * MB);
        let all = map.all_extents();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, 0);
        assert_eq!(all[1].id, 1);
        assert_eq!(all[2].id, 2);
        assert_eq!(all[0].size, 4 * MB);
        assert_eq!(all[2].size, 4 * MB);
    }

    #[test]
    fn test_all_extents_non_aligned() {
        let map = ExtentMap::new(10 * MB, 4 * MB);
        let all = map.all_extents();
        assert_eq!(all.len(), 3);
        assert_eq!(all[2].size, 2 * MB);
    }

    #[test]
    fn test_extent_serialization() {
        let e = Extent::new(42, 168 * MB, 4 * MB);
        let json = serde_json::to_string(&e).unwrap();
        let decoded: Extent = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, e);
    }

    #[test]
    fn test_zero_volume() {
        let map = ExtentMap::new(0, 4 * MB);
        assert_eq!(map.extent_count(), 0);
        assert!(map.extent_for_offset(0).is_none());
        assert!(map.all_extents().is_empty());
    }

    #[test]
    fn test_single_byte_volume() {
        let map = ExtentMap::new(1, 4 * MB);
        assert_eq!(map.extent_count(), 1);
        let e = map.extent_by_id(0).unwrap();
        assert_eq!(e.size, 1);
    }
}
