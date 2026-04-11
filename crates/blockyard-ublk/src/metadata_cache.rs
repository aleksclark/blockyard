//! Metadata cache for client-side placement and volume information (P4A.3, §4.3).
//!
//! Caches placement epoch, cluster map (node addresses), volume protection
//! policy, and extent mappings. Supports atomic refresh via interior mutability.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::ops::Range;

use parking_lot::RwLock;

use blockyard_common::{EpochId, ExtentId, NodeId, ProtectionPolicy, VolumeId};

/// A cached extent mapping entry.
#[derive(Debug, Clone)]
pub struct CachedExtentMapping {
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub replica_locations: Vec<NodeId>,
    pub checksums: Vec<Vec<u8>>,
    pub size_bytes: u64,
}

/// Per-volume cached metadata.
#[derive(Debug, Clone)]
pub struct CachedVolumeInfo {
    pub volume_id: VolumeId,
    pub size_bytes: u64,
    pub block_size: u32,
    pub protection: ProtectionPolicy,
    pub extent_mappings: BTreeMap<u64, CachedExtentMapping>,
}

/// Node address in the cluster map.
#[derive(Debug, Clone)]
pub struct NodeAddress {
    pub node_id: NodeId,
    pub addr: SocketAddr,
}

/// Inner state that can be atomically swapped.
#[derive(Debug, Clone)]
struct CacheInner {
    epoch: EpochId,
    cluster_map: BTreeMap<String, NodeAddress>,
    volumes: BTreeMap<String, CachedVolumeInfo>,
}

impl Default for CacheInner {
    fn default() -> Self {
        Self {
            epoch: EpochId::new(0),
            cluster_map: BTreeMap::new(),
            volumes: BTreeMap::new(),
        }
    }
}

/// Metadata cache supporting atomic refresh (P4A.3).
///
/// All reads are lock-free through `parking_lot::RwLock` (readers never block
/// each other). Refresh replaces the entire inner state atomically.
#[derive(Debug)]
pub struct MetadataCache {
    inner: RwLock<CacheInner>,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(CacheInner::default()),
        }
    }

    pub fn current_epoch(&self) -> EpochId {
        self.inner.read().epoch
    }

    pub fn set_epoch(&self, epoch: EpochId) {
        self.inner.write().epoch = epoch;
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<NodeAddress> {
        self.inner
            .read()
            .cluster_map
            .get(&node_id.to_string())
            .cloned()
    }

    pub fn set_node(&self, node_id: NodeId, addr: SocketAddr) {
        let entry = NodeAddress { node_id, addr };
        self.inner
            .write()
            .cluster_map
            .insert(node_id.to_string(), entry);
    }

    pub fn remove_node(&self, node_id: &NodeId) {
        self.inner.write().cluster_map.remove(&node_id.to_string());
    }

    pub fn list_nodes(&self) -> Vec<NodeAddress> {
        self.inner.read().cluster_map.values().cloned().collect()
    }

    pub fn get_volume(&self, volume_id: &VolumeId) -> Option<CachedVolumeInfo> {
        self.inner
            .read()
            .volumes
            .get(&volume_id.to_string())
            .cloned()
    }

    pub fn set_volume(&self, info: CachedVolumeInfo) {
        self.inner
            .write()
            .volumes
            .insert(info.volume_id.to_string(), info);
    }

    pub fn remove_volume(&self, volume_id: &VolumeId) {
        self.inner.write().volumes.remove(&volume_id.to_string());
    }

    pub fn get_extent_mapping(
        &self,
        volume_id: &VolumeId,
        block_start: u64,
    ) -> Option<CachedExtentMapping> {
        let inner = self.inner.read();
        inner
            .volumes
            .get(&volume_id.to_string())?
            .extent_mappings
            .get(&block_start)
            .cloned()
    }

    pub fn set_extent_mapping(
        &self,
        volume_id: &VolumeId,
        block_start: u64,
        mapping: CachedExtentMapping,
    ) {
        let mut inner = self.inner.write();
        if let Some(vol) = inner.volumes.get_mut(&volume_id.to_string()) {
            vol.extent_mappings.insert(block_start, mapping);
        }
    }

    /// Get the extent mapping that covers a given block range.
    /// Finds the mapping whose key is <= block_range.start and whose
    /// range covers the requested start.
    pub fn find_extent_for_range(
        &self,
        volume_id: &VolumeId,
        block_range: &Range<u64>,
    ) -> Option<(u64, CachedExtentMapping)> {
        let inner = self.inner.read();
        let vol = inner.volumes.get(&volume_id.to_string())?;
        let (block_start, mapping) = vol
            .extent_mappings
            .range(..=block_range.start)
            .next_back()
            .map(|(k, v)| (*k, v.clone()))?;

        // Verify the requested range actually falls within this extent's coverage.
        // The extent covers [block_start, block_start + extent_blocks).
        if mapping.size_bytes > 0 {
            let block_size = vol.block_size.max(1) as u64;
            let extent_blocks = mapping.size_bytes.div_ceil(block_size);
            let extent_end_block = block_start + extent_blocks;
            if block_range.start >= extent_end_block {
                return None; // requested range is beyond this extent
            }
        }

        Some((block_start, mapping))
    }

    /// Atomically refresh the entire cache with a new snapshot.
    pub fn refresh(&self, epoch: EpochId, nodes: Vec<NodeAddress>, volumes: Vec<CachedVolumeInfo>) {
        let mut cluster_map = BTreeMap::new();
        for n in nodes {
            cluster_map.insert(n.node_id.to_string(), n);
        }
        let mut inner = self.inner.write();
        let mut vol_map = BTreeMap::new();
        for mut v in volumes {
            if let Some(existing) = inner.volumes.get(&v.volume_id.to_string()) {
                if v.extent_mappings.is_empty() && !existing.extent_mappings.is_empty() {
                    v.extent_mappings = existing.extent_mappings.clone();
                }
            }
            vol_map.insert(v.volume_id.to_string(), v);
        }
        inner.epoch = epoch;
        inner.cluster_map = cluster_map;
        inner.volumes = vol_map;
    }
}

impl Default for MetadataCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn test_cache_new_defaults() {
        let cache = MetadataCache::new();
        assert_eq!(cache.current_epoch(), EpochId::new(0));
        assert!(cache.list_nodes().is_empty());
    }

    #[test]
    fn test_cache_default_trait() {
        let cache = MetadataCache::default();
        assert_eq!(cache.current_epoch(), EpochId::new(0));
    }

    #[test]
    fn test_cache_set_get_epoch() {
        let cache = MetadataCache::new();
        cache.set_epoch(EpochId::new(42));
        assert_eq!(cache.current_epoch(), EpochId::new(42));
    }

    #[test]
    fn test_cache_set_get_node() {
        let cache = MetadataCache::new();
        let nid = NodeId::generate();
        cache.set_node(nid, addr("10.0.0.1:9800"));
        let node = cache.get_node(&nid).unwrap();
        assert_eq!(node.node_id, nid);
        assert_eq!(node.addr, addr("10.0.0.1:9800"));
    }

    #[test]
    fn test_cache_get_node_not_found() {
        let cache = MetadataCache::new();
        assert!(cache.get_node(&NodeId::generate()).is_none());
    }

    #[test]
    fn test_cache_remove_node() {
        let cache = MetadataCache::new();
        let nid = NodeId::generate();
        cache.set_node(nid, addr("10.0.0.1:9800"));
        cache.remove_node(&nid);
        assert!(cache.get_node(&nid).is_none());
    }

    #[test]
    fn test_cache_list_nodes() {
        let cache = MetadataCache::new();
        let n1 = NodeId::generate();
        let n2 = NodeId::generate();
        cache.set_node(n1, addr("10.0.0.1:9800"));
        cache.set_node(n2, addr("10.0.0.2:9800"));
        assert_eq!(cache.list_nodes().len(), 2);
    }

    #[test]
    fn test_cache_set_get_volume() {
        let cache = MetadataCache::new();
        let vid = VolumeId::generate();
        let info = CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        };
        cache.set_volume(info);
        let vol = cache.get_volume(&vid).unwrap();
        assert_eq!(vol.volume_id, vid);
        assert_eq!(vol.size_bytes, 1024);
    }

    #[test]
    fn test_cache_get_volume_not_found() {
        let cache = MetadataCache::new();
        assert!(cache.get_volume(&VolumeId::generate()).is_none());
    }

    #[test]
    fn test_cache_remove_volume() {
        let cache = MetadataCache::new();
        let vid = VolumeId::generate();
        let info = CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        };
        cache.set_volume(info);
        cache.remove_volume(&vid);
        assert!(cache.get_volume(&vid).is_none());
    }

    #[test]
    fn test_cache_set_get_extent_mapping() {
        let cache = MetadataCache::new();
        let vid = VolumeId::generate();
        let eid = ExtentId::generate();
        let n1 = NodeId::generate();
        let info = CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        };
        cache.set_volume(info);

        let mapping = CachedExtentMapping {
            extent_id: eid,
            extent_version: 1,
            replica_locations: vec![n1],
            checksums: vec![vec![0xAA]],
            size_bytes: 524288,
        };
        cache.set_extent_mapping(&vid, 0, mapping);

        let got = cache.get_extent_mapping(&vid, 0).unwrap();
        assert_eq!(got.extent_id, eid);
        assert_eq!(got.extent_version, 1);
    }

    #[test]
    fn test_cache_get_extent_mapping_no_volume() {
        let cache = MetadataCache::new();
        assert!(cache.get_extent_mapping(&VolumeId::generate(), 0).is_none());
    }

    #[test]
    fn test_cache_get_extent_mapping_no_mapping() {
        let cache = MetadataCache::new();
        let vid = VolumeId::generate();
        let info = CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        };
        cache.set_volume(info);
        assert!(cache.get_extent_mapping(&vid, 0).is_none());
    }

    #[test]
    fn test_cache_find_extent_for_range() {
        let cache = MetadataCache::new();
        let vid = VolumeId::generate();
        let eid = ExtentId::generate();
        let mut mappings = BTreeMap::new();
        mappings.insert(
            0,
            CachedExtentMapping {
                extent_id: eid,
                extent_version: 1,
                replica_locations: vec![NodeId::generate()],
                checksums: vec![],
                size_bytes: 524288,
            },
        );
        let info = CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: mappings,
        };
        cache.set_volume(info);

        let result = cache.find_extent_for_range(&vid, &(0..64));
        assert!(result.is_some());
        let (block_start, mapping) = result.unwrap();
        assert_eq!(block_start, 0);
        assert_eq!(mapping.extent_id, eid);
    }

    #[test]
    fn test_cache_find_extent_for_range_not_found() {
        let cache = MetadataCache::new();
        let vid = VolumeId::generate();
        let info = CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        };
        cache.set_volume(info);
        assert!(cache.find_extent_for_range(&vid, &(0..64)).is_none());
    }

    #[test]
    fn test_cache_refresh_atomic() {
        let cache = MetadataCache::new();
        let n1 = NodeId::generate();
        let vid = VolumeId::generate();

        cache.set_node(n1, addr("10.0.0.1:9800"));
        cache.set_epoch(EpochId::new(1));

        let n2 = NodeId::generate();
        let new_nodes = vec![NodeAddress {
            node_id: n2,
            addr: addr("10.0.0.2:9800"),
        }];
        let new_volumes = vec![CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 2048,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 2 },
            extent_mappings: BTreeMap::new(),
        }];
        cache.refresh(EpochId::new(5), new_nodes, new_volumes);

        assert_eq!(cache.current_epoch(), EpochId::new(5));
        assert!(cache.get_node(&n1).is_none());
        assert!(cache.get_node(&n2).is_some());
        assert!(cache.get_volume(&vid).is_some());
    }

    #[test]
    fn test_cache_refresh_clears_old_data() {
        let cache = MetadataCache::new();
        let vid1 = VolumeId::generate();
        let info = CachedVolumeInfo {
            volume_id: vid1,
            size_bytes: 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
            extent_mappings: BTreeMap::new(),
        };
        cache.set_volume(info);
        assert!(cache.get_volume(&vid1).is_some());

        cache.refresh(EpochId::new(1), vec![], vec![]);

        assert!(cache.get_volume(&vid1).is_none());
        assert!(cache.list_nodes().is_empty());
    }

    #[test]
    fn test_cache_debug() {
        let cache = MetadataCache::new();
        let debug = format!("{:?}", cache);
        assert!(debug.contains("MetadataCache"));
    }

    #[test]
    fn test_cached_extent_mapping_clone() {
        let m = CachedExtentMapping {
            extent_id: ExtentId::generate(),
            extent_version: 5,
            replica_locations: vec![NodeId::generate()],
            checksums: vec![vec![1, 2, 3]],
            size_bytes: 1024,
        };
        let cloned = m.clone();
        assert_eq!(m.extent_id, cloned.extent_id);
        assert_eq!(m.extent_version, cloned.extent_version);
    }

    #[test]
    fn test_cached_volume_info_clone() {
        let info = CachedVolumeInfo {
            volume_id: VolumeId::generate(),
            size_bytes: 4096,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        };
        let cloned = info.clone();
        assert_eq!(info.volume_id, cloned.volume_id);
    }

    #[test]
    fn test_node_address_clone() {
        let na = NodeAddress {
            node_id: NodeId::generate(),
            addr: addr("10.0.0.1:9800"),
        };
        let cloned = na.clone();
        assert_eq!(na.node_id, cloned.node_id);
        assert_eq!(na.addr, cloned.addr);
    }

    #[test]
    fn test_cache_set_node_overwrite() {
        let cache = MetadataCache::new();
        let nid = NodeId::generate();
        cache.set_node(nid, addr("10.0.0.1:9800"));
        cache.set_node(nid, addr("10.0.0.2:9800"));
        let node = cache.get_node(&nid).unwrap();
        assert_eq!(node.addr, addr("10.0.0.2:9800"));
    }

    #[test]
    fn test_cache_set_extent_mapping_no_volume_is_noop() {
        let cache = MetadataCache::new();
        let vid = VolumeId::generate();
        let mapping = CachedExtentMapping {
            extent_id: ExtentId::generate(),
            extent_version: 1,
            replica_locations: vec![],
            checksums: vec![],
            size_bytes: 0,
        };
        cache.set_extent_mapping(&vid, 0, mapping);
        assert!(cache.get_extent_mapping(&vid, 0).is_none());
    }

    #[test]
    fn test_find_extent_no_volume() {
        let cache = MetadataCache::new();
        assert!(
            cache
                .find_extent_for_range(&VolumeId::generate(), &(0..64))
                .is_none()
        );
    }

    #[test]
    fn test_remove_node_not_present() {
        let cache = MetadataCache::new();
        cache.remove_node(&NodeId::generate());
    }

    #[test]
    fn test_remove_volume_not_present() {
        let cache = MetadataCache::new();
        cache.remove_volume(&VolumeId::generate());
    }

    #[test]
    fn test_find_extent_inflated_size_no_phantom_match() {
        let cache = MetadataCache::new();
        let vid = VolumeId::generate();
        let eid = ExtentId::generate();
        let mut mappings = BTreeMap::new();
        mappings.insert(
            0,
            CachedExtentMapping {
                extent_id: eid,
                extent_version: 1,
                replica_locations: vec![NodeId::generate()],
                checksums: vec![],
                size_bytes: 4096,
            },
        );
        let info = CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: mappings,
        };
        cache.set_volume(info);

        assert!(cache.find_extent_for_range(&vid, &(0..1)).is_some());
        assert!(
            cache.find_extent_for_range(&vid, &(1..2)).is_none(),
            "block 1 should not match a 4096-byte extent at block 0 (1 block only)"
        );
        assert!(
            cache.find_extent_for_range(&vid, &(128..129)).is_none(),
            "block 128 must not match; old bug with inflated size_bytes=524288 would match"
        );
    }

    #[test]
    fn test_find_extent_uses_volume_block_size() {
        let cache = MetadataCache::new();
        let vid = VolumeId::generate();
        let eid = ExtentId::generate();
        let mut mappings = BTreeMap::new();
        mappings.insert(
            0,
            CachedExtentMapping {
                extent_id: eid,
                extent_version: 1,
                replica_locations: vec![NodeId::generate()],
                checksums: vec![],
                size_bytes: 8192,
            },
        );
        let info = CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: mappings,
        };
        cache.set_volume(info);

        assert!(cache.find_extent_for_range(&vid, &(0..1)).is_some());
        assert!(cache.find_extent_for_range(&vid, &(1..2)).is_some());
        assert!(cache.find_extent_for_range(&vid, &(2..3)).is_none());
    }

    #[test]
    fn test_refresh_preserves_extent_mappings() {
        let cache = MetadataCache::new();
        let vid = VolumeId::generate();
        let eid = ExtentId::generate();
        let n1 = NodeId::generate();

        cache.set_volume(CachedVolumeInfo {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            block_size: 4096,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_mappings: BTreeMap::new(),
        });
        cache.set_extent_mapping(
            &vid,
            0,
            CachedExtentMapping {
                extent_id: eid,
                extent_version: 1,
                replica_locations: vec![n1],
                checksums: vec![vec![0xAA]],
                size_bytes: 4096,
            },
        );
        assert!(cache.get_extent_mapping(&vid, 0).is_some());

        let n2 = NodeId::generate();
        cache.refresh(
            EpochId::new(5),
            vec![NodeAddress {
                node_id: n2,
                addr: addr("10.0.0.2:9800"),
            }],
            vec![CachedVolumeInfo {
                volume_id: vid,
                size_bytes: 1024 * 1024,
                block_size: 4096,
                protection: ProtectionPolicy::Replicated { replicas: 3 },
                extent_mappings: BTreeMap::new(),
            }],
        );

        assert_eq!(cache.current_epoch(), EpochId::new(5));
        let mapping = cache.get_extent_mapping(&vid, 0);
        assert!(
            mapping.is_some(),
            "refresh must preserve locally-cached extent mappings"
        );
        assert_eq!(mapping.unwrap().extent_id, eid);
    }
}
