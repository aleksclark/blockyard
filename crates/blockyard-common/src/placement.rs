//! Deterministic placement engine for extent mapping (§4.5).
//!
//! Removes Raft from the IO hot path by computing extent IDs and node
//! assignments deterministically from `(volume_id, block_num)`. Both the
//! write and read paths use the same computation, so no consensus round-trip
//! is needed to locate data.

use uuid::Uuid;

use crate::{ExtentId, NodeId, VolumeId};

/// Deterministic placement engine.
///
/// Given `(volume_id, block_num)` and a sorted node list, computes:
/// - A deterministic [`ExtentId`] (derived from blake3 hash).
/// - A deterministic extent version (always 1 — overwrites reuse the same version).
/// - An ordered set of target nodes for replicas / EC fragments.
pub struct PlacementEngine;

impl PlacementEngine {
    /// Derive a deterministic [`ExtentId`] from volume + extent number.
    ///
    /// With configurable extent_size, multiple blocks map to the same extent.
    /// Use `block_to_extent()` to convert a block_num to an extent_num first.
    pub fn extent_id_for_extent(volume_id: VolumeId, extent_num: u64) -> ExtentId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"blockyard.extent.v1");
        hasher.update(volume_id.as_uuid().as_bytes());
        hasher.update(&extent_num.to_le_bytes());
        let hash = hasher.finalize();
        // Safety: hash is 32 bytes, slicing first 16 is always valid.
        let bytes: [u8; 16] = hash.as_bytes()[..16]
            .try_into()
            .expect("blake3 hash is always >= 16 bytes");
        ExtentId::new(Uuid::from_bytes(bytes))
    }

    /// Derive a deterministic [`ExtentId`] from volume + block number.
    ///
    /// Backward-compatible convenience: treats each block as its own extent
    /// (extent_blocks=1). For multi-block extents, use `block_to_extent()`
    /// followed by `extent_id_for_extent()`.
    pub fn extent_id_for_block(volume_id: VolumeId, block_num: u64) -> ExtentId {
        Self::extent_id_for_extent(volume_id, block_num)
    }

    /// Convert a block number to (extent_num, block_offset_within_extent).
    ///
    /// `extent_blocks` = extent_size / block_size (number of blocks per extent).
    /// For the default 1:1 mapping, extent_blocks = 1.
    pub fn block_to_extent(block_num: u64, extent_blocks: u64) -> (u64, u64) {
        let extent_blocks = extent_blocks.max(1);
        let extent_num = block_num / extent_blocks;
        let block_offset = block_num % extent_blocks;
        (extent_num, block_offset)
    }

    /// The extent version for deterministic placement is always 1.
    ///
    /// Overwrites write to the same `extent_id` with the same version,
    /// replacing the data in-place on the storage nodes.
    pub fn extent_version() -> u64 {
        1
    }

    /// Select `count` nodes deterministically for a given volume + block.
    ///
    /// Uses rendezvous (highest-random-weight) hashing: hash each
    /// `(volume, block, node)` triple, sort by hash, take first `count`.
    pub fn select_nodes(
        volume_id: VolumeId,
        block_num: u64,
        available_nodes: &[NodeId],
        count: usize,
    ) -> Vec<NodeId> {
        if available_nodes.len() <= count {
            return available_nodes.to_vec();
        }

        let mut scored: Vec<(u64, NodeId)> = available_nodes
            .iter()
            .map(|&node_id| {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"blockyard.placement.v1");
                hasher.update(volume_id.as_uuid().as_bytes());
                hasher.update(&block_num.to_le_bytes());
                hasher.update(node_id.as_uuid().as_bytes());
                let hash = hasher.finalize();
                let score = u64::from_le_bytes(
                    hash.as_bytes()[..8]
                        .try_into()
                        .expect("blake3 hash is always >= 8 bytes"),
                );
                (score, node_id)
            })
            .collect();

        scored.sort_by_key(|(score, _)| *score);
        scored.into_iter().take(count).map(|(_, id)| id).collect()
    }

    /// For EC volumes: select K+M nodes for a stripe.
    ///
    /// EC uses per-block extents too — each block is encoded independently.
    /// `replica_locations[i]` = node holding fragment `i`.
    pub fn select_ec_nodes(
        volume_id: VolumeId,
        block_num: u64,
        available_nodes: &[NodeId],
        data_chunks: u8,
        parity_chunks: u8,
    ) -> Vec<NodeId> {
        let total = (data_chunks + parity_chunks) as usize;
        Self::select_nodes(volume_id, block_num, available_nodes, total)
    }
}

impl std::fmt::Debug for PlacementEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlacementEngine").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extent_id_deterministic() {
        let vid = VolumeId::generate();
        let id1 = PlacementEngine::extent_id_for_block(vid, 42);
        let id2 = PlacementEngine::extent_id_for_block(vid, 42);
        assert_eq!(id1, id2, "same inputs must produce same extent_id");
    }

    #[test]
    fn test_extent_id_different_blocks() {
        let vid = VolumeId::generate();
        let id1 = PlacementEngine::extent_id_for_block(vid, 0);
        let id2 = PlacementEngine::extent_id_for_block(vid, 1);
        assert_ne!(id1, id2, "different blocks must produce different extent_ids");
    }

    #[test]
    fn test_extent_id_different_volumes() {
        let vid1 = VolumeId::generate();
        let vid2 = VolumeId::generate();
        let id1 = PlacementEngine::extent_id_for_block(vid1, 0);
        let id2 = PlacementEngine::extent_id_for_block(vid2, 0);
        assert_ne!(
            id1, id2,
            "different volumes must produce different extent_ids"
        );
    }

    #[test]
    fn test_extent_version_always_one() {
        assert_eq!(PlacementEngine::extent_version(), 1);
    }

    #[test]
    fn test_select_nodes_deterministic() {
        let vid = VolumeId::generate();
        let nodes: Vec<NodeId> = (0..5).map(|_| NodeId::generate()).collect();
        let sel1 = PlacementEngine::select_nodes(vid, 0, &nodes, 3);
        let sel2 = PlacementEngine::select_nodes(vid, 0, &nodes, 3);
        assert_eq!(sel1, sel2, "same inputs must produce same node selection");
    }

    #[test]
    fn test_select_nodes_correct_count() {
        let vid = VolumeId::generate();
        let nodes: Vec<NodeId> = (0..10).map(|_| NodeId::generate()).collect();
        let selected = PlacementEngine::select_nodes(vid, 0, &nodes, 3);
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn test_select_nodes_insufficient_nodes() {
        let vid = VolumeId::generate();
        let nodes: Vec<NodeId> = (0..2).map(|_| NodeId::generate()).collect();
        let selected = PlacementEngine::select_nodes(vid, 0, &nodes, 5);
        assert_eq!(
            selected.len(),
            2,
            "should return all available when fewer than requested"
        );
    }

    #[test]
    fn test_select_nodes_exact_count() {
        let vid = VolumeId::generate();
        let nodes: Vec<NodeId> = (0..3).map(|_| NodeId::generate()).collect();
        let selected = PlacementEngine::select_nodes(vid, 0, &nodes, 3);
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn test_select_nodes_empty() {
        let vid = VolumeId::generate();
        let nodes: Vec<NodeId> = vec![];
        let selected = PlacementEngine::select_nodes(vid, 0, &nodes, 3);
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_nodes_different_blocks_may_differ() {
        let vid = VolumeId::generate();
        let nodes: Vec<NodeId> = (0..10).map(|_| NodeId::generate()).collect();
        let sel0 = PlacementEngine::select_nodes(vid, 0, &nodes, 3);
        let sel1 = PlacementEngine::select_nodes(vid, 1, &nodes, 3);
        // With 10 nodes and 3 selected, different blocks will likely pick different sets.
        // This is probabilistic but virtually guaranteed.
        // We just check they're valid length.
        assert_eq!(sel0.len(), 3);
        assert_eq!(sel1.len(), 3);
    }

    #[test]
    fn test_select_ec_nodes() {
        let vid = VolumeId::generate();
        let nodes: Vec<NodeId> = (0..10).map(|_| NodeId::generate()).collect();
        let selected = PlacementEngine::select_ec_nodes(vid, 0, &nodes, 4, 2);
        assert_eq!(selected.len(), 6);
    }

    #[test]
    fn test_select_ec_nodes_insufficient() {
        let vid = VolumeId::generate();
        let nodes: Vec<NodeId> = (0..3).map(|_| NodeId::generate()).collect();
        let selected = PlacementEngine::select_ec_nodes(vid, 0, &nodes, 4, 2);
        assert_eq!(selected.len(), 3, "should return all available when fewer than K+M");
    }

    #[test]
    fn test_placement_engine_debug() {
        let debug = format!("{:?}", PlacementEngine);
        assert!(debug.contains("PlacementEngine"));
    }

    #[test]
    fn test_block_to_extent_single_block() {
        let (extent_num, offset) = PlacementEngine::block_to_extent(42, 1);
        assert_eq!(extent_num, 42);
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_block_to_extent_multi_block() {
        // 128 blocks per extent (512KB / 4KB)
        let (extent_num, offset) = PlacementEngine::block_to_extent(0, 128);
        assert_eq!(extent_num, 0);
        assert_eq!(offset, 0);

        let (extent_num, offset) = PlacementEngine::block_to_extent(127, 128);
        assert_eq!(extent_num, 0);
        assert_eq!(offset, 127);

        let (extent_num, offset) = PlacementEngine::block_to_extent(128, 128);
        assert_eq!(extent_num, 1);
        assert_eq!(offset, 0);

        let (extent_num, offset) = PlacementEngine::block_to_extent(256, 128);
        assert_eq!(extent_num, 2);
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_block_to_extent_zero_extent_blocks_clamps_to_one() {
        let (extent_num, offset) = PlacementEngine::block_to_extent(5, 0);
        assert_eq!(extent_num, 5);
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_extent_id_for_extent_deterministic() {
        let vid = VolumeId::generate();
        let id1 = PlacementEngine::extent_id_for_extent(vid, 42);
        let id2 = PlacementEngine::extent_id_for_extent(vid, 42);
        assert_eq!(id1, id2, "same inputs must produce same extent_id");
    }

    #[test]
    fn test_extent_id_for_extent_different_extents() {
        let vid = VolumeId::generate();
        let id1 = PlacementEngine::extent_id_for_extent(vid, 0);
        let id2 = PlacementEngine::extent_id_for_extent(vid, 1);
        assert_ne!(id1, id2, "different extents must produce different extent_ids");
    }

    #[test]
    fn test_extent_id_for_block_matches_extent() {
        // extent_id_for_block(v, n) == extent_id_for_extent(v, n)
        let vid = VolumeId::generate();
        let id_block = PlacementEngine::extent_id_for_block(vid, 7);
        let id_extent = PlacementEngine::extent_id_for_extent(vid, 7);
        assert_eq!(id_block, id_extent);
    }

    #[test]
    fn test_select_nodes_all_unique() {
        let vid = VolumeId::generate();
        let nodes: Vec<NodeId> = (0..10).map(|_| NodeId::generate()).collect();
        let selected = PlacementEngine::select_nodes(vid, 0, &nodes, 5);
        let unique: std::collections::HashSet<_> = selected.iter().collect();
        assert_eq!(
            unique.len(),
            selected.len(),
            "selected nodes must be unique"
        );
    }

    #[test]
    fn test_select_nodes_subset_of_available() {
        let vid = VolumeId::generate();
        let nodes: Vec<NodeId> = (0..10).map(|_| NodeId::generate()).collect();
        let selected = PlacementEngine::select_nodes(vid, 0, &nodes, 3);
        for sel in &selected {
            assert!(
                nodes.contains(sel),
                "selected node must be from available set"
            );
        }
    }
}
