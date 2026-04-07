use blockyard_common::types::NodeId;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::ec_placement::EcPlacement;
use crate::erasure::{ErasureCodec, ErasureError};

/// A reconstruction plan describing which chunks are available, which are
/// missing, and whether reconstruction is possible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconstructionPlan {
    /// Extent this plan is for.
    pub extent_id: u64,
    /// Chunks that are still available: (chunk_index, node_id).
    pub available_chunks: Vec<(usize, NodeId)>,
    /// Chunks that are missing due to failed nodes: (chunk_index, node_id).
    pub missing_chunks: Vec<(usize, NodeId)>,
    /// Whether reconstruction is possible (i.e., at least k chunks remain).
    pub can_reconstruct: bool,
}

/// Build a reconstruction plan by comparing the EC placement against a list
/// of failed nodes.
///
/// A chunk is considered "missing" if its assigned node appears in
/// `failed_nodes`.  Reconstruction is possible when the number of available
/// (non-missing) chunks is >= the number of data shards (k).
pub fn plan_reconstruction(placement: &EcPlacement, failed_nodes: &[NodeId]) -> ReconstructionPlan {
    let failed_set: std::collections::HashSet<NodeId> = failed_nodes.iter().copied().collect();

    let mut available = Vec::new();
    let mut missing = Vec::new();

    for chunk in &placement.chunks {
        if failed_set.contains(&chunk.node_id) {
            missing.push((chunk.chunk_index, chunk.node_id));
        } else {
            available.push((chunk.chunk_index, chunk.node_id));
        }
    }

    // Determine k: the number of data shards is the count of chunks with
    // is_parity == false.
    let k = placement.chunks.iter().filter(|c| !c.is_parity).count();
    let can_reconstruct = available.len() >= k;

    debug!(
        extent_id = placement.extent_id,
        available = available.len(),
        missing = missing.len(),
        k,
        can_reconstruct,
        "planned reconstruction"
    );

    ReconstructionPlan {
        extent_id: placement.extent_id,
        available_chunks: available,
        missing_chunks: missing,
        can_reconstruct,
    }
}

/// Reconstruct original data from available chunks using the given codec.
///
/// This is a thin wrapper around `codec.decode()` that provides a
/// convenient entry point for the reconstruction path.
pub fn reconstruct(
    codec: &ErasureCodec,
    chunks: &mut [Option<Vec<u8>>],
) -> Result<Vec<u8>, ErasureError> {
    codec.decode(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ec_placement::ChunkLocation;
    use crate::erasure::ErasureCodec;

    /// Build an EcPlacement for RS(k, m) with chunk i on node i+1.
    fn make_placement(extent_id: u64, k: usize, m: usize) -> EcPlacement {
        let total = k + m;
        let chunks = (0..total)
            .map(|i| ChunkLocation {
                chunk_index: i,
                node_id: (i + 1) as u64,
                is_parity: i >= k,
            })
            .collect();
        EcPlacement { extent_id, chunks }
    }

    // ── No failures ─────────────────────────────────────────────────────

    #[test]
    fn test_plan_no_failures() {
        let placement = make_placement(1, 4, 2);
        let plan = plan_reconstruction(&placement, &[]);

        assert_eq!(plan.extent_id, 1);
        assert_eq!(plan.available_chunks.len(), 6);
        assert!(plan.missing_chunks.is_empty());
        assert!(plan.can_reconstruct);
    }

    // ── 1 data node failure ─────────────────────────────────────────────

    #[test]
    fn test_plan_one_data_node_failure() {
        let placement = make_placement(1, 4, 2);
        // Node 1 stores chunk 0 (data).
        let plan = plan_reconstruction(&placement, &[1]);

        assert_eq!(plan.available_chunks.len(), 5);
        assert_eq!(plan.missing_chunks.len(), 1);
        assert_eq!(plan.missing_chunks[0], (0, 1));
        assert!(plan.can_reconstruct);
    }

    // ── 1 parity node failure ───────────────────────────────────────────

    #[test]
    fn test_plan_one_parity_node_failure() {
        let placement = make_placement(1, 4, 2);
        // Node 5 stores chunk 4 (parity).
        let plan = plan_reconstruction(&placement, &[5]);

        assert_eq!(plan.available_chunks.len(), 5);
        assert_eq!(plan.missing_chunks.len(), 1);
        assert!(plan.can_reconstruct);
    }

    // ── m node failures → can still reconstruct ─────────────────────────

    #[test]
    fn test_plan_m_failures_all_parity() {
        let placement = make_placement(1, 4, 2);
        // Lose both parity nodes (5 and 6).
        let plan = plan_reconstruction(&placement, &[5, 6]);

        assert_eq!(plan.available_chunks.len(), 4);
        assert_eq!(plan.missing_chunks.len(), 2);
        assert!(plan.can_reconstruct);
    }

    #[test]
    fn test_plan_m_failures_mixed() {
        let placement = make_placement(1, 4, 2);
        // Lose 1 data (node 2, chunk 1) + 1 parity (node 5, chunk 4).
        let plan = plan_reconstruction(&placement, &[2, 5]);

        assert_eq!(plan.available_chunks.len(), 4);
        assert_eq!(plan.missing_chunks.len(), 2);
        assert!(plan.can_reconstruct);
    }

    #[test]
    fn test_plan_m_failures_all_data() {
        let placement = make_placement(1, 4, 2);
        // Lose 2 data nodes (node 1, node 2).
        let plan = plan_reconstruction(&placement, &[1, 2]);

        assert_eq!(plan.available_chunks.len(), 4);
        assert_eq!(plan.missing_chunks.len(), 2);
        assert!(plan.can_reconstruct);
    }

    // ── m+1 node failures → cannot reconstruct ──────────────────────────

    #[test]
    fn test_plan_m_plus_1_failures() {
        let placement = make_placement(1, 4, 2);
        // Lose 3 nodes.
        let plan = plan_reconstruction(&placement, &[1, 2, 5]);

        assert_eq!(plan.available_chunks.len(), 3);
        assert_eq!(plan.missing_chunks.len(), 3);
        assert!(
            !plan.can_reconstruct,
            "should not be able to reconstruct with only 3 of 4 needed"
        );
    }

    #[test]
    fn test_plan_all_nodes_failed() {
        let placement = make_placement(1, 4, 2);
        let plan = plan_reconstruction(&placement, &[1, 2, 3, 4, 5, 6]);

        assert!(plan.available_chunks.is_empty());
        assert_eq!(plan.missing_chunks.len(), 6);
        assert!(!plan.can_reconstruct);
    }

    // ── Reconstruction wrapper ──────────────────────────────────────────

    #[test]
    fn test_reconstruct_wrapper_success() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data = b"reconstruct wrapper test data!!";
        let encoded = codec.encode(data).unwrap();

        let mut chunks: Vec<Option<Vec<u8>>> = encoded.into_iter().map(Some).collect();
        // Drop 1 data + 1 parity.
        chunks[0] = None;
        chunks[5] = None;

        let result = reconstruct(&codec, &mut chunks).unwrap();
        assert_eq!(&result[..data.len()], data.as_slice());
    }

    #[test]
    fn test_reconstruct_wrapper_insufficient() {
        let codec = ErasureCodec::new(4, 2).unwrap();
        let data = b"this will fail to reconstruct";
        let encoded = codec.encode(data).unwrap();

        let mut chunks: Vec<Option<Vec<u8>>> = encoded.into_iter().map(Some).collect();
        // Drop 3 chunks (> m=2).
        chunks[0] = None;
        chunks[1] = None;
        chunks[4] = None;

        let result = reconstruct(&codec, &mut chunks);
        assert!(result.is_err());
    }

    // ── RS(2,1) plans ───────────────────────────────────────────────────

    #[test]
    fn test_plan_rs_2_1_no_failures() {
        let placement = make_placement(1, 2, 1);
        let plan = plan_reconstruction(&placement, &[]);
        assert!(plan.can_reconstruct);
        assert_eq!(plan.available_chunks.len(), 3);
    }

    #[test]
    fn test_plan_rs_2_1_one_failure() {
        let placement = make_placement(1, 2, 1);
        let plan = plan_reconstruction(&placement, &[1]);
        assert!(plan.can_reconstruct);
        assert_eq!(plan.available_chunks.len(), 2);
    }

    #[test]
    fn test_plan_rs_2_1_two_failures() {
        let placement = make_placement(1, 2, 1);
        let plan = plan_reconstruction(&placement, &[1, 3]);
        // Only 1 chunk left, need 2 (k=2).
        assert!(!plan.can_reconstruct);
    }

    // ── RS(6,3) plans ───────────────────────────────────────────────────

    #[test]
    fn test_plan_rs_6_3_three_failures() {
        let placement = make_placement(1, 6, 3);
        let plan = plan_reconstruction(&placement, &[1, 5, 9]);
        assert_eq!(plan.available_chunks.len(), 6);
        assert!(plan.can_reconstruct);
    }

    #[test]
    fn test_plan_rs_6_3_four_failures() {
        let placement = make_placement(1, 6, 3);
        let plan = plan_reconstruction(&placement, &[1, 2, 7, 8]);
        assert_eq!(plan.available_chunks.len(), 5);
        // Need 6, only have 5.
        assert!(!plan.can_reconstruct);
    }

    // ── Failed nodes not in placement are ignored ───────────────────────

    #[test]
    fn test_plan_irrelevant_failed_node() {
        let placement = make_placement(1, 4, 2);
        // Node 99 doesn't host any chunks.
        let plan = plan_reconstruction(&placement, &[99]);
        assert_eq!(plan.available_chunks.len(), 6);
        assert!(plan.missing_chunks.is_empty());
        assert!(plan.can_reconstruct);
    }
}
