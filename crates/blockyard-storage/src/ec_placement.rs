use blockyard_common::types::{NodeId, NodeInfo, NodeState, ZfsHealthState};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::debug;

use crate::placement::PlacementEngine;

/// Describes the placement of all chunks of an erasure-coded extent across
/// the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EcPlacement {
    /// Extent identifier this placement is for.
    pub extent_id: u64,
    /// Ordered list of chunk locations (indices 0..k are data, k..k+m are
    /// parity).
    pub chunks: Vec<ChunkLocation>,
}

/// Location of a single chunk within an erasure-coded extent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkLocation {
    /// Chunk index within the extent (0..k+m).
    pub chunk_index: usize,
    /// Node that stores this chunk.
    pub node_id: NodeId,
    /// Whether this chunk is a parity chunk.
    pub is_parity: bool,
}

/// Place the k+m chunks of an erasure-coded extent across different nodes,
/// respecting failure domain constraints.
///
/// Each chunk is placed on a distinct node.  If a `failure_domain` tag is
/// specified in the placement engine, chunks are spread across different
/// failure domains using round-robin interleaving.
///
/// # Errors
///
/// Returns an error if there are fewer eligible nodes than k+m.
pub fn place_extent_chunks(
    extent_id: u64,
    k: usize,
    m: usize,
    candidates: &[NodeInfo],
    _placement_engine: &PlacementEngine,
) -> blockyard_common::Result<EcPlacement> {
    let total = k + m;

    // Filter to only healthy/online nodes.
    let eligible: Vec<&NodeInfo> = candidates
        .iter()
        .filter(|n| n.state == NodeState::Healthy && n.zfs_health == ZfsHealthState::Online)
        .collect();

    if eligible.len() < total {
        return Err(blockyard_common::Error::Storage(format!(
            "not enough eligible nodes for EC({k}+{m}): need {total}, have {}",
            eligible.len()
        )));
    }

    // Group nodes by failure domain.  If a node doesn't have the domain tag,
    // treat each such node as its own domain to maximise spread.
    let selected = spread_and_select(&eligible, total);

    let mut chunks = Vec::with_capacity(total);
    for (i, &node_id) in selected.iter().enumerate() {
        chunks.push(ChunkLocation {
            chunk_index: i,
            node_id,
            is_parity: i >= k,
        });
    }

    debug!(
        extent_id,
        k,
        m,
        nodes = ?selected,
        "placed EC extent chunks"
    );

    Ok(EcPlacement { extent_id, chunks })
}

/// Spread nodes across failure domains by round-robin interleaving, then
/// select `count` nodes sorted by free capacity (descending).
fn spread_and_select(eligible: &[&NodeInfo], count: usize) -> Vec<NodeId> {
    // Group by a simple domain key: we use the "rack" tag if present,
    // otherwise fall back to a unique-per-node key.
    let mut domain_groups: HashMap<String, Vec<&NodeInfo>> = HashMap::new();
    for node in eligible {
        let domain = node
            .tags
            .get("rack")
            .cloned()
            .unwrap_or_else(|| format!("__node_{}", node.id));
        domain_groups.entry(domain).or_default().push(node);
    }

    // Sort nodes within each domain by free capacity descending.
    for nodes in domain_groups.values_mut() {
        nodes.sort_by(|a, b| {
            let a_free = a.capacity_bytes.saturating_sub(a.used_bytes);
            let b_free = b.capacity_bytes.saturating_sub(b.used_bytes);
            b_free.cmp(&a_free)
        });
    }

    // Round-robin across domains to maximise spread.
    let mut domain_iters: Vec<_> = domain_groups
        .values()
        .map(|nodes| nodes.iter().copied())
        .collect();

    let mut result = Vec::new();
    let mut seen = HashSet::new();
    let mut exhausted = vec![false; domain_iters.len()];

    loop {
        if result.len() >= count {
            break;
        }
        let mut added = false;
        for (i, iter) in domain_iters.iter_mut().enumerate() {
            if result.len() >= count {
                break;
            }
            if exhausted[i] {
                continue;
            }
            if let Some(node) = iter.next() {
                if seen.insert(node.id) {
                    result.push(node.id);
                    added = true;
                }
            } else {
                exhausted[i] = true;
            }
        }
        if !added {
            break;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn gb(n: u64) -> u64 {
        n * 1024 * 1024 * 1024
    }

    fn make_node(id: NodeId, tags: &[(&str, &str)], capacity: u64, used: u64) -> NodeInfo {
        NodeInfo {
            id,
            name: format!("node-{id}"),
            addr: format!("127.0.0.1:{}", 7400 + id).parse().unwrap(),
            data_addr: format!("127.0.0.1:{}", 7500 + id).parse().unwrap(),
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            state: NodeState::Healthy,
            zfs_health: ZfsHealthState::Online,
            capacity_bytes: capacity,
            used_bytes: used,
            incarnation: 1,
        }
    }

    // ── Basic placement ──────────────────────────────────────────────────

    #[test]
    fn test_place_rs_4_2_across_6_nodes() {
        let engine = PlacementEngine::new();
        let candidates: Vec<NodeInfo> = (1..=6)
            .map(|id| make_node(id, &[], gb(100), 0))
            .collect();

        let placement = place_extent_chunks(42, 4, 2, &candidates, &engine).unwrap();
        assert_eq!(placement.extent_id, 42);
        assert_eq!(placement.chunks.len(), 6);

        // All 6 different nodes.
        let node_ids: HashSet<NodeId> = placement.chunks.iter().map(|c| c.node_id).collect();
        assert_eq!(node_ids.len(), 6);

        // Check data vs parity flags.
        for chunk in &placement.chunks {
            if chunk.chunk_index < 4 {
                assert!(!chunk.is_parity, "chunk {} should be data", chunk.chunk_index);
            } else {
                assert!(chunk.is_parity, "chunk {} should be parity", chunk.chunk_index);
            }
        }
    }

    #[test]
    fn test_place_rs_2_1_across_3_nodes() {
        let engine = PlacementEngine::new();
        let candidates: Vec<NodeInfo> = (1..=3)
            .map(|id| make_node(id, &[], gb(100), 0))
            .collect();

        let placement = place_extent_chunks(1, 2, 1, &candidates, &engine).unwrap();
        assert_eq!(placement.chunks.len(), 3);

        let node_ids: HashSet<NodeId> = placement.chunks.iter().map(|c| c.node_id).collect();
        assert_eq!(node_ids.len(), 3);
    }

    // ── Not enough nodes → error ────────────────────────────────────────

    #[test]
    fn test_not_enough_nodes() {
        let engine = PlacementEngine::new();
        let candidates: Vec<NodeInfo> = (1..=4)
            .map(|id| make_node(id, &[], gb(100), 0))
            .collect();

        let result = place_extent_chunks(1, 4, 2, &candidates, &engine);
        assert!(result.is_err());
    }

    #[test]
    fn test_not_enough_healthy_nodes() {
        let engine = PlacementEngine::new();
        let mut candidates: Vec<NodeInfo> = (1..=6)
            .map(|id| make_node(id, &[], gb(100), 0))
            .collect();
        // Mark 2 as failed.
        candidates[4].state = NodeState::Failed;
        candidates[5].state = NodeState::Failed;

        let result = place_extent_chunks(1, 4, 2, &candidates, &engine);
        assert!(result.is_err());
    }

    // ── Failure domain spreading ────────────────────────────────────────

    #[test]
    fn test_failure_domain_spreading() {
        let engine = PlacementEngine::new();
        let candidates = vec![
            make_node(1, &[("rack", "r1")], gb(100), 0),
            make_node(2, &[("rack", "r1")], gb(100), 0),
            make_node(3, &[("rack", "r2")], gb(100), 0),
            make_node(4, &[("rack", "r2")], gb(100), 0),
            make_node(5, &[("rack", "r3")], gb(100), 0),
            make_node(6, &[("rack", "r3")], gb(100), 0),
        ];

        let placement = place_extent_chunks(1, 4, 2, &candidates, &engine).unwrap();
        assert_eq!(placement.chunks.len(), 6);

        // Verify we use nodes from all 3 racks.
        let racks: HashSet<String> = placement
            .chunks
            .iter()
            .map(|c| {
                candidates
                    .iter()
                    .find(|n| n.id == c.node_id)
                    .unwrap()
                    .tags
                    .get("rack")
                    .unwrap()
                    .clone()
            })
            .collect();
        assert_eq!(racks.len(), 3, "should spread across all 3 racks");
    }

    #[test]
    fn test_failure_domain_6_racks_6_nodes() {
        let engine = PlacementEngine::new();
        let candidates: Vec<NodeInfo> = (1..=6)
            .map(|id| make_node(id, &[("rack", &format!("r{id}"))], gb(100), 0))
            .collect();

        let placement = place_extent_chunks(1, 4, 2, &candidates, &engine).unwrap();

        // Each chunk on a different rack.
        let racks: HashSet<String> = placement
            .chunks
            .iter()
            .map(|c| {
                candidates
                    .iter()
                    .find(|n| n.id == c.node_id)
                    .unwrap()
                    .tags
                    .get("rack")
                    .unwrap()
                    .clone()
            })
            .collect();
        assert_eq!(racks.len(), 6, "all 6 racks should be used");
    }

    // ── Excludes faulted ZFS nodes ──────────────────────────────────────

    #[test]
    fn test_excludes_faulted_zfs() {
        let engine = PlacementEngine::new();
        let mut candidates: Vec<NodeInfo> = (1..=7)
            .map(|id| make_node(id, &[], gb(100), 0))
            .collect();
        candidates[0].zfs_health = ZfsHealthState::Faulted;

        let placement = place_extent_chunks(1, 4, 2, &candidates, &engine).unwrap();
        let node_ids: HashSet<NodeId> = placement.chunks.iter().map(|c| c.node_id).collect();
        assert!(!node_ids.contains(&1), "faulted node should be excluded");
    }

    // ── More nodes than needed → selects subset ─────────────────────────

    #[test]
    fn test_more_nodes_than_needed() {
        let engine = PlacementEngine::new();
        let candidates: Vec<NodeInfo> = (1..=10)
            .map(|id| make_node(id, &[], gb(100), 0))
            .collect();

        let placement = place_extent_chunks(1, 2, 1, &candidates, &engine).unwrap();
        assert_eq!(placement.chunks.len(), 3);

        let node_ids: HashSet<NodeId> = placement.chunks.iter().map(|c| c.node_id).collect();
        assert_eq!(node_ids.len(), 3, "should only use 3 nodes for RS(2,1)");
    }

    // ── Chunk index ordering ────────────────────────────────────────────

    #[test]
    fn test_chunk_indices_sequential() {
        let engine = PlacementEngine::new();
        let candidates: Vec<NodeInfo> = (1..=6)
            .map(|id| make_node(id, &[], gb(100), 0))
            .collect();

        let placement = place_extent_chunks(1, 4, 2, &candidates, &engine).unwrap();
        for (i, chunk) in placement.chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_index, i);
        }
    }
}
