//! Volume placement engine — maps extents to physical disks across nodes.
//!
//! The placement engine respects protection policies, spreading replicas
//! and erasure coding fragments across distinct nodes, preferring nodes
//! with more free space.

use std::collections::HashMap;

use blockyard_common::{DiskId, DiskState, ExtentId, NodeId, ProtectionPolicy};
use tracing::debug;

/// Metadata for a cluster disk, matching the Raft state machine's `ClusterDisk`.
///
/// We use a local struct to avoid a dependency on `blockyard-raft` from
/// `blockyard-storage`. The caller maps from Raft types before invoking.
#[derive(Debug, Clone)]
pub struct PlacementDisk {
    pub disk_id: DiskId,
    pub node_id: NodeId,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub state: DiskState,
}

impl PlacementDisk {
    /// Free bytes on this disk.
    pub fn free_bytes(&self) -> u64 {
        self.capacity_bytes.saturating_sub(self.used_bytes)
    }
}

/// Minimal node info for placement decisions.
#[derive(Debug, Clone)]
pub struct PlacementNode {
    pub node_id: NodeId,
}

/// Result of placing a single extent.
#[derive(Debug, Clone)]
pub struct ExtentPlacement {
    pub extent_id: ExtentId,
    /// Primary disk for this extent.
    pub primary_disk: DiskId,
    /// Replica disks (for Replicated policy; excludes primary).
    pub replica_disks: Vec<DiskId>,
    /// Fragment disks (for ErasureCoded policy; data+parity).
    pub fragment_disks: Vec<DiskId>,
}

/// Errors that can occur during placement.
#[derive(Debug, thiserror::Error)]
pub enum PlacementError {
    #[error("insufficient nodes: need {required}, have {available}")]
    InsufficientNodes { required: usize, available: usize },

    #[error("insufficient disks: need {required}, have {available}")]
    InsufficientDisks { required: usize, available: usize },

    #[error("no healthy disks available")]
    NoHealthyDisks,

    #[error("invalid protection policy: {0}")]
    InvalidPolicy(String),
}

/// Volume metadata subset needed for placement.
#[derive(Debug, Clone)]
pub struct PlacementVolumeInfo {
    pub size_bytes: u64,
    pub protection: ProtectionPolicy,
    pub extent_size_bytes: u64,
}

impl Default for PlacementVolumeInfo {
    fn default() -> Self {
        Self {
            size_bytes: 0,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        }
    }
}

/// Default extent size: 64 MiB.
pub const DEFAULT_EXTENT_SIZE: u64 = 64 * 1024 * 1024;

/// The placement engine: generates extent-to-disk assignments for volumes.
#[derive(Debug)]
pub struct PlacementEngine {
    /// Extent size in bytes.
    pub extent_size_bytes: u64,
}

impl Default for PlacementEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl PlacementEngine {
    /// Create a new placement engine with default settings.
    pub fn new() -> Self {
        Self {
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        }
    }

    /// Create a new placement engine with a custom extent size.
    pub fn with_extent_size(extent_size_bytes: u64) -> Self {
        Self { extent_size_bytes }
    }

    /// Place a volume's extents across available disks.
    ///
    /// Placement rules:
    /// - Spread replicas across different NODES (never two replicas on same node)
    /// - Prefer nodes with more free space
    /// - For EC: data+parity fragments on different nodes
    /// - Handle insufficient nodes (error)
    pub fn place_volume(
        &self,
        volume: &PlacementVolumeInfo,
        disks: &[PlacementDisk],
        nodes: &[PlacementNode],
    ) -> Result<Vec<ExtentPlacement>, PlacementError> {
        if let Err(e) = volume.protection.validate() {
            return Err(PlacementError::InvalidPolicy(e.to_string()));
        }

        // Filter to healthy/suspect disks that allow allocation.
        let allocatable_disks: Vec<&PlacementDisk> = disks
            .iter()
            .filter(|d| d.state.allows_allocation())
            .collect();

        if allocatable_disks.is_empty() {
            return Err(PlacementError::NoHealthyDisks);
        }

        // Build node set from the provided nodes list.
        let node_set: HashMap<NodeId, bool> = nodes.iter().map(|n| (n.node_id, true)).collect();

        // Build a map of node_id -> disks (sorted by free space descending).
        let mut node_disks: HashMap<NodeId, Vec<&PlacementDisk>> = HashMap::new();
        for disk in &allocatable_disks {
            if node_set.contains_key(&disk.node_id) {
                node_disks.entry(disk.node_id).or_default().push(disk);
            }
        }

        // Sort each node's disks by free space (descending).
        for disks_vec in node_disks.values_mut() {
            disks_vec.sort_by_key(|d| std::cmp::Reverse(d.free_bytes()));
        }

        // Sort nodes by total free space (descending) for preferring nodes with more free space.
        let mut sorted_nodes: Vec<(NodeId, u64)> = node_disks
            .iter()
            .map(|(nid, disks_vec)| {
                let total_free: u64 = disks_vec.iter().map(|d| d.free_bytes()).sum();
                (*nid, total_free)
            })
            .collect();
        sorted_nodes.sort_by(|a, b| b.1.cmp(&a.1));

        let available_nodes = sorted_nodes.len();

        // Calculate how many extents we need.
        let extent_size = volume.extent_size_bytes;
        let num_extents = if volume.size_bytes == 0 {
            0
        } else {
            volume.size_bytes.div_ceil(extent_size)
        };

        match volume.protection {
            ProtectionPolicy::Replicated { replicas } => {
                let required_nodes = replicas as usize;
                if available_nodes < required_nodes {
                    return Err(PlacementError::InsufficientNodes {
                        required: required_nodes,
                        available: available_nodes,
                    });
                }

                let mut placements = Vec::with_capacity(num_extents as usize);

                for i in 0..num_extents {
                    let extent_id = ExtentId::generate();

                    // Round-robin starting node offset to spread load across extents.
                    let mut selected_disks = Vec::with_capacity(required_nodes);
                    let start_offset = i as usize % available_nodes;

                    for j in 0..required_nodes {
                        let node_idx = (start_offset + j) % available_nodes;
                        let (node_id, _) = sorted_nodes[node_idx];
                        let node_disk_list = &node_disks[&node_id];
                        // Pick the disk with most free space on this node.
                        // Use round-robin within node for multi-extent volumes.
                        let disk_idx = (i as usize / available_nodes) % node_disk_list.len();
                        selected_disks.push(node_disk_list[disk_idx].disk_id);
                    }

                    let primary = selected_disks[0];
                    let replicas_vec = selected_disks[1..].to_vec();

                    debug!(
                        extent_id = %extent_id,
                        primary = %primary,
                        replica_count = replicas_vec.len(),
                        "placed extent"
                    );

                    placements.push(ExtentPlacement {
                        extent_id,
                        primary_disk: primary,
                        replica_disks: replicas_vec,
                        fragment_disks: vec![],
                    });
                }

                Ok(placements)
            }

            ProtectionPolicy::ErasureCoded {
                data_chunks,
                parity_chunks,
            } => {
                let total_fragments = (data_chunks + parity_chunks) as usize;
                if available_nodes < total_fragments {
                    return Err(PlacementError::InsufficientNodes {
                        required: total_fragments,
                        available: available_nodes,
                    });
                }

                let mut placements = Vec::with_capacity(num_extents as usize);

                for i in 0..num_extents {
                    let extent_id = ExtentId::generate();
                    let start_offset = i as usize % available_nodes;

                    let mut fragment_disks = Vec::with_capacity(total_fragments);
                    for j in 0..total_fragments {
                        let node_idx = (start_offset + j) % available_nodes;
                        let (node_id, _) = sorted_nodes[node_idx];
                        let node_disk_list = &node_disks[&node_id];
                        let disk_idx = (i as usize / available_nodes) % node_disk_list.len();
                        fragment_disks.push(node_disk_list[disk_idx].disk_id);
                    }

                    let primary = fragment_disks[0];

                    debug!(
                        extent_id = %extent_id,
                        primary = %primary,
                        fragment_count = fragment_disks.len(),
                        "placed EC extent"
                    );

                    placements.push(ExtentPlacement {
                        extent_id,
                        primary_disk: primary,
                        replica_disks: vec![],
                        fragment_disks,
                    });
                }

                Ok(placements)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_nodes(count: usize) -> Vec<PlacementNode> {
        (0..count)
            .map(|_| PlacementNode {
                node_id: NodeId::generate(),
            })
            .collect()
    }

    fn make_disks(
        nodes: &[PlacementNode],
        disks_per_node: usize,
        capacity: u64,
    ) -> Vec<PlacementDisk> {
        let mut disks = Vec::new();
        for node in nodes {
            for _ in 0..disks_per_node {
                disks.push(PlacementDisk {
                    disk_id: DiskId::generate(),
                    node_id: node.node_id,
                    capacity_bytes: capacity,
                    used_bytes: 0,
                    state: DiskState::Healthy,
                });
            }
        }
        disks
    }

    fn make_disks_varied_usage(
        nodes: &[PlacementNode],
        disks_per_node: usize,
        capacity: u64,
        usage_fraction: &[f64],
    ) -> Vec<PlacementDisk> {
        let mut disks = Vec::new();
        for (i, node) in nodes.iter().enumerate() {
            let usage = if i < usage_fraction.len() {
                (capacity as f64 * usage_fraction[i]) as u64
            } else {
                0
            };
            for _ in 0..disks_per_node {
                disks.push(PlacementDisk {
                    disk_id: DiskId::generate(),
                    node_id: node.node_id,
                    capacity_bytes: capacity,
                    used_bytes: usage,
                    state: DiskState::Healthy,
                });
            }
        }
        disks
    }

    #[test]
    fn test_place_volume_replicated_3_nodes() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(3);
        let disks = make_disks(&nodes, 2, 1_000_000_000);

        let volume = PlacementVolumeInfo {
            size_bytes: 128 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let placements = engine.place_volume(&volume, &disks, &nodes).unwrap();
        assert_eq!(placements.len(), 2); // 128 MiB / 64 MiB = 2 extents

        for placement in &placements {
            // Each extent should have primary + 2 replicas = 3 total
            assert_eq!(placement.replica_disks.len(), 2);
            assert!(placement.fragment_disks.is_empty());

            // Verify all disks are on different nodes
            let mut disk_node_map: HashMap<DiskId, NodeId> = HashMap::new();
            for disk in &disks {
                disk_node_map.insert(disk.disk_id, disk.node_id);
            }

            let mut used_nodes = vec![disk_node_map[&placement.primary_disk]];
            for rd in &placement.replica_disks {
                let node = disk_node_map[rd];
                assert!(
                    !used_nodes.contains(&node),
                    "replica placed on same node as another replica"
                );
                used_nodes.push(node);
            }
        }
    }

    #[test]
    fn test_place_volume_replicated_single_replica() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(1);
        let disks = make_disks(&nodes, 1, 1_000_000_000);

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let placements = engine.place_volume(&volume, &disks, &nodes).unwrap();
        assert_eq!(placements.len(), 1);
        assert!(placements[0].replica_disks.is_empty());
    }

    #[test]
    fn test_place_volume_insufficient_nodes_replicated() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(2);
        let disks = make_disks(&nodes, 2, 1_000_000_000);

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let result = engine.place_volume(&volume, &disks, &nodes);
        assert!(result.is_err());
        match result.unwrap_err() {
            PlacementError::InsufficientNodes {
                required,
                available,
            } => {
                assert_eq!(required, 3);
                assert_eq!(available, 2);
            }
            other => panic!("expected InsufficientNodes, got {:?}", other),
        }
    }

    #[test]
    fn test_place_volume_erasure_coded() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(6);
        let disks = make_disks(&nodes, 1, 1_000_000_000);

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::ErasureCoded {
                data_chunks: 4,
                parity_chunks: 2,
            },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let placements = engine.place_volume(&volume, &disks, &nodes).unwrap();
        assert_eq!(placements.len(), 1);
        assert_eq!(placements[0].fragment_disks.len(), 6);
        assert!(placements[0].replica_disks.is_empty());

        // Verify all fragments on different nodes
        let mut disk_node_map: HashMap<DiskId, NodeId> = HashMap::new();
        for disk in &disks {
            disk_node_map.insert(disk.disk_id, disk.node_id);
        }
        let mut used_nodes = std::collections::HashSet::new();
        for fd in &placements[0].fragment_disks {
            let node = disk_node_map[fd];
            assert!(
                used_nodes.insert(node),
                "EC fragment placed on same node as another fragment"
            );
        }
    }

    #[test]
    fn test_place_volume_insufficient_nodes_ec() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(3);
        let disks = make_disks(&nodes, 2, 1_000_000_000);

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::ErasureCoded {
                data_chunks: 4,
                parity_chunks: 2,
            },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let result = engine.place_volume(&volume, &disks, &nodes);
        assert!(result.is_err());
        match result.unwrap_err() {
            PlacementError::InsufficientNodes {
                required,
                available,
            } => {
                assert_eq!(required, 6);
                assert_eq!(available, 3);
            }
            other => panic!("expected InsufficientNodes, got {:?}", other),
        }
    }

    #[test]
    fn test_place_volume_no_healthy_disks() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(3);
        let mut disks = make_disks(&nodes, 1, 1_000_000_000);
        // Mark all disks as failed
        for d in &mut disks {
            d.state = DiskState::Failed;
        }

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let result = engine.place_volume(&volume, &disks, &nodes);
        assert!(result.is_err());
        match result.unwrap_err() {
            PlacementError::NoHealthyDisks => {}
            other => panic!("expected NoHealthyDisks, got {:?}", other),
        }
    }

    #[test]
    fn test_place_volume_zero_size() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(3);
        let disks = make_disks(&nodes, 1, 1_000_000_000);

        let volume = PlacementVolumeInfo {
            size_bytes: 0,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let placements = engine.place_volume(&volume, &disks, &nodes).unwrap();
        assert!(placements.is_empty());
    }

    #[test]
    fn test_place_volume_invalid_policy() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(3);
        let disks = make_disks(&nodes, 1, 1_000_000_000);

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 0 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let result = engine.place_volume(&volume, &disks, &nodes);
        assert!(result.is_err());
        match result.unwrap_err() {
            PlacementError::InvalidPolicy(_) => {}
            other => panic!("expected InvalidPolicy, got {:?}", other),
        }
    }

    #[test]
    fn test_place_volume_prefers_nodes_with_more_free_space() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(3);
        // Node 0: 90% used, Node 1: 50% used, Node 2: 10% used
        let disks = make_disks_varied_usage(&nodes, 1, 1_000_000_000, &[0.9, 0.5, 0.1]);

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let placements = engine.place_volume(&volume, &disks, &nodes).unwrap();
        assert_eq!(placements.len(), 1);

        // The primary should be on the node with most free space (node 2)
        let disk_node_map: HashMap<DiskId, NodeId> =
            disks.iter().map(|d| (d.disk_id, d.node_id)).collect();
        let primary_node = disk_node_map[&placements[0].primary_disk];
        assert_eq!(
            primary_node, nodes[2].node_id,
            "primary should be on node with most free space"
        );
    }

    #[test]
    fn test_place_volume_multiple_extents() {
        let engine = PlacementEngine::with_extent_size(32 * 1024 * 1024);
        let nodes = make_nodes(3);
        let disks = make_disks(&nodes, 2, 1_000_000_000);

        let volume = PlacementVolumeInfo {
            size_bytes: 128 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 2 },
            extent_size_bytes: 32 * 1024 * 1024,
        };

        let placements = engine.place_volume(&volume, &disks, &nodes).unwrap();
        assert_eq!(placements.len(), 4); // 128 MiB / 32 MiB = 4 extents

        for placement in &placements {
            assert_eq!(placement.replica_disks.len(), 1);
        }
    }

    #[test]
    fn test_place_volume_excludes_failed_disks() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(3);
        let mut disks = make_disks(&nodes, 1, 1_000_000_000);
        // Mark first node's disk as failed
        disks[0].state = DiskState::Failed;

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 2 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let placements = engine.place_volume(&volume, &disks, &nodes).unwrap();
        assert_eq!(placements.len(), 1);

        // Verify the failed disk is not used
        let failed_disk = disks[0].disk_id;
        assert_ne!(placements[0].primary_disk, failed_disk);
        for rd in &placements[0].replica_disks {
            assert_ne!(*rd, failed_disk);
        }
    }

    #[test]
    fn test_place_volume_excludes_draining_disks() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(3);
        let mut disks = make_disks(&nodes, 1, 1_000_000_000);
        disks[0].state = DiskState::Draining;

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 2 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let placements = engine.place_volume(&volume, &disks, &nodes).unwrap();
        let draining_disk = disks[0].disk_id;
        assert_ne!(placements[0].primary_disk, draining_disk);
    }

    #[test]
    fn test_place_volume_suspect_disks_usable() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(1);
        let mut disks = make_disks(&nodes, 1, 1_000_000_000);
        disks[0].state = DiskState::Suspect;

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let placements = engine.place_volume(&volume, &disks, &nodes).unwrap();
        assert_eq!(placements.len(), 1);
        assert_eq!(placements[0].primary_disk, disks[0].disk_id);
    }

    #[test]
    fn test_placement_engine_default() {
        let engine = PlacementEngine::default();
        assert_eq!(engine.extent_size_bytes, DEFAULT_EXTENT_SIZE);
    }

    #[test]
    fn test_placement_engine_custom_extent_size() {
        let engine = PlacementEngine::with_extent_size(1024 * 1024);
        assert_eq!(engine.extent_size_bytes, 1024 * 1024);
    }

    #[test]
    fn test_placement_disk_free_bytes() {
        let disk = PlacementDisk {
            disk_id: DiskId::generate(),
            node_id: NodeId::generate(),
            capacity_bytes: 1000,
            used_bytes: 300,
            state: DiskState::Healthy,
        };
        assert_eq!(disk.free_bytes(), 700);
    }

    #[test]
    fn test_placement_disk_free_bytes_overflow() {
        let disk = PlacementDisk {
            disk_id: DiskId::generate(),
            node_id: NodeId::generate(),
            capacity_bytes: 100,
            used_bytes: 200,
            state: DiskState::Healthy,
        };
        assert_eq!(disk.free_bytes(), 0);
    }

    #[test]
    fn test_placement_error_display_insufficient_nodes() {
        let err = PlacementError::InsufficientNodes {
            required: 3,
            available: 2,
        };
        assert!(err.to_string().contains("insufficient nodes"));
        assert!(err.to_string().contains("3"));
        assert!(err.to_string().contains("2"));
    }

    #[test]
    fn test_placement_error_display_insufficient_disks() {
        let err = PlacementError::InsufficientDisks {
            required: 5,
            available: 3,
        };
        assert!(err.to_string().contains("insufficient disks"));
    }

    #[test]
    fn test_placement_error_display_no_healthy_disks() {
        let err = PlacementError::NoHealthyDisks;
        assert!(err.to_string().contains("no healthy disks"));
    }

    #[test]
    fn test_placement_error_display_invalid_policy() {
        let err = PlacementError::InvalidPolicy("bad".into());
        assert!(err.to_string().contains("invalid protection policy"));
    }

    #[test]
    fn test_extent_placement_debug() {
        let p = ExtentPlacement {
            extent_id: ExtentId::generate(),
            primary_disk: DiskId::generate(),
            replica_disks: vec![],
            fragment_disks: vec![],
        };
        let debug = format!("{:?}", p);
        assert!(debug.contains("ExtentPlacement"));
    }

    #[test]
    fn test_placement_volume_info_default() {
        let info = PlacementVolumeInfo::default();
        assert_eq!(info.size_bytes, 0);
        assert_eq!(info.extent_size_bytes, DEFAULT_EXTENT_SIZE);
    }

    #[test]
    fn test_place_volume_ec_insufficient_nodes_exact() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(5);
        let disks = make_disks(&nodes, 1, 1_000_000_000);

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::ErasureCoded {
                data_chunks: 4,
                parity_chunks: 2,
            },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let result = engine.place_volume(&volume, &disks, &nodes);
        assert!(result.is_err());
    }

    #[test]
    fn test_place_volume_ec_exactly_enough_nodes() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(6);
        let disks = make_disks(&nodes, 1, 1_000_000_000);

        let volume = PlacementVolumeInfo {
            size_bytes: 128 * 1024 * 1024,
            protection: ProtectionPolicy::ErasureCoded {
                data_chunks: 4,
                parity_chunks: 2,
            },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let placements = engine.place_volume(&volume, &disks, &nodes).unwrap();
        assert_eq!(placements.len(), 2);
    }

    #[test]
    fn test_place_volume_replicas_spread_across_nodes() {
        let engine = PlacementEngine::new();
        let nodes = make_nodes(5);
        let disks = make_disks(&nodes, 3, 1_000_000_000);

        let volume = PlacementVolumeInfo {
            size_bytes: 64 * 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
            extent_size_bytes: DEFAULT_EXTENT_SIZE,
        };

        let placements = engine.place_volume(&volume, &disks, &nodes).unwrap();
        let disk_node_map: HashMap<DiskId, NodeId> =
            disks.iter().map(|d| (d.disk_id, d.node_id)).collect();

        for placement in &placements {
            let mut used_nodes = std::collections::HashSet::new();
            used_nodes.insert(disk_node_map[&placement.primary_disk]);
            for rd in &placement.replica_disks {
                let node = disk_node_map[rd];
                assert!(used_nodes.insert(node), "two replicas on same node");
            }
        }
    }

    #[test]
    fn test_placement_error_debug() {
        let err = PlacementError::NoHealthyDisks;
        let debug = format!("{:?}", err);
        assert!(debug.contains("NoHealthyDisks"));
    }

    #[test]
    fn test_placement_node_debug() {
        let node = PlacementNode {
            node_id: NodeId::generate(),
        };
        let debug = format!("{:?}", node);
        assert!(debug.contains("PlacementNode"));
    }

    #[test]
    fn test_placement_disk_debug() {
        let disk = PlacementDisk {
            disk_id: DiskId::generate(),
            node_id: NodeId::generate(),
            capacity_bytes: 1000,
            used_bytes: 0,
            state: DiskState::Healthy,
        };
        let debug = format!("{:?}", disk);
        assert!(debug.contains("PlacementDisk"));
    }
}
