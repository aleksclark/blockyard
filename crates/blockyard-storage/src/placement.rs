use blockyard_common::types::{NodeId, NodeInfo, NodeState, VolumeSpec, ZfsHealthState};
use std::collections::HashMap;
use tracing::debug;

pub struct PlacementEngine;

impl PlacementEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn place_volume(
        &self,
        spec: &VolumeSpec,
        candidates: &[NodeInfo],
    ) -> blockyard_common::Result<Vec<NodeId>> {
        let eligible = self.filter_eligible(spec, candidates);

        if eligible.len() < spec.replicas as usize {
            return Err(blockyard_common::Error::Storage(format!(
                "not enough eligible nodes: need {}, have {}",
                spec.replicas,
                eligible.len()
            )));
        }

        let spread = self.spread_by_failure_domain(spec, &eligible);

        let selected = self.balance_by_capacity(spread, spec.replicas as usize);

        debug!(
            volume = %spec.name,
            selected = ?selected,
            "placed volume"
        );

        Ok(selected)
    }

    fn filter_eligible<'a>(
        &self,
        spec: &VolumeSpec,
        candidates: &'a [NodeInfo],
    ) -> Vec<&'a NodeInfo> {
        candidates
            .iter()
            .filter(|n| {
                n.state == NodeState::Healthy
                    && n.zfs_health == ZfsHealthState::Online
                    && self.matches_affinity(spec, n)
                    && self.matches_anti_affinity(spec, n)
            })
            .collect()
    }

    fn matches_affinity(&self, spec: &VolumeSpec, node: &NodeInfo) -> bool {
        spec.affinity
            .iter()
            .all(|(k, v)| node.tags.get(k).is_some_and(|tv| tv == v))
    }

    fn matches_anti_affinity(&self, spec: &VolumeSpec, node: &NodeInfo) -> bool {
        spec.anti_affinity
            .iter()
            .all(|(k, v)| node.tags.get(k).is_none_or(|tv| tv != v))
    }

    fn spread_by_failure_domain<'a>(
        &self,
        spec: &VolumeSpec,
        eligible: &[&'a NodeInfo],
    ) -> Vec<&'a NodeInfo> {
        if spec.failure_domain == "node" || spec.failure_domain.is_empty() {
            return eligible.to_vec();
        }

        let mut domain_groups: HashMap<String, Vec<&'a NodeInfo>> = HashMap::new();
        for node in eligible {
            let domain = node
                .tags
                .get(&spec.failure_domain)
                .cloned()
                .unwrap_or_else(|| format!("__untagged_{}", node.id));
            domain_groups.entry(domain).or_default().push(node);
        }

        let mut result = Vec::new();
        let mut domain_iters: Vec<_> = domain_groups
            .values()
            .map(|nodes| nodes.iter().copied())
            .collect();

        let mut exhausted = vec![false; domain_iters.len()];
        loop {
            let mut added = false;
            for (i, iter) in domain_iters.iter_mut().enumerate() {
                if exhausted[i] {
                    continue;
                }
                if let Some(node) = iter.next() {
                    result.push(node);
                    added = true;
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

    fn balance_by_capacity(&self, mut candidates: Vec<&NodeInfo>, count: usize) -> Vec<NodeId> {
        candidates.sort_by(|a, b| {
            let a_free = Self::total_free_capacity(a);
            let b_free = Self::total_free_capacity(b);
            b_free.cmp(&a_free)
        });

        candidates.iter().take(count).map(|n| n.id).collect()
    }

    /// Compute total free capacity for a node.  If the node reports
    /// per-pool information, the total is the sum across all pools.
    /// Otherwise we fall back to the top-level capacity fields.
    fn total_free_capacity(node: &NodeInfo) -> u64 {
        if node.pools.is_empty() {
            node.capacity_bytes.saturating_sub(node.used_bytes)
        } else {
            node.pools
                .iter()
                .map(|p| p.capacity_bytes.saturating_sub(p.used_bytes))
                .sum()
        }
    }

    pub fn should_exclude_node(&self, node: &NodeInfo) -> bool {
        node.state != NodeState::Healthy || node.zfs_health == ZfsHealthState::Faulted
    }

    pub fn needs_re_replication(&self, node: &NodeInfo) -> bool {
        node.zfs_health == ZfsHealthState::Faulted || node.state == NodeState::Failed
    }
}

impl Default for PlacementEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockyard_common::types::{ReadPolicy, WriteConsistency};
    use std::collections::HashSet;
    use uuid::Uuid;

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
            pools: Vec::new(),
        }
    }

    fn make_spec(replicas: u32) -> VolumeSpec {
        VolumeSpec {
            id: Uuid::new_v4(),
            name: "test-vol".to_string(),
            size_bytes: 100 * 1024 * 1024 * 1024,
            replicas,
            consistency: WriteConsistency::Majority,
            read_policy: ReadPolicy::Any,
            affinity: HashMap::new(),
            anti_affinity: HashMap::new(),
            failure_domain: "node".to_string(),
        }
    }

    fn gb(n: u64) -> u64 {
        n * 1024 * 1024 * 1024
    }

    #[test]
    fn test_place_volume_basic() {
        let engine = PlacementEngine::new();
        let candidates = vec![
            make_node(1, &[], gb(100), gb(10)),
            make_node(2, &[], gb(100), gb(20)),
            make_node(3, &[], gb(100), gb(30)),
        ];
        let spec = make_spec(3);
        let result = engine.place_volume(&spec, &candidates).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_place_volume_not_enough_nodes() {
        let engine = PlacementEngine::new();
        let candidates = vec![make_node(1, &[], gb(100), 0)];
        let spec = make_spec(3);
        let result = engine.place_volume(&spec, &candidates);
        assert!(result.is_err());
    }

    #[test]
    fn test_place_volume_prefers_more_free_space() {
        let engine = PlacementEngine::new();
        let candidates = vec![
            make_node(1, &[], gb(100), gb(90)),
            make_node(2, &[], gb(100), gb(10)),
            make_node(3, &[], gb(100), gb(50)),
        ];
        let spec = make_spec(2);
        let result = engine.place_volume(&spec, &candidates).unwrap();
        assert_eq!(result[0], 2);
        assert_eq!(result[1], 3);
    }

    #[test]
    fn test_place_volume_with_affinity() {
        let engine = PlacementEngine::new();
        let candidates = vec![
            make_node(1, &[("storage_class", "ssd")], gb(100), 0),
            make_node(2, &[("storage_class", "hdd")], gb(100), 0),
            make_node(3, &[("storage_class", "ssd")], gb(100), 0),
            make_node(4, &[("storage_class", "ssd")], gb(100), 0),
        ];
        let mut spec = make_spec(3);
        spec.affinity.insert("storage_class".into(), "ssd".into());

        let result = engine.place_volume(&spec, &candidates).unwrap();
        assert_eq!(result.len(), 3);
        assert!(!result.contains(&2));
    }

    #[test]
    fn test_place_volume_with_anti_affinity() {
        let engine = PlacementEngine::new();
        let candidates = vec![
            make_node(1, &[("storage_class", "ssd")], gb(100), 0),
            make_node(2, &[("storage_class", "hdd")], gb(100), 0),
            make_node(3, &[("storage_class", "ssd")], gb(100), 0),
        ];
        let mut spec = make_spec(2);
        spec.anti_affinity
            .insert("storage_class".into(), "ssd".into());

        let result = engine.place_volume(&spec, &candidates);
        assert!(result.is_err());
    }

    #[test]
    fn test_place_volume_excludes_failed_nodes() {
        let engine = PlacementEngine::new();
        let mut node3 = make_node(3, &[], gb(100), 0);
        node3.state = NodeState::Failed;
        let candidates = vec![
            make_node(1, &[], gb(100), 0),
            make_node(2, &[], gb(100), 0),
            node3,
        ];
        let spec = make_spec(3);
        let result = engine.place_volume(&spec, &candidates);
        assert!(result.is_err());
    }

    #[test]
    fn test_place_volume_excludes_faulted_zfs() {
        let engine = PlacementEngine::new();
        let mut node3 = make_node(3, &[], gb(100), 0);
        node3.zfs_health = ZfsHealthState::Faulted;
        let candidates = vec![
            make_node(1, &[], gb(100), 0),
            make_node(2, &[], gb(100), 0),
            node3,
        ];
        let spec = make_spec(3);
        let result = engine.place_volume(&spec, &candidates);
        assert!(result.is_err());
    }

    #[test]
    fn test_place_volume_failure_domain_rack() {
        let engine = PlacementEngine::new();
        let candidates = vec![
            make_node(1, &[("rack", "r1")], gb(100), 0),
            make_node(2, &[("rack", "r1")], gb(100), 0),
            make_node(3, &[("rack", "r2")], gb(100), 0),
            make_node(4, &[("rack", "r3")], gb(100), 0),
        ];
        let mut spec = make_spec(3);
        spec.failure_domain = "rack".into();

        let result = engine.place_volume(&spec, &candidates).unwrap();
        assert_eq!(result.len(), 3);

        let rack_of = |id: NodeId| -> &str {
            candidates
                .iter()
                .find(|n| n.id == id)
                .unwrap()
                .tags
                .get("rack")
                .unwrap()
                .as_str()
        };

        let racks: HashSet<&str> = result.iter().map(|id| rack_of(*id)).collect();
        assert!(racks.len() >= 2);
    }

    #[test]
    fn test_should_exclude_node_failed() {
        let engine = PlacementEngine::new();
        let mut node = make_node(1, &[], gb(100), 0);
        node.state = NodeState::Failed;
        assert!(engine.should_exclude_node(&node));
    }

    #[test]
    fn test_should_exclude_node_faulted_zfs() {
        let engine = PlacementEngine::new();
        let mut node = make_node(1, &[], gb(100), 0);
        node.zfs_health = ZfsHealthState::Faulted;
        assert!(engine.should_exclude_node(&node));
    }

    #[test]
    fn test_should_not_exclude_healthy_node() {
        let engine = PlacementEngine::new();
        let node = make_node(1, &[], gb(100), 0);
        assert!(!engine.should_exclude_node(&node));
    }

    #[test]
    fn test_needs_re_replication_faulted() {
        let engine = PlacementEngine::new();
        let mut node = make_node(1, &[], gb(100), 0);
        node.zfs_health = ZfsHealthState::Faulted;
        assert!(engine.needs_re_replication(&node));
    }

    #[test]
    fn test_needs_re_replication_failed() {
        let engine = PlacementEngine::new();
        let mut node = make_node(1, &[], gb(100), 0);
        node.state = NodeState::Failed;
        assert!(engine.needs_re_replication(&node));
    }

    #[test]
    fn test_needs_re_replication_healthy() {
        let engine = PlacementEngine::new();
        let node = make_node(1, &[], gb(100), 0);
        assert!(!engine.needs_re_replication(&node));
    }

    #[test]
    fn test_default() {
        let _engine = PlacementEngine::default();
    }

    // ── Per-volume affinity enforcement ──────────────────────────────────

    #[test]
    fn test_place_volume_affinity_tags_checked() {
        let engine = PlacementEngine::new();
        // 4 nodes, but only 2 match the affinity tag "region" = "us-east"
        let candidates = vec![
            make_node(1, &[("region", "us-east")], gb(100), 0),
            make_node(2, &[("region", "us-west")], gb(100), 0),
            make_node(3, &[("region", "us-east")], gb(100), 0),
            make_node(4, &[("region", "eu-west")], gb(100), 0),
        ];
        let mut spec = make_spec(2);
        spec.affinity.insert("region".into(), "us-east".into());

        let result = engine.place_volume(&spec, &candidates).unwrap();
        assert_eq!(result.len(), 2);
        // Only nodes 1 and 3 are in us-east
        for id in &result {
            assert!(
                *id == 1 || *id == 3,
                "unexpected node {id} selected – should only have nodes with region=us-east"
            );
        }
    }

    #[test]
    fn test_place_volume_affinity_not_enough_matching() {
        let engine = PlacementEngine::new();
        let candidates = vec![
            make_node(1, &[("region", "us-east")], gb(100), 0),
            make_node(2, &[("region", "us-west")], gb(100), 0),
            make_node(3, &[("region", "us-west")], gb(100), 0),
        ];
        let mut spec = make_spec(3);
        spec.affinity.insert("region".into(), "us-east".into());

        // Only 1 node matches, but we need 3 replicas
        let result = engine.place_volume(&spec, &candidates);
        assert!(result.is_err());
    }

    #[test]
    fn test_place_volume_multi_tag_affinity() {
        let engine = PlacementEngine::new();
        let candidates = vec![
            make_node(
                1,
                &[("region", "us-east"), ("storage_class", "ssd")],
                gb(100),
                0,
            ),
            make_node(
                2,
                &[("region", "us-east"), ("storage_class", "hdd")],
                gb(100),
                0,
            ),
            make_node(
                3,
                &[("region", "us-east"), ("storage_class", "ssd")],
                gb(100),
                0,
            ),
            make_node(
                4,
                &[("region", "us-west"), ("storage_class", "ssd")],
                gb(100),
                0,
            ),
        ];
        let mut spec = make_spec(2);
        spec.affinity.insert("region".into(), "us-east".into());
        spec.affinity.insert("storage_class".into(), "ssd".into());

        let result = engine.place_volume(&spec, &candidates).unwrap();
        assert_eq!(result.len(), 2);
        // Only nodes 1 and 3 match both tags
        for id in &result {
            assert!(*id == 1 || *id == 3);
        }
    }

    // ── Failure domain spreading with >3 racks ──────────────────────────

    #[test]
    fn test_place_volume_failure_domain_more_than_3_racks() {
        let engine = PlacementEngine::new();
        let candidates = vec![
            make_node(1, &[("rack", "r1")], gb(100), 0),
            make_node(2, &[("rack", "r2")], gb(100), 0),
            make_node(3, &[("rack", "r3")], gb(100), 0),
            make_node(4, &[("rack", "r4")], gb(100), 0),
            make_node(5, &[("rack", "r5")], gb(100), 0),
        ];
        let mut spec = make_spec(3);
        spec.failure_domain = "rack".into();

        let result = engine.place_volume(&spec, &candidates).unwrap();
        assert_eq!(result.len(), 3);

        // All selected nodes should be in different racks
        let rack_of = |id: NodeId| -> String {
            candidates
                .iter()
                .find(|n| n.id == id)
                .unwrap()
                .tags
                .get("rack")
                .unwrap()
                .clone()
        };
        let racks: HashSet<String> = result.iter().map(|id| rack_of(*id)).collect();
        assert_eq!(
            racks.len(),
            3,
            "with 5 distinct racks and 3 replicas, all replicas should be in different racks"
        );
    }

    #[test]
    fn test_place_volume_failure_domain_5_racks_5_replicas() {
        let engine = PlacementEngine::new();
        let candidates = vec![
            make_node(1, &[("rack", "r1")], gb(100), 0),
            make_node(2, &[("rack", "r2")], gb(100), 0),
            make_node(3, &[("rack", "r3")], gb(100), 0),
            make_node(4, &[("rack", "r4")], gb(100), 0),
            make_node(5, &[("rack", "r5")], gb(100), 0),
        ];
        let mut spec = make_spec(5);
        spec.failure_domain = "rack".into();

        let result = engine.place_volume(&spec, &candidates).unwrap();
        assert_eq!(result.len(), 5);

        let rack_of = |id: NodeId| -> String {
            candidates
                .iter()
                .find(|n| n.id == id)
                .unwrap()
                .tags
                .get("rack")
                .unwrap()
                .clone()
        };
        let racks: HashSet<String> = result.iter().map(|id| rack_of(*id)).collect();
        assert_eq!(racks.len(), 5, "all 5 racks should be represented");
    }

    #[test]
    fn test_place_volume_failure_domain_4_racks_uneven() {
        let engine = PlacementEngine::new();
        // 4 racks but rack r1 has 3 nodes, others have 1
        let candidates = vec![
            make_node(1, &[("rack", "r1")], gb(100), gb(10)),
            make_node(2, &[("rack", "r1")], gb(100), gb(20)),
            make_node(3, &[("rack", "r1")], gb(100), gb(30)),
            make_node(4, &[("rack", "r2")], gb(100), 0),
            make_node(5, &[("rack", "r3")], gb(100), 0),
            make_node(6, &[("rack", "r4")], gb(100), 0),
        ];
        let mut spec = make_spec(3);
        spec.failure_domain = "rack".into();

        let result = engine.place_volume(&spec, &candidates).unwrap();
        assert_eq!(result.len(), 3);

        let rack_of = |id: NodeId| -> String {
            candidates
                .iter()
                .find(|n| n.id == id)
                .unwrap()
                .tags
                .get("rack")
                .unwrap()
                .clone()
        };
        let racks: HashSet<String> = result.iter().map(|id| rack_of(*id)).collect();
        // With round-robin interleaving across 4 racks, the first 3 picks should
        // each be from a different rack.
        assert!(
            racks.len() >= 3,
            "spread should maximise rack diversity: got {} racks from {:?}",
            racks.len(),
            result,
        );
    }

    // ── Multi-pool placement tests ────────────────────────────────────────

    #[test]
    fn test_placement_prefers_multi_pool_node() {
        use blockyard_common::types::PoolInfo;

        let engine = PlacementEngine::new();

        // Node 1 has 2 pools (100GB each), node 2 has 1 pool (100GB).
        let mut node1 = make_node(1, &[], 0, 0);
        node1.pools = vec![
            PoolInfo {
                name: "ssd".into(),
                capacity_bytes: gb(100),
                used_bytes: 0,
                health: ZfsHealthState::Online,
            },
            PoolInfo {
                name: "hdd".into(),
                capacity_bytes: gb(100),
                used_bytes: 0,
                health: ZfsHealthState::Online,
            },
        ];

        let node2 = make_node(2, &[], gb(100), 0);
        let node3 = make_node(3, &[], gb(50), 0);

        let candidates = vec![node1, node2, node3];
        let spec = make_spec(2);
        let result = engine.place_volume(&spec, &candidates).unwrap();
        assert_eq!(result.len(), 2);

        // Node 1 (200GB free via 2 pools) should be selected first.
        assert_eq!(result[0], 1);
    }

    #[test]
    fn test_total_free_capacity_with_pools() {
        use blockyard_common::types::PoolInfo;

        let mut node = make_node(1, &[], gb(50), gb(10));
        node.pools = vec![
            PoolInfo {
                name: "ssd".into(),
                capacity_bytes: gb(100),
                used_bytes: gb(30),
                health: ZfsHealthState::Online,
            },
            PoolInfo {
                name: "hdd".into(),
                capacity_bytes: gb(200),
                used_bytes: gb(50),
                health: ZfsHealthState::Online,
            },
        ];

        // With pools, total free = (100-30) + (200-50) = 70 + 150 = 220GB.
        let free = PlacementEngine::total_free_capacity(&node);
        assert_eq!(free, gb(220));
    }

    #[test]
    fn test_total_free_capacity_without_pools() {
        let node = make_node(1, &[], gb(100), gb(30));
        // Without pools, falls back to top-level fields: 100 - 30 = 70GB.
        let free = PlacementEngine::total_free_capacity(&node);
        assert_eq!(free, gb(70));
    }
}
