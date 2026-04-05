use blockyard_common::types::{NodeId, NodeInfo, VolumeSpec};

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
        let mut selected: Vec<NodeId> = Vec::new();

        let filtered: Vec<&NodeInfo> = candidates
            .iter()
            .filter(|n| {
                spec.affinity
                    .iter()
                    .all(|(k, v)| n.tags.get(k).is_some_and(|tv| tv == v))
            })
            .collect();

        for node in filtered.iter().take(spec.replicas as usize) {
            selected.push(node.id);
        }

        Ok(selected)
    }
}

impl Default for PlacementEngine {
    fn default() -> Self {
        Self::new()
    }
}
