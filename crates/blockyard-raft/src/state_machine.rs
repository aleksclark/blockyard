//! Metadata state machine: the strongly consistent replicated state that stores
//! cluster membership, placement map, volume metadata, extent mappings, and
//! protection policies (P3.2).

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::Range;

use serde::{Deserialize, Serialize};

use blockyard_common::{
    EpochId, ExtentId, LeaseRequest, LeaseResponse, LeaseVersion, NodeId, OperationId,
    ProtectionPolicy, SessionId, VolumeId, VolumeLease,
};

use crate::request::MetadataRequest;
use crate::response::MetadataResponse;
use crate::typ::{LogId, StoredMembership};

/// Metadata for a single volume.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMetadata {
    pub volume_id: VolumeId,
    pub size_bytes: u64,
    pub protection: ProtectionPolicy,
    pub created_at_epoch: EpochId,
}

/// A committed extent mapping (§4.5.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtentMapping {
    pub extent_id: ExtentId,
    pub extent_version: u64,
    pub epoch: EpochId,
    pub block_range: Range<u64>,
    pub replica_locations: Vec<NodeId>,
    pub checksums: Vec<Vec<u8>>,
    pub operation_id: Option<OperationId>,
}

/// Node membership record in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub node_id: NodeId,
    pub addr: String,
}

/// The full metadata state machine state.
///
/// This is the data replicated via Raft. It contains:
/// - Cluster membership (data-node level, distinct from Raft membership)
/// - Placement map
/// - Volume metadata
/// - Extent mappings
/// - Current placement epoch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataStateMachineData {
    pub last_applied: Option<LogId>,
    pub last_membership: StoredMembership,

    /// Current placement epoch (§2.4), monotonically increasing.
    pub epoch: EpochId,

    /// Cluster node membership (application-level, not Raft membership).
    pub nodes: BTreeMap<String, ClusterNode>,

    /// Volume metadata keyed by VolumeId string.
    pub volumes: BTreeMap<String, VolumeMetadata>,

    /// Extent mappings keyed by volume ID string, then by block range start.
    pub extent_mappings: BTreeMap<String, BTreeMap<u64, ExtentMapping>>,

    /// Index from operation ID to (volume_id_str, block_range_start) for
    /// committed state query (P3.6).
    pub operation_index: BTreeMap<String, (String, u64)>,

    /// Index from extent version to (volume_id_str, block_range_start) for
    /// committed state query by extent version (P3.6).
    pub extent_version_index: BTreeMap<u64, (String, u64)>,

    /// Placement map: arbitrary string key → ordered list of nodes.
    pub placement_map: BTreeMap<String, Vec<NodeId>>,

    /// Volume write leases keyed by VolumeId string (P6.1).
    pub leases: BTreeMap<String, VolumeLease>,

    /// Global monotonically increasing lease version counter (P6.1).
    pub lease_version_counter: LeaseVersion,

    /// Counter for assigning sequential raft u64 node IDs.
    #[serde(default)]
    pub raft_id_counter: u64,

    /// Mapping from blockyard NodeId (UUID) to raft u64 node ID.
    #[serde(default)]
    pub node_raft_map: HashMap<NodeId, u64>,
}

impl Default for MetadataStateMachineData {
    fn default() -> Self {
        Self {
            last_applied: None,
            last_membership: StoredMembership::default(),
            epoch: EpochId::new(0),
            nodes: BTreeMap::new(),
            volumes: BTreeMap::new(),
            extent_mappings: BTreeMap::new(),
            operation_index: BTreeMap::new(),
            extent_version_index: BTreeMap::new(),
            placement_map: BTreeMap::new(),
            leases: BTreeMap::new(),
            lease_version_counter: 0,
            raft_id_counter: 0,
            node_raft_map: HashMap::new(),
        }
    }
}

impl MetadataStateMachineData {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a metadata request to the state machine, returning a response.
    ///
    /// This is the core state transition function.
    pub fn apply_request(&mut self, req: &MetadataRequest) -> MetadataResponse {
        match req {
            MetadataRequest::AddNode { node_id, addr } => {
                let key = node_id.to_string();
                self.nodes.insert(
                    key,
                    ClusterNode {
                        node_id: *node_id,
                        addr: addr.clone(),
                    },
                );
                MetadataResponse::ok()
            }

            MetadataRequest::RemoveNode { node_id } => {
                let key = node_id.to_string();
                if self.nodes.remove(&key).is_some() {
                    MetadataResponse::ok()
                } else {
                    MetadataResponse::error(format!("node {node_id} not found"))
                }
            }

            MetadataRequest::CreateVolume {
                volume_id,
                size_bytes,
                protection,
            } => {
                let key = volume_id.to_string();
                if self.volumes.contains_key(&key) {
                    return MetadataResponse::error(format!("volume {volume_id} already exists"));
                }
                if let Err(e) = protection.validate() {
                    return MetadataResponse::error(format!("invalid protection policy: {e}"));
                }
                self.volumes.insert(
                    key.clone(),
                    VolumeMetadata {
                        volume_id: *volume_id,
                        size_bytes: *size_bytes,
                        protection: *protection,
                        created_at_epoch: self.epoch,
                    },
                );
                self.extent_mappings.insert(key, BTreeMap::new());
                MetadataResponse::ok()
            }

            MetadataRequest::DeleteVolume { volume_id } => {
                let key = volume_id.to_string();
                if self.volumes.remove(&key).is_none() {
                    return MetadataResponse::error(format!("volume {volume_id} not found"));
                }
                if let Some(mappings) = self.extent_mappings.remove(&key) {
                    for mapping in mappings.values() {
                        if let Some(op_id) = &mapping.operation_id {
                            self.operation_index.remove(&op_id.to_string());
                        }
                        self.extent_version_index.remove(&mapping.extent_version);
                    }
                }
                self.leases.remove(&key);
                MetadataResponse::ok()
            }

            MetadataRequest::AdvanceEpoch => {
                let new_val = self.epoch.as_u64().saturating_add(1);
                self.epoch = EpochId::new(new_val);
                MetadataResponse::epoch(self.epoch)
            }

            MetadataRequest::CommitExtentMapping {
                volume_id,
                block_range,
                extent_id,
                extent_version,
                epoch,
                replica_locations,
                checksums,
                operation_id,
                previous_version,
            } => {
                let vol_key = volume_id.to_string();

                if !self.volumes.contains_key(&vol_key) {
                    return MetadataResponse::error(format!("volume {volume_id} not found"));
                }

                if *epoch != self.epoch {
                    return MetadataResponse::error(format!(
                        "stale epoch: request has {}, current is {}",
                        epoch, self.epoch
                    ));
                }

                let vol_mappings = self
                    .extent_mappings
                    .get_mut(&vol_key)
                    .expect("extent_mappings entry must exist for existing volume");

                if let Some(prev_ver) = previous_version {
                    if let Some(existing) = vol_mappings.get(&block_range.start) {
                        if existing.extent_version != *prev_ver {
                            return MetadataResponse::error(format!(
                                "CAS failure: expected version {prev_ver}, found {}",
                                existing.extent_version
                            ));
                        }
                    } else if *prev_ver != 0 {
                        return MetadataResponse::error(format!(
                            "CAS failure: no existing mapping at block {}, expected version {prev_ver}",
                            block_range.start
                        ));
                    }
                }

                if let Some(existing) = vol_mappings.get(&block_range.start) {
                    if let Some(op_id) = &existing.operation_id {
                        self.operation_index.remove(&op_id.to_string());
                    }
                    self.extent_version_index.remove(&existing.extent_version);
                }

                let mapping = ExtentMapping {
                    extent_id: *extent_id,
                    extent_version: *extent_version,
                    epoch: *epoch,
                    block_range: block_range.clone(),
                    replica_locations: replica_locations.clone(),
                    checksums: checksums.clone(),
                    operation_id: *operation_id,
                };

                if let Some(op_id) = operation_id {
                    self.operation_index
                        .insert(op_id.to_string(), (vol_key.clone(), block_range.start));
                }
                self.extent_version_index
                    .insert(*extent_version, (vol_key.clone(), block_range.start));

                vol_mappings.insert(block_range.start, mapping);
                MetadataResponse::epoch(self.epoch)
            }

            MetadataRequest::UpdatePlacementMap { assignments } => {
                for (key, nodes) in assignments {
                    self.placement_map.insert(key.clone(), nodes.clone());
                }
                MetadataResponse::ok()
            }

            MetadataRequest::Lease(lease_req) => {
                MetadataResponse::Lease(self.apply_lease_request(lease_req))
            }

            MetadataRequest::RegisterNode { node_id, addr } => {
                // If already registered, return the existing raft ID
                if let Some(&existing_raft_id) = self.node_raft_map.get(node_id) {
                    // Update address in cluster nodes
                    let key = node_id.to_string();
                    self.nodes.insert(
                        key,
                        ClusterNode {
                            node_id: *node_id,
                            addr: addr.clone(),
                        },
                    );
                    return MetadataResponse::NodeRegistered(existing_raft_id);
                }

                // Assign next raft ID
                self.raft_id_counter += 1;
                let raft_id = self.raft_id_counter;
                self.node_raft_map.insert(*node_id, raft_id);

                // Also add to cluster nodes
                let key = node_id.to_string();
                self.nodes.insert(
                    key,
                    ClusterNode {
                        node_id: *node_id,
                        addr: addr.clone(),
                    },
                );

                MetadataResponse::NodeRegistered(raft_id)
            }
        }
    }

    /// Look up an extent mapping by operation ID (P3.6).
    pub fn lookup_by_operation_id(&self, op_id: &OperationId) -> Option<&ExtentMapping> {
        let key = op_id.to_string();
        let (vol_key, block_start) = self.operation_index.get(&key)?;
        self.extent_mappings.get(vol_key)?.get(block_start)
    }

    /// Look up an extent mapping by extent version (P3.6).
    pub fn lookup_by_extent_version(&self, version: u64) -> Option<&ExtentMapping> {
        let (vol_key, block_start) = self.extent_version_index.get(&version)?;
        self.extent_mappings.get(vol_key)?.get(block_start)
    }

    /// Get all extent mappings for a volume.
    pub fn get_volume_mappings(
        &self,
        volume_id: &VolumeId,
    ) -> Option<&BTreeMap<u64, ExtentMapping>> {
        self.extent_mappings.get(&volume_id.to_string())
    }

    /// Get volume metadata.
    pub fn get_volume(&self, volume_id: &VolumeId) -> Option<&VolumeMetadata> {
        self.volumes.get(&volume_id.to_string())
    }

    /// Get all volumes.
    pub fn list_volumes(&self) -> Vec<&VolumeMetadata> {
        self.volumes.values().collect()
    }

    /// Get a cluster node.
    pub fn get_node(&self, node_id: &NodeId) -> Option<&ClusterNode> {
        self.nodes.get(&node_id.to_string())
    }

    /// Get all cluster nodes.
    pub fn list_nodes(&self) -> Vec<&ClusterNode> {
        self.nodes.values().collect()
    }

    /// Get the current placement epoch (P3.3).
    pub fn current_epoch(&self) -> EpochId {
        self.epoch
    }

    /// Look up the raft u64 ID for a blockyard NodeId.
    pub fn get_raft_id(&self, node_id: &NodeId) -> Option<u64> {
        self.node_raft_map.get(node_id).copied()
    }

    /// Look up the blockyard NodeId for a raft u64 ID.
    pub fn get_node_id_by_raft_id(&self, raft_id: u64) -> Option<NodeId> {
        self.node_raft_map
            .iter()
            .find(|&(_, &v)| v == raft_id)
            .map(|(&k, _)| k)
    }

    /// Get the current raft_id_counter value.
    pub fn raft_id_counter(&self) -> u64 {
        self.raft_id_counter
    }

    /// Apply a lease request to the state machine (P6.1).
    fn apply_lease_request(&mut self, req: &LeaseRequest) -> LeaseResponse {
        match req {
            LeaseRequest::Acquire {
                volume_id,
                session_id,
                now_ms,
                ttl_ms,
            } => {
                let key = volume_id.to_string();

                if !self.volumes.contains_key(&key) {
                    return LeaseResponse::Denied {
                        reason: format!("volume {volume_id} not found"),
                    };
                }

                if let Some(existing) = self.leases.get(&key) {
                    if !existing.is_expired(*now_ms) && !existing.is_held_by(*session_id) {
                        return LeaseResponse::Denied {
                            reason: format!(
                                "volume {volume_id} lease held by session {}",
                                existing.holder
                            ),
                        };
                    }
                }

                self.lease_version_counter += 1;
                let lease = VolumeLease {
                    volume_id: *volume_id,
                    holder: *session_id,
                    granted_at_ms: *now_ms,
                    expires_at_ms: now_ms + ttl_ms,
                    lease_version: self.lease_version_counter,
                };
                let version = lease.lease_version;
                let expires = lease.expires_at_ms;
                self.leases.insert(key, lease);

                LeaseResponse::Granted {
                    lease_version: version,
                    expires_at_ms: expires,
                }
            }

            LeaseRequest::Renew {
                volume_id,
                session_id,
                now_ms,
                ttl_ms,
            } => {
                let key = volume_id.to_string();

                let existing = match self.leases.get(&key) {
                    Some(l) => l,
                    None => {
                        return LeaseResponse::Denied {
                            reason: format!("no lease exists for volume {volume_id}"),
                        };
                    }
                };

                if !existing.is_held_by(*session_id) {
                    return LeaseResponse::Denied {
                        reason: format!(
                            "lease for volume {volume_id} held by different session {}",
                            existing.holder
                        ),
                    };
                }

                if existing.is_expired(*now_ms) {
                    return LeaseResponse::Denied {
                        reason: format!(
                            "lease for volume {volume_id} has expired, must re-acquire"
                        ),
                    };
                }

                self.lease_version_counter += 1;
                let lease = self.leases.get_mut(&key).expect("lease must exist");
                lease.expires_at_ms = now_ms + ttl_ms;
                lease.lease_version = self.lease_version_counter;

                LeaseResponse::Renewed {
                    lease_version: lease.lease_version,
                    expires_at_ms: lease.expires_at_ms,
                }
            }

            LeaseRequest::Release {
                volume_id,
                session_id,
            } => {
                let key = volume_id.to_string();

                match self.leases.get(&key) {
                    Some(existing) if existing.is_held_by(*session_id) => {
                        self.leases.remove(&key);
                        LeaseResponse::Released
                    }
                    Some(_) => LeaseResponse::Denied {
                        reason: format!(
                            "cannot release lease for volume {volume_id}: held by different session"
                        ),
                    },
                    None => LeaseResponse::Released,
                }
            }
        }
    }

    /// Get the active lease for a volume, if any.
    pub fn get_lease(&self, volume_id: &VolumeId) -> Option<&VolumeLease> {
        self.leases.get(&volume_id.to_string())
    }

    /// Validate a lease version for write fencing (P6.2).
    ///
    /// Returns `true` if the given session holds a valid, non-expired lease
    /// with matching version for the given volume.
    pub fn validate_lease(
        &self,
        volume_id: &VolumeId,
        session_id: SessionId,
        lease_version: LeaseVersion,
        now_ms: u64,
    ) -> Result<(), String> {
        let key = volume_id.to_string();
        let lease = self
            .leases
            .get(&key)
            .ok_or_else(|| format!("no lease for volume {volume_id}"))?;

        if !lease.is_held_by(session_id) {
            return Err(format!(
                "lease for volume {volume_id} held by session {}, not {session_id}",
                lease.holder
            ));
        }

        if lease.is_expired(now_ms) {
            return Err(format!("lease for volume {volume_id} has expired"));
        }

        if lease.lease_version != lease_version {
            return Err(format!(
                "stale lease version: request has {lease_version}, current is {}",
                lease.lease_version
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockyard_common::ProtectionPolicy;

    fn make_node_id() -> NodeId {
        NodeId::generate()
    }

    fn make_volume_id() -> VolumeId {
        VolumeId::generate()
    }

    fn make_extent_id() -> ExtentId {
        ExtentId::generate()
    }

    fn make_operation_id() -> OperationId {
        OperationId::generate()
    }

    #[test]
    fn test_new_state_machine_is_empty() {
        let sm = MetadataStateMachineData::new();
        assert!(sm.last_applied.is_none());
        assert_eq!(sm.epoch, EpochId::new(0));
        assert!(sm.nodes.is_empty());
        assert!(sm.volumes.is_empty());
        assert!(sm.extent_mappings.is_empty());
    }

    #[test]
    fn test_add_node() {
        let mut sm = MetadataStateMachineData::new();
        let nid = make_node_id();
        let resp = sm.apply_request(&MetadataRequest::AddNode {
            node_id: nid,
            addr: "10.0.0.1:9800".into(),
        });
        assert!(!resp.is_error());
        assert!(sm.get_node(&nid).is_some());
        assert_eq!(sm.get_node(&nid).unwrap().addr, "10.0.0.1:9800");
    }

    #[test]
    fn test_add_node_overwrite() {
        let mut sm = MetadataStateMachineData::new();
        let nid = make_node_id();
        sm.apply_request(&MetadataRequest::AddNode {
            node_id: nid,
            addr: "10.0.0.1:9800".into(),
        });
        sm.apply_request(&MetadataRequest::AddNode {
            node_id: nid,
            addr: "10.0.0.2:9800".into(),
        });
        assert_eq!(sm.get_node(&nid).unwrap().addr, "10.0.0.2:9800");
    }

    #[test]
    fn test_remove_node() {
        let mut sm = MetadataStateMachineData::new();
        let nid = make_node_id();
        sm.apply_request(&MetadataRequest::AddNode {
            node_id: nid,
            addr: "10.0.0.1:9800".into(),
        });
        let resp = sm.apply_request(&MetadataRequest::RemoveNode { node_id: nid });
        assert!(!resp.is_error());
        assert!(sm.get_node(&nid).is_none());
    }

    #[test]
    fn test_remove_node_not_found() {
        let mut sm = MetadataStateMachineData::new();
        let nid = make_node_id();
        let resp = sm.apply_request(&MetadataRequest::RemoveNode { node_id: nid });
        assert!(resp.is_error());
    }

    #[test]
    fn test_create_volume() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let resp = sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
        });
        assert!(!resp.is_error());
        let vol = sm.get_volume(&vid).unwrap();
        assert_eq!(vol.size_bytes, 1024 * 1024);
        assert_eq!(vol.protection, ProtectionPolicy::Replicated { replicas: 3 });
        assert_eq!(vol.created_at_epoch, EpochId::new(0));
    }

    #[test]
    fn test_create_volume_duplicate() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
        });
        let resp = sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 2048,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
        });
        assert!(resp.is_error());
    }

    #[test]
    fn test_create_volume_invalid_protection() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let resp = sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 0 },
        });
        assert!(resp.is_error());
    }

    #[test]
    fn test_delete_volume() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
        });
        let resp = sm.apply_request(&MetadataRequest::DeleteVolume { volume_id: vid });
        assert!(!resp.is_error());
        assert!(sm.get_volume(&vid).is_none());
        assert!(sm.get_volume_mappings(&vid).is_none());
    }

    #[test]
    fn test_delete_volume_not_found() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let resp = sm.apply_request(&MetadataRequest::DeleteVolume { volume_id: vid });
        assert!(resp.is_error());
    }

    #[test]
    fn test_delete_volume_cleans_up_indexes() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid = make_extent_id();
        let op_id = make_operation_id();

        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
        });
        sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid,
            extent_version: 1,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: Some(op_id),
            previous_version: None,
        });

        assert!(sm.lookup_by_operation_id(&op_id).is_some());
        assert!(sm.lookup_by_extent_version(1).is_some());

        sm.apply_request(&MetadataRequest::DeleteVolume { volume_id: vid });

        assert!(sm.lookup_by_operation_id(&op_id).is_none());
        assert!(sm.lookup_by_extent_version(1).is_none());
    }

    #[test]
    fn test_advance_epoch() {
        let mut sm = MetadataStateMachineData::new();
        assert_eq!(sm.current_epoch(), EpochId::new(0));

        let resp = sm.apply_request(&MetadataRequest::AdvanceEpoch);
        match resp {
            MetadataResponse::Epoch(e) => assert_eq!(e, EpochId::new(1)),
            _ => panic!("expected Epoch response"),
        }
        assert_eq!(sm.current_epoch(), EpochId::new(1));
    }

    #[test]
    fn test_advance_epoch_monotonic() {
        let mut sm = MetadataStateMachineData::new();
        for i in 1..=10 {
            sm.apply_request(&MetadataRequest::AdvanceEpoch);
            assert_eq!(sm.current_epoch(), EpochId::new(i));
        }
    }

    #[test]
    fn test_commit_extent_mapping() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid = make_extent_id();
        let n1 = make_node_id();
        let n2 = make_node_id();

        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 2 },
        });

        let resp = sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid,
            extent_version: 1,
            epoch: EpochId::new(0),
            replica_locations: vec![n1, n2],
            checksums: vec![vec![0xaa], vec![0xbb]],
            operation_id: None,
            previous_version: None,
        });
        assert!(!resp.is_error());

        let mappings = sm.get_volume_mappings(&vid).unwrap();
        let m = mappings.get(&0).unwrap();
        assert_eq!(m.extent_id, eid);
        assert_eq!(m.extent_version, 1);
        assert_eq!(m.replica_locations, vec![n1, n2]);
    }

    #[test]
    fn test_commit_extent_mapping_stale_epoch() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid = make_extent_id();

        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
        });
        sm.apply_request(&MetadataRequest::AdvanceEpoch);

        let resp = sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid,
            extent_version: 1,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: None,
        });
        assert!(resp.is_error());
    }

    #[test]
    fn test_commit_extent_mapping_volume_not_found() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid = make_extent_id();

        let resp = sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid,
            extent_version: 1,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: None,
        });
        assert!(resp.is_error());
    }

    #[test]
    fn test_commit_extent_mapping_cas_success() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid1 = make_extent_id();
        let eid2 = make_extent_id();

        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
        });
        sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid1,
            extent_version: 1,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: None,
        });

        let resp = sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid2,
            extent_version: 2,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: Some(1),
        });
        assert!(!resp.is_error());

        let mappings = sm.get_volume_mappings(&vid).unwrap();
        assert_eq!(mappings.get(&0).unwrap().extent_id, eid2);
    }

    #[test]
    fn test_commit_extent_mapping_cas_failure() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid1 = make_extent_id();
        let eid2 = make_extent_id();

        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
        });
        sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid1,
            extent_version: 1,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: None,
        });

        let resp = sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid2,
            extent_version: 2,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: Some(99),
        });
        assert!(resp.is_error());
    }

    #[test]
    fn test_commit_extent_mapping_cas_no_existing_nonzero() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid = make_extent_id();

        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
        });

        let resp = sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid,
            extent_version: 1,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: Some(5),
        });
        assert!(resp.is_error());
    }

    #[test]
    fn test_commit_extent_mapping_cas_no_existing_zero() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid = make_extent_id();

        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
        });

        let resp = sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid,
            extent_version: 1,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: Some(0),
        });
        assert!(!resp.is_error());
    }

    #[test]
    fn test_lookup_by_operation_id() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid = make_extent_id();
        let op_id = make_operation_id();

        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
        });
        sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid,
            extent_version: 1,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: Some(op_id),
            previous_version: None,
        });

        let mapping = sm.lookup_by_operation_id(&op_id).unwrap();
        assert_eq!(mapping.extent_id, eid);
    }

    #[test]
    fn test_lookup_by_operation_id_not_found() {
        let sm = MetadataStateMachineData::new();
        let op_id = make_operation_id();
        assert!(sm.lookup_by_operation_id(&op_id).is_none());
    }

    #[test]
    fn test_lookup_by_extent_version() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid = make_extent_id();

        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
        });
        sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid,
            extent_version: 42,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: None,
        });

        let mapping = sm.lookup_by_extent_version(42).unwrap();
        assert_eq!(mapping.extent_id, eid);
    }

    #[test]
    fn test_lookup_by_extent_version_not_found() {
        let sm = MetadataStateMachineData::new();
        assert!(sm.lookup_by_extent_version(999).is_none());
    }

    #[test]
    fn test_list_volumes() {
        let mut sm = MetadataStateMachineData::new();
        let v1 = make_volume_id();
        let v2 = make_volume_id();
        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: v1,
            size_bytes: 100,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
        });
        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: v2,
            size_bytes: 200,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
        });
        assert_eq!(sm.list_volumes().len(), 2);
    }

    #[test]
    fn test_list_nodes() {
        let mut sm = MetadataStateMachineData::new();
        let n1 = make_node_id();
        let n2 = make_node_id();
        sm.apply_request(&MetadataRequest::AddNode {
            node_id: n1,
            addr: "10.0.0.1:9800".into(),
        });
        sm.apply_request(&MetadataRequest::AddNode {
            node_id: n2,
            addr: "10.0.0.2:9800".into(),
        });
        assert_eq!(sm.list_nodes().len(), 2);
    }

    #[test]
    fn test_update_placement_map() {
        let mut sm = MetadataStateMachineData::new();
        let n1 = make_node_id();
        let n2 = make_node_id();
        let mut assignments = BTreeMap::new();
        assignments.insert("zone-a".into(), vec![n1, n2]);

        let resp = sm.apply_request(&MetadataRequest::UpdatePlacementMap { assignments });
        assert!(!resp.is_error());
        assert_eq!(sm.placement_map.get("zone-a").unwrap().len(), 2);
    }

    #[test]
    fn test_update_placement_map_overwrite() {
        let mut sm = MetadataStateMachineData::new();
        let n1 = make_node_id();
        let n2 = make_node_id();

        let mut a1 = BTreeMap::new();
        a1.insert("zone-a".into(), vec![n1]);
        sm.apply_request(&MetadataRequest::UpdatePlacementMap { assignments: a1 });

        let mut a2 = BTreeMap::new();
        a2.insert("zone-a".into(), vec![n1, n2]);
        sm.apply_request(&MetadataRequest::UpdatePlacementMap { assignments: a2 });

        assert_eq!(sm.placement_map.get("zone-a").unwrap().len(), 2);
    }

    #[test]
    fn test_commit_replaces_old_mapping_indexes() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid1 = make_extent_id();
        let eid2 = make_extent_id();
        let op1 = make_operation_id();
        let op2 = make_operation_id();

        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
        });

        sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid1,
            extent_version: 1,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: Some(op1),
            previous_version: None,
        });

        sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid2,
            extent_version: 2,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: Some(op2),
            previous_version: None,
        });

        assert!(sm.lookup_by_operation_id(&op1).is_none());
        assert!(sm.lookup_by_extent_version(1).is_none());
        assert!(sm.lookup_by_operation_id(&op2).is_some());
        assert!(sm.lookup_by_extent_version(2).is_some());
    }

    #[test]
    fn test_create_volume_at_advanced_epoch() {
        let mut sm = MetadataStateMachineData::new();
        sm.apply_request(&MetadataRequest::AdvanceEpoch);
        sm.apply_request(&MetadataRequest::AdvanceEpoch);
        let vid = make_volume_id();
        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
        });
        assert_eq!(
            sm.get_volume(&vid).unwrap().created_at_epoch,
            EpochId::new(2)
        );
    }

    #[test]
    fn test_default_impl() {
        let sm = MetadataStateMachineData::default();
        assert!(sm.nodes.is_empty());
        assert!(sm.volumes.is_empty());
        assert_eq!(sm.epoch, EpochId::new(0));
    }

    #[test]
    fn test_serde_roundtrip() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
        });
        sm.apply_request(&MetadataRequest::AdvanceEpoch);

        let json = serde_json::to_string(&sm).unwrap();
        let restored: MetadataStateMachineData = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.epoch, EpochId::new(1));
        assert!(restored.get_volume(&vid).is_some());
    }

    #[test]
    fn test_response_ok() {
        let r = MetadataResponse::ok();
        assert!(!r.is_error());
    }

    #[test]
    fn test_response_epoch() {
        let r = MetadataResponse::epoch(EpochId::new(5));
        assert!(!r.is_error());
        match r {
            MetadataResponse::Epoch(e) => assert_eq!(e, EpochId::new(5)),
            _ => panic!("expected Epoch"),
        }
    }

    #[test]
    fn test_response_error() {
        let r = MetadataResponse::error("bad");
        assert!(r.is_error());
    }

    #[test]
    fn test_erasure_coded_volume() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let resp = sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 4096,
            protection: ProtectionPolicy::ErasureCoded {
                data_chunks: 4,
                parity_chunks: 2,
            },
        });
        assert!(!resp.is_error());
        let vol = sm.get_volume(&vid).unwrap();
        assert_eq!(
            vol.protection,
            ProtectionPolicy::ErasureCoded {
                data_chunks: 4,
                parity_chunks: 2
            }
        );
    }

    #[test]
    fn test_multiple_block_ranges() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let eid1 = make_extent_id();
        let eid2 = make_extent_id();

        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024 * 1024,
            protection: ProtectionPolicy::Replicated { replicas: 1 },
        });

        sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 0..64,
            extent_id: eid1,
            extent_version: 1,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: None,
        });
        sm.apply_request(&MetadataRequest::CommitExtentMapping {
            volume_id: vid,
            block_range: 64..128,
            extent_id: eid2,
            extent_version: 2,
            epoch: EpochId::new(0),
            replica_locations: vec![],
            checksums: vec![],
            operation_id: None,
            previous_version: None,
        });

        let mappings = sm.get_volume_mappings(&vid).unwrap();
        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings.get(&0).unwrap().extent_id, eid1);
        assert_eq!(mappings.get(&64).unwrap().extent_id, eid2);
    }

    fn make_session_id() -> SessionId {
        SessionId::generate()
    }

    fn create_volume(sm: &mut MetadataStateMachineData, vid: VolumeId) {
        sm.apply_request(&MetadataRequest::CreateVolume {
            volume_id: vid,
            size_bytes: 1024,
            protection: ProtectionPolicy::Replicated { replicas: 3 },
        });
    }

    #[test]
    fn test_acquire_lease_success() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        create_volume(&mut sm, vid);

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Granted {
                lease_version,
                expires_at_ms,
            }) => {
                assert_eq!(lease_version, 1);
                assert_eq!(expires_at_ms, 31_000);
            }
            other => panic!("expected Granted, got {other:?}"),
        }

        let lease = sm.get_lease(&vid).unwrap();
        assert!(lease.is_held_by(sid));
        assert!(!lease.is_expired(1000));
    }

    #[test]
    fn test_acquire_lease_volume_not_found() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Denied { reason }) => {
                assert!(reason.contains("not found"));
            }
            other => panic!("expected Denied, got {other:?}"),
        }
    }

    #[test]
    fn test_acquire_lease_already_held() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid1 = make_session_id();
        let sid2 = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid1,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid2,
            now_ms: 2000,
            ttl_ms: 30_000,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Denied { reason }) => {
                assert!(reason.contains("held by session"));
            }
            other => panic!("expected Denied, got {other:?}"),
        }
    }

    #[test]
    fn test_acquire_lease_after_expiry() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid1 = make_session_id();
        let sid2 = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid1,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid2,
            now_ms: 31_000,
            ttl_ms: 30_000,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Granted { lease_version, .. }) => {
                assert_eq!(lease_version, 2);
            }
            other => panic!("expected Granted, got {other:?}"),
        }

        let lease = sm.get_lease(&vid).unwrap();
        assert!(lease.is_held_by(sid2));
    }

    #[test]
    fn test_acquire_lease_same_session_reacquire() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 5000,
            ttl_ms: 30_000,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Granted { lease_version, .. }) => {
                assert_eq!(lease_version, 2);
            }
            other => panic!("expected Granted, got {other:?}"),
        }
    }

    #[test]
    fn test_renew_lease_success() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Renew {
            volume_id: vid,
            session_id: sid,
            now_ms: 15_000,
            ttl_ms: 30_000,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Renewed {
                lease_version,
                expires_at_ms,
            }) => {
                assert_eq!(lease_version, 2);
                assert_eq!(expires_at_ms, 45_000);
            }
            other => panic!("expected Renewed, got {other:?}"),
        }
    }

    #[test]
    fn test_renew_lease_no_lease_exists() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Renew {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Denied { reason }) => {
                assert!(reason.contains("no lease exists"));
            }
            other => panic!("expected Denied, got {other:?}"),
        }
    }

    #[test]
    fn test_renew_lease_wrong_session() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid1 = make_session_id();
        let sid2 = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid1,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Renew {
            volume_id: vid,
            session_id: sid2,
            now_ms: 5000,
            ttl_ms: 30_000,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Denied { reason }) => {
                assert!(reason.contains("different session"));
            }
            other => panic!("expected Denied, got {other:?}"),
        }
    }

    #[test]
    fn test_renew_lease_expired() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Renew {
            volume_id: vid,
            session_id: sid,
            now_ms: 50_000,
            ttl_ms: 30_000,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Denied { reason }) => {
                assert!(reason.contains("expired"));
            }
            other => panic!("expected Denied, got {other:?}"),
        }
    }

    #[test]
    fn test_release_lease_success() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Release {
            volume_id: vid,
            session_id: sid,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Released) => {}
            other => panic!("expected Released, got {other:?}"),
        }

        assert!(sm.get_lease(&vid).is_none());
    }

    #[test]
    fn test_release_lease_wrong_session() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid1 = make_session_id();
        let sid2 = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid1,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Release {
            volume_id: vid,
            session_id: sid2,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Denied { reason }) => {
                assert!(reason.contains("different session"));
            }
            other => panic!("expected Denied, got {other:?}"),
        }

        assert!(sm.get_lease(&vid).is_some());
    }

    #[test]
    fn test_release_lease_no_lease() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();

        let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Release {
            volume_id: vid,
            session_id: sid,
        }));

        match resp {
            MetadataResponse::Lease(LeaseResponse::Released) => {}
            other => panic!("expected Released, got {other:?}"),
        }
    }

    #[test]
    fn test_validate_lease_success() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        assert!(sm.validate_lease(&vid, sid, 1, 5000).is_ok());
    }

    #[test]
    fn test_validate_lease_no_lease() {
        let sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        let result = sm.validate_lease(&vid, sid, 1, 1000);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no lease"));
    }

    #[test]
    fn test_validate_lease_wrong_session() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid1 = make_session_id();
        let sid2 = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid1,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        let result = sm.validate_lease(&vid, sid2, 1, 5000);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("held by session"));
    }

    #[test]
    fn test_validate_lease_expired() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        let result = sm.validate_lease(&vid, sid, 1, 50_000);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expired"));
    }

    #[test]
    fn test_validate_lease_stale_version() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Renew {
            volume_id: vid,
            session_id: sid,
            now_ms: 10_000,
            ttl_ms: 30_000,
        }));

        let result = sm.validate_lease(&vid, sid, 1, 15_000);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("stale lease version"));
    }

    #[test]
    fn test_lease_version_monotonic() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        create_volume(&mut sm, vid);

        let mut versions = Vec::new();
        for i in 0..5 {
            let resp = sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
                volume_id: vid,
                session_id: sid,
                now_ms: i * 50_000,
                ttl_ms: 30_000,
            }));
            match resp {
                MetadataResponse::Lease(LeaseResponse::Granted { lease_version, .. }) => {
                    versions.push(lease_version);
                }
                other => panic!("expected Granted, got {other:?}"),
            }
        }

        for window in versions.windows(2) {
            assert!(
                window[1] > window[0],
                "versions must be strictly increasing"
            );
        }
    }

    #[test]
    fn test_delete_volume_cleans_up_lease() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        assert!(sm.get_lease(&vid).is_some());
        sm.apply_request(&MetadataRequest::DeleteVolume { volume_id: vid });
        assert!(sm.get_lease(&vid).is_none());
    }

    #[test]
    fn test_lease_serde_roundtrip_in_state_machine() {
        let mut sm = MetadataStateMachineData::new();
        let vid = make_volume_id();
        let sid = make_session_id();
        create_volume(&mut sm, vid);

        sm.apply_request(&MetadataRequest::Lease(LeaseRequest::Acquire {
            volume_id: vid,
            session_id: sid,
            now_ms: 1000,
            ttl_ms: 30_000,
        }));

        let json = serde_json::to_string(&sm).unwrap();
        let restored: MetadataStateMachineData = serde_json::from_str(&json).unwrap();
        let lease = restored.get_lease(&vid).unwrap();
        assert!(lease.is_held_by(sid));
        assert_eq!(lease.lease_version, 1);
    }

    #[test]
    fn test_register_node_assigns_sequential_raft_ids() {
        let mut sm = MetadataStateMachineData::new();
        let n1 = make_node_id();
        let n2 = make_node_id();

        let resp1 = sm.apply_request(&MetadataRequest::RegisterNode {
            node_id: n1,
            addr: "10.0.0.1:9810".into(),
        });
        let resp2 = sm.apply_request(&MetadataRequest::RegisterNode {
            node_id: n2,
            addr: "10.0.0.2:9810".into(),
        });

        assert!(matches!(resp1, MetadataResponse::NodeRegistered(1)));
        assert!(matches!(resp2, MetadataResponse::NodeRegistered(2)));
        assert_eq!(sm.raft_id_counter(), 2);
    }

    #[test]
    fn test_register_node_idempotent_returns_same_id() {
        let mut sm = MetadataStateMachineData::new();
        let n1 = make_node_id();

        let resp1 = sm.apply_request(&MetadataRequest::RegisterNode {
            node_id: n1,
            addr: "10.0.0.1:9810".into(),
        });
        let resp2 = sm.apply_request(&MetadataRequest::RegisterNode {
            node_id: n1,
            addr: "10.0.0.1:9999".into(),
        });

        assert!(matches!(resp1, MetadataResponse::NodeRegistered(1)));
        assert!(matches!(resp2, MetadataResponse::NodeRegistered(1)));
        assert_eq!(sm.raft_id_counter(), 1);
    }

    #[test]
    fn test_register_node_adds_to_cluster_nodes() {
        let mut sm = MetadataStateMachineData::new();
        let n1 = make_node_id();

        sm.apply_request(&MetadataRequest::RegisterNode {
            node_id: n1,
            addr: "10.0.0.1:9810".into(),
        });

        let node = sm.get_node(&n1).unwrap();
        assert_eq!(node.node_id, n1);
        assert_eq!(node.addr, "10.0.0.1:9810");
    }

    #[test]
    fn test_register_node_updates_addr_on_reregister() {
        let mut sm = MetadataStateMachineData::new();
        let n1 = make_node_id();

        sm.apply_request(&MetadataRequest::RegisterNode {
            node_id: n1,
            addr: "10.0.0.1:9810".into(),
        });
        sm.apply_request(&MetadataRequest::RegisterNode {
            node_id: n1,
            addr: "10.0.0.1:9999".into(),
        });

        assert_eq!(sm.get_node(&n1).unwrap().addr, "10.0.0.1:9999");
    }

    #[test]
    fn test_get_raft_id() {
        let mut sm = MetadataStateMachineData::new();
        let n1 = make_node_id();

        assert!(sm.get_raft_id(&n1).is_none());
        sm.apply_request(&MetadataRequest::RegisterNode {
            node_id: n1,
            addr: "10.0.0.1:9810".into(),
        });
        assert_eq!(sm.get_raft_id(&n1), Some(1));
    }

    #[test]
    fn test_get_node_id_by_raft_id() {
        let mut sm = MetadataStateMachineData::new();
        let n1 = make_node_id();

        sm.apply_request(&MetadataRequest::RegisterNode {
            node_id: n1,
            addr: "10.0.0.1:9810".into(),
        });

        assert_eq!(sm.get_node_id_by_raft_id(1), Some(n1));
        assert_eq!(sm.get_node_id_by_raft_id(999), None);
    }

    #[test]
    fn test_raft_id_counter_accessor() {
        let sm = MetadataStateMachineData::new();
        assert_eq!(sm.raft_id_counter(), 0);
    }

    #[test]
    fn test_node_raft_map_survives_serde() {
        let mut sm = MetadataStateMachineData::new();
        let n1 = make_node_id();
        let n2 = make_node_id();

        sm.apply_request(&MetadataRequest::RegisterNode {
            node_id: n1,
            addr: "10.0.0.1:9810".into(),
        });
        sm.apply_request(&MetadataRequest::RegisterNode {
            node_id: n2,
            addr: "10.0.0.2:9810".into(),
        });

        let json = serde_json::to_string(&sm).unwrap();
        let restored: MetadataStateMachineData = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.get_raft_id(&n1), Some(1));
        assert_eq!(restored.get_raft_id(&n2), Some(2));
        assert_eq!(restored.raft_id_counter(), 2);
    }
}
