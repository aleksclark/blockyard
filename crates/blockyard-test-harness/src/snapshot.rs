use std::collections::HashMap;
use std::fmt;
use std::time::Instant;

use blockyard_common::VolumeId;
use tracing::info;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotId(pub u64);

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "snap-{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct BlockRecord {
    pub volume_id: VolumeId,
    pub offset: u64,
    pub data: Vec<u8>,
    pub checksum: String,
}

#[derive(Debug, Clone)]
pub struct VolumeSnapshot {
    pub id: SnapshotId,
    pub volume_id: VolumeId,
    pub blocks: HashMap<u64, BlockRecord>,
    pub created_at: Instant,
}

impl VolumeSnapshot {
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn total_bytes(&self) -> u64 {
        self.blocks.values().map(|b| b.data.len() as u64).sum()
    }

    pub fn verify_against(&self, current_blocks: &HashMap<u64, Vec<u8>>) -> SnapshotVerifyResult {
        let mut matching = 0u64;
        let mut mismatched = Vec::new();
        let mut missing = Vec::new();

        for (offset, record) in &self.blocks {
            match current_blocks.get(offset) {
                Some(data) => {
                    let current_checksum = blake3::hash(data).to_hex().to_string();
                    if current_checksum == record.checksum {
                        matching += 1;
                    } else {
                        mismatched.push(SnapshotMismatch {
                            offset: *offset,
                            expected_checksum: record.checksum.clone(),
                            actual_checksum: current_checksum,
                        });
                    }
                }
                None => {
                    missing.push(*offset);
                }
            }
        }

        SnapshotVerifyResult {
            snapshot_id: self.id,
            total_blocks: self.blocks.len() as u64,
            matching,
            mismatched,
            missing,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotMismatch {
    pub offset: u64,
    pub expected_checksum: String,
    pub actual_checksum: String,
}

#[derive(Debug, Clone)]
pub struct SnapshotVerifyResult {
    pub snapshot_id: SnapshotId,
    pub total_blocks: u64,
    pub matching: u64,
    pub mismatched: Vec<SnapshotMismatch>,
    pub missing: Vec<u64>,
}

impl SnapshotVerifyResult {
    pub fn is_ok(&self) -> bool {
        self.mismatched.is_empty() && self.missing.is_empty()
    }

    pub fn mismatch_count(&self) -> usize {
        self.mismatched.len()
    }

    pub fn missing_count(&self) -> usize {
        self.missing.len()
    }
}

impl fmt::Display for SnapshotVerifyResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {} total, {} matching, {} mismatched, {} missing",
            self.snapshot_id,
            self.total_blocks,
            self.matching,
            self.mismatched.len(),
            self.missing.len()
        )
    }
}

pub struct SnapshotManager {
    next_id: u64,
    snapshots: HashMap<SnapshotId, VolumeSnapshot>,
}

impl fmt::Debug for SnapshotManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotManager")
            .field("snapshot_count", &self.snapshots.len())
            .field("next_id", &self.next_id)
            .finish()
    }
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            snapshots: HashMap::new(),
        }
    }

    pub fn take_snapshot(
        &mut self,
        volume_id: VolumeId,
        blocks: HashMap<u64, (Vec<u8>, String)>,
    ) -> SnapshotId {
        let id = SnapshotId(self.next_id);
        self.next_id += 1;

        let block_records: HashMap<u64, BlockRecord> = blocks
            .into_iter()
            .map(|(offset, (data, checksum))| {
                (
                    offset,
                    BlockRecord {
                        volume_id,
                        offset,
                        data,
                        checksum,
                    },
                )
            })
            .collect();

        let snapshot = VolumeSnapshot {
            id,
            volume_id,
            blocks: block_records,
            created_at: Instant::now(),
        };

        info!(
            "took snapshot {} of volume {} ({} blocks)",
            id,
            volume_id,
            snapshot.block_count()
        );
        self.snapshots.insert(id, snapshot);
        id
    }

    pub fn get(&self, id: &SnapshotId) -> Option<&VolumeSnapshot> {
        self.snapshots.get(id)
    }

    pub fn verify(
        &self,
        id: &SnapshotId,
        current_blocks: &HashMap<u64, Vec<u8>>,
    ) -> Option<SnapshotVerifyResult> {
        self.snapshots
            .get(id)
            .map(|snap| snap.verify_against(current_blocks))
    }

    pub fn snapshot_count(&self) -> usize {
        self.snapshots.len()
    }

    pub fn snapshot_ids(&self) -> Vec<SnapshotId> {
        self.snapshots.keys().copied().collect()
    }
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_volume_id() -> VolumeId {
        VolumeId::generate()
    }

    #[test]
    fn test_snapshot_id_display() {
        assert_eq!(format!("{}", SnapshotId(0)), "snap-0");
        assert_eq!(format!("{}", SnapshotId(42)), "snap-42");
    }

    #[test]
    fn test_snapshot_manager_new() {
        let mgr = SnapshotManager::new();
        assert_eq!(mgr.snapshot_count(), 0);
        assert!(mgr.snapshot_ids().is_empty());
    }

    #[test]
    fn test_snapshot_manager_default() {
        let mgr = SnapshotManager::default();
        assert_eq!(mgr.snapshot_count(), 0);
    }

    #[test]
    fn test_take_snapshot() {
        let vol = test_volume_id();
        let mut mgr = SnapshotManager::new();

        let data = vec![0xAB; 4096];
        let checksum = blake3::hash(&data).to_hex().to_string();
        let mut blocks = HashMap::new();
        blocks.insert(0u64, (data, checksum));

        let id = mgr.take_snapshot(vol, blocks);
        assert_eq!(id, SnapshotId(0));
        assert_eq!(mgr.snapshot_count(), 1);

        let snap = mgr.get(&id).unwrap();
        assert_eq!(snap.volume_id, vol);
        assert_eq!(snap.block_count(), 1);
        assert_eq!(snap.total_bytes(), 4096);
    }

    #[test]
    fn test_take_multiple_snapshots() {
        let vol = test_volume_id();
        let mut mgr = SnapshotManager::new();

        let id1 = mgr.take_snapshot(vol, HashMap::new());
        let id2 = mgr.take_snapshot(vol, HashMap::new());

        assert_eq!(id1, SnapshotId(0));
        assert_eq!(id2, SnapshotId(1));
        assert_eq!(mgr.snapshot_count(), 2);
    }

    #[test]
    fn test_verify_snapshot_all_match() {
        let vol = test_volume_id();
        let mut mgr = SnapshotManager::new();

        let data = vec![0xCD; 4096];
        let checksum = blake3::hash(&data).to_hex().to_string();
        let mut snap_blocks = HashMap::new();
        snap_blocks.insert(0u64, (data.clone(), checksum));

        let id = mgr.take_snapshot(vol, snap_blocks);

        let mut current = HashMap::new();
        current.insert(0u64, data);

        let result = mgr.verify(&id, &current).unwrap();
        assert!(result.is_ok());
        assert_eq!(result.matching, 1);
        assert_eq!(result.mismatch_count(), 0);
        assert_eq!(result.missing_count(), 0);
    }

    #[test]
    fn test_verify_snapshot_mismatch() {
        let vol = test_volume_id();
        let mut mgr = SnapshotManager::new();

        let data = vec![0xCD; 4096];
        let checksum = blake3::hash(&data).to_hex().to_string();
        let mut snap_blocks = HashMap::new();
        snap_blocks.insert(0u64, (data, checksum));

        let id = mgr.take_snapshot(vol, snap_blocks);

        let mut current = HashMap::new();
        current.insert(0u64, vec![0xFF; 4096]);

        let result = mgr.verify(&id, &current).unwrap();
        assert!(!result.is_ok());
        assert_eq!(result.mismatch_count(), 1);
    }

    #[test]
    fn test_verify_snapshot_missing() {
        let vol = test_volume_id();
        let mut mgr = SnapshotManager::new();

        let data = vec![0xCD; 4096];
        let checksum = blake3::hash(&data).to_hex().to_string();
        let mut snap_blocks = HashMap::new();
        snap_blocks.insert(0u64, (data, checksum));

        let id = mgr.take_snapshot(vol, snap_blocks);

        let current = HashMap::new();
        let result = mgr.verify(&id, &current).unwrap();
        assert!(!result.is_ok());
        assert_eq!(result.missing_count(), 1);
    }

    #[test]
    fn test_verify_nonexistent_snapshot() {
        let mgr = SnapshotManager::new();
        let result = mgr.verify(&SnapshotId(99), &HashMap::new());
        assert!(result.is_none());
    }

    #[test]
    fn test_snapshot_verify_result_display() {
        let result = SnapshotVerifyResult {
            snapshot_id: SnapshotId(0),
            total_blocks: 10,
            matching: 8,
            mismatched: vec![SnapshotMismatch {
                offset: 0,
                expected_checksum: "a".to_string(),
                actual_checksum: "b".to_string(),
            }],
            missing: vec![4096],
        };
        let s = format!("{}", result);
        assert!(s.contains("snap-0"));
        assert!(s.contains("10 total"));
        assert!(s.contains("8 matching"));
        assert!(s.contains("1 mismatched"));
        assert!(s.contains("1 missing"));
    }

    #[test]
    fn test_volume_snapshot_verify_against() {
        let vol = test_volume_id();
        let data1 = vec![0xAA; 512];
        let data2 = vec![0xBB; 512];
        let chk1 = blake3::hash(&data1).to_hex().to_string();
        let chk2 = blake3::hash(&data2).to_hex().to_string();

        let mut blocks = HashMap::new();
        blocks.insert(
            0u64,
            BlockRecord {
                volume_id: vol,
                offset: 0,
                data: data1.clone(),
                checksum: chk1,
            },
        );
        blocks.insert(
            512u64,
            BlockRecord {
                volume_id: vol,
                offset: 512,
                data: data2.clone(),
                checksum: chk2,
            },
        );

        let snap = VolumeSnapshot {
            id: SnapshotId(0),
            volume_id: vol,
            blocks,
            created_at: Instant::now(),
        };

        let mut current = HashMap::new();
        current.insert(0u64, data1);
        current.insert(512u64, data2);

        let result = snap.verify_against(&current);
        assert!(result.is_ok());
        assert_eq!(result.matching, 2);
    }

    #[test]
    fn test_get_nonexistent_snapshot() {
        let mgr = SnapshotManager::new();
        assert!(mgr.get(&SnapshotId(0)).is_none());
    }

    #[test]
    fn test_snapshot_ids() {
        let vol = test_volume_id();
        let mut mgr = SnapshotManager::new();
        mgr.take_snapshot(vol, HashMap::new());
        mgr.take_snapshot(vol, HashMap::new());

        let ids = mgr.snapshot_ids();
        assert_eq!(ids.len(), 2);
    }
}
