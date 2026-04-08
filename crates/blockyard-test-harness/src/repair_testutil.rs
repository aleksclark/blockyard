use std::collections::HashMap;

use blockyard_common::{DiskId, ExtentId};
use blockyard_storage::background::repair::{
    EcReconstructor, FragmentReader, RepairExtentReader, RepairExtentWriter,
};
use blockyard_storage::extent::committed_extent_path;
use blockyard_storage::{ExtentIndex, ExtentStore, StorageClass};
use bytes::Bytes;
use tempfile::TempDir;

pub struct TestRepairReader {
    pub stores: HashMap<DiskId, (ExtentStore, TempDir)>,
}

impl TestRepairReader {
    pub fn new() -> Self {
        Self {
            stores: HashMap::new(),
        }
    }

    pub fn add_store(&mut self, disk_id: DiskId, store: ExtentStore, tmpdir: TempDir) {
        self.stores.insert(disk_id, (store, tmpdir));
    }
}

impl Default for TestRepairReader {
    fn default() -> Self {
        Self::new()
    }
}

impl RepairExtentReader for TestRepairReader {
    fn read_extent(
        &self,
        source_disk: DiskId,
        extent_id: ExtentId,
        version: u64,
    ) -> Result<Bytes, String> {
        let (store, _) = self
            .stores
            .get(&source_disk)
            .ok_or_else(|| format!("no store for disk {source_disk}"))?;
        let (data, _) = store
            .read_extent(extent_id, version)
            .map_err(|e| format!("{e}"))?;
        Ok(Bytes::from(data))
    }
}

pub struct TestRepairWriter {
    pub stores: HashMap<DiskId, (ExtentStore, TempDir)>,
}

impl TestRepairWriter {
    pub fn new() -> Self {
        Self {
            stores: HashMap::new(),
        }
    }

    pub fn add_store(&mut self, disk_id: DiskId, store: ExtentStore, tmpdir: TempDir) {
        self.stores.insert(disk_id, (store, tmpdir));
    }

    pub fn write_count(&self) -> usize {
        let mut count = 0;
        for (store, _) in self.stores.values() {
            let index = ExtentIndex::new();
            if let Ok(report) = store.recover(&index) {
                count += report.committed_recovered;
            }
        }
        count
    }

    pub fn read_back(
        &self,
        disk_id: DiskId,
        extent_id: ExtentId,
        version: u64,
    ) -> Option<(Vec<u8>, String)> {
        let (store, _) = self.stores.get(&disk_id)?;
        store.read_extent(extent_id, version).ok()
    }
}

impl Default for TestRepairWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl RepairExtentWriter for TestRepairWriter {
    fn write_extent(
        &self,
        target_disk: DiskId,
        extent_id: ExtentId,
        version: u64,
        data: &[u8],
    ) -> Result<(), String> {
        let (store, _) = self
            .stores
            .get(&target_disk)
            .ok_or_else(|| format!("no store for disk {target_disk}"))?;
        let checksum = blockyard_storage::extent::compute_checksum(data);
        let committed_path = committed_extent_path(store.mount_path(), extent_id, version);
        if committed_path.exists() {
            std::fs::remove_file(&committed_path).map_err(|e| format!("{e}"))?;
            let meta_path = blockyard_storage::extent::extent_meta_path(
                store.mount_path(),
                extent_id,
                version,
            );
            if meta_path.exists() {
                let _ = std::fs::remove_file(&meta_path);
            }
        }
        store
            .stage_extent(extent_id, version, data)
            .map_err(|e| format!("{e}"))?;
        store
            .commit_extent(
                extent_id,
                version,
                &checksum,
                data.len() as u64,
                StorageClass::Default,
            )
            .map_err(|e| format!("{e}"))?;
        Ok(())
    }
}

pub struct StubFragmentReader;

impl FragmentReader for StubFragmentReader {
    fn read_fragment(
        &self,
        _source_disk: DiskId,
        _extent_id: ExtentId,
        _fragment_index: usize,
    ) -> Result<Bytes, String> {
        Err("not implemented".into())
    }
}

pub struct StubEcReconstructor;

impl EcReconstructor for StubEcReconstructor {
    fn reconstruct(
        &self,
        _data_count: usize,
        _parity_count: usize,
        _fragments: Vec<Option<Bytes>>,
        _original_len: usize,
    ) -> Result<Bytes, String> {
        Err("not implemented".into())
    }
}

pub struct TestExtentReader {
    pub stores: Vec<(DiskId, ExtentStore)>,
    pub entries: Vec<(DiskId, ExtentId, u64, String)>,
}

impl TestExtentReader {
    pub fn new(stores: Vec<(DiskId, ExtentStore)>) -> Self {
        Self {
            stores,
            entries: Vec::new(),
        }
    }

    pub fn register(
        &mut self,
        disk_id: DiskId,
        extent_id: ExtentId,
        version: u64,
        checksum: String,
    ) {
        self.entries.push((disk_id, extent_id, version, checksum));
    }

    pub fn store_for(&self, disk_id: DiskId) -> Option<&ExtentStore> {
        self.stores
            .iter()
            .find(|(d, _)| *d == disk_id)
            .map(|(_, s)| s)
    }
}

impl blockyard_storage::background::scrub::ExtentReader for TestExtentReader {
    fn read_extent(
        &self,
        disk_id: DiskId,
        extent_id: ExtentId,
        version: u64,
    ) -> Result<(Vec<u8>, String), String> {
        let store = self.store_for(disk_id).ok_or("disk not found")?;
        let path = committed_extent_path(store.mount_path(), extent_id, version);
        let data = std::fs::read(&path).map_err(|e| format!("read error: {e}"))?;
        let checksum = blockyard_storage::extent::compute_checksum(&data);
        Ok((data, checksum))
    }

    fn list_extents(
        &self,
        disk_id: DiskId,
    ) -> Vec<blockyard_storage::background::scrub::ScrubExtentEntry> {
        self.entries
            .iter()
            .filter(|(d, _, _, _)| *d == disk_id)
            .map(|(d, e, v, c)| blockyard_storage::background::scrub::ScrubExtentEntry {
                extent_id: *e,
                disk_id: *d,
                expected_checksum: c.clone(),
                version: *v,
            })
            .collect()
    }

    fn list_disks(&self) -> Vec<DiskId> {
        self.stores.iter().map(|(d, _)| *d).collect()
    }
}
