use std::collections::HashMap;

use blockyard_common::{DiskId, EpochId, ExtentId, NodeId, OperationId, SessionId, VolumeId};
use blockyard_storage::extent::compute_checksum;
use blockyard_storage::{ExtentStore, StorageClass};
use blockyard_ublk::DataNodeClient;
use blockyard_ublk::traits::{WriteAck, WriteAckError};
use bytes::Bytes;
use parking_lot::Mutex;
use tempfile::TempDir;

pub struct DiskBackedTestDataNode {
    stores: Mutex<HashMap<NodeId, (ExtentStore, TempDir)>>,
    fail_nodes: Mutex<Vec<NodeId>>,
}

impl DiskBackedTestDataNode {
    pub fn new() -> Self {
        Self {
            stores: Mutex::new(HashMap::new()),
            fail_nodes: Mutex::new(Vec::new()),
        }
    }

    pub fn set_fail_nodes(&self, nodes: Vec<NodeId>) {
        *self.fail_nodes.lock() = nodes;
    }

    pub fn get_or_create_store(&self, node_id: NodeId) -> DiskId {
        let mut stores = self.stores.lock();
        stores
            .entry(node_id)
            .or_insert_with(|| {
                let tmpdir = TempDir::new().expect("create tempdir for node");
                let disk_id = DiskId::generate();
                let store = ExtentStore::new(tmpdir.path().to_path_buf(), disk_id);
                store.init_directories().expect("init directories");
                (store, tmpdir)
            })
            .0
            .disk_id()
    }

    pub fn read_back(
        &self,
        node_id: NodeId,
        extent_id: ExtentId,
        version: u64,
    ) -> Result<(Vec<u8>, String), String> {
        let stores = self.stores.lock();
        let (store, _) = stores
            .get(&node_id)
            .ok_or_else(|| format!("no store for node {node_id}"))?;
        store
            .read_extent(extent_id, version)
            .map_err(|e| format!("{e}"))
    }

    pub fn stored_count(&self) -> usize {
        self.stores.lock().len()
    }
}

impl Default for DiskBackedTestDataNode {
    fn default() -> Self {
        Self::new()
    }
}

impl DataNodeClient for DiskBackedTestDataNode {
    async fn write_extent(
        &self,
        node_id: NodeId,
        _operation_id: OperationId,
        _session_id: SessionId,
        _volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        _epoch: EpochId,
        data: Bytes,
        checksum: String,
    ) -> Result<WriteAck, blockyard_common::Error> {
        let fail_nodes = self.fail_nodes.lock();
        if fail_nodes.contains(&node_id) {
            return Ok(WriteAck {
                node_id,
                success: false,
                checksum,
                error: Some(WriteAckError::DiskUnavailable),
            });
        }
        drop(fail_nodes);

        self.get_or_create_store(node_id);
        let mut stores = self.stores.lock();
        let (store, _) = stores.get_mut(&node_id).unwrap();
        let (_, disk_checksum) = store
            .stage_extent(extent_id, extent_version, &data)
            .map_err(|e| blockyard_common::Error::Storage(format!("{e}")))?;
        store
            .commit_extent(
                extent_id,
                extent_version,
                &disk_checksum,
                data.len() as u64,
                StorageClass::Default,
            )
            .map_err(|e| blockyard_common::Error::Storage(format!("{e}")))?;

        Ok(WriteAck {
            node_id,
            success: true,
            checksum,
            error: None,
        })
    }
}

pub fn create_test_extent_store(tmpdir: &TempDir) -> (ExtentStore, DiskId) {
    let disk_id = DiskId::generate();
    let store = ExtentStore::new(tmpdir.path().to_path_buf(), disk_id);
    store.init_directories().expect("init directories");
    (store, disk_id)
}

pub fn deterministic_data(extent_index: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|pos| ((extent_index.wrapping_mul(0x37)) ^ pos) as u8)
        .collect()
}

pub fn write_test_extent(
    store: &ExtentStore,
    extent_index: usize,
    version: u64,
) -> (ExtentId, u64, String, Vec<u8>) {
    let extent_id = ExtentId::generate();
    let data = deterministic_data(extent_index, 4096);
    let (_staged, checksum) = store
        .stage_extent(extent_id, version, &data)
        .expect("stage extent");
    store
        .commit_extent(
            extent_id,
            version,
            &checksum,
            data.len() as u64,
            StorageClass::Default,
        )
        .expect("commit extent");
    (extent_id, version, checksum, data)
}

pub fn verify_disk_data(
    data_client: &DiskBackedTestDataNode,
    nodes: &[NodeId],
    extent_id: ExtentId,
    version: u64,
    expected_data: &[u8],
) -> usize {
    let expected_checksum = compute_checksum(expected_data);
    let mut readable_count = 0;
    for nid in nodes {
        if let Ok((disk_data, disk_checksum)) = data_client.read_back(*nid, extent_id, version) {
            assert_eq!(disk_data, expected_data, "data on disk must match original");
            assert_eq!(
                disk_checksum, expected_checksum,
                "checksum from disk must match computed"
            );
            readable_count += 1;
        }
    }
    readable_count
}
