//! BlockyardClient trait and mock implementation.

use anyhow::Result;

use blockyard_common::{DiskId, NodeId, VolumeId};

use crate::types::{ClusterStatus, DiskInfo, MountInfo, NodeInfo, VolumeCreateParams, VolumeInfo};

pub trait BlockyardClient: Send + Sync {
    fn volume_create(
        &self,
        params: VolumeCreateParams,
    ) -> impl std::future::Future<Output = Result<VolumeInfo>> + Send;
    fn volume_delete(&self, id: VolumeId) -> impl std::future::Future<Output = Result<()>> + Send;
    fn volume_list(&self) -> impl std::future::Future<Output = Result<Vec<VolumeInfo>>> + Send;
    fn volume_inspect(
        &self,
        id: VolumeId,
    ) -> impl std::future::Future<Output = Result<VolumeInfo>> + Send;

    fn disk_list(&self) -> impl std::future::Future<Output = Result<Vec<DiskInfo>>> + Send;
    fn disk_inspect(
        &self,
        id: DiskId,
    ) -> impl std::future::Future<Output = Result<DiskInfo>> + Send;
    fn disk_drain(&self, id: DiskId) -> impl std::future::Future<Output = Result<()>> + Send;
    fn disk_remove(&self, id: DiskId) -> impl std::future::Future<Output = Result<()>> + Send;

    fn node_list(&self) -> impl std::future::Future<Output = Result<Vec<NodeInfo>>> + Send;
    fn node_inspect(
        &self,
        id: NodeId,
    ) -> impl std::future::Future<Output = Result<NodeInfo>> + Send;
    fn node_decommission(&self, id: NodeId)
    -> impl std::future::Future<Output = Result<()>> + Send;

    fn cluster_status(&self) -> impl std::future::Future<Output = Result<ClusterStatus>> + Send;

    fn mount(
        &self,
        volume_id: VolumeId,
        device_path: Option<String>,
    ) -> impl std::future::Future<Output = Result<MountInfo>> + Send;
    fn unmount(&self, volume_id: VolumeId) -> impl std::future::Future<Output = Result<()>> + Send;
}

#[cfg(any(test, feature = "testutil"))]
pub mod mock {
    use super::*;
    use crate::types::*;
    use blockyard_common::{DiskState, EpochId, ProtectionPolicy};
    use chrono::Utc;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    pub struct MockClient {
        pub volumes: Mutex<HashMap<VolumeId, VolumeInfo>>,
        pub disks: Mutex<Vec<DiskInfo>>,
        pub nodes: Mutex<Vec<NodeInfo>>,
        pub mounts: Mutex<HashMap<VolumeId, MountInfo>>,
        pub fail_next: Mutex<Option<String>>,
    }

    impl Default for MockClient {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockClient {
        pub fn new() -> Self {
            Self {
                volumes: Mutex::new(HashMap::new()),
                disks: Mutex::new(Vec::new()),
                nodes: Mutex::new(Vec::new()),
                mounts: Mutex::new(HashMap::new()),
                fail_next: Mutex::new(None),
            }
        }

        pub fn with_sample_data() -> Self {
            let client = Self::new();

            let node1 = NodeId::generate();
            let node2 = NodeId::generate();
            let node3 = NodeId::generate();

            let vol = VolumeInfo {
                id: VolumeId::generate(),
                name: "test-volume".into(),
                size_bytes: 10 * 1024 * 1024 * 1024,
                protection: ProtectionPolicy::Replicated { replicas: 3 },
                state: VolumeState::Healthy,
                replica_nodes: vec![node1, node2, node3],
                created_at: Utc::now(),
            };
            client.volumes.lock().insert(vol.id, vol);

            let disk1 = DiskInfo {
                id: DiskId::generate(),
                node_id: node1,
                path: "/dev/sda".into(),
                state: DiskState::Healthy,
                total_bytes: 1_000_000_000_000,
                used_bytes: 400_000_000_000,
                extent_count: 150,
                error_count: 0,
            };
            let disk2 = DiskInfo {
                id: DiskId::generate(),
                node_id: node2,
                path: "/dev/sdb".into(),
                state: DiskState::Healthy,
                total_bytes: 1_000_000_000_000,
                used_bytes: 350_000_000_000,
                extent_count: 120,
                error_count: 0,
            };
            client.disks.lock().extend([disk1, disk2]);

            let nodes = vec![
                NodeInfo {
                    id: node1,
                    address: "10.0.0.1:9800".into(),
                    state: NodeState::Online,
                    disk_count: 4,
                    volume_count: 10,
                    uptime_seconds: 86400,
                },
                NodeInfo {
                    id: node2,
                    address: "10.0.0.2:9800".into(),
                    state: NodeState::Online,
                    disk_count: 4,
                    volume_count: 8,
                    uptime_seconds: 86400,
                },
                NodeInfo {
                    id: node3,
                    address: "10.0.0.3:9800".into(),
                    state: NodeState::Online,
                    disk_count: 4,
                    volume_count: 12,
                    uptime_seconds: 43200,
                },
            ];
            *client.nodes.lock() = nodes;

            client
        }

        fn check_fail(&self) -> Result<()> {
            if let Some(msg) = self.fail_next.lock().take() {
                anyhow::bail!("{}", msg);
            }
            Ok(())
        }

        pub fn set_fail_next(&self, msg: &str) {
            *self.fail_next.lock() = Some(msg.to_string());
        }
    }

    impl BlockyardClient for MockClient {
        async fn volume_create(&self, params: VolumeCreateParams) -> Result<VolumeInfo> {
            self.check_fail()?;
            let info = VolumeInfo {
                id: VolumeId::generate(),
                name: params.name,
                size_bytes: params.size_bytes,
                protection: params.protection,
                state: VolumeState::Healthy,
                replica_nodes: vec![NodeId::generate()],
                created_at: Utc::now(),
            };
            self.volumes.lock().insert(info.id, info.clone());
            Ok(info)
        }

        async fn volume_delete(&self, id: VolumeId) -> Result<()> {
            self.check_fail()?;
            self.volumes
                .lock()
                .remove(&id)
                .ok_or_else(|| anyhow::anyhow!("volume {} not found", id))?;
            Ok(())
        }

        async fn volume_list(&self) -> Result<Vec<VolumeInfo>> {
            self.check_fail()?;
            Ok(self.volumes.lock().values().cloned().collect())
        }

        async fn volume_inspect(&self, id: VolumeId) -> Result<VolumeInfo> {
            self.check_fail()?;
            self.volumes
                .lock()
                .get(&id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("volume {} not found", id))
        }

        async fn disk_list(&self) -> Result<Vec<DiskInfo>> {
            self.check_fail()?;
            Ok(self.disks.lock().clone())
        }

        async fn disk_inspect(&self, id: DiskId) -> Result<DiskInfo> {
            self.check_fail()?;
            self.disks
                .lock()
                .iter()
                .find(|d| d.id == id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("disk {} not found", id))
        }

        async fn disk_drain(&self, id: DiskId) -> Result<()> {
            self.check_fail()?;
            let mut disks = self.disks.lock();
            let disk = disks
                .iter_mut()
                .find(|d| d.id == id)
                .ok_or_else(|| anyhow::anyhow!("disk {} not found", id))?;
            disk.state = DiskState::Draining;
            Ok(())
        }

        async fn disk_remove(&self, id: DiskId) -> Result<()> {
            self.check_fail()?;
            let mut disks = self.disks.lock();
            let disk = disks
                .iter_mut()
                .find(|d| d.id == id)
                .ok_or_else(|| anyhow::anyhow!("disk {} not found", id))?;
            disk.state = DiskState::Removed;
            Ok(())
        }

        async fn node_list(&self) -> Result<Vec<NodeInfo>> {
            self.check_fail()?;
            Ok(self.nodes.lock().clone())
        }

        async fn node_inspect(&self, id: NodeId) -> Result<NodeInfo> {
            self.check_fail()?;
            self.nodes
                .lock()
                .iter()
                .find(|n| n.id == id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("node {} not found", id))
        }

        async fn node_decommission(&self, id: NodeId) -> Result<()> {
            self.check_fail()?;
            let mut nodes = self.nodes.lock();
            let node = nodes
                .iter_mut()
                .find(|n| n.id == id)
                .ok_or_else(|| anyhow::anyhow!("node {} not found", id))?;
            node.state = NodeState::Decommissioning;
            Ok(())
        }

        async fn cluster_status(&self) -> Result<ClusterStatus> {
            self.check_fail()?;
            let nodes = self.nodes.lock();
            let disks = self.disks.lock();
            let volumes = self.volumes.lock();

            let nodes_online = nodes
                .iter()
                .filter(|n| n.state == NodeState::Online)
                .count() as u32;
            let total_capacity: u64 = disks.iter().map(|d| d.total_bytes).sum();
            let used_capacity: u64 = disks.iter().map(|d| d.used_bytes).sum();

            let quorum_health = if nodes_online == nodes.len() as u32 {
                QuorumHealth::Healthy
            } else if nodes_online > nodes.len() as u32 / 2 {
                QuorumHealth::Degraded
            } else {
                QuorumHealth::Lost
            };

            Ok(ClusterStatus {
                node_count: nodes.len() as u32,
                nodes_online,
                volume_count: volumes.len() as u32,
                disk_count: disks.len() as u32,
                placement_epoch: EpochId::new(1),
                quorum_health,
                total_capacity_bytes: total_capacity,
                used_capacity_bytes: used_capacity,
            })
        }

        async fn mount(
            &self,
            volume_id: VolumeId,
            device_path: Option<String>,
        ) -> Result<MountInfo> {
            self.check_fail()?;
            if !self.volumes.lock().contains_key(&volume_id) {
                anyhow::bail!("volume {} not found", volume_id);
            }
            let info = MountInfo {
                volume_id,
                device_path: device_path.unwrap_or_else(|| "/dev/ublk0".into()),
                mount_point: None,
            };
            self.mounts.lock().insert(volume_id, info.clone());
            Ok(info)
        }

        async fn unmount(&self, volume_id: VolumeId) -> Result<()> {
            self.check_fail()?;
            self.mounts
                .lock()
                .remove(&volume_id)
                .ok_or_else(|| anyhow::anyhow!("volume {} is not mounted", volume_id))?;
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn test_mock_volume_create() {
            let client = MockClient::new();
            let params = VolumeCreateParams {
                name: "test-vol".into(),
                size_bytes: 1024,
                protection: ProtectionPolicy::Replicated { replicas: 3 },
            };
            let vol = client.volume_create(params).await.unwrap();
            assert_eq!(vol.name, "test-vol");
            assert_eq!(vol.size_bytes, 1024);
        }

        #[tokio::test]
        async fn test_mock_volume_list() {
            let client = MockClient::with_sample_data();
            let vols = client.volume_list().await.unwrap();
            assert_eq!(vols.len(), 1);
        }

        #[tokio::test]
        async fn test_mock_volume_inspect() {
            let client = MockClient::with_sample_data();
            let vols = client.volume_list().await.unwrap();
            let vol = client.volume_inspect(vols[0].id).await.unwrap();
            assert_eq!(vol.name, "test-volume");
        }

        #[tokio::test]
        async fn test_mock_volume_inspect_not_found() {
            let client = MockClient::new();
            let result = client.volume_inspect(VolumeId::generate()).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_mock_volume_delete() {
            let client = MockClient::with_sample_data();
            let vols = client.volume_list().await.unwrap();
            client.volume_delete(vols[0].id).await.unwrap();
            let vols = client.volume_list().await.unwrap();
            assert!(vols.is_empty());
        }

        #[tokio::test]
        async fn test_mock_volume_delete_not_found() {
            let client = MockClient::new();
            let result = client.volume_delete(VolumeId::generate()).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_mock_disk_list() {
            let client = MockClient::with_sample_data();
            let disks = client.disk_list().await.unwrap();
            assert_eq!(disks.len(), 2);
        }

        #[tokio::test]
        async fn test_mock_disk_inspect() {
            let client = MockClient::with_sample_data();
            let disks = client.disk_list().await.unwrap();
            let disk = client.disk_inspect(disks[0].id).await.unwrap();
            assert_eq!(disk.path, "/dev/sda");
        }

        #[tokio::test]
        async fn test_mock_disk_inspect_not_found() {
            let client = MockClient::new();
            let result = client.disk_inspect(DiskId::generate()).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_mock_disk_drain() {
            let client = MockClient::with_sample_data();
            let disks = client.disk_list().await.unwrap();
            client.disk_drain(disks[0].id).await.unwrap();
            let disk = client.disk_inspect(disks[0].id).await.unwrap();
            assert_eq!(disk.state, DiskState::Draining);
        }

        #[tokio::test]
        async fn test_mock_disk_drain_not_found() {
            let client = MockClient::new();
            let result = client.disk_drain(DiskId::generate()).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_mock_disk_remove() {
            let client = MockClient::with_sample_data();
            let disks = client.disk_list().await.unwrap();
            client.disk_remove(disks[0].id).await.unwrap();
            let disk = client.disk_inspect(disks[0].id).await.unwrap();
            assert_eq!(disk.state, DiskState::Removed);
        }

        #[tokio::test]
        async fn test_mock_disk_remove_not_found() {
            let client = MockClient::new();
            let result = client.disk_remove(DiskId::generate()).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_mock_node_list() {
            let client = MockClient::with_sample_data();
            let nodes = client.node_list().await.unwrap();
            assert_eq!(nodes.len(), 3);
        }

        #[tokio::test]
        async fn test_mock_node_inspect() {
            let client = MockClient::with_sample_data();
            let nodes = client.node_list().await.unwrap();
            let node = client.node_inspect(nodes[0].id).await.unwrap();
            assert_eq!(node.address, "10.0.0.1:9800");
        }

        #[tokio::test]
        async fn test_mock_node_inspect_not_found() {
            let client = MockClient::new();
            let result = client.node_inspect(NodeId::generate()).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_mock_node_decommission() {
            let client = MockClient::with_sample_data();
            let nodes = client.node_list().await.unwrap();
            client.node_decommission(nodes[0].id).await.unwrap();
            let node = client.node_inspect(nodes[0].id).await.unwrap();
            assert_eq!(node.state, NodeState::Decommissioning);
        }

        #[tokio::test]
        async fn test_mock_node_decommission_not_found() {
            let client = MockClient::new();
            let result = client.node_decommission(NodeId::generate()).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_mock_cluster_status() {
            let client = MockClient::with_sample_data();
            let status = client.cluster_status().await.unwrap();
            assert_eq!(status.node_count, 3);
            assert_eq!(status.nodes_online, 3);
            assert_eq!(status.volume_count, 1);
            assert_eq!(status.disk_count, 2);
            assert_eq!(status.quorum_health, QuorumHealth::Healthy);
        }

        #[tokio::test]
        async fn test_mock_cluster_status_empty() {
            let client = MockClient::new();
            let status = client.cluster_status().await.unwrap();
            assert_eq!(status.node_count, 0);
            assert_eq!(status.volume_count, 0);
        }

        #[tokio::test]
        async fn test_mock_mount() {
            let client = MockClient::with_sample_data();
            let vols = client.volume_list().await.unwrap();
            let mount = client.mount(vols[0].id, None).await.unwrap();
            assert_eq!(mount.volume_id, vols[0].id);
            assert_eq!(mount.device_path, "/dev/ublk0");
        }

        #[tokio::test]
        async fn test_mock_mount_custom_device() {
            let client = MockClient::with_sample_data();
            let vols = client.volume_list().await.unwrap();
            let mount = client
                .mount(vols[0].id, Some("/dev/ublk5".into()))
                .await
                .unwrap();
            assert_eq!(mount.device_path, "/dev/ublk5");
        }

        #[tokio::test]
        async fn test_mock_mount_volume_not_found() {
            let client = MockClient::new();
            let result = client.mount(VolumeId::generate(), None).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_mock_unmount() {
            let client = MockClient::with_sample_data();
            let vols = client.volume_list().await.unwrap();
            client.mount(vols[0].id, None).await.unwrap();
            client.unmount(vols[0].id).await.unwrap();
        }

        #[tokio::test]
        async fn test_mock_unmount_not_mounted() {
            let client = MockClient::with_sample_data();
            let vols = client.volume_list().await.unwrap();
            let result = client.unmount(vols[0].id).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_mock_fail_next() {
            let client = MockClient::new();
            client.set_fail_next("connection refused");
            let result = client.volume_list().await;
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("connection refused")
            );
        }

        #[tokio::test]
        async fn test_mock_fail_next_clears() {
            let client = MockClient::with_sample_data();
            client.set_fail_next("timeout");
            let _ = client.volume_list().await;
            let result = client.volume_list().await;
            assert!(result.is_ok());
        }

        #[tokio::test]
        async fn test_mock_with_sample_data_has_volumes() {
            let client = MockClient::with_sample_data();
            let vols = client.volume_list().await.unwrap();
            assert!(!vols.is_empty());
            assert_eq!(vols[0].name, "test-volume");
        }

        #[tokio::test]
        async fn test_mock_with_sample_data_has_nodes() {
            let client = MockClient::with_sample_data();
            let nodes = client.node_list().await.unwrap();
            assert_eq!(nodes.len(), 3);
            assert!(nodes.iter().all(|n| n.state == NodeState::Online));
        }

        #[tokio::test]
        async fn test_mock_with_sample_data_has_disks() {
            let client = MockClient::with_sample_data();
            let disks = client.disk_list().await.unwrap();
            assert_eq!(disks.len(), 2);
        }
    }
}
