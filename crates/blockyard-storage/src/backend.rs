use blockyard_common::types::{VolumeId, ZfsPoolHealth};
use std::collections::HashMap;

pub trait StorageBackend: Send + Sync {
    fn create_zvol(
        &self,
        volume_id: &VolumeId,
        size_bytes: u64,
    ) -> impl std::future::Future<Output = blockyard_common::Result<()>> + Send;

    fn destroy_zvol(
        &self,
        volume_id: &VolumeId,
    ) -> impl std::future::Future<Output = blockyard_common::Result<()>> + Send;

    fn resize_zvol(
        &self,
        volume_id: &VolumeId,
        new_size: u64,
    ) -> impl std::future::Future<Output = blockyard_common::Result<()>> + Send;

    fn snapshot_zvol(
        &self,
        volume_id: &VolumeId,
        snap_name: &str,
    ) -> impl std::future::Future<Output = blockyard_common::Result<()>> + Send;

    fn pool_capacity(
        &self,
    ) -> impl std::future::Future<Output = blockyard_common::Result<(u64, u64)>> + Send;

    fn pool_health(
        &self,
    ) -> impl std::future::Future<Output = blockyard_common::Result<ZfsPoolHealth>> + Send;

    fn list_zvols(
        &self,
    ) -> impl std::future::Future<Output = blockyard_common::Result<Vec<ZvolInfo>>> + Send;

    fn pool_name(&self) -> &str;
}

#[derive(Debug, Clone)]
pub struct ZvolInfo {
    pub name: String,
    pub volume_id: VolumeId,
    pub size_bytes: u64,
    pub used_bytes: u64,
}

pub struct MemoryBackend {
    pool_name: String,
    zvols: parking_lot::Mutex<HashMap<VolumeId, ZvolEntry>>,
    capacity: u64,
}

#[derive(Debug, Clone)]
struct ZvolEntry {
    volume_id: VolumeId,
    size_bytes: u64,
    snapshots: Vec<String>,
}

impl MemoryBackend {
    pub fn new(pool_name: String, capacity: u64) -> Self {
        Self {
            pool_name,
            zvols: parking_lot::Mutex::new(HashMap::new()),
            capacity,
        }
    }

    fn used_bytes(&self) -> u64 {
        self.zvols.lock().values().map(|z| z.size_bytes).sum()
    }
}

impl StorageBackend for MemoryBackend {
    async fn create_zvol(
        &self,
        volume_id: &VolumeId,
        size_bytes: u64,
    ) -> blockyard_common::Result<()> {
        let mut zvols = self.zvols.lock();
        if zvols.contains_key(volume_id) {
            return Err(blockyard_common::Error::Storage(format!(
                "zvol already exists: {volume_id}"
            )));
        }
        let used: u64 = zvols.values().map(|z| z.size_bytes).sum();
        if used + size_bytes > self.capacity {
            return Err(blockyard_common::Error::Storage("pool full".to_string()));
        }
        zvols.insert(
            *volume_id,
            ZvolEntry {
                volume_id: *volume_id,
                size_bytes,
                snapshots: Vec::new(),
            },
        );
        Ok(())
    }

    async fn destroy_zvol(&self, volume_id: &VolumeId) -> blockyard_common::Result<()> {
        self.zvols
            .lock()
            .remove(volume_id)
            .ok_or_else(|| blockyard_common::Error::VolumeNotFound(volume_id.to_string()))?;
        Ok(())
    }

    async fn resize_zvol(
        &self,
        volume_id: &VolumeId,
        new_size: u64,
    ) -> blockyard_common::Result<()> {
        let mut zvols = self.zvols.lock();
        let entry = zvols
            .get_mut(volume_id)
            .ok_or_else(|| blockyard_common::Error::VolumeNotFound(volume_id.to_string()))?;
        entry.size_bytes = new_size;
        Ok(())
    }

    async fn snapshot_zvol(
        &self,
        volume_id: &VolumeId,
        snap_name: &str,
    ) -> blockyard_common::Result<()> {
        let mut zvols = self.zvols.lock();
        let entry = zvols
            .get_mut(volume_id)
            .ok_or_else(|| blockyard_common::Error::VolumeNotFound(volume_id.to_string()))?;
        entry.snapshots.push(snap_name.to_string());
        Ok(())
    }

    async fn pool_capacity(&self) -> blockyard_common::Result<(u64, u64)> {
        Ok((self.capacity, self.used_bytes()))
    }

    async fn pool_health(&self) -> blockyard_common::Result<ZfsPoolHealth> {
        let used = self.used_bytes();
        Ok(ZfsPoolHealth {
            pool_name: self.pool_name.clone(),
            state: blockyard_common::types::ZfsHealthState::Online,
            capacity_bytes: self.capacity,
            used_bytes: used,
            free_bytes: self.capacity.saturating_sub(used),
            ..Default::default()
        })
    }

    async fn list_zvols(&self) -> blockyard_common::Result<Vec<ZvolInfo>> {
        Ok(self
            .zvols
            .lock()
            .values()
            .map(|z| ZvolInfo {
                name: format!("{}/vol-{}", self.pool_name, z.volume_id),
                volume_id: z.volume_id,
                size_bytes: z.size_bytes,
                used_bytes: z.size_bytes / 2,
            })
            .collect())
    }

    fn pool_name(&self) -> &str {
        &self.pool_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn gb(n: u64) -> u64 {
        n * 1024 * 1024 * 1024
    }

    #[tokio::test]
    async fn test_memory_backend_create_zvol() {
        let backend = MemoryBackend::new("test".into(), gb(100));
        let vol_id = Uuid::new_v4();
        backend.create_zvol(&vol_id, gb(10)).await.unwrap();

        let zvols = backend.list_zvols().await.unwrap();
        assert_eq!(zvols.len(), 1);
        assert_eq!(zvols[0].volume_id, vol_id);
    }

    #[tokio::test]
    async fn test_memory_backend_create_duplicate() {
        let backend = MemoryBackend::new("test".into(), gb(100));
        let vol_id = Uuid::new_v4();
        backend.create_zvol(&vol_id, gb(10)).await.unwrap();
        let result = backend.create_zvol(&vol_id, gb(10)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_backend_pool_full() {
        let backend = MemoryBackend::new("test".into(), gb(10));
        let vol_id = Uuid::new_v4();
        let result = backend.create_zvol(&vol_id, gb(20)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_backend_destroy_zvol() {
        let backend = MemoryBackend::new("test".into(), gb(100));
        let vol_id = Uuid::new_v4();
        backend.create_zvol(&vol_id, gb(10)).await.unwrap();
        backend.destroy_zvol(&vol_id).await.unwrap();
        assert!(backend.list_zvols().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_memory_backend_destroy_not_found() {
        let backend = MemoryBackend::new("test".into(), gb(100));
        let result = backend.destroy_zvol(&Uuid::new_v4()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_backend_resize_zvol() {
        let backend = MemoryBackend::new("test".into(), gb(100));
        let vol_id = Uuid::new_v4();
        backend.create_zvol(&vol_id, gb(10)).await.unwrap();
        backend.resize_zvol(&vol_id, gb(20)).await.unwrap();

        let zvols = backend.list_zvols().await.unwrap();
        assert_eq!(zvols[0].size_bytes, gb(20));
    }

    #[tokio::test]
    async fn test_memory_backend_resize_not_found() {
        let backend = MemoryBackend::new("test".into(), gb(100));
        let result = backend.resize_zvol(&Uuid::new_v4(), gb(10)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_backend_snapshot() {
        let backend = MemoryBackend::new("test".into(), gb(100));
        let vol_id = Uuid::new_v4();
        backend.create_zvol(&vol_id, gb(10)).await.unwrap();
        backend.snapshot_zvol(&vol_id, "snap-1").await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_backend_snapshot_not_found() {
        let backend = MemoryBackend::new("test".into(), gb(100));
        let result = backend.snapshot_zvol(&Uuid::new_v4(), "snap-1").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_backend_pool_capacity() {
        let backend = MemoryBackend::new("test".into(), gb(100));
        let vol_id = Uuid::new_v4();
        backend.create_zvol(&vol_id, gb(30)).await.unwrap();

        let (total, used) = backend.pool_capacity().await.unwrap();
        assert_eq!(total, gb(100));
        assert_eq!(used, gb(30));
    }

    #[tokio::test]
    async fn test_memory_backend_pool_health() {
        let backend = MemoryBackend::new("test".into(), gb(100));
        let health = backend.pool_health().await.unwrap();
        assert_eq!(health.pool_name, "test");
        assert_eq!(
            health.state,
            blockyard_common::types::ZfsHealthState::Online
        );
        assert_eq!(health.capacity_bytes, gb(100));
    }

    #[test]
    fn test_memory_backend_pool_name() {
        let backend = MemoryBackend::new("mypool".into(), gb(100));
        assert_eq!(backend.pool_name(), "mypool");
    }
}
