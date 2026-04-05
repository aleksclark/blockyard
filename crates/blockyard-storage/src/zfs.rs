use blockyard_common::types::VolumeId;
use tracing::info;

pub struct ZfsBackend {
    pool_name: String,
}

impl ZfsBackend {
    pub fn new(pool_name: String) -> Self {
        Self { pool_name }
    }

    pub fn pool_name(&self) -> &str {
        &self.pool_name
    }

    pub async fn create_zvol(
        &self,
        volume_id: &VolumeId,
        size_bytes: u64,
    ) -> blockyard_common::Result<()> {
        let zvol_name = format!("{}/vol-{}", self.pool_name, volume_id);
        info!(zvol = %zvol_name, size = size_bytes, "creating zvol");
        Ok(())
    }

    pub async fn destroy_zvol(&self, volume_id: &VolumeId) -> blockyard_common::Result<()> {
        let zvol_name = format!("{}/vol-{}", self.pool_name, volume_id);
        info!(zvol = %zvol_name, "destroying zvol");
        Ok(())
    }

    pub async fn resize_zvol(
        &self,
        volume_id: &VolumeId,
        new_size: u64,
    ) -> blockyard_common::Result<()> {
        let zvol_name = format!("{}/vol-{}", self.pool_name, volume_id);
        info!(zvol = %zvol_name, new_size, "resizing zvol");
        Ok(())
    }

    pub async fn snapshot_zvol(
        &self,
        volume_id: &VolumeId,
        snap_name: &str,
    ) -> blockyard_common::Result<()> {
        let zvol_name = format!("{}/vol-{}@{}", self.pool_name, volume_id, snap_name);
        info!(snapshot = %zvol_name, "creating snapshot");
        Ok(())
    }

    pub async fn pool_capacity(&self) -> blockyard_common::Result<(u64, u64)> {
        Ok((0, 0))
    }
}
