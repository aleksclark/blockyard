use blockyard_common::types::{VolumeId, ZfsHealthState, ZfsPoolHealth};
use tracing::{info, warn};

use crate::backend::StorageBackend;

pub struct ZfsBackend {
    pool_name: String,
}

impl ZfsBackend {
    pub fn new(pool_name: String) -> Self {
        Self { pool_name }
    }
}

impl StorageBackend for ZfsBackend {
    async fn create_zvol(&self, volume_id: &VolumeId, size_bytes: u64) -> blockyard_common::Result<()> {
        let zvol_name = format!("{}/vol-{}", self.pool_name, volume_id);
        info!(zvol = %zvol_name, size = size_bytes, "creating zvol");

        let output = tokio::process::Command::new("zfs")
            .args(["create", "-V", &format!("{size_bytes}"), &zvol_name])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(blockyard_common::Error::Storage(format!(
                "zfs create failed: {stderr}"
            )));
        }
        Ok(())
    }

    async fn destroy_zvol(&self, volume_id: &VolumeId) -> blockyard_common::Result<()> {
        let zvol_name = format!("{}/vol-{}", self.pool_name, volume_id);
        info!(zvol = %zvol_name, "destroying zvol");

        let output = tokio::process::Command::new("zfs")
            .args(["destroy", &zvol_name])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(blockyard_common::Error::Storage(format!(
                "zfs destroy failed: {stderr}"
            )));
        }
        Ok(())
    }

    async fn resize_zvol(&self, volume_id: &VolumeId, new_size: u64) -> blockyard_common::Result<()> {
        let zvol_name = format!("{}/vol-{}", self.pool_name, volume_id);
        info!(zvol = %zvol_name, new_size, "resizing zvol");

        let output = tokio::process::Command::new("zfs")
            .args(["set", &format!("volsize={new_size}"), &zvol_name])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(blockyard_common::Error::Storage(format!(
                "zfs resize failed: {stderr}"
            )));
        }
        Ok(())
    }

    async fn snapshot_zvol(&self, volume_id: &VolumeId, snap_name: &str) -> blockyard_common::Result<()> {
        let snap_full = format!("{}/vol-{}@{}", self.pool_name, volume_id, snap_name);
        info!(snapshot = %snap_full, "creating snapshot");

        let output = tokio::process::Command::new("zfs")
            .args(["snapshot", &snap_full])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(blockyard_common::Error::Storage(format!(
                "zfs snapshot failed: {stderr}"
            )));
        }
        Ok(())
    }

    async fn pool_capacity(&self) -> blockyard_common::Result<(u64, u64)> {
        let output = tokio::process::Command::new("zpool")
            .args(["list", "-Hp", "-o", "size,alloc", &self.pool_name])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(blockyard_common::Error::Storage(format!(
                "zpool list failed: {stderr}"
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let parts: Vec<&str> = stdout.trim().split_whitespace().collect();
        if parts.len() < 2 {
            return Err(blockyard_common::Error::Storage(
                "unexpected zpool list output".into(),
            ));
        }

        let total: u64 = parts[0].parse().map_err(|e| {
            blockyard_common::Error::Storage(format!("parse capacity: {e}"))
        })?;
        let used: u64 = parts[1].parse().map_err(|e| {
            blockyard_common::Error::Storage(format!("parse used: {e}"))
        })?;

        Ok((total, used))
    }

    async fn pool_health(&self) -> blockyard_common::Result<ZfsPoolHealth> {
        let output = tokio::process::Command::new("zpool")
            .args([
                "list", "-Hp", "-o",
                "name,size,alloc,free,frag,health",
                &self.pool_name,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(blockyard_common::Error::Storage(format!(
                "zpool list failed: {stderr}"
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let parts: Vec<&str> = stdout.trim().split_whitespace().collect();
        if parts.len() < 6 {
            return Err(blockyard_common::Error::Storage(
                "unexpected zpool list output".into(),
            ));
        }

        let state = match parts[5] {
            "ONLINE" => ZfsHealthState::Online,
            "DEGRADED" => ZfsHealthState::Degraded,
            "FAULTED" => ZfsHealthState::Faulted,
            other => {
                warn!(state = other, "unknown pool health state");
                ZfsHealthState::Unknown
            }
        };

        let capacity: u64 = parts[1].parse().unwrap_or(0);
        let used: u64 = parts[2].parse().unwrap_or(0);
        let free: u64 = parts[3].parse().unwrap_or(0);
        let frag: u8 = parts[4].trim_end_matches('%').parse().unwrap_or(0);

        let (read_err, write_err, cksum_err) = self.parse_error_counts().await;

        Ok(ZfsPoolHealth {
            pool_name: self.pool_name.clone(),
            state,
            capacity_bytes: capacity,
            used_bytes: used,
            free_bytes: free,
            fragmentation_pct: frag,
            checksum_errors: cksum_err,
            read_errors: read_err,
            write_errors: write_err,
            scrub_errors: 0,
            last_scrub_timestamp: None,
            vdevs: Vec::new(),
        })
    }

    async fn list_zvols(&self) -> blockyard_common::Result<Vec<crate::backend::ZvolInfo>> {
        let output = tokio::process::Command::new("zfs")
            .args([
                "list", "-Hp", "-t", "volume", "-o", "name,volsize,used",
                "-r", &self.pool_name,
            ])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(blockyard_common::Error::Storage(format!(
                "zfs list failed: {stderr}"
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut zvols = Vec::new();

        for line in stdout.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 3 {
                continue;
            }
            let name = parts[0];
            let vol_id_str = name
                .strip_prefix(&format!("{}/vol-", self.pool_name))
                .unwrap_or(name);
            let volume_id = vol_id_str.parse().unwrap_or_default();
            let size_bytes: u64 = parts[1].parse().unwrap_or(0);
            let used_bytes: u64 = parts[2].parse().unwrap_or(0);
            zvols.push(crate::backend::ZvolInfo {
                name: name.to_string(),
                volume_id,
                size_bytes,
                used_bytes,
            });
        }

        Ok(zvols)
    }

    fn pool_name(&self) -> &str {
        &self.pool_name
    }
}

impl ZfsBackend {
    async fn parse_error_counts(&self) -> (u64, u64, u64) {
        let output = tokio::process::Command::new("zpool")
            .args(["status", &self.pool_name])
            .output()
            .await;

        let output = match output {
            Ok(o) if o.status.success() => o,
            _ => return (0, 0, 0),
        };

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut total_read = 0u64;
        let mut total_write = 0u64;
        let mut total_cksum = 0u64;

        for line in stdout.lines() {
            let trimmed = line.trim();
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if parts.len() >= 5 {
                if let (Ok(r), Ok(w), Ok(c)) = (
                    parts[parts.len() - 3].parse::<u64>(),
                    parts[parts.len() - 2].parse::<u64>(),
                    parts[parts.len() - 1].parse::<u64>(),
                ) {
                    total_read += r;
                    total_write += w;
                    total_cksum += c;
                }
            }
        }

        (total_read, total_write, total_cksum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zfs_backend_pool_name() {
        let backend = ZfsBackend::new("testpool".to_string());
        assert_eq!(backend.pool_name(), "testpool");
    }
}
