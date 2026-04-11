//! Client-side volume mount implementation.
//!
//! Mount is a CLIENT-SIDE operation: the CLI process fetches volume/cluster
//! info from the management API, builds all pipeline components locally,
//! creates a UBLK device, and stays alive as the device owner. On signal
//! (SIGTERM/SIGINT) it releases the lease and stops the device.
//!
//! When the `ublk-kernel` feature is not enabled, mount falls back to the
//! `BlockyardClient::mount()` method (mock/HTTP).

use anyhow::Result;

use blockyard_common::VolumeId;

use crate::client::BlockyardClient;
use crate::types::MountInfo;

/// Execute a client-side mount with real UBLK kernel device.
///
/// This is only available with the `ublk-kernel` feature. It:
/// 1. Fetches volume info and node list from the cluster
/// 2. Builds all pipeline components locally
/// 3. Acquires a write lease
/// 4. Creates a UBLK kernel device
/// 5. Blocks until SIGTERM/SIGINT
/// 6. Releases the lease and stops the device
#[cfg(feature = "ublk-kernel")]
pub async fn execute_mount_kernel(
    endpoint: &str,
    volume_id: VolumeId,
    _device: Option<String>,
    client: &impl BlockyardClient,
) -> Result<MountInfo> {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Context;

    use blockyard_common::{EpochId, SessionId};
    use blockyard_ublk::block_handler::{ClusterBlockHandler, VolumeConfig};
    use blockyard_ublk::lease_manager::LeaseManager;
    use blockyard_ublk::metadata_cache::{CachedVolumeInfo, MetadataCache};
    use blockyard_ublk::session::ClientSession;
    use blockyard_ublk::stale_epoch::StaleEpochHandler;
    use blockyard_ublk::tcp_client::TcpDataNodeClient;
    use blockyard_ublk::ublk::{UblkDevice, UblkDeviceConfig};
    use blockyard_ublk::watermark::WriteWatermark;
    use blockyard_ublk::HttpMetadataClient;

    const LEASE_TTL: Duration = Duration::from_secs(30);

    let vol = client
        .volume_inspect(volume_id)
        .await
        .context("failed to fetch volume info")?;

    let nodes = client
        .node_list()
        .await
        .context("failed to fetch node list")?;

    let data_client = Arc::new(TcpDataNodeClient::new());
    let metadata_cache = Arc::new(MetadataCache::new());

    metadata_cache.set_epoch(EpochId::new(0));

    for node in &nodes {
        let addr: std::net::SocketAddr = node
            .address
            .parse()
            .context(format!("invalid node address: {}", node.address))?;
        data_client.register_node(node.id, addr);
        metadata_cache.set_node(node.id, addr);
    }

    metadata_cache.set_volume(CachedVolumeInfo {
        volume_id: vol.id,
        size_bytes: vol.size_bytes,
        protection: vol.protection.clone(),
        extent_mappings: BTreeMap::new(),
    });

    // Load existing extent mappings from the metadata server so data
    // written in previous mount sessions is visible.
    {
        let http = reqwest::Client::new();
        let url = format!("{}/api/v1/volumes/{}/extent-mappings", endpoint, volume_id);
        if let Ok(resp) = http.get(&url).send().await {
            if resp.status().is_success() {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    if let Some(arr) = body.get("mappings").and_then(|m| m.as_array()) {
                        let mut loaded = 0u64;
                        for entry in arr {
                            let block_start = entry
                                .get("block_start")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0);
                            let block_end = entry
                                .get("block_end")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0);
                            let extent_id_str =
                                entry.get("extent_id").and_then(|v| v.as_str()).unwrap_or("");
                            let extent_id: blockyard_common::ExtentId = match extent_id_str.parse()
                            {
                                Ok(id) => id,
                                Err(_) => continue,
                            };
                            let extent_version = entry
                                .get("extent_version")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(1);
                            let replica_locations: Vec<blockyard_common::NodeId> = entry
                                .get("replica_locations")
                                .and_then(|v| v.as_array())
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|v| v.as_str()?.parse().ok())
                                        .collect()
                                })
                                .unwrap_or_default();
                            let checksums: Vec<Vec<u8>> = entry
                                .get("checksums")
                                .and_then(|v| v.as_array())
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|v| {
                                            let s = v.as_str()?;
                                            let bytes: Vec<u8> = (0..s.len())
                                                .step_by(2)
                                                .filter_map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
                                                .collect();
                                            Some(bytes)
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();

                            let size_bytes = (block_end - block_start) * 4096;
                            metadata_cache.set_extent_mapping(
                                &volume_id,
                                block_start,
                                blockyard_ublk::metadata_cache::CachedExtentMapping {
                                    extent_id,
                                    extent_version,
                                    replica_locations,
                                    checksums,
                                    size_bytes,
                                },
                            );
                            loaded += 1;
                        }
                        tracing::info!(count = loaded, "loaded extent mappings from metadata server");
                    }
                }
            }
        }
    }

    let session = Arc::new(ClientSession::new(volume_id));
    let session_id = SessionId::generate();
    let watermark = Arc::new(WriteWatermark::new());
    let stale_handler = Arc::new(StaleEpochHandler::new());
    let lease_manager = Arc::new(LeaseManager::new(volume_id, session_id, LEASE_TTL));

    let metadata_client = Arc::new(HttpMetadataClient::new(endpoint.to_string()));

    lease_manager
        .acquire(metadata_client.as_ref())
        .await
        .context("failed to acquire write lease")?;

    tracing::info!(%volume_id, "write lease acquired");

    let volume_config = VolumeConfig {
        volume_id: vol.id,
        size_bytes: vol.size_bytes,
        block_size: 4096,
        protection: vol.protection,
    };

    let handler = ClusterBlockHandler::new(
        volume_config,
        data_client,
        metadata_client.clone(),
        Arc::clone(&lease_manager),
        session,
        Arc::clone(&metadata_cache),
        watermark,
        stale_handler,
    );

    let ublk_config = UblkDeviceConfig {
        device_size_bytes: vol.size_bytes,
        block_size: 4096,
        queue_depth: 128,
        num_queues: 1,
    };

    let device = UblkDevice::new(handler, ublk_config);
    let device_path = device
        .start_kernel()
        .await
        .context("failed to start UBLK kernel device")?;

    tracing::info!(%volume_id, %device_path, "UBLK device created");

    let info = MountInfo {
        volume_id,
        device_path: device_path.clone(),
        mount_point: None,
    };

    println!("Volume {volume_id} mounted at {device_path}");
    println!("Press Ctrl+C to unmount");

    tokio::signal::ctrl_c()
        .await
        .context("failed to wait for signal")?;

    tracing::info!(%volume_id, "received shutdown signal, cleaning up");

    device.stop().await;
    let _ = lease_manager.release(metadata_client.as_ref()).await;

    tracing::info!(%volume_id, "volume unmounted");
    Ok(info)
}

/// Execute mount via the BlockyardClient trait (mock/HTTP fallback).
///
/// Used when the `ublk-kernel` feature is not available, or for testing.
pub async fn execute_mount_fallback(
    volume_id: VolumeId,
    device: Option<String>,
    client: &impl BlockyardClient,
) -> Result<MountInfo> {
    client.mount(volume_id, device).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::mock::MockClient;

    #[tokio::test]
    async fn test_mount_fallback_success() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let vid = vols[0].id;
        let info = execute_mount_fallback(vid, None, &client).await.unwrap();
        assert_eq!(info.volume_id, vid);
        assert_eq!(info.device_path, "/dev/ublk0");
    }

    #[tokio::test]
    async fn test_mount_fallback_custom_device() {
        let client = MockClient::with_sample_data();
        let vols = client.volume_list().await.unwrap();
        let vid = vols[0].id;
        let info = execute_mount_fallback(vid, Some("/dev/ublk5".into()), &client)
            .await
            .unwrap();
        assert_eq!(info.device_path, "/dev/ublk5");
    }

    #[tokio::test]
    async fn test_mount_fallback_volume_not_found() {
        let client = MockClient::new();
        let vid = VolumeId::generate();
        let result = execute_mount_fallback(vid, None, &client).await;
        assert!(result.is_err());
    }
}
