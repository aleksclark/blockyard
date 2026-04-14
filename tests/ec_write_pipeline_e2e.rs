//! End-to-end integration test that reproduces EC write failure through
//! the full write pipeline (not just raw TCP).
//!
//! This test sets up the client-side pipeline exactly the way `byard mount`
//! does (see crates/blockyard-cli/src/mount.rs) and executes a write through
//! EcWritePipeline against a real 3-node cluster. The goal is to reproduce
//! the "insufficient acks (0/3)" bug that occurs when mounting an EC volume.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use blockyard_common::{EpochId, NodeId, ProtectionPolicy, VolumeId};
use blockyard_test_harness::process_harness::{
    RealProcessCluster, build_binary, unique_base_port,
};
use blockyard_ublk::ec_write_pipeline::EcWritePipeline;
use blockyard_ublk::http_metadata_client::HttpMetadataClient;
use blockyard_ublk::metadata_cache::{CachedVolumeInfo, MetadataCache};
use blockyard_ublk::session::ClientSession;
use blockyard_ublk::stale_epoch::StaleEpochHandler;
use blockyard_ublk::tcp_client::TcpDataNodeClient;
use blockyard_ublk::watermark::WriteWatermark;
use blockyard_ublk::write_pipeline::WriteOutcome;

/// Start a 3-node cluster and create an EC(2,1) volume.
/// Returns the cluster, the volume ID, and the management URL.
async fn setup_ec_cluster() -> (RealProcessCluster, VolumeId, String) {
    let binary = build_binary().await.expect("build blockyard binary");
    let base_port = unique_base_port();
    let cluster = RealProcessCluster::new(3, binary, base_port);
    cluster.start_all().await.expect("start 3-node cluster");
    cluster
        .wait_cluster_healthy(Duration::from_secs(30))
        .await
        .expect("cluster healthy");

    let vol = cluster
        .create_volume(
            "ec-pipeline-vol",
            1024 * 1024 * 64, // 64 MiB
            serde_json::json!({"ErasureCoded": {"data_chunks": 2, "parity_chunks": 1}}),
        )
        .await
        .expect("create EC(2,1) volume");

    let vol_id_str = vol["id"].as_str().expect("volume id in response");
    let volume_id: VolumeId = vol_id_str.parse().expect("parse VolumeId from API");
    let mgmt_url = cluster.node(0).mgmt_url();

    (cluster, volume_id, mgmt_url)
}

/// Fetch the node list from the management API.
/// Returns a vec of (NodeId, SocketAddr) pairs.
async fn fetch_nodes_from_api(
    mgmt_url: &str,
) -> Vec<(NodeId, std::net::SocketAddr)> {
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/nodes", mgmt_url);
    let resp = client.get(&url).send().await.expect("fetch node list");
    let nodes: Vec<serde_json::Value> = resp.json().await.expect("parse node list");

    nodes
        .into_iter()
        .map(|n| {
            let id_str = n["id"].as_str().expect("node id");
            let addr_str = n["address"].as_str().expect("node address");
            let node_id: NodeId = id_str.parse().expect("parse NodeId");
            let addr: std::net::SocketAddr = addr_str.parse().expect("parse SocketAddr");
            (node_id, addr)
        })
        .collect()
}

/// EC write pipeline e2e test — reproduces the full mount.rs write path.
///
/// Steps:
/// 1. Start a 3-node cluster with real blockyard binaries
/// 2. Create an EC(2,1) volume via the API
/// 3. Set up the client-side pipeline exactly like mount.rs does:
///    - Fetch nodes from API
///    - Create TcpDataNodeClient and register all node addresses
///    - Create MetadataCache, set epoch to 0, set nodes, set volume info
///    - Create ClientSession, WriteWatermark, StaleEpochHandler
///    - Create EcWritePipeline with the real TcpDataNodeClient
/// 4. Execute a write through the EcWritePipeline
/// 5. Assert the write succeeds (WriteOutcome::Committed)
///
/// This test reproduces the bug where EC writes fail with
/// "insufficient acks (0/3)" because all 3 fragment writes fail.
#[tokio::test]
async fn test_ec_write_pipeline_full_mount_path() {
    // Step 1 & 2: Start cluster and create EC volume
    let (cluster, volume_id, mgmt_url) = setup_ec_cluster().await;

    // Step 3a: Fetch nodes from API (exactly like mount.rs line 58-61)
    let nodes = fetch_nodes_from_api(&mgmt_url).await;
    assert!(
        nodes.len() >= 3,
        "expected at least 3 nodes from API, got {}",
        nodes.len()
    );

    eprintln!("--- Fetched {} nodes from API ---", nodes.len());
    for (id, addr) in &nodes {
        eprintln!("  node {} @ {}", id, addr);
    }

    // Step 3b: Create TcpDataNodeClient and register all node addresses
    // (mount.rs lines 63, 68-75)
    let data_client = Arc::new(TcpDataNodeClient::new());
    let metadata_cache = Arc::new(MetadataCache::new());

    // Set epoch to 0 (mount.rs line 66)
    metadata_cache.set_epoch(EpochId::new(0));

    // Register each node with both the data client and metadata cache
    // (mount.rs lines 68-75)
    for (node_id, addr) in &nodes {
        data_client.register_node(*node_id, *addr);
        metadata_cache.set_node(*node_id, *addr);
    }

    // Step 3c: Set volume info in cache (mount.rs lines 77-83)
    let volume_info = CachedVolumeInfo {
        volume_id,
        size_bytes: 1024 * 1024 * 64,
        block_size: 4096,
        protection: ProtectionPolicy::ErasureCoded {
            data_chunks: 2,
            parity_chunks: 1,
        },
        extent_mappings: BTreeMap::new(),
    };
    metadata_cache.set_volume(volume_info);

    // Verify cache is set up correctly
    let cached_vol = metadata_cache
        .get_volume(&volume_id)
        .expect("volume should be in cache");
    eprintln!(
        "--- Cached volume: id={}, size={}, block_size={}, protection={:?} ---",
        cached_vol.volume_id, cached_vol.size_bytes, cached_vol.block_size, cached_vol.protection
    );
    let cached_nodes = metadata_cache.list_nodes();
    eprintln!("--- Cached {} nodes ---", cached_nodes.len());
    for n in &cached_nodes {
        eprintln!("  node {} @ {}", n.node_id, n.addr);
    }

    // Step 3d: Create session, watermark, stale epoch handler
    // (mount.rs lines 88-91)
    let session = Arc::new(ClientSession::new(volume_id));
    let watermark = Arc::new(WriteWatermark::new());
    let stale_handler = Arc::new(StaleEpochHandler::new());

    // Step 3e: Create HttpMetadataClient (mount.rs line 94)
    let metadata_client = Arc::new(HttpMetadataClient::new(mgmt_url.clone()));

    // Step 3f: Create EcWritePipeline (this is what ClusterBlockHandler does
    // internally for EC volumes)
    let pipeline = EcWritePipeline::new(
        data_client,
        metadata_client,
        Arc::clone(&metadata_cache),
        session,
        watermark,
        stale_handler,
    );

    // Step 4: Execute a write through the pipeline
    // Write a single block (4096 bytes) at block offset 0
    let write_data = Bytes::from(vec![0xAB_u8; 4096]);
    let block_range = 0..1_u64; // one block

    eprintln!("--- Executing EC write: volume={}, blocks=0..1, data_len={} ---",
        volume_id, write_data.len());

    let result = pipeline
        .execute(volume_id, block_range, write_data)
        .await;

    // Step 5: Check the result
    match &result {
        Ok(WriteOutcome::Committed { epoch }) => {
            eprintln!("--- EC write COMMITTED at epoch {} ---", epoch);
        }
        Ok(WriteOutcome::InsufficientAcks { acked, required }) => {
            eprintln!(
                "--- EC write FAILED: insufficient acks ({}/{}) ---",
                acked, required
            );
            eprintln!("This reproduces the bug: all fragment writes failed.");
            eprintln!("Expected: Committed, Got: InsufficientAcks({}/{})", acked, required);
        }
        Ok(WriteOutcome::StaleEpoch) => {
            eprintln!("--- EC write returned StaleEpoch ---");
        }
        Ok(WriteOutcome::MetadataCommitFailed { reason }) => {
            eprintln!("--- EC write MetadataCommitFailed: {} ---", reason);
        }
        Err(e) => {
            eprintln!("--- EC write ERROR: {} ---", e);
        }
    }

    // The test asserts that the write succeeds.
    // If the bug is present, this will fail with InsufficientAcks(0/3).
    let outcome = result.expect("EC write should not return an error");
    assert_eq!(
        outcome,
        WriteOutcome::Committed {
            epoch: EpochId::new(0)
        },
        "EC write through full pipeline should succeed with Committed, \
         but got {:?}. This indicates the EC write pipeline bug is present.",
        outcome
    );

    // Keep cluster reference alive until end so nodes aren't dropped early
    drop(cluster);
}

/// Same test but with multiple blocks to verify multi-block EC writes.
#[tokio::test]
async fn test_ec_write_pipeline_multi_block() {
    let (cluster, volume_id, mgmt_url) = setup_ec_cluster().await;

    let nodes = fetch_nodes_from_api(&mgmt_url).await;
    assert!(nodes.len() >= 3);

    let data_client = Arc::new(TcpDataNodeClient::new());
    let metadata_cache = Arc::new(MetadataCache::new());
    metadata_cache.set_epoch(EpochId::new(0));

    for (node_id, addr) in &nodes {
        data_client.register_node(*node_id, *addr);
        metadata_cache.set_node(*node_id, *addr);
    }

    metadata_cache.set_volume(CachedVolumeInfo {
        volume_id,
        size_bytes: 1024 * 1024 * 64,
        block_size: 4096,
        protection: ProtectionPolicy::ErasureCoded {
            data_chunks: 2,
            parity_chunks: 1,
        },
        extent_mappings: BTreeMap::new(),
    });

    let session = Arc::new(ClientSession::new(volume_id));
    let watermark = Arc::new(WriteWatermark::new());
    let stale_handler = Arc::new(StaleEpochHandler::new());
    let metadata_client = Arc::new(HttpMetadataClient::new(mgmt_url));

    let pipeline = EcWritePipeline::new(
        data_client,
        metadata_client,
        Arc::clone(&metadata_cache),
        session,
        watermark,
        stale_handler,
    );

    // Write 4 blocks (16 KiB) at block offset 0
    let write_data = Bytes::from(vec![0xCD_u8; 4096 * 4]);
    let block_range = 0..4_u64;

    eprintln!("--- Executing multi-block EC write: blocks=0..4, data_len={} ---",
        write_data.len());

    let result = pipeline
        .execute(volume_id, block_range, write_data)
        .await;

    match &result {
        Ok(WriteOutcome::Committed { epoch }) => {
            eprintln!("--- Multi-block EC write COMMITTED at epoch {} ---", epoch);
        }
        Ok(other) => {
            eprintln!("--- Multi-block EC write outcome: {:?} ---", other);
        }
        Err(e) => {
            eprintln!("--- Multi-block EC write ERROR: {} ---", e);
        }
    }

    let outcome = result.expect("multi-block EC write should not return an error");
    assert_eq!(
        outcome,
        WriteOutcome::Committed {
            epoch: EpochId::new(0)
        },
        "Multi-block EC write through full pipeline should succeed, got {:?}",
        outcome
    );

    drop(cluster);
}
