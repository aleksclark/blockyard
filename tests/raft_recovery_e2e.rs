//! End-to-end tests for raft recovery and stale extent file handling.
//!
//! These tests reproduce two production bugs:
//!
//! 1. **Raft peer registry address bug (node.rs recovery path):**
//!    On restart, the PeerRegistry was populated with data-plane addresses
//!    (port 9800) instead of raft RPC addresses (port 9810). This broke
//!    raft replication, causing the management API to hang and cascading
//!    into DiskUnavailable errors on the write path.
//!
//! 2. **Extent immutability violation (extent.rs commit_extent):**
//!    With deterministic placement, overwriting a block produces the same
//!    (extent_id, version=1). The ExtentStore rejected these as "immutability
//!    violations" instead of allowing atomic overwrite via rename(2).
//!    This caused all writes to fail after the first write to any block.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use blockyard_common::{EpochId, NodeId, PlacementEngine, ProtectionPolicy, VolumeId};
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

/// Concrete pipeline type used in all tests.
type TestPipeline = EcWritePipeline<TcpDataNodeClient, HttpMetadataClient>;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Start a 3-node cluster and create an EC(2,1) volume.
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
            "ec-test-vol",
            1024 * 1024 * 64,
            serde_json::json!({"ErasureCoded": {"data_chunks": 2, "parity_chunks": 1}}),
        )
        .await
        .expect("create EC(2,1) volume");

    let vol_id_str = vol["id"].as_str().expect("volume id");
    let volume_id: VolumeId = vol_id_str.parse().expect("parse VolumeId");
    let mgmt_url = cluster.node(0).mgmt_url();

    (cluster, volume_id, mgmt_url)
}

/// Fetch node list from the management API.
async fn fetch_nodes_from_api(mgmt_url: &str) -> Vec<(NodeId, std::net::SocketAddr)> {
    let client = reqwest::Client::new();
    let url = format!("{}/api/v1/nodes", mgmt_url);
    let resp = client.get(&url).send().await.expect("fetch node list");
    let nodes: Vec<serde_json::Value> = resp.json().await.expect("parse node list");
    nodes
        .into_iter()
        .map(|n| {
            let id_str = n["id"].as_str().expect("node id");
            let addr_str = n["address"].as_str().expect("node address");
            (
                id_str.parse().expect("parse NodeId"),
                addr_str.parse().expect("parse SocketAddr"),
            )
        })
        .collect()
}

/// Build the client-side EC write pipeline exactly like byard mount does.
fn build_pipeline(
    nodes: &[(NodeId, std::net::SocketAddr)],
    volume_id: VolumeId,
    mgmt_url: &str,
) -> TestPipeline {
    let data_client = Arc::new(TcpDataNodeClient::new());
    let metadata_cache = Arc::new(MetadataCache::new());

    metadata_cache.set_epoch(EpochId::new(0));
    for (node_id, addr) in nodes {
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
    let metadata_client = Arc::new(HttpMetadataClient::new(mgmt_url.to_string()));

    EcWritePipeline::new(
        data_client,
        metadata_client,
        metadata_cache,
        session,
        watermark,
        stale_handler,
    )
}

/// Execute a write and assert it commits successfully.
async fn assert_write_succeeds(
    pipeline: &TestPipeline,
    volume_id: VolumeId,
    block_range: std::ops::Range<u64>,
    fill_byte: u8,
    label: &str,
) {
    let data_len = (block_range.end - block_range.start) as usize * 4096;
    let data = Bytes::from(vec![fill_byte; data_len]);

    let result = pipeline
        .execute(volume_id, block_range.clone(), data)
        .await;

    match &result {
        Ok(WriteOutcome::Committed { epoch }) => {
            eprintln!("  [{label}] COMMITTED at epoch {epoch}");
        }
        Ok(WriteOutcome::InsufficientAcks { acked, required }) => {
            panic!(
                "[{label}] InsufficientAcks({acked}/{required}) — \
                 this indicates writes are being rejected"
            );
        }
        Ok(other) => {
            panic!("[{label}] unexpected outcome: {other:?}");
        }
        Err(e) => {
            panic!("[{label}] pipeline error: {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Regression test for the raft PeerRegistry address bug.
///
/// The recovery code path in node.rs populated the PeerRegistry with
/// data-plane addresses (port 9800) instead of raft addresses (port 9810).
/// After restarting, raft RPCs went to the wrong port, breaking consensus.
///
/// This test:
/// 1. Starts a 3-node cluster, writes data
/// 2. Restarts ALL nodes (triggers raft recovery path)
/// 3. Verifies the cluster is still healthy
/// 4. Writes new data — this would fail before the fix
#[tokio::test]
async fn test_write_survives_full_cluster_restart() {
    let (mut cluster, volume_id, mgmt_url) = setup_ec_cluster().await;

    // Pre-restart write
    let nodes = fetch_nodes_from_api(&mgmt_url).await;
    let pipeline = build_pipeline(&nodes, volume_id, &mgmt_url);
    assert_write_succeeds(&pipeline, volume_id, 0..1, 0xAA, "pre-restart").await;

    eprintln!("--- Restarting all 3 nodes ---");
    cluster
        .restart_all_nodes()
        .await
        .expect("restart_all_nodes");

    cluster
        .wait_cluster_healthy(Duration::from_secs(30))
        .await
        .expect("cluster healthy after restart");

    // Rebuild pipeline with fresh node addresses
    let new_mgmt = cluster.node(0).mgmt_url();
    let new_nodes = fetch_nodes_from_api(&new_mgmt).await;
    let pipeline2 = build_pipeline(&new_nodes, volume_id, &new_mgmt);

    // Post-restart write — same block (overwrite) AND new block
    assert_write_succeeds(&pipeline2, volume_id, 0..1, 0xBB, "post-restart overwrite").await;
    assert_write_succeeds(&pipeline2, volume_id, 100..101, 0xCC, "post-restart new block").await;

    drop(cluster);
}

/// Regression test for the extent immutability violation bug.
///
/// With deterministic placement, writing to the same block twice produces
/// the same (extent_id, version=1). The old code rejected the second write
/// as an "immutability violation". The fix allows atomic overwrite.
///
/// This test writes the same block multiple times and verifies all succeed.
#[tokio::test]
async fn test_overwrite_same_block_multiple_times() {
    let (cluster, volume_id, mgmt_url) = setup_ec_cluster().await;

    let nodes = fetch_nodes_from_api(&mgmt_url).await;
    let pipeline = build_pipeline(&nodes, volume_id, &mgmt_url);

    // Write block 0 three times with different data
    for (i, byte) in [0xAA_u8, 0xBB, 0xCC].iter().enumerate() {
        assert_write_succeeds(
            &pipeline,
            volume_id,
            0..1,
            *byte,
            &format!("write-{i}"),
        )
        .await;
    }

    // Write a multi-block range twice
    assert_write_succeeds(&pipeline, volume_id, 10..14, 0xDD, "multi-block-1").await;
    assert_write_succeeds(&pipeline, volume_id, 10..14, 0xEE, "multi-block-2").await;

    drop(cluster);
}

/// Test that pre-existing (stale) extent files on disk don't block writes.
///
/// Simulates the scenario where raft was wiped but data directories were not:
/// old committed extent files remain on disk. When a new volume maps to the
/// same extent IDs (unlikely in prod but easy to trigger by reusing volume_id),
/// writes must succeed by overwriting the stale files.
#[tokio::test]
async fn test_writes_with_stale_extent_files_on_disk() {
    let binary = build_binary().await.expect("build binary");
    let base_port = unique_base_port();
    let cluster = RealProcessCluster::new(3, binary, base_port);
    cluster.start_all().await.expect("start cluster");
    cluster
        .wait_cluster_healthy(Duration::from_secs(30))
        .await
        .expect("healthy");

    let vol = cluster
        .create_volume(
            "stale-test-vol",
            1024 * 1024 * 64,
            serde_json::json!({"ErasureCoded": {"data_chunks": 2, "parity_chunks": 1}}),
        )
        .await
        .expect("create volume");
    let volume_id: VolumeId = vol["id"].as_str().unwrap().parse().unwrap();

    // Write some data so extent files exist on disk
    let mgmt_url = cluster.node(0).mgmt_url();
    let nodes = fetch_nodes_from_api(&mgmt_url).await;
    let pipeline = build_pipeline(&nodes, volume_id, &mgmt_url);
    assert_write_succeeds(&pipeline, volume_id, 0..4, 0x11, "initial-write").await;

    // Verify extent files were created
    for i in 0..3 {
        let count = cluster
            .count_committed_extents(i)
            .expect("count extents");
        eprintln!("  node {i} has {count} committed extent files after initial write");
        assert!(count > 0, "node {i} should have committed extents");
    }

    // Now overwrite the same blocks — this triggers the overwrite path
    // because the same (extent_id, version=1) already exists on disk
    assert_write_succeeds(&pipeline, volume_id, 0..4, 0x22, "overwrite-stale").await;

    drop(cluster);
}

/// Test that after restart, stale extent files don't prevent new writes.
///
/// This is the combined scenario: raft recovery path + stale files.
/// The cluster is written to, restarted, then written to again at the
/// same block offsets.
#[tokio::test]
async fn test_restart_with_stale_extents_then_overwrite() {
    let (mut cluster, volume_id, mgmt_url) = setup_ec_cluster().await;

    // Write blocks 0-3
    let nodes = fetch_nodes_from_api(&mgmt_url).await;
    let pipeline = build_pipeline(&nodes, volume_id, &mgmt_url);
    assert_write_succeeds(&pipeline, volume_id, 0..4, 0xAA, "pre-restart").await;

    // Restart all nodes (raft recovery + stale extent files on disk)
    eprintln!("--- Restarting cluster (stale extents remain on disk) ---");
    cluster
        .restart_all_nodes()
        .await
        .expect("restart");

    cluster
        .wait_cluster_healthy(Duration::from_secs(30))
        .await
        .expect("healthy after restart");

    // Overwrite the SAME blocks — hits both the raft recovery path
    // and the extent overwrite path
    let new_mgmt = cluster.node(0).mgmt_url();
    let new_nodes = fetch_nodes_from_api(&new_mgmt).await;
    let pipeline2 = build_pipeline(&new_nodes, volume_id, &new_mgmt);

    assert_write_succeeds(&pipeline2, volume_id, 0..4, 0xBB, "post-restart overwrite").await;

    // Also write new blocks to verify fresh writes work too
    assert_write_succeeds(&pipeline2, volume_id, 100..104, 0xCC, "post-restart fresh").await;

    drop(cluster);
}

/// Test pre-seeded stale extent files with known IDs.
///
/// Uses the PlacementEngine to compute what extent IDs a volume will use,
/// pre-seeds those files on disk, then verifies writes still succeed.
#[tokio::test]
async fn test_pre_seeded_stale_extents() {
    let binary = build_binary().await.expect("build binary");
    let base_port = unique_base_port();
    let cluster = RealProcessCluster::new(3, binary, base_port);
    cluster.start_all().await.expect("start cluster");
    cluster
        .wait_cluster_healthy(Duration::from_secs(30))
        .await
        .expect("healthy");

    // Create volume and compute the extent IDs that will be used for block 0
    let vol = cluster
        .create_volume(
            "preseed-vol",
            1024 * 1024 * 64,
            serde_json::json!({"ErasureCoded": {"data_chunks": 2, "parity_chunks": 1}}),
        )
        .await
        .expect("create volume");
    let volume_id: VolumeId = vol["id"].as_str().unwrap().parse().unwrap();

    // Compute what extent_id block 0 will produce
    let extent_size_blocks = 524288u64 / 4096; // extent_size / block_size = 128
    let extent_num = 0 * extent_size_blocks; // block_to_extent(0) = extent_num 0
    let extent_id = PlacementEngine::extent_id_for_extent(volume_id, extent_num);
    let extent_id_str = extent_id.to_string();

    eprintln!("  Computed extent_id for block 0: {extent_id_str}");

    // Pre-seed this exact extent file on all nodes
    let stale_data = vec![0xFF_u8; 4096];
    for i in 0..3 {
        let seeded = cluster
            .pre_seed_extent_files(i, &[&extent_id_str], 1, &stale_data)
            .expect("pre-seed");
        eprintln!("  Seeded {seeded} files on node {i}");
    }

    // Now write to block 0 — this must overwrite the stale files
    let mgmt_url = cluster.node(0).mgmt_url();
    let nodes = fetch_nodes_from_api(&mgmt_url).await;
    let pipeline = build_pipeline(&nodes, volume_id, &mgmt_url);

    assert_write_succeeds(&pipeline, volume_id, 0..1, 0x42, "overwrite-preseeded").await;

    drop(cluster);
}

/// Test rolling restart: restart nodes one at a time, writing between each.
///
/// NOTE: This test is timing-sensitive — a freshly restarted node may not
/// have its disks registered in time for the next write. Run with
/// `--ignored` to include it.
#[tokio::test]
#[ignore]
async fn test_rolling_restart_with_writes() {
    let (mut cluster, volume_id, mgmt_url) = setup_ec_cluster().await;

    let nodes = fetch_nodes_from_api(&mgmt_url).await;
    let pipeline = build_pipeline(&nodes, volume_id, &mgmt_url);
    assert_write_succeeds(&pipeline, volume_id, 0..1, 0x10, "before-rolling").await;

    for restart_idx in [2, 1, 0] {
        eprintln!("--- Rolling restart: node {restart_idx} ---");
        cluster
            .restart_node(restart_idx)
            .await
            .unwrap_or_else(|e| panic!("restart node {restart_idx}: {e}"));

        cluster
            .wait_cluster_healthy(Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("healthy after restarting node {restart_idx}: {e}"));

        // Extra settle time for the restarted node to register its disks
        // and for raft to stabilize replication to the new member.
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Rebuild pipeline (leader may have changed)
        let new_mgmt = cluster.node(0).mgmt_url();
        let new_nodes = fetch_nodes_from_api(&new_mgmt).await;
        let p = build_pipeline(&new_nodes, volume_id, &new_mgmt);

        let block = restart_idx as u64 * 10;
        assert_write_succeeds(
            &p,
            volume_id,
            block..block + 1,
            (0x20 + restart_idx) as u8,
            &format!("after-restart-{restart_idx}"),
        )
        .await;
    }

    drop(cluster);
}

/// Test that cluster health API responds on all nodes after full restart.
#[tokio::test]
async fn test_cluster_health_api_after_restart() {
    let (mut cluster, _volume_id, _mgmt_url) = setup_ec_cluster().await;

    cluster
        .restart_all_nodes()
        .await
        .expect("restart");

    cluster
        .wait_cluster_healthy(Duration::from_secs(30))
        .await
        .expect("healthy");

    // Verify status on every node
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    for i in 0..3 {
        let url = format!("{}/api/v1/cluster/status", cluster.node(i).mgmt_url());
        let resp = client.get(&url).send().await;
        match resp {
            Ok(r) if r.status().is_success() => {
                let body: serde_json::Value = r.json().await.unwrap();
                let node_count = body["node_count"].as_u64().unwrap_or(0);
                let health = body["quorum_health"].as_str().unwrap_or("unknown");
                eprintln!("  node {i}: node_count={node_count}, quorum={health}");
                assert!(
                    node_count >= 3,
                    "node {i} should see >= 3 nodes, got {node_count}"
                );
                assert_eq!(
                    health, "healthy",
                    "node {i} quorum should be healthy, got {health}"
                );
            }
            Ok(r) => panic!("node {i} returned status {}", r.status()),
            Err(e) => panic!("node {i} unreachable: {e}"),
        }
    }

    drop(cluster);
}
