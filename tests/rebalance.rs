#![allow(unused_imports, dead_code)]
mod harness;

use harness::cluster::{ClusterConfig, TestCluster};
use harness::faults::{Fault, FaultInjector};
use harness::{
    CLIENT_NODE, MOUNT_PATH, STORAGE_NODES, ensure_all_nodes_running, mount_volume, start_cluster,
    unmount_volume, verify_file, write_test_file,
};
use std::time::Duration;

fn require_vm_env() -> bool {
    std::env::var("BLOCKYARD_INTEGRATION").is_ok()
}

fn running_cluster(node_count: usize) -> TestCluster {
    TestCluster::assume_running(ClusterConfig {
        node_count,
        ..Default::default()
    })
}

// ── Test 1: Files intact during node crash ────────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn files_intact_during_node_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write several files.
    let mut checksums = Vec::new();
    for i in 0..5 {
        let path = format!("{mount_path}/rebal_{i}.bin");
        let md5 = write_test_file(&cluster, CLIENT_NODE, &path, 512).await;
        checksums.push((path, md5));
    }

    // Crash node-2.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify all files are still readable on the client.
    for (path, expected_md5) in &checksums {
        let valid = verify_file(&cluster, CLIENT_NODE, path, expected_md5).await;
        assert!(valid, "checksum mismatch for {path} during node crash");
    }

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 2: Files intact after recovery ───────────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn files_intact_after_recovery() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write files.
    let mut checksums = Vec::new();
    for i in 0..5 {
        let path = format!("{mount_path}/recovery_{i}.bin");
        let md5 = write_test_file(&cluster, CLIENT_NODE, &path, 256).await;
        checksums.push((path, md5));
    }

    // Crash node-2.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Restart the crashed node.
    ensure_all_nodes_running(&cluster).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify all files after recovery.
    for (path, expected_md5) in &checksums {
        let valid = verify_file(&cluster, CLIENT_NODE, path, expected_md5).await;
        assert!(valid, "checksum mismatch for {path} after recovery");
    }

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}
