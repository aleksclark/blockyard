#![allow(unused_imports, dead_code)]
mod harness;

use harness::cluster::{ClusterConfig, TestCluster};
use harness::faults::{Fault, FaultInjector};
use harness::{
    CLIENT_NODE, MOUNT_PATH, STORAGE_NODES, ensure_all_nodes_running, mount_volume, read_text_file,
    start_cluster, unmount_volume, verify_file, write_test_file, write_text_file,
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

// ── Test 1: Single file survives leader crash ─────────────────────────────

#[tokio::test]
#[ignore]
async fn write_file_survives_leader_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write a 1MB file and capture its md5.
    let file_path = format!("{mount_path}/testfile.bin");
    let md5 = write_test_file(&cluster, CLIENT_NODE, &file_path, 1024).await;
    assert!(!md5.is_empty(), "md5 should not be empty");

    // Crash the leader (node 0).
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify the file is still readable and checksum matches.
    let valid = verify_file(&cluster, CLIENT_NODE, &file_path, &md5).await;
    assert!(valid, "file checksum mismatch after leader crash");

    // Cleanup.
    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 2: Multiple files survive crash ──────────────────────────────────

#[tokio::test]
#[ignore]
async fn multiple_files_survive_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write 10 files and capture their checksums.
    let mut checksums = Vec::new();
    for i in 0..10 {
        let path = format!("{mount_path}/file_{i}.bin");
        let md5 = write_test_file(&cluster, CLIENT_NODE, &path, 128).await;
        checksums.push((path, md5));
    }

    // Crash node-1.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify all 10 files.
    for (path, expected_md5) in &checksums {
        let valid = verify_file(&cluster, CLIENT_NODE, path, expected_md5).await;
        assert!(valid, "checksum mismatch for {path} after crash");
    }

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 3: Large file integrity after crash ──────────────────────────────

#[tokio::test]
#[ignore]
async fn large_file_integrity_after_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write a 100MB file.
    let file_path = format!("{mount_path}/bigfile.bin");
    let md5 = write_test_file(&cluster, CLIENT_NODE, &file_path, 100 * 1024).await;
    assert!(!md5.is_empty(), "md5 should not be empty");

    // Crash node-2.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify the large file's checksum.
    let valid = verify_file(&cluster, CLIENT_NODE, &file_path, &md5).await;
    assert!(valid, "large file checksum mismatch after crash");

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}
