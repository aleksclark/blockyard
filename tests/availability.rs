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

// ── Test 1: Filesystem available after one node crash ─────────────────────

#[tokio::test]
#[ignore]
async fn filesystem_available_after_one_node_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write a file before the crash.
    let pre_path = format!("{mount_path}/pre_crash.bin");
    let pre_md5 = write_test_file(&cluster, CLIENT_NODE, &pre_path, 512).await;

    // Crash one storage node.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Write another file *after* the crash — filesystem should still work.
    let post_path = format!("{mount_path}/post_crash.bin");
    let post_md5 = write_test_file(&cluster, CLIENT_NODE, &post_path, 256).await;

    // Both files should be readable.
    assert!(
        verify_file(&cluster, CLIENT_NODE, &pre_path, &pre_md5).await,
        "pre-crash file checksum mismatch"
    );
    assert!(
        verify_file(&cluster, CLIENT_NODE, &post_path, &post_md5).await,
        "post-crash file checksum mismatch"
    );

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 2: Leader failover with no data loss ─────────────────────────────

#[tokio::test]
#[ignore]
async fn leader_failover_no_data_loss() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write data before failover.
    let file_path = format!("{mount_path}/before_failover.bin");
    let md5 = write_test_file(&cluster, CLIENT_NODE, &file_path, 1024).await;

    // Crash the leader (node 0).
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 0 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify existing file.
    assert!(
        verify_file(&cluster, CLIENT_NODE, &file_path, &md5).await,
        "pre-failover file checksum mismatch"
    );

    // Write new data after failover.
    let new_path = format!("{mount_path}/after_failover.bin");
    let new_md5 = write_test_file(&cluster, CLIENT_NODE, &new_path, 512).await;
    assert!(
        verify_file(&cluster, CLIENT_NODE, &new_path, &new_md5).await,
        "post-failover file checksum mismatch"
    );

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 3: Node pause and resume — files intact ──────────────────────────

#[tokio::test]
#[ignore]
async fn node_pause_resume_files_intact() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write a file.
    let file_path = format!("{mount_path}/pausetest.bin");
    let md5 = write_test_file(&cluster, CLIENT_NODE, &file_path, 256).await;

    // Pause a storage node.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodePause { node_id: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Resume the node.
    injector
        .inject(&Fault::NodeResume { node_id: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify the file is intact.
    assert!(
        verify_file(&cluster, CLIENT_NODE, &file_path, &md5).await,
        "file checksum mismatch after pause/resume"
    );

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 4: Network partition heal — files intact ─────────────────────────

#[tokio::test]
#[ignore]
async fn partition_heal_files_intact() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write a file.
    let file_path = format!("{mount_path}/partition_test.bin");
    let md5 = write_test_file(&cluster, CLIENT_NODE, &file_path, 512).await;

    // Partition two storage nodes from each other.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NetworkPartition { from: 0, to: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Heal the partition.
    injector
        .inject(&Fault::NetworkHeal { from: 0, to: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify the file is intact.
    assert!(
        verify_file(&cluster, CLIENT_NODE, &file_path, &md5).await,
        "file checksum mismatch after partition heal"
    );

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}
