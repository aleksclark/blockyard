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

// ── Test 1: Write/read data matches exactly ───────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn write_read_data_matches() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write known-content text files.
    let test_cases = vec![
        (format!("{mount_path}/hello.txt"), "hello blockyard"),
        (format!("{mount_path}/numbers.txt"), "1234567890"),
        (format!("{mount_path}/multiline.txt"), "line1\nline2\nline3"),
    ];

    for (path, content) in &test_cases {
        write_text_file(&cluster, CLIENT_NODE, path, content).await;
    }

    // Sync to ensure data is flushed.
    let _ = cluster.ssh_exec(CLIENT_NODE, "sync").await;

    // Read back and verify content matches.
    for (path, expected) in &test_cases {
        let actual = read_text_file(&cluster, CLIENT_NODE, path).await;
        assert_eq!(actual.trim(), *expected, "content mismatch for {path}");
    }

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 2: Crash all, restart, data survives ─────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn crash_all_restart_data_survives() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write test files.
    let mut checksums = Vec::new();
    for i in 0..5 {
        let path = format!("{mount_path}/survive_{i}.bin");
        let md5 = write_test_file(&cluster, CLIENT_NODE, &path, 256).await;
        checksums.push((path, md5));
    }

    // Sync, then unmount before crashing.
    let _ = cluster.ssh_exec(CLIENT_NODE, "sync").await;
    unmount_volume(&cluster, CLIENT_NODE).await;

    // Crash all 3 storage nodes.
    let injector = FaultInjector::new(&cluster);
    for &node_id in STORAGE_NODES {
        injector
            .inject(&Fault::NodeCrash { node_id })
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Restart all storage nodes.
    ensure_all_nodes_running(&cluster).await;
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Remount the volume.
    // Note: we skip mkfs here — the data should persist on the block device.
    // Re-mount without reformatting.
    let _ = cluster
        .ssh_exec(CLIENT_NODE, "modprobe ublk_drv || true")
        .await;
    let _ = cluster
        .ssh_exec(
            CLIENT_NODE,
            "nohup blockyard mount test-vol --backend ublk > /var/log/blockyard-mount.log 2>&1 &",
        )
        .await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let _ = cluster
        .ssh_exec(
            CLIENT_NODE,
            &format!("mkdir -p {MOUNT_PATH} && mount {UBLK_DEV} {MOUNT_PATH}"),
        )
        .await;

    // Verify all files.
    for (path, expected_md5) in &checksums {
        let valid = verify_file(&cluster, CLIENT_NODE, path, expected_md5).await;
        assert!(valid, "checksum mismatch for {path} after full restart");
    }

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

use harness::UBLK_DEV;

// ── Test 3: Filesystem sync survives crash ────────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn filesystem_sync_survives_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write and explicitly sync.
    let file_path = format!("{mount_path}/synced.bin");
    let md5 = write_test_file(&cluster, CLIENT_NODE, &file_path, 1024).await;
    let _ = cluster.ssh_exec(CLIENT_NODE, "sync").await;

    // Crash one storage node.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Restart the crashed node.
    ensure_all_nodes_running(&cluster).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify the synced file is still intact.
    let valid = verify_file(&cluster, CLIENT_NODE, &file_path, &md5).await;
    assert!(valid, "synced file checksum mismatch after crash/restart");

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 4: Many small files integrity ────────────────────────────────────

#[tokio::test]
// Requires BLOCKYARD_INTEGRATION=1 and running QEMU VM cluster
#[ignore]
async fn many_small_files_integrity() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Create a subdirectory to hold many files.
    let _ = cluster
        .ssh_exec(CLIENT_NODE, &format!("mkdir -p {mount_path}/small"))
        .await;

    // Write 1000 small files (1KB each) using a batch command for speed.
    let batch_cmd = format!(
        "for i in $(seq 0 999); do dd if=/dev/urandom of={mount_path}/small/f_$i bs=1K count=1 2>/dev/null; done && sync"
    );
    let _ = cluster.ssh_exec(CLIENT_NODE, &batch_cmd).await;

    // Capture md5 of all files.
    let md5_output = cluster
        .ssh_exec(
            CLIENT_NODE,
            &format!("md5sum {mount_path}/small/f_* | sort"),
        )
        .await
        .unwrap();

    // Crash a storage node.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify all 1000 files' checksums.
    let md5_after = cluster
        .ssh_exec(
            CLIENT_NODE,
            &format!("md5sum {mount_path}/small/f_* | sort"),
        )
        .await
        .unwrap();

    assert_eq!(
        md5_output.trim(),
        md5_after.trim(),
        "small files checksums changed after crash"
    );

    // Verify count.
    let count_output = cluster
        .ssh_exec(CLIENT_NODE, &format!("ls {mount_path}/small/ | wc -l"))
        .await
        .unwrap();
    let count: u32 = count_output.trim().parse().unwrap_or(0);
    assert_eq!(count, 1000, "expected 1000 files, found {count}");

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}
