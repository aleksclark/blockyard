#![allow(unused_imports, dead_code)]
mod harness;

use harness::cluster::{ClusterConfig, TestCluster};
use harness::faults::{Fault, FaultInjector};
use harness::{
    CLIENT_NODE, MOUNT_PATH, STORAGE_NODES, UBLK_DEV, ensure_all_nodes_running, mount_volume,
    read_text_file, start_cluster, unmount_volume, verify_file, write_test_file, write_text_file,
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

// ── Test 1: Basic mount, format, write, read, unmount cycle ───────────────

#[tokio::test]
#[ignore]
async fn mount_format_write_read() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write a file.
    let file_path = format!("{mount_path}/ublk_test.bin");
    let md5 = write_test_file(&cluster, CLIENT_NODE, &file_path, 1024).await;
    assert!(!md5.is_empty(), "md5 should not be empty");

    // Read it back.
    let valid = verify_file(&cluster, CLIENT_NODE, &file_path, &md5).await;
    assert!(valid, "file checksum mismatch after write/read cycle");

    // Check filesystem reports correct usage.
    let df_output = cluster
        .ssh_exec(CLIENT_NODE, &format!("df -h {mount_path}"))
        .await
        .unwrap();
    assert!(
        df_output.contains(MOUNT_PATH),
        "df should show the mount point"
    );

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 2: Remount data persists ─────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn remount_data_persists() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write files.
    let mut checksums = Vec::new();
    for i in 0..3 {
        let path = format!("{mount_path}/persist_{i}.bin");
        let md5 = write_test_file(&cluster, CLIENT_NODE, &path, 512).await;
        checksums.push((format!("{MOUNT_PATH}/persist_{i}.bin"), md5));
    }

    // Sync and unmount.
    let _ = cluster.ssh_exec(CLIENT_NODE, "sync").await;
    unmount_volume(&cluster, CLIENT_NODE).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Remount without reformatting.
    let _ = cluster
        .ssh_exec(CLIENT_NODE, "modprobe ublk_drv || true")
        .await;
    let _ = cluster
        .ssh_exec(
            CLIENT_NODE,
            "nohup blockyard mount test-vol --backend ublk > /var/log/blockyard-mount.log 2>&1 &",
        )
        .await;

    // Wait for block device.
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Ok(out) = cluster
            .ssh_exec(CLIENT_NODE, &format!("test -b {UBLK_DEV} && echo ok"))
            .await
        {
            if out.trim() == "ok" {
                break;
            }
        }
    }

    // Mount without mkfs (data should persist).
    let _ = cluster
        .ssh_exec(
            CLIENT_NODE,
            &format!("mkdir -p {MOUNT_PATH} && mount {UBLK_DEV} {MOUNT_PATH}"),
        )
        .await;

    // Verify all files.
    for (path, expected_md5) in &checksums {
        let valid = verify_file(&cluster, CLIENT_NODE, path, expected_md5).await;
        assert!(valid, "checksum mismatch for {path} after remount");
    }

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 3: Mount survives storage node crash ─────────────────────────────

#[tokio::test]
#[ignore]
async fn mount_survives_storage_node_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(4);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, STORAGE_NODES).await;

    let mount_path = mount_volume(&cluster, CLIENT_NODE, "test-vol").await;

    // Write a file.
    let file_path = format!("{mount_path}/survive.bin");
    let md5 = write_test_file(&cluster, CLIENT_NODE, &file_path, 1024).await;

    // Crash a storage node.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 1 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify the file is still readable.
    let valid = verify_file(&cluster, CLIENT_NODE, &file_path, &md5).await;
    assert!(valid, "file checksum mismatch after storage node crash");

    // Can still write new data.
    let new_path = format!("{mount_path}/after_crash.bin");
    let new_md5 = write_test_file(&cluster, CLIENT_NODE, &new_path, 256).await;
    assert!(
        verify_file(&cluster, CLIENT_NODE, &new_path, &new_md5).await,
        "new file checksum mismatch after storage node crash"
    );

    unmount_volume(&cluster, CLIENT_NODE).await;
    ensure_all_nodes_running(&cluster).await;
}
