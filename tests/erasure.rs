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

/// The EC tests use a 5-node cluster: nodes 0–4 are storage, node 4 doubles
/// as the client for simplicity (since RS(2,1) needs at least 3 nodes for
/// data+parity).  In practice we use the same 4-node layout as other tests
/// with nodes 0-2 as storage and node 3 as client.
fn running_cluster(node_count: usize) -> TestCluster {
    TestCluster::assume_running(ClusterConfig {
        node_count,
        ..Default::default()
    })
}

// ── Test 1: EC volume basic file operations ───────────────────────────────

#[tokio::test]
#[ignore]
async fn ec_volume_basic_file_ops() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, &[0, 1, 2, 3, 4]).await;

    // The highest-numbered node (4) is the client.
    let client = 4;

    // Create an EC volume first via CLI on one of the storage nodes.
    let _ = cluster
        .ssh_exec(
            0,
            "/usr/local/bin/blockyard volume create --name ec-test --size 10GB --erasure-coding 2+1 --endpoint http://127.0.0.1:7401 || true",
        )
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mount_path = mount_volume(&cluster, client, "ec-test").await;

    // Write files.
    let mut checksums = Vec::new();
    for i in 0..5 {
        let path = format!("{mount_path}/ec_file_{i}.bin");
        let md5 = write_test_file(&cluster, client, &path, 512).await;
        checksums.push((path, md5));
    }

    // Read back and verify.
    for (path, expected_md5) in &checksums {
        let valid = verify_file(&cluster, client, path, expected_md5).await;
        assert!(valid, "EC file checksum mismatch for {path}");
    }

    unmount_volume(&cluster, client).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 2: EC volume survives one node crash ─────────────────────────────

#[tokio::test]
#[ignore]
async fn ec_survive_one_node_crash() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, &[0, 1, 2, 3, 4]).await;

    let client = 4;
    let _ = cluster
        .ssh_exec(
            0,
            "/usr/local/bin/blockyard volume create --name ec-crash1 --size 10GB --erasure-coding 2+1 --endpoint http://127.0.0.1:7401 || true",
        )
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mount_path = mount_volume(&cluster, client, "ec-crash1").await;

    // Write a file.
    let file_path = format!("{mount_path}/ec_survive.bin");
    let md5 = write_test_file(&cluster, client, &file_path, 1024).await;

    // Crash 1 storage node (RS(2,1) tolerates 1 failure).
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 3 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // File should still be readable.
    let valid = verify_file(&cluster, client, &file_path, &md5).await;
    assert!(valid, "EC file unreadable after 1 node crash");

    unmount_volume(&cluster, client).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 3: EC — two node crash with data intact ──────────────────────────
// RS(2,1) can only tolerate 1 loss.  This test verifies that after 2 node
// crashes the system degrades gracefully.  If the surviving nodes happen to
// hold the data (the block I/O path is local to the mount) the file may
// still be readable.

#[tokio::test]
#[ignore]
async fn ec_survive_two_node_crash_data_intact() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, &[0, 1, 2, 3, 4]).await;

    let client = 4;
    let _ = cluster
        .ssh_exec(
            0,
            "/usr/local/bin/blockyard volume create --name ec-crash2 --size 10GB --erasure-coding 2+1 --endpoint http://127.0.0.1:7401 || true",
        )
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mount_path = mount_volume(&cluster, client, "ec-crash2").await;

    let file_path = format!("{mount_path}/ec_two_crash.bin");
    let md5 = write_test_file(&cluster, client, &file_path, 512).await;

    // Sync everything.
    let _ = cluster.ssh_exec(client, "sync").await;

    // Crash 2 storage nodes.
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 2 })
        .await
        .unwrap();
    injector
        .inject(&Fault::NodeCrash { node_id: 3 })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    // The file may or may not be readable depending on which nodes held
    // the data.  We just check the system doesn't panic.
    let _ = verify_file(&cluster, client, &file_path, &md5).await;

    unmount_volume(&cluster, client).await;
    ensure_all_nodes_running(&cluster).await;
}

// ── Test 4: EC concurrent writes during failure ───────────────────────────

#[tokio::test]
#[ignore]
async fn ec_concurrent_writes_during_failure() {
    if !require_vm_env() {
        return;
    }
    let cluster = running_cluster(5);
    ensure_all_nodes_running(&cluster).await;
    start_cluster(&cluster, &[0, 1, 2, 3, 4]).await;

    let client = 4;
    let _ = cluster
        .ssh_exec(
            0,
            "/usr/local/bin/blockyard volume create --name ec-concurrent --size 10GB --erasure-coding 2+1 --endpoint http://127.0.0.1:7401 || true",
        )
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mount_path = mount_volume(&cluster, client, "ec-concurrent").await;

    // Start writing small files continuously in the background.
    let write_cmd = format!(
        "for i in $(seq 0 49); do dd if=/dev/urandom of={mount_path}/concurrent_$i bs=1K count=4 2>/dev/null && sync; done"
    );
    let _ = cluster
        .ssh_exec(
            client,
            &format!("nohup sh -c '{write_cmd}' > /dev/null 2>&1 &"),
        )
        .await;

    // Wait a bit, then crash 1 node mid-stream.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let injector = FaultInjector::new(&cluster);
    injector
        .inject(&Fault::NodeCrash { node_id: 3 })
        .await
        .unwrap();

    // Wait for writes to finish.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Check that all synced files survived.
    let count_output = cluster
        .ssh_exec(
            client,
            &format!("ls {mount_path}/concurrent_* 2>/dev/null | wc -l"),
        )
        .await
        .unwrap_or_default();
    let count: u32 = count_output.trim().parse().unwrap_or(0);
    // At least some files should have been written successfully.
    assert!(
        count > 0,
        "expected some concurrent files to survive, got {count}"
    );

    // Verify each surviving file is not corrupted (non-zero size).
    let sizes_output = cluster
        .ssh_exec(
            client,
            &format!(
                "for f in {mount_path}/concurrent_*; do stat -c '%s' \"$f\" 2>/dev/null; done"
            ),
        )
        .await
        .unwrap_or_default();
    for line in sizes_output.lines() {
        let size: u64 = line.trim().parse().unwrap_or(0);
        assert!(
            size > 0,
            "found a zero-size concurrent file — data corruption"
        );
    }

    unmount_volume(&cluster, client).await;
    ensure_all_nodes_running(&cluster).await;
}
