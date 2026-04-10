//! Full-stack UBLK filesystem tests with real blockyard processes.
//!
//! These tests require root (or CAP_SYS_ADMIN) for ublk device creation
//! and are marked #[ignore] for normal test runs.

use std::time::Duration;

use blockyard_test_harness::process_harness::{
    RealProcessCluster, build_binary, unique_base_port,
};

#[tokio::test]
#[ignore = "requires root and ublk kernel module"]
async fn test_mount_format_write_read() {
    let binary = build_binary().await.expect("build binary");
    let base_port = unique_base_port();
    let cluster = RealProcessCluster::new(3, binary, base_port);
    cluster.start_all().await.expect("start cluster");
    cluster
        .wait_cluster_healthy(Duration::from_secs(30))
        .await
        .expect("cluster healthy");

    let vol = cluster
        .create_volume(
            "ublk-mount-test",
            1024 * 1024 * 256,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let _vol_id = vol["id"].as_str().unwrap().to_string();

    // TODO(ublk): When UblkDevice::start() is implemented:
    // 1. Create ClusterBlockHandler with real TCP/HTTP clients
    // 2. Create UblkDevice, start it → get /dev/ublkbN
    // 3. mkfs.ext4 /dev/ublkbN
    // 4. mount /dev/ublkbN /tmp/mnt
    // 5. Write a test file, compute sha256
    // 6. Unmount, remount
    // 7. Verify sha256 matches
    //
    // For now, verify cluster is up and volume was created
    let status = cluster.cluster_status().await.expect("status");
    assert_eq!(status["quorum_health"].as_str(), Some("healthy"));
}

#[tokio::test]
#[ignore = "requires root and ublk kernel module"]
async fn test_mount_node_failure_fs_survives() {
    let binary = build_binary().await.expect("build binary");
    let base_port = unique_base_port();
    let cluster = RealProcessCluster::new(3, binary, base_port);
    cluster.start_all().await.expect("start cluster");
    cluster
        .wait_cluster_healthy(Duration::from_secs(30))
        .await
        .expect("cluster healthy");

    let vol = cluster
        .create_volume(
            "ublk-fault-test",
            1024 * 1024 * 256,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let _vol_id = vol["id"].as_str().unwrap().to_string();

    // TODO(ublk): When UblkDevice::start() is implemented:
    // 1. Mount ext4 on ublk device
    // 2. Start writing files
    // 3. Kill one of 3 nodes
    // 4. Continue writing files — verify no IO errors
    // 5. Unmount, remount, fsck
    //
    // For now, verify we can kill a node and remaining cluster is healthy
    cluster.kill_node(2).expect("kill node 2");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let mut any_alive = false;
    for i in 0..2 {
        let url = format!("{}/api/v1/cluster/status", cluster.node(i).mgmt_url());
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().is_success() {
                any_alive = true;
                break;
            }
        }
    }
    assert!(any_alive, "surviving nodes should still respond");
}
