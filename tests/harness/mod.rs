pub mod checker;
pub mod cluster;
pub mod faults;
pub mod workload;

use cluster::TestCluster;
use std::time::Duration;

/// The mount point used on the client VM for UBLK-backed volumes.
pub const MOUNT_PATH: &str = "/mnt/blockyard";

/// The UBLK block device path created by `blockyard mount`.
pub const UBLK_DEV: &str = "/dev/ublkb0";

/// Default storage node indices (the first 3 nodes in the cluster).
pub const STORAGE_NODES: &[usize] = &[0, 1, 2];

/// Default client node index (the 4th node).
pub const CLIENT_NODE: usize = 3;

/// Ensure all storage nodes have blockyard running.  Starts the process on
/// any node where it isn't already running.
pub async fn ensure_all_nodes_running(cluster: &TestCluster) {
    for node in cluster.running_nodes() {
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            cluster.ssh_exec(
                node.id,
                "pgrep -x blockyard >/dev/null 2>&1 || RUST_LOG=info nohup /usr/local/bin/blockyard start --config /etc/blockyard/config.toml > /var/log/blockyard.log 2>&1 &",
            ),
        ).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
}

/// Start the blockyard storage cluster on the given nodes.
pub async fn start_cluster(cluster: &TestCluster, storage_nodes: &[usize]) {
    for &node_id in storage_nodes {
        cluster.start_blockyard(node_id).await.unwrap_or_else(|e| {
            panic!("failed to start blockyard on node {node_id}: {e}");
        });
    }
    // Give the cluster time to elect a leader and converge.
    tokio::time::sleep(Duration::from_secs(3)).await;
}

/// Mount a blockyard volume on `client_node` using the UBLK backend.
///
/// Steps:
/// 1. `modprobe ublk_drv`
/// 2. `blockyard mount <vol_name> --backend ublk &` (background)
/// 3. Wait for the block device to appear
/// 4. `mkfs.ext4 -F /dev/ublkb0`
/// 5. `mount /dev/ublkb0 /mnt/blockyard`
///
/// Returns the mount path.
pub async fn mount_volume(cluster: &TestCluster, client_node: usize, vol_name: &str) -> String {
    // Load the ublk kernel module (idempotent).
    let _ = cluster
        .ssh_exec(client_node, "modprobe ublk_drv || true")
        .await;

    // Start blockyard mount in the background.
    let mount_cmd = format!(
        "nohup /usr/local/bin/blockyard mount {vol_name} --backend ublk > /var/log/blockyard-mount.log 2>&1 &"
    );
    cluster
        .ssh_exec(client_node, &mount_cmd)
        .await
        .unwrap_or_else(|e| panic!("failed to start blockyard mount: {e}"));

    // Wait for the block device to appear (up to 10 seconds).
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Ok(out) = cluster
            .ssh_exec(client_node, &format!("test -b {UBLK_DEV} && echo ok"))
            .await
        {
            if out.trim() == "ok" {
                break;
            }
        }
    }

    // Format and mount.
    cluster
        .ssh_exec(client_node, &format!("mkfs.ext4 -F {UBLK_DEV}"))
        .await
        .unwrap_or_else(|e| panic!("mkfs.ext4 failed: {e}"));

    cluster
        .ssh_exec(
            client_node,
            &format!("mkdir -p {MOUNT_PATH} && mount {UBLK_DEV} {MOUNT_PATH}"),
        )
        .await
        .unwrap_or_else(|e| panic!("mount failed: {e}"));

    MOUNT_PATH.to_string()
}

/// Unmount the volume and kill the blockyard mount process on `client_node`.
pub async fn unmount_volume(cluster: &TestCluster, client_node: usize) {
    let _ = cluster
        .ssh_exec(
            client_node,
            &format!("sync && umount {MOUNT_PATH} 2>/dev/null || true"),
        )
        .await;
    let _ = cluster
        .ssh_exec(
            client_node,
            "pkill -9 -f 'blockyard mount' 2>/dev/null || true",
        )
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// Write a random test file of `size_kb` KB to `path` on `node` and return its md5 hash.
pub async fn write_test_file(
    cluster: &TestCluster,
    node: usize,
    path: &str,
    size_kb: u32,
) -> String {
    let cmd = format!(
        "dd if=/dev/urandom of={path} bs=1K count={size_kb} 2>/dev/null && sync && md5sum {path} | awk '{{print $1}}'"
    );
    let output = cluster
        .ssh_exec(node, &cmd)
        .await
        .unwrap_or_else(|e| panic!("write_test_file({path}) failed: {e}"));
    output.trim().to_string()
}

/// Verify that the file at `path` on `node` has the expected md5 hash.
pub async fn verify_file(
    cluster: &TestCluster,
    node: usize,
    path: &str,
    expected_md5: &str,
) -> bool {
    let cmd = format!("md5sum {path} | awk '{{print $1}}'");
    match cluster.ssh_exec(node, &cmd).await {
        Ok(output) => output.trim() == expected_md5,
        Err(_) => false,
    }
}

/// Write a text file with the given content to `path` on `node`.
pub async fn write_text_file(cluster: &TestCluster, node: usize, path: &str, content: &str) {
    let cmd = format!("printf '%s' '{content}' > {path} && sync");
    cluster
        .ssh_exec(node, &cmd)
        .await
        .unwrap_or_else(|e| panic!("write_text_file({path}) failed: {e}"));
}

/// Read a text file from `path` on `node` and return its content.
pub async fn read_text_file(cluster: &TestCluster, node: usize, path: &str) -> String {
    let cmd = format!("cat {path}");
    cluster
        .ssh_exec(node, &cmd)
        .await
        .unwrap_or_else(|e| panic!("read_text_file({path}) failed: {e}"))
}
