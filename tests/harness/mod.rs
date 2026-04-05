pub mod checker;
pub mod cluster;
pub mod faults;
pub mod workload;

use cluster::TestCluster;
use std::time::Duration;

pub async fn ensure_all_nodes_running(cluster: &TestCluster) {
    for node in cluster.running_nodes() {
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            cluster.ssh_exec(
                node.id,
                "pgrep -x blockyard >/dev/null 2>&1 || RUST_LOG=info nohup /usr/local/bin/blockyard start --config /etc/blockyard.toml > /var/log/blockyard.log 2>&1 &",
            ),
        ).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
}
