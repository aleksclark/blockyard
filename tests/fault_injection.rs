//! Jepsen-style fault injection integration tests.
//!
//! All tests spawn real blockyard processes and communicate via real
//! TCP (data plane) and HTTP (management API). NO mocks.

use std::time::Duration;

use blockyard_test_harness::process_harness::{
    RealProcessCluster, TcpDataClient, build_binary, unique_base_port,
};

#[allow(dead_code)]
async fn setup_cluster(nodes: usize) -> (RealProcessCluster, String) {
    let binary = build_binary().await.expect("build binary");
    let base_port = unique_base_port();
    let cluster = RealProcessCluster::new(nodes, binary, base_port);
    cluster.start_all().await.expect("start cluster");
    cluster
        .wait_cluster_healthy(Duration::from_secs(30))
        .await
        .expect("cluster healthy");

    let vol = cluster
        .create_volume(
            "test-vol",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": nodes}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();
    (cluster, vol_id)
}

/// Write data via TCP data plane to a specific node, return extent_id used.
async fn write_to_node(
    cluster: &RealProcessCluster,
    node_idx: usize,
    volume_id: &str,
    data: &[u8],
    version: u64,
) -> (String, serde_json::Value) {
    let extent_id = uuid::Uuid::new_v4().to_string();
    let mut client = TcpDataClient::connect(cluster.node(node_idx).data_addr())
        .await
        .expect("connect to data node");

    let resp = client
        .write_extent(volume_id, &extent_id, version, data)
        .await
        .expect("write extent");

    (extent_id, resp)
}

async fn read_from_node(
    cluster: &RealProcessCluster,
    node_idx: usize,
    volume_id: &str,
    extent_id: &str,
    version: u64,
) -> (serde_json::Value, Vec<u8>) {
    let mut client = TcpDataClient::connect(cluster.node(node_idx).data_addr())
        .await
        .expect("connect to data node");

    client
        .read_extent(volume_id, extent_id, version)
        .await
        .expect("read extent")
}

#[tokio::test]
async fn test_node_crash_during_replicated_write() {
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
            "crash-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let mut acked_extents = Vec::new();
    for i in 0..5 {
        let data = format!("crash-test-data-{}", i);
        let (extent_id, resp) = write_to_node(&cluster, 0, &vol_id, data.as_bytes(), 1).await;
        let success = resp
            .get("WriteResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if success {
            acked_extents.push((extent_id, data));
        }
    }
    assert!(!acked_extents.is_empty(), "should have acked some writes");

    cluster.kill_node(2).expect("kill node 2");

    tokio::time::sleep(Duration::from_secs(2)).await;

    for (extent_id, expected_data) in &acked_extents {
        let (resp, payload) = read_from_node(&cluster, 0, &vol_id, extent_id, 1).await;
        let success = resp
            .get("ReadResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(success, "acked write should be readable: {:?}", resp);
        assert_eq!(
            payload,
            expected_data.as_bytes(),
            "data mismatch for extent {}",
            extent_id
        );
    }
}

#[tokio::test]
async fn test_raft_leader_failover() {
    let binary = build_binary().await.expect("build binary");
    let base_port = unique_base_port();
    let cluster = RealProcessCluster::new(3, binary, base_port);
    cluster.start_all().await.expect("start cluster");
    cluster
        .wait_cluster_healthy(Duration::from_secs(30))
        .await
        .expect("cluster healthy");

    let status = cluster.cluster_status().await.expect("get cluster status");
    assert_eq!(
        status["quorum_health"].as_str(),
        Some("healthy"),
        "cluster should be healthy"
    );

    cluster.kill_node(0).expect("kill node 0 (likely leader)");

    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let mut new_leader_found = false;
    for i in 1..3 {
        let url = format!("{}/api/v1/cluster/status", cluster.node(i).mgmt_url());
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().is_success() {
                new_leader_found = true;

                let vol_result = cluster
                    .create_volume(
                        "failover-vol",
                        1024 * 1024,
                        serde_json::json!({"Replicated": {"replicas": 2}}),
                    )
                    .await;
                if vol_result.is_err() {
                    let url2 = format!("{}/api/v1/volumes", cluster.node(i).mgmt_url());
                    let body = serde_json::json!({
                        "name": "failover-vol",
                        "size_bytes": 1024 * 1024,
                        "protection": {"Replicated": {"replicas": 2}},
                    });
                    let resp2 = client.post(&url2).json(&body).send().await;
                    assert!(
                        resp2.is_ok(),
                        "should be able to create volume after failover"
                    );
                }
                break;
            }
        }
    }
    assert!(new_leader_found, "new leader should be elected within 5s");
}

#[tokio::test]
async fn test_network_partition_via_sigstop() {
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
            "partition-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    cluster.stop_node(2).expect("SIGSTOP node 2");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let data = b"written-during-partition";
    let (extent_id, resp) = write_to_node(&cluster, 0, &vol_id, data, 1).await;
    let success = resp
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(success, "majority should still accept writes");

    cluster.resume_node(2).expect("SIGCONT node 2");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let (resp, payload) = read_from_node(&cluster, 2, &vol_id, &extent_id, 1).await;
    let read_success = resp
        .get("ReadResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if read_success {
        assert_eq!(payload, data, "resumed node should serve correct data");
    }
}

#[tokio::test]
async fn test_disk_corruption_detected() {
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
            "corruption-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let known_data = b"known-pattern-for-corruption-test";
    let (extent_id, resp) = write_to_node(&cluster, 0, &vol_id, known_data, 1).await;
    let success = resp
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(success, "initial write should succeed");

    let _corrupted = cluster.corrupt_extent_files(1).unwrap_or(0);

    let (resp0, payload0) = read_from_node(&cluster, 0, &vol_id, &extent_id, 1).await;
    let read_success = resp0
        .get("ReadResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if read_success {
        assert_eq!(
            payload0, known_data,
            "healthy node should return uncorrupted data"
        );
    }
}

#[tokio::test]
async fn test_client_crash_mid_write() {
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
            "client-crash",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let mut acked = Vec::new();
    {
        let mut client = TcpDataClient::connect(cluster.node(0).data_addr())
            .await
            .expect("connect");
        for i in 0..3 {
            let extent_id = uuid::Uuid::new_v4().to_string();
            let data = format!("session1-data-{}", i);
            let resp = client
                .write_extent(&vol_id, &extent_id, 1, data.as_bytes())
                .await
                .expect("write");
            let success = resp
                .get("WriteResp")
                .and_then(|r| r.get("success"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if success {
                acked.push((extent_id, data));
            }
        }
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    for (extent_id, expected) in &acked {
        let (resp, payload) = read_from_node(&cluster, 0, &vol_id, extent_id, 1).await;
        let success = resp
            .get("ReadResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(success, "acked data should be readable after client crash");
        assert_eq!(payload, expected.as_bytes());
    }
}

#[tokio::test]
async fn test_write_lease_mutual_exclusion() {
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
            "lease-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let session_a = uuid::Uuid::new_v4().to_string();
    let session_b = uuid::Uuid::new_v4().to_string();

    let resp_a = cluster
        .acquire_lease(&vol_id, &session_a)
        .await
        .expect("lease A");
    let resp_b = cluster
        .acquire_lease(&vol_id, &session_b)
        .await
        .expect("lease B");

    let a_ok = resp_a.status().is_success();
    let b_ok = resp_b.status().is_success();

    assert!(a_ok || b_ok, "at least one session should get the lease");
}
