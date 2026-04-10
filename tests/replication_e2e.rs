//! Replication consistency end-to-end tests with real blockyard processes.
//!
//! Tests byte-identical replicas, read-after-write consistency,
//! and node rejoin data convergence.

use std::time::Duration;

use blockyard_test_harness::process_harness::{
    RealProcessCluster, TcpDataClient, build_binary, unique_base_port,
};

#[tokio::test]
async fn test_replicas_byte_identical() {
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
            "replica-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let data = b"replicated-data-that-must-be-identical-on-all-nodes";
    let extent_id = uuid::Uuid::new_v4().to_string();

    let mut writer = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect writer");
    let resp = writer
        .write_extent(&vol_id, &extent_id, 1, data)
        .await
        .expect("write");
    let success = resp
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(success, "replicated write should succeed");

    let mut payloads = Vec::new();
    for i in 0..3 {
        let mut reader = TcpDataClient::connect(cluster.node(i).data_addr())
            .await
            .expect("connect reader");
        let (resp, payload) = reader
            .read_extent(&vol_id, &extent_id, 1)
            .await
            .expect("read");
        let read_ok = resp
            .get("ReadResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if read_ok {
            payloads.push((i, payload));
        }
    }

    if payloads.len() >= 2 {
        let (_, ref first) = payloads[0];
        for (node_idx, payload) in &payloads[1..] {
            assert_eq!(
                payload, first,
                "node {} has different data than node {}",
                node_idx, payloads[0].0
            );
        }
        assert_eq!(first.as_slice(), data, "data should match what was written");
    }
}

#[tokio::test]
async fn test_read_after_write_consistency() {
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
            "raw-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    for i in 0..10 {
        let data = format!("raw-data-{:04}", i);
        let extent_id = uuid::Uuid::new_v4().to_string();

        let mut writer = TcpDataClient::connect(cluster.node(0).data_addr())
            .await
            .expect("connect writer");
        let write_resp = writer
            .write_extent(&vol_id, &extent_id, 1, data.as_bytes())
            .await
            .expect("write");
        let w_success = write_resp
            .get("WriteResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !w_success {
            continue;
        }

        let mut reader = TcpDataClient::connect(cluster.node(0).data_addr())
            .await
            .expect("connect reader");
        let (read_resp, payload) = reader
            .read_extent(&vol_id, &extent_id, 1)
            .await
            .expect("read");
        let r_success = read_resp
            .get("ReadResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(r_success, "read-after-write should succeed for extent {}", i);
        assert_eq!(
            payload,
            data.as_bytes(),
            "read-after-write data mismatch for extent {}",
            i
        );
    }
}

#[tokio::test]
async fn test_node_rejoin_data_consistent() {
    let binary = build_binary().await.expect("build binary");
    let base_port = unique_base_port();
    let mut cluster = RealProcessCluster::new(3, binary, base_port);
    cluster.start_all().await.expect("start cluster");
    cluster
        .wait_cluster_healthy(Duration::from_secs(30))
        .await
        .expect("cluster healthy");

    let vol = cluster
        .create_volume(
            "rejoin-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let data_before = b"data-before-crash";
    let eid_before = uuid::Uuid::new_v4().to_string();
    let mut w1 = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let resp1 = w1
        .write_extent(&vol_id, &eid_before, 1, data_before)
        .await
        .expect("write before");
    assert!(
        resp1
            .get("WriteResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        "write before kill should succeed"
    );

    cluster.kill_node(2).expect("kill node 2");
    tokio::time::sleep(Duration::from_secs(2)).await;

    let data_during = b"data-while-node-down";
    let eid_during = uuid::Uuid::new_v4().to_string();
    let mut w2 = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let resp2 = w2
        .write_extent(&vol_id, &eid_during, 1, data_during)
        .await
        .expect("write during");
    let w2_ok = resp2
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    cluster
        .restart_node(2)
        .await
        .expect("restart node 2");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut r_before = TcpDataClient::connect(cluster.node(2).data_addr())
        .await
        .expect("connect to restarted node");
    let (resp_rb, payload_rb) = r_before
        .read_extent(&vol_id, &eid_before, 1)
        .await
        .expect("read before-data from restarted node");
    let rb_ok = resp_rb
        .get("ReadResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if rb_ok {
        assert_eq!(
            payload_rb, data_before,
            "restarted node should have pre-crash data"
        );
    }

    if w2_ok {
        let mut r_during = TcpDataClient::connect(cluster.node(2).data_addr())
            .await
            .expect("connect to restarted node again");
        let (resp_rd, payload_rd) = r_during
            .read_extent(&vol_id, &eid_during, 1)
            .await
            .expect("read during-data from restarted node");
        let rd_ok = resp_rd
            .get("ReadResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if rd_ok {
            assert_eq!(
                payload_rd, data_during,
                "restarted node should have caught up via raft"
            );
        }
    }
}
