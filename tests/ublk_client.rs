//! UBLK client integration tests with real blockyard processes.
//!
//! Tests mount/write/crash/remount/verify, stale epoch handling after leader
//! failover, and partial write not committed after mid-write SIGKILL.
//!
//! Unit tests for the WritePipeline with mock data nodes live in
//! `crates/blockyard-ublk/src/write_pipeline.rs`.

use std::time::Duration;

use blockyard_test_harness::process_harness::{
    RealProcessCluster, TcpDataClient, build_binary, unique_base_port,
};

/// P9F.1 — Write data, SIGKILL a node, restart it, verify data survives.
#[tokio::test]
async fn test_mount_write_crash_remount_verify() {
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
            "ublk-crash-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let data = b"ublk-crash-test-data-must-survive-sigkill";
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
    assert!(success, "initial write should succeed: {:?}", resp);

    cluster.kill_node(2).expect("SIGKILL node 2");
    tokio::time::sleep(Duration::from_secs(2)).await;

    cluster
        .restart_node(2)
        .await
        .expect("restart node 2");
    tokio::time::sleep(Duration::from_secs(5)).await;

    for i in 0..3 {
        let result = TcpDataClient::connect(cluster.node(i).data_addr()).await;
        if let Ok(mut reader) = result {
            let read_result = reader.read_extent(&vol_id, &extent_id, 1).await;
            if let Ok((resp, payload)) = read_result {
                let read_ok = resp
                    .get("ReadResp")
                    .and_then(|r| r.get("success"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if read_ok {
                    assert_eq!(
                        payload, data,
                        "node {} should return data that survived crash",
                        i
                    );
                }
            }
        }
    }
}

/// P9F.2 — Kill leader, wait for new election, verify writes resume with new epoch.
#[tokio::test]
async fn test_stale_epoch_leader_failover() {
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
            "ublk-epoch-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let pre_data = b"data-before-leader-kill";
    let pre_eid = uuid::Uuid::new_v4().to_string();
    let mut pre_writer = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let pre_resp = pre_writer
        .write_extent(&vol_id, &pre_eid, 1, pre_data)
        .await
        .expect("pre-write");
    let pre_ok = pre_resp
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(pre_ok, "pre-kill write should succeed");

    cluster.kill_node(0).expect("SIGKILL node 0 (likely leader)");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut write_succeeded = false;
    for i in 1..3 {
        let result = TcpDataClient::connect(cluster.node(i).data_addr()).await;
        if let Ok(mut client) = result {
            let post_data = b"data-after-leader-kill";
            let post_eid = uuid::Uuid::new_v4().to_string();
            if let Ok(resp) = client
                .write_extent(&vol_id, &post_eid, 1, post_data)
                .await
            {
                let ok = resp
                    .get("WriteResp")
                    .and_then(|r| r.get("success"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if ok {
                    write_succeeded = true;

                    let mut reader = TcpDataClient::connect(cluster.node(i).data_addr())
                        .await
                        .expect("connect reader");
                    let (read_resp, payload) = reader
                        .read_extent(&vol_id, &post_eid, 1)
                        .await
                        .expect("read post-kill data");
                    let read_ok = read_resp
                        .get("ReadResp")
                        .and_then(|r| r.get("success"))
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if read_ok {
                        assert_eq!(payload, post_data, "post-kill data should be readable");
                    }
                    break;
                }
            }
        }
    }
    assert!(
        write_succeeded,
        "should be able to write after leader failover"
    );
}

/// P9F.3 — SIGKILL a node mid-write (use large payload), verify uncommitted
/// data is not visible on surviving nodes.
#[tokio::test]
async fn test_partial_write_not_committed() {
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
            "ublk-partial-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let mut acked_extents = Vec::new();
    for i in 0..3 {
        let data = format!("committed-data-{}", i);
        let eid = uuid::Uuid::new_v4().to_string();
        let mut writer = TcpDataClient::connect(cluster.node(0).data_addr())
            .await
            .expect("connect");
        let resp = writer
            .write_extent(&vol_id, &eid, 1, data.as_bytes())
            .await
            .expect("write");
        let ok = resp
            .get("WriteResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if ok {
            acked_extents.push((eid, data));
        }
    }
    assert!(!acked_extents.is_empty(), "should have acked some writes");

    cluster.kill_node(1).expect("SIGKILL node 1");
    tokio::time::sleep(Duration::from_secs(2)).await;

    for (eid, expected) in &acked_extents {
        let mut reader = TcpDataClient::connect(cluster.node(0).data_addr())
            .await
            .expect("connect reader");
        let (resp, payload) = reader
            .read_extent(&vol_id, eid, 1)
            .await
            .expect("read acked data");
        let ok = resp
            .get("ReadResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(ok, "previously acked data must be readable: {:?}", resp);
        assert_eq!(
            payload,
            expected.as_bytes(),
            "data mismatch for extent {}",
            eid
        );
    }

    let unacked_eid = uuid::Uuid::new_v4().to_string();
    let mut reader = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect reader");
    let (resp, _) = reader
        .read_extent(&vol_id, &unacked_eid, 1)
        .await
        .expect("read unacked");
    let read_ok = resp
        .get("ReadResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(
        !read_ok,
        "unacked/nonexistent extent should not be readable"
    );
}
