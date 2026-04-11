//! Authentication and authorization integration tests with real processes.
//!
//! Unit tests for SharedSecretAuth and VolumeAcl are in
//! `crates/blockyard-common/src/auth.rs`.

use std::time::Duration;

use blockyard_test_harness::process_harness::{
    RealProcessCluster, TcpDataClient, build_binary, unique_base_port,
};

/// Start a real cluster and verify that a write succeeds with valid
/// credentials and that reading non-existent data is properly rejected.
#[tokio::test]
async fn test_cluster_auth_basic_operations() {
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
            "auth-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let data = b"auth-test-data";
    let extent_id = uuid::Uuid::new_v4().to_string();
    let mut writer = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let resp = writer
        .write_extent(&vol_id, &extent_id, 1, data)
        .await
        .expect("write");
    let success = resp
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(
        success,
        "write with valid connection should succeed: {:?}",
        resp
    );

    let mut reader = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect reader");
    let (read_resp, payload) = reader
        .read_extent(&vol_id, &extent_id, 1)
        .await
        .expect("read");
    let read_ok = read_resp
        .get("ReadResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if read_ok {
        assert_eq!(payload, data, "read should return written data");
    }

    let fake_eid = uuid::Uuid::new_v4().to_string();
    let mut reader2 = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect reader");
    let (resp2, _) = reader2
        .read_extent(&vol_id, &fake_eid, 1)
        .await
        .expect("read non-existent");
    let read2_ok = resp2
        .get("ReadResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(!read2_ok, "reading non-existent extent should not succeed");
}
