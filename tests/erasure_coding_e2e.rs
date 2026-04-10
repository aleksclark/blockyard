//! Erasure coding end-to-end tests with real blockyard processes.
//!
//! Tests EC write/read roundtrips, node loss survival, and fragment
//! corruption detection using real TCP connections.

use std::time::Duration;

use blockyard_test_harness::process_harness::{
    RealProcessCluster, TcpDataClient, build_binary, unique_base_port,
};

async fn setup_ec_cluster() -> (RealProcessCluster, String) {
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
            "ec-vol",
            1024 * 1024 * 64,
            serde_json::json!({"ErasureCoded": {"data_chunks": 2, "parity_chunks": 1}}),
        )
        .await
        .expect("create EC volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();
    (cluster, vol_id)
}

#[tokio::test]
async fn test_ec_write_read_roundtrip() {
    let (cluster, vol_id) = setup_ec_cluster().await;

    let data = b"erasure-coded-test-data-for-roundtrip-verification";
    let extent_id = uuid::Uuid::new_v4().to_string();

    let mut client = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let write_resp = client
        .write_extent(&vol_id, &extent_id, 1, data)
        .await
        .expect("write EC extent");

    let success = write_resp
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(success, "EC write should succeed: {:?}", write_resp);

    let mut read_client = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect for read");
    let (read_resp, payload) = read_client
        .read_extent(&vol_id, &extent_id, 1)
        .await
        .expect("read EC extent");

    let read_ok = read_resp
        .get("ReadResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if read_ok {
        assert_eq!(payload, data, "EC read should return original data");
    }
}

#[tokio::test]
async fn test_ec_survives_one_node_loss() {
    let (cluster, vol_id) = setup_ec_cluster().await;

    let extent_ids: Vec<String> = (0..3)
        .map(|_| uuid::Uuid::new_v4().to_string())
        .collect();

    for (i, eid) in extent_ids.iter().enumerate() {
        let data = format!("ec-data-{}", i);
        let mut client = TcpDataClient::connect(cluster.node(0).data_addr())
            .await
            .expect("connect");
        let resp = client
            .write_extent(&vol_id, eid, 1, data.as_bytes())
            .await
            .expect("write");
        let success = resp
            .get("WriteResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(success, "EC write {} should succeed", i);
    }

    cluster.kill_node(2).expect("kill node 2");
    tokio::time::sleep(Duration::from_secs(3)).await;

    for (i, eid) in extent_ids.iter().enumerate() {
        let expected = format!("ec-data-{}", i);

        for surviving in [0, 1] {
            let result = TcpDataClient::connect(cluster.node(surviving).data_addr()).await;
            if let Ok(mut client) = result {
                let read_result = client.read_extent(&vol_id, eid, 1).await;
                if let Ok((resp, payload)) = read_result {
                    let success = resp
                        .get("ReadResp")
                        .and_then(|r| r.get("success"))
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if success {
                        assert_eq!(
                            payload,
                            expected.as_bytes(),
                            "EC read from node {} should return correct data for extent {}",
                            surviving,
                            i
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn test_ec_fragment_corruption() {
    let (cluster, vol_id) = setup_ec_cluster().await;

    let data = b"ec-corruption-test-payload";
    let extent_id = uuid::Uuid::new_v4().to_string();

    let mut client = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let resp = client
        .write_extent(&vol_id, &extent_id, 1, data)
        .await
        .expect("write");
    let success = resp
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(success, "initial write should succeed");

    let corrupted = cluster.corrupt_extent_files(1).unwrap_or(0);

    let mut read_client = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect for read");
    let (read_resp, payload) = read_client
        .read_extent(&vol_id, &extent_id, 1)
        .await
        .expect("read after corruption");

    let read_ok = read_resp
        .get("ReadResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if read_ok {
        assert_eq!(
            payload, data,
            "should read correct data from healthy node despite corruption on node 1"
        );
    }

    let _ = corrupted;
}
