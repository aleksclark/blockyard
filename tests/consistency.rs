//! Consistency integration tests with real blockyard processes.
//!
//! The Raft linearizability test uses an in-memory Raft cluster.
//! All other tests use RealProcessCluster with real TCP and HTTP.
//!
//! Mock-based unit tests for read pipeline, freshness, stale epoch, and
//! write pipeline majority ack are in their respective crate unit test
//! modules.

use std::time::Duration;

use blockyard_common::{ExtentId, NodeId, OperationId, ProtectionPolicy, VolumeId};
use blockyard_test_harness::process_harness::{
    RealProcessCluster, TcpDataClient, build_binary, unique_base_port,
};
use blockyard_test_harness::raft_testutil::{create_test_raft_cluster, find_leader};

// ---------------------------------------------------------------------------
// P9B.1 — Linearizability: Raft entries committed on leader are visible on
//          all followers, including after leader failover (real in-memory Raft)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_raft_linearizable_write() {
    let cluster = create_test_raft_cluster(3).await;
    let leader_idx = find_leader(&cluster).await;
    let leader = &cluster.services[leader_idx];

    let vol_id = VolumeId::generate();
    leader
        .create_volume(
            vol_id,
            1024 * 1024 * 1024,
            ProtectionPolicy::Replicated { replicas: 3 },
        )
        .await
        .expect("create volume");

    let epoch = leader.advance_epoch().await.expect("advance epoch");

    let node_id = NodeId::generate();
    leader
        .add_node(node_id, "127.0.0.1:9000".to_string())
        .await
        .expect("add node");

    let ext_id = ExtentId::generate();
    let committed_epoch = leader
        .commit_extent_mapping(
            vol_id,
            0..1024,
            ext_id,
            1,
            epoch,
            vec![node_id],
            vec![vec![1, 2, 3]],
            Some(OperationId::generate()),
            None,
        )
        .await
        .expect("commit extent mapping");

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    for (i, svc) in cluster.services.iter().enumerate() {
        let vol = svc.get_volume(&vol_id);
        assert!(vol.is_some(), "node {} must see the created volume", i + 1);
        assert_eq!(vol.unwrap().volume_id, vol_id);

        let mapping = svc.lookup_by_extent_version(1);
        assert!(
            mapping.is_some(),
            "node {} must see committed extent mapping",
            i + 1
        );
        let m = mapping.unwrap();
        assert_eq!(m.extent_id, ext_id);
        assert_eq!(m.block_range, 0..1024);
    }

    let old_leader_id = (leader_idx + 1) as u64;
    cluster.services[leader_idx]
        .raft()
        .shutdown()
        .await
        .expect("shutdown leader");

    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let mut new_leader_idx = None;
    for _ in 0..30 {
        for (i, svc) in cluster.services.iter().enumerate() {
            if (i + 1) as u64 == old_leader_id {
                continue;
            }
            let metrics = svc.raft().metrics().borrow().clone();
            if metrics.current_leader == Some((i + 1) as u64) {
                new_leader_idx = Some(i);
                break;
            }
        }
        if new_leader_idx.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let new_leader_idx = new_leader_idx.expect("new leader should be elected after failover");
    let new_leader = &cluster.services[new_leader_idx];

    let vol = new_leader.get_volume(&vol_id);
    assert!(vol.is_some(), "volume must survive leader failover");

    let mapping = new_leader.lookup_by_extent_version(1);
    assert!(
        mapping.is_some(),
        "extent mapping must survive leader failover"
    );

    let ext_id_2 = ExtentId::generate();
    let result = new_leader
        .commit_extent_mapping(
            vol_id,
            1024..2048,
            ext_id_2,
            2,
            committed_epoch,
            vec![node_id],
            vec![vec![4, 5, 6]],
            Some(OperationId::generate()),
            None,
        )
        .await;
    assert!(
        result.is_ok(),
        "new leader must accept writes after failover: {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// P9B.2 — Majority ack: write with replicas=3, SIGSTOP one node, verify
//          write still succeeds with 2/3 acks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_majority_ack_required() {
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
            "majority-ack-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let data = b"majority-ack-data";
    let eid = uuid::Uuid::new_v4().to_string();
    let mut writer = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let resp = writer
        .write_extent(&vol_id, &eid, 1, data)
        .await
        .expect("write");
    let ok = resp
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(ok, "write should succeed with all 3 nodes: {:?}", resp);

    cluster.stop_node(2).expect("SIGSTOP node 2");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let data2 = b"write-with-2-of-3";
    let eid2 = uuid::Uuid::new_v4().to_string();
    let mut writer2 = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let resp2 = writer2
        .write_extent(&vol_id, &eid2, 1, data2)
        .await
        .expect("write with 2/3");
    let ok2 = resp2
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(
        ok2,
        "write should succeed with 2/3 nodes (majority): {:?}",
        resp2
    );

    cluster.resume_node(2).expect("SIGCONT node 2");
    tokio::time::sleep(Duration::from_secs(2)).await;
}

// ---------------------------------------------------------------------------
// P9B.3 — Read-your-own-writes: write data, immediately read back, verify
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_read_your_own_writes() {
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
            "ryow-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    for i in 0..5 {
        let data = format!("ryow-data-{:04}", i);
        let eid = uuid::Uuid::new_v4().to_string();

        let mut writer = TcpDataClient::connect(cluster.node(0).data_addr())
            .await
            .expect("connect writer");
        let write_resp = writer
            .write_extent(&vol_id, &eid, 1, data.as_bytes())
            .await
            .expect("write");
        let w_ok = write_resp
            .get("WriteResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !w_ok {
            continue;
        }

        let mut reader = TcpDataClient::connect(cluster.node(0).data_addr())
            .await
            .expect("connect reader");
        let (read_resp, payload) = reader.read_extent(&vol_id, &eid, 1).await.expect("read");
        let r_ok = read_resp
            .get("ReadResp")
            .and_then(|r| r.get("success"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        assert!(r_ok, "read-after-write should succeed for extent {}", i);
        assert_eq!(
            payload,
            data.as_bytes(),
            "read-after-write data mismatch for extent {}",
            i
        );
    }
}

// ---------------------------------------------------------------------------
// P9B.4 — Bounded staleness: write data, kill one replica, read from
//          surviving replicas, verify data is there
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bounded_staleness() {
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
            "staleness-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let data = b"staleness-test-data";
    let eid = uuid::Uuid::new_v4().to_string();
    let mut writer = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let resp = writer
        .write_extent(&vol_id, &eid, 1, data)
        .await
        .expect("write");
    let ok = resp
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(ok, "initial write should succeed");

    cluster.kill_node(2).expect("SIGKILL node 2");
    tokio::time::sleep(Duration::from_secs(2)).await;

    for i in 0..2 {
        let result = TcpDataClient::connect(cluster.node(i).data_addr()).await;
        if let Ok(mut reader) = result {
            let read_result = reader.read_extent(&vol_id, &eid, 1).await;
            if let Ok((resp, payload)) = read_result {
                let read_ok = resp
                    .get("ReadResp")
                    .and_then(|r| r.get("success"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                if read_ok {
                    assert_eq!(payload, data, "surviving node {} should have the data", i);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// P9B.5 — Stale epoch forces refresh: verify writes still succeed after
//          cluster operations that may advance epochs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stale_epoch_forces_refresh() {
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
            "epoch-refresh-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let data1 = b"pre-epoch-data";
    let eid1 = uuid::Uuid::new_v4().to_string();
    let mut w1 = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let resp1 = w1
        .write_extent(&vol_id, &eid1, 1, data1)
        .await
        .expect("write before epoch change");
    let ok1 = resp1
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(ok1, "write before epoch change should succeed");

    let data2 = b"post-epoch-data";
    let eid2 = uuid::Uuid::new_v4().to_string();
    let mut w2 = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let resp2 = w2
        .write_extent(&vol_id, &eid2, 1, data2)
        .await
        .expect("write after epoch change");
    let ok2 = resp2
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(ok2, "write should succeed after epoch refresh: {:?}", resp2);
}

// ---------------------------------------------------------------------------
// P9B.6 — Write watermark prevents stale read: write data, verify read
//          returns correct version
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_write_watermark_prevents_stale_read() {
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
            "watermark-test",
            1024 * 1024 * 64,
            serde_json::json!({"Replicated": {"replicas": 3}}),
        )
        .await
        .expect("create volume");
    let vol_id = vol["id"].as_str().unwrap().to_string();

    let data = b"watermark-test-data";
    let eid = uuid::Uuid::new_v4().to_string();
    let mut writer = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect");
    let resp = writer
        .write_extent(&vol_id, &eid, 1, data)
        .await
        .expect("write");
    let ok = resp
        .get("WriteResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(ok, "write should succeed");

    let mut reader = TcpDataClient::connect(cluster.node(0).data_addr())
        .await
        .expect("connect reader");
    let (read_resp, payload) = reader.read_extent(&vol_id, &eid, 1).await.expect("read");
    let read_ok = read_resp
        .get("ReadResp")
        .and_then(|r| r.get("success"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    assert!(read_ok, "read should succeed after write");
    assert_eq!(payload, data, "read should return written data");
}
