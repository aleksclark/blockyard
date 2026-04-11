//! Observability integration tests — verify metrics are recorded during real
//! Raft cluster operations.
//!
//! Mock-based metric recording tests are in their respective crate unit test
//! modules:
//! - `blockyard-common/src/metrics.rs` — volume IO, disk state, metric constants
//! - `blockyard-storage/src/background/scrub.rs` — scrub metrics
//! - `blockyard-storage/src/background/repair.rs` — repair metrics

use blockyard_common::metrics::{record_metadata_commit_latency, set_metadata_quorum_health};
use blockyard_common::{
    ExtentId, InMemoryRecorder, Labels, METADATA_COMMIT_LATENCY_SECONDS, METADATA_QUORUM_HEALTH,
    NodeId, OperationId, ProtectionPolicy, VolumeId,
};
use blockyard_test_harness::raft_testutil::{create_test_raft_cluster, find_leader};

// ===========================================================================
// Metadata quorum health and commit latency with real Raft cluster
// ===========================================================================

#[tokio::test]
async fn test_metadata_quorum_health_metrics() {
    let recorder = InMemoryRecorder::new();
    let cluster = create_test_raft_cluster(3).await;
    let leader_idx = find_leader(&cluster).await;
    let leader = &cluster.services[leader_idx];

    let raft_group_id = "rg-meta-1";
    set_metadata_quorum_health(&recorder, raft_group_id, true);

    let vol_id = VolumeId::generate();
    let start = std::time::Instant::now();
    leader
        .create_volume(
            vol_id,
            1024 * 1024 * 1024,
            ProtectionPolicy::Replicated { replicas: 3 },
        )
        .await
        .expect("create volume");
    let latency = start.elapsed().as_secs_f64();
    record_metadata_commit_latency(&recorder, raft_group_id, latency);

    let epoch = leader.advance_epoch().await.expect("advance epoch");
    let node_id = NodeId::generate();
    leader
        .add_node(node_id, "127.0.0.1:9000".to_string())
        .await
        .expect("add node");

    let start2 = std::time::Instant::now();
    let ext_id = ExtentId::generate();
    leader
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
    let latency2 = start2.elapsed().as_secs_f64();
    record_metadata_commit_latency(&recorder, raft_group_id, latency2);

    let labels = Labels::from_pairs(&[("raft_group_id", raft_group_id)]);
    assert_eq!(
        recorder.gauge(METADATA_QUORUM_HEALTH, &labels),
        Some(1.0),
        "quorum should be healthy"
    );
    let observations = recorder.histogram(METADATA_COMMIT_LATENCY_SECONDS, &labels);
    assert_eq!(
        observations.len(),
        2,
        "should have 2 commit latency observations"
    );
    assert!(
        observations.iter().all(|&v| v > 0.0),
        "latencies must be positive"
    );

    set_metadata_quorum_health(&recorder, raft_group_id, false);
    assert_eq!(
        recorder.gauge(METADATA_QUORUM_HEALTH, &labels),
        Some(0.0),
        "quorum should be unhealthy after toggle"
    );
}
