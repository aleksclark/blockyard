//! Phase 9C — Availability integration tests using real Raft consensus.

use std::sync::Arc;
use std::time::{Duration, Instant};

use blockyard_common::{ExtentId, NodeId, OperationId, ProtectionPolicy, VolumeId};
use blockyard_raft::{LogStore, NetworkFactory, StateMachineStore};
use blockyard_test_harness::raft_testutil::{
    create_test_raft_cluster, wait_for_leader,
};

// ---------------------------------------------------------------------------
// P9C.1 — 1-of-3 crash: writes continue after leader re-election
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_1_of_3_crash_writes_continue() {
    let cluster = create_test_raft_cluster(3).await;
    let leader_idx = wait_for_leader(&cluster).await;
    let leader = &cluster.services[leader_idx];

    let vol = VolumeId::generate();
    leader
        .create_volume(vol, 1_000_000, ProtectionPolicy::Replicated { replicas: 3 })
        .await
        .expect("create volume");

    let epoch = leader.advance_epoch().await.expect("advance epoch");

    let node_id = NodeId::generate();
    leader
        .add_node(node_id, "10.0.0.1:9000".into())
        .await
        .expect("add node");

    let crashed_id = (leader_idx + 1) as u64;
    cluster.router.write().remove_node(crashed_id);
    cluster.services[leader_idx]
        .raft()
        .shutdown()
        .await
        .expect("shutdown leader");

    let election_start = Instant::now();

    let mut new_leader_idx = None;
    for _ in 0..40 {
        for (i, svc) in cluster.services.iter().enumerate() {
            if (i + 1) as u64 == crashed_id {
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let election_time = election_start.elapsed();
    let new_leader_idx = new_leader_idx.expect("new leader should be elected");
    let new_leader = &cluster.services[new_leader_idx];

    assert!(
        election_time < Duration::from_secs(2),
        "election took {:?}, should be < 2s",
        election_time
    );

    let vol_check = new_leader.get_volume(&vol);
    assert!(vol_check.is_some(), "volume must survive leader crash");

    let ext = ExtentId::generate();
    let result = new_leader
        .commit_extent_mapping(
            vol,
            0..512,
            ext,
            1,
            epoch,
            vec![node_id],
            vec![vec![1]],
            Some(OperationId::generate()),
            None,
        )
        .await;
    assert!(
        result.is_ok(),
        "writes must continue on new leader: {:?}",
        result.err()
    );

    let node_check = new_leader.get_node(&node_id);
    assert!(node_check.is_some(), "committed node must survive crash");
}

// ---------------------------------------------------------------------------
// P9C.2 — 1-of-5 crash: unaffected volumes have zero downtime
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_1_of_5_crash_zero_downtime_unaffected() {
    let cluster = create_test_raft_cluster(5).await;
    let leader_idx = wait_for_leader(&cluster).await;
    let leader = &cluster.services[leader_idx];

    let vol1 = VolumeId::generate();
    let vol2 = VolumeId::generate();
    leader
        .create_volume(
            vol1,
            1_000_000,
            ProtectionPolicy::Replicated { replicas: 3 },
        )
        .await
        .expect("create vol1");
    leader
        .create_volume(
            vol2,
            1_000_000,
            ProtectionPolicy::Replicated { replicas: 3 },
        )
        .await
        .expect("create vol2");

    let epoch = leader.advance_epoch().await.expect("advance");
    let nid = NodeId::generate();
    leader
        .add_node(nid, "10.0.0.2:9000".into())
        .await
        .expect("add node");

    let non_leader_idx = if leader_idx == 0 { 1 } else { 0 };
    let crashed_id = (non_leader_idx + 1) as u64;
    cluster.router.write().remove_node(crashed_id);
    cluster.services[non_leader_idx]
        .raft()
        .shutdown()
        .await
        .expect("shutdown non-leader");

    tokio::time::sleep(Duration::from_millis(300)).await;

    for i in 0..5 {
        let ext = ExtentId::generate();
        let result = leader
            .commit_extent_mapping(
                vol1,
                (i * 100)..((i + 1) * 100),
                ext,
                i + 1,
                epoch,
                vec![nid],
                vec![vec![(i as u8) + 1]],
                Some(OperationId::generate()),
                None,
            )
            .await;
        assert!(
            result.is_ok(),
            "write {} to vol1 should succeed after non-leader crash: {:?}",
            i,
            result.err()
        );
    }

    for i in 0..5 {
        let ext = ExtentId::generate();
        let result = leader
            .commit_extent_mapping(
                vol2,
                (i * 100)..((i + 1) * 100),
                ext,
                i + 10,
                epoch,
                vec![nid],
                vec![vec![(i as u8) + 10]],
                Some(OperationId::generate()),
                None,
            )
            .await;
        assert!(
            result.is_ok(),
            "write {} to vol2 should succeed: {:?}",
            i,
            result.err()
        );
    }

    tokio::time::sleep(Duration::from_millis(300)).await;

    for (i, svc) in cluster.services.iter().enumerate() {
        if (i + 1) as u64 == crashed_id {
            continue;
        }
        assert!(
            svc.get_volume(&vol1).is_some(),
            "surviving node {} must see vol1",
            i + 1
        );
        assert!(
            svc.get_volume(&vol2).is_some(),
            "surviving node {} must see vol2",
            i + 1
        );
    }
}

// ---------------------------------------------------------------------------
// P9C.3 — Volume readable from surviving nodes during minority partition
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_volume_readable_minority_partition() {
    let cluster = create_test_raft_cluster(5).await;
    let leader_idx = wait_for_leader(&cluster).await;
    let leader = &cluster.services[leader_idx];

    let vol = VolumeId::generate();
    leader
        .create_volume(vol, 1_000_000, ProtectionPolicy::Replicated { replicas: 3 })
        .await
        .expect("create volume");

    let epoch = leader.advance_epoch().await.expect("advance");
    let nid = NodeId::generate();
    leader
        .add_node(nid, "10.0.0.3:9000".into())
        .await
        .expect("add node");

    let ext = ExtentId::generate();
    leader
        .commit_extent_mapping(
            vol,
            0..512,
            ext,
            1,
            epoch,
            vec![nid],
            vec![vec![1, 2, 3]],
            Some(OperationId::generate()),
            None,
        )
        .await
        .expect("pre-partition commit");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let minority_ids: Vec<u64> = vec![4, 5];
    for id in &minority_ids {
        cluster.router.write().remove_node(*id);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let vol_check = leader.get_volume(&vol);
    assert!(
        vol_check.is_some(),
        "volume must remain readable from majority side"
    );

    let mapping = leader.lookup_by_extent_version(1);
    assert!(
        mapping.is_some(),
        "committed mapping must be readable during partition"
    );

    let ext2 = ExtentId::generate();
    let result = leader
        .commit_extent_mapping(
            vol,
            512..1024,
            ext2,
            2,
            epoch,
            vec![nid],
            vec![vec![4, 5, 6]],
            Some(OperationId::generate()),
            None,
        )
        .await;
    assert!(
        result.is_ok(),
        "writes should continue on majority side: {:?}",
        result.err()
    );

    for id in &minority_ids {
        let log_store = LogStore::new();
        let sm_store = StateMachineStore::new();
        let network = NetworkFactory::new(Arc::clone(&cluster.router));
        let raft = openraft::Raft::<blockyard_raft::TypeConfig>::new(
            *id,
            Arc::new(openraft::Config {
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }),
            network,
            log_store,
            sm_store.clone(),
        )
        .await
        .expect("recreate node");
        cluster.router.write().add_node(*id, raft);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let m2 = leader.lookup_by_extent_version(2);
    assert!(m2.is_some(), "post-partition writes should be committed");
}

// ---------------------------------------------------------------------------
// P9C.4 — New leader elected within 2 seconds after leader crash
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_new_leader_elected_within_2s() {
    let cluster = create_test_raft_cluster(5).await;
    let leader_idx = wait_for_leader(&cluster).await;
    let leader = &cluster.services[leader_idx];

    let vol = VolumeId::generate();
    leader
        .create_volume(vol, 1_000_000, ProtectionPolicy::Replicated { replicas: 3 })
        .await
        .expect("create volume");

    let old_leader_id = (leader_idx + 1) as u64;
    cluster.router.write().remove_node(old_leader_id);

    let election_start = Instant::now();
    cluster.services[leader_idx]
        .raft()
        .shutdown()
        .await
        .expect("shutdown leader");

    let mut new_leader_id: Option<u64> = None;
    for _ in 0..40 {
        for (i, svc) in cluster.services.iter().enumerate() {
            if (i + 1) as u64 == old_leader_id {
                continue;
            }
            let metrics = svc.raft().metrics().borrow().clone();
            if let Some(lid) = metrics.current_leader {
                if lid != old_leader_id {
                    new_leader_id = Some(lid);
                    break;
                }
            }
        }
        if new_leader_id.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let election_time = election_start.elapsed();

    assert!(
        new_leader_id.is_some(),
        "new leader must be elected after leader crash"
    );
    assert_ne!(
        new_leader_id.unwrap(),
        old_leader_id,
        "new leader must differ from crashed leader"
    );
    assert!(
        election_time < Duration::from_secs(2),
        "election took {:?}, must be < 2s",
        election_time
    );

    let new_idx = (new_leader_id.unwrap() - 1) as usize;
    let new_leader = &cluster.services[new_idx];
    let vol_check = new_leader.get_volume(&vol);
    assert!(
        vol_check.is_some(),
        "new leader must retain committed volume"
    );

    let epoch = new_leader.advance_epoch().await;
    assert!(
        epoch.is_ok(),
        "new leader must accept writes: {:?}",
        epoch.err()
    );
}
