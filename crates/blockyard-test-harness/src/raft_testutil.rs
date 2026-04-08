use std::collections::BTreeMap;
use std::sync::Arc;

use blockyard_raft::{LogStore, MetadataService, NetworkFactory, Router, StateMachineStore};
use openraft::BasicNode;
use parking_lot::RwLock;

pub struct TestRaftCluster {
    pub services: Vec<MetadataService>,
    pub router: Arc<RwLock<Router>>,
}

pub async fn create_test_raft_cluster(node_count: u64) -> TestRaftCluster {
    let router = Arc::new(RwLock::new(Router::new()));
    let config = Arc::new(openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    });

    let mut services = Vec::new();
    for node_id in 1..=node_count {
        let log_store = LogStore::new();
        let sm_store = StateMachineStore::new();
        let network = NetworkFactory::new(Arc::clone(&router));
        let raft = openraft::Raft::<blockyard_raft::TypeConfig>::new(
            node_id,
            config.clone(),
            network,
            log_store,
            sm_store.clone(),
        )
        .await
        .expect("failed to create Raft node");
        router.write().add_node(node_id, raft.clone());
        services.push(MetadataService::new(raft, sm_store));
    }

    let mut nodes = BTreeMap::new();
    for id in 1..=node_count {
        nodes.insert(id, BasicNode::default());
    }
    services[0]
        .raft()
        .initialize(nodes)
        .await
        .expect("failed to initialize cluster");

    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    TestRaftCluster { services, router }
}

pub async fn find_leader(cluster: &TestRaftCluster) -> usize {
    for _ in 0..30 {
        for (i, svc) in cluster.services.iter().enumerate() {
            let metrics = svc.raft().metrics().borrow().clone();
            if metrics.current_leader == Some((i + 1) as u64) {
                return i;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("no leader elected within timeout");
}

pub async fn find_leader_optional(cluster: &TestRaftCluster) -> Option<usize> {
    for _ in 0..30 {
        for (i, svc) in cluster.services.iter().enumerate() {
            let metrics = svc.raft().metrics().borrow().clone();
            if metrics.current_leader == Some((i + 1) as u64) {
                return Some(i);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    None
}

pub async fn wait_for_leader(cluster: &TestRaftCluster) -> usize {
    find_leader_optional(cluster)
        .await
        .expect("leader should be elected within timeout")
}
