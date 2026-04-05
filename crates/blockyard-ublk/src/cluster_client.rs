use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{info, warn};

pub struct ClusterClient {
    volume_name: String,
    cluster_addrs: Vec<SocketAddr>,
    leader_addr: RwLock<Option<SocketAddr>>,
    request_id: AtomicU64,
}

impl ClusterClient {
    pub fn new(volume_name: String, cluster_addrs: Vec<SocketAddr>) -> Self {
        let leader = cluster_addrs.first().copied();
        Self {
            volume_name,
            cluster_addrs,
            leader_addr: RwLock::new(leader),
            request_id: AtomicU64::new(1),
        }
    }

    pub fn volume_name(&self) -> &str {
        &self.volume_name
    }

    pub fn leader_addr(&self) -> Option<SocketAddr> {
        *self.leader_addr.read()
    }

    pub fn set_leader(&self, addr: SocketAddr) {
        *self.leader_addr.write() = Some(addr);
        info!(leader = %addr, volume = %self.volume_name, "leader updated");
    }

    pub fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn cluster_addrs(&self) -> &[SocketAddr] {
        &self.cluster_addrs
    }

    pub async fn discover_leader(&self) -> blockyard_common::Result<SocketAddr> {
        if let Some(addr) = self.leader_addr() {
            return Ok(addr);
        }
        Err(blockyard_common::Error::Protocol(
            "no leader available".to_string(),
        ))
    }

    pub fn follow_leader(&self, new_leader: SocketAddr) {
        let old = self.leader_addr();
        if old != Some(new_leader) {
            warn!(
                old = ?old,
                new = %new_leader,
                "following new leader for volume {}",
                self.volume_name
            );
            self.set_leader(new_leader);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_client_new() {
        let addrs: Vec<SocketAddr> = vec![
            "10.0.0.1:7400".parse().unwrap(),
            "10.0.0.2:7400".parse().unwrap(),
        ];
        let client = ClusterClient::new("vol-1".into(), addrs.clone());
        assert_eq!(client.volume_name(), "vol-1");
        assert_eq!(client.cluster_addrs().len(), 2);
        assert_eq!(client.leader_addr(), Some(addrs[0]));
    }

    #[test]
    fn test_cluster_client_empty_addrs() {
        let client = ClusterClient::new("vol-1".into(), vec![]);
        assert!(client.leader_addr().is_none());
    }

    #[test]
    fn test_cluster_client_set_leader() {
        let client = ClusterClient::new("vol-1".into(), vec![]);
        let addr: SocketAddr = "10.0.0.5:7400".parse().unwrap();
        client.set_leader(addr);
        assert_eq!(client.leader_addr(), Some(addr));
    }

    #[test]
    fn test_cluster_client_next_request_id() {
        let client = ClusterClient::new("vol-1".into(), vec![]);
        assert_eq!(client.next_request_id(), 1);
        assert_eq!(client.next_request_id(), 2);
        assert_eq!(client.next_request_id(), 3);
    }

    #[test]
    fn test_cluster_client_follow_leader() {
        let addr1: SocketAddr = "10.0.0.1:7400".parse().unwrap();
        let addr2: SocketAddr = "10.0.0.2:7400".parse().unwrap();
        let client = ClusterClient::new("vol-1".into(), vec![addr1]);
        assert_eq!(client.leader_addr(), Some(addr1));

        client.follow_leader(addr2);
        assert_eq!(client.leader_addr(), Some(addr2));
    }

    #[tokio::test]
    async fn test_cluster_client_discover_leader() {
        let addr: SocketAddr = "10.0.0.1:7400".parse().unwrap();
        let client = ClusterClient::new("vol-1".into(), vec![addr]);
        let leader = client.discover_leader().await.unwrap();
        assert_eq!(leader, addr);
    }

    #[tokio::test]
    async fn test_cluster_client_discover_leader_none() {
        let client = ClusterClient::new("vol-1".into(), vec![]);
        let result = client.discover_leader().await;
        assert!(result.is_err());
    }
}
