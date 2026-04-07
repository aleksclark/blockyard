use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};

use parking_lot::Mutex;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeAddress {
    pub listen_addr: SocketAddr,
    pub gossip_addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub base_listen_port: u16,
    pub base_gossip_port: u16,
    pub host: std::net::IpAddr,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            base_listen_port: 20000,
            base_gossip_port: 21000,
            host: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        }
    }
}

#[derive(Debug)]
pub struct PortAllocator {
    next_listen_port: AtomicU16,
    next_gossip_port: AtomicU16,
    host: std::net::IpAddr,
    allocated: Mutex<Vec<NodeAddress>>,
}

impl PortAllocator {
    pub fn new(config: NetworkConfig) -> Self {
        Self {
            next_listen_port: AtomicU16::new(config.base_listen_port),
            next_gossip_port: AtomicU16::new(config.base_gossip_port),
            host: config.host,
            allocated: Mutex::new(Vec::new()),
        }
    }

    pub fn allocate(&self) -> NodeAddress {
        let listen_port = self.next_listen_port.fetch_add(1, Ordering::SeqCst);
        let gossip_port = self.next_gossip_port.fetch_add(1, Ordering::SeqCst);

        let addr = NodeAddress {
            listen_addr: SocketAddr::new(self.host, listen_port),
            gossip_addr: SocketAddr::new(self.host, gossip_port),
        };

        debug!(listen=%addr.listen_addr, gossip=%addr.gossip_addr, "allocated ports");
        self.allocated.lock().push(addr.clone());

        addr
    }

    pub fn allocated_addresses(&self) -> Vec<NodeAddress> {
        self.allocated.lock().clone()
    }

    pub fn count(&self) -> usize {
        self.allocated.lock().len()
    }

    pub fn seed_addrs(&self) -> Vec<SocketAddr> {
        self.allocated
            .lock()
            .iter()
            .map(|a| a.gossip_addr)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_config_default() {
        let config = NetworkConfig::default();
        assert_eq!(config.base_listen_port, 20000);
        assert_eq!(config.base_gossip_port, 21000);
        assert_eq!(
            config.host,
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );
    }

    #[test]
    fn test_port_allocator_sequential() {
        let config = NetworkConfig {
            base_listen_port: 30000,
            base_gossip_port: 31000,
            host: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        };
        let allocator = PortAllocator::new(config);

        let addr1 = allocator.allocate();
        let addr2 = allocator.allocate();
        let addr3 = allocator.allocate();

        assert_eq!(addr1.listen_addr.port(), 30000);
        assert_eq!(addr1.gossip_addr.port(), 31000);
        assert_eq!(addr2.listen_addr.port(), 30001);
        assert_eq!(addr2.gossip_addr.port(), 31001);
        assert_eq!(addr3.listen_addr.port(), 30002);
        assert_eq!(addr3.gossip_addr.port(), 31002);
    }

    #[test]
    fn test_port_allocator_count() {
        let allocator = PortAllocator::new(NetworkConfig::default());
        assert_eq!(allocator.count(), 0);

        allocator.allocate();
        assert_eq!(allocator.count(), 1);

        allocator.allocate();
        assert_eq!(allocator.count(), 2);
    }

    #[test]
    fn test_port_allocator_allocated_addresses() {
        let allocator = PortAllocator::new(NetworkConfig::default());
        let a1 = allocator.allocate();
        let a2 = allocator.allocate();

        let all = allocator.allocated_addresses();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0], a1);
        assert_eq!(all[1], a2);
    }

    #[test]
    fn test_port_allocator_seed_addrs() {
        let config = NetworkConfig {
            base_listen_port: 32000,
            base_gossip_port: 33000,
            host: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        };
        let allocator = PortAllocator::new(config);

        allocator.allocate();
        allocator.allocate();

        let seeds = allocator.seed_addrs();
        assert_eq!(seeds.len(), 2);
        assert_eq!(seeds[0].port(), 33000);
        assert_eq!(seeds[1].port(), 33001);
    }

    #[test]
    fn test_node_address_clone_eq() {
        let addr = NodeAddress {
            listen_addr: "127.0.0.1:5000".parse().unwrap(),
            gossip_addr: "127.0.0.1:5001".parse().unwrap(),
        };
        let cloned = addr.clone();
        assert_eq!(addr, cloned);
    }

    #[test]
    fn test_port_allocator_custom_host() {
        let config = NetworkConfig {
            base_listen_port: 40000,
            base_gossip_port: 41000,
            host: "192.168.1.1".parse().unwrap(),
        };
        let allocator = PortAllocator::new(config);
        let addr = allocator.allocate();
        assert_eq!(addr.listen_addr.ip().to_string(), "192.168.1.1");
    }
}
