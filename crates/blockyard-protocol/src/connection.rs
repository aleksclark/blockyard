use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::{debug, warn};

pub struct ConnectionPool {
    connections: Arc<Mutex<HashMap<SocketAddr, Vec<TcpStream>>>>,
    max_per_host: usize,
}

impl ConnectionPool {
    pub fn new(max_per_host: usize) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            max_per_host,
        }
    }

    pub async fn get(&self, addr: SocketAddr) -> blockyard_common::Result<TcpStream> {
        {
            let mut pool = self.connections.lock();
            if let Some(conns) = pool.get_mut(&addr) {
                if let Some(stream) = conns.pop() {
                    debug!(addr = %addr, "reusing pooled connection");
                    return Ok(stream);
                }
            }
        }

        debug!(addr = %addr, "opening new connection");
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    pub fn put(&self, addr: SocketAddr, stream: TcpStream) {
        let mut pool = self.connections.lock();
        let conns = pool.entry(addr).or_default();
        if conns.len() < self.max_per_host {
            conns.push(stream);
        } else {
            warn!(addr = %addr, "connection pool full, dropping connection");
        }
    }

    pub fn pool_size(&self, addr: SocketAddr) -> usize {
        self.connections.lock().get(&addr).map_or(0, |c| c.len())
    }

    pub fn total_connections(&self) -> usize {
        self.connections.lock().values().map(|v| v.len()).sum()
    }

    pub fn clear(&self) {
        self.connections.lock().clear();
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new(4)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_new() {
        let pool = ConnectionPool::new(8);
        assert_eq!(pool.total_connections(), 0);
    }

    #[test]
    fn test_pool_default() {
        let pool = ConnectionPool::default();
        assert_eq!(pool.max_per_host, 4);
    }

    #[tokio::test]
    async fn test_pool_get_creates_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new(4);

        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });

        let stream = pool.get(addr).await.unwrap();
        let remote = stream.peer_addr().unwrap();
        assert_eq!(remote, addr);

        accept.await.unwrap();
    }

    #[tokio::test]
    async fn test_pool_put_and_reuse() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let pool = ConnectionPool::new(4);

        let accept = tokio::spawn(async move { listener.accept().await.unwrap() });

        let stream = pool.get(addr).await.unwrap();
        accept.await.unwrap();

        pool.put(addr, stream);
        assert_eq!(pool.pool_size(addr), 1);
        assert_eq!(pool.total_connections(), 1);
    }

    #[test]
    fn test_pool_size_empty() {
        let pool = ConnectionPool::new(4);
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        assert_eq!(pool.pool_size(addr), 0);
    }

    #[test]
    fn test_pool_clear() {
        let pool = ConnectionPool::new(4);
        pool.clear();
        assert_eq!(pool.total_connections(), 0);
    }
}
