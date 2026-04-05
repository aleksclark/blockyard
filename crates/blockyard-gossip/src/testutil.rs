use crate::transport::Transport;
use std::net::SocketAddr;
use tokio::sync::mpsc;

pub struct InMemoryTransport {
    addr: SocketAddr,
    tx: mpsc::UnboundedSender<(Vec<u8>, SocketAddr, SocketAddr)>,
    rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<(Vec<u8>, SocketAddr)>>,
}

#[derive(Clone)]
pub struct InMemoryNetwork {
    tx: mpsc::UnboundedSender<(Vec<u8>, SocketAddr, SocketAddr)>,
}

impl InMemoryNetwork {
    pub fn new() -> (
        Self,
        mpsc::UnboundedReceiver<(Vec<u8>, SocketAddr, SocketAddr)>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    pub fn create_transport(
        &self,
        addr: SocketAddr,
    ) -> (
        InMemoryTransport,
        mpsc::UnboundedSender<(Vec<u8>, SocketAddr)>,
    ) {
        let (deliver_tx, deliver_rx) = mpsc::unbounded_channel();
        let transport = InMemoryTransport {
            addr,
            tx: self.tx.clone(),
            rx: tokio::sync::Mutex::new(deliver_rx),
        };
        (transport, deliver_tx)
    }
}

impl Transport for InMemoryTransport {
    async fn send_to(&self, data: &[u8], target: SocketAddr) -> blockyard_common::Result<()> {
        self.tx
            .send((data.to_vec(), self.addr, target))
            .map_err(|e| blockyard_common::Error::Gossip(format!("send failed: {e}")))?;
        Ok(())
    }

    async fn recv_from(&self) -> blockyard_common::Result<(Vec<u8>, SocketAddr)> {
        self.rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| blockyard_common::Error::Gossip("channel closed".into()))
    }

    fn local_addr(&self) -> blockyard_common::Result<SocketAddr> {
        Ok(self.addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_transport() {
        let (net, mut router_rx) = InMemoryNetwork::new();

        let addr1: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:1002".parse().unwrap();

        let (t1, _deliver1) = net.create_transport(addr1);
        let (_t2, deliver2) = net.create_transport(addr2);

        t1.send_to(b"test-msg", addr2).await.unwrap();

        let (data, from, to) = router_rx.recv().await.unwrap();
        assert_eq!(data, b"test-msg");
        assert_eq!(from, addr1);
        assert_eq!(to, addr2);

        deliver2.send((data, from)).unwrap();

        let (received, received_from) = _t2.recv_from().await.unwrap();
        assert_eq!(received, b"test-msg");
        assert_eq!(received_from, addr1);
    }

    #[test]
    fn test_in_memory_transport_local_addr() {
        let (net, _rx) = InMemoryNetwork::new();
        let addr: SocketAddr = "10.0.0.1:9999".parse().unwrap();
        let (t, _deliver) = net.create_transport(addr);
        assert_eq!(t.local_addr().unwrap(), addr);
    }
}
