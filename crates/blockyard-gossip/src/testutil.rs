//! Test utilities for the gossip crate.
//!
//! Provides an in-memory transport implementation for testing the SWIM
//! protocol without real network I/O.

use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::protocol::GossipMessage;
use crate::transport::{GossipTransport, TransportError};

/// In-memory transport for unit tests.
///
/// Captures all sent messages and allows feeding received messages.
#[derive(Debug)]
pub struct InMemoryTransport {
    addr: SocketAddr,
    sent: Arc<Mutex<Vec<(GossipMessage, SocketAddr)>>>,
    rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<(GossipMessage, SocketAddr)>>,
    tx: mpsc::UnboundedSender<(GossipMessage, SocketAddr)>,
}

impl InMemoryTransport {
    /// Create a new in-memory transport with the given local address.
    pub fn new(addr: SocketAddr) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            addr,
            sent: Arc::new(Mutex::new(Vec::new())),
            rx: tokio::sync::Mutex::new(rx),
            tx,
        }
    }

    /// Get a copy of all messages that have been sent.
    pub fn sent_messages(&self) -> Vec<(GossipMessage, SocketAddr)> {
        self.sent.lock().clone()
    }

    /// Feed a message into the receive queue.
    pub fn inject_message(&self, msg: GossipMessage, from: SocketAddr) {
        let _ = self.tx.send((msg, from));
    }

    /// Return the send channel for feeding messages from another context.
    pub fn sender(&self) -> mpsc::UnboundedSender<(GossipMessage, SocketAddr)> {
        self.tx.clone()
    }

    /// Clear the sent message log.
    pub fn clear_sent(&self) {
        self.sent.lock().clear();
    }
}

impl GossipTransport for InMemoryTransport {
    async fn send_to(&self, msg: &GossipMessage, addr: SocketAddr) -> Result<(), TransportError> {
        self.sent.lock().push((msg.clone(), addr));
        Ok(())
    }

    async fn recv_from(&self) -> Result<(GossipMessage, SocketAddr), TransportError> {
        let mut rx = self.rx.lock().await;
        rx.recv().await.ok_or_else(|| {
            TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "channel closed",
            ))
        })
    }

    fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockyard_common::NodeId;

    fn test_addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    #[test]
    fn test_in_memory_transport_new() {
        let t = InMemoryTransport::new(test_addr());
        assert_eq!(t.local_addr(), test_addr());
        assert!(t.sent_messages().is_empty());
    }

    #[tokio::test]
    async fn test_in_memory_transport_send() {
        let t = InMemoryTransport::new(test_addr());
        let msg = GossipMessage::Join {
            node_id: NodeId::generate(),
            addr: test_addr(),
        };
        let dest: SocketAddr = "127.0.0.1:9001".parse().unwrap();

        t.send_to(&msg, dest).await.unwrap();

        let sent = t.sent_messages();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, msg);
        assert_eq!(sent[0].1, dest);
    }

    #[tokio::test]
    async fn test_in_memory_transport_recv() {
        let t = InMemoryTransport::new(test_addr());
        let msg = GossipMessage::Join {
            node_id: NodeId::generate(),
            addr: test_addr(),
        };
        let from: SocketAddr = "127.0.0.1:9001".parse().unwrap();

        t.inject_message(msg.clone(), from);

        let (received, received_from) = GossipTransport::recv_from(&t).await.unwrap();
        assert_eq!(received, msg);
        assert_eq!(received_from, from);
    }

    #[tokio::test]
    async fn test_in_memory_transport_clear_sent() {
        let t = InMemoryTransport::new(test_addr());
        let msg = GossipMessage::Join {
            node_id: NodeId::generate(),
            addr: test_addr(),
        };

        t.send_to(&msg, test_addr()).await.unwrap();
        assert_eq!(t.sent_messages().len(), 1);

        t.clear_sent();
        assert!(t.sent_messages().is_empty());
    }

    #[test]
    fn test_in_memory_transport_sender() {
        let t = InMemoryTransport::new(test_addr());
        let sender = t.sender();
        let msg = GossipMessage::Join {
            node_id: NodeId::generate(),
            addr: test_addr(),
        };
        sender.send((msg, test_addr())).unwrap();
    }

    #[test]
    fn test_in_memory_transport_debug() {
        let t = InMemoryTransport::new(test_addr());
        let dbg = format!("{:?}", t);
        assert!(dbg.contains("InMemoryTransport"));
    }

    #[tokio::test]
    async fn test_in_memory_transport_multiple_sends() {
        let t = InMemoryTransport::new(test_addr());
        for i in 0..5 {
            let msg = GossipMessage::Ping {
                from: NodeId::generate(),
                from_addr: test_addr(),
                seq: i,
                updates: vec![],
            };
            t.send_to(&msg, test_addr()).await.unwrap();
        }
        assert_eq!(t.sent_messages().len(), 5);
    }
}
