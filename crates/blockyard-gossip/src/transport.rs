//! Transport abstraction for gossip protocol communication.
//!
//! The [`GossipTransport`] trait allows swapping the real UDP transport
//! for an in-memory implementation during testing.

use std::net::SocketAddr;

use crate::protocol::GossipMessage;

/// Errors from the transport layer.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("message too large: {size} bytes (max {max})")]
    MessageTooLarge { size: usize, max: usize },
}

/// Maximum UDP datagram payload size for gossip messages.
pub const MAX_MESSAGE_SIZE: usize = 65_000;

/// Trait for sending and receiving gossip messages.
pub trait GossipTransport: Send + Sync + 'static {
    fn send_to(
        &self,
        msg: &GossipMessage,
        addr: SocketAddr,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> + Send;

    fn recv_from(
        &self,
    ) -> impl std::future::Future<Output = Result<(GossipMessage, SocketAddr), TransportError>> + Send;

    fn local_addr(&self) -> SocketAddr;
}

/// UDP-based transport using tokio.
#[derive(Debug)]
pub struct UdpTransport {
    socket: tokio::net::UdpSocket,
}

impl UdpTransport {
    /// Bind a new UDP transport to the given address.
    pub async fn bind(addr: SocketAddr) -> Result<Self, TransportError> {
        let socket = tokio::net::UdpSocket::bind(addr).await?;
        Ok(Self { socket })
    }
}

impl GossipTransport for UdpTransport {
    async fn send_to(&self, msg: &GossipMessage, addr: SocketAddr) -> Result<(), TransportError> {
        let data = msg.encode()?;
        if data.len() > MAX_MESSAGE_SIZE {
            return Err(TransportError::MessageTooLarge {
                size: data.len(),
                max: MAX_MESSAGE_SIZE,
            });
        }
        self.socket.send_to(&data, addr).await?;
        Ok(())
    }

    async fn recv_from(&self) -> Result<(GossipMessage, SocketAddr), TransportError> {
        let mut buf = vec![0u8; MAX_MESSAGE_SIZE];
        let (len, addr) = self.socket.recv_from(&mut buf).await?;
        let msg = GossipMessage::decode(&buf[..len])?;
        Ok((msg, addr))
    }

    fn local_addr(&self) -> SocketAddr {
        self.socket
            .local_addr()
            .expect("bound socket must have local address")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockyard_common::NodeId;

    #[tokio::test]
    async fn test_udp_transport_bind() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport = UdpTransport::bind(addr).await.unwrap();
        let local = transport.local_addr();
        assert_eq!(local.ip(), std::net::Ipv4Addr::LOCALHOST);
        assert_ne!(local.port(), 0);
    }

    #[tokio::test]
    async fn test_udp_transport_send_recv() {
        let t1 = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let t2 = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let msg = GossipMessage::Ping {
            from: NodeId::generate(),
            from_addr: t1.local_addr(),
            seq: 42,
            updates: vec![],
        };

        t1.send_to(&msg, t2.local_addr()).await.unwrap();
        let (received, from_addr) = t2.recv_from().await.unwrap();

        assert_eq!(received, msg);
        assert_eq!(from_addr, t1.local_addr());
    }

    #[tokio::test]
    async fn test_udp_transport_join_message() {
        let t1 = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let t2 = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let msg = GossipMessage::Join {
            node_id: NodeId::generate(),
            addr: t1.local_addr(),
        };

        t1.send_to(&msg, t2.local_addr()).await.unwrap();
        let (received, _) = t2.recv_from().await.unwrap();
        assert_eq!(received, msg);
    }

    #[tokio::test]
    async fn test_udp_transport_ack_message() {
        let t1 = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let t2 = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let msg = GossipMessage::Ack {
            from: NodeId::generate(),
            from_addr: t1.local_addr(),
            seq: 7,
            updates: vec![],
        };

        t1.send_to(&msg, t2.local_addr()).await.unwrap();
        let (received, _) = t2.recv_from().await.unwrap();
        assert_eq!(received, msg);
    }

    #[test]
    fn test_transport_error_display() {
        let err = TransportError::Io(std::io::Error::new(std::io::ErrorKind::AddrInUse, "in use"));
        assert!(err.to_string().contains("in use"));
    }

    #[test]
    fn test_transport_error_message_too_large() {
        let err = TransportError::MessageTooLarge {
            size: 100_000,
            max: 65_000,
        };
        assert!(err.to_string().contains("100000"));
        assert!(err.to_string().contains("65000"));
    }

    #[test]
    fn test_transport_error_serialize() {
        let err: Result<GossipMessage, _> = serde_json::from_slice(b"not json");
        let transport_err = TransportError::Serialize(err.unwrap_err());
        assert!(transport_err.to_string().contains("serialization"));
    }

    #[test]
    fn test_transport_error_debug() {
        let err = TransportError::MessageTooLarge { size: 1, max: 0 };
        let dbg = format!("{:?}", err);
        assert!(dbg.contains("MessageTooLarge"));
    }

    #[test]
    fn test_max_message_size_constant() {
        assert_eq!(MAX_MESSAGE_SIZE, 65_000);
    }

    #[tokio::test]
    async fn test_udp_transport_debug() {
        let t = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let dbg = format!("{:?}", t);
        assert!(dbg.contains("UdpTransport"));
    }

    #[test]
    fn test_transport_error_from_io() {
        let io_err = std::io::Error::other("test");
        let err: TransportError = io_err.into();
        assert!(matches!(err, TransportError::Io(_)));
    }

    #[test]
    fn test_transport_error_from_serde() {
        let serde_err = serde_json::from_slice::<GossipMessage>(b"bad").unwrap_err();
        let err: TransportError = serde_err.into();
        assert!(matches!(err, TransportError::Serialize(_)));
    }
}
