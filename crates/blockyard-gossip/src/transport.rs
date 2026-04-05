use std::net::SocketAddr;

pub trait Transport: Send + Sync {
    fn send_to(
        &self,
        data: &[u8],
        target: SocketAddr,
    ) -> impl std::future::Future<Output = blockyard_common::Result<()>> + Send;

    fn recv_from(
        &self,
    ) -> impl std::future::Future<Output = blockyard_common::Result<(Vec<u8>, SocketAddr)>> + Send;

    fn local_addr(&self) -> blockyard_common::Result<SocketAddr>;
}

pub struct UdpTransport {
    socket: tokio::net::UdpSocket,
}

impl UdpTransport {
    pub async fn bind(addr: SocketAddr) -> blockyard_common::Result<Self> {
        let socket = tokio::net::UdpSocket::bind(addr).await?;
        Ok(Self { socket })
    }
}

impl Transport for UdpTransport {
    async fn send_to(&self, data: &[u8], target: SocketAddr) -> blockyard_common::Result<()> {
        self.socket.send_to(data, target).await?;
        Ok(())
    }

    async fn recv_from(&self) -> blockyard_common::Result<(Vec<u8>, SocketAddr)> {
        let mut buf = vec![0u8; 65536];
        let (n, addr) = self.socket.recv_from(&mut buf).await?;
        buf.truncate(n);
        Ok((buf, addr))
    }

    fn local_addr(&self) -> blockyard_common::Result<SocketAddr> {
        Ok(self.socket.local_addr()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_udp_transport_bind() {
        let t = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = t.local_addr().unwrap();
        assert_ne!(addr.port(), 0);
    }

    #[tokio::test]
    async fn test_udp_transport_send_recv() {
        let t1 = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let t2 = UdpTransport::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr2 = t2.local_addr().unwrap();

        t1.send_to(b"hello", addr2).await.unwrap();
        let (data, from) = t2.recv_from().await.unwrap();
        assert_eq!(data, b"hello");
        assert_eq!(from, t1.local_addr().unwrap());
    }
}
