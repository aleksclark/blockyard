use crate::wire::{OpType, Request, Response, Status, RESPONSE_HEADER_SIZE};
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

pub trait RequestHandler: Send + Sync + 'static {
    fn handle_read(
        &self,
        volume_id: u64,
        offset: u64,
        length: u32,
    ) -> impl std::future::Future<Output = Result<Bytes, Status>> + Send;

    fn handle_write(
        &self,
        volume_id: u64,
        offset: u64,
        data: Bytes,
    ) -> impl std::future::Future<Output = Result<(), Status>> + Send;

    fn handle_flush(
        &self,
        volume_id: u64,
    ) -> impl std::future::Future<Output = Result<(), Status>> + Send;

    fn handle_trim(
        &self,
        volume_id: u64,
        offset: u64,
        length: u32,
    ) -> impl std::future::Future<Output = Result<(), Status>> + Send;
}

pub struct ProtocolServer<H: RequestHandler> {
    listen_addr: SocketAddr,
    handler: Arc<H>,
}

impl<H: RequestHandler> ProtocolServer<H> {
    pub fn new(listen_addr: SocketAddr, handler: H) -> Self {
        Self {
            listen_addr,
            handler: Arc::new(handler),
        }
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    pub async fn run(&self) -> blockyard_common::Result<()> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        let local_addr = listener.local_addr()?;
        info!(addr = %local_addr, "protocol server listening");

        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    debug!(peer = %peer, "accepted connection");
                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, handler).await {
                            warn!(peer = %peer, error = %e, "connection error");
                        }
                    });
                }
                Err(e) => {
                    error!(error = %e, "accept failed");
                }
            }
        }
    }

    async fn handle_connection(
        mut stream: TcpStream,
        handler: Arc<H>,
    ) -> blockyard_common::Result<()> {
        stream.set_nodelay(true)?;
        let mut buf = BytesMut::with_capacity(64 * 1024);

        loop {
            let n = stream.read_buf(&mut buf).await?;
            if n == 0 {
                return Ok(());
            }

            while let Some(req) = Request::decode(&mut buf) {
                let response = Self::dispatch(&handler, &req).await;
                let mut resp_buf = BytesMut::with_capacity(RESPONSE_HEADER_SIZE + response.data.len());
                response.encode(&mut resp_buf);
                stream.write_all(&resp_buf).await?;
                stream.flush().await?;
            }
        }
    }

    async fn dispatch(handler: &H, req: &Request) -> Response {
        let request_id = req.request_id;

        match req.op {
            OpType::Read => match handler.handle_read(req.volume_id, req.offset, req.length).await
            {
                Ok(data) => Response {
                    request_id,
                    status: Status::Ok,
                    data,
                },
                Err(status) => Response {
                    request_id,
                    status,
                    data: Bytes::new(),
                },
            },
            OpType::Write => {
                match handler
                    .handle_write(req.volume_id, req.offset, req.data.clone())
                    .await
                {
                    Ok(()) => Response {
                        request_id,
                        status: Status::Ok,
                        data: Bytes::new(),
                    },
                    Err(status) => Response {
                        request_id,
                        status,
                        data: Bytes::new(),
                    },
                }
            }
            OpType::Flush => match handler.handle_flush(req.volume_id).await {
                Ok(()) => Response {
                    request_id,
                    status: Status::Ok,
                    data: Bytes::new(),
                },
                Err(status) => Response {
                    request_id,
                    status,
                    data: Bytes::new(),
                },
            },
            OpType::Trim => {
                match handler
                    .handle_trim(req.volume_id, req.offset, req.length)
                    .await
                {
                    Ok(()) => Response {
                        request_id,
                        status: Status::Ok,
                        data: Bytes::new(),
                    },
                    Err(status) => Response {
                        request_id,
                        status,
                        data: Bytes::new(),
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct MemHandler {
        data: Arc<Mutex<HashMap<(u64, u64), Vec<u8>>>>,
    }

    impl MemHandler {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    impl RequestHandler for MemHandler {
        async fn handle_read(
            &self,
            volume_id: u64,
            offset: u64,
            length: u32,
        ) -> Result<Bytes, Status> {
            let map = self.data.lock();
            match map.get(&(volume_id, offset)) {
                Some(d) => Ok(Bytes::copy_from_slice(&d[..length as usize])),
                None => Ok(Bytes::from(vec![0u8; length as usize])),
            }
        }

        async fn handle_write(
            &self,
            volume_id: u64,
            offset: u64,
            data: Bytes,
        ) -> Result<(), Status> {
            self.data
                .lock()
                .insert((volume_id, offset), data.to_vec());
            Ok(())
        }

        async fn handle_flush(&self, _volume_id: u64) -> Result<(), Status> {
            Ok(())
        }

        async fn handle_trim(
            &self,
            volume_id: u64,
            offset: u64,
            _length: u32,
        ) -> Result<(), Status> {
            self.data.lock().remove(&(volume_id, offset));
            Ok(())
        }
    }

    #[test]
    fn test_protocol_server_new() {
        let handler = MemHandler::new();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let server = ProtocolServer::new(addr, handler);
        assert_eq!(server.listen_addr(), addr);
    }

    #[tokio::test]
    async fn test_dispatch_read() {
        let handler = MemHandler::new();
        let req = Request {
            request_id: 1,
            op: OpType::Read,
            volume_id: 1,
            offset: 0,
            length: 4,
            data: Bytes::new(),
        };
        let resp = ProtocolServer::<MemHandler>::dispatch(&handler, &req).await;
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(resp.data.len(), 4);
    }

    #[tokio::test]
    async fn test_dispatch_write_then_read() {
        let handler = MemHandler::new();
        let write_req = Request {
            request_id: 1,
            op: OpType::Write,
            volume_id: 1,
            offset: 0,
            length: 4,
            data: Bytes::from(vec![0xAA, 0xBB, 0xCC, 0xDD]),
        };
        let resp = ProtocolServer::<MemHandler>::dispatch(&handler, &write_req).await;
        assert_eq!(resp.status, Status::Ok);

        let read_req = Request {
            request_id: 2,
            op: OpType::Read,
            volume_id: 1,
            offset: 0,
            length: 4,
            data: Bytes::new(),
        };
        let resp = ProtocolServer::<MemHandler>::dispatch(&handler, &read_req).await;
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(resp.data, Bytes::from(vec![0xAA, 0xBB, 0xCC, 0xDD]));
    }

    #[tokio::test]
    async fn test_dispatch_flush() {
        let handler = MemHandler::new();
        let req = Request {
            request_id: 1,
            op: OpType::Flush,
            volume_id: 1,
            offset: 0,
            length: 0,
            data: Bytes::new(),
        };
        let resp = ProtocolServer::<MemHandler>::dispatch(&handler, &req).await;
        assert_eq!(resp.status, Status::Ok);
    }

    #[tokio::test]
    async fn test_dispatch_trim() {
        let handler = MemHandler::new();
        handler.data.lock().insert((1, 0), vec![1, 2, 3, 4]);

        let req = Request {
            request_id: 1,
            op: OpType::Trim,
            volume_id: 1,
            offset: 0,
            length: 4,
            data: Bytes::new(),
        };
        let resp = ProtocolServer::<MemHandler>::dispatch(&handler, &req).await;
        assert_eq!(resp.status, Status::Ok);
        assert!(handler.data.lock().get(&(1, 0)).is_none());
    }

    async fn read_response(
        client: &mut TcpStream,
        resp_buf: &mut BytesMut,
    ) -> Response {
        loop {
            let n = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                client.read_buf(resp_buf),
            )
            .await
            .expect("read_response timed out")
            .unwrap();
            assert!(n > 0, "unexpected EOF from server");
            if let Some(resp) = Response::decode(resp_buf) {
                return resp;
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_end_to_end_tcp() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let handler = Arc::new(MemHandler::new());

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let h = handler.clone();
            tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                ProtocolServer::<MemHandler>::handle_connection(stream, h)
                    .await
                    .ok();
            });

            tokio::task::yield_now().await;

            let mut client = TcpStream::connect(addr).await.unwrap();
            client.set_nodelay(true).unwrap();
            let mut resp_buf = BytesMut::with_capacity(256);

            // Write
            let mut buf = BytesMut::new();
            Request {
                request_id: 1,
                op: OpType::Write,
                volume_id: 1,
                offset: 0,
                length: 4,
                data: Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            }
            .encode(&mut buf);
            client.write_all(&buf).await.unwrap();
            client.flush().await.unwrap();

            let resp = read_response(&mut client, &mut resp_buf).await;
            assert_eq!(resp.request_id, 1);
            assert_eq!(resp.status, Status::Ok);

            // Read back
            let mut buf = BytesMut::new();
            Request {
                request_id: 2,
                op: OpType::Read,
                volume_id: 1,
                offset: 0,
                length: 4,
                data: Bytes::new(),
            }
            .encode(&mut buf);
            client.write_all(&buf).await.unwrap();
            client.flush().await.unwrap();

            let resp = read_response(&mut client, &mut resp_buf).await;
            assert_eq!(resp.request_id, 2);
            assert_eq!(resp.status, Status::Ok);
            assert_eq!(resp.data, Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        })
        .await
        .expect("test timed out");
    }
}
