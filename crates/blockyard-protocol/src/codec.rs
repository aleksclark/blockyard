use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::wire::{Request, Response, REQUEST_HEADER_SIZE, RESPONSE_HEADER_SIZE};

pub struct BlockProtocolCodec {
    is_server: bool,
}

impl BlockProtocolCodec {
    pub fn server() -> Self {
        Self { is_server: true }
    }

    pub fn client() -> Self {
        Self { is_server: false }
    }
}

impl Decoder for BlockProtocolCodec {
    type Item = RequestOrResponse;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if self.is_server {
            if src.len() < REQUEST_HEADER_SIZE {
                return Ok(None);
            }
            match Request::decode(src) {
                Some(req) => Ok(Some(RequestOrResponse::Req(req))),
                None => Ok(None),
            }
        } else {
            if src.len() < RESPONSE_HEADER_SIZE {
                return Ok(None);
            }
            match Response::decode(src) {
                Some(resp) => Ok(Some(RequestOrResponse::Resp(resp))),
                None => Ok(None),
            }
        }
    }
}

impl Encoder<Request> for BlockProtocolCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

impl Encoder<Response> for BlockProtocolCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

#[derive(Debug)]
pub enum RequestOrResponse {
    Req(Request),
    Resp(Response),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{OpType, Status};
    use bytes::Bytes;

    #[test]
    fn test_server_codec_decode_request() {
        let mut codec = BlockProtocolCodec::server();
        let req = Request {
            request_id: 1,
            op: OpType::Write,
            volume_id: 10,
            offset: 0,
            length: 4,
            data: Bytes::from(vec![0xAA; 4]),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        match decoded {
            RequestOrResponse::Req(r) => {
                assert_eq!(r.request_id, 1);
                assert_eq!(r.op, OpType::Write);
            }
            _ => panic!("expected Request"),
        }
    }

    #[test]
    fn test_client_codec_decode_response() {
        let mut codec = BlockProtocolCodec::client();
        let resp = Response {
            request_id: 5,
            status: Status::Ok,
            data: Bytes::from(vec![0xBB; 8]),
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf);

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        match decoded {
            RequestOrResponse::Resp(r) => {
                assert_eq!(r.request_id, 5);
                assert_eq!(r.status, Status::Ok);
                assert_eq!(r.data.len(), 8);
            }
            _ => panic!("expected Response"),
        }
    }

    #[test]
    fn test_codec_decode_partial() {
        let mut codec = BlockProtocolCodec::server();
        let mut buf = BytesMut::from(&[0u8; 5][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn test_codec_encode_request() {
        let mut codec = BlockProtocolCodec::client();
        let req = Request {
            request_id: 1,
            op: OpType::Read,
            volume_id: 1,
            offset: 0,
            length: 0,
            data: Bytes::new(),
        };
        let mut buf = BytesMut::new();
        Encoder::<Request>::encode(&mut codec, req, &mut buf).unwrap();
        assert_eq!(buf.len(), REQUEST_HEADER_SIZE);
    }

    #[test]
    fn test_codec_encode_response() {
        let mut codec = BlockProtocolCodec::server();
        let resp = Response {
            request_id: 1,
            status: Status::IoError,
            data: Bytes::new(),
        };
        let mut buf = BytesMut::new();
        Encoder::<Response>::encode(&mut codec, resp, &mut buf).unwrap();
        assert_eq!(buf.len(), RESPONSE_HEADER_SIZE);
    }
}
