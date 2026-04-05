use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OpType {
    Read = 0x01,
    Write = 0x02,
    Flush = 0x03,
    Trim = 0x04,
}

impl OpType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Read),
            0x02 => Some(Self::Write),
            0x03 => Some(Self::Flush),
            0x04 => Some(Self::Trim),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Status {
    Ok = 0x00,
    NotFound = 0x01,
    NoQuorum = 0x02,
    IoError = 0x03,
    InvalidRequest = 0x04,
}

impl Status {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x00 => Some(Self::Ok),
            0x01 => Some(Self::NotFound),
            0x02 => Some(Self::NoQuorum),
            0x03 => Some(Self::IoError),
            0x04 => Some(Self::InvalidRequest),
            _ => None,
        }
    }
}

/// Binary wire format:
///
/// Request:
///   [8B request_id] [1B op_type] [8B volume_id] [8B offset] [4B length] [4B data_len] [data...]
///   Header = 33 bytes
///   `length` = semantic size (bytes to read, bytes being written, etc.)
///   `data_len` = actual payload bytes following the header
///
/// Response:
///   [8B request_id] [1B status] [4B data_len] [data...]
///   Header = 13 bytes
#[derive(Debug, Clone)]
pub struct Request {
    pub request_id: u64,
    pub op: OpType,
    pub volume_id: u64,
    pub offset: u64,
    pub length: u32,
    pub data: Bytes,
}

pub const REQUEST_HEADER_SIZE: usize = 33;
pub const RESPONSE_HEADER_SIZE: usize = 13;

impl Request {
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.request_id);
        buf.put_u8(self.op as u8);
        buf.put_u64(self.volume_id);
        buf.put_u64(self.offset);
        buf.put_u32(self.length);
        buf.put_u32(self.data.len() as u32);
        if !self.data.is_empty() {
            buf.extend_from_slice(&self.data);
        }
    }

    pub fn decode(buf: &mut BytesMut) -> Option<Self> {
        if buf.len() < REQUEST_HEADER_SIZE {
            return None;
        }

        let data_len = {
            let mut peek = &buf[..];
            peek.advance(8 + 1 + 8 + 8 + 4);
            peek.get_u32()
        };

        let total = REQUEST_HEADER_SIZE + data_len as usize;
        if buf.len() < total {
            return None;
        }

        let request_id = buf.get_u64();
        let op_byte = buf.get_u8();
        let op = OpType::from_u8(op_byte)?;
        let volume_id = buf.get_u64();
        let offset = buf.get_u64();
        let length = buf.get_u32();
        let data_len = buf.get_u32();

        let data = if data_len > 0 {
            buf.split_to(data_len as usize).freeze()
        } else {
            Bytes::new()
        };

        Some(Self {
            request_id,
            op,
            volume_id,
            offset,
            length,
            data,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    pub request_id: u64,
    pub status: Status,
    pub data: Bytes,
}

impl Response {
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64(self.request_id);
        buf.put_u8(self.status as u8);
        buf.put_u32(self.data.len() as u32);
        if !self.data.is_empty() {
            buf.extend_from_slice(&self.data);
        }
    }

    pub fn decode(buf: &mut BytesMut) -> Option<Self> {
        if buf.len() < RESPONSE_HEADER_SIZE {
            return None;
        }

        let length = {
            let mut peek = &buf[..];
            peek.advance(8 + 1);
            peek.get_u32()
        };

        let total = RESPONSE_HEADER_SIZE + length as usize;
        if buf.len() < total {
            return None;
        }

        let request_id = buf.get_u64();
        let status_byte = buf.get_u8();
        let status = Status::from_u8(status_byte)?;
        let length = buf.get_u32();

        let data = if length > 0 {
            buf.split_to(length as usize).freeze()
        } else {
            Bytes::new()
        };

        Some(Self {
            request_id,
            status,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_op_type_from_u8() {
        assert_eq!(OpType::from_u8(0x01), Some(OpType::Read));
        assert_eq!(OpType::from_u8(0x02), Some(OpType::Write));
        assert_eq!(OpType::from_u8(0x03), Some(OpType::Flush));
        assert_eq!(OpType::from_u8(0x04), Some(OpType::Trim));
        assert_eq!(OpType::from_u8(0xFF), None);
        assert_eq!(OpType::from_u8(0x00), None);
    }

    #[test]
    fn test_status_from_u8() {
        assert_eq!(Status::from_u8(0x00), Some(Status::Ok));
        assert_eq!(Status::from_u8(0x01), Some(Status::NotFound));
        assert_eq!(Status::from_u8(0x02), Some(Status::NoQuorum));
        assert_eq!(Status::from_u8(0x03), Some(Status::IoError));
        assert_eq!(Status::from_u8(0x04), Some(Status::InvalidRequest));
        assert_eq!(Status::from_u8(0xFF), None);
    }

    #[test]
    fn test_request_encode_decode_read() {
        let req = Request {
            request_id: 42,
            op: OpType::Read,
            volume_id: 100,
            offset: 4096,
            length: 512,
            data: Bytes::new(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        assert_eq!(buf.len(), REQUEST_HEADER_SIZE);

        let decoded = Request::decode(&mut buf).unwrap();
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.op, OpType::Read);
        assert_eq!(decoded.volume_id, 100);
        assert_eq!(decoded.offset, 4096);
        assert_eq!(decoded.length, 512);
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn test_request_encode_decode_write() {
        let data = Bytes::from(vec![0xAB; 512]);
        let req = Request {
            request_id: 1,
            op: OpType::Write,
            volume_id: 5,
            offset: 0,
            length: 512,
            data: data.clone(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        assert_eq!(buf.len(), REQUEST_HEADER_SIZE + 512);

        let decoded = Request::decode(&mut buf).unwrap();
        assert_eq!(decoded.request_id, 1);
        assert_eq!(decoded.op, OpType::Write);
        assert_eq!(decoded.length, 512);
        assert_eq!(decoded.data, data);
    }

    #[test]
    fn test_request_decode_incomplete_header() {
        let mut buf = BytesMut::from(&[0u8; 10][..]);
        assert!(Request::decode(&mut buf).is_none());
    }

    #[test]
    fn test_request_decode_incomplete_data() {
        let req = Request {
            request_id: 1,
            op: OpType::Write,
            volume_id: 1,
            offset: 0,
            length: 1024,
            data: Bytes::from(vec![0u8; 1024]),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        buf.truncate(REQUEST_HEADER_SIZE + 500);
        assert!(Request::decode(&mut buf).is_none());
    }

    #[test]
    fn test_request_flush() {
        let req = Request {
            request_id: 99,
            op: OpType::Flush,
            volume_id: 10,
            offset: 0,
            length: 0,
            data: Bytes::new(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        let decoded = Request::decode(&mut buf).unwrap();
        assert_eq!(decoded.op, OpType::Flush);
    }

    #[test]
    fn test_request_trim() {
        let req = Request {
            request_id: 7,
            op: OpType::Trim,
            volume_id: 3,
            offset: 8192,
            length: 4096,
            data: Bytes::new(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf);
        let decoded = Request::decode(&mut buf).unwrap();
        assert_eq!(decoded.op, OpType::Trim);
        assert_eq!(decoded.offset, 8192);
        assert_eq!(decoded.length, 4096);
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn test_response_encode_decode_ok_empty() {
        let resp = Response {
            request_id: 42,
            status: Status::Ok,
            data: Bytes::new(),
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf);
        assert_eq!(buf.len(), RESPONSE_HEADER_SIZE);

        let decoded = Response::decode(&mut buf).unwrap();
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.status, Status::Ok);
        assert!(decoded.data.is_empty());
    }

    #[test]
    fn test_response_encode_decode_with_data() {
        let data = Bytes::from(vec![0xCD; 256]);
        let resp = Response {
            request_id: 10,
            status: Status::Ok,
            data: data.clone(),
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf);
        assert_eq!(buf.len(), RESPONSE_HEADER_SIZE + 256);

        let decoded = Response::decode(&mut buf).unwrap();
        assert_eq!(decoded.request_id, 10);
        assert_eq!(decoded.data, data);
    }

    #[test]
    fn test_response_decode_incomplete() {
        let mut buf = BytesMut::from(&[0u8; 5][..]);
        assert!(Response::decode(&mut buf).is_none());
    }

    #[test]
    fn test_response_error_statuses() {
        for status in [
            Status::NotFound,
            Status::NoQuorum,
            Status::IoError,
            Status::InvalidRequest,
        ] {
            let resp = Response {
                request_id: 1,
                status,
                data: Bytes::new(),
            };
            let mut buf = BytesMut::new();
            resp.encode(&mut buf);
            let decoded = Response::decode(&mut buf).unwrap();
            assert_eq!(decoded.status, status);
        }
    }

    #[test]
    fn test_multiple_requests_in_buffer() {
        let mut buf = BytesMut::new();
        for i in 0..3 {
            let req = Request {
                request_id: i,
                op: OpType::Read,
                volume_id: 1,
                offset: i * 4096,
                length: 4096,
                data: Bytes::new(),
            };
            req.encode(&mut buf);
        }
        assert_eq!(buf.len(), REQUEST_HEADER_SIZE * 3);

        for i in 0..3 {
            let decoded = Request::decode(&mut buf).unwrap();
            assert_eq!(decoded.request_id, i);
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn test_multiple_responses_in_buffer() {
        let mut buf = BytesMut::new();
        for i in 0..3 {
            let resp = Response {
                request_id: i,
                status: Status::Ok,
                data: Bytes::from(vec![i as u8; 4]),
            };
            resp.encode(&mut buf);
        }

        for i in 0..3 {
            let decoded = Response::decode(&mut buf).unwrap();
            assert_eq!(decoded.request_id, i);
            assert_eq!(decoded.data.len(), 4);
        }
        assert!(buf.is_empty());
    }
}
