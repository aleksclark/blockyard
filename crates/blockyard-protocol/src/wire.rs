use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OpType {
    Read = 0x01,
    Write = 0x02,
    Flush = 0x03,
    Trim = 0x04,
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

#[derive(Debug, Clone)]
pub struct Request {
    pub request_id: u64,
    pub op: OpType,
    pub volume_id: u64,
    pub offset: u64,
    pub length: u32,
    pub data: Bytes,
}

#[derive(Debug, Clone)]
pub struct Response {
    pub request_id: u64,
    pub status: Status,
    pub data: Bytes,
}
