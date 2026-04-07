//! Mock implementations for testing the read pipeline.

use std::collections::HashMap;
use std::sync::Arc;

use blockyard_common::{EpochId, ExtentId, NodeId, SessionId, VolumeId};
use bytes::Bytes;
use parking_lot::Mutex;

use crate::error::ReadError;
use crate::traits::{DataNodeReader, HealthReporter, MetadataProvider};
use crate::types::{
    CorruptionReport, DataNodeReadResult, ExtentMapping, ReadFailureReport,
};

/// Defines how a mock data node responds to reads.
#[derive(Debug, Clone)]
pub enum NodeBehavior {
    Success { checksum: String, data: Bytes },
    Fail(String),
}

/// Mock metadata provider for testing.
#[derive(Debug, Clone)]
pub struct MockMetadata {
    mappings: HashMap<(VolumeId, ExtentId), ExtentMapping>,
    refreshed: HashMap<(VolumeId, ExtentId), ExtentMapping>,
    watermark: u64,
}

impl MockMetadata {
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
            refreshed: HashMap::new(),
            watermark: 0,
        }
    }

    pub fn with_mapping(mut self, vol: VolumeId, ext: ExtentId, m: ExtentMapping) -> Self {
        self.mappings.insert((vol, ext), m);
        self
    }

    pub fn with_refreshed_mapping(
        mut self,
        vol: VolumeId,
        ext: ExtentId,
        m: ExtentMapping,
    ) -> Self {
        self.refreshed.insert((vol, ext), m);
        self
    }

    pub fn with_watermark(mut self, w: u64) -> Self {
        self.watermark = w;
        self
    }
}

impl MetadataProvider for MockMetadata {
    async fn get_extent_mapping(
        &self,
        volume_id: VolumeId,
        extent_id: ExtentId,
    ) -> Result<Option<ExtentMapping>, ReadError> {
        Ok(self.mappings.get(&(volume_id, extent_id)).cloned())
    }

    async fn get_write_watermark(
        &self,
        _session_id: SessionId,
        _volume_id: VolumeId,
    ) -> Result<u64, ReadError> {
        Ok(self.watermark)
    }

    async fn refresh_extent_mapping(
        &self,
        volume_id: VolumeId,
        extent_id: ExtentId,
    ) -> Result<Option<ExtentMapping>, ReadError> {
        Ok(self.refreshed.get(&(volume_id, extent_id)).cloned())
    }
}

/// Mock data node reader for testing.
#[derive(Debug, Clone)]
pub struct MockDataReader {
    behaviors: HashMap<NodeId, NodeBehavior>,
}

impl MockDataReader {
    pub fn new() -> Self {
        Self {
            behaviors: HashMap::new(),
        }
    }

    pub fn with_node_behavior(mut self, node: NodeId, behavior: NodeBehavior) -> Self {
        self.behaviors.insert(node, behavior);
        self
    }
}

impl DataNodeReader for MockDataReader {
    async fn read_extent(
        &self,
        node_id: NodeId,
        _volume_id: VolumeId,
        extent_id: ExtentId,
        extent_version: u64,
        _offset: u64,
        _length: u64,
    ) -> Result<DataNodeReadResult, ReadError> {
        match self.behaviors.get(&node_id) {
            Some(NodeBehavior::Success { checksum, data }) => Ok(DataNodeReadResult {
                extent_id,
                extent_version,
                checksum: checksum.clone(),
                data: data.clone(),
            }),
            Some(NodeBehavior::Fail(reason)) => Err(ReadError::DataNodeReadFailed {
                node_id,
                reason: reason.clone(),
            }),
            None => Err(ReadError::DataNodeReadFailed {
                node_id,
                reason: "no behavior configured for node".into(),
            }),
        }
    }
}

/// Mock health reporter that records all reports for assertions.
#[derive(Debug, Clone)]
pub struct MockHealthReporter {
    corruptions: Arc<Mutex<Vec<CorruptionReport>>>,
    failures: Arc<Mutex<Vec<ReadFailureReport>>>,
}

impl MockHealthReporter {
    pub fn new() -> Self {
        Self {
            corruptions: Arc::new(Mutex::new(Vec::new())),
            failures: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn corruption_reports(&self) -> Vec<CorruptionReport> {
        self.corruptions.lock().clone()
    }

    pub fn failure_reports(&self) -> Vec<ReadFailureReport> {
        self.failures.lock().clone()
    }
}

impl HealthReporter for MockHealthReporter {
    async fn report_corruption(&self, report: CorruptionReport) {
        self.corruptions.lock().push(report);
    }

    async fn report_read_failure(&self, report: ReadFailureReport) {
        self.failures.lock().push(report);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ReplicaLocation;

    fn node_id(n: u8) -> NodeId {
        use uuid::Uuid;
        NodeId::new(Uuid::from_bytes([n; 16]))
    }

    fn test_mapping(vol: VolumeId, ext: ExtentId) -> ExtentMapping {
        ExtentMapping {
            volume_id: vol,
            extent_id: ext,
            extent_version: 1,
            epoch: EpochId::new(1),
            replicas: vec![ReplicaLocation {
                node_id: node_id(1),
                is_local: true,
            }],
            checksum: "test".into(),
        }
    }

    #[tokio::test]
    async fn test_mock_metadata_get_mapping() {
        let vol = VolumeId::generate();
        let ext = ExtentId::generate();
        let m = test_mapping(vol, ext);

        let meta = MockMetadata::new().with_mapping(vol, ext, m.clone());
        let result = meta.get_extent_mapping(vol, ext).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().extent_version, 1);
    }

    #[tokio::test]
    async fn test_mock_metadata_missing_mapping() {
        let meta = MockMetadata::new();
        let result = meta
            .get_extent_mapping(VolumeId::generate(), ExtentId::generate())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_mock_metadata_watermark() {
        let meta = MockMetadata::new().with_watermark(42);
        let w = meta
            .get_write_watermark(SessionId::generate(), VolumeId::generate())
            .await
            .unwrap();
        assert_eq!(w, 42);
    }

    #[tokio::test]
    async fn test_mock_metadata_refresh() {
        let vol = VolumeId::generate();
        let ext = ExtentId::generate();
        let fresh = test_mapping(vol, ext);

        let meta = MockMetadata::new().with_refreshed_mapping(vol, ext, fresh);
        let result = meta.refresh_extent_mapping(vol, ext).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_mock_data_reader_success() {
        let nid = node_id(1);
        let reader = MockDataReader::new().with_node_behavior(
            nid,
            NodeBehavior::Success {
                checksum: "ok".into(),
                data: Bytes::from_static(b"payload"),
            },
        );

        let result = reader
            .read_extent(nid, VolumeId::generate(), ExtentId::generate(), 1, 0, 7)
            .await
            .unwrap();
        assert_eq!(result.data, Bytes::from_static(b"payload"));
        assert_eq!(result.checksum, "ok");
    }

    #[tokio::test]
    async fn test_mock_data_reader_fail() {
        let nid = node_id(1);
        let reader =
            MockDataReader::new().with_node_behavior(nid, NodeBehavior::Fail("error".into()));

        let err = reader
            .read_extent(nid, VolumeId::generate(), ExtentId::generate(), 1, 0, 1)
            .await
            .unwrap_err();
        assert!(matches!(err, ReadError::DataNodeReadFailed { .. }));
    }

    #[tokio::test]
    async fn test_mock_data_reader_unconfigured_node() {
        let reader = MockDataReader::new();
        let err = reader
            .read_extent(
                node_id(99),
                VolumeId::generate(),
                ExtentId::generate(),
                1,
                0,
                1,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ReadError::DataNodeReadFailed { .. }));
    }

    #[tokio::test]
    async fn test_mock_health_reporter_corruption() {
        let reporter = MockHealthReporter::new();
        reporter
            .report_corruption(CorruptionReport {
                node_id: node_id(1),
                extent_id: ExtentId::generate(),
                extent_version: 1,
                expected_checksum: "a".into(),
                actual_checksum: "b".into(),
            })
            .await;

        assert_eq!(reporter.corruption_reports().len(), 1);
    }

    #[tokio::test]
    async fn test_mock_health_reporter_failure() {
        let reporter = MockHealthReporter::new();
        reporter
            .report_read_failure(ReadFailureReport {
                node_id: node_id(1),
                extent_id: ExtentId::generate(),
                reason: "timeout".into(),
            })
            .await;

        assert_eq!(reporter.failure_reports().len(), 1);
    }
}
