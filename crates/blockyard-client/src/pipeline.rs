//! Read pipeline — the core client read path (§4.6, P4C.1–P4C.4).
//!
//! Implements the full read flow:
//! 1. Resolve minimum visible version from session watermark
//! 2. Resolve extent mapping for the target logical block range
//! 3. Verify mapping version >= minimum required version
//! 4. Select source replica (prefer local, then lowest-latency)
//! 5. Issue data read to selected source
//! 6. Verify checksums
//! 7. Return data
//!
//! On failure: try next replica. On corruption: report + try next.
//! If all replicas fail/corrupt: return error.

use std::time::Instant;

use blockyard_common::SessionId;
use tracing::{debug, warn};

use crate::error::ReadError;
use crate::traits::{DataNodeReader, HealthReporter, MetadataProvider, ReplicaSelector};
use crate::types::{CorruptionReport, ExtentMapping, ReadFailureReport, ReadRequest, ReadResult};

/// The client read pipeline, generic over its dependencies.
///
/// Orchestrates reading extent data with read-your-own-writes enforcement,
/// replica fallback, and corruption detection.
#[derive(Debug)]
pub struct ReadPipeline<M, D, H, S> {
    metadata: M,
    data_reader: D,
    health: H,
    selector: S,
    session_id: SessionId,
}

impl<M, D, H, S> ReadPipeline<M, D, H, S>
where
    M: MetadataProvider,
    D: DataNodeReader,
    H: HealthReporter,
    S: ReplicaSelector,
{
    pub fn new(metadata: M, data_reader: D, health: H, selector: S, session_id: SessionId) -> Self {
        Self {
            metadata,
            data_reader,
            health,
            selector,
            session_id,
        }
    }

    /// Execute a read request through the full pipeline.
    pub async fn read(&self, request: &ReadRequest) -> Result<ReadResult, ReadError> {
        if request.length == 0 {
            return Err(ReadError::InvalidRequest(
                "read length must be non-zero".into(),
            ));
        }

        let watermark = self
            .metadata
            .get_write_watermark(self.session_id, request.volume_id)
            .await?;

        debug!(
            volume_id = %request.volume_id,
            extent_id = %request.extent_id,
            watermark = watermark,
            "resolving extent mapping"
        );

        let mapping = self.resolve_mapping(request, watermark).await?;

        self.verify_mapping_version(&mapping, watermark)?;

        if mapping.replicas.is_empty() {
            return Err(ReadError::NoHealthyReplicas {
                extent_id: request.extent_id,
            });
        }

        let ordered_nodes = self.selector.select_replicas(&mapping.replicas);
        if ordered_nodes.is_empty() {
            return Err(ReadError::NoHealthyReplicas {
                extent_id: request.extent_id,
            });
        }

        self.read_with_fallback(request, &mapping, &ordered_nodes)
            .await
    }

    async fn resolve_mapping(
        &self,
        request: &ReadRequest,
        watermark: u64,
    ) -> Result<ExtentMapping, ReadError> {
        let mapping = self
            .metadata
            .get_extent_mapping(request.volume_id, request.extent_id)
            .await?
            .ok_or(ReadError::ExtentNotFound {
                volume_id: request.volume_id,
                extent_id: request.extent_id,
            })?;

        if mapping.extent_version < watermark {
            debug!(
                extent_id = %request.extent_id,
                cached_version = mapping.extent_version,
                watermark = watermark,
                "cached mapping stale, refreshing"
            );

            let refreshed = self
                .metadata
                .refresh_extent_mapping(request.volume_id, request.extent_id)
                .await?
                .ok_or(ReadError::ExtentNotFound {
                    volume_id: request.volume_id,
                    extent_id: request.extent_id,
                })?;

            return Ok(refreshed);
        }

        Ok(mapping)
    }

    fn verify_mapping_version(
        &self,
        mapping: &ExtentMapping,
        watermark: u64,
    ) -> Result<(), ReadError> {
        if mapping.extent_version < watermark {
            return Err(ReadError::StaleMapping {
                extent_id: mapping.extent_id,
                mapping_version: mapping.extent_version,
                required_version: watermark,
            });
        }
        Ok(())
    }

    async fn read_with_fallback(
        &self,
        request: &ReadRequest,
        mapping: &ExtentMapping,
        ordered_nodes: &[blockyard_common::NodeId],
    ) -> Result<ReadResult, ReadError> {
        let mut last_error: Option<ReadError> = None;
        let mut all_corrupt = true;

        for &node_id in ordered_nodes {
            let start = Instant::now();

            match self
                .data_reader
                .read_extent(
                    node_id,
                    request.volume_id,
                    request.extent_id,
                    mapping.extent_version,
                    request.offset,
                    request.length,
                )
                .await
            {
                Ok(result) => {
                    if result.checksum != mapping.checksum {
                        warn!(
                            node_id = %node_id,
                            extent_id = %request.extent_id,
                            expected = %mapping.checksum,
                            actual = %result.checksum,
                            "checksum mismatch, reporting corruption"
                        );

                        self.health
                            .report_corruption(CorruptionReport {
                                node_id,
                                extent_id: request.extent_id,
                                extent_version: mapping.extent_version,
                                expected_checksum: mapping.checksum.clone(),
                                actual_checksum: result.checksum.clone(),
                            })
                            .await;

                        self.selector.mark_suspect(node_id);

                        last_error = Some(ReadError::ChecksumMismatch {
                            node_id,
                            extent_id: request.extent_id,
                            expected: mapping.checksum.clone(),
                            actual: result.checksum,
                        });
                        continue;
                    }

                    let latency = start.elapsed().as_micros() as u64;
                    self.selector.record_success(node_id, latency);

                    debug!(
                        node_id = %node_id,
                        extent_id = %request.extent_id,
                        latency_us = latency,
                        "read succeeded"
                    );

                    return Ok(ReadResult {
                        extent_id: result.extent_id,
                        extent_version: result.extent_version,
                        data: result.data,
                        source_node: node_id,
                    });
                }
                Err(e) => {
                    all_corrupt = false;
                    warn!(
                        node_id = %node_id,
                        extent_id = %request.extent_id,
                        error = %e,
                        "read failed, trying next replica"
                    );

                    self.health
                        .report_read_failure(ReadFailureReport {
                            node_id,
                            extent_id: request.extent_id,
                            reason: e.to_string(),
                        })
                        .await;

                    self.selector.record_failure(node_id);
                    last_error = Some(e);
                }
            }
        }

        if all_corrupt {
            Err(ReadError::AllReplicasCorrupt {
                extent_id: request.extent_id,
            })
        } else {
            last_error.map_or_else(
                || {
                    Err(ReadError::AllReplicasFailed {
                        extent_id: request.extent_id,
                    })
                },
                Err,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selector::LatencyAwareSelector;
    use crate::testutil::{MockDataReader, MockHealthReporter, MockMetadata, NodeBehavior};
    use crate::types::ReplicaLocation;
    use blockyard_common::{EpochId, ExtentId, SessionId, VolumeId};
    use bytes::Bytes;

    fn session() -> SessionId {
        SessionId::generate()
    }

    fn volume() -> VolumeId {
        VolumeId::generate()
    }

    fn extent() -> ExtentId {
        ExtentId::generate()
    }

    fn node_id(n: u8) -> blockyard_common::NodeId {
        use uuid::Uuid;
        blockyard_common::NodeId::new(Uuid::from_bytes([n; 16]))
    }

    fn mapping(vol: VolumeId, ext: ExtentId, version: u64, nodes: &[u8]) -> ExtentMapping {
        ExtentMapping {
            volume_id: vol,
            extent_id: ext,
            extent_version: version,
            epoch: EpochId::new(1),
            replicas: nodes
                .iter()
                .map(|&n| ReplicaLocation {
                    node_id: node_id(n),
                    is_local: n == 1,
                })
                .collect(),
            checksum: "abc123".into(),
        }
    }

    fn make_pipeline(
        meta: MockMetadata,
        reader: MockDataReader,
    ) -> ReadPipeline<MockMetadata, MockDataReader, MockHealthReporter, LatencyAwareSelector> {
        ReadPipeline::new(
            meta,
            reader,
            MockHealthReporter::new(),
            LatencyAwareSelector::new(),
            session(),
        )
    }

    #[tokio::test]
    async fn test_basic_read_success() {
        let vol = volume();
        let ext = extent();
        let m = mapping(vol, ext, 5, &[1, 2]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, m.clone())
            .with_watermark(0);

        let reader = MockDataReader::new().with_node_behavior(
            node_id(1),
            NodeBehavior::Success {
                checksum: "abc123".into(),
                data: Bytes::from_static(b"hello"),
            },
        );

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 5,
            })
            .await
            .unwrap();

        assert_eq!(result.data, Bytes::from_static(b"hello"));
        assert_eq!(result.source_node, node_id(1));
        assert_eq!(result.extent_version, 5);
    }

    #[tokio::test]
    async fn test_read_zero_length_fails() {
        let vol = volume();
        let ext = extent();

        let meta = MockMetadata::new().with_watermark(0);
        let reader = MockDataReader::new();
        let pipeline = make_pipeline(meta, reader);

        let err = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 0,
            })
            .await
            .unwrap_err();

        assert!(matches!(err, ReadError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn test_extent_not_found() {
        let vol = volume();
        let ext = extent();

        let meta = MockMetadata::new().with_watermark(0);
        let reader = MockDataReader::new();
        let pipeline = make_pipeline(meta, reader);

        let err = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 100,
            })
            .await
            .unwrap_err();

        assert!(matches!(err, ReadError::ExtentNotFound { .. }));
    }

    #[tokio::test]
    async fn test_read_your_own_writes_refreshes_stale_mapping() {
        let vol = volume();
        let ext = extent();
        let stale = mapping(vol, ext, 3, &[1]);
        let fresh = mapping(vol, ext, 10, &[1]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, stale)
            .with_refreshed_mapping(vol, ext, fresh)
            .with_watermark(5);

        let reader = MockDataReader::new().with_node_behavior(
            node_id(1),
            NodeBehavior::Success {
                checksum: "abc123".into(),
                data: Bytes::from_static(b"data"),
            },
        );

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 4,
            })
            .await
            .unwrap();

        assert_eq!(result.extent_version, 10);
    }

    #[tokio::test]
    async fn test_read_your_own_writes_stale_after_refresh_fails() {
        let vol = volume();
        let ext = extent();
        let stale = mapping(vol, ext, 3, &[1]);
        let still_stale = mapping(vol, ext, 4, &[1]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, stale)
            .with_refreshed_mapping(vol, ext, still_stale)
            .with_watermark(5);

        let reader = MockDataReader::new().with_node_behavior(
            node_id(1),
            NodeBehavior::Success {
                checksum: "abc123".into(),
                data: Bytes::from_static(b"data"),
            },
        );

        let pipeline = make_pipeline(meta, reader);
        let err = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 4,
            })
            .await
            .unwrap_err();

        assert!(matches!(err, ReadError::StaleMapping { .. }));
    }

    #[tokio::test]
    async fn test_replica_fallback_on_failure() {
        let vol = volume();
        let ext = extent();
        let m = mapping(vol, ext, 5, &[1, 2, 3]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, m.clone())
            .with_watermark(0);

        let reader = MockDataReader::new()
            .with_node_behavior(node_id(1), NodeBehavior::Fail("node down".into()))
            .with_node_behavior(
                node_id(2),
                NodeBehavior::Success {
                    checksum: "abc123".into(),
                    data: Bytes::from_static(b"fallback"),
                },
            );

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 8,
            })
            .await
            .unwrap();

        assert_eq!(result.source_node, node_id(2));
        assert_eq!(result.data, Bytes::from_static(b"fallback"));
    }

    #[tokio::test]
    async fn test_all_replicas_fail() {
        let vol = volume();
        let ext = extent();
        let m = mapping(vol, ext, 5, &[1, 2]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, m.clone())
            .with_watermark(0);

        let reader = MockDataReader::new()
            .with_node_behavior(node_id(1), NodeBehavior::Fail("down".into()))
            .with_node_behavior(node_id(2), NodeBehavior::Fail("down".into()));

        let pipeline = make_pipeline(meta, reader);
        let err = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 100,
            })
            .await
            .unwrap_err();

        assert!(matches!(err, ReadError::DataNodeReadFailed { .. }));
    }

    #[tokio::test]
    async fn test_checksum_mismatch_fallback() {
        let vol = volume();
        let ext = extent();
        let m = mapping(vol, ext, 5, &[1, 2]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, m.clone())
            .with_watermark(0);

        let reader = MockDataReader::new()
            .with_node_behavior(
                node_id(1),
                NodeBehavior::Success {
                    checksum: "CORRUPT".into(),
                    data: Bytes::from_static(b"bad"),
                },
            )
            .with_node_behavior(
                node_id(2),
                NodeBehavior::Success {
                    checksum: "abc123".into(),
                    data: Bytes::from_static(b"good"),
                },
            );

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 4,
            })
            .await
            .unwrap();

        assert_eq!(result.source_node, node_id(2));
        assert_eq!(result.data, Bytes::from_static(b"good"));
    }

    #[tokio::test]
    async fn test_all_replicas_corrupt() {
        let vol = volume();
        let ext = extent();
        let m = mapping(vol, ext, 5, &[1, 2]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, m.clone())
            .with_watermark(0);

        let reader = MockDataReader::new()
            .with_node_behavior(
                node_id(1),
                NodeBehavior::Success {
                    checksum: "BAD1".into(),
                    data: Bytes::from_static(b"corrupt"),
                },
            )
            .with_node_behavior(
                node_id(2),
                NodeBehavior::Success {
                    checksum: "BAD2".into(),
                    data: Bytes::from_static(b"also_corrupt"),
                },
            );

        let pipeline = make_pipeline(meta, reader);
        let err = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 10,
            })
            .await
            .unwrap_err();

        assert!(matches!(err, ReadError::AllReplicasCorrupt { .. }));
    }

    #[tokio::test]
    async fn test_corruption_reported_to_health() {
        let vol = volume();
        let ext = extent();
        let m = mapping(vol, ext, 5, &[1, 2]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, m.clone())
            .with_watermark(0);

        let reader = MockDataReader::new()
            .with_node_behavior(
                node_id(1),
                NodeBehavior::Success {
                    checksum: "BAD".into(),
                    data: Bytes::from_static(b"corrupt"),
                },
            )
            .with_node_behavior(
                node_id(2),
                NodeBehavior::Success {
                    checksum: "abc123".into(),
                    data: Bytes::from_static(b"good"),
                },
            );

        let health = MockHealthReporter::new();
        let pipeline = ReadPipeline::new(
            meta,
            reader,
            health.clone(),
            LatencyAwareSelector::new(),
            session(),
        );

        pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 4,
            })
            .await
            .unwrap();

        let corruptions = health.corruption_reports();
        assert_eq!(corruptions.len(), 1);
        assert_eq!(corruptions[0].node_id, node_id(1));
        assert_eq!(corruptions[0].expected_checksum, "abc123");
        assert_eq!(corruptions[0].actual_checksum, "BAD");
    }

    #[tokio::test]
    async fn test_failure_reported_to_health() {
        let vol = volume();
        let ext = extent();
        let m = mapping(vol, ext, 5, &[1, 2]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, m.clone())
            .with_watermark(0);

        let reader = MockDataReader::new()
            .with_node_behavior(node_id(1), NodeBehavior::Fail("timeout".into()))
            .with_node_behavior(
                node_id(2),
                NodeBehavior::Success {
                    checksum: "abc123".into(),
                    data: Bytes::from_static(b"ok"),
                },
            );

        let health = MockHealthReporter::new();
        let pipeline = ReadPipeline::new(
            meta,
            reader,
            health.clone(),
            LatencyAwareSelector::new(),
            session(),
        );

        pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 2,
            })
            .await
            .unwrap();

        let failures = health.failure_reports();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].node_id, node_id(1));
    }

    #[tokio::test]
    async fn test_no_replicas_available() {
        let vol = volume();
        let ext = extent();
        let m = ExtentMapping {
            volume_id: vol,
            extent_id: ext,
            extent_version: 5,
            epoch: EpochId::new(1),
            replicas: vec![],
            checksum: "abc123".into(),
        };

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, m)
            .with_watermark(0);
        let reader = MockDataReader::new();
        let pipeline = make_pipeline(meta, reader);

        let err = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 100,
            })
            .await
            .unwrap_err();

        assert!(matches!(err, ReadError::NoHealthyReplicas { .. }));
    }

    #[tokio::test]
    async fn test_watermark_zero_skips_refresh() {
        let vol = volume();
        let ext = extent();
        let m = mapping(vol, ext, 1, &[1]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, m)
            .with_watermark(0);

        let reader = MockDataReader::new().with_node_behavior(
            node_id(1),
            NodeBehavior::Success {
                checksum: "abc123".into(),
                data: Bytes::from_static(b"data"),
            },
        );

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 4,
            })
            .await
            .unwrap();

        assert_eq!(result.extent_version, 1);
    }

    #[tokio::test]
    async fn test_mixed_corruption_and_failure_fallback() {
        let vol = volume();
        let ext = extent();
        let m = mapping(vol, ext, 5, &[1, 2, 3]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, m.clone())
            .with_watermark(0);

        let reader = MockDataReader::new()
            .with_node_behavior(
                node_id(1),
                NodeBehavior::Success {
                    checksum: "BAD".into(),
                    data: Bytes::from_static(b"corrupt"),
                },
            )
            .with_node_behavior(node_id(2), NodeBehavior::Fail("timeout".into()))
            .with_node_behavior(
                node_id(3),
                NodeBehavior::Success {
                    checksum: "abc123".into(),
                    data: Bytes::from_static(b"good"),
                },
            );

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 4,
            })
            .await
            .unwrap();

        assert_eq!(result.source_node, node_id(3));
    }

    #[tokio::test]
    async fn test_refresh_returns_none() {
        let vol = volume();
        let ext = extent();
        let stale = mapping(vol, ext, 1, &[1]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, stale)
            .with_watermark(5);

        let reader = MockDataReader::new();
        let pipeline = make_pipeline(meta, reader);

        let err = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 100,
            })
            .await
            .unwrap_err();

        assert!(matches!(err, ReadError::ExtentNotFound { .. }));
    }

    #[tokio::test]
    async fn test_mapping_version_equals_watermark_passes() {
        let vol = volume();
        let ext = extent();
        let m = mapping(vol, ext, 5, &[1]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, m)
            .with_watermark(5);

        let reader = MockDataReader::new().with_node_behavior(
            node_id(1),
            NodeBehavior::Success {
                checksum: "abc123".into(),
                data: Bytes::from_static(b"exact"),
            },
        );

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(&ReadRequest {
                volume_id: vol,
                extent_id: ext,
                offset: 0,
                length: 5,
            })
            .await
            .unwrap();

        assert_eq!(result.extent_version, 5);
    }
}
