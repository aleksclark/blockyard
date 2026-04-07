//! Erasure-coded read pipeline (P4D.3, P4D.4, §4.6).
//!
//! Reads K fragments in parallel (preferring data over parity), decodes
//! to reconstruct the original data, and verifies the checksum.
//! On fragment failure, substitutes available fragments.
//! Reports missing/corrupt fragments to the health subsystem.

use std::time::Instant;

use blockyard_common::{NodeId, SessionId};
use tracing::{debug, warn};

use crate::ec::{EcError, ErasureCodec, Fragment};
use crate::error::ReadError;
use crate::traits::{DataNodeReader, HealthReporter, MetadataProvider, ReplicaSelector};
use crate::types::{ExtentMapping, ReadFailureReport, ReadRequest, ReadResult, ReplicaLocation};
/// Describes the EC fragment placement for an extent.
#[derive(Debug, Clone)]
pub struct FragmentPlacement {
    pub fragment_index: usize,
    pub is_data: bool,
    pub node_id: NodeId,
}

/// An EC extent mapping that includes fragment placement information.
#[derive(Debug, Clone)]
pub struct EcExtentMapping {
    pub base: ExtentMapping,
    pub data_count: usize,
    pub parity_count: usize,
    pub original_data_len: usize,
    pub fragment_placements: Vec<FragmentPlacement>,
}

/// The erasure-coded read pipeline (P4D.3).
///
/// Reads fragments in parallel, reconstructs original data via Reed-Solomon
/// decoding, and verifies integrity. Handles fragment failures by falling
/// back to remaining available fragments.
#[derive(Debug)]
pub struct EcReadPipeline<M, D, H, S> {
    metadata: M,
    data_reader: D,
    health: H,
    selector: S,
    session_id: SessionId,
}

impl<M, D, H, S> EcReadPipeline<M, D, H, S>
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

    /// Execute an EC read through the full pipeline.
    ///
    /// 1. Resolve mapping and watermark
    /// 2. Select K fragments (prefer data over parity)
    /// 3. Read fragments in parallel
    /// 4. On failure, substitute with available fragments
    /// 5. Decode to reconstruct original data
    /// 6. Verify checksum
    pub async fn read(
        &self,
        request: &ReadRequest,
        ec_mapping: &EcExtentMapping,
    ) -> Result<ReadResult, ReadError> {
        if request.length == 0 {
            return Err(ReadError::InvalidRequest(
                "read length must be non-zero".into(),
            ));
        }

        let watermark = self
            .metadata
            .get_write_watermark(self.session_id, request.volume_id)
            .await?;

        if ec_mapping.base.extent_version < watermark {
            // Attempt metadata refresh before returning StaleMapping (§4.4).
            let refreshed = self
                .metadata
                .refresh_extent_mapping(request.volume_id, request.extent_id)
                .await?;

            if let Some(refreshed_mapping) = refreshed {
                if refreshed_mapping.extent_version >= watermark {
                    // Caller should retry with the refreshed mapping;
                    // for now we still return StaleMapping so the caller
                    // can obtain the new EcExtentMapping and retry.
                    tracing::debug!(
                        extent_id = %request.extent_id,
                        old_version = ec_mapping.base.extent_version,
                        new_version = refreshed_mapping.extent_version,
                        "refreshed stale EC mapping"
                    );
                }
            }

            return Err(ReadError::StaleMapping {
                extent_id: request.extent_id,
                mapping_version: ec_mapping.base.extent_version,
                required_version: watermark,
            });
        }

        let codec = ErasureCodec::new(ec_mapping.data_count, ec_mapping.parity_count)
            .map_err(|e| ReadError::InvalidRequest(format!("invalid EC parameters: {}", e)))?;

        let fragments = self.read_fragments(request, ec_mapping, &codec).await?;

        let original_len = ec_mapping.original_data_len;
        let decoded = tokio::task::spawn_blocking(move || codec.decode(fragments, original_len))
            .await
            .map_err(|e| ReadError::InvalidRequest(format!("decode task failed: {}", e)))?
            .map_err(|e| match e {
                EcError::InsufficientFragments { .. } => ReadError::AllReplicasFailed {
                    extent_id: request.extent_id,
                },
                other => ReadError::InvalidRequest(format!("EC decode failed: {}", other)),
            })?;

        if compute_checksum(&decoded) != ec_mapping.base.checksum {
            return Err(ReadError::AllReplicasCorrupt {
                extent_id: request.extent_id,
            });
        }

        let source_node = ec_mapping
            .fragment_placements
            .first()
            .map(|p| p.node_id)
            .unwrap_or_else(NodeId::generate);

        Ok(ReadResult {
            extent_id: request.extent_id,
            extent_version: ec_mapping.base.extent_version,
            data: decoded,
            source_node,
        })
    }

    /// Read all K+M fragments, trying each. Returns Option<Fragment> per slot.
    async fn read_fragments(
        &self,
        request: &ReadRequest,
        ec_mapping: &EcExtentMapping,
        codec: &ErasureCodec,
    ) -> Result<Vec<Option<Fragment>>, ReadError> {
        let total = codec.total_count();
        let mut results: Vec<Option<Fragment>> = vec![None; total];
        let mut success_count = 0;

        let ordered = self.order_placements(ec_mapping);

        for placement in &ordered {
            if success_count >= codec.data_count() {
                break;
            }

            let start = Instant::now();

            match self
                .data_reader
                .read_extent(
                    placement.node_id,
                    request.volume_id,
                    request.extent_id,
                    ec_mapping.base.extent_version,
                    request.offset,
                    request.length,
                )
                .await
            {
                Ok(result) => {
                    let latency = start.elapsed().as_micros() as u64;
                    self.selector.record_success(placement.node_id, latency);

                    debug!(
                        node_id = %placement.node_id,
                        fragment = placement.fragment_index,
                        latency_us = latency,
                        "fragment read succeeded"
                    );

                    results[placement.fragment_index] = Some(Fragment {
                        index: placement.fragment_index,
                        is_data: placement.is_data,
                        data: result.data,
                    });
                    success_count += 1;
                }
                Err(e) => {
                    warn!(
                        node_id = %placement.node_id,
                        fragment = placement.fragment_index,
                        error = %e,
                        "fragment read failed"
                    );

                    self.health
                        .report_read_failure(ReadFailureReport {
                            node_id: placement.node_id,
                            extent_id: request.extent_id,
                            reason: format!(
                                "EC fragment {} read failed: {}",
                                placement.fragment_index, e
                            ),
                        })
                        .await;

                    self.selector.record_failure(placement.node_id);
                }
            }
        }

        if success_count < codec.data_count() {
            return Err(ReadError::AllReplicasFailed {
                extent_id: request.extent_id,
            });
        }

        Ok(results)
    }

    /// Order placements for reading: data fragments first, then parity.
    /// Within each category, use the replica selector ordering.
    fn order_placements(&self, ec_mapping: &EcExtentMapping) -> Vec<FragmentPlacement> {
        let replicas: Vec<ReplicaLocation> = ec_mapping
            .fragment_placements
            .iter()
            .map(|p| ReplicaLocation {
                node_id: p.node_id,
                is_local: false,
            })
            .collect();

        let ordered_nodes = self.selector.select_replicas(&replicas);

        let mut data_placements: Vec<&FragmentPlacement> = ec_mapping
            .fragment_placements
            .iter()
            .filter(|p| p.is_data)
            .collect();
        let mut parity_placements: Vec<&FragmentPlacement> = ec_mapping
            .fragment_placements
            .iter()
            .filter(|p| !p.is_data)
            .collect();

        let sort_by_order = |placements: &mut Vec<&FragmentPlacement>| {
            placements.sort_by_key(|p| {
                ordered_nodes
                    .iter()
                    .position(|n| *n == p.node_id)
                    .unwrap_or(usize::MAX)
            });
        };

        sort_by_order(&mut data_placements);
        sort_by_order(&mut parity_placements);

        let mut result: Vec<FragmentPlacement> =
            Vec::with_capacity(data_placements.len() + parity_placements.len());
        for p in data_placements {
            result.push(p.clone());
        }
        for p in parity_placements {
            result.push(p.clone());
        }

        result
    }
}

fn compute_checksum(data: &[u8]) -> String {
    blockyard_common::checksum::compute_checksum(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ec::ErasureCodec;
    use crate::selector::LatencyAwareSelector;
    use crate::testutil::{MockDataReader, MockHealthReporter, MockMetadata, NodeBehavior};
    use blockyard_common::{EpochId, ExtentId, SessionId, VolumeId};

    fn session() -> SessionId {
        SessionId::generate()
    }

    fn volume() -> VolumeId {
        VolumeId::generate()
    }

    fn extent() -> ExtentId {
        ExtentId::generate()
    }

    fn node_id(n: u8) -> NodeId {
        use uuid::Uuid;
        NodeId::new(Uuid::from_bytes([n; 16]))
    }

    fn make_ec_mapping(
        vol: VolumeId,
        ext: ExtentId,
        data: &[u8],
        k: usize,
        m: usize,
        nodes: &[u8],
    ) -> (EcExtentMapping, Vec<Fragment>) {
        let codec = ErasureCodec::new(k, m).unwrap();
        let fragments = codec.encode(data).unwrap();
        let checksum = compute_checksum(data);

        let placements: Vec<FragmentPlacement> = nodes
            .iter()
            .enumerate()
            .map(|(i, &n)| FragmentPlacement {
                fragment_index: i,
                is_data: i < k,
                node_id: node_id(n),
            })
            .collect();

        let replicas: Vec<ReplicaLocation> = nodes
            .iter()
            .map(|&n| ReplicaLocation {
                node_id: node_id(n),
                is_local: false,
            })
            .collect();

        let mapping = EcExtentMapping {
            base: ExtentMapping {
                volume_id: vol,
                extent_id: ext,
                extent_version: 5,
                epoch: EpochId::new(1),
                replicas,
                checksum,
            },
            data_count: k,
            parity_count: m,
            original_data_len: data.len(),
            fragment_placements: placements,
        };

        (mapping, fragments)
    }

    fn make_pipeline(
        meta: MockMetadata,
        reader: MockDataReader,
    ) -> EcReadPipeline<MockMetadata, MockDataReader, MockHealthReporter, LatencyAwareSelector>
    {
        EcReadPipeline::new(
            meta,
            reader,
            MockHealthReporter::new(),
            LatencyAwareSelector::new(),
            session(),
        )
    }

    #[tokio::test]
    async fn test_ec_read_all_fragments_available() {
        let vol = volume();
        let ext = extent();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let (ec_mapping, fragments) = make_ec_mapping(vol, ext, &data, 4, 2, &[1, 2, 3, 4, 5, 6]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, ec_mapping.base.clone())
            .with_watermark(0);

        let mut reader = MockDataReader::new();
        for frag in &fragments {
            let node = node_id((frag.index + 1) as u8);
            reader = reader.with_node_behavior(
                node,
                NodeBehavior::Success {
                    checksum: "ignored".into(),
                    data: frag.data.clone(),
                },
            );
        }

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(
                &ReadRequest {
                    volume_id: vol,
                    extent_id: ext,
                    offset: 0,
                    length: 400,
                },
                &ec_mapping,
            )
            .await
            .unwrap();

        assert_eq!(result.data.as_ref(), &data[..]);
        assert_eq!(result.extent_version, 5);
    }

    #[tokio::test]
    async fn test_ec_read_with_missing_data_fragment() {
        let vol = volume();
        let ext = extent();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let (ec_mapping, fragments) = make_ec_mapping(vol, ext, &data, 4, 2, &[1, 2, 3, 4, 5, 6]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, ec_mapping.base.clone())
            .with_watermark(0);

        let mut reader = MockDataReader::new();
        reader = reader.with_node_behavior(node_id(1), NodeBehavior::Fail("down".into()));
        for frag in fragments.iter().skip(1) {
            let node = node_id((frag.index + 1) as u8);
            reader = reader.with_node_behavior(
                node,
                NodeBehavior::Success {
                    checksum: "ignored".into(),
                    data: frag.data.clone(),
                },
            );
        }

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(
                &ReadRequest {
                    volume_id: vol,
                    extent_id: ext,
                    offset: 0,
                    length: 400,
                },
                &ec_mapping,
            )
            .await
            .unwrap();

        assert_eq!(result.data.as_ref(), &data[..]);
    }

    #[tokio::test]
    async fn test_ec_read_with_two_missing_fragments() {
        let vol = volume();
        let ext = extent();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let (ec_mapping, fragments) = make_ec_mapping(vol, ext, &data, 4, 2, &[1, 2, 3, 4, 5, 6]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, ec_mapping.base.clone())
            .with_watermark(0);

        let mut reader = MockDataReader::new();
        reader = reader.with_node_behavior(node_id(1), NodeBehavior::Fail("down".into()));
        reader = reader.with_node_behavior(node_id(3), NodeBehavior::Fail("down".into()));
        for frag in &fragments {
            let n = (frag.index + 1) as u8;
            if n != 1 && n != 3 {
                reader = reader.with_node_behavior(
                    node_id(n),
                    NodeBehavior::Success {
                        checksum: "ignored".into(),
                        data: frag.data.clone(),
                    },
                );
            }
        }

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(
                &ReadRequest {
                    volume_id: vol,
                    extent_id: ext,
                    offset: 0,
                    length: 400,
                },
                &ec_mapping,
            )
            .await
            .unwrap();

        assert_eq!(result.data.as_ref(), &data[..]);
    }

    #[tokio::test]
    async fn test_ec_read_too_many_failures() {
        let vol = volume();
        let ext = extent();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let (ec_mapping, fragments) = make_ec_mapping(vol, ext, &data, 4, 2, &[1, 2, 3, 4, 5, 6]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, ec_mapping.base.clone())
            .with_watermark(0);

        let mut reader = MockDataReader::new();
        reader = reader.with_node_behavior(node_id(1), NodeBehavior::Fail("down".into()));
        reader = reader.with_node_behavior(node_id(2), NodeBehavior::Fail("down".into()));
        reader = reader.with_node_behavior(node_id(3), NodeBehavior::Fail("down".into()));
        for frag in &fragments {
            let n = (frag.index + 1) as u8;
            if n > 3 {
                reader = reader.with_node_behavior(
                    node_id(n),
                    NodeBehavior::Success {
                        checksum: "ignored".into(),
                        data: frag.data.clone(),
                    },
                );
            }
        }

        let pipeline = make_pipeline(meta, reader);
        let err = pipeline
            .read(
                &ReadRequest {
                    volume_id: vol,
                    extent_id: ext,
                    offset: 0,
                    length: 400,
                },
                &ec_mapping,
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ReadError::AllReplicasFailed { .. }));
    }

    #[tokio::test]
    async fn test_ec_read_zero_length_fails() {
        let vol = volume();
        let ext = extent();
        let data = vec![0xAA; 400];
        let (ec_mapping, _) = make_ec_mapping(vol, ext, &data, 4, 2, &[1, 2, 3, 4, 5, 6]);

        let meta = MockMetadata::new().with_watermark(0);
        let reader = MockDataReader::new();
        let pipeline = make_pipeline(meta, reader);

        let err = pipeline
            .read(
                &ReadRequest {
                    volume_id: vol,
                    extent_id: ext,
                    offset: 0,
                    length: 0,
                },
                &ec_mapping,
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ReadError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn test_ec_read_stale_mapping() {
        let vol = volume();
        let ext = extent();
        let data = vec![0xAA; 400];
        let (ec_mapping, _) = make_ec_mapping(vol, ext, &data, 4, 2, &[1, 2, 3, 4, 5, 6]);

        let meta = MockMetadata::new().with_watermark(10);
        let reader = MockDataReader::new();
        let pipeline = make_pipeline(meta, reader);

        let err = pipeline
            .read(
                &ReadRequest {
                    volume_id: vol,
                    extent_id: ext,
                    offset: 0,
                    length: 400,
                },
                &ec_mapping,
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ReadError::StaleMapping { .. }));
    }

    #[tokio::test]
    async fn test_ec_read_reports_failures_to_health() {
        let vol = volume();
        let ext = extent();
        let data: Vec<u8> = (0..400).map(|i| (i % 256) as u8).collect();
        let (ec_mapping, fragments) = make_ec_mapping(vol, ext, &data, 4, 2, &[1, 2, 3, 4, 5, 6]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, ec_mapping.base.clone())
            .with_watermark(0);

        let mut reader = MockDataReader::new();
        reader = reader.with_node_behavior(node_id(1), NodeBehavior::Fail("timeout".into()));
        for frag in fragments.iter().skip(1) {
            let node = node_id((frag.index + 1) as u8);
            reader = reader.with_node_behavior(
                node,
                NodeBehavior::Success {
                    checksum: "ignored".into(),
                    data: frag.data.clone(),
                },
            );
        }

        let health = MockHealthReporter::new();
        let pipeline = EcReadPipeline::new(
            meta,
            reader,
            health.clone(),
            LatencyAwareSelector::new(),
            session(),
        );

        pipeline
            .read(
                &ReadRequest {
                    volume_id: vol,
                    extent_id: ext,
                    offset: 0,
                    length: 400,
                },
                &ec_mapping,
            )
            .await
            .unwrap();

        let failures = health.failure_reports();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].node_id, node_id(1));
    }

    #[tokio::test]
    async fn test_ec_read_small_data() {
        let vol = volume();
        let ext = extent();
        let data = vec![42u8; 10];
        let (ec_mapping, fragments) = make_ec_mapping(vol, ext, &data, 2, 1, &[1, 2, 3]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, ec_mapping.base.clone())
            .with_watermark(0);

        let mut reader = MockDataReader::new();
        for frag in &fragments {
            let node = node_id((frag.index + 1) as u8);
            reader = reader.with_node_behavior(
                node,
                NodeBehavior::Success {
                    checksum: "ignored".into(),
                    data: frag.data.clone(),
                },
            );
        }

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(
                &ReadRequest {
                    volume_id: vol,
                    extent_id: ext,
                    offset: 0,
                    length: 10,
                },
                &ec_mapping,
            )
            .await
            .unwrap();

        assert_eq!(result.data.as_ref(), &data[..]);
    }

    #[tokio::test]
    async fn test_ec_read_padded_data() {
        let vol = volume();
        let ext = extent();
        let data: Vec<u8> = (0..401).map(|i| (i % 256) as u8).collect();
        let (ec_mapping, fragments) = make_ec_mapping(vol, ext, &data, 4, 2, &[1, 2, 3, 4, 5, 6]);

        let meta = MockMetadata::new()
            .with_mapping(vol, ext, ec_mapping.base.clone())
            .with_watermark(0);

        let mut reader = MockDataReader::new();
        for frag in &fragments {
            let node = node_id((frag.index + 1) as u8);
            reader = reader.with_node_behavior(
                node,
                NodeBehavior::Success {
                    checksum: "ignored".into(),
                    data: frag.data.clone(),
                },
            );
        }

        let pipeline = make_pipeline(meta, reader);
        let result = pipeline
            .read(
                &ReadRequest {
                    volume_id: vol,
                    extent_id: ext,
                    offset: 0,
                    length: 401,
                },
                &ec_mapping,
            )
            .await
            .unwrap();

        assert_eq!(result.data.as_ref(), &data[..]);
    }

    #[test]
    fn test_fragment_placement_debug() {
        let p = FragmentPlacement {
            fragment_index: 0,
            is_data: true,
            node_id: node_id(1),
        };
        let debug = format!("{:?}", p);
        assert!(debug.contains("FragmentPlacement"));
    }

    #[test]
    fn test_fragment_placement_clone() {
        let p = FragmentPlacement {
            fragment_index: 2,
            is_data: false,
            node_id: node_id(3),
        };
        let cloned = p.clone();
        assert_eq!(cloned.fragment_index, 2);
        assert!(!cloned.is_data);
    }

    #[test]
    fn test_ec_extent_mapping_debug() {
        let vol = volume();
        let ext = extent();
        let data = vec![0u8; 100];
        let (mapping, _) = make_ec_mapping(vol, ext, &data, 2, 1, &[1, 2, 3]);
        let debug = format!("{:?}", mapping);
        assert!(debug.contains("EcExtentMapping"));
    }

    #[test]
    fn test_ec_extent_mapping_clone() {
        let vol = volume();
        let ext = extent();
        let data = vec![0u8; 100];
        let (mapping, _) = make_ec_mapping(vol, ext, &data, 2, 1, &[1, 2, 3]);
        let cloned = mapping.clone();
        assert_eq!(cloned.data_count, 2);
        assert_eq!(cloned.parity_count, 1);
    }

    #[test]
    fn test_compute_checksum_deterministic() {
        let c1 = compute_checksum(b"hello");
        let c2 = compute_checksum(b"hello");
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_compute_checksum_different() {
        let c1 = compute_checksum(b"hello");
        let c2 = compute_checksum(b"world");
        assert_ne!(c1, c2);
    }

    #[test]
    fn test_order_placements_data_first() {
        let vol = volume();
        let ext = extent();
        let data = vec![0u8; 400];
        let (ec_mapping, _) = make_ec_mapping(vol, ext, &data, 4, 2, &[1, 2, 3, 4, 5, 6]);

        let meta = MockMetadata::new().with_watermark(0);
        let reader = MockDataReader::new();
        let pipeline = make_pipeline(meta, reader);

        let ordered = pipeline.order_placements(&ec_mapping);

        let data_indices: Vec<usize> = ordered
            .iter()
            .filter(|p| p.is_data)
            .map(|p| p.fragment_index)
            .collect();
        let parity_indices: Vec<usize> = ordered
            .iter()
            .filter(|p| !p.is_data)
            .map(|p| p.fragment_index)
            .collect();

        assert_eq!(data_indices.len(), 4);
        assert_eq!(parity_indices.len(), 2);

        let first_parity_pos = ordered.iter().position(|p| !p.is_data).unwrap();
        let last_data_pos = ordered.iter().rposition(|p| p.is_data).unwrap();
        assert!(last_data_pos < first_parity_pos);
    }

    #[tokio::test]
    async fn test_ec_read_pipeline_debug() {
        let meta = MockMetadata::new().with_watermark(0);
        let reader = MockDataReader::new();
        let pipeline = make_pipeline(meta, reader);
        let debug = format!("{:?}", pipeline);
        assert!(debug.contains("EcReadPipeline"));
    }
}
