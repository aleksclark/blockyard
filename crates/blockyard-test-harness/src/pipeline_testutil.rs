use std::collections::BTreeMap;
use std::sync::Arc;

use blockyard_common::{EpochId, ExtentId, NodeId, ProtectionPolicy, VolumeId};
use blockyard_ublk::metadata_cache::{CachedExtentMapping, CachedVolumeInfo};
use blockyard_ublk::{
    ClientSession, DataNodeClient, MetadataCache, MetadataClient, StaleEpochHandler, WritePipeline,
    WriteWatermark,
};

pub struct TestPipelineSetup<D: DataNodeClient, M: MetadataClient> {
    pub pipeline: WritePipeline<D, M>,
    pub cache: Arc<MetadataCache>,
    pub session: Arc<ClientSession>,
    pub watermark: Arc<WriteWatermark>,
    pub stale_handler: Arc<StaleEpochHandler>,
}

pub fn setup_test_pipeline<D: DataNodeClient, M: MetadataClient>(
    volume_id: VolumeId,
    epoch: EpochId,
    node_ids: &[NodeId],
    data_client: Arc<D>,
    metadata_client: Arc<M>,
) -> TestPipelineSetup<D, M> {
    let cache = Arc::new(MetadataCache::new());
    cache.set_epoch(epoch);

    for (i, nid) in node_ids.iter().enumerate() {
        let addr: std::net::SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        cache.set_node(*nid, addr);
    }

    let ext_id = ExtentId::generate();
    let mapping = CachedExtentMapping {
        extent_id: ext_id,
        extent_version: 0,
        replica_locations: node_ids.to_vec(),
        checksums: vec![],
    };
    cache.set_extent_mapping(&volume_id, 0, mapping);

    let vol_info = CachedVolumeInfo {
        volume_id,
        size_bytes: 1024 * 1024,
        protection: ProtectionPolicy::Replicated {
            replicas: node_ids.len() as u8,
        },
        extent_mappings: BTreeMap::new(),
    };
    cache.set_volume(vol_info);

    let session = Arc::new(ClientSession::new(volume_id));
    let watermark = Arc::new(WriteWatermark::with_initial(epoch));
    let stale_handler = Arc::new(StaleEpochHandler::new());

    let pipeline = WritePipeline::new(
        data_client,
        metadata_client,
        cache.clone(),
        session.clone(),
        watermark.clone(),
        stale_handler.clone(),
    );

    TestPipelineSetup {
        pipeline,
        cache,
        session,
        watermark,
        stale_handler,
    }
}
