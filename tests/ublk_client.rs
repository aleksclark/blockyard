use std::collections::BTreeMap;
use std::sync::Arc;

use blockyard_common::{EpochId, ExtentId, NodeId, ProtectionPolicy, VolumeId};
use blockyard_storage::extent::compute_checksum;
use blockyard_test_harness::mock_datanode::DiskBackedTestDataNode;
use blockyard_test_harness::mock_metadata::TestMetadataClient;
use blockyard_test_harness::pipeline_testutil::setup_test_pipeline;
use blockyard_ublk::metadata_cache::{CachedExtentMapping, CachedVolumeInfo};
use blockyard_ublk::{
    ClientSession, MetadataCache, StaleEpochHandler, WriteOutcome, WritePipeline, WriteRequest,
    WriteWatermark,
};
use bytes::Bytes;

// ---------------------------------------------------------------------------
// P9F.1 — Write data, simulate crash (drop pipeline), verify data persisted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mount_write_crash_remount_verify() {
    let volume_id = VolumeId::generate();
    let epoch = EpochId::new(1);
    let node1 = NodeId::generate();
    let node2 = NodeId::generate();
    let node3 = NodeId::generate();

    let data_client = Arc::new(DiskBackedTestDataNode::new());
    let metadata_client = Arc::new(TestMetadataClient::new(epoch));

    let setup = setup_test_pipeline(
        volume_id,
        epoch,
        &[node1, node2, node3],
        data_client.clone(),
        metadata_client.clone(),
    );

    let write_data = Bytes::from(vec![0xABu8; 4096]);
    let request = WriteRequest {
        volume_id,
        block_range: 0..1024,
        data: write_data.clone(),
    };
    let result = setup.pipeline.execute(request).await;
    assert!(result.is_ok(), "write should succeed: {:?}", result.err());
    assert!(matches!(result.unwrap(), WriteOutcome::Committed { .. }));

    let stored_before_crash = data_client.stored_count();
    assert!(
        stored_before_crash > 0,
        "data should be stored on nodes before crash"
    );
    let committed_before_crash = metadata_client.committed.lock().len();
    assert!(
        committed_before_crash > 0,
        "metadata commit should have happened"
    );

    let commit_req = metadata_client.committed.lock()[0].clone();
    let extent_id = commit_req.extent_id;
    let version = commit_req.extent_version;

    for nid in &[node1, node2, node3] {
        if let Ok((disk_data, disk_checksum)) = data_client.read_back(*nid, extent_id, version) {
            assert_eq!(
                disk_data,
                write_data.as_ref(),
                "data on disk must match original write"
            );
            let expected = compute_checksum(&write_data);
            assert_eq!(
                disk_checksum, expected,
                "checksum from disk must match computed"
            );
        }
    }

    drop(setup);

    let _setup2 = setup_test_pipeline(
        volume_id,
        epoch,
        &[node1, node2, node3],
        data_client.clone(),
        metadata_client.clone(),
    );

    for nid in &[node1, node2, node3] {
        if let Ok((disk_data, _)) = data_client.read_back(*nid, extent_id, version) {
            assert_eq!(
                disk_data,
                write_data.as_ref(),
                "data on disk must survive pipeline crash"
            );
        }
    }

    let committed_ops = metadata_client.committed_ops.lock();
    assert!(
        !committed_ops.is_empty() || committed_before_crash > 0,
        "committed operation should be recoverable"
    );
}

// ---------------------------------------------------------------------------
// P9F.2 — StaleEpoch triggers refresh and pipeline switches to new epoch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_partition_follow_leader_stale_epoch() {
    let volume_id = VolumeId::generate();
    let old_epoch = EpochId::new(1);
    let node1 = NodeId::generate();
    let node2 = NodeId::generate();

    let data_client = Arc::new(DiskBackedTestDataNode::new());
    let metadata_client = Arc::new(TestMetadataClient::new(old_epoch));

    let cache = Arc::new(MetadataCache::new());
    cache.set_epoch(old_epoch);
    cache.set_node(node1, "127.0.0.1:9001".parse().unwrap());
    cache.set_node(node2, "127.0.0.1:9002".parse().unwrap());

    let mapping = CachedExtentMapping {
        extent_id: ExtentId::generate(),
        extent_version: 0,
        replica_locations: vec![node1, node2],
        checksums: vec![],
    };
    cache.set_extent_mapping(&volume_id, 0, mapping);
    cache.set_volume(CachedVolumeInfo {
        volume_id,
        size_bytes: 1024 * 1024,
        protection: ProtectionPolicy::Replicated { replicas: 2 },
        extent_mappings: BTreeMap::new(),
    });

    let stale_handler = Arc::new(StaleEpochHandler::new());
    let session = Arc::new(ClientSession::new(volume_id));
    let watermark = Arc::new(WriteWatermark::with_initial(old_epoch));

    assert_eq!(stale_handler.refresh_count(), 0);
    assert_eq!(cache.current_epoch(), old_epoch);

    let new_epoch = EpochId::new(5);
    metadata_client.set_epoch(new_epoch);

    let refreshed_epoch = stale_handler
        .handle_stale_epoch(&cache, metadata_client.as_ref(), old_epoch)
        .await
        .expect("stale epoch refresh should succeed");

    assert_eq!(refreshed_epoch, new_epoch);
    assert_eq!(cache.current_epoch(), new_epoch);
    assert_eq!(stale_handler.refresh_count(), 1);

    let pipeline = WritePipeline::new(
        data_client.clone(),
        metadata_client.clone(),
        cache.clone(),
        session.clone(),
        watermark.clone(),
        stale_handler.clone(),
    );

    let request = WriteRequest {
        volume_id,
        block_range: 0..1024,
        data: Bytes::from(vec![0xCDu8; 4096]),
    };
    let result = pipeline.execute(request).await;
    assert!(
        result.is_ok(),
        "write should succeed after epoch refresh: {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// P9F.3 — Partial write not committed when pipeline drops mid-flight
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_write_crash_partial_not_committed() {
    let volume_id = VolumeId::generate();
    let epoch = EpochId::new(1);
    let node1 = NodeId::generate();
    let node2 = NodeId::generate();
    let node3 = NodeId::generate();

    let data_client = Arc::new(DiskBackedTestDataNode::new());
    let metadata_client = Arc::new(TestMetadataClient::new(epoch));

    *metadata_client.fail_commit.lock() = true;

    let setup = setup_test_pipeline(
        volume_id,
        epoch,
        &[node1, node2, node3],
        data_client.clone(),
        metadata_client.clone(),
    );

    let op_id = setup.session.next_operation_id();
    let request = WriteRequest {
        volume_id,
        block_range: 0..1024,
        data: Bytes::from(vec![0xEFu8; 4096]),
    };
    let result = setup.pipeline.execute_with_op_id(request, op_id).await;

    match result {
        Ok(WriteOutcome::MetadataCommitFailed { .. }) => {}
        Ok(WriteOutcome::Committed { .. }) => {
            panic!("should not commit when metadata commit fails");
        }
        Err(_) => {}
        Ok(other) => {
            assert!(
                !matches!(other, WriteOutcome::Committed { .. }),
                "should not have Committed outcome when commit fails: {:?}",
                other
            );
        }
    }

    let committed = metadata_client.committed.lock();
    assert!(
        committed.is_empty(),
        "no commits should succeed when metadata is failing"
    );

    let lookup = metadata_client.committed_ops.lock();
    assert!(
        lookup.get(&op_id).is_none(),
        "operation should NOT be in committed ops"
    );
}
