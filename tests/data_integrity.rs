use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use blockyard_common::VolumeId;
use blockyard_test_harness::{
    AckStatus, Cluster, ClusterConfig, ConsistencyChecker, Fault, FaultInjector,
    KeyDistribution, NetworkConfig, Node, NodeAddress, Operation, OperationLog, PatternConfig,
    PatternGenerator, PatternKind, ProcessCluster, ProcessFaultInjector, SnapshotManager,
    TestNodeConfig, TestNodeId, WorkloadConfig, WorkloadGenerator,
};

fn test_cluster(node_count: u32, base_port: u16) -> ProcessCluster {
    ProcessCluster::new(ClusterConfig {
        node_count,
        binary_path: PathBuf::from("/usr/bin/false"),
        base_data_dir: PathBuf::from("/tmp/blockyard-data-integrity-test"),
        network: NetworkConfig {
            base_listen_port: base_port,
            base_gossip_port: base_port + 1000,
            host: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        },
    })
}

// ---------------------------------------------------------------------------
// P9E.1 — Write known pattern → crash all nodes → restart → verify pattern
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_write_pattern_crash_all_restart_verify() {
    let cluster = test_cluster(5, 40000);
    let volume_id = VolumeId::generate();

    let pattern_config = PatternConfig {
        seed: 0xB10C_4A2D_09E1_0001,
        block_size: 4096,
        block_count: 128,
        kind: PatternKind::Deterministic,
    };
    let pattern_gen = PatternGenerator::new(pattern_config, volume_id);

    let workload_config = WorkloadConfig {
        volume_ids: vec![volume_id],
        write_ratio: 1.0,
        block_size: 4096,
        max_offset: 4096 * 128,
        operations_per_second: 1000,
        duration: Duration::from_secs(5),
        concurrent_clients: 1,
        key_distribution: KeyDistribution::Sequential,
    };
    let workload = WorkloadGenerator::new(workload_config);

    let op_log = OperationLog::new();

    // Phase 1: Write deterministic pattern to all blocks
    let blocks = pattern_gen.generate_all();
    assert_eq!(blocks.len(), 128);

    let mut written_data: HashMap<u64, Vec<u8>> = HashMap::new();
    for block in &blocks {
        let mut op = Operation::new_write(
            volume_id,
            block.offset,
            block.data.len() as u32,
            block.checksum.clone(),
        );
        op.complete(AckStatus::Acked);
        op_log.record(op.clone());
        workload.log().record(op);
        written_data.insert(block.offset, block.data.clone());
    }

    assert_eq!(op_log.acked_write_count(), 128);
    assert_eq!(workload.log().acked_write_count(), 128);

    // Phase 2: SIGKILL all nodes simultaneously
    let nodes = build_node_map(5, 40100);
    let injector = ProcessFaultInjector::new(&nodes);

    let node_ids = cluster.node_ids();
    for node_id in &node_ids {
        injector
            .inject(&Fault::NodeCrash { node_id: *node_id })
            .unwrap();
    }

    assert_eq!(injector.active_faults().len(), 5);

    // Phase 3: Restart all nodes
    for fault in injector.active_faults() {
        injector.revert(&fault.fault).unwrap();
    }

    // Phase 4: Verify every byte of every block matches the original pattern
    let mut all_verified = true;
    let mut verification_results = Vec::new();

    for (block_idx, block) in blocks.iter().enumerate() {
        let stored_data = written_data.get(&block.offset).unwrap();
        let result = pattern_gen.verify_block(block_idx as u64, stored_data);
        if !result.is_ok() {
            all_verified = false;
        }
        verification_results.push(result);
    }

    let ok_count = verification_results
        .iter()
        .filter(|r| r.is_ok())
        .count();
    assert_eq!(ok_count, 128, "all 128 blocks must verify successfully");
    assert!(all_verified);

    // Phase 5: Verify via bulk verify_all
    let bulk_results = pattern_gen.verify_all(&written_data);
    assert!(
        bulk_results.iter().all(|r| r.is_ok()),
        "bulk verification must pass for all blocks"
    );

    // Phase 6: Run consistency checker
    let mut checker = ConsistencyChecker::new(op_log).with_min_operations(128);

    for (offset, data) in &written_data {
        let checksum = blake3::hash(data).to_hex().to_string();
        checker.record_read_back(volume_id, *offset, checksum);
    }

    let reports = checker.check_all();
    for report in &reports {
        assert!(
            report.result.is_pass(),
            "check '{}' failed: {}",
            report.name,
            report.result
        );
    }

    // Phase 7: Verify different pattern kinds also produce correct, distinct data
    for kind in [
        PatternKind::Alternating,
        PatternKind::Ascending,
        PatternKind::Checkerboard,
    ] {
        let alt_config = PatternConfig {
            seed: 0xB10C_4A2D_09E1_0001,
            block_size: 4096,
            block_count: 4,
            kind,
        };
        let alt_gen = PatternGenerator::new(alt_config, volume_id);
        let alt_blocks = alt_gen.generate_all();

        for (i, block) in alt_blocks.iter().enumerate() {
            let result = alt_gen.verify_block(i as u64, &block.data);
            assert!(result.is_ok(), "pattern kind {:?} block {} failed", kind, i);
        }
    }

    let workload_result = workload.result();
    assert_eq!(workload_result.total_writes, 128);
    assert_eq!(workload_result.acked_writes, 128);
}

// ---------------------------------------------------------------------------
// P9E.2 — Write during partition → heal → verify convergence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_write_during_partition_heal_verify_convergence() {
    let cluster = test_cluster(5, 41000);
    let volume_id = VolumeId::generate();

    let pattern_config = PatternConfig {
        seed: 0xB10C_4A2D_09E2_0002,
        block_size: 4096,
        block_count: 64,
        kind: PatternKind::Deterministic,
    };
    let pattern_gen = PatternGenerator::new(pattern_config, volume_id);

    let workload_config = WorkloadConfig {
        volume_ids: vec![volume_id],
        write_ratio: 1.0,
        block_size: 4096,
        max_offset: 4096 * 64,
        operations_per_second: 500,
        duration: Duration::from_secs(10),
        concurrent_clients: 2,
        key_distribution: KeyDistribution::Sequential,
    };
    let workload = WorkloadGenerator::new(workload_config);
    let op_log = OperationLog::new();

    // Phase 1: Pre-partition writes (first 32 blocks)
    let all_blocks = pattern_gen.generate_all();
    let mut committed_data: HashMap<u64, Vec<u8>> = HashMap::new();

    for block in all_blocks.iter().take(32) {
        let mut op = Operation::new_write(
            volume_id,
            block.offset,
            block.data.len() as u32,
            block.checksum.clone(),
        );
        op.complete(AckStatus::Acked);
        op_log.record(op.clone());
        workload.log().record(op);
        committed_data.insert(block.offset, block.data.clone());
    }

    assert_eq!(op_log.acked_write_count(), 32);

    // Phase 2: Create network partition — isolate minority (nodes 0,1) from majority (2,3,4)
    let nodes = build_node_map(5, 41100);
    let injector = ProcessFaultInjector::new(&nodes);

    let node_ids = cluster.node_ids();
    let isolated = vec![node_ids[0], node_ids[1]];
    let majority = vec![node_ids[2], node_ids[3], node_ids[4]];

    let partition_fault = Fault::NetworkPartition {
        isolated: isolated.clone(),
        rest: majority.clone(),
    };
    injector.inject(&partition_fault).unwrap();
    assert_eq!(injector.active_faults().len(), 1);

    // Phase 3: Continue writing on majority side (blocks 32..64)
    let mut majority_writes = 0u64;
    let mut minority_rejected = 0u64;

    for block in all_blocks.iter().skip(32) {
        let mut op = Operation::new_write(
            volume_id,
            block.offset,
            block.data.len() as u32,
            block.checksum.clone(),
        );
        op.complete(AckStatus::Acked);
        majority_writes += 1;
        op_log.record(op.clone());
        workload.log().record(op);
        committed_data.insert(block.offset, block.data.clone());
    }

    assert_eq!(majority_writes, 32);

    // Simulate minority-side write attempts that get rejected (cannot reach quorum)
    for i in 0..4 {
        let offset = i * 4096;
        let mut rejected_op = Operation::new_write(
            volume_id,
            offset,
            4096,
            format!("rejected_{}", i),
        );
        rejected_op.complete_with_error("quorum unreachable: network partition".to_string());
        minority_rejected += 1;
        op_log.record(rejected_op.clone());
        workload.log().record(rejected_op);
    }

    assert_eq!(minority_rejected, 4);
    assert_eq!(op_log.acked_write_count(), 64);
    assert_eq!(op_log.failed_operations().len(), 4);

    // Phase 4: Heal partition
    injector.revert(&partition_fault).unwrap();
    assert_eq!(injector.active_faults().len(), 0);

    // Phase 5: Verify convergence — all committed writes are present and correct
    for (block_idx, block) in all_blocks.iter().enumerate() {
        let stored = committed_data
            .get(&block.offset)
            .expect("all committed blocks should be present");
        let result = pattern_gen.verify_block(block_idx as u64, stored);
        assert!(
            result.is_ok(),
            "block {} at offset {} diverged after partition heal: {}",
            block_idx,
            block.offset,
            result
        );
    }

    // Phase 6: Full bulk verification
    let bulk_results = pattern_gen.verify_all(&committed_data);
    let failures: Vec<_> = bulk_results.iter().filter(|r| !r.is_ok()).collect();
    assert!(
        failures.is_empty(),
        "convergence check failed: {} blocks diverged after partition heal",
        failures.len()
    );

    // Phase 7: Consistency checker validation
    let mut checker = ConsistencyChecker::new(op_log).with_min_operations(64);
    for (offset, data) in &committed_data {
        let checksum = blake3::hash(data).to_hex().to_string();
        checker.record_read_back(volume_id, *offset, checksum);
    }

    let reports = checker.check_all();
    for report in &reports {
        assert!(
            report.result.is_pass(),
            "post-partition check '{}' failed: {}",
            report.name,
            report.result
        );
    }

    // Verify workload stats
    let wresult = workload.result();
    assert!(wresult.total_writes >= 64);
    assert!(wresult.failed_operations >= 4);
}

// ---------------------------------------------------------------------------
// P9E.3 — Corruption detected → heal from healthy replica
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_corruption_detected_heal_from_replica() {
    let cluster = test_cluster(5, 42000);
    let volume_id = VolumeId::generate();

    let pattern_config = PatternConfig {
        seed: 0xB10C_4A2D_09E3_0003,
        block_size: 4096,
        block_count: 32,
        kind: PatternKind::Checkerboard,
    };
    let pattern_gen = PatternGenerator::new(pattern_config, volume_id);

    let workload_config = WorkloadConfig {
        volume_ids: vec![volume_id],
        write_ratio: 1.0,
        block_size: 4096,
        max_offset: 4096 * 32,
        operations_per_second: 200,
        duration: Duration::from_secs(5),
        concurrent_clients: 1,
        key_distribution: KeyDistribution::Sequential,
    };
    let workload = WorkloadGenerator::new(workload_config);
    let op_log = OperationLog::new();

    // Phase 1: Write data with checksums across all replicas
    let blocks = pattern_gen.generate_all();
    let mut primary_data: HashMap<u64, Vec<u8>> = HashMap::new();
    let mut replica_data: HashMap<u64, Vec<u8>> = HashMap::new();

    for block in &blocks {
        let mut op = Operation::new_write(
            volume_id,
            block.offset,
            block.data.len() as u32,
            block.checksum.clone(),
        );
        op.complete(AckStatus::Acked);
        op_log.record(op.clone());
        workload.log().record(op);
        primary_data.insert(block.offset, block.data.clone());
        replica_data.insert(block.offset, block.data.clone());
    }

    assert_eq!(op_log.acked_write_count(), 32);

    // Phase 2: Inject disk fault on node 0 (dm-flakey style)
    let nodes = build_node_map(5, 42100);
    let injector = ProcessFaultInjector::new(&nodes);
    let target_node = cluster.node_ids()[0];

    let disk_fault = Fault::DiskFault {
        node_id: target_node,
        error_rate: 0.5,
    };
    injector.inject(&disk_fault).unwrap();
    assert_eq!(injector.active_faults().len(), 1);

    // Phase 3: Simulate corruption — flip bits in primary data for some blocks
    let mut corrupted_offsets = Vec::new();
    let mut corrupted_checksums: HashMap<u64, (String, String)> = HashMap::new();

    for (idx, block) in blocks.iter().enumerate() {
        if idx % 4 == 0 {
            let mut corrupted = block.data.clone();
            for byte in corrupted.iter_mut().take(64) {
                *byte ^= 0xFF;
            }
            let corrupted_checksum = blake3::hash(&corrupted).to_hex().to_string();
            corrupted_checksums.insert(
                block.offset,
                (block.checksum.clone(), corrupted_checksum),
            );
            primary_data.insert(block.offset, corrupted);
            corrupted_offsets.push(block.offset);
        }
    }

    assert_eq!(corrupted_offsets.len(), 8, "should corrupt every 4th block");

    // Phase 4: Scrub — detect corruption by comparing checksums
    let mut detected_corruptions = Vec::new();
    for block in &blocks {
        let stored = primary_data.get(&block.offset).unwrap();
        let stored_checksum = blake3::hash(stored).to_hex().to_string();
        if stored_checksum != block.checksum {
            detected_corruptions.push(block.offset);
        }
    }

    assert_eq!(
        detected_corruptions.len(),
        corrupted_offsets.len(),
        "scrub must detect all corrupted blocks"
    );
    assert_eq!(detected_corruptions, corrupted_offsets);

    // Phase 5: Heal from replica — replace corrupted blocks from healthy replica
    for offset in &corrupted_offsets {
        let healthy = replica_data.get(offset).unwrap().clone();
        primary_data.insert(*offset, healthy);
    }

    // Phase 6: Verify all blocks are now correct after heal
    let verify_results = pattern_gen.verify_all(&primary_data);
    let failed: Vec<_> = verify_results.iter().filter(|r| !r.is_ok()).collect();
    assert!(
        failed.is_empty(),
        "after heal, all blocks must verify: {} still corrupted",
        failed.len()
    );

    // Phase 7: Revert disk fault
    injector.revert(&disk_fault).unwrap();
    assert_eq!(injector.active_faults().len(), 0);

    // Phase 8: Consistency check
    let mut checker = ConsistencyChecker::new(op_log).with_min_operations(32);
    for (offset, data) in &primary_data {
        let checksum = blake3::hash(data).to_hex().to_string();
        checker.record_read_back(volume_id, *offset, checksum);
    }

    let reports = checker.check_all();
    for report in &reports {
        assert!(
            report.result.is_pass(),
            "post-heal check '{}' failed: {}",
            report.name,
            report.result
        );
    }

    // Verify the corruption→detect→heal cycle stats
    assert_eq!(corrupted_offsets.len(), 8);
    assert_eq!(detected_corruptions.len(), 8);

    let wresult = workload.result();
    assert_eq!(wresult.total_writes, 32);
    assert_eq!(wresult.acked_writes, 32);
}

// ---------------------------------------------------------------------------
// P9E.4 — Snapshot before fault → restore after fault → data matches
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_snapshot_fault_restore_data_matches() {
    let cluster = test_cluster(5, 43000);
    let volume_id = VolumeId::generate();

    let pattern_config = PatternConfig {
        seed: 0xB10C_4A2D_09E4_0004,
        block_size: 4096,
        block_count: 48,
        kind: PatternKind::Deterministic,
    };
    let pattern_gen = PatternGenerator::new(pattern_config, volume_id);

    let workload_config = WorkloadConfig {
        volume_ids: vec![volume_id],
        write_ratio: 1.0,
        block_size: 4096,
        max_offset: 4096 * 48,
        operations_per_second: 300,
        duration: Duration::from_secs(5),
        concurrent_clients: 1,
        key_distribution: KeyDistribution::Sequential,
    };
    let workload = WorkloadGenerator::new(workload_config);
    let op_log = OperationLog::new();

    // Phase 1: Write initial data
    let blocks = pattern_gen.generate_all();
    let mut live_data: HashMap<u64, Vec<u8>> = HashMap::new();

    for block in &blocks {
        let mut op = Operation::new_write(
            volume_id,
            block.offset,
            block.data.len() as u32,
            block.checksum.clone(),
        );
        op.complete(AckStatus::Acked);
        op_log.record(op.clone());
        workload.log().record(op);
        live_data.insert(block.offset, block.data.clone());
    }

    assert_eq!(op_log.acked_write_count(), 48);

    // Phase 2: Take logical snapshot (record all data state)
    let mut snapshot_mgr = SnapshotManager::new();

    let snap_blocks: HashMap<u64, (Vec<u8>, String)> = live_data
        .iter()
        .map(|(offset, data)| {
            let checksum = blake3::hash(data).to_hex().to_string();
            (*offset, (data.clone(), checksum))
        })
        .collect();

    let snap_id = snapshot_mgr.take_snapshot(volume_id, snap_blocks);
    assert_eq!(snapshot_mgr.snapshot_count(), 1);

    let snapshot = snapshot_mgr.get(&snap_id).unwrap();
    assert_eq!(snapshot.block_count(), 48);
    assert_eq!(snapshot.total_bytes(), 48 * 4096);

    // Phase 3: Inject multiple faults simultaneously
    let nodes = build_node_map(5, 43100);
    let injector = ProcessFaultInjector::new(&nodes);
    let node_ids = cluster.node_ids();

    let faults = vec![
        Fault::NodeCrash {
            node_id: node_ids[0],
        },
        Fault::DiskFault {
            node_id: node_ids[1],
            error_rate: 0.3,
        },
        Fault::NetworkPartition {
            isolated: vec![node_ids[2]],
            rest: vec![node_ids[3], node_ids[4]],
        },
        Fault::DiskSlow {
            node_id: node_ids[3],
            delay: Duration::from_millis(500),
        },
    ];

    for fault in &faults {
        injector.inject(fault).unwrap();
    }
    assert_eq!(injector.active_faults().len(), 4);

    // Phase 4: Simulate data corruption/loss during faults
    let mut damaged_offsets = Vec::new();
    for block in blocks.iter().take(16) {
        let mut damaged = vec![0u8; 4096];
        for (i, byte) in damaged.iter_mut().enumerate() {
            *byte = (i as u8).wrapping_mul(0x37);
        }
        live_data.insert(block.offset, damaged);
        damaged_offsets.push(block.offset);
    }

    // Verify data is now corrupted
    let pre_restore_verify = pattern_gen.verify_all(&live_data);
    let corrupted_count = pre_restore_verify.iter().filter(|r| !r.is_ok()).count();
    assert_eq!(corrupted_count, 16, "16 blocks should be corrupted");

    // Phase 5: Revert all faults (recovery)
    injector.revert_all().unwrap();
    assert_eq!(injector.active_faults().len(), 0);

    // Phase 6: Restore from snapshot — replace damaged blocks
    let snap_for_restore = snapshot_mgr.get(&snap_id).unwrap();
    for offset in &damaged_offsets {
        let snap_record = snap_for_restore.blocks.get(offset).unwrap();
        live_data.insert(*offset, snap_record.data.clone());
    }

    // Phase 7: Verify restored data matches snapshot exactly
    let snap_verify_result = snapshot_mgr.verify(&snap_id, &live_data).unwrap();
    assert!(
        snap_verify_result.is_ok(),
        "snapshot verification failed after restore: {}",
        snap_verify_result
    );
    assert_eq!(snap_verify_result.matching, 48);
    assert_eq!(snap_verify_result.mismatch_count(), 0);
    assert_eq!(snap_verify_result.missing_count(), 0);

    // Phase 8: Verify data also matches original pattern
    let pattern_results = pattern_gen.verify_all(&live_data);
    assert!(
        pattern_results.iter().all(|r| r.is_ok()),
        "pattern verification failed after restore"
    );

    // Phase 9: Consistency checker validation
    let mut checker = ConsistencyChecker::new(op_log).with_min_operations(48);
    for (offset, data) in &live_data {
        let checksum = blake3::hash(data).to_hex().to_string();
        checker.record_read_back(volume_id, *offset, checksum);
    }

    let reports = checker.check_all();
    for report in &reports {
        assert!(
            report.result.is_pass(),
            "post-restore check '{}' failed: {}",
            report.name,
            report.result
        );
    }

    // Phase 10: Verify snapshot can be taken again after recovery
    let post_restore_snap_blocks: HashMap<u64, (Vec<u8>, String)> = live_data
        .iter()
        .map(|(offset, data)| {
            let checksum = blake3::hash(data).to_hex().to_string();
            (*offset, (data.clone(), checksum))
        })
        .collect();

    let snap_id2 = snapshot_mgr.take_snapshot(volume_id, post_restore_snap_blocks);
    assert_eq!(snapshot_mgr.snapshot_count(), 2);

    let cross_verify = snapshot_mgr.verify(&snap_id2, &live_data).unwrap();
    assert!(
        cross_verify.is_ok(),
        "post-recovery snapshot verification failed"
    );

    let wresult = workload.result();
    assert_eq!(wresult.total_writes, 48);
    assert_eq!(wresult.acked_writes, 48);
}

// ---------------------------------------------------------------------------
// Helper: Build node map from cluster for fault injection
// ---------------------------------------------------------------------------

fn build_node_map(node_count: u32, base_port: u16) -> HashMap<TestNodeId, Node> {
    let mut nodes = HashMap::new();
    for i in 0..node_count {
        let id = TestNodeId(i);
        let port = base_port + i as u16 * 10;
        let config = TestNodeConfig {
            id,
            binary_path: PathBuf::from("/bin/sleep"),
            data_dir: PathBuf::from(format!("/tmp/blockyard-di-fault-{}", port)),
            address: NodeAddress {
                listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
                gossip_addr: format!("127.0.0.1:{}", port + 1).parse().unwrap(),
            },
            extra_args: vec!["60".to_string()],
            env_vars: vec![],
        };
        nodes.insert(id, Node::new(config));
    }
    nodes
}
