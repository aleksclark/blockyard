use std::time::Duration;

use blockyard_common::VolumeId;
use blockyard_test_harness::checker::ConsistencyChecker;
use blockyard_test_harness::scenario::{
    AckPolicy, MountState, ReadPolicy, ScenarioConfig, ScenarioContext, UblkMount,
};
use blockyard_test_harness::vm::NodeId;
use blockyard_test_harness::workload::{AckStatus, Operation, OperationLog, WorkloadConfig};

fn write_heavy_workload(volume_id: VolumeId) -> WorkloadConfig {
    WorkloadConfig {
        volume_ids: vec![volume_id],
        write_ratio: 1.0,
        block_size: 4096,
        max_offset: 4096 * 128,
        operations_per_second: 100,
        duration: Duration::from_secs(5),
        concurrent_clients: 1,
        ..Default::default()
    }
}

fn assert_all_checks_pass(reports: &[blockyard_test_harness::checker::CheckReport]) {
    for report in reports {
        assert!(
            report.result.is_pass(),
            "check '{}' failed: {}",
            report.name,
            report.result
        );
    }
}

// ---------------------------------------------------------------------------
// P9F.1 — Mount → write → kill mount process → remount → verify data
// ---------------------------------------------------------------------------

/// Mount volume, write known pattern, SIGKILL the mount process, remount,
/// verify all previously-acked writes are intact.
#[test]
fn test_mount_write_kill_remount_verify() {
    let volume_id = VolumeId::generate();
    let config = ScenarioConfig::new(3)
        .with_ack_policy(AckPolicy::All)
        .with_read_policy(ReadPolicy::Leader)
        .with_workload(write_heavy_workload(volume_id))
        .with_base_port(44000);
    let ctx = ScenarioContext::new(config);

    let mut mount = UblkMount::new(volume_id);
    assert_eq!(mount.state, MountState::Unmounted);

    mount.mount(ctx.leader());
    assert!(mount.is_mounted());
    assert_eq!(mount.connected_leader, Some(NodeId(0)));

    let writes = ctx.simulate_writes_with_acks(20, volume_id);
    let acked_writes: Vec<_> = writes
        .iter()
        .filter(|op| op.status == AckStatus::Acked)
        .cloned()
        .collect();
    assert!(!acked_writes.is_empty());

    for _ in &acked_writes {
        mount.record_write();
    }
    assert_eq!(mount.writes_completed, acked_writes.len() as u64);

    mount.crash();
    assert_eq!(mount.state, MountState::Crashed);
    assert!(!mount.is_mounted());

    mount.remount(ctx.leader());
    assert!(mount.is_mounted());
    assert_eq!(mount.connected_leader, Some(ctx.leader()));

    let new_log = OperationLog::new();
    for op in ctx.workload.log().all() {
        new_log.record(op);
    }
    let mut checker = ConsistencyChecker::new(new_log);

    for w in &acked_writes {
        if let Some(cs) = &w.data_checksum {
            let mut read_op = Operation::new_read(w.volume_id, w.offset, w.length);
            read_op.complete(AckStatus::Acked);
            ctx.workload.log().record(read_op);
            mount.record_read();

            checker.record_read_back(w.volume_id, w.offset, cs.clone());
        }
    }

    let reports = checker.check_all();
    assert_all_checks_pass(&reports);

    assert_eq!(mount.reads_completed, acked_writes.len() as u64);
}

// ---------------------------------------------------------------------------
// P9F.2 — Mount → partition client from leader → client follows new leader →
//          writes succeed
// ---------------------------------------------------------------------------

/// Mount volume, partition the client from the current leader, verify client
/// discovers and follows new leader, and writes continue to succeed.
#[test]
fn test_mount_partition_follow_new_leader() {
    let volume_id = VolumeId::generate();
    let config = ScenarioConfig::new(5)
        .with_ack_policy(AckPolicy::Majority)
        .with_read_policy(ReadPolicy::Leader)
        .with_workload(write_heavy_workload(volume_id))
        .with_base_port(44100);
    let mut ctx = ScenarioContext::new(config);

    let mut mount = UblkMount::new(volume_id);
    mount.mount(ctx.leader());
    assert!(mount.is_mounted());

    let pre_writes = ctx.simulate_writes_with_acks(10, volume_id);
    let pre_acked: Vec<_> = pre_writes
        .iter()
        .filter(|op| op.status == AckStatus::Acked)
        .collect();
    assert!(!pre_acked.is_empty());
    for _ in &pre_acked {
        mount.record_write();
    }

    let old_leader = ctx.leader();
    let new_leader = ctx.simulate_leader_election(old_leader);
    assert_ne!(new_leader, old_leader);

    mount.follow_new_leader(new_leader);
    assert_eq!(mount.connected_leader, Some(new_leader));
    assert!(mount.is_mounted());

    let post_writes = ctx.simulate_writes_with_acks(15, volume_id);
    let post_acked: Vec<_> = post_writes
        .iter()
        .filter(|op| op.status == AckStatus::Acked)
        .collect();
    assert!(
        !post_acked.is_empty(),
        "writes must succeed after following new leader"
    );
    for _ in &post_acked {
        mount.record_write();
    }

    ctx.recover_node(old_leader);

    let all_writes: Vec<Operation> = pre_writes
        .iter()
        .chain(post_writes.iter())
        .cloned()
        .collect();

    let new_log = OperationLog::new();
    for op in ctx.workload.log().all() {
        new_log.record(op);
    }
    let mut checker = ConsistencyChecker::new(new_log);

    for w in &all_writes {
        if w.status == AckStatus::Acked {
            if let Some(cs) = &w.data_checksum {
                checker.record_read_back(w.volume_id, w.offset, cs.clone());
            }
        }
    }

    let reports = checker.check_all();
    assert_all_checks_pass(&reports);

    assert!(
        mount.writes_completed >= 20,
        "total writes should be at least 20, got {}",
        mount.writes_completed
    );
}

// ---------------------------------------------------------------------------
// P9F.3 — Mount → write through ext4 → crash node → remount → fsck passes
// ---------------------------------------------------------------------------

/// Mount volume, write through filesystem (simulated), crash the data node
/// serving the volume, remount on a surviving node, verify data integrity
/// (simulated fsck).
#[test]
fn test_mount_write_crash_node_remount_fsck() {
    let volume_id = VolumeId::generate();
    let config = ScenarioConfig::new(3)
        .with_ack_policy(AckPolicy::All)
        .with_read_policy(ReadPolicy::Leader)
        .with_workload(write_heavy_workload(volume_id))
        .with_base_port(44200);
    let mut ctx = ScenarioContext::new(config);

    let mut mount = UblkMount::new(volume_id);
    mount.mount(ctx.leader());
    assert!(mount.is_mounted());

    let writes = ctx.simulate_writes_with_acks(25, volume_id);
    let acked_writes: Vec<_> = writes
        .iter()
        .filter(|op| op.status == AckStatus::Acked)
        .cloned()
        .collect();
    assert!(!acked_writes.is_empty());
    for _ in &acked_writes {
        mount.record_write();
    }

    let crashed_node = ctx.leader();
    let new_leader = ctx.simulate_leader_election(crashed_node);
    assert_ne!(new_leader, crashed_node);

    mount.crash();
    assert_eq!(mount.state, MountState::Crashed);

    mount.remount(new_leader);
    assert!(mount.is_mounted());
    assert_eq!(mount.connected_leader, Some(new_leader));

    let new_log = OperationLog::new();
    for op in ctx.workload.log().all() {
        new_log.record(op);
    }
    let mut checker = ConsistencyChecker::new(new_log);

    let mut fsck_errors = 0u32;
    for w in &acked_writes {
        if let Some(cs) = &w.data_checksum {
            let mut read_op = Operation::new_read(w.volume_id, w.offset, w.length);
            read_op.complete(AckStatus::Acked);
            ctx.workload.log().record(read_op);
            mount.record_read();

            checker.record_read_back(w.volume_id, w.offset, cs.clone());
        } else {
            fsck_errors += 1;
        }
    }

    assert_eq!(fsck_errors, 0, "fsck: no data integrity errors expected");

    let reports = checker.check_all();
    assert_all_checks_pass(&reports);

    let integrity = checker.check_data_integrity();
    assert!(
        integrity.result.is_pass(),
        "data integrity (fsck equivalent) must pass after crash recovery: {}",
        integrity.result
    );

    let no_lost = checker.check_no_lost_acks();
    assert!(
        no_lost.result.is_pass(),
        "no acked writes should be lost after node crash and remount: {}",
        no_lost.result
    );
}
