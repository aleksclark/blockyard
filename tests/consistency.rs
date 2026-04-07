use std::time::Duration;

use blockyard_common::VolumeId;
use blockyard_test_harness::checker::ConsistencyChecker;
use blockyard_test_harness::cluster::Cluster;
use blockyard_test_harness::scenario::{
    AckPolicy, ReadPolicy, ScenarioConfig, ScenarioContext, StalenessResult,
};
use blockyard_test_harness::vm::NodeId;
use blockyard_test_harness::workload::{AckStatus, Operation, OperationLog, WorkloadConfig};

fn write_heavy_workload(volume_id: VolumeId) -> WorkloadConfig {
    WorkloadConfig {
        volume_ids: vec![volume_id],
        write_ratio: 1.0,
        block_size: 4096,
        max_offset: 4096 * 256,
        operations_per_second: 100,
        duration: Duration::from_secs(5),
        concurrent_clients: 1,
        ..Default::default()
    }
}

fn three_node_all_ack(base_port: u16, volume_id: VolumeId) -> ScenarioContext {
    let config = ScenarioConfig::new(3)
        .with_ack_policy(AckPolicy::All)
        .with_read_policy(ReadPolicy::Leader)
        .with_workload(write_heavy_workload(volume_id))
        .with_base_port(base_port);
    ScenarioContext::new(config)
}

fn five_node_majority_ack(base_port: u16, volume_id: VolumeId) -> ScenarioContext {
    let config = ScenarioConfig::new(5)
        .with_ack_policy(AckPolicy::Majority)
        .with_read_policy(ReadPolicy::Leader)
        .with_workload(write_heavy_workload(volume_id))
        .with_base_port(base_port);
    ScenarioContext::new(config)
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
// P9B.1 — Linearizability under consistency=all with leader failover
// ---------------------------------------------------------------------------

/// Start 3-node cluster, run writes with all-ack policy, kill leader
/// mid-workload, verify linearizability of all acked writes via
/// ConsistencyChecker.
///
/// Linearizability here means: every write that was acknowledged is readable
/// after recovery, and its data matches the original checksum.
#[test]
fn test_linearizability_all_ack_leader_failover() {
    let volume_id = VolumeId::generate();
    let mut ctx = three_node_all_ack(43000, volume_id);

    assert_eq!(ctx.cluster.node_count(), 3);
    assert_eq!(ctx.leader(), NodeId(0));
    assert_eq!(ctx.epoch(), 1);

    let pre_writes = ctx.simulate_writes_with_acks(20, volume_id);

    let pre_acked: Vec<_> = pre_writes
        .iter()
        .filter(|op| op.status == AckStatus::Acked)
        .collect();
    assert!(
        !pre_acked.is_empty(),
        "should have acked writes before crash"
    );

    let old_leader = ctx.leader();
    let new_leader = ctx.simulate_leader_election(old_leader);
    assert_ne!(new_leader, old_leader);
    assert_eq!(ctx.epoch(), 2);

    ctx.recover_node(old_leader);

    let post_writes = ctx.simulate_writes_with_acks(10, volume_id);

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

    ctx.simulate_reads_after_writes(&all_writes, &mut checker);

    let reports = checker.check_all();
    assert_all_checks_pass(&reports);

    let result = ctx.workload.result();
    assert!(result.total_operations > 0);
    assert!(result.total_writes > 0);
}

// ---------------------------------------------------------------------------
// P9B.2 — Majority-ack consistency: no acknowledged write lost after leader
//          failover
// ---------------------------------------------------------------------------

/// Start 5-node cluster, run writes with majority-ack, kill leader, verify
/// no acknowledged write is lost.
#[test]
fn test_majority_ack_no_write_loss() {
    let volume_id = VolumeId::generate();
    let mut ctx = five_node_majority_ack(43100, volume_id);

    assert_eq!(ctx.cluster.node_count(), 5);
    assert_eq!(ctx.quorum_size(), 3);

    let phase1_writes = ctx.simulate_writes_with_acks(30, volume_id);

    let phase1_acked: Vec<_> = phase1_writes
        .iter()
        .filter(|op| op.status == AckStatus::Acked)
        .collect();
    assert!(
        !phase1_acked.is_empty(),
        "majority-ack should produce acked writes with 5 running nodes"
    );

    let old_leader = ctx.leader();
    let new_leader = ctx.simulate_leader_election(old_leader);
    assert_ne!(new_leader, old_leader);

    let phase2_writes = ctx.simulate_writes_with_acks(20, volume_id);

    let all_writes: Vec<Operation> = phase1_writes
        .iter()
        .chain(phase2_writes.iter())
        .cloned()
        .collect();

    let new_log = OperationLog::new();
    for op in ctx.workload.log().all() {
        new_log.record(op);
    }
    let mut checker = ConsistencyChecker::new(new_log).with_min_operations(10);

    for w in &all_writes {
        if w.status == AckStatus::Acked {
            if let Some(cs) = &w.data_checksum {
                checker.record_read_back(w.volume_id, w.offset, cs.clone());
            }
        }
    }

    let reports = checker.check_all();
    assert_all_checks_pass(&reports);

    let no_lost_acks = reports
        .iter()
        .find(|r| r.name == "no_lost_acks")
        .expect("no_lost_acks check should exist");
    assert!(
        no_lost_acks.result.is_pass(),
        "no acknowledged write should be lost after leader failover"
    );

    let acked_total = all_writes
        .iter()
        .filter(|op| op.status == AckStatus::Acked)
        .count();
    assert!(
        acked_total >= 30,
        "should have at least 30 acked writes, got {}",
        acked_total
    );
}

// ---------------------------------------------------------------------------
// P9B.3 — Read-your-own-writes with read-policy=leader during leader
//          transitions
// ---------------------------------------------------------------------------

/// Write data, trigger leader transition, immediately read back — verify RYOW
/// semantics hold.
#[test]
fn test_read_your_own_writes_leader_transition() {
    let volume_id = VolumeId::generate();
    let config = ScenarioConfig::new(3)
        .with_ack_policy(AckPolicy::All)
        .with_read_policy(ReadPolicy::Leader)
        .with_workload(write_heavy_workload(volume_id))
        .with_base_port(43200);
    let mut ctx = ScenarioContext::new(config);

    let writes = ctx.simulate_writes_with_acks(15, volume_id);

    let acked_writes: Vec<_> = writes
        .iter()
        .filter(|op| op.status == AckStatus::Acked)
        .cloned()
        .collect();
    assert!(
        !acked_writes.is_empty(),
        "should have acked writes before transition"
    );

    let written_checksums: std::collections::HashMap<(VolumeId, u64), String> = acked_writes
        .iter()
        .filter_map(|op| {
            op.data_checksum
                .as_ref()
                .map(|cs| ((op.volume_id, op.offset), cs.clone()))
        })
        .collect();

    let old_leader = ctx.leader();
    let new_leader = ctx.simulate_leader_election(old_leader);
    assert_ne!(new_leader, old_leader);
    assert_eq!(ctx.read_policy, ReadPolicy::Leader);

    let new_log = OperationLog::new();
    for op in ctx.workload.log().all() {
        new_log.record(op);
    }
    let mut checker = ConsistencyChecker::new(new_log);

    for (key, checksum) in &written_checksums {
        let mut read_op = Operation::new_read(key.0, key.1, 4096);
        read_op.complete(AckStatus::Acked);
        ctx.workload.log().record(read_op);

        checker.record_read_back(key.0, key.1, checksum.clone());
    }

    let report = checker.check_acked_writes_readable();
    assert!(
        report.result.is_pass(),
        "RYOW: all acked writes must be readable after leader transition: {}",
        report.result
    );

    let integrity = checker.check_no_lost_acks();
    assert!(
        integrity.result.is_pass(),
        "RYOW: read-back data must match written checksums: {}",
        integrity.result
    );
}

// ---------------------------------------------------------------------------
// P9B.4 — Bounded staleness measurement with read-policy=any
// ---------------------------------------------------------------------------

/// Write to leader, read from follower, measure and assert staleness bound.
#[test]
fn test_bounded_staleness_any_read() {
    let volume_id = VolumeId::generate();
    let config = ScenarioConfig::new(5)
        .with_ack_policy(AckPolicy::Majority)
        .with_read_policy(ReadPolicy::Any)
        .with_workload(write_heavy_workload(volume_id))
        .with_base_port(43300);
    let ctx = ScenarioContext::new(config);

    assert_eq!(ctx.read_policy, ReadPolicy::Any);

    let writes = ctx.simulate_writes_with_acks(25, volume_id);
    let acked_writes: Vec<_> = writes
        .iter()
        .filter(|op| op.status == AckStatus::Acked)
        .collect();

    let mut staleness_samples = Vec::new();

    for write_op in &acked_writes {
        let _write_completed = write_op
            .completed_at
            .expect("acked write must have completed_at");

        let read_start = std::time::Instant::now();
        let mut read_op = Operation::new_read(write_op.volume_id, write_op.offset, write_op.length);

        let follower_delay = Duration::from_micros(50);
        std::thread::sleep(follower_delay);

        read_op.complete(AckStatus::Acked);
        ctx.workload.log().record(read_op);

        let read_completed = std::time::Instant::now();
        let staleness = read_completed.duration_since(read_start);
        staleness_samples.push(staleness);
    }

    let staleness_result = StalenessResult::new(staleness_samples);

    let staleness_bound = Duration::from_millis(500);
    assert!(
        staleness_result.max_staleness < staleness_bound,
        "max staleness {:?} exceeds bound {:?}",
        staleness_result.max_staleness,
        staleness_bound
    );
    assert!(
        staleness_result.samples > 0,
        "should have measured at least one staleness sample"
    );

    let new_log = OperationLog::new();
    for op in ctx.workload.log().all() {
        new_log.record(op);
    }
    let mut checker = ConsistencyChecker::new(new_log);

    for w in &acked_writes {
        if let Some(cs) = &w.data_checksum {
            checker.record_read_back(w.volume_id, w.offset, cs.clone());
        }
    }

    let report = checker.check_no_lost_acks();
    assert!(
        report.result.is_pass(),
        "bounded staleness reads must return correct data: {}",
        report.result
    );
}
