use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use tracing::info;

use crate::checker::{CheckReport, ConsistencyChecker};
use crate::cluster::{Cluster, ClusterConfig, ProcessCluster};
use crate::network::NetworkConfig;
use crate::vm::NodeId;
use crate::workload::{
    AckStatus, Operation, OperationLog, WorkloadConfig, WorkloadGenerator,
};
use blockyard_common::VolumeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckPolicy {
    All,
    Majority,
    One,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPolicy {
    Leader,
    Any,
}

#[derive(Debug, Clone)]
pub struct ScenarioConfig {
    pub node_count: u32,
    pub ack_policy: AckPolicy,
    pub read_policy: ReadPolicy,
    pub workload: WorkloadConfig,
    pub base_port: u16,
}

impl ScenarioConfig {
    pub fn new(node_count: u32) -> Self {
        Self {
            node_count,
            ack_policy: AckPolicy::All,
            read_policy: ReadPolicy::Leader,
            workload: WorkloadConfig::default(),
            base_port: 40000,
        }
    }

    pub fn with_ack_policy(mut self, policy: AckPolicy) -> Self {
        self.ack_policy = policy;
        self
    }

    pub fn with_read_policy(mut self, policy: ReadPolicy) -> Self {
        self.read_policy = policy;
        self
    }

    pub fn with_workload(mut self, config: WorkloadConfig) -> Self {
        self.workload = config;
        self
    }

    pub fn with_base_port(mut self, port: u16) -> Self {
        self.base_port = port;
        self
    }
}

#[derive(Debug)]
pub struct ScenarioContext {
    pub cluster: ProcessCluster,
    pub workload: WorkloadGenerator,
    pub ack_policy: AckPolicy,
    pub read_policy: ReadPolicy,
    leader_id: NodeId,
    epoch: u64,
    alive_nodes: HashSet<NodeId>,
}

impl ScenarioContext {
    pub fn new(config: ScenarioConfig) -> Self {
        let network = NetworkConfig {
            base_listen_port: config.base_port,
            base_gossip_port: config.base_port + 1000,
            host: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        };
        let cluster_config = ClusterConfig::new(
            config.node_count,
            PathBuf::from("/usr/bin/false"),
            PathBuf::from("/tmp/blockyard-scenario"),
        )
        .with_network(network);

        let cluster = ProcessCluster::new(cluster_config);
        let alive_nodes: HashSet<NodeId> = (0..config.node_count).map(NodeId).collect();

        Self {
            cluster,
            workload: WorkloadGenerator::new(config.workload),
            ack_policy: config.ack_policy,
            read_policy: config.read_policy,
            leader_id: NodeId(0),
            epoch: 1,
            alive_nodes,
        }
    }

    pub fn leader(&self) -> NodeId {
        self.leader_id
    }

    pub fn set_leader(&mut self, id: NodeId) {
        self.leader_id = id;
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn bump_epoch(&mut self) -> u64 {
        self.epoch += 1;
        self.epoch
    }

    pub fn quorum_size(&self) -> usize {
        self.cluster.node_count() / 2 + 1
    }

    pub fn alive_count(&self) -> usize {
        self.alive_nodes.len()
    }

    pub fn crash_node(&mut self, id: NodeId) {
        self.alive_nodes.remove(&id);
        info!("simulated crash: {} (alive={})", id, self.alive_nodes.len());
    }

    pub fn recover_node(&mut self, id: NodeId) {
        self.alive_nodes.insert(id);
        info!("simulated recovery: {} (alive={})", id, self.alive_nodes.len());
    }

    pub fn simulate_leader_election(&mut self, crashed_leader: NodeId) -> NodeId {
        self.crash_node(crashed_leader);
        let ids = self.cluster.node_ids();
        let new_leader = ids
            .iter()
            .find(|id| **id != crashed_leader && self.alive_nodes.contains(id))
            .copied()
            .unwrap_or(NodeId(0));
        self.leader_id = new_leader;
        self.bump_epoch();
        info!(
            "leader election: {} -> {} (epoch={})",
            crashed_leader, new_leader, self.epoch
        );
        new_leader
    }

    pub fn simulate_writes_with_acks(
        &self,
        count: usize,
        volume_id: VolumeId,
    ) -> Vec<Operation> {
        let mut operations = Vec::with_capacity(count);
        for i in 0..count {
            let offset = (i as u64) * self.workload.config().block_size as u64;
            let (mut op, _data) = self.workload.generate_write(volume_id, offset);

            let ack_status = self.determine_ack_status();
            op.complete(ack_status);
            self.workload.log().record(op.clone());
            operations.push(op);
        }
        operations
    }

    pub fn simulate_reads_after_writes(
        &self,
        writes: &[Operation],
        checker: &mut ConsistencyChecker,
    ) {
        for write_op in writes {
            if write_op.status == AckStatus::Acked {
                if let Some(checksum) = &write_op.data_checksum {
                    let mut read_op =
                        Operation::new_read(write_op.volume_id, write_op.offset, write_op.length);
                    read_op.complete(AckStatus::Acked);
                    self.workload.log().record(read_op);
                    checker.record_read_back(
                        write_op.volume_id,
                        write_op.offset,
                        checksum.clone(),
                    );
                }
            }
        }
    }

    fn determine_ack_status(&self) -> AckStatus {
        let alive = self.alive_nodes.len();
        let required = match self.ack_policy {
            AckPolicy::All => self.cluster.node_count(),
            AckPolicy::Majority => self.quorum_size(),
            AckPolicy::One => 1,
        };
        if alive >= required {
            AckStatus::Acked
        } else {
            AckStatus::Nacked
        }
    }
}

pub fn run_consistency_check(ctx: &ScenarioContext) -> Vec<CheckReport> {
    let log = ctx.workload.log();
    let ops = log.all();
    let new_log = OperationLog::new();
    for op in ops {
        new_log.record(op);
    }
    let checker = ConsistencyChecker::new(new_log);
    checker.check_all()
}

pub fn build_checker_with_readback(
    ctx: &ScenarioContext,
    writes: &[Operation],
) -> ConsistencyChecker {
    let log = ctx.workload.log();
    let ops = log.all();
    let new_log = OperationLog::new();
    for op in ops {
        new_log.record(op);
    }
    let mut checker = ConsistencyChecker::new(new_log);
    for w in writes {
        if w.status == AckStatus::Acked {
            if let Some(cs) = &w.data_checksum {
                checker.record_read_back(w.volume_id, w.offset, cs.clone());
            }
        }
    }
    checker
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountState {
    Unmounted,
    Mounted,
    Crashed,
}

#[derive(Debug)]
pub struct UblkMount {
    pub volume_id: VolumeId,
    pub state: MountState,
    pub connected_leader: Option<NodeId>,
    pub writes_completed: u64,
    pub reads_completed: u64,
}

impl UblkMount {
    pub fn new(volume_id: VolumeId) -> Self {
        Self {
            volume_id,
            state: MountState::Unmounted,
            connected_leader: None,
            writes_completed: 0,
            reads_completed: 0,
        }
    }

    pub fn mount(&mut self, leader: NodeId) {
        self.state = MountState::Mounted;
        self.connected_leader = Some(leader);
        info!(
            "ublk mount: volume={} leader={}",
            self.volume_id, leader
        );
    }

    pub fn unmount(&mut self) {
        self.state = MountState::Unmounted;
        self.connected_leader = None;
        info!("ublk unmount: volume={}", self.volume_id);
    }

    pub fn crash(&mut self) {
        self.state = MountState::Crashed;
        info!("ublk crash: volume={}", self.volume_id);
    }

    pub fn remount(&mut self, leader: NodeId) {
        self.state = MountState::Mounted;
        self.connected_leader = Some(leader);
        info!(
            "ublk remount: volume={} leader={}",
            self.volume_id, leader
        );
    }

    pub fn follow_new_leader(&mut self, new_leader: NodeId) {
        self.connected_leader = Some(new_leader);
        info!(
            "ublk follow leader: volume={} new_leader={}",
            self.volume_id, new_leader
        );
    }

    pub fn is_mounted(&self) -> bool {
        self.state == MountState::Mounted
    }

    pub fn record_write(&mut self) {
        self.writes_completed += 1;
    }

    pub fn record_read(&mut self) {
        self.reads_completed += 1;
    }
}

#[derive(Debug)]
pub struct StalenessResult {
    pub max_staleness: Duration,
    pub average_staleness: Duration,
    pub samples: usize,
}

impl StalenessResult {
    pub fn new(samples: Vec<Duration>) -> Self {
        let count = samples.len();
        if count == 0 {
            return Self {
                max_staleness: Duration::ZERO,
                average_staleness: Duration::ZERO,
                samples: 0,
            };
        }
        let max = samples.iter().max().copied().unwrap_or(Duration::ZERO);
        let total: Duration = samples.iter().sum();
        let avg = total / count as u32;
        Self {
            max_staleness: max,
            average_staleness: avg,
            samples: count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scenario_config_new() {
        let config = ScenarioConfig::new(3);
        assert_eq!(config.node_count, 3);
        assert_eq!(config.ack_policy, AckPolicy::All);
        assert_eq!(config.read_policy, ReadPolicy::Leader);
    }

    #[test]
    fn test_scenario_config_builders() {
        let config = ScenarioConfig::new(5)
            .with_ack_policy(AckPolicy::Majority)
            .with_read_policy(ReadPolicy::Any)
            .with_base_port(50000);
        assert_eq!(config.ack_policy, AckPolicy::Majority);
        assert_eq!(config.read_policy, ReadPolicy::Any);
        assert_eq!(config.base_port, 50000);
    }

    #[test]
    fn test_scenario_context_creation() {
        let config = ScenarioConfig::new(3).with_base_port(42000);
        let ctx = ScenarioContext::new(config);
        assert_eq!(ctx.leader(), NodeId(0));
        assert_eq!(ctx.epoch(), 1);
        assert_eq!(ctx.quorum_size(), 2);
        assert_eq!(ctx.cluster.node_count(), 3);
    }

    #[test]
    fn test_scenario_context_leader_election() {
        let config = ScenarioConfig::new(3).with_base_port(42100);
        let mut ctx = ScenarioContext::new(config);
        assert_eq!(ctx.leader(), NodeId(0));

        let new_leader = ctx.simulate_leader_election(NodeId(0));
        assert_ne!(new_leader, NodeId(0));
        assert_eq!(ctx.epoch(), 2);
    }

    #[test]
    fn test_scenario_context_bump_epoch() {
        let config = ScenarioConfig::new(3).with_base_port(42200);
        let mut ctx = ScenarioContext::new(config);
        assert_eq!(ctx.epoch(), 1);
        assert_eq!(ctx.bump_epoch(), 2);
        assert_eq!(ctx.bump_epoch(), 3);
    }

    #[test]
    fn test_scenario_context_quorum_sizes() {
        let config3 = ScenarioConfig::new(3).with_base_port(42300);
        let ctx3 = ScenarioContext::new(config3);
        assert_eq!(ctx3.quorum_size(), 2);

        let config5 = ScenarioConfig::new(5).with_base_port(42400);
        let ctx5 = ScenarioContext::new(config5);
        assert_eq!(ctx5.quorum_size(), 3);
    }

    #[test]
    fn test_ublk_mount_lifecycle() {
        let vol = VolumeId::generate();
        let mut mount = UblkMount::new(vol);
        assert_eq!(mount.state, MountState::Unmounted);
        assert!(!mount.is_mounted());

        mount.mount(NodeId(0));
        assert!(mount.is_mounted());
        assert_eq!(mount.connected_leader, Some(NodeId(0)));

        mount.record_write();
        mount.record_write();
        mount.record_read();
        assert_eq!(mount.writes_completed, 2);
        assert_eq!(mount.reads_completed, 1);

        mount.crash();
        assert_eq!(mount.state, MountState::Crashed);
        assert!(!mount.is_mounted());

        mount.remount(NodeId(1));
        assert!(mount.is_mounted());
        assert_eq!(mount.connected_leader, Some(NodeId(1)));
    }

    #[test]
    fn test_ublk_mount_follow_leader() {
        let vol = VolumeId::generate();
        let mut mount = UblkMount::new(vol);
        mount.mount(NodeId(0));
        assert_eq!(mount.connected_leader, Some(NodeId(0)));

        mount.follow_new_leader(NodeId(2));
        assert_eq!(mount.connected_leader, Some(NodeId(2)));
        assert!(mount.is_mounted());
    }

    #[test]
    fn test_ublk_mount_unmount() {
        let vol = VolumeId::generate();
        let mut mount = UblkMount::new(vol);
        mount.mount(NodeId(0));
        assert!(mount.is_mounted());

        mount.unmount();
        assert!(!mount.is_mounted());
        assert_eq!(mount.connected_leader, None);
    }

    #[test]
    fn test_staleness_result_empty() {
        let result = StalenessResult::new(vec![]);
        assert_eq!(result.max_staleness, Duration::ZERO);
        assert_eq!(result.average_staleness, Duration::ZERO);
        assert_eq!(result.samples, 0);
    }

    #[test]
    fn test_staleness_result_with_samples() {
        let samples = vec![
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(30),
        ];
        let result = StalenessResult::new(samples);
        assert_eq!(result.max_staleness, Duration::from_millis(30));
        assert_eq!(result.average_staleness, Duration::from_millis(20));
        assert_eq!(result.samples, 3);
    }

    #[test]
    fn test_ack_policy_variants() {
        assert_ne!(AckPolicy::All, AckPolicy::Majority);
        assert_ne!(AckPolicy::All, AckPolicy::One);
        assert_ne!(AckPolicy::Majority, AckPolicy::One);
    }

    #[test]
    fn test_read_policy_variants() {
        assert_ne!(ReadPolicy::Leader, ReadPolicy::Any);
    }

    #[test]
    fn test_mount_state_variants() {
        assert_ne!(MountState::Unmounted, MountState::Mounted);
        assert_ne!(MountState::Mounted, MountState::Crashed);
    }

    #[test]
    fn test_scenario_crash_and_recover_node() {
        let config = ScenarioConfig::new(3).with_base_port(42500);
        let mut ctx = ScenarioContext::new(config);
        assert_eq!(ctx.alive_count(), 3);

        ctx.crash_node(NodeId(1));
        assert_eq!(ctx.alive_count(), 2);

        ctx.recover_node(NodeId(1));
        assert_eq!(ctx.alive_count(), 3);
    }

    #[test]
    fn test_scenario_writes_all_ack() {
        let vol = VolumeId::generate();
        let wl = WorkloadConfig {
            volume_ids: vec![vol],
            write_ratio: 1.0,
            ..Default::default()
        };
        let config = ScenarioConfig::new(3)
            .with_ack_policy(AckPolicy::All)
            .with_workload(wl)
            .with_base_port(42600);
        let ctx = ScenarioContext::new(config);

        let ops = ctx.simulate_writes_with_acks(5, vol);
        assert_eq!(ops.len(), 5);
        assert!(ops.iter().all(|op| op.status == AckStatus::Acked));
    }

    #[test]
    fn test_scenario_writes_after_crash_nacked() {
        let vol = VolumeId::generate();
        let wl = WorkloadConfig {
            volume_ids: vec![vol],
            write_ratio: 1.0,
            ..Default::default()
        };
        let config = ScenarioConfig::new(3)
            .with_ack_policy(AckPolicy::All)
            .with_workload(wl)
            .with_base_port(42700);
        let mut ctx = ScenarioContext::new(config);

        ctx.crash_node(NodeId(2));
        let ops = ctx.simulate_writes_with_acks(3, vol);
        assert!(ops.iter().all(|op| op.status == AckStatus::Nacked));
    }

    #[test]
    fn test_scenario_writes_majority_survives_one_crash() {
        let vol = VolumeId::generate();
        let wl = WorkloadConfig {
            volume_ids: vec![vol],
            write_ratio: 1.0,
            ..Default::default()
        };
        let config = ScenarioConfig::new(5)
            .with_ack_policy(AckPolicy::Majority)
            .with_workload(wl)
            .with_base_port(42800);
        let mut ctx = ScenarioContext::new(config);

        ctx.crash_node(NodeId(4));
        let ops = ctx.simulate_writes_with_acks(5, vol);
        assert!(ops.iter().all(|op| op.status == AckStatus::Acked));
    }
}
