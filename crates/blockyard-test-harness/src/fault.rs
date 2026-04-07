use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::vm::{Node, NodeId, NodeState};

#[derive(Debug, Clone, PartialEq)]
pub enum Fault {
    NodeCrash {
        node_id: NodeId,
    },
    NodePause {
        node_id: NodeId,
    },
    NodeResume {
        node_id: NodeId,
    },
    NetworkPartition {
        isolated: Vec<NodeId>,
        rest: Vec<NodeId>,
    },
    AsymmetricPartition {
        from: NodeId,
        to: NodeId,
    },
    NetworkDelay {
        node_id: NodeId,
        delay: Duration,
    },
    NetworkPacketLoss {
        node_id: NodeId,
        loss_percent: u8,
    },
    DiskSlow {
        node_id: NodeId,
        delay: Duration,
    },
    DiskFault {
        node_id: NodeId,
        error_rate: f64,
    },
    ClockSkew {
        node_id: NodeId,
        skew: Duration,
        forward: bool,
    },
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fault::NodeCrash { node_id } => write!(f, "crash({})", node_id),
            Fault::NodePause { node_id } => write!(f, "pause({})", node_id),
            Fault::NodeResume { node_id } => write!(f, "resume({})", node_id),
            Fault::NetworkPartition { isolated, rest } => {
                write!(
                    f,
                    "partition(isolated=[{}], rest=[{}])",
                    isolated
                        .iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    rest.iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                )
            }
            Fault::AsymmetricPartition { from, to } => {
                write!(f, "asymmetric_partition({} -> {})", from, to)
            }
            Fault::NetworkDelay { node_id, delay } => {
                write!(f, "net_delay({}, {:?})", node_id, delay)
            }
            Fault::NetworkPacketLoss {
                node_id,
                loss_percent,
            } => write!(f, "packet_loss({}, {}%)", node_id, loss_percent),
            Fault::DiskSlow { node_id, delay } => {
                write!(f, "disk_slow({}, {:?})", node_id, delay)
            }
            Fault::DiskFault {
                node_id,
                error_rate,
            } => write!(f, "disk_fault({}, {:.1}%)", node_id, error_rate * 100.0),
            Fault::ClockSkew {
                node_id,
                skew,
                forward,
            } => {
                let direction = if *forward { "+" } else { "-" };
                write!(f, "clock_skew({}, {}{:?})", node_id, direction, skew)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct FaultRecord {
    pub fault: Fault,
    pub applied_at: std::time::Instant,
    pub reverted: bool,
}

pub trait FaultInjector: Send + Sync {
    fn inject(&self, fault: &Fault) -> anyhow::Result<()>;
    fn revert(&self, fault: &Fault) -> anyhow::Result<()>;
    fn active_faults(&self) -> Vec<FaultRecord>;
    fn revert_all(&self) -> anyhow::Result<()>;
}

pub struct ProcessFaultInjector<'a> {
    nodes: &'a HashMap<NodeId, Node>,
    active: RwLock<Vec<FaultRecord>>,
}

impl<'a> fmt::Debug for ProcessFaultInjector<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessFaultInjector")
            .field("active_count", &self.active.read().len())
            .finish()
    }
}

impl<'a> ProcessFaultInjector<'a> {
    pub fn new(nodes: &'a HashMap<NodeId, Node>) -> Self {
        Self {
            nodes,
            active: RwLock::new(Vec::new()),
        }
    }

    fn get_node(&self, id: &NodeId) -> anyhow::Result<&Node> {
        self.nodes
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("node {} not found", id))
    }

    fn record_fault(&self, fault: &Fault) {
        self.active.write().push(FaultRecord {
            fault: fault.clone(),
            applied_at: std::time::Instant::now(),
            reverted: false,
        });
    }
}

impl FaultInjector for ProcessFaultInjector<'_> {
    fn inject(&self, fault: &Fault) -> anyhow::Result<()> {
        info!("injecting fault: {}", fault);

        match fault {
            Fault::NodeCrash { node_id } => {
                let node = self.get_node(node_id)?;
                node.kill()?;
                self.record_fault(fault);
            }
            Fault::NodePause { node_id } => {
                let node = self.get_node(node_id)?;
                node.pause()?;
                self.record_fault(fault);
            }
            Fault::NodeResume { node_id } => {
                let node = self.get_node(node_id)?;
                node.resume()?;
                let mut active = self.active.write();
                for record in active.iter_mut() {
                    if let Fault::NodePause { node_id: pid } = &record.fault {
                        if pid == node_id {
                            record.reverted = true;
                        }
                    }
                }
            }
            Fault::NetworkPartition { isolated, rest } => {
                info!(
                    "simulating network partition: isolated={:?}, rest={:?}",
                    isolated, rest
                );
                debug!(
                    "process-mode partition: connection-level isolation not implemented, \
                     use VM mode for real iptables partitions"
                );
                self.record_fault(fault);
            }
            Fault::AsymmetricPartition { from, to } => {
                info!(
                    "simulating asymmetric partition: {} -> {} blocked",
                    from, to
                );
                debug!(
                    "process-mode asymmetric partition: requires proxy-based interception, \
                     use VM mode for real iptables rules"
                );
                self.record_fault(fault);
            }
            Fault::NetworkDelay { node_id, delay } => {
                info!("simulating network delay of {:?} on {}", delay, node_id);
                debug!(
                    "process-mode delay: requires tc netem or proxy, \
                     use VM mode for real network delay"
                );
                self.record_fault(fault);
            }
            Fault::NetworkPacketLoss {
                node_id,
                loss_percent,
            } => {
                info!(
                    "simulating {}% packet loss on {}",
                    loss_percent, node_id
                );
                debug!(
                    "process-mode packet loss: requires tc netem or proxy, \
                     use VM mode for real packet loss"
                );
                self.record_fault(fault);
            }
            Fault::DiskSlow { node_id, delay } => {
                info!(
                    "simulating slow disk ({:?} delay) on {}",
                    delay, node_id
                );
                debug!(
                    "process-mode disk slow: requires dm-delay, \
                     use VM mode for real disk delay"
                );
                self.record_fault(fault);
            }
            Fault::DiskFault {
                node_id,
                error_rate,
            } => {
                info!(
                    "simulating disk fault ({:.1}% error rate) on {}",
                    error_rate * 100.0,
                    node_id
                );
                debug!(
                    "process-mode disk fault: requires dm-flakey, \
                     use VM mode for real disk faults"
                );
                self.record_fault(fault);
            }
            Fault::ClockSkew {
                node_id,
                skew,
                forward,
            } => {
                let direction = if *forward { "forward" } else { "backward" };
                info!(
                    "simulating clock skew ({:?} {}) on {}",
                    skew, direction, node_id
                );
                debug!(
                    "process-mode clock skew: requires settimeofday or tokio time pause, \
                     use VM mode for real clock manipulation"
                );
                self.record_fault(fault);
            }
        }

        Ok(())
    }

    fn revert(&self, fault: &Fault) -> anyhow::Result<()> {
        info!("reverting fault: {}", fault);

        match fault {
            Fault::NodeCrash { node_id } => {
                let node = self.get_node(node_id)?;
                if node.state() == NodeState::Crashed {
                    node.start()?;
                }
            }
            Fault::NodePause { node_id } => {
                let node = self.get_node(node_id)?;
                if node.state() == NodeState::Paused {
                    node.resume()?;
                }
            }
            Fault::NodeResume { .. } => {}
            Fault::NetworkPartition { .. }
            | Fault::AsymmetricPartition { .. }
            | Fault::NetworkDelay { .. }
            | Fault::NetworkPacketLoss { .. }
            | Fault::DiskSlow { .. }
            | Fault::DiskFault { .. }
            | Fault::ClockSkew { .. } => {
                debug!("reverting simulated fault: {}", fault);
            }
        }

        let mut active = self.active.write();
        for record in active.iter_mut() {
            if record.fault == *fault && !record.reverted {
                record.reverted = true;
                break;
            }
        }

        Ok(())
    }

    fn active_faults(&self) -> Vec<FaultRecord> {
        self.active
            .read()
            .iter()
            .filter(|r| !r.reverted)
            .cloned()
            .collect()
    }

    fn revert_all(&self) -> anyhow::Result<()> {
        info!("reverting all active faults");
        let faults: Vec<Fault> = self
            .active
            .read()
            .iter()
            .filter(|r| !r.reverted)
            .map(|r| r.fault.clone())
            .collect();

        for fault in &faults {
            if let Err(e) = self.revert(fault) {
                warn!("failed to revert fault {}: {}", fault, e);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::NodeAddress;
    use crate::vm::NodeConfig;
    use std::path::PathBuf;

    fn test_nodes() -> HashMap<NodeId, Node> {
        let mut nodes = HashMap::new();
        for i in 0..5 {
            let id = NodeId(i);
            let port = 15000 + i as u16 * 10;
            let config = NodeConfig {
                id,
                binary_path: PathBuf::from("/bin/sleep"),
                data_dir: PathBuf::from(format!("/tmp/fault-test-{i}")),
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

    #[test]
    fn test_fault_display() {
        assert_eq!(
            format!("{}", Fault::NodeCrash { node_id: NodeId(1) }),
            "crash(node-1)"
        );
        assert_eq!(
            format!("{}", Fault::NodePause { node_id: NodeId(2) }),
            "pause(node-2)"
        );
        assert_eq!(
            format!("{}", Fault::NodeResume { node_id: NodeId(2) }),
            "resume(node-2)"
        );
        assert_eq!(
            format!(
                "{}",
                Fault::NetworkPartition {
                    isolated: vec![NodeId(0)],
                    rest: vec![NodeId(1), NodeId(2)]
                }
            ),
            "partition(isolated=[node-0], rest=[node-1, node-2])"
        );
        assert_eq!(
            format!(
                "{}",
                Fault::AsymmetricPartition {
                    from: NodeId(0),
                    to: NodeId(1)
                }
            ),
            "asymmetric_partition(node-0 -> node-1)"
        );
        assert_eq!(
            format!(
                "{}",
                Fault::NetworkDelay {
                    node_id: NodeId(0),
                    delay: Duration::from_millis(100)
                }
            ),
            "net_delay(node-0, 100ms)"
        );
        assert_eq!(
            format!(
                "{}",
                Fault::NetworkPacketLoss {
                    node_id: NodeId(0),
                    loss_percent: 25
                }
            ),
            "packet_loss(node-0, 25%)"
        );
        assert_eq!(
            format!(
                "{}",
                Fault::DiskSlow {
                    node_id: NodeId(0),
                    delay: Duration::from_millis(50)
                }
            ),
            "disk_slow(node-0, 50ms)"
        );
        assert_eq!(
            format!(
                "{}",
                Fault::DiskFault {
                    node_id: NodeId(0),
                    error_rate: 0.05
                }
            ),
            "disk_fault(node-0, 5.0%)"
        );
        assert_eq!(
            format!(
                "{}",
                Fault::ClockSkew {
                    node_id: NodeId(0),
                    skew: Duration::from_secs(10),
                    forward: true
                }
            ),
            "clock_skew(node-0, +10s)"
        );
        assert_eq!(
            format!(
                "{}",
                Fault::ClockSkew {
                    node_id: NodeId(0),
                    skew: Duration::from_secs(5),
                    forward: false
                }
            ),
            "clock_skew(node-0, -5s)"
        );
    }

    #[test]
    fn test_fault_injector_crash_kill() {
        let dir = tempfile::tempdir().unwrap();
        let mut nodes = HashMap::new();
        let id = NodeId(0);
        let config = NodeConfig {
            id,
            binary_path: PathBuf::from("/bin/sleep"),
            data_dir: dir.path().to_path_buf(),
            address: NodeAddress {
                listen_addr: "127.0.0.1:15100".parse().unwrap(),
                gossip_addr: "127.0.0.1:15101".parse().unwrap(),
            },
            extra_args: vec!["60".to_string()],
            env_vars: vec![],
        };
        nodes.insert(id, Node::new(config));

        nodes.get(&id).unwrap().start().unwrap();
        assert_eq!(nodes.get(&id).unwrap().state(), NodeState::Running);

        let injector = ProcessFaultInjector::new(&nodes);
        injector
            .inject(&Fault::NodeCrash { node_id: id })
            .unwrap();
        assert_eq!(nodes.get(&id).unwrap().state(), NodeState::Crashed);
        assert_eq!(injector.active_faults().len(), 1);
    }

    #[test]
    fn test_fault_injector_pause_resume() {
        let dir = tempfile::tempdir().unwrap();
        let mut nodes = HashMap::new();
        let id = NodeId(0);
        let config = NodeConfig {
            id,
            binary_path: PathBuf::from("/bin/sleep"),
            data_dir: dir.path().to_path_buf(),
            address: NodeAddress {
                listen_addr: "127.0.0.1:15200".parse().unwrap(),
                gossip_addr: "127.0.0.1:15201".parse().unwrap(),
            },
            extra_args: vec!["60".to_string()],
            env_vars: vec![],
        };
        nodes.insert(id, Node::new(config));
        nodes.get(&id).unwrap().start().unwrap();

        let injector = ProcessFaultInjector::new(&nodes);

        injector
            .inject(&Fault::NodePause { node_id: id })
            .unwrap();
        assert_eq!(nodes.get(&id).unwrap().state(), NodeState::Paused);
        assert_eq!(injector.active_faults().len(), 1);

        injector
            .inject(&Fault::NodeResume { node_id: id })
            .unwrap();
        assert_eq!(nodes.get(&id).unwrap().state(), NodeState::Running);
        assert_eq!(injector.active_faults().len(), 0);

        nodes.get(&id).unwrap().stop().unwrap();
    }

    #[test]
    fn test_fault_injector_network_partition() {
        let nodes = test_nodes();
        let injector = ProcessFaultInjector::new(&nodes);

        let fault = Fault::NetworkPartition {
            isolated: vec![NodeId(0), NodeId(1)],
            rest: vec![NodeId(2), NodeId(3), NodeId(4)],
        };
        injector.inject(&fault).unwrap();
        assert_eq!(injector.active_faults().len(), 1);

        injector.revert(&fault).unwrap();
        assert_eq!(injector.active_faults().len(), 0);
    }

    #[test]
    fn test_fault_injector_asymmetric_partition() {
        let nodes = test_nodes();
        let injector = ProcessFaultInjector::new(&nodes);

        let fault = Fault::AsymmetricPartition {
            from: NodeId(0),
            to: NodeId(1),
        };
        injector.inject(&fault).unwrap();
        assert_eq!(injector.active_faults().len(), 1);
    }

    #[test]
    fn test_fault_injector_network_delay() {
        let nodes = test_nodes();
        let injector = ProcessFaultInjector::new(&nodes);

        let fault = Fault::NetworkDelay {
            node_id: NodeId(0),
            delay: Duration::from_millis(100),
        };
        injector.inject(&fault).unwrap();
        assert_eq!(injector.active_faults().len(), 1);
    }

    #[test]
    fn test_fault_injector_packet_loss() {
        let nodes = test_nodes();
        let injector = ProcessFaultInjector::new(&nodes);

        let fault = Fault::NetworkPacketLoss {
            node_id: NodeId(0),
            loss_percent: 50,
        };
        injector.inject(&fault).unwrap();
        assert_eq!(injector.active_faults().len(), 1);
    }

    #[test]
    fn test_fault_injector_disk_slow() {
        let nodes = test_nodes();
        let injector = ProcessFaultInjector::new(&nodes);

        let fault = Fault::DiskSlow {
            node_id: NodeId(0),
            delay: Duration::from_millis(50),
        };
        injector.inject(&fault).unwrap();
        assert_eq!(injector.active_faults().len(), 1);
    }

    #[test]
    fn test_fault_injector_disk_fault() {
        let nodes = test_nodes();
        let injector = ProcessFaultInjector::new(&nodes);

        let fault = Fault::DiskFault {
            node_id: NodeId(0),
            error_rate: 0.1,
        };
        injector.inject(&fault).unwrap();
        assert_eq!(injector.active_faults().len(), 1);
    }

    #[test]
    fn test_fault_injector_clock_skew() {
        let nodes = test_nodes();
        let injector = ProcessFaultInjector::new(&nodes);

        let fault = Fault::ClockSkew {
            node_id: NodeId(0),
            skew: Duration::from_secs(30),
            forward: true,
        };
        injector.inject(&fault).unwrap();
        assert_eq!(injector.active_faults().len(), 1);
    }

    #[test]
    fn test_fault_injector_revert_all() {
        let nodes = test_nodes();
        let injector = ProcessFaultInjector::new(&nodes);

        injector
            .inject(&Fault::NetworkDelay {
                node_id: NodeId(0),
                delay: Duration::from_millis(100),
            })
            .unwrap();
        injector
            .inject(&Fault::DiskSlow {
                node_id: NodeId(1),
                delay: Duration::from_millis(50),
            })
            .unwrap();
        injector
            .inject(&Fault::ClockSkew {
                node_id: NodeId(2),
                skew: Duration::from_secs(10),
                forward: true,
            })
            .unwrap();

        assert_eq!(injector.active_faults().len(), 3);
        injector.revert_all().unwrap();
        assert_eq!(injector.active_faults().len(), 0);
    }

    #[test]
    fn test_fault_injector_node_not_found() {
        let nodes = HashMap::new();
        let injector = ProcessFaultInjector::new(&nodes);

        let result = injector.inject(&Fault::NodeCrash {
            node_id: NodeId(99),
        });
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_fault_record_clone() {
        let record = FaultRecord {
            fault: Fault::NodeCrash { node_id: NodeId(0) },
            applied_at: std::time::Instant::now(),
            reverted: false,
        };
        let cloned = record.clone();
        assert_eq!(cloned.fault, record.fault);
        assert!(!cloned.reverted);
    }

    #[test]
    fn test_revert_crash_restarts_node() {
        let dir = tempfile::tempdir().unwrap();
        let mut nodes = HashMap::new();
        let id = NodeId(0);
        let config = NodeConfig {
            id,
            binary_path: PathBuf::from("/bin/sleep"),
            data_dir: dir.path().to_path_buf(),
            address: NodeAddress {
                listen_addr: "127.0.0.1:15300".parse().unwrap(),
                gossip_addr: "127.0.0.1:15301".parse().unwrap(),
            },
            extra_args: vec!["60".to_string()],
            env_vars: vec![],
        };
        nodes.insert(id, Node::new(config));
        nodes.get(&id).unwrap().start().unwrap();

        let injector = ProcessFaultInjector::new(&nodes);
        let fault = Fault::NodeCrash { node_id: id };
        injector.inject(&fault).unwrap();
        assert_eq!(nodes.get(&id).unwrap().state(), NodeState::Crashed);

        injector.revert(&fault).unwrap();
        assert_eq!(nodes.get(&id).unwrap().state(), NodeState::Running);
        assert_eq!(injector.active_faults().len(), 0);

        nodes.get(&id).unwrap().stop().unwrap();
    }

    #[test]
    fn test_revert_pause_resumes_node() {
        let dir = tempfile::tempdir().unwrap();
        let mut nodes = HashMap::new();
        let id = NodeId(0);
        let config = NodeConfig {
            id,
            binary_path: PathBuf::from("/bin/sleep"),
            data_dir: dir.path().to_path_buf(),
            address: NodeAddress {
                listen_addr: "127.0.0.1:15400".parse().unwrap(),
                gossip_addr: "127.0.0.1:15401".parse().unwrap(),
            },
            extra_args: vec!["60".to_string()],
            env_vars: vec![],
        };
        nodes.insert(id, Node::new(config));
        nodes.get(&id).unwrap().start().unwrap();

        let injector = ProcessFaultInjector::new(&nodes);
        let fault = Fault::NodePause { node_id: id };
        injector.inject(&fault).unwrap();
        assert_eq!(nodes.get(&id).unwrap().state(), NodeState::Paused);

        injector.revert(&fault).unwrap();
        assert_eq!(nodes.get(&id).unwrap().state(), NodeState::Running);

        nodes.get(&id).unwrap().stop().unwrap();
    }
}
