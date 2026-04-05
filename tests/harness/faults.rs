use crate::harness::cluster::TestCluster;
use std::time::Duration;
use tokio::process::Command;

#[derive(Debug, Clone)]
pub enum Fault {
    NodeCrash { node_id: usize },
    NodePause { node_id: usize },
    NodeResume { node_id: usize },
    NetworkPartition { from: usize, to: usize },
    NetworkHeal { from: usize, to: usize },
    AsymmetricPartition { blocked_from: usize, blocked_to: usize },
    NetworkDelay { node_id: usize, latency: Duration },
    NetworkLoss { node_id: usize, loss_pct: u8 },
    NetworkReset { node_id: usize },
    DiskSlow { node_id: usize, latency: Duration },
    DiskFault { node_id: usize },
    DiskHeal { node_id: usize },
    ClockSkew { node_id: usize, offset: i64 },
    FillDisk { node_id: usize },
}

impl Fault {
    pub fn description(&self) -> String {
        match self {
            Self::NodeCrash { node_id } => format!("crash node {node_id}"),
            Self::NodePause { node_id } => format!("pause node {node_id}"),
            Self::NodeResume { node_id } => format!("resume node {node_id}"),
            Self::NetworkPartition { from, to } => {
                format!("partition node {from} <-> node {to}")
            }
            Self::NetworkHeal { from, to } => {
                format!("heal partition node {from} <-> node {to}")
            }
            Self::AsymmetricPartition {
                blocked_from,
                blocked_to,
            } => format!("asymmetric partition {blocked_from} -> {blocked_to}"),
            Self::NetworkDelay { node_id, latency } => {
                format!("add {latency:?} delay to node {node_id}")
            }
            Self::NetworkLoss { node_id, loss_pct } => {
                format!("add {loss_pct}% packet loss to node {node_id}")
            }
            Self::NetworkReset { node_id } => format!("reset network on node {node_id}"),
            Self::DiskSlow { node_id, latency } => {
                format!("slow disk on node {node_id} by {latency:?}")
            }
            Self::DiskFault { node_id } => format!("inject disk fault on node {node_id}"),
            Self::DiskHeal { node_id } => format!("heal disk on node {node_id}"),
            Self::ClockSkew { node_id, offset } => {
                format!("skew clock on node {node_id} by {offset}s")
            }
            Self::FillDisk { node_id } => format!("fill disk on node {node_id}"),
        }
    }
}

pub struct FaultInjector<'a> {
    cluster: &'a TestCluster,
}

impl<'a> FaultInjector<'a> {
    pub fn new(cluster: &'a TestCluster) -> Self {
        Self { cluster }
    }

    pub async fn inject(&self, fault: &Fault) -> anyhow::Result<()> {
        println!("  [FAULT] {}", fault.description());
        match fault {
            Fault::NodeCrash { node_id } => {
                self.cluster.kill_blockyard(*node_id).await?;
            }
            Fault::NodePause { node_id } => {
                self.cluster.pause_blockyard(*node_id).await?;
            }
            Fault::NodeResume { node_id } => {
                self.cluster.resume_blockyard(*node_id).await?;
            }
            Fault::NetworkPartition { from, to } => {
                let to_node = self.cluster.node(*to).ok_or_else(|| {
                    anyhow::anyhow!("node {to} not found")
                })?;
                self.cluster
                    .ssh_exec(
                        *from,
                        &format!(
                            "iptables -A INPUT -s {} -j DROP && iptables -A OUTPUT -d {} -j DROP",
                            to_node.blockyard_addr().ip(),
                            to_node.blockyard_addr().ip()
                        ),
                    )
                    .await?;
                let from_node = self.cluster.node(*from).ok_or_else(|| {
                    anyhow::anyhow!("node {from} not found")
                })?;
                self.cluster
                    .ssh_exec(
                        *to,
                        &format!(
                            "iptables -A INPUT -s {} -j DROP && iptables -A OUTPUT -d {} -j DROP",
                            from_node.blockyard_addr().ip(),
                            from_node.blockyard_addr().ip()
                        ),
                    )
                    .await?;
            }
            Fault::NetworkHeal { from, to } => {
                self.cluster
                    .ssh_exec(*from, "iptables -F INPUT && iptables -F OUTPUT")
                    .await?;
                self.cluster
                    .ssh_exec(*to, "iptables -F INPUT && iptables -F OUTPUT")
                    .await?;
            }
            Fault::AsymmetricPartition {
                blocked_from,
                blocked_to,
            } => {
                let to_node = self.cluster.node(*blocked_to).ok_or_else(|| {
                    anyhow::anyhow!("node {blocked_to} not found")
                })?;
                self.cluster
                    .ssh_exec(
                        *blocked_from,
                        &format!(
                            "iptables -A OUTPUT -d {} -j DROP",
                            to_node.blockyard_addr().ip()
                        ),
                    )
                    .await?;
            }
            Fault::NetworkDelay { node_id, latency } => {
                let ms = latency.as_millis();
                self.cluster
                    .ssh_exec(
                        *node_id,
                        &format!("tc qdisc add dev eth0 root netem delay {ms}ms"),
                    )
                    .await?;
            }
            Fault::NetworkLoss { node_id, loss_pct } => {
                self.cluster
                    .ssh_exec(
                        *node_id,
                        &format!("tc qdisc add dev eth0 root netem loss {loss_pct}%"),
                    )
                    .await?;
            }
            Fault::NetworkReset { node_id } => {
                self.cluster
                    .ssh_exec(*node_id, "tc qdisc del dev eth0 root 2>/dev/null; iptables -F INPUT; iptables -F OUTPUT")
                    .await?;
            }
            Fault::DiskSlow { node_id, latency } => {
                let ms = latency.as_millis();
                self.cluster
                    .ssh_exec(
                        *node_id,
                        &format!(
                            "dmsetup create delay-disk --table '0 $(blockdev --getsz /dev/vdb) delay /dev/vdb 0 {ms}'"
                        ),
                    )
                    .await?;
            }
            Fault::DiskFault { node_id } => {
                self.cluster
                    .ssh_exec(
                        *node_id,
                        "dmsetup create flakey-disk --table '0 $(blockdev --getsz /dev/vdb) flakey /dev/vdb 0 0 5'",
                    )
                    .await?;
            }
            Fault::DiskHeal { node_id } => {
                self.cluster
                    .ssh_exec(*node_id, "dmsetup remove delay-disk 2>/dev/null; dmsetup remove flakey-disk 2>/dev/null")
                    .await?;
            }
            Fault::ClockSkew { node_id, offset } => {
                let sign = if *offset >= 0 { "+" } else { "" };
                self.cluster
                    .ssh_exec(
                        *node_id,
                        &format!("date -s \"{sign}{offset} seconds\""),
                    )
                    .await?;
            }
            Fault::FillDisk { node_id } => {
                self.cluster
                    .ssh_exec(
                        *node_id,
                        "dd if=/dev/zero of=/blockyard-pool-fill bs=1M count=99999 2>/dev/null || true",
                    )
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn inject_sequence(
        &self,
        faults: &[Fault],
        interval: Duration,
    ) -> anyhow::Result<()> {
        for fault in faults {
            self.inject(fault).await?;
            tokio::time::sleep(interval).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fault_description() {
        let cases = vec![
            (
                Fault::NodeCrash { node_id: 0 },
                "crash node 0",
            ),
            (
                Fault::NodePause { node_id: 1 },
                "pause node 1",
            ),
            (
                Fault::NodeResume { node_id: 1 },
                "resume node 1",
            ),
            (
                Fault::NetworkPartition { from: 0, to: 1 },
                "partition node 0 <-> node 1",
            ),
            (
                Fault::NetworkHeal { from: 0, to: 1 },
                "heal partition node 0 <-> node 1",
            ),
            (
                Fault::AsymmetricPartition {
                    blocked_from: 0,
                    blocked_to: 1,
                },
                "asymmetric partition 0 -> 1",
            ),
            (
                Fault::NetworkDelay {
                    node_id: 2,
                    latency: Duration::from_millis(100),
                },
                "add 100ms delay to node 2",
            ),
            (
                Fault::NetworkLoss {
                    node_id: 3,
                    loss_pct: 10,
                },
                "add 10% packet loss to node 3",
            ),
            (
                Fault::NetworkReset { node_id: 0 },
                "reset network on node 0",
            ),
            (
                Fault::DiskSlow {
                    node_id: 0,
                    latency: Duration::from_millis(50),
                },
                "slow disk on node 0 by 50ms",
            ),
            (
                Fault::DiskFault { node_id: 1 },
                "inject disk fault on node 1",
            ),
            (
                Fault::DiskHeal { node_id: 1 },
                "heal disk on node 1",
            ),
            (
                Fault::ClockSkew {
                    node_id: 0,
                    offset: 30,
                },
                "skew clock on node 0 by 30s",
            ),
            (
                Fault::FillDisk { node_id: 2 },
                "fill disk on node 2",
            ),
        ];
        for (fault, expected) in cases {
            assert_eq!(fault.description(), expected);
        }
    }

    #[test]
    fn test_fault_injector_new() {
        use crate::harness::cluster::{ClusterConfig, TestCluster};
        let cluster = TestCluster::new(ClusterConfig::default());
        let _injector = FaultInjector::new(&cluster);
    }
}
