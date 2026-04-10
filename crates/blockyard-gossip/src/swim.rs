//! SWIM failure detection protocol implementation.
//!
//! Implements the probe cycle: each period, the local node picks a random
//! target to ping. If no ack arrives within the timeout, indirect probes
//! (ping-req) are sent through relay nodes. If still no ack, the target
//! is marked suspect. Suspects that don't refute within the suspicion
//! timeout are declared dead.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use blockyard_common::NodeId;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::{debug, trace, warn};

use crate::member::MemberList;
use crate::protocol::GossipMessage;
use crate::transport::GossipTransport;

/// Number of relay nodes to use for indirect probes.
const INDIRECT_PROBE_COUNT: usize = 3;

/// Maximum number of membership updates to piggyback on each message.
const MAX_PIGGYBACK_UPDATES: usize = 10;

/// Configuration for the SWIM protocol.
#[derive(Debug, Clone)]
pub struct SwimConfig {
    pub gossip_interval: Duration,
    pub suspicion_timeout: Duration,
    pub probe_timeout: Duration,
}

impl SwimConfig {
    /// Create a config from gossip_interval_ms and suspicion_mult.
    pub fn new(gossip_interval_ms: u64, suspicion_mult: u32) -> Self {
        let gossip_interval = Duration::from_millis(gossip_interval_ms);
        let suspicion_timeout = gossip_interval * suspicion_mult;
        let probe_timeout = gossip_interval / 2;
        Self {
            gossip_interval,
            suspicion_timeout,
            probe_timeout,
        }
    }
}

/// Tracks pending probes awaiting acknowledgement.
#[derive(Debug)]
struct PendingProbe {
    _target: NodeId,
    _sent_at: Instant,
    indirect_sent: bool,
}

/// SWIM protocol state machine.
#[derive(Debug)]
pub struct SwimProtocol {
    members: Arc<MemberList>,
    config: SwimConfig,
    seq: Mutex<u64>,
    pending_probes: Mutex<HashMap<u64, PendingProbe>>,
    ack_notify: Notify,
}

impl SwimProtocol {
    /// Create a new SWIM protocol instance.
    pub fn new(members: Arc<MemberList>, config: SwimConfig) -> Self {
        Self {
            members,
            config,
            seq: Mutex::new(0),
            pending_probes: Mutex::new(HashMap::new()),
            ack_notify: Notify::new(),
        }
    }

    /// Return the member list.
    pub fn members(&self) -> &Arc<MemberList> {
        &self.members
    }

    /// Allocate the next sequence number.
    fn next_seq(&self) -> u64 {
        let mut seq = self.seq.lock();
        *seq += 1;
        *seq
    }

    /// Run one probe cycle: pick a target, send a ping, wait for ack.
    pub async fn probe_cycle<T: GossipTransport>(&self, transport: &T) {
        let targets = self.members.probe_targets();
        if targets.is_empty() {
            trace!("no probe targets, skipping cycle");
            return;
        }

        let idx = self.next_seq() as usize % targets.len();
        let target = &targets[idx];

        let seq = self.next_seq();
        let updates = self.members.recent_updates(MAX_PIGGYBACK_UPDATES);

        let ping = GossipMessage::Ping {
            from: self.members.local_id(),
            from_addr: transport.local_addr(),
            seq,
            updates,
        };

        {
            let mut pending = self.pending_probes.lock();
            pending.insert(
                seq,
                PendingProbe {
                    _target: target.node_id,
                    _sent_at: Instant::now(),
                    indirect_sent: false,
                },
            );
        }

        debug!(
            target_node = %target.node_id,
            target_addr = %target.addr,
            seq,
            "sending ping"
        );

        if let Err(e) = transport.send_to(&ping, target.addr).await {
            warn!(error = %e, target = %target.node_id, "failed to send ping");
        }

        let deadline = Instant::now() + self.config.probe_timeout;
        let acked = self.wait_for_ack(seq, deadline).await;

        if !acked {
            self.send_indirect_probes(transport, target.node_id, target.addr, seq)
                .await;

            let indirect_deadline = Instant::now() + self.config.probe_timeout;
            let indirect_acked = self.wait_for_ack(seq, indirect_deadline).await;

            if !indirect_acked {
                debug!(target = %target.node_id, "no ack after indirect probes, marking suspect");
                self.members.mark_suspect(target.node_id);
            }
        }

        self.pending_probes.lock().remove(&seq);
    }

    /// Wait for an ack with the given sequence number until the deadline.
    async fn wait_for_ack(&self, seq: u64, deadline: Instant) -> bool {
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }

            let is_pending = self.pending_probes.lock().contains_key(&seq);
            if !is_pending {
                return true;
            }

            tokio::select! {
                () = self.ack_notify.notified() => {
                    if !self.pending_probes.lock().contains_key(&seq) {
                        return true;
                    }
                }
                () = tokio::time::sleep(remaining) => {
                    return !self.pending_probes.lock().contains_key(&seq);
                }
            }
        }
    }

    /// Send indirect probes via relay nodes.
    async fn send_indirect_probes<T: GossipTransport>(
        &self,
        transport: &T,
        target_id: NodeId,
        target_addr: SocketAddr,
        seq: u64,
    ) {
        let candidates = self.members.indirect_probe_candidates(target_id);
        let count = candidates.len().min(INDIRECT_PROBE_COUNT);

        if count == 0 {
            debug!("no candidates for indirect probe");
            return;
        }

        {
            let mut pending = self.pending_probes.lock();
            if let Some(probe) = pending.get_mut(&seq) {
                probe.indirect_sent = true;
            }
        }

        let updates = self.members.recent_updates(MAX_PIGGYBACK_UPDATES);

        for relay in &candidates[..count] {
            let ping_req = GossipMessage::PingReq {
                from: self.members.local_id(),
                from_addr: transport.local_addr(),
                target: target_id,
                target_addr,
                seq,
                updates: updates.clone(),
            };

            debug!(
                relay = %relay.node_id,
                target = %target_id,
                "sending ping-req via relay"
            );

            if let Err(e) = transport.send_to(&ping_req, relay.addr).await {
                warn!(error = %e, relay = %relay.node_id, "failed to send ping-req");
            }
        }
    }

    /// Handle an incoming gossip message and produce responses.
    pub async fn handle_message<T: GossipTransport>(
        &self,
        msg: GossipMessage,
        from: SocketAddr,
        transport: &T,
    ) {
        self.members.apply_updates(msg.updates());

        match msg {
            GossipMessage::Ping {
                from: sender_id,
                from_addr,
                seq,
                ..
            } => {
                let ack = GossipMessage::Ack {
                    from: self.members.local_id(),
                    from_addr: transport.local_addr(),
                    seq,
                    updates: self.members.recent_updates(MAX_PIGGYBACK_UPDATES),
                };
                if let Err(e) = transport.send_to(&ack, from_addr).await {
                    warn!(error = %e, target = %sender_id, "failed to send ack");
                }
            }
            GossipMessage::PingReq {
                from: sender_id,
                from_addr,
                target,
                target_addr,
                seq,
                ..
            } => {
                let ping = GossipMessage::Ping {
                    from: self.members.local_id(),
                    from_addr: transport.local_addr(),
                    seq,
                    updates: self.members.recent_updates(MAX_PIGGYBACK_UPDATES),
                };
                if let Err(e) = transport.send_to(&ping, target_addr).await {
                    warn!(error = %e, target = %target, "failed to relay ping");
                }

                let _ = (sender_id, from_addr, from);
            }
            GossipMessage::Ack { seq, .. } => {
                let removed = self.pending_probes.lock().remove(&seq).is_some();
                if removed {
                    self.ack_notify.notify_waiters();
                }
            }
            GossipMessage::Join { node_id, addr, .. } => {
                self.members
                    .apply_update(&crate::protocol::MembershipUpdate {
                        node_id,
                        addr,
                        state: crate::protocol::MemberState::Alive,
                        incarnation: 0,
                    });

                let ack = GossipMessage::Ack {
                    from: self.members.local_id(),
                    from_addr: transport.local_addr(),
                    seq: 0,
                    updates: self.members.recent_updates(MAX_PIGGYBACK_UPDATES),
                };
                if let Err(e) = transport.send_to(&ack, addr).await {
                    warn!(error = %e, node = %node_id, "failed to send join ack");
                }
            }
            GossipMessage::Alive {
                node_id,
                addr,
                incarnation,
            } => {
                self.members
                    .apply_update(&crate::protocol::MembershipUpdate {
                        node_id,
                        addr,
                        state: crate::protocol::MemberState::Alive,
                        incarnation,
                    });
            }
            GossipMessage::Suspect {
                node_id,
                addr,
                incarnation,
            } => {
                self.members
                    .apply_update(&crate::protocol::MembershipUpdate {
                        node_id,
                        addr,
                        state: crate::protocol::MemberState::Suspect,
                        incarnation,
                    });
            }
            GossipMessage::Dead {
                node_id,
                addr,
                incarnation,
            } => {
                self.members
                    .apply_update(&crate::protocol::MembershipUpdate {
                        node_id,
                        addr,
                        state: crate::protocol::MemberState::Dead,
                        incarnation,
                    });
            }
        }
    }

    /// Check suspects that have exceeded the suspicion timeout and mark them dead.
    pub fn reap_suspects(&self) {
        let now = Instant::now();
        let suspects: Vec<NodeId> = self
            .members
            .probe_targets()
            .into_iter()
            .filter(|m| {
                m.state == crate::protocol::MemberState::Suspect
                    && now.duration_since(m.state_change) >= self.config.suspicion_timeout
            })
            .map(|m| m.node_id)
            .collect();

        for node_id in suspects {
            debug!(node = %node_id, "suspicion timeout, marking dead");
            self.members.mark_dead(node_id);
        }
    }

    /// Send Join messages to seed nodes.
    pub async fn join_seeds<T: GossipTransport>(&self, transport: &T, seeds: &[SocketAddr]) {
        let join = GossipMessage::Join {
            node_id: self.members.local_id(),
            addr: transport.local_addr(),
        };

        for seed in seeds {
            if *seed == transport.local_addr() {
                continue;
            }
            debug!(seed = %seed, "sending join to seed node");
            if let Err(e) = transport.send_to(&join, *seed).await {
                warn!(error = %e, seed = %seed, "failed to send join to seed");
            }
        }
    }

    /// Return the swim config.
    pub fn config(&self) -> &SwimConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{MemberState, MembershipUpdate};
    use crate::testutil::InMemoryTransport;

    fn make_addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn make_config() -> SwimConfig {
        SwimConfig::new(100, 4)
    }

    fn make_protocol(addr: SocketAddr) -> (Arc<MemberList>, SwimProtocol) {
        let id = NodeId::generate();
        let members = Arc::new(MemberList::new(id, addr));
        let proto = SwimProtocol::new(Arc::clone(&members), make_config());
        (members, proto)
    }

    #[test]
    fn test_swim_config_new() {
        let cfg = SwimConfig::new(1000, 4);
        assert_eq!(cfg.gossip_interval, Duration::from_millis(1000));
        assert_eq!(cfg.suspicion_timeout, Duration::from_millis(4000));
        assert_eq!(cfg.probe_timeout, Duration::from_millis(500));
    }

    #[test]
    fn test_swim_config_clone() {
        let cfg = SwimConfig::new(200, 3);
        let cloned = cfg.clone();
        assert_eq!(cfg.gossip_interval, cloned.gossip_interval);
    }

    #[test]
    fn test_swim_config_debug() {
        let cfg = SwimConfig::new(100, 2);
        let dbg = format!("{:?}", cfg);
        assert!(dbg.contains("SwimConfig"));
    }

    #[test]
    fn test_swim_protocol_new() {
        let (members, proto) = make_protocol(make_addr(9000));
        assert_eq!(proto.members().local_id(), members.local_id());
    }

    #[test]
    fn test_next_seq() {
        let (_, proto) = make_protocol(make_addr(9000));
        assert_eq!(proto.next_seq(), 1);
        assert_eq!(proto.next_seq(), 2);
        assert_eq!(proto.next_seq(), 3);
    }

    #[tokio::test]
    async fn test_probe_cycle_no_targets() {
        let (_, proto) = make_protocol(make_addr(9000));
        let transport = InMemoryTransport::new(make_addr(9000));
        proto.probe_cycle(&transport).await;
        assert!(transport.sent_messages().is_empty());
    }

    #[tokio::test]
    async fn test_probe_cycle_sends_ping() {
        let addr = make_addr(9000);
        let (members, proto) = make_protocol(addr);

        let remote_id = NodeId::generate();
        let remote_addr = make_addr(9001);
        members.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: remote_addr,
            state: MemberState::Alive,
            incarnation: 0,
        });

        let transport = InMemoryTransport::new(addr);
        proto.probe_cycle(&transport).await;

        let sent = transport.sent_messages();
        assert!(!sent.is_empty());
        let (msg, dest) = &sent[0];
        assert_eq!(*dest, remote_addr);
        assert!(matches!(msg, GossipMessage::Ping { .. }));
    }

    #[tokio::test]
    async fn test_handle_ping_sends_ack() {
        let addr = make_addr(9000);
        let (_, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        let remote_id = NodeId::generate();
        let remote_addr = make_addr(9001);
        let ping = GossipMessage::Ping {
            from: remote_id,
            from_addr: remote_addr,
            seq: 1,
            updates: vec![],
        };

        proto.handle_message(ping, remote_addr, &transport).await;

        let sent = transport.sent_messages();
        assert_eq!(sent.len(), 1);
        let (msg, dest) = &sent[0];
        assert_eq!(*dest, remote_addr);
        assert!(matches!(msg, GossipMessage::Ack { .. }));
    }

    #[tokio::test]
    async fn test_handle_join_adds_member_and_sends_ack() {
        let addr = make_addr(9000);
        let (members, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        let joiner_id = NodeId::generate();
        let joiner_addr = make_addr(9001);
        let join = GossipMessage::Join {
            node_id: joiner_id,
            addr: joiner_addr,
        };

        proto.handle_message(join, joiner_addr, &transport).await;

        assert!(members.get(&joiner_id).is_some());
        assert_eq!(members.get(&joiner_id).unwrap().state, MemberState::Alive);

        let sent = transport.sent_messages();
        assert_eq!(sent.len(), 1);
        assert!(matches!(&sent[0].0, GossipMessage::Ack { .. }));
    }

    #[tokio::test]
    async fn test_handle_ack_removes_pending_probe() {
        let addr = make_addr(9000);
        let (_, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        {
            let mut pending = proto.pending_probes.lock();
            pending.insert(
                42,
                PendingProbe {
                    _target: NodeId::generate(),
                    _sent_at: Instant::now(),
                    indirect_sent: false,
                },
            );
        }

        let ack = GossipMessage::Ack {
            from: NodeId::generate(),
            from_addr: make_addr(9001),
            seq: 42,
            updates: vec![],
        };

        proto.handle_message(ack, make_addr(9001), &transport).await;
        assert!(!proto.pending_probes.lock().contains_key(&42));
    }

    #[tokio::test]
    async fn test_handle_alive_message() {
        let addr = make_addr(9000);
        let (members, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        let node_id = NodeId::generate();
        let alive = GossipMessage::Alive {
            node_id,
            addr: make_addr(9001),
            incarnation: 5,
        };

        proto
            .handle_message(alive, make_addr(9001), &transport)
            .await;
        let m = members.get(&node_id).unwrap();
        assert_eq!(m.state, MemberState::Alive);
        assert_eq!(m.incarnation, 5);
    }

    #[tokio::test]
    async fn test_handle_suspect_message() {
        let addr = make_addr(9000);
        let (members, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        let node_id = NodeId::generate();
        members.apply_update(&MembershipUpdate {
            node_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 0,
        });

        let suspect = GossipMessage::Suspect {
            node_id,
            addr: make_addr(9001),
            incarnation: 1,
        };

        proto
            .handle_message(suspect, make_addr(9001), &transport)
            .await;
        assert_eq!(members.get(&node_id).unwrap().state, MemberState::Suspect);
    }

    #[tokio::test]
    async fn test_handle_dead_message() {
        let addr = make_addr(9000);
        let (members, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        let node_id = NodeId::generate();
        members.apply_update(&MembershipUpdate {
            node_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 0,
        });

        let dead = GossipMessage::Dead {
            node_id,
            addr: make_addr(9001),
            incarnation: 1,
        };

        proto
            .handle_message(dead, make_addr(9001), &transport)
            .await;
        assert_eq!(members.get(&node_id).unwrap().state, MemberState::Dead);
    }

    #[tokio::test]
    async fn test_handle_ping_req_relays_ping() {
        let addr = make_addr(9000);
        let (_, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        let from_id = NodeId::generate();
        let from_addr = make_addr(9001);
        let target_id = NodeId::generate();
        let target_addr = make_addr(9002);

        let ping_req = GossipMessage::PingReq {
            from: from_id,
            from_addr,
            target: target_id,
            target_addr,
            seq: 10,
            updates: vec![],
        };

        proto.handle_message(ping_req, from_addr, &transport).await;

        let sent = transport.sent_messages();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].1, target_addr);
        assert!(matches!(&sent[0].0, GossipMessage::Ping { .. }));
    }

    #[test]
    fn test_reap_suspects_no_suspects() {
        let (_, proto) = make_protocol(make_addr(9000));
        proto.reap_suspects();
    }

    #[tokio::test]
    async fn test_join_seeds() {
        let addr = make_addr(9000);
        let (_, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        let seeds = vec![make_addr(9001), make_addr(9002), addr];
        proto.join_seeds(&transport, &seeds).await;

        let sent = transport.sent_messages();
        assert_eq!(sent.len(), 2);
        for (msg, _) in &sent {
            assert!(matches!(msg, GossipMessage::Join { .. }));
        }
    }

    #[tokio::test]
    async fn test_join_seeds_empty() {
        let addr = make_addr(9000);
        let (_, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        proto.join_seeds(&transport, &[]).await;
        assert!(transport.sent_messages().is_empty());
    }

    #[test]
    fn test_protocol_config() {
        let (_, proto) = make_protocol(make_addr(9000));
        assert_eq!(proto.config().gossip_interval, Duration::from_millis(100));
    }

    #[test]
    fn test_protocol_debug() {
        let (_, proto) = make_protocol(make_addr(9000));
        let dbg = format!("{:?}", proto);
        assert!(dbg.contains("SwimProtocol"));
    }

    #[tokio::test]
    async fn test_handle_ping_with_updates() {
        let addr = make_addr(9000);
        let (members, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        let new_node = NodeId::generate();
        let ping = GossipMessage::Ping {
            from: NodeId::generate(),
            from_addr: make_addr(9001),
            seq: 1,
            updates: vec![MembershipUpdate {
                node_id: new_node,
                addr: make_addr(9002),
                state: MemberState::Alive,
                incarnation: 0,
            }],
        };

        proto
            .handle_message(ping, make_addr(9001), &transport)
            .await;
        assert!(members.get(&new_node).is_some());
    }

    #[tokio::test]
    async fn test_send_indirect_probes() {
        let addr = make_addr(9000);
        let (members, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        let target = NodeId::generate();
        let relay1 = NodeId::generate();
        let relay2 = NodeId::generate();

        members.apply_update(&MembershipUpdate {
            node_id: target,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 0,
        });
        members.apply_update(&MembershipUpdate {
            node_id: relay1,
            addr: make_addr(9002),
            state: MemberState::Alive,
            incarnation: 0,
        });
        members.apply_update(&MembershipUpdate {
            node_id: relay2,
            addr: make_addr(9003),
            state: MemberState::Alive,
            incarnation: 0,
        });

        {
            proto.pending_probes.lock().insert(
                99,
                PendingProbe {
                    _target: target,
                    _sent_at: Instant::now(),
                    indirect_sent: false,
                },
            );
        }

        proto
            .send_indirect_probes(&transport, target, make_addr(9001), 99)
            .await;

        let sent = transport.sent_messages();
        assert_eq!(sent.len(), 2);
        for (msg, _) in &sent {
            assert!(matches!(msg, GossipMessage::PingReq { .. }));
        }
    }

    #[tokio::test]
    async fn test_send_indirect_probes_no_candidates() {
        let addr = make_addr(9000);
        let (_, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        proto
            .send_indirect_probes(&transport, NodeId::generate(), make_addr(9001), 1)
            .await;

        assert!(transport.sent_messages().is_empty());
    }

    #[tokio::test]
    async fn test_ack_unknown_seq_is_noop() {
        let addr = make_addr(9000);
        let (_, proto) = make_protocol(addr);
        let transport = InMemoryTransport::new(addr);

        let ack = GossipMessage::Ack {
            from: NodeId::generate(),
            from_addr: make_addr(9001),
            seq: 999,
            updates: vec![],
        };

        proto.handle_message(ack, make_addr(9001), &transport).await;
        assert!(transport.sent_messages().is_empty());
    }
}
