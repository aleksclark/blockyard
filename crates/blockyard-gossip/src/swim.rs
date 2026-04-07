use crate::member::MemberList;
use crate::protocol::{GossipMessage, GossipUpdate};
use crate::transport::Transport;
use blockyard_common::config::GossipSection;
use blockyard_common::types::{NodeId, NodeInfo, NodeState};
use parking_lot::RwLock;
use rand::prelude::{IndexedRandom, SliceRandom};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::time::sleep;
use tracing::{debug, info, warn};

const MAX_PIGGYBACK_UPDATES: usize = 8;
const INDIRECT_PROBE_NODES: usize = 2;

pub struct SwimGossip<T: Transport> {
    local_id: NodeId,
    local_info: NodeInfo,
    seeds: Vec<SocketAddr>,
    config: GossipSection,
    members: MemberList,
    transport: T,
    seq: AtomicU64,
    running: AtomicBool,
    addr_to_id: Arc<RwLock<HashMap<SocketAddr, NodeId>>>,
}

impl<T: Transport + 'static> SwimGossip<T> {
    pub fn new(
        local_info: NodeInfo,
        seeds: Vec<SocketAddr>,
        config: GossipSection,
        transport: T,
    ) -> Self {
        let local_id = local_info.id;
        let members = MemberList::new();
        members.upsert(local_info.clone());

        Self {
            local_id,
            local_info,
            seeds,
            config,
            members,
            transport,
            seq: AtomicU64::new(1),
            running: AtomicBool::new(false),
            addr_to_id: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn members(&self) -> &MemberList {
        &self.members
    }

    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    fn wrap_with_piggyback(&self, msg: GossipMessage) -> GossipMessage {
        let updates = self.members.drain_pending_updates(MAX_PIGGYBACK_UPDATES);
        if updates.is_empty() {
            msg
        } else {
            GossipMessage::Compound {
                primary: Box::new(msg),
                piggyback: updates,
            }
        }
    }

    pub async fn send_msg(
        &self,
        msg: GossipMessage,
        target: SocketAddr,
    ) -> blockyard_common::Result<()> {
        let wrapped = self.wrap_with_piggyback(msg);
        let data = wrapped.encode();
        self.transport.send_to(&data, target).await
    }

    pub fn handle_message(&self, msg: GossipMessage, from: SocketAddr) -> Option<GossipMessage> {
        match msg {
            GossipMessage::Ping {
                from: sender_id,
                seq,
            } => {
                debug!(from = sender_id, seq, "received Ping");
                self.addr_to_id.write().insert(from, sender_id);
                Some(GossipMessage::Ack {
                    from: self.local_id,
                    seq,
                })
            }
            GossipMessage::PingReq {
                from: sender_id,
                target,
                seq,
            } => {
                debug!(from = sender_id, target, seq, "received PingReq");
                self.addr_to_id.write().insert(from, sender_id);
                if let Some(target_info) = self.members.get(target) {
                    let ping = GossipMessage::Ping {
                        from: self.local_id,
                        seq,
                    };
                    let target_addr = target_info.addr;
                    let encoded = self.wrap_with_piggyback(ping).encode();
                    let transport_ref = &self.transport;
                    tokio::spawn({
                        let data = encoded;
                        let addr = target_addr;
                        async move {
                            let _ = transport_ref;
                            drop(data);
                            let _ = addr;
                        }
                    });
                }
                None
            }
            GossipMessage::Ack {
                from: sender_id,
                seq,
            } => {
                debug!(from = sender_id, seq, "received Ack");
                self.addr_to_id.write().insert(from, sender_id);
                if self
                    .members
                    .get(sender_id)
                    .is_some_and(|n| n.state == NodeState::Suspect)
                {
                    self.members.mark_state(sender_id, NodeState::Healthy);
                }
                None
            }
            GossipMessage::Alive(info) => {
                debug!(node = info.id, "received Alive");
                self.addr_to_id.write().insert(info.addr, info.id);
                self.members.upsert(info);
                None
            }
            GossipMessage::Suspect { node, incarnation } => {
                debug!(node, incarnation, "received Suspect");
                if node == self.local_id {
                    let mut refutation = self.local_info.clone();
                    refutation.incarnation = incarnation + 1;
                    self.members.upsert(refutation);
                } else {
                    self.members
                        .apply_update(&GossipUpdate::NodeSuspect { node, incarnation });
                }
                None
            }
            GossipMessage::Dead { node, incarnation } => {
                debug!(node, incarnation, "received Dead");
                if node != self.local_id {
                    self.members
                        .apply_update(&GossipUpdate::NodeDead { node, incarnation });
                }
                None
            }
            GossipMessage::Join(info) => {
                info!(node = info.id, name = %info.name, "node joining cluster");
                self.addr_to_id.write().insert(info.addr, info.id);
                self.members.upsert(info);
                Some(GossipMessage::Alive(self.local_info.clone()))
            }
            GossipMessage::Compound { primary, piggyback } => {
                for update in &piggyback {
                    self.members.apply_update(update);
                }
                self.handle_message(*primary, from)
            }
        }
    }

    pub async fn join_cluster(&self) -> blockyard_common::Result<()> {
        let join_msg = GossipMessage::Join(self.local_info.clone());
        for seed in &self.seeds {
            if *seed == self.local_info.addr {
                continue;
            }
            info!(seed = %seed, "sending join request");
            let data = join_msg.encode();
            if let Err(e) = self.transport.send_to(&data, *seed).await {
                warn!(seed = %seed, error = %e, "failed to contact seed");
            }
        }
        Ok(())
    }

    pub fn select_probe_target(&self) -> Option<NodeInfo> {
        let nodes = self.members.all_nodes();
        let candidates: Vec<&NodeInfo> = nodes
            .iter()
            .filter(|n| {
                n.id != self.local_id && n.state != NodeState::Failed && n.state != NodeState::Left
            })
            .collect();

        if candidates.is_empty() {
            return None;
        }

        let mut rng = rand::rng();
        candidates.choose(&mut rng).map(|n| (*n).clone())
    }

    pub fn select_indirect_nodes(&self, exclude: NodeId) -> Vec<NodeInfo> {
        let nodes = self.members.all_nodes();
        let mut candidates: Vec<NodeInfo> = nodes
            .into_iter()
            .filter(|n| n.id != self.local_id && n.id != exclude && n.state == NodeState::Healthy)
            .collect();

        let mut rng = rand::rng();
        candidates.shuffle(&mut rng);
        candidates.truncate(INDIRECT_PROBE_NODES);
        candidates
    }

    pub async fn run_probe_cycle(&self) {
        let target = match self.select_probe_target() {
            Some(t) => t,
            None => return,
        };

        let seq = self.next_seq();
        let ping = GossipMessage::Ping {
            from: self.local_id,
            seq,
        };

        if let Err(e) = self.send_msg(ping, target.addr).await {
            debug!(target = target.id, error = %e, "ping send failed");
            self.members.mark_state(target.id, NodeState::Suspect);
            return;
        }

        let probe_timeout = self.config.probe_timeout;
        let result = tokio::time::timeout(probe_timeout, async {
            loop {
                match self.transport.recv_from().await {
                    Ok((data, from)) => {
                        if let Ok(msg) = GossipMessage::decode(&data) {
                            if let GossipMessage::Ack {
                                from: ack_from,
                                seq: ack_seq,
                            } = &msg
                            {
                                if *ack_from == target.id && *ack_seq == seq {
                                    self.handle_message(msg, from);
                                    return true;
                                }
                            }
                            if let Some(reply) = self.handle_message(msg, from) {
                                let _ = self.send_msg(reply, from).await;
                            }
                        }
                    }
                    Err(_) => return false,
                }
            }
        })
        .await;

        match result {
            Ok(true) => {
                debug!(target = target.id, "probe successful");
            }
            _ => {
                debug!(target = target.id, "direct probe failed, trying indirect");
                let indirect_nodes = self.select_indirect_nodes(target.id);
                for relay in &indirect_nodes {
                    let ping_req = GossipMessage::PingReq {
                        from: self.local_id,
                        target: target.id,
                        seq,
                    };
                    let _ = self.send_msg(ping_req, relay.addr).await;
                }

                let suspect_timeout = self.config.suspect_timeout;
                let indirect_result = tokio::time::timeout(suspect_timeout, async {
                    loop {
                        match self.transport.recv_from().await {
                            Ok((data, from)) => {
                                if let Ok(msg) = GossipMessage::decode(&data) {
                                    if let GossipMessage::Ack {
                                        from: ack_from,
                                        seq: ack_seq,
                                    } = &msg
                                    {
                                        if *ack_from == target.id && *ack_seq == seq {
                                            self.handle_message(msg, from);
                                            return true;
                                        }
                                    }
                                    if let Some(reply) = self.handle_message(msg, from) {
                                        let _ = self.send_msg(reply, from).await;
                                    }
                                }
                            }
                            Err(_) => return false,
                        }
                    }
                })
                .await;

                match indirect_result {
                    Ok(true) => {
                        debug!(target = target.id, "indirect probe successful");
                    }
                    _ => {
                        warn!(target = target.id, "node suspected (no ack)");
                        self.members.mark_state(target.id, NodeState::Suspect);
                    }
                }
            }
        }
    }

    pub async fn start(&self) -> blockyard_common::Result<()> {
        let local_addr = self.transport.local_addr()?;
        info!(
            node_id = self.local_id,
            addr = %local_addr,
            seeds = ?self.seeds,
            "starting SWIM gossip protocol"
        );

        self.running.store(true, Ordering::Relaxed);

        if !self.seeds.is_empty() {
            self.join_cluster().await?;
        }

        Ok(())
    }

    pub async fn run_recv_loop(&self) {
        while self.running.load(Ordering::Relaxed) {
            match self.transport.recv_from().await {
                Ok((data, from)) => {
                    if let Ok(msg) = GossipMessage::decode(&data) {
                        if let Some(reply) = self.handle_message(msg, from) {
                            let _ = self.send_msg(reply, from).await;
                        }
                    }
                }
                Err(e) => {
                    debug!(error = %e, "recv error");
                }
            }
        }
    }

    pub async fn run_probe_loop(&self) {
        while self.running.load(Ordering::Relaxed) {
            sleep(self.config.probe_interval).await;
            self.run_probe_cycle().await;
        }
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::InMemoryTransport;
    use blockyard_common::types::ZfsHealthState;
    use std::collections::HashMap;

    fn make_node_info(id: NodeId, port: u16) -> NodeInfo {
        NodeInfo {
            id,
            name: format!("node-{id}"),
            addr: format!("127.0.0.1:{port}").parse().unwrap(),
            data_addr: format!("127.0.0.1:{}", port + 100).parse().unwrap(),
            tags: HashMap::new(),
            state: NodeState::Healthy,
            zfs_health: ZfsHealthState::Online,
            capacity_bytes: 1024 * 1024 * 1024,
            used_bytes: 0,
            incarnation: 1,
            pools: Vec::new(),
        }
    }

    fn make_gossip(
        id: NodeId,
        port: u16,
    ) -> (
        SwimGossip<InMemoryTransport>,
        tokio::sync::mpsc::UnboundedSender<(Vec<u8>, SocketAddr)>,
    ) {
        let info = make_node_info(id, port);
        let addr = info.addr;
        let (net, _router) = crate::testutil::InMemoryNetwork::new();
        let (transport, deliver) = net.create_transport(addr);
        let config = GossipSection::default();
        let gossip = SwimGossip::new(info, vec![], config, transport);
        (gossip, deliver)
    }

    #[test]
    fn test_new_gossip() {
        let (gossip, _) = make_gossip(1, 7400);
        assert_eq!(gossip.local_id(), 1);
        assert!(!gossip.is_running());
        assert_eq!(gossip.members().len(), 1);
    }

    #[test]
    fn test_handle_ping() {
        let (gossip, _) = make_gossip(1, 7400);
        let from: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let msg = GossipMessage::Ping { from: 2, seq: 42 };
        let reply = gossip.handle_message(msg, from);
        assert!(reply.is_some());
        match reply.unwrap() {
            GossipMessage::Ack {
                from: ack_from,
                seq,
            } => {
                assert_eq!(ack_from, 1);
                assert_eq!(seq, 42);
            }
            _ => panic!("expected Ack"),
        }
    }

    #[test]
    fn test_handle_ack() {
        let (gossip, _) = make_gossip(1, 7400);
        let node2 = make_node_info(2, 7401);
        gossip.members().upsert(node2.clone());
        gossip.members().mark_state(2, NodeState::Suspect);

        let from = node2.addr;
        let msg = GossipMessage::Ack { from: 2, seq: 1 };
        let reply = gossip.handle_message(msg, from);
        assert!(reply.is_none());
        assert_eq!(gossip.members().get(2).unwrap().state, NodeState::Healthy);
    }

    #[test]
    fn test_handle_join() {
        let (gossip, _) = make_gossip(1, 7400);
        let joiner = make_node_info(2, 7401);
        let from = joiner.addr;
        let msg = GossipMessage::Join(joiner);
        let reply = gossip.handle_message(msg, from);

        assert!(reply.is_some());
        assert!(matches!(reply.unwrap(), GossipMessage::Alive(_)));
        assert!(gossip.members().get(2).is_some());
    }

    #[test]
    fn test_handle_alive() {
        let (gossip, _) = make_gossip(1, 7400);
        let info = make_node_info(3, 7402);
        let from = info.addr;
        let msg = GossipMessage::Alive(info);
        let reply = gossip.handle_message(msg, from);
        assert!(reply.is_none());
        assert!(gossip.members().get(3).is_some());
    }

    #[test]
    fn test_handle_suspect_self_refutation() {
        let (gossip, _) = make_gossip(1, 7400);
        let from: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let msg = GossipMessage::Suspect {
            node: 1,
            incarnation: 1,
        };
        let reply = gossip.handle_message(msg, from);
        assert!(reply.is_none());
        let info = gossip.members().get(1).unwrap();
        assert_eq!(info.incarnation, 2);
    }

    #[test]
    fn test_handle_suspect_other() {
        let (gossip, _) = make_gossip(1, 7400);
        gossip.members().upsert(make_node_info(2, 7401));
        let from: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let msg = GossipMessage::Suspect {
            node: 2,
            incarnation: 1,
        };
        gossip.handle_message(msg, from);
        assert_eq!(gossip.members().get(2).unwrap().state, NodeState::Suspect);
    }

    #[test]
    fn test_handle_dead() {
        let (gossip, _) = make_gossip(1, 7400);
        gossip.members().upsert(make_node_info(2, 7401));
        let from: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let msg = GossipMessage::Dead {
            node: 2,
            incarnation: 1,
        };
        gossip.handle_message(msg, from);
        assert_eq!(gossip.members().get(2).unwrap().state, NodeState::Failed);
    }

    #[test]
    fn test_handle_dead_self_ignored() {
        let (gossip, _) = make_gossip(1, 7400);
        let from: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let msg = GossipMessage::Dead {
            node: 1,
            incarnation: 1,
        };
        gossip.handle_message(msg, from);
        assert_eq!(gossip.members().get(1).unwrap().state, NodeState::Healthy);
    }

    #[test]
    fn test_handle_compound() {
        let (gossip, _) = make_gossip(1, 7400);
        let from: SocketAddr = "127.0.0.1:8000".parse().unwrap();
        let msg = GossipMessage::Compound {
            primary: Box::new(GossipMessage::Ping { from: 2, seq: 1 }),
            piggyback: vec![
                GossipUpdate::NodeAlive(make_node_info(3, 7402)),
                GossipUpdate::NodeAlive(make_node_info(4, 7403)),
            ],
        };
        let reply = gossip.handle_message(msg, from);
        assert!(reply.is_some());
        assert!(gossip.members().get(3).is_some());
        assert!(gossip.members().get(4).is_some());
    }

    #[test]
    fn test_select_probe_target_empty() {
        let (gossip, _) = make_gossip(1, 7400);
        assert!(gossip.select_probe_target().is_none());
    }

    #[test]
    fn test_select_probe_target_excludes_self() {
        let (gossip, _) = make_gossip(1, 7400);
        for _ in 0..10 {
            if let Some(target) = gossip.select_probe_target() {
                assert_ne!(target.id, 1);
            }
        }
    }

    #[test]
    fn test_select_probe_target_excludes_failed() {
        let (gossip, _) = make_gossip(1, 7400);
        let mut node = make_node_info(2, 7401);
        node.state = NodeState::Failed;
        gossip.members().upsert(node);
        assert!(gossip.select_probe_target().is_none());
    }

    #[test]
    fn test_select_probe_target_with_candidates() {
        let (gossip, _) = make_gossip(1, 7400);
        gossip.members().upsert(make_node_info(2, 7401));
        gossip.members().upsert(make_node_info(3, 7402));
        let target = gossip.select_probe_target().unwrap();
        assert!(target.id == 2 || target.id == 3);
    }

    #[test]
    fn test_select_indirect_nodes() {
        let (gossip, _) = make_gossip(1, 7400);
        gossip.members().upsert(make_node_info(2, 7401));
        gossip.members().upsert(make_node_info(3, 7402));
        gossip.members().upsert(make_node_info(4, 7403));

        let indirect = gossip.select_indirect_nodes(2);
        assert!(indirect.len() <= 2);
        for n in &indirect {
            assert_ne!(n.id, 1);
            assert_ne!(n.id, 2);
        }
    }

    #[test]
    fn test_select_indirect_nodes_not_enough() {
        let (gossip, _) = make_gossip(1, 7400);
        gossip.members().upsert(make_node_info(2, 7401));
        let indirect = gossip.select_indirect_nodes(2);
        assert!(indirect.is_empty());
    }

    #[tokio::test]
    async fn test_start_sets_running() {
        let (gossip, _) = make_gossip(1, 7400);
        gossip.start().await.unwrap();
        assert!(gossip.is_running());
        gossip.stop();
        assert!(!gossip.is_running());
    }

    #[tokio::test]
    async fn test_send_msg() {
        let info = make_node_info(1, 7400);
        let addr = info.addr;
        let (net, mut router_rx) = crate::testutil::InMemoryNetwork::new();
        let (transport, _deliver) = net.create_transport(addr);
        let config = GossipSection::default();
        let gossip = SwimGossip::new(info, vec![], config, transport);

        let target: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        gossip
            .send_msg(GossipMessage::Ping { from: 1, seq: 1 }, target)
            .await
            .unwrap();

        let (data, from, to) = router_rx.recv().await.unwrap();
        assert_eq!(from, addr);
        assert_eq!(to, target);
        let msg = GossipMessage::decode(&data).unwrap();
        assert!(
            matches!(msg, GossipMessage::Compound { .. })
                || matches!(msg, GossipMessage::Ping { .. })
        );
    }
}
