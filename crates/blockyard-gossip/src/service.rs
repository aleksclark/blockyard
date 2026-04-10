//! High-level gossip service API.
//!
//! [`GossipService`] wraps the SWIM protocol, transport, and member list
//! into a single entry point. Call [`start`](GossipService::start) to
//! spawn the gossip protocol loop, and [`stop`](GossipService::stop) to
//! shut it down.

use std::net::SocketAddr;
use std::sync::Arc;

use blockyard_common::{GossipSection, NodeId};
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::member::MemberList;
use crate::swim::{SwimConfig, SwimProtocol};
use crate::transport::{GossipTransport, UdpTransport};

/// Callback type for membership change events.
pub type MemberCallback = Box<dyn Fn(NodeId, SocketAddr) + Send + Sync>;

/// High-level gossip service that manages the SWIM protocol lifecycle.
pub struct GossipService {
    node_id: NodeId,
    config: GossipSection,
    members: Arc<MemberList>,
    protocol: Arc<SwimProtocol>,
    stop_tx: watch::Sender<bool>,
    stop_rx: watch::Receiver<bool>,
    task_handles: Mutex<Vec<JoinHandle<()>>>,
    on_join: Mutex<Option<MemberCallback>>,
    on_leave: Mutex<Option<MemberCallback>>,
}

impl std::fmt::Debug for GossipService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GossipService")
            .field("node_id", &self.node_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl GossipService {
    /// Create a new gossip service from config.
    pub fn new(node_id: NodeId, config: GossipSection) -> Self {
        let members = Arc::new(MemberList::new(node_id, config.bind_addr));
        let swim_config = SwimConfig::new(config.gossip_interval_ms, config.suspicion_mult);
        let protocol = Arc::new(SwimProtocol::new(Arc::clone(&members), swim_config));
        let (stop_tx, stop_rx) = watch::channel(false);

        Self {
            node_id,
            config,
            members,
            protocol,
            stop_tx,
            stop_rx,
            task_handles: Mutex::new(Vec::new()),
            on_join: Mutex::new(None),
            on_leave: Mutex::new(None),
        }
    }

    /// Set a callback invoked when a new member joins (becomes Alive).
    pub fn on_member_join(&self, callback: MemberCallback) {
        *self.on_join.lock() = Some(callback);
    }

    /// Set a callback invoked when a member leaves (becomes Dead).
    pub fn on_member_leave(&self, callback: MemberCallback) {
        *self.on_leave.lock() = Some(callback);
    }

    /// Start the gossip protocol. Binds to the configured address,
    /// joins seed nodes, and spawns the protocol loop tasks.
    pub async fn start(&self) -> Result<(), crate::transport::TransportError> {
        let transport = Arc::new(UdpTransport::bind(self.config.bind_addr).await?);
        let actual_addr = transport.local_addr();
        info!(addr = %actual_addr, node = %self.node_id, "gossip service started");

        self.protocol
            .join_seeds(&*transport, &self.config.seed_nodes)
            .await;

        let recv_handle = self.spawn_recv_loop(Arc::clone(&transport));
        let probe_handle = self.spawn_probe_loop(Arc::clone(&transport));
        let reap_handle = self.spawn_reap_loop();

        let mut handles = self.task_handles.lock();
        handles.push(recv_handle);
        handles.push(probe_handle);
        handles.push(reap_handle);

        Ok(())
    }

    /// Start the gossip protocol with a custom transport (for testing).
    pub async fn start_with_transport<T: GossipTransport + 'static>(&self, transport: Arc<T>) {
        self.protocol
            .join_seeds(&*transport, &self.config.seed_nodes)
            .await;

        let recv_handle = self.spawn_recv_loop(Arc::clone(&transport));
        let probe_handle = self.spawn_probe_loop(Arc::clone(&transport));
        let reap_handle = self.spawn_reap_loop();

        let mut handles = self.task_handles.lock();
        handles.push(recv_handle);
        handles.push(probe_handle);
        handles.push(reap_handle);
    }

    /// Stop the gossip protocol and wait for all tasks to finish.
    pub async fn stop(&self) {
        let _ = self.stop_tx.send(true);

        let handles: Vec<_> = {
            let mut h = self.task_handles.lock();
            std::mem::take(&mut *h)
        };

        for handle in handles {
            handle.abort();
            let _ = handle.await;
        }

        info!(node = %self.node_id, "gossip service stopped");
    }

    /// Get the current list of alive members with their addresses.
    pub fn members(&self) -> Vec<(NodeId, SocketAddr)> {
        self.members
            .alive_members()
            .into_iter()
            .map(|m| (m.node_id, m.addr))
            .collect()
    }

    /// Get the full member list.
    pub fn member_list(&self) -> &Arc<MemberList> {
        &self.members
    }

    /// Get the local node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Spawn the receive loop: reads messages from transport and dispatches them.
    fn spawn_recv_loop<T: GossipTransport + 'static>(&self, transport: Arc<T>) -> JoinHandle<()> {
        let protocol = Arc::clone(&self.protocol);
        let mut stop_rx = self.stop_rx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = transport.recv_from() => {
                        match result {
                            Ok((msg, from)) => {
                                protocol.handle_message(msg, from, &*transport).await;
                            }
                            Err(e) => {
                                debug!(error = %e, "recv error, continuing");
                            }
                        }
                    }
                    _ = stop_rx.changed() => {
                        debug!("recv loop stopping");
                        return;
                    }
                }
            }
        })
    }

    /// Spawn the probe loop: runs periodic SWIM probe cycles.
    fn spawn_probe_loop<T: GossipTransport + 'static>(&self, transport: Arc<T>) -> JoinHandle<()> {
        let protocol = Arc::clone(&self.protocol);
        let interval = protocol.config().gossip_interval;
        let mut stop_rx = self.stop_rx.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        protocol.probe_cycle(&*transport).await;
                    }
                    _ = stop_rx.changed() => {
                        debug!("probe loop stopping");
                        return;
                    }
                }
            }
        })
    }

    /// Spawn the reap loop: periodically checks for suspects that should be declared dead.
    fn spawn_reap_loop(&self) -> JoinHandle<()> {
        let protocol = Arc::clone(&self.protocol);
        let interval = protocol.config().gossip_interval;
        let mut stop_rx = self.stop_rx.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        protocol.reap_suspects();
                    }
                    _ = stop_rx.changed() => {
                        debug!("reap loop stopping");
                        return;
                    }
                }
            }
        })
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

    fn make_config(port: u16) -> GossipSection {
        GossipSection {
            bind_addr: make_addr(port),
            seed_nodes: vec![],
            gossip_interval_ms: 100,
            suspicion_mult: 4,
        }
    }

    #[test]
    fn test_gossip_service_new() {
        let id = NodeId::generate();
        let svc = GossipService::new(id, make_config(9000));
        assert_eq!(svc.node_id(), id);
    }

    #[test]
    fn test_gossip_service_members_initially_self() {
        let id = NodeId::generate();
        let svc = GossipService::new(id, make_config(9000));
        let members = svc.members();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].0, id);
    }

    #[test]
    fn test_gossip_service_member_list() {
        let id = NodeId::generate();
        let svc = GossipService::new(id, make_config(9000));
        assert_eq!(svc.member_list().local_id(), id);
    }

    #[test]
    fn test_gossip_service_debug() {
        let svc = GossipService::new(NodeId::generate(), make_config(9000));
        let dbg = format!("{:?}", svc);
        assert!(dbg.contains("GossipService"));
    }

    #[tokio::test]
    async fn test_gossip_service_start_stop() {
        let svc = GossipService::new(NodeId::generate(), make_config(0));
        svc.start().await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        svc.stop().await;
    }

    #[tokio::test]
    async fn test_gossip_service_start_with_transport() {
        let svc = GossipService::new(NodeId::generate(), make_config(9000));
        let transport = Arc::new(InMemoryTransport::new(make_addr(9000)));

        svc.start_with_transport(transport).await;

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        svc.stop().await;
    }

    #[tokio::test]
    async fn test_gossip_service_with_seeds() {
        let seed_addr = make_addr(9099);
        let config = GossipSection {
            bind_addr: make_addr(9000),
            seed_nodes: vec![seed_addr],
            gossip_interval_ms: 100,
            suspicion_mult: 4,
        };

        let svc = GossipService::new(NodeId::generate(), config);
        let transport = Arc::new(InMemoryTransport::new(make_addr(9000)));

        svc.start_with_transport(Arc::clone(&transport)).await;

        let sent = transport.sent_messages();
        assert!(!sent.is_empty());
        assert!(matches!(&sent[0].0, GossipMessage::Join { .. }));

        svc.stop().await;
    }

    #[test]
    fn test_gossip_service_on_member_join() {
        let svc = GossipService::new(NodeId::generate(), make_config(9000));
        let called = Arc::new(Mutex::new(false));
        let called_clone = Arc::clone(&called);
        svc.on_member_join(Box::new(move |_, _| {
            *called_clone.lock() = true;
        }));
        assert!(svc.on_join.lock().is_some());
    }

    #[test]
    fn test_gossip_service_on_member_leave() {
        let svc = GossipService::new(NodeId::generate(), make_config(9000));
        let called = Arc::new(Mutex::new(false));
        let called_clone = Arc::clone(&called);
        svc.on_member_leave(Box::new(move |_, _| {
            *called_clone.lock() = true;
        }));
        assert!(svc.on_leave.lock().is_some());
    }

    #[test]
    fn test_gossip_service_members_after_update() {
        let id = NodeId::generate();
        let svc = GossipService::new(id, make_config(9000));

        let remote_id = NodeId::generate();
        svc.member_list().apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 0,
        });

        let members = svc.members();
        assert_eq!(members.len(), 2);
    }

    #[tokio::test]
    async fn test_gossip_service_stop_idempotent() {
        let svc = GossipService::new(NodeId::generate(), make_config(0));
        svc.start().await.unwrap();
        svc.stop().await;
        svc.stop().await;
    }

    use crate::protocol::GossipMessage;

    #[test]
    fn test_gossip_service_members_excludes_dead() {
        let id = NodeId::generate();
        let svc = GossipService::new(id, make_config(9000));

        let remote_id = NodeId::generate();
        svc.member_list().apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Dead,
            incarnation: 0,
        });

        let members = svc.members();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].0, id);
    }
}
