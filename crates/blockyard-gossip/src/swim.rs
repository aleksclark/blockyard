use crate::member::MemberList;
use blockyard_common::config::GossipSection;
use blockyard_common::types::NodeId;
use std::net::SocketAddr;
use tracing::info;

pub struct SwimGossip {
    local_id: NodeId,
    listen_addr: SocketAddr,
    seeds: Vec<SocketAddr>,
    config: GossipSection,
    members: MemberList,
}

impl SwimGossip {
    pub fn new(
        local_id: NodeId,
        listen_addr: SocketAddr,
        seeds: Vec<SocketAddr>,
        config: GossipSection,
    ) -> Self {
        Self {
            local_id,
            listen_addr,
            seeds,
            config,
            members: MemberList::new(),
        }
    }

    pub fn members(&self) -> &MemberList {
        &self.members
    }

    pub async fn start(&self) -> blockyard_common::Result<()> {
        info!(
            node_id = self.local_id,
            addr = %self.listen_addr,
            seeds = ?self.seeds,
            "starting SWIM gossip protocol"
        );
        // TODO: bind UDP socket, begin probe loop, handle join
        Ok(())
    }
}
