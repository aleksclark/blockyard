use blockyard_common::types::RaftGroupId;
use std::collections::HashMap;
use tracing::info;

pub struct MultiRaft {
    groups: HashMap<RaftGroupId, ()>,
}

impl MultiRaft {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
        }
    }

    pub async fn start(&mut self) -> blockyard_common::Result<()> {
        info!("initializing Multi-Raft engine");
        Ok(())
    }
}

impl Default for MultiRaft {
    fn default() -> Self {
        Self::new()
    }
}
