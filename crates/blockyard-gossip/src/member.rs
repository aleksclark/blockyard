//! Cluster membership tracking with incarnation-based conflict resolution.
//!
//! The [`MemberList`] maintains the current view of cluster membership.
//! State transitions follow the SWIM protocol: Alive -> Suspect -> Dead.
//! Incarnation numbers prevent stale state from overriding fresh state.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;

use blockyard_common::NodeId;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::protocol::{MemberState, MembershipUpdate};

/// Information about a single cluster member.
#[derive(Debug, Clone)]
pub struct Member {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub state: MemberState,
    pub incarnation: u64,
    pub state_change: Instant,
}

impl Member {
    /// Create a new alive member.
    pub fn new(node_id: NodeId, addr: SocketAddr) -> Self {
        Self {
            node_id,
            addr,
            state: MemberState::Alive,
            incarnation: 0,
            state_change: Instant::now(),
        }
    }

    /// Create a member with a specific state and incarnation.
    pub fn with_state(
        node_id: NodeId,
        addr: SocketAddr,
        state: MemberState,
        incarnation: u64,
    ) -> Self {
        Self {
            node_id,
            addr,
            state,
            incarnation,
            state_change: Instant::now(),
        }
    }

    /// Convert to a membership update for piggybacking.
    pub fn to_update(&self) -> MembershipUpdate {
        MembershipUpdate {
            node_id: self.node_id,
            addr: self.addr,
            state: self.state,
            incarnation: self.incarnation,
        }
    }
}

/// Thread-safe membership list for the gossip protocol.
#[derive(Debug)]
pub struct MemberList {
    local_id: NodeId,
    members: RwLock<HashMap<NodeId, Member>>,
    incarnation: RwLock<u64>,
}

impl MemberList {
    /// Create a new membership list for a node.
    pub fn new(local_id: NodeId, local_addr: SocketAddr) -> Self {
        let mut members = HashMap::new();
        members.insert(local_id, Member::new(local_id, local_addr));
        Self {
            local_id,
            members: RwLock::new(members),
            incarnation: RwLock::new(0),
        }
    }

    /// Return the local node ID.
    pub fn local_id(&self) -> NodeId {
        self.local_id
    }

    /// Return the current incarnation number for the local node.
    pub fn incarnation(&self) -> u64 {
        *self.incarnation.read()
    }

    /// Increment and return the local incarnation number.
    pub fn next_incarnation(&self) -> u64 {
        let mut inc = self.incarnation.write();
        *inc += 1;
        *inc
    }

    /// Get a snapshot of a specific member.
    pub fn get(&self, node_id: &NodeId) -> Option<Member> {
        self.members.read().get(node_id).cloned()
    }

    /// Get a snapshot of all current members (excluding dead).
    pub fn alive_members(&self) -> Vec<Member> {
        self.members
            .read()
            .values()
            .filter(|m| m.state != MemberState::Dead)
            .cloned()
            .collect()
    }

    /// Get all members regardless of state.
    pub fn all_members(&self) -> Vec<Member> {
        self.members.read().values().cloned().collect()
    }

    /// Get members suitable for probing (alive or suspect, excluding self).
    pub fn probe_targets(&self) -> Vec<Member> {
        let local = self.local_id;
        self.members
            .read()
            .values()
            .filter(|m| m.node_id != local && m.state != MemberState::Dead)
            .cloned()
            .collect()
    }

    /// Get alive members excluding self and a specific target (for indirect probes).
    pub fn indirect_probe_candidates(&self, exclude: NodeId) -> Vec<Member> {
        let local = self.local_id;
        self.members
            .read()
            .values()
            .filter(|m| m.node_id != local && m.node_id != exclude && m.state == MemberState::Alive)
            .cloned()
            .collect()
    }

    /// Apply a membership update, respecting incarnation ordering.
    /// Returns `true` if the update was applied (state actually changed).
    pub fn apply_update(&self, update: &MembershipUpdate) -> bool {
        if update.node_id == self.local_id {
            return self.handle_local_update(update);
        }

        let mut members = self.members.write();
        if let Some(existing) = members.get(&update.node_id) {
            if !should_override(existing, update) {
                return false;
            }
        }

        let old_state = members.get(&update.node_id).map(|m| m.state);
        let member = Member::with_state(
            update.node_id,
            update.addr,
            update.state,
            update.incarnation,
        );
        members.insert(update.node_id, member);

        let changed = old_state != Some(update.state);
        if changed {
            match update.state {
                MemberState::Alive => {
                    info!(node = %update.node_id, addr = %update.addr, "member alive");
                }
                MemberState::Suspect => {
                    warn!(node = %update.node_id, addr = %update.addr, "member suspect");
                }
                MemberState::Dead => {
                    warn!(node = %update.node_id, addr = %update.addr, "member dead");
                }
            }
        }
        changed
    }

    /// Mark a node as suspect. Returns `true` if the state transitioned.
    pub fn mark_suspect(&self, node_id: NodeId) -> bool {
        let mut members = self.members.write();
        if let Some(member) = members.get_mut(&node_id) {
            if member.state == MemberState::Alive {
                member.state = MemberState::Suspect;
                member.state_change = Instant::now();
                warn!(node = %node_id, "marking member as suspect");
                return true;
            }
        }
        false
    }

    /// Mark a node as dead. Returns `true` if the state transitioned.
    pub fn mark_dead(&self, node_id: NodeId) -> bool {
        let mut members = self.members.write();
        if let Some(member) = members.get_mut(&node_id) {
            if member.state != MemberState::Dead {
                member.state = MemberState::Dead;
                member.state_change = Instant::now();
                warn!(node = %node_id, "marking member as dead");
                return true;
            }
        }
        false
    }

    /// Collect recent membership updates for piggybacking on messages.
    /// Returns updates for all known members.
    pub fn recent_updates(&self, max_count: usize) -> Vec<MembershipUpdate> {
        let members = self.members.read();
        let mut updates: Vec<_> = members.values().map(|m| m.to_update()).collect();
        updates.sort_by(|a, b| b.incarnation.cmp(&a.incarnation));
        updates.truncate(max_count);
        updates
    }

    /// Apply a batch of piggybacked membership updates.
    pub fn apply_updates(&self, updates: &[MembershipUpdate]) {
        for update in updates {
            self.apply_update(update);
        }
    }

    /// Return the number of members (all states).
    pub fn len(&self) -> usize {
        self.members.read().len()
    }

    /// Return whether the member list is empty.
    pub fn is_empty(&self) -> bool {
        self.members.read().is_empty()
    }

    /// Handle an update about the local node (e.g., someone marking us suspect).
    fn handle_local_update(&self, update: &MembershipUpdate) -> bool {
        match update.state {
            MemberState::Suspect | MemberState::Dead => {
                debug!(
                    state = ?update.state,
                    remote_incarnation = update.incarnation,
                    "received state claim about local node, incrementing incarnation"
                );
                self.next_incarnation();
                false
            }
            MemberState::Alive => false,
        }
    }
}

/// Determine whether an incoming update should override existing member state.
fn should_override(existing: &Member, update: &MembershipUpdate) -> bool {
    if update.incarnation > existing.incarnation {
        return true;
    }
    if update.incarnation < existing.incarnation {
        return false;
    }
    state_priority(update.state) > state_priority(existing.state)
}

/// Higher priority wins on equal incarnation: Dead > Suspect > Alive.
fn state_priority(state: MemberState) -> u8 {
    match state {
        MemberState::Alive => 0,
        MemberState::Suspect => 1,
        MemberState::Dead => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    #[test]
    fn test_member_new() {
        let id = NodeId::generate();
        let addr = make_addr(9000);
        let m = Member::new(id, addr);
        assert_eq!(m.node_id, id);
        assert_eq!(m.addr, addr);
        assert_eq!(m.state, MemberState::Alive);
        assert_eq!(m.incarnation, 0);
    }

    #[test]
    fn test_member_with_state() {
        let id = NodeId::generate();
        let addr = make_addr(9000);
        let m = Member::with_state(id, addr, MemberState::Suspect, 5);
        assert_eq!(m.state, MemberState::Suspect);
        assert_eq!(m.incarnation, 5);
    }

    #[test]
    fn test_member_to_update() {
        let id = NodeId::generate();
        let addr = make_addr(9000);
        let m = Member::with_state(id, addr, MemberState::Dead, 3);
        let upd = m.to_update();
        assert_eq!(upd.node_id, id);
        assert_eq!(upd.addr, addr);
        assert_eq!(upd.state, MemberState::Dead);
        assert_eq!(upd.incarnation, 3);
    }

    #[test]
    fn test_member_clone() {
        let m = Member::new(NodeId::generate(), make_addr(9000));
        let cloned = m.clone();
        assert_eq!(m.node_id, cloned.node_id);
        assert_eq!(m.state, cloned.state);
    }

    #[test]
    fn test_member_debug() {
        let m = Member::new(NodeId::generate(), make_addr(9000));
        let dbg = format!("{:?}", m);
        assert!(dbg.contains("Member"));
    }

    #[test]
    fn test_member_list_new() {
        let id = NodeId::generate();
        let list = MemberList::new(id, make_addr(9000));
        assert_eq!(list.local_id(), id);
        assert_eq!(list.len(), 1);
        assert!(!list.is_empty());
    }

    #[test]
    fn test_member_list_incarnation() {
        let list = MemberList::new(NodeId::generate(), make_addr(9000));
        assert_eq!(list.incarnation(), 0);
        assert_eq!(list.next_incarnation(), 1);
        assert_eq!(list.next_incarnation(), 2);
        assert_eq!(list.incarnation(), 2);
    }

    #[test]
    fn test_member_list_get() {
        let id = NodeId::generate();
        let list = MemberList::new(id, make_addr(9000));
        let m = list.get(&id).unwrap();
        assert_eq!(m.node_id, id);
        assert!(list.get(&NodeId::generate()).is_none());
    }

    #[test]
    fn test_apply_update_new_member() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));

        let remote_id = NodeId::generate();
        let update = MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 0,
        };
        assert!(list.apply_update(&update));
        assert_eq!(list.len(), 2);

        let m = list.get(&remote_id).unwrap();
        assert_eq!(m.state, MemberState::Alive);
    }

    #[test]
    fn test_apply_update_higher_incarnation_overrides() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let remote_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 1,
        });

        let changed = list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Suspect,
            incarnation: 2,
        });
        assert!(changed);
        assert_eq!(list.get(&remote_id).unwrap().state, MemberState::Suspect);
    }

    #[test]
    fn test_apply_update_lower_incarnation_rejected() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let remote_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Suspect,
            incarnation: 5,
        });

        let changed = list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 3,
        });
        assert!(!changed);
        assert_eq!(list.get(&remote_id).unwrap().state, MemberState::Suspect);
    }

    #[test]
    fn test_apply_update_same_incarnation_higher_priority_wins() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let remote_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 1,
        });

        let changed = list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Suspect,
            incarnation: 1,
        });
        assert!(changed);
        assert_eq!(list.get(&remote_id).unwrap().state, MemberState::Suspect);
    }

    #[test]
    fn test_apply_update_same_incarnation_lower_priority_rejected() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let remote_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Suspect,
            incarnation: 1,
        });

        let changed = list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 1,
        });
        assert!(!changed);
        assert_eq!(list.get(&remote_id).unwrap().state, MemberState::Suspect);
    }

    #[test]
    fn test_apply_update_local_node_suspect_increments_incarnation() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));

        let result = list.apply_update(&MembershipUpdate {
            node_id: local_id,
            addr: make_addr(9000),
            state: MemberState::Suspect,
            incarnation: 0,
        });
        assert!(!result);
        assert_eq!(list.incarnation(), 1);
    }

    #[test]
    fn test_apply_update_local_node_dead_increments_incarnation() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));

        list.apply_update(&MembershipUpdate {
            node_id: local_id,
            addr: make_addr(9000),
            state: MemberState::Dead,
            incarnation: 0,
        });
        assert_eq!(list.incarnation(), 1);
    }

    #[test]
    fn test_apply_update_local_node_alive_no_change() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));

        let result = list.apply_update(&MembershipUpdate {
            node_id: local_id,
            addr: make_addr(9000),
            state: MemberState::Alive,
            incarnation: 0,
        });
        assert!(!result);
        assert_eq!(list.incarnation(), 0);
    }

    #[test]
    fn test_mark_suspect() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let remote_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 0,
        });

        assert!(list.mark_suspect(remote_id));
        assert_eq!(list.get(&remote_id).unwrap().state, MemberState::Suspect);
    }

    #[test]
    fn test_mark_suspect_already_suspect() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let remote_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Suspect,
            incarnation: 0,
        });

        assert!(!list.mark_suspect(remote_id));
    }

    #[test]
    fn test_mark_suspect_unknown_node() {
        let list = MemberList::new(NodeId::generate(), make_addr(9000));
        assert!(!list.mark_suspect(NodeId::generate()));
    }

    #[test]
    fn test_mark_dead() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let remote_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 0,
        });

        assert!(list.mark_dead(remote_id));
        assert_eq!(list.get(&remote_id).unwrap().state, MemberState::Dead);
    }

    #[test]
    fn test_mark_dead_already_dead() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let remote_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Dead,
            incarnation: 0,
        });

        assert!(!list.mark_dead(remote_id));
    }

    #[test]
    fn test_mark_dead_unknown_node() {
        let list = MemberList::new(NodeId::generate(), make_addr(9000));
        assert!(!list.mark_dead(NodeId::generate()));
    }

    #[test]
    fn test_alive_members_excludes_dead() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let alive_id = NodeId::generate();
        let dead_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: alive_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 0,
        });
        list.apply_update(&MembershipUpdate {
            node_id: dead_id,
            addr: make_addr(9002),
            state: MemberState::Dead,
            incarnation: 0,
        });

        let alive = list.alive_members();
        assert_eq!(alive.len(), 2);
        assert!(alive.iter().all(|m| m.state != MemberState::Dead));
    }

    #[test]
    fn test_alive_members_includes_suspect() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let suspect_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: suspect_id,
            addr: make_addr(9001),
            state: MemberState::Suspect,
            incarnation: 0,
        });

        let alive = list.alive_members();
        assert_eq!(alive.len(), 2);
    }

    #[test]
    fn test_all_members() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));

        list.apply_update(&MembershipUpdate {
            node_id: NodeId::generate(),
            addr: make_addr(9001),
            state: MemberState::Dead,
            incarnation: 0,
        });

        assert_eq!(list.all_members().len(), 2);
    }

    #[test]
    fn test_probe_targets_excludes_self_and_dead() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let alive_id = NodeId::generate();
        let dead_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: alive_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 0,
        });
        list.apply_update(&MembershipUpdate {
            node_id: dead_id,
            addr: make_addr(9002),
            state: MemberState::Dead,
            incarnation: 0,
        });

        let targets = list.probe_targets();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].node_id, alive_id);
    }

    #[test]
    fn test_indirect_probe_candidates() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let a = NodeId::generate();
        let b = NodeId::generate();
        let c = NodeId::generate();

        for (id, port) in [(a, 9001), (b, 9002), (c, 9003)] {
            list.apply_update(&MembershipUpdate {
                node_id: id,
                addr: make_addr(port),
                state: MemberState::Alive,
                incarnation: 0,
            });
        }

        let candidates = list.indirect_probe_candidates(a);
        assert_eq!(candidates.len(), 2);
        assert!(
            candidates
                .iter()
                .all(|m| m.node_id != a && m.node_id != local_id)
        );
    }

    #[test]
    fn test_indirect_probe_candidates_excludes_suspect() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let a = NodeId::generate();
        let b = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: a,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 0,
        });
        list.apply_update(&MembershipUpdate {
            node_id: b,
            addr: make_addr(9002),
            state: MemberState::Suspect,
            incarnation: 0,
        });

        let candidates = list.indirect_probe_candidates(a);
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_recent_updates() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));

        for port in 9001..9004 {
            list.apply_update(&MembershipUpdate {
                node_id: NodeId::generate(),
                addr: make_addr(port),
                state: MemberState::Alive,
                incarnation: 0,
            });
        }

        let updates = list.recent_updates(10);
        assert_eq!(updates.len(), 4);

        let limited = list.recent_updates(2);
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn test_apply_updates_batch() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));

        let updates = vec![
            MembershipUpdate {
                node_id: NodeId::generate(),
                addr: make_addr(9001),
                state: MemberState::Alive,
                incarnation: 0,
            },
            MembershipUpdate {
                node_id: NodeId::generate(),
                addr: make_addr(9002),
                state: MemberState::Alive,
                incarnation: 0,
            },
        ];

        list.apply_updates(&updates);
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn test_member_list_debug() {
        let list = MemberList::new(NodeId::generate(), make_addr(9000));
        let dbg = format!("{:?}", list);
        assert!(dbg.contains("MemberList"));
    }

    #[test]
    fn test_state_priority_ordering() {
        assert!(state_priority(MemberState::Dead) > state_priority(MemberState::Suspect));
        assert!(state_priority(MemberState::Suspect) > state_priority(MemberState::Alive));
    }

    #[test]
    fn test_should_override_higher_incarnation() {
        let m = Member::with_state(NodeId::generate(), make_addr(9000), MemberState::Alive, 1);
        let upd = MembershipUpdate {
            node_id: m.node_id,
            addr: m.addr,
            state: MemberState::Alive,
            incarnation: 2,
        };
        assert!(should_override(&m, &upd));
    }

    #[test]
    fn test_should_override_lower_incarnation() {
        let m = Member::with_state(NodeId::generate(), make_addr(9000), MemberState::Alive, 5);
        let upd = MembershipUpdate {
            node_id: m.node_id,
            addr: m.addr,
            state: MemberState::Suspect,
            incarnation: 3,
        };
        assert!(!should_override(&m, &upd));
    }

    #[test]
    fn test_should_override_same_incarnation_higher_state() {
        let m = Member::with_state(NodeId::generate(), make_addr(9000), MemberState::Alive, 1);
        let upd = MembershipUpdate {
            node_id: m.node_id,
            addr: m.addr,
            state: MemberState::Dead,
            incarnation: 1,
        };
        assert!(should_override(&m, &upd));
    }

    #[test]
    fn test_should_override_same_incarnation_lower_state() {
        let m = Member::with_state(NodeId::generate(), make_addr(9000), MemberState::Dead, 1);
        let upd = MembershipUpdate {
            node_id: m.node_id,
            addr: m.addr,
            state: MemberState::Alive,
            incarnation: 1,
        };
        assert!(!should_override(&m, &upd));
    }

    #[test]
    fn test_apply_update_no_state_change_returns_false() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let remote_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 1,
        });

        let changed = list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 2,
        });
        assert!(!changed);
    }

    #[test]
    fn test_mark_dead_from_suspect() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let remote_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Suspect,
            incarnation: 0,
        });

        assert!(list.mark_dead(remote_id));
        assert_eq!(list.get(&remote_id).unwrap().state, MemberState::Dead);
    }

    #[test]
    fn test_is_empty_false() {
        let list = MemberList::new(NodeId::generate(), make_addr(9000));
        assert!(!list.is_empty());
    }

    #[test]
    fn test_same_incarnation_same_state_no_override() {
        let m = Member::with_state(NodeId::generate(), make_addr(9000), MemberState::Suspect, 3);
        let upd = MembershipUpdate {
            node_id: m.node_id,
            addr: m.addr,
            state: MemberState::Suspect,
            incarnation: 3,
        };
        assert!(!should_override(&m, &upd));
    }

    #[test]
    fn test_dead_to_alive_higher_incarnation() {
        let local_id = NodeId::generate();
        let list = MemberList::new(local_id, make_addr(9000));
        let remote_id = NodeId::generate();

        list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Dead,
            incarnation: 1,
        });

        let changed = list.apply_update(&MembershipUpdate {
            node_id: remote_id,
            addr: make_addr(9001),
            state: MemberState::Alive,
            incarnation: 5,
        });
        assert!(changed);
        assert_eq!(list.get(&remote_id).unwrap().state, MemberState::Alive);
    }
}
