//! In-memory Raft log storage and state machine store for metadata consensus.
//!
//! Implements `RaftLogStorage` and `RaftStateMachine` (openraft storage-v2)
//! backed by in-memory data structures. Production deployments will replace
//! this with a persistent (RocksDB) implementation; the state machine logic
//! in [`MetadataStateMachineData`] remains the same.
//!
//! Crash recovery (P3.7) is handled by Raft itself: on restart the committed
//! log is replayed in order through [`RaftStateMachine::apply`], restoring
//! the state machine to its committed state. The snapshot mechanism provides
//! compaction so replay doesn't start from genesis.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::{
    Entry, EntryPayload, LogId, LogState, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use parking_lot::RwLock;

use crate::response::MetadataResponse;
use crate::state_machine::MetadataStateMachineData;
use crate::typ::TypeConfig;

/// In-memory Raft log store.
#[derive(Debug)]
pub struct LogStore {
    inner: Arc<RwLock<LogStoreInner>>,
}

#[derive(Debug)]
struct LogStoreInner {
    vote: Option<Vote<u64>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    last_purged: Option<LogId<u64>>,
}

impl LogStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(LogStoreInner {
                vote: None,
                log: BTreeMap::new(),
                last_purged: None,
            })),
        }
    }
}

impl Default for LogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl LogStore {
    /// Insert entries directly for testing (bypasses the `LogFlushed` callback).
    #[cfg(test)]
    fn insert_entries(&self, entries: Vec<Entry<TypeConfig>>) {
        let mut inner = self.inner.write();
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
    }
}

impl Clone for LogStore {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let inner = self.inner.read();
        let entries: Vec<_> = inner.log.range(range).map(|(_, v)| v.clone()).collect();
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.inner.read();
        let last_log_id = inner.log.iter().next_back().map(|(_, e)| e.log_id);
        let last_purged = inner.last_purged;
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write();
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let inner = self.inner.read();
        Ok(inner.vote)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.write();
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write();
        let to_remove: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in to_remove {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut inner = self.inner.write();
        let to_remove: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in to_remove {
            inner.log.remove(&k);
        }
        inner.last_purged = Some(log_id);
        Ok(())
    }
}

/// In-memory state machine store wrapping [`MetadataStateMachineData`].
#[derive(Debug)]
pub struct StateMachineStore {
    data: Arc<RwLock<MetadataStateMachineData>>,
    snapshot_idx: Arc<std::sync::atomic::AtomicU64>,
    current_snapshot: Arc<RwLock<Option<StoredSnapshot>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, openraft::BasicNode>,
    data: Vec<u8>,
}

use serde::{Deserialize, Serialize};

impl StateMachineStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(MetadataStateMachineData::new())),
            snapshot_idx: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            current_snapshot: Arc::new(RwLock::new(None)),
        }
    }

    /// Get a read-only reference to the state machine data for queries.
    pub fn data(&self) -> parking_lot::RwLockReadGuard<'_, MetadataStateMachineData> {
        self.data.read()
    }
}

impl Default for StateMachineStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for StateMachineStore {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
            snapshot_idx: Arc::clone(&self.snapshot_idx),
            current_snapshot: Arc::clone(&self.current_snapshot),
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let (last_applied, last_membership, sm_json) = {
            let data = self.data.read();
            let json = serde_json::to_vec(&*data).map_err(|e| StorageError::IO {
                source: StorageIOError::read_state_machine(&e),
            })?;
            (data.last_applied, data.last_membership.clone(), json)
        };

        self.snapshot_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let idx = self.snapshot_idx.load(std::sync::atomic::Ordering::Relaxed);
        let snapshot_id = if let Some(last) = last_applied {
            format!("{}-{}-{}", last.leader_id, last.index, idx)
        } else {
            format!("--{}", idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership,
            snapshot_id,
        };

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: sm_json.clone(),
        };
        {
            let mut snap = self.current_snapshot.write();
            *snap = Some(stored);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(sm_json)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        let data = self.data.read();
        Ok((data.last_applied, data.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<MetadataResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();
        let mut data = self.data.write();

        for entry in entries {
            data.last_applied = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(MetadataResponse::ok());
                }
                EntryPayload::Normal(req) => {
                    let resp = data.apply_request(&req);
                    responses.push(resp);
                }
                EntryPayload::Membership(mem) => {
                    data.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    responses.push(MetadataResponse::ok());
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let raw = snapshot.into_inner();

        let new_data: MetadataStateMachineData =
            serde_json::from_slice(&raw).map_err(|e| StorageError::IO {
                source: StorageIOError::read_state_machine(&e),
            })?;

        {
            let mut data = self.data.write();
            *data = new_data;
            data.last_applied = meta.last_log_id;
            data.last_membership = meta.last_membership.clone();
        }

        {
            let mut snap = self.current_snapshot.write();
            *snap = Some(StoredSnapshot {
                meta: meta.clone(),
                data: raw,
            });
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let snap = self.current_snapshot.read();
        match &*snap {
            Some(stored) => Ok(Some(Snapshot {
                meta: stored.meta.clone(),
                snapshot: Box::new(Cursor::new(stored.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::MetadataRequest;
    use blockyard_common::{EpochId, ExtentId, OperationId, ProtectionPolicy, VolumeId};

    fn make_entry(index: u64, term: u64, payload: EntryPayload<TypeConfig>) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(term, 1), index),
            payload,
        }
    }

    fn make_blank(index: u64, term: u64) -> Entry<TypeConfig> {
        make_entry(index, term, EntryPayload::Blank)
    }

    fn make_normal(index: u64, term: u64, req: MetadataRequest) -> Entry<TypeConfig> {
        make_entry(index, term, EntryPayload::Normal(req))
    }

    #[tokio::test]
    async fn test_log_store_append_and_read() {
        let mut store = LogStore::new();
        let entries = vec![make_blank(1, 1), make_blank(2, 1)];
        store.insert_entries(entries);

        let read = store.try_get_log_entries(1..=2).await.unwrap();
        assert_eq!(read.len(), 2);
    }

    #[tokio::test]
    async fn test_log_store_state() {
        let mut store = LogStore::new();
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());

        store.insert_entries(vec![make_blank(1, 1)]);

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 1);
    }

    #[tokio::test]
    async fn test_log_store_vote() {
        let mut store = LogStore::new();
        assert!(store.read_vote().await.unwrap().is_none());

        let vote = Vote::new(1, 1);
        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn test_log_store_truncate() {
        let mut store = LogStore::new();
        let entries = vec![make_blank(1, 1), make_blank(2, 1), make_blank(3, 1)];
        store.insert_entries(entries);

        let lid = LogId::new(openraft::CommittedLeaderId::new(1, 1), 2);
        store.truncate(lid).await.unwrap();

        let read = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn test_log_store_purge() {
        let mut store = LogStore::new();
        let entries = vec![make_blank(1, 1), make_blank(2, 1), make_blank(3, 1)];
        store.insert_entries(entries);

        let lid = LogId::new(openraft::CommittedLeaderId::new(1, 1), 2);
        store.purge(lid).await.unwrap();

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);

        let read = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].log_id.index, 3);
    }

    #[tokio::test]
    async fn test_log_store_get_reader() {
        let mut store = LogStore::new();
        store.insert_entries(vec![make_blank(1, 1)]);

        let mut reader = store.get_log_reader().await;
        let entries = reader.try_get_log_entries(1..=1).await.unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn test_state_machine_apply_blank() {
        let mut sm = StateMachineStore::new();
        let entries = vec![make_blank(1, 1)];
        let resps = sm.apply(entries).await.unwrap();
        assert_eq!(resps.len(), 1);
        assert!(!resps[0].is_error());

        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 1);
    }

    #[tokio::test]
    async fn test_state_machine_apply_create_volume() {
        let mut sm = StateMachineStore::new();
        let vid = VolumeId::generate();
        let entry = make_normal(
            1,
            1,
            MetadataRequest::CreateVolume {
                volume_id: vid,
                size_bytes: 1024,
                protection: ProtectionPolicy::Replicated { replicas: 3 },
            },
        );

        let resps = sm.apply(vec![entry]).await.unwrap();
        assert!(!resps[0].is_error());

        let data = sm.data();
        assert!(data.get_volume(&vid).is_some());
    }

    #[tokio::test]
    async fn test_state_machine_apply_multiple() {
        let mut sm = StateMachineStore::new();
        let vid = VolumeId::generate();
        let eid = ExtentId::generate();

        let entries = vec![
            make_normal(
                1,
                1,
                MetadataRequest::CreateVolume {
                    volume_id: vid,
                    size_bytes: 1024,
                    protection: ProtectionPolicy::Replicated { replicas: 1 },
                },
            ),
            make_normal(
                2,
                1,
                MetadataRequest::CommitExtentMapping {
                    volume_id: vid,
                    block_range: 0..64,
                    extent_id: eid,
                    extent_version: 1,
                    epoch: EpochId::new(0),
                    replica_locations: vec![],
                    checksums: vec![],
                    operation_id: None,
                    previous_version: None,
                },
            ),
        ];

        let resps = sm.apply(entries).await.unwrap();
        assert_eq!(resps.len(), 2);

        let data = sm.data();
        assert!(data.get_volume_mappings(&vid).unwrap().contains_key(&0));
    }

    #[tokio::test]
    async fn test_state_machine_snapshot_roundtrip() {
        let mut sm = StateMachineStore::new();
        let vid = VolumeId::generate();
        let entry = make_normal(
            1,
            1,
            MetadataRequest::CreateVolume {
                volume_id: vid,
                size_bytes: 4096,
                protection: ProtectionPolicy::Replicated { replicas: 3 },
            },
        );
        sm.apply(vec![entry]).await.unwrap();

        let snap = sm.build_snapshot().await.unwrap();
        assert!(snap.meta.last_log_id.is_some());

        let mut sm2 = StateMachineStore::new();
        let raw = snap.snapshot.into_inner();
        let snap_data = Box::new(Cursor::new(raw));
        sm2.install_snapshot(&snap.meta, snap_data).await.unwrap();

        let data = sm2.data();
        assert!(data.get_volume(&vid).is_some());
    }

    #[tokio::test]
    async fn test_state_machine_get_snapshot() {
        let mut sm = StateMachineStore::new();
        assert!(sm.get_current_snapshot().await.unwrap().is_none());

        sm.apply(vec![make_blank(1, 1)]).await.unwrap();
        sm.build_snapshot().await.unwrap();

        let snap = sm.get_current_snapshot().await.unwrap();
        assert!(snap.is_some());
    }

    #[tokio::test]
    async fn test_state_machine_begin_receiving() {
        let mut sm = StateMachineStore::new();
        let buf = sm.begin_receiving_snapshot().await.unwrap();
        assert_eq!(buf.into_inner().len(), 0);
    }

    #[tokio::test]
    async fn test_state_machine_membership_entry() {
        let mut sm = StateMachineStore::new();
        let mut voter_set = std::collections::BTreeSet::new();
        voter_set.insert(1u64);
        let mut node_map = BTreeMap::new();
        node_map.insert(1u64, openraft::BasicNode::default());
        let members = openraft::Membership::new(vec![voter_set], node_map);

        let entry = make_entry(1, 1, EntryPayload::Membership(members));
        let resps = sm.apply(vec![entry]).await.unwrap();
        assert!(!resps[0].is_error());
    }

    #[tokio::test]
    async fn test_state_machine_query_data() {
        let mut sm = StateMachineStore::new();
        let vid = VolumeId::generate();
        let eid = ExtentId::generate();
        let op_id = OperationId::generate();

        let entries = vec![
            make_normal(
                1,
                1,
                MetadataRequest::CreateVolume {
                    volume_id: vid,
                    size_bytes: 1024,
                    protection: ProtectionPolicy::Replicated { replicas: 1 },
                },
            ),
            make_normal(
                2,
                1,
                MetadataRequest::CommitExtentMapping {
                    volume_id: vid,
                    block_range: 0..64,
                    extent_id: eid,
                    extent_version: 7,
                    epoch: EpochId::new(0),
                    replica_locations: vec![],
                    checksums: vec![],
                    operation_id: Some(op_id),
                    previous_version: None,
                },
            ),
        ];
        sm.apply(entries).await.unwrap();

        let data = sm.data();
        assert!(data.lookup_by_operation_id(&op_id).is_some());
        assert!(data.lookup_by_extent_version(7).is_some());
    }

    #[tokio::test]
    async fn test_state_machine_snapshot_builder() {
        let mut sm = StateMachineStore::new();
        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        assert!(snap.meta.last_log_id.is_none());
    }

    #[tokio::test]
    async fn test_log_store_empty_read() {
        let mut store = LogStore::new();
        let read = store.try_get_log_entries(1..=10).await.unwrap();
        assert!(read.is_empty());
    }
}
