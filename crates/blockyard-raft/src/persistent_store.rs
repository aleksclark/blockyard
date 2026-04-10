//! Persistent Raft log storage and state machine store backed by redb.
//!
//! Provides durable storage for Raft log entries, vote, and state machine data
//! so that node restarts do not lose committed state. The in-memory stores in
//! [`crate::store`] remain available for fast unit tests.

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::{
    Entry, EntryPayload, LogId, LogState, OptionalSend, RaftLogReader, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use parking_lot::RwLock;
use redb::{Database, ReadableTable, TableDefinition};

use crate::response::MetadataResponse;
use crate::state_machine::MetadataStateMachineData;
use crate::typ::TypeConfig;

const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("log");
const VOTE_TABLE: TableDefinition<(), &[u8]> = TableDefinition::new("vote");
const PURGE_TABLE: TableDefinition<(), &[u8]> = TableDefinition::new("purge");
const STATE_MACHINE_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("state_machine");
const SNAPSHOT_TABLE: TableDefinition<(), &[u8]> = TableDefinition::new("snapshot");

fn io_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::write(&e),
    }
}

fn read_io_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::read(&e),
    }
}

/// Persistent Raft log store backed by redb.
#[derive(Debug)]
pub struct PersistentLogStore {
    db: Arc<Database>,
}

impl PersistentLogStore {
    /// Open or create a persistent log store at the given path.
    pub fn new(path: &Path) -> Result<Self, redb::Error> {
        let db = Database::create(path)?;
        {
            let txn = db.begin_write()?;
            let _ = txn.open_table(LOG_TABLE)?;
            let _ = txn.open_table(VOTE_TABLE)?;
            let _ = txn.open_table(PURGE_TABLE)?;
            txn.commit()?;
        }
        Ok(Self { db: Arc::new(db) })
    }
}

impl PersistentLogStore {
    #[cfg(test)]
    fn insert_entries(&self, entries: Vec<Entry<TypeConfig>>) {
        let txn = self.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(LOG_TABLE).unwrap();
            for entry in entries {
                let data = serde_json::to_vec(&entry).unwrap();
                table.insert(entry.log_id.index, data.as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
    }
}

impl Clone for PersistentLogStore {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}

impl RaftLogReader<TypeConfig> for PersistentLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let txn = self.db.begin_read().map_err(read_io_err)?;
        let table = txn.open_table(LOG_TABLE).map_err(read_io_err)?;
        let mut entries = Vec::new();
        for item in table.range(range).map_err(read_io_err)? {
            let (_, v) = item.map_err(read_io_err)?;
            let entry: Entry<TypeConfig> =
                serde_json::from_slice(v.value()).map_err(read_io_err)?;
            entries.push(entry);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for PersistentLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let txn = self.db.begin_read().map_err(read_io_err)?;

        let log_table = txn.open_table(LOG_TABLE).map_err(read_io_err)?;
        let last_log_id = match log_table.last().map_err(read_io_err)? {
            Some((_, v)) => {
                let entry: Entry<TypeConfig> =
                    serde_json::from_slice(v.value()).map_err(read_io_err)?;
                Some(entry.log_id)
            }
            None => None,
        };

        let purge_table = txn.open_table(PURGE_TABLE).map_err(read_io_err)?;
        let last_purged = match purge_table.get(()).map_err(read_io_err)? {
            Some(v) => {
                let lid: LogId<u64> =
                    serde_json::from_slice(v.value()).map_err(read_io_err)?;
                Some(lid)
            }
            None => None,
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let data = serde_json::to_vec(vote).map_err(io_err)?;
        let txn = self.db.begin_write().map_err(io_err)?;
        {
            let mut table = txn.open_table(VOTE_TABLE).map_err(io_err)?;
            table.insert((), data.as_slice()).map_err(io_err)?;
        }
        txn.commit().map_err(io_err)?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let txn = self.db.begin_read().map_err(read_io_err)?;
        let table = txn.open_table(VOTE_TABLE).map_err(read_io_err)?;
        match table.get(()).map_err(read_io_err)? {
            Some(v) => {
                let vote: Vote<u64> =
                    serde_json::from_slice(v.value()).map_err(read_io_err)?;
                Ok(Some(vote))
            }
            None => Ok(None),
        }
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
        let txn = self.db.begin_write().map_err(io_err)?;
        {
            let mut table = txn.open_table(LOG_TABLE).map_err(io_err)?;
            for entry in entries {
                let data = serde_json::to_vec(&entry).map_err(io_err)?;
                table.insert(entry.log_id.index, data.as_slice()).map_err(io_err)?;
            }
        }
        txn.commit().map_err(io_err)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let txn = self.db.begin_write().map_err(io_err)?;
        {
            let mut table = txn.open_table(LOG_TABLE).map_err(io_err)?;
            let to_remove: Vec<u64> = table
                .range(log_id.index..)
                .map_err(io_err)?
                .map(|item| item.map(|(k, _)| k.value()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(io_err)?;
            for k in to_remove {
                table.remove(k).map_err(io_err)?;
            }
        }
        txn.commit().map_err(io_err)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let txn = self.db.begin_write().map_err(io_err)?;
        {
            let mut log_table = txn.open_table(LOG_TABLE).map_err(io_err)?;
            let to_remove: Vec<u64> = log_table
                .range(..=log_id.index)
                .map_err(io_err)?
                .map(|item| item.map(|(k, _)| k.value()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(io_err)?;
            for k in to_remove {
                log_table.remove(k).map_err(io_err)?;
            }
            let mut purge_table = txn.open_table(PURGE_TABLE).map_err(io_err)?;
            let data = serde_json::to_vec(&log_id).map_err(io_err)?;
            purge_table.insert((), data.as_slice()).map_err(io_err)?;
        }
        txn.commit().map_err(io_err)?;
        Ok(())
    }
}

/// Persistent state machine store backed by redb.
#[derive(Debug)]
pub struct PersistentStateMachineStore {
    db: Arc<Database>,
    data: Arc<RwLock<MetadataStateMachineData>>,
    snapshot_idx: Arc<std::sync::atomic::AtomicU64>,
}

impl PersistentStateMachineStore {
    /// Open or create a persistent state machine store at the given path.
    ///
    /// Loads previously persisted state from redb on startup, restoring
    /// last_applied, membership, and the full MetadataStateMachineData.
    pub fn new(path: &Path) -> Result<Self, redb::Error> {
        let db = Database::create(path)?;
        {
            let txn = db.begin_write()?;
            let _ = txn.open_table(STATE_MACHINE_TABLE)?;
            let _ = txn.open_table(SNAPSHOT_TABLE)?;
            txn.commit()?;
        }

        let mut sm_data = MetadataStateMachineData::new();

        {
            let txn = db.begin_read()?;
            let table = txn.open_table(STATE_MACHINE_TABLE)?;

            if let Some(v) = table.get("data")? {
                if let Ok(restored) = serde_json::from_slice::<MetadataStateMachineData>(v.value())
                {
                    sm_data = restored;
                }
            }

            if let Some(v) = table.get("last_applied")? {
                if let Ok(lid) = serde_json::from_slice::<Option<LogId<u64>>>(v.value()) {
                    sm_data.last_applied = lid;
                }
            }

            if let Some(v) = table.get("membership")? {
                if let Ok(mem) =
                    serde_json::from_slice::<StoredMembership<u64, openraft::BasicNode>>(v.value())
                {
                    sm_data.last_membership = mem;
                }
            }
        }

        Ok(Self {
            db: Arc::new(db),
            data: Arc::new(RwLock::new(sm_data)),
            snapshot_idx: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// Get a read-only reference to the state machine data for queries.
    pub fn data(&self) -> parking_lot::RwLockReadGuard<'_, MetadataStateMachineData> {
        self.data.read()
    }

    fn persist_state(&self) -> Result<(), StorageError<u64>> {
        let data = self.data.read();
        let sm_json = serde_json::to_vec(&*data).map_err(io_err)?;
        let la_json = serde_json::to_vec(&data.last_applied).map_err(io_err)?;
        let mem_json = serde_json::to_vec(&data.last_membership).map_err(io_err)?;
        drop(data);

        let txn = self.db.begin_write().map_err(io_err)?;
        {
            let mut table = txn.open_table(STATE_MACHINE_TABLE).map_err(io_err)?;
            table.insert("data", sm_json.as_slice()).map_err(io_err)?;
            table
                .insert("last_applied", la_json.as_slice())
                .map_err(io_err)?;
            table
                .insert("membership", mem_json.as_slice())
                .map_err(io_err)?;
        }
        txn.commit().map_err(io_err)?;
        Ok(())
    }
}

impl Clone for PersistentStateMachineStore {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            data: Arc::clone(&self.data),
            snapshot_idx: Arc::clone(&self.snapshot_idx),
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for PersistentStateMachineStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let (last_applied, last_membership, sm_json) = {
            let data = self.data.read();
            let json = serde_json::to_vec(&*data).map_err(|e| StorageError::IO {
                source: StorageIOError::read_state_machine(&e),
            })?;
            (data.last_applied, data.last_membership.clone(), json)
        };

        self.snapshot_idx
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let idx = self
            .snapshot_idx
            .load(std::sync::atomic::Ordering::Relaxed);
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

        let txn = self.db.begin_write().map_err(io_err)?;
        {
            let mut table = txn.open_table(SNAPSHOT_TABLE).map_err(io_err)?;
            let snap_data = serde_json::to_vec(&StoredSnapshotData {
                meta: meta.clone(),
                data: sm_json.clone(),
            })
            .map_err(io_err)?;
            table
                .insert((), snap_data.as_slice())
                .map_err(io_err)?;
        }
        txn.commit().map_err(io_err)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(sm_json)),
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredSnapshotData {
    meta: SnapshotMeta<u64, openraft::BasicNode>,
    data: Vec<u8>,
}

impl RaftStateMachine<TypeConfig> for PersistentStateMachineStore {
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
        drop(data);

        self.persist_state()?;

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

        self.persist_state()?;

        let txn = self.db.begin_write().map_err(io_err)?;
        {
            let mut table = txn.open_table(SNAPSHOT_TABLE).map_err(io_err)?;
            let snap_data = serde_json::to_vec(&StoredSnapshotData {
                meta: meta.clone(),
                data: raw,
            })
            .map_err(io_err)?;
            table
                .insert((), snap_data.as_slice())
                .map_err(io_err)?;
        }
        txn.commit().map_err(io_err)?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let txn = self.db.begin_read().map_err(read_io_err)?;
        let table = txn.open_table(SNAPSHOT_TABLE).map_err(read_io_err)?;
        match table.get(()).map_err(read_io_err)? {
            Some(v) => {
                let stored: StoredSnapshotData =
                    serde_json::from_slice(v.value()).map_err(read_io_err)?;
                Ok(Some(Snapshot {
                    meta: stored.meta,
                    snapshot: Box::new(Cursor::new(stored.data)),
                }))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::MetadataRequest;
    use blockyard_common::{EpochId, ExtentId, ProtectionPolicy, VolumeId};

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

    fn temp_db_path(name: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        (dir, path)
    }

    // --- PersistentLogStore tests ---

    #[tokio::test]
    async fn test_persistent_log_append_and_read() {
        let (_dir, path) = temp_db_path("log.redb");
        let mut store = PersistentLogStore::new(&path).unwrap();

        let entries = vec![make_blank(1, 1), make_blank(2, 1)];
        store.insert_entries(entries);

        let read = store.try_get_log_entries(1..=2).await.unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].log_id.index, 1);
        assert_eq!(read[1].log_id.index, 2);
    }

    #[tokio::test]
    async fn test_persistent_log_state_empty() {
        let (_dir, path) = temp_db_path("log.redb");
        let mut store = PersistentLogStore::new(&path).unwrap();

        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn test_persistent_log_vote_roundtrip() {
        let (_dir, path) = temp_db_path("log.redb");
        let mut store = PersistentLogStore::new(&path).unwrap();

        assert!(store.read_vote().await.unwrap().is_none());

        let vote = Vote::new(1, 1);
        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn test_persistent_log_vote_survives_reopen() {
        let (_dir, path) = temp_db_path("log.redb");

        let vote = Vote::new(2, 3);
        {
            let mut store = PersistentLogStore::new(&path).unwrap();
            store.save_vote(&vote).await.unwrap();
        }

        let mut store2 = PersistentLogStore::new(&path).unwrap();
        assert_eq!(store2.read_vote().await.unwrap(), Some(vote));
    }

    #[tokio::test]
    async fn test_persistent_log_entries_survive_reopen() {
        let (_dir, path) = temp_db_path("log.redb");

        {
            let store = PersistentLogStore::new(&path).unwrap();
            let entries = vec![make_blank(1, 1), make_blank(2, 1), make_blank(3, 1)];
            store.insert_entries(entries);
        }

        let mut store2 = PersistentLogStore::new(&path).unwrap();
        let read = store2.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 3);

        let state = store2.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 3);
    }

    #[tokio::test]
    async fn test_persistent_log_truncate() {
        let (_dir, path) = temp_db_path("log.redb");
        let mut store = PersistentLogStore::new(&path).unwrap();

        let entries = vec![make_blank(1, 1), make_blank(2, 1), make_blank(3, 1)];
        store.insert_entries(entries);

        let lid = LogId::new(openraft::CommittedLeaderId::new(1, 1), 2);
        store.truncate(lid).await.unwrap();

        let read = store.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn test_persistent_log_purge() {
        let (_dir, path) = temp_db_path("log.redb");
        let mut store = PersistentLogStore::new(&path).unwrap();

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
    async fn test_persistent_log_purge_survives_reopen() {
        let (_dir, path) = temp_db_path("log.redb");

        {
            let mut store = PersistentLogStore::new(&path).unwrap();
            let entries = vec![make_blank(1, 1), make_blank(2, 1), make_blank(3, 1)];
            store.insert_entries(entries);
            let lid = LogId::new(openraft::CommittedLeaderId::new(1, 1), 1);
            store.purge(lid).await.unwrap();
        }

        let mut store2 = PersistentLogStore::new(&path).unwrap();
        let state = store2.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 1);

        let read = store2.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(read.len(), 2);
    }

    #[tokio::test]
    async fn test_persistent_log_get_reader() {
        let (_dir, path) = temp_db_path("log.redb");
        let mut store = PersistentLogStore::new(&path).unwrap();

        let entries = vec![make_blank(1, 1)];
        store.insert_entries(entries);

        let mut reader = store.get_log_reader().await;
        let entries = reader.try_get_log_entries(1..=1).await.unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn test_persistent_log_empty_read() {
        let (_dir, path) = temp_db_path("log.redb");
        let mut store = PersistentLogStore::new(&path).unwrap();
        let read = store.try_get_log_entries(1..=10).await.unwrap();
        assert!(read.is_empty());
    }

    // --- PersistentStateMachineStore tests ---

    #[tokio::test]
    async fn test_persistent_sm_apply_blank() {
        let (_dir, path) = temp_db_path("sm.redb");
        let mut sm = PersistentStateMachineStore::new(&path).unwrap();

        let entries = vec![make_blank(1, 1)];
        let resps = sm.apply(entries).await.unwrap();
        assert_eq!(resps.len(), 1);
        assert!(!resps[0].is_error());

        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 1);
    }

    #[tokio::test]
    async fn test_persistent_sm_apply_create_volume() {
        let (_dir, path) = temp_db_path("sm.redb");
        let mut sm = PersistentStateMachineStore::new(&path).unwrap();

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
        assert!(sm.data().get_volume(&vid).is_some());
    }

    #[tokio::test]
    async fn test_persistent_sm_state_survives_reopen() {
        let (_dir, path) = temp_db_path("sm.redb");
        let vid = VolumeId::generate();

        {
            let mut sm = PersistentStateMachineStore::new(&path).unwrap();
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
        }

        let mut sm2 = PersistentStateMachineStore::new(&path).unwrap();
        assert!(sm2.data().get_volume(&vid).is_some());
        let (last, _) = sm2.applied_state().await.unwrap();
        assert_eq!(last.unwrap().index, 1);
    }

    #[tokio::test]
    async fn test_persistent_sm_snapshot_roundtrip() {
        let (_dir, path) = temp_db_path("sm.redb");
        let mut sm = PersistentStateMachineStore::new(&path).unwrap();

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

        let (_dir2, path2) = temp_db_path("sm2.redb");
        let mut sm2 = PersistentStateMachineStore::new(&path2).unwrap();
        let raw = snap.snapshot.into_inner();
        let snap_data = Box::new(Cursor::new(raw));
        sm2.install_snapshot(&snap.meta, snap_data).await.unwrap();

        assert!(sm2.data().get_volume(&vid).is_some());
    }

    #[tokio::test]
    async fn test_persistent_sm_snapshot_survives_reopen() {
        let (_dir, path) = temp_db_path("sm.redb");
        let vid = VolumeId::generate();

        {
            let mut sm = PersistentStateMachineStore::new(&path).unwrap();
            let entry = make_normal(
                1,
                1,
                MetadataRequest::CreateVolume {
                    volume_id: vid,
                    size_bytes: 1024,
                    protection: ProtectionPolicy::Replicated { replicas: 1 },
                },
            );
            sm.apply(vec![entry]).await.unwrap();
            sm.build_snapshot().await.unwrap();
        }

        let mut sm2 = PersistentStateMachineStore::new(&path).unwrap();
        let snap = sm2.get_current_snapshot().await.unwrap();
        assert!(snap.is_some());
        let snap = snap.unwrap();
        assert!(snap.meta.last_log_id.is_some());
    }

    #[tokio::test]
    async fn test_persistent_sm_membership_entry() {
        let (_dir, path) = temp_db_path("sm.redb");
        let mut sm = PersistentStateMachineStore::new(&path).unwrap();

        let mut voter_set = std::collections::BTreeSet::new();
        voter_set.insert(1u64);
        let mut node_map = std::collections::BTreeMap::new();
        node_map.insert(1u64, openraft::BasicNode::default());
        let members = openraft::Membership::new(vec![voter_set], node_map);

        let entry = make_entry(1, 1, EntryPayload::Membership(members));
        let resps = sm.apply(vec![entry]).await.unwrap();
        assert!(!resps[0].is_error());
    }

    #[tokio::test]
    async fn test_persistent_sm_begin_receiving() {
        let (_dir, path) = temp_db_path("sm.redb");
        let mut sm = PersistentStateMachineStore::new(&path).unwrap();
        let buf = sm.begin_receiving_snapshot().await.unwrap();
        assert_eq!(buf.into_inner().len(), 0);
    }

    #[tokio::test]
    async fn test_persistent_sm_get_snapshot_empty() {
        let (_dir, path) = temp_db_path("sm.redb");
        let mut sm = PersistentStateMachineStore::new(&path).unwrap();
        assert!(sm.get_current_snapshot().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_persistent_sm_multiple_entries() {
        let (_dir, path) = temp_db_path("sm.redb");
        let mut sm = PersistentStateMachineStore::new(&path).unwrap();

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
        assert!(sm.data().get_volume_mappings(&vid).unwrap().contains_key(&0));
    }

    #[tokio::test]
    async fn test_persistent_sm_concurrent_read_write() {
        let (_dir, path) = temp_db_path("sm.redb");
        let sm = PersistentStateMachineStore::new(&path).unwrap();

        let sm_clone = sm.clone();
        let vid = VolumeId::generate();

        let writer = {
            let mut sm_w = sm_clone.clone();
            let vid = vid;
            tokio::spawn(async move {
                let entry = make_normal(
                    1,
                    1,
                    MetadataRequest::CreateVolume {
                        volume_id: vid,
                        size_bytes: 1024,
                        protection: ProtectionPolicy::Replicated { replicas: 1 },
                    },
                );
                sm_w.apply(vec![entry]).await.unwrap();
            })
        };

        writer.await.unwrap();

        let data = sm.data();
        assert!(data.get_volume(&vid).is_some());
    }

    #[tokio::test]
    async fn test_persistent_sm_install_snapshot_across_reopen() {
        let (_dir, path) = temp_db_path("sm.redb");
        let vid = VolumeId::generate();

        let snap_meta;
        let snap_raw;

        {
            let mut sm = PersistentStateMachineStore::new(&path).unwrap();
            let entry = make_normal(
                1,
                1,
                MetadataRequest::CreateVolume {
                    volume_id: vid,
                    size_bytes: 2048,
                    protection: ProtectionPolicy::Replicated { replicas: 2 },
                },
            );
            sm.apply(vec![entry]).await.unwrap();
            let snap = sm.build_snapshot().await.unwrap();
            snap_meta = snap.meta;
            snap_raw = snap.snapshot.into_inner();
        }

        let (_dir2, path2) = temp_db_path("sm2.redb");
        {
            let mut sm2 = PersistentStateMachineStore::new(&path2).unwrap();
            sm2.install_snapshot(&snap_meta, Box::new(Cursor::new(snap_raw)))
                .await
                .unwrap();
        }

        let sm3 = PersistentStateMachineStore::new(&path2).unwrap();
        assert!(sm3.data().get_volume(&vid).is_some());
    }
}
