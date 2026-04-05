use crate::types::{RaftRequest, RaftResponse, TypeConfig};
use openraft::storage::RaftLogStorage;
use openraft::{Entry, LogId, LogState, StorageError, Vote};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MemLogStore {
    inner: Arc<Mutex<MemLogStoreInner>>,
}

#[derive(Debug)]
struct MemLogStoreInner {
    log: BTreeMap<u64, Entry<TypeConfig>>,
    vote: Option<Vote<TypeConfig>>,
    committed: Option<LogId<TypeConfig>>,
    purged: Option<LogId<TypeConfig>>,
}

impl MemLogStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemLogStoreInner {
                log: BTreeMap::new(),
                vote: None,
                committed: None,
                purged: None,
            })),
        }
    }

    pub fn log_len(&self) -> usize {
        self.inner.lock().log.len()
    }
}

impl Default for MemLogStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftLogStorage<TypeConfig> for MemLogStore {
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<TypeConfig>> {
        let inner = self.inner.lock();
        let last = inner.log.values().last().map(|e| *e.get_log_id());
        let last_purged = inner.purged;
        Ok(LogState {
            last_log_id: last,
            last_purged_log_id: last_purged,
        })
    }

    async fn save_vote(
        &mut self,
        vote: &Vote<TypeConfig>,
    ) -> Result<(), StorageError<TypeConfig>> {
        self.inner.lock().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<TypeConfig>>, StorageError<TypeConfig>> {
        Ok(self.inner.lock().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<TypeConfig>>,
    ) -> Result<(), StorageError<TypeConfig>> {
        self.inner.lock().committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<TypeConfig>>, StorageError<TypeConfig>> {
        Ok(self.inner.lock().committed)
    }

    async fn append<I: IntoIterator<Item = Entry<TypeConfig>> + Send>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<TypeConfig>> {
        let mut inner = self.inner.lock();
        for entry in entries {
            let log_id = *entry.get_log_id();
            inner.log.insert(log_id.index, entry);
        }
        drop(inner);
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<TypeConfig>,
    ) -> Result<(), StorageError<TypeConfig>> {
        let mut inner = self.inner.lock();
        let keys_to_remove: Vec<u64> = inner
            .log
            .range(log_id.index..)
            .map(|(k, _)| *k)
            .collect();
        for key in keys_to_remove {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<TypeConfig>,
    ) -> Result<(), StorageError<TypeConfig>> {
        let mut inner = self.inner.lock();
        inner.purged = Some(log_id);
        let keys_to_remove: Vec<u64> = inner
            .log
            .range(..=log_id.index)
            .map(|(k, _)| *k)
            .collect();
        for key in keys_to_remove {
            inner.log.remove(&key);
        }
        Ok(())
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<TypeConfig>> {
        let inner = self.inner.lock();
        let entries: Vec<_> = inner.log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_log_store_new() {
        let store = MemLogStore::new();
        assert_eq!(store.log_len(), 0);
    }

    #[test]
    fn test_mem_log_store_default() {
        let store = MemLogStore::default();
        assert_eq!(store.log_len(), 0);
    }

    #[tokio::test]
    async fn test_mem_log_store_get_log_state_empty() {
        let mut store = MemLogStore::new();
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn test_mem_log_store_save_read_vote() {
        let mut store = MemLogStore::new();
        assert!(store.read_vote().await.unwrap().is_none());

        let vote = Vote::new(1, 0);
        store.save_vote(&vote).await.unwrap();
        let read = store.read_vote().await.unwrap().unwrap();
        assert_eq!(read, vote);
    }

    #[tokio::test]
    async fn test_mem_log_store_save_read_committed() {
        let mut store = MemLogStore::new();
        assert!(store.read_committed().await.unwrap().is_none());

        let log_id = LogId::new(openraft::CommittedLeaderId::new(1, 0), 5);
        store.save_committed(Some(log_id)).await.unwrap();
        let read = store.read_committed().await.unwrap().unwrap();
        assert_eq!(read, log_id);
    }
}
