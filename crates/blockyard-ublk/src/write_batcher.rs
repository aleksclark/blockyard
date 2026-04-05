use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_MAX_BATCH: usize = 64;
const DEFAULT_MAX_DELAY: Duration = Duration::from_millis(1);
const DEFAULT_ALIGNMENT: u64 = 4096;

#[derive(Debug, Clone)]
pub struct BatchedWrite {
    pub offset: u64,
    pub data: Bytes,
}

#[derive(Debug, Clone)]
pub struct WriteBatcherConfig {
    pub max_batch_size: usize,
    pub max_delay: Duration,
    pub alignment: u64,
}

impl Default for WriteBatcherConfig {
    fn default() -> Self {
        Self {
            max_batch_size: DEFAULT_MAX_BATCH,
            max_delay: DEFAULT_MAX_DELAY,
            alignment: DEFAULT_ALIGNMENT,
        }
    }
}

pub struct WriteBatcher {
    config: WriteBatcherConfig,
    pending: Arc<Mutex<PendingState>>,
}

struct PendingState {
    queue: VecDeque<BatchedWrite>,
    first_enqueue: Option<Instant>,
}

impl WriteBatcher {
    pub fn new(config: WriteBatcherConfig) -> Self {
        Self {
            config,
            pending: Arc::new(Mutex::new(PendingState {
                queue: VecDeque::new(),
                first_enqueue: None,
            })),
        }
    }

    pub fn enqueue(&self, offset: u64, data: Bytes) {
        let aligned_offset = (offset / self.config.alignment) * self.config.alignment;
        let mut state = self.pending.lock();
        if state.first_enqueue.is_none() {
            state.first_enqueue = Some(Instant::now());
        }
        state.queue.push_back(BatchedWrite {
            offset: aligned_offset,
            data,
        });
    }

    pub fn should_flush(&self) -> bool {
        let state = self.pending.lock();
        if state.queue.len() >= self.config.max_batch_size {
            return true;
        }
        if let Some(first) = state.first_enqueue {
            if first.elapsed() >= self.config.max_delay {
                return true;
            }
        }
        false
    }

    pub fn flush(&self) -> Vec<BatchedWrite> {
        let mut state = self.pending.lock();
        let writes: Vec<BatchedWrite> = state.queue.drain(..).collect();
        state.first_enqueue = None;
        writes
    }

    pub fn pending_count(&self) -> usize {
        self.pending.lock().queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.lock().queue.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_batcher_new() {
        let batcher = WriteBatcher::new(WriteBatcherConfig::default());
        assert!(batcher.is_empty());
        assert_eq!(batcher.pending_count(), 0);
    }

    #[test]
    fn test_write_batcher_enqueue() {
        let batcher = WriteBatcher::new(WriteBatcherConfig::default());
        batcher.enqueue(0, Bytes::from(vec![1, 2, 3, 4]));
        assert_eq!(batcher.pending_count(), 1);
        assert!(!batcher.is_empty());
    }

    #[test]
    fn test_write_batcher_alignment() {
        let batcher = WriteBatcher::new(WriteBatcherConfig {
            alignment: 4096,
            ..Default::default()
        });
        batcher.enqueue(5000, Bytes::from(vec![1]));
        let writes = batcher.flush();
        assert_eq!(writes[0].offset, 4096);
    }

    #[test]
    fn test_write_batcher_flush() {
        let batcher = WriteBatcher::new(WriteBatcherConfig::default());
        batcher.enqueue(0, Bytes::from(vec![1]));
        batcher.enqueue(4096, Bytes::from(vec![2]));
        let writes = batcher.flush();
        assert_eq!(writes.len(), 2);
        assert!(batcher.is_empty());
    }

    #[test]
    fn test_write_batcher_flush_empty() {
        let batcher = WriteBatcher::new(WriteBatcherConfig::default());
        let writes = batcher.flush();
        assert!(writes.is_empty());
    }

    #[test]
    fn test_write_batcher_should_flush_by_count() {
        let batcher = WriteBatcher::new(WriteBatcherConfig {
            max_batch_size: 2,
            ..Default::default()
        });
        batcher.enqueue(0, Bytes::from(vec![1]));
        assert!(!batcher.should_flush());
        batcher.enqueue(4096, Bytes::from(vec![2]));
        assert!(batcher.should_flush());
    }

    #[test]
    fn test_write_batcher_should_flush_by_time() {
        let batcher = WriteBatcher::new(WriteBatcherConfig {
            max_delay: Duration::from_millis(0),
            ..Default::default()
        });
        batcher.enqueue(0, Bytes::from(vec![1]));
        std::thread::sleep(Duration::from_millis(1));
        assert!(batcher.should_flush());
    }

    #[test]
    fn test_write_batcher_no_flush_when_empty() {
        let batcher = WriteBatcher::new(WriteBatcherConfig::default());
        assert!(!batcher.should_flush());
    }

    #[test]
    fn test_batched_write_data() {
        let batcher = WriteBatcher::new(WriteBatcherConfig::default());
        let data = Bytes::from(vec![0xAA; 4096]);
        batcher.enqueue(0, data.clone());
        let writes = batcher.flush();
        assert_eq!(writes[0].data, data);
    }
}
