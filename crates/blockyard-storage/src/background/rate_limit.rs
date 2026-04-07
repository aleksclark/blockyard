//! Token-bucket rate limiter for background IO budget (P7.7, §5.13).
//!
//! Provides a simple async-compatible token bucket that background workers
//! acquire tokens from before performing IO, ensuring foreground traffic
//! is not starved.

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::{Duration, Instant};
use tracing::debug;

/// Token-bucket rate limiter.
///
/// Tokens are replenished at a fixed rate up to a maximum capacity.
/// Background workers must acquire tokens before performing IO.
#[derive(Debug)]
pub struct TokenBucket {
    capacity: u64,
    refill_rate: u64,
    tokens: AtomicU64,
    last_refill: parking_lot::Mutex<Instant>,
}

impl TokenBucket {
    /// Create a new token bucket with the given capacity and refill rate (tokens/sec).
    pub fn new(capacity: u64, refill_rate: u64) -> Self {
        Self {
            capacity,
            refill_rate,
            tokens: AtomicU64::new(capacity),
            last_refill: parking_lot::Mutex::new(Instant::now()),
        }
    }

    /// Try to acquire `count` tokens without blocking.
    ///
    /// Returns `true` if tokens were acquired, `false` otherwise.
    pub fn try_acquire(&self, count: u64) -> bool {
        self.refill();
        let current = self.tokens.load(Ordering::Relaxed);
        if current >= count {
            // CAS loop for correctness
            match self.tokens.compare_exchange(
                current,
                current - count,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => true,
                Err(_) => {
                    // Retry once on contention
                    let current = self.tokens.load(Ordering::Relaxed);
                    if current >= count {
                        self.tokens
                            .compare_exchange(
                                current,
                                current - count,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            )
                            .is_ok()
                    } else {
                        false
                    }
                }
            }
        } else {
            false
        }
    }

    /// Acquire `count` tokens, waiting if necessary.
    ///
    /// Polls every 10ms until tokens are available.
    pub async fn acquire(&self, count: u64) {
        loop {
            if self.try_acquire(count) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Get current token count (approximate, for observability).
    pub fn available(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Relaxed)
    }

    /// Get the capacity of this bucket.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Get the refill rate (tokens/sec).
    pub fn refill_rate(&self) -> u64 {
        self.refill_rate
    }

    /// Refill tokens based on elapsed time.
    fn refill(&self) {
        let mut last = self.last_refill.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*last);
        let new_tokens = (elapsed.as_millis() as u64 * self.refill_rate) / 1000;

        if new_tokens > 0 {
            let current = self.tokens.load(Ordering::Relaxed);
            let new_total = (current + new_tokens).min(self.capacity);
            self.tokens.store(new_total, Ordering::Relaxed);
            *last = now;
            debug!(
                new_tokens = new_tokens,
                total = new_total,
                "token bucket refilled"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_bucket_starts_full() {
        let bucket = TokenBucket::new(100, 50);
        assert_eq!(bucket.available(), 100);
    }

    #[test]
    fn test_try_acquire_success() {
        let bucket = TokenBucket::new(100, 50);
        assert!(bucket.try_acquire(50));
        assert_eq!(bucket.available(), 50);
    }

    #[test]
    fn test_try_acquire_exact_capacity() {
        let bucket = TokenBucket::new(100, 50);
        assert!(bucket.try_acquire(100));
        assert_eq!(bucket.available(), 0);
    }

    #[test]
    fn test_try_acquire_insufficient_tokens() {
        let bucket = TokenBucket::new(100, 50);
        assert!(bucket.try_acquire(80));
        assert!(!bucket.try_acquire(30));
    }

    #[test]
    fn test_try_acquire_zero() {
        let bucket = TokenBucket::new(100, 50);
        assert!(bucket.try_acquire(0));
        assert_eq!(bucket.available(), 100);
    }

    #[tokio::test]
    async fn test_acquire_immediate() {
        let bucket = TokenBucket::new(100, 50);
        bucket.acquire(50).await;
        assert_eq!(bucket.available(), 50);
    }

    #[tokio::test]
    async fn test_acquire_waits_for_refill() {
        let bucket = TokenBucket::new(100, 10_000);
        assert!(bucket.try_acquire(100));
        // With 10k tokens/sec refill, acquiring 10 tokens should succeed quickly
        let start = Instant::now();
        bucket.acquire(10).await;
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_secs(1));
    }

    #[test]
    fn test_capacity() {
        let bucket = TokenBucket::new(200, 100);
        assert_eq!(bucket.capacity(), 200);
    }

    #[test]
    fn test_refill_rate() {
        let bucket = TokenBucket::new(200, 100);
        assert_eq!(bucket.refill_rate(), 100);
    }

    #[test]
    fn test_refill_caps_at_capacity() {
        let bucket = TokenBucket::new(100, 100_000);
        assert!(bucket.try_acquire(50));
        // Force a refill with enough elapsed time
        std::thread::sleep(std::time::Duration::from_millis(20));
        let available = bucket.available();
        assert!(available <= 100, "available={available} should be <= 100");
    }

    #[test]
    fn test_multiple_acquires() {
        let bucket = TokenBucket::new(100, 50);
        assert!(bucket.try_acquire(30));
        assert!(bucket.try_acquire(30));
        assert!(bucket.try_acquire(30));
        assert!(!bucket.try_acquire(30));
    }

    #[test]
    fn test_debug_impl() {
        let bucket = TokenBucket::new(100, 50);
        let debug = format!("{:?}", bucket);
        assert!(debug.contains("TokenBucket"));
    }

    #[tokio::test]
    async fn test_concurrent_acquire() {
        use std::sync::Arc;
        let bucket = Arc::new(TokenBucket::new(100, 0));
        let mut handles = Vec::new();
        for _ in 0..10 {
            let b = Arc::clone(&bucket);
            handles.push(tokio::spawn(async move { b.try_acquire(10) }));
        }
        let mut successes = 0;
        for h in handles {
            if h.await.unwrap_or(false) {
                successes += 1;
            }
        }
        // With 100 tokens and 10 per acquire, at most 10 can succeed
        assert!(successes <= 10);
        assert!(successes >= 1);
    }
}
