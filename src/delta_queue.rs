//! Streaming delta queue for real-time event delivery
//!
//! This module provides a buffered queue for streaming deltas that are sent
//! during function execution to enable real-time SSE streaming to clients.
//!
//! ## Architecture
//!
//! - **DeltaQueue**: Thread-safe buffer with configurable max size
//! - **DeltaMessage**: Individual delta with event_type, data, sequence
//! - **Overflow policy**: Drop oldest when buffer full (FIFO)
//! - **Metrics**: Track queued, sent, dropped, errors
//!
//! ## Usage
//!
//! ```rust,ignore
//! let queue = DeltaQueue::new(1000);
//!
//! // Queue delta (called from language SDK via FFI)
//! queue.push(DeltaMessage {
//!     invocation_id: "run-123".to_string(),
//!     event_type: "output.delta".to_string(),
//!     output_data: b"\"Hello \"".to_vec(),
//!     content_index: 0,
//!     sequence: 1,
//!     metadata: HashMap::new(),
//!     queued_at: Instant::now(),
//! })?;
//!
//! // Flush to gRPC stream (called by background task)
//! while let Some(delta) = queue.pop() {
//!     stream.send(delta)?;
//! }
//! ```

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Individual streaming delta message
#[derive(Debug, Clone)]
pub struct DeltaMessage {
    /// Invocation/run ID
    pub invocation_id: String,

    /// Event type: "output.start", "output.delta", "output.stop", "run.completed", etc.
    pub event_type: String,

    /// Output payload as JSON bytes
    pub output_data: Vec<u8>,

    /// Index for parallel content blocks (e.g., thinking[0], message[0])
    pub content_index: i32,

    /// Global sequence number for ordering and resumability
    pub sequence: i64,

    /// Metadata including tenant_id, deployment_id, etc.
    pub metadata: HashMap<String, String>,

    /// When delta was queued (for metrics)
    pub queued_at: Instant,

    /// Source timestamp in nanoseconds when the event was created at the SDK
    /// Used for correct logical ordering of events in the journal
    pub source_timestamp_ns: i64,
}

/// Thread-safe delta queue with overflow protection
///
/// Buffers streaming deltas in memory and provides FIFO access with automatic
/// oldest-delta dropping when buffer is full.
#[derive(Clone)]
pub struct DeltaQueue {
    /// Internal queue protected by mutex
    queue: Arc<Mutex<VecDeque<DeltaMessage>>>,

    /// Maximum number of deltas to buffer
    max_size: usize,

    /// Metrics for monitoring queue health
    metrics: Arc<Mutex<DeltaMetrics>>,
}

/// Metrics for delta queue monitoring
#[derive(Debug, Default)]
struct DeltaMetrics {
    /// Total deltas queued
    deltas_queued: u64,

    /// Total deltas successfully sent
    deltas_sent: u64,

    /// Total deltas dropped due to overflow
    deltas_dropped: u64,

    /// Total send errors
    send_errors: u64,
}

impl DeltaQueue {
    /// Create a new delta queue with specified maximum size
    ///
    /// # Arguments
    ///
    /// * `max_size` - Maximum number of deltas to buffer (recommended: 1000)
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let queue = DeltaQueue::new(1000);
    /// ```
    pub fn new(max_size: usize) -> Self {
        log::info!("Creating delta queue with max_size={}", max_size);

        DeltaQueue {
            queue: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            metrics: Arc::new(Mutex::new(DeltaMetrics::default())),
        }
    }

    /// Push a delta to the queue
    ///
    /// If the queue is at capacity, the oldest delta is dropped (FIFO).
    ///
    /// # Arguments
    ///
    /// * `delta` - Delta message to queue
    ///
    /// # Returns
    ///
    /// `Ok(())` on success
    pub fn push(&self, delta: DeltaMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut queue = self.queue.lock().map_err(|e| {
            format!("Failed to lock delta queue for push: {}", e)
        })?;

        // Check if buffer is full
        if queue.len() >= self.max_size {
            // Drop oldest delta to make room
            if let Some(dropped) = queue.pop_front() {
                log::warn!(
                    "Delta queue full ({}), dropped oldest delta: type={} seq={} invocation={}",
                    self.max_size,
                    dropped.event_type,
                    dropped.sequence,
                    dropped.invocation_id
                );

                // Update metrics
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.deltas_dropped += 1;
                }
            }
        }

        // Add delta to queue
        log::debug!(
            "Queued delta: type={} seq={} invocation={} queue_size={}",
            delta.event_type,
            delta.sequence,
            delta.invocation_id,
            queue.len() + 1
        );

        queue.push_back(delta);

        // Update metrics
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.deltas_queued += 1;
        }

        Ok(())
    }

    /// Pop the next delta from the queue (FIFO)
    ///
    /// # Returns
    ///
    /// `Some(delta)` if queue is not empty, `None` otherwise
    pub fn pop(&self) -> Option<DeltaMessage> {
        let mut queue = self.queue.lock().ok()?;
        queue.pop_front()
    }

    /// Re-queue a delta at the front (used when send fails)
    ///
    /// This preserves delta ordering when a send fails and needs retry.
    ///
    /// # Arguments
    ///
    /// * `delta` - Delta to re-queue
    pub fn push_front(&self, delta: DeltaMessage) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut queue = self.queue.lock().map_err(|e| {
            format!("Failed to lock delta queue for push_front: {}", e)
        })?;

        log::debug!(
            "Re-queuing delta at front: type={} seq={} invocation={}",
            delta.event_type,
            delta.sequence,
            delta.invocation_id
        );

        queue.push_front(delta);
        Ok(())
    }

    /// Get current queue length
    ///
    /// # Returns
    ///
    /// Number of deltas currently buffered
    pub fn len(&self) -> usize {
        self.queue.lock().map(|q| q.len()).unwrap_or(0)
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Record a successful delta send (for metrics)
    pub fn record_sent(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.deltas_sent += 1;
        }
    }

    /// Record a delta send error (for metrics)
    pub fn record_error(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.send_errors += 1;
        }
    }

    /// Get current metrics snapshot
    ///
    /// # Returns
    ///
    /// Tuple of (queued, sent, dropped, errors)
    pub fn get_metrics(&self) -> (u64, u64, u64, u64) {
        if let Ok(metrics) = self.metrics.lock() {
            (
                metrics.deltas_queued,
                metrics.deltas_sent,
                metrics.deltas_dropped,
                metrics.send_errors,
            )
        } else {
            (0, 0, 0, 0)
        }
    }

    /// Get age of oldest delta in queue
    ///
    /// # Returns
    ///
    /// Duration since oldest delta was queued, or None if queue is empty
    pub fn oldest_age(&self) -> Option<std::time::Duration> {
        let queue = self.queue.lock().ok()?;
        queue.front().map(|delta| delta.queued_at.elapsed())
    }

    /// Drain all deltas from the queue
    ///
    /// This method removes and returns all queued deltas in FIFO order.
    /// Used for synchronous flushing before function completion.
    ///
    /// # Returns
    ///
    /// Vector of all queued deltas (empty if queue is empty)
    pub fn drain_all(&self) -> Vec<DeltaMessage> {
        let mut queue = match self.queue.lock() {
            Ok(q) => q,
            Err(e) => {
                log::error!("Failed to lock delta queue for drain: {}", e);
                return Vec::new();
            }
        };

        let mut deltas = Vec::with_capacity(queue.len());
        while let Some(delta) = queue.pop_front() {
            deltas.push(delta);
        }

        log::debug!("Drained {} deltas from queue", deltas.len());
        deltas
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_delta(seq: i64) -> DeltaMessage {
        DeltaMessage {
            invocation_id: "test-run".to_string(),
            event_type: "output.delta".to_string(),
            output_data: format!("\"chunk-{}\"", seq).into_bytes(),
            content_index: 0,
            sequence: seq,
            metadata: HashMap::new(),
            queued_at: Instant::now(),
            source_timestamp_ns: 0, // Test value
        }
    }

    #[test]
    fn test_delta_queue_basic() {
        let queue = DeltaQueue::new(10);

        // Push delta
        queue.push(create_test_delta(1)).unwrap();
        assert_eq!(queue.len(), 1);

        // Pop delta
        let delta = queue.pop().unwrap();
        assert_eq!(delta.sequence, 1);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_delta_queue_overflow() {
        let queue = DeltaQueue::new(3);

        // Fill queue
        queue.push(create_test_delta(1)).unwrap();
        queue.push(create_test_delta(2)).unwrap();
        queue.push(create_test_delta(3)).unwrap();
        assert_eq!(queue.len(), 3);

        // Overflow - should drop oldest (seq=1)
        queue.push(create_test_delta(4)).unwrap();
        assert_eq!(queue.len(), 3);

        // Verify oldest was dropped
        let delta = queue.pop().unwrap();
        assert_eq!(delta.sequence, 2); // seq=1 was dropped
    }

    #[test]
    fn test_delta_queue_push_front() {
        let queue = DeltaQueue::new(10);

        queue.push(create_test_delta(1)).unwrap();
        queue.push(create_test_delta(2)).unwrap();

        // Pop one
        let delta = queue.pop().unwrap();
        assert_eq!(delta.sequence, 1);

        // Re-queue it
        queue.push_front(delta).unwrap();

        // Should be at front again
        let delta = queue.pop().unwrap();
        assert_eq!(delta.sequence, 1);
    }

    #[test]
    fn test_delta_queue_metrics() {
        let queue = DeltaQueue::new(10);

        queue.push(create_test_delta(1)).unwrap();
        queue.push(create_test_delta(2)).unwrap();

        let (queued, sent, dropped, errors) = queue.get_metrics();
        assert_eq!(queued, 2);
        assert_eq!(sent, 0);
        assert_eq!(dropped, 0);
        assert_eq!(errors, 0);

        queue.record_sent();
        let (_, sent, _, _) = queue.get_metrics();
        assert_eq!(sent, 1);

        queue.record_error();
        let (_, _, _, errors) = queue.get_metrics();
        assert_eq!(errors, 1);
    }

    #[test]
    fn test_delta_queue_fifo_order() {
        let queue = DeltaQueue::new(10);

        // Push in order
        for i in 1..=5 {
            queue.push(create_test_delta(i)).unwrap();
        }

        // Pop should be FIFO
        for i in 1..=5 {
            let delta = queue.pop().unwrap();
            assert_eq!(delta.sequence, i);
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_delta_queue_drain_all() {
        let queue = DeltaQueue::new(10);

        for i in 1..=5 {
            queue.push(create_test_delta(i)).unwrap();
        }
        assert_eq!(queue.len(), 5);

        let deltas = queue.drain_all();
        assert_eq!(deltas.len(), 5);
        assert!(queue.is_empty());

        // Verify order
        for (i, delta) in deltas.iter().enumerate() {
            assert_eq!(delta.sequence, (i + 1) as i64);
        }
    }
}
