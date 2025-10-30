//! Workflow checkpoint queue for progressive durability
//!
//! This module provides a buffered queue for workflow checkpoints that are sent
//! during workflow execution to preserve progress and enable crash recovery.
//!
//! ## Architecture
//!
//! - **CheckpointQueue**: Thread-safe buffer with configurable max size
//! - **CheckpointMessage**: Individual checkpoint with type, data, sequence
//! - **Overflow policy**: Drop oldest when buffer full (FIFO)
//! - **Metrics**: Track queued, sent, dropped, errors
//!
//! ## Usage
//!
//! ```rust,ignore
//! let queue = CheckpointQueue::new(1000);
//!
//! // Queue checkpoint (called from language SDK via FFI)
//! queue.push(CheckpointMessage {
//!     invocation_id: "run-123".to_string(),
//!     checkpoint_type: "workflow.state.changed".to_string(),
//!     checkpoint_data: b"{\"key\": \"x\", \"value\": 1}".to_vec(),
//!     sequence_number: 1,
//!     metadata: HashMap::new(),
//! })?;
//!
//! // Flush to gRPC stream (called by Worker)
//! while let Some(checkpoint) = queue.pop() {
//!     stream.send(checkpoint)?;
//! }
//! ```

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Individual checkpoint message containing workflow progress
#[derive(Debug, Clone)]
pub struct CheckpointMessage {
    /// Workflow run ID
    pub invocation_id: String,

    /// Event type: "workflow.state.changed", "workflow.step.started", "workflow.step.completed"
    pub checkpoint_type: String,

    /// Checkpoint payload as JSON bytes
    pub checkpoint_data: Vec<u8>,

    /// Monotonic sequence number for ordering
    pub sequence_number: i64,

    /// Metadata including tenant_id, deployment_id, etc.
    pub metadata: HashMap<String, String>,

    /// When checkpoint was queued (for metrics)
    pub queued_at: Instant,
}

/// Thread-safe checkpoint queue with overflow protection
///
/// Buffers checkpoints in memory and provides FIFO access with automatic
/// oldest-checkpoint dropping when buffer is full.
#[derive(Clone)]
pub struct CheckpointQueue {
    /// Internal queue protected by mutex
    queue: Arc<Mutex<VecDeque<CheckpointMessage>>>,

    /// Maximum number of checkpoints to buffer
    max_size: usize,

    /// Metrics for monitoring queue health
    metrics: Arc<Mutex<CheckpointMetrics>>,
}

/// Metrics for checkpoint queue monitoring
#[derive(Debug, Default)]
struct CheckpointMetrics {
    /// Total checkpoints queued
    checkpoints_queued: u64,

    /// Total checkpoints successfully sent
    checkpoints_sent: u64,

    /// Total checkpoints dropped due to overflow
    checkpoints_dropped: u64,

    /// Total send errors
    send_errors: u64,
}

impl CheckpointQueue {
    /// Create a new checkpoint queue with specified maximum size
    ///
    /// # Arguments
    ///
    /// * `max_size` - Maximum number of checkpoints to buffer (recommended: 1000)
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let queue = CheckpointQueue::new(1000);
    /// ```
    pub fn new(max_size: usize) -> Self {
        log::info!("Creating checkpoint queue with max_size={}", max_size);

        CheckpointQueue {
            queue: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            metrics: Arc::new(Mutex::new(CheckpointMetrics::default())),
        }
    }

    /// Push a checkpoint to the queue
    ///
    /// If the queue is at capacity, the oldest checkpoint is dropped (FIFO).
    ///
    /// # Arguments
    ///
    /// * `checkpoint` - Checkpoint message to queue
    ///
    /// # Returns
    ///
    /// `Ok(())` on success
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// queue.push(CheckpointMessage {
    ///     invocation_id: "run-123".to_string(),
    ///     checkpoint_type: "workflow.state.changed".to_string(),
    ///     checkpoint_data: b"{}".to_vec(),
    ///     sequence_number: 1,
    ///     metadata: HashMap::new(),
    ///     queued_at: Instant::now(),
    /// })?;
    /// ```
    pub fn push(&self, checkpoint: CheckpointMessage) -> Result<(), Box<dyn std::error::Error>> {
        let mut queue = self.queue.lock().map_err(|e| {
            format!("Failed to lock checkpoint queue for push: {}", e)
        })?;

        // Check if buffer is full
        if queue.len() >= self.max_size {
            // Drop oldest checkpoint to make room
            if let Some(dropped) = queue.pop_front() {
                log::warn!(
                    "Checkpoint queue full ({}), dropped oldest checkpoint: type={} seq={} invocation={}",
                    self.max_size,
                    dropped.checkpoint_type,
                    dropped.sequence_number,
                    dropped.invocation_id
                );

                // Update metrics
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.checkpoints_dropped += 1;
                }
            }
        }

        // Add checkpoint to queue
        log::debug!(
            "Queued checkpoint: type={} seq={} invocation={} queue_size={}",
            checkpoint.checkpoint_type,
            checkpoint.sequence_number,
            checkpoint.invocation_id,
            queue.len() + 1
        );

        queue.push_back(checkpoint);

        // Update metrics
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.checkpoints_queued += 1;
        }

        Ok(())
    }

    /// Pop the next checkpoint from the queue (FIFO)
    ///
    /// # Returns
    ///
    /// `Some(checkpoint)` if queue is not empty, `None` otherwise
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// while let Some(checkpoint) = queue.pop() {
    ///     // Send checkpoint via gRPC
    ///     stream.send(checkpoint)?;
    /// }
    /// ```
    pub fn pop(&self) -> Option<CheckpointMessage> {
        let mut queue = self.queue.lock().ok()?;
        queue.pop_front()
    }

    /// Re-queue a checkpoint at the front (used when send fails)
    ///
    /// This preserves checkpoint ordering when a send fails and needs retry.
    ///
    /// # Arguments
    ///
    /// * `checkpoint` - Checkpoint to re-queue
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// if let Err(e) = stream.send(&checkpoint) {
    ///     // Send failed, re-queue for retry
    ///     queue.push_front(checkpoint)?;
    /// }
    /// ```
    pub fn push_front(&self, checkpoint: CheckpointMessage) -> Result<(), Box<dyn std::error::Error>> {
        let mut queue = self.queue.lock().map_err(|e| {
            format!("Failed to lock checkpoint queue for push_front: {}", e)
        })?;

        log::debug!(
            "Re-queuing checkpoint at front: type={} seq={} invocation={}",
            checkpoint.checkpoint_type,
            checkpoint.sequence_number,
            checkpoint.invocation_id
        );

        queue.push_front(checkpoint);
        Ok(())
    }

    /// Get current queue length
    ///
    /// # Returns
    ///
    /// Number of checkpoints currently buffered
    pub fn len(&self) -> usize {
        self.queue.lock().map(|q| q.len()).unwrap_or(0)
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Record a successful checkpoint send (for metrics)
    pub fn record_sent(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.checkpoints_sent += 1;
        }
    }

    /// Record a checkpoint send error (for metrics)
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
                metrics.checkpoints_queued,
                metrics.checkpoints_sent,
                metrics.checkpoints_dropped,
                metrics.send_errors,
            )
        } else {
            (0, 0, 0, 0)
        }
    }

    /// Get age of oldest checkpoint in queue
    ///
    /// # Returns
    ///
    /// Duration since oldest checkpoint was queued, or None if queue is empty
    pub fn oldest_age(&self) -> Option<std::time::Duration> {
        let queue = self.queue.lock().ok()?;
        queue.front().map(|checkpoint| checkpoint.queued_at.elapsed())
    }

    /// Drain all checkpoints from the queue
    ///
    /// This method removes and returns all queued checkpoints in FIFO order.
    /// Used for synchronous flushing before workflow completion.
    ///
    /// # Returns
    ///
    /// Vector of all queued checkpoints (empty if queue is empty)
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Drain all checkpoints for immediate flushing
    /// let checkpoints = queue.drain_all();
    /// for checkpoint in checkpoints {
    ///     stream.send(checkpoint)?;
    ///     queue.record_sent();
    /// }
    /// ```
    pub fn drain_all(&self) -> Vec<CheckpointMessage> {
        let mut queue = match self.queue.lock() {
            Ok(q) => q,
            Err(e) => {
                log::error!("Failed to lock checkpoint queue for drain: {}", e);
                return Vec::new();
            }
        };

        let mut checkpoints = Vec::with_capacity(queue.len());
        while let Some(checkpoint) = queue.pop_front() {
            checkpoints.push(checkpoint);
        }

        log::debug!("Drained {} checkpoints from queue", checkpoints.len());
        checkpoints
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_checkpoint(seq: i64) -> CheckpointMessage {
        CheckpointMessage {
            invocation_id: "test-run".to_string(),
            checkpoint_type: "workflow.state.changed".to_string(),
            checkpoint_data: format!("{{\"seq\": {}}}", seq).into_bytes(),
            sequence_number: seq,
            metadata: HashMap::new(),
            queued_at: Instant::now(),
        }
    }

    #[test]
    fn test_checkpoint_queue_basic() {
        let queue = CheckpointQueue::new(10);

        // Push checkpoint
        queue.push(create_test_checkpoint(1)).unwrap();
        assert_eq!(queue.len(), 1);

        // Pop checkpoint
        let checkpoint = queue.pop().unwrap();
        assert_eq!(checkpoint.sequence_number, 1);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_checkpoint_queue_overflow() {
        let queue = CheckpointQueue::new(3);

        // Fill queue
        queue.push(create_test_checkpoint(1)).unwrap();
        queue.push(create_test_checkpoint(2)).unwrap();
        queue.push(create_test_checkpoint(3)).unwrap();
        assert_eq!(queue.len(), 3);

        // Overflow - should drop oldest (seq=1)
        queue.push(create_test_checkpoint(4)).unwrap();
        assert_eq!(queue.len(), 3);

        // Verify oldest was dropped
        let checkpoint = queue.pop().unwrap();
        assert_eq!(checkpoint.sequence_number, 2); // seq=1 was dropped
    }

    #[test]
    fn test_checkpoint_queue_push_front() {
        let queue = CheckpointQueue::new(10);

        queue.push(create_test_checkpoint(1)).unwrap();
        queue.push(create_test_checkpoint(2)).unwrap();

        // Pop one
        let checkpoint = queue.pop().unwrap();
        assert_eq!(checkpoint.sequence_number, 1);

        // Re-queue it
        queue.push_front(checkpoint).unwrap();

        // Should be at front again
        let checkpoint = queue.pop().unwrap();
        assert_eq!(checkpoint.sequence_number, 1);
    }

    #[test]
    fn test_checkpoint_queue_metrics() {
        let queue = CheckpointQueue::new(10);

        queue.push(create_test_checkpoint(1)).unwrap();
        queue.push(create_test_checkpoint(2)).unwrap();

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
    fn test_checkpoint_queue_fifo_order() {
        let queue = CheckpointQueue::new(10);

        // Push in order
        for i in 1..=5 {
            queue.push(create_test_checkpoint(i)).unwrap();
        }

        // Pop should be FIFO
        for i in 1..=5 {
            let checkpoint = queue.pop().unwrap();
            assert_eq!(checkpoint.sequence_number, i);
        }

        assert!(queue.is_empty());
    }
}
