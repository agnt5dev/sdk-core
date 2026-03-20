//! Unified journal event queue for all event types
//!
//! This module provides a single buffered queue for all journal events, replacing
//! the fragmented CheckpointQueue, DeltaQueue, SpanExportQueue, and LogExportQueue.
//!
//! ## Architecture
//!
//! - **JournalEventQueue**: Thread-safe buffer with configurable max size
//! - **JournalEventMessage**: Unified event with classification flags
//! - **Overflow policy**: Drop oldest when buffer full (FIFO)
//! - **Metrics**: Track queued, sent, dropped, errors
//!
//! ## Event Classification
//!
//! Events are classified into two categories:
//!
//! - **Boundary events**: Persisted to journal_events table (workflow.*, agent.*, lm.call.*, etc.)
//! - **SSE-only events**: Forwarded to SSE stream but NOT persisted (output.delta, log, etc.)
//!
//! ## Usage
//!
//! ```rust,ignore
//! let queue = JournalEventQueue::new(JournalQueueConfig::default());
//!
//! // Queue a boundary event (persisted)
//! queue.push(JournalEventMessage {
//!     run_id: "run-123".to_string(),
//!     event_type: "workflow.step.completed".to_string(),
//!     data: b"{\"step\": \"fetch\"}".to_vec(),
//!     is_sse_only: false,
//!     ..Default::default()
//! })?;
//!
//! // Queue an SSE-only event (not persisted)
//! queue.push(JournalEventMessage {
//!     run_id: "run-123".to_string(),
//!     event_type: "output.delta".to_string(),
//!     data: b"\"Hello \"".to_vec(),
//!     is_sse_only: true,
//!     ..Default::default()
//! })?;
//!
//! // Drain batch for sending
//! let batch = queue.drain_batch(100);
//! ```

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Configuration for the journal event queue
#[derive(Debug, Clone)]
pub struct JournalQueueConfig {
    /// Maximum number of events to buffer
    pub max_size: usize,
    /// Maximum batch size for drain_batch
    pub batch_size: usize,
    /// Flush interval in milliseconds (for reference, not enforced by queue)
    pub flush_interval_ms: u64,
}

impl Default for JournalQueueConfig {
    fn default() -> Self {
        Self {
            max_size: 5000,
            batch_size: 100,
            flush_interval_ms: 50,
        }
    }
}

impl JournalQueueConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        let max_size = std::env::var("AGNT5_JOURNAL_QUEUE_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5000);

        let batch_size = std::env::var("AGNT5_JOURNAL_BATCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        let flush_interval_ms = std::env::var("AGNT5_JOURNAL_FLUSH_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(50);

        Self {
            max_size,
            batch_size,
            flush_interval_ms,
        }
    }
}

/// Unified event message for all event types
#[derive(Debug, Clone)]
pub struct JournalEventMessage {
    // Identity
    /// Run ID for this event
    pub run_id: String,
    /// Event type (e.g., "workflow.step.completed", "output.delta", "log")
    pub event_type: String,
    /// Event payload as JSON bytes
    pub data: Vec<u8>,

    // Event correlation
    /// Correlation ID for pairing started↔completed events
    pub correlation_id: String,
    /// Parent correlation ID for hierarchy (tree view)
    pub parent_correlation_id: String,

    // Metadata
    /// Optional tenant ID
    pub tenant_id: Option<String>,
    /// Source timestamp in nanoseconds (when event was created)
    pub source_timestamp_ns: i64,
    /// Additional metadata (display-friendly key-value pairs)
    pub metadata: HashMap<String, String>,

    // Queue management
    /// When event was queued (for metrics)
    pub queued_at: Instant,
    /// Whether this is a streaming request (affects delivery mode)
    pub is_streaming: bool,
    /// If true, forward to SSE only (no persist) - for deltas and logs
    pub is_sse_only: bool,

    // Content indexing (for streaming deltas)
    /// Index for parallel content blocks
    pub content_index: i32,
    /// Sequence number for ordering
    pub sequence: i64,
}

impl Default for JournalEventMessage {
    fn default() -> Self {
        Self {
            run_id: String::new(),
            event_type: String::new(),
            data: Vec::new(),
            correlation_id: String::new(),
            parent_correlation_id: String::new(),
            tenant_id: None,
            source_timestamp_ns: 0,
            metadata: HashMap::new(),
            queued_at: Instant::now(),
            is_streaming: false,
            is_sse_only: false,
            content_index: 0,
            sequence: 0,
        }
    }
}

impl JournalEventMessage {
    /// Check if this event type is an SSE-only event (delta, log, etc.)
    pub fn is_sse_only_event_type(event_type: &str) -> bool {
        // SSE-only event types (not persisted to journal_events)
        // These are streaming/observability events that don't affect replay
        event_type.starts_with("output.")
            || event_type.starts_with("lm.stream.")
            || event_type.starts_with("lm.message.")
            || event_type.starts_with("lm.thinking.")
            || event_type.starts_with("lm.tool_call.")  // LLM tool call content blocks (transient deltas)
            || event_type.starts_with("progress.")
            || event_type.starts_with("log")  // log, log.info, log.warn, log.error, etc.
    }

    /// Check if this event type is a checkpoint event that requires sync acknowledgement
    ///
    /// Checkpoint events block until the platform acknowledges persistence. This ensures
    /// correct event ordering for lifecycle events that affect workflow state.
    ///
    /// Checkpoint events include:
    /// - `*.started`, `*.completed`, `*.failed`, `*.paused`
    /// - `approval.requested`, `approval.resolved`
    ///
    /// This is the inverse of `is_sse_only_event_type()` - if an event is NOT SSE-only,
    /// it's a checkpoint event.
    pub fn is_checkpoint_event_type(event_type: &str) -> bool {
        !Self::is_sse_only_event_type(event_type)
    }

    /// Create with automatic is_sse_only detection based on event_type
    pub fn new(
        run_id: String,
        event_type: String,
        data: Vec<u8>,
    ) -> Self {
        let is_sse_only = Self::is_sse_only_event_type(&event_type);
        Self {
            run_id,
            event_type,
            data,
            is_sse_only,
            queued_at: Instant::now(),
            ..Default::default()
        }
    }
}

/// Metrics for journal queue monitoring
#[derive(Debug, Default, Clone)]
pub struct JournalQueueMetrics {
    /// Total events queued
    pub events_queued: u64,
    /// Total events successfully sent
    pub events_sent: u64,
    /// Total events dropped due to overflow
    pub events_dropped: u64,
    /// Total send errors
    pub send_errors: u64,
    /// Boundary events sent (persisted)
    pub boundary_events_sent: u64,
    /// SSE-only events sent (not persisted)
    pub sse_only_events_sent: u64,
}

/// Thread-safe unified journal event queue
///
/// Buffers all event types in memory and provides FIFO access with automatic
/// oldest-event dropping when buffer is full.
#[derive(Clone)]
pub struct JournalEventQueue {
    /// Internal queue protected by mutex
    queue: Arc<Mutex<VecDeque<JournalEventMessage>>>,
    /// Configuration
    config: JournalQueueConfig,
    /// Metrics for monitoring queue health
    metrics: Arc<Mutex<JournalQueueMetrics>>,
}

impl JournalEventQueue {
    /// Create a new journal event queue with specified configuration
    pub fn new(config: JournalQueueConfig) -> Self {
        log::info!(
            "Creating unified journal event queue: max_size={}, batch_size={}, flush_interval_ms={}",
            config.max_size,
            config.batch_size,
            config.flush_interval_ms
        );

        JournalEventQueue {
            queue: Arc::new(Mutex::new(VecDeque::with_capacity(config.max_size))),
            config,
            metrics: Arc::new(Mutex::new(JournalQueueMetrics::default())),
        }
    }

    /// Push an event to the queue
    ///
    /// If the queue is at capacity, the oldest event is dropped (FIFO).
    pub fn push(&self, event: JournalEventMessage) -> Result<(), String> {
        let mut queue = self.queue.lock().map_err(|e| {
            format!("Failed to lock journal queue for push: {}", e)
        })?;

        // Check if buffer is full
        if queue.len() >= self.config.max_size {
            // Drop oldest event to make room
            if let Some(dropped) = queue.pop_front() {
                log::warn!(
                    "Journal queue full ({}), dropped oldest event: type={} run_id={}",
                    self.config.max_size,
                    dropped.event_type,
                    dropped.run_id
                );

                // Update metrics
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.events_dropped += 1;
                }
            }
        }

        log::debug!(
            "Queued journal event: type={} run_id={} is_sse_only={} queue_size={}",
            event.event_type,
            event.run_id,
            event.is_sse_only,
            queue.len() + 1
        );

        queue.push_back(event);

        // Update metrics
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.events_queued += 1;
        }

        Ok(())
    }

    /// Pop the next event from the queue (FIFO)
    pub fn pop(&self) -> Option<JournalEventMessage> {
        let mut queue = self.queue.lock().ok()?;
        queue.pop_front()
    }

    /// Re-queue an event at the front (used when send fails)
    pub fn push_front(&self, event: JournalEventMessage) -> Result<(), String> {
        let mut queue = self.queue.lock().map_err(|e| {
            format!("Failed to lock journal queue for push_front: {}", e)
        })?;

        log::debug!(
            "Re-queuing event at front: type={} run_id={}",
            event.event_type,
            event.run_id
        );

        queue.push_front(event);
        Ok(())
    }

    /// Drain up to N events from the queue for batch sending
    pub fn drain_batch(&self, max: usize) -> Vec<JournalEventMessage> {
        let mut queue = match self.queue.lock() {
            Ok(q) => q,
            Err(e) => {
                log::error!("Failed to lock journal queue for drain_batch: {}", e);
                return Vec::new();
            }
        };

        let count = std::cmp::min(max, queue.len());
        let mut batch = Vec::with_capacity(count);

        for _ in 0..count {
            if let Some(event) = queue.pop_front() {
                batch.push(event);
            }
        }

        if !batch.is_empty() {
            log::debug!(
                "Drained {} events from journal queue (remaining={})",
                batch.len(),
                queue.len()
            );
        }

        batch
    }

    /// Drain all events from the queue
    pub fn drain_all(&self) -> Vec<JournalEventMessage> {
        let mut queue = match self.queue.lock() {
            Ok(q) => q,
            Err(e) => {
                log::error!("Failed to lock journal queue for drain_all: {}", e);
                return Vec::new();
            }
        };

        let mut events = Vec::with_capacity(queue.len());
        while let Some(event) = queue.pop_front() {
            events.push(event);
        }

        log::debug!("Drained {} events from journal queue", events.len());
        events
    }

    /// Drain all events for a specific run_id from the queue.
    /// Events for other runs remain in the queue (order preserved).
    pub fn drain_run_events(&self, run_id: &str) -> Vec<JournalEventMessage> {
        let mut queue = match self.queue.lock() {
            Ok(q) => q,
            Err(e) => {
                log::error!("Failed to lock journal queue for drain_run_events: {}", e);
                return Vec::new();
            }
        };

        let mut matched = Vec::new();
        let mut remaining = std::collections::VecDeque::with_capacity(queue.len());

        while let Some(event) = queue.pop_front() {
            if event.run_id == run_id {
                matched.push(event);
            } else {
                remaining.push_back(event);
            }
        }

        *queue = remaining;

        if !matched.is_empty() {
            log::debug!(
                "Drained {} events for run_id={} (remaining={})",
                matched.len(),
                run_id,
                queue.len()
            );
        }

        matched
    }

    /// Get current queue length
    pub fn len(&self) -> usize {
        self.queue.lock().map(|q| q.len()).unwrap_or(0)
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Record a successful event send (for metrics)
    pub fn record_sent(&self, is_sse_only: bool) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.events_sent += 1;
            if is_sse_only {
                metrics.sse_only_events_sent += 1;
            } else {
                metrics.boundary_events_sent += 1;
            }
        }
    }

    /// Record batch of successful sends
    pub fn record_sent_batch(&self, count: usize, sse_only_count: usize) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.events_sent += count as u64;
            metrics.sse_only_events_sent += sse_only_count as u64;
            metrics.boundary_events_sent += (count - sse_only_count) as u64;
        }
    }

    /// Record an event send error (for metrics)
    pub fn record_error(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.send_errors += 1;
        }
    }

    /// Get current metrics snapshot
    pub fn metrics(&self) -> JournalQueueMetrics {
        self.metrics
            .lock()
            .map(|m| m.clone())
            .unwrap_or_default()
    }

    /// Get current metrics as tuple (queued, sent, dropped, errors)
    pub fn get_metrics(&self) -> (u64, u64, u64, u64) {
        if let Ok(metrics) = self.metrics.lock() {
            (
                metrics.events_queued,
                metrics.events_sent,
                metrics.events_dropped,
                metrics.send_errors,
            )
        } else {
            (0, 0, 0, 0)
        }
    }

    /// Get age of oldest event in queue
    pub fn oldest_age(&self) -> Option<std::time::Duration> {
        let queue = self.queue.lock().ok()?;
        queue.front().map(|event| event.queued_at.elapsed())
    }

    /// Get the configured batch size
    pub fn batch_size(&self) -> usize {
        self.config.batch_size
    }

    /// Get the configured flush interval in milliseconds
    pub fn flush_interval_ms(&self) -> u64 {
        self.config.flush_interval_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_event(event_type: &str, seq: i64) -> JournalEventMessage {
        JournalEventMessage {
            run_id: "test-run".to_string(),
            event_type: event_type.to_string(),
            data: format!("{{\"seq\": {}}}", seq).into_bytes(),
            is_sse_only: JournalEventMessage::is_sse_only_event_type(event_type),
            sequence: seq,
            source_timestamp_ns: 1234567890000000000 + seq * 1000000,
            queued_at: Instant::now(),
            ..Default::default()
        }
    }

    #[test]
    fn test_journal_queue_basic() {
        let queue = JournalEventQueue::new(JournalQueueConfig {
            max_size: 10,
            ..Default::default()
        });

        // Push event
        queue.push(create_test_event("workflow.started", 1)).unwrap();
        assert_eq!(queue.len(), 1);

        // Pop event
        let event = queue.pop().unwrap();
        assert_eq!(event.sequence, 1);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_journal_queue_overflow() {
        let queue = JournalEventQueue::new(JournalQueueConfig {
            max_size: 3,
            ..Default::default()
        });

        // Fill queue
        queue.push(create_test_event("workflow.started", 1)).unwrap();
        queue.push(create_test_event("workflow.step.started", 2)).unwrap();
        queue.push(create_test_event("workflow.step.completed", 3)).unwrap();
        assert_eq!(queue.len(), 3);

        // Overflow - should drop oldest (seq=1)
        queue.push(create_test_event("workflow.completed", 4)).unwrap();
        assert_eq!(queue.len(), 3);

        // Verify oldest was dropped
        let event = queue.pop().unwrap();
        assert_eq!(event.sequence, 2); // seq=1 was dropped
    }

    #[test]
    fn test_sse_only_detection() {
        // Boundary events (persisted)
        assert!(!JournalEventMessage::is_sse_only_event_type("workflow.started"));
        assert!(!JournalEventMessage::is_sse_only_event_type("agent.completed"));
        assert!(!JournalEventMessage::is_sse_only_event_type("lm.call.started"));
        assert!(!JournalEventMessage::is_sse_only_event_type("tool.call.completed"));

        // SSE-only events (not persisted)
        assert!(JournalEventMessage::is_sse_only_event_type("output.delta"));
        assert!(JournalEventMessage::is_sse_only_event_type("output.start"));
        assert!(JournalEventMessage::is_sse_only_event_type("output.stop"));
        assert!(JournalEventMessage::is_sse_only_event_type("lm.stream.delta"));
        assert!(JournalEventMessage::is_sse_only_event_type("lm.message.delta"));
        assert!(JournalEventMessage::is_sse_only_event_type("lm.thinking.delta"));
        assert!(JournalEventMessage::is_sse_only_event_type("lm.tool_call.start"));
        assert!(JournalEventMessage::is_sse_only_event_type("lm.tool_call.delta"));
        assert!(JournalEventMessage::is_sse_only_event_type("lm.tool_call.stop"));
        assert!(JournalEventMessage::is_sse_only_event_type("progress.update"));
        assert!(JournalEventMessage::is_sse_only_event_type("log"));
        assert!(JournalEventMessage::is_sse_only_event_type("log.info"));
        assert!(JournalEventMessage::is_sse_only_event_type("log.warn"));
        assert!(JournalEventMessage::is_sse_only_event_type("log.error"));
    }

    #[test]
    fn test_checkpoint_event_detection() {
        // Checkpoint events (require sync ack) - inverse of SSE-only
        assert!(JournalEventMessage::is_checkpoint_event_type("workflow.started"));
        assert!(JournalEventMessage::is_checkpoint_event_type("workflow.completed"));
        assert!(JournalEventMessage::is_checkpoint_event_type("workflow.failed"));
        assert!(JournalEventMessage::is_checkpoint_event_type("workflow.paused"));
        assert!(JournalEventMessage::is_checkpoint_event_type("workflow.step.started"));
        assert!(JournalEventMessage::is_checkpoint_event_type("workflow.step.completed"));
        assert!(JournalEventMessage::is_checkpoint_event_type("workflow.step.paused"));
        assert!(JournalEventMessage::is_checkpoint_event_type("agent.started"));
        assert!(JournalEventMessage::is_checkpoint_event_type("agent.completed"));
        assert!(JournalEventMessage::is_checkpoint_event_type("approval.requested"));
        assert!(JournalEventMessage::is_checkpoint_event_type("approval.resolved"));
        assert!(JournalEventMessage::is_checkpoint_event_type("lm.call.started"));
        assert!(JournalEventMessage::is_checkpoint_event_type("lm.call.completed"));
        assert!(JournalEventMessage::is_checkpoint_event_type("tool.call.started"));
        assert!(JournalEventMessage::is_checkpoint_event_type("tool.call.completed"));

        // NOT checkpoint events (SSE-only)
        assert!(!JournalEventMessage::is_checkpoint_event_type("output.delta"));
        assert!(!JournalEventMessage::is_checkpoint_event_type("lm.stream.delta"));
        assert!(!JournalEventMessage::is_checkpoint_event_type("lm.message.delta"));
        assert!(!JournalEventMessage::is_checkpoint_event_type("lm.thinking.delta"));
        assert!(!JournalEventMessage::is_checkpoint_event_type("progress.update"));
        assert!(!JournalEventMessage::is_checkpoint_event_type("log"));
    }

    #[test]
    fn test_drain_batch() {
        let queue = JournalEventQueue::new(JournalQueueConfig {
            max_size: 10,
            batch_size: 3,
            ..Default::default()
        });

        // Add 5 events
        for i in 1..=5 {
            queue.push(create_test_event("workflow.step.completed", i)).unwrap();
        }
        assert_eq!(queue.len(), 5);

        // Drain batch of 3
        let batch = queue.drain_batch(3);
        assert_eq!(batch.len(), 3);
        assert_eq!(queue.len(), 2);

        // Verify order
        assert_eq!(batch[0].sequence, 1);
        assert_eq!(batch[1].sequence, 2);
        assert_eq!(batch[2].sequence, 3);

        // Drain remaining
        let batch = queue.drain_batch(10);
        assert_eq!(batch.len(), 2);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_push_front() {
        let queue = JournalEventQueue::new(JournalQueueConfig::default());

        queue.push(create_test_event("workflow.started", 1)).unwrap();
        queue.push(create_test_event("workflow.completed", 2)).unwrap();

        // Pop one
        let event = queue.pop().unwrap();
        assert_eq!(event.sequence, 1);

        // Re-queue it
        queue.push_front(event).unwrap();

        // Should be at front again
        let event = queue.pop().unwrap();
        assert_eq!(event.sequence, 1);
    }

    #[test]
    fn test_metrics() {
        let queue = JournalEventQueue::new(JournalQueueConfig::default());

        queue.push(create_test_event("workflow.started", 1)).unwrap();
        queue.push(create_test_event("output.delta", 2)).unwrap();

        let (queued, sent, dropped, errors) = queue.get_metrics();
        assert_eq!(queued, 2);
        assert_eq!(sent, 0);
        assert_eq!(dropped, 0);
        assert_eq!(errors, 0);

        // Record sends
        queue.record_sent(false); // boundary
        queue.record_sent(true);  // sse-only

        let metrics = queue.metrics();
        assert_eq!(metrics.events_sent, 2);
        assert_eq!(metrics.boundary_events_sent, 1);
        assert_eq!(metrics.sse_only_events_sent, 1);

        queue.record_error();
        let (_, _, _, errors) = queue.get_metrics();
        assert_eq!(errors, 1);
    }

    #[test]
    fn test_fifo_order() {
        let queue = JournalEventQueue::new(JournalQueueConfig::default());

        // Push in order
        for i in 1..=5 {
            queue.push(create_test_event("workflow.step.completed", i)).unwrap();
        }

        // Pop should be FIFO
        for i in 1..=5 {
            let event = queue.pop().unwrap();
            assert_eq!(event.sequence, i);
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_new_with_auto_classification() {
        let boundary = JournalEventMessage::new(
            "run-1".to_string(),
            "workflow.started".to_string(),
            b"{}".to_vec(),
        );
        assert!(!boundary.is_sse_only);

        let sse_only = JournalEventMessage::new(
            "run-1".to_string(),
            "output.delta".to_string(),
            b"hello".to_vec(),
        );
        assert!(sse_only.is_sse_only);
    }
}
