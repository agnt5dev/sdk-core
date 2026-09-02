//! Unified journal event queue for all event types
//!
//! This module provides a single buffered queue for all journal events, replacing
//! the fragmented CheckpointQueue, DeltaQueue, SpanExportQueue, and LogExportQueue.
//!
//! ## Architecture
//!
//! - **JournalEventQueue**: Thread-safe buffer with configurable max size
//! - **JournalEventMessage**: Unified event with classification flags
//! - **Overflow policy**: Drop telemetry only; reject correctness events when
//!   no telemetry slot can be reclaimed
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

use std::collections::{HashMap, HashSet, VecDeque};
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
            || event_type.starts_with("lm.content_block.")
            || event_type.starts_with("lm.message.")
            || event_type.starts_with("lm.thinking.")
            || event_type.starts_with("lm.tool_call.")  // LLM tool call content blocks (transient deltas)
            || event_type.starts_with("progress.")
            || event_type.starts_with("log") // log, log.info, log.warn, log.error, etc.
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
    pub fn new(run_id: String, event_type: String, data: Vec<u8>) -> Self {
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
/// Buffers all event types in memory and provides FIFO access. When the buffer
/// is full, only SSE-only telemetry can be evicted. Correctness events are
/// rejected if no telemetry slot can be reclaimed.
#[derive(Clone)]
pub struct JournalEventQueue {
    /// Indexed queue state protected by one short-lived mutex.
    queue: Arc<Mutex<JournalQueueState>>,
    /// Configuration
    config: JournalQueueConfig,
    /// Metrics for monitoring queue health
    metrics: Arc<Mutex<JournalQueueMetrics>>,
    /// Runs whose queued events are being held for their `CompleteJob` bundle.
    /// The periodic flusher never selects a held run; an explicit per-run
    /// drain (an inline checkpoint, the completion itself) still takes them.
    held_runs: Arc<Mutex<HashSet<String>>>,
}

#[derive(Default)]
struct JournalQueueState {
    /// Global FIFO order. IDs drained through the per-run index remain as
    /// tombstones until an ordinary drain encounters them or compaction runs.
    order: VecDeque<u64>,
    events: HashMap<u64, JournalEventMessage>,
    by_run: HashMap<String, VecDeque<u64>>,
    next_id: u64,
}

impl JournalQueueState {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            order: VecDeque::with_capacity(capacity),
            events: HashMap::with_capacity(capacity),
            by_run: HashMap::new(),
            next_id: 0,
        }
    }

    fn insert(&mut self, event: JournalEventMessage, front: bool) {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("journal queue event id exhausted");
        let run_id = event.run_id.clone();

        self.events.insert(id, event);
        if front {
            self.order.push_front(id);
            self.by_run.entry(run_id).or_default().push_front(id);
        } else {
            self.order.push_back(id);
            self.by_run.entry(run_id).or_default().push_back(id);
        }
    }

    fn remove_run_reference(&mut self, run_id: &str, id: u64) {
        let mut remove_run = false;
        if let Some(ids) = self.by_run.get_mut(run_id) {
            if ids.front() == Some(&id) {
                ids.pop_front();
            } else if let Some(position) = ids.iter().position(|queued_id| *queued_id == id) {
                ids.remove(position);
            }
            remove_run = ids.is_empty();
        }
        if remove_run {
            self.by_run.remove(run_id);
        }
    }

    fn remove_event(&mut self, id: u64) -> Option<JournalEventMessage> {
        let event = self.events.remove(&id)?;
        self.remove_run_reference(&event.run_id, id);
        Some(event)
    }

    fn maybe_compact_order(&mut self) {
        const MIN_TOMBSTONES_BEFORE_COMPACTION: usize = 1024;
        let tombstones = self.order.len().saturating_sub(self.events.len());
        if tombstones >= MIN_TOMBSTONES_BEFORE_COMPACTION
            && self.order.len() > self.events.len().saturating_mul(2)
        {
            self.order.retain(|id| self.events.contains_key(id));
        }
    }
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
            queue: Arc::new(Mutex::new(JournalQueueState::with_capacity(
                config.max_size,
            ))),
            config,
            metrics: Arc::new(Mutex::new(JournalQueueMetrics::default())),
            held_runs: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Push an event to the queue
    ///
    /// If the queue is at capacity, the oldest SSE-only event is dropped. A
    /// correctness event is never evicted: if the queue contains only
    /// correctness events, a new correctness event is rejected and new
    /// telemetry is dropped.
    pub fn push(&self, event: JournalEventMessage) -> Result<(), String> {
        let mut queue = self
            .queue
            .lock()
            .map_err(|e| format!("Failed to lock journal queue for push: {}", e))?;

        // Reclaim telemetry until the buffer has room. push_front() may have
        // temporarily taken the queue over capacity while preserving a failed
        // correctness batch, so this can require more than one eviction.
        while queue.events.len() >= self.config.max_size {
            let oldest_telemetry = queue.order.iter().enumerate().find_map(|(index, id)| {
                queue.events.get(id).and_then(|queued| {
                    JournalEventMessage::is_sse_only_event_type(&queued.event_type)
                        .then_some((index, *id))
                })
            });

            if let Some((index, id)) = oldest_telemetry {
                queue.order.remove(index);
                let dropped = queue
                    .remove_event(id)
                    .expect("telemetry id came from the same queue");
                log::warn!(
                    "Journal queue full ({}), dropped oldest telemetry event: type={} run_id={}",
                    self.config.max_size,
                    dropped.event_type,
                    dropped.run_id
                );

                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.events_dropped += 1;
                }
            } else if JournalEventMessage::is_sse_only_event_type(&event.event_type) {
                log::warn!(
                    "Journal queue full ({}), dropped incoming telemetry event: type={} run_id={}",
                    self.config.max_size,
                    event.event_type,
                    event.run_id
                );
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.events_dropped += 1;
                }
                return Ok(());
            } else {
                return Err(format!(
                    "Journal queue full ({}) with correctness events; rejected event: type={} run_id={}",
                    self.config.max_size, event.event_type, event.run_id
                ));
            }
        }

        log::debug!(
            "Queued journal event: type={} run_id={} is_sse_only={} queue_size={}",
            event.event_type,
            event.run_id,
            event.is_sse_only,
            queue.events.len() + 1
        );

        queue.insert(event, false);

        // Update metrics
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.events_queued += 1;
        }

        Ok(())
    }

    /// Pop the next event from the queue (FIFO)
    pub fn pop(&self) -> Option<JournalEventMessage> {
        let mut queue = self.queue.lock().ok()?;
        while let Some(id) = queue.order.pop_front() {
            if let Some(event) = queue.remove_event(id) {
                return Some(event);
            }
        }
        None
    }

    /// Re-queue an event at the front (used when send fails)
    pub fn push_front(&self, event: JournalEventMessage) -> Result<(), String> {
        let mut queue = self
            .queue
            .lock()
            .map_err(|e| format!("Failed to lock journal queue for push_front: {}", e))?;

        log::debug!(
            "Re-queuing event at front: type={} run_id={}",
            event.event_type,
            event.run_id
        );

        queue.insert(event, true);
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

        let mut batch = Vec::with_capacity(std::cmp::min(max, queue.events.len()));

        while batch.len() < max {
            let Some(id) = queue.order.pop_front() else {
                break;
            };
            if let Some(event) = queue.remove_event(id) {
                batch.push(event);
            }
        }

        if !batch.is_empty() {
            log::debug!(
                "Drained {} events from journal queue (remaining={})",
                batch.len(),
                queue.events.len()
            );
        }

        batch
    }

    /// Return the distinct run IDs represented by the next `max` live events.
    /// The background sender uses this to acquire ordering barriers before it
    /// removes those events from the queue.
    pub fn peek_batch_run_ids(&self, max: usize) -> Vec<String> {
        let queue = match self.queue.lock() {
            Ok(queue) => queue,
            Err(error) => {
                log::error!("Failed to lock journal queue for peek_batch_run_ids: {error}");
                return Vec::new();
            }
        };
        let held = self.held_runs_snapshot();

        let mut run_ids = Vec::new();
        let mut seen = HashSet::new();
        for event in queue
            .order
            .iter()
            .filter_map(|id| queue.events.get(id))
            .take(max)
        {
            if held.contains(&event.run_id) {
                continue;
            }
            if seen.insert(event.run_id.clone()) {
                run_ids.push(event.run_id.clone());
            }
        }
        run_ids
    }

    /// Keep `run_id`'s queued events out of the periodic flush until
    /// `release_run` or an explicit per-run drain.
    pub fn hold_run(&self, run_id: &str) {
        self.held_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(run_id.to_string());
    }

    /// Return the run to normal periodic flushing. Returns whether it was held.
    pub fn release_run(&self, run_id: &str) -> bool {
        self.held_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(run_id)
    }

    pub fn is_held(&self, run_id: &str) -> bool {
        self.held_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(run_id)
    }

    fn held_runs_snapshot(&self) -> HashSet<String> {
        self.held_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Drain up to `max` events belonging to runs whose ordering barriers are
    /// already held. Events from other runs retain their relative FIFO order.
    pub fn drain_batch_for_runs(
        &self,
        max: usize,
        allowed_run_ids: &HashSet<String>,
    ) -> Vec<JournalEventMessage> {
        let mut queue = match self.queue.lock() {
            Ok(queue) => queue,
            Err(error) => {
                log::error!("Failed to lock journal queue for drain_batch_for_runs: {error}");
                return Vec::new();
            }
        };
        let mut batch = Vec::with_capacity(std::cmp::min(max, queue.events.len()));
        let mut skipped = VecDeque::new();
        let mut examined = 0;

        while examined < max {
            let Some(id) = queue.order.pop_front() else {
                break;
            };
            let Some(event) = queue.events.get(&id) else {
                continue;
            };
            examined += 1;
            if allowed_run_ids.contains(&event.run_id) {
                if let Some(event) = queue.remove_event(id) {
                    batch.push(event);
                }
            } else {
                skipped.push_back(id);
            }
        }

        while let Some(id) = skipped.pop_back() {
            queue.order.push_front(id);
        }

        if !batch.is_empty() {
            log::debug!(
                "Drained {} barrier-protected events from journal queue (remaining={})",
                batch.len(),
                queue.events.len()
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

        let mut events = Vec::with_capacity(queue.events.len());
        while let Some(id) = queue.order.pop_front() {
            if let Some(event) = queue.remove_event(id) {
                events.push(event);
            }
        }
        queue.by_run.clear();
        queue.events.clear();

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

        let ids = queue.by_run.remove(run_id).unwrap_or_default();
        let mut matched = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(event) = queue.events.remove(&id) {
                matched.push(event);
            }
        }
        queue.maybe_compact_order();

        if !matched.is_empty() {
            log::debug!(
                "Drained {} events for run_id={} (remaining={})",
                matched.len(),
                run_id,
                queue.events.len()
            );
        }

        matched
    }

    /// Get current queue length
    pub fn len(&self) -> usize {
        self.queue.lock().map(|q| q.events.len()).unwrap_or(0)
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether this queue still contains an event for `run_id`.
    ///
    /// Pull completion uses this while holding the run's flush barrier: an
    /// empty result then means no flush for that run is queued or in flight.
    pub fn contains_run(&self, run_id: &str) -> bool {
        self.queue
            .lock()
            .map(|queue| queue.by_run.contains_key(run_id))
            .unwrap_or(true)
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
        self.metrics.lock().map(|m| m.clone()).unwrap_or_default()
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
        queue
            .order
            .iter()
            .find_map(|id| queue.events.get(id))
            .map(|event| event.queued_at.elapsed())
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

    fn create_run_event(run_id: &str, event_type: &str, seq: i64) -> JournalEventMessage {
        JournalEventMessage {
            run_id: run_id.to_string(),
            ..create_test_event(event_type, seq)
        }
    }

    #[test]
    fn test_journal_queue_basic() {
        let queue = JournalEventQueue::new(JournalQueueConfig {
            max_size: 10,
            ..Default::default()
        });

        // Push event
        queue
            .push(create_test_event("workflow.started", 1))
            .unwrap();
        assert_eq!(queue.len(), 1);

        // Pop event
        let event = queue.pop().unwrap();
        assert_eq!(event.sequence, 1);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_journal_queue_overflow_drops_oldest_telemetry() {
        let queue = JournalEventQueue::new(JournalQueueConfig {
            max_size: 3,
            ..Default::default()
        });

        // Fill queue
        queue.push(create_test_event("output.delta", 1)).unwrap();
        queue.push(create_test_event("log.info", 2)).unwrap();
        queue.push(create_test_event("progress.update", 3)).unwrap();
        assert_eq!(queue.len(), 3);

        // Overflow - should drop oldest telemetry (seq=1)
        queue.push(create_test_event("output.delta", 4)).unwrap();
        assert_eq!(queue.len(), 3);

        // Verify oldest was dropped
        let event = queue.pop().unwrap();
        assert_eq!(event.sequence, 2);
        assert_eq!(queue.metrics().events_dropped, 1);
    }

    #[test]
    fn test_journal_queue_overflow_never_evicts_correctness_event() {
        let queue = JournalEventQueue::new(JournalQueueConfig {
            max_size: 3,
            ..Default::default()
        });

        queue
            .push(create_test_event("workflow.started", 1))
            .unwrap();
        queue.push(create_test_event("output.delta", 2)).unwrap();
        queue
            .push(create_test_event("workflow.step.started", 3))
            .unwrap();

        queue
            .push(create_test_event("workflow.step.completed", 4))
            .unwrap();

        let sequences: Vec<_> = queue
            .drain_all()
            .into_iter()
            .map(|event| event.sequence)
            .collect();
        assert_eq!(sequences, vec![1, 3, 4]);
        assert_eq!(queue.metrics().events_dropped, 1);
    }

    #[test]
    fn test_journal_queue_rejects_correctness_when_only_correctness_is_buffered() {
        let queue = JournalEventQueue::new(JournalQueueConfig {
            max_size: 2,
            ..Default::default()
        });

        queue
            .push(create_test_event("workflow.started", 1))
            .unwrap();
        queue
            .push(create_test_event("workflow.step.started", 2))
            .unwrap();

        let error = queue
            .push(create_test_event("workflow.step.completed", 3))
            .unwrap_err();

        assert!(error.contains("rejected event: type=workflow.step.completed"));
        let sequences: Vec<_> = queue
            .drain_all()
            .into_iter()
            .map(|event| event.sequence)
            .collect();
        assert_eq!(sequences, vec![1, 2]);
        assert_eq!(queue.metrics().events_dropped, 0);
    }

    #[test]
    fn test_journal_queue_drops_incoming_telemetry_before_correctness() {
        let queue = JournalEventQueue::new(JournalQueueConfig {
            max_size: 2,
            ..Default::default()
        });

        queue
            .push(create_test_event("workflow.started", 1))
            .unwrap();
        queue
            .push(create_test_event("workflow.step.started", 2))
            .unwrap();

        queue.push(create_test_event("output.delta", 3)).unwrap();

        let sequences: Vec<_> = queue
            .drain_all()
            .into_iter()
            .map(|event| event.sequence)
            .collect();
        assert_eq!(sequences, vec![1, 2]);
        assert_eq!(queue.metrics().events_dropped, 1);
    }

    #[test]
    fn test_sse_only_detection() {
        // Boundary events (persisted)
        assert!(!JournalEventMessage::is_sse_only_event_type(
            "workflow.started"
        ));
        assert!(!JournalEventMessage::is_sse_only_event_type(
            "agent.completed"
        ));
        assert!(!JournalEventMessage::is_sse_only_event_type(
            "lm.call.started"
        ));
        assert!(!JournalEventMessage::is_sse_only_event_type(
            "tool.call.completed"
        ));

        // SSE-only events (not persisted)
        assert!(JournalEventMessage::is_sse_only_event_type("output.delta"));
        assert!(JournalEventMessage::is_sse_only_event_type("output.start"));
        assert!(JournalEventMessage::is_sse_only_event_type("output.stop"));
        assert!(JournalEventMessage::is_sse_only_event_type(
            "lm.stream.delta"
        ));
        assert!(JournalEventMessage::is_sse_only_event_type(
            "lm.content_block.started"
        ));
        assert!(JournalEventMessage::is_sse_only_event_type(
            "lm.content_block.delta"
        ));
        assert!(JournalEventMessage::is_sse_only_event_type(
            "lm.content_block.completed"
        ));
        assert!(JournalEventMessage::is_sse_only_event_type(
            "lm.message.delta"
        ));
        assert!(JournalEventMessage::is_sse_only_event_type(
            "lm.thinking.delta"
        ));
        assert!(JournalEventMessage::is_sse_only_event_type(
            "lm.tool_call.start"
        ));
        assert!(JournalEventMessage::is_sse_only_event_type(
            "lm.tool_call.delta"
        ));
        assert!(JournalEventMessage::is_sse_only_event_type(
            "lm.tool_call.stop"
        ));
        assert!(JournalEventMessage::is_sse_only_event_type(
            "progress.update"
        ));
        assert!(JournalEventMessage::is_sse_only_event_type("log"));
        assert!(JournalEventMessage::is_sse_only_event_type("log.info"));
        assert!(JournalEventMessage::is_sse_only_event_type("log.warn"));
        assert!(JournalEventMessage::is_sse_only_event_type("log.error"));
    }

    #[test]
    fn test_checkpoint_event_detection() {
        // Checkpoint events (require sync ack) - inverse of SSE-only
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "workflow.started"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "workflow.completed"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "workflow.failed"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "workflow.paused"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "workflow.step.started"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "workflow.step.completed"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "workflow.step.paused"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "agent.started"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "agent.completed"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "approval.requested"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "approval.resolved"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "lm.call.started"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "lm.call.completed"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "lm.completed"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "tool.call.started"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "tool.call.completed"
        ));
        assert!(JournalEventMessage::is_checkpoint_event_type(
            "tool_call.started"
        ));

        // NOT checkpoint events (SSE-only)
        assert!(!JournalEventMessage::is_checkpoint_event_type(
            "output.delta"
        ));
        assert!(!JournalEventMessage::is_checkpoint_event_type(
            "lm.stream.delta"
        ));
        assert!(!JournalEventMessage::is_checkpoint_event_type(
            "lm.message.delta"
        ));
        assert!(!JournalEventMessage::is_checkpoint_event_type(
            "lm.thinking.delta"
        ));
        assert!(!JournalEventMessage::is_checkpoint_event_type(
            "progress.update"
        ));
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
            queue
                .push(create_test_event("workflow.step.completed", i))
                .unwrap();
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
    fn test_drain_run_events_uses_index_and_preserves_global_fifo() {
        let queue = JournalEventQueue::new(JournalQueueConfig {
            max_size: 10,
            ..Default::default()
        });

        queue
            .push(create_run_event("run-a", "output.delta", 1))
            .unwrap();
        queue
            .push(create_run_event("run-b", "output.delta", 2))
            .unwrap();
        queue
            .push(create_run_event("run-a", "workflow.step.completed", 3))
            .unwrap();
        queue
            .push(create_run_event("run-c", "output.delta", 4))
            .unwrap();

        let run_a: Vec<_> = queue
            .drain_run_events("run-a")
            .into_iter()
            .map(|event| event.sequence)
            .collect();
        assert_eq!(run_a, vec![1, 3]);
        assert!(!queue.contains_run("run-a"));
        assert!(queue.contains_run("run-b"));
        assert_eq!(queue.len(), 2);

        let remaining: Vec<_> = queue
            .drain_batch(10)
            .into_iter()
            .map(|event| event.sequence)
            .collect();
        assert_eq!(remaining, vec![2, 4]);
        assert!(queue.is_empty());
    }

    #[test]
    fn test_drain_batch_for_runs_does_not_take_unlocked_front_event() {
        let queue = JournalEventQueue::new(JournalQueueConfig {
            max_size: 10,
            ..Default::default()
        });
        queue
            .push(create_run_event("run-a", "output.delta", 1))
            .unwrap();
        queue
            .push(create_run_event("run-b", "output.delta", 2))
            .unwrap();
        queue
            .push(create_run_event("run-a", "output.delta", 3))
            .unwrap();

        assert_eq!(queue.peek_batch_run_ids(2), vec!["run-a", "run-b"]);

        let allowed = HashSet::from(["run-a".to_string()]);
        let drained: Vec<_> = queue
            .drain_batch_for_runs(2, &allowed)
            .into_iter()
            .map(|event| event.sequence)
            .collect();
        assert_eq!(drained, vec![1]);

        let remaining: Vec<_> = queue
            .drain_all()
            .into_iter()
            .map(|event| event.sequence)
            .collect();
        assert_eq!(remaining, vec![2, 3]);
    }

    #[test]
    fn held_runs_are_skipped_by_the_periodic_peek_but_drained_explicitly() {
        let queue = JournalEventQueue::new(JournalQueueConfig::default());
        queue.hold_run("held");
        for (run_id, event_type) in [
            ("held", "run.started"),
            ("other", "run.started"),
            ("held", "function.completed"),
        ] {
            queue
                .push(JournalEventMessage::new(
                    run_id.to_string(),
                    event_type.to_string(),
                    Vec::new(),
                ))
                .unwrap();
        }

        assert!(queue.is_held("held"));
        assert_eq!(queue.peek_batch_run_ids(10), vec!["other".to_string()]);
        let allowed: HashSet<String> = queue.peek_batch_run_ids(10).into_iter().collect();
        let periodic = queue.drain_batch_for_runs(10, &allowed);
        assert_eq!(periodic.len(), 1);
        assert_eq!(periodic[0].run_id, "other");
        assert!(queue.contains_run("held"), "held events stay queued");

        let held = queue.drain_run_events("held");
        assert_eq!(
            held.iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>(),
            ["run.started", "function.completed"]
        );
        assert!(queue.release_run("held"));
        assert!(!queue.release_run("held"));
        assert!(!queue.is_held("held"));
    }

    #[test]
    fn test_push_front() {
        let queue = JournalEventQueue::new(JournalQueueConfig::default());

        queue
            .push(create_test_event("workflow.started", 1))
            .unwrap();
        queue
            .push(create_test_event("workflow.completed", 2))
            .unwrap();

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

        queue
            .push(create_test_event("workflow.started", 1))
            .unwrap();
        queue.push(create_test_event("output.delta", 2)).unwrap();

        let (queued, sent, dropped, errors) = queue.get_metrics();
        assert_eq!(queued, 2);
        assert_eq!(sent, 0);
        assert_eq!(dropped, 0);
        assert_eq!(errors, 0);

        // Record sends
        queue.record_sent(false); // boundary
        queue.record_sent(true); // sse-only

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
            queue
                .push(create_test_event("workflow.step.completed", i))
                .unwrap();
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
