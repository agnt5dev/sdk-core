//! Span export queue for real-time SSE streaming
//!
//! This module provides a buffered queue for span export requests that are sent
//! during workflow execution for real-time SSE streaming of execution traces.
//!
//! ## Architecture
//!
//! - **SpanExportQueue**: Thread-safe buffer with configurable max size
//! - **SpanExportRequest**: Individual span data for journal export
//! - **Overflow policy**: Drop oldest when buffer full (FIFO)
//! - **Metrics**: Track queued, sent, dropped, errors
//!
//! ## Usage
//!
//! ```rust,ignore
//! let queue = SpanExportQueue::new(1000);
//!
//! // Queue span export (called from language SDK via FFI - sync, non-blocking)
//! queue.push(SpanExportRequest {
//!     run_id: "run-123".to_string(),
//!     tenant_id: Some("tenant-1".to_string()),
//!     trace_id: "abc123".to_string(),
//!     span_id: "def456".to_string(),
//!     name: "workflow.task.my_function".to_string(),
//!     kind: "function".to_string(),
//!     start_time_ns: 1234567890,
//!     end_time_ns: 1234567900,
//!     status_code: "ok".to_string(),
//!     ..Default::default()
//! })?;
//!
//! // Flush to journal (called by Worker in Tokio runtime - async)
//! while let Some(request) = queue.pop() {
//!     export_span_to_journal(&request.run_id, &span_data, ...).await?;
//! }
//! ```

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Individual span export request for journal streaming
#[derive(Debug, Clone)]
pub struct SpanExportRequest {
    /// Run ID (invocation ID) to associate the span with
    pub run_id: String,

    /// Optional tenant ID
    pub tenant_id: Option<String>,

    /// OpenTelemetry trace ID
    pub trace_id: String,

    /// OpenTelemetry span ID
    pub span_id: String,

    /// Parent span ID (if any)
    pub parent_span_id: Option<String>,

    /// Span name (e.g., "workflow.task.my_function")
    pub name: String,

    /// Span kind (e.g., "function", "workflow", "agent")
    pub kind: String,

    /// Start time in nanoseconds since Unix epoch
    pub start_time_ns: i64,

    /// End time in nanoseconds since Unix epoch
    pub end_time_ns: i64,

    /// Status code: "ok", "error", or "unset"
    pub status_code: String,

    /// Optional status description (usually error message)
    pub status_description: Option<String>,

    /// Optional span attributes
    pub attributes: Option<HashMap<String, String>>,

    /// When the request was queued (for metrics)
    pub queued_at: Instant,
}

impl Default for SpanExportRequest {
    fn default() -> Self {
        Self {
            run_id: String::new(),
            tenant_id: None,
            trace_id: String::new(),
            span_id: String::new(),
            parent_span_id: None,
            name: String::new(),
            kind: "internal".to_string(),
            start_time_ns: 0,
            end_time_ns: 0,
            status_code: "ok".to_string(),
            status_description: None,
            attributes: None,
            queued_at: Instant::now(),
        }
    }
}

/// Thread-safe span export queue with overflow protection
///
/// Buffers span export requests in memory and provides FIFO access with automatic
/// oldest-request dropping when buffer is full.
///
/// This queue uses `std::sync::Mutex` (NOT `tokio::sync::Mutex`) so it can be
/// safely used from any thread, including Python FFI callbacks that don't have
/// access to a Tokio runtime.
#[derive(Clone)]
pub struct SpanExportQueue {
    /// Internal queue protected by mutex
    queue: Arc<Mutex<VecDeque<SpanExportRequest>>>,

    /// Maximum number of requests to buffer
    max_size: usize,

    /// Metrics for monitoring queue health
    metrics: Arc<Mutex<SpanExportMetrics>>,
}

/// Metrics for span export queue monitoring
#[derive(Debug, Default)]
struct SpanExportMetrics {
    /// Total spans queued
    spans_queued: u64,

    /// Total spans successfully sent
    spans_sent: u64,

    /// Total spans dropped due to overflow
    spans_dropped: u64,

    /// Total send errors
    send_errors: u64,
}

impl SpanExportQueue {
    /// Create a new span export queue with specified maximum size
    ///
    /// # Arguments
    ///
    /// * `max_size` - Maximum number of span requests to buffer (recommended: 1000)
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let queue = SpanExportQueue::new(1000);
    /// ```
    pub fn new(max_size: usize) -> Self {
        log::info!("Creating span export queue with max_size={}", max_size);

        SpanExportQueue {
            queue: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            metrics: Arc::new(Mutex::new(SpanExportMetrics::default())),
        }
    }

    /// Push a span export request to the queue (sync, non-blocking)
    ///
    /// If the queue is at capacity, the oldest request is dropped (FIFO).
    /// This method is safe to call from any thread, including Python FFI callbacks.
    ///
    /// # Arguments
    ///
    /// * `request` - Span export request to queue
    ///
    /// # Returns
    ///
    /// `Ok(())` on success
    pub fn push(&self, request: SpanExportRequest) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut queue = self.queue.lock().map_err(|e| {
            format!("Failed to lock span export queue for push: {}", e)
        })?;

        // Check if buffer is full
        if queue.len() >= self.max_size {
            // Drop oldest request to make room
            if let Some(dropped) = queue.pop_front() {
                log::warn!(
                    "Span export queue full ({}), dropped oldest span: name={} run_id={}",
                    self.max_size,
                    dropped.name,
                    dropped.run_id
                );

                // Update metrics
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.spans_dropped += 1;
                }
            }
        }

        // Add request to queue
        log::debug!(
            "Queued span export: name={} run_id={} queue_size={}",
            request.name,
            request.run_id,
            queue.len() + 1
        );

        queue.push_back(request);

        // Update metrics
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.spans_queued += 1;
        }

        Ok(())
    }

    /// Pop the next span export request from the queue (FIFO)
    ///
    /// # Returns
    ///
    /// `Some(request)` if queue is not empty, `None` otherwise
    pub fn pop(&self) -> Option<SpanExportRequest> {
        let mut queue = self.queue.lock().ok()?;
        queue.pop_front()
    }

    /// Get current queue length
    ///
    /// # Returns
    ///
    /// Number of span requests currently buffered
    pub fn len(&self) -> usize {
        self.queue.lock().map(|q| q.len()).unwrap_or(0)
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Record a successful span export (for metrics)
    pub fn record_sent(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.spans_sent += 1;
        }
    }

    /// Record a span export error (for metrics)
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
                metrics.spans_queued,
                metrics.spans_sent,
                metrics.spans_dropped,
                metrics.send_errors,
            )
        } else {
            (0, 0, 0, 0)
        }
    }

    /// Get age of oldest span request in queue
    ///
    /// # Returns
    ///
    /// Duration since oldest request was queued, or None if queue is empty
    pub fn oldest_age(&self) -> Option<std::time::Duration> {
        let queue = self.queue.lock().ok()?;
        queue.front().map(|request| request.queued_at.elapsed())
    }

    /// Drain all span requests from the queue
    ///
    /// This method removes and returns all queued requests in FIFO order.
    /// Used for synchronous flushing before workflow completion.
    ///
    /// # Returns
    ///
    /// Vector of all queued requests (empty if queue is empty)
    pub fn drain_all(&self) -> Vec<SpanExportRequest> {
        let mut queue = match self.queue.lock() {
            Ok(q) => q,
            Err(e) => {
                log::error!("Failed to lock span export queue for drain: {}", e);
                return Vec::new();
            }
        };

        let mut requests = Vec::with_capacity(queue.len());
        while let Some(request) = queue.pop_front() {
            requests.push(request);
        }

        log::debug!("Drained {} span export requests from queue", requests.len());
        requests
    }
}

// ============================================================================
// Log Export Queue
// ============================================================================

/// Individual log export request for journal streaming
#[derive(Debug, Clone)]
pub struct LogExportRequest {
    /// Run ID (invocation ID) to associate the log with
    pub run_id: String,

    /// Optional tenant ID
    pub tenant_id: Option<String>,

    /// OpenTelemetry trace ID
    pub trace_id: String,

    /// OpenTelemetry span ID
    pub span_id: String,

    /// Log timestamp in nanoseconds since Unix epoch
    pub timestamp_ns: i64,

    /// Log severity: "trace", "debug", "info", "warn", "error"
    pub severity: String,

    /// Log message body
    pub body: String,

    /// Optional log attributes
    pub attributes: Option<HashMap<String, String>>,

    /// When the request was queued (for metrics)
    pub queued_at: Instant,
}

impl Default for LogExportRequest {
    fn default() -> Self {
        Self {
            run_id: String::new(),
            tenant_id: None,
            trace_id: String::new(),
            span_id: String::new(),
            timestamp_ns: 0,
            severity: "info".to_string(),
            body: String::new(),
            attributes: None,
            queued_at: Instant::now(),
        }
    }
}

/// Thread-safe log export queue with overflow protection
///
/// Buffers log export requests in memory and provides FIFO access with automatic
/// oldest-request dropping when buffer is full.
///
/// This queue uses `std::sync::Mutex` (NOT `tokio::sync::Mutex`) so it can be
/// safely used from any thread, including Python FFI callbacks that don't have
/// access to a Tokio runtime.
#[derive(Clone)]
pub struct LogExportQueue {
    /// Internal queue protected by mutex
    queue: Arc<Mutex<VecDeque<LogExportRequest>>>,

    /// Maximum number of requests to buffer
    max_size: usize,

    /// Metrics for monitoring queue health
    metrics: Arc<Mutex<LogExportMetrics>>,
}

/// Metrics for log export queue monitoring
#[derive(Debug, Default)]
struct LogExportMetrics {
    /// Total logs queued
    logs_queued: u64,

    /// Total logs successfully sent
    logs_sent: u64,

    /// Total logs dropped due to overflow
    logs_dropped: u64,

    /// Total send errors
    send_errors: u64,
}

impl LogExportQueue {
    /// Create a new log export queue with specified maximum size
    ///
    /// # Arguments
    ///
    /// * `max_size` - Maximum number of log requests to buffer (recommended: 5000)
    pub fn new(max_size: usize) -> Self {
        log::info!("Creating log export queue with max_size={}", max_size);

        LogExportQueue {
            queue: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            metrics: Arc::new(Mutex::new(LogExportMetrics::default())),
        }
    }

    /// Push a log export request to the queue (sync, non-blocking)
    ///
    /// If the queue is at capacity, the oldest request is dropped (FIFO).
    /// This method is safe to call from any thread, including Python FFI callbacks.
    pub fn push(&self, request: LogExportRequest) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut queue = self.queue.lock().map_err(|e| {
            format!("Failed to lock log export queue for push: {}", e)
        })?;

        // Check if buffer is full
        if queue.len() >= self.max_size {
            // Drop oldest request to make room
            if let Some(dropped) = queue.pop_front() {
                log::warn!(
                    "Log export queue full ({}), dropped oldest log: severity={} run_id={}",
                    self.max_size,
                    dropped.severity,
                    dropped.run_id
                );

                // Update metrics
                if let Ok(mut metrics) = self.metrics.lock() {
                    metrics.logs_dropped += 1;
                }
            }
        }

        // Add request to queue
        log::trace!(
            "Queued log export: severity={} run_id={} queue_size={}",
            request.severity,
            request.run_id,
            queue.len() + 1
        );

        queue.push_back(request);

        // Update metrics
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.logs_queued += 1;
        }

        Ok(())
    }

    /// Pop the next log export request from the queue (FIFO)
    pub fn pop(&self) -> Option<LogExportRequest> {
        let mut queue = self.queue.lock().ok()?;
        queue.pop_front()
    }

    /// Get current queue length
    pub fn len(&self) -> usize {
        self.queue.lock().map(|q| q.len()).unwrap_or(0)
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Record a successful log export (for metrics)
    pub fn record_sent(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.logs_sent += 1;
        }
    }

    /// Record a log export error (for metrics)
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
                metrics.logs_queued,
                metrics.logs_sent,
                metrics.logs_dropped,
                metrics.send_errors,
            )
        } else {
            (0, 0, 0, 0)
        }
    }

    /// Drain all log requests from the queue
    pub fn drain_all(&self) -> Vec<LogExportRequest> {
        let mut queue = match self.queue.lock() {
            Ok(q) => q,
            Err(e) => {
                log::error!("Failed to lock log export queue for drain: {}", e);
                return Vec::new();
            }
        };

        let mut requests = Vec::with_capacity(queue.len());
        while let Some(request) = queue.pop_front() {
            requests.push(request);
        }

        log::debug!("Drained {} log export requests from queue", requests.len());
        requests
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_request(name: &str) -> SpanExportRequest {
        SpanExportRequest {
            run_id: "test-run".to_string(),
            tenant_id: Some("test-tenant".to_string()),
            trace_id: "trace-123".to_string(),
            span_id: "span-456".to_string(),
            parent_span_id: None,
            name: name.to_string(),
            kind: "function".to_string(),
            start_time_ns: 1000,
            end_time_ns: 2000,
            status_code: "ok".to_string(),
            status_description: None,
            attributes: None,
            queued_at: Instant::now(),
        }
    }

    #[test]
    fn test_span_export_queue_basic() {
        let queue = SpanExportQueue::new(10);

        // Push request
        queue.push(create_test_request("span-1")).unwrap();
        assert_eq!(queue.len(), 1);

        // Pop request
        let request = queue.pop().unwrap();
        assert_eq!(request.name, "span-1");
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_span_export_queue_overflow() {
        let queue = SpanExportQueue::new(3);

        // Fill queue
        queue.push(create_test_request("span-1")).unwrap();
        queue.push(create_test_request("span-2")).unwrap();
        queue.push(create_test_request("span-3")).unwrap();
        assert_eq!(queue.len(), 3);

        // Overflow - should drop oldest (span-1)
        queue.push(create_test_request("span-4")).unwrap();
        assert_eq!(queue.len(), 3);

        // Verify oldest was dropped
        let request = queue.pop().unwrap();
        assert_eq!(request.name, "span-2"); // span-1 was dropped
    }

    #[test]
    fn test_span_export_queue_metrics() {
        let queue = SpanExportQueue::new(10);

        queue.push(create_test_request("span-1")).unwrap();
        queue.push(create_test_request("span-2")).unwrap();

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
    fn test_span_export_queue_fifo_order() {
        let queue = SpanExportQueue::new(10);

        // Push in order
        for i in 1..=5 {
            queue.push(create_test_request(&format!("span-{}", i))).unwrap();
        }

        // Pop should be FIFO
        for i in 1..=5 {
            let request = queue.pop().unwrap();
            assert_eq!(request.name, format!("span-{}", i));
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_span_export_queue_drain() {
        let queue = SpanExportQueue::new(10);

        queue.push(create_test_request("span-1")).unwrap();
        queue.push(create_test_request("span-2")).unwrap();
        queue.push(create_test_request("span-3")).unwrap();

        let drained = queue.drain_all();
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].name, "span-1");
        assert_eq!(drained[1].name, "span-2");
        assert_eq!(drained[2].name, "span-3");

        assert!(queue.is_empty());
    }
}
