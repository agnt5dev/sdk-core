use crate::client::WorkerCoordinatorClient;
use crate::error::Result;
use crate::journal_queue::{JournalEventMessage, JournalEventQueue, JournalQueueConfig};
use crate::pb::{
    CheckpointAck, ComponentInfo, DispatchComponentResponse, HealthCheck, RegisterService,
    RuntimeMessage, RuntimeMessageType, ServiceMessage, UnregisterService, WorkerHealthStatus,
    WorkflowCheckpoint,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Key for tracking pending checkpoint acknowledgements
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct PendingAckKey {
    run_id: String,
    sequence_number: i64,
}

/// Pending acknowledgement tracker for async checkpoint events (used by async emit_checkpoint_sync)
type PendingAcks = Arc<TokioMutex<HashMap<PendingAckKey, oneshot::Sender<CheckpointAck>>>>;

/// Pending acknowledgement tracker for truly synchronous checkpoint events
/// Uses std::sync primitives for blocking operations from sync Python code
type SyncPendingAcks = Arc<std::sync::Mutex<HashMap<PendingAckKey, std::sync::mpsc::Sender<CheckpointAck>>>>;

/// Connection states for tracking worker status
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Error(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkerConfig {
    pub service_name: String,
    pub service_version: String,
    pub service_type: String,

    pub worker_id: String,
    pub coordinator_endpoint: String,
}

impl WorkerConfig {
    pub fn new(service_name: String, service_version: String, service_type: String) -> Self {
        // Generate a default worker ID, but allow override from environment
        let default_worker_id = Uuid::new_v4().to_string();
        let worker_id = std::env::var("AGNT5_WORKER_ID").unwrap_or_else(|_| default_worker_id);

        let coordinator_endpoint = std::env::var("AGNT5_COORDINATOR_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:34186".to_string());

        Self {
            service_name,
            service_version,
            service_type,
            worker_id,
            coordinator_endpoint,
        }
    }
}

/// Blacklist patterns for sensitive environment variables
/// These patterns are checked (case-insensitive) to prevent leaking credentials
pub const AGNT5_METADATA_BLACKLIST_PATTERNS: &[&str] = &[
    "_KEY",
    "_SECRET",
    "_TOKEN",
    "_PASSWORD",
    "_CREDENTIAL",
    "_API_KEY",
    "_AUTH_TOKEN",
    "_PRIVATE_KEY",
];

/// Check if an environment variable should be excluded from metadata
/// Returns true if the variable name matches any blacklist pattern
pub fn is_sensitive_env_var(key: &str) -> bool {
    let key_upper = key.to_uppercase();
    AGNT5_METADATA_BLACKLIST_PATTERNS
        .iter()
        .any(|pattern| key_upper.ends_with(pattern))
}

/// Collect all AGNT5_* environment variables for registration metadata
/// Excludes sensitive variables based on blacklist patterns
pub fn collect_agnt5_env_vars() -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    for (key, value) in std::env::vars() {
        if key.starts_with("AGNT5_") && !is_sensitive_env_var(&key) {
            metadata.insert(key, value);
        }
    }
    metadata
}

#[derive(Clone)]
pub struct Worker {
    config: WorkerConfig,
    components: Vec<ComponentInfo>,
    metadata: HashMap<String, String>,
    connection_state: Arc<std::sync::Mutex<ConnectionState>>,
    /// Unified journal event queue (replaces checkpoint_queue, delta_queue, span_export_queue, log_export_queue)
    journal_queue: JournalEventQueue,
    /// Pending checkpoint acknowledgements for async emit (tokio oneshot)
    pending_acks: PendingAcks,
    /// Sender for checkpoint events that need synchronous acknowledgement (async version)
    checkpoint_tx: Arc<TokioMutex<Option<flume::Sender<ServiceMessage>>>>,
    /// Sender for checkpoint events - sync version using std::sync::Mutex
    /// This allows truly blocking sends from sync Python code
    sync_checkpoint_tx: Arc<std::sync::Mutex<Option<flume::Sender<ServiceMessage>>>>,
    /// Pending checkpoint acknowledgements for truly synchronous emit (std::sync::mpsc)
    /// Uses blocking channels so sync Python code can wait without async runtime
    sync_pending_acks: SyncPendingAcks,
}

// Implement Debug manually to avoid requiring Debug on JournalEventQueue's internals
impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("config", &self.config)
            .field("components", &self.components)
            .field("metadata", &self.metadata)
            .field("connection_state", &self.connection_state)
            .field("journal_queue_size", &self.journal_queue.len())
            .field("pending_acks", &"<PendingAcks>")
            .field("sync_pending_acks", &"<SyncPendingAcks>")
            .finish()
    }
}

impl Worker {
    /// Create a new worker
    pub fn new(
        config: WorkerConfig,
        components: Vec<ComponentInfo>,
        metadata: HashMap<String, String>,
    ) -> Self {
        // Create unified journal queue with config from environment
        let journal_config = JournalQueueConfig::from_env();

        debug!(
            "Creating worker with unified journal queue: max_size={}, batch_size={}, flush_interval_ms={}",
            journal_config.max_size, journal_config.batch_size, journal_config.flush_interval_ms
        );

        Self {
            config,
            components,
            metadata,
            connection_state: Arc::new(std::sync::Mutex::new(ConnectionState::Disconnected)),
            journal_queue: JournalEventQueue::new(journal_config),
            pending_acks: Arc::new(TokioMutex::new(HashMap::new())),
            checkpoint_tx: Arc::new(TokioMutex::new(None)),
            sync_checkpoint_tx: Arc::new(std::sync::Mutex::new(None)),
            sync_pending_acks: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Get a clone of the journal event queue for use by language SDKs
    pub fn journal_queue(&self) -> JournalEventQueue {
        self.journal_queue.clone()
    }

    /// Set components for the worker
    pub fn set_components(&mut self, components: Vec<ComponentInfo>) {
        self.components = components;
    }

    /// Update service metadata
    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) {
        self.metadata = metadata;
    }

    /// Get current connection state
    pub fn connection_state(&self) -> ConnectionState {
        self.connection_state
            .lock()
            .unwrap_or_else(|poisoned| {
                warn!("Connection state mutex poisoned, recovering");
                poisoned.into_inner()
            })
            .clone()
    }

    /// Set connection state
    fn set_connection_state(&self, state: ConnectionState) {
        let mut guard = self.connection_state.lock().unwrap_or_else(|poisoned| {
            warn!("Connection state mutex poisoned during set, recovering");
            poisoned.into_inner()
        });
        *guard = state;
    }

    /// Queue a journal event for delivery to the platform
    ///
    /// This is the unified method for queueing all event types. Events are classified as:
    /// - Boundary events: Persisted to journal_events table (workflow.*, agent.*, lm.call.*, etc.)
    /// - SSE-only events: Forwarded to SSE stream but NOT persisted (output.delta, log, etc.)
    ///
    /// # Arguments
    ///
    /// * `event` - The journal event message to queue
    pub fn queue_event(&self, event: JournalEventMessage) -> Result<()> {
        self.journal_queue.push(event).map_err(|e| {
            crate::error::SdkError::Internal(format!("Failed to queue journal event: {}", e))
        })?;
        Ok(())
    }

    /// Queue a workflow checkpoint for progressive durability (legacy API)
    ///
    /// This method wraps the unified queue_event for backward compatibility.
    /// Use queue_event directly for new code.
    pub fn queue_checkpoint(
        &self,
        invocation_id: String,
        checkpoint_type: String,
        checkpoint_data: Vec<u8>,
        sequence_number: i64,
        metadata: HashMap<String, String>,
        source_timestamp_ns: i64,
        correlation_id: String,
        parent_correlation_id: String,
    ) -> Result<()> {
        let event = JournalEventMessage {
            run_id: invocation_id,
            event_type: checkpoint_type,
            data: checkpoint_data,
            sequence: sequence_number,
            metadata,
            source_timestamp_ns,
            correlation_id,
            parent_correlation_id,
            is_sse_only: false, // Checkpoints are boundary events (persisted)
            queued_at: std::time::Instant::now(),
            ..Default::default()
        };

        self.queue_event(event)
    }

    /// Get journal queue metrics
    ///
    /// Returns (queued, sent, dropped, errors)
    pub fn journal_metrics(&self) -> (u64, u64, u64, u64) {
        self.journal_queue.get_metrics()
    }

    /// Drain all buffered events for synchronous flushing
    ///
    /// This method removes and returns all queued events.
    /// Used before sending workflow completion response to ensure
    /// events arrive before run.completed event.
    pub fn drain_events(&self) -> Vec<JournalEventMessage> {
        self.journal_queue.drain_all()
    }

    /// Emit a checkpoint event synchronously and wait for acknowledgement
    ///
    /// This method blocks until the platform acknowledges that the event has been
    /// persisted to the journal. This ensures correct event ordering for lifecycle
    /// events that affect workflow state.
    ///
    /// # Arguments
    ///
    /// * `run_id` - The run ID this checkpoint belongs to
    /// * `event_type` - The event type (e.g., "approval.requested", "workflow.step.paused")
    /// * `event_data` - JSON-encoded event payload
    /// * `sequence_number` - Sequence number for ordering within execution
    /// * `metadata` - Additional metadata
    /// * `source_timestamp_ns` - Nanosecond timestamp when event was created
    /// * `timeout_ms` - Timeout in milliseconds to wait for acknowledgement
    ///
    /// # Returns
    ///
    /// Ok(()) if the checkpoint was acknowledged, or an error if:
    /// - The connection is not established
    /// - The timeout was reached
    /// - The send failed
    pub async fn emit_checkpoint_sync(
        &self,
        run_id: String,
        event_type: String,
        event_data: Vec<u8>,
        sequence_number: i64,
        metadata: HashMap<String, String>,
        source_timestamp_ns: i64,
        timeout_ms: u64,
    ) -> Result<()> {
        // Get the checkpoint sender (must be connected)
        let tx = {
            let guard = self.checkpoint_tx.lock().await;
            guard.clone().ok_or_else(|| {
                crate::error::SdkError::Connection {
                    message: "Worker not connected, cannot emit checkpoint".to_string(),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?
        };

        // Create oneshot channel for ack
        let (ack_tx, ack_rx) = oneshot::channel();

        // Register pending ack
        let key = PendingAckKey {
            run_id: run_id.clone(),
            sequence_number,
        };
        {
            let mut pending = self.pending_acks.lock().await;
            pending.insert(key.clone(), ack_tx);
        }

        // Merge service metadata (tenant_id, deployment_id) with passed metadata
        // Service metadata provides authoritative tenant/deployment info needed for journal writes
        let mut merged_metadata = metadata;
        for (key, value) in &self.metadata {
            if !merged_metadata.contains_key(key) {
                merged_metadata.insert(key.clone(), value.clone());
            }
        }

        // Build the checkpoint message
        let checkpoint = WorkflowCheckpoint {
            invocation_id: run_id.clone(),
            checkpoint_type: event_type.clone(),
            checkpoint_data: event_data,
            sequence_number,
            metadata: merged_metadata,
            source_timestamp_ns,
        };

        let service_message = ServiceMessage {
            worker_id: self.config.worker_id.clone(),
            metadata: std::collections::HashMap::new(),
            message_type: Some(
                crate::pb::service_message::MessageType::WorkflowCheckpoint(checkpoint),
            ),
        };

        // Send the checkpoint
        if let Err(e) = tx.send_async(service_message).await {
            // Remove pending ack on send failure
            let mut pending = self.pending_acks.lock().await;
            pending.remove(&key);
            return Err(crate::error::SdkError::Connection {
                message: format!("Failed to send checkpoint: {}", e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            });
        }

        debug!(
            "Sent checkpoint event, waiting for ack: run_id={} event_type={} seq={}",
            run_id, event_type, sequence_number
        );

        // Wait for ack with timeout
        let timeout = Duration::from_millis(timeout_ms);
        match tokio::time::timeout(timeout, ack_rx).await {
            Ok(Ok(ack)) => {
                debug!(
                    "Received checkpoint ack: run_id={} event_type={} seq={}",
                    ack.run_id, ack.event_type, ack.sequence_number
                );
                Ok(())
            }
            Ok(Err(_)) => {
                // Channel was dropped (sender gone)
                warn!(
                    "Checkpoint ack channel dropped: run_id={} event_type={} seq={}",
                    run_id, event_type, sequence_number
                );
                // Remove from pending (may have been cleaned up already)
                let mut pending = self.pending_acks.lock().await;
                pending.remove(&key);
                Err(crate::error::SdkError::Internal(
                    "Checkpoint ack channel dropped".to_string(),
                ))
            }
            Err(_) => {
                // Timeout
                warn!(
                    "Checkpoint ack timeout after {}ms: run_id={} event_type={} seq={} (platform may not support acks yet)",
                    timeout_ms, run_id, event_type, sequence_number
                );
                // Remove pending ack on timeout
                let mut pending = self.pending_acks.lock().await;
                pending.remove(&key);
                // Return Ok for graceful degradation with old platforms
                // The event was sent, we just didn't get confirmation
                Ok(())
            }
        }
    }

    /// Emit a checkpoint event and block until the platform acknowledges it (TRULY SYNCHRONOUS)
    ///
    /// This is the sync version that can be called from non-async Python code.
    /// It uses std::sync primitives to block the calling thread until the ack is received.
    ///
    /// # Arguments
    ///
    /// * `run_id` - The run/invocation ID this checkpoint belongs to
    /// * `event_type` - The checkpoint event type (e.g., "approval.requested", "workflow.paused")
    /// * `event_data` - The event payload as bytes
    /// * `sequence_number` - Sequence number for ordering
    /// * `metadata` - Additional metadata for the event
    /// * `source_timestamp_ns` - Nanosecond timestamp when event was created
    /// * `timeout_ms` - Timeout in milliseconds to wait for acknowledgement
    ///
    /// # Returns
    ///
    /// Ok(()) if the checkpoint was acknowledged, or an error if:
    /// - The connection is not established
    /// - The timeout was reached
    /// - The send failed
    pub fn emit_checkpoint_sync_blocking(
        &self,
        run_id: String,
        event_type: String,
        event_data: Vec<u8>,
        sequence_number: i64,
        metadata: HashMap<String, String>,
        source_timestamp_ns: i64,
        timeout_ms: u64,
    ) -> Result<()> {
        // Get the checkpoint sender (must be connected)
        // Using std::sync::Mutex so this can be called from sync code
        let tx = {
            let guard = self.sync_checkpoint_tx.lock().map_err(|e| {
                crate::error::SdkError::Internal(format!("Failed to lock sync_checkpoint_tx: {}", e))
            })?;
            guard.clone().ok_or_else(|| {
                crate::error::SdkError::Connection {
                    message: "Worker not connected, cannot emit checkpoint".to_string(),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?
        };

        // Create sync channel for ack (std::sync::mpsc)
        let (ack_tx, ack_rx) = std::sync::mpsc::channel();

        // Register pending ack in the sync map
        let key = PendingAckKey {
            run_id: run_id.clone(),
            sequence_number,
        };
        {
            let mut pending = self.sync_pending_acks.lock().map_err(|e| {
                crate::error::SdkError::Internal(format!("Failed to lock sync_pending_acks: {}", e))
            })?;
            pending.insert(key.clone(), ack_tx);
        }

        // Merge service metadata (tenant_id, deployment_id) with passed metadata
        // Service metadata provides authoritative tenant/deployment info needed for journal writes
        let mut merged_metadata = metadata;

        for (key, value) in &self.metadata {
            if !merged_metadata.contains_key(key) {
                merged_metadata.insert(key.clone(), value.clone());
            }
        }

        // Build the checkpoint message
        let checkpoint = WorkflowCheckpoint {
            invocation_id: run_id.clone(),
            checkpoint_type: event_type.clone(),
            checkpoint_data: event_data,
            sequence_number,
            metadata: merged_metadata,
            source_timestamp_ns,
        };

        let service_message = ServiceMessage {
            worker_id: self.config.worker_id.clone(),
            metadata: std::collections::HashMap::new(),
            message_type: Some(
                crate::pb::service_message::MessageType::WorkflowCheckpoint(checkpoint),
            ),
        };

        // Send the checkpoint using blocking send (flume supports this)
        if let Err(e) = tx.send(service_message) {
            // Remove pending ack on send failure
            if let Ok(mut pending) = self.sync_pending_acks.lock() {
                pending.remove(&key);
            }
            return Err(crate::error::SdkError::Connection {
                message: format!("Failed to send checkpoint: {}", e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            });
        }

        debug!(
            "Sent checkpoint event (sync blocking), waiting for ack: run_id={} event_type={} seq={}",
            run_id, event_type, sequence_number
        );

        // Wait for ack with timeout using blocking recv
        let timeout = Duration::from_millis(timeout_ms);
        match ack_rx.recv_timeout(timeout) {
            Ok(ack) => {
                debug!(
                    "Received checkpoint ack (sync blocking): run_id={} event_type={} seq={}",
                    ack.run_id, ack.event_type, ack.sequence_number
                );
                Ok(())
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                warn!(
                    "Checkpoint ack timeout (sync blocking) after {}ms: run_id={} event_type={} seq={} (platform may not support acks yet)",
                    timeout_ms, run_id, event_type, sequence_number
                );
                // Remove pending ack on timeout
                if let Ok(mut pending) = self.sync_pending_acks.lock() {
                    pending.remove(&key);
                }
                // Return Ok for graceful degradation with old platforms
                Ok(())
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                warn!(
                    "Checkpoint ack channel disconnected (sync blocking): run_id={} event_type={} seq={}",
                    run_id, event_type, sequence_number
                );
                // Remove from pending
                if let Ok(mut pending) = self.sync_pending_acks.lock() {
                    pending.remove(&key);
                }
                Err(crate::error::SdkError::Internal(
                    "Checkpoint ack channel disconnected".to_string(),
                ))
            }
        }
    }

    /// Handle a checkpoint acknowledgement from the platform
    ///
    /// This is called internally when the dispatch loop receives a CheckpointAck message.
    /// It checks both async (tokio oneshot) and sync (std::sync::mpsc) pending acks.
    async fn handle_checkpoint_ack(&self, ack: CheckpointAck) {
        let key = PendingAckKey {
            run_id: ack.run_id.clone(),
            sequence_number: ack.sequence_number,
        };

        // First, try the async pending acks (tokio oneshot)
        let async_sender = {
            let mut pending = self.pending_acks.lock().await;
            pending.remove(&key)
        };

        if let Some(tx) = async_sender {
            if tx.send(ack).is_err() {
                debug!("Checkpoint ack receiver dropped (async, caller may have timed out)");
            }
            return;
        }

        // Second, try the sync pending acks (std::sync::mpsc)
        let sync_sender = {
            if let Ok(mut pending) = self.sync_pending_acks.lock() {
                pending.remove(&key)
            } else {
                None
            }
        };

        if let Some(tx) = sync_sender {
            if tx.send(ack).is_err() {
                debug!("Checkpoint ack receiver dropped (sync, caller may have timed out)");
            }
            return;
        }

        debug!(
            "Received ack for unknown checkpoint: run_id={} seq={}",
            key.run_id, key.sequence_number
        );
    }

    /// Queue a streaming delta for real-time delivery to clients (legacy API)
    ///
    /// This method wraps the unified queue_event for backward compatibility.
    /// Use queue_event directly for new code.
    pub fn queue_delta(
        &self,
        invocation_id: String,
        event_type: String,
        output_data: Vec<u8>,
        content_index: i32,
        sequence: i64,
        metadata: HashMap<String, String>,
        source_timestamp_ns: i64,
        correlation_id: String,
        parent_correlation_id: String,
    ) -> Result<()> {
        let is_sse_only = JournalEventMessage::is_sse_only_event_type(&event_type);

        let event = JournalEventMessage {
            run_id: invocation_id,
            event_type,
            data: output_data,
            content_index,
            sequence,
            metadata,
            source_timestamp_ns,
            correlation_id,
            parent_correlation_id,
            is_sse_only,
            queued_at: std::time::Instant::now(),
            ..Default::default()
        };

        self.queue_event(event)
    }

    /// Run the worker with a message handler
    ///
    /// The handler is now `Fn + Clone` instead of `FnMut` to enable concurrent execution.
    /// Multiple worker tasks can invoke the handler in parallel.
    pub async fn run<F, Fut>(&self, message_handler: F) -> Result<()>
    where
        F: Fn(RuntimeMessage, flume::Sender<ServiceMessage>) -> Fut + Send + Clone + 'static,
        Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send + 'static,
    {
        info!("Starting worker {}", self.config.worker_id);

        // Initialize telemetry automatically in async context
        if let Err(e) = crate::telemetry::init_telemetry(
            &self.config.service_name,
            &self.config.service_version,
        ) {
            warn!("Failed to initialize telemetry: {}", e);
        }

        // Create shutdown broadcast channel for immediate response
        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);
        let mut shutdown_rx = shutdown_tx.subscribe();
        let shutdown_tx = Arc::new(shutdown_tx);

        // Spawn signal handler that broadcasts immediate notification
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            info!("Received shutdown signal (Ctrl+C)");
            let _ = shutdown_tx_clone.send(()); // Broadcast to all receivers
        });

        // Retry configuration with jitter to prevent thundering herd
        let max_retries = 5;
        let base_delay = std::time::Duration::from_secs(1);
        let mut retry_count = 0;

        loop {
            // Check for shutdown signal (non-blocking)
            if let Ok(()) = shutdown_rx.try_recv() {
                info!(
                    "Worker {} shutting down due to signal",
                    self.config.worker_id
                );
                return Ok(());
            }

            // Exponential backoff with jitter
            if retry_count > 0 {
                let exp_delay = base_delay * 2_u32.pow((retry_count - 1).min(5));
                // Add jitter (±25% of delay)
                let jitter = rand::random::<f64>() * 0.5 - 0.25;
                let jitter_ms = (exp_delay.as_millis() as f64 * jitter) as u64;
                let delay = exp_delay + std::time::Duration::from_millis(jitter_ms);

                info!(
                    "Worker {} reconnect attempt {} (waiting {:?})",
                    self.config.worker_id, retry_count, delay
                );

                // Use select to allow shutdown during delay
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {},
                    _ = shutdown_rx.recv() => {
                        info!("Worker {} shutting down during reconnect delay", self.config.worker_id);
                        return Ok(());
                    }
                }
            }

            // Try to connect and run
            self.set_connection_state(ConnectionState::Connecting);

            // Create a new receiver for this connection attempt
            let shutdown_rx_inner = shutdown_tx.subscribe();

            match self
                .try_connect_and_run(message_handler.clone(), shutdown_rx_inner)
                .await
            {
                Ok(()) => {
                    self.set_connection_state(ConnectionState::Disconnected);
                    return Ok(());
                }
                Err(e) => {
                    let error_msg = format!(
                        "Worker {} connection failed (attempt {}): {}",
                        self.config.worker_id,
                        retry_count + 1,
                        e
                    );
                    error!("{}", error_msg);
                    self.set_connection_state(ConnectionState::Error(error_msg));

                    retry_count += 1;
                    if retry_count >= max_retries {
                        // After max retries, exit instead of infinite loop
                        error!(
                            "Worker {} failed to connect after {} attempts, exiting",
                            self.config.worker_id, max_retries
                        );
                        self.set_connection_state(ConnectionState::Error(format!(
                            "Failed to connect after {} attempts",
                            max_retries
                        )));
                        return Err(anyhow::anyhow!(
                            "Worker {} failed to connect to coordinator after {} attempts",
                            self.config.worker_id,
                            max_retries
                        ).into());
                    }
                }
            }
        }
    }

    /// Internal method to connect and run until disconnection
    async fn try_connect_and_run<F, Fut>(
        &self,
        message_handler: F,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<()>
    where
        F: Fn(RuntimeMessage, flume::Sender<ServiceMessage>) -> Fut + Send + Clone + 'static,
        Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send + 'static,
    {
        let mut client =
            WorkerCoordinatorClient::connect(self.config.coordinator_endpoint.clone()).await?;

        // Create registration message with components
        // Merge user-provided metadata with auto-collected AGNT5_* env vars
        let mut metadata = self.metadata.clone();
        metadata.extend(collect_agnt5_env_vars());

        let registration = RegisterService {
            service_name: self.config.service_name.clone(),
            service_version: self.config.service_version.clone(),
            service_type: self.config.service_type.clone(),
            components: self.components.clone(),
            metadata,
        };

        // Use the working pattern - create stream with immediate registration
        let (tx, rx) = client
            .create_worker_stream_with_registration(self.config.worker_id.clone(), registration)
            .await?;

        debug!(
            "Worker {} registered successfully and connected",
            self.config.worker_id
        );
        self.set_connection_state(ConnectionState::Connected);

        // Store the tx for emit_checkpoint_sync to use (async version)
        {
            let mut guard = self.checkpoint_tx.lock().await;
            *guard = Some(tx.clone());
        }

        // Store the tx for emit_checkpoint_sync_blocking to use (sync version)
        {
            if let Ok(mut guard) = self.sync_checkpoint_tx.lock() {
                *guard = Some(tx.clone());
            }
        }

        // Start heartbeat task
        let heartbeat_task = self.spawn_heartbeat_task(tx.clone());

        // Start unified journal event flush task (replaces checkpoint, delta, span, log flush tasks)
        let journal_flush_task = self.spawn_journal_flush_task(tx.clone());

        // Get concurrency configuration
        let max_concurrency = std::env::var("AGNT5_MAX_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        debug!(
            "Worker {} starting with concurrency limit: {}",
            self.config.worker_id, max_concurrency
        );

        // Create task pool channels
        // Task dispatch channel (bounded for backpressure)
        let (task_tx, task_rx) = flume::bounded::<RuntimeMessage>(max_concurrency * 2);
        // Response collection channel (unbounded - responses must flow)
        let (response_tx, response_rx) = flume::unbounded::<ServiceMessage>();

        // Spawn worker pool
        let mut worker_handles = Vec::new();
        for worker_id in 0..max_concurrency {
            let task_rx = task_rx.clone();
            let response_tx = response_tx.clone();
            let handler = message_handler.clone();
            let worker_name = format!("{}-{}", self.config.worker_id, worker_id);

            let handle = tokio::spawn(async move {

                while let Ok(runtime_message) = task_rx.recv_async().await {
                    let tx_clone = response_tx.clone();
                    match handler(runtime_message, tx_clone).await {
                        Ok(Some(response)) => {
                            if let Err(e) = response_tx.send_async(response).await {
                                error!("Worker {} failed to send response: {}", worker_name, e);
                                break;
                            }
                        }
                        Ok(None) => {
                            // No response needed
                        }
                        Err(e) => {
                            error!("Worker {} handler error: {}", worker_name, e);
                        }
                    }
                }
            });

            worker_handles.push(handle);
        }

        // Main dispatch loop
        let dispatch_result = loop {
            tokio::select! {
                // Dispatch incoming messages to worker pool
                result = rx.recv_async() => {
                    match result {
                        Ok(runtime_message) => {
                            // Check if this is a CheckpointAck message - handle internally
                            if runtime_message.message_type == RuntimeMessageType::CheckpointAck as i32 {
                                if let Some(crate::pb::runtime_message::MessageData::CheckpointAck(ack)) =
                                    runtime_message.message_data
                                {
                                    debug!(
                                        "Received CheckpointAck: run_id={} seq={} event_type={}",
                                        ack.run_id, ack.sequence_number, ack.event_type
                                    );
                                    self.handle_checkpoint_ack(ack).await;
                                    continue; // Don't dispatch to worker pool
                                }
                            }

                            // Send to worker pool (bounded channel provides backpressure)
                            if let Err(e) = task_tx.send_async(runtime_message).await {
                                error!("Failed to dispatch message to worker pool: {}", e);
                                break Err(crate::error::SdkError::Connection {
                                    message: format!("Task dispatch failed: {}", e),
                                    code: crate::error::ErrorCode::ConnectionFailed,
                                    source: None,
                                });
                            }
                        }
                        Err(e) => {
                            error!("Channel error for worker {}, will reconnect: {}", self.config.worker_id, e);
                            break Err(crate::error::SdkError::Connection {
                                message: format!("Receive failed: {}", e),
                                code: crate::error::ErrorCode::ConnectionFailed,
                                source: None,
                            });
                        }
                    }
                }

                // Forward responses from worker pool to coordinator
                response = response_rx.recv_async() => {
                    match response {
                        Ok(service_message) => {
                            if let Err(e) = tx.send_async(service_message).await {
                                error!("Failed to send response to coordinator: {}", e);
                                break Err(crate::error::SdkError::Connection {
                                    message: format!("Send failed: {}", e),
                                    code: crate::error::ErrorCode::ConnectionFailed,
                                    source: None,
                                });
                            }
                        }
                        Err(e) => {
                            error!("Response channel error: {}", e);
                            break Err(crate::error::SdkError::Connection {
                                message: format!("Response receive failed: {}", e),
                                code: crate::error::ErrorCode::ConnectionFailed,
                                source: None,
                            });
                        }
                    }
                }

                // Wait for shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("Worker {} received shutdown signal, stopping gracefully", self.config.worker_id);
                    break Ok(());
                }
            }
        };

        // Cleanup: close channels and wait for workers
        info!("Worker {} shutting down task pool", self.config.worker_id);
        drop(task_tx); // Signal workers to exit
        drop(task_rx); // Close receiver
        drop(response_tx); // Close response sender

        // Clear checkpoint_tx to prevent emit_checkpoint_sync from sending after disconnect (async)
        {
            let mut guard = self.checkpoint_tx.lock().await;
            *guard = None;
        }

        // Clear sync_checkpoint_tx to prevent emit_checkpoint_sync_blocking from sending after disconnect
        {
            if let Ok(mut guard) = self.sync_checkpoint_tx.lock() {
                *guard = None;
            }
        }

        // Cancel any pending checkpoint acks (async)
        {
            let mut pending = self.pending_acks.lock().await;
            let count = pending.len();
            if count > 0 {
                debug!("Cancelling {} pending async checkpoint acks on disconnect", count);
            }
            pending.clear();
        }

        // Cancel any pending sync checkpoint acks
        {
            if let Ok(mut pending) = self.sync_pending_acks.lock() {
                let count = pending.len();
                if count > 0 {
                    debug!("Cancelling {} pending sync checkpoint acks on disconnect", count);
                }
                pending.clear();
            }
        }

        // Wait for all worker tasks to complete
        for handle in worker_handles {
            let _ = handle.await;
        }

        // Send shutdown message and stop background tasks
        let _ = self.send_shutdown_message(&tx).await;
        heartbeat_task.abort();
        journal_flush_task.abort();

        dispatch_result
    }

    /// Spawn a simple heartbeat task that sends periodic health checks
    fn spawn_heartbeat_task(
        &self,
        tx: flume::Sender<ServiceMessage>,
    ) -> tokio::task::JoinHandle<()> {
        let worker_id = self.config.worker_id.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

            loop {
                interval.tick().await;

                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;

                let health_check = HealthCheck {
                    timestamp,
                    status: WorkerHealthStatus::WorkerHealthHealthy.into(),
                    metrics: std::collections::HashMap::new(),
                    message: "Worker healthy".to_string(),
                };

                let service_message = ServiceMessage {
                    worker_id: worker_id.clone(),
                    metadata: std::collections::HashMap::new(),
                    message_type: Some(crate::pb::service_message::MessageType::HealthCheck(
                        health_check,
                    )),
                };

                // Send heartbeat - if it fails, the channel is closed so we exit
                if tx.send_async(service_message).await.is_err() {
                    break;
                }

                // Heartbeat sent successfully
            }
        })
    }

    /// Spawn unified journal event flush task
    ///
    /// This task periodically flushes all buffered events to the coordinator via gRPC stream.
    /// It handles both:
    /// - Boundary events (workflow.*, agent.*, lm.call.*): Sent as DispatchComponentResponse for persistence
    /// - SSE-only events (output.delta, log, etc.): Sent as DispatchComponentResponse for SSE forwarding only
    ///
    /// The coordinator will persist boundary events and forward SSE-only events to the stream.
    fn spawn_journal_flush_task(
        &self,
        tx: flume::Sender<ServiceMessage>,
    ) -> tokio::task::JoinHandle<()> {
        let worker_id = self.config.worker_id.clone();
        let journal_queue = self.journal_queue.clone();
        let flush_interval_ms = journal_queue.flush_interval_ms();
        let batch_size = journal_queue.batch_size();

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(flush_interval_ms));

            loop {
                interval.tick().await;

                // Drain batch of events
                let batch = journal_queue.drain_batch(batch_size);
                if batch.is_empty() {
                    continue;
                }

                let mut sent_count = 0;
                let mut sse_only_count = 0;

                for event in batch {
                    // Track SSE-only events for metrics
                    if event.is_sse_only {
                        sse_only_count += 1;
                    }

                    // Build metadata with cid, pcid, tenant_id, and deployment_id
                    // Using short keys (cid, pcid) to reduce JSONB storage overhead
                    let mut metadata = event.metadata.clone();
                    if !event.correlation_id.is_empty() {
                        metadata.insert("cid".to_string(), event.correlation_id.clone());
                    }
                    if !event.parent_correlation_id.is_empty() {
                        metadata.insert("pcid".to_string(), event.parent_correlation_id.clone());
                    }

                    // Include tenant_id and deployment_id from environment variables if present
                    if let Ok(tenant_id) = std::env::var("AGNT5_TENANT_ID") {
                        if !tenant_id.is_empty() {
                            metadata.insert("tenant_id".to_string(), tenant_id);
                        }
                    }
                    if let Ok(deployment_id) = std::env::var("AGNT5_DEPLOYMENT_ID") {
                        if !deployment_id.is_empty() {
                            metadata.insert("deployment_id".to_string(), deployment_id);
                        }
                    }

                    // Send as DispatchComponentResponse for now (existing proto)
                    // The coordinator will:
                    // - Persist boundary events to journal_events table
                    // - Forward SSE-only events to SSE stream without persistence
                    let response = DispatchComponentResponse {
                        invocation_id: event.run_id.clone(),
                        success: true,
                        result: Some(
                            crate::pb::dispatch_component_response::Result::OutputData(
                                event.data.clone(),
                            ),
                        ),
                        error_message: String::new(),
                        metadata,
                        event_type: event.event_type.clone(),
                        content_index: event.content_index,
                        sequence: event.sequence,
                        attempt: 0,
                        source_timestamp_ns: event.source_timestamp_ns,
                    };

                    let service_message = ServiceMessage {
                        worker_id: worker_id.clone(),
                        metadata: std::collections::HashMap::new(),
                        message_type: Some(
                            crate::pb::service_message::MessageType::FunctionResponse(response),
                        ),
                    };

                    // Send event - if it fails, re-queue and exit
                    if let Err(e) = tx.send_async(service_message).await {
                        warn!(
                            "Failed to send journal event, re-queuing: type={} run_id={} error={}",
                            event.event_type, event.run_id, e
                        );

                        // Re-queue at front to preserve order
                        if let Err(e) = journal_queue.push_front(event) {
                            error!("Failed to re-queue journal event: {}", e);
                        }

                        journal_queue.record_error();
                        break; // Channel closed, exit task
                    }

                    sent_count += 1;
                }

                if sent_count > 0 {
                    journal_queue.record_sent_batch(sent_count, sse_only_count);
                    debug!(
                        "Flushed {} journal events to coordinator (boundary={}, sse_only={}, queue_size={})",
                        sent_count,
                        sent_count - sse_only_count,
                        sse_only_count,
                        journal_queue.len()
                    );
                }
            }
        })
    }

    /// Send graceful shutdown message
    async fn send_shutdown_message(&self, tx: &flume::Sender<ServiceMessage>) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let unregister = UnregisterService {
            reason: "Worker shutdown".to_string(),
            timestamp,
        };

        let service_message = ServiceMessage {
            worker_id: self.config.worker_id.clone(),
            metadata: std::collections::HashMap::new(),
            message_type: Some(crate::pb::service_message::MessageType::UnregisterService(
                unregister,
            )),
        };

        match tx.send_async(service_message).await {
            Ok(_) => {
                info!(
                    "Sent graceful shutdown message for worker {}",
                    self.config.worker_id
                );
                // Give a moment for the message to be processed
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                Ok(())
            }
            Err(e) => {
                warn!(
                    "Failed to send shutdown message for worker {}: {}",
                    self.config.worker_id, e
                );
                Err(crate::error::SdkError::Connection {
                    message: format!("Shutdown message failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                })
            }
        }
    }
}
