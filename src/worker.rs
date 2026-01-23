use crate::client::WorkerCoordinatorClient;
use crate::error::Result;
use crate::journal_queue::{JournalEventMessage, JournalEventQueue, JournalQueueConfig};
use crate::pb::{
    ComponentInfo, DispatchComponentResponse, HealthCheck, RegisterService, RuntimeMessage,
    ServiceMessage, UnregisterService, WorkerHealthStatus,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

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

        info!(
            "Creating worker with unified journal queue: max_size={}, batch_size={}, flush_interval_ms={}",
            journal_config.max_size, journal_config.batch_size, journal_config.flush_interval_ms
        );

        Self {
            config,
            components,
            metadata,
            connection_state: Arc::new(std::sync::Mutex::new(ConnectionState::Disconnected)),
            journal_queue: JournalEventQueue::new(journal_config),
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

        info!(
            "Worker {} registered successfully and connected",
            self.config.worker_id
        );
        self.set_connection_state(ConnectionState::Connected);

        // Start heartbeat task
        let heartbeat_task = self.spawn_heartbeat_task(tx.clone());

        // Start unified journal event flush task (replaces checkpoint, delta, span, log flush tasks)
        let journal_flush_task = self.spawn_journal_flush_task(tx.clone());

        // Get concurrency configuration
        let max_concurrency = std::env::var("AGNT5_MAX_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        info!(
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
