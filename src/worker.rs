use crate::checkpoint::{CheckpointMessage, CheckpointQueue};
use crate::client::WorkerCoordinatorClient;
use crate::error::Result;
use crate::pb::{
    ComponentInfo, HealthCheck, RegisterService, RuntimeMessage, ServiceMessage, UnregisterService,
    WorkerHealthStatus, WorkflowCheckpoint,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};
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
    pub tenant_id: String,
    pub deployment_id: String,
}

impl WorkerConfig {
    pub fn new(service_name: String, service_version: String, service_type: String) -> Self {
        // Generate a default worker ID, but allow override from environment
        let default_worker_id = Uuid::new_v4().to_string();
        let worker_id = std::env::var("AGNT5_WORKER_ID").unwrap_or_else(|_| default_worker_id);

        let coordinator_endpoint = std::env::var("AGNT5_COORDINATOR_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:34186".to_string());

        // Check if we're in development mode for better defaults
        let is_dev_mode = std::env::var("AGNT5_DEV_MODE").unwrap_or_else(|_| "false".to_string())
            == "true"
            || std::env::var("AGNT5_ENVIRONMENT").unwrap_or_else(|_| "".to_string())
                == "development"
            || std::env::var("AGNT5_JOURNAL_BACKEND").unwrap_or_else(|_| "".to_string())
                == "embedded"
            || std::env::var("AGNT5_ORCHESTRATION_BACKEND").unwrap_or_else(|_| "".to_string())
                == "sqlite";

        let tenant_id = std::env::var("AGNT5_TENANT_ID").unwrap_or_else(|_| {
            if is_dev_mode {
                // Check for dev-specific override first
                std::env::var("AGNT5_DEV_TENANT_ID")
                    .unwrap_or_else(|_| "00000000-0000-0000-0000-000000000001".to_string())
            } else {
                "default".to_string()
            }
        });

        let deployment_id = std::env::var("AGNT5_DEPLOYMENT_ID").unwrap_or_else(|_| {
            if is_dev_mode {
                // Check for dev-specific override first
                std::env::var("AGNT5_DEV_DEPLOYMENT_ID")
                    .unwrap_or_else(|_| "00000000-0000-0000-0000-000000000002".to_string())
            } else {
                "default".to_string()
            }
        });

        Self {
            service_name,
            service_version,
            service_type,
            worker_id,
            coordinator_endpoint,
            tenant_id,
            deployment_id,
        }
    }
}

#[derive(Clone)]
pub struct Worker {
    config: WorkerConfig,
    components: Vec<ComponentInfo>,
    metadata: HashMap<String, String>,
    connection_state: Arc<std::sync::Mutex<ConnectionState>>,
    checkpoint_queue: CheckpointQueue,
}

// Implement Debug manually to avoid requiring Debug on CheckpointQueue's internals
impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("config", &self.config)
            .field("components", &self.components)
            .field("metadata", &self.metadata)
            .field("connection_state", &self.connection_state)
            .field("checkpoint_queue_size", &self.checkpoint_queue.len())
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
        // Get checkpoint buffer size from environment or use default
        let checkpoint_buffer_size = std::env::var("AGNT5_CHECKPOINT_BUFFER_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        info!("Creating worker with checkpoint buffer size: {}", checkpoint_buffer_size);

        Self {
            config,
            components,
            metadata,
            connection_state: Arc::new(std::sync::Mutex::new(ConnectionState::Disconnected)),
            checkpoint_queue: CheckpointQueue::new(checkpoint_buffer_size),
        }
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

    /// Queue a workflow checkpoint for progressive durability
    ///
    /// This is called from language SDKs via FFI during workflow execution to send
    /// checkpoints (state changes, step completions) to the platform in real-time.
    ///
    /// Checkpoints are buffered and sent via gRPC when the stream is active.
    ///
    /// # Arguments
    ///
    /// * `invocation_id` - Workflow run ID
    /// * `checkpoint_type` - Event type ("workflow.state.changed", "workflow.step.started", etc.)
    /// * `checkpoint_data` - JSON payload as bytes
    /// * `sequence_number` - Monotonic sequence for ordering
    /// * `metadata` - Additional metadata (tenant_id, deployment_id, etc.)
    pub fn queue_checkpoint(
        &self,
        invocation_id: String,
        checkpoint_type: String,
        checkpoint_data: Vec<u8>,
        sequence_number: i64,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        let checkpoint = CheckpointMessage {
            invocation_id: invocation_id.clone(),
            checkpoint_type: checkpoint_type.clone(),
            checkpoint_data,
            sequence_number,
            metadata,
            queued_at: std::time::Instant::now(),
        };

        self.checkpoint_queue.push(checkpoint).map_err(|e| {
            crate::error::SdkError::Internal(format!("Failed to queue checkpoint: {}", e))
        })?;

        Ok(())
    }

    /// Get checkpoint queue metrics
    ///
    /// Returns (queued, sent, dropped, errors)
    pub fn checkpoint_metrics(&self) -> (u64, u64, u64, u64) {
        self.checkpoint_queue.get_metrics()
    }

    /// Drain all buffered checkpoints for synchronous flushing
    ///
    /// This method removes and returns all queued checkpoints.
    /// Used before sending workflow completion response to ensure
    /// checkpoints arrive before run.completed event.
    ///
    /// # Returns
    ///
    /// Vector of (invocation_id, checkpoint_type, checkpoint_data, sequence_number, metadata) tuples
    pub fn drain_checkpoints(&self) -> Vec<(String, String, Vec<u8>, i64, HashMap<String, String>)> {
        let checkpoints = self.checkpoint_queue.drain_all();

        checkpoints
            .into_iter()
            .map(|cp| {
                (
                    cp.invocation_id,
                    cp.checkpoint_type,
                    cp.checkpoint_data,
                    cp.sequence_number,
                    cp.metadata,
                )
            })
            .collect()
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
        let registration = RegisterService {
            service_name: self.config.service_name.clone(),
            service_version: self.config.service_version.clone(),
            service_type: self.config.service_type.clone(),
            components: self.components.clone(),
            tenant_id: self.config.tenant_id.clone(),
            deployment_id: self.config.deployment_id.clone(),
            metadata: self.metadata.clone(),
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

        // Start checkpoint flushing task
        let checkpoint_task = self.spawn_checkpoint_flush_task(tx.clone());

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
        checkpoint_task.abort();

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

    /// Spawn checkpoint flushing task for progressive workflow durability
    ///
    /// This task periodically flushes buffered checkpoints to the coordinator via gRPC stream.
    /// Checkpoints are sent immediately when queued if the queue was previously empty,
    /// otherwise they're batched and sent periodically.
    fn spawn_checkpoint_flush_task(
        &self,
        tx: flume::Sender<ServiceMessage>,
    ) -> tokio::task::JoinHandle<()> {
        let worker_id = self.config.worker_id.clone();
        let checkpoint_queue = self.checkpoint_queue.clone();

        // Get flush interval from environment or use default (100ms for low latency)
        let flush_interval_ms = std::env::var("AGNT5_CHECKPOINT_FLUSH_INTERVAL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100); // Default 100ms for low-latency checkpoint streaming

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                std::time::Duration::from_millis(flush_interval_ms)
            );

            loop {
                interval.tick().await;

                // Flush all queued checkpoints
                let mut sent_count = 0;
                while let Some(checkpoint) = checkpoint_queue.pop() {
                    let workflow_checkpoint = WorkflowCheckpoint {
                        invocation_id: checkpoint.invocation_id.clone(),
                        checkpoint_type: checkpoint.checkpoint_type.clone(),
                        checkpoint_data: checkpoint.checkpoint_data.clone(),
                        sequence_number: checkpoint.sequence_number,
                        metadata: checkpoint.metadata.clone(),
                    };

                    let service_message = ServiceMessage {
                        worker_id: worker_id.clone(),
                        message_type: Some(
                            crate::pb::service_message::MessageType::WorkflowCheckpoint(
                                workflow_checkpoint,
                            ),
                        ),
                    };

                    // Send checkpoint - if it fails, re-queue and exit
                    if let Err(e) = tx.send_async(service_message).await {
                        warn!(
                            "Failed to send checkpoint, re-queuing: type={} seq={} error={}",
                            checkpoint.checkpoint_type, checkpoint.sequence_number, e
                        );

                        // Re-queue at front to preserve order
                        if let Err(e) = checkpoint_queue.push_front(checkpoint) {
                            error!("Failed to re-queue checkpoint: {}", e);
                        }

                        checkpoint_queue.record_error();
                        break; // Channel closed, exit task
                    }

                    checkpoint_queue.record_sent();
                    sent_count += 1;
                }

                if sent_count > 0 {
                    info!(
                        "Flushed {} checkpoints to coordinator (queue_size={})",
                        sent_count,
                        checkpoint_queue.len()
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
