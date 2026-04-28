use crate::client::{self, EngineClient, WorkerCoordinatorClient};
use crate::error::Result;
use crate::journal_queue::{JournalEventMessage, JournalEventQueue, JournalQueueConfig};
use crate::pb::{
    execution_engine_service_client::ExecutionEngineServiceClient, CompleteJobRequest,
    ComponentInfo, DispatchComponentResponse, EventStreamMessage, HealthCheck, PollJobsRequest,
    RegisterService, RuntimeMessage, RuntimeMessageType, ServiceMessage, UnregisterService,
    WorkerHealthStatus, WriteCheckpointRequest,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as TokioMutex;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::coordinator_routing::CoordinatorRouting;

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

    /// Execution Engine endpoint for journal writes and checkpoints.
    /// In production with Envoy, this equals coordinator_endpoint (Envoy routes by gRPC service).
    /// In standalone/dev mode, EE runs on a separate port (default: 34185).
    /// Env: AGNT5_EE_ENDPOINT. Defaults to coordinator_endpoint.
    pub ee_endpoint: String,

    /// Maximum connection retry attempts before exiting.
    /// 0 = infinite retry (worker never exits due to connection issues)
    /// Default: 5
    pub max_retries: u32,

    /// AGNT5 Engine endpoint for direct event writes.
    /// When set, all event paths (checkpoints, boundary, SSE-only) route to the engine's
    /// Append/AppendBatch RPCs instead of the Go Execution Engine.
    /// Env: AGNT5_ENGINE_URL. None = use Go EE (default).
    pub engine_endpoint: Option<String>,
}

impl WorkerConfig {
    pub fn new(service_name: String, service_version: String, service_type: String) -> Self {
        // Generate a default worker ID, but allow override from environment
        let default_worker_id = Uuid::new_v4().to_string();
        let worker_id = std::env::var("AGNT5_WORKER_ID").unwrap_or_else(|_| default_worker_id);

        let coordinator_endpoint = std::env::var("AGNT5_COORDINATOR_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:34186".to_string());

        // EE endpoint defaults to coordinator endpoint (works with Envoy routing).
        // In standalone/dev mode, set AGNT5_EE_ENDPOINT to the EE port (e.g., http://localhost:34185).
        let ee_endpoint =
            std::env::var("AGNT5_EE_ENDPOINT").unwrap_or_else(|_| coordinator_endpoint.clone());

        // Parse max retries from environment (0 = infinite, default: 5)
        let max_retries = std::env::var("AGNT5_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);

        // Engine endpoint — when set, bypasses Go EE for all event writes.
        let engine_endpoint = std::env::var("AGNT5_ENGINE_URL").ok();

        Self {
            service_name,
            service_version,
            service_type,
            worker_id,
            coordinator_endpoint,
            ee_endpoint,
            max_retries,
            engine_endpoint,
        }
    }

    pub fn resolved_coordinator_endpoint(&self) -> String {
        let routing = CoordinatorRouting::from_env();
        routing.endpoint_for_worker(&self.worker_id, &self.coordinator_endpoint)
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
/// Excludes sensitive variables based on blacklist patterns.
/// Also injects system info (hostname, OS, arch) as AGNT5_SYS_* keys.
pub fn collect_agnt5_env_vars() -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    for (key, value) in std::env::vars() {
        if key.starts_with("AGNT5_") && !is_sensitive_env_var(&key) {
            metadata.insert(key, value);
        }
    }

    // System info — always set, not overridable by env vars
    if let Ok(h) = hostname::get() {
        metadata.insert(
            "AGNT5_SYS_HOSTNAME".into(),
            h.to_string_lossy().into_owned(),
        );
    }
    metadata.insert("AGNT5_SYS_OS".into(), std::env::consts::OS.into());
    metadata.insert("AGNT5_SYS_ARCH".into(), std::env::consts::ARCH.into());

    metadata
}

fn canonical_project_id_from_metadata(metadata: &HashMap<String, String>) -> Option<String> {
    metadata
        .get("project_id")
        .cloned()
        .or_else(|| metadata.get("tenant_id").cloned())
}

fn canonical_project_id_from_env() -> String {
    std::env::var("AGNT5_PROJECT_ID")
        .ok()
        .or_else(|| std::env::var("AGNT5_TENANT_ID").ok())
        .unwrap_or_default()
}

fn with_project_metadata(
    mut metadata: HashMap<String, String>,
    project_id: &str,
) -> HashMap<String, String> {
    if !project_id.is_empty() {
        metadata
            .entry("project_id".to_string())
            .or_insert_with(|| project_id.to_string());
        // Legacy compatibility for coordinator/engine paths that still expect tenant_id.
        metadata
            .entry("tenant_id".to_string())
            .or_insert_with(|| project_id.to_string());
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
    /// Lazily-connected EE gRPC client for WriteCheckpoint unary RPCs.
    /// Used by emit_checkpoint_sync/emit_checkpoint_sync_blocking to persist checkpoints
    /// directly to EE, replacing the old WorkflowCheckpoint→CheckpointAck stream round-trip.
    ee_client: Arc<TokioMutex<Option<ExecutionEngineServiceClient<Channel>>>>,
    /// Tokio runtime handle captured in run() for use by emit_checkpoint_sync_blocking.
    /// Python threads (via PyO3) are NOT tokio threads, so they can't use Handle::current().
    tokio_handle: Arc<std::sync::Mutex<Option<tokio::runtime::Handle>>>,
    /// Tracks which run_ids have is_streaming=true. Ephemeral events are skipped
    /// for non-streaming runs since nobody is listening via SSE.
    streaming_runs: Arc<std::sync::Mutex<HashMap<String, bool>>>,
    /// Phase 5: lease_id stash keyed by invocation_id. Populated on
    /// DispatchComponentRequest receipt (when req.lease_id is non-empty) and
    /// drained on response forward so the echoed lease_id lands in
    /// DispatchComponentResponse.lease_id without requiring language bindings
    /// to thread the value through their handler code.
    pending_lease_ids: Arc<std::sync::Mutex<HashMap<String, String>>>,
    /// EventStream sender for SSE-only events (EE path). Set during run().
    event_stream_tx: Arc<std::sync::Mutex<Option<flume::Sender<EventStreamMessage>>>>,
    /// Dispatch stream sender (bidirectional gRPC to WC). Used by emit_checkpoint_sync
    /// to flush pending SSE-only events before terminal checkpoints, ensuring they
    /// arrive while the invocation is still tracked in pendingStreamInvocations.
    dispatch_tx: Arc<std::sync::Mutex<Option<flume::Sender<ServiceMessage>>>>,
    /// Sticky owner endpoint learned from a registration redirect.
    owner_endpoint_hint: Arc<std::sync::Mutex<Option<String>>>,
    /// Lazily-connected Engine gRPC client. When AGNT5_ENGINE_URL is set, all event paths
    /// route through this client instead of the Go EE.
    engine_client: Arc<TokioMutex<Option<EngineClient>>>,
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
            .field("streaming_runs", &self.streaming_runs)
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
            ee_client: Arc::new(TokioMutex::new(None)),
            tokio_handle: Arc::new(std::sync::Mutex::new(None)),
            streaming_runs: Arc::new(std::sync::Mutex::new(HashMap::new())),
            pending_lease_ids: Arc::new(std::sync::Mutex::new(HashMap::new())),
            event_stream_tx: Arc::new(std::sync::Mutex::new(None)),
            dispatch_tx: Arc::new(std::sync::Mutex::new(None)),
            owner_endpoint_hint: Arc::new(std::sync::Mutex::new(None)),
            engine_client: Arc::new(TokioMutex::new(None)),
        }
    }

    /// Get a clone of the journal event queue for use by language SDKs
    pub fn journal_queue(&self) -> JournalEventQueue {
        self.journal_queue.clone()
    }

    /// Set components for the worker.
    /// Note: Built-in scorers are NOT registered as components. The platform
    /// routes scorer requests to any available worker without component lookup,
    /// and the worker handles them via the Rust fast-path or language SDK.
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

    fn preferred_coordinator_endpoint(&self) -> String {
        if let Ok(guard) = self.owner_endpoint_hint.lock() {
            if let Some(endpoint) = guard.clone() {
                return endpoint;
            }
        }
        self.config.resolved_coordinator_endpoint()
    }

    fn set_owner_endpoint_hint(&self, endpoint: Option<String>) {
        if let Ok(mut guard) = self.owner_endpoint_hint.lock() {
            *guard = endpoint;
        }
    }

    fn clear_owner_endpoint_hint(&self) -> bool {
        if let Ok(mut guard) = self.owner_endpoint_hint.lock() {
            let had_hint = guard.is_some();
            *guard = None;
            return had_hint;
        }
        false
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
        let tenant_id = canonical_project_id_from_metadata(&metadata);
        let event = JournalEventMessage {
            run_id: invocation_id,
            event_type: checkpoint_type,
            data: checkpoint_data,
            sequence: sequence_number,
            metadata,
            source_timestamp_ns,
            correlation_id,
            parent_correlation_id,
            tenant_id,
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

    /// Ensure the EE gRPC client is connected, lazily creating it on first use.
    async fn ensure_ee_client(&self) -> Result<ExecutionEngineServiceClient<Channel>> {
        let mut guard = self.ee_client.lock().await;
        if let Some(ref client) = *guard {
            return Ok(client.clone());
        }

        // Connect to EE. In production, Envoy routes by gRPC service name
        // (ee_endpoint == coordinator_endpoint). In dev mode, EE is on a separate port.
        let endpoint = &self.config.ee_endpoint;
        debug!("Connecting EE client to {}", endpoint);

        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| crate::error::SdkError::Connection {
                message: format!("Invalid EE endpoint {}: {}", endpoint, e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            })?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .connect()
            .await
            .map_err(|e| {
                debug!("EE connection to {} failed: {:?}", endpoint, e);
                crate::error::SdkError::Connection {
                    message: format!("EE connection failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?;

        let client = ExecutionEngineServiceClient::new(channel);
        *guard = Some(client.clone());
        debug!("EE client connected to {}", endpoint);
        Ok(client)
    }

    /// Ensure the Engine gRPC client is connected, lazily creating it on first use.
    /// Returns None if AGNT5_ENGINE_URL is not configured.
    async fn ensure_engine_client(&self) -> Result<Option<EngineClient>> {
        let endpoint = match &self.config.engine_endpoint {
            Some(ep) => ep.clone(),
            None => return Ok(None),
        };

        let mut guard = self.engine_client.lock().await;
        if let Some(ref client) = *guard {
            return Ok(Some(client.clone()));
        }

        debug!("Connecting Engine client to {}", endpoint);
        let client = EngineClient::connect(&endpoint).await?;
        *guard = Some(client.clone());
        debug!("Engine client connected to {}", endpoint);
        Ok(Some(client))
    }

    /// Emit a checkpoint event synchronously and wait for acknowledgement.
    ///
    /// Sends a WriteCheckpoint unary RPC directly to the Execution Engine.
    /// The RPC response serves as the acknowledgement — no stream round-trip needed.
    ///
    /// # Arguments
    ///
    /// * `run_id` - The run ID this checkpoint belongs to
    /// * `event_type` - The event type (e.g., "approval.requested", "workflow.step.paused")
    /// * `event_data` - JSON-encoded event payload
    /// * `sequence_number` - Sequence number for ordering within execution
    /// * `metadata` - Additional metadata
    /// * `source_timestamp_ns` - Nanosecond timestamp when event was created
    /// * `timeout_ms` - Timeout in milliseconds for the RPC call
    ///
    /// # Returns
    ///
    /// Ok(()) if the checkpoint was persisted, or an error if the RPC failed.
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
        // ── Engine path: when AGNT5_ENGINE_URL is set, route directly to engine ──
        if let Some(mut engine) = self.ensure_engine_client().await? {
            // Before terminal checkpoints, flush any pending events for this run
            // directly to the engine via AppendBatch.
            if event_type == "run.completed" || event_type == "run.failed" {
                let pending = self.journal_queue.drain_run_events(&run_id);
                if !pending.is_empty() {
                    let tenant_id = canonical_project_id_from_metadata(&metadata)
                        .or_else(|| canonical_project_id_from_metadata(&self.metadata))
                        .unwrap_or_default();
                    let records: Vec<_> = pending
                        .iter()
                        .map(|e| {
                            client::build_engine_record(
                                tenant_id.clone(),
                                e.run_id.clone(),
                                e.event_type.clone(),
                                e.data.clone(),
                                e.source_timestamp_ns,
                                String::new(),
                                e.correlation_id.clone(),
                                e.parent_correlation_id.clone(),
                                e.metadata.clone(),
                            )
                        })
                        .collect();
                    if let Err(e) = engine.append_batch(records).await {
                        warn!(
                            "Engine: failed to flush {} pre-checkpoint events for run_id={}: {}",
                            pending.len(),
                            run_id,
                            e
                        );
                    } else {
                        debug!(
                            "Engine: flushed {} events before {} for run_id={}",
                            pending.len(),
                            event_type,
                            run_id
                        );
                    }
                }
            }

            let mut merged_metadata = metadata;
            for (k, v) in &self.metadata {
                if !merged_metadata.contains_key(k) {
                    merged_metadata.insert(k.clone(), v.clone());
                }
            }
            let canonical_project_id =
                canonical_project_id_from_metadata(&merged_metadata).unwrap_or_default();
            merged_metadata = with_project_metadata(merged_metadata, &canonical_project_id);
            let correlation_id = merged_metadata.remove("cid").unwrap_or_default();
            let parent_event_id = merged_metadata.remove("pcid").unwrap_or_default();
            let tenant_id = merged_metadata
                .remove("project_id")
                .or_else(|| merged_metadata.remove("tenant_id"))
                .unwrap_or_default();
            let experiment_id = merged_metadata.get("experiment_id").cloned();

            let record = client::build_engine_record(
                tenant_id,
                run_id.clone(),
                event_type.clone(),
                event_data,
                source_timestamp_ns,
                String::new(), // step_key — checkpoints don't set this directly
                correlation_id,
                parent_event_id,
                merged_metadata,
            );

            let timeout = Duration::from_millis(timeout_ms);
            let start = Instant::now();
            let result = match tokio::time::timeout(timeout, engine.append(record)).await {
                Ok(Ok((_offset, _ts))) => {
                    debug!(
                        "Engine checkpoint persisted: run_id={} event_type={} seq={}",
                        run_id, event_type, sequence_number
                    );
                    Ok(())
                }
                Ok(Err(e)) => {
                    warn!(
                        "Engine Append failed: run_id={} event_type={} seq={} error={}",
                        run_id, event_type, sequence_number, e
                    );
                    // Clear cached client for reconnection
                    {
                        let mut guard = self.engine_client.lock().await;
                        *guard = None;
                    }
                    Err(e)
                }
                Err(_) => {
                    warn!(
                        "Engine Append timeout after {}ms: run_id={} event_type={} seq={}",
                        timeout_ms, run_id, event_type, sequence_number
                    );
                    Ok(()) // Graceful degradation
                }
            };

            let duration_secs = start.elapsed().as_secs_f64();
            crate::telemetry::record_checkpoint(
                &event_type,
                duration_secs,
                result.is_ok(),
                experiment_id.as_deref(),
            );

            return result;
        }

        // ── Legacy EE path (AGNT5_ENGINE_URL not set) ──

        // Before sending terminal checkpoints (run.completed/run.failed), flush any
        // pending SSE-only events (logs, deltas) for this run.
        // Route through EventStream (EE) which is the single SSE publisher.
        // Falls back to dispatch stream (WC) only if EventStream is unavailable.
        if event_type == "run.completed" || event_type == "run.failed" {
            let pending = self.journal_queue.drain_run_events(&run_id);
            if !pending.is_empty() {
                let es_tx = self.event_stream_tx.lock().ok().and_then(|g| g.clone());
                let dispatch = self.dispatch_tx.lock().ok().and_then(|g| g.clone());

                for event in &pending {
                    // Prefer EventStream (EE) — the single SSE publisher
                    if let Some(ref es) = es_tx {
                        let es_msg = EventStreamMessage {
                            run_id: event.run_id.clone(),
                            event_type: event.event_type.clone(),
                            data: event.data.clone(),
                            trace_id: String::new(),
                            span_id: String::new(),
                            tenant_id: canonical_project_id_from_metadata(&event.metadata)
                                .unwrap_or_default(),
                            source_timestamp_ns: event.source_timestamp_ns,
                            worker_id: self.config.worker_id.clone(),
                        };
                        if let Err(e) = es.send_async(es_msg).await {
                            warn!("Failed to flush pre-checkpoint event via EventStream: type={} run_id={} error={}", event.event_type, event.run_id, e);
                        }
                    } else if let Some(ref dtx) = dispatch {
                        // Fallback: dispatch stream (WC) — only works for streamed invocations
                        let mut meta = event.metadata.clone();
                        if !event.correlation_id.is_empty() {
                            meta.insert("cid".to_string(), event.correlation_id.clone());
                        }
                        if !event.parent_correlation_id.is_empty() {
                            meta.insert("pcid".to_string(), event.parent_correlation_id.clone());
                        }

                        // Phase 5: look up stashed lease_id for this invocation so
                        // SSE passthrough events carry the fence token. Intermediate
                        // events don't drain the entry — terminal ack still needs it.
                        let stashed_lease_id = if let Ok(map) = self.pending_lease_ids.lock() {
                            map.get(&event.run_id).cloned().unwrap_or_default()
                        } else {
                            String::new()
                        };
                        let response = DispatchComponentResponse {
                            invocation_id: event.run_id.clone(),
                            success: true,
                            result: Some(
                                crate::pb::dispatch_component_response::Result::OutputData(
                                    event.data.clone(),
                                ),
                            ),
                            error_message: String::new(),
                            metadata: meta,
                            event_type: event.event_type.clone(),
                            content_index: event.content_index,
                            sequence: event.sequence,
                            attempt: 0,
                            source_timestamp_ns: event.source_timestamp_ns,
                            lease_id: stashed_lease_id,
                        };

                        let service_message = ServiceMessage {
                            worker_id: self.config.worker_id.clone(),
                            metadata: std::collections::HashMap::new(),
                            message_type: Some(
                                crate::pb::service_message::MessageType::FunctionResponse(response),
                            ),
                        };

                        if let Err(e) = dtx.send_async(service_message).await {
                            warn!("Failed to flush pre-checkpoint event via dispatch: type={} run_id={} error={}", event.event_type, event.run_id, e);
                        }
                    }
                }
                debug!(
                    "Flushed {} SSE-only events before {}: run_id={}",
                    pending.len(),
                    event_type,
                    run_id
                );
            }
        }

        // Merge service metadata (tenant_id, deployment_id) with passed metadata
        let mut merged_metadata = metadata;
        for (k, v) in &self.metadata {
            if !merged_metadata.contains_key(k) {
                merged_metadata.insert(k.clone(), v.clone());
            }
        }
        let canonical_project_id =
            canonical_project_id_from_metadata(&merged_metadata).unwrap_or_default();
        merged_metadata = with_project_metadata(merged_metadata, &canonical_project_id);

        // Extract correlation/parent IDs from metadata
        let correlation_id = merged_metadata.remove("cid").unwrap_or_default();
        let parent_event_id = merged_metadata.remove("pcid").unwrap_or_default();

        // Extract experiment_id before metadata is moved into the request
        let experiment_id = merged_metadata.get("experiment_id").cloned();

        let request = WriteCheckpointRequest {
            run_id: run_id.clone(),
            checkpoint_type: event_type.clone(),
            checkpoint_data: event_data,
            sequence_number,
            trace_id: String::new(),
            tenant_id: canonical_project_id,
            source_timestamp_ns,
            correlation_id,
            parent_event_id,
            metadata: merged_metadata,
        };

        // Get EE client and call WriteCheckpoint with timeout
        let mut ee_client = self.ensure_ee_client().await?;

        let timeout = Duration::from_millis(timeout_ms);
        let start = Instant::now();
        let result = match tokio::time::timeout(timeout, ee_client.write_checkpoint(request)).await
        {
            Ok(Ok(response)) => {
                let resp = response.into_inner();
                if resp.success {
                    debug!(
                        "Checkpoint persisted: run_id={} event_type={} seq={} journal_seq={}",
                        run_id, event_type, sequence_number, resp.sequence_number
                    );
                    Ok(())
                } else {
                    warn!(
                        "Checkpoint rejected: run_id={} event_type={} seq={} error={}",
                        run_id, event_type, sequence_number, resp.error_message
                    );
                    Err(crate::error::SdkError::Internal(format!(
                        "Checkpoint rejected: {}",
                        resp.error_message
                    )))
                }
            }
            Ok(Err(e)) => {
                warn!(
                    "WriteCheckpoint RPC failed: run_id={} event_type={} seq={} error={}",
                    run_id, event_type, sequence_number, e
                );
                // Clear cached client on RPC failure so next call reconnects
                {
                    let mut guard = self.ee_client.lock().await;
                    *guard = None;
                }
                Err(crate::error::SdkError::Connection {
                    message: format!("WriteCheckpoint failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                })
            }
            Err(_) => {
                warn!(
                    "WriteCheckpoint timeout after {}ms: run_id={} event_type={} seq={}",
                    timeout_ms, run_id, event_type, sequence_number
                );
                // Return Ok for graceful degradation — event may have been persisted
                Ok(())
            }
        };

        // Record OTEL metrics for checkpoint round-trip
        let duration_secs = start.elapsed().as_secs_f64();
        crate::telemetry::record_checkpoint(
            &event_type,
            duration_secs,
            result.is_ok(),
            experiment_id.as_deref(),
        );

        result
    }

    /// Emit a checkpoint event and block until the platform acknowledges it (TRULY SYNCHRONOUS)
    ///
    /// This is the sync version that can be called from non-async Python code.
    /// It creates a temporary tokio runtime to execute the async WriteCheckpoint RPC.
    ///
    /// # Arguments
    ///
    /// * `run_id` - The run/invocation ID this checkpoint belongs to
    /// * `event_type` - The checkpoint event type (e.g., "approval.requested", "workflow.paused")
    /// * `event_data` - The event payload as bytes
    /// * `sequence_number` - Sequence number for ordering
    /// * `metadata` - Additional metadata for the event
    /// * `source_timestamp_ns` - Nanosecond timestamp when event was created
    /// * `timeout_ms` - Timeout in milliseconds for the RPC call
    ///
    /// # Returns
    ///
    /// Ok(()) if the checkpoint was persisted, or an error if the RPC failed.
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
        let worker = self.clone();

        // Detect whether we're on a tokio thread or not.
        // Python threads (via PyO3 allow_threads) are NOT tokio threads,
        // so Handle::current() would panic. Use the stored handle instead.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // On a tokio thread — use block_in_place to yield the thread back to tokio
            tokio::task::block_in_place(move || {
                handle.block_on(async move {
                    worker
                        .emit_checkpoint_sync(
                            run_id,
                            event_type,
                            event_data,
                            sequence_number,
                            metadata,
                            source_timestamp_ns,
                            timeout_ms,
                        )
                        .await
                })
            })
        } else {
            // Not on a tokio thread (e.g., Python asyncio event loop via PyO3)
            // Use the stored handle captured in run()
            let handle = {
                let guard = self.tokio_handle.lock().map_err(|e| {
                    crate::error::SdkError::Internal(format!("Failed to lock tokio_handle: {}", e))
                })?;
                guard
                    .clone()
                    .ok_or_else(|| crate::error::SdkError::Connection {
                        message: "Worker not running, cannot emit checkpoint".to_string(),
                        code: crate::error::ErrorCode::ConnectionFailed,
                        source: None,
                    })?
            };

            handle.block_on(async move {
                worker
                    .emit_checkpoint_sync(
                        run_id,
                        event_type,
                        event_data,
                        sequence_number,
                        metadata,
                        source_timestamp_ns,
                        timeout_ms,
                    )
                    .await
            })
        }
    }

    /// Emit a batch of events in a single AppendBatch RPC.
    ///
    /// Used for non-terminal events (e.g., run.started + function.started) that
    /// can be batched to reduce gRPC overhead. Each event tuple contains:
    /// (run_id, event_type, data, sequence, metadata, timestamp_ns)
    pub async fn emit_checkpoint_batch(
        &self,
        events: Vec<(String, String, Vec<u8>, i64, HashMap<String, String>, i64)>,
    ) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        if let Some(mut engine) = self.ensure_engine_client().await? {
            let records: Vec<_> = events
                .into_iter()
                .map(|(run_id, event_type, data, _seq, metadata, ts)| {
                    let mut merged = metadata;
                    for (k, v) in &self.metadata {
                        if !merged.contains_key(k) {
                            merged.insert(k.clone(), v.clone());
                        }
                    }
                    let cid = merged.remove("cid").unwrap_or_default();
                    let pcid = merged.remove("pcid").unwrap_or_default();
                    let canonical_project_id =
                        canonical_project_id_from_metadata(&merged).unwrap_or_default();
                    let mut merged = with_project_metadata(merged, &canonical_project_id);
                    let tenant_id = merged
                        .remove("project_id")
                        .or_else(|| merged.remove("tenant_id"))
                        .unwrap_or_default();

                    client::build_engine_record(
                        tenant_id,
                        run_id,
                        event_type,
                        data,
                        ts,
                        String::new(),
                        cid,
                        pcid,
                        merged,
                    )
                })
                .collect();

            let count = records.len();
            engine.append_batch(records).await?;
            debug!("Engine batch checkpoint: {} events persisted", count);
            return Ok(());
        }

        // Legacy EE path doesn't support batch — fall back to individual emits
        warn!("emit_checkpoint_batch requires AGNT5_ENGINE_URL, events will be dropped");
        Ok(())
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
        let tenant_id = canonical_project_id_from_metadata(&metadata);

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
            tenant_id,
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

        // Capture the tokio runtime handle for emit_checkpoint_sync_blocking.
        // Python threads (via PyO3) are not tokio threads, so they need a stored handle.
        {
            if let Ok(mut guard) = self.tokio_handle.lock() {
                *guard = Some(tokio::runtime::Handle::current());
            }
        }

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
        let max_retries = self.config.max_retries;
        let infinite_retry = max_retries == 0;
        let base_delay = std::time::Duration::from_secs(1);
        let mut retry_count: u32 = 0;

        // Track disconnect time for reconnection duration metrics
        let mut disconnect_instant: Option<Instant> = None;

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
                let delay_secs = delay.as_secs_f64();

                // User-friendly reconnection messages (printed directly, not via tracing,
                // since these are user-facing status and should always be visible).
                //
                // Suppress the first two retries — most transient failures (notably
                // registration redirects per dev/bugs/coordinator-redirect-leaks-pod-dns.md,
                // brief network blips) recover within one retry. Surfacing them as
                // "[WARN] Connection lost" alarms users on every cold start.
                // Below the threshold, log at debug only.
                const QUIET_RETRY_THRESHOLD: u32 = 3;
                if retry_count >= QUIET_RETRY_THRESHOLD {
                    if infinite_retry {
                        eprintln!(
                            "[WARN] Reconnecting in {:.1}s... (attempt {})",
                            delay_secs, retry_count
                        );
                    } else {
                        eprintln!(
                            "[WARN] Reconnecting in {:.1}s... (attempt {}/{})",
                            delay_secs, retry_count, max_retries
                        );
                    }
                } else {
                    debug!(
                        retry = retry_count,
                        delay_secs, "Reconnecting silently before user-visible warning"
                    );
                }

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
            crate::telemetry::update_connection_state(1); // 1 = connecting
            if disconnect_instant.is_none() && retry_count > 0 {
                disconnect_instant = Some(Instant::now());
            }
            let was_reconnecting = retry_count > 0;

            // Create a new receiver for this connection attempt
            let shutdown_rx_inner = shutdown_tx.subscribe();

            match self
                .try_connect_and_run(
                    message_handler.clone(),
                    shutdown_rx_inner,
                    was_reconnecting,
                    disconnect_instant,
                )
                .await
            {
                Ok(()) => {
                    self.set_connection_state(ConnectionState::Disconnected);
                    crate::telemetry::update_connection_state(0); // 0 = disconnected
                    return Ok(());
                }
                Err(e) => {
                    if let crate::error::SdkError::RegistrationRedirect { endpoint, message } = &e {
                        self.set_owner_endpoint_hint(Some(endpoint.clone()));
                        // Redirect is an expected control-plane response — the loop
                        // handles it. Debug only. See dev/bugs/coordinator-redirect-leaks-pod-dns.md.
                        debug!(
                            "Registration redirected to owner coordinator {}: {}",
                            endpoint, message
                        );
                        continue;
                    }

                    if self.clear_owner_endpoint_hint() {
                        debug!(
                            "Cleared redirected owner coordinator hint after connection failure; retrying through configured routing"
                        );
                    }

                    // Check if we had a working session (Connected) that dropped,
                    // vs. failing to connect in the first place.
                    let was_connected =
                        matches!(self.connection_state(), ConnectionState::Connected);

                    // Record failed reconnection attempt (only for actual connect failures,
                    // not for an active session that dropped)
                    if retry_count > 0 && !was_connected {
                        crate::telemetry::record_reconnection_attempt(false);
                    }

                    // Store error for state tracking (used internally)
                    let error_msg =
                        format!("Connection failed (attempt {}): {}", retry_count + 1, e);
                    debug!("{}", error_msg);
                    self.set_connection_state(ConnectionState::Error(error_msg));
                    crate::telemetry::update_connection_state(0); // 0 = disconnected

                    if was_connected {
                        // Had a working session that dropped — reset retry count
                        // so backoff starts fresh for this new disconnect.
                        retry_count = 1;
                        // Capture disconnect instant for duration tracking
                        disconnect_instant = Some(Instant::now());
                    } else {
                        retry_count += 1;
                    }

                    // Check if we've exceeded max retries (skip check for infinite retry mode)
                    if !infinite_retry && retry_count >= max_retries {
                        // After max retries, exit instead of infinite loop
                        error!("Failed to connect after {} attempts, exiting", max_retries);
                        self.set_connection_state(ConnectionState::Error(format!(
                            "Failed to connect after {} attempts",
                            max_retries
                        )));
                        return Err(anyhow::anyhow!(
                            "Worker failed to connect to coordinator after {} attempts",
                            max_retries
                        )
                        .into());
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
        is_reconnect: bool,
        disconnect_instant: Option<Instant>,
    ) -> Result<()>
    where
        F: Fn(RuntimeMessage, flume::Sender<ServiceMessage>) -> Fut + Send + Clone + 'static,
        Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send + 'static,
    {
        // On reconnect, refresh membership from control plane before choosing endpoint.
        // On first connect, use the preferred endpoint (which may already include
        // a redirect hint from a previous NACK).
        let coordinator_endpoint = if is_reconnect {
            CoordinatorRouting::resolve(&self.config.worker_id, &self.config.coordinator_endpoint)
                .await
        } else {
            self.preferred_coordinator_endpoint()
        };
        let mut client = WorkerCoordinatorClient::connect(coordinator_endpoint.clone()).await?;

        // Create registration message with components
        // Merge user-provided metadata with auto-collected AGNT5_* env vars
        let mut metadata = self.metadata.clone();
        metadata.extend(collect_agnt5_env_vars());

        // Phase 6: declare data-path mode at registration. Default PUSH;
        // `AGNT5_WORKER_MODE=pull` opts into PULL (worker calls PollJobs
        // instead of receiving dispatches over the bidirectional stream).
        let is_pull_mode = matches!(
            std::env::var("AGNT5_WORKER_MODE").ok().as_deref(),
            Some("pull") | Some("PULL")
        );
        let mode = if is_pull_mode {
            crate::pb::WorkerMode::Pull as i32
        } else {
            crate::pb::WorkerMode::Push as i32
        };
        // Phase 6: stamp deployment_id from env so the coordinator's
        // proto-field path picks it up. Falls back to metadata key on
        // older coordinators that haven't been rebuilt yet.
        let deployment_id = std::env::var("AGNT5_DEPLOYMENT_ID").unwrap_or_default();

        // Phase 7a: declare concurrency budget so the coordinator can
        // size headroom reservations per priority class. Hoisted out of
        // the worker pool setup below so a single env read drives both
        // the local pool size and the registration field.
        let max_concurrency: u32 = std::env::var("AGNT5_MAX_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100);

        let registration = RegisterService {
            service_name: self.config.service_name.clone(),
            service_version: self.config.service_version.clone(),
            service_type: self.config.service_type.clone(),
            components: self.components.clone(),
            metadata,
            mode,
            deployment_id,
            max_concurrency,
        };

        // Use the working pattern - create stream with immediate registration
        let (tx, rx) = client
            .create_worker_stream_with_registration(self.config.worker_id.clone(), registration)
            .await?;

        self.set_owner_endpoint_hint(Some(coordinator_endpoint.clone()));

        if is_reconnect {
            eprintln!(
                "[INFO] Reconnected to coordinator ({})",
                coordinator_endpoint
            );
        } else {
            eprintln!("[INFO] Connected to coordinator ({})", coordinator_endpoint);
        }
        debug!("Worker {} registered successfully", self.config.worker_id);
        self.set_connection_state(ConnectionState::Connected);
        crate::telemetry::update_connection_state(2); // 2 = connected

        // Write health marker file so K8s readiness probe passes
        self.write_health_marker();

        // Record reconnection metrics on successful reconnect
        if is_reconnect {
            crate::telemetry::record_reconnection_attempt(true);
            if let Some(disc_instant) = disconnect_instant {
                crate::telemetry::record_reconnection_duration(
                    disc_instant.elapsed().as_secs_f64(),
                );
            }
        }

        // Open EventStream on EE for ephemeral events (SSE-only: tokens, progress, logs).
        // EE is the single SSE publisher — WC no longer publishes to Centrifuge.
        let event_stream_tx = match self.ensure_ee_client().await {
            Ok(mut ee_client) => {
                match crate::client::create_ee_event_stream(
                    &mut ee_client,
                    self.config.worker_id.clone(),
                )
                .await
                {
                    Ok(es_tx) => {
                        debug!("EE EventStream opened for SSE-only events");
                        Some(es_tx)
                    }
                    Err(e) => {
                        warn!("Failed to open EE EventStream, SSE-only events will use dispatch stream: {}", e);
                        None
                    }
                }
            }
            Err(e) => {
                warn!("Failed to get EE client for EventStream, SSE-only events will use dispatch stream: {}", e);
                None
            }
        };

        // Store senders so emit_checkpoint_sync can flush pending events
        if let Ok(mut guard) = self.event_stream_tx.lock() {
            *guard = event_stream_tx.clone();
        }
        if let Ok(mut guard) = self.dispatch_tx.lock() {
            *guard = Some(tx.clone());
        }

        // Start heartbeat task
        let heartbeat_task = self.spawn_heartbeat_task(tx.clone());

        // Start unified journal event flush task (replaces checkpoint, delta, span, log flush tasks)
        let journal_flush_task = self.spawn_journal_flush_task(tx.clone(), event_stream_tx.clone());

        // Reuse the concurrency budget computed for the registration
        // message above so the local pool size and the value reported
        // to the coordinator stay in lock-step. `usize` cast for the
        // pool channel + spawn loop below.
        let max_concurrency = max_concurrency as usize;

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

        // Phase 8: PULL workers own the poll task; PUSH workers never
        // spawn it. The legacy `AGNT5_POLL_ENABLED` env gate has been
        // removed — mode declared at registration is now the only switch.
        let poll_task = if is_pull_mode {
            let poll_shutdown = shutdown_rx.resubscribe();
            Some(self.spawn_poll_task(task_tx.clone(), poll_shutdown, max_concurrency))
        } else {
            None
        };

        // Main dispatch loop
        let dispatch_result = loop {
            tokio::select! {
                // Dispatch incoming messages to worker pool
                result = rx.recv_async() => {
                    match result {
                        Ok(runtime_message) => {
                            // Legacy CheckpointAck messages from older WC — ignore silently.
                            // Checkpoints now use WriteCheckpoint unary RPC to EE directly.
                            if runtime_message.message_type == RuntimeMessageType::CheckpointAck as i32 {
                                debug!("Ignoring legacy CheckpointAck on dispatch stream");
                                continue;
                            }

                            // WORKER_REPLACED: another connection registered with our
                            // worker_id. Shut down permanently — do NOT reconnect.
                            if runtime_message.message_type == RuntimeMessageType::WorkerReplaced as i32 {
                                warn!(
                                    "Worker {} received WORKER_REPLACED — another instance took over. Shutting down.",
                                    self.config.worker_id
                                );
                                eprintln!(
                                    "[WARN] Another worker instance connected with the same worker ID. This worker is shutting down."
                                );
                                break Ok(());
                            }

                            // Track is_streaming per run for ephemeral event gating
                            if let Some(ref msg_data) = runtime_message.message_data {
                                if let crate::pb::runtime_message::MessageData::DispatchComponent(ref req) = msg_data {
                                    if req.is_streaming {
                                        let run_id = if let Some(idx) = req.invocation_id.find(':') {
                                            req.invocation_id[..idx].to_string()
                                        } else {
                                            req.invocation_id.clone()
                                        };
                                        if let Ok(mut map) = self.streaming_runs.lock() {
                                            map.insert(run_id, true);
                                        }
                                    }
                                    // Phase 5: stash lease_id keyed by invocation_id so we
                                    // can echo it on the outbound response. This keeps
                                    // language bindings unaware of the fence token.
                                    if !req.lease_id.is_empty() {
                                        if let Ok(mut map) = self.pending_lease_ids.lock() {
                                            map.insert(req.invocation_id.clone(), req.lease_id.clone());
                                        }
                                    }
                                }
                            }

                            // Fast path: handle built-in scorers directly in Rust
                            if let Some(ref msg_data) = runtime_message.message_data {
                                if let crate::pb::runtime_message::MessageData::DispatchComponent(ref req) = msg_data {
                                    // component_type 10 = COMPONENT_TYPE_SCORER
                                    if req.component_type == 10 {
                                        if let Some(result) = crate::eval::builtin_scorer::execute(&req.component_name, &req.input_data) {
                                            let output_data = serde_json::to_vec(&result).unwrap_or_default();
                                            // Phase 5: drain stashed lease_id so the fast path
                                            // acks under the same fence as the request.
                                            let lease_id = if !req.lease_id.is_empty() {
                                                if let Ok(mut map) = self.pending_lease_ids.lock() {
                                                    map.remove(&req.invocation_id);
                                                }
                                                req.lease_id.clone()
                                            } else {
                                                String::new()
                                            };
                                            let response = DispatchComponentResponse {
                                                invocation_id: req.invocation_id.clone(),
                                                success: true,
                                                result: Some(
                                                    crate::pb::dispatch_component_response::Result::OutputData(output_data.clone()),
                                                ),
                                                error_message: String::new(),
                                                metadata: req.metadata.clone(),
                                                event_type: "run.completed".to_string(),
                                                content_index: 0,
                                                sequence: 0,
                                                attempt: 0,
                                                source_timestamp_ns: 0,
                                                lease_id,
                                            };
                                            let service_message = ServiceMessage {
                                                worker_id: self.config.worker_id.clone(),
                                                metadata: std::collections::HashMap::new(),
                                                message_type: Some(
                                                    crate::pb::service_message::MessageType::FunctionResponse(response),
                                                ),
                                            };
                                            if let Err(e) = response_tx.send_async(service_message).await {
                                                error!("Failed to send built-in scorer response: {}", e);
                                            }

                                            // Emit boundary events to EE via WriteCheckpoint so
                                            // journal entries are created and NATS terminal events
                                            // are published (the gateway waits on these).
                                            let run_id = if let Some(idx) = req.invocation_id.find(':') {
                                                req.invocation_id[..idx].to_string()
                                            } else {
                                                req.invocation_id.clone()
                                            };

                                            let timestamp_ns = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_nanos() as i64;

                                            // run.started
                                            if let Err(e) = self.emit_checkpoint_sync(
                                                run_id.clone(),
                                                "run.started".to_string(),
                                                req.input_data.clone(),
                                                0,
                                                req.metadata.clone(),
                                                timestamp_ns,
                                                5000,
                                            ).await {
                                                warn!("Built-in scorer: failed to emit run.started checkpoint: {}", e);
                                            }

                                            // run.completed
                                            if let Err(e) = self.emit_checkpoint_sync(
                                                run_id,
                                                "run.completed".to_string(),
                                                output_data.clone(),
                                                1,
                                                req.metadata.clone(),
                                                timestamp_ns,
                                                5000,
                                            ).await {
                                                warn!("Built-in scorer: failed to emit run.completed checkpoint: {}", e);
                                            }

                                            continue;
                                        }
                                        // Not a fast-path scorer — fall through to language handler
                                    }
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
                            debug!("Channel closed for worker {}, will reconnect: {}", self.config.worker_id, e);
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
                        Ok(mut service_message) => {
                            // Phase 5: stamp the stashed lease_id onto the response
                            // so the coordinator's fencing check passes. On terminal
                            // events we drain the map entry; on intermediate streaming
                            // events we leave it so the terminal ack still finds it.
                            // Clean up streaming_runs tracking for terminal events
                            if let Some(crate::pb::service_message::MessageType::FunctionResponse(ref mut resp)) = service_message.message_type {
                                let is_terminal = resp.event_type == "run.completed" || resp.event_type == "run.failed";
                                if resp.lease_id.is_empty() {
                                    if let Ok(mut map) = self.pending_lease_ids.lock() {
                                        if is_terminal {
                                            if let Some(lease_id) = map.remove(&resp.invocation_id) {
                                                resp.lease_id = lease_id;
                                            }
                                        } else if let Some(lease_id) = map.get(&resp.invocation_id) {
                                            resp.lease_id = lease_id.clone();
                                        }
                                    }
                                }
                                if is_terminal {
                                    let run_id = if let Some(idx) = resp.invocation_id.find(':') {
                                        resp.invocation_id[..idx].to_string()
                                    } else {
                                        resp.invocation_id.clone()
                                    };
                                    if let Ok(mut map) = self.streaming_runs.lock() {
                                        map.remove(&run_id);
                                    }
                                }
                            }

                            // Phase 8: route by declared worker mode, not by
                            // per-response metadata tagging. A PULL worker
                            // always acks via `CompleteJob`; a PUSH worker
                            // always responds over the bidirectional stream.
                            // Phase 7b's scheduler guarantees PULL workers
                            // never receive push dispatches, so the stream
                            // carries only control/heartbeat traffic for them.
                            if is_pull_mode {
                                self.handle_polled_job_response(service_message).await;
                            } else {
                                if let Err(e) = tx.send_async(service_message).await {
                                    error!("Failed to send response to coordinator: {}", e);
                                    break Err(crate::error::SdkError::Connection {
                                        message: format!("Send failed: {}", e),
                                        code: crate::error::ErrorCode::ConnectionFailed,
                                        source: None,
                                    });
                                }
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

        // Cancel poll task
        if let Some(task) = poll_task {
            task.abort();
        }

        // Cleanup: close channels and wait for workers
        info!("Worker {} shutting down task pool", self.config.worker_id);
        drop(task_tx); // Signal workers to exit
        drop(task_rx); // Close receiver
        drop(response_tx); // Close response sender

        // Clear cached EE client on disconnect so it reconnects on next connection
        {
            let mut guard = self.ee_client.lock().await;
            *guard = None;
        }

        // Wait for all worker tasks to complete
        for handle in worker_handles {
            let _ = handle.await;
        }

        // Remove health marker file so K8s readiness probe fails
        self.remove_health_marker();

        // Send shutdown message and stop background tasks
        let _ = self.send_shutdown_message(&tx).await;
        heartbeat_task.abort();
        journal_flush_task.abort();

        // Clear streaming runs tracking AFTER flush task is aborted,
        // so the flush task can drain any remaining SSE events first
        if let Ok(mut map) = self.streaming_runs.lock() {
            map.clear();
        }

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
    /// This task periodically flushes all buffered events to EE.
    /// Events are routed based on type:
    /// - SSE-only events (output.delta, log, etc.): Sent via EventStream for real-time SSE delivery
    /// - Boundary events (workflow.*, agent.*, lm.call.*): Sent via WriteJournalEventsBatch to EE for durable persistence + SSE
    ///
    /// All events go directly to EE — the dispatch stream is only used as a fallback
    /// for SSE-only events when EventStream is unavailable.
    fn spawn_journal_flush_task(
        &self,
        dispatch_tx: flume::Sender<ServiceMessage>,
        event_stream_tx: Option<flume::Sender<EventStreamMessage>>,
    ) -> tokio::task::JoinHandle<()> {
        let worker_id = self.config.worker_id.clone();
        let journal_queue = self.journal_queue.clone();
        let flush_interval_ms = journal_queue.flush_interval_ms();
        let batch_size = journal_queue.batch_size();
        let streaming_runs = self.streaming_runs.clone();
        let pending_lease_ids = self.pending_lease_ids.clone();
        let ee_endpoint = self.config.ee_endpoint.clone();
        let engine_endpoint = self.config.engine_endpoint.clone();

        // Cache project_id/deployment_id to avoid repeated env lookups per event.
        // `tenant_id` remains a legacy alias for compatibility with engine/EE APIs.
        let cached_project_id = canonical_project_id_from_env();
        let cached_deployment_id = std::env::var("AGNT5_DEPLOYMENT_ID").unwrap_or_default();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(flush_interval_ms));

            // Lazily-connected EE client for boundary event writes.
            // Separate from the Worker's ee_client to avoid lock contention with emit_checkpoint_sync.
            let mut ee_client: Option<ExecutionEngineServiceClient<Channel>> = None;

            // Lazily-connected Engine client (when AGNT5_ENGINE_URL is set).
            let mut engine: Option<EngineClient> = None;

            loop {
                interval.tick().await;

                // Drain batch of events
                let batch = journal_queue.drain_batch(batch_size);
                if batch.is_empty() {
                    continue;
                }

                // ── Engine path: send ALL events via AppendBatch ──
                if let Some(ref ep) = engine_endpoint {
                    // Ensure engine client is connected
                    if engine.is_none() {
                        match EngineClient::connect(ep).await {
                            Ok(c) => {
                                debug!("Flush task: Engine client connected to {}", ep);
                                engine = Some(c);
                            }
                            Err(e) => {
                                warn!("Flush task: failed to connect to Engine {}: {}", ep, e);
                                // Re-queue all events for next flush
                                for event in batch.into_iter().rev() {
                                    journal_queue.push_front(event).ok();
                                }
                                journal_queue.record_error();
                                continue;
                            }
                        }
                    }

                    // Convert ALL events to engine Records (no SSE-only/boundary split)
                    let originals: Vec<JournalEventMessage> = batch;
                    let records: Vec<_> = originals
                        .iter()
                        .map(|e| {
                            let tenant = if let Some(ref tid) = e.tenant_id {
                                tid.clone()
                            } else {
                                cached_project_id.clone()
                            };
                            client::build_engine_record(
                                tenant,
                                e.run_id.clone(),
                                e.event_type.clone(),
                                e.data.clone(),
                                e.source_timestamp_ns,
                                String::new(),
                                e.correlation_id.clone(),
                                e.parent_correlation_id.clone(),
                                e.metadata.clone(),
                            )
                        })
                        .collect();

                    if let Some(ref mut eng) = engine {
                        match eng.append_batch(records).await {
                            Ok(written) => {
                                journal_queue.record_sent_batch(written as usize, 0);
                                debug!(
                                    "Flush task: wrote {} events to Engine (queue_size={})",
                                    written,
                                    journal_queue.len()
                                );
                            }
                            Err(e) => {
                                warn!("Flush task: Engine AppendBatch failed: {}", e);
                                engine = None; // Clear for reconnection
                                for event in originals.into_iter().rev() {
                                    journal_queue.push_front(event).ok();
                                }
                                journal_queue.record_error();
                            }
                        }
                    }
                    continue; // Skip EE path entirely
                }

                // ── Legacy EE path (AGNT5_ENGINE_URL not set) ──

                let mut sent_count = 0;
                let mut sse_only_count = 0;
                let mut boundary_events: Vec<(usize, crate::pb::WriteJournalEventRequest)> =
                    Vec::new();
                let mut boundary_originals: Vec<JournalEventMessage> = Vec::new();

                for event in batch {
                    let is_sse_only = event.is_sse_only;

                    // Route SSE-only events through EventStream if available.
                    // Skip ephemeral events for non-streaming runs — nobody is listening via SSE.
                    if is_sse_only {
                        let is_run_streaming = match streaming_runs.lock() {
                            Ok(map) => map.get(&event.run_id).copied().unwrap_or(false),
                            Err(poisoned) => {
                                warn!("streaming_runs mutex poisoned, assuming non-streaming for run_id={}", event.run_id);
                                poisoned
                                    .into_inner()
                                    .get(&event.run_id)
                                    .copied()
                                    .unwrap_or(false)
                            }
                        };
                        if !is_run_streaming {
                            continue; // Skip — no SSE listeners for this run
                        }
                        if let Some(ref es_tx) = event_stream_tx {
                            let es_msg = EventStreamMessage {
                                run_id: event.run_id.clone(),
                                event_type: event.event_type.clone(),
                                data: event.data.clone(),
                                trace_id: String::new(),
                                span_id: String::new(),
                                tenant_id: cached_project_id.clone(),
                                source_timestamp_ns: event.source_timestamp_ns,
                                worker_id: worker_id.clone(),
                            };

                            if let Err(e) = es_tx.send_async(es_msg).await {
                                warn!(
                                    "EventStream send failed, falling back to dispatch stream: type={} run_id={} error={}",
                                    event.event_type, event.run_id, e
                                );
                                // Fall through to dispatch stream fallback below
                            } else {
                                sse_only_count += 1;
                                sent_count += 1;
                                continue; // Successfully sent via EventStream
                            }
                        }
                        // No EventStream or EventStream failed — fallback to dispatch stream for SSE-only
                        let mut metadata = event.metadata.clone();
                        metadata = with_project_metadata(metadata, &cached_project_id);
                        if !cached_deployment_id.is_empty() {
                            metadata
                                .insert("deployment_id".to_string(), cached_deployment_id.clone());
                        }
                        // Phase 5: stamp stashed lease_id on SSE-only fallback responses.
                        let stashed_lease_id = match pending_lease_ids.lock() {
                            Ok(map) => map.get(&event.run_id).cloned().unwrap_or_default(),
                            Err(poisoned) => poisoned
                                .into_inner()
                                .get(&event.run_id)
                                .cloned()
                                .unwrap_or_default(),
                        };
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
                            lease_id: stashed_lease_id,
                        };
                        let service_message = ServiceMessage {
                            worker_id: worker_id.clone(),
                            metadata: std::collections::HashMap::new(),
                            message_type: Some(
                                crate::pb::service_message::MessageType::FunctionResponse(response),
                            ),
                        };
                        if let Err(e) = dispatch_tx.send_async(service_message).await {
                            warn!("Failed to send SSE-only event via dispatch fallback: type={} run_id={} error={}", event.event_type, event.run_id, e);
                            journal_queue.push_front(event).ok();
                            journal_queue.record_error();
                            break;
                        }
                        sse_only_count += 1;
                        sent_count += 1;
                        continue;
                    }

                    // Boundary event — collect for batch WriteJournalEventsBatch to EE
                    let mut metadata = event.metadata.clone();
                    metadata = with_project_metadata(metadata, &cached_project_id);
                    if !cached_deployment_id.is_empty() {
                        metadata
                            .entry("deployment_id".to_string())
                            .or_insert_with(|| cached_deployment_id.clone());
                    }
                    let tenant_id = metadata
                        .remove("project_id")
                        .or_else(|| metadata.remove("tenant_id"))
                        .unwrap_or_default();

                    let req = crate::pb::WriteJournalEventRequest {
                        run_id: event.run_id.clone(),
                        event_type: event.event_type.clone(),
                        data: event.data.clone(),
                        trace_id: String::new(),
                        span_id: String::new(),
                        tenant_id,
                        source_timestamp_ns: event.source_timestamp_ns,
                        correlation_id: event.correlation_id.clone(),
                        parent_event_id: event.parent_correlation_id.clone(),
                        metadata,
                    };

                    boundary_events.push((boundary_originals.len(), req));
                    boundary_originals.push(event);
                }

                // Send boundary events to EE via WriteJournalEventsBatch
                if !boundary_events.is_empty() {
                    let requests: Vec<crate::pb::WriteJournalEventRequest> =
                        boundary_events.into_iter().map(|(_, req)| req).collect();
                    let batch_count = requests.len();

                    // Ensure EE client is connected
                    if ee_client.is_none() {
                        match Channel::from_shared(ee_endpoint.clone()) {
                            Ok(ch) => {
                                match ch
                                    .connect_timeout(Duration::from_secs(10))
                                    .timeout(Duration::from_secs(30))
                                    .connect()
                                    .await
                                {
                                    Ok(channel) => {
                                        debug!(
                                            "Flush task: EE client connected to {}",
                                            ee_endpoint
                                        );
                                        ee_client =
                                            Some(ExecutionEngineServiceClient::new(channel));
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Flush task: failed to connect to EE {}: {}",
                                            ee_endpoint, e
                                        );
                                        // Re-queue all boundary events for next flush
                                        for event in boundary_originals.into_iter().rev() {
                                            journal_queue.push_front(event).ok();
                                        }
                                        journal_queue.record_error();
                                        // Continue — SSE-only events were already sent
                                        if sent_count > 0 {
                                            journal_queue
                                                .record_sent_batch(sent_count, sse_only_count);
                                        }
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Flush task: invalid EE endpoint {}: {}", ee_endpoint, e);
                                for event in boundary_originals.into_iter().rev() {
                                    journal_queue.push_front(event).ok();
                                }
                                journal_queue.record_error();
                                if sent_count > 0 {
                                    journal_queue.record_sent_batch(sent_count, sse_only_count);
                                }
                                continue;
                            }
                        }
                    }

                    if let Some(ref mut client) = ee_client {
                        let batch_req =
                            crate::pb::WriteJournalEventsBatchRequest { events: requests };
                        match client.write_journal_events_batch(batch_req).await {
                            Ok(resp) => {
                                let r = resp.into_inner();
                                sent_count += r.written_count as usize;
                                if !r.errors.is_empty() {
                                    warn!(
                                        "Flush task: {} boundary events had errors (written={})",
                                        r.errors.len(),
                                        r.written_count
                                    );
                                    for err in &r.errors {
                                        warn!("  event[{}]: {}", err.index, err.error_message);
                                    }
                                } else {
                                    debug!(
                                        "Flush task: wrote {} boundary events to EE",
                                        batch_count
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("Flush task: WriteJournalEventsBatch failed: {}", e);
                                // Clear client for reconnection
                                ee_client = None;
                                // Re-queue boundary events for next flush
                                for event in boundary_originals.into_iter().rev() {
                                    journal_queue.push_front(event).ok();
                                }
                                journal_queue.record_error();
                            }
                        }
                    }
                }

                if sent_count > 0 {
                    journal_queue.record_sent_batch(sent_count, sse_only_count);
                    debug!(
                        "Flushed {} journal events (boundary={}, sse_only={}, queue_size={})",
                        sent_count,
                        sent_count - sse_only_count,
                        sse_only_count,
                        journal_queue.len()
                    );
                }
            }
        })
    }

    /// Spawn a polling task that claims jobs from the durable queue (managed edition).
    /// Runs alongside the streaming dispatch loop. Uses exponential backoff when no jobs.
    fn spawn_poll_task(
        &self,
        task_tx: flume::Sender<RuntimeMessage>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
        max_concurrency: usize,
    ) -> tokio::task::JoinHandle<()> {
        let worker_id = self.config.worker_id.clone();
        let endpoint = self.config.resolved_coordinator_endpoint();
        let component_ids: Vec<String> = self.components.iter().map(|c| c.name.clone()).collect();
        let project_id = canonical_project_id_from_env();
        let deployment_id = std::env::var("AGNT5_DEPLOYMENT_ID").ok();
        let streaming_runs = self.streaming_runs.clone();

        // Polling config from env
        let initial_backoff_ms: u64 = std::env::var("AGNT5_POLL_INITIAL_BACKOFF_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);
        let max_backoff_ms: u64 = std::env::var("AGNT5_POLL_MAX_BACKOFF_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30000);

        tokio::spawn(async move {
            // Skip polling if no project context (not in managed mode).
            if project_id.is_empty() {
                eprintln!("[INFO] Job queue polling disabled (AGNT5_PROJECT_ID / AGNT5_TENANT_ID not set)");
                return;
            }

            // Skip if no component IDs available
            if component_ids.is_empty() {
                eprintln!("[INFO] Job queue polling disabled (no components registered)");
                return;
            }

            // Create dedicated gRPC client for polling
            let mut client = match WorkerCoordinatorClient::connect(endpoint.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[WARN] Job queue poll task failed to connect: {}", e);
                    return;
                }
            };

            eprintln!(
                "[INFO] Job queue polling started ({} components, tenant={})",
                component_ids.len(),
                project_id
            );

            let mut backoff = Duration::from_millis(initial_backoff_ms);
            let max_backoff = Duration::from_millis(max_backoff_ms);

            loop {
                // Wait with backoff, respecting shutdown
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Poll task shutting down");
                        return;
                    }
                    _ = tokio::time::sleep(backoff) => {}
                }

                // Capacity-aware: only poll when worker pool has spare slots
                let queue_len = task_tx.len();
                let available = max_concurrency.saturating_sub(queue_len);
                if available == 0 {
                    debug!("Poll task: worker pool full (queue={}), waiting", queue_len);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }

                let max_jobs = available.min(10) as i32; // Cap at 10 per poll

                match client
                    .poll_jobs(PollJobsRequest {
                        worker_id: worker_id.clone(),
                        component_ids: component_ids.clone(),
                        max_jobs,
                        tenant_id: project_id.clone(),
                        deployment_id: deployment_id.clone(),
                        claim_timeout_ms: 300_000,
                    })
                    .await
                {
                    Ok(resp) if resp.jobs.is_empty() => {
                        // No work — log the current backoff so empty-poll behavior is visible.
                        eprintln!(
                            "[INFO] Job queue: no jobs available, next poll in {}ms",
                            backoff.as_millis()
                        );
                        backoff = std::cmp::min(backoff * 2, max_backoff);
                    }
                    Ok(resp) => {
                        // Reset backoff on successful poll
                        backoff = Duration::from_millis(initial_backoff_ms);
                        let job_count = resp.jobs.len();
                        eprintln!("[INFO] Job queue: claimed {} jobs", job_count);

                        for job in resp.jobs {
                            // Convert JobAssignment → RuntimeMessage (DispatchComponentRequest).
                            //
                            // Phase 8: the legacy `_source=poll`/`_job_id`/
                            // `_tenant_id` synthetic metadata tags are gone.
                            // The dispatch loop now routes responses by worker
                            // mode (PULL → CompleteJob, PUSH → stream), and
                            // `handle_polled_job_response` derives job_id from
                            // `resp.invocation_id` (which equals run_id which
                            // equals job_id on the coordinator's poll path)
                            // and project identity from `AGNT5_PROJECT_ID`
                            // (falling back to legacy `AGNT5_TENANT_ID`).
                            let mut metadata = job.metadata.clone();
                            if !job.trace_id.is_empty() {
                                metadata.insert("trace_id".to_string(), job.trace_id.clone());
                            }

                            // Check stream_mode before metadata is moved into the struct
                            let is_streaming =
                                metadata.get("stream_mode").map_or(false, |m| m == "full");
                            let session_id =
                                metadata.get("session_id").cloned().unwrap_or_default();
                            let user_id = metadata.get("user_id").cloned().unwrap_or_default();

                            // Phase 5: extract lease/priority/deployment from metadata
                            // (PULL path: coordinator stuffs lease info into JobAssignment.metadata).
                            let lease_id = metadata.get("lease_id").cloned().unwrap_or_default();
                            let deployment_id =
                                metadata.get("deployment_id").cloned().unwrap_or_default();
                            let priority = metadata
                                .get("priority")
                                .and_then(|v| v.parse::<i32>().ok())
                                .unwrap_or(0);

                            // Use invocation_id = run_id (this is how the WC dispatches)
                            let runtime_message = RuntimeMessage {
                                worker_id: String::new(),
                                message_type: RuntimeMessageType::InvokeFunction as i32,
                                metadata: HashMap::new(),
                                message_data: Some(
                                    crate::pb::runtime_message::MessageData::DispatchComponent(
                                        crate::pb::DispatchComponentRequest {
                                            invocation_id: job.run_id.clone(),
                                            service_name: String::new(),
                                            component_type: job.component_type,
                                            component_name: job.component_name.clone(),
                                            input_data: job.input_data,
                                            metadata,
                                            attempt: job.attempt,
                                            // Unused fields for polled jobs
                                            object_id: String::new(),
                                            method_name: String::new(),
                                            flow_instance_id: String::new(),
                                            flow_step: 0,
                                            state_snapshot: Vec::new(),
                                            journal_position: 0,
                                            step_checkpoints: Vec::new(),
                                            session_id,
                                            user_id,
                                            is_streaming,
                                            priority,
                                            deployment_id,
                                            lease_id,
                                            // Phase 7f: polled jobs inherit the policy the
                                            // orchestrator already stamped on the run's
                                            // `run.queued` metadata; the worker-side path
                                            // never reads `retry_policy` so None is fine.
                                            retry_policy: None,
                                        },
                                    ),
                                ),
                            };

                            // Track streaming runs for polled jobs (same as dispatch stream path)
                            // Without this, the journal flush task drops SSE-only events (logs, deltas)
                            // because it doesn't know the run has an SSE listener.
                            if is_streaming {
                                let run_id = job.run_id.clone();
                                if let Ok(mut map) = streaming_runs.lock() {
                                    map.insert(run_id, true);
                                }
                            }

                            if let Err(e) = task_tx.send_async(runtime_message).await {
                                warn!("Poll task: failed to dispatch job to worker pool: {}", e);
                                break;
                            }
                        }

                        eprintln!(
                            "[INFO] Job queue: dispatched {} jobs to worker pool",
                            job_count
                        );
                    }
                    Err(e) => {
                        // Phase 8: the Unimplemented fallback is gone — the
                        // coordinator always implements PollJobs/CompleteJob
                        // now that managed-edition gating has been removed.
                        // Any error here is a real transport/server problem,
                        // so back off and retry instead of silently stopping.
                        eprintln!("[WARN] Job queue poll error: {}", e);
                        backoff = std::cmp::min(backoff * 2, max_backoff);
                    }
                }
            }
        })
    }

    /// Handle a polled job response by calling CompleteJob RPC.
    ///
    /// Called from the dispatch loop on PULL workers (mode-based routing,
    /// Phase 8). On the coordinator's poll path `job_id == run_id`, so we
    /// derive the job_id from `resp.invocation_id` — stripping any
    /// `:suffix` the worker appends for streaming invocations. Project identity
    /// comes from `AGNT5_PROJECT_ID`, falling back to legacy `AGNT5_TENANT_ID`.
    async fn handle_polled_job_response(&self, service_message: ServiceMessage) {
        let (job_id, success, output_data, error_message, error_code) =
            match &service_message.message_type {
                Some(crate::pb::service_message::MessageType::FunctionResponse(resp)) => {
                    // Derive job_id from invocation_id (strip streaming suffix).
                    let jid = if let Some(idx) = resp.invocation_id.find(':') {
                        resp.invocation_id[..idx].to_string()
                    } else {
                        resp.invocation_id.clone()
                    };
                    if jid.is_empty() {
                        warn!("Polled job response missing invocation_id; dropping");
                        return;
                    }
                    let output = match &resp.result {
                        Some(crate::pb::dispatch_component_response::Result::OutputData(data)) => {
                            data.clone()
                        }
                        _ => Vec::new(),
                    };
                    (
                        jid,
                        resp.success,
                        output,
                        resp.error_message.clone(),
                        resp.metadata.get("error_code").cloned().unwrap_or_default(),
                    )
                }
                _ => {
                    warn!("Unexpected message type for polled job completion");
                    return;
                }
            };

        // Phase 8: PULL workers derive tenant_id from the same env var the
        // poll task uses for the PollJobs request.
        let tenant_id = canonical_project_id_from_env();

        // Call CompleteJob RPC
        let endpoint = self.config.resolved_coordinator_endpoint();
        let worker_id = self.config.worker_id.clone();

        // Spawn a task to avoid blocking the dispatch loop
        tokio::spawn(async move {
            let mut client = match WorkerCoordinatorClient::connect(endpoint).await {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "Failed to connect for CompleteJob: job_id={} error={}",
                        job_id, e
                    );
                    return;
                }
            };

            match client
                .complete_job(CompleteJobRequest {
                    job_id: job_id.clone(),
                    worker_id,
                    success,
                    output_data,
                    error_message,
                    error_code,
                    metadata: HashMap::new(),
                    tenant_id,
                })
                .await
            {
                Ok(_) => {
                    debug!("CompleteJob succeeded: job_id={}", job_id);
                }
                Err(e) => {
                    error!("CompleteJob failed: job_id={} error={}", job_id, e);
                }
            }
        });
    }

    /// Write a health marker file so the K8s readiness probe passes.
    /// The file is written to `$AGNT5_HEALTH_DIR/worker_{id}.txt`.
    fn write_health_marker(&self) {
        let health_dir = std::env::var("AGNT5_HEALTH_DIR").unwrap_or_else(|_| "/tmp/health".into());
        if let Err(e) = std::fs::create_dir_all(&health_dir) {
            warn!("Failed to create health dir {}: {}", health_dir, e);
            return;
        }
        let path = format!("{}/worker_{}.txt", health_dir, self.config.worker_id);
        if let Err(e) = std::fs::write(&path, "") {
            warn!("Failed to write health marker {}: {}", path, e);
        } else {
            debug!("Wrote health marker file {}", path);
        }
    }

    /// Remove the health marker file so the K8s readiness probe fails.
    fn remove_health_marker(&self) {
        let health_dir = std::env::var("AGNT5_HEALTH_DIR").unwrap_or_else(|_| "/tmp/health".into());
        let path = format!("{}/worker_{}.txt", health_dir, self.config.worker_id);
        if let Err(e) = std::fs::remove_file(&path) {
            // Not an error if the file doesn't exist (e.g., first connect failed before marker was written)
            if e.kind() != std::io::ErrorKind::NotFound {
                warn!("Failed to remove health marker {}: {}", path, e);
            }
        } else {
            debug!("Removed health marker file {}", path);
        }
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
                debug!(
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
