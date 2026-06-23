use crate::client::{self, EngineClient, WorkerCoordinatorClient};
use crate::error::{Result, SdkError};
use crate::journal_queue::{JournalEventMessage, JournalEventQueue, JournalQueueConfig};
use crate::pb::{
    execution_engine_service_client::ExecutionEngineServiceClient, CompleteJobRequest,
    ComponentInfo, DispatchComponentResponse, EventStreamMessage, HealthCheck, JobAssignment,
    PollJobRequest, RegisterService, RegisterWorkerSessionRequest, RenewJobLeaseRequest,
    ReportWorkerCapacityRequest, RuntimeMessage, RuntimeMessageType, ServiceMessage,
    UnregisterService, WorkerCapability, WorkerHealthStatus, WorkerSlotPolicy,
    WriteCheckpointRequest,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as TokioMutex;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const PARKED_WORKER_SESSION_REGISTER_ATTEMPTS: usize = 3;
const PARKED_WORKER_SESSION_REGISTER_RETRY_MS: u64 = 1_000;
const PARKED_WORKER_SESSION_TRANSIENT_RETRY_MAX_MS: u64 = 32_000;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParkedWorkerSessionRegistrationResult {
    Registered(String),
    Rejected,
}

#[derive(Clone)]
struct ParkedWorkerSessionRegistration {
    worker_id: String,
    project_id: String,
    deployment_id: String,
    min_slots: usize,
    max_slots: usize,
    capabilities: Vec<WorkerCapability>,
    components: Vec<ComponentInfo>,
    service_name: String,
    service_version: String,
    service_type: String,
}

impl ParkedWorkerSessionRegistration {
    fn request(&self) -> RegisterWorkerSessionRequest {
        RegisterWorkerSessionRequest {
            worker_id: self.worker_id.clone(),
            project_id: self.project_id.clone(),
            deployment_id: self.deployment_id.clone(),
            max_slots: self.max_slots as u32,
            slot_policy: Some(WorkerSlotPolicy {
                min_slots: self.min_slots as u32,
                max_slots: self.max_slots as u32,
                target_cpu_usage: 0.75,
                target_memory_usage: 0.80,
                ramp_throttle_ms: 1_000,
            }),
            capabilities: self.capabilities.clone(),
            components: self.components.clone(),
            service_name: self.service_name.clone(),
            service_version: self.service_version.clone(),
            service_type: self.service_type.clone(),
        }
    }
}

fn take_correlation_ids(metadata: &mut HashMap<String, String>) -> (String, String) {
    let correlation_id = metadata
        .remove("cid")
        .or_else(|| metadata.remove("correlation_id"))
        .unwrap_or_default();
    let parent_correlation_id = metadata
        .remove("pcid")
        .or_else(|| metadata.remove("parent_correlation_id"))
        .unwrap_or_default();
    (correlation_id, parent_correlation_id)
}

fn runtime_message_from_job_assignment(
    job: JobAssignment,
) -> (RuntimeMessage, bool, String, String) {
    let mut metadata = job.metadata.clone();
    if !job.trace_id.is_empty() {
        metadata.insert("trace_id".to_string(), job.trace_id.clone());
    }
    if !job.lease_id.is_empty() {
        metadata
            .entry("lease_id".to_string())
            .or_insert_with(|| job.lease_id.clone());
    }
    if job.lease_expires_at_ms > 0 {
        metadata
            .entry("lease_expires_at_ms".to_string())
            .or_insert_with(|| job.lease_expires_at_ms.to_string());
    }

    let is_streaming = metadata.get("stream_mode").map_or(false, |m| m == "full");
    let session_id = metadata.get("session_id").cloned().unwrap_or_default();
    let user_id = metadata.get("user_id").cloned().unwrap_or_default();
    let lease_id = if !job.lease_id.is_empty() {
        job.lease_id.clone()
    } else {
        metadata.get("lease_id").cloned().unwrap_or_default()
    };
    let deployment_id = metadata.get("deployment_id").cloned().unwrap_or_default();
    let priority = metadata
        .get("priority")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);
    let run_id = job.run_id.clone();

    let runtime_message = RuntimeMessage {
        worker_id: String::new(),
        message_type: RuntimeMessageType::InvokeFunction as i32,
        metadata: HashMap::new(),
        message_data: Some(crate::pb::runtime_message::MessageData::DispatchComponent(
            crate::pb::DispatchComponentRequest {
                invocation_id: job.run_id,
                service_name: String::new(),
                component_type: job.component_type,
                component_name: job.component_name,
                input_data: job.input_data,
                metadata,
                attempt: job.attempt,
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
                lease_id: lease_id.clone(),
                retry_policy: None,
            },
        )),
    };

    (runtime_message, is_streaming, run_id, lease_id)
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

fn worker_capabilities(components: &[ComponentInfo]) -> Vec<WorkerCapability> {
    let mut seen = HashSet::new();
    let mut capabilities = Vec::with_capacity(
        components.len() + crate::eval::builtin_scorer::BUILTIN_SCORER_NAMES.len(),
    );
    for component in components {
        if !component.name.is_empty() && seen.insert(component.name.clone()) {
            capabilities.push(WorkerCapability {
                component_type: component.component_type,
                component_name: component.name.clone(),
            });
        }
    }
    for scorer in crate::eval::builtin_scorer::BUILTIN_SCORER_NAMES {
        if crate::eval::builtin_scorer::can_execute_locally(scorer)
            && seen.insert((*scorer).to_string())
        {
            capabilities.push(WorkerCapability {
                component_type: crate::pb::ComponentType::Scorer as i32,
                component_name: (*scorer).to_string(),
            });
        }
    }
    capabilities
}

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

    /// Declared concurrency budget: the max in-flight handler invocations
    /// this worker can serve. Sets both the local pool size and the
    /// `max_concurrency` reported at registration (the coordinator's
    /// per-priority headroom denominator). Language bindings can set this
    /// directly; otherwise it falls back to the `AGNT5_MAX_CONCURRENCY` env
    /// var and finally a default of 100. `None` = "not explicitly set".
    pub max_concurrency: Option<u32>,
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

        // Concurrency budget: seed from the env var so existing deployments
        // keep working; language bindings may overwrite before `run()`.
        let max_concurrency = std::env::var("AGNT5_MAX_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok());

        Self {
            service_name,
            service_version,
            service_type,
            worker_id,
            coordinator_endpoint,
            ee_endpoint,
            max_retries,
            engine_endpoint,
            max_concurrency,
        }
    }

    /// Endpoint the worker dials. Used to be a client-side Maglev lookup
    /// that picked the "owning" coordinator pod to skip a registration
    /// redirect; the runtime no longer redirects, so this is just the
    /// configured endpoint.
    pub fn resolved_coordinator_endpoint(&self) -> String {
        self.coordinator_endpoint.clone()
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
    metadata.get("project_id").cloned()
}

fn canonical_project_id_from_env() -> String {
    std::env::var("AGNT5_PROJECT_ID").ok().unwrap_or_default()
}

fn with_project_metadata(
    mut metadata: HashMap<String, String>,
    project_id: &str,
) -> HashMap<String, String> {
    if !project_id.is_empty() {
        metadata
            .entry("project_id".to_string())
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
    /// Per-invocation soft-cancel channels keyed by run_id. A oneshot sender
    /// is registered while a dispatched invocation runs; a CancelExecution
    /// message from the coordinator fires it, the pool task's `select!` drops
    /// the handler future (soft cancel) and frees the slot. Keyed by run_id to
    /// match the coordinator's cancellation key.
    cancel_tokens: Arc<std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
    /// Optional language-registered cooperative cancel hook. When set, a
    /// CancelExecution invokes it with the run_id so the language binding can
    /// cancel its own task/promise (raising CancelledError / aborting the
    /// AbortSignal), letting the handler unwind and run cleanup. When absent,
    /// we fall back to the soft oneshot drop above (frees the slot but lets the
    /// language coroutine run to completion).
    cancel_hook: Arc<std::sync::Mutex<Option<Box<dyn Fn(String) + Send + Sync>>>>,
    /// EventStream sender for SSE-only events (EE path). Set during run().
    event_stream_tx: Arc<std::sync::Mutex<Option<flume::Sender<EventStreamMessage>>>>,
    /// Dispatch stream sender (bidirectional gRPC to WC). Used by emit_checkpoint_sync
    /// to flush pending SSE-only events before terminal checkpoints, ensuring they
    /// arrive while the invocation is still tracked in pendingStreamInvocations.
    dispatch_tx: Arc<std::sync::Mutex<Option<flume::Sender<ServiceMessage>>>>,
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

/// Extract the cancellation key (run_id) for a dispatched invocation, if this
/// message carries one. Returns None for non-dispatch messages. The run_id is
/// the part of `invocation_id` before the first `:` (sub-invocation suffix).
fn dispatch_run_key(msg: &RuntimeMessage) -> Option<String> {
    match &msg.message_data {
        Some(crate::pb::runtime_message::MessageData::DispatchComponent(req)) => Some(
            req.invocation_id
                .split(':')
                .next()
                .unwrap_or(&req.invocation_id)
                .to_string(),
        ),
        _ => None,
    }
}

// RAII guard so the in-flight count is decremented even if a handler panics or
// is cancelled. Parked polling uses this same guard so each parked slot maps to
// one active handler invocation, not one queued local message.
struct InFlightGuard(Arc<std::sync::atomic::AtomicUsize>);

impl InFlightGuard {
    fn enter(c: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
        c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        InFlightGuard(c.clone())
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

async fn execute_runtime_message_for_response<F, Fut>(
    worker_name: &str,
    runtime_message: RuntimeMessage,
    response_tx: flume::Sender<ServiceMessage>,
    handler: F,
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    cancel_tokens: Arc<std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
) -> Option<ServiceMessage>
where
    F: Fn(RuntimeMessage, flume::Sender<ServiceMessage>) -> Fut + Send + Clone + 'static,
    Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send + 'static,
{
    let _in_flight = InFlightGuard::enter(&in_flight);
    let tx_clone = response_tx.clone();

    let run_key = dispatch_run_key(&runtime_message);
    let result = if let Some(key) = run_key.clone() {
        let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
        if let Ok(mut m) = cancel_tokens.lock() {
            m.insert(key.clone(), cancel_tx);
        }
        let outcome = tokio::select! {
            res = handler(runtime_message, tx_clone) => Some(res),
            _ = cancel_rx => None,
        };
        if let Ok(mut m) = cancel_tokens.lock() {
            m.remove(&key);
        }
        outcome
    } else {
        Some(handler(runtime_message, tx_clone).await)
    };

    let response = match result {
        Some(Ok(Some(response))) => Some(response),
        Some(Ok(None)) => None,
        Some(Err(e)) => {
            error!("Worker {} handler error: {}", worker_name, e);
            None
        }
        None => {
            debug!("Worker {} invocation cancelled by request", worker_name);
            None
        }
    };

    response
}

async fn execute_runtime_message<F, Fut>(
    worker_name: &str,
    runtime_message: RuntimeMessage,
    response_tx: flume::Sender<ServiceMessage>,
    handler: F,
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    cancel_tokens: Arc<std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
) where
    F: Fn(RuntimeMessage, flume::Sender<ServiceMessage>) -> Fut + Send + Clone + 'static,
    Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send + 'static,
{
    if let Some(response) = execute_runtime_message_for_response(
        worker_name,
        runtime_message,
        response_tx.clone(),
        handler,
        in_flight,
        cancel_tokens,
    )
    .await
    {
        if let Err(e) = response_tx.send_async(response).await {
            error!("Worker {} failed to send response: {}", worker_name, e);
        }
    }
}

struct PolledJobCompletion {
    job_id: String,
    success: bool,
    output_data: Vec<u8>,
    error_message: String,
    error_code: String,
    lease_id: String,
}

fn polled_job_completion_from_service_message(
    service_message: &ServiceMessage,
    fallback_lease_id: &str,
) -> Option<PolledJobCompletion> {
    match &service_message.message_type {
        Some(crate::pb::service_message::MessageType::FunctionResponse(resp)) => {
            let job_id = if let Some(idx) = resp.invocation_id.find(':') {
                resp.invocation_id[..idx].to_string()
            } else {
                resp.invocation_id.clone()
            };
            if job_id.is_empty() {
                warn!("Polled job response missing invocation_id; dropping");
                return None;
            }

            let output_data = match &resp.result {
                Some(crate::pb::dispatch_component_response::Result::OutputData(data)) => {
                    data.clone()
                }
                _ => Vec::new(),
            };
            let lease_id = if resp.lease_id.is_empty() {
                fallback_lease_id.to_string()
            } else {
                resp.lease_id.clone()
            };

            Some(PolledJobCompletion {
                job_id,
                success: resp.success,
                output_data,
                error_message: resp.error_message.clone(),
                error_code: resp.metadata.get("error_code").cloned().unwrap_or_default(),
                lease_id,
            })
        }
        _ => {
            warn!("Unexpected message type for polled job completion");
            None
        }
    }
}

async fn complete_polled_job_with_client(
    client: &mut WorkerCoordinatorClient,
    worker_id: &str,
    worker_session_id: &str,
    tenant_id: &str,
    completion: PolledJobCompletion,
) -> Result<()> {
    let job_id = completion.job_id.clone();
    match client
        .complete_job(CompleteJobRequest {
            job_id: completion.job_id,
            worker_id: worker_id.to_string(),
            success: completion.success,
            output_data: completion.output_data,
            error_message: completion.error_message,
            error_code: completion.error_code,
            metadata: HashMap::new(),
            project_id: tenant_id.to_string(),
            lease_id: completion.lease_id,
            worker_session_id: worker_session_id.to_string(),
        })
        .await
    {
        Ok(_) => {
            debug!("CompleteJob succeeded: job_id={}", job_id);
            Ok(())
        }
        Err(e) => {
            error!("CompleteJob failed: job_id={} error={}", job_id, e);
            Err(e)
        }
    }
}

async fn complete_or_forward_parked_response(
    client: &mut WorkerCoordinatorClient,
    service_message: ServiceMessage,
    fallback_lease_id: &str,
    worker_id: &str,
    worker_session_id: &Arc<TokioMutex<String>>,
    tenant_id: &str,
    slot_idx: usize,
    response_tx: &flume::Sender<ServiceMessage>,
) -> bool {
    let Some(completion) =
        polled_job_completion_from_service_message(&service_message, fallback_lease_id)
    else {
        if let Err(e) = response_tx.send_async(service_message).await {
            error!(
                "Parked poll slot {} failed to send response: {}",
                slot_idx, e
            );
        }
        return false;
    };

    let job_id = completion.job_id.clone();
    let started = Instant::now();
    let current_session_id = worker_session_id.lock().await.clone();
    if let Err(e) = complete_polled_job_with_client(
        client,
        worker_id,
        &current_session_id,
        tenant_id,
        completion,
    )
    .await
    {
        warn!("Parked poll slot {} CompleteJob failed: {}", slot_idx, e);
        return false;
    } else {
        let elapsed = started.elapsed();
        if elapsed > Duration::from_millis(500) {
            warn!(
                "Parked poll slot {} CompleteJob was slow: job_id={} elapsed_ms={}",
                slot_idx,
                job_id,
                elapsed.as_millis()
            );
        } else {
            debug!(
                "Parked poll slot {} CompleteJob acked: job_id={} elapsed_ms={}",
                slot_idx,
                job_id,
                elapsed.as_millis()
            );
        }
    }

    true
}

fn parked_lease_renew_interval_ms(lease_timeout_ms: i64) -> u64 {
    let timeout_ms = lease_timeout_ms.max(10_000) as u64;
    let renewal_ms = (timeout_ms / 2).clamp(5_000, 60_000);
    renewal_ms.min(timeout_ms.saturating_sub(1_000).max(1_000))
}

fn parked_lease_renew_interval_with_jitter_ms(lease_timeout_ms: i64) -> u64 {
    let base = parked_lease_renew_interval_ms(lease_timeout_ms);
    let jitter = rand::random::<f64>() * 0.20 - 0.10;
    ((base as f64) * (1.0 + jitter)).round().max(1_000.0) as u64
}

fn parked_lease_danger_retry_ms(lease_timeout_ms: i64) -> u64 {
    let timeout_ms = lease_timeout_ms.max(10_000) as u64;
    (timeout_ms / 10).clamp(500, 5_000)
}

async fn report_worker_capacity_with_client(
    client: &mut WorkerCoordinatorClient,
    worker_id: &str,
    worker_session_id: &str,
    open_poll_slots: Arc<std::sync::atomic::AtomicUsize>,
    active_slots: Arc<std::sync::atomic::AtomicUsize>,
    desired_slots: usize,
    effective_max_slots: usize,
) {
    let open_poll_slots = open_poll_slots.load(std::sync::atomic::Ordering::Relaxed) as u32;
    let active_slots = active_slots.load(std::sync::atomic::Ordering::Relaxed) as u32;
    if let Err(e) = client
        .report_worker_capacity(ReportWorkerCapacityRequest {
            worker_id: worker_id.to_string(),
            worker_session_id: worker_session_id.to_string(),
            open_poll_slots,
            active_slots,
            desired_slots: desired_slots as u32,
            effective_max_slots: effective_max_slots as u32,
            cpu_usage: 0.0,
            memory_usage: 0.0,
            observed_at_ms: current_time_ms(),
        })
        .await
    {
        debug!("ReportWorkerCapacity failed: {}", e);
    }
}

fn spawn_parked_capacity_reporter(
    mut client: WorkerCoordinatorClient,
    worker_id: String,
    worker_session_id: Arc<TokioMutex<String>>,
    open_poll_slots: Arc<std::sync::atomic::AtomicUsize>,
    active_slots: Arc<std::sync::atomic::AtomicUsize>,
    desired_slots: usize,
    effective_max_slots: usize,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    let interval_ms = env_usize("AGNT5_CAPACITY_REPORT_INTERVAL_MS")
        .unwrap_or(5_000)
        .clamp(1_000, 60_000) as u64;
    tokio::spawn(async move {
        let current_session_id = worker_session_id.lock().await.clone();
        report_worker_capacity_with_client(
            &mut client,
            &worker_id,
            &current_session_id,
            open_poll_slots.clone(),
            active_slots.clone(),
            desired_slots,
            effective_max_slots,
        )
        .await;

        let mut interval = tokio::time::interval(Duration::from_millis(interval_ms));
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => return,
                _ = interval.tick() => {
                    let current_session_id = worker_session_id.lock().await.clone();
                    report_worker_capacity_with_client(
                        &mut client,
                        &worker_id,
                        &current_session_id,
                        open_poll_slots.clone(),
                        active_slots.clone(),
                        desired_slots,
                        effective_max_slots,
                    )
                    .await;
                }
            }
        }
    })
}

fn is_worker_session_inactive_error(error: &SdkError) -> bool {
    // Keep in sync with runtime's `Status::permission_denied("worker session is not active")`.
    error
        .to_string()
        .to_ascii_lowercase()
        .contains("worker session is not active")
}

fn is_parked_worker_session_registration_rejection(error: &SdkError) -> bool {
    let error = error.to_string().to_ascii_lowercase();
    [
        "invalid argument",
        "permission denied",
        "unauthenticated",
        "failed precondition",
        "out of range",
        "unimplemented",
    ]
    .iter()
    .any(|needle| error.contains(needle))
}

fn parked_worker_session_was_refreshed(
    current_session_id: &str,
    observed_session_id: &str,
) -> bool {
    current_session_id != observed_session_id
}

fn exit_parked_worker_process(reason: &str) -> ! {
    error!("{}", reason);
    // Managed pull workers cannot make progress without a valid session. Exiting is intentional:
    // Kubernetes should replace an unrecoverable worker instead of leaving it Ready but unable to poll.
    std::process::exit(1);
}

async fn register_parked_worker_session_with_retries(
    client: &mut WorkerCoordinatorClient,
    registration: &ParkedWorkerSessionRegistration,
    reason: &str,
) -> ParkedWorkerSessionRegistrationResult {
    let mut rejection_attempts = 0;
    let mut transient_attempts = 0;
    let mut transient_delay = Duration::from_millis(PARKED_WORKER_SESSION_REGISTER_RETRY_MS);
    loop {
        match client.register_worker_session(registration.request()).await {
            Ok(session) => {
                return ParkedWorkerSessionRegistrationResult::Registered(
                    session.worker_session_id,
                );
            }
            Err(e) if is_parked_worker_session_registration_rejection(&e) => {
                rejection_attempts += 1;
                warn!(
                    "{} attempt {}/{} failed: {}",
                    reason, rejection_attempts, PARKED_WORKER_SESSION_REGISTER_ATTEMPTS, e
                );
                if rejection_attempts >= PARKED_WORKER_SESSION_REGISTER_ATTEMPTS {
                    return ParkedWorkerSessionRegistrationResult::Rejected;
                }
                tokio::time::sleep(Duration::from_millis(
                    PARKED_WORKER_SESSION_REGISTER_RETRY_MS,
                ))
                .await;
            }
            Err(e) => {
                transient_attempts += 1;
                warn!(
                    "{} transient attempt {} failed: {}; retrying in {}ms",
                    reason,
                    transient_attempts,
                    e,
                    transient_delay.as_millis()
                );
                tokio::time::sleep(transient_delay).await;
                transient_delay = (transient_delay * 2).min(Duration::from_millis(
                    PARKED_WORKER_SESSION_TRANSIENT_RETRY_MAX_MS,
                ));
            }
        }
    }
}

async fn try_refresh_parked_worker_session_once(
    client: &mut WorkerCoordinatorClient,
    session_id: &Arc<TokioMutex<String>>,
    observed_session_id: &str,
    registration: &ParkedWorkerSessionRegistration,
    slot_idx: usize,
) -> bool {
    let current_session_id = session_id.lock().await.clone();
    if parked_worker_session_was_refreshed(&current_session_id, observed_session_id) {
        debug!(
            "Parked poll slot {} observed stale worker session; another slot refreshed it",
            slot_idx
        );
        return true;
    }

    warn!(
        "Parked poll slot {} detected inactive worker session; re-registering parked worker session",
        slot_idx
    );
    match register_parked_worker_session_with_retries(
        client,
        registration,
        "RegisterWorkerSession retry after inactive worker session",
    )
    .await
    {
        ParkedWorkerSessionRegistrationResult::Registered(new_session_id) => {
            let mut current_session_id = session_id.lock().await;
            if parked_worker_session_was_refreshed(&current_session_id, observed_session_id) {
                debug!(
                    "Parked poll slot {} discarding refreshed session; another slot already updated it",
                    slot_idx
                );
                return true;
            }
            *current_session_id = new_session_id;
            info!(
                "Parked poll slot {} refreshed inactive worker session",
                slot_idx
            );
            true
        }
        ParkedWorkerSessionRegistrationResult::Rejected => false,
    }
}

async fn refresh_parked_worker_session(
    client: &mut WorkerCoordinatorClient,
    session_id: &Arc<TokioMutex<String>>,
    observed_session_id: &str,
    registration: &ParkedWorkerSessionRegistration,
    slot_idx: usize,
) -> bool {
    try_refresh_parked_worker_session_once(
        client,
        session_id,
        observed_session_id,
        registration,
        slot_idx,
    )
    .await
}

fn spawn_parked_lease_renewal(
    mut client: WorkerCoordinatorClient,
    worker_id: String,
    worker_session_id: Arc<TokioMutex<String>>,
    run_id: String,
    lease_id: String,
    lease_timeout_ms: i64,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) -> Option<tokio::task::JoinHandle<()>> {
    if lease_id.is_empty() {
        return None;
    }

    Some(tokio::spawn(async move {
        let mut interval =
            Duration::from_millis(parked_lease_renew_interval_with_jitter_ms(lease_timeout_ms));
        loop {
            tokio::select! {
                _ = &mut stop_rx => {
                    return;
                }
                _ = tokio::time::sleep(interval) => {}
            }

            match client
                .renew_job_lease(RenewJobLeaseRequest {
                    worker_id: worker_id.clone(),
                    worker_session_id: worker_session_id.lock().await.clone(),
                    run_id: run_id.clone(),
                    lease_id: lease_id.clone(),
                    lease_timeout_ms,
                })
                .await
            {
                Ok(resp) if resp.renewed => {
                    debug!(
                        "Renewed parked poll lease run_id={} lease_id={} expires_at_ms={}",
                        run_id, lease_id, resp.lease_expires_at_ms
                    );
                    interval = Duration::from_millis(parked_lease_renew_interval_with_jitter_ms(
                        lease_timeout_ms,
                    ));
                }
                Ok(_) => {
                    warn!(
                        "Parked poll lease renewal returned renewed=false: run_id={} lease_id={}",
                        run_id, lease_id
                    );
                    return;
                }
                Err(e) => {
                    warn!(
                        "Parked poll lease renewal failed: run_id={} lease_id={} error={}",
                        run_id, lease_id, e
                    );
                    interval =
                        Duration::from_millis(parked_lease_danger_retry_ms(lease_timeout_ms));
                }
            }
        }
    }))
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
            cancel_tokens: Arc::new(std::sync::Mutex::new(HashMap::new())),
            cancel_hook: Arc::new(std::sync::Mutex::new(None)),
            event_stream_tx: Arc::new(std::sync::Mutex::new(None)),
            dispatch_tx: Arc::new(std::sync::Mutex::new(None)),
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

    /// Remove per-run tracking entries (lease stash, streaming flag) for a
    /// finished invocation.
    ///
    /// Language SDKs that deliver results via the event queue (e.g. Python)
    /// never send a terminal DispatchComponentResponse, so the cleanup in
    /// `forward_worker_response` never runs for them. Without this hook the
    /// maps grow by one entry per dispatch until the process OOMs under
    /// sustained load. Mirrors `forward_worker_response`: lease entries are
    /// keyed by the full invocation_id, the streaming flag by the base run_id.
    fn cleanup_run_tracking(&self, invocation_id: &str) {
        if let Ok(mut map) = self.pending_lease_ids.lock() {
            map.remove(invocation_id);
        }
        let run_id = invocation_id.split(':').next().unwrap_or(invocation_id);
        if let Ok(mut map) = self.streaming_runs.lock() {
            map.remove(run_id);
        }
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
        let is_terminal = event_type == "run.completed" || event_type == "run.failed";

        // ── Engine path: when AGNT5_ENGINE_URL is set, route directly to engine ──
        if let Some(mut engine) = self.ensure_engine_client().await? {
            // Before terminal checkpoints, flush any pending events for this run
            // directly to the engine via AppendBatch.
            if is_terminal {
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
            let (correlation_id, parent_event_id) = take_correlation_ids(&mut merged_metadata);
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

            if is_terminal {
                self.cleanup_run_tracking(&run_id);
            }

            return result;
        }

        // ── Legacy EE path (AGNT5_ENGINE_URL not set) ──

        // Before sending terminal checkpoints (run.completed/run.failed), flush any
        // pending SSE-only events (logs, deltas) for this run.
        // Route through EventStream (EE) which is the single SSE publisher.
        // Falls back to dispatch stream (WC) only if EventStream is unavailable.
        if is_terminal {
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
                            project_id: canonical_project_id_from_metadata(&event.metadata)
                                .unwrap_or_default(),
                            source_timestamp_ns: event.source_timestamp_ns,
                            worker_id: self.config.worker_id.clone(),
                        };
                        if let Err(e) = es.send_async(es_msg).await {
                            warn!(
                                "Failed to flush pre-checkpoint event via EventStream: type={} run_id={} error={}",
                                event.event_type, event.run_id, e
                            );
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
                            warn!(
                                "Failed to flush pre-checkpoint event via dispatch: type={} run_id={} error={}",
                                event.event_type, event.run_id, e
                            );
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
        let (correlation_id, parent_event_id) = take_correlation_ids(&mut merged_metadata);

        // Extract experiment_id before metadata is moved into the request
        let experiment_id = merged_metadata.get("experiment_id").cloned();

        let request = WriteCheckpointRequest {
            run_id: run_id.clone(),
            checkpoint_type: event_type.clone(),
            checkpoint_data: event_data,
            sequence_number,
            trace_id: String::new(),
            project_id: canonical_project_id,
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

        if is_terminal {
            self.cleanup_run_tracking(&run_id);
        }

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
            let originals: Vec<_> = events
                .into_iter()
                .map(|(run_id, event_type, data, sequence, metadata, ts)| {
                    let mut merged = metadata;
                    for (k, v) in &self.metadata {
                        if !merged.contains_key(k) {
                            merged.insert(k.clone(), v.clone());
                        }
                    }
                    let (cid, pcid) = take_correlation_ids(&mut merged);
                    JournalEventMessage {
                        run_id,
                        event_type,
                        data,
                        correlation_id: cid,
                        parent_correlation_id: pcid,
                        tenant_id: None,
                        source_timestamp_ns: ts,
                        metadata: merged,
                        queued_at: Instant::now(),
                        is_streaming: false,
                        is_sse_only: false,
                        content_index: 0,
                        sequence,
                    }
                })
                .collect();

            let records: Vec<_> = originals
                .iter()
                .map(|event| {
                    let canonical_project_id =
                        canonical_project_id_from_metadata(&event.metadata).unwrap_or_default();
                    let mut metadata =
                        with_project_metadata(event.metadata.clone(), &canonical_project_id);
                    let tenant_id = metadata
                        .remove("project_id")
                        .or_else(|| metadata.remove("tenant_id"))
                        .unwrap_or_default();

                    client::build_engine_record(
                        tenant_id,
                        event.run_id.clone(),
                        event.event_type.clone(),
                        event.data.clone(),
                        event.source_timestamp_ns,
                        String::new(),
                        event.correlation_id.clone(),
                        event.parent_correlation_id.clone(),
                        metadata,
                    )
                })
                .collect();

            let count = originals.len();
            match engine.append_batch(records).await {
                Ok(_) => {
                    debug!("Engine batch checkpoint: {} events persisted", count);
                }
                Err(e) => {
                    warn!(
                        "Engine batch checkpoint failed for {} non-terminal events; queued for retry: {}",
                        count, e
                    );
                    {
                        let mut guard = self.engine_client.lock().await;
                        *guard = None;
                    }
                    for event in originals.into_iter().rev() {
                        self.journal_queue.push_front(event).ok();
                    }
                    self.journal_queue.record_error();
                }
            }
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
    /// Register a cooperative cancel hook (see the `cancel_hook` field).
    /// Called by language bindings before `run()`. The hook receives the
    /// run_id of the invocation to cancel and should cancel the language-level
    /// task/promise for it.
    pub fn set_cancel_hook<F>(&self, hook: F)
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        if let Ok(mut guard) = self.cancel_hook.lock() {
            *guard = Some(Box::new(hook));
        }
    }

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
                    // Surface the very first connect failure to the user so
                    // misconfigurations (wrong URL, missing API key, DNS
                    // failure) are immediately visible. Subsequent retries
                    // stay quiet under QUIET_RETRY_THRESHOLD to avoid
                    // alarming on transient blips during cold starts.
                    if retry_count == 0 && !was_connected {
                        eprintln!("[ERROR] Connection failed: {}", e);
                    }
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
        // The runtime accepts any worker on any serving coordinator —
        // there's no per-worker "owning" pod to route to anymore, so we
        // dial the configured endpoint on both first connect and
        // reconnect. The fenced routing projection inside the cluster
        // handles dispatch authority transparently.
        let coordinator_endpoint = self.config.resolved_coordinator_endpoint();
        // Surface the in-flight handshake so users don't stare at silence
        // during the (up to) 10s connect timeout. Pairs with the
        // "[INFO] Connected/Reconnected to coordinator" line below.
        if is_reconnect {
            eprintln!(
                "[INFO] Reconnecting to coordinator ({})...",
                coordinator_endpoint
            );
        } else {
            eprintln!(
                "[INFO] Connecting to coordinator ({})...",
                coordinator_endpoint
            );
        }
        let mut client = WorkerCoordinatorClient::connect(coordinator_endpoint.clone()).await?;

        // Create registration message with components
        // Merge user-provided metadata with auto-collected AGNT5_* env vars
        let mut metadata = self.metadata.clone();
        metadata.extend(collect_agnt5_env_vars());

        // Phase 6: declare data-path mode. Default PUSH;
        // `AGNT5_WORKER_MODE=pull` now means parked long-poll assignment
        // (`RegisterWorkerSession` + `PollJob`). The legacy batch `PollJobs`
        // loop is intentionally gone.
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
        // size headroom reservations per priority class. Resolved from
        // config (set by a language binding or seeded from the
        // `AGNT5_MAX_CONCURRENCY` env var in `WorkerConfig::new`), default
        // 100. Drives both the local pool size and the registration field.
        let max_concurrency: u32 = self.config.max_concurrency.unwrap_or(100);

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

        // Pull workers do not need the stateful dispatch stream for work
        // assignment. They register a worker session and pull work via unary
        // Engine RPCs below. Push workers keep the stream path.
        let (tx, rx, _runtime_msg_tx_hold) = if is_pull_mode {
            let (tx, _outgoing_rx) = flume::bounded::<ServiceMessage>(1000);
            let (runtime_msg_tx, runtime_msg_rx) = flume::bounded::<RuntimeMessage>(1000);
            (tx, runtime_msg_rx, Some(runtime_msg_tx))
        } else {
            let (tx, rx) = client
                .create_worker_stream_with_registration(self.config.worker_id.clone(), registration)
                .await?;
            (tx, rx, None)
        };

        if is_reconnect {
            if is_pull_mode {
                eprintln!(
                    "[INFO] Reconnected to coordinator ({}) for parked polling",
                    coordinator_endpoint
                );
            } else {
                eprintln!(
                    "[INFO] Reconnected to coordinator ({})",
                    coordinator_endpoint
                );
            }
        } else if is_pull_mode {
            eprintln!(
                "[INFO] Connected to coordinator ({}) for parked polling",
                coordinator_endpoint
            );
        } else {
            eprintln!("[INFO] Connected to coordinator ({})", coordinator_endpoint);
        }
        if is_pull_mode {
            debug!(
                "Worker {} connected for parked polling",
                self.config.worker_id
            );
        } else {
            debug!("Worker {} registered successfully", self.config.worker_id);
        }
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
                        warn!(
                            "Failed to open EE EventStream, SSE-only events will use dispatch stream: {}",
                            e
                        );
                        None
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Failed to get EE client for EventStream, SSE-only events will use dispatch stream: {}",
                    e
                );
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

        // Live in-flight counter (handler invocations the worker pool is
        // currently executing), shared between the pool tasks and the
        // heartbeat task. The coordinator reconciles its per-worker routing
        // load against this authoritative value (see `HealthCheck.in_flight`)
        // so a missed dispatch-completion decrement on its side cannot wedge
        // routing for an idle worker.
        let in_flight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // Start heartbeat task for stream-backed modes. Pull workers
        // heartbeat by keeping PollJob requests open and renewing active
        // leases; there is no dispatch stream to send HealthCheck on.
        let heartbeat_task = if is_pull_mode {
            None
        } else {
            Some(self.spawn_heartbeat_task(tx.clone(), in_flight.clone()))
        };

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
            let in_flight = in_flight.clone();
            let cancel_tokens = self.cancel_tokens.clone();

            let handle = tokio::spawn(async move {
                while let Ok(runtime_message) = task_rx.recv_async().await {
                    execute_runtime_message(
                        &worker_name,
                        runtime_message,
                        response_tx.clone(),
                        handler.clone(),
                        in_flight.clone(),
                        cancel_tokens.clone(),
                    )
                    .await;
                }
            });

            worker_handles.push(handle);
        }

        // Pull workers own the parked long-poll task; PUSH workers never
        // spawn it. The legacy batch PollJobs path has been removed.
        let poll_task = if is_pull_mode {
            let poll_shutdown = shutdown_rx.resubscribe();
            Some(self.spawn_parked_poll_task(
                response_tx.clone(),
                message_handler.clone(),
                poll_shutdown,
                max_concurrency,
                in_flight.clone(),
            ))
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

                            // COORDINATOR_DRAINING: this coordinator is leaving
                            // service. Stop accepting new dispatches on this
                            // stream, drain already-started work below, then
                            // reconnect through the configured endpoint.
                            if runtime_message.message_type == RuntimeMessageType::CoordinatorDraining as i32 {
                                warn!(
                                    "Worker {} received COORDINATOR_DRAINING — draining local work before reconnect.",
                                    self.config.worker_id
                                );
                                eprintln!(
                                    "[INFO] Coordinator is draining. Worker will reconnect after in-flight work completes."
                                );
                                break Err(crate::error::SdkError::Connection {
                                    message: "coordinator draining".to_string(),
                                    code: crate::error::ErrorCode::ConnectionFailed,
                                    source: None,
                                });
                            }

                            // CancelExecution: fire the soft-cancel channel for
                            // the invocation if it's running locally. Handled
                            // here (not in the pool) so it can't queue behind
                            // the very invocation it's cancelling.
                            if runtime_message.message_type == RuntimeMessageType::CancelExecution as i32 {
                                if let Some(crate::pb::runtime_message::MessageData::CancelExecution(ref req)) =
                                    runtime_message.message_data
                                {
                                    let run_key = req
                                        .invocation_id
                                        .split(':')
                                        .next()
                                        .unwrap_or(&req.invocation_id)
                                        .to_string();
                                    // Prefer cooperative cancellation via the
                                    // language hook: it cancels the language
                                    // task so the handler unwinds and runs
                                    // cleanup, then the handler future resolves
                                    // naturally and frees the slot. Without a
                                    // hook, fall back to the soft oneshot drop.
                                    let hooked = self
                                        .cancel_hook
                                        .lock()
                                        .ok()
                                        .and_then(|g| g.as_ref().map(|h| h(run_key.clone())))
                                        .is_some();
                                    if hooked {
                                        info!(
                                            "Worker {} cancel hook invoked for {}",
                                            self.config.worker_id, run_key
                                        );
                                    } else {
                                        let fired = self
                                            .cancel_tokens
                                            .lock()
                                            .ok()
                                            .and_then(|mut m| m.remove(&run_key))
                                            .map(|tx| {
                                                let _ = tx.send(());
                                            })
                                            .is_some();
                                        if fired {
                                            info!(
                                                "Worker {} soft-cancelling invocation {}",
                                                self.config.worker_id, run_key
                                            );
                                        } else {
                                            debug!(
                                                "Worker {} CancelExecution for {} — no in-flight invocation",
                                                self.config.worker_id, run_key
                                            );
                                        }
                                    }
                                    // A cancelled run never emits run.completed/
                                    // run.failed from this worker (the gateway
                                    // authors run.cancelled), so drop its
                                    // tracking entries here.
                                    self.cleanup_run_tracking(&req.invocation_id);
                                }
                                continue;
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

                                            let checkpoint_worker = self.clone();
                                            let response_tx = response_tx.clone();
                                            let input_data = req.input_data.clone();
                                            let metadata = req.metadata.clone();
                                            tokio::spawn(async move {
                                                if let Err(e) = checkpoint_worker.emit_checkpoint_sync(
                                                    run_id.clone(),
                                                    "run.started".to_string(),
                                                    input_data,
                                                    0,
                                                    metadata.clone(),
                                                    timestamp_ns,
                                                    5000,
                                                ).await {
                                                    warn!("Built-in scorer: failed to emit run.started checkpoint: {}", e);
                                                }

                                                if let Err(e) = checkpoint_worker.emit_checkpoint_sync(
                                                    run_id,
                                                    "run.completed".to_string(),
                                                    output_data,
                                                    1,
                                                    metadata,
                                                    timestamp_ns,
                                                    5000,
                                                ).await {
                                                    warn!("Built-in scorer: failed to emit run.completed checkpoint: {}", e);
                                                }

                                                if let Err(e) = response_tx.send_async(service_message).await {
                                                    error!("Failed to send built-in scorer response: {}", e);
                                                }
                                            });

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
                        Ok(service_message) => {
                            if let Err(e) = self.forward_worker_response(service_message, is_pull_mode, &tx).await {
                                error!("Failed to send response to coordinator: {}", e);
                                break Err(e);
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

        // Wait for all worker tasks to complete. During coordinator drain this
        // lets the old stream finish only work that had already started before
        // reconnecting through the configured endpoint.
        for handle in worker_handles {
            let _ = handle.await;
        }

        while let Ok(service_message) = response_rx.try_recv() {
            if let Err(e) = self
                .forward_worker_response(service_message, is_pull_mode, &tx)
                .await
            {
                warn!("Failed to flush drained worker response: {}", e);
                break;
            }
        }

        // Remove health marker file so K8s readiness probe fails
        self.remove_health_marker();

        // Send shutdown message and stop background tasks
        let _ = self.send_shutdown_message(&tx).await;
        if let Some(task) = heartbeat_task {
            task.abort();
        }
        journal_flush_task.abort();

        // Clear per-run tracking AFTER flush task is aborted, so the flush
        // task can drain any remaining SSE events first. In-flight work has
        // already completed (worker handles awaited above), so no invocation
        // still needs its lease stash.
        if let Ok(mut map) = self.streaming_runs.lock() {
            map.clear();
        }
        if let Ok(mut map) = self.pending_lease_ids.lock() {
            map.clear();
        }

        dispatch_result
    }

    async fn forward_worker_response(
        &self,
        mut service_message: ServiceMessage,
        is_pull_mode: bool,
        tx: &flume::Sender<ServiceMessage>,
    ) -> Result<()> {
        // Phase 5: stamp the stashed lease_id onto the response so the
        // coordinator's fencing check passes. On terminal events we drain
        // the map entry; on intermediate streaming events we leave it so the
        // terminal ack still finds it. Also clean up streaming_runs tracking
        // for terminal events.
        if let Some(crate::pb::service_message::MessageType::FunctionResponse(ref mut resp)) =
            service_message.message_type
        {
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

        // Phase 8: route by declared worker mode, not by per-response metadata
        // tagging. A PULL worker always acks via CompleteJob; a PUSH worker
        // always responds over the bidirectional stream.
        if is_pull_mode {
            self.handle_polled_job_response(service_message).await;
        } else {
            tx.send_async(service_message).await.map_err(|e| {
                crate::error::SdkError::Connection {
                    message: format!("Send failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?;
        }
        Ok(())
    }

    /// Spawn a simple heartbeat task that sends periodic health checks
    fn spawn_heartbeat_task(
        &self,
        tx: flume::Sender<ServiceMessage>,
        in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
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
                    in_flight: Some(in_flight.load(std::sync::atomic::Ordering::Relaxed) as u32),
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
        let worker_id_outer = self.config.worker_id.clone();
        let journal_queue_outer = self.journal_queue.clone();
        let streaming_runs_outer = self.streaming_runs.clone();
        let pending_lease_ids_outer = self.pending_lease_ids.clone();
        let ee_endpoint_outer = self.config.ee_endpoint.clone();
        let engine_endpoint_outer = self.config.engine_endpoint.clone();

        // Supervisor — restart the inner flush loop on panic with bounded
        // backoff. h2-0.4.13 panics with PoisonError under concurrent stream
        // contention (timeout cancels racing other polls); without this
        // supervisor, a single h2 panic kills the flush task forever and
        // events pile up in the queue indefinitely. The inner loop is
        // panic-resilient at the data-handling layer (see streaming_runs
        // mutex poison handling) — this catches the deeper transport panics.
        tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_millis(100);
            const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);
            loop {
                // Clone per attempt so each inner task owns its capture.
                let worker_id = worker_id_outer.clone();
                let journal_queue = journal_queue_outer.clone();
                let flush_interval_ms = journal_queue.flush_interval_ms();
                let batch_size = journal_queue.batch_size().max(1);
                let max_batches_per_tick = std::env::var("AGNT5_JOURNAL_MAX_BATCHES_PER_TICK")
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(32)
                    .max(1);
                let streaming_runs = streaming_runs_outer.clone();
                let pending_lease_ids = pending_lease_ids_outer.clone();
                let ee_endpoint = ee_endpoint_outer.clone();
                let engine_endpoint = engine_endpoint_outer.clone();
                let dispatch_tx = dispatch_tx.clone();
                let event_stream_tx = event_stream_tx.clone();

                // Cache project_id/deployment_id to avoid repeated env lookups per event.
                // `tenant_id` remains a legacy alias for compatibility with engine/EE APIs.
                let cached_project_id = canonical_project_id_from_env();
                let cached_deployment_id = std::env::var("AGNT5_DEPLOYMENT_ID").unwrap_or_default();

                let inner = tokio::spawn(async move {
                    let mut interval =
                        tokio::time::interval(Duration::from_millis(flush_interval_ms));

                    // Lazily-connected EE client for boundary event writes.
                    // Separate from the Worker's ee_client to avoid lock contention with emit_checkpoint_sync.
                    let mut ee_client: Option<ExecutionEngineServiceClient<Channel>> = None;

                    // Lazily-connected Engine client (when AGNT5_ENGINE_URL is set).
                    let mut engine: Option<EngineClient> = None;

                    loop {
                        interval.tick().await;

                        // Drain more than one nominal batch when backlog is already present.
                        // This preserves the normal small-batch latency path while allowing
                        // the flush task to catch up instead of hard-capping at one batch
                        // per interval.
                        let queued = journal_queue.len();
                        let drain_limit = if queued > batch_size {
                            queued.min(batch_size.saturating_mul(max_batches_per_tick))
                        } else {
                            batch_size
                        };
                        let batch = journal_queue.drain_batch(drain_limit);
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
                                        warn!(
                                            "Flush task: failed to connect to Engine {}: {}",
                                            ep, e
                                        );
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
                                        warn!(
                                            "streaming_runs mutex poisoned, assuming non-streaming for run_id={}",
                                            event.run_id
                                        );
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
                                        project_id: cached_project_id.clone(),
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
                                    metadata.insert(
                                        "deployment_id".to_string(),
                                        cached_deployment_id.clone(),
                                    );
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
                                        crate::pb::service_message::MessageType::FunctionResponse(
                                            response,
                                        ),
                                    ),
                                };
                                if let Err(e) = dispatch_tx.send_async(service_message).await {
                                    warn!(
                                        "Failed to send SSE-only event via dispatch fallback: type={} run_id={} error={}",
                                        event.event_type, event.run_id, e
                                    );
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
                                project_id: tenant_id,
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
                                                ee_client = Some(
                                                    ExecutionEngineServiceClient::new(channel),
                                                );
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
                                                    journal_queue.record_sent_batch(
                                                        sent_count,
                                                        sse_only_count,
                                                    );
                                                }
                                                continue;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "Flush task: invalid EE endpoint {}: {}",
                                            ee_endpoint, e
                                        );
                                        for event in boundary_originals.into_iter().rev() {
                                            journal_queue.push_front(event).ok();
                                        }
                                        journal_queue.record_error();
                                        if sent_count > 0 {
                                            journal_queue
                                                .record_sent_batch(sent_count, sse_only_count);
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
                                                warn!(
                                                    "  event[{}]: {}",
                                                    err.index, err.error_message
                                                );
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
                });

                match inner.await {
                    // Inner task ended without panic — flush loop runs
                    // forever in normal operation, so this branch only
                    // fires on shutdown/abort. Exit the supervisor too.
                    Ok(()) => {
                        debug!(
                            worker_id = %worker_id_outer,
                            "Journal flush task exited cleanly; supervisor shutting down"
                        );
                        return;
                    }
                    Err(e) if e.is_panic() => {
                        error!(
                            worker_id = %worker_id_outer,
                            error = ?e,
                            backoff_ms = backoff.as_millis() as u64,
                            "Journal flush task panicked (likely h2 transport); restarting after backoff"
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                    Err(_cancelled) => {
                        // External cancellation — caller called .abort()
                        // on the supervisor's JoinHandle. Exit cleanly.
                        debug!(
                            worker_id = %worker_id_outer,
                            "Journal flush task cancelled; supervisor shutting down"
                        );
                        return;
                    }
                }
            }
        })
    }

    /// Spawn parked one-job pollers. Each slot owns exactly one outstanding
    /// PollJob request or one active handler invocation. Dynamic ramping to
    /// `max_slots` is added after local E2E validation.
    fn spawn_parked_poll_task<F, Fut>(
        &self,
        response_tx: flume::Sender<ServiceMessage>,
        message_handler: F,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
        max_concurrency: usize,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
    ) -> tokio::task::JoinHandle<()>
    where
        F: Fn(RuntimeMessage, flume::Sender<ServiceMessage>) -> Fut + Send + Clone + 'static,
        Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send + 'static,
    {
        let worker_id = self.config.worker_id.clone();
        let endpoint = self.config.resolved_coordinator_endpoint();
        let project_id = canonical_project_id_from_env();
        let deployment_id = std::env::var("AGNT5_DEPLOYMENT_ID").unwrap_or_default();
        let capabilities = worker_capabilities(&self.components);
        let components = self.components.clone();
        let service_name = self.config.service_name.clone();
        let service_version = self.config.service_version.clone();
        let service_type = self.config.service_type.clone();
        let streaming_runs = self.streaming_runs.clone();
        let pending_lease_ids = self.pending_lease_ids.clone();
        let cancel_tokens = self.cancel_tokens.clone();
        let configured_max_slots = env_usize("AGNT5_MAX_SLOTS").unwrap_or(max_concurrency);
        let max_slots = configured_max_slots
            .clamp(1, max_concurrency.max(1))
            .min(100);
        let configured_min_slots = env_usize("AGNT5_MIN_SLOTS").unwrap_or(1);
        let min_slots = configured_min_slots.clamp(1, max_slots);
        let claim_timeout_ms = std::env::var("AGNT5_CLAIM_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(300_000);

        tokio::spawn(async move {
            if project_id.is_empty() {
                eprintln!("[INFO] Parked polling disabled (AGNT5_PROJECT_ID not set)");
                return;
            }
            if deployment_id.is_empty() {
                eprintln!("[INFO] Parked polling disabled (AGNT5_DEPLOYMENT_ID not set)");
                return;
            }

            let mut client = match WorkerCoordinatorClient::connect(endpoint.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[WARN] Parked poll task failed to connect: {}", e);
                    return;
                }
            };
            let registration = ParkedWorkerSessionRegistration {
                worker_id: worker_id.clone(),
                project_id: project_id.clone(),
                deployment_id: deployment_id.clone(),
                min_slots,
                max_slots,
                capabilities,
                components,
                service_name,
                service_version,
                service_type,
            };
            let initial_session_id = match register_parked_worker_session_with_retries(
                &mut client,
                &registration,
                "RegisterWorkerSession",
            )
            .await
            {
                ParkedWorkerSessionRegistrationResult::Registered(session_id) => session_id,
                ParkedWorkerSessionRegistrationResult::Rejected => exit_parked_worker_process(
                    "RegisterWorkerSession was rejected after 3 attempts; exiting worker process",
                ),
            };
            let worker_session_id = Arc::new(TokioMutex::new(initial_session_id));

            eprintln!(
                "[INFO] Parked polling started (deployment={}, min_slots={}, max_slots={})",
                deployment_id, min_slots, max_slots
            );

            let open_poll_slots = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let capacity_reporter = spawn_parked_capacity_reporter(
                client.clone(),
                worker_id.clone(),
                worker_session_id.clone(),
                open_poll_slots.clone(),
                in_flight.clone(),
                min_slots,
                max_slots,
                shutdown_rx.resubscribe(),
            );

            let mut slots = tokio::task::JoinSet::new();
            for slot_idx in 0..min_slots {
                let mut client = client.clone();
                let worker_id = worker_id.clone();
                let worker_session_id = worker_session_id.clone();
                let registration = registration.clone();
                let project_id = project_id.clone();
                let response_tx = response_tx.clone();
                let handler = message_handler.clone();
                let in_flight = in_flight.clone();
                let cancel_tokens = cancel_tokens.clone();
                let streaming_runs = streaming_runs.clone();
                let pending_lease_ids = pending_lease_ids.clone();
                let open_poll_slots = open_poll_slots.clone();
                slots.spawn(async move {
                    let worker_name = format!("{}-parked-{}", worker_id, slot_idx);
                    loop {
                        let current_session_id = worker_session_id.lock().await.clone();
                        open_poll_slots.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let poll_result = client
                            .poll_job(PollJobRequest {
                                worker_id: worker_id.clone(),
                                worker_session_id: current_session_id.clone(),
                                wait_ms: 30_000,
                                claim_timeout_ms,
                            })
                            .await;
                        open_poll_slots.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

                        match poll_result {
                            Ok(resp) => {
                                let Some(job) = resp.job else {
                                    continue;
                                };
                                let lease_timeout_ms = job
                                    .metadata
                                    .get("lease_timeout_ms")
                                    .and_then(|v| v.parse::<i64>().ok())
                                    .filter(|v| *v > 0)
                                    .unwrap_or(claim_timeout_ms);
                                let (runtime_message, is_streaming, run_id, lease_id) =
                                    runtime_message_from_job_assignment(job);
                                let completion_run_id = run_id.clone();
                                let completion_lease_id = lease_id.clone();
                                if !lease_id.is_empty() {
                                    if let Ok(mut map) = pending_lease_ids.lock() {
                                        map.insert(run_id.clone(), lease_id.clone());
                                    }
                                }
                                if is_streaming {
                                    if let Ok(mut map) = streaming_runs.lock() {
                                        map.insert(run_id.clone(), true);
                                    }
                                }

                                let (renew_stop_tx, renew_handle) =
                                    tokio::sync::oneshot::channel::<()>();
                                let renewal = spawn_parked_lease_renewal(
                                    client.clone(),
                                    worker_id.clone(),
                                    worker_session_id.clone(),
                                    run_id.clone(),
                                    lease_id.clone(),
                                    lease_timeout_ms,
                                    renew_handle,
                                );

                                let (slot_response_tx, slot_response_rx) =
                                    flume::unbounded::<ServiceMessage>();
                                let returned_response = execute_runtime_message_for_response(
                                    &worker_name,
                                    runtime_message,
                                    slot_response_tx.clone(),
                                    handler.clone(),
                                    in_flight.clone(),
                                    cancel_tokens.clone(),
                                )
                                .await;
                                drop(slot_response_tx);

                                let mut completed = false;
                                if let Some(service_message) = returned_response {
                                    completed = complete_or_forward_parked_response(
                                        &mut client,
                                        service_message,
                                        &completion_lease_id,
                                        &worker_id,
                                        &worker_session_id,
                                        &project_id,
                                        slot_idx,
                                        &response_tx,
                                    )
                                    .await;
                                }
                                while let Ok(service_message) = slot_response_rx.try_recv() {
                                    if completed
                                        && polled_job_completion_from_service_message(
                                            &service_message,
                                            &completion_lease_id,
                                        )
                                        .is_some()
                                    {
                                        debug!(
                                            "Parked poll slot {} dropping duplicate completion for job_id={}",
                                            slot_idx, completion_run_id
                                        );
                                        continue;
                                    }
                                    completed = complete_or_forward_parked_response(
                                        &mut client,
                                        service_message,
                                        &completion_lease_id,
                                        &worker_id,
                                        &worker_session_id,
                                        &project_id,
                                        slot_idx,
                                        &response_tx,
                                    )
                                    .await
                                        || completed;
                                }

                                if completed {
                                    if let Ok(mut map) = pending_lease_ids.lock() {
                                        map.remove(&completion_run_id);
                                    }
                                    if let Ok(mut map) = streaming_runs.lock() {
                                        map.remove(&completion_run_id);
                                    }
                                }

                                let _ = renew_stop_tx.send(());
                                if let Some(handle) = renewal {
                                    let _ = handle.await;
                                }
                            }
                            Err(e) if is_worker_session_inactive_error(&e) => {
                                warn!("Parked poll slot {} error: {}", slot_idx, e);
                                if !refresh_parked_worker_session(
                                    &mut client,
                                    &worker_session_id,
                                    &current_session_id,
                                    &registration,
                                    slot_idx,
                                )
                                .await
                                {
                                    exit_parked_worker_process(
                                        "RegisterWorkerSession retry was rejected after 3 attempts; exiting worker process",
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("Parked poll slot {} error: {}", slot_idx, e);
                                tokio::time::sleep(Duration::from_millis(1_000)).await;
                            }
                        }
                    }
                });
            }

            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        info!("Parked poll task shutting down");
                        slots.abort_all();
                        while slots.join_next().await.is_some() {}
                        capacity_reporter.abort();
                        return;
                    }
                    result = slots.join_next() => {
                        match result {
                            Some(Ok(())) => {}
                            Some(Err(e)) => warn!("Parked poll slot exited: {}", e),
                            None => {
                                capacity_reporter.abort();
                                return;
                            }
                        }
                    }
                }
            }
        })
    }
    /// Handle a polled job response by calling CompleteJob RPC.
    ///
    /// Called from parked long-poll workers. On the poll path `job_id == run_id`, so we
    /// derive the job_id from `resp.invocation_id` — stripping any
    /// `:suffix` the worker appends for streaming invocations. Project identity
    /// comes from `AGNT5_PROJECT_ID`.
    async fn handle_polled_job_response(&self, service_message: ServiceMessage) {
        let (job_id, success, output_data, error_message, error_code, lease_id) =
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
                        resp.lease_id.clone(),
                    )
                }
                _ => {
                    warn!("Unexpected message type for polled job completion");
                    return;
                }
            };

        // Pull workers derive tenant_id from the same env var the parked
        // PollJob task uses.
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
                    project_id: tenant_id,
                    lease_id,
                    worker_session_id: String::new(),
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

#[cfg(test)]
mod tests {
    use super::{
        is_parked_worker_session_registration_rejection, is_worker_session_inactive_error,
        parked_lease_danger_retry_ms, parked_lease_renew_interval_ms,
        parked_lease_renew_interval_with_jitter_ms, parked_worker_session_was_refreshed,
        runtime_message_from_job_assignment, take_correlation_ids, ParkedWorkerSessionRegistration,
        Worker, WorkerConfig,
    };
    use crate::error::{ErrorCode, SdkError};
    use crate::pb::{runtime_message, JobAssignment};
    use std::collections::HashMap;

    #[test]
    fn cleanup_run_tracking_removes_per_run_entries() {
        let config = WorkerConfig::new(
            "svc".to_string(),
            "1.0.0".to_string(),
            "standalone".to_string(),
        );
        let worker = Worker::new(config, Vec::new(), HashMap::new());

        worker
            .pending_lease_ids
            .lock()
            .unwrap()
            .insert("run-1".to_string(), "lease-1".to_string());
        worker
            .pending_lease_ids
            .lock()
            .unwrap()
            .insert("run-2".to_string(), "lease-2".to_string());
        worker
            .streaming_runs
            .lock()
            .unwrap()
            .insert("run-1".to_string(), true);

        worker.cleanup_run_tracking("run-1");

        assert!(!worker
            .pending_lease_ids
            .lock()
            .unwrap()
            .contains_key("run-1"));
        assert!(worker
            .pending_lease_ids
            .lock()
            .unwrap()
            .contains_key("run-2"));
        assert!(!worker.streaming_runs.lock().unwrap().contains_key("run-1"));
    }

    #[test]
    fn cleanup_run_tracking_strips_sub_invocation_suffix_for_streaming_flag() {
        let config = WorkerConfig::new(
            "svc".to_string(),
            "1.0.0".to_string(),
            "standalone".to_string(),
        );
        let worker = Worker::new(config, Vec::new(), HashMap::new());

        // Lease entries are keyed by the full invocation_id; the streaming
        // flag is keyed by the base run_id (before the first ':').
        worker
            .pending_lease_ids
            .lock()
            .unwrap()
            .insert("run-1:0".to_string(), "lease-1".to_string());
        worker
            .streaming_runs
            .lock()
            .unwrap()
            .insert("run-1".to_string(), true);

        worker.cleanup_run_tracking("run-1:0");

        assert!(worker.pending_lease_ids.lock().unwrap().is_empty());
        assert!(worker.streaming_runs.lock().unwrap().is_empty());
    }

    #[test]
    fn job_assignment_conversion_preserves_typed_lease() {
        let job = JobAssignment {
            job_id: "run-1".to_string(),
            run_id: "run-1".to_string(),
            component_id: String::new(),
            component_type: crate::pb::ComponentType::Function as i32,
            component_name: "do_work".to_string(),
            input_data: br#"{"x":1}"#.to_vec(),
            metadata: HashMap::from([
                ("stream_mode".to_string(), "full".to_string()),
                ("deployment_id".to_string(), "dep-1".to_string()),
            ]),
            attempt: 2,
            timeout_ms: 0,
            trace_id: "trace-1".to_string(),
            lease_id: "lease-1".to_string(),
            lease_expires_at_ms: 123_456,
        };

        let (message, is_streaming, run_id, lease_id) = runtime_message_from_job_assignment(job);

        assert!(is_streaming);
        assert_eq!(run_id, "run-1");
        assert_eq!(lease_id, "lease-1");
        match message.message_data {
            Some(runtime_message::MessageData::DispatchComponent(req)) => {
                assert_eq!(req.invocation_id, "run-1");
                assert_eq!(req.component_name, "do_work");
                assert_eq!(req.attempt, 2);
                assert_eq!(req.deployment_id, "dep-1");
                assert_eq!(req.lease_id, "lease-1");
                assert_eq!(
                    req.metadata.get("lease_id").map(String::as_str),
                    Some("lease-1")
                );
                assert_eq!(
                    req.metadata.get("lease_expires_at_ms").map(String::as_str),
                    Some("123456")
                );
                assert_eq!(
                    req.metadata.get("trace_id").map(String::as_str),
                    Some("trace-1")
                );
            }
            other => panic!("expected dispatch component, got {other:?}"),
        }
    }

    #[test]
    fn parked_lease_renew_intervals_are_bounded() {
        assert_eq!(parked_lease_renew_interval_ms(120_000), 60_000);
        assert_eq!(parked_lease_danger_retry_ms(120_000), 5_000);
        assert_eq!(parked_lease_renew_interval_ms(2_000), 5_000);
        assert_eq!(parked_lease_danger_retry_ms(2_000), 1_000);

        for _ in 0..100 {
            let jittered = parked_lease_renew_interval_with_jitter_ms(120_000);
            assert!(
                (54_000..=66_000).contains(&jittered),
                "jittered interval out of ±10% range: {jittered}"
            );
        }
    }

    #[test]
    fn worker_session_inactive_errors_are_detected() {
        let error = SdkError::Connection {
            message: "PollJob failed: code: 'The caller does not have permission to execute the specified operation', message: \"worker session is not active\"".to_string(),
            code: ErrorCode::ConnectionFailed,
            source: None,
        };

        assert!(is_worker_session_inactive_error(&error));
    }

    #[test]
    fn parked_worker_session_registration_classifies_rejections() {
        let rejected = SdkError::Connection {
            message:
                "RegisterWorkerSession failed: code: 'Invalid argument', message: \"bad worker\""
                    .to_string(),
            code: ErrorCode::ConnectionFailed,
            source: None,
        };
        let transient = SdkError::Connection {
            message: "RegisterWorkerSession failed: code: 'The service is currently unavailable'"
                .to_string(),
            code: ErrorCode::ConnectionFailed,
            source: None,
        };

        assert!(is_parked_worker_session_registration_rejection(&rejected));
        assert!(!is_parked_worker_session_registration_rejection(&transient));
    }

    #[test]
    fn parked_worker_session_refreshed_detects_stale_observed_session() {
        assert!(parked_worker_session_was_refreshed(
            "new-session",
            "old-session"
        ));
        assert!(!parked_worker_session_was_refreshed(
            "same-session",
            "same-session"
        ));
    }

    #[test]
    fn parked_worker_session_registration_builds_repeatable_request() {
        let registration = ParkedWorkerSessionRegistration {
            worker_id: "worker-1".into(),
            project_id: "project-1".into(),
            deployment_id: "deployment-1".into(),
            min_slots: 2,
            max_slots: 5,
            capabilities: vec![crate::pb::WorkerCapability {
                component_type: crate::pb::ComponentType::Function as i32,
                component_name: "do_work".into(),
            }],
            components: vec![crate::pb::ComponentInfo {
                component_type: crate::pb::ComponentType::Function as i32,
                name: "do_work".into(),
                ..Default::default()
            }],
            service_name: "svc".into(),
            service_version: "1.2.3".into(),
            service_type: "worker".into(),
        };

        let first = registration.request();
        let second = registration.request();

        assert_eq!(first.worker_id, "worker-1");
        assert_eq!(first.project_id, "project-1");
        assert_eq!(first.deployment_id, "deployment-1");
        assert_eq!(first.max_slots, 5);
        assert_eq!(first.slot_policy.as_ref().unwrap().min_slots, 2);
        assert_eq!(first.slot_policy.as_ref().unwrap().max_slots, 5);
        assert_eq!(first.capabilities.len(), 1);
        assert_eq!(first.components.len(), 1);
        assert_eq!(second.service_name, "svc");
        assert_eq!(second.service_version, "1.2.3");
        assert_eq!(second.service_type, "worker");
    }

    #[test]
    fn take_correlation_ids_accepts_canonical_keys() {
        let mut metadata = HashMap::from([
            ("correlation_id".to_string(), "span-1".to_string()),
            ("parent_correlation_id".to_string(), "parent-1".to_string()),
            ("other".to_string(), "value".to_string()),
        ]);

        let (correlation_id, parent_correlation_id) = take_correlation_ids(&mut metadata);

        assert_eq!(correlation_id, "span-1");
        assert_eq!(parent_correlation_id, "parent-1");
        assert!(!metadata.contains_key("correlation_id"));
        assert!(!metadata.contains_key("parent_correlation_id"));
        assert_eq!(metadata.get("other").map(String::as_str), Some("value"));
    }

    #[test]
    fn take_correlation_ids_prefers_legacy_short_keys() {
        let mut metadata = HashMap::from([
            ("cid".to_string(), "short-span".to_string()),
            ("pcid".to_string(), "short-parent".to_string()),
            ("correlation_id".to_string(), "canonical-span".to_string()),
            (
                "parent_correlation_id".to_string(),
                "canonical-parent".to_string(),
            ),
        ]);

        let (correlation_id, parent_correlation_id) = take_correlation_ids(&mut metadata);

        assert_eq!(correlation_id, "short-span");
        assert_eq!(parent_correlation_id, "short-parent");
        assert!(metadata.contains_key("correlation_id"));
        assert!(metadata.contains_key("parent_correlation_id"));
    }
}
