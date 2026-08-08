use crate::client::{self, EngineClient, WorkerCoordinatorClient};
use crate::error::{Result, SdkError};
use crate::journal_queue::{JournalEventMessage, JournalEventQueue, JournalQueueConfig};
use crate::pb::{
    execution_engine_service_client::ExecutionEngineServiceClient, runtime_message,
    runtime_service_request, runtime_service_response, service_message, CompleteJobRequest,
    CompleteJobResponse, ComponentInfo, DispatchComponentRequest, DispatchComponentResponse,
    EntityStateLoadResult, EntityStateSaveResult, EventStreamMessage, GetEntityStateRequest,
    GetEntityStateResponse, HealthCheck, JobAssignment, LeaseRenewalOutcome, PollJobRequest,
    PutEntityStateRequest, PutEntityStateResponse, RegisterService, RegisterWorkerSessionRequest,
    RenewJobLeaseRequest, ReportWorkerCapacityRequest, RuntimeMessage, RuntimeMessageType,
    RuntimeServiceResponse, ServiceMessage, UnregisterService, WorkerCapability,
    WorkerHealthStatus, WorkerMode, WorkerSlotPolicy, WriteCheckpointRequest,
};
use sha2::{Digest as _, Sha256};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as TokioMutex;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

const PARKED_WORKER_SESSION_REGISTER_ATTEMPTS: usize = 3;
const PARKED_WORKER_SESSION_REGISTER_RETRY_MS: u64 = 1_000;
const PARKED_WORKER_SESSION_TRANSIENT_RETRY_MAX_MS: u64 = 32_000;
const PARKED_COMPLETION_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);
const PARKED_COMPLETE_JOB_ATTEMPTS: usize = 3;
const PARKED_COMPLETE_JOB_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);
const PARKED_COMPLETE_JOB_RETRY_DELAY: Duration = Duration::from_millis(100);
const DEPLOYMENT_ARTIFACT_DOMAIN: &[u8] = b"agnt5.deployment-artifact.v1\0";
const ASSIGNMENT_AUTHORED_RETRY_METADATA_KEYS: &[&str] = &[
    "attempt",
    "max_attempts",
    "initial_interval_ms",
    "max_interval_ms",
    "backoff_type",
    "backoff_multiplier",
];

/// Slot lifecycle events sent from parked poll slots to the ramp supervisor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParkedSlotEvent {
    /// A claimed job has reached the language runtime and begun execution.
    Started { active_started: usize },
    /// A surplus idle slot retired itself (`total_slots` already decremented).
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerSlotPhase {
    ClaimedNotStarted,
    Executing,
    Terminalizing,
}

#[derive(Debug)]
struct WorkerSlotEntry {
    generation: u64,
    phase: WorkerSlotPhase,
    claimed_at: Instant,
}

#[derive(Default)]
struct WorkerSlotPhaseState {
    entries: HashMap<String, WorkerSlotEntry>,
    next_generation: u64,
    started_notifier: Option<tokio::sync::mpsc::UnboundedSender<ParkedSlotEvent>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct WorkerSlotPhaseSnapshot {
    claimed_not_started: usize,
    executing: usize,
    terminalizing: usize,
}

impl WorkerSlotPhaseState {
    fn snapshot(&self) -> WorkerSlotPhaseSnapshot {
        let mut snapshot = WorkerSlotPhaseSnapshot::default();
        for entry in self.entries.values() {
            match entry.phase {
                WorkerSlotPhase::ClaimedNotStarted => snapshot.claimed_not_started += 1,
                WorkerSlotPhase::Executing => snapshot.executing += 1,
                WorkerSlotPhase::Terminalizing => snapshot.terminalizing += 1,
            }
        }
        snapshot
    }
}

/// Tracks pull jobs from claim through language start and terminal acknowledgement.
/// The state mutex is never held across I/O and contains at most one entry per
/// active pull slot.
#[derive(Clone, Default)]
struct WorkerSlotPhases {
    state: Arc<std::sync::Mutex<WorkerSlotPhaseState>>,
}

impl WorkerSlotPhases {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, WorkerSlotPhaseState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn publish(snapshot: WorkerSlotPhaseSnapshot) {
        crate::telemetry::record_worker_slot_phases(
            "pull",
            snapshot.claimed_not_started as u64,
            snapshot.executing as u64,
            snapshot.terminalizing as u64,
        );
    }

    fn set_started_notifier(&self, notifier: tokio::sync::mpsc::UnboundedSender<ParkedSlotEvent>) {
        self.lock_state().started_notifier = Some(notifier);
    }

    fn claim(&self, run_id: String) -> WorkerSlotPhaseGuard {
        let (generation, replaced, snapshot) = {
            let mut state = self.lock_state();
            let generation = state.next_generation;
            state.next_generation = state.next_generation.wrapping_add(1);
            let replaced = state
                .entries
                .insert(
                    run_id.clone(),
                    WorkerSlotEntry {
                        generation,
                        phase: WorkerSlotPhase::ClaimedNotStarted,
                        claimed_at: Instant::now(),
                    },
                )
                .is_some();
            (generation, replaced, state.snapshot())
        };
        if replaced {
            warn!(run_id, "Replacing duplicate active pull-slot phase entry");
        }
        Self::publish(snapshot);
        WorkerSlotPhaseGuard {
            phases: self.clone(),
            run_id,
            generation,
        }
    }

    fn mark_started(&self, run_id: &str) {
        let transition = {
            let mut state = self.lock_state();
            let Some(entry) = state.entries.get_mut(run_id) else {
                return;
            };
            if entry.phase != WorkerSlotPhase::ClaimedNotStarted {
                return;
            }
            entry.phase = WorkerSlotPhase::Executing;
            let claim_to_start = entry.claimed_at.elapsed();
            let notifier = state.started_notifier.clone();
            (claim_to_start, notifier, state.snapshot())
        };
        crate::telemetry::record_worker_claim_to_start("pull", transition.0.as_secs_f64());
        Self::publish(transition.2);
        if let Some(notifier) = transition.1 {
            let active_started = transition.2.executing + transition.2.terminalizing;
            let _ = notifier.send(ParkedSlotEvent::Started { active_started });
        }
    }

    fn mark_terminalizing(&self, run_id: &str) {
        let snapshot = {
            let mut state = self.lock_state();
            let Some(entry) = state.entries.get_mut(run_id) else {
                return;
            };
            if entry.phase == WorkerSlotPhase::Terminalizing {
                return;
            }
            entry.phase = WorkerSlotPhase::Terminalizing;
            state.snapshot()
        };
        Self::publish(snapshot);
    }

    fn finish(&self, run_id: &str, generation: u64) {
        let result = {
            let mut state = self.lock_state();
            let Some(entry) = state.entries.get(run_id) else {
                return;
            };
            if entry.generation != generation {
                return;
            }
            let residency = entry.claimed_at.elapsed();
            state.entries.remove(run_id);
            (residency, state.snapshot())
        };
        crate::telemetry::record_worker_slot_residency("pull", result.0.as_secs_f64());
        Self::publish(result.1);
    }

    #[cfg(test)]
    fn snapshot(&self) -> WorkerSlotPhaseSnapshot {
        self.lock_state().snapshot()
    }
}

struct WorkerSlotPhaseGuard {
    phases: WorkerSlotPhases,
    run_id: String,
    generation: u64,
}

impl Drop for WorkerSlotPhaseGuard {
    fn drop(&mut self) {
        self.phases.finish(&self.run_id, self.generation);
    }
}

/// Per-run ordering barriers for journal flushes and acknowledged checkpoints.
/// Cross-run ordering is not part of the journal contract, so unrelated runs
/// must not share a network-duration critical section.
#[derive(Clone, Default)]
struct RunFlushLocks {
    locks: Arc<std::sync::Mutex<HashMap<String, std::sync::Weak<TokioMutex<()>>>>>,
}

impl RunFlushLocks {
    fn lock_for_run(&self, run_id: &str) -> Arc<TokioMutex<()>> {
        let mut locks = match self.locks.lock() {
            Ok(locks) => locks,
            Err(poisoned) => poisoned.into_inner(),
        };

        if let Some(lock) = locks.get(run_id).and_then(std::sync::Weak::upgrade) {
            return lock;
        }

        // Weak entries let completed runs disappear without coordinating a
        // cleanup with the final guard. Compact stale keys opportunistically.
        if locks.len() >= 4096 {
            locks.retain(|_, lock| lock.strong_count() > 0);
        }

        let lock = Arc::new(TokioMutex::new(()));
        locks.insert(run_id.to_string(), Arc::downgrade(&lock));
        lock
    }

    async fn lock_run(&self, run_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        self.lock_for_run(run_id).lock_owned().await
    }

    async fn lock_runs<I>(&self, run_ids: I) -> Vec<tokio::sync::OwnedMutexGuard<()>>
    where
        I: IntoIterator<Item = String>,
    {
        let mut run_ids: Vec<_> = run_ids.into_iter().collect();
        run_ids.sort_unstable();
        run_ids.dedup();
        let locks: Vec<_> = run_ids
            .iter()
            .map(|run_id| self.lock_for_run(run_id))
            .collect();
        let mut guards = Vec::with_capacity(locks.len());
        for lock in locks {
            guards.push(lock.lock_owned().await);
        }
        guards
    }
}

async fn await_checkpoint_ack<F, T>(
    future: F,
    timeout_ms: u64,
    operation: &str,
    run_id: &str,
    event_type: &str,
    sequence_number: i64,
) -> Result<T>
where
    F: Future<Output = T>,
{
    tokio::time::timeout(Duration::from_millis(timeout_ms), future)
        .await
        .map_err(|_| SdkError::Timeout {
            message: format!(
                "{operation} acknowledgement timed out after {timeout_ms}ms for \
                 run_id={run_id} event_type={event_type} seq={sequence_number}; \
                 persistence outcome is unknown"
            ),
            operation: operation.to_string(),
            duration_ms: Some(timeout_ms),
        })
}

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
    supported_protocol_capabilities: Vec<String>,
    required_protocol_capabilities: Vec<String>,
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
            supported_protocol_capabilities: self.supported_protocol_capabilities.clone(),
            required_protocol_capabilities: self.required_protocol_capabilities.clone(),
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

fn record_groups_by_run(records: &[crate::pb::Record]) -> Vec<Vec<usize>> {
    let mut group_by_run = HashMap::<String, usize>::new();
    let mut groups = Vec::<Vec<usize>>::new();
    for (index, record) in records.iter().enumerate() {
        let group_index = match group_by_run.get(&record.run_id) {
            Some(index) => *index,
            None => {
                let index = groups.len();
                group_by_run.insert(record.run_id.clone(), index);
                groups.push(Vec::new());
                index
            }
        };
        groups[group_index].push(index);
    }
    groups
}

struct AppendGroupProgress {
    committed: Vec<bool>,
    written_total: i32,
}

impl AppendGroupProgress {
    fn new(record_count: usize) -> Self {
        Self {
            committed: vec![false; record_count],
            written_total: 0,
        }
    }

    fn acknowledge(&mut self, group: &[usize], written: i32) {
        self.written_total = self.written_total.saturating_add(written);
        for &index in group {
            self.committed[index] = true;
        }
    }

    fn failure(self, error: SdkError) -> (SdkError, Vec<bool>, i32) {
        (error, self.committed, self.written_total)
    }
}

fn uncommitted_records_in_reverse<T>(records: Vec<T>, committed: &[bool]) -> Vec<T> {
    records
        .into_iter()
        .enumerate()
        .rev()
        .filter_map(|(index, record)| (!committed[index]).then_some(record))
        .collect()
}

async fn append_records_by_run(
    engine: &mut EngineClient,
    records: &[crate::pb::Record],
) -> std::result::Result<i32, (SdkError, Vec<bool>, i32)> {
    let mut progress = AppendGroupProgress::new(records.len());
    for group in record_groups_by_run(records) {
        let batch = group.iter().map(|index| records[*index].clone()).collect();
        match engine.append_batch(batch).await {
            Ok(written) => progress.acknowledge(&group, written),
            Err(error) => return Err(progress.failure(error)),
        }
    }
    Ok(progress.written_total)
}

fn polled_job_attempt(job: &JobAssignment) -> Result<u32> {
    u32::try_from(job.attempt).map_err(|_| SdkError::InvalidMessage {
        message: format!(
            "PollJob returned negative attempt {} for job {}",
            job.attempt, job.job_id
        ),
        field: Some("attempt".to_string()),
    })
}

fn runtime_message_from_job_assignment(
    job: JobAssignment,
    configured_claim_timeout_ms: i64,
) -> Result<(
    RuntimeMessage,
    bool,
    String,
    String,
    u32,
    i64,
    i64,
    HashMap<String, String>,
)> {
    if job.job_id.is_empty() || job.run_id.is_empty() {
        return Err(SdkError::InvalidMessage {
            message: "PollJob assignment requires nonempty job_id and run_id".to_string(),
            field: Some("job_id/run_id".to_string()),
        });
    }
    if job.job_id != job.run_id {
        return Err(SdkError::InvalidMessage {
            message: format!(
                "PollJob assignment job_id {} does not match run_id {}",
                job.job_id, job.run_id
            ),
            field: Some("job_id/run_id".to_string()),
        });
    }
    if job.lease_id.is_empty() {
        return Err(SdkError::InvalidMessage {
            message: format!(
                "PollJob assignment for job {} has no typed lease_id",
                job.job_id
            ),
            field: Some("lease_id".to_string()),
        });
    }
    if configured_claim_timeout_ms <= 0 {
        return Err(SdkError::Configuration {
            message: "parked pull claim timeout must be positive".to_string(),
            field: Some("AGNT5_CLAIM_TIMEOUT_MS".to_string()),
        });
    }
    let attempt = polled_job_attempt(&job)?;

    let mut metadata = job.metadata.clone();
    // A JobAssignment can only arrive through PollJob. Make that transport
    // decision visible to component code even when older queued records did
    // not carry the gateway's dispatch_mode stamp.
    // PollJob is the authority for this execution mode. Caller-authored queued
    // metadata must not downgrade a leased pull assignment to the unfenced
    // push response path in a language SDK.
    metadata.insert("dispatch_mode".to_string(), "pull".to_string());
    if !job.trace_id.is_empty() {
        metadata.insert("trace_id".to_string(), job.trace_id.clone());
    }
    metadata.insert("lease_id".to_string(), job.lease_id.clone());
    metadata.remove("lease_expires_at_ms");
    if job.lease_expires_at_ms > 0 {
        metadata.insert(
            "lease_expires_at_ms".to_string(),
            job.lease_expires_at_ms.to_string(),
        );
    }

    let is_streaming = metadata.get("stream_mode").map_or(false, |m| m == "full");
    let session_id = metadata.get("session_id").cloned().unwrap_or_default();
    let user_id = metadata.get("user_id").cloned().unwrap_or_default();
    let lease_id = job.lease_id.clone();
    let deployment_id = metadata.get("deployment_id").cloned().unwrap_or_default();
    let priority = metadata
        .get("priority")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0);
    let run_id = job.run_id.clone();
    let mut completion_metadata = HashMap::new();
    for key in &ASSIGNMENT_AUTHORED_RETRY_METADATA_KEYS[1..] {
        if let Some(value) = metadata.get(*key) {
            completion_metadata.insert((*key).to_string(), value.clone());
        }
    }
    completion_metadata.insert("attempt".to_string(), attempt.to_string());

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

    Ok((
        runtime_message,
        is_streaming,
        run_id,
        lease_id,
        attempt,
        configured_claim_timeout_ms,
        job.lease_expires_at_ms,
        completion_metadata,
    ))
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
    /// lease_id stash keyed by invocation_id. Populated on
    /// DispatchComponentRequest receipt (when req.lease_id is non-empty) and
    /// drained on response forward so the echoed lease_id lands in
    /// DispatchComponentResponse.lease_id without requiring language bindings
    /// to thread the value through their handler code.
    pending_lease_ids: Arc<std::sync::Mutex<HashMap<String, String>>>,
    /// Executions whose last known durable lease authority was revoked. The
    /// entry survives handler cancellation so a late language response cannot
    /// be forwarded after a cooperative cancellation race.
    revoked_executions: Arc<std::sync::Mutex<HashSet<String>>>,
    /// Per-execution stop handles for push lease renewal tasks. Pull slots own
    /// their stop handles directly because each slot awaits one job at a time.
    lease_renewal_stops: Arc<std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
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
    /// Serializes queue flushes and checkpoints within each run. Unrelated
    /// runs can persist checkpoints concurrently.
    journal_flush_locks: RunFlushLocks,
    /// Pull-slot phase state shared with language event emitters. Ramping is
    /// triggered only after `run.started`, preventing a blocked language event
    /// loop from claiming the worker's full concurrency budget.
    slot_phases: WorkerSlotPhases,
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

fn stamp_dispatch_mode(runtime_message: &mut RuntimeMessage, dispatch_mode: &str) {
    if let Some(crate::pb::runtime_message::MessageData::DispatchComponent(request)) =
        runtime_message.message_data.as_mut()
    {
        request
            .metadata
            .insert("dispatch_mode".to_string(), dispatch_mode.to_string());
    }
}

/// Stamp the durable execution authority carried by one dispatch onto the
/// metadata visible to language handlers. Lifecycle records emitted by those
/// handlers reuse these fields so the runtime can reject writes after the
/// lease expires or moves to another worker.
fn stamp_execution_authority_metadata(
    runtime_message: &mut RuntimeMessage,
    worker_id: &str,
    worker_session_id: &str,
    dispatch_mode: &str,
) {
    let Some(crate::pb::runtime_message::MessageData::DispatchComponent(request)) =
        runtime_message.message_data.as_mut()
    else {
        return;
    };

    request
        .metadata
        .insert("dispatch_mode".to_string(), dispatch_mode.to_string());
    request
        .metadata
        .insert("worker_id".to_string(), worker_id.to_string());
    request.metadata.insert(
        "worker_session_id".to_string(),
        worker_session_id.to_string(),
    );
    request
        .metadata
        .insert("lease_id".to_string(), request.lease_id.clone());
    request
        .metadata
        .insert("lease_attempt".to_string(), request.attempt.to_string());
}

fn stamp_protocol_capability(runtime_message: &mut RuntimeMessage, capability: &str) {
    let Some(crate::pb::runtime_message::MessageData::DispatchComponent(request)) =
        runtime_message.message_data.as_mut()
    else {
        return;
    };
    request
        .metadata
        .insert(capability.to_string(), "true".to_string());
}

fn canonical_activation_component_config(config: &HashMap<String, String>) -> String {
    let mut entries = config.iter().collect::<Vec<_>>();
    entries.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
    serde_json::to_string(&serde_json::json!([
        "object",
        entries
            .into_iter()
            .map(|(key, value)| serde_json::json!([key, ["string", value]]))
            .collect::<Vec<_>>()
    ]))
    .expect("canonical activation component config must serialize")
}

fn configured_activation_artifact_sha256(metadata: &HashMap<String, String>) -> Option<String> {
    metadata
        .get("activation_artifact_sha256")
        .cloned()
        .or_else(|| std::env::var("AGNT5_ACTIVATION_ARTIFACT_SHA256").ok())
        .filter(|value| !value.is_empty())
}

fn decode_activation_artifact_sha256(value: &str) -> Option<[u8; 32]> {
    use base64::Engine as _;

    if let Ok(decoded) = hex::decode(value) {
        if let Ok(digest) = decoded.try_into() {
            return Some(digest);
        }
    }
    [
        base64::engine::general_purpose::STANDARD.decode(value),
        base64::engine::general_purpose::STANDARD_NO_PAD.decode(value),
        base64::engine::general_purpose::URL_SAFE.decode(value),
        base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(value),
    ]
    .into_iter()
    .filter_map(|decoded| decoded.ok())
    .find_map(|decoded| decoded.try_into().ok())
}

fn valid_activation_artifact_sha256(value: &str) -> bool {
    decode_activation_artifact_sha256(value).is_some()
}

fn deployment_artifact_sha256(deployment_id: &str) -> Option<[u8; 32]> {
    let deployment_id = Uuid::parse_str(deployment_id).ok()?.to_string();
    let mut digest = Sha256::new();
    digest.update(DEPLOYMENT_ARTIFACT_DOMAIN);
    digest.update(deployment_id.as_bytes());
    Some(digest.finalize().into())
}

fn configured_deployment_artifact_sha256(metadata: &HashMap<String, String>) -> Option<[u8; 32]> {
    ["AGNT5_DEPLOYMENT_ID", "deployment_id"]
        .into_iter()
        .filter_map(|key| metadata.get(key))
        .map(String::as_str)
        .find_map(deployment_artifact_sha256)
        .or_else(|| {
            std::env::var("AGNT5_DEPLOYMENT_ID")
                .ok()
                .and_then(|value| deployment_artifact_sha256(&value))
        })
}

fn worker_protocol_capabilities_for_metadata(
    metadata: &HashMap<String, String>,
) -> Result<(Vec<String>, Vec<String>)> {
    let (supported, required) = crate::client::worker_protocol_capabilities();
    if !supported
        .iter()
        .any(|capability| capability == crate::client::DURABLE_ACTIVATION_V1_CAPABILITY)
    {
        return Ok((supported, required));
    }
    if let Some(configured) = configured_activation_artifact_sha256(metadata) {
        if valid_activation_artifact_sha256(&configured) {
            let configured = decode_activation_artifact_sha256(&configured)
                .expect("validated activation artifact digest");
            if configured_deployment_artifact_sha256(metadata)
                .is_none_or(|expected| expected == configured)
            {
                return Ok((supported, required));
            }
        }
    }

    let message = "durable_activation_v1 requires the control-plane deployment artifact identity";
    if required
        .iter()
        .any(|capability| capability == crate::client::DURABLE_ACTIVATION_V1_CAPABILITY)
    {
        return Err(SdkError::Activation {
            message: message.to_string(),
            code: crate::error::ErrorCode::DurabilityUnavailable,
            activation_id: None,
            attempt: None,
        });
    }
    eprintln!(
        "[WARN] agnt5 durable activation degraded: {message}; legacy checkpoints remain enabled"
    );
    Ok((Vec::new(), Vec::new()))
}

fn activation_definition_configs(components: &[ComponentInfo]) -> HashMap<String, String> {
    components
        .iter()
        .map(|component| {
            (
                component.name.clone(),
                canonical_activation_component_config(&component.config),
            )
        })
        .collect()
}

fn stamp_activation_dispatch_metadata(
    runtime_message: &mut RuntimeMessage,
    worker_id: &str,
    worker_session_id: &str,
    service_version: &str,
    worker_metadata: &HashMap<String, String>,
    definition_configs: &HashMap<String, String>,
) -> Result<()> {
    let Some(crate::pb::runtime_message::MessageData::DispatchComponent(request)) =
        runtime_message.message_data.as_mut()
    else {
        return Ok(());
    };

    request.metadata.insert(
        crate::client::DURABLE_ACTIVATION_V1_CAPABILITY.to_string(),
        "true".to_string(),
    );
    request
        .metadata
        .insert("worker_id".to_string(), worker_id.to_string());
    request.metadata.insert(
        "worker_session_id".to_string(),
        worker_session_id.to_string(),
    );
    request
        .metadata
        .insert("lease_id".to_string(), request.lease_id.clone());
    request
        .metadata
        .entry("run_authority".to_string())
        .or_insert_with(|| request.invocation_id.clone());
    request
        .metadata
        .entry("lease_authority".to_string())
        .or_insert_with(|| request.lease_id.clone());
    request
        .metadata
        .insert("component_name".to_string(), request.component_name.clone());
    request.metadata.insert(
        "activation_definition_version".to_string(),
        service_version.to_string(),
    );
    request.metadata.insert(
        "activation_definition_config".to_string(),
        definition_configs
            .get(&request.component_name)
            .cloned()
            .unwrap_or_else(|| "[\"object\",[]]".to_string()),
    );

    if let Some(value) = worker_metadata.get("project_id") {
        request
            .metadata
            .entry("project_id".to_string())
            .or_insert_with(|| value.clone());
    }
    let worker_artifact =
        configured_activation_artifact_sha256(worker_metadata).ok_or_else(|| {
            SdkError::Activation {
                message: "negotiated durable activation is missing the worker artifact identity"
                    .to_string(),
                code: crate::error::ErrorCode::DurabilityUnavailable,
                activation_id: None,
                attempt: None,
            }
        })?;
    let worker_digest = decode_activation_artifact_sha256(&worker_artifact).ok_or_else(|| {
        SdkError::Activation {
            message: "negotiated durable activation has an invalid worker artifact identity"
                .to_string(),
            code: crate::error::ErrorCode::DurabilityUnavailable,
            activation_id: None,
            attempt: None,
        }
    })?;
    if let Some(run_artifact) = request.metadata.get("activation_artifact_sha256") {
        if decode_activation_artifact_sha256(run_artifact) != Some(worker_digest) {
            return Err(SdkError::Activation {
                message:
                    "worker artifact identity does not match the run's pinned deployment artifact"
                        .to_string(),
                code: crate::error::ErrorCode::NondeterministicReplay,
                activation_id: None,
                attempt: None,
            });
        }
    } else {
        request
            .metadata
            .insert("activation_artifact_sha256".to_string(), worker_artifact);
    }
    Ok(())
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

struct DurableSuspensionEnvelope {
    invocation_id: String,
    metadata: HashMap<String, String>,
    attempt: i32,
    lease_id: String,
}

fn durable_suspension_envelope(
    runtime_message: &RuntimeMessage,
) -> Option<DurableSuspensionEnvelope> {
    let crate::pb::runtime_message::MessageData::DispatchComponent(request) =
        runtime_message.message_data.as_ref()?
    else {
        return None;
    };
    Some(DurableSuspensionEnvelope {
        invocation_id: request.invocation_id.clone(),
        metadata: request.metadata.clone(),
        attempt: request.attempt,
        lease_id: request.lease_id.clone(),
    })
}

async fn execute_runtime_message_for_response<F, Fut>(
    worker_name: &str,
    response_worker_id: &str,
    mut runtime_message: RuntimeMessage,
    response_tx: flume::Sender<ServiceMessage>,
    handler: F,
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    cancel_tokens: Arc<std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
    revoked_executions: Arc<std::sync::Mutex<HashSet<String>>>,
    dispatch_mode: &'static str,
) -> Option<ServiceMessage>
where
    F: Fn(RuntimeMessage, flume::Sender<ServiceMessage>) -> Fut + Send + Clone + 'static,
    Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send + 'static,
{
    let _in_flight = InFlightGuard::enter(&in_flight);
    let tx_clone = response_tx.clone();
    stamp_dispatch_mode(&mut runtime_message, dispatch_mode);
    let suspension_envelope = durable_suspension_envelope(&runtime_message);

    let run_key = dispatch_run_key(&runtime_message);
    if run_key
        .as_deref()
        .is_some_and(|run_id| execution_is_revoked(&revoked_executions, run_id))
    {
        warn!(
            "Worker {} refusing dispatch after local execution authority was revoked",
            worker_name
        );
        return None;
    }
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

    if run_key
        .as_deref()
        .is_some_and(|run_id| execution_is_revoked(&revoked_executions, run_id))
    {
        warn!(
            "Worker {} suppressing response after execution authority loss",
            worker_name
        );
        return None;
    }

    let response = match result {
        Some(Ok(Some(response))) => Some(response),
        Some(Ok(None)) => None,
        Some(Err(SdkError::DurableSuspension { suspension })) => {
            suspension_envelope.as_ref().map(|envelope| {
                durable_suspension_service_message(response_worker_id, envelope, *suspension)
            })
        }
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
    response_worker_id: &str,
    runtime_message: RuntimeMessage,
    response_tx: flume::Sender<ServiceMessage>,
    handler: F,
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    cancel_tokens: Arc<std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
    revoked_executions: Arc<std::sync::Mutex<HashSet<String>>>,
) where
    F: Fn(RuntimeMessage, flume::Sender<ServiceMessage>) -> Fut + Send + Clone + 'static,
    Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send + 'static,
{
    if let Some(response) = execute_runtime_message_for_response(
        worker_name,
        response_worker_id,
        runtime_message,
        response_tx.clone(),
        handler,
        in_flight,
        cancel_tokens,
        revoked_executions,
        "push",
    )
    .await
    {
        if let Err(e) = response_tx.send_async(response).await {
            error!("Worker {} failed to send response: {}", worker_name, e);
        }
    }
}

fn durable_suspension_service_message(
    worker_id: &str,
    envelope: &DurableSuspensionEnvelope,
    suspension: crate::pb::WorkerSuspension,
) -> ServiceMessage {
    ServiceMessage {
        worker_id: worker_id.to_string(),
        metadata: HashMap::new(),
        message_type: Some(crate::pb::service_message::MessageType::FunctionResponse(
            DispatchComponentResponse {
                invocation_id: envelope.invocation_id.clone(),
                success: true,
                result: Some(
                    crate::pb::dispatch_component_response::Result::WorkerSuspension(suspension),
                ),
                error_message: String::new(),
                metadata: envelope.metadata.clone(),
                event_type: "workflow.paused".to_string(),
                content_index: 0,
                sequence: 0,
                attempt: envelope.attempt,
                source_timestamp_ns: 0,
                lease_id: envelope.lease_id.clone(),
            },
        )),
    }
}

struct PolledJobCompletion {
    job_id: String,
    success: bool,
    output_data: Vec<u8>,
    error_message: String,
    error_code: String,
    event_type: String,
    metadata: HashMap<String, String>,
    lease_id: String,
    attempt: u32,
}

#[async_trait::async_trait]
trait CompleteJobSender: Send {
    async fn send_complete_job(
        &mut self,
        request: CompleteJobRequest,
    ) -> Result<CompleteJobResponse>;
}

#[async_trait::async_trait]
impl CompleteJobSender for WorkerCoordinatorClient {
    async fn send_complete_job(
        &mut self,
        request: CompleteJobRequest,
    ) -> Result<CompleteJobResponse> {
        self.complete_job(request).await
    }
}

#[async_trait::async_trait]
trait EntityStateSender: Send {
    async fn send_get_entity_state(
        &mut self,
        request: GetEntityStateRequest,
    ) -> Result<GetEntityStateResponse>;

    async fn send_put_entity_state(
        &mut self,
        request: PutEntityStateRequest,
    ) -> Result<PutEntityStateResponse>;
}

#[async_trait::async_trait]
impl EntityStateSender for WorkerCoordinatorClient {
    async fn send_get_entity_state(
        &mut self,
        request: GetEntityStateRequest,
    ) -> Result<GetEntityStateResponse> {
        self.get_entity_state(request).await
    }

    async fn send_put_entity_state(
        &mut self,
        request: PutEntityStateRequest,
    ) -> Result<PutEntityStateResponse> {
        self.put_entity_state(request).await
    }
}

#[allow(clippy::too_many_arguments)]
async fn parked_runtime_service_response<S: EntityStateSender>(
    sender: &mut S,
    service_message: &ServiceMessage,
    project_id: &str,
    run_id: &str,
    worker_id: &str,
    worker_session_id: &str,
    lease_id: &str,
    attempt: u32,
) -> Option<RuntimeMessage> {
    let service_message::MessageType::RuntimeService(request) =
        service_message.message_type.as_ref()?
    else {
        return None;
    };

    let request_id = request.request_id.clone();
    let response = match request.operation.as_ref() {
        Some(runtime_service_request::Operation::EntityStateLoad(load)) => {
            match sender
                .send_get_entity_state(GetEntityStateRequest {
                    project_id: project_id.to_string(),
                    entity_type: load.entity_type.clone(),
                    entity_key: load.entity_key.clone(),
                    scope: load.scope.clone(),
                    scope_id: load.scope_id.clone(),
                })
                .await
            {
                Ok(result) => RuntimeServiceResponse {
                    request_id,
                    success: true,
                    error_message: String::new(),
                    result: Some(runtime_service_response::Result::EntityStateLoad(
                        EntityStateLoadResult {
                            found: result.found,
                            state_json: result.state_json,
                            version: result.version,
                        },
                    )),
                },
                Err(error) => RuntimeServiceResponse {
                    request_id,
                    success: false,
                    error_message: error.to_string(),
                    result: None,
                },
            }
        }
        Some(runtime_service_request::Operation::EntityStateSave(save)) => {
            match sender
                .send_put_entity_state(PutEntityStateRequest {
                    project_id: project_id.to_string(),
                    entity_type: save.entity_type.clone(),
                    entity_key: save.entity_key.clone(),
                    scope: save.scope.clone(),
                    scope_id: save.scope_id.clone(),
                    state_json: save.state_json.clone(),
                    expected_version: save.expected_version,
                    run_id: run_id.to_string(),
                    worker_id: worker_id.to_string(),
                    worker_session_id: worker_session_id.to_string(),
                    lease_id: lease_id.to_string(),
                    attempt: Some(attempt),
                    operation_id: request.request_id.clone(),
                })
                .await
            {
                Ok(result) => RuntimeServiceResponse {
                    request_id,
                    success: true,
                    error_message: String::new(),
                    result: Some(runtime_service_response::Result::EntityStateSave(
                        EntityStateSaveResult {
                            new_version: result.new_version,
                        },
                    )),
                },
                Err(error) => RuntimeServiceResponse {
                    request_id,
                    success: false,
                    error_message: error.to_string(),
                    result: None,
                },
            }
        }
        _ => RuntimeServiceResponse {
            request_id,
            success: false,
            error_message: "runtime service operation is not available through parked polling yet"
                .to_string(),
            result: None,
        },
    };

    Some(RuntimeMessage {
        worker_id: worker_id.to_string(),
        message_type: RuntimeMessageType::RuntimeService as i32,
        metadata: HashMap::new(),
        message_data: Some(runtime_message::MessageData::RuntimeServiceResponse(
            response,
        )),
    })
}

async fn complete_job_with_retry<S: CompleteJobSender>(
    sender: &mut S,
    request: CompleteJobRequest,
    attempts: usize,
    attempt_timeout: Duration,
    retry_delay: Duration,
) -> Result<()> {
    let attempts = attempts.max(1);
    let job_id = request.job_id.clone();
    let mut last_error = None;

    for attempt in 1..=attempts {
        let outcome =
            tokio::time::timeout(attempt_timeout, sender.send_complete_job(request.clone())).await;
        match outcome {
            Ok(Ok(response)) if response.acknowledged => return Ok(()),
            Ok(Ok(_)) => {
                last_error = Some(SdkError::Internal(format!(
                    "CompleteJob was not acknowledged for job {job_id}"
                )));
            }
            Ok(Err(error)) => {
                last_error = Some(error);
            }
            Err(_) => {
                last_error = Some(SdkError::Timeout {
                    message: format!("CompleteJob timed out for job {job_id}"),
                    operation: "CompleteJob".to_string(),
                    duration_ms: Some(attempt_timeout.as_millis() as u64),
                });
            }
        }

        if attempt < attempts {
            warn!(
                "CompleteJob attempt {}/{} failed for job_id={}; retrying",
                attempt, attempts, job_id
            );
            tokio::time::sleep(retry_delay).await;
        }
    }

    Err(last_error.unwrap_or_else(|| {
        SdkError::Internal(format!(
            "CompleteJob failed without an outcome for job {job_id}"
        ))
    }))
}

fn polled_job_completion_from_service_message(
    service_message: &ServiceMessage,
    assigned_job_id: &str,
    assigned_lease_id: &str,
    assigned_attempt: u32,
    assigned_completion_metadata: &HashMap<String, String>,
) -> Option<PolledJobCompletion> {
    match &service_message.message_type {
        Some(crate::pb::service_message::MessageType::FunctionResponse(resp))
            if is_terminal_worker_response(&resp.event_type) =>
        {
            let output_data = match &resp.result {
                Some(crate::pb::dispatch_component_response::Result::OutputData(data)) => {
                    data.clone()
                }
                _ => Vec::new(),
            };

            let mut metadata = resp.metadata.clone();
            for key in ASSIGNMENT_AUTHORED_RETRY_METADATA_KEYS {
                metadata.remove(*key);
            }
            metadata.extend(assigned_completion_metadata.clone());

            Some(PolledJobCompletion {
                job_id: assigned_job_id.to_string(),
                success: resp.success,
                output_data,
                error_message: resp.error_message.clone(),
                error_code: metadata.get("error_code").cloned().unwrap_or_default(),
                event_type: resp.event_type.clone(),
                metadata,
                lease_id: assigned_lease_id.to_string(),
                attempt: assigned_attempt,
            })
        }
        _ => None,
    }
}

fn polled_job_suspension_request(
    service_message: &ServiceMessage,
    project_id: &str,
    run_id: &str,
) -> Option<crate::pb::SuspendActivationRequest> {
    let Some(crate::pb::service_message::MessageType::FunctionResponse(response)) =
        &service_message.message_type
    else {
        return None;
    };
    let Some(crate::pb::dispatch_component_response::Result::WorkerSuspension(suspension)) =
        &response.result
    else {
        return None;
    };
    Some(crate::pb::SuspendActivationRequest {
        project_id: project_id.to_string(),
        run_id: run_id.to_string(),
        activation_id: suspension.activation_id.clone(),
        attempt: suspension.attempt,
        fence_token: suspension.fence_token.clone(),
        timer_key: suspension.timer_key.clone(),
        ready_at_ms: suspension.ready_at_ms,
        input_digest: suspension.input_digest.clone(),
        definition_digest: suspension.definition_digest.clone(),
        continuation: suspension.continuation.clone(),
        delay_ms: suspension.delay_ms,
    })
}

async fn wait_for_parked_run_events_flush(
    journal_queue: &JournalEventQueue,
    journal_flush_locks: &RunFlushLocks,
    run_id: &str,
) -> bool {
    let deadline = tokio::time::Instant::now() + PARKED_COMPLETION_FLUSH_TIMEOUT;
    loop {
        {
            // The periodic sender holds this run's barrier until its drained
            // events have been acknowledged or requeued. If this run is absent
            // while we hold it, CompleteJob cannot overtake a child event.
            let _flush_guard = journal_flush_locks.lock_run(run_id).await;
            if !journal_queue.contains_run(run_id) {
                return true;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(
            journal_queue.flush_interval_ms().max(1),
        ))
        .await;
    }
}

fn complete_job_request_from_polled_completion(
    worker_id: &str,
    worker_session_id: &str,
    tenant_id: &str,
    completion: PolledJobCompletion,
) -> CompleteJobRequest {
    let mut metadata = completion.metadata;
    metadata.insert("completion_event_type".to_string(), completion.event_type);
    CompleteJobRequest {
        job_id: completion.job_id,
        worker_id: worker_id.to_string(),
        success: completion.success,
        output_data: completion.output_data,
        error_message: completion.error_message,
        error_code: completion.error_code,
        metadata,
        project_id: tenant_id.to_string(),
        lease_id: completion.lease_id,
        worker_session_id: worker_session_id.to_string(),
        attempt: Some(completion.attempt),
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
    let request = complete_job_request_from_polled_completion(
        worker_id,
        worker_session_id,
        tenant_id,
        completion,
    );
    match complete_job_with_retry(
        client,
        request,
        PARKED_COMPLETE_JOB_ATTEMPTS,
        PARKED_COMPLETE_JOB_ATTEMPT_TIMEOUT,
        PARKED_COMPLETE_JOB_RETRY_DELAY,
    )
    .await
    {
        Ok(()) => {
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
    assigned_job_id: &str,
    assigned_lease_id: &str,
    assigned_attempt: u32,
    assigned_completion_metadata: &HashMap<String, String>,
    worker_id: &str,
    worker_session_id: &Arc<TokioMutex<String>>,
    tenant_id: &str,
    slot_idx: usize,
    response_tx: &flume::Sender<ServiceMessage>,
    journal_queue: &JournalEventQueue,
    journal_flush_locks: &RunFlushLocks,
) -> bool {
    if is_cancelled_worker_response(&service_message) {
        debug!(
            "Parked poll slot {} observed runtime-authored cancellation for job_id={}",
            slot_idx, assigned_job_id
        );
        return true;
    }

    if let Some(request) =
        polled_job_suspension_request(&service_message, tenant_id, assigned_job_id)
    {
        match client.suspend_activation(request).await {
            Ok(receipt) if receipt.accepted => {
                debug!(
                    "Parked poll slot {} suspended job_id={} timer_id={}",
                    slot_idx, assigned_job_id, receipt.timer_id
                );
                return true;
            }
            Ok(_) => {
                warn!(
                    "Parked poll slot {} received an unaccepted suspension receipt for job_id={}",
                    slot_idx, assigned_job_id
                );
                return false;
            }
            Err(error) => {
                warn!(
                    "Parked poll slot {} failed to suspend job_id={}: {}",
                    slot_idx, assigned_job_id, error
                );
                return false;
            }
        }
    }

    let Some(completion) = polled_job_completion_from_service_message(
        &service_message,
        assigned_job_id,
        assigned_lease_id,
        assigned_attempt,
        assigned_completion_metadata,
    ) else {
        if let Err(e) = response_tx.send_async(service_message).await {
            error!(
                "Parked poll slot {} failed to send response: {}",
                slot_idx, e
            );
        }
        return false;
    };

    let job_id = completion.job_id.clone();
    if !wait_for_parked_run_events_flush(journal_queue, journal_flush_locks, &job_id).await {
        warn!(
            "Parked poll slot {} refusing to overtake unflushed events for job_id={}",
            slot_idx, job_id
        );
        return false;
    }
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

fn active_lease_renew_interval_ms(lease_timeout_ms: i64) -> u64 {
    let timeout_ms = lease_timeout_ms.max(2) as u64;
    (timeout_ms / 2).clamp(1, 60_000)
}

fn active_lease_renew_interval_with_jitter_ms(lease_timeout_ms: i64) -> u64 {
    let base = active_lease_renew_interval_ms(lease_timeout_ms);
    let jitter = rand::random::<f64>() * 0.20 - 0.10;
    ((base as f64) * (1.0 + jitter)).round().max(1.0) as u64
}

fn active_lease_danger_retry_ms(lease_timeout_ms: i64) -> u64 {
    let timeout_ms = lease_timeout_ms.max(2) as u64;
    (timeout_ms / 10).clamp(1, 5_000)
}

#[derive(Clone)]
enum ActiveLeaseSession {
    Push,
    Pull(Arc<TokioMutex<String>>),
}

#[derive(Clone)]
struct ActiveLeaseAuthority {
    worker_id: String,
    project_id: String,
    deployment_id: String,
    run_id: String,
    lease_id: String,
    attempt: u32,
    lease_timeout_ms: i64,
    lease_expires_at_ms: i64,
    session: ActiveLeaseSession,
}

impl ActiveLeaseAuthority {
    fn mode_label(&self) -> &'static str {
        match &self.session {
            ActiveLeaseSession::Push => "push",
            ActiveLeaseSession::Pull(_) => "pull",
        }
    }

    async fn renewal_request(&self) -> RenewJobLeaseRequest {
        let (worker_session_id, mode) = match &self.session {
            ActiveLeaseSession::Push => (String::new(), WorkerMode::Push),
            ActiveLeaseSession::Pull(session_id) => {
                (session_id.lock().await.clone(), WorkerMode::Pull)
            }
        };
        RenewJobLeaseRequest {
            worker_id: self.worker_id.clone(),
            worker_session_id,
            run_id: self.run_id.clone(),
            lease_id: self.lease_id.clone(),
            lease_timeout_ms: self.lease_timeout_ms,
            attempt: Some(self.attempt),
            mode: mode as i32,
            project_id: self.project_id.clone(),
            deployment_id: self.deployment_id.clone(),
        }
    }
}

fn execution_is_revoked(
    revoked_executions: &Arc<std::sync::Mutex<HashSet<String>>>,
    run_id: &str,
) -> bool {
    revoked_executions
        .lock()
        .map(|runs| runs.contains(run_id))
        .unwrap_or(true)
}

fn revoke_execution_authority(
    run_id: &str,
    revoked_executions: &Arc<std::sync::Mutex<HashSet<String>>>,
    cancel_tokens: &Arc<std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
    cancel_hook: &Arc<std::sync::Mutex<Option<Box<dyn Fn(String) + Send + Sync>>>>,
) {
    if let Ok(mut runs) = revoked_executions.lock() {
        runs.insert(run_id.to_string());
    }
    let hooked = cancel_hook
        .lock()
        .ok()
        .and_then(|hook| hook.as_ref().map(|hook| hook(run_id.to_string())))
        .is_some();
    if !hooked {
        if let Some(cancel) = cancel_tokens
            .lock()
            .ok()
            .and_then(|mut tokens| tokens.remove(run_id))
        {
            let _ = cancel.send(());
        }
    }
}

async fn report_worker_capacity_with_client(
    client: &mut WorkerCoordinatorClient,
    worker_id: &str,
    worker_session_id: &str,
    open_poll_slots: Arc<std::sync::atomic::AtomicUsize>,
    active_slots: Arc<std::sync::atomic::AtomicUsize>,
    desired_slots: Arc<std::sync::atomic::AtomicUsize>,
    effective_max_slots: usize,
) {
    let open_poll_slots = open_poll_slots.load(std::sync::atomic::Ordering::Relaxed) as u32;
    let active_slots = active_slots.load(std::sync::atomic::Ordering::Relaxed) as u32;
    let desired_slots = desired_slots.load(std::sync::atomic::Ordering::Relaxed);
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
    desired_slots: Arc<std::sync::atomic::AtomicUsize>,
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
            desired_slots.clone(),
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
                        desired_slots.clone(),
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
                if let Err(error) = crate::client::validate_protocol_capabilities(
                    &registration.supported_protocol_capabilities,
                    &registration.required_protocol_capabilities,
                    &session.supported_protocol_capabilities,
                    &session.required_protocol_capabilities,
                ) {
                    warn!("{} protocol negotiation failed: {}", reason, error);
                    return ParkedWorkerSessionRegistrationResult::Rejected;
                }
                client.retain_negotiated_protocol_capabilities(
                    &registration.supported_protocol_capabilities,
                    &session.supported_protocol_capabilities,
                );
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
    refresh_lock: &Arc<TokioMutex<()>>,
) -> bool {
    // Single-flight the refresh: hold the lock across the stale-check +
    // register + store so concurrent ramped slots don't each register a fresh
    // session. The winner stores its new ID; losers acquire the lock afterward,
    // see the refreshed session in `try_refresh_parked_worker_session_once`, and
    // return early without registering.
    let _refresh_guard = refresh_lock.lock().await;
    try_refresh_parked_worker_session_once(
        client,
        session_id,
        observed_session_id,
        registration,
        slot_idx,
    )
    .await
}

/// Shared state for one parked polling session. Bundled behind an `Arc` so the
/// supervisor can spawn additional slots with a single clone while ramping.
/// The message handler stays outside: each slot owns its own clone so the
/// context does not force `Sync` on the handler type.
struct ParkedPollContext {
    client: WorkerCoordinatorClient,
    worker_id: String,
    worker_session_id: Arc<TokioMutex<String>>,
    registration: ParkedWorkerSessionRegistration,
    service_version: String,
    worker_metadata: HashMap<String, String>,
    activation_definition_configs: HashMap<String, String>,
    project_id: String,
    response_tx: flume::Sender<ServiceMessage>,
    in_flight: Arc<std::sync::atomic::AtomicUsize>,
    cancel_tokens: Arc<std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
    cancel_hook: Arc<std::sync::Mutex<Option<Box<dyn Fn(String) + Send + Sync>>>>,
    revoked_executions: Arc<std::sync::Mutex<HashSet<String>>>,
    streaming_runs: Arc<std::sync::Mutex<HashMap<String, bool>>>,
    pending_lease_ids: Arc<std::sync::Mutex<HashMap<String, String>>>,
    journal_queue: JournalEventQueue,
    journal_flush_locks: RunFlushLocks,
    slot_phases: WorkerSlotPhases,
    open_poll_slots: Arc<std::sync::atomic::AtomicUsize>,
    /// Live slot count (parked + busy), shared with the supervisor.
    total_slots: Arc<std::sync::atomic::AtomicUsize>,
    /// Slots currently executing a handler; drives ramp-up decisions. Kept
    /// separate from `in_flight`, which also counts stream-dispatched work.
    busy_slots: Arc<std::sync::atomic::AtomicUsize>,
    events_tx: tokio::sync::mpsc::UnboundedSender<ParkedSlotEvent>,
    /// Single-flights worker-session re-registration. A burst of ramped slots can
    /// all observe the same inactive session at once; without this, each would
    /// `RegisterWorkerSession` and the locally stored ID (chosen by lock order)
    /// can diverge from the coordinator's last-write-wins session, leaving every
    /// slot polling with a rejected session. Held across the whole refresh so
    /// losers observe the winner's session and skip re-registering.
    session_refresh_lock: Arc<TokioMutex<()>>,
    claim_timeout_ms: i64,
    min_slots: usize,
    retire_empty_polls: usize,
}

/// How many new slots to spawn when a parked slot goes busy: grow the fleet
/// toward `2 * busy` (a parked buffer equal to current demand) so a burst of N
/// jobs reaches N concurrent handlers in ~log2(N) poll round-trips, without
/// spawning for demand that already-parked slots can absorb and never
/// exceeding `max_slots` total.
fn parked_ramp_spawn_count(total_slots: usize, busy_slots: usize, max_slots: usize) -> usize {
    busy_slots
        .saturating_mul(2)
        .min(max_slots)
        .saturating_sub(total_slots)
}

/// Retire one surplus slot, but only while at least `min_slots` *idle* pollers
/// would remain. `idle = total_slots - busy_slots`; a slot may retire only when
/// `total_slots > min_slots + busy_slots`, so a busy handler never leaves the
/// fleet with fewer than `min_slots` outstanding `PollJob` requests. Flooring on
/// total alone lets the last idle poller retire while the sole remaining slot is
/// busy, which reintroduces serial execution for sparse long-running work. Since
/// `busy_slots >= 0`, this also preserves the hard `min_slots` floor on the total.
/// Returns true when the calling slot won the right to retire.
fn try_retire_parked_slot(
    total_slots: &std::sync::atomic::AtomicUsize,
    busy_slots: &std::sync::atomic::AtomicUsize,
    min_slots: usize,
) -> bool {
    total_slots
        .fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |current| {
                let busy = busy_slots.load(std::sync::atomic::Ordering::Relaxed);
                (current > min_slots.saturating_add(busy)).then(|| current - 1)
            },
        )
        .is_ok()
}

/// Spawn one more parked poll slot into the supervisor's JoinSet, bumping the
/// live slot count.
fn spawn_parked_slot<F, Fut>(
    slots: &mut tokio::task::JoinSet<()>,
    ctx: &Arc<ParkedPollContext>,
    handler: F,
    next_slot_id: &mut usize,
) where
    F: Fn(RuntimeMessage, flume::Sender<ServiceMessage>) -> Fut + Send + Clone + 'static,
    Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send + 'static,
{
    ctx.total_slots
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let slot_id = *next_slot_id;
    *next_slot_id += 1;
    slots.spawn(run_parked_poll_slot(ctx.clone(), handler, slot_id));
}

/// One parked poll slot: owns exactly one outstanding PollJob request or one
/// active handler invocation. The language runtime emits `Started` through the
/// shared phase tracker before the supervisor ramps capacity, so a job stuck
/// waiting to enter the Python/Node event loop cannot cause additional claims.
/// Surplus idle slots retire after enough consecutive empty polls.
async fn run_parked_poll_slot<F, Fut>(ctx: Arc<ParkedPollContext>, handler: F, slot_id: usize)
where
    F: Fn(RuntimeMessage, flume::Sender<ServiceMessage>) -> Fut + Send + Clone + 'static,
    Fut: std::future::Future<Output = Result<Option<ServiceMessage>>> + Send + 'static,
{
    let mut client = ctx.client.clone();
    let worker_name = format!("{}-parked-{}", ctx.worker_id, slot_id);
    // Deterministic jitter so surplus slots don't all retire on the same tick.
    let retire_threshold = ctx.retire_empty_polls + (slot_id % 2);
    let mut consecutive_empty = 0usize;
    loop {
        let current_session_id = ctx.worker_session_id.lock().await.clone();
        ctx.open_poll_slots
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let poll_result = client
            .poll_job(PollJobRequest {
                worker_id: ctx.worker_id.clone(),
                worker_session_id: current_session_id.clone(),
                wait_ms: 30_000,
                claim_timeout_ms: ctx.claim_timeout_ms,
            })
            .await;
        ctx.open_poll_slots
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

        match poll_result {
            Ok(resp) => {
                let Some(job) = resp.job else {
                    consecutive_empty += 1;
                    if consecutive_empty >= retire_threshold
                        && try_retire_parked_slot(&ctx.total_slots, &ctx.busy_slots, ctx.min_slots)
                    {
                        debug!(
                            "Parked poll slot {} retiring after {} empty polls",
                            slot_id, consecutive_empty
                        );
                        let _ = ctx.events_tx.send(ParkedSlotEvent::Retired);
                        return;
                    }
                    continue;
                };
                consecutive_empty = 0;
                // Busy-count guard (same RAII semantics as the in-flight guard):
                // ramp decisions must see accurate demand even if the handler
                // panics or is cancelled.
                let _busy = InFlightGuard::enter(&ctx.busy_slots);
                let (
                    mut runtime_message,
                    is_streaming,
                    run_id,
                    lease_id,
                    completion_attempt,
                    lease_timeout_ms,
                    lease_expires_at_ms,
                    completion_metadata,
                ) = match runtime_message_from_job_assignment(job, ctx.claim_timeout_ms) {
                    Ok(assignment) => assignment,
                    Err(error) => {
                        error!(
                            "Parked poll slot {} refusing invalid assignment: {}",
                            slot_id, error
                        );
                        continue;
                    }
                };
                let _slot_phase = ctx.slot_phases.claim(run_id.clone());
                stamp_execution_authority_metadata(
                    &mut runtime_message,
                    &ctx.worker_id,
                    &current_session_id,
                    "pull",
                );
                if client
                    .negotiated_protocol_capability(crate::client::DURABLE_ACTIVATION_V1_CAPABILITY)
                {
                    if let Err(error) = stamp_activation_dispatch_metadata(
                        &mut runtime_message,
                        &ctx.worker_id,
                        &current_session_id,
                        &ctx.service_version,
                        &ctx.worker_metadata,
                        &ctx.activation_definition_configs,
                    ) {
                        error!(
                            run_id,
                            error = %error,
                            "Parked poll slot refusing assignment with mismatched artifact identity"
                        );
                        continue;
                    }
                }
                if client
                    .negotiated_protocol_capability(crate::client::DURABLE_SUSPENSION_V1_CAPABILITY)
                {
                    stamp_protocol_capability(
                        &mut runtime_message,
                        crate::client::DURABLE_SUSPENSION_V1_CAPABILITY,
                    );
                }
                let completion_run_id = run_id.clone();
                let completion_lease_id = lease_id.clone();
                if !lease_id.is_empty() {
                    if let Ok(mut map) = ctx.pending_lease_ids.lock() {
                        map.insert(run_id.clone(), lease_id.clone());
                    }
                }
                if is_streaming {
                    if let Ok(mut map) = ctx.streaming_runs.lock() {
                        map.insert(run_id.clone(), true);
                    }
                }
                if let Ok(mut revoked) = ctx.revoked_executions.lock() {
                    revoked.remove(&run_id);
                }

                let (renew_stop_tx, renew_handle) = tokio::sync::oneshot::channel::<()>();
                let renewal = spawn_active_lease_renewal(
                    client.clone(),
                    ActiveLeaseAuthority {
                        worker_id: ctx.worker_id.clone(),
                        project_id: ctx.project_id.clone(),
                        deployment_id: ctx.registration.deployment_id.clone(),
                        run_id: run_id.clone(),
                        lease_id: lease_id.clone(),
                        attempt: completion_attempt,
                        lease_timeout_ms,
                        lease_expires_at_ms,
                        session: ActiveLeaseSession::Pull(ctx.worker_session_id.clone()),
                    },
                    ctx.revoked_executions.clone(),
                    ctx.cancel_tokens.clone(),
                    ctx.cancel_hook.clone(),
                    renew_handle,
                );

                let (slot_response_tx, slot_response_rx) = flume::unbounded::<ServiceMessage>();
                let handler_future = execute_runtime_message_for_response(
                    &worker_name,
                    &ctx.worker_id,
                    runtime_message,
                    slot_response_tx.clone(),
                    handler.clone(),
                    ctx.in_flight.clone(),
                    ctx.cancel_tokens.clone(),
                    ctx.revoked_executions.clone(),
                    "pull",
                );
                tokio::pin!(handler_future);
                let mut buffered_responses = Vec::new();
                let returned_response = loop {
                    tokio::select! {
                        response = &mut handler_future => break response,
                        outgoing = slot_response_rx.recv_async() => {
                            let Ok(outgoing) = outgoing else {
                                break handler_future.await;
                            };
                            let current_session_id =
                                ctx.worker_session_id.lock().await.clone();
                            if let Some(runtime_response) = parked_runtime_service_response(
                                &mut client,
                                &outgoing,
                                &ctx.project_id,
                                &completion_run_id,
                                &ctx.worker_id,
                                &current_session_id,
                                &completion_lease_id,
                                completion_attempt,
                            )
                            .await
                            {
                                match handler.clone()(
                                    runtime_response,
                                    slot_response_tx.clone(),
                                )
                                .await
                                {
                                    Ok(Some(response)) => buffered_responses.push(response),
                                    Ok(None) => {}
                                    Err(error) => {
                                        error!(
                                            "Parked poll slot {} failed to deliver unary runtime response: {}",
                                            slot_id, error
                                        );
                                    }
                                }
                            } else {
                                buffered_responses.push(outgoing);
                            }
                        }
                    }
                };
                drop(slot_response_tx);
                while let Ok(service_message) = slot_response_rx.try_recv() {
                    buffered_responses.push(service_message);
                }

                // The language handler has returned. Any remaining slot time is
                // terminal event flushing, CompleteJob acknowledgement, or lease
                // cleanup rather than user-code execution.
                ctx.slot_phases.mark_terminalizing(&run_id);

                let mut completed = false;
                if let Some(service_message) = returned_response
                    .filter(|_| !execution_is_revoked(&ctx.revoked_executions, &run_id))
                {
                    completed = complete_or_forward_parked_response(
                        &mut client,
                        service_message,
                        &completion_run_id,
                        &completion_lease_id,
                        completion_attempt,
                        &completion_metadata,
                        &ctx.worker_id,
                        &ctx.worker_session_id,
                        &ctx.project_id,
                        slot_id,
                        &ctx.response_tx,
                        &ctx.journal_queue,
                        &ctx.journal_flush_locks,
                    )
                    .await;
                }
                for service_message in buffered_responses {
                    if execution_is_revoked(&ctx.revoked_executions, &run_id) {
                        warn!(
                            "Parked poll slot {} suppressing response after lease authority loss for run_id={}",
                            slot_id, completion_run_id
                        );
                        continue;
                    }
                    if completed
                        && polled_job_completion_from_service_message(
                            &service_message,
                            &completion_run_id,
                            &completion_lease_id,
                            completion_attempt,
                            &completion_metadata,
                        )
                        .is_some()
                    {
                        debug!(
                            "Parked poll slot {} dropping duplicate completion for job_id={}",
                            slot_id, completion_run_id
                        );
                        continue;
                    }
                    completed = complete_or_forward_parked_response(
                        &mut client,
                        service_message,
                        &completion_run_id,
                        &completion_lease_id,
                        completion_attempt,
                        &completion_metadata,
                        &ctx.worker_id,
                        &ctx.worker_session_id,
                        &ctx.project_id,
                        slot_id,
                        &ctx.response_tx,
                        &ctx.journal_queue,
                        &ctx.journal_flush_locks,
                    )
                    .await
                        || completed;
                }

                if completed || execution_is_revoked(&ctx.revoked_executions, &run_id) {
                    if let Ok(mut map) = ctx.pending_lease_ids.lock() {
                        map.remove(&completion_run_id);
                    }
                    if let Ok(mut map) = ctx.streaming_runs.lock() {
                        map.remove(&completion_run_id);
                    }
                }

                let _ = renew_stop_tx.send(());
                if let Some(handle) = renewal {
                    let _ = handle.await;
                }
            }
            Err(e) if is_worker_session_inactive_error(&e) => {
                consecutive_empty = 0;
                warn!("Parked poll slot {} error: {}", slot_id, e);
                if !refresh_parked_worker_session(
                    &mut client,
                    &ctx.worker_session_id,
                    &current_session_id,
                    &ctx.registration,
                    slot_id,
                    &ctx.session_refresh_lock,
                )
                .await
                {
                    exit_parked_worker_process(
                        "RegisterWorkerSession retry was rejected after 3 attempts; exiting worker process",
                    );
                }
            }
            Err(e) => {
                consecutive_empty = 0;
                warn!("Parked poll slot {} error: {}", slot_id, e);
                tokio::time::sleep(Duration::from_millis(1_000)).await;
            }
        }
    }
}

fn spawn_active_lease_renewal(
    mut client: WorkerCoordinatorClient,
    authority: ActiveLeaseAuthority,
    revoked_executions: Arc<std::sync::Mutex<HashSet<String>>>,
    cancel_tokens: Arc<std::sync::Mutex<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
    cancel_hook: Arc<std::sync::Mutex<Option<Box<dyn Fn(String) + Send + Sync>>>>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
) -> Option<tokio::task::JoinHandle<()>> {
    if authority.lease_id.is_empty() {
        return None;
    }

    Some(tokio::spawn(async move {
        let mut lease_expires_at_ms = if authority.lease_expires_at_ms > 0 {
            authority.lease_expires_at_ms
        } else {
            current_time_ms().saturating_add(authority.lease_timeout_ms)
        };
        let mut delay_ms = active_lease_renew_interval_with_jitter_ms(authority.lease_timeout_ms);
        loop {
            let now_ms = current_time_ms();
            if now_ms >= lease_expires_at_ms {
                crate::telemetry::record_lease_renewal(authority.mode_label(), "expired");
                warn!(
                    "Execution lease expired before renewal was confirmed: run_id={} lease_id={}",
                    authority.run_id, authority.lease_id
                );
                revoke_execution_authority(
                    &authority.run_id,
                    &revoked_executions,
                    &cancel_tokens,
                    &cancel_hook,
                );
                return;
            }
            let remaining_ms = lease_expires_at_ms.saturating_sub(now_ms) as u64;
            let sleep_ms = delay_ms.min(remaining_ms.max(1));
            tokio::select! {
                _ = &mut stop_rx => {
                    return;
                }
                _ = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {}
            }

            if current_time_ms() >= lease_expires_at_ms {
                crate::telemetry::record_lease_renewal(authority.mode_label(), "danger_expired");
                warn!(
                    "Execution entered the lease danger boundary without a confirmed renewal: run_id={} lease_id={}",
                    authority.run_id, authority.lease_id
                );
                revoke_execution_authority(
                    &authority.run_id,
                    &revoked_executions,
                    &cancel_tokens,
                    &cancel_hook,
                );
                return;
            }

            match client
                .renew_job_lease(authority.renewal_request().await)
                .await
            {
                Ok(resp) if resp.renewed => {
                    crate::telemetry::record_lease_renewal(authority.mode_label(), "renewed");
                    debug!(
                        "Renewed execution lease run_id={} lease_id={} expires_at_ms={}",
                        authority.run_id, authority.lease_id, resp.lease_expires_at_ms
                    );
                    lease_expires_at_ms = resp.lease_expires_at_ms;
                    delay_ms =
                        active_lease_renew_interval_with_jitter_ms(authority.lease_timeout_ms);
                }
                Ok(resp) => {
                    let outcome = LeaseRenewalOutcome::try_from(resp.outcome)
                        .unwrap_or(LeaseRenewalOutcome::Unspecified);
                    warn!(
                        "Execution lease authority was revoked: run_id={} lease_id={} outcome={}",
                        authority.run_id,
                        authority.lease_id,
                        outcome.as_str_name(),
                    );
                    crate::telemetry::record_lease_renewal(
                        authority.mode_label(),
                        match outcome {
                            LeaseRenewalOutcome::AuthorityLost => "authority_lost",
                            LeaseRenewalOutcome::Terminal => "terminal",
                            LeaseRenewalOutcome::SessionInactive => "session_inactive",
                            LeaseRenewalOutcome::Unspecified | LeaseRenewalOutcome::Renewed => {
                                "rejected"
                            }
                        },
                    );
                    revoke_execution_authority(
                        &authority.run_id,
                        &revoked_executions,
                        &cancel_tokens,
                        &cancel_hook,
                    );
                    return;
                }
                Err(e) => {
                    crate::telemetry::record_lease_renewal(authority.mode_label(), "indeterminate");
                    warn!(
                        "Execution lease renewal is indeterminate; retrying inside the known lease window: run_id={} lease_id={} error={}",
                        authority.run_id, authority.lease_id, e
                    );
                    delay_ms = active_lease_danger_retry_ms(authority.lease_timeout_ms);
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
            revoked_executions: Arc::new(std::sync::Mutex::new(HashSet::new())),
            lease_renewal_stops: Arc::new(std::sync::Mutex::new(HashMap::new())),
            cancel_tokens: Arc::new(std::sync::Mutex::new(HashMap::new())),
            cancel_hook: Arc::new(std::sync::Mutex::new(None)),
            event_stream_tx: Arc::new(std::sync::Mutex::new(None)),
            dispatch_tx: Arc::new(std::sync::Mutex::new(None)),
            engine_client: Arc::new(TokioMutex::new(None)),
            journal_flush_locks: RunFlushLocks::default(),
            slot_phases: WorkerSlotPhases::default(),
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
        if event.event_type == "run.started" {
            self.mark_execution_started(&event.run_id);
        }
        self.journal_queue.push(event).map_err(|e| {
            crate::error::SdkError::Internal(format!("Failed to queue journal event: {}", e))
        })?;
        Ok(())
    }

    /// Signal that a claimed pull job has reached the language runtime.
    /// Language bindings call this at `run.started` before checkpoint I/O.
    pub fn mark_execution_started(&self, run_id: &str) {
        let run_id = run_id.split(':').next().unwrap_or(run_id);
        self.slot_phases.mark_started(run_id);
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

    fn start_push_lease_renewal(
        &self,
        client: WorkerCoordinatorClient,
        request: &DispatchComponentRequest,
    ) {
        if request.lease_id.is_empty() {
            return;
        }
        let run_id = request
            .invocation_id
            .split(':')
            .next()
            .unwrap_or(&request.invocation_id)
            .to_string();
        let Ok(attempt) = u32::try_from(request.attempt) else {
            warn!(
                "Refusing push lease renewal with negative attempt: run_id={} attempt={}",
                run_id, request.attempt
            );
            revoke_execution_authority(
                &run_id,
                &self.revoked_executions,
                &self.cancel_tokens,
                &self.cancel_hook,
            );
            return;
        };
        let project_id = request
            .metadata
            .get("project_id")
            .cloned()
            .or_else(|| canonical_project_id_from_metadata(&self.metadata))
            .unwrap_or_default();
        let deployment_id = if request.deployment_id.is_empty() {
            request
                .metadata
                .get("deployment_id")
                .cloned()
                .or_else(|| self.metadata.get("deployment_id").cloned())
                .unwrap_or_default()
        } else {
            request.deployment_id.clone()
        };
        if project_id.is_empty() || deployment_id.is_empty() {
            warn!(
                "Push lease has incomplete routing authority; cancelling execution: run_id={}",
                run_id
            );
            revoke_execution_authority(
                &run_id,
                &self.revoked_executions,
                &self.cancel_tokens,
                &self.cancel_hook,
            );
            return;
        }
        let lease_timeout_ms = request
            .metadata
            .get("lease_timeout_ms")
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(120_000);
        let lease_expires_at_ms = request
            .metadata
            .get("lease_expires_at_ms")
            .and_then(|value| value.parse::<i64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or_else(|| current_time_ms().saturating_add(lease_timeout_ms));

        if let Ok(mut revoked) = self.revoked_executions.lock() {
            revoked.remove(&run_id);
        }
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        if let Ok(mut stops) = self.lease_renewal_stops.lock() {
            if let Some(previous) = stops.insert(run_id.clone(), stop_tx) {
                let _ = previous.send(());
            }
        }
        let _ = spawn_active_lease_renewal(
            client,
            ActiveLeaseAuthority {
                worker_id: self.config.worker_id.clone(),
                project_id,
                deployment_id,
                run_id,
                lease_id: request.lease_id.clone(),
                attempt,
                lease_timeout_ms,
                lease_expires_at_ms,
                session: ActiveLeaseSession::Push,
            },
            self.revoked_executions.clone(),
            self.cancel_tokens.clone(),
            self.cancel_hook.clone(),
            stop_rx,
        );
    }

    fn stop_execution_lease_renewal(&self, run_id: &str) {
        if let Some(stop) = self
            .lease_renewal_stops
            .lock()
            .ok()
            .and_then(|mut stops| stops.remove(run_id))
        {
            let _ = stop.send(());
        }
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
        self.stop_execution_lease_renewal(run_id);
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
        let authority_run_id = run_id.split(':').next().unwrap_or(&run_id);
        if execution_is_revoked(&self.revoked_executions, authority_run_id) {
            return Err(SdkError::Internal(format!(
                "execution authority was revoked for run_id={authority_run_id}"
            )));
        }
        if event_type == "run.started" {
            self.mark_execution_started(&run_id);
        }
        let is_terminal = event_type == "run.completed" || event_type == "run.failed";
        let is_durable_checkpoint = JournalEventMessage::is_checkpoint_event_type(&event_type);
        // A durable checkpoint is an ordering boundary for every transient
        // event queued before it, not just for run.completed/run.failed.
        // Holding the same per-run barrier as the periodic flush task prevents
        // a drained, in-flight batch from being overtaken by the checkpoint.
        let _journal_flush_guard = if is_durable_checkpoint {
            Some(self.journal_flush_locks.lock_run(&run_id).await)
        } else {
            None
        };

        // ── Engine path: when AGNT5_ENGINE_URL is set, route directly to engine ──
        if let Some(mut engine) = self.ensure_engine_client().await? {
            // Publish pending transient events through EventStream and persist
            // queued durable boundaries before appending this checkpoint.
            // Both calls are acknowledged while this run's flush barrier is held.
            if is_durable_checkpoint {
                let pending = self.journal_queue.drain_run_events(&run_id);
                if !pending.is_empty() {
                    let tenant_id = canonical_project_id_from_metadata(&metadata)
                        .or_else(|| canonical_project_id_from_metadata(&self.metadata))
                        .unwrap_or_default();
                    let transient: Vec<_> = pending
                        .iter()
                        .filter(|event| event.is_sse_only)
                        .map(|event| EventStreamMessage {
                            run_id: event.run_id.clone(),
                            event_type: event.event_type.clone(),
                            data: event.data.clone(),
                            trace_id: event.correlation_id.clone(),
                            span_id: event.parent_correlation_id.clone(),
                            project_id: canonical_project_id_from_metadata(&event.metadata)
                                .unwrap_or_else(|| tenant_id.clone()),
                            source_timestamp_ns: event.source_timestamp_ns,
                            worker_id: self.config.worker_id.clone(),
                        })
                        .collect();
                    let durable_records: Vec<_> = pending
                        .iter()
                        .filter(|event| !event.is_sse_only)
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
                    if let Err(error) = engine.stream_events(transient).await {
                        for event in pending.into_iter().rev() {
                            self.journal_queue.push_front(event).ok();
                        }
                        self.journal_queue.record_error();
                        let mut guard = self.engine_client.lock().await;
                        *guard = None;
                        return Err(error);
                    }
                    if !durable_records.is_empty() {
                        if let Err(error) = engine.append_batch(durable_records).await {
                            for event in
                                pending.into_iter().rev().filter(|event| !event.is_sse_only)
                            {
                                self.journal_queue.push_front(event).ok();
                            }
                            self.journal_queue.record_error();
                            let mut guard = self.engine_client.lock().await;
                            *guard = None;
                            return Err(error);
                        }
                    }
                    debug!(
                        "Engine: flushed {} events before {} for run_id={}",
                        pending.len(),
                        event_type,
                        run_id
                    );
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

            let start = Instant::now();
            let result = match await_checkpoint_ack(
                engine.append(record),
                timeout_ms,
                "Engine.Append",
                &run_id,
                &event_type,
                sequence_number,
            )
            .await
            {
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
                Err(error) => {
                    warn!("{error}");
                    Err(error)
                }
            };

            let duration_secs = start.elapsed().as_secs_f64();
            crate::telemetry::record_checkpoint(
                &event_type,
                duration_secs,
                result.is_ok(),
                experiment_id.as_deref(),
            );

            if is_terminal && result.is_ok() {
                self.cleanup_run_tracking(&run_id);
            }

            return result;
        }

        // ── Legacy EE path (AGNT5_ENGINE_URL not set) ──

        // Before sending a durable checkpoint, flush any pending SSE-only
        // events (logs, deltas) for this run.
        // Route through EventStream (EE) which is the single SSE publisher.
        // Falls back to dispatch stream (WC) only if EventStream is unavailable.
        if is_durable_checkpoint {
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

                        // look up stashed lease_id for this invocation so
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

        let start = Instant::now();
        let result = match await_checkpoint_ack(
            ee_client.write_checkpoint(request),
            timeout_ms,
            "ExecutionEngine.WriteCheckpoint",
            &run_id,
            &event_type,
            sequence_number,
        )
        .await
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
            Err(error) => {
                warn!("{error}");
                Err(error)
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

        if is_terminal && result.is_ok() {
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

        for (run_id, event_type, ..) in &events {
            if event_type == "run.started" {
                self.mark_execution_started(run_id);
            }
        }

        let mut run_ids: Vec<_> = events.iter().map(|event| event.0.clone()).collect();
        run_ids.sort_unstable();
        run_ids.dedup();
        let _journal_flush_guards = self
            .journal_flush_locks
            .lock_runs(run_ids.iter().cloned())
            .await;

        if let Some(mut engine) = self.ensure_engine_client().await? {
            // The batch is an ordering boundary just like a single acknowledged
            // checkpoint. Flush anything already queued for these runs before
            // appending the batch so an earlier stream frame cannot be overtaken.
            for run_id in &run_ids {
                let pending = self.journal_queue.drain_run_events(run_id);
                if pending.is_empty() {
                    continue;
                }

                let tenant_id = pending
                    .iter()
                    .find_map(|event| canonical_project_id_from_metadata(&event.metadata))
                    .or_else(|| canonical_project_id_from_metadata(&self.metadata))
                    .unwrap_or_default();
                let transient: Vec<_> = pending
                    .iter()
                    .filter(|event| event.is_sse_only)
                    .map(|event| EventStreamMessage {
                        run_id: event.run_id.clone(),
                        event_type: event.event_type.clone(),
                        data: event.data.clone(),
                        trace_id: event.correlation_id.clone(),
                        span_id: event.parent_correlation_id.clone(),
                        project_id: canonical_project_id_from_metadata(&event.metadata)
                            .unwrap_or_else(|| tenant_id.clone()),
                        source_timestamp_ns: event.source_timestamp_ns,
                        worker_id: self.config.worker_id.clone(),
                    })
                    .collect();
                let durable_records: Vec<_> = pending
                    .iter()
                    .filter(|event| !event.is_sse_only)
                    .map(|event| {
                        client::build_engine_record(
                            tenant_id.clone(),
                            event.run_id.clone(),
                            event.event_type.clone(),
                            event.data.clone(),
                            event.source_timestamp_ns,
                            String::new(),
                            event.correlation_id.clone(),
                            event.parent_correlation_id.clone(),
                            event.metadata.clone(),
                        )
                    })
                    .collect();

                if let Err(error) = engine.stream_events(transient).await {
                    for event in pending.into_iter().rev() {
                        self.journal_queue.push_front(event).ok();
                    }
                    self.journal_queue.record_error();
                    let mut guard = self.engine_client.lock().await;
                    *guard = None;
                    return Err(error);
                }
                if !durable_records.is_empty() {
                    if let Err(error) = engine.append_batch(durable_records).await {
                        for event in pending.into_iter().rev().filter(|event| !event.is_sse_only) {
                            self.journal_queue.push_front(event).ok();
                        }
                        self.journal_queue.record_error();
                        let mut guard = self.engine_client.lock().await;
                        *guard = None;
                        return Err(error);
                    }
                }
            }

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
            match append_records_by_run(&mut engine, &records).await {
                Ok(_) => {
                    debug!("Engine batch checkpoint: {} events persisted", count);
                }
                Err((e, committed, _written)) => {
                    warn!(
                        "Engine batch checkpoint failed for {} non-terminal events; queued for retry: {}",
                        count, e
                    );
                    {
                        let mut guard = self.engine_client.lock().await;
                        *guard = None;
                    }
                    for event in uncommitted_records_in_reverse(originals, &committed) {
                        self.journal_queue.push_front(event).ok();
                    }
                    self.journal_queue.record_error();
                    return Err(e);
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
        if let Some(artifact) = configured_activation_artifact_sha256(&metadata) {
            metadata.insert("activation_artifact_sha256".to_string(), artifact);
        }

        // declare data-path mode. Default PUSH;
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
        // stamp deployment_id from env so the coordinator's
        // proto-field path picks it up. Falls back to metadata key on
        // older coordinators that haven't been rebuilt yet.
        let deployment_id = std::env::var("AGNT5_DEPLOYMENT_ID").unwrap_or_default();

        // declare concurrency budget so the coordinator can
        // size headroom reservations per priority class. Resolved from
        // config (set by a language binding or seeded from the
        // `AGNT5_MAX_CONCURRENCY` env var in `WorkerConfig::new`), default
        // 100. Drives both the local pool size and the registration field.
        let max_concurrency: u32 = self.config.max_concurrency.unwrap_or(100);

        let capabilities = worker_capabilities(&self.components);
        let (supported_protocol_capabilities, required_protocol_capabilities) =
            worker_protocol_capabilities_for_metadata(&metadata)?;
        let dispatch_worker_metadata = metadata.clone();
        let dispatch_activation_definition_configs =
            activation_definition_configs(&self.components);
        let registration = RegisterService {
            service_name: self.config.service_name.clone(),
            service_version: self.config.service_version.clone(),
            service_type: self.config.service_type.clone(),
            components: self.components.clone(),
            metadata,
            mode,
            deployment_id,
            max_concurrency,
            capabilities,
            supported_protocol_capabilities: supported_protocol_capabilities.clone(),
            required_protocol_capabilities: required_protocol_capabilities.clone(),
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
            let response_worker_id = self.config.worker_id.clone();
            let in_flight = in_flight.clone();
            let cancel_tokens = self.cancel_tokens.clone();
            let revoked_executions = self.revoked_executions.clone();

            let handle = tokio::spawn(async move {
                while let Ok(runtime_message) = task_rx.recv_async().await {
                    execute_runtime_message(
                        &worker_name,
                        &response_worker_id,
                        runtime_message,
                        response_tx.clone(),
                        handler.clone(),
                        in_flight.clone(),
                        cancel_tokens.clone(),
                        revoked_executions.clone(),
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
                supported_protocol_capabilities.clone(),
                required_protocol_capabilities.clone(),
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
                        Ok(mut runtime_message) => {
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

                            if client.negotiated_protocol_capability(
                                crate::client::DURABLE_ACTIVATION_V1_CAPABILITY,
                            ) {
                                if let Err(error) = stamp_activation_dispatch_metadata(
                                    &mut runtime_message,
                                    &self.config.worker_id,
                                    &self.config.worker_id,
                                    &self.config.service_version,
                                    &dispatch_worker_metadata,
                                    &dispatch_activation_definition_configs,
                                ) {
                                    break Err(error);
                                }
                            }
                            if client.negotiated_protocol_capability(
                                crate::client::DURABLE_SUSPENSION_V1_CAPABILITY,
                            ) {
                                stamp_protocol_capability(
                                    &mut runtime_message,
                                    crate::client::DURABLE_SUSPENSION_V1_CAPABILITY,
                                );
                            }

                            stamp_execution_authority_metadata(
                                &mut runtime_message,
                                &self.config.worker_id,
                                &self.config.worker_id,
                                "push",
                            );

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
                                    self.start_push_lease_renewal(client.clone(), req);
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
                                    // stash lease_id keyed by invocation_id so we
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
                                            // drain stashed lease_id so the fast path
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
        if let Ok(mut stops) = self.lease_renewal_stops.lock() {
            for (_, stop) in stops.drain() {
                let _ = stop.send(());
            }
        }
        if let Ok(mut revoked) = self.revoked_executions.lock() {
            revoked.clear();
        }

        dispatch_result
    }

    async fn forward_worker_response(
        &self,
        mut service_message: ServiceMessage,
        is_pull_mode: bool,
        tx: &flume::Sender<ServiceMessage>,
    ) -> Result<()> {
        // stamp the stashed lease_id onto the response so the
        // coordinator's fencing check passes. On terminal events we drain
        // the map entry; on intermediate streaming events we leave it so the
        // terminal ack still finds it. Also clean up streaming_runs tracking
        // for terminal events.
        if let Some(crate::pb::service_message::MessageType::FunctionResponse(ref mut resp)) =
            service_message.message_type
        {
            let run_id = resp
                .invocation_id
                .split(':')
                .next()
                .unwrap_or(&resp.invocation_id)
                .to_string();
            if execution_is_revoked(&self.revoked_executions, &run_id) {
                warn!(
                    "Suppressing worker response after lease authority loss: run_id={}",
                    run_id
                );
                self.cleanup_run_tracking(&resp.invocation_id);
                return Ok(());
            }
            let is_terminal = is_terminal_worker_response(&resp.event_type);
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
                self.cleanup_run_tracking(&resp.invocation_id);
            }
        }

        // route by declared worker mode, not by per-response metadata
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
        let revoked_executions_outer = self.revoked_executions.clone();
        let journal_flush_locks_outer = self.journal_flush_locks.clone();
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
                let revoked_executions = revoked_executions_outer.clone();
                let journal_flush_locks = journal_flush_locks_outer.clone();
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
                        let run_ids = journal_queue.peek_batch_run_ids(drain_limit);
                        let _flush_guards = journal_flush_locks.lock_runs(run_ids.clone()).await;
                        let protected_runs: HashSet<_> = run_ids.into_iter().collect();
                        let batch: Vec<_> = journal_queue
                            .drain_batch_for_runs(drain_limit, &protected_runs)
                            .into_iter()
                            .filter(|event| {
                                let run_id =
                                    event.run_id.split(':').next().unwrap_or(&event.run_id);
                                !execution_is_revoked(&revoked_executions, run_id)
                            })
                            .collect();
                        if batch.is_empty() {
                            continue;
                        }

                        // ── Engine path: transient stream + durable AppendBatch ──
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

                            let mut transient_events = Vec::new();
                            let mut durable_originals = Vec::new();
                            for event in batch {
                                if event.is_sse_only {
                                    let is_run_streaming = match streaming_runs.lock() {
                                        Ok(map) => map.get(&event.run_id).copied().unwrap_or(false),
                                        Err(poisoned) => poisoned
                                            .into_inner()
                                            .get(&event.run_id)
                                            .copied()
                                            .unwrap_or(false),
                                    };
                                    if is_run_streaming {
                                        transient_events.push(EventStreamMessage {
                                            run_id: event.run_id.clone(),
                                            event_type: event.event_type.clone(),
                                            data: event.data.clone(),
                                            trace_id: event.correlation_id.clone(),
                                            span_id: event.parent_correlation_id.clone(),
                                            project_id: canonical_project_id_from_metadata(
                                                &event.metadata,
                                            )
                                            .or_else(|| event.tenant_id.clone())
                                            .unwrap_or_else(|| cached_project_id.clone()),
                                            source_timestamp_ns: event.source_timestamp_ns,
                                            worker_id: worker_id.clone(),
                                        });
                                    }
                                } else {
                                    durable_originals.push(event);
                                }
                            }

                            let records: Vec<_> = durable_originals
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
                                let streamed = match eng.stream_events(transient_events).await {
                                    Ok(count) => count as usize,
                                    Err(e) => {
                                        warn!("Flush task: Engine EventStream failed: {}", e);
                                        journal_queue.record_error();
                                        0
                                    }
                                };
                                if records.is_empty() {
                                    if streamed > 0 {
                                        journal_queue.record_sent_batch(streamed, streamed);
                                    }
                                    continue;
                                }
                                match append_records_by_run(eng, &records).await {
                                    Ok(written) => {
                                        journal_queue.record_sent_batch(
                                            written as usize + streamed,
                                            streamed,
                                        );
                                        debug!(
                                            "Flush task: published {} transient and wrote {} durable events to Engine (queue_size={})",
                                            streamed, written,
                                            journal_queue.len()
                                        );
                                    }
                                    Err((e, committed, written)) => {
                                        warn!("Flush task: Engine AppendBatch failed: {}", e);
                                        engine = None; // Clear for reconnection
                                        for event in uncommitted_records_in_reverse(
                                            durable_originals,
                                            &committed,
                                        ) {
                                            journal_queue.push_front(event).ok();
                                        }
                                        if written > 0 || streamed > 0 {
                                            journal_queue.record_sent_batch(
                                                written as usize + streamed,
                                                streamed,
                                            );
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
                                // stamp stashed lease_id on SSE-only fallback responses.
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
    /// PollJob request or one active handler invocation. A supervisor ramps the
    /// slot count toward `max_slots` while jobs are arriving and surplus idle
    /// slots retire back down to `min_slots` after consecutive empty polls.
    fn spawn_parked_poll_task<F, Fut>(
        &self,
        response_tx: flume::Sender<ServiceMessage>,
        message_handler: F,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
        max_concurrency: usize,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        supported_protocol_capabilities: Vec<String>,
        required_protocol_capabilities: Vec<String>,
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
        let activation_definition_configs = activation_definition_configs(&components);
        let mut worker_metadata = self.metadata.clone();
        if let Some(artifact) = configured_activation_artifact_sha256(&worker_metadata) {
            worker_metadata.insert("activation_artifact_sha256".to_string(), artifact);
        }
        let service_name = self.config.service_name.clone();
        let service_version = self.config.service_version.clone();
        let service_type = self.config.service_type.clone();
        let streaming_runs = self.streaming_runs.clone();
        let pending_lease_ids = self.pending_lease_ids.clone();
        let journal_queue = self.journal_queue.clone();
        let journal_flush_locks = self.journal_flush_locks.clone();
        let slot_phases = self.slot_phases.clone();
        let cancel_tokens = self.cancel_tokens.clone();
        let cancel_hook = self.cancel_hook.clone();
        let revoked_executions = self.revoked_executions.clone();
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
        let retire_empty_polls = env_usize("AGNT5_SLOT_RETIRE_EMPTY_POLLS")
            .filter(|v| *v > 0)
            .unwrap_or(2);

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
                service_version: service_version.clone(),
                service_type,
                supported_protocol_capabilities,
                required_protocol_capabilities,
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
            let total_slots = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let busy_slots = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let capacity_reporter = spawn_parked_capacity_reporter(
                client.clone(),
                worker_id.clone(),
                worker_session_id.clone(),
                open_poll_slots.clone(),
                busy_slots.clone(),
                total_slots.clone(),
                max_slots,
                shutdown_rx.resubscribe(),
            );

            let (events_tx, mut events_rx) =
                tokio::sync::mpsc::unbounded_channel::<ParkedSlotEvent>();
            slot_phases.set_started_notifier(events_tx.clone());
            let ctx = Arc::new(ParkedPollContext {
                client,
                worker_id,
                worker_session_id,
                registration,
                service_version,
                worker_metadata,
                activation_definition_configs,
                project_id,
                response_tx,
                in_flight,
                cancel_tokens,
                cancel_hook,
                revoked_executions,
                streaming_runs,
                pending_lease_ids,
                journal_queue,
                journal_flush_locks,
                slot_phases,
                open_poll_slots,
                total_slots: total_slots.clone(),
                busy_slots: busy_slots.clone(),
                events_tx,
                session_refresh_lock: Arc::new(TokioMutex::new(())),
                claim_timeout_ms,
                min_slots,
                retire_empty_polls,
            });

            let mut slots = tokio::task::JoinSet::new();
            let mut next_slot_id = 0usize;
            for _ in 0..min_slots {
                spawn_parked_slot(&mut slots, &ctx, message_handler.clone(), &mut next_slot_id);
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
                    // `ctx` holds an `events_tx` clone, so recv() never yields None here.
                    event = events_rx.recv() => {
                        if let Some(ParkedSlotEvent::Started { active_started }) = event {
                            let spawn = parked_ramp_spawn_count(
                                total_slots.load(std::sync::atomic::Ordering::Relaxed),
                                active_started,
                                max_slots,
                            );
                            for _ in 0..spawn {
                                spawn_parked_slot(
                                    &mut slots,
                                    &ctx,
                                    message_handler.clone(),
                                    &mut next_slot_id,
                                );
                            }
                            if spawn > 0 {
                                debug!(
                                    "Parked poll ramp: spawned {} slot(s) (total={})",
                                    spawn,
                                    total_slots.load(std::sync::atomic::Ordering::Relaxed)
                                );
                            }
                        }
                    }
                    result = slots.join_next() => {
                        match result {
                            // Clean exit is self-retirement; the slot already
                            // decremented total_slots.
                            Some(Ok(())) => {}
                            Some(Err(e)) => {
                                warn!("Parked poll slot exited abnormally: {}", e);
                                let _ = total_slots.fetch_update(
                                    std::sync::atomic::Ordering::Relaxed,
                                    std::sync::atomic::Ordering::Relaxed,
                                    |current| current.checked_sub(1),
                                );
                            }
                            // JoinSet drained unexpectedly; reconcile the count and
                            // rebuild the floor below instead of exiting.
                            None => total_slots.store(0, std::sync::atomic::Ordering::Relaxed),
                        }
                        while total_slots.load(std::sync::atomic::Ordering::Relaxed) < min_slots {
                            spawn_parked_slot(
                                &mut slots,
                                &ctx,
                                message_handler.clone(),
                                &mut next_slot_id,
                            );
                        }
                    }
                }
            }
        })
    }
    /// Reject the obsolete pull-response path, which has no parked slot
    /// assignment and therefore cannot supply the required session + attempt
    /// fence. Modern pull execution completes inside
    /// `complete_or_forward_parked_response`.
    async fn handle_polled_job_response(&self, service_message: ServiceMessage) {
        let invocation_id = match &service_message.message_type {
            Some(crate::pb::service_message::MessageType::FunctionResponse(response)) => {
                response.invocation_id.as_str()
            }
            _ => "",
        };
        error!(
            invocation_id,
            "Dropping unfenced legacy pull completion; parked PollJob completion is required"
        );
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

fn is_terminal_worker_response(event_type: &str) -> bool {
    matches!(
        event_type,
        "run.completed" | "run.failed" | "run.paused" | "workflow.paused"
    )
}

fn is_cancelled_worker_response(service_message: &ServiceMessage) -> bool {
    matches!(
        &service_message.message_type,
        Some(crate::pb::service_message::MessageType::FunctionResponse(resp))
            if resp.event_type == "run.cancelled"
    )
}

#[cfg(test)]
mod tests {
    use super::{
        activation_definition_configs, active_lease_danger_retry_ms,
        active_lease_renew_interval_ms, active_lease_renew_interval_with_jitter_ms,
        await_checkpoint_ack, canonical_activation_component_config,
        complete_job_request_from_polled_completion, complete_job_with_retry,
        deployment_artifact_sha256, durable_suspension_service_message, execution_is_revoked,
        is_cancelled_worker_response, is_parked_worker_session_registration_rejection,
        is_terminal_worker_response, is_worker_session_inactive_error, parked_ramp_spawn_count,
        parked_runtime_service_response, parked_worker_session_was_refreshed,
        polled_job_completion_from_service_message, polled_job_suspension_request,
        record_groups_by_run, runtime_message_from_job_assignment,
        stamp_activation_dispatch_metadata, stamp_dispatch_mode,
        stamp_execution_authority_metadata, stamp_protocol_capability, take_correlation_ids,
        try_retire_parked_slot, uncommitted_records_in_reverse, valid_activation_artifact_sha256,
        wait_for_parked_run_events_flush, worker_capabilities, ActiveLeaseAuthority,
        ActiveLeaseSession, AppendGroupProgress, CompleteJobSender, EntityStateSender,
        ParkedSlotEvent, ParkedWorkerSessionRegistration, RunFlushLocks, Worker, WorkerConfig,
        WorkerSlotPhaseSnapshot, WorkerSlotPhases,
    };
    use crate::error::{ErrorCode, SdkError};
    use crate::journal_queue::{JournalEventMessage, JournalEventQueue, JournalQueueConfig};
    use crate::pb::{
        dispatch_component_response, runtime_message, runtime_service_request,
        runtime_service_response, service_message, CompleteJobRequest, CompleteJobResponse,
        DispatchComponentResponse, EntityStateSaveRequest, GetEntityStateRequest,
        GetEntityStateResponse, JobAssignment, PutEntityStateRequest, PutEntityStateResponse,
        RuntimeServiceRequest, ServiceMessage, WorkerMode,
    };
    use std::collections::{HashMap, VecDeque};
    use std::time::Duration;

    struct ScriptedCompleteJobSender {
        outcomes: VecDeque<crate::error::Result<CompleteJobResponse>>,
        requests: Vec<CompleteJobRequest>,
    }

    #[derive(Default)]
    struct RecordingEntityStateSender {
        get_requests: Vec<GetEntityStateRequest>,
        put_requests: Vec<PutEntityStateRequest>,
    }

    #[async_trait::async_trait]
    impl EntityStateSender for RecordingEntityStateSender {
        async fn send_get_entity_state(
            &mut self,
            request: GetEntityStateRequest,
        ) -> crate::error::Result<GetEntityStateResponse> {
            self.get_requests.push(request);
            Ok(GetEntityStateResponse {
                found: false,
                state_json: Vec::new(),
                version: 0,
            })
        }

        async fn send_put_entity_state(
            &mut self,
            request: PutEntityStateRequest,
        ) -> crate::error::Result<PutEntityStateResponse> {
            self.put_requests.push(request);
            Ok(PutEntityStateResponse { new_version: 4 })
        }
    }

    #[async_trait::async_trait]
    impl CompleteJobSender for ScriptedCompleteJobSender {
        async fn send_complete_job(
            &mut self,
            request: CompleteJobRequest,
        ) -> crate::error::Result<CompleteJobResponse> {
            self.requests.push(request);
            self.outcomes
                .pop_front()
                .unwrap_or_else(|| Err(SdkError::Internal("missing scripted outcome".to_string())))
        }
    }

    #[tokio::test]
    async fn checkpoint_ack_timeout_fails_closed_with_unknown_outcome() {
        let error = await_checkpoint_ack(
            std::future::pending::<()>(),
            0,
            "Engine.Append",
            "run-1",
            "workflow.step.completed",
            7,
        )
        .await
        .expect_err("checkpoint acknowledgement timeout must fail closed");

        match error {
            SdkError::Timeout {
                message,
                operation,
                duration_ms,
            } => {
                assert_eq!(operation, "Engine.Append");
                assert_eq!(duration_ms, Some(0));
                assert!(message.contains("run_id=run-1"));
                assert!(message.contains("event_type=workflow.step.completed"));
                assert!(message.contains("seq=7"));
                assert!(message.contains("persistence outcome is unknown"));
            }
            other => panic!("expected typed timeout error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn checkpoint_ack_timeout_wrapper_preserves_completed_and_inner_error_outcomes() {
        let result = await_checkpoint_ack(
            async { Ok::<_, SdkError>(42) },
            100,
            "ExecutionEngine.WriteCheckpoint",
            "run-1",
            "run.completed",
            8,
        )
        .await
        .expect("completed future should not time out");

        assert_eq!(result.expect("inner result should be unchanged"), 42);

        let result = await_checkpoint_ack(
            async { Err::<(), _>(SdkError::Internal("append rejected".to_string())) },
            100,
            "Engine.Append",
            "run-1",
            "run.completed",
            9,
        )
        .await
        .expect("completed future should not time out");

        assert!(matches!(
            result,
            Err(SdkError::Internal(message)) if message == "append rejected"
        ));
    }

    #[tokio::test]
    async fn active_push_lease_request_carries_complete_authority_tuple() {
        let authority = ActiveLeaseAuthority {
            worker_id: "worker-1".to_string(),
            project_id: "project-1".to_string(),
            deployment_id: "deployment-1".to_string(),
            run_id: "run-1".to_string(),
            lease_id: "lease-1".to_string(),
            attempt: 3,
            lease_timeout_ms: 120_000,
            lease_expires_at_ms: 200_000,
            session: ActiveLeaseSession::Push,
        };

        let request = authority.renewal_request().await;

        assert_eq!(request.worker_id, "worker-1");
        assert!(request.worker_session_id.is_empty());
        assert_eq!(request.project_id, "project-1");
        assert_eq!(request.deployment_id, "deployment-1");
        assert_eq!(request.run_id, "run-1");
        assert_eq!(request.lease_id, "lease-1");
        assert_eq!(request.attempt, Some(3));
        assert_eq!(request.mode, WorkerMode::Push as i32);
    }

    #[tokio::test]
    async fn revoked_execution_suppresses_late_push_response() {
        let config = WorkerConfig::new(
            "svc".to_string(),
            "1.0.0".to_string(),
            "standalone".to_string(),
        );
        let worker = Worker::new(config, Vec::new(), HashMap::new());
        worker
            .revoked_executions
            .lock()
            .unwrap()
            .insert("run-1".to_string());
        let (tx, rx) = flume::bounded(1);
        let message = ServiceMessage {
            worker_id: "worker-1".to_string(),
            metadata: HashMap::new(),
            message_type: Some(service_message::MessageType::FunctionResponse(
                DispatchComponentResponse {
                    invocation_id: "run-1".to_string(),
                    success: true,
                    event_type: "run.completed".to_string(),
                    lease_id: "lease-stale".to_string(),
                    ..Default::default()
                },
            )),
        };

        worker
            .forward_worker_response(message, false, &tx)
            .await
            .unwrap();

        assert!(rx.try_recv().is_err());
        assert!(execution_is_revoked(&worker.revoked_executions, "run-1"));
    }

    #[test]
    fn authority_revocation_invokes_language_cancel_hook() {
        let config = WorkerConfig::new(
            "svc".to_string(),
            "1.0.0".to_string(),
            "standalone".to_string(),
        );
        let worker = Worker::new(config, Vec::new(), HashMap::new());
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled_by_hook = cancelled.clone();
        worker.set_cancel_hook(move |run_id| {
            assert_eq!(run_id, "run-1");
            cancelled_by_hook.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        super::revoke_execution_authority(
            "run-1",
            &worker.revoked_executions,
            &worker.cancel_tokens,
            &worker.cancel_hook,
        );

        assert!(cancelled.load(std::sync::atomic::Ordering::SeqCst));
        assert!(execution_is_revoked(&worker.revoked_executions, "run-1"));
    }

    #[test]
    fn paused_worker_responses_are_terminal() {
        for event_type in ["run.paused", "workflow.paused"] {
            assert!(is_terminal_worker_response(event_type));
        }
        assert!(!is_terminal_worker_response("run.cancelled"));
        assert!(!is_terminal_worker_response("output.delta"));
    }

    #[test]
    fn cancelled_worker_response_is_consumed_without_completion() {
        let message = ServiceMessage {
            message_type: Some(crate::pb::service_message::MessageType::FunctionResponse(
                DispatchComponentResponse {
                    event_type: "run.cancelled".to_string(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        assert!(is_cancelled_worker_response(&message));
        assert!(polled_job_completion_from_service_message(
            &message,
            "run-1",
            "lease-1",
            1,
            &HashMap::new(),
        )
        .is_none());
    }

    #[test]
    fn push_dispatch_mode_overrides_caller_metadata() {
        let mut message = crate::pb::RuntimeMessage {
            message_data: Some(runtime_message::MessageData::DispatchComponent(
                crate::pb::DispatchComponentRequest {
                    metadata: HashMap::from([("dispatch_mode".to_string(), "pull".to_string())]),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        stamp_dispatch_mode(&mut message, "push");

        let Some(runtime_message::MessageData::DispatchComponent(request)) = message.message_data
        else {
            panic!("dispatch request");
        };
        assert_eq!(
            request.metadata.get("dispatch_mode").map(String::as_str),
            Some("push")
        );
    }

    #[test]
    fn execution_authority_metadata_overrides_caller_values() {
        let mut message = crate::pb::RuntimeMessage {
            message_data: Some(runtime_message::MessageData::DispatchComponent(
                crate::pb::DispatchComponentRequest {
                    lease_id: "lease-7".to_string(),
                    attempt: 7,
                    metadata: HashMap::from([
                        ("dispatch_mode".to_string(), "push".to_string()),
                        ("worker_id".to_string(), "forged-worker".to_string()),
                        ("lease_id".to_string(), "forged-lease".to_string()),
                        ("lease_attempt".to_string(), "99".to_string()),
                    ]),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        stamp_execution_authority_metadata(&mut message, "worker-1", "session-1", "pull");

        let Some(runtime_message::MessageData::DispatchComponent(request)) = message.message_data
        else {
            panic!("dispatch request");
        };
        assert_eq!(
            request.metadata.get("dispatch_mode").map(String::as_str),
            Some("pull")
        );
        assert_eq!(
            request.metadata.get("worker_id").map(String::as_str),
            Some("worker-1")
        );
        assert_eq!(
            request
                .metadata
                .get("worker_session_id")
                .map(String::as_str),
            Some("session-1")
        );
        assert_eq!(
            request.metadata.get("lease_id").map(String::as_str),
            Some("lease-7")
        );
        assert_eq!(
            request.metadata.get("lease_attempt").map(String::as_str),
            Some("7")
        );
    }

    #[test]
    fn activation_component_config_is_canonical_and_sorted() {
        assert_eq!(
            canonical_activation_component_config(&HashMap::from([
                ("z".to_string(), "last".to_string()),
                ("a".to_string(), "first\nline".to_string()),
            ])),
            r#"["object",[["a",["string","first\nline"]],["z",["string","last"]]]]"#
        );
    }

    #[test]
    fn activation_artifact_identity_accepts_exactly_32_encoded_bytes() {
        assert!(valid_activation_artifact_sha256(
            "6161616161616161616161616161616161616161616161616161616161616161"
        ));
        assert!(valid_activation_artifact_sha256(
            "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE="
        ));
        assert!(!valid_activation_artifact_sha256("6161"));
        assert!(!valid_activation_artifact_sha256("not-a-digest"));
    }

    #[test]
    fn deployment_artifact_identity_is_domain_separated_and_canonical() {
        assert_eq!(
            hex::encode(
                deployment_artifact_sha256("01234567-89AB-CDEF-0123-456789ABCDEF").unwrap()
            ),
            "c51344c186e74ccb1ce5b5f6122362285d9dd3a4b125d442280de1dcacae8c9f"
        );
        assert!(deployment_artifact_sha256("not-a-deployment-id").is_none());
    }

    #[test]
    fn negotiated_activation_metadata_uses_typed_dispatch_authority() {
        let mut message = crate::pb::RuntimeMessage {
            message_data: Some(runtime_message::MessageData::DispatchComponent(
                crate::pb::DispatchComponentRequest {
                    invocation_id: "run-1".to_string(),
                    component_name: "workflow".to_string(),
                    lease_id: "lease-1".to_string(),
                    metadata: HashMap::from([
                        (
                            "run_authority".to_string(),
                            "runtime-run-authority".to_string(),
                        ),
                        (
                            "lease_authority".to_string(),
                            "runtime-lease-authority".to_string(),
                        ),
                        (
                            "activation_artifact_sha256".to_string(),
                            "c51344c186e74ccb1ce5b5f6122362285d9dd3a4b125d442280de1dcacae8c9f"
                                .to_string(),
                        ),
                    ]),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let components = vec![crate::pb::ComponentInfo {
            name: "workflow".to_string(),
            config: HashMap::from([("model".to_string(), "gpt-5".to_string())]),
            ..Default::default()
        }];

        stamp_activation_dispatch_metadata(
            &mut message,
            "worker-1",
            "session-1",
            "1.2.3",
            &HashMap::from([
                ("project_id".to_string(), "project-1".to_string()),
                (
                    "activation_artifact_sha256".to_string(),
                    "c51344c186e74ccb1ce5b5f6122362285d9dd3a4b125d442280de1dcacae8c9f".to_string(),
                ),
            ]),
            &activation_definition_configs(&components),
        )
        .unwrap();

        let Some(runtime_message::MessageData::DispatchComponent(request)) = message.message_data
        else {
            panic!("dispatch request");
        };
        assert_eq!(
            request
                .metadata
                .get(crate::client::DURABLE_ACTIVATION_V1_CAPABILITY)
                .map(String::as_str),
            Some("true")
        );
        assert_eq!(
            request
                .metadata
                .get("worker_session_id")
                .map(String::as_str),
            Some("session-1")
        );
        assert_eq!(
            request.metadata.get("lease_id").map(String::as_str),
            Some("lease-1")
        );
        assert_eq!(
            request.metadata.get("run_authority").map(String::as_str),
            Some("runtime-run-authority")
        );
        assert_eq!(
            request.metadata.get("lease_authority").map(String::as_str),
            Some("runtime-lease-authority")
        );
        assert_eq!(
            request
                .metadata
                .get("activation_definition_config")
                .map(String::as_str),
            Some(r#"["object",[["model",["string","gpt-5"]]]]"#)
        );
        assert_eq!(
            request
                .metadata
                .get("activation_artifact_sha256")
                .map(String::as_str),
            Some("c51344c186e74ccb1ce5b5f6122362285d9dd3a4b125d442280de1dcacae8c9f")
        );
    }

    #[test]
    fn negotiated_activation_rejects_worker_artifact_mismatch() {
        let mut message = crate::pb::RuntimeMessage {
            message_data: Some(runtime_message::MessageData::DispatchComponent(
                crate::pb::DispatchComponentRequest {
                    invocation_id: "run-1".to_string(),
                    component_name: "workflow".to_string(),
                    lease_id: "lease-1".to_string(),
                    metadata: HashMap::from([(
                        "activation_artifact_sha256".to_string(),
                        "00".repeat(32),
                    )]),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        let error = stamp_activation_dispatch_metadata(
            &mut message,
            "worker-1",
            "session-1",
            "1.2.3",
            &HashMap::from([("activation_artifact_sha256".to_string(), "11".repeat(32))]),
            &HashMap::new(),
        )
        .expect_err("mismatched worker code must not execute a pinned run");
        assert!(matches!(
            error,
            SdkError::Activation {
                code: ErrorCode::NondeterministicReplay,
                ..
            }
        ));
    }

    #[test]
    fn negotiated_suspension_capability_is_visible_to_language_handlers() {
        let mut message = crate::pb::RuntimeMessage {
            message_data: Some(runtime_message::MessageData::DispatchComponent(
                crate::pb::DispatchComponentRequest::default(),
            )),
            ..Default::default()
        };
        stamp_protocol_capability(
            &mut message,
            crate::client::DURABLE_SUSPENSION_V1_CAPABILITY,
        );
        let Some(runtime_message::MessageData::DispatchComponent(request)) = message.message_data
        else {
            panic!("dispatch request");
        };
        assert_eq!(
            request
                .metadata
                .get(crate::client::DURABLE_SUSPENSION_V1_CAPABILITY)
                .map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn durable_engine_records_are_grouped_by_run_without_reordering_each_run() {
        let records = vec![
            crate::pb::Record {
                run_id: "run-a".into(),
                event_type: "a-1".into(),
                ..Default::default()
            },
            crate::pb::Record {
                run_id: "run-b".into(),
                event_type: "b-1".into(),
                ..Default::default()
            },
            crate::pb::Record {
                run_id: "run-a".into(),
                event_type: "a-2".into(),
                ..Default::default()
            },
        ];

        assert_eq!(record_groups_by_run(&records), vec![vec![0, 2], vec![1]]);
    }

    #[test]
    fn durable_engine_partial_group_failure_requeues_only_unacknowledged_records() {
        let mut progress = AppendGroupProgress::new(4);
        progress.acknowledge(&[0, 2], 2);
        let (_error, committed, written) =
            progress.failure(SdkError::Internal("run-b failed".to_string()));

        assert_eq!(committed, vec![true, false, true, false]);
        assert_eq!(written, 2);
        assert_eq!(
            uncommitted_records_in_reverse(vec![0, 1, 2, 3], &committed),
            vec![3, 1]
        );
    }

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
                ("dispatch_mode".to_string(), "push".to_string()),
                ("lease_id".to_string(), "forged-lease".to_string()),
                ("lease_expires_at_ms".to_string(), "999999999".to_string()),
                ("lease_timeout_ms".to_string(), "1".to_string()),
                ("max_attempts".to_string(), "5".to_string()),
            ]),
            attempt: 2,
            timeout_ms: 0,
            trace_id: "trace-1".to_string(),
            lease_id: "lease-1".to_string(),
            lease_expires_at_ms: 123_456,
        };

        let (
            message,
            is_streaming,
            run_id,
            lease_id,
            attempt,
            renewal_timeout_ms,
            lease_expires_at_ms,
            completion_metadata,
        ) = runtime_message_from_job_assignment(job, 60_000).expect("valid typed assignment");

        assert!(is_streaming);
        assert_eq!(run_id, "run-1");
        assert_eq!(lease_id, "lease-1");
        assert_eq!(attempt, 2);
        assert_eq!(renewal_timeout_ms, 60_000);
        assert_eq!(lease_expires_at_ms, 123_456);
        assert_eq!(
            completion_metadata.get("attempt").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            completion_metadata.get("max_attempts").map(String::as_str),
            Some("5")
        );
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
                assert_eq!(
                    req.metadata.get("dispatch_mode").map(String::as_str),
                    Some("pull")
                );
            }
            other => panic!("expected dispatch component, got {other:?}"),
        }
    }

    #[test]
    fn negative_polled_job_attempt_is_rejected_before_execution() {
        let error = runtime_message_from_job_assignment(
            JobAssignment {
                job_id: "run-negative-attempt".to_string(),
                run_id: "run-negative-attempt".to_string(),
                lease_id: "lease-negative-attempt".to_string(),
                attempt: -1,
                ..Default::default()
            },
            60_000,
        )
        .expect_err("negative attempt must fail closed");

        assert!(error.to_string().contains("negative attempt -1"));
    }

    #[test]
    fn assignment_does_not_fall_back_to_forged_lease_metadata() {
        let error = runtime_message_from_job_assignment(
            JobAssignment {
                job_id: "run-forged-lease".to_string(),
                run_id: "run-forged-lease".to_string(),
                lease_id: String::new(),
                lease_expires_at_ms: 0,
                metadata: HashMap::from([
                    ("lease_id".to_string(), "forged-lease".to_string()),
                    ("lease_expires_at_ms".to_string(), "999999999".to_string()),
                ]),
                ..Default::default()
            },
            60_000,
        )
        .expect_err("typed lease is required before executing user code");

        assert!(error.to_string().contains("no typed lease_id"));
    }

    #[test]
    fn parked_completion_preserves_assignment_attempt_and_lease() {
        let service_message = ServiceMessage {
            message_type: Some(crate::pb::service_message::MessageType::FunctionResponse(
                DispatchComponentResponse {
                    invocation_id: "forged-run:component-0".to_string(),
                    success: true,
                    result: Some(dispatch_component_response::Result::OutputData(
                        br#"{"ok":true}"#.to_vec(),
                    )),
                    metadata: HashMap::from([
                        ("attempt".to_string(), "999".to_string()),
                        ("max_attempts".to_string(), "999".to_string()),
                        ("initial_interval_ms".to_string(), "999".to_string()),
                        ("max_interval_ms".to_string(), "999".to_string()),
                        ("backoff_type".to_string(), "forged".to_string()),
                        ("backoff_multiplier".to_string(), "999".to_string()),
                        ("pause_index".to_string(), "2".to_string()),
                        (
                            "step_events".to_string(),
                            r#"{"0":"Ada","1":"blue"}"#.to_string(),
                        ),
                    ]),
                    event_type: "workflow.paused".to_string(),
                    lease_id: "forged-lease".to_string(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        let completion = polled_job_completion_from_service_message(
            &service_message,
            "run-1",
            "lease-7",
            7,
            &HashMap::from([
                ("attempt".to_string(), "7".to_string()),
                ("max_attempts".to_string(), "5".to_string()),
                ("initial_interval_ms".to_string(), "100".to_string()),
                ("max_interval_ms".to_string(), "1000".to_string()),
                ("backoff_type".to_string(), "exponential".to_string()),
                ("backoff_multiplier".to_string(), "2".to_string()),
            ]),
        )
        .expect("function response should become a fenced completion");

        assert_eq!(completion.job_id, "run-1");
        assert_eq!(completion.lease_id, "lease-7");
        assert_eq!(completion.attempt, 7);
        assert_eq!(completion.event_type, "workflow.paused");
        assert_eq!(
            completion.metadata.get("attempt").map(String::as_str),
            Some("7")
        );
        assert_eq!(
            completion.metadata.get("max_attempts").map(String::as_str),
            Some("5")
        );
        assert_eq!(
            completion
                .metadata
                .get("initial_interval_ms")
                .map(String::as_str),
            Some("100")
        );
        assert_eq!(
            completion
                .metadata
                .get("max_interval_ms")
                .map(String::as_str),
            Some("1000")
        );
        assert_eq!(
            completion.metadata.get("backoff_type").map(String::as_str),
            Some("exponential")
        );
        assert_eq!(
            completion
                .metadata
                .get("backoff_multiplier")
                .map(String::as_str),
            Some("2")
        );
        assert_eq!(
            completion.metadata.get("pause_index").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            completion.metadata.get("step_events").map(String::as_str),
            Some(r#"{"0":"Ada","1":"blue"}"#)
        );
    }

    #[test]
    fn parked_completion_drops_response_retry_policy_missing_from_assignment() {
        let service_message = ServiceMessage {
            message_type: Some(crate::pb::service_message::MessageType::FunctionResponse(
                DispatchComponentResponse {
                    success: false,
                    metadata: HashMap::from([
                        ("attempt".to_string(), "999".to_string()),
                        ("max_attempts".to_string(), "999".to_string()),
                        ("initial_interval_ms".to_string(), "999".to_string()),
                        ("max_interval_ms".to_string(), "999".to_string()),
                        ("backoff_type".to_string(), "forged".to_string()),
                        ("backoff_multiplier".to_string(), "999".to_string()),
                    ]),
                    event_type: "run.failed".to_string(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        let completion = polled_job_completion_from_service_message(
            &service_message,
            "run-1",
            "lease-7",
            7,
            &HashMap::from([("attempt".to_string(), "7".to_string())]),
        )
        .expect("terminal response should become a completion");

        assert_eq!(
            completion.metadata,
            HashMap::from([("attempt".to_string(), "7".to_string())])
        );
    }

    #[test]
    fn parked_completion_builds_authoritative_complete_job_request() {
        let completion = super::PolledJobCompletion {
            job_id: "run-1".to_string(),
            success: false,
            output_data: Vec::new(),
            error_message: "failed".to_string(),
            error_code: "TEST_FAILURE".to_string(),
            event_type: "run.failed".to_string(),
            metadata: HashMap::from([
                ("attempt".to_string(), "7".to_string()),
                ("max_attempts".to_string(), "5".to_string()),
                ("initial_interval_ms".to_string(), "100".to_string()),
                ("max_interval_ms".to_string(), "1000".to_string()),
                ("backoff_type".to_string(), "exponential".to_string()),
                ("backoff_multiplier".to_string(), "2".to_string()),
                (
                    "completion_event_type".to_string(),
                    "forged.event".to_string(),
                ),
            ]),
            lease_id: "lease-7".to_string(),
            attempt: 7,
        };

        let request = complete_job_request_from_polled_completion(
            "worker-1",
            "session-1",
            "project-1",
            completion,
        );

        assert_eq!(request.job_id, "run-1");
        assert_eq!(request.worker_id, "worker-1");
        assert_eq!(request.worker_session_id, "session-1");
        assert_eq!(request.project_id, "project-1");
        assert_eq!(request.lease_id, "lease-7");
        assert_eq!(request.attempt, Some(7));
        assert_eq!(
            request
                .metadata
                .get("completion_event_type")
                .map(String::as_str),
            Some("run.failed")
        );
        for (key, expected) in [
            ("attempt", "7"),
            ("max_attempts", "5"),
            ("initial_interval_ms", "100"),
            ("max_interval_ms", "1000"),
            ("backoff_type", "exponential"),
            ("backoff_multiplier", "2"),
        ] {
            assert_eq!(
                request.metadata.get(key).map(String::as_str),
                Some(expected),
                "unexpected CompleteJob metadata for {key}"
            );
        }
    }

    #[test]
    fn parked_suspension_uses_assignment_owned_project_and_run_scope() {
        let suspension = crate::pb::WorkerSuspension {
            activation_id: "activation-1".into(),
            attempt: 2,
            fence_token: b"fence-2".to_vec(),
            timer_key: "sleep:backoff".into(),
            ready_at_ms: 0,
            input_digest: vec![1; 32],
            definition_digest: vec![2; 32],
            continuation: br#"{"step":2}"#.to_vec(),
            delay_ms: 5_000,
        };
        let response = ServiceMessage {
            worker_id: "worker-1".into(),
            metadata: HashMap::new(),
            message_type: Some(service_message::MessageType::FunctionResponse(
                DispatchComponentResponse {
                    invocation_id: "worker-supplied-run".into(),
                    success: true,
                    result: Some(dispatch_component_response::Result::WorkerSuspension(
                        suspension.clone(),
                    )),
                    ..Default::default()
                },
            )),
        };

        let request = polled_job_suspension_request(&response, "project-1", "run-1")
            .expect("typed suspension");
        assert_eq!(request.project_id, "project-1");
        assert_eq!(request.run_id, "run-1");
        assert_eq!(request.activation_id, suspension.activation_id);
        assert_eq!(request.delay_ms, suspension.delay_ms);
        assert_eq!(request.ready_at_ms, 0);
    }

    #[test]
    fn core_timer_error_maps_to_typed_worker_response() {
        let suspension = crate::pb::WorkerSuspension {
            activation_id: "activation-1".into(),
            attempt: 2,
            fence_token: b"fence-2".to_vec(),
            timer_key: "sleep:sleep_0".into(),
            delay_ms: 5_000,
            ..Default::default()
        };
        let envelope = super::DurableSuspensionEnvelope {
            invocation_id: "run-1".into(),
            metadata: HashMap::from([("project_id".into(), "project-1".into())]),
            attempt: 2,
            lease_id: "lease-2".into(),
        };

        let message = durable_suspension_service_message("worker-1", &envelope, suspension.clone());
        assert_eq!(message.worker_id, "worker-1");
        let Some(service_message::MessageType::FunctionResponse(response)) = message.message_type
        else {
            panic!("expected function response");
        };
        assert!(response.success);
        assert_eq!(response.invocation_id, "run-1");
        assert_eq!(response.event_type, "workflow.paused");
        assert_eq!(response.attempt, 2);
        assert_eq!(response.lease_id, "lease-2");
        assert_eq!(
            response.result,
            Some(dispatch_component_response::Result::WorkerSuspension(
                suspension
            ))
        );
    }

    #[test]
    fn parked_nonterminal_response_does_not_complete_assignment() {
        let service_message = ServiceMessage {
            message_type: Some(crate::pb::service_message::MessageType::FunctionResponse(
                DispatchComponentResponse {
                    invocation_id: "run-1".to_string(),
                    success: true,
                    event_type: "output.delta".to_string(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        assert!(polled_job_completion_from_service_message(
            &service_message,
            "run-1",
            "lease-7",
            7,
            &HashMap::new(),
        )
        .is_none());
    }

    #[tokio::test]
    async fn parked_entity_state_save_becomes_unary_fenced_engine_request() {
        let service_message = ServiceMessage {
            message_type: Some(service_message::MessageType::RuntimeService(
                RuntimeServiceRequest {
                    request_id: "state-request-1".to_string(),
                    session_id: String::new(),
                    operation: Some(runtime_service_request::Operation::EntityStateSave(
                        EntityStateSaveRequest {
                            entity_type: "WorkflowEntity".to_string(),
                            entity_key: "ks_sequential".to_string(),
                            state_json: br#"{"total_steps":2}"#.to_vec(),
                            expected_version: 3,
                            scope: "run".to_string(),
                            scope_id: "run-1".to_string(),
                        },
                    )),
                },
            )),
            ..Default::default()
        };
        let mut sender = RecordingEntityStateSender::default();

        let runtime_response = parked_runtime_service_response(
            &mut sender,
            &service_message,
            "project-1",
            "run-1",
            "worker-1",
            "session-1",
            "lease-1",
            7,
        )
        .await
        .expect("entity request should be handled inside the parked slot");

        assert_eq!(sender.put_requests.len(), 1);
        let request = &sender.put_requests[0];
        assert_eq!(request.project_id, "project-1");
        assert_eq!(request.run_id, "run-1");
        assert_eq!(request.worker_id, "worker-1");
        assert_eq!(request.worker_session_id, "session-1");
        assert_eq!(request.lease_id, "lease-1");
        assert_eq!(request.attempt, Some(7));
        assert_eq!(request.operation_id, "state-request-1");
        assert_eq!(request.expected_version, 3);

        let runtime_message::MessageData::RuntimeServiceResponse(response) =
            runtime_response.message_data.expect("runtime response")
        else {
            panic!("expected RuntimeServiceResponse");
        };
        assert!(response.success);
        assert_eq!(response.request_id, "state-request-1");
        match response.result {
            Some(runtime_service_response::Result::EntityStateSave(result)) => {
                assert_eq!(result.new_version, 4);
            }
            _ => panic!("expected entity-state save result"),
        }
    }

    #[tokio::test]
    async fn parked_completion_retries_until_runtime_acknowledges() {
        let mut sender = ScriptedCompleteJobSender {
            outcomes: VecDeque::from([
                Err(SdkError::Connection {
                    message: "temporary CompleteJob failure".to_string(),
                    code: ErrorCode::ConnectionFailed,
                    source: None,
                }),
                Ok(CompleteJobResponse {
                    acknowledged: false,
                }),
                Ok(CompleteJobResponse { acknowledged: true }),
            ]),
            requests: Vec::new(),
        };
        let request = CompleteJobRequest {
            job_id: "run-1".to_string(),
            lease_id: "lease-7".to_string(),
            attempt: Some(7),
            ..Default::default()
        };

        complete_job_with_retry(
            &mut sender,
            request,
            3,
            std::time::Duration::from_millis(50),
            std::time::Duration::ZERO,
        )
        .await
        .expect("third acknowledged response should succeed");

        assert_eq!(sender.requests.len(), 3);
        assert!(sender
            .requests
            .iter()
            .all(|request| request.lease_id == "lease-7" && request.attempt == Some(7)));
    }

    #[tokio::test]
    async fn parked_completion_waits_for_run_events_to_finish_flushing() {
        let queue = JournalEventQueue::new(JournalQueueConfig {
            flush_interval_ms: 1,
            ..Default::default()
        });
        queue
            .push(JournalEventMessage {
                run_id: "run-1".to_string(),
                event_type: "function.completed".to_string(),
                ..Default::default()
            })
            .unwrap();
        let flush_locks = RunFlushLocks::default();
        let queue_for_sender = queue.clone();
        let locks_for_sender = flush_locks.clone();
        let sender = tokio::spawn(async move {
            tokio::task::yield_now().await;
            let _guard = locks_for_sender.lock_run("run-1").await;
            let drained = queue_for_sender.drain_run_events("run-1");
            assert_eq!(drained.len(), 1);
            // Model an acknowledged send while the periodic flush lock remains
            // held. The completion barrier must not return during this window.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        });

        assert!(wait_for_parked_run_events_flush(&queue, &flush_locks, "run-1").await);
        sender.await.unwrap();
        assert!(!queue.contains_run("run-1"));
    }

    #[tokio::test]
    async fn run_flush_locks_serialize_same_run_without_blocking_other_runs() {
        let locks = RunFlushLocks::default();
        let run_a_guard = locks.lock_run("run-a").await;

        let same_run = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            locks.lock_run("run-a"),
        )
        .await;
        assert!(
            same_run.is_err(),
            "same run must wait for its ordering barrier"
        );

        let other_run = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            locks.lock_run("run-b"),
        )
        .await;
        assert!(
            other_run.is_ok(),
            "unrelated run must not share the barrier"
        );

        drop(run_a_guard);
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            locks.lock_run("run-a"),
        )
        .await
        .is_ok());
    }

    #[test]
    fn active_lease_renew_intervals_are_bounded() {
        assert_eq!(active_lease_renew_interval_ms(120_000), 60_000);
        assert_eq!(active_lease_danger_retry_ms(120_000), 5_000);
        assert_eq!(active_lease_renew_interval_ms(2_000), 1_000);
        assert_eq!(active_lease_danger_retry_ms(2_000), 200);
        assert_eq!(active_lease_renew_interval_ms(6), 3);
        assert_eq!(active_lease_danger_retry_ms(6), 1);

        for _ in 0..100 {
            let jittered = active_lease_renew_interval_with_jitter_ms(120_000);
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
    fn parked_ramp_spawn_count_doubles_demand_within_max_slots() {
        // One busy slot out of one total: spawn one more (1 -> 2).
        assert_eq!(parked_ramp_spawn_count(1, 1, 100), 1);
        // Doubling wave: 4 busy of 4 total spawns 4 more.
        assert_eq!(parked_ramp_spawn_count(4, 4, 100), 4);
        // Already-parked slots absorb demand: no spawn while total >= 2 * busy.
        assert_eq!(parked_ramp_spawn_count(4, 1, 100), 0);
        assert_eq!(parked_ramp_spawn_count(4, 2, 100), 0);
        // Spawn only the shortfall toward 2 * busy.
        assert_eq!(parked_ramp_spawn_count(4, 3, 100), 2);
        // Headroom caps the wave.
        assert_eq!(parked_ramp_spawn_count(8, 8, 10), 2);
        // Saturated fleet spawns nothing.
        assert_eq!(parked_ramp_spawn_count(10, 10, 10), 0);
        assert_eq!(parked_ramp_spawn_count(12, 10, 10), 0);
        // Idle fleet spawns nothing.
        assert_eq!(parked_ramp_spawn_count(4, 0, 10), 0);
    }

    #[tokio::test]
    async fn stalled_claim_does_not_signal_slot_ramp_before_language_start() {
        let phases = WorkerSlotPhases::default();
        let (events_tx, mut events_rx) = tokio::sync::mpsc::unbounded_channel();
        phases.set_started_notifier(events_tx);

        // Model a deliberately stalled language scheduler: four pull jobs are
        // claimed, but none has emitted run.started. The supervisor must
        // receive no ramp signal in this state.
        let guards: Vec<_> = (0..4)
            .map(|index| phases.claim(format!("slow-run-{index}")))
            .collect();
        assert_eq!(
            phases.snapshot(),
            WorkerSlotPhaseSnapshot {
                claimed_not_started: 4,
                executing: 0,
                terminalizing: 0,
            }
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), events_rx.recv())
                .await
                .is_err()
        );

        phases.mark_started("slow-run-0");
        let event = events_rx.recv().await;
        assert_eq!(event, Some(ParkedSlotEvent::Started { active_started: 1 }));
        let Some(ParkedSlotEvent::Started { active_started }) = event else {
            unreachable!();
        };
        assert_eq!(parked_ramp_spawn_count(4, active_started, 16), 0);
        assert_eq!(
            phases.snapshot(),
            WorkerSlotPhaseSnapshot {
                claimed_not_started: 3,
                executing: 1,
                terminalizing: 0,
            }
        );

        // Duplicate run.started events are idempotent and cannot cause extra
        // ramp waves.
        phases.mark_started("slow-run-0");
        assert!(events_rx.try_recv().is_err());

        phases.mark_terminalizing("slow-run-0");
        assert_eq!(
            phases.snapshot(),
            WorkerSlotPhaseSnapshot {
                claimed_not_started: 3,
                executing: 0,
                terminalizing: 1,
            }
        );
        drop(guards);
        assert_eq!(phases.snapshot(), WorkerSlotPhaseSnapshot::default());
    }

    #[test]
    fn try_retire_parked_slot_never_drops_below_min_slots() {
        let total = std::sync::atomic::AtomicUsize::new(3);
        let busy = std::sync::atomic::AtomicUsize::new(0);
        assert!(try_retire_parked_slot(&total, &busy, 1));
        assert!(try_retire_parked_slot(&total, &busy, 1));
        assert_eq!(total.load(std::sync::atomic::Ordering::Relaxed), 1);
        // At the floor: retirement is refused and the count is unchanged.
        assert!(!try_retire_parked_slot(&total, &busy, 1));
        assert_eq!(total.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn try_retire_parked_slot_keeps_min_idle_pollers_while_busy() {
        // 1 busy + 2 idle, min_slots = 1: idle may shrink only to the floor, so
        // exactly one idle slot retires and the other stays polling — a busy
        // handler must never leave zero outstanding PollJob requests.
        let total = std::sync::atomic::AtomicUsize::new(3);
        let busy = std::sync::atomic::AtomicUsize::new(1);
        assert!(try_retire_parked_slot(&total, &busy, 1)); // 3 -> 2 (idle 2 -> 1)
        assert_eq!(total.load(std::sync::atomic::Ordering::Relaxed), 2);
        // idle == min_slots now (total 2 - busy 1 == 1): further retirement refused.
        assert!(!try_retire_parked_slot(&total, &busy, 1));
        assert_eq!(total.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    #[test]
    fn try_retire_parked_slot_respects_floor_under_concurrent_callers() {
        let total = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(8));
        let busy = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let min_slots = 2;
        let retired: usize = (0..8)
            .map(|_| {
                let total = total.clone();
                let busy = busy.clone();
                std::thread::spawn(move || try_retire_parked_slot(&total, &busy, min_slots))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|retired| *retired)
            .count();
        assert_eq!(retired, 6);
        assert_eq!(total.load(std::sync::atomic::Ordering::Relaxed), min_slots);
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
            supported_protocol_capabilities: vec![
                crate::client::DURABLE_ACTIVATION_V1_CAPABILITY.into()
            ],
            required_protocol_capabilities: Vec::new(),
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
    fn worker_capabilities_include_declared_components_and_local_builtin_scorers() {
        let capabilities = worker_capabilities(&[crate::pb::ComponentInfo {
            component_type: crate::pb::ComponentType::Function as i32,
            name: "do_work".into(),
            ..Default::default()
        }]);

        assert!(capabilities.iter().any(|cap| {
            cap.component_type == crate::pb::ComponentType::Function as i32
                && cap.component_name == "do_work"
        }));
        for scorer in [
            "json_valid",
            "step_efficiency",
            "plan_quality",
            "plan_adherence",
        ] {
            assert!(
                capabilities.iter().any(|cap| {
                    cap.component_type == crate::pb::ComponentType::Scorer as i32
                        && cap.component_name == scorer
                }),
                "missing local built-in scorer capability {scorer}"
            );
        }
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
