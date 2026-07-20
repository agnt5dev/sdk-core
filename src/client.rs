use crate::error::{Result, SdkError};
use crate::pb::{
    engine_service_client::EngineServiceClient,
    execution_engine_service_client::ExecutionEngineServiceClient,
    worker_coordinator_service_client::WorkerCoordinatorServiceClient, AppendBatchRequest,
    AppendRequest, CheckpointRequest, CheckpointType, CompleteJobRequest, CompleteJobResponse,
    DurableStepCheckpoint, EventStreamMessage, FindByStepKeyRequest, PollJobRequest,
    PollJobResponse, Record, RegisterService, RegisterWorkerSessionRequest,
    RegisterWorkerSessionResponse, RenewJobLeaseRequest, RenewJobLeaseResponse,
    ReportWorkerCapacityRequest, ReportWorkerCapacityResponse, RuntimeMessage, ServiceMessage,
};
use std::collections::HashMap;
use std::time::Duration;
use tonic::transport::Channel;
use tonic::Code;
use tracing::{debug, error};

/// Simple client for communicating with the Worker Coordinator service.
///
/// Holds two gRPC clients multiplexed over the same `tonic::Channel`:
/// - `client`: WorkerCoordinatorService (worker registration, dispatch streaming)
/// - `engine_client`: EngineService (durable execution: checkpoint, event stream,
///   parked job polling/complete, memoization lookup via find_by_step_key)
///
/// The durable execution RPCs used to live on WorkerCoordinatorService and moved
/// to EngineService as part of the journal-owner consolidation. Both clients
/// share one HTTP/2 connection since `tonic::Channel` is cheap to clone and
/// multiplexes streams.
#[derive(Debug, Clone)]
pub struct WorkerCoordinatorClient {
    client: WorkerCoordinatorServiceClient<Channel>,
    engine_client: EngineServiceClient<Channel>,
}

const WORKER_COORDINATOR_RPC_TIMEOUT: Duration = Duration::from_secs(45);
const PARKED_POLL_CLIENT_GRACE: Duration = Duration::from_secs(5);
const MAX_PARKED_POLL_WAIT_MS: i64 = 30_000;

fn poll_job_deadline(req: &PollJobRequest) -> Duration {
    let wait_ms = req.wait_ms.clamp(0, MAX_PARKED_POLL_WAIT_MS) as u64;
    Duration::from_millis(wait_ms).saturating_add(PARKED_POLL_CLIENT_GRACE)
}

fn is_idle_poll_timeout(status: &tonic::Status) -> bool {
    matches!(status.code(), Code::Cancelled | Code::DeadlineExceeded)
        && status.message().to_ascii_lowercase().contains("timeout")
}

impl WorkerCoordinatorClient {
    /// Create a new client connected to the Worker Coordinator
    pub async fn connect(endpoint: String) -> Result<Self> {
        debug!("Connecting to Worker Coordinator at {}", endpoint);

        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| SdkError::Connection {
                message: format!("Invalid endpoint {}: {}", endpoint, e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            })?
            .connect_timeout(Duration::from_secs(10))
            .timeout(WORKER_COORDINATOR_RPC_TIMEOUT)
            .http2_adaptive_window(true)
            .connect()
            .await
            .map_err(|e| {
                // Expected during reconnection — debug level to avoid noisy logs
                debug!("Connection to {} failed: {:?}", endpoint, e);
                e
            })?;

        let client = WorkerCoordinatorServiceClient::new(channel.clone());
        let engine_client = EngineServiceClient::new(channel);

        Ok(Self {
            client,
            engine_client,
        })
    }

    /// Create a worker stream with immediate registration (based on working pattern)
    pub async fn create_worker_stream_with_registration(
        &mut self,
        worker_id: String,
        registration: RegisterService,
    ) -> Result<(
        flume::Sender<ServiceMessage>,
        flume::Receiver<RuntimeMessage>,
    )> {
        // Create the registration message first
        let registration_message = ServiceMessage {
            worker_id: worker_id.clone(),
            metadata: HashMap::new(),
            message_type: Some(crate::pb::service_message::MessageType::RegisterService(
                registration,
            )),
        };

        // Create bounded channels for ongoing communication (reasonable default capacity)
        let (outgoing_tx, outgoing_rx) = flume::bounded::<ServiceMessage>(1000);
        let (runtime_msg_tx, runtime_msg_rx) = flume::bounded::<RuntimeMessage>(1000);

        // Create stream that yields registration immediately, then handles ongoing messages
        let outgoing_stream = async_stream::stream! {
            // First, yield the registration message immediately
            yield registration_message;

            // Then, handle ongoing messages from the channel
            loop {
                match outgoing_rx.recv_async().await {
                    Ok(msg) => {
                        yield msg;
                    },
                    Err(_) => {
                        break;
                    }
                }
            }
        };

        // Expose the worker ID as gRPC metadata so L7 proxies can route
        // reconnects consistently before reading the protobuf stream body.
        let worker_id_header = tonic::metadata::MetadataValue::try_from(worker_id.as_str())
            .map_err(|e| SdkError::Connection {
                message: format!("Invalid worker_id for routing metadata: {}", e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            })?;
        let mut request = tonic::Request::new(outgoing_stream);
        request
            .metadata_mut()
            .insert("x-agnt5-worker-id", worker_id_header);

        // Establish the gRPC stream
        let mut response_stream = self
            .client
            .worker_stream(request)
            .await
            .map_err(|e| {
                debug!("Failed to create gRPC worker stream: {}", e);
                SdkError::Connection {
                    message: format!("gRPC stream failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?
            .into_inner();

        let registration_response =
            tokio::time::timeout(Duration::from_secs(10), response_stream.message())
                .await
                .map_err(|_| {
                    error!("Timeout waiting for registration response");
                    SdkError::Connection {
                        message: "Registration timeout - no response from runtime".to_string(),
                        code: crate::error::ErrorCode::ConnectionTimeout,
                        source: None,
                    }
                })?
                .map_err(|e| {
                    debug!("Failed to receive registration response: {}", e);
                    SdkError::Connection {
                        message: format!("Stream error: {}", e),
                        code: crate::error::ErrorCode::ConnectionFailed,
                        source: None,
                    }
                })?;

        // Process registration response. Note: the runtime no longer
        // emits redirect NACKs (any serving coordinator accepts any
        // worker), so we only need to handle ack=true and outright
        // failures here.
        if let Some(runtime_message) = registration_response {
            match &runtime_message.message_data {
                Some(crate::pb::runtime_message::MessageData::RegisterServiceResponse(resp)) => {
                    if !resp.ack {
                        error!("Registration failed: {}", resp.error);
                        return Err(SdkError::Connection {
                            message: format!("Registration failed: {}", resp.error),
                            code: crate::error::ErrorCode::ConnectionFailed,
                            source: None,
                        });
                    }
                }
                _ => {
                    error!("Unexpected response type to registration");
                    return Err(SdkError::Connection {
                        message: "Unexpected response to registration".to_string(),
                        code: crate::error::ErrorCode::InvalidMessage,
                        source: None,
                    });
                }
            }
        } else {
            error!("No registration response received");
            return Err(SdkError::Connection {
                message: "No registration response received".to_string(),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            });
        }

        // Spawn simple task to forward stream messages to runtime channel
        tokio::spawn(async move {
            while let Some(message_result) =
                tokio_stream::StreamExt::next(&mut response_stream).await
            {
                match message_result {
                    Ok(runtime_message) => {
                        if runtime_msg_tx.send_async(runtime_message).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        debug!("Stream closed: {}", e);
                        break;
                    }
                }
            }
        });

        Ok((outgoing_tx, runtime_msg_rx))
    }

    /// Open an EventStream for sending ephemeral events (SSE-only: tokens, progress, logs, spans).
    ///
    /// Returns a sender for EventStreamMessage. Events sent through this channel are published
    /// to Centrifuge/Redis for real-time SSE delivery without journal persistence.
    /// Drop the sender to close the stream.
    pub async fn create_event_stream(
        &mut self,
        worker_id: String,
    ) -> Result<flume::Sender<EventStreamMessage>> {
        let (tx, rx) = flume::bounded::<EventStreamMessage>(1000);

        let stream = async_stream::stream! {
            loop {
                match rx.recv_async().await {
                    Ok(msg) => yield msg,
                    Err(_) => break, // Sender dropped, close stream
                }
            }
        };

        // EventStream now lives on EngineService (moved from WorkerCoordinatorService).
        let mut client = self.engine_client.clone();
        tokio::spawn(async move {
            match client.event_stream(stream).await {
                Ok(response) => {
                    let ack = response.into_inner();
                    debug!(
                        "EventStream closed: success={} events_received={}",
                        ack.success, ack.events_received
                    );
                }
                Err(e) => {
                    debug!("EventStream error: {}", e);
                }
            }
        });

        debug!("EventStream opened for worker {}", worker_id);
        Ok(tx)
    }

    /// Send a step checkpoint and check for memoized result
    ///
    /// This method sends a checkpoint for a workflow step and checks if the step
    /// result is already memoized. If memoized, returns the cached output.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - Tenant the run belongs to (required for engine lookups)
    /// * `run_id` - The workflow run ID
    /// * `step_key` - Unique key for this step (e.g., "step:greet:0")
    /// * `step_name` - Human-readable step name
    /// * `step_type` - Type of step (e.g., "function", "activity", "llm_call")
    /// * `checkpoint_type` - Type of checkpoint (started, completed, failed)
    /// * `payload` - Checkpoint payload (input for started, output for completed)
    /// * `error_message` - Error message (for failed checkpoints)
    /// * `error_type` - Error type (for failed checkpoints)
    /// * `latency_ms` - Step execution latency in milliseconds
    ///
    /// # Returns
    ///
    /// `CheckpointResult` containing memoization status and cached output if available
    pub async fn checkpoint(
        &mut self,
        tenant_id: String,
        run_id: String,
        step_key: String,
        step_name: String,
        step_type: String,
        checkpoint_type: CheckpointType,
        payload: Option<Vec<u8>>,
        error_message: Option<String>,
        error_type: Option<String>,
        latency_ms: Option<i64>,
    ) -> Result<CheckpointResult> {
        debug!(
            "Sending checkpoint: tenant_id={}, run_id={}, step_key={}, type={:?}",
            tenant_id, run_id, step_key, checkpoint_type
        );

        let checkpoint = DurableStepCheckpoint {
            run_id,
            step_key,
            step_name,
            step_type,
            r#type: checkpoint_type.into(),
            payload: payload.unwrap_or_default(),
            error_message: error_message.unwrap_or_default(),
            error_type: error_type.unwrap_or_default(),
            tokens_in: 0,
            tokens_out: 0,
            cost_usd: 0.0,
            latency_ms: latency_ms.unwrap_or(0),
            model_provider: String::new(),
            model_version: String::new(),
        };

        let request = CheckpointRequest {
            checkpoint: Some(checkpoint),
            project_id: tenant_id,
        };

        // Checkpoint moved from WorkerCoordinatorService → EngineService.
        let response = self
            .engine_client
            .checkpoint(request)
            .await
            .map_err(|e| {
                debug!("Checkpoint RPC failed: {}", e);
                SdkError::Connection {
                    message: format!("Checkpoint failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?
            .into_inner();

        Ok(CheckpointResult {
            success: response.success,
            error_message: if response.error_message.is_empty() {
                None
            } else {
                Some(response.error_message)
            },
            memoized: response.memoized,
            cached_output: if response.cached_output.is_empty() {
                None
            } else {
                Some(response.cached_output)
            },
        })
    }

    /// Check if a step result is memoized without sending a full checkpoint.
    ///
    /// Uses `EngineService.FindByStepKey` as the canonical memoization lookup
    /// (replaces the legacy `WorkerCoordinatorService.GetMemoizedStep` RPC,
    /// which has been removed).
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - The tenant that owns the run (required by the engine's
    ///   `(tenant_id, run_id)` cache key).
    /// * `run_id` - The workflow run ID.
    /// * `step_key` - Unique key for this step.
    ///
    /// # Returns
    ///
    /// `Some(output)` if the step is memoized, `None` otherwise. Returns the
    /// record's `data` field (the completed step's journal payload).
    pub async fn get_memoized_step(
        &mut self,
        tenant_id: String,
        run_id: String,
        step_key: String,
    ) -> Result<Option<Vec<u8>>> {
        debug!(
            "Checking memoization: tenant_id={}, run_id={}, step_key={}",
            tenant_id, run_id, step_key
        );

        let request = FindByStepKeyRequest {
            project_id: tenant_id,
            run_id,
            step_key,
        };

        let response = self
            .engine_client
            .find_by_step_key(request)
            .await
            .map_err(|e| {
                debug!("FindByStepKey RPC failed: {}", e);
                SdkError::Connection {
                    message: format!("FindByStepKey failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?
            .into_inner();

        if response.found {
            if let Some(record) = response.record {
                if !record.data.is_empty() {
                    return Ok(Some(record.data));
                }
            }
        }
        Ok(None)
    }

    /// Register a parked-poll worker session with the Engine.
    pub async fn register_worker_session(
        &mut self,
        req: RegisterWorkerSessionRequest,
    ) -> Result<RegisterWorkerSessionResponse> {
        let response = self
            .engine_client
            .register_worker_session(req)
            .await
            .map_err(|e| {
                debug!("RegisterWorkerSession RPC failed: {}", e);
                SdkError::Connection {
                    message: format!("RegisterWorkerSession failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?
            .into_inner();

        Ok(response)
    }

    /// Park one worker slot until a job is available or the Engine times out.
    pub async fn poll_job(&mut self, req: PollJobRequest) -> Result<PollJobResponse> {
        let timeout = poll_job_deadline(&req);
        let mut request = tonic::Request::new(req);
        request.set_timeout(timeout);
        let response = match self.engine_client.poll_job(request).await {
            Ok(response) => response,
            Err(e) => {
                if is_idle_poll_timeout(&e) {
                    debug!("PollJob idle timeout: {}", e);
                    return Ok(PollJobResponse { job: None });
                }
                debug!("PollJob RPC failed: {}", e);
                return Err(SdkError::Connection {
                    message: format!("PollJob failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                });
            }
        }
        .into_inner();

        Ok(response)
    }

    /// Renew an active job lease for a parked-poll assignment.
    pub async fn renew_job_lease(
        &mut self,
        req: RenewJobLeaseRequest,
    ) -> Result<RenewJobLeaseResponse> {
        let response = self
            .engine_client
            .renew_job_lease(req)
            .await
            .map_err(|e| {
                debug!("RenewJobLease RPC failed: {}", e);
                SdkError::Connection {
                    message: format!("RenewJobLease failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?
            .into_inner();

        Ok(response)
    }

    /// Report current parked-poll capacity and active slot usage.
    pub async fn report_worker_capacity(
        &mut self,
        req: ReportWorkerCapacityRequest,
    ) -> Result<ReportWorkerCapacityResponse> {
        let response = self
            .engine_client
            .report_worker_capacity(req)
            .await
            .map_err(|e| {
                debug!("ReportWorkerCapacity RPC failed: {}", e);
                SdkError::Connection {
                    message: format!("ReportWorkerCapacity failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?
            .into_inner();

        Ok(response)
    }

    /// Report the result of a polled job back to the engine.
    /// Updates job_queue, run status, journal, and batch counters.
    ///
    /// CompleteJob now lives on EngineService (moved from WorkerCoordinatorService).
    pub async fn complete_job(&mut self, req: CompleteJobRequest) -> Result<CompleteJobResponse> {
        let response = self
            .engine_client
            .complete_job(req)
            .await
            .map_err(|e| {
                error!("CompleteJob RPC failed: {}", e);
                SdkError::Connection {
                    message: format!("CompleteJob failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?
            .into_inner();

        Ok(response)
    }
}

/// Open an EventStream on the Execution Engine for sending ephemeral events (SSE-only).
///
/// Same pattern as WC's create_event_stream but routes to EE, which is the single
/// SSE publisher. Drop the sender to close the stream.
pub async fn create_ee_event_stream(
    ee_client: &mut ExecutionEngineServiceClient<Channel>,
    worker_id: String,
) -> Result<flume::Sender<EventStreamMessage>> {
    let (tx, rx) = flume::bounded::<EventStreamMessage>(1000);

    let stream = async_stream::stream! {
        loop {
            match rx.recv_async().await {
                Ok(msg) => yield msg,
                Err(_) => break, // Sender dropped, close stream
            }
        }
    };

    let mut client = ee_client.clone();
    tokio::spawn(async move {
        match client.event_stream(stream).await {
            Ok(response) => {
                let ack = response.into_inner();
                debug!(
                    "EE EventStream closed: success={} events_received={}",
                    ack.success, ack.events_received
                );
            }
            Err(e) => {
                debug!("EE EventStream error: {}", e);
            }
        }
    });

    debug!("EE EventStream opened for worker {}", worker_id);
    Ok(tx)
}

// =============================================================================
// Engine Client — routes events to the AGNT5 Rust engine (Append/AppendBatch)
// =============================================================================

/// Pool size for engine gRPC connections.
/// Each connection is an independent h2 session, distributing load to avoid
/// the h2 PoisonError that occurs when 100+ concurrent requests share one connection.
const ENGINE_POOL_SIZE: usize = 8;
const ENGINE_RPC_RETRY_ATTEMPTS: usize = 20;
const ENGINE_RPC_RETRY_DELAY: Duration = Duration::from_millis(100);

fn is_retryable_engine_status(status: &tonic::Status) -> bool {
    let message = status.message().to_ascii_lowercase();
    matches!(
        status.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
    ) || is_retryable_engine_message(&message)
}

fn is_retryable_engine_message(message: &str) -> bool {
    message.contains("upstream connect error")
        || message.contains("disconnect/reset before headers")
        || message.contains("connection termination")
        || message.contains("connection refused")
        || message.contains("broken pipe")
        || message.contains("h2 protocol error")
        || message.contains("timeout expired")
        || message.contains("no sequencer available")
        || message.contains("this node is not the sequencer")
        || message.contains("stale epoch")
        || message.contains("future epoch")
        || message.contains("epoch ahead")
        || message.contains("not partition owner")
        || message.contains("partition not writable")
        || message.contains("quorum not reached")
        || message.contains("no connected peers for quorum replication")
        || message.contains("not connected for catch-up")
        || message.contains("catch-up failed")
}

async fn sleep_engine_retry(attempt: usize) {
    let multiplier = (attempt + 1) as u32;
    tokio::time::sleep(ENGINE_RPC_RETRY_DELAY * multiplier).await;
}

/// Client for communicating with the AGNT5 Engine.
///
/// Uses a pool of N independent gRPC connections with round-robin selection.
/// This prevents the h2 PoisonError that occurs when many concurrent checkpoint
/// events are routed through a single HTTP/2 connection.
#[derive(Debug, Clone)]
pub struct EngineClient {
    clients: Vec<EngineServiceClient<Channel>>,
    next: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl EngineClient {
    /// Connect to the engine at the given endpoint with a pool of connections.
    pub async fn connect(endpoint: &str) -> Result<Self> {
        debug!(
            "Connecting to Engine at {} (pool_size={})",
            endpoint, ENGINE_POOL_SIZE
        );

        let uri = if endpoint.contains("://") {
            endpoint.to_string()
        } else {
            format!("http://{}", endpoint)
        };

        let mut clients = Vec::with_capacity(ENGINE_POOL_SIZE);
        for i in 0..ENGINE_POOL_SIZE {
            let channel = Channel::from_shared(uri.clone())
                .map_err(|e| SdkError::Connection {
                    message: format!("Invalid engine endpoint {}: {}", endpoint, e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                })?
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .http2_adaptive_window(true)
                .connect()
                .await
                .map_err(|e| {
                    debug!("Engine connection {} to {} failed: {:?}", i, endpoint, e);
                    SdkError::Connection {
                        message: format!("Engine connection failed: {}", e),
                        code: crate::error::ErrorCode::ConnectionFailed,
                        source: None,
                    }
                })?;
            clients.push(EngineServiceClient::new(channel));
        }

        debug!(
            "Engine client pool connected ({} connections)",
            ENGINE_POOL_SIZE
        );
        Ok(Self {
            clients,
            next: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    /// Get the next client from the pool (round-robin).
    fn next_client(&mut self) -> &mut EngineServiceClient<Channel> {
        let idx = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.clients.len();
        &mut self.clients[idx]
    }

    /// Append a single record to the engine.
    pub async fn append(&mut self, record: Record) -> Result<(u64, i64)> {
        for attempt in 0..ENGINE_RPC_RETRY_ATTEMPTS {
            match self
                .next_client()
                .append(AppendRequest {
                    record: Some(record.clone()),
                })
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    return Ok((response.offset, response.timestamp_ns));
                }
                Err(status)
                    if attempt + 1 < ENGINE_RPC_RETRY_ATTEMPTS
                        && is_retryable_engine_status(&status) =>
                {
                    debug!(
                        attempt = attempt + 1,
                        max = ENGINE_RPC_RETRY_ATTEMPTS,
                        "Engine Append hit retryable gRPC status: {}",
                        status
                    );
                    sleep_engine_retry(attempt).await;
                }
                Err(status) => {
                    debug!("Engine Append failed: {}", status);
                    return Err(SdkError::Connection {
                        message: format!("Engine Append failed: {}", status),
                        code: crate::error::ErrorCode::ConnectionFailed,
                        source: None,
                    });
                }
            }
        }

        unreachable!("engine append retry loop always returns")
    }

    /// Append a batch of records to the engine.
    pub async fn append_batch(&mut self, records: Vec<Record>) -> Result<i32> {
        for attempt in 0..ENGINE_RPC_RETRY_ATTEMPTS {
            match self
                .next_client()
                .append_batch(AppendBatchRequest {
                    records: records.clone(),
                })
                .await
            {
                Ok(response) => return Ok(response.into_inner().written_count),
                Err(status)
                    if attempt + 1 < ENGINE_RPC_RETRY_ATTEMPTS
                        && is_retryable_engine_status(&status) =>
                {
                    debug!(
                        attempt = attempt + 1,
                        max = ENGINE_RPC_RETRY_ATTEMPTS,
                        "Engine AppendBatch hit retryable gRPC status: {}",
                        status
                    );
                    sleep_engine_retry(attempt).await;
                }
                Err(status) => {
                    debug!("Engine AppendBatch failed: {}", status);
                    return Err(SdkError::Connection {
                        message: format!("Engine AppendBatch failed: {}", status),
                        code: crate::error::ErrorCode::ConnectionFailed,
                        source: None,
                    });
                }
            }
        }

        unreachable!("engine append batch retry loop always returns")
    }

    /// Publish a bounded batch of ephemeral events and wait until the runtime
    /// acknowledges every frame. Closing each batch supplies the ordering
    /// barrier needed before a durable terminal event is appended.
    pub async fn stream_events(&mut self, events: Vec<EventStreamMessage>) -> Result<i64> {
        if events.is_empty() {
            return Ok(0);
        }
        let expected = events.len() as i64;
        let response = self
            .next_client()
            .event_stream(tokio_stream::iter(events))
            .await
            .map_err(|status| SdkError::Connection {
                message: format!("Engine EventStream failed: {status}"),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            })?
            .into_inner();
        if !response.success || response.events_received != expected {
            return Err(SdkError::Connection {
                message: format!(
                    "Engine EventStream acknowledged {} events, want {}",
                    response.events_received, expected
                ),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            });
        }
        Ok(response.events_received)
    }
}

/// Build an engine `Record` from SDK event fields.
pub fn build_engine_record(
    tenant_id: String,
    run_id: String,
    event_type: String,
    data: Vec<u8>,
    timestamp_ns: i64,
    step_key: String,
    correlation_id: String,
    parent_event_id: String,
    metadata: HashMap<String, String>,
) -> Record {
    Record {
        offset: 0, // Assigned by engine
        project_id: tenant_id,
        run_id,
        event_type,
        data,
        timestamp_ns,
        step_key,
        correlation_id,
        parent_event_id,
        metadata,
        data_type: "json".to_string(),
        data_checksum: vec![],
        data_compressed: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_engine_status_includes_partition_handoff_errors() {
        let status = tonic::Status::internal(
            "Engine AppendBatch failed: no sequencer available after retry",
        );
        assert!(is_retryable_engine_status(&status));

        let status = tonic::Status::failed_precondition("stale epoch on forward: 162 < 163");
        assert!(is_retryable_engine_status(&status));
    }

    #[test]
    fn retryable_engine_status_does_not_retry_plain_internal_errors() {
        let status = tonic::Status::internal("serialization failed");
        assert!(!is_retryable_engine_status(&status));
    }
}

/// Result of a checkpoint operation
#[derive(Debug, Clone)]
pub struct CheckpointResult {
    /// Whether the checkpoint was processed successfully
    pub success: bool,
    /// Error message if the checkpoint failed
    pub error_message: Option<String>,
    /// Whether the step was already memoized (for STEP_STARTED checkpoints)
    pub memoized: bool,
    /// Cached output if memoized
    pub cached_output: Option<Vec<u8>>,
}
