use crate::error::{Result, SdkError};
use crate::pb::{
    worker_coordinator_service_client::WorkerCoordinatorServiceClient, CheckpointRequest,
    CheckpointType, CompleteJobRequest, CompleteJobResponse, DurableStepCheckpoint,
    GetMemoizedStepRequest, PollJobsRequest, PollJobsResponse, RegisterService, RuntimeMessage,
    ServiceMessage,
};
use std::collections::HashMap;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, error};

/// Simple client for communicating with the Worker Coordinator service
#[derive(Debug, Clone)]
pub struct WorkerCoordinatorClient {
    client: WorkerCoordinatorServiceClient<Channel>,
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
            .timeout(Duration::from_secs(30))
            .http2_adaptive_window(true)
            .connect()
            .await
            .map_err(|e| {
                // Expected during reconnection — debug level to avoid noisy logs
                debug!("Connection to {} failed: {:?}", endpoint, e);
                e
            })?;

        let client = WorkerCoordinatorServiceClient::new(channel);

        Ok(Self { client })
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

        // Establish the gRPC stream
        let mut response_stream = self
            .client
            .worker_stream(outgoing_stream)
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

        // Process registration response
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

    /// Send a step checkpoint and check for memoized result
    ///
    /// This method sends a checkpoint for a workflow step and checks if the step
    /// result is already memoized. If memoized, returns the cached output.
    ///
    /// # Arguments
    ///
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
            "Sending checkpoint: run_id={}, step_key={}, type={:?}",
            run_id, step_key, checkpoint_type
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
        };

        let response = self
            .client
            .checkpoint(request)
            .await
            .map_err(|e| {
                error!("Checkpoint RPC failed: {}", e);
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

    /// Check if a step result is memoized without sending a full checkpoint
    ///
    /// Use this for quick memoization lookups before executing expensive steps.
    ///
    /// # Arguments
    ///
    /// * `run_id` - The workflow run ID
    /// * `step_key` - Unique key for this step
    ///
    /// # Returns
    ///
    /// `Some(output)` if the step is memoized, `None` otherwise
    pub async fn get_memoized_step(
        &mut self,
        run_id: String,
        step_key: String,
    ) -> Result<Option<Vec<u8>>> {
        debug!(
            "Checking memoization: run_id={}, step_key={}",
            run_id, step_key
        );

        let request = GetMemoizedStepRequest { run_id, step_key };

        let response = self
            .client
            .get_memoized_step(request)
            .await
            .map_err(|e| {
                error!("GetMemoizedStep RPC failed: {}", e);
                SdkError::Connection {
                    message: format!("GetMemoizedStep failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?
            .into_inner();

        if response.found && !response.output.is_empty() {
            Ok(Some(response.output))
        } else {
            Ok(None)
        }
    }

    /// Poll for available jobs from the durable queue (managed edition).
    /// Workers call this with exponential backoff to claim pending jobs.
    pub async fn poll_jobs(&mut self, req: PollJobsRequest) -> Result<PollJobsResponse> {
        let response = self
            .client
            .poll_jobs(req)
            .await
            .map_err(|e| {
                debug!("PollJobs RPC failed: {}", e);
                SdkError::Connection {
                    message: format!("PollJobs failed: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                }
            })?
            .into_inner();

        Ok(response)
    }

    /// Report the result of a polled job back to the coordinator.
    /// Updates job_queue, run status, journal, and batch counters.
    pub async fn complete_job(&mut self, req: CompleteJobRequest) -> Result<CompleteJobResponse> {
        let response = self
            .client
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
