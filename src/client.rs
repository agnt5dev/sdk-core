use crate::error::{Result, SdkError};
use crate::pb::{
    engine_service_client::EngineServiceClient,
    execution_engine_service_client::ExecutionEngineServiceClient,
    worker_coordinator_service_client::WorkerCoordinatorServiceClient, AppendBatchRequest,
    AppendBatchResponse, AppendRequest, BeginActivationRequest, BeginActivationResponse,
    CheckpointRequest, CheckpointType, CompleteActivationRequest, CompleteActivationResponse,
    CompleteJobRequest, CompleteJobResponse, DurableStepCheckpoint, EventStreamMessage,
    FailActivationRequest, FailActivationResponse, FindByStepKeyRequest, GetEntityStateRequest,
    GetEntityStateResponse, PollJobRequest, PollJobResponse, PutEntityStateRequest,
    PutEntityStateResponse, Record, RegisterService, RegisterWorkerSessionRequest,
    RegisterWorkerSessionResponse, RenewJobLeaseRequest, RenewJobLeaseResponse,
    ReportWorkerCapacityRequest, ReportWorkerCapacityResponse, RuntimeMessage, ServiceMessage,
    SuspendActivationRequest, SuspendActivationResponse,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Code;
use tonic::{Request, Status};
use tracing::{debug, error};

const EXTERNAL_DISCOVERY_PATH: &str = "/api/v1/worker-discovery";
const EXTERNAL_TOKEN_PATH: &str = "/api/v1/worker-token";
const AUTH_PROFILE_BOOTSTRAP_MTLS: &str = "bootstrap-mtls";
const AUTH_PROFILE_TOKEN_AUTH: &str = "token-auth";

#[derive(Clone, Debug)]
struct BearerInterceptor {
    token: Arc<RwLock<Option<MetadataValue<tonic::metadata::Ascii>>>>,
}

impl tonic::service::Interceptor for BearerInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        if let Some(token) = self.token.read().unwrap_or_else(|p| p.into_inner()).clone() {
            request.metadata_mut().insert("authorization", token);
        }
        Ok(request)
    }
}

type AuthenticatedChannel = InterceptedService<Channel, BearerInterceptor>;

#[derive(Clone, Debug, Deserialize)]
struct ExternalWorkerAuthority {
    project_id: String,
    environment_id: String,
    deployment_id: String,
    worker_pool_id: String,
    #[serde(default)]
    runtime_endpoint: String,
    #[serde(default)]
    protocol: String,
    #[serde(default = "default_external_worker_auth_profile")]
    auth_profile: String,
    #[serde(default)]
    identity_endpoint: String,
}

fn default_external_worker_auth_profile() -> String {
    AUTH_PROFILE_TOKEN_AUTH.to_string()
}

#[derive(Debug, Serialize)]
struct ExternalWorkerTokenRequest<'a> {
    project_id: &'a str,
    environment_id: &'a str,
    deployment_id: &'a str,
    worker_pool_id: &'a str,
}

impl<'a> From<&'a ExternalWorkerAuthority> for ExternalWorkerTokenRequest<'a> {
    fn from(authority: &'a ExternalWorkerAuthority) -> Self {
        Self {
            project_id: &authority.project_id,
            environment_id: &authority.environment_id,
            deployment_id: &authority.deployment_id,
            worker_pool_id: &authority.worker_pool_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct ExternalWorkerTokenResponse {
    workload_token: String,
    expires_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone, Debug)]
struct ExternalWorkerBootstrap {
    control_plane_url: String,
    credential: String,
    authority: ExternalWorkerAuthority,
}

pub(crate) fn remote_worker_bootstrap_enabled() -> bool {
    std::env::var("AGNT5_API_KEY_FILE").is_ok_and(|value| !value.trim().is_empty())
}

async fn external_worker_bootstrap() -> Result<(
    String,
    BearerInterceptor,
    Option<ClientTlsConfig>,
    Option<String>,
)> {
    let control_plane_url = std::env::var("AGNT5_CONTROL_PLANE_URL")
        .unwrap_or_else(|_| "https://api.agnt5.com".to_string())
        .trim_end_matches('/')
        .to_string();
    let key_path = std::env::var("AGNT5_API_KEY_FILE").map_err(|_| SdkError::Connection {
        message: "AGNT5_API_KEY_FILE is required for external workers".to_string(),
        code: crate::error::ErrorCode::ConnectionFailed,
        source: None,
    })?;
    let credential = std::fs::read_to_string(&key_path)
        .map_err(|error| SdkError::Connection {
            message: format!("read external worker credential {}: {}", key_path, error),
            code: crate::error::ErrorCode::ConnectionFailed,
            source: None,
        })?
        .trim()
        .to_string();
    if credential.is_empty() {
        return Err(SdkError::Connection {
            message: "external worker credential file is empty".to_string(),
            code: crate::error::ErrorCode::ConnectionFailed,
            source: None,
        });
    }
    let environment = std::env::var("AGNT5_ENVIRONMENT").unwrap_or_default();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|error| SdkError::Connection {
            message: format!("create external worker bootstrap client: {}", error),
            code: crate::error::ErrorCode::ConnectionFailed,
            source: None,
        })?;
    let response = http
        .post(format!("{}{}", control_plane_url, EXTERNAL_DISCOVERY_PATH))
        .header("X-API-KEY", &credential)
        .json(&serde_json::json!({
            "environment": environment,
            "supported_auth_profiles": [AUTH_PROFILE_BOOTSTRAP_MTLS, AUTH_PROFILE_TOKEN_AUTH]
        }))
        .send()
        .await
        .map_err(|error| SdkError::Connection {
            message: format!("external worker discovery failed: {}", error),
            code: crate::error::ErrorCode::ConnectionFailed,
            source: None,
        })?;
    if !response.status().is_success() {
        return Err(SdkError::Connection {
            message: format!("external worker discovery returned {}", response.status()),
            code: crate::error::ErrorCode::ConnectionFailed,
            source: None,
        });
    }
    let authority: ExternalWorkerAuthority =
        response
            .json()
            .await
            .map_err(|error| SdkError::Connection {
                message: format!("decode external worker discovery response: {}", error),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            })?;
    if authority.protocol != "pull.v1" || authority.runtime_endpoint.trim().is_empty() {
        return Err(SdkError::Connection {
            message: "external worker discovery returned an unsupported target".to_string(),
            code: crate::error::ErrorCode::ConnectionFailed,
            source: None,
        });
    }
    match authority.auth_profile.as_str() {
        AUTH_PROFILE_BOOTSTRAP_MTLS => {
            if authority.identity_endpoint.trim().is_empty() {
                return Err(SdkError::Connection {
                    message:
                        "worker discovery selected bootstrap-mtls without an identity endpoint"
                            .to_string(),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                });
            }
            let identity_authority = crate::external_worker_identity::Authority {
                project_id: authority.project_id,
                environment_id: authority.environment_id,
                deployment_id: authority.deployment_id,
                worker_pool_id: authority.worker_pool_id,
                runtime_endpoint: authority.runtime_endpoint,
                protocol: authority.protocol,
            };
            let identity = crate::external_worker_identity::connection_identity(
                control_plane_url,
                authority.identity_endpoint,
                credential,
                identity_authority,
            )
            .await?;
            eprintln!("[INFO] worker authenticated with bootstrap mTLS");
            return Ok((
                identity.endpoint,
                BearerInterceptor {
                    token: identity.authorization,
                },
                Some(identity.tls),
                Some(identity.worker_id),
            ));
        }
        AUTH_PROFILE_TOKEN_AUTH => {}
        profile => {
            return Err(SdkError::Connection {
                message: format!(
                    "worker discovery selected unsupported authentication profile {profile:?}"
                ),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            });
        }
    }
    let bootstrap = ExternalWorkerBootstrap {
        control_plane_url,
        credential,
        authority,
    };
    let initial = exchange_external_worker_token(&http, &bootstrap).await?;
    let token = bearer_metadata(&initial.workload_token)?;
    let shared = Arc::new(RwLock::new(Some(token)));
    spawn_external_worker_token_refresh(
        http,
        bootstrap.clone(),
        shared.clone(),
        initial.expires_at,
    );

    // Discovery is authoritative. These values are consumed immediately after
    // connect when the pull session registration is constructed.
    std::env::set_var("AGNT5_PROJECT_ID", &bootstrap.authority.project_id);
    std::env::set_var("AGNT5_DEPLOYMENT_ID", &bootstrap.authority.deployment_id);
    std::env::set_var("AGNT5_WORKERPOOL_ID", &bootstrap.authority.worker_pool_id);
    std::env::set_var("AGNT5_WORKER_MODE", "pull");
    eprintln!("[INFO] worker authenticated with token auth");
    Ok((
        bootstrap.authority.runtime_endpoint.clone(),
        BearerInterceptor { token: shared },
        None,
        None,
    ))
}

async fn exchange_external_worker_token(
    http: &reqwest::Client,
    bootstrap: &ExternalWorkerBootstrap,
) -> Result<ExternalWorkerTokenResponse> {
    let response = http
        .post(format!(
            "{}{}",
            bootstrap.control_plane_url, EXTERNAL_TOKEN_PATH
        ))
        .header("X-API-KEY", &bootstrap.credential)
        .json(&ExternalWorkerTokenRequest::from(&bootstrap.authority))
        .send()
        .await
        .map_err(|error| SdkError::Connection {
            message: format!("external worker token exchange failed: {}", error),
            code: crate::error::ErrorCode::ConnectionFailed,
            source: None,
        })?;
    if !response.status().is_success() {
        return Err(SdkError::Connection {
            message: format!(
                "external worker token exchange returned {}",
                response.status()
            ),
            code: crate::error::ErrorCode::ConnectionFailed,
            source: None,
        });
    }
    response.json().await.map_err(|error| SdkError::Connection {
        message: format!("decode external worker token response: {}", error),
        code: crate::error::ErrorCode::ConnectionFailed,
        source: None,
    })
}

fn bearer_metadata(token: &str) -> Result<MetadataValue<tonic::metadata::Ascii>> {
    format!("Bearer {}", token)
        .parse()
        .map_err(|error| SdkError::Connection {
            message: format!("invalid external worker token metadata: {}", error),
            code: crate::error::ErrorCode::ConnectionFailed,
            source: None,
        })
}

fn spawn_external_worker_token_refresh(
    http: reqwest::Client,
    bootstrap: ExternalWorkerBootstrap,
    token: Arc<RwLock<Option<MetadataValue<tonic::metadata::Ascii>>>>,
    mut expires_at: chrono::DateTime<chrono::Utc>,
) {
    tokio::spawn(async move {
        loop {
            let delay = (expires_at - chrono::Utc::now() - chrono::Duration::seconds(60))
                .to_std()
                .unwrap_or(Duration::from_secs(1));
            tokio::time::sleep(delay).await;
            match exchange_external_worker_token(&http, &bootstrap).await {
                Ok(refreshed) => match bearer_metadata(&refreshed.workload_token) {
                    Ok(value) => {
                        *token.write().unwrap_or_else(|p| p.into_inner()) = Some(value);
                        expires_at = refreshed.expires_at;
                    }
                    Err(error) => error!("External worker token refresh rejected: {}", error),
                },
                Err(error) => {
                    error!("External worker token refresh failed: {}; retrying", error);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    });
}

pub const DURABLE_ACTIVATION_V1_CAPABILITY: &str = "durable_activation_v1";
pub const DURABLE_SUSPENSION_V1_CAPABILITY: &str = "durable_suspension_v1";

pub fn worker_protocol_capabilities() -> (Vec<String>, Vec<String>) {
    match std::env::var("AGNT5_DURABLE_ACTIVATION_MODE")
        .unwrap_or_else(|_| "preferred".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "disabled" => (Vec::new(), Vec::new()),
        "required" => (
            vec![
                DURABLE_ACTIVATION_V1_CAPABILITY.to_string(),
                DURABLE_SUSPENSION_V1_CAPABILITY.to_string(),
            ],
            vec![DURABLE_ACTIVATION_V1_CAPABILITY.to_string()],
        ),
        _ => (
            vec![
                DURABLE_ACTIVATION_V1_CAPABILITY.to_string(),
                DURABLE_SUSPENSION_V1_CAPABILITY.to_string(),
            ],
            Vec::new(),
        ),
    }
}

pub(crate) fn validate_protocol_capabilities(
    worker_supported: &[String],
    worker_required: &[String],
    runtime_supported: &[String],
    runtime_required: &[String],
) -> Result<()> {
    if let Some(required) = worker_required
        .iter()
        .find(|required| !runtime_supported.contains(required))
    {
        return Err(SdkError::Activation {
            message: format!("runtime did not negotiate required protocol capability: {required}"),
            code: crate::error::ErrorCode::DurabilityUnavailable,
            activation_id: None,
            attempt: None,
        });
    }
    if let Some(required) = runtime_required
        .iter()
        .find(|required| !worker_supported.contains(required))
    {
        return Err(SdkError::Activation {
            message: format!("runtime requires unsupported worker protocol capability: {required}"),
            code: crate::error::ErrorCode::DurabilityUnavailable,
            activation_id: None,
            attempt: None,
        });
    }
    if worker_supported
        .iter()
        .any(|capability| capability == DURABLE_ACTIVATION_V1_CAPABILITY)
        && !worker_required
            .iter()
            .any(|capability| capability == DURABLE_ACTIVATION_V1_CAPABILITY)
        && !runtime_supported
            .iter()
            .any(|capability| capability == DURABLE_ACTIVATION_V1_CAPABILITY)
    {
        eprintln!(
            "[WARN] agnt5 durable activation degraded: runtime did not advertise durable_activation_v1; legacy checkpoints remain enabled"
        );
    }
    Ok(())
}

fn negotiated_protocol_capabilities(
    worker_supported: &[String],
    runtime_supported: &[String],
) -> Vec<String> {
    worker_supported
        .iter()
        .filter(|capability| runtime_supported.contains(capability))
        .fold(Vec::new(), |mut negotiated, capability| {
            if !negotiated.contains(capability) {
                negotiated.push(capability.clone());
            }
            negotiated
        })
}

fn activation_status(operation: &str, status: tonic::Status) -> SdkError {
    crate::runtime_adapter::activation_status_error(operation, status)
}

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
    client: WorkerCoordinatorServiceClient<AuthenticatedChannel>,
    engine_client: EngineServiceClient<AuthenticatedChannel>,
    negotiated_protocol_capabilities: Arc<RwLock<Vec<String>>>,
    authoritative_worker_id: Option<String>,
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
        let (endpoint, interceptor, tls, authoritative_worker_id) =
            if remote_worker_bootstrap_enabled() {
                external_worker_bootstrap().await?
            } else {
                (
                    endpoint,
                    BearerInterceptor {
                        token: Arc::new(RwLock::new(None)),
                    },
                    None,
                    None,
                )
            };
        debug!("Connecting to Worker Coordinator at {}", endpoint);

        let mut channel =
            Channel::from_shared(endpoint.clone()).map_err(|e| SdkError::Connection {
                message: format!("Invalid endpoint {}: {}", endpoint, e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            })?;
        if let Some(tls) = tls {
            channel = channel.tls_config(tls).map_err(|e| SdkError::Connection {
                message: format!("Invalid worker TLS configuration: {}", e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            })?;
        }
        let channel = channel
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

        let client =
            WorkerCoordinatorServiceClient::with_interceptor(channel.clone(), interceptor.clone());
        let engine_client = EngineServiceClient::with_interceptor(channel, interceptor);

        Ok(Self {
            client,
            engine_client,
            negotiated_protocol_capabilities: Arc::new(RwLock::new(Vec::new())),
            authoritative_worker_id,
        })
    }

    /// Use the server-assigned ID for certificate-bound customer workers.
    pub(crate) fn effective_worker_id<'a>(&'a self, configured: &'a str) -> &'a str {
        self.authoritative_worker_id
            .as_deref()
            .unwrap_or(configured)
    }

    pub(crate) fn retain_negotiated_protocol_capabilities(
        &self,
        worker_supported: &[String],
        runtime_supported: &[String],
    ) {
        let negotiated = negotiated_protocol_capabilities(worker_supported, runtime_supported);
        let mut capabilities = self
            .negotiated_protocol_capabilities
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *capabilities = negotiated;
    }

    /// Return whether this worker session actually negotiated a protocol
    /// capability with the runtime. Clones share the same session state.
    pub fn negotiated_protocol_capability(&self, capability: &str) -> bool {
        self.negotiated_protocol_capabilities
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .any(|negotiated| negotiated == capability)
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
        let worker_id = self.effective_worker_id(&worker_id).to_string();
        let worker_supported_protocols = registration.supported_protocol_capabilities.clone();
        let worker_required_protocols = registration.required_protocol_capabilities.clone();
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
                    validate_protocol_capabilities(
                        &worker_supported_protocols,
                        &worker_required_protocols,
                        &resp.supported_protocol_capabilities,
                        &resp.required_protocol_capabilities,
                    )?;
                    self.retain_negotiated_protocol_capabilities(
                        &worker_supported_protocols,
                        &resp.supported_protocol_capabilities,
                    );
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
        mut req: RegisterWorkerSessionRequest,
    ) -> Result<RegisterWorkerSessionResponse> {
        req.worker_id = self.effective_worker_id(&req.worker_id).to_string();
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
    pub async fn poll_job(&mut self, mut req: PollJobRequest) -> Result<PollJobResponse> {
        req.worker_id = self.effective_worker_id(&req.worker_id).to_string();
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

    /// Read entity state through the same unary Engine service used by parked
    /// polling. This is request-scoped and does not require a worker stream.
    pub async fn get_entity_state(
        &mut self,
        req: GetEntityStateRequest,
    ) -> Result<GetEntityStateResponse> {
        self.engine_client
            .get_entity_state(req)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| SdkError::Connection {
                message: format!("GetEntityState failed: {error}"),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            })
    }

    /// Persist entity state through a unary, lease-fenced Engine request.
    pub async fn put_entity_state(
        &mut self,
        req: PutEntityStateRequest,
    ) -> Result<PutEntityStateResponse> {
        self.engine_client
            .put_entity_state(req)
            .await
            .map(|response| response.into_inner())
            .map_err(|error| SdkError::Connection {
                message: format!("PutEntityState failed: {error}"),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            })
    }

    /// Renew an active push or pull execution lease.
    pub async fn renew_job_lease(
        &mut self,
        mut req: RenewJobLeaseRequest,
    ) -> Result<RenewJobLeaseResponse> {
        req.worker_id = self.effective_worker_id(&req.worker_id).to_string();
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
        mut req: ReportWorkerCapacityRequest,
    ) -> Result<ReportWorkerCapacityResponse> {
        req.worker_id = self.effective_worker_id(&req.worker_id).to_string();
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
    pub async fn complete_job(
        &mut self,
        mut req: CompleteJobRequest,
    ) -> Result<CompleteJobResponse> {
        req.worker_id = self.effective_worker_id(&req.worker_id).to_string();
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

    /// Suspend a polled job through the Engine RPC. Pull workers do not keep a
    /// bidirectional dispatch stream, so they cannot return this typed result
    /// through WorkerStream like push workers do.
    pub async fn suspend_activation(
        &mut self,
        req: SuspendActivationRequest,
    ) -> Result<SuspendActivationResponse> {
        self.engine_client
            .suspend_activation(req)
            .await
            .map(|response| response.into_inner())
            .map_err(|status| activation_status("SuspendActivation", status))
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
const ENGINE_ACTIVATION_RPC_ATTEMPTS: usize = 6;
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

fn validate_append_batch_records(records: &[Record]) -> Result<()> {
    let Some(first) = records.first() else {
        return Ok(());
    };
    if records
        .iter()
        .skip(1)
        .any(|record| record.run_id != first.run_id)
    {
        return Err(SdkError::InvalidArgument {
            message: "AppendBatch records must share one run_id so the request cannot span journal partitions".to_string(),
            argument: Some("records".to_string()),
        });
    }
    Ok(())
}

fn validate_append_batch_response(response: &AppendBatchResponse, expected: usize) -> Result<i32> {
    if response.offsets.len() != expected {
        return Err(SdkError::Internal(format!(
            "Engine AppendBatch response cardinality mismatch: expected {expected}, offsets={}",
            response.offsets.len()
        )));
    }
    if response.written_count < 0 || response.written_count as usize > expected {
        return Err(SdkError::Internal(format!(
            "Engine AppendBatch written_count out of range: expected 0..={expected}, got {}",
            response.written_count
        )));
    }
    Ok(response.written_count)
}

async fn sleep_engine_retry(attempt: usize) {
    let multiplier = (attempt + 1) as u32;
    tokio::time::sleep(ENGINE_RPC_RETRY_DELAY * multiplier).await;
}

fn should_retry_activation_status(status: &tonic::Status, attempt: usize) -> bool {
    attempt + 1 < ENGINE_ACTIVATION_RPC_ATTEMPTS
        && status.details().is_empty()
        && is_retryable_engine_status(status)
}

/// Client for communicating with the AGNT5 Engine.
///
/// Uses a pool of N independent gRPC connections with round-robin selection.
/// This prevents the h2 PoisonError that occurs when many concurrent checkpoint
/// events are routed through a single HTTP/2 connection.
#[derive(Debug, Clone)]
pub struct EngineClient {
    clients: Vec<EngineServiceClient<AuthenticatedChannel>>,
    next: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    authoritative_worker_id: Option<String>,
}

impl EngineClient {
    /// Connect to the engine at the given endpoint with a pool of connections.
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let (endpoint, interceptor, tls, authoritative_worker_id) =
            if remote_worker_bootstrap_enabled() {
                external_worker_bootstrap().await?
            } else {
                (
                    endpoint.to_string(),
                    BearerInterceptor {
                        token: Arc::new(RwLock::new(None)),
                    },
                    None,
                    None,
                )
            };
        debug!(
            "Connecting to Engine at {} (pool_size={})",
            endpoint, ENGINE_POOL_SIZE
        );

        let uri = if endpoint.contains("://") {
            endpoint.clone()
        } else {
            format!("http://{}", endpoint)
        };

        let mut clients = Vec::with_capacity(ENGINE_POOL_SIZE);
        for i in 0..ENGINE_POOL_SIZE {
            let mut channel =
                Channel::from_shared(uri.clone()).map_err(|e| SdkError::Connection {
                    message: format!("Invalid engine endpoint {}: {}", endpoint, e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                })?;
            if let Some(tls) = tls.clone() {
                channel = channel.tls_config(tls).map_err(|e| SdkError::Connection {
                    message: format!("Invalid worker TLS configuration: {}", e),
                    code: crate::error::ErrorCode::ConnectionFailed,
                    source: None,
                })?;
            }
            let channel = channel
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
            clients.push(EngineServiceClient::with_interceptor(
                channel,
                interceptor.clone(),
            ));
        }

        debug!(
            "Engine client pool connected ({} connections)",
            ENGINE_POOL_SIZE
        );
        Ok(Self {
            clients,
            next: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            authoritative_worker_id,
        })
    }

    /// Get the next client from the pool (round-robin).
    fn next_client(&mut self) -> &mut EngineServiceClient<AuthenticatedChannel> {
        let idx = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.clients.len();
        &mut self.clients[idx]
    }

    /// Admit one logical activation and return the journal-authoritative decision.
    pub async fn begin_activation(
        &mut self,
        request: BeginActivationRequest,
    ) -> Result<BeginActivationResponse> {
        for attempt in 0..ENGINE_ACTIVATION_RPC_ATTEMPTS {
            match self.next_client().begin_activation(request.clone()).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) if should_retry_activation_status(&status, attempt) => {
                    debug!(
                        attempt = attempt + 1,
                        status = %status,
                        "BeginActivation hit a transient gRPC status; retrying the exact command"
                    );
                    sleep_engine_retry(attempt).await;
                }
                Err(status) => return Err(activation_status("BeginActivation", status)),
            }
        }
        unreachable!("activation retry loop always returns")
    }

    /// Commit one fenced activation completion and wait for its durability acknowledgement.
    pub async fn complete_activation(
        &mut self,
        request: CompleteActivationRequest,
    ) -> Result<CompleteActivationResponse> {
        for attempt in 0..ENGINE_ACTIVATION_RPC_ATTEMPTS {
            match self
                .next_client()
                .complete_activation(request.clone())
                .await
            {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) if should_retry_activation_status(&status, attempt) => {
                    debug!(
                        attempt = attempt + 1,
                        status = %status,
                        "CompleteActivation hit a transient gRPC status; retrying the exact command"
                    );
                    sleep_engine_retry(attempt).await;
                }
                Err(status) => return Err(activation_status("CompleteActivation", status)),
            }
        }
        unreachable!("activation retry loop always returns")
    }

    /// Commit one fenced activation failure and wait for its durability acknowledgement.
    pub async fn fail_activation(
        &mut self,
        request: FailActivationRequest,
    ) -> Result<FailActivationResponse> {
        for attempt in 0..ENGINE_ACTIVATION_RPC_ATTEMPTS {
            match self.next_client().fail_activation(request.clone()).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) if should_retry_activation_status(&status, attempt) => {
                    debug!(
                        attempt = attempt + 1,
                        status = %status,
                        "FailActivation hit a transient gRPC status; retrying the exact command"
                    );
                    sleep_engine_retry(attempt).await;
                }
                Err(status) => return Err(activation_status("FailActivation", status)),
            }
        }
        unreachable!("activation retry loop always returns")
    }

    /// Atomically park one fenced activation and its parent run until a
    /// durable timer generation is ready.
    pub async fn suspend_activation(
        &mut self,
        request: SuspendActivationRequest,
    ) -> Result<SuspendActivationResponse> {
        for attempt in 0..ENGINE_ACTIVATION_RPC_ATTEMPTS {
            match self.next_client().suspend_activation(request.clone()).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(status) if should_retry_activation_status(&status, attempt) => {
                    debug!(
                        attempt = attempt + 1,
                        status = %status,
                        "SuspendActivation hit a transient gRPC status; retrying the exact command"
                    );
                    sleep_engine_retry(attempt).await;
                }
                Err(status) => return Err(activation_status("SuspendActivation", status)),
            }
        }
        unreachable!("activation retry loop always returns")
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
        validate_append_batch_records(&records)?;
        let expected = records.len();
        for attempt in 0..ENGINE_RPC_RETRY_ATTEMPTS {
            match self
                .next_client()
                .append_batch(AppendBatchRequest {
                    records: records.clone(),
                })
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    return validate_append_batch_response(&response, expected);
                }
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
    pub async fn stream_events(&mut self, mut events: Vec<EventStreamMessage>) -> Result<i64> {
        if events.is_empty() {
            return Ok(0);
        }
        if let Some(worker_id) = &self.authoritative_worker_id {
            for event in &mut events {
                event.worker_id.clone_from(worker_id);
            }
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
    fn external_worker_token_request_contains_only_immutable_authority() {
        let authority = ExternalWorkerAuthority {
            project_id: "project-1".into(),
            environment_id: "environment-1".into(),
            deployment_id: "deployment-1".into(),
            worker_pool_id: "pool-1".into(),
            runtime_endpoint: "https://runtime.example".into(),
            protocol: "pull.v1".into(),
            auth_profile: AUTH_PROFILE_TOKEN_AUTH.into(),
            identity_endpoint: String::new(),
        };

        let value = serde_json::to_value(ExternalWorkerTokenRequest::from(&authority)).unwrap();

        assert_eq!(
            value,
            serde_json::json!({
                "project_id": "project-1",
                "environment_id": "environment-1",
                "deployment_id": "deployment-1",
                "worker_pool_id": "pool-1"
            })
        );
    }

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

    #[test]
    fn activation_rpc_retry_is_bounded_to_transient_statuses() {
        let lagging_replica =
            tonic::Status::unavailable("activation actv1_test is not yet visible on this replica");
        assert!(should_retry_activation_status(&lagging_replica, 0));
        assert!(!should_retry_activation_status(
            &lagging_replica,
            ENGINE_ACTIVATION_RPC_ATTEMPTS - 1
        ));

        let conflict = tonic::Status::already_exists("payload conflict");
        assert!(!should_retry_activation_status(&conflict, 0));

        let typed_conflict = tonic::Status::with_details(
            tonic::Code::Unavailable,
            "typed activation outcome",
            bytes::Bytes::from_static(b"activation-error-detail"),
        );
        assert!(!should_retry_activation_status(&typed_conflict, 0));
    }

    #[tokio::test]
    async fn append_batch_rejects_multi_run_before_retry_or_rpc() {
        let mut client = EngineClient {
            clients: Vec::new(),
            next: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            authoritative_worker_id: None,
        };
        let records = vec![
            Record {
                run_id: "run-a".into(),
                event_type: "step.started".into(),
                ..Default::default()
            },
            Record {
                run_id: "run-b".into(),
                event_type: "step.completed".into(),
                ..Default::default()
            },
        ];

        let error = client.append_batch(records).await.unwrap_err();

        assert!(matches!(error, SdkError::InvalidArgument { .. }));
        assert_eq!(
            client.next.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "preflight rejection must happen before the retry loop selects a connection"
        );
    }

    #[test]
    fn append_batch_rejects_malformed_response_counts() {
        for response in [
            AppendBatchResponse {
                offsets: vec![1],
                written_count: 1,
            },
            AppendBatchResponse {
                offsets: vec![1, 2],
                written_count: -1,
            },
            AppendBatchResponse {
                offsets: vec![1, 2],
                written_count: 3,
            },
        ] {
            let error = validate_append_batch_response(&response, 2).unwrap_err();
            assert!(matches!(error, SdkError::Internal(_)));
        }
    }

    #[test]
    fn protocol_capability_negotiation_fails_closed_when_required() {
        let durable = DURABLE_ACTIVATION_V1_CAPABILITY.to_string();
        let error = validate_protocol_capabilities(
            std::slice::from_ref(&durable),
            std::slice::from_ref(&durable),
            &[],
            &[],
        )
        .unwrap_err();
        assert_eq!(error.code(), crate::error::ErrorCode::DurabilityUnavailable);

        let error =
            validate_protocol_capabilities(&[], &[], &[], &["runtime_v2".into()]).unwrap_err();
        assert_eq!(error.code(), crate::error::ErrorCode::DurabilityUnavailable);

        validate_protocol_capabilities(
            std::slice::from_ref(&durable),
            std::slice::from_ref(&durable),
            std::slice::from_ref(&durable),
            &[],
        )
        .unwrap();
    }

    #[test]
    fn protocol_capability_negotiation_retains_only_the_intersection() {
        assert_eq!(
            negotiated_protocol_capabilities(
                &[
                    DURABLE_ACTIVATION_V1_CAPABILITY.into(),
                    "worker_only".into(),
                    DURABLE_ACTIVATION_V1_CAPABILITY.into(),
                ],
                &[
                    DURABLE_ACTIVATION_V1_CAPABILITY.into(),
                    "runtime_only".into(),
                ],
            ),
            vec![DURABLE_ACTIVATION_V1_CAPABILITY.to_string()]
        );
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
