use crate::error::Result;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::Context;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

#[derive(Debug, Clone)]
pub struct InvocationRequest {
    pub run_id: String,
    pub service_name: String,
    pub handler_name: String,
    pub input_data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct InvocationResponse {
    pub run_id: String,
    pub output_data: Vec<u8>,
    pub success: bool,
    pub error_message: Option<String>,
    pub metadata: HashMap<String, String>,
}

pub struct StreamingInvocationRequest {}

pub struct StreamingInvocationResponse {}

#[async_trait::async_trait]
pub trait StateManager: Send + Sync {
    async fn get(&self, key: String) -> Result<Vec<u8>>;
    async fn set(&self, key: String, value: Vec<u8>) -> Result<()>;
    async fn delete(&self, key: String) -> Result<()>;
}

pub struct RuntimeContext {
    pub run_id: String,
    pub service_name: String,
    pub component_name: String,
    pub tenant_id: String,
    pub deployment_id: String,
    pub metadata: HashMap<String, String>,
    pub state_manager: Arc<dyn StateManager>,

    // OpenTelemetry context for trace propagation
    pub otel_context: Option<Context>,

    // Pre-extracted correlation IDs for easy access in logging
    pub trace_id: Option<String>,
    pub span_id: Option<String>,

    // Whether this is a streaming request (for real-time SSE journal export)
    pub is_streaming: bool,
}

impl RuntimeContext {
    /// Create a new RuntimeContext with OpenTelemetry trace context
    pub fn with_trace_context(
        run_id: String,
        service_name: String,
        component_name: String,
        tenant_id: String,
        deployment_id: String,
        metadata: HashMap<String, String>,
        otel_context: Context,
        state_manager: Arc<dyn StateManager>,
        is_streaming: bool,
    ) -> Self {
        // Extract trace_id and span_id for logging correlation
        let (trace_id, span_id) = extract_trace_ids(&otel_context);

        Self {
            run_id,
            service_name,
            component_name,
            tenant_id,
            deployment_id,
            metadata,
            state_manager,
            otel_context: Some(otel_context),
            trace_id,
            span_id,
            is_streaming,
        }
    }

    /// Create a basic RuntimeContext without OpenTelemetry context
    pub fn new(
        run_id: String,
        service_name: String,
        component_name: String,
        tenant_id: String,
        deployment_id: String,
        metadata: HashMap<String, String>,
        state_manager: Arc<dyn StateManager>,
    ) -> Self {
        Self {
            run_id,
            service_name,
            component_name,
            tenant_id,
            deployment_id,
            metadata,
            state_manager,
            otel_context: None,
            trace_id: None,
            span_id: None,
            is_streaming: false,
        }
    }
}

/// Extract trace_id and span_id from OpenTelemetry context for logging
fn extract_trace_ids(ctx: &Context) -> (Option<String>, Option<String>) {
    let span = ctx.span();
    let span_ctx = span.span_context();

    if span_ctx.is_valid() {
        (
            Some(span_ctx.trace_id().to_string()),
            Some(span_ctx.span_id().to_string()),
        )
    } else {
        (None, None)
    }
}

pub struct RuntimeCapabilities {
    pub supports_websockets: bool,
    pub supports_sse: bool,
    pub supports_bidirectional: bool,
    pub max_payload_size: usize,
    pub timeout_seconds: u32,
}

#[async_trait::async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn handle_request(
        &self,
        ctx: &RuntimeContext,
        request: InvocationRequest,
    ) -> Result<InvocationResponse>;

    async fn handle_stream(
        &self,
        ctx: &RuntimeContext,
        request: StreamingInvocationRequest,
    ) -> Result<StreamingInvocationResponse>;
}

/// Dummy state manager for basic testing
pub struct DummyStateManager;

#[async_trait::async_trait]
impl StateManager for DummyStateManager {
    async fn get(&self, _key: String) -> Result<Vec<u8>> {
        Err(crate::error::SdkError::Other(anyhow::anyhow!(
            "State management not implemented"
        )))
    }

    async fn set(&self, _key: String, _value: Vec<u8>) -> Result<()> {
        Err(crate::error::SdkError::Other(anyhow::anyhow!(
            "State management not implemented"
        )))
    }

    async fn delete(&self, _key: String) -> Result<()> {
        Err(crate::error::SdkError::Other(anyhow::anyhow!(
            "State management not implemented"
        )))
    }
}

/// Entity state load result
#[derive(Debug, Clone)]
pub struct EntityStateLoadResult {
    pub found: bool,
    pub state_json: Vec<u8>,
    pub version: i64,
}

/// Entity state save result
#[derive(Debug, Clone)]
pub struct EntityStateSaveResult {
    pub new_version: i64,
}

/// Entity state manager that communicates with Worker Coordinator via gRPC
/// Implements bulk load/save pattern for entity state operations
pub struct EntityStateManager {
    stream_sender: flume::Sender<crate::pb::ServiceMessage>,
    pending_requests:
        Arc<Mutex<HashMap<String, oneshot::Sender<crate::pb::RuntimeServiceResponse>>>>,
    _tenant_id: String,
    session_id: String,
}

impl EntityStateManager {
    /// Create a new EntityStateManager with stream access
    pub fn new(
        stream_sender: flume::Sender<crate::pb::ServiceMessage>,
        tenant_id: String,
        session_id: String,
    ) -> Self {
        Self {
            stream_sender,
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            _tenant_id: tenant_id,
            session_id,
        }
    }

    /// Handle RuntimeServiceResponse from the worker stream
    /// This should be called by the worker when it receives a RuntimeServiceResponse
    pub async fn handle_response(&self, response: crate::pb::RuntimeServiceResponse) {
        let request_id = response.request_id.clone();

        let mut pending = self.pending_requests.lock().await;
        if let Some(sender) = pending.remove(&request_id) {
            let _ = sender.send(response);
        }
    }

    /// Load entire entity state from platform
    pub async fn load_state(
        &self,
        entity_type: String,
        entity_key: String,
    ) -> Result<EntityStateLoadResult> {
        use crate::pb::{
            runtime_service_request, EntityStateLoadRequest, RuntimeServiceRequest, ServiceMessage,
        };

        // Generate unique request ID
        let request_id = uuid::Uuid::new_v4().to_string();

        // Create oneshot channel for response
        let (response_tx, response_rx) = oneshot::channel();

        // Store in pending requests
        self.pending_requests
            .lock()
            .await
            .insert(request_id.clone(), response_tx);

        // Create RuntimeServiceRequest
        let request = RuntimeServiceRequest {
            request_id: request_id.clone(),
            session_id: self.session_id.clone(),
            operation: Some(runtime_service_request::Operation::EntityStateLoad(
                EntityStateLoadRequest {
                    entity_type,
                    entity_key,
                    scope: String::new(),    // Default to global scope
                    scope_id: String::new(), // Empty for global scope
                },
            )),
        };

        // Send via worker stream
        let message = ServiceMessage {
            worker_id: String::new(), // Will be filled by worker
            metadata: HashMap::new(),
            message_type: Some(crate::pb::service_message::MessageType::RuntimeService(
                request,
            )),
        };

        self.stream_sender.send_async(message).await.map_err(|e| {
            crate::error::SdkError::Connection {
                message: format!("Failed to send load request: {}", e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            }
        })?;

        // Wait for response with timeout
        let response = tokio::time::timeout(std::time::Duration::from_secs(10), response_rx)
            .await
            .map_err(|_| {
                crate::error::SdkError::Other(anyhow::anyhow!("Entity state load timeout"))
            })?
            .map_err(|_| {
                crate::error::SdkError::Other(anyhow::anyhow!("Response channel closed"))
            })?;

        // Check if request succeeded
        if !response.success {
            return Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "Entity state load failed: {}",
                response.error_message
            )));
        }

        // Extract result
        match response.result {
            Some(crate::pb::runtime_service_response::Result::EntityStateLoad(result)) => {
                Ok(EntityStateLoadResult {
                    found: result.found,
                    state_json: result.state_json,
                    version: result.version,
                })
            }
            _ => Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "Unexpected response type for entity state load"
            ))),
        }
    }

    /// Save entire entity state to platform with optimistic locking
    pub async fn save_state(
        &self,
        entity_type: String,
        entity_key: String,
        state_json: Vec<u8>,
        expected_version: i64,
    ) -> Result<EntityStateSaveResult> {
        use crate::pb::{
            runtime_service_request, EntityStateSaveRequest, RuntimeServiceRequest, ServiceMessage,
        };

        // Generate unique request ID
        let request_id = uuid::Uuid::new_v4().to_string();

        // Create oneshot channel for response
        let (response_tx, response_rx) = oneshot::channel();

        // Store in pending requests
        self.pending_requests
            .lock()
            .await
            .insert(request_id.clone(), response_tx);

        // Create RuntimeServiceRequest
        let request = RuntimeServiceRequest {
            request_id: request_id.clone(),
            session_id: self.session_id.clone(),
            operation: Some(runtime_service_request::Operation::EntityStateSave(
                EntityStateSaveRequest {
                    entity_type,
                    entity_key,
                    state_json,
                    expected_version,
                    scope: String::new(),    // Default to global scope
                    scope_id: String::new(), // Empty for global scope
                },
            )),
        };

        // Send via worker stream
        let message = ServiceMessage {
            worker_id: String::new(), // Will be filled by worker
            metadata: HashMap::new(),
            message_type: Some(crate::pb::service_message::MessageType::RuntimeService(
                request,
            )),
        };

        self.stream_sender.send_async(message).await.map_err(|e| {
            crate::error::SdkError::Connection {
                message: format!("Failed to send save request: {}", e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            }
        })?;

        // Wait for response with timeout
        let response = tokio::time::timeout(std::time::Duration::from_secs(10), response_rx)
            .await
            .map_err(|_| {
                crate::error::SdkError::Other(anyhow::anyhow!("Entity state save timeout"))
            })?
            .map_err(|_| {
                crate::error::SdkError::Other(anyhow::anyhow!("Response channel closed"))
            })?;

        // Check if request succeeded
        if !response.success {
            return Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "Entity state save failed: {}",
                response.error_message
            )));
        }

        // Extract result
        match response.result {
            Some(crate::pb::runtime_service_response::Result::EntityStateSave(result)) => {
                Ok(EntityStateSaveResult {
                    new_version: result.new_version,
                })
            }
            _ => Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "Unexpected response type for entity state save"
            ))),
        }
    }

    // -------------------------------------------------------------------
    // Session / Message operations (Phase 2 — conversation memory)
    // -------------------------------------------------------------------

    /// Send a message to a conversation thread
    pub async fn send_message(
        &self,
        correlation_id: String,
        message_type: String,
        payload: Vec<u8>,
        from_service: String,
    ) -> Result<String> {
        use crate::pb::{
            runtime_service_request, MessageSendRequest, RuntimeServiceRequest, ServiceMessage,
        };

        let request_id = uuid::Uuid::new_v4().to_string();
        let (response_tx, response_rx) = oneshot::channel();
        self.pending_requests
            .lock()
            .await
            .insert(request_id.clone(), response_tx);

        let request = RuntimeServiceRequest {
            request_id: request_id.clone(),
            session_id: self.session_id.clone(),
            operation: Some(runtime_service_request::Operation::MessageSend(
                MessageSendRequest {
                    correlation_id,
                    message_type,
                    payload,
                    from_service,
                },
            )),
        };

        let message = ServiceMessage {
            worker_id: String::new(),
            metadata: HashMap::new(),
            message_type: Some(crate::pb::service_message::MessageType::RuntimeService(
                request,
            )),
        };

        self.stream_sender.send_async(message).await.map_err(|e| {
            crate::error::SdkError::Connection {
                message: format!("Failed to send message request: {}", e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            }
        })?;

        let response = tokio::time::timeout(std::time::Duration::from_secs(10), response_rx)
            .await
            .map_err(|_| crate::error::SdkError::Other(anyhow::anyhow!("Message send timeout")))?
            .map_err(|_| {
                crate::error::SdkError::Other(anyhow::anyhow!("Response channel closed"))
            })?;

        if !response.success {
            return Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "Message send failed: {}",
                response.error_message
            )));
        }

        match response.result {
            Some(crate::pb::runtime_service_response::Result::MessageSend(result)) => {
                Ok(result.message_id)
            }
            _ => Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "Unexpected response type for message send"
            ))),
        }
    }

    /// List messages in a conversation by correlation ID
    pub async fn list_messages(&self, correlation_id: String, limit: i32) -> Result<Vec<Vec<u8>>> {
        use crate::pb::{
            runtime_service_request, MessageListRequest, RuntimeServiceRequest, ServiceMessage,
        };

        let request_id = uuid::Uuid::new_v4().to_string();
        let (response_tx, response_rx) = oneshot::channel();
        self.pending_requests
            .lock()
            .await
            .insert(request_id.clone(), response_tx);

        let request = RuntimeServiceRequest {
            request_id: request_id.clone(),
            session_id: self.session_id.clone(),
            operation: Some(runtime_service_request::Operation::MessageList(
                MessageListRequest {
                    correlation_id,
                    limit,
                    after_message_id: String::new(),
                },
            )),
        };

        let message = ServiceMessage {
            worker_id: String::new(),
            metadata: HashMap::new(),
            message_type: Some(crate::pb::service_message::MessageType::RuntimeService(
                request,
            )),
        };

        self.stream_sender.send_async(message).await.map_err(|e| {
            crate::error::SdkError::Connection {
                message: format!("Failed to send message list request: {}", e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            }
        })?;

        let response = tokio::time::timeout(std::time::Duration::from_secs(10), response_rx)
            .await
            .map_err(|_| crate::error::SdkError::Other(anyhow::anyhow!("Message list timeout")))?
            .map_err(|_| {
                crate::error::SdkError::Other(anyhow::anyhow!("Response channel closed"))
            })?;

        if !response.success {
            return Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "Message list failed: {}",
                response.error_message
            )));
        }

        match response.result {
            Some(crate::pb::runtime_service_response::Result::MessageList(result)) => {
                Ok(result.messages)
            }
            _ => Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "Unexpected response type for message list"
            ))),
        }
    }

    /// Create a session
    pub async fn create_session(
        &self,
        session_id: String,
        component_name: String,
        session_type: String,
    ) -> Result<String> {
        use crate::pb::{
            runtime_service_request, RuntimeServiceRequest, ServiceMessage, SessionCreateRequest,
        };

        let request_id = uuid::Uuid::new_v4().to_string();
        let (response_tx, response_rx) = oneshot::channel();
        self.pending_requests
            .lock()
            .await
            .insert(request_id.clone(), response_tx);

        let request = RuntimeServiceRequest {
            request_id: request_id.clone(),
            session_id: self.session_id.clone(),
            operation: Some(runtime_service_request::Operation::SessionCreate(
                SessionCreateRequest {
                    session_id,
                    component_name,
                    session_type,
                    state: vec![],
                    metadata: vec![],
                    expires_at_ns: 0,
                },
            )),
        };

        let message = ServiceMessage {
            worker_id: String::new(),
            metadata: HashMap::new(),
            message_type: Some(crate::pb::service_message::MessageType::RuntimeService(
                request,
            )),
        };

        self.stream_sender.send_async(message).await.map_err(|e| {
            crate::error::SdkError::Connection {
                message: format!("Failed to send session create request: {}", e),
                code: crate::error::ErrorCode::ConnectionFailed,
                source: None,
            }
        })?;

        let response = tokio::time::timeout(std::time::Duration::from_secs(10), response_rx)
            .await
            .map_err(|_| crate::error::SdkError::Other(anyhow::anyhow!("Session create timeout")))?
            .map_err(|_| {
                crate::error::SdkError::Other(anyhow::anyhow!("Response channel closed"))
            })?;

        if !response.success {
            return Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "Session create failed: {}",
                response.error_message
            )));
        }

        match response.result {
            Some(crate::pb::runtime_service_response::Result::SessionCreate(result)) => {
                Ok(result.session_id)
            }
            _ => Err(crate::error::SdkError::Other(anyhow::anyhow!(
                "Unexpected response type for session create"
            ))),
        }
    }
}
