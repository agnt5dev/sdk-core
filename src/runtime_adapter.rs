use crate::error::Result;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::Context;
use std::collections::HashMap;
use std::sync::Arc;

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
