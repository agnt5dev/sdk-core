use std::collections::HashMap;
use std::sync::Arc;
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct InvocationRequest {
    pub invocation_id: String,
    pub service_name: String,
    pub handler_name: String,
    pub input_data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct InvocationResponse {
    pub invocation_id: String,
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
    pub invocation_id: String,
    pub service_name: String,
    pub component_name: String,
    pub tenant_id: String,
    pub deployment_id: String,
    pub metadata: HashMap<String, String>,
    pub state_manager: Arc<dyn StateManager>,
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
        Err(crate::error::SdkError::Other(anyhow::anyhow!("State management not implemented")))
    }

    async fn set(&self, _key: String, _value: Vec<u8>) -> Result<()> {
        Err(crate::error::SdkError::Other(anyhow::anyhow!("State management not implemented")))
    }

    async fn delete(&self, _key: String) -> Result<()> {
        Err(crate::error::SdkError::Other(anyhow::anyhow!("State management not implemented")))
    }
}
