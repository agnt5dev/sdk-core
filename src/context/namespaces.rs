use std::{sync::Arc, time::Duration};

use serde_json::Value;

use crate::{
    adk::runtime_client::RuntimeServiceClient,
    error::{Result, SdkError},
};

use super::config::ContextConfig;

#[derive(Debug, Clone)]
pub struct CoreContext {
    state: Arc<ContextState>,
}

#[derive(Debug)]
struct ContextState {
    client: Option<Arc<RuntimeServiceClient>>,
    config: ContextConfig,
}

impl CoreContext {
    pub fn new(client: Option<Arc<RuntimeServiceClient>>, config: ContextConfig) -> Self {
        let state = ContextState { client, config };
        Self {
            state: Arc::new(state),
        }
    }

    pub fn with_runtime(client: Arc<RuntimeServiceClient>, config: ContextConfig) -> Self {
        Self::new(Some(client), config)
    }

    pub fn config(&self) -> &ContextConfig {
        &self.state.config
    }

    pub fn runtime_client(&self) -> Option<&Arc<RuntimeServiceClient>> {
        self.state.client.as_ref()
    }

    pub fn tasks(&self) -> TaskNamespace {
        TaskNamespace::new(self.state.clone())
    }

    pub fn signals(&self) -> SignalNamespace {
        SignalNamespace::new(self.state.clone())
    }

    pub fn timers(&self) -> TimerNamespace {
        TimerNamespace::new(self.state.clone())
    }

    pub fn llm(&self) -> LlmNamespace {
        LlmNamespace::new(self.state.clone())
    }
}

#[derive(Debug, Clone)]
pub struct TaskNamespace {
    state: Arc<ContextState>,
}

impl TaskNamespace {
    fn new(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    pub async fn call(&self, request: TaskRequest) -> Result<TaskHandle> {
        let _ = (&self.state, &request);
        Err(SdkError::Unavailable(
            "Task orchestration not yet implemented".to_string(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskRequest {
    pub target_service: String,
    pub handler: String,
    pub payload: Value,
    pub key: Option<String>,
}

impl TaskRequest {
    pub fn new(
        target_service: impl Into<String>,
        handler: impl Into<String>,
        payload: Value,
    ) -> Self {
        Self {
            target_service: target_service.into(),
            handler: handler.into(),
            payload,
            key: None,
        }
    }

    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskHandle {
    pub request: TaskRequest,
}

#[derive(Debug, Clone)]
pub struct SignalNamespace {
    state: Arc<ContextState>,
}

impl SignalNamespace {
    fn new(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    pub async fn wait(&self, name: &str) -> Result<Value> {
        let _ = (&self.state, name);
        Err(SdkError::Unavailable(
            "Signal waiting not yet implemented".to_string(),
        ))
    }

    pub async fn emit(&self, name: &str, payload: Value) -> Result<()> {
        let _ = (&self.state, name, &payload);
        Err(SdkError::Unavailable(
            "Signal emission not yet implemented".to_string(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct TimerNamespace {
    state: Arc<ContextState>,
}

impl TimerNamespace {
    fn new(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    pub async fn sleep(&self, duration: Duration) -> Result<()> {
        let _ = (&self.state, duration);
        Err(SdkError::Unavailable(
            "Durable sleep not yet implemented".to_string(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct LlmNamespace {
    state: Arc<ContextState>,
}

impl LlmNamespace {
    fn new(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    pub async fn generate(&self, _request: serde_json::Value) -> Result<Value> {
        let _ = &self.state;
        Err(SdkError::Unavailable(
            "LLM generation not yet implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::context::config::ContextConfig;

    use super::{CoreContext, TaskRequest};

    #[test]
    fn context_stores_configuration() {
        let cfg = ContextConfig::new("tenant", "session", "run", 0).with_invocation_id("invoke");
        let ctx = CoreContext::new(None, cfg.clone());

        assert_eq!(ctx.config().invocation_id, cfg.invocation_id);
        assert_eq!(ctx.config().tenant_id, cfg.tenant_id);
    }

    #[tokio::test]
    async fn task_namespace_returns_placeholder_error() {
        let cfg = ContextConfig::new("tenant", "session", "run", 0);
        let ctx = CoreContext::new(None, cfg);
        let request = TaskRequest::new("analytics", "process", json!({"foo": "bar"}));
        let result = ctx.tasks().call(request).await;

        assert!(result.is_err());
    }
}
