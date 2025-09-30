use std::{collections::HashMap, sync::Arc, time::Duration};

use serde_json::{self, Value};
use uuid::Uuid;

use crate::{
    adk::runtime_client::RuntimeServiceClient,
    error::{Result, SdkError},
    pb::{
        runtime_service_request::Operation as RuntimeOperation,
        runtime_service_response::Result as RuntimeResult, RuntimeServiceRequest, TaskSpawnRequest,
    },
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

    pub fn functions(&self) -> FunctionNamespace {
        FunctionNamespace::new(self.state.clone())
    }

    pub fn signals(&self) -> SignalNamespace {
        SignalNamespace::new(self.state.clone())
    }

    pub fn timers(&self) -> TimerNamespace {
        TimerNamespace::new(self.state.clone())
    }

    pub fn language_model(&self) -> LanguageModelNamespace {
        LanguageModelNamespace::new(self.state.clone())
    }
}

#[derive(Debug, Clone)]
pub struct FunctionNamespace {
    state: Arc<ContextState>,
}

impl FunctionNamespace {
    fn new(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    fn state(&self) -> Result<Arc<ContextState>> {
        if self.state.client.is_none() {
            return Err(SdkError::Unavailable(
                "runtime client not available for function namespace".into(),
            ));
        }

        Ok(self.state.clone())
    }

    pub async fn call(&self, request: FunctionCall) -> Result<FunctionHandle> {
        let state = self.state()?;
        let client = state.client.as_ref().ok_or_else(|| {
            SdkError::Unavailable("runtime client missing for function call".into())
        })?;

        let payload = serde_json::to_vec(&request.payload)?;
        let invocation_id = Uuid::new_v4().to_string();
        let dedupe_id = request.key.clone().unwrap_or_else(|| {
            format!(
                "{}:{}:{}:{}",
                state.config.run_id, state.config.step_id, request.target_service, request.handler
            )
        });

        let mut metadata: HashMap<String, String> = state.config.metadata.clone();
        for (key, value) in &request.metadata {
            metadata.insert(key.clone(), value.clone());
        }
        metadata.insert("target_service".into(), request.target_service.clone());
        metadata.insert("handler".into(), request.handler.clone());
        metadata.insert("attempt".into(), state.config.attempt.to_string());
        metadata.insert("run_id".into(), state.config.run_id.clone());
        metadata.insert("step_id".into(), state.config.step_id.clone());
        if let Some(parent) = &state.config.invocation_id {
            metadata.insert("parent_invocation_id".into(), parent.clone());
        }

        let task_target = format!("{}::{}", request.target_service, request.handler);

        let runtime_request = RuntimeServiceRequest {
            request_id: String::new(),
            tenant_id: state.config.tenant_id.clone(),
            session_id: state.config.session_id.clone(),
            operation: Some(RuntimeOperation::TaskSpawn(TaskSpawnRequest {
                run_id: state.config.run_id.clone(),
                step_id: state.config.step_id.clone(),
                task_target,
                invocation_id: invocation_id.clone(),
                payload,
                dedupe_id: dedupe_id.clone(),
                metadata,
            })),
        };

        let response = client.request(runtime_request).await?;

        match response.result {
            Some(RuntimeResult::TaskSpawn(result)) => Ok(FunctionHandle {
                request,
                invocation_id: result.invocation_id,
                dedupe_id,
                status: if result.status.is_empty() {
                    "ENQUEUED".to_string()
                } else {
                    result.status
                },
            }),
            _ => Err(SdkError::Internal(
                "unexpected runtime response for function invocation".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCall {
    pub target_service: String,
    pub handler: String,
    pub payload: Value,
    pub key: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl FunctionCall {
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
            metadata: HashMap::new(),
        }
    }

    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_metadata<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionHandle {
    pub request: FunctionCall,
    pub invocation_id: String,
    pub dedupe_id: String,
    pub status: String,
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
pub struct LanguageModelNamespace {
    state: Arc<ContextState>,
}

impl LanguageModelNamespace {
    fn new(state: Arc<ContextState>) -> Self {
        Self { state }
    }

    pub async fn generate(&self, _request: serde_json::Value) -> Result<Value> {
        let _ = &self.state;
        Err(SdkError::Unavailable(
            "Language model generation not yet implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::context::config::ContextConfig;

    use super::{CoreContext, FunctionCall};

    #[test]
    fn context_stores_configuration() {
        let cfg =
            ContextConfig::new("tenant", "session", "run", "step", 0).with_invocation_id("invoke");
        let ctx = CoreContext::new(None, cfg.clone());

        assert_eq!(ctx.config().invocation_id, cfg.invocation_id);
        assert_eq!(ctx.config().tenant_id, cfg.tenant_id);
    }

    #[tokio::test]
    async fn function_namespace_returns_placeholder_error() {
        let cfg = ContextConfig::new("tenant", "session", "run", "step", 0);
        let ctx = CoreContext::new(None, cfg);
        let request = FunctionCall::new("analytics", "process", json!({"foo": "bar"}));
        let result = ctx.functions().call(request).await;

        assert!(result.is_err());
    }

    #[test]
    fn function_call_builder_supports_metadata() {
        let call = FunctionCall::new("svc", "handler", json!({}))
            .with_key("dedupe")
            .with_metadata("priority", "high");

        assert_eq!(call.key.as_deref(), Some("dedupe"));
        assert_eq!(call.metadata.get("priority"), Some(&"high".to_string()));
    }
}
