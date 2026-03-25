use std::{sync::Arc, time::Duration};

use serde_json::Value;
use uuid::Uuid;

use crate::{
    adk::runtime_client::RuntimeServiceClient,
    error::{Result, SdkError},
};

use super::config::ContextConfig;
use super::registry::{FunctionCall, InvocationContext};

#[derive(Debug, Clone)]
pub struct CoreContext {
    state: Arc<ContextState>,
}

#[derive(Debug)]
struct ContextState {
    client: Option<Arc<RuntimeServiceClient>>,
    config: ContextConfig,
}

impl ContextState {
    fn function_registry(&self) -> Arc<super::registry::FunctionRegistry> {
        Arc::clone(&self.config.function_registry)
    }
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

    pub async fn call(&self, request: FunctionCall) -> Result<FunctionHandle> {
        let registry = self.state.function_registry();
        let invocation_id = Uuid::new_v4().to_string();
        let invocation_ctx = InvocationContext::from(&self.state.config);
        let original_request = request.clone();

        match registry.invoke(request, invocation_ctx).await {
            Ok(output) => Ok(FunctionHandle::succeeded(
                original_request,
                invocation_id,
                output,
            )),
            Err(err) => match &err {
                SdkError::InvalidArgument { .. } => Err(err),
                _ => Ok(FunctionHandle::failed(
                    original_request,
                    invocation_id,
                    err.to_string(),
                )),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct FunctionHandle {
    result: Arc<FunctionResult>,
}

impl FunctionHandle {
    pub async fn result(&self) -> FunctionResult {
        (*self.result).clone()
    }

    pub fn status(&self) -> FunctionStatus {
        self.result.status
    }

    fn succeeded(request: FunctionCall, invocation_id: String, output: Value) -> Self {
        let result = FunctionResult {
            request,
            invocation_id,
            status: FunctionStatus::Succeeded,
            output: Some(output),
            error: None,
        };
        Self {
            result: Arc::new(result),
        }
    }

    fn failed(request: FunctionCall, invocation_id: String, error: String) -> Self {
        let result = FunctionResult {
            request,
            invocation_id,
            status: FunctionStatus::Failed,
            output: None,
            error: Some(error),
        };
        Self {
            result: Arc::new(result),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionStatus {
    Pending,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone)]
pub struct FunctionResult {
    pub request: FunctionCall,
    pub invocation_id: String,
    pub status: FunctionStatus,
    pub output: Option<Value>,
    pub error: Option<String>,
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
        Err(SdkError::Unavailable {
            message: "Signal waiting not yet implemented".to_string(),
            service: None,
        })
    }

    pub async fn emit(&self, name: &str, payload: Value) -> Result<()> {
        let _ = (&self.state, name, &payload);
        Err(SdkError::Unavailable {
            message: "Signal emission not yet implemented".to_string(),
            service: None,
        })
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
        Err(SdkError::Unavailable {
            message: "Durable sleep not yet implemented".to_string(),
            service: None,
        })
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
        Err(SdkError::Unavailable {
            message: "Language model generation not yet implemented".to_string(),
            service: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use crate::context::config::ContextConfig;
    use crate::context::registry::{FunctionCall, FunctionRegistry};
    use crate::error::SdkError;

    use super::{CoreContext, FunctionStatus};

    #[test]
    fn context_stores_configuration() {
        let cfg =
            ContextConfig::new("tenant", "session", "run", "step", 0).with_invocation_id("invoke");
        let ctx = CoreContext::new(None, cfg.clone());

        assert_eq!(ctx.config().invocation_id, cfg.invocation_id);
        assert_eq!(ctx.config().tenant_id, cfg.tenant_id);
    }

    #[tokio::test]
    async fn function_namespace_invokes_registered_function() {
        let registry = Arc::new(FunctionRegistry::new());
        registry.register("analytics", "process", |call, ctx| async move {
            assert_eq!(ctx.run_id, "run");
            assert!(call.metadata.get("corr_id").is_some());
            Ok(json!({
                "echo": call.payload,
            }))
        });

        let cfg = ContextConfig::new("tenant", "session", "run", "step", 0)
            .with_function_registry(Arc::clone(&registry));
        let ctx = CoreContext::new(None, cfg);
        let request = FunctionCall::new("analytics", "process", json!({"foo": "bar"}))
            .with_metadata("corr_id", "123");

        let handle = ctx.functions().call(request).await.expect("call succeeds");
        assert_eq!(handle.status(), FunctionStatus::Succeeded);

        let result = handle.result().await;
        assert_eq!(result.status, FunctionStatus::Succeeded);
        assert_eq!(result.output.unwrap()["echo"]["foo"], "bar");
        assert!(!result.invocation_id.is_empty());
    }

    #[tokio::test]
    async fn function_namespace_handles_handler_error() {
        let registry = Arc::new(FunctionRegistry::new());
        registry.register("svc", "fail", |_call, _ctx| async move {
            Err(crate::error::SdkError::Invocation {
                message: "boom".into(),
                function_name: None,
            })
        });

        let cfg = ContextConfig::new("tenant", "session", "run", "step", 0)
            .with_function_registry(Arc::clone(&registry));
        let ctx = CoreContext::new(None, cfg);
        let request = FunctionCall::new("svc", "fail", json!({}));

        let handle = ctx.functions().call(request).await.expect("call succeeds");
        let result = handle.result().await;
        assert_eq!(result.status, FunctionStatus::Failed);
        assert!(result.error.unwrap().contains("boom"));
    }

    #[tokio::test]
    async fn function_namespace_errors_when_unregistered() {
        let cfg = ContextConfig::new("tenant", "session", "run", "step", 0);
        let ctx = CoreContext::new(None, cfg);
        let request = FunctionCall::new("missing", "handler", json!({}));

        let err = ctx.functions().call(request).await;
        assert!(matches!(err, Err(SdkError::InvalidArgument { .. })));
    }
}
