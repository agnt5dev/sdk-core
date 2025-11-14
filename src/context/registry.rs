use std::collections::HashMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use serde_json::Value;

use crate::context::config::ContextConfig;
use crate::error::{Result, SdkError};

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

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[derive(Debug, Clone)]
pub struct InvocationContext {
    pub tenant_id: String,
    pub session_id: String,
    pub run_id: String,
    pub step_id: String,
    pub attempt: u32,
    pub parent_invocation_id: Option<String>,
}

impl From<&ContextConfig> for InvocationContext {
    fn from(config: &ContextConfig) -> Self {
        Self {
            tenant_id: config.tenant_id.clone(),
            session_id: config.session_id.clone(),
            run_id: config.run_id.clone(),
            step_id: config.step_id.clone(),
            attempt: config.attempt,
            parent_invocation_id: config.invocation_id.clone(),
        }
    }
}

pub struct FunctionRegistry {
    handlers: Mutex<HashMap<FunctionKey, FunctionCallback>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            handlers: Mutex::new(HashMap::new()),
        }
    }

    pub fn register<F, Fut>(&self, service: impl Into<String>, handler: impl Into<String>, func: F)
    where
        F: Fn(FunctionCall, InvocationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        let key = FunctionKey::new(service.into(), handler.into());
        let callback: FunctionCallback = Arc::new(move |call, ctx| {
            let fut = func(call, ctx);
            Box::pin(fut)
        });

        let mut handlers = self
            .handlers
            .lock()
            .expect("function registry lock poisoned");
        handlers.insert(key, callback);
    }

    pub fn unregister(&self, service: &str, handler: &str) {
        let mut handlers = self
            .handlers
            .lock()
            .expect("function registry lock poisoned");
        handlers.remove(&FunctionKey::new(service.to_string(), handler.to_string()));
    }

    pub async fn invoke(&self, call: FunctionCall, ctx: InvocationContext) -> Result<Value> {
        let key = FunctionKey::new(call.target_service.clone(), call.handler.clone());
        let handler = {
            let handlers = self
                .handlers
                .lock()
                .expect("function registry lock poisoned");
            handlers.get(&key).cloned()
        };

        let callback = handler.ok_or_else(|| {
            SdkError::InvalidArgument {
                message: format!(
                    "no function registered for {}::{}",
                    call.target_service, call.handler
                ),
                argument: Some("handler".to_string()),
            }
        })?;

        callback(call, ctx).await
    }
}

type FunctionCallback =
    Arc<dyn Fn(FunctionCall, InvocationContext) -> BoxFuture<'static, Result<Value>> + Send + Sync>;

#[derive(Debug, Clone, Eq)]
struct FunctionKey {
    service: String,
    handler: String,
}

impl FunctionKey {
    fn new(service: String, handler: String) -> Self {
        Self { service, handler }
    }
}

impl PartialEq for FunctionKey {
    fn eq(&self, other: &Self) -> bool {
        self.service == other.service && self.handler == other.handler
    }
}

impl Hash for FunctionKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.service.hash(state);
        self.handler.hash(state);
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for FunctionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.handlers.lock().map(|h| h.len()).unwrap_or_default();
        f.debug_struct("FunctionRegistry")
            .field("handlers", &len)
            .finish()
    }
}
