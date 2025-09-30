use std::collections::HashMap;
use std::sync::Arc;

use super::registry::FunctionRegistry;

/// Identifiers and options required to bootstrap a durable Context instance.
#[derive(Clone)]
pub struct ContextConfig {
    pub tenant_id: String,
    pub session_id: String,
    pub run_id: String,
    pub step_id: String,
    pub attempt: u32,
    pub invocation_id: Option<String>,
    pub metadata: HashMap<String, String>,
    pub function_registry: Arc<FunctionRegistry>,
}

impl ContextConfig {
    pub fn new(
        tenant_id: impl Into<String>,
        session_id: impl Into<String>,
        run_id: impl Into<String>,
        step_id: impl Into<String>,
        attempt: u32,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            session_id: session_id.into(),
            run_id: run_id.into(),
            step_id: step_id.into(),
            attempt,
            invocation_id: None,
            metadata: HashMap::new(),
            function_registry: Arc::new(FunctionRegistry::new()),
        }
    }

    pub fn with_invocation_id(mut self, invocation_id: impl Into<String>) -> Self {
        self.invocation_id = Some(invocation_id.into());
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

    pub fn with_function_registry(mut self, registry: Arc<FunctionRegistry>) -> Self {
        self.function_registry = registry;
        self
    }
}

impl std::fmt::Debug for ContextConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextConfig")
            .field("tenant_id", &self.tenant_id)
            .field("session_id", &self.session_id)
            .field("run_id", &self.run_id)
            .field("step_id", &self.step_id)
            .field("attempt", &self.attempt)
            .field("invocation_id", &self.invocation_id)
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl Default for ContextConfig {
    fn default() -> Self {
        Self {
            tenant_id: String::new(),
            session_id: String::new(),
            run_id: String::new(),
            step_id: String::new(),
            attempt: 0,
            invocation_id: None,
            metadata: HashMap::new(),
            function_registry: Arc::new(FunctionRegistry::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ContextConfig;

    #[test]
    fn builder_style_helpers_set_fields() {
        let cfg = ContextConfig::new("tenant", "session", "run", "step", 1)
            .with_invocation_id("invoke")
            .with_metadata("region", "us-west");

        assert_eq!(cfg.tenant_id, "tenant");
        assert_eq!(cfg.session_id, "session");
        assert_eq!(cfg.run_id, "run");
        assert_eq!(cfg.step_id, "step");
        assert_eq!(cfg.attempt, 1);
        assert_eq!(cfg.invocation_id.as_deref(), Some("invoke"));
        assert_eq!(
            cfg.metadata.get("region").map(String::as_str),
            Some("us-west")
        );
    }
}
