//! Context scaffolding for the ADK.
//!
//! Structured namespaces expose placeholder implementations that will be wired
//! to the durable runtime in future milestones.

pub mod runtime;
pub mod signals;
pub mod tasks;
pub mod timers;
pub mod utils;

use std::sync::Arc;

use crate::adk::runtime_client::RuntimeServiceClient;

#[derive(Debug, Clone)]
pub struct ContextRuntimeConfig {
    pub client: Arc<RuntimeServiceClient>,
    pub tenant_id: String,
    pub session_id: String,
    pub run_id: String,
    pub step_id: String,
    pub attempt: i32,
    pub invocation_id: Option<String>,
}

impl ContextRuntimeConfig {
    pub fn new(
        client: Arc<RuntimeServiceClient>,
        tenant_id: impl Into<String>,
        session_id: impl Into<String>,
        run_id: impl Into<String>,
        step_id: impl Into<String>,
        attempt: i32,
    ) -> Self {
        Self {
            client,
            tenant_id: tenant_id.into(),
            session_id: session_id.into(),
            run_id: run_id.into(),
            step_id: step_id.into(),
            attempt,
            invocation_id: None,
        }
    }

    pub fn with_invocation_id(mut self, invocation_id: impl Into<String>) -> Self {
        self.invocation_id = Some(invocation_id.into());
        self
    }
}

#[derive(Debug)]
pub(crate) struct ContextRuntimeState {
    pub client: Arc<RuntimeServiceClient>,
    pub tenant_id: String,
    pub session_id: String,
    pub run_id: String,
    pub step_id: String,
    pub attempt: i32,
    pub invocation_id: Option<String>,
}

impl ContextRuntimeState {
    fn new(config: ContextRuntimeConfig) -> Arc<Self> {
        Arc::new(Self {
            client: config.client,
            tenant_id: config.tenant_id,
            session_id: config.session_id,
            run_id: config.run_id,
            step_id: config.step_id,
            attempt: config.attempt,
            invocation_id: config.invocation_id,
        })
    }
}

pub use runtime::RuntimeControls;
pub use signals::SignalControls;
pub use tasks::TaskControls;
pub use timers::TimerControls;
pub use utils::DeterministicUtils;

/// Shared placeholder context handle that encapsulates logical namespaces.
#[derive(Debug, Clone, Default)]
pub struct ContextHandle {
    runtime: RuntimeControls,
    signals: SignalControls,
    tasks: TaskControls,
    timers: TimerControls,
    utils: DeterministicUtils,
}

impl ContextHandle {
    /// Create a placeholder context handle.
    pub fn new_placeholder() -> Self {
        Self::default()
    }

    pub fn new_runtime(config: ContextRuntimeConfig) -> Self {
        let state = ContextRuntimeState::new(config);
        Self {
            runtime: RuntimeControls::with_state(Some(Arc::clone(&state))),
            signals: SignalControls::with_state(Some(Arc::clone(&state))),
            tasks: TaskControls::with_state(Some(Arc::clone(&state))),
            timers: TimerControls::with_state(Some(state)),
            utils: DeterministicUtils::default(),
        }
    }

    pub fn runtime(&self) -> RuntimeControls {
        self.runtime.clone()
    }

    pub fn signals(&self) -> SignalControls {
        self.signals.clone()
    }

    pub fn tasks(&self) -> TaskControls {
        self.tasks.clone()
    }

    pub fn timers(&self) -> TimerControls {
        self.timers.clone()
    }

    pub fn utils(&self) -> DeterministicUtils {
        self.utils.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_return_errors() {
        let ctx = ContextHandle::new_placeholder();
        assert!(ctx.runtime().checkpoint(None).is_err());
        assert!(ctx.runtime().fail("oops").is_err());
        assert!(ctx.signals().wait("sig", None).is_err());
        assert!(ctx.tasks().spawn("task").is_err());
        assert!(ctx
            .timers()
            .sleep(std::time::Duration::from_secs(1))
            .is_err());
    }

    #[test]
    fn utils_now_returns_timestamp() {
        let ctx = ContextHandle::new_placeholder();
        let now = ctx.utils().now().unwrap();
        assert!(now > 0);
    }
}
