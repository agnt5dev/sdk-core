//! Placeholder agent handle for the ADK.
//!
//! The real implementation will encapsulate durable runtime wiring and language
//! model orchestration. For now we expose a lightweight type so other modules
//! can depend on the early scaffolding without pulling in unfinished details.

#[derive(Debug, Clone, Default)]
pub struct AgentHandle;

impl AgentHandle {
    /// Temporary constructor used while the detailed agent implementation is
    /// under development.
    pub fn new_placeholder() -> Self {
        Self
    }
}
