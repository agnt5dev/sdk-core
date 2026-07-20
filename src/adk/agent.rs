//! Lightweight agent handle for the ADK.

#[derive(Debug, Clone, Default)]
pub struct AgentHandle;

impl AgentHandle {
    /// Create an agent handle.
    pub fn new_placeholder() -> Self {
        Self
    }
}
