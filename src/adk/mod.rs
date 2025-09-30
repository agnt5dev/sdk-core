//! Agent Development Toolkit (ADK) core module scaffolding.
//!
//! This module tree currently provides placeholder types that will be expanded as
//! the ADK implementation progresses. Keeping the structure in place allows the
//! Python bindings and higher-level layers to compile while detailed behaviour is
//! implemented incrementally.

pub mod agent;
pub mod context;
pub mod memory;
pub mod runtime_client;
pub mod session;
pub mod tool;

pub use agent::AgentHandle;
pub use context::{
    ContextHandle, ContextRuntimeConfig, DeterministicUtils, RuntimeControls, SignalControls,
    TaskControls, TimerControls,
};
pub use memory::{InMemoryMemoryBackend, MemoryBackend, MemoryHandle, MemoryItem};
pub use runtime_client::RuntimeServiceClient;
pub use session::{
    InMemorySessionBackend, SessionEvent, SessionHandle, SessionStateHandle, SessionStateScope,
};
pub use tool::{ToolDefinition, ToolHandle, ToolRegistry};
