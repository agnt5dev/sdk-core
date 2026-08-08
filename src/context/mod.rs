//! Durable Context for AGNT5 SDK
//!
//! The namespaces share runtime-backed durability primitives while preserving
//! small language-neutral public surfaces.

pub mod config;
pub mod namespaces;
pub mod registry;
pub mod timer;

pub use config::ContextConfig;
pub use namespaces::{
    CoreContext, FunctionHandle, FunctionNamespace, FunctionResult, FunctionStatus,
    LanguageModelNamespace, SignalNamespace, TimerNamespace,
};
pub use registry::{FunctionCall, FunctionRegistry, InvocationContext};
pub use timer::TimerActivationClient;
