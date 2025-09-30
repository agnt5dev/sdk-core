//! Next-generation durable Context implementation scaffolding.
//!
//! This module currently exposes placeholders that will be wired to the
//! runtime service and FFI bridge in subsequent iterations. The initial
//! structure mirrors the namespaces available to language SDKs so that tests
//! and wrappers can begin integrating against stable type signatures.

pub mod config;
pub mod namespaces;

pub use config::ContextConfig;
pub use namespaces::{
    CoreContext, LlmNamespace, SignalNamespace, TaskHandle, TaskNamespace, TaskRequest,
    TimerNamespace,
};
