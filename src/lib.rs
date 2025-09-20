// AGNT5 SDK Core - Simple, focused foundation for worker coordination

pub mod client;
pub mod error;
pub mod llm;
pub mod logging;
pub mod runtime_adapter;
pub mod telemetry;
pub mod worker;

// Re-export main types
pub use client::WorkerCoordinatorClient;
pub use error::{Result, SdkError};
pub use llm::LlmClient;
pub use logging::{clear_error_buffer, get_error_buffer, init_logging};
pub use runtime_adapter::{
    DummyStateManager, InvocationRequest, InvocationResponse, RuntimeAdapter, RuntimeCapabilities,
    RuntimeContext, StateManager,
};
pub use telemetry::{
    create_function_span, end_span, extract_context_from_runtime_message, init_telemetry,
    record_span_error, record_span_success, shutdown_telemetry,
};
pub use worker::Worker;

// Generated protobuf modules
pub mod pb {
    tonic::include_proto!("api.v1");
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
