// AGNT5 SDK Core - Simple, focused foundation for worker coordination

pub mod adk;
pub mod client;
pub mod context;
pub mod error;
pub mod llm;
pub mod lm;
pub mod logging;
pub mod runtime_adapter;
pub mod telemetry;
pub mod worker;

// Re-export main types
pub use adk::{
    AgentHandle, ContextHandle, DeterministicUtils, InMemoryMemoryBackend, InMemorySessionBackend,
    MemoryHandle, MemoryItem, RuntimeControls, RuntimeServiceClient, SessionEvent, SessionHandle,
    SessionStateHandle, SessionStateScope, SignalControls, TaskControls, TimerControls,
    ToolDefinition, ToolHandle, ToolRegistry,
};
pub use client::WorkerCoordinatorClient;
pub use context::{
    ContextConfig, CoreContext, FunctionCall, FunctionHandle, FunctionNamespace, FunctionRegistry,
    FunctionResult, FunctionStatus, LanguageModelNamespace, SignalNamespace, TimerNamespace,
};
pub use error::{Result, SdkError};
pub use llm::LlmClient;
pub use lm::{
    generate, stream, AnthropicConfig, AnthropicProvider, AzureOpenAiConfig, AzureOpenAiProvider,
    BedrockConfig, BedrockProvider, GenerateRequest, GenerateResponse, GenerationConfig,
    GroqConfig, GroqProvider, JsonSchemaFormat, LanguageModel, Message, MessageRole, OpenAiConfig,
    OpenAiProvider, OpenRouterConfig, OpenRouterProvider, ResponseFormat, StreamChunk,
    StreamHandle, StreamRequest, TokenUsage, ToolChoice,
};
pub use logging::{clear_error_buffer, get_error_buffer, init_logging};
pub use runtime_adapter::{
    DummyStateManager, InvocationRequest, InvocationResponse, RuntimeAdapter, RuntimeCapabilities,
    RuntimeContext, StateManager,
};
pub use telemetry::{
    create_component_span, create_function_span, end_span, extract_context_from_runtime_message,
    init_telemetry, record_span_error, record_span_success, shutdown_telemetry,
};
pub use worker::Worker;

// Re-export flume for language bindings
pub use flume;

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
