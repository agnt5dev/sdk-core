// AGNT5 SDK Core - Simple, focused foundation for worker coordination

pub mod adk;
pub mod chat;
pub mod client;
pub mod context;
pub mod error;
pub mod eval;
pub mod journal_queue;
pub mod lm;
pub mod logging;
pub mod mcp;
pub mod memory_service;
pub mod runtime_adapter;
pub mod sandbox;
pub mod span_filter;
pub mod telemetry;
pub mod webhook;
pub mod worker;

// Re-export main types
pub use adk::{
    AgentHandle, ContextHandle, DeterministicUtils, RuntimeControls, RuntimeServiceClient,
    SignalControls, TaskControls, TimerControls, ToolDefinition, ToolHandle, ToolRegistry,
};
pub use client::{build_engine_record, CheckpointResult, EngineClient, WorkerCoordinatorClient};
pub use context::{
    ContextConfig, CoreContext, FunctionCall, FunctionHandle, FunctionNamespace, FunctionRegistry,
    FunctionResult, FunctionStatus, LanguageModelNamespace, SignalNamespace, TimerNamespace,
};
pub use error::{Result, SdkError};
pub use eval::{
    contains, exact_match, json_valid, levenshtein, llm_judge, regex_match, trace_score,
    ContainsConfig, ExactMatchConfig, LevenshteinConfig, LlmJudgeConfig, RegexConfig, ScorerInput,
    ScorerResult, TraceAssertion, TraceEvent,
};
pub use journal_queue::{
    JournalEventMessage, JournalEventQueue, JournalQueueConfig, JournalQueueMetrics,
};
pub use lm::{
    generate, stream, AnthropicConfig, AnthropicProvider, AzureOpenAiConfig, AzureOpenAiProvider,
    BasetenConfig, BasetenProvider, BedrockConfig, BedrockProvider, BuiltInTool, Embedder,
    EmbedderRegistry, FireworksConfig, FireworksProvider, GenerateRequest, GenerateResponse,
    GenerationConfig, GroqConfig, GroqProvider, JsonSchemaFormat, LanguageModel, LeptonConfig,
    LeptonProvider, Message, MessageRole, Modality, OpenAiChatConfig, OpenAiChatProvider,
    OpenAiConfig, OpenAiEmbedder, OpenAiEmbedderConfig, OpenAiEmbeddingModel, OpenAiProvider,
    OpenRouterConfig, OpenRouterProvider, PromptRef, ReasoningEffort, ResponseFormat, StreamChunk,
    StreamHandle, StreamRequest, TogetherConfig, TogetherProvider, TokenUsage, ToolChoice,
};
pub use logging::{clear_error_buffer, get_error_buffer, init_logging};
pub use runtime_adapter::{
    DummyStateManager, EntityStateLoadResult, EntityStateManager, EntityStateSaveResult,
    InvocationRequest, InvocationResponse, RuntimeAdapter, RuntimeCapabilities, RuntimeContext,
    StateManager,
};
pub use sandbox::{
    CreateSandboxOptions, DaytonaProviderConfig, DaytonaSandbox, DaytonaSandboxProvider,
    E2bProviderConfig, E2bSandbox, E2bSandboxProvider, ExecuteCodeRequest, ExecuteCodeResult,
    FileInfo, Language, ListFilesResult, ModalProviderConfig, ModalSandbox, ModalSandboxProvider,
    NorthflankProviderConfig, NorthflankSandbox, NorthflankSandboxProvider, ReadFileResult,
    RemoteSandbox, RemoteSandboxConfig, SandboxAuth, SandboxBackend, SandboxBackendKind,
    SandboxCapabilities, SandboxExecutor, SandboxHealthResult, SandboxInfo, SandboxProvider,
    SandboxRegistry, SandboxWorkspace, StreamEvent, TogetherProviderConfig, TogetherSandbox,
    TogetherSandboxProvider, VercelProviderConfig, VercelSandbox, VercelSandboxProvider,
    WriteFileRequest, WriteFileResult,
};
#[cfg(feature = "wasm-sandbox")]
pub use sandbox::{WasmSandbox, WasmSandboxConfig};
pub use telemetry::{
    create_component_span, create_function_span, create_sandbox_span, end_span,
    extract_context_from_runtime_message, init_telemetry, record_execution_request,
    record_execution_request_with_attrs, record_sandbox_error, record_sandbox_success,
    record_span_error, record_span_success, record_worker_memory_bytes, shutdown_telemetry,
};
pub use worker::Worker;

// MCP (Model Context Protocol) support
pub use mcp::{
    McpClient, McpError, McpResult, McpTool, McpToolWithServer, ServerConfig, SseConfig,
    StdioConfig, Transport,
};

// Re-export flume for language bindings
pub use flume;

// Generated protobuf modules
pub mod pb {
    tonic::include_proto!("api.v1");
}

// Re-export checkpoint types for language bindings
pub use pb::CheckpointType;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
