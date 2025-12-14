// AGNT5 SDK Core - Simple, focused foundation for worker coordination

pub mod adk;
pub mod checkpoint;
pub mod client;
pub mod span_export_queue;
pub mod context;
pub mod error;
pub mod journal_exporter;
pub mod lm;
pub mod logging;
pub mod runtime_adapter;
pub mod span_filter;
pub mod telemetry;
pub mod vectordb;
pub mod worker;

// Re-export main types
pub use adk::{
    AgentHandle, ContextHandle, DeterministicUtils, RuntimeControls, RuntimeServiceClient,
    SignalControls, TaskControls, TimerControls, ToolDefinition, ToolHandle, ToolRegistry,
};
pub use checkpoint::{CheckpointMessage, CheckpointQueue};
pub use span_export_queue::{LogExportQueue, LogExportRequest, SpanExportQueue, SpanExportRequest};
pub use client::{CheckpointResult, WorkerCoordinatorClient};
pub use context::{
    ContextConfig, CoreContext, FunctionCall, FunctionHandle, FunctionNamespace, FunctionRegistry,
    FunctionResult, FunctionStatus, LanguageModelNamespace, SignalNamespace, TimerNamespace,
};
pub use error::{Result, SdkError};
pub use lm::{
    generate, stream, AnthropicConfig, AnthropicProvider, AzureOpenAiConfig, AzureOpenAiProvider,
    BedrockConfig, BedrockProvider, BuiltInTool, Embedder, EmbedderRegistry, GenerateRequest,
    GenerateResponse, GenerationConfig, GroqConfig, GroqProvider, JsonSchemaFormat, LanguageModel,
    Message, MessageRole, Modality, OpenAiChatConfig, OpenAiChatProvider, OpenAiConfig,
    OpenAiEmbedder, OpenAiEmbedderConfig, OpenAiEmbeddingModel, OpenAiProvider, OpenRouterConfig,
    OpenRouterProvider, ReasoningEffort, ResponseFormat, StreamChunk, StreamHandle, StreamRequest,
    TokenUsage, ToolChoice,
};
pub use logging::{clear_error_buffer, get_error_buffer, init_logging};
pub use runtime_adapter::{
    DummyStateManager, EntityStateLoadResult, EntityStateManager, EntityStateSaveResult,
    InvocationRequest, InvocationResponse, RuntimeAdapter, RuntimeCapabilities, RuntimeContext,
    StateManager,
};
pub use telemetry::{
    create_component_span, create_function_span, end_span, extract_context_from_runtime_message,
    init_telemetry, record_execution_request, record_execution_request_with_attrs,
    record_span_error, record_span_success, shutdown_telemetry,
};
pub use journal_exporter::{
    create_journal_log_data, create_journal_span_data, export_log_to_journal,
    export_span_to_journal, get_journal_client, JournalClient, JournalExporterConfig,
    JournalLogData, JournalSpanData, JournalSpanStatus,
};
pub use vectordb::{
    Agnt5Provider, Agnt5ProviderConfig, Collection, DistanceMetric, PgVectorProvider,
    QdrantProvider, SearchQuery, SearchResult, VectorDatabase, VectorDbRegistry, VectorEntry,
    VectorFilter, VectorMetadata,
};
pub use worker::Worker;

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
