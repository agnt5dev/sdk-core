// AGNT5 SDK Core - Universal LLM Integration
// Provides direct LLM API access with OpenTelemetry integration

pub mod models;
pub mod provider;
pub mod providers;
pub mod registry;
pub mod telemetry;
pub mod vectordb;

// Re-export core types for easy access
pub use models::{
    ChatCompletionRequest, ChatCompletionResponse, ChatMessage, ChatMessageContent,
    CompletionRequest, CompletionResponse, EmbeddingsRequest, EmbeddingsResponse, ReasoningConfig,
    StreamingResponse, Usage,
};
pub use provider::ProviderConfig;
pub use provider::{get_vendor_name, Provider, ProviderType};
pub use registry::LlmRegistry;
pub use telemetry::LlmSpan;
pub use vectordb::{
    rag::{DocumentProcessor, RagConfig, RagPipeline},
    Collection, DistanceMetric, SearchQuery, SearchResult, VectorDatabase, VectorDbRegistry,
    VectorEntry, VectorMetadata,
};

// Public API for SDK consumers
use crate::error::{Result, SdkError};
use std::sync::Arc;

/// Main LLM client for SDK usage
pub struct LlmClient {
    registry: Arc<LlmRegistry>,
}

impl LlmClient {
    /// Create a new LLM client with providers loaded from environment
    pub fn new() -> Result<Self> {
        let mut registry = LlmRegistry::new();
        registry.load_from_environment()?;

        Ok(Self {
            registry: Arc::new(registry),
        })
    }

    /// Create LLM client with custom registry
    pub fn with_registry(registry: LlmRegistry) -> Self {
        Self {
            registry: Arc::new(registry),
        }
    }

    /// Execute a chat completion request
    pub async fn chat_completion(
        &self,
        provider_name: &str,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse> {
        let provider = self.registry.get_provider(provider_name).ok_or_else(|| {
            SdkError::Other(anyhow::anyhow!("Provider not found: {}", provider_name))
        })?;

        // Create telemetry span for this operation
        let mut span = telemetry::LlmSpan::start_chat_completion(&request, provider.r#type());

        match provider.chat_completion(request).await {
            Ok(response) => {
                span.log_success(&response);
                Ok(response)
            }
            Err(error) => {
                span.log_error(&error);
                Err(error)
            }
        }
    }

    /// Execute a completion request
    pub async fn completion(
        &self,
        provider_name: &str,
        request: CompletionRequest,
    ) -> Result<CompletionResponse> {
        let provider = self.registry.get_provider(provider_name).ok_or_else(|| {
            SdkError::Other(anyhow::anyhow!("Provider not found: {}", provider_name))
        })?;

        let mut span = telemetry::LlmSpan::start_completion(&request, provider.r#type());

        match provider.completion(request).await {
            Ok(response) => {
                span.log_success(&response);
                Ok(response)
            }
            Err(error) => {
                span.log_error(&error);
                Err(error)
            }
        }
    }

    /// Execute an embeddings request
    pub async fn embeddings(
        &self,
        provider_name: &str,
        request: EmbeddingsRequest,
    ) -> Result<EmbeddingsResponse> {
        let provider = self.registry.get_provider(provider_name).ok_or_else(|| {
            SdkError::Other(anyhow::anyhow!("Provider not found: {}", provider_name))
        })?;

        let mut span = telemetry::LlmSpan::start_embeddings(&request, provider.r#type());

        match provider.embeddings(request).await {
            Ok(response) => {
                span.log_success(&response);
                Ok(response)
            }
            Err(error) => {
                span.log_error(&error);
                Err(error)
            }
        }
    }

    /// List available providers
    pub fn list_providers(&self) -> Vec<String> {
        self.registry.list_providers()
    }
}

impl Default for LlmClient {
    fn default() -> Self {
        Self::new().expect("Failed to create default LLM client")
    }
}
