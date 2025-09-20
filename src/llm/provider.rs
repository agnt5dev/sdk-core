// Core provider trait and types for LLM integration
use async_trait::async_trait;
use std::borrow::Cow;

use crate::error::{Result, SdkError};
use super::models::{
    ChatCompletionRequest, ChatCompletionResponse,
    CompletionRequest, CompletionResponse,
    EmbeddingsRequest, EmbeddingsResponse,
};

/// Enumeration of supported LLM provider types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProviderType {
    OpenAI,
    Anthropic,
    Azure,
    Bedrock,
    VertexAI,
    OpenRouter,
}

impl std::fmt::Display for ProviderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProviderType::OpenAI => write!(f, "openai"),
            ProviderType::Anthropic => write!(f, "anthropic"),
            ProviderType::Azure => write!(f, "azure"),
            ProviderType::Bedrock => write!(f, "bedrock"),
            ProviderType::VertexAI => write!(f, "vertexai"),
            ProviderType::OpenRouter => write!(f, "openrouter"),
        }
    }
}

impl std::str::FromStr for ProviderType {
    type Err = SdkError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "openai" => Ok(ProviderType::OpenAI),
            "anthropic" => Ok(ProviderType::Anthropic),
            "azure" => Ok(ProviderType::Azure),
            "bedrock" => Ok(ProviderType::Bedrock),
            "vertexai" | "vertex_ai" => Ok(ProviderType::VertexAI),
            "openrouter" | "open_router" => Ok(ProviderType::OpenRouter),
            _ => Err(SdkError::Other(anyhow::anyhow!("Unknown provider type: {}", s))),
        }
    }
}

/// Core trait that all LLM providers must implement
#[async_trait]
pub trait Provider: Send + Sync {
    /// Get the provider's unique identifier
    fn key(&self) -> String;

    /// Get the provider type
    fn r#type(&self) -> ProviderType;

    /// Execute a chat completion request
    async fn chat_completion(
        &self,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse>;

    /// Execute a completion request
    async fn completion(
        &self,
        request: CompletionRequest,
    ) -> Result<CompletionResponse>;

    /// Execute an embeddings request
    async fn embeddings(
        &self,
        request: EmbeddingsRequest,
    ) -> Result<EmbeddingsResponse>;

    /// Check if the provider is healthy/available
    async fn health_check(&self) -> Result<()> {
        // Default implementation - can be overridden by providers
        Ok(())
    }
}

/// Maps provider type to standardized vendor names for OpenTelemetry reporting
pub fn get_vendor_name(provider_type: &ProviderType) -> Cow<'static, str> {
    match provider_type {
        ProviderType::OpenAI => Cow::Borrowed("openai"),
        ProviderType::Azure => Cow::Borrowed("Azure"),
        ProviderType::Anthropic => Cow::Borrowed("Anthropic"),
        ProviderType::Bedrock => Cow::Borrowed("AWS"),
        ProviderType::VertexAI => Cow::Borrowed("Google"),
        ProviderType::OpenRouter => Cow::Borrowed("OpenRouter"),
    }
}

/// Configuration for a provider instance
#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub key: String,
    pub api_key: String,
    pub provider_type: ProviderType,
    pub params: std::collections::HashMap<String, String>,
}

impl ProviderConfig {
    pub fn new(key: String, api_key: String, provider_type: ProviderType) -> Self {
        Self {
            key,
            api_key,
            provider_type,
            params: std::collections::HashMap::new(),
        }
    }

    pub fn with_param(mut self, key: String, value: String) -> Self {
        self.params.insert(key, value);
        self
    }

    pub fn get_param(&self, key: &str) -> Option<&String> {
        self.params.get(key)
    }
}