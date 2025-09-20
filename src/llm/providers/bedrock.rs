// AWS Bedrock provider implementation
use async_trait::async_trait;

use super::super::models::{
    ChatCompletionRequest, ChatCompletionResponse, CompletionRequest, CompletionResponse,
    EmbeddingsRequest, EmbeddingsResponse,
};
use super::super::provider::{Provider, ProviderConfig, ProviderType};
use crate::error::{Result, SdkError};

pub struct BedrockProvider {
    config: ProviderConfig,
}

impl BedrockProvider {
    pub fn new(config: &ProviderConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    fn region(&self) -> String {
        self.config
            .get_param("region")
            .unwrap_or(&"us-east-1".to_string())
            .clone()
    }
}

#[async_trait]
impl Provider for BedrockProvider {
    fn key(&self) -> String {
        self.config.key.clone()
    }

    fn r#type(&self) -> ProviderType {
        ProviderType::Bedrock
    }

    async fn chat_completion(
        &self,
        _request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse> {
        // AWS Bedrock integration requires AWS SDK dependencies which need Rust 1.86+
        // Uncomment AWS dependencies in Cargo.toml and upgrade Rust to enable Bedrock
        Err(SdkError::Other(anyhow::anyhow!(
            "Bedrock provider requires AWS SDK dependencies (commented out in Cargo.toml). \
             Uncomment aws-sdk-bedrockruntime and related dependencies, then upgrade to Rust 1.86+ to enable Bedrock support."
        )))
    }

    async fn completion(&self, _request: CompletionRequest) -> Result<CompletionResponse> {
        // AWS Bedrock integration requires AWS SDK dependencies which need Rust 1.86+
        Err(SdkError::Other(anyhow::anyhow!(
            "Bedrock provider requires AWS SDK dependencies (commented out in Cargo.toml). \
             Uncomment aws-sdk-bedrockruntime and related dependencies, then upgrade to Rust 1.86+ to enable Bedrock support."
        )))
    }

    async fn embeddings(&self, _request: EmbeddingsRequest) -> Result<EmbeddingsResponse> {
        // AWS Bedrock integration requires AWS SDK dependencies which need Rust 1.86+
        Err(SdkError::Other(anyhow::anyhow!(
            "Bedrock provider requires AWS SDK dependencies (commented out in Cargo.toml). \
             Uncomment aws-sdk-bedrockruntime and related dependencies, then upgrade to Rust 1.86+ to enable Bedrock support."
        )))
    }

    async fn health_check(&self) -> Result<()> {
        // AWS Bedrock integration requires AWS SDK dependencies which need Rust 1.86+
        Err(SdkError::Other(anyhow::anyhow!(
            "Bedrock provider requires AWS SDK dependencies (commented out in Cargo.toml). \
             Uncomment aws-sdk-bedrockruntime and related dependencies, then upgrade to Rust 1.86+ to enable Bedrock support."
        )))
    }
}
