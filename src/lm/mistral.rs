use std::env;

use async_trait::async_trait;

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    GenerateRequest, GenerateResponse, LanguageModel, StreamHandle, StreamRequest,
};
use super::openai_chat::{OpenAiChatConfig, OpenAiChatProvider};

const DEFAULT_BASE_URL: &str = "https://api.mistral.ai/v1";
const MODEL_PREFIX: &str = "mistral";

/// Configuration for the Mistral provider.
///
/// Mistral AI offers high-performance open-weight and commercial models.
/// Notable models include Mistral Large, Mistral Medium, Mistral Small,
/// Codestral, and the open-weight Mistral 7B and Mixtral series.
#[derive(Clone, Debug)]
pub struct MistralConfig {
    pub api_key: String,
    pub base_url: String,
}

impl MistralConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: DEFAULT_BASE_URL.to_string(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn from_env() -> SdkResult<Self> {
        let api_key = env::var("MISTRAL_API_KEY")
            .map_err(|_| SdkError::Configuration {
                message: "MISTRAL_API_KEY must be set".to_string(),
                field: Some("MISTRAL_API_KEY".to_string()),
            })?;

        let mut config = MistralConfig::new(api_key);

        if let Ok(base) = env::var("MISTRAL_BASE_URL") {
            if !base.trim().is_empty() {
                config.base_url = base;
            }
        }

        Ok(config)
    }
}

/// Provider implementation for Mistral AI models.
///
/// Mistral provides OpenAI-compatible API endpoints. This provider wraps
/// the OpenAI Chat provider with Mistral-specific configuration.
///
/// # Example
///
/// ```no_run
/// use agnt5_sdk_core::lm::{MistralProvider, GenerateRequest};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let provider = MistralProvider::from_env()?;
/// let response = provider.generate(
///     GenerateRequest::new("mistral/mistral-large-latest")
///         .user_message("Explain machine learning")
/// ).await?;
/// println!("{}", response.text);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct MistralProvider {
    inner: OpenAiChatProvider,
}

impl MistralProvider {
    pub fn new(config: MistralConfig) -> SdkResult<Self> {
        let inner_config = OpenAiChatConfig::new(config.api_key)
            .with_base_url(config.base_url)
            .with_model_prefix(Some(MODEL_PREFIX));

        let inner = OpenAiChatProvider::new(inner_config)?;
        Ok(Self { inner })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = MistralConfig::from_env()?;
        Self::new(config)
    }

    pub async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        self.inner.generate(request).await
    }

    pub async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        self.inner.stream(request).await
    }
}

#[async_trait]
impl LanguageModel for MistralProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        self.inner.generate(request).await
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        self.inner.stream(request).await
    }
}
