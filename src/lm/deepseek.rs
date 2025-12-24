use std::env;

use async_trait::async_trait;

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    GenerateRequest, GenerateResponse, LanguageModel, StreamHandle, StreamRequest,
};
use super::openai_chat::{OpenAiChatConfig, OpenAiChatProvider};

const DEFAULT_BASE_URL: &str = "https://api.deepseek.com/v1";
const MODEL_PREFIX: &str = "deepseek";

/// Configuration for the DeepSeek provider.
///
/// DeepSeek offers high-performance language models with exceptional cost/performance ratio.
/// Notable models include DeepSeek-V3 and DeepSeek-R1 (reasoning model).
#[derive(Clone, Debug)]
pub struct DeepSeekConfig {
    pub api_key: String,
    pub base_url: String,
}

impl DeepSeekConfig {
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
        let api_key = env::var("DEEPSEEK_API_KEY")
            .map_err(|_| SdkError::Configuration {
                message: "DEEPSEEK_API_KEY must be set".to_string(),
                field: Some("DEEPSEEK_API_KEY".to_string()),
            })?;

        let mut config = DeepSeekConfig::new(api_key);

        if let Ok(base) = env::var("DEEPSEEK_BASE_URL") {
            if !base.trim().is_empty() {
                config.base_url = base;
            }
        }

        Ok(config)
    }
}

/// Provider implementation for DeepSeek models.
///
/// DeepSeek provides OpenAI-compatible API endpoints. This provider wraps
/// the OpenAI Chat provider with DeepSeek-specific configuration.
///
/// # Example
///
/// ```no_run
/// use agnt5_sdk_core::lm::{DeepSeekProvider, GenerateRequest};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let provider = DeepSeekProvider::from_env()?;
/// let response = provider.generate(
///     GenerateRequest::new("deepseek/deepseek-chat")
///         .user_message("Explain quantum computing")
/// ).await?;
/// println!("{}", response.text);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct DeepSeekProvider {
    inner: OpenAiChatProvider,
}

impl DeepSeekProvider {
    pub fn new(config: DeepSeekConfig) -> SdkResult<Self> {
        let inner_config = OpenAiChatConfig::new(config.api_key)
            .with_base_url(config.base_url)
            .with_model_prefix(Some(MODEL_PREFIX));

        let inner = OpenAiChatProvider::new(inner_config)?;
        Ok(Self { inner })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = DeepSeekConfig::from_env()?;
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
impl LanguageModel for DeepSeekProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        self.inner.generate(request).await
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        self.inner.stream(request).await
    }
}
