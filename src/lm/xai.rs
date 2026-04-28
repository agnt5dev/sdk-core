use std::env;

use async_trait::async_trait;

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    GenerateRequest, GenerateResponse, LanguageModel, StreamHandle, StreamRequest,
};
use super::openai_chat::{OpenAiChatConfig, OpenAiChatProvider};

const DEFAULT_BASE_URL: &str = "https://api.x.ai/v1";
const MODEL_PREFIX: &str = "xai";

/// Configuration for the xAI provider.
///
/// xAI provides the Grok family of language models with competitive performance
/// and unique capabilities trained on X (Twitter) data.
#[derive(Clone, Debug)]
pub struct XaiConfig {
    pub api_key: String,
    pub base_url: String,
}

impl XaiConfig {
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
        let api_key = env::var("XAI_API_KEY").map_err(|_| SdkError::Configuration {
            message: "XAI_API_KEY must be set".to_string(),
            field: Some("XAI_API_KEY".to_string()),
        })?;

        let mut config = XaiConfig::new(api_key);

        if let Ok(base) = env::var("XAI_BASE_URL") {
            if !base.trim().is_empty() {
                config.base_url = base;
            }
        }

        Ok(config)
    }
}

/// Provider implementation for xAI (Grok) models.
///
/// xAI provides OpenAI-compatible API endpoints. This provider wraps
/// the OpenAI Chat provider with xAI-specific configuration.
///
/// # Example
///
/// ```no_run
/// use agnt5_sdk_core::lm::{XaiProvider, GenerateRequest};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let provider = XaiProvider::from_env()?;
/// let response = provider.generate(
///     GenerateRequest::new("xai/grok-2")
///         .user_message("Explain the latest trends in AI")
/// ).await?;
/// println!("{}", response.text);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct XaiProvider {
    inner: OpenAiChatProvider,
}

impl XaiProvider {
    pub fn new(config: XaiConfig) -> SdkResult<Self> {
        let inner_config = OpenAiChatConfig::new(config.api_key)
            .with_base_url(config.base_url)
            .with_model_prefix(Some(MODEL_PREFIX));

        let inner = OpenAiChatProvider::new(inner_config)?;
        Ok(Self { inner })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = XaiConfig::from_env()?;
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
impl LanguageModel for XaiProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        self.inner.generate(request).await
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        self.inner.stream(request).await
    }
}
