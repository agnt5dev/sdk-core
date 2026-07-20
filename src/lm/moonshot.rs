use std::env;

use async_trait::async_trait;

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    GenerateRequest, GenerateResponse, LanguageModel, StreamHandle, StreamRequest,
};
use super::openai_chat::{OpenAiChatConfig, OpenAiChatProvider};

const DEFAULT_BASE_URL: &str = "https://api.moonshot.ai/v1";
const MODEL_PREFIX: &str = "moonshot";

/// Configuration for the Moonshot AI (Kimi) provider.
#[derive(Clone, Debug)]
pub struct MoonshotConfig {
    pub api_key: String,
    pub base_url: String,
}

impl MoonshotConfig {
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
        let api_key = env::var("MOONSHOT_API_KEY").map_err(|_| SdkError::Configuration {
            message: "MOONSHOT_API_KEY must be set".to_string(),
            field: Some("MOONSHOT_API_KEY".to_string()),
        })?;

        let mut config = MoonshotConfig::new(api_key);
        if let Ok(base) = env::var("MOONSHOT_BASE_URL") {
            if !base.trim().is_empty() {
                config.base_url = base;
            }
        }
        Ok(config)
    }
}

/// OpenAI-compatible provider for Moonshot AI's Kimi models.
#[derive(Clone)]
pub struct MoonshotProvider {
    inner: OpenAiChatProvider,
}

impl MoonshotProvider {
    pub fn new(config: MoonshotConfig) -> SdkResult<Self> {
        let inner_config = OpenAiChatConfig::new(config.api_key)
            .with_base_url(config.base_url)
            .with_model_prefix(Some(MODEL_PREFIX));
        let inner = OpenAiChatProvider::new(inner_config)?;
        Ok(Self { inner })
    }

    pub fn from_env() -> SdkResult<Self> {
        Self::new(MoonshotConfig::from_env()?)
    }

    pub async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        self.inner.generate(request).await
    }

    pub async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        self.inner.stream(request).await
    }
}

#[async_trait]
impl LanguageModel for MoonshotProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        self.inner.generate(request).await
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        self.inner.stream(request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_official_api_base_by_default() {
        let config = MoonshotConfig::new("test-key");
        assert_eq!(config.base_url, "https://api.moonshot.ai/v1");
    }

    #[test]
    fn supports_custom_openai_compatible_base_url() {
        let config = MoonshotConfig::new("test-key").with_base_url("http://moonshot.test/v1");
        assert_eq!(config.base_url, "http://moonshot.test/v1");
    }
}
