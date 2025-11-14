use std::env;

use async_trait::async_trait;

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    GenerateRequest, GenerateResponse, LanguageModel, StreamHandle, StreamRequest,
};
use super::openai_chat::{OpenAiChatConfig, OpenAiChatProvider};

const DEFAULT_BASE_URL: &str = "https://api.groq.com/openai/v1";
const MODEL_PREFIX: &str = "groq";

#[derive(Clone, Debug)]
pub struct GroqConfig {
    pub api_key: String,
    pub base_url: String,
}

impl GroqConfig {
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
        let api_key = env::var("GROQ_API_KEY")
            .map_err(|_| SdkError::Configuration {
                message: "GROQ_API_KEY must be set".to_string(),
                field: Some("GROQ_API_KEY".to_string()),
            })?;

        let mut config = GroqConfig::new(api_key);

        if let Ok(base) = env::var("GROQ_BASE_URL") {
            if !base.trim().is_empty() {
                config.base_url = base;
            }
        }

        Ok(config)
    }
}

#[derive(Clone)]
pub struct GroqProvider {
    inner: OpenAiChatProvider,
}

impl GroqProvider {
    pub fn new(config: GroqConfig) -> SdkResult<Self> {
        let inner_config = OpenAiChatConfig::new(config.api_key)
            .with_base_url(config.base_url)
            .with_model_prefix(Some(MODEL_PREFIX));

        let inner = OpenAiChatProvider::new(inner_config)?;
        Ok(Self { inner })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = GroqConfig::from_env()?;
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
impl LanguageModel for GroqProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        self.inner.generate(request).await
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        self.inner.stream(request).await
    }
}
