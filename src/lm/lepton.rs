use std::env;

use async_trait::async_trait;

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    GenerateRequest, GenerateResponse, LanguageModel, StreamHandle, StreamRequest,
};
use super::openai_chat::{OpenAiChatConfig, OpenAiChatProvider};

const MODEL_PREFIX: &str = "lepton";

#[derive(Clone, Debug)]
pub struct LeptonConfig {
    pub api_key: String,
    pub base_url: String,
}

impl LeptonConfig {
    pub fn new(api_key: impl Into<String>, base_url: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: base_url.into(),
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn from_env() -> SdkResult<Self> {
        let api_key = env::var("LEPTON_API_KEY")
            .or_else(|_| env::var("LEPTON_API_TOKEN"))
            .map_err(|_| SdkError::Configuration {
                message: "LEPTON_API_KEY or LEPTON_API_TOKEN must be set".to_string(),
                field: Some("LEPTON_API_KEY".to_string()),
            })?;

        let base_url = env::var("LEPTON_BASE_URL")
            .or_else(|_| env::var("LEPTON_API_BASE"))
            .map_err(|_| SdkError::Configuration {
                message: "LEPTON_BASE_URL must be set to the OpenAI-compatible endpoint"
                    .to_string(),
                field: Some("LEPTON_BASE_URL".to_string()),
            })?;

        Ok(LeptonConfig::new(api_key, base_url))
    }
}

#[derive(Clone)]
pub struct LeptonProvider {
    inner: OpenAiChatProvider,
}

impl LeptonProvider {
    pub fn new(config: LeptonConfig) -> SdkResult<Self> {
        if config.base_url.trim().is_empty() {
            return Err(SdkError::Configuration {
                message: "Lepton provider requires LEPTON_BASE_URL or an explicit base URL"
                    .to_string(),
                field: Some("base_url".to_string()),
            });
        }

        let inner_config = OpenAiChatConfig::new(config.api_key)
            .with_base_url(config.base_url)
            .with_model_prefix(Some(MODEL_PREFIX));

        let inner = OpenAiChatProvider::new(inner_config)?;
        Ok(Self { inner })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = LeptonConfig::from_env()?;
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
impl LanguageModel for LeptonProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        self.inner.generate(request).await
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        self.inner.stream(request).await
    }
}
