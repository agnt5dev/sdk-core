use std::env;

use async_trait::async_trait;

use crate::error::{Result as SdkResult, SdkError};

use super::interface::{
    GenerateRequest, GenerateResponse, LanguageModel, StreamHandle, StreamRequest,
};
use super::openai::{OpenAiConfig, OpenAiProvider};

const DEFAULT_BASE_URL: &str = "https://openrouter.ai/api/v1";
const MODEL_PREFIX: &str = "openrouter";

#[derive(Clone, Debug)]
pub struct OpenRouterConfig {
    pub api_key: String,
    pub base_url: String,
    pub referer: Option<String>,
    pub app_id: Option<String>,
}

impl OpenRouterConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: DEFAULT_BASE_URL.to_string(),
            referer: None,
            app_id: None,
        }
    }

    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_referer(mut self, referer: impl Into<String>) -> Self {
        self.referer = Some(referer.into());
        self
    }

    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    pub fn from_env() -> SdkResult<Self> {
        let api_key = env::var("OPENROUTER_API_KEY")
            .map_err(|_| SdkError::Configuration("OPENROUTER_API_KEY must be set".to_string()))?;

        let mut config = OpenRouterConfig::new(api_key);

        if let Ok(base) = env::var("OPENROUTER_BASE_URL") {
            if !base.trim().is_empty() {
                config.base_url = base;
            }
        }

        if let Ok(referer) = env::var("OPENROUTER_REFERER") {
            if !referer.trim().is_empty() {
                config.referer = Some(referer);
            }
        }

        if let Ok(app_id) = env::var("OPENROUTER_APP_ID") {
            if !app_id.trim().is_empty() {
                config.app_id = Some(app_id);
            }
        }

        Ok(config)
    }
}

#[derive(Clone)]
pub struct OpenRouterProvider {
    inner: OpenAiProvider,
}

impl OpenRouterProvider {
    pub fn new(config: OpenRouterConfig) -> SdkResult<Self> {
        let mut inner_config = OpenAiConfig::new(config.api_key)
            .with_base_url(config.base_url)
            .with_model_prefix(Some(MODEL_PREFIX));

        if let Some(referer) = config.referer {
            inner_config = inner_config.with_header("HTTP-Referer", referer);
        }

        if let Some(app_id) = config.app_id {
            inner_config = inner_config.with_header("X-Title", app_id);
        }

        let inner = OpenAiProvider::new(inner_config)?;
        Ok(Self { inner })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = OpenRouterConfig::from_env()?;
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
impl LanguageModel for OpenRouterProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        self.inner.generate(request).await
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        self.inner.stream(request).await
    }
}
