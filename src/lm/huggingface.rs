use std::env;

use async_trait::async_trait;
use crate::error::{Result as SdkResult, SdkError};

use super::openai_chat::{OpenAiChatConfig, OpenAiChatProvider};

use super::interface::{
    GenerateRequest, GenerateResponse, LanguageModel, StreamHandle, StreamRequest,
};

const DEFAULT_BASE_URL: &str = "https://router.huggingface.co/v1";

#[derive(Clone, Debug)]
pub struct HuggingFaceConfig {
    pub api_key: String,
    pub base_url: String,
}


impl HuggingFaceConfig {
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
        let api_key = env::var("HUGGINGFACE_API_KEY")
            .or_else(|_| env::var("HF_TOKEN"))
            .map_err(|_| SdkError::Configuration {
                message: "HUGGINGFACE_API_KEY or HF_TOKEN must be set".to_string(),
                field: Some("HUGGINGFACE_API_KEY".to_string()),
            })?;

        let mut config = HuggingFaceConfig::new(api_key);

        if let Ok(base) = env::var("HUGGINGFACE_BASE_URL") {
            if !base.trim().is_empty() {
                config.base_url = base;
            }
        }

        Ok(config)
    }
}


#[derive(Clone)]
pub struct HuggingFaceProvider {
    inner: OpenAiChatProvider,
}


impl HuggingFaceProvider {
    pub fn new(config: HuggingFaceConfig) -> SdkResult<Self> {
        
        let inner_config = OpenAiChatConfig::new(config.api_key)
            .with_base_url(config.base_url)
            .with_model_prefix(None::<String>);  

        let inner_provider = OpenAiChatProvider::new(inner_config)?;

        Ok(Self {
            inner: inner_provider,
        })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = HuggingFaceConfig::from_env()?;
        Self::new(config)
    }

    fn normalize_model(model: &str) -> String {
        let trimmed = model.trim();
        // Support both "hf/" and "huggingface/" prefixes (backwards compatibility)
        if let Some(rest) = trimmed.strip_prefix("hf/") {
            rest.to_string()
        } else if let Some(rest) = trimmed.strip_prefix("huggingface/") {
            rest.to_string()
        } else {
            trimmed.to_string()
        }
    }
}

#[async_trait]
impl LanguageModel for HuggingFaceProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        let normalized_model = Self::normalize_model(&request.model);
        let normalized_request = GenerateRequest {
            model: normalized_model,
            ..request
        };
        self.inner.generate(normalized_request).await
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        let normalized_model = Self::normalize_model(&request.model);
        let normalized_request = StreamRequest {
            model: normalized_model,
            ..request
        };
        self.inner.stream(normalized_request).await
    }

}