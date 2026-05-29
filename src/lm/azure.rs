use std::env;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;

use crate::error::{Result as SdkResult, SdkError};

use super::http;
use super::interface::{
    generate as generate_via_model, stream as stream_via_model, GenerateRequest, GenerateResponse,
    LanguageModel, StreamHandle, StreamRequest,
};
use super::openai_common::{
    stream_handle_from_response, ChatCompletionPayload, ChatCompletionResponse,
};

const DEFAULT_API_VERSION: &str = "2024-02-01";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(600); // 10 minutes to match official OpenAI SDK (Azure uses OpenAI API)
const MODEL_PREFIX: &str = "azure";

#[derive(Clone, Debug)]
pub struct AzureOpenAiConfig {
    pub api_key: String,
    pub endpoint: String,
    pub api_version: String,
    pub timeout: Duration,
    pub retry_config: http::RetryConfig,
}

impl AzureOpenAiConfig {
    pub fn new(api_key: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            endpoint: endpoint.into(),
            api_version: DEFAULT_API_VERSION.to_string(),
            timeout: DEFAULT_TIMEOUT,
            retry_config: http::RetryConfig::from_env(),
        }
    }

    pub fn with_api_version(mut self, version: impl Into<String>) -> Self {
        self.api_version = version.into();
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn from_env() -> SdkResult<Self> {
        let api_key = env::var("AZURE_OPENAI_API_KEY").map_err(|_| SdkError::Configuration {
            message: "AZURE_OPENAI_API_KEY must be set".to_string(),
            field: Some("AZURE_OPENAI_API_KEY".to_string()),
        })?;

        let endpoint = env::var("AZURE_OPENAI_ENDPOINT").map_err(|_| SdkError::Configuration {
            message: "AZURE_OPENAI_ENDPOINT must be set".to_string(),
            field: Some("AZURE_OPENAI_ENDPOINT".to_string()),
        })?;

        let mut config = AzureOpenAiConfig::new(api_key, endpoint);

        if let Ok(version) = env::var("AZURE_OPENAI_API_VERSION") {
            if !version.trim().is_empty() {
                config.api_version = version;
            }
        }

        if let Ok(timeout) = env::var("AZURE_OPENAI_TIMEOUT_SECS") {
            if let Ok(secs) = timeout.parse::<u64>() {
                config.timeout = Duration::from_secs(secs);
            }
        }

        Ok(config)
    }
}

#[derive(Clone)]
pub struct AzureOpenAiProvider {
    http: Client,
    config: AzureOpenAiConfig,
}

impl AzureOpenAiProvider {
    pub fn new(config: AzureOpenAiConfig) -> SdkResult<Self> {
        let http = http::build_http_client(config.timeout)?;

        Ok(Self { http, config })
    }

    pub fn from_env() -> SdkResult<Self> {
        let config = AzureOpenAiConfig::from_env()?;
        Self::new(config)
    }

    fn request(&self, deployment: &str) -> reqwest::RequestBuilder {
        let endpoint = self.config.endpoint.trim_end_matches('/');
        let url = format!(
            "{endpoint}/openai/deployments/{deployment}/chat/completions?api-version={}",
            self.config.api_version
        );

        self.http
            .post(url)
            .header("api-key", &self.config.api_key)
            .header("Content-Type", "application/json")
    }

    fn normalize_model(&self, model: &str) -> SdkResult<String> {
        let trimmed = model.trim();
        if let Some((prefix, rest)) = trimmed.split_once('/') {
            if prefix != MODEL_PREFIX {
                return Err(SdkError::Configuration {
                    message: format!(
                        "Azure provider expects model ids prefixed with `{MODEL_PREFIX}/`; got `{prefix}`"
                    ),
                    field: Some("model".to_string()),
                });
            }
            let deployment = rest.trim();
            if deployment.is_empty() {
                return Err(SdkError::Configuration {
                    message: "Azure model id must include deployment after `azure/` prefix"
                        .to_string(),
                    field: Some("model".to_string()),
                });
            }
            Ok(deployment.to_string())
        } else {
            Err(SdkError::Configuration {
                message: format!("Azure model ids must be prefixed with `{MODEL_PREFIX}/`"),
                field: Some("model".to_string()),
            })
        }
    }

    pub async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        generate_via_model(self, request).await
    }

    pub async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        stream_via_model(self, request).await
    }
}

#[async_trait]
impl LanguageModel for AzureOpenAiProvider {
    async fn generate(&self, request: GenerateRequest) -> SdkResult<GenerateResponse> {
        validate_request(&request)?;
        let deployment = self.normalize_model(&request.model)?;
        let payload = ChatCompletionPayload::from_request(&request, deployment.clone(), false);

        let response = http::send_with_retry(
            || self.request(&deployment).json(&payload),
            &self.config.retry_config,
            "azure",
            request.config.timeout,
        )
        .await?;

        let metadata = http::extract_metadata(&response);
        let parsed: ChatCompletionResponse = response
            .json()
            .await
            .map_err(|err| http::classify_reqwest_error(err, "azure"))?;

        let mut result = parsed.into_generate_response(request.config.response_format.clone())?;
        result.metadata = Some(metadata);
        Ok(result)
    }

    async fn stream(&self, request: StreamRequest) -> SdkResult<StreamHandle> {
        validate_request(&request)?;
        let deployment = self.normalize_model(&request.model)?;
        let payload = ChatCompletionPayload::from_request(&request, deployment.clone(), true);

        let response = http::send_with_retry(
            || {
                self.request(&deployment)
                    .header("Accept", "text/event-stream")
                    .json(&payload)
            },
            &self.config.retry_config,
            "azure",
            request.config.timeout,
        )
        .await?;
        stream_handle_from_response(
            response,
            request.config.response_format.clone(),
            self.config.timeout.as_secs(),
        )
    }
}

fn validate_request(request: &GenerateRequest) -> SdkResult<()> {
    if request.system_prompt.is_none() && request.messages.is_empty() {
        return Err(SdkError::Configuration {
            message:
                "at least a system prompt or one message is required for Azure OpenAI requests"
                    .to_string(),
            field: None,
        });
    }
    Ok(())
}
