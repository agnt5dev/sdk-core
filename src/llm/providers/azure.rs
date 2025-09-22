// Azure OpenAI provider implementation
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::super::models::{
    ChatCompletion, ChatCompletionRequest, ChatCompletionResponse, CompletionRequest,
    CompletionResponse, EmbeddingsRequest, EmbeddingsResponse,
};
use super::super::provider::{Provider, ProviderConfig, ProviderType};
use crate::error::{Result, SdkError};

/// Azure OpenAI-specific request format
#[derive(Serialize, Deserialize, Clone)]
struct AzureChatCompletionRequest {
    #[serde(flatten)]
    base: ChatCompletionRequest,
}

impl From<ChatCompletionRequest> for AzureChatCompletionRequest {
    fn from(mut base: ChatCompletionRequest) -> Self {
        // Azure uses max_tokens instead of max_completion_tokens
        if let Some(max_completion) = base.max_completion_tokens {
            base.max_tokens = Some(max_completion);
            base.max_completion_tokens = None;
        }

        // Remove reasoning field as Azure may not support it yet
        base.reasoning = None;

        Self { base }
    }
}

pub struct AzureProvider {
    config: ProviderConfig,
    http_client: Client,
}

impl AzureProvider {
    pub fn new(config: &ProviderConfig) -> Self {
        Self {
            config: config.clone(),
            http_client: Client::new(),
        }
    }

    fn base_url(&self) -> Result<String> {
        let endpoint = self
            .config
            .get_param("endpoint")
            .ok_or_else(|| SdkError::Other(anyhow::anyhow!("Azure endpoint not configured")))?;

        let _api_version = self
            .config
            .get_param("api_version")
            .unwrap_or(&"2024-02-01".to_string());

        Ok(format!("{}/openai", endpoint.trim_end_matches('/')))
    }

    fn api_version(&self) -> String {
        self.config
            .get_param("api_version")
            .unwrap_or(&"2024-02-01".to_string())
            .clone()
    }
}

#[async_trait]
impl Provider for AzureProvider {
    fn key(&self) -> String {
        self.config.key.clone()
    }

    fn r#type(&self) -> ProviderType {
        ProviderType::Azure
    }

    async fn chat_completion(
        &self,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse> {
        // Check for streaming - not yet implemented
        if request.stream.unwrap_or(false) {
            return Err(SdkError::Other(anyhow::anyhow!(
                "Streaming not yet implemented for Azure - use stream: false"
            )));
        }

        let base_url = self.base_url()?;
        let deployment_name = self.config.get_param("deployment_name").ok_or_else(|| {
            SdkError::Other(anyhow::anyhow!("Azure deployment_name not configured"))
        })?;

        let url = format!(
            "{}/deployments/{}/chat/completions?api-version={}",
            base_url,
            deployment_name,
            self.api_version()
        );

        let azure_request = AzureChatCompletionRequest::from(request);

        let response = self
            .http_client
            .post(&url)
            .header("api-key", &self.config.api_key)
            .header("Content-Type", "application/json")
            .json(&azure_request)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Azure OpenAI API request error: {}", e);
                SdkError::Other(anyhow::anyhow!("Azure OpenAI API request failed: {}", e))
            })?;

        let status = response.status();
        if status.is_success() {
            let completion: ChatCompletion = response.json().await.map_err(|e| {
                tracing::error!("Azure OpenAI API response parsing error: {}", e);
                SdkError::Other(anyhow::anyhow!(
                    "Failed to parse Azure OpenAI response: {}",
                    e
                ))
            })?;

            Ok(ChatCompletionResponse::NonStream(completion))
        } else {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            tracing::error!("Azure OpenAI API error ({}): {}", status, error_text);
            Err(SdkError::Other(anyhow::anyhow!(
                "Azure OpenAI API error ({}): {}",
                status,
                error_text
            )))
        }
    }

    async fn completion(&self, request: CompletionRequest) -> Result<CompletionResponse> {
        let base_url = self.base_url()?;
        let deployment_name = self.config.get_param("deployment_name").ok_or_else(|| {
            SdkError::Other(anyhow::anyhow!("Azure deployment_name not configured"))
        })?;

        let url = format!(
            "{}/deployments/{}/completions?api-version={}",
            base_url,
            deployment_name,
            self.api_version()
        );

        let response = self
            .http_client
            .post(&url)
            .header("api-key", &self.config.api_key)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Azure OpenAI completions API request error: {}", e);
                SdkError::Other(anyhow::anyhow!(
                    "Azure OpenAI completions API request failed: {}",
                    e
                ))
            })?;

        let status = response.status();
        if status.is_success() {
            let completion: CompletionResponse = response.json().await.map_err(|e| {
                tracing::error!("Azure OpenAI completions API response parsing error: {}", e);
                SdkError::Other(anyhow::anyhow!(
                    "Failed to parse Azure OpenAI completions response: {}",
                    e
                ))
            })?;

            Ok(completion)
        } else {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            tracing::error!(
                "Azure OpenAI completions API error ({}): {}",
                status,
                error_text
            );
            Err(SdkError::Other(anyhow::anyhow!(
                "Azure OpenAI completions API error ({}): {}",
                status,
                error_text
            )))
        }
    }

    async fn embeddings(&self, request: EmbeddingsRequest) -> Result<EmbeddingsResponse> {
        let base_url = self.base_url()?;
        let deployment_name = self.config.get_param("deployment_name").ok_or_else(|| {
            SdkError::Other(anyhow::anyhow!("Azure deployment_name not configured"))
        })?;

        let url = format!(
            "{}/deployments/{}/embeddings?api-version={}",
            base_url,
            deployment_name,
            self.api_version()
        );

        let response = self
            .http_client
            .post(&url)
            .header("api-key", &self.config.api_key)
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Azure OpenAI embeddings API request error: {}", e);
                SdkError::Other(anyhow::anyhow!(
                    "Azure OpenAI embeddings API request failed: {}",
                    e
                ))
            })?;

        let status = response.status();
        if status.is_success() {
            let embeddings: EmbeddingsResponse = response.json().await.map_err(|e| {
                tracing::error!("Azure OpenAI embeddings API response parsing error: {}", e);
                SdkError::Other(anyhow::anyhow!(
                    "Failed to parse Azure OpenAI embeddings response: {}",
                    e
                ))
            })?;

            Ok(embeddings)
        } else {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            tracing::error!(
                "Azure OpenAI embeddings API error ({}): {}",
                status,
                error_text
            );
            Err(SdkError::Other(anyhow::anyhow!(
                "Azure OpenAI embeddings API error ({}): {}",
                status,
                error_text
            )))
        }
    }

    async fn health_check(&self) -> Result<()> {
        // Simple health check - try to get models list
        let base_url = self.base_url()?;
        let url = format!("{}/models?api-version={}", base_url, self.api_version());

        let response = self
            .http_client
            .get(&url)
            .header("api-key", &self.config.api_key)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Azure OpenAI health check request error: {}", e);
                SdkError::Other(anyhow::anyhow!("Azure OpenAI health check failed: {}", e))
            })?;

        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            tracing::error!(
                "Azure OpenAI health check failed ({}): {}",
                status,
                error_text
            );
            Err(SdkError::Other(anyhow::anyhow!(
                "Azure OpenAI health check failed ({}): {}",
                status,
                error_text
            )))
        }
    }
}
