// OpenAI provider implementation adapted from Hub
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::super::models::{
    ChatCompletion, ChatCompletionRequest, ChatCompletionResponse, CompletionRequest,
    CompletionResponse, EmbeddingsRequest, EmbeddingsResponse,
};
use super::super::provider::{Provider, ProviderConfig, ProviderType};
use crate::error::{Result, SdkError};

/// OpenAI-specific request format with reasoning support
#[derive(Serialize, Deserialize, Clone)]
struct OpenAIChatCompletionRequest {
    #[serde(flatten)]
    base: ChatCompletionRequest,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning_effort: Option<String>,
}

impl From<ChatCompletionRequest> for OpenAIChatCompletionRequest {
    fn from(mut base: ChatCompletionRequest) -> Self {
        let reasoning_effort = base.reasoning.as_ref().and_then(|r| r.to_openai_effort());

        // Handle max_completion_tokens logic - use max_completion_tokens if provided and > 0,
        // otherwise fall back to max_tokens
        base.max_completion_tokens = match (base.max_completion_tokens, base.max_tokens) {
            (Some(v), _) if v > 0 => Some(v),
            (_, Some(v)) if v > 0 => Some(v),
            _ => None,
        };

        base.max_tokens = None;

        // Remove reasoning field from base request since OpenAI uses reasoning_effort
        base.reasoning = None;

        Self {
            base,
            reasoning_effort,
        }
    }
}

pub struct OpenAIProvider {
    config: ProviderConfig,
    http_client: Client,
}

impl OpenAIProvider {
    pub fn new(config: &ProviderConfig) -> Self {
        Self {
            config: config.clone(),
            http_client: Client::new(),
        }
    }

    fn base_url(&self) -> String {
        self.config
            .get_param("base_url")
            .unwrap_or(&"https://api.openai.com/v1".to_string())
            .clone()
    }
}

#[async_trait]
impl Provider for OpenAIProvider {
    fn key(&self) -> String {
        self.config.key.clone()
    }

    fn r#type(&self) -> ProviderType {
        ProviderType::OpenAI
    }

    async fn chat_completion(
        &self,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse> {
        // Validate reasoning config if present
        if let Some(reasoning) = &request.reasoning {
            if let Err(e) = reasoning.validate() {
                tracing::error!("Invalid reasoning config: {}", e);
                return Err(SdkError::Other(anyhow::anyhow!(
                    "Invalid reasoning config: {}",
                    e
                )));
            }
        }

        // Convert to OpenAI-specific request format
        let openai_request = OpenAIChatCompletionRequest::from(request.clone());

        let response = self
            .http_client
            .post(format!("{}/chat/completions", self.base_url()))
            .header("Authorization", format!("Bearer {}", self.config.api_key))
            .header("Content-Type", "application/json")
            .json(&openai_request)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("OpenAI API request error: {}", e);
                SdkError::Other(anyhow::anyhow!("OpenAI API request failed: {}", e))
            })?;

        let status = response.status();
        if status.is_success() {
            if request.stream.unwrap_or(false) {
                // For now, return an error for streaming since SSE parsing is complex
                // This removes the panic but indicates streaming isn't yet supported
                return Err(SdkError::Other(anyhow::anyhow!(
                    "Streaming not yet implemented for OpenAI - use stream: false"
                )));
            } else {
                let completion: ChatCompletion = response.json().await.map_err(|e| {
                    tracing::error!("OpenAI API response parsing error: {}", e);
                    SdkError::Other(anyhow::anyhow!("Failed to parse OpenAI response: {}", e))
                })?;

                Ok(ChatCompletionResponse::NonStream(completion))
            }
        } else {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            tracing::error!("OpenAI API error ({}): {}", status, error_text);
            Err(SdkError::Other(anyhow::anyhow!(
                "OpenAI API error ({}): {}",
                status,
                error_text
            )))
        }
    }

    async fn completion(&self, request: CompletionRequest) -> Result<CompletionResponse> {
        let response = self
            .http_client
            .post(format!("{}/completions", self.base_url()))
            .header("Authorization", format!("Bearer {}", self.config.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("OpenAI completions API request error: {}", e);
                SdkError::Other(anyhow::anyhow!(
                    "OpenAI completions API request failed: {}",
                    e
                ))
            })?;

        let status = response.status();
        if status.is_success() {
            let completion: CompletionResponse = response.json().await.map_err(|e| {
                tracing::error!("OpenAI completions API response parsing error: {}", e);
                SdkError::Other(anyhow::anyhow!(
                    "Failed to parse OpenAI completions response: {}",
                    e
                ))
            })?;

            Ok(completion)
        } else {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            tracing::error!("OpenAI completions API error ({}): {}", status, error_text);
            Err(SdkError::Other(anyhow::anyhow!(
                "OpenAI completions API error ({}): {}",
                status,
                error_text
            )))
        }
    }

    async fn embeddings(&self, request: EmbeddingsRequest) -> Result<EmbeddingsResponse> {
        let response = self
            .http_client
            .post(format!("{}/embeddings", self.base_url()))
            .header("Authorization", format!("Bearer {}", self.config.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("OpenAI embeddings API request error: {}", e);
                SdkError::Other(anyhow::anyhow!(
                    "OpenAI embeddings API request failed: {}",
                    e
                ))
            })?;

        let status = response.status();
        if status.is_success() {
            let embeddings: EmbeddingsResponse = response.json().await.map_err(|e| {
                tracing::error!("OpenAI embeddings API response parsing error: {}", e);
                SdkError::Other(anyhow::anyhow!(
                    "Failed to parse OpenAI embeddings response: {}",
                    e
                ))
            })?;

            Ok(embeddings)
        } else {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            tracing::error!("OpenAI embeddings API error ({}): {}", status, error_text);
            Err(SdkError::Other(anyhow::anyhow!(
                "OpenAI embeddings API error ({}): {}",
                status,
                error_text
            )))
        }
    }

    async fn health_check(&self) -> Result<()> {
        // Simple health check by calling the models endpoint
        let response = self
            .http_client
            .get(format!("{}/models", self.base_url()))
            .header("Authorization", format!("Bearer {}", self.config.api_key))
            .send()
            .await
            .map_err(|e| SdkError::Other(anyhow::anyhow!("OpenAI health check failed: {}", e)))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(SdkError::Other(anyhow::anyhow!(
                "OpenAI health check failed with status: {}",
                response.status()
            )))
        }
    }
}
