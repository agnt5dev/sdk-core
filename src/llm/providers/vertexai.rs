// Google Vertex AI provider implementation
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::super::models::{
    ChatChoice, ChatCompletion, ChatCompletionRequest, ChatCompletionResponse, ChatMessage,
    ChatMessageContent, CompletionRequest, CompletionResponse, EmbeddingsRequest,
    EmbeddingsResponse, Usage,
};
use super::super::provider::{Provider, ProviderConfig, ProviderType};
use crate::error::{Result, SdkError};
use chrono;
use uuid;

/// Gemini API request format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeminiRequest {
    contents: Vec<GeminiContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    generation_config: Option<GeminiGenerationConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeminiContent {
    role: String,
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeminiPart {
    text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeminiGenerationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "maxOutputTokens")]
    max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "topP")]
    top_p: Option<f32>,
}

/// Gemini API response format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeminiResponse {
    candidates: Vec<GeminiCandidate>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "usageMetadata")]
    usage_metadata: Option<GeminiUsage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeminiCandidate {
    content: GeminiContent,
    #[serde(skip_serializing_if = "Option::is_none", rename = "finishReason")]
    finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeminiUsage {
    #[serde(rename = "promptTokenCount")]
    prompt_token_count: u32,
    #[serde(rename = "candidatesTokenCount")]
    candidates_token_count: u32,
    #[serde(rename = "totalTokenCount")]
    total_token_count: u32,
}

pub struct VertexAIProvider {
    config: ProviderConfig,
    http_client: Client,
}

impl VertexAIProvider {
    pub fn new(config: &ProviderConfig) -> Self {
        Self {
            config: config.clone(),
            http_client: Client::new(),
        }
    }

    fn project_id(&self) -> Result<String> {
        self.config
            .get_param("project_id")
            .ok_or_else(|| {
                SdkError::Other(anyhow::anyhow!("Google Cloud project ID not configured"))
            })
            .map(|s| s.clone())
    }

    fn location(&self) -> String {
        self.config
            .get_param("location")
            .unwrap_or(&"us-central1".to_string())
            .clone()
    }

    fn convert_to_gemini_request(&self, request: ChatCompletionRequest) -> Result<GeminiRequest> {
        let mut contents = Vec::new();

        for message in request.messages {
            let role = match message.role.as_str() {
                "user" => "user",
                "assistant" => "model",
                "system" => "user", // Gemini treats system messages as user messages
                _ => "user",        // Default fallback
            };

            if let Some(content) = message.content {
                let text = match content {
                    ChatMessageContent::String(text) => text,
                    ChatMessageContent::Array(parts) => {
                        // For now, just concatenate text parts
                        parts
                            .into_iter()
                            .filter_map(|part| part.text)
                            .collect::<Vec<_>>()
                            .join("\n")
                    }
                };

                contents.push(GeminiContent {
                    role: role.to_string(),
                    parts: vec![GeminiPart { text }],
                });
            }
        }

        let generation_config = if request.temperature.is_some()
            || request.max_tokens.is_some()
            || request.max_completion_tokens.is_some()
            || request.top_p.is_some()
        {
            Some(GeminiGenerationConfig {
                temperature: request.temperature,
                max_output_tokens: request.max_completion_tokens.or(request.max_tokens),
                top_p: request.top_p,
            })
        } else {
            None
        };

        Ok(GeminiRequest {
            contents,
            generation_config,
        })
    }

    fn convert_from_gemini_response(
        &self,
        response: GeminiResponse,
        model: &str,
    ) -> Result<ChatCompletion> {
        let candidate =
            response.candidates.into_iter().next().ok_or_else(|| {
                SdkError::Other(anyhow::anyhow!("No candidates in Gemini response"))
            })?;

        let content = candidate
            .content
            .parts
            .into_iter()
            .map(|part| part.text)
            .collect::<Vec<_>>()
            .join("");

        let choice = ChatChoice {
            index: 0,
            message: ChatMessage {
                role: "assistant".to_string(),
                content: Some(ChatMessageContent::String(content)),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                refusal: None,
            },
            finish_reason: candidate.finish_reason,
            logprobs: None,
        };

        let usage = response
            .usage_metadata
            .map(|u| Usage {
                prompt_tokens: u.prompt_token_count,
                completion_tokens: u.candidates_token_count,
                total_tokens: u.total_token_count,
                completion_tokens_details: None,
                prompt_tokens_details: None,
            })
            .unwrap_or_default();

        Ok(ChatCompletion {
            id: format!("gemini-{}", uuid::Uuid::new_v4()),
            object: Some("chat.completion".to_string()),
            created: Some(chrono::Utc::now().timestamp() as u64),
            model: model.to_string(),
            choices: vec![choice],
            usage,
            system_fingerprint: None,
        })
    }
}

#[async_trait]
impl Provider for VertexAIProvider {
    fn key(&self) -> String {
        self.config.key.clone()
    }

    fn r#type(&self) -> ProviderType {
        ProviderType::VertexAI
    }

    async fn chat_completion(
        &self,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse> {
        // Check for streaming - not yet implemented
        if request.stream.unwrap_or(false) {
            return Err(SdkError::Other(anyhow::anyhow!(
                "Streaming not yet implemented for VertexAI - use stream: false"
            )));
        }

        // Extract the model from the request BEFORE converting it
        let model = request.model.clone();

        // Convert to Gemini API format
        let gemini_request = self.convert_to_gemini_request(request)?;

        // Use public Gemini API (simpler than Vertex AI endpoint)
        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
            model, self.config.api_key
        );

        let response = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&gemini_request)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Google Gemini API request error: {}", e);
                SdkError::Other(anyhow::anyhow!("Google Gemini API request failed: {}", e))
            })?;

        let status = response.status();
        if status.is_success() {
            let gemini_response: GeminiResponse = response.json().await.map_err(|e| {
                tracing::error!("Google Gemini API response parsing error: {}", e);
                SdkError::Other(anyhow::anyhow!(
                    "Failed to parse Google Gemini response: {}",
                    e
                ))
            })?;

            let completion = self.convert_from_gemini_response(gemini_response, &model)?;
            Ok(ChatCompletionResponse::NonStream(completion))
        } else {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            tracing::error!("Google Gemini API error ({}): {}", status, error_text);
            Err(SdkError::Other(anyhow::anyhow!(
                "Google Gemini API error ({}): {}",
                status,
                error_text
            )))
        }
    }

    async fn completion(&self, _request: CompletionRequest) -> Result<CompletionResponse> {
        // Gemini API doesn't support legacy completions - use chat completions instead
        Err(SdkError::Other(anyhow::anyhow!(
            "Legacy completions not supported by Gemini API - use chat completions instead"
        )))
    }

    async fn embeddings(&self, _request: EmbeddingsRequest) -> Result<EmbeddingsResponse> {
        // Gemini API embeddings would require a different endpoint
        // For now, recommend using a dedicated embedding service
        Err(SdkError::Other(anyhow::anyhow!(
            "Embeddings not yet implemented for VertexAI - use a dedicated embedding provider"
        )))
    }

    async fn health_check(&self) -> Result<()> {
        // Simple health check - try to get models list
        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models?key={}",
            self.config.api_key
        );

        let response = self.http_client.get(&url).send().await.map_err(|e| {
            tracing::error!("Google Gemini API health check request error: {}", e);
            SdkError::Other(anyhow::anyhow!(
                "Google Gemini API health check failed: {}",
                e
            ))
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
                "Google Gemini API health check failed ({}): {}",
                status,
                error_text
            );
            Err(SdkError::Other(anyhow::anyhow!(
                "Google Gemini API health check failed ({}): {}",
                status,
                error_text
            )))
        }
    }
}
