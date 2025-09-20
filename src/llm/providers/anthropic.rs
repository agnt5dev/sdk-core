// Anthropic provider implementation adapted from Hub
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::error::{Result, SdkError};
use super::super::provider::{Provider, ProviderType, ProviderConfig};
use super::super::models::{
    ChatCompletionRequest, ChatCompletionResponse, ChatCompletion, ChatChoice, ChatMessage, ChatMessageContent,
    CompletionRequest, CompletionResponse,
    EmbeddingsRequest, EmbeddingsResponse,
    Usage,
};

/// Anthropic-specific message format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnthropicMessage {
    role: String,
    content: String,
}

/// Anthropic-specific chat completion request
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnthropicChatCompletionRequest {
    model: String,
    max_tokens: u32,
    messages: Vec<AnthropicMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stop_sequences: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

/// Anthropic-specific response format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnthropicChatCompletionResponse {
    id: String,
    #[serde(rename = "type")]
    response_type: String,
    role: String,
    content: Vec<AnthropicContentBlock>,
    model: String,
    stop_reason: Option<String>,
    stop_sequence: Option<String>,
    usage: AnthropicUsage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnthropicContentBlock {
    #[serde(rename = "type")]
    content_type: String,
    text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnthropicUsage {
    input_tokens: u32,
    output_tokens: u32,
}

/// Anthropic streaming event types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum AnthropicStreamEvent {
    #[serde(rename = "message_start")]
    MessageStart { message: AnthropicStreamMessage },
    #[serde(rename = "content_block_start")]
    ContentBlockStart { index: u32, content_block: AnthropicContentBlock },
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta { index: u32, delta: AnthropicContentDelta },
    #[serde(rename = "content_block_stop")]
    ContentBlockStop { index: u32 },
    #[serde(rename = "message_delta")]
    MessageDelta { delta: AnthropicMessageDelta, usage: Option<AnthropicUsage> },
    #[serde(rename = "message_stop")]
    MessageStop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnthropicStreamMessage {
    id: String,
    #[serde(rename = "type")]
    message_type: String,
    role: String,
    content: Vec<serde_json::Value>,
    model: String,
    usage: AnthropicUsage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnthropicContentDelta {
    #[serde(rename = "type")]
    delta_type: String,
    text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnthropicMessageDelta {
    stop_reason: Option<String>,
    stop_sequence: Option<String>,
}

impl From<ChatCompletionRequest> for AnthropicChatCompletionRequest {
    fn from(request: ChatCompletionRequest) -> Self {
        let mut system_message = None;
        let mut messages = Vec::new();

        // Extract system message and convert others
        for message in request.messages {
            match message.role.as_str() {
                "system" => {
                    if let Some(ChatMessageContent::String(content)) = message.content {
                        // Prepend thinking prompt if reasoning is configured
                        let content = if let Some(reasoning) = &request.reasoning {
                            if let Some(thinking_prompt) = reasoning.to_thinking_prompt() {
                                format!("{}\n\n{}", thinking_prompt, content)
                            } else {
                                content
                            }
                        } else {
                            content
                        };
                        system_message = Some(content);
                    }
                }
                "user" | "assistant" => {
                    if let Some(content) = message.content {
                        let content_str = match content {
                            ChatMessageContent::String(text) => text,
                            ChatMessageContent::Array(parts) => {
                                // For now, just concatenate text parts
                                parts.into_iter()
                                    .filter_map(|part| part.text)
                                    .collect::<Vec<_>>()
                                    .join("\n")
                            }
                        };
                        messages.push(AnthropicMessage {
                            role: message.role,
                            content: content_str,
                        });
                    }
                }
                _ => {
                    // Skip other roles like "tool" for now
                    tracing::warn!("Skipping unsupported message role: {}", message.role);
                }
            }
        }

        // Use max_completion_tokens if available, otherwise default to 1024
        let max_tokens = request.max_completion_tokens
            .or(request.max_tokens)
            .unwrap_or(1024);

        Self {
            model: request.model,
            max_tokens,
            messages,
            system: system_message,
            temperature: request.temperature,
            top_p: request.top_p,
            stop_sequences: request.stop,
            stream: request.stream,
        }
    }
}

impl From<AnthropicChatCompletionResponse> for ChatCompletion {
    fn from(response: AnthropicChatCompletionResponse) -> Self {
        let content = response.content
            .into_iter()
            .map(|block| block.text)
            .collect::<Vec<_>>()
            .join("");

        let choice = ChatChoice {
            index: 0,
            message: ChatMessage {
                role: response.role,
                content: Some(ChatMessageContent::String(content)),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                refusal: None,
            },
            finish_reason: response.stop_reason,
            logprobs: None,
        };

        let usage = Usage::new(response.usage.input_tokens, response.usage.output_tokens);

        Self {
            id: response.id,
            object: Some("chat.completion".to_string()),
            created: Some(chrono::Utc::now().timestamp() as u64),
            model: response.model,
            choices: vec![choice],
            usage,
            system_fingerprint: None,
        }
    }
}

pub struct AnthropicProvider {
    config: ProviderConfig,
    http_client: Client,
}

impl AnthropicProvider {
    pub fn new(config: &ProviderConfig) -> Self {
        Self {
            config: config.clone(),
            http_client: Client::new(),
        }
    }

    fn base_url(&self) -> String {
        self.config
            .get_param("base_url")
            .unwrap_or(&"https://api.anthropic.com/v1".to_string())
            .clone()
    }
}

#[async_trait]
impl Provider for AnthropicProvider {
    fn key(&self) -> String {
        self.config.key.clone()
    }

    fn r#type(&self) -> ProviderType {
        ProviderType::Anthropic
    }

    async fn chat_completion(
        &self,
        request: ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse> {
        // Validate reasoning config if present
        if let Some(reasoning) = &request.reasoning {
            if let Err(e) = reasoning.validate() {
                tracing::error!("Invalid reasoning config: {}", e);
                return Err(SdkError::Other(anyhow::anyhow!("Invalid reasoning config: {}", e)));
            }

            if let Some(max_tokens) = reasoning.max_tokens {
                tracing::info!("✅ Anthropic reasoning enabled with max_tokens: {}", max_tokens);
            } else if let Some(thinking_prompt) = reasoning.to_thinking_prompt() {
                tracing::info!(
                    "✅ Anthropic reasoning enabled with effort level: {:?} -> prompt: \"{}\"",
                    reasoning.effort,
                    thinking_prompt.chars().take(50).collect::<String>() + "..."
                );
            }
        }

        let anthropic_request = AnthropicChatCompletionRequest::from(request);

        let response = self
            .http_client
            .post(format!("{}/messages", self.base_url()))
            .header("x-api-key", &self.config.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("Content-Type", "application/json")
            .json(&anthropic_request)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Anthropic API request error: {}", e);
                SdkError::Other(anyhow::anyhow!("Anthropic API request failed: {}", e))
            })?;

        let status = response.status();
        if status.is_success() {
            if anthropic_request.stream.unwrap_or(false) {
                // Anthropic streaming requires complex SSE parsing and conversion
                // For now, return an error instead of panicking
                return Err(SdkError::Other(anyhow::anyhow!(
                    "Streaming not yet implemented for Anthropic - use stream: false"
                )));
            } else {
                let anthropic_response: AnthropicChatCompletionResponse = response
                    .json()
                    .await
                    .map_err(|e| {
                        tracing::error!("Anthropic API response parsing error: {}", e);
                        SdkError::Other(anyhow::anyhow!("Failed to parse Anthropic response: {}", e))
                    })?;

                Ok(ChatCompletionResponse::NonStream(anthropic_response.into()))
            }
        } else {
            let error_text = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
            tracing::error!("Anthropic API error ({}): {}", status, error_text);
            Err(SdkError::Other(anyhow::anyhow!(
                "Anthropic API error ({}): {}",
                status,
                error_text
            )))
        }
    }

    async fn completion(
        &self,
        _request: CompletionRequest,
    ) -> Result<CompletionResponse> {
        // Anthropic doesn't support the legacy completions endpoint
        Err(SdkError::Other(anyhow::anyhow!(
            "Anthropic does not support the legacy completions API. Use chat_completion instead."
        )))
    }

    async fn embeddings(
        &self,
        _request: EmbeddingsRequest,
    ) -> Result<EmbeddingsResponse> {
        // Anthropic doesn't provide embeddings API
        Err(SdkError::Other(anyhow::anyhow!(
            "Anthropic does not provide an embeddings API"
        )))
    }

    async fn health_check(&self) -> Result<()> {
        // Anthropic doesn't have a specific health endpoint, so we'll make a minimal request
        let minimal_request = AnthropicChatCompletionRequest {
            model: "claude-3-haiku-20240307".to_string(),
            max_tokens: 1,
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: "Hi".to_string(),
            }],
            system: None,
            temperature: None,
            top_p: None,
            stop_sequences: None,
            stream: Some(false),
        };

        let response = self
            .http_client
            .post(format!("{}/messages", self.base_url()))
            .header("x-api-key", &self.config.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("Content-Type", "application/json")
            .json(&minimal_request)
            .send()
            .await
            .map_err(|e| {
                SdkError::Other(anyhow::anyhow!("Anthropic health check failed: {}", e))
            })?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(SdkError::Other(anyhow::anyhow!(
                "Anthropic health check failed with status: {}",
                response.status()
            )))
        }
    }
}