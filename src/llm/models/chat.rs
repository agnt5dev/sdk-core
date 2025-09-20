// Chat completion models extracted and adapted from Hub
use serde::{Deserialize, Serialize};
use super::{ToolDefinition, ToolCall, ToolChoice, ResponseFormat, Usage};

/// Configuration for reasoning/thinking mode (o1-style models)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReasoningConfig {
    /// Reasoning effort level: "low", "medium", "high"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,

    /// Alternative to effort - specify max reasoning tokens
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,

    /// Whether to exclude reasoning from response (default: false)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude: Option<bool>,
}

impl ReasoningConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.effort.is_some() && self.max_tokens.is_some() {
            tracing::warn!("Both effort and max_tokens specified - prioritizing max_tokens");
        }

        // Only validate effort if max_tokens is not present
        if let Some(effort) = &self.effort {
            if effort.trim().is_empty() {
                if self.max_tokens.is_none() {
                    return Err("Effort cannot be empty string".to_string());
                }
            } else if self.max_tokens.is_none()
                && !["low", "medium", "high"].contains(&effort.as_str())
            {
                return Err("Invalid effort value. Must be 'low', 'medium', or 'high'".to_string());
            }
        }

        Ok(())
    }

    /// For OpenAI/Azure - Direct passthrough (but prioritize max_tokens over effort)
    pub fn to_openai_effort(&self) -> Option<String> {
        if self.max_tokens.is_some() {
            None
        } else {
            self.effort
                .as_ref()
                .filter(|e| !e.trim().is_empty())
                .cloned()
        }
    }

    /// For Vertex AI (Gemini) - Use max_tokens directly
    pub fn to_gemini_thinking_budget(&self) -> Option<i32> {
        self.max_tokens.map(|tokens| tokens as i32)
    }

    /// For Anthropic/Bedrock - Custom prompt generation
    pub fn to_thinking_prompt(&self) -> Option<String> {
        if self.max_tokens.is_some() {
            Some("Think through this step-by-step with detailed reasoning.".to_string())
        } else {
            match self.effort.as_deref() {
                Some(effort) if !effort.trim().is_empty() => match effort {
                    "high" => {
                        Some("Think through this step-by-step with detailed reasoning.".to_string())
                    }
                    "medium" => Some("Consider this problem thoughtfully.".to_string()),
                    "low" => Some("Think about this briefly.".to_string()),
                    _ => None,
                },
                _ => None,
            }
        }
    }
}

/// Message content can be either string or array of content parts
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ChatMessageContent {
    String(String),
    Array(Vec<ContentPart>),
}

/// Individual content part for multimodal messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentPart {
    pub r#type: String, // "text", "image_url", etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_url: Option<ImageUrl>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageUrl {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>, // "low", "high", "auto"
}

/// A single chat message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String, // "system", "user", "assistant", "tool"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<ChatMessageContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
}

/// Chat completion request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,

    // Generation parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Vec<String>>,

    // Token limits
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u32>,

    // Penalties
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,

    // Tools and functions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,

    // Response format
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ResponseFormat>,

    // Advanced features
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ReasoningConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_logprobs: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// A single choice in the chat completion response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatChoice {
    pub index: u32,
    pub message: ChatMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<serde_json::Value>,
}

/// Complete chat completion response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletion {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<u64>,
    pub model: String,
    pub choices: Vec<ChatChoice>,
    pub usage: Usage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_fingerprint: Option<String>,
}

/// Chat completion response (streaming or non-streaming)
pub enum ChatCompletionResponse {
    NonStream(ChatCompletion),
    Stream(Box<dyn futures::Stream<Item = Result<super::streaming::ChatCompletionChunk, crate::error::SdkError>> + Send + Unpin>),
}

impl std::fmt::Debug for ChatCompletionResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChatCompletionResponse::NonStream(completion) => {
                f.debug_tuple("NonStream").field(completion).finish()
            }
            ChatCompletionResponse::Stream(_) => {
                f.debug_tuple("Stream").field(&"<stream>").finish()
            }
        }
    }
}

impl ChatCompletionResponse {
    pub fn is_stream(&self) -> bool {
        matches!(self, ChatCompletionResponse::Stream(_))
    }
}